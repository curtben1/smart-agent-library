// program to watch a shared yjs document and if it sees its own id in the document with a task, 
// download that task, edit the yjs to show the current state and upload the finished data once complete

import grpc from "@grpc/grpc-js";
// @ts-expect-error - Using JS module without types
import { VoltClient } from "@tdxvolt/volt-client-grpc";
import * as Y from "yjs";
import { v4 as uuidv4, v4 } from "uuid";
import { sign, verify } from "verifiable-credential-toolkit";
import winston, { log } from 'winston';
import { Sign } from "crypto";
import { SignedTaskCredential } from "./types/shared.types";
import { TaskMetadata } from "./types/config.types";
import { PublishWireRequest, PublishWireResponse, Resource, SaveResourceRequest, Status, SubscribeWireResponse } from "./types/volt.types";
import { cli } from "winston/lib/winston/config";
import { writeFieldToSynapseSubdoc } from "./agent_schemas.js";
import { EXTERNAL_PUMP_TASK_SCHEMA, SYNAPSE_ID } from "./agent_schemas.js";
// @ts-ignore
import { YArray } from "yjs/dist/src/internals";

const mapname = "GENERIC_MAP_NAME";


// Self-contained logger configuration
const logger = winston.createLogger({
    level: process.env.LOG_LEVEL || 'info',
    format: winston.format.combine(
        winston.format.timestamp(),
        winston.format.errors({ stack: true }),
        winston.format.json(),
        winston.format.printf(({ timestamp, level, message, ...meta }) => {
            const metaStr = Object.keys(meta).length ? ` ${JSON.stringify(meta)}` : '';
            return `${timestamp} [${level.toUpperCase()}]: ${message}${metaStr}`;
        })
    ),
    transports: [
        new winston.transports.Console({
            format: winston.format.combine(
                winston.format.colorize(),
                winston.format.simple()
            )
        }),
        new winston.transports.File({
            filename: 'smart_agent.log',
            maxsize: 5242880, // 5MB
            maxFiles: 5,
            tailable: true
        })
    ],
    exceptionHandlers: [
        new winston.transports.File({ filename: 'exceptions.log' })
    ],
    rejectionHandlers: [
        new winston.transports.File({ filename: 'rejections.log' })
    ]
});

// This will only show warn and error logs
logger.level = 'info';


/**
 * @typedef {Object} WireSubscription
 * @property {(callback: function(string, Array<string>, Error): void)} onData 
 * Add a callback to be called when new data arrives
 *  - arg0: chunk - The data chunk received
 *  - arg1: allChunks - All data chunks received so far
 *  - arg2: error - Any error that occurred during data reception
 * @property {() => Array<string>} getAllData - Get all data chunks received so far
 * @property {() => void} close - Close the wire subscription
 */

var voltClient: VoltClient;

/**
 * Initialises the Volt client with the provided configuration.
 * @param {string} voltConfig The path to the Volt configuration file 
 * @returns {Promise<VoltClient>} Resolves with the initialised Volt client.
 */
export async function getAndInitialiseVoltClient(voltConfig: string): Promise<VoltClient> {
    logger.info("initialising Volt client");
    voltClient = new VoltClient(grpc);
    await voltClient.initialise(voltConfig);
    return voltClient;

}





interface WireSubscription {
    onData: (callback: Function) => () => boolean;
    onError: (callback: Function) => () => boolean;
    getAllData: () => Buffer[];
    close: () => void;
}


//TODO: continue to think about moving this to a generator
/**
 * Subscribe to a wire and return an interface for handling incoming data
 * @param {string} wireName - The ID of the task/wire to subscribe to
 * @returns An object with the following methods to handle incoming data.
 * - onData: Add a callback to be called when new data arrives
 * - onError: Add a callback to be called when an error occurs
 * - getAllData: Get all data chunks received so far
 * 
 */
export async function subscribeToWire(wireName: string): Promise<WireSubscription> {
    logger.info("in SUBSCRIBE TO WIRE");
    try {
        const chunks: Buffer[] = [];
        const callbacks: Set<Function> = new Set();
        const errorCallbacks: Set<Function> = new Set();

        const canAccess = await voltClient.CanAccessResource({ resource_id: wireName, access: "read" }).catch((error: Error) => {
            logger.error("Error checking access to wire resource: %o", error);
        });
        if (![1, 2, 6, 7, "POLICY_DECISION_PERMIT"].includes(canAccess.decision)) {
            console.warn(`Access denied to wire resource ${wireName}. Decision: ${canAccess.decision} \n This process will be retried, expect some warnings in the logs while waiting for retries`);
            // throw new Error(`Access denied to wire resource ${wireName}. Decision: ${canAccess.decision}`);
        }
        else {
            console.log(`Access granted to wire resource ${wireName}. Decision: ${canAccess.decision}`);
        }
        const wireStream = await voltClient.SubscribeWire({ wire_id: wireName });
        logger.info("wire subscribed for wireName: %s", wireName);

        wireStream.on("data", (data: SubscribeWireResponse) => {
            // logger.info("wire data received: %s", data.chunk);
            if (!data.chunk) {
                logger.warn("Received data event with no chunk");
                return;
            }
            chunks.push(data.chunk);

            callbacks.forEach(callback => {
                try {
                    callback(data.chunk, chunks);
                } catch (callbackError) {
                    logger.error("Error in wire data callback: %o", callbackError);
                }
            });
        });

        wireStream.on("error", (error: Error) => {

            logger.warn(`Error in wire stream for wireName ${wireName}: ${error} - you can ignore this safely unless other breakages occur, likely due to wire creation delays`);
            if (errorCallbacks.size > 0) {
                errorCallbacks.forEach(errorCallback => errorCallback(error));
            } else {
                logger.error("Unhandled wire stream error: %o", error);
            }
        });

        const wireInterface = {
            onData: (callback: Function) => {
                callbacks.add(callback);
                return () => callbacks.delete(callback);
            },
            onError: (callback: Function) => {
                errorCallbacks.add(callback);
                return () => errorCallbacks.delete(callback);
            },
            getAllData: () => [...chunks],
            close: () => {
                if (typeof wireStream.destroy === 'function') {
                    wireStream.destroy();
                } else if (typeof wireStream.end === 'function') {
                    wireStream.end();
                } else if (typeof wireStream.cancel === 'function') {
                    wireStream.cancel();
                }
                callbacks.clear();
                errorCallbacks.clear();

            }
        }

        return wireInterface
    } catch (error) {
        logger.error("Error subscribing to wire: %o", error);
        throw error;
    }
}

/**
 * Function to initialise the sub-documents used by the agent in the yjs, in theory should all be done by the host
 */
export function initialiseSubDocs(agentStateMap: Y.Map<Y.Doc>) {
    if (!agentStateMap.has("pythonTaskList")) {
        agentStateMap.set("pythonTaskList", new Y.Doc());
    }

    if (!agentStateMap.has("nodeTaskList")) {
        agentStateMap.set("nodeTaskList", new Y.Doc());
    }

    if (!agentStateMap.has("agentList")) {
        agentStateMap.set("agentList", new Y.Doc());
    }

    if (!agentStateMap.has("taskOutputs")) {
        agentStateMap.set("taskOutputs", new Y.Doc());
    }
}


/**
 * @param {Y.Doc} subdoc - The subdocument you want the map from 
 */
function getMapFromSubDoc<T = unknown>(subdoc: Y.Doc): Y.Map<T> {
    subdoc.load();
    return subdoc.getMap(mapname);
}

/**
 * puts an install command onto the taskList.
 * @param {string} taskID - The unique ID for the task.
 * @param {string} taskName - The name of the task to be installed.
 * @param {string} taskLocation - The location of the task, typically a URL or martketplace uuid TODO: waiting on Toby file size limitation check
 * @param {Y.Doc} taskList - The Yjs map to store tasks and their credentials.
 */
export function installTask(taskID: string, taskName: string, taskLocation: string, sourceType: string, taskList: Y.Doc, execute_after_timestamp_ms?: number) {

    const taskVC = create_signed_task({ "task-id": taskID, "action": "new-task", "name": taskName, "location": taskLocation, source: sourceType, ...(execute_after_timestamp_ms ? { execute_after_timestamp_ms: execute_after_timestamp_ms } : {}) });
    // getMapFromSubDoc(taskList).set(taskID, { credential: taskVC });
    writeFieldToSynapseSubdoc(voltClient, taskID, { credential: taskVC }, taskList.guid, mapname);
}

interface Synapse_Write_Object { synapse_id: string, document_id: string, path: string }

interface StartTaskInputArgs { taskID: string; taskList: Y.Doc; std_in?: object; cli_args?: string, continuous?: boolean, outer_output_pump_location?: string, synapse_write_path?: Synapse_Write_Object, execute_after_timestamp_ms?: number }

/**
 * @deprecated
 * @param taskID - The unique ID for the task.
 * @param taskList - The Yjs map to store tasks and their credentials.
 * Creates and adds a signed start task to the task list.
 */
export function startTask(taskID: string, taskList: Y.Doc): void;
/**
 * @deprecated
 * Deprecated: Due to ambiguity, use startTaskWithCliArgs instead.
 * Creates and adds a signed start task to the task list.
 * @param  taskID - The unique ID for the task.
 * @param taskList - The Yjs map to store tasks and their credentials.
 * @param  cli_args - Command-line arguments to pass to the agent.
 */
export function startTask(taskID: string, taskList: Y.Doc, cli_args: string): void;
/**
 * Creates and adds a signed start task to the task list.
 * @param input_args - Object containing taskID, taskList, and optional std_in/cli_args.
 */
export function startTask(input_args: StartTaskInputArgs): void;
export function startTask( //TODO: add in translation schemas somehow
    taskIDOrArgs: string | StartTaskInputArgs,
    taskList?: Y.Doc,
    cli_args?: string
): void {
    // Handle legacy signature
    if (typeof taskIDOrArgs === 'string') {
        const taskDetails: SignedTaskCredential["credentialSubject"] = { "task-id": taskIDOrArgs, "action": "run-task", "continuous": false };
        cli_args ? taskDetails["cli_args"] = cli_args : null;
        const taskVC2 = create_signed_task(taskDetails);
        // getMapFromSubDoc(taskList!).set(v4(), { credential: taskVC2 });
        if (taskList) {

            writeFieldToSynapseSubdoc(voltClient, v4(), { credential: taskVC2 }, taskList.guid, mapname)
        }

        return;
    }

    // Handle new object signature
    const { taskID, taskList: tl, std_in, cli_args: ca, continuous, outer_output_pump_location, synapse_write_path, execute_after_timestamp_ms } = taskIDOrArgs;
    let taskVC2: SignedTaskCredential;
    if (outer_output_pump_location) {
        logger.warn("This form of custom location will be deprecated once the volt schema issues are resolved, synapse_write_path is preferred and will be the primary future method.")

    }
    const baseTask: SignedTaskCredential["credentialSubject"] = {
        "task-id": taskID,
        action: "run-task",
        ...(continuous !== undefined ? { continuous } : {}),
        ...(outer_output_pump_location ? { outer_output_pump_location } : {}),
        ...(synapse_write_path ? { synapse_write_path } : {}),
        ...(execute_after_timestamp_ms ? { execute_after_timestamp_ms: execute_after_timestamp_ms } : {})
    };

    if (ca && std_in) {
        taskVC2 = create_signed_task({ ...baseTask, cli_args: ca, std_in });
    } else if (ca) {
        taskVC2 = create_signed_task({ ...baseTask, cli_args: ca });
    } else if (std_in) {
        taskVC2 = create_signed_task({ ...baseTask, std_in });
    } else {
        taskVC2 = create_signed_task(baseTask);
    }

    writeFieldToSynapseSubdoc(voltClient, v4(), { credential: taskVC2 }, tl.guid, mapname)
}

export function startTaskWithStdin(taskID: string, taskList: Y.Doc, std_in: object) {
    startTask({ taskID: taskID, taskList: taskList, std_in: std_in });

}
/**
 * Creates and adds a signed start task to the task list. runtask
 * @param  taskID - The unique ID for the task.
 * @param taskList - The Yjs map to store tasks and their credentials.
 * @param  cli_args - Command-line arguments to pass to the agent.
 */
export function startTaskWithCliArgs(taskID: string, taskList: Y.Doc, cli_args: string) {
    startTask({ taskID: taskID, taskList: taskList, cli_args: cli_args });
}



/**
 * Waits for a task to finish.
 * @param {string} taskID - The unique ID for the task. 
 * @returns {Promise<void>} - Resolves when the task is finished, rejects on error.
 */
export async function waitForTaskFinished(taskID: string): Promise<void> {
    const indicator = `task-finished-${taskID}`;
    const watchStream = await voltClient.WatchSynapsePath({
        start: {
            synapse_id: SYNAPSE_ID,
            document_id: `wire-${taskID}`,
            path: [`$.${mapname}.messages`],
        },
    });

    return new Promise<void>((resolve, reject) => {
        let done = false;

        const cleanup = () => {
            watchStream.off?.("data", onData);
            watchStream.off?.("end", onEnd);
            watchStream.off?.("error", onError);
            watchStream.destroy?.();
        };

        const finish = (err?: unknown) => {
            if (done) return;
            done = true;
            cleanup();
            err ? reject(err) : resolve();
        };

        const onData = (msg: WireMessage) => {
            logger.info(`the message is ${JSON.stringify(msg)}`);
            if (msg.update) {
                const msgValue = JSON.parse(msg.update.value).message;
                if (msgValue.includes(indicator))
                    finish();
            }

        };

        const onEnd = () => finish(new Error(`Watch ended before ${indicator}`));
        const onError = (err: unknown) => finish(err);


        watchStream.on("data", onData);
        watchStream.once("end", onEnd);
        watchStream.once("error", onError);
    });
}

type WireMessage = {
    update: { value: string }, status: object
}


/**
 * Creates and adds a signed uninstall task to the task list.
 * @param {string} taskID - The unique ID for the task.
 * @param {Y.Doc} taskList - The Yjs map to store tasks and their credentials.
 */
export function uninstallTask(taskID: string, taskList: Y.Doc, execute_after_timestamp_ms?: number) {
    const taskVC3 = create_signed_task({ "task-id": taskID, "action": "uninstall-task", ...(execute_after_timestamp_ms ? { execute_after_timestamp_ms: execute_after_timestamp_ms } : {}) });
    // getMapFromSubDoc(taskList).set(v4(), { credential: taskVC3 });
    writeFieldToSynapseSubdoc(voltClient, v4(), { credential: taskVC3 }, taskList.guid, mapname)

}




/**
 * Requests and retrieves metadata for a task.
 * @param {string} taskID - The unique ID for the task.
 * @param {Y.Doc} taskList - The Yjs map to store tasks and their credentials.
 * @returns {Promise<TaskMetadata>} - Resolves with the metadata object for the task.
 */
export async function getTaskMetadata(taskID: string, rootDoc: Y.Doc): Promise<TaskMetadata> {
    logger.info(`Getting metadata for taskID: ${taskID}`);
    const ymap = rootDoc.getMap(mapname);
    let generalMetadataDoc = ymap.get("task-metadatas") as Y.Doc;
    if (!generalMetadataDoc) {
        logger.info("No general metadata doc found, creating new one");
        generalMetadataDoc = new Y.Doc({ guid: "general-metadata-doc" });
        ymap.set("task-metadatas", generalMetadataDoc);
    }
    generalMetadataDoc.load();
    await waitForDocSync(generalMetadataDoc);
    const generalMetadataMap = generalMetadataDoc.getMap(mapname);
    if (!generalMetadataMap.has(taskID)) {
        logger.info(`No metadata doc found for taskID: ${taskID}, creating new one`);
        const thisMetadataDoc = new Y.Doc({ guid: `metadata-${taskID}` });
        generalMetadataMap.set(taskID, thisMetadataDoc);
    }
    const thisMetadataDoc = generalMetadataMap.get(taskID) as Y.Doc;

    thisMetadataDoc.load();
    await waitForDocSync(thisMetadataDoc);

    const finalMetadata = thisMetadataDoc.getMap(mapname).get("metadata") as TaskMetadata;
    logger.info(`Metadata retrieved for taskID: ${taskID}, metadata: ${JSON.stringify(finalMetadata)}`);
    return finalMetadata;

}

/**
 * Requests and retrieves status for a task.
 * @param {string} taskID - The unique ID for the task.
 * @param {Y.Doc} taskList - The Ydoc containing the tasks ymap
 * @returns  - Resolves with the status object for the task.
 */
export async function getTaskStatus(taskID: string, rootDoc: Y.Doc) {
    // const request = {
    //     start: {
    //         synapse_id: SYNAPSE_ID,
    //         document_id: `status-${taskID}`,
    //         path: ["$.mapname.*"],
    //     },
    // };
    // const watchStream = await voltClient.WatchSynapsePath(request);
    // watchStream.on("data", (response: string) => {
    //     console.log(response)
    // });

    // watchStream.on("end", () => {
    //     console.log("watch ended");
    // });

    // watchStream.on("error", (err: any) => {
    //     console.log(err);
    // });
    const ymap = rootDoc.getMap(mapname);
    const thisStatusDoc = new Y.Doc({ guid: `status-${taskID}` });
    let generalStatusDoc = ymap.get("task-statuses") as Y.Doc;
    if (!generalStatusDoc) {
        generalStatusDoc = new Y.Doc({ guid: "general-status-doc" });
        ymap.set("wires", generalStatusDoc);
    }
    const generalStatusMap = generalStatusDoc.getMap(mapname);
    generalStatusMap.set(taskID, thisStatusDoc);
    thisStatusDoc.load();
    await waitForDocSync(thisStatusDoc);
    return thisStatusDoc.getMap(mapname).get("status");

}


export function create_signed_task(task: SignedTaskCredential["credentialSubject"]): SignedTaskCredential {

    // turn the task into a vc and sign it
    const validFrom = new Date().toISOString();
    logger.info("task: %o", task);
    const unsigned_vc = {
        "@context": [
            "https://www.w3.org/ns/credentials/v2"
            // Add other contexts if needed for your specific credentialSubject
        ],
        "id": `urn:uuid:${uuidv4()}`, // unique ID
        "type": [
            "VerifiableCredential", "taskCredential"
        ],
        "issuer": "did:example:issuerDid",
        "validFrom": validFrom,
        "credentialSubject": task
    };
    const private_key = getPrivateKey();
    const vc = sign(unsigned_vc, private_key);
    const vc_object: SignedTaskCredential = mapToObject(vc);
    return vc_object;
}

export async function deleteWire(wire_id: string) {
    const deleteResourceRequest = { resource_id: wire_id, recursive: true };
    return voltClient
        .DeleteResource(deleteResourceRequest)
        .then((response: { status: Status }) => {
            logger.info(`Wire resource with ID ${wire_id} deleted successfully.`);
            return response;
        }).catch((err: Error) => {
            logger.error("Error deleting wire resource: [%s]", err.message);
            throw err;
        });
}

/** Creates a persistent wire resource in Volt.
 * @param {string} wire_id - The ID of the wire to create.
 * @returns {Promise<Object>} Resolves with the created wire resource.
 */
export async function createPersistentWire(wire_id: string): Promise<Resource> {
    const wireMetadata = {
        name: wire_id,
        kind: ["volt:wire", "volt:database", "volt:sqlite-database"],
        attribute: [
            {
                attribute_id: "volt:wire-persist",
                data_type: "ATTRIBUTE_DATA_TYPE_BOOLEAN",
                value: [{ boolean: true }],
            },
            {
                attribute_id: "volt:wire-persist-table",
                data_type: "ATTRIBUTE_DATA_TYPE_STRING",
                value: [{ string: "wire_data" }],
            }
        ]
    };


    // Create the wire resource.
    return voltClient
        .SaveResource({
            resource: wireMetadata,
            create: true
        })
        .then((response: SaveResourceRequest) => {
            logger.info(`created wire resource: ${response.resource.id}`);
            return response.resource;
        })
        .catch((err: Error) => {
            logger.error("failure: [%s]", err.message);
            throw err;
        });
}



export function observeTaskOutputs(taskOutputs: Y.Doc, taskID: string) {
    logger.info("observing taskOutputsMap for taskID: %s", taskID);
    const taskOutputsMap: Y.Map<Y.Doc> = getMapFromSubDoc(taskOutputs)
    taskOutputsMap.observe(() => {
        for (const [key, value] of taskOutputsMap.entries()) {
            value.load();
            const taskOutputText = value.getText();

            logger.info("in the observe %s", key);

            if (key == taskID) {
                logger.info("found taskID: %s in taskOutputsMap", taskID);
                taskOutputText.observe(async () => {
                    const lines = taskOutputText.toString().split('\n');
                    const currentLine = lines[lines.length - 2];
                    logger.info("stdOut: %s", currentLine);
                });
            }
        }
    });
}

async function set_subdoc_schema(subdoc_id: string, schema: string) {
    try {
        const mainSchemaResp = await voltClient.SetSynapseDocumentMetadata({
            database_id: SYNAPSE_ID,
            document_id: subdoc_id,
            metadata: [{
                name: mapname,
                type: "map",
                json_schema: schema,
            }],
        });
        if (mainSchemaResp.status?.code) {
            throw new Error(
                `Failed to set main doc schema: ${mainSchemaResp.status.message}`,
            );
        } else {
            logger.info(`Set schema for subdoc ${subdoc_id}: %s`, schema);
        }

    } catch (err) {
        console.error(`Error in set_subdoc_schema for ${subdoc_id}:`, err);
        throw err;
    }
}




export async function* streamContinuousTaskOutput(rootDoc: Y.Doc, taskId: string): AsyncGenerator<object> {
    const rootDocumentMap: Y.Map<Y.Doc> = rootDoc.getMap(mapname);
    let externalPumpDoc = rootDocumentMap.get("externalPumps");
    let timer = 0;
    while (!externalPumpDoc) {
        // wait and check again, in case the doc is being created by another agent at the same time
        await new Promise(resolve => setTimeout(resolve, 1000));
        timer++;
        externalPumpDoc = rootDocumentMap.get("externalPumps");
        if (timer > 30) { // after 30 seconds of waiting, throw an error
            throw new Error("No external pumps doc found");
        }
    }
    externalPumpDoc.load();
    await waitForDocSync(externalPumpDoc);

    let externalPumpMap: Y.Map<Y.Doc> = externalPumpDoc.getMap(mapname);
    if (!externalPumpMap) {
        throw new Error("No external pump map found");
    }
    if (!externalPumpMap.has(taskId)) {
        externalPumpMap.set(taskId, new Y.Doc({ guid: taskId }));
        const taskPump = externalPumpMap.get(taskId)
        if (!taskPump) {
            throw new Error("Failed to create task pump doc for taskId: " + taskId);
        }
        taskPump.load();
        waitForDocSync(taskPump).then(() => {
            set_subdoc_schema(taskId, EXTERNAL_PUMP_TASK_SCHEMA);
        });
    }

    const taskPumpDoc = externalPumpMap.get(taskId);
    if (!taskPumpDoc) {
        throw new Error("No task pump doc found for taskId: " + taskId);
    }
    taskPumpDoc.load();
    await waitForDocSync(taskPumpDoc);
    let taskPumpArray: YArray<object> = taskPumpDoc.getArray("resultArray");
    for (let i = 0; i < taskPumpArray.length; i++) {
        const value = taskPumpArray.get(i);
        if (value) {
            logger.info("Existing continuous output chunk: %s", value);
            yield value;
        }
    }

    const queue: object[] = [];
    let wake: (() => void) | null = null;

    const push = (value: object) => {
        queue.push(value);
        if (wake) {
            wake();
            wake = null;
        }
    };

    const observeLogic = (event: Y.YArrayEvent<object>) => {
        for (const deltaItem of event.changes.delta) {
            if ("insert" in deltaItem && Array.isArray(deltaItem.insert)) {
                for (const inserted of deltaItem.insert) {
                    if (inserted) {
                        logger.info("New continuous output chunk: %s", inserted);
                        push(inserted);
                    }
                }
            }
        }
    };

    taskPumpArray.observe(observeLogic);

    try {
        while (true) {
            if (queue.length === 0) {
                await new Promise<void>((resolve) => { wake = resolve; });
            }
            while (queue.length > 0) {
                yield queue.shift() as object;
            }
        }
    } finally {
        taskPumpArray.unobserve(observeLogic);
    }
}

/**
 * Asynchronous function that only resolves when the provided Y.Doc is synced.
 * @param {Y.Doc} doc 
 * @returns 
 */
export async function waitForDocSync(doc: Y.Doc) {
    const anyDoc = doc;
    if (anyDoc.isSynced) return;

    if (anyDoc._syncWaitPromise) {
        return anyDoc._syncWaitPromise;
    }

    anyDoc._syncWaitPromise = new Promise((resolve) => {
        const handler = (isSynced: boolean) => {
            if (!isSynced) return;

            anyDoc.isSynced = true;
            doc.off("sync", handler);
            resolve();
        };

        doc.on("sync", handler);
    });

    return anyDoc._syncWaitPromise;
}

/**
 * Asynchronous function to retrieve task output JSON from a Y.Doc based on taskId.
 * @param {Y.Doc} ydoc 
 * @param {string} taskId 
 * @returns 
 */
export async function getTaskOutputJson(ydoc: Y.Doc, taskId: string): Promise<string> {

    return new Promise(async (resolve, reject) => {
        const rootDocumentMap: Y.Map<Y.Doc> = ydoc.getMap(mapname);
        let externalPumpDoc = rootDocumentMap.get("externalPumps");
        if (!externalPumpDoc) {
            externalPumpDoc = new Y.Doc();
            logger.info("Creating new external pumps doc");
            rootDocumentMap.set("externalPumps", externalPumpDoc);

        }
        externalPumpDoc.load();
        await waitForDocSync(externalPumpDoc);

        let externalPumpMap: Y.Map<Y.Doc> = externalPumpDoc.getMap(mapname);
        if (!externalPumpMap) {
            reject("No output found for taskId: " + taskId);
            return;
        }
        let taskPumpDoc = externalPumpMap.get(taskId);
        if (!taskPumpDoc) {
            reject("No output found for taskId: " + taskId);
            return;
        }
        taskPumpDoc.load();
        await waitForDocSync(taskPumpDoc);

        let taskPumpText = taskPumpDoc.getText();
        if (taskPumpText && taskPumpText.toString().length > 0) {
            resolve(taskPumpText.toString());
        } else {
            logger.error("No output found for taskId: %s", taskId);
            reject("No output found");
        }
    });
}


function mapToObject(map: any): any {
    if (Array.isArray(map)) {
        logger.info("map is an array", map);
        return map;
    }

    if (!(map instanceof Map) && typeof map !== 'object') {
        return map;
    }

    const obj: Record<any, any> = {};
    const entries = map instanceof Map ? map.entries() : Object.entries(map);

    for (const [key, value] of entries) {
        obj[key] = mapToObject(value);
    }

    return obj;
}



function getPrivateKey() {
    // should read from a file and parse it but for testing purposes
    return new Uint8Array([
        249, 36, 149, 249, 249, 117, 133, 209,
        234, 131, 132, 144, 15, 129, 114, 114,
        244, 234, 241, 239, 198, 73, 72, 185,
        156, 200, 237, 170, 2, 142, 41, 36
    ]);
}

