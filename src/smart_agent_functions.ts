// program to watch a shared yjs document and if it sees its own id in the document with a task, 
// download that task, edit the yjs to show the current state and upload the finished data once complete

import grpc from "@grpc/grpc-js";
// @ts-expect-error - Using JS module without types
import { VoltClient } from "@tdxvolt/volt-client-grpc";
import * as Y from "yjs";
import { v4 as uuidv4, v4 } from "uuid";
import { sign, verify } from "verifiable-credential-toolkit";
import winston from 'winston';
import { Sign } from "crypto";
import { SignedTaskCredential } from "./types/shared.types";
import { TaskMetadata } from "./types/config.types";
import { PublishWireRequest, PublishWireResponse, Resource, SaveResourceRequest, Status, SubscribeWireResponse } from "./types/volt.types";

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

// Method 1: Change the logger level to disable info logs
// This will only show warn and error logs
logger.level = 'warn';


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
export async function getAndInitialiseVoltClient(voltConfig:string): Promise<VoltClient> {
    logger.info("initialising Volt client");
    voltClient = new VoltClient(grpc);
    await voltClient.initialise(voltConfig);
    return voltClient;

}

interface InstallAndLaunchParams {
    agent_name: string;
    zip: string;
    callback: (taskID: string, taskOutput: any, taskListMap: Y.Map<any>, taskOutputsMap: Y.Map<any>, spareArgs: any) => void;
    cli_args?: string;
    taskList: Y.Doc;
    taskOutputs: Y.Doc;
    spareArgs: any;
}

/**
 * Installs and launches a new agent task, subscribes to its wire, and observes outputs.
 * 
 * @param {Object} params - Parameters for installing and launching the agent.
 * @param {string} params.agent_name - The name of the agent to install and launch.
 * @param {string} params.zip - The URL or location of the agent zip file.
 * @param {Function} params.callback - Callback function to execute when the task finishes.
 * @param {string} [params.cli_args] - Command-line arguments to pass to the agent (optional).
 * @param {Y.Doc} params.taskList - Yjs Doc (subdoc) containing a YMap to store tasks and their credentials.
 * @param {Y.Map} params.taskOutputs - Yjs map to store outputs from tasks.
 * @param {Object} params.spareArgs - Additional arguments to pass to the callback.
 * @returns {Promise<void>} Resolves when the task is installed and launched.
 */
export async function install_and_launch({ agent_name, zip, callback, cli_args, taskList, taskOutputs, spareArgs }: InstallAndLaunchParams): Promise<void> {
    if (!cli_args) {
        logger.info("no cli_args provided, setting to empty string");
        cli_args = "";
    }

    const taskID = v4();
    const task_finished_indicator = `task-finished-${taskID}`;

    const taskListMap = getMapFromSubDoc(taskList);
    const taskOutputsMap:Y.Map<Y.Doc> = getMapFromSubDoc(taskOutputs)


    const taskVC = create_signed_task({
        "task-id": taskID,
        "action": "new-task",
        "name": agent_name,
        "location": zip,
        source: "http",
        "task_finished-indicator": task_finished_indicator
    });
    taskListMap.set(taskID, { credential: taskVC });

    try {
        const wireSubscription = await subscribeToWire(taskID);
        wireSubscription.onData((chunk: string , allChunks: any, error: any) => {
            if (chunk.includes(task_finished_indicator)) {
                logger.info("task finished indicator found in wire data");
                callback(taskID, taskOutputsMap.get(taskID), taskListMap, taskOutputsMap, spareArgs);
                const uninstall_task_vc = create_signed_task({ "task-id": taskID, "action": "uninstall-task", "name": "uninstall_task" });
                taskListMap.set(taskID, { credential: uninstall_task_vc });
            } else if (chunk.includes("task-failed-"+taskID)){
                logger.info("task failed indicator found in wire data");
                const uninstall_task_vc = create_signed_task({ "task-id": taskID, "action": "uninstall-task", "name": "uninstall_task" });
                taskListMap.set(taskID, { credential: uninstall_task_vc });
            }
        });
    } catch (error) {
        logger.error("error subscribing to wire: %o", error);
    }

    observeTaskOutputs(taskOutputs, taskID);
    const run_task_vc = create_signed_task({
        "task-id": taskID,
        "action": "run-task",
        "name": agent_name,
        "cli_args": cli_args

    });
    taskListMap.set(taskID, { credential: run_task_vc });
}




/**
 * Publishes a message to a specific inter-agent wire.
 * @param {string} wireId - The ID of the wire to publish to.
 * @param {string} message - The message to publish.
 * @returns {Promise<void>} Resolves when the message is successfully published.
 */
export async function publishToInterAgentWire(wireId:string, message:string): Promise<void> {
    // create a wire with the given alias
    return new Promise((resolve, reject) => {
        const sendableMessage = Buffer.from(message);

        logger.info("publishing to wire: %s", wireId);
        const pub = voltClient.PublishWire({
            wire_id: wireId,
            chunk: sendableMessage
        });

        let count = 0;
        const timer = setInterval(() => {
            // If running on node, you can receive raw Buffers.

            if (count > 10) {
                clearInterval(timer);
                pub.end();
            }
            count++;
        }, 1000);

        pub.on("end", () => {
            logger.info("publish ended");
            resolve();
        });

        pub.on("error", (err:Error) => {
            logger.error("publication error: [%s]", err.message);
            reject(err);
        });
    });


    // push the data to the wire, see publishToWire in index.js for example
}

/**
 * Generator function that can be used to subscribe to a wire and retrieve data in sequence using next() calls.
 * @param {*} wireAlias The alias of the wire to subscribe to.
 * @param {*} wireID The ID of the wire to subscribe to.
 * @returns {AsyncGenerator<string>} An async generator that yields data chunks as they arrive.
 */
export async function* wireGenerator(wireAlias:string, wireID:string): AsyncGenerator<string> {
    logger.info("subscribing to wire with alias:", wireAlias);
    logger.info("wireID:", wireID);

    // Start wire subscription immediately
    
    // Create async iterable that handles both existing and new data
    const createDataStream = async function* () {
        const wire = await subscribeToWire(wireID);

                // Handle new data using a promise-based approach
        let streamEnded = false;
        const dataQueue:string[] = [];
        
        // Set up data handlers
        const unsubscribeData = wire.onData((chunk:Buffer) => {
            logger.info("New data chunk received:", chunk.toString('utf-8'));
            dataQueue.push(chunk.toString('utf-8'));
        });
        
        const unsubscribeError = wire.onError((error:Error) => {
            logger.warn("Wire error:", error);
            streamEnded = true;
        });


        
        // Get existing transmissions
        let existingTransmissions = [];
        try {
            existingTransmissions = await getExistingTransmissions(wireID, voltClient);
        } catch (error) {
            existingTransmissions = [{ "payload": "test value" }];
            logger.warn("Error fetching existing transmissions:", error);
        }
        logger.info("Existing transmissions:", existingTransmissions.length);

        // Yield existing transmissions first
        for (const transmission of existingTransmissions) {
            logger.info("Yielding existing transmission:", transmission);
            yield transmission.payload.toString('utf-8');
        }


        try {
            // Yield new data as it arrives
            while (!streamEnded) {
                if (dataQueue.length > 0) {
                    const chunk = dataQueue.shift();
                    logger.info("Yielding new chunk:", chunk);
                    yield chunk;
                } else {
                    
                    // Wait for new data
                    await new Promise(resolve => setTimeout(resolve, 100));
                }
            }
            logger.info("Stream ended, no more data to yield.");
        } finally {
            logger.info("Stream ended, cleaning up...");
            // Clean up subscriptions
            if (unsubscribeData) unsubscribeData();
            if (unsubscribeError) unsubscribeError();
            wire.close();
        }
    };

    // Delegate to the combined stream
    yield* createDataStream();
}

async function getExistingTransmissions(alias:string , voltClient:VoltClient) {
    const results = await voltClient.SqlExecuteJSON({
        database_id: alias,
        statement: "SELECT * FROM wire_data"
    }).catch((error:Error) => {
        logger.error("Error fetching existing transmissions:", error);
    });

    return results;
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
export async function subscribeToWire(wireName:string):Promise<WireSubscription> {
    logger.info("in SUBSCRIBE TO WIRE");
    try {
        const chunks: Buffer[]= [];
        const callbacks:Set<Function> = new Set();
        const errorCallbacks:Set<Function>  = new Set();


        // Default error handler
        let defaultErrorHandler = (error: Error) => {
            logger.error("Unhandled wire stream error: %o", error);
        };

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

        wireStream.on("error", (error:Error) => {

            logger.error("Error in wire stream: %o", error);
            if (errorCallbacks.size > 0) {
                errorCallbacks.forEach(errorCallback => errorCallback(error));
            } else {
                defaultErrorHandler(error);
            }
        });

        const wireInterface = {
            onData: (callback:Function) => {
                callbacks.add(callback);
                return () => callbacks.delete(callback);
            },
            onError: (callback:Function) => {
                errorCallbacks.add(callback);
                return () => errorCallbacks.delete(callback);
            },
            getAllData: () => [...chunks],
            close: () => {
                wireStream.cancel();
                callbacks.clear();
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
export function initialiseSubDocs(agentStateMap:Y.Map<Y.Doc>) {
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
function getMapFromSubDoc<T = unknown>(subdoc:Y.Doc): Y.Map<T> {
    subdoc.load();
    return subdoc.getMap();
}

/**
 * puts an install command onto the taskList.
 * @param {string} taskID - The unique ID for the task.
 * @param {string} taskName - The name of the task to be installed.
 * @param {string} taskLocation - The location of the task, typically a URL or martketplace uuid TODO: waiting on Toby file size limitation check
 * @param {Y.Doc} taskList - The Yjs map to store tasks and their credentials.
 */
export function installTask(taskID: string, taskName: string, taskLocation: string, sourceType: string, taskList: Y.Doc) {
    const taskVC = create_signed_task({ "task-id": taskID, "action": "new-task", "name": taskName, "location": taskLocation, source: sourceType });
    getMapFromSubDoc(taskList).set(taskID, { credential: taskVC });
}

/**
 * Creates and adds a signed start task to the task list.
 * @param {string} taskID - The unique ID for the task.
 * @param {Y.Doc} taskList - The Yjs map to store tasks and their credentials.
 * @param {string} cli_args - Command-line arguments to pass to the agent.
 */
export function startTask(taskID:string, taskList:Y.Doc, cli_args:string) {
    const taskVC2 = create_signed_task({ "task-id": taskID, "action": "run-task", "cli_args": cli_args })
    getMapFromSubDoc(taskList).set(v4(), { credential: taskVC2 });
}

/**
 * Waits for a task to finish.
 * @param {string} taskID - The unique ID for the task. 
 * @returns {Promise<void>} - Resolves when the task is finished, rejects on error.
 */
export async function waitForTaskFinished(taskID:string):Promise<void> {
    const task_finished_indicator = `task-finished-${taskID}`;
    logger.info("Waiting for task to finish with ID:", taskID);
    return new Promise(async (resolve, reject) => {
        try {
            const wireSubscription = await subscribeToWire(taskID);
            logger.info("Subscribed to wire for task ID:", taskID);
            wireSubscription.onData((chunk:string, allChunks:string[], error:Error) => {
                logger.info("chunk received: %s", chunk);
                if (error) {
                    logger.info("Error receiving task finished indicator: %o", error);
                    wireSubscription.close();
                    reject(error);

                }
                if (chunk.includes(task_finished_indicator)) {
                    logger.info("task finished indicator found in wire data");
                    resolve();
                }
                if (chunk.includes("task-failed-"+taskID)){
                    logger.info("task failed indicator found in wire data");
                    reject(new Error("Task failed"));
                }
            });
        } catch (error) {
            logger.warn("error subscribing to wire: %o", error);
            reject(error);
        }
    });
}

/**
 * Creates and adds a signed uninstall task to the task list.
 * @param {string} taskID - The unique ID for the task.
 * @param {Y.Doc} taskList - The Yjs map to store tasks and their credentials.
 */
export function uninstallTask(taskID:string, taskList:Y.Doc) {
    const taskVC3 = create_signed_task({ "task-id": taskID, "action": "uninstall-task" })
    getMapFromSubDoc(taskList).set(v4(), { credential: taskVC3 });
}




/**
 * Requests and retrieves metadata for a task.
 * @param {string} taskID - The unique ID for the task.
 * @param {Y.Doc} taskList - The Yjs map to store tasks and their credentials.
 * @returns {Promise<Object>} - Resolves with the metadata object for the task.
 */
export function getTaskMetadata(taskID:string, taskList:Y.Doc):Promise<TaskMetadata> {
    const taskVersionVC = create_signed_task({ "task-id": taskID, "action": "task-version" });
    const metadataPromise = new Promise<TaskMetadata>((resolve, reject) => {
        handleWireSubscription(resolve, reject, taskID);
    });
    getMapFromSubDoc(taskList).set(v4(), { credential: taskVersionVC });
    return metadataPromise;

}

/**
 * Requests and retrieves status for a task.
 * @param {string} taskID - The unique ID for the task.
 * @param {Y.Doc} taskList - The Ydoc containing the tasks ymap
 * @returns  - Resolves with the status object for the task.
 */
export function getTaskStatus(taskID:string, taskList:Y.Doc) {
    const taskStatusVC = create_signed_task({ "task-id": taskID, "action": "task-status" });
    const taskStatusPromise = new Promise(async (resolve, reject) => {
        try {
            const wireSubscription = await subscribeToWire(taskID);
            wireSubscription.onData(
                handleWireStatus(resolve, reject, wireSubscription, taskID)
            );

        } catch (error) {
            logger.error("Error subscribing to wire: %o", error);
            reject(error);
        }
    });
    getMapFromSubDoc(taskList).set(v4(), { credential: taskStatusVC });
    return taskStatusPromise;
}


export function create_signed_task(task:SignedTaskCredential["credentialSubject"]): SignedTaskCredential {

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
    const vc_object:SignedTaskCredential = mapToObject(vc);
    return vc_object;
}

export async function deleteWire(wire_id:string) {
    const deleteResourceRequest = {resource_id: wire_id, recursive: true};
    return voltClient
    .DeleteResource(deleteResourceRequest)
    .then((response:{status:Status}) => {
        logger.info(`Wire resource with ID ${wire_id} deleted successfully.`);
        return response;
    }).catch((err:Error) => {
        logger.error("Error deleting wire resource: [%s]", err.message);
        throw err;
    });
}

/** Creates a persistent wire resource in Volt.
 * @param {string} wire_id - The ID of the wire to create.
 * @returns {Promise<Object>} Resolves with the created wire resource.
 */
export async function createPersistentWire(wire_id:string):Promise<Resource> {
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
        .then((response:SaveResourceRequest) => {
            logger.info(`created wire resource: ${response.resource.id}`);
            return response.resource;
        })
        .catch((err:Error) => {
            logger.error("failure: [%s]", err.message);
            throw err;
        });
}



export function observeTaskOutputs(taskOutputs:Y.Doc, taskID:string) {
    logger.info("observing taskOutputsMap for taskID: %s", taskID);
    const taskOutputsMap:Y.Map<Y.Doc> = getMapFromSubDoc(taskOutputs)
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

/**
 * Asynchronous function that only resolves when the provided Y.Doc is synced.
 * @param {Y.Doc} doc 
 * @returns 
 */
export async function waitForDocSync(doc:Y.Doc) {
    const anyDoc = doc;
    if (anyDoc.isSynced) return;

    if (anyDoc._syncWaitPromise) {
        return anyDoc._syncWaitPromise;
    }

    anyDoc._syncWaitPromise = new Promise((resolve) => {
        const handler = (isSynced:boolean) => {
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
export async function getTaskOutputJson(ydoc:Y.Doc, taskId:string):Promise<string> {

    return new Promise(async (resolve, reject) => {
        const rootDocumentMap:Y.Map<Y.Doc> = ydoc.getMap();
        let externalPumpDoc = rootDocumentMap.get("externalPumps");
        if (!externalPumpDoc) {
            externalPumpDoc = new Y.Doc();
            logger.info("Creating new external pumps doc");
            rootDocumentMap.set("externalPumps", externalPumpDoc);
        }
        externalPumpDoc.load();
        await waitForDocSync(externalPumpDoc);

        let externalPumpMap:Y.Map<Y.Doc>  = externalPumpDoc.getMap();
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


async function handleWireSubscription(resolve: { (value: TaskMetadata | PromiseLike<TaskMetadata>): void; (arg0: any): void; }, reject: { (reason?: any): void; (arg0: unknown): void; }, taskID: string) {
    try {
        const wireSubscription = await subscribeToWire(taskID);
        wireSubscription.onData((chunk: string, allChunks: string[], error: Error) => {
            if (error) {
                logger.error("Error receiving task metadata: %o", error);
                reject(error);
                wireSubscription.close();
                return;
            }
            if (chunk) {
                logger.info("Received task metadata chunk: %s", chunk);
                try {
                    if (chunk === "task-finished-" + taskID) {
                        logger.info("Task finished indicator overwrote metadata, ignoring chunk and getting next ");
                        return;
                    }
                    const metadata = JSON.parse(chunk);
                    resolve(metadata);
                    wireSubscription.close();
                } catch (parseError) {
                    logger.error("Error parsing task metadata chunk: %o", parseError);
                    reject(parseError);
                    wireSubscription.close();
                }
            }
        });
    } catch (error) {
        logger.error("Error subscribing to wire: %o", error);
        reject(error);
    }
}

function handleWireStatus(resolve: { (value: unknown): void; (arg0: any): void; }, reject: { (reason?: any): void; (arg0: unknown): void; }, wireSubscription:WireSubscription, taskID: string) {
    return function (chunk: string, allChunks: string[], error: Error) {
        if (error) {
            logger.error("Error receiving task status: %o", error);
            reject(error);
            wireSubscription.close();
            return;
        }
        if (chunk) {
            logger.info("Received task status chunk: %s", chunk);
            try {
                if (chunk === "task-finished-" + taskID) {
                    logger.info("Task status received, but task is finished, ignoring chunk");
                    return;
                }
                const status = JSON.parse(chunk);
                resolve(status);
                wireSubscription.close();
            } catch (parseError) {
                logger.error("Error parsing task status chunk: %o", parseError);
                logger.info("chunk: %s", chunk);
                reject(parseError);
                wireSubscription.close();
            }
        }
    };
}

function mapToObject(map: any): any {
    if (Array.isArray(map)) {
        logger.info("map is an array", map);
        return map;
    }

    if (!(map instanceof Map) && typeof map !== 'object') {
        return map;
    }

    const obj:Record<any,any> = {};
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

