// program to watch a shared yjs document and if it sees its own id in the document with a task, 
// download that task, edit the yjs to show the current state and upload the finished data once complete

import grpc from "@grpc/grpc-js";
// @ts-expect-error - Using JS module without types
import { VoltClient } from "@tdxvolt/volt-client-grpc";
import * as Y from "yjs";
import { v4 } from "uuid";
import winston from 'winston';
import { Sign } from "crypto";
import { HostInfo, SignedTaskCredential, SignedTaskCredentialWrapper, SynapseWriteObject, TargetHostValidation, TrustGrantCredential, TrustGrantRequest } from "./types/shared.types";
import { signCredentialAs, SigningIdentity, signingIdentityFromVoltConfig, trustGrantKeyOf } from "./taskSigning.js";
import { TaskMetadata } from "./types/config.types";
import { PublishWireRequest, PublishWireResponse, Resource, SaveResourceRequest, Status, SubscribeWireResponse } from "./types/volt.types";
import { cli } from "winston/lib/winston/config";
import { writeFieldToSynapseSubdoc } from "./agent_schemas.js";
import {
    EXTERNAL_PUMP_RESULT_ARRAY_SCHEMA,
    EXTERNAL_PUMP_RESULT_TEXT_SCHEMA,
    EXTERNAL_PUMP_TASK_SCHEMA,
    SYNAPSE_ID
} from "./agent_schemas.js";
// @ts-ignore
import { YArray } from "yjs/dist/src/internals";

export * from "./types/flow.types.js";
export { FLOW_EXPORT_SCHEMAS_BY_VERSION } from "./flow/flowSchema.js";
export { FlowValidationError, validateSemanticFlow } from "./flow/flowValidation.js";
export { deploySemanticFlow, planSemanticFlowDeployment } from "./flow/flowDeployment.js";
export { watchForTaskListEntry, type PublishedTaskAction } from "./flow/taskListEntryWatching.js";
export type { TrustGrantCredential, TrustGrantRequest } from "./types/shared.types.js";
export { readPublicSigningIdentity, trustGrantKeyOf, type PublicSigningIdentity } from "./taskSigning.js";

const mapname = "GENERIC_MAP_NAME";

/** The root a continuous task's records are appended to in its default output pump document. */
const RESULT_ARRAY_ROOT_NAME = "resultArray";

/**
 * The root a non-continuous task's final result is written to in its default output pump document,
 * unless the task named another with `output_pump_root`. A document's unnamed root cannot be
 * addressed on the synapse at all, so a result written there can be neither watched nor read by path.
 */
export const DEFAULT_RESULT_TEXT_ROOT_NAME = "resultText";

/** The names an output pump result root may take: a name the synapse can address as `$.<name>`. */
const ADDRESSABLE_ROOT_NAME_PATTERN = /^[A-Za-z][A-Za-z0-9_-]*$/;


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
var signingIdentity: SigningIdentity | undefined;

/**
 * Initialises the Volt client with the provided configuration. Every task credential and trust grant
 * that the library publishes afterwards is signed by the identity in that configuration.
 * @param {string} voltConfig The path to the Volt configuration file 
 * @returns {Promise<VoltClient>} Resolves with the initialised Volt client.
 */
export async function getAndInitialiseVoltClient(voltConfig: string): Promise<VoltClient> {
    logger.info("initialising Volt client");
    signingIdentity = signingIdentityFromVoltConfig(voltConfig);
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
    output_pump_root?: string;
    target_host?: string;
    validate_target_host?: boolean;
    rootDoc?: Y.Doc;
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
 * @param {string} [params.output_pump_root] - Renames the text root the task's final result lands on
 * in its default output pump, which is `resultText` otherwise.
 * @param {string} [params.target_host] - host_id of the only host allowed to run this task. The
 * install is published with it, which routes the follow-up run and uninstall to the same host.
 * @param {boolean} [params.validate_target_host] - Off by default. Set with rootDoc to check the
 * target against the `hostList` before publishing anything.
 * @param {Y.Doc} [params.rootDoc] - The synapse root document, only read when validating.
 * @returns {Promise<void>} Resolves when the task is installed and launched.
 */
export async function install_and_launch({ agent_name, zip, callback, cli_args, taskList, taskOutputs, spareArgs, output_pump_root, target_host, validate_target_host, rootDoc }: InstallAndLaunchParams): Promise<void> {
    if (!cli_args) {
        logger.info("no cli_args provided, setting to empty string");
        cli_args = "";
    }

    const taskID = v4();
    const task_finished_indicator = `task-finished-${taskID}`;

    const taskListMap = getMapFromSubDoc(taskList);
    const taskOutputsMap: Y.Map<Y.Doc> = getMapFromSubDoc(taskOutputs)


    const targetHostField = await buildTargetHostField({ target_host, validate_target_host, rootDoc }, taskList);

    const taskVC = create_signed_task({
        "task-id": taskID,
        "action": "new-task",
        "name": agent_name,
        "location": zip,
        source: "http",
        "task_finished-indicator": task_finished_indicator,
        ...targetHostField
    });
    // taskListMap.set(taskID, { credential: taskVC });
    writeFieldToSynapseSubdoc(voltClient, taskID, { credential: taskVC }, taskList.guid, mapname)


    try {
        const wireSubscription = await subscribeToWire(`wireid-${taskID}`);
        wireSubscription.onData((chunk: string, allChunks: any, error: any) => {
            if (chunk.includes(task_finished_indicator)) {
                logger.info("task finished indicator found in wire data");
                callback(taskID, taskOutputsMap.get(taskID), taskListMap, taskOutputsMap, spareArgs);
                const uninstall_task_vc = create_signed_task({ "task-id": taskID, "action": "uninstall-task", "name": "uninstall_task" });
                // taskListMap.set(taskID, { credential: uninstall_task_vc });
                writeFieldToSynapseSubdoc(voltClient, taskID, { credential: uninstall_task_vc }, taskList.guid, mapname)

            } else if (chunk.includes("task-failed-" + taskID)) {
                logger.info("task failed indicator found in wire data");
                const uninstall_task_vc = create_signed_task({ "task-id": taskID, "action": "uninstall-task", "name": "uninstall_task" });
                // taskListMap.set(taskID, { credential: uninstall_task_vc });
                writeFieldToSynapseSubdoc(voltClient, taskID, { credential: uninstall_task_vc }, taskList.guid, mapname)

            }
        });
    } catch (error) {
        logger.error("error subscribing to wire: %o", error);
    }

    observeTaskOutputs(taskOutputs, taskID);
    warnIfOutputPumpRootIsUnusable(taskID, output_pump_root);
    const run_task_vc = create_signed_task({
        "task-id": taskID,
        "action": "run-task",
        "name": agent_name,
        "cli_args": cli_args,
        ...(output_pump_root ? { output_pump_root } : {})

    });
    // taskListMap.set(taskID, { credential: run_task_vc });
    writeFieldToSynapseSubdoc(voltClient, taskID, { credential: run_task_vc }, taskList.guid, mapname)

}




/**
 * Publishes a message to a specific inter-agent wire.
 * @param {string} wireId - The ID of the wire to publish to.
 * @param {string} message - The message to publish.
 * @returns {Promise<void>} Resolves when the message is successfully published.
 */
export async function publishToInterAgentWire(wireId: string, message: string): Promise<void> {
    // create a wire with the given alias
    return new Promise((resolve, reject) => {
        const sendableMessage = Buffer.from(message);

        logger.info("publishing to wire: %s", wireId);
        const pub = voltClient.PublishWire({
            wire_id: `@${wireId}`,
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

        pub.on("error", (err: Error) => {
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
export async function* wireGenerator(wireAlias: string, wireID: string): AsyncGenerator<string> {
    logger.info("subscribing to wire with alias:", wireAlias);
    logger.info("wireID:", wireID);

    // Start wire subscription immediately

    // Create async iterable that handles both existing and new data
    const createDataStream = async function* () {
        const wire = await subscribeToWire(wireID);

        // Handle new data using a promise-based approach
        let streamEnded = false;
        const dataQueue: string[] = [];

        // Set up data handlers
        const unsubscribeData = wire.onData((chunk: Buffer) => {
            logger.info("New data chunk received:", chunk.toString('utf-8'));
            dataQueue.push(chunk.toString('utf-8'));
        });

        const unsubscribeError = wire.onError((error: Error) => {
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

async function getExistingTransmissions(alias: string, voltClient: VoltClient) {
    const results = await voltClient.SqlExecuteJSON({
        database_id: alias,
        statement: "SELECT * FROM wire_data"
    }).catch((error: Error) => {
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
export async function subscribeToWire(wireName: string): Promise<WireSubscription> {
    logger.info("in SUBSCRIBE TO WIRE");
    try {
        const chunks: Buffer[] = [];
        const callbacks: Set<Function> = new Set();
        const errorCallbacks: Set<Function> = new Set();

        const canAccess = await voltClient.CanAccessResource({ resource_id: `@${wireName}`, access: "read" }).catch((error: Error) => {
            logger.error("Error checking access to wire resource: %o", error);
        });
        if (![1, 2, 6, 7, "POLICY_DECISION_PERMIT"].includes(canAccess.decision)) {
            console.warn(`Access denied to wire resource ${wireName}. Decision: ${canAccess.decision} \n This process will be retried, expect some warnings in the logs while waiting for retries`);
            // throw new Error(`Access denied to wire resource ${wireName}. Decision: ${canAccess.decision}`);
        }
        else {
            console.log(`Access granted to wire resource ${wireName}. Decision: ${canAccess.decision}`);
        }
        const wireStream = await voltClient.SubscribeWire({ wire_id: `@${wireName}` });
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

    if (!agentStateMap.has("hostList")) {
        agentStateMap.set("hostList", new Y.Doc());
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
 * How long a host's heartbeat may go unrefreshed before the hosts themselves consider it dead. A
 * targeted task published at a host past this threshold is never executed and never reassigned.
 */
export const TARGET_HOST_STALENESS_THRESHOLD_MS = 90_000;

const HOST_LIST_DOC_NAME = "hostList";
const AGENT_LIST_SYNC_TIMEOUT_MS = 5000;

const RUNTIME_NAMES_BY_TASK_LIST_NAME: Record<string, string[]> = {
    nodeTaskList: ["nodejs", "node", "javascript"],
    pythonTaskList: ["python", "python3"]
};

/**
 * Thrown instead of publishing when a `target_host` names a host that could not execute the task.
 * A targeted task has no fallback host, so publishing to a bad target would queue it indefinitely.
 * Read `validation` for the specific reason.
 */
export class TargetHostUnavailableError extends Error {
    readonly validation: TargetHostValidation;

    constructor(validation: TargetHostValidation) {
        super(describeTargetHostValidation(validation));
        this.name = "TargetHostUnavailableError";
        this.validation = validation;
    }
}

function describeTargetHostValidation(validation: TargetHostValidation): string {
    switch (validation.status) {
        case "usable":
            return `target host ${validation.targetHost} is live and supports the task's runtime`;
        case "host-list-unavailable":
            return `cannot verify target host ${validation.targetHost}: no ${HOST_LIST_DOC_NAME} sub-document is present on the supplied root document`;
        case "unknown-host":
            return `target host ${validation.targetHost} is not in the ${HOST_LIST_DOC_NAME}, so no host would ever execute this task`;
        case "stale-host":
            return `target host ${validation.targetHost} last checked in at ${validation.lastSeen ?? "an unreadable timestamp"}, ${validation.millisecondsSinceLastSeen ?? "an unknown number of"}ms ago, beyond the ${TARGET_HOST_STALENESS_THRESHOLD_MS}ms staleness threshold`;
        case "runtime-unsupported":
            return `target host ${validation.targetHost} reports runtimes [${validation.hostRuntimes.join(", ")}], none of which match [${validation.requiredRuntimes.join(", ")}] required by the task list being published to`;
    }
}

async function waitForDocSyncOrTimeout(doc: Y.Doc, timeoutMilliseconds: number): Promise<void> {
    await Promise.race([waitForDocSync(doc), delay(timeoutMilliseconds)]);
}

async function loadAgentListMap(rootDoc: Y.Doc): Promise<Y.Map<HostInfo> | undefined> {
    const rootMap: Y.Map<Y.Doc> = rootDoc.getMap(mapname);
    const agentListDoc = rootMap.get(HOST_LIST_DOC_NAME);
    if (!agentListDoc) return undefined;

    agentListDoc.load();
    await waitForDocSyncOrTimeout(agentListDoc, AGENT_LIST_SYNC_TIMEOUT_MS);
    return agentListDoc.getMap(mapname);
}

/** The runtimes a host must report to be a valid target for tasks on the given task list. */
function requiredRuntimesForTaskList(rootDoc: Y.Doc, taskListDoc: Y.Doc): string[] {
    const rootMap: Y.Map<Y.Doc> = rootDoc.getMap(mapname);
    for (const [taskListName, runtimeNames] of Object.entries(RUNTIME_NAMES_BY_TASK_LIST_NAME)) {
        if (rootMap.get(taskListName)?.guid === taskListDoc.guid) return runtimeNames;
    }
    return [];
}

function checkTargetHostHeartbeat(targetHost: string, hostEntry: HostInfo): TargetHostValidation | null {
    const millisecondsSinceLastSeen = hostEntry.lastSeen ? Date.now() - Date.parse(hostEntry.lastSeen) : NaN;
    if (Number.isNaN(millisecondsSinceLastSeen)) {
        return { status: "stale-host", targetHost, lastSeen: hostEntry.lastSeen };
    }
    if (millisecondsSinceLastSeen >= TARGET_HOST_STALENESS_THRESHOLD_MS) {
        return { status: "stale-host", targetHost, lastSeen: hostEntry.lastSeen, millisecondsSinceLastSeen };
    }
    return null;
}

function checkTargetHostRuntimes(
    targetHost: string,
    hostEntry: HostInfo,
    taskListDoc: Y.Doc,
    rootDoc: Y.Doc
): TargetHostValidation {
    const requiredRuntimes = requiredRuntimesForTaskList(rootDoc, taskListDoc);
    if (requiredRuntimes.length === 0) {
        logger.warn(`task list ${taskListDoc.guid} does not match a known runtime task list, skipping the runtime check for target host ${targetHost}`);
        return { status: "usable", targetHost };
    }

    const hostRuntimes = hostEntry.runtimes ?? [];
    const supportsRequiredRuntime = hostRuntimes.some(runtime => requiredRuntimes.includes(runtime.toLowerCase()));
    if (!supportsRequiredRuntime) {
        return { status: "runtime-unsupported", targetHost, requiredRuntimes, hostRuntimes };
    }
    return { status: "usable", targetHost };
}

/**
 * Checks whether a host could actually execute a task published to the given task list: that the
 * host_id is present in the `hostList`, that its heartbeat is inside the hosts' own staleness
 * threshold, and that it reports the runtime of that task list. Returns the reason it is unusable
 * rather than throwing.
 * @param targetHost - The `host_id` under which the host appears in the `hostList`.
 * @param taskListDoc - The runtime task list sub-document the task would be published to.
 * @param rootDoc - The synapse root document holding the `hostList` sub-document.
 */
export async function validateTargetHost(targetHost: string, taskListDoc: Y.Doc, rootDoc: Y.Doc): Promise<TargetHostValidation> {
    const agentList = await loadAgentListMap(rootDoc);
    if (!agentList) return { status: "host-list-unavailable", targetHost };

    const hostEntry = agentList.get(targetHost);
    if (!hostEntry) return { status: "unknown-host", targetHost };

    const heartbeatFailure = checkTargetHostHeartbeat(targetHost, hostEntry);
    if (heartbeatFailure) return heartbeatFailure;

    return checkTargetHostRuntimes(targetHost, hostEntry, taskListDoc, rootDoc);
}

/**
 * Throws {@link TargetHostUnavailableError} unless the named host could execute a task published to
 * the given task list.
 */
export async function assertTargetHostUsable(targetHost: string, taskListDoc: Y.Doc, rootDoc: Y.Doc): Promise<void> {
    const validation = await validateTargetHost(targetHost, taskListDoc, rootDoc);
    if (validation.status !== "usable") {
        throw new TargetHostUnavailableError(validation);
    }
    logger.info(`target host ${targetHost} validated for task list ${taskListDoc.guid}`);
}

/**
 * How a task names the host that must execute it.
 *
 * Publishing is set and forget by default: the credential is written with the target and nothing is
 * checked, so a bad target is only visible as a task that never gets an owner. Set
 * `validate_target_host` with a `rootDoc` to trade that for a pre-publish check.
 */
export interface TargetHostOptions {
    /** `host_id` of the only host allowed to execute the task, as keyed in the `hostList`. */
    target_host?: string;
    /**
     * Off by default. When set, the target is checked against the `hostList` before anything is
     * published and {@link TargetHostUnavailableError} is thrown instead of queuing a task no host
     * would run. Requires `rootDoc`, and makes the publish call worth awaiting.
     */
    validate_target_host?: boolean;
    /** The synapse root document holding the `hostList`. Only read when validating. */
    rootDoc?: Y.Doc;
}

/**
 * The `target_host` fragment to merge into a credentialSubject before signing, empty for untargeted
 * tasks. Only reads the `hostList` when validation was asked for, in which case it throws
 * {@link TargetHostUnavailableError} rather than letting a doomed task be published.
 */
async function buildTargetHostField(
    targetHostOptions: TargetHostOptions,
    taskListDoc: Y.Doc
): Promise<{ target_host?: string }> {
    const { target_host, validate_target_host, rootDoc } = targetHostOptions;
    if (!target_host) return {};

    if (!validate_target_host) {
        logger.info(`publishing task targeted at host ${target_host} without validation; set validate_target_host to check the target first`);
        return { target_host };
    }

    if (!rootDoc) {
        throw new Error(`validate_target_host was set for target ${target_host} but no rootDoc was supplied; the agentList lives on the synapse root document and cannot be read without it`);
    }

    await assertTargetHostUsable(target_host, taskListDoc, rootDoc);
    return { target_host };
}

export interface InstallTaskInputArgs {
    taskID: string;
    taskName: string;
    taskLocation: string;
    sourceType: string;
    taskList: Y.Doc;
    execute_after_timestamp_ms?: number;
    target_host?: string;
    validate_target_host?: boolean;
    rootDoc?: Y.Doc;
}

/**
 * puts an install command onto the taskList.
 * @param {string} taskID - The unique ID for the task.
 * @param {string} taskName - The name of the task to be installed.
 * @param {string} taskLocation - The location of the task, typically a URL or martketplace uuid TODO: waiting on Toby file size limitation check
 * @param {Y.Doc} taskList - The Yjs map to store tasks and their credentials.
 */
export function installTask(taskID: string, taskName: string, taskLocation: string, sourceType: string, taskList: Y.Doc, execute_after_timestamp_ms?: number): Promise<void>;
/**
 * puts an install command onto the taskList, optionally pinning the task to a single host.
 *
 * Setting `target_host` makes the named host the only one that will ever execute this task and every
 * later action for the same task-id; no other host takes it and there is no fallback if that host is
 * offline. The target is published as given and nothing is checked unless `validate_target_host` is
 * set with a `rootDoc`, which throws {@link TargetHostUnavailableError} instead of queuing a task no
 * host would run.
 * @param input_args - Object containing the install details plus optional target host options.
 */
export function installTask(input_args: InstallTaskInputArgs): Promise<void>;
export async function installTask(
    taskIDOrArgs: string | InstallTaskInputArgs,
    taskName?: string,
    taskLocation?: string,
    sourceType?: string,
    taskList?: Y.Doc,
    execute_after_timestamp_ms?: number
): Promise<void> {
    const installArgs: InstallTaskInputArgs = typeof taskIDOrArgs === "string"
        ? {
            taskID: taskIDOrArgs,
            taskName: taskName!,
            taskLocation: taskLocation!,
            sourceType: sourceType!,
            taskList: taskList!,
            execute_after_timestamp_ms
        }
        : taskIDOrArgs;

    const targetHostField = await buildTargetHostField(installArgs, installArgs.taskList);

    const taskVC = create_signed_task({
        "task-id": installArgs.taskID,
        "action": "new-task",
        "name": installArgs.taskName,
        "location": installArgs.taskLocation,
        source: installArgs.sourceType,
        ...(installArgs.execute_after_timestamp_ms ? { execute_after_timestamp_ms: installArgs.execute_after_timestamp_ms } : {}),
        ...targetHostField
    });
    // getMapFromSubDoc(taskList).set(taskID, { credential: taskVC });
    writeFieldToSynapseSubdoc(voltClient, installArgs.taskID, { credential: taskVC }, installArgs.taskList.guid, mapname);
}

interface StartTaskInputArgs { taskID: string; taskList: Y.Doc; std_in?: object; cli_args?: string, continuous?: boolean, outer_output_pump_location?: string, output_pump_root?: string, synapse_write_path?: SynapseWriteObject, execute_after_timestamp_ms?: number, target_host?: string, validate_target_host?: boolean, rootDoc?: Y.Doc }

/**
 * @deprecated
 * @param taskID - The unique ID for the task.
 * @param taskList - The Yjs map to store tasks and their credentials.
 * Creates and adds a signed start task to the task list.
 */
export function startTask(taskID: string, taskList: Y.Doc): Promise<void>;
/**
 * @deprecated
 * Deprecated: Due to ambiguity, use startTaskWithCliArgs instead.
 * Creates and adds a signed start task to the task list.
 * @param  taskID - The unique ID for the task.
 * @param taskList - The Yjs map to store tasks and their credentials.
 * @param  cli_args - Command-line arguments to pass to the agent.
 */
export function startTask(taskID: string, taskList: Y.Doc, cli_args: string): Promise<void>;
/**
 * Creates and adds a signed start task to the task list.
 *
 * `target_host` is only needed when the install of this task was not itself driven by that host; an
 * install published with a target already routes every later action for the same task-id to it, so
 * omitting the field is the recommended default. Set `validate_target_host` with a `rootDoc` to have
 * the target checked against the `hostList` before publishing.
 *
 * `output_pump_root` renames the text root a non-continuous task's final result lands on in its
 * default output pump, which is `resultText` otherwise. It is only worth setting when a specific
 * consumer expects a specific path, and that consumer has to be told the name.
 *
 * `synapse_write_path` asks the host to write a copy of the output to a document of your choosing.
 * Its `additional_parse_options` make that write per record, so a continuous task builds a keyed
 * collection under `path` instead of overwriting one key; readers of such a task have to watch
 * `$.<root>.*` rather than a single key, and the destination document's roots must already be
 * registered.
 * @param input_args - Object containing taskID, taskList, and optional std_in/cli_args/target host options.
 */
export function startTask(input_args: StartTaskInputArgs): Promise<void>;
export async function startTask( //TODO: add in translation schemas somehow
    taskIDOrArgs: string | StartTaskInputArgs,
    taskList?: Y.Doc,
    cli_args?: string
): Promise<void> {
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
    const { taskID, taskList: tl, std_in, cli_args: ca, continuous, outer_output_pump_location, output_pump_root, synapse_write_path, execute_after_timestamp_ms } = taskIDOrArgs;
    let taskVC2: SignedTaskCredential;
    if (outer_output_pump_location) {
        logger.warn("This form of custom location will be deprecated once the volt schema issues are resolved, synapse_write_path is preferred and will be the primary future method.")

    }
    warnIfOutputPumpRootIsUnusable(taskID, output_pump_root);
    const targetHostField = await buildTargetHostField(taskIDOrArgs, tl);
    const baseTask: SignedTaskCredential["credentialSubject"] = {
        "task-id": taskID,
        action: "run-task",
        ...(continuous !== undefined ? { continuous } : {}),
        ...(outer_output_pump_location ? { outer_output_pump_location } : {}),
        ...(output_pump_root ? { output_pump_root } : {}),
        ...(synapse_write_path ? { synapse_write_path } : {}),
        ...(execute_after_timestamp_ms ? { execute_after_timestamp_ms: execute_after_timestamp_ms } : {}),
        ...targetHostField
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

/**
 * Warns when a requested output pump root is a name the host will refuse, which it does silently in
 * favour of `resultText` — leaving a publisher that believed it had renamed the root reading an empty
 * one. The name is published either way, since only the host decides where the result lands.
 */
function warnIfOutputPumpRootIsUnusable(taskID: string, output_pump_root?: string) {
    if (!output_pump_root) return;

    const isUsableRootName =
        ADDRESSABLE_ROOT_NAME_PATTERN.test(output_pump_root) &&
        output_pump_root !== RESULT_ARRAY_ROOT_NAME &&
        output_pump_root !== mapname;
    if (!isUsableRootName) {
        logger.warn(
            "task %s asks for output pump root %s, which the synapse cannot address as $.%s; the host will write to %s instead",
            taskID,
            output_pump_root,
            output_pump_root,
            DEFAULT_RESULT_TEXT_ROOT_NAME
        );
    }
}

export function startTaskWithStdin(taskID: string, taskList: Y.Doc, std_in: object): Promise<void> {
    return startTask({ taskID: taskID, taskList: taskList, std_in: std_in });

}
/**
 * Creates and adds a signed start task to the task list. runtask
 * @param  taskID - The unique ID for the task.
 * @param taskList - The Yjs map to store tasks and their credentials.
 * @param  cli_args - Command-line arguments to pass to the agent.
 */
export function startTaskWithCliArgs(taskID: string, taskList: Y.Doc, cli_args: string): Promise<void> {
    return startTask({ taskID: taskID, taskList: taskList, cli_args: cli_args });
}



/**
 * Waits for a task to finish.
 * @param {string} taskID - The unique ID for the task. 
 * @returns {Promise<void>} - Resolves when the task is finished, rejects on error.
 */
export async function waitForTaskFinished(taskID: string): Promise<void> {
    const task_finished_indicator = `task-finished-${taskID}`;
    logger.info("Waiting for task to finish with ID:", taskID);
    let retries = 0;
    const maxRetries = 100

    const trySubscribe = (): Promise<void> => {
        return new Promise(async (resolve, reject) => {
            try {
                const wireSubscription = await subscribeToWire(`wireid-${taskID}`);
                logger.info("Subscribed to wire for task ID:", taskID);
                wireSubscription.onData((chunk: string, allChunks: string[], error: Error) => {
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
                    if (chunk.includes("task-failed-" + taskID)) {
                        logger.info("task failed indicator found in wire data");
                        reject(new Error("Task failed"));
                    }
                });

                wireSubscription.onError(async (error: Error) => {
                    wireSubscription.close();
                    if (retries < maxRetries) {
                        retries++;
                        logger.info(`Wire subscription failed, retrying (${retries}/${maxRetries})...`);
                        await new Promise(r => setTimeout(r, 1000 * retries)); // Exponential backoff
                        resolve(trySubscribe());
                    } else {
                        reject(error);
                    }
                });
            } catch (error) {
                if (retries < maxRetries) {
                    retries++;
                    await new Promise(r => setTimeout(r, 1000 * retries));
                    resolve(trySubscribe());
                } else {
                    reject(error);
                }
            }
        });
    };
    return trySubscribe();
}

/**
 * Creates and adds a signed stop task to the task list. The host that runs the task ends its process
 * and keeps it installed, so a later run task starts it again.
 * @param {string} taskID - The unique ID for the task.
 * @param {Y.Doc} taskList - The Yjs map to store tasks and their credentials.
 * @param targetHostOptions - A `target_host` is only needed when the task's install was not driven by
 * that host. Validation is off unless asked for.
 */
export async function stopTask(taskID: string, taskList: Y.Doc, execute_after_timestamp_ms?: number, targetHostOptions: TargetHostOptions = {}): Promise<void> {
    const targetHostField = await buildTargetHostField(targetHostOptions, taskList);
    const stopTaskVC = create_signed_task({ "task-id": taskID, "action": "stop-task", ...(execute_after_timestamp_ms ? { execute_after_timestamp_ms: execute_after_timestamp_ms } : {}), ...targetHostField });
    writeFieldToSynapseSubdoc(voltClient, v4(), { credential: stopTaskVC }, taskList.guid, mapname)
}

/**
 * Creates and adds a signed uninstall task to the task list.
 * @param {string} taskID - The unique ID for the task.
 * @param {Y.Doc} taskList - The Yjs map to store tasks and their credentials.
 * @param targetHostOptions - A `target_host` is only needed when the task's install was not driven by
 * that host; a targeted install already routes its uninstall to the same host, and other hosts ignore
 * the entry either way. Validation is off unless asked for.
 */
export async function uninstallTask(taskID: string, taskList: Y.Doc, execute_after_timestamp_ms?: number, targetHostOptions: TargetHostOptions = {}): Promise<void> {
    const targetHostField = await buildTargetHostField(targetHostOptions, taskList);
    const taskVC3 = create_signed_task({ "task-id": taskID, "action": "uninstall-task", ...(execute_after_timestamp_ms ? { execute_after_timestamp_ms: execute_after_timestamp_ms } : {}), ...targetHostField });
    // getMapFromSubDoc(taskList).set(v4(), { credential: taskVC3 });
    writeFieldToSynapseSubdoc(voltClient, v4(), { credential: taskVC3 }, taskList.guid, mapname)

}




/**
 * Requests and retrieves metadata for a task.
 * @param {string} taskID - The unique ID for the task.
 * @param {Y.Doc} taskList - The Yjs map to store tasks and their credentials.
 * @param targetHostOptions - A `target_host` is only needed when the task's install was not driven by
 * that host. Validation is off unless asked for.
 * @returns {Promise<Object>} - Resolves with the metadata object for the task.
 */
export async function getTaskMetadata(taskID: string, taskList: Y.Doc, targetHostOptions: TargetHostOptions = {}): Promise<TaskMetadata> {
    const targetHostField = await buildTargetHostField(targetHostOptions, taskList);
    const taskVersionVC = create_signed_task({ "task-id": taskID, "action": "task-version", ...targetHostField });
    const metadataPromise = new Promise<TaskMetadata>((resolve, reject) => {
        handleWireSubscription(resolve, reject, taskID);
    });
    // getMapFromSubDoc(taskList).set(v4(), { credential: taskVersionVC });
    writeFieldToSynapseSubdoc(voltClient, v4(), { credential: taskVersionVC }, taskList.guid, mapname)

    return metadataPromise;

}

/**
 * Requests and retrieves status for a task.
 * @param {string} taskID - The unique ID for the task.
 * @param {Y.Doc} taskList - The Ydoc containing the tasks ymap
 * @param targetHostOptions - A `target_host` is only needed when the task's install was not driven by
 * that host. Validation is off unless asked for.
 * @returns  - Resolves with the host's status string for the task, such as `processing` or `stopped`.
 */
export async function getTaskStatus(taskID: string, taskList: Y.Doc, targetHostOptions: TargetHostOptions = {}): Promise<string> {
    const targetHostField = await buildTargetHostField(targetHostOptions, taskList);
    const taskStatusVC = create_signed_task({ "task-id": taskID, "action": "task-status", ...targetHostField });
    const taskStatusPromise = new Promise<string>(async (resolve, reject) => {
        try {
            const wireSubscription = await subscribeToWire(`wireid-${taskID}`);
            wireSubscription.onData(
                handleWireStatus(resolve, reject, wireSubscription, taskID)
            );

        } catch (error) {
            logger.error("Error subscribing to wire: %o", error);
            reject(error);
        }
    });
    // getMapFromSubDoc(taskList).set(v4(), { credential: taskStatusVC });
    writeFieldToSynapseSubdoc(voltClient, v4(), { credential: taskStatusVC }, taskList.guid, mapname)

    return taskStatusPromise;
}


/** Signs a task as the identity of the Volt configuration given to getAndInitialiseVoltClient. */
export function create_signed_task(task: SignedTaskCredential["credentialSubject"]): SignedTaskCredential {
    logger.info("task: %o", task);
    return signCredentialAs<SignedTaskCredential>(requireSigningIdentity(), [TASK_CREDENTIAL_TYPE], task);
}

const TASK_CREDENTIAL_TYPE = "taskCredential";
const TRUST_GRANT_CREDENTIAL_TYPE = "trustGrant";
const TRUSTED_ISSUERS_DOC_GUID = "trusted-issuers";
const ED25519_PUBLIC_KEY_LENGTH = 32;

function requireSigningIdentity(): SigningIdentity {
    if (!signingIdentity) throw new Error("call getAndInitialiseVoltClient before signing a credential");
    return signingIdentity;
}

/**
 * Signs a trust grant for an issuer and writes it to the trustedIssuers document. Hosts accept the
 * grant only when this library's identity is their trust root. The document exists once a host has
 * started against the synapse.
 */
export async function publishTrustGrant(request: TrustGrantRequest): Promise<TrustGrantCredential> {
    if (Buffer.from(request.publicKey, "base64").length !== ED25519_PUBLIC_KEY_LENGTH) {
        throw new Error(`the public key of ${request.issuerDid} must be a base64 ${ED25519_PUBLIC_KEY_LENGTH}-byte Ed25519 key`);
    }
    const grant = signCredentialAs<TrustGrantCredential>(
        requireSigningIdentity(),
        [TRUST_GRANT_CREDENTIAL_TYPE],
        {
            "issuer-did": request.issuerDid,
            "public-key": request.publicKey,
            ...(request.actions ? { actions: request.actions } : {})
        },
        request.validUntil ? { validUntil: request.validUntil } : {}
    );
    await writeFieldToSynapseSubdoc(voltClient, trustGrantKeyOf(request.issuerDid), grant, TRUSTED_ISSUERS_DOC_GUID, mapname);
    return grant;
}

/** Replaces the trust grant for an issuer with null, so hosts stop accepting its new tasks. */
export async function revokeTrustGrant(issuerDid: string): Promise<void> {
    await writeFieldToSynapseSubdoc(voltClient, trustGrantKeyOf(issuerDid), null, TRUSTED_ISSUERS_DOC_GUID, mapname);
}

/** The trust grants that a synced trustedIssuers document holds, without revoked entries. */
export function listTrustGrants(trustedIssuersDocument: Y.Doc): TrustGrantCredential[] {
    const grants = Array.from(trustedIssuersDocument.getMap<TrustGrantCredential | null>(mapname).values());
    return grants.filter((grant): grant is TrustGrantCredential => grant !== null);
}

export async function deleteWire(wire_id: string) {
    const deleteResourceRequest = { resource_id: `@${wire_id}`, recursive: true };
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
                    logger.info(`stdOut: ${currentLine}`);
                });
            }
        }
    });
}

interface SynapseDocumentRoot { name: string; type: "map" | "array" | "text"; jsonSchema: string }

async function setSynapseDocumentRoots(documentId: string, roots: SynapseDocumentRoot[]) {
    try {
        const registration = await voltClient.SetSynapseDocumentMetadata({
            database_id: SYNAPSE_ID,
            document_id: documentId,
            metadata: roots.map((root) => ({ name: root.name, type: root.type, json_schema: root.jsonSchema })),
        });
        if (registration.status?.code) {
            throw new Error(
                `Failed to register the roots of ${documentId}: ${registration.status.message}`,
            );
        }
        logger.info(`Set roots for subdoc ${documentId}: %o`, roots.map((root) => `${root.name} (${root.type})`));
    } catch (err) {
        console.error(`Error in setSynapseDocumentRoots for ${documentId}:`, err);
        throw err;
    }
}

/** The synapse document id of a task's default output pump sub-document. */
function externalPumpDocumentIdFor(taskId: string): string {
    return `external-pump-${taskId}`;
}

/**
 * Registers the roots an external pump document holds: the map root, the array a continuous task
 * appends its records to, and the text root a one-shot task's final result is written to. Registering
 * them is what lets a consumer watch the pump at `$.resultArray[*]` or at the result text root, which
 * the synapse reports nothing for while unregistered. All three are named in the one call because
 * registering replaces a document's metadata rather than adding to it.
 */
function registerExternalPumpDocumentRoots(externalPumpDocumentId: string, resultTextRootName: string) {
    return setSynapseDocumentRoots(externalPumpDocumentId, [
        { name: mapname, type: "map", jsonSchema: EXTERNAL_PUMP_TASK_SCHEMA },
        { name: RESULT_ARRAY_ROOT_NAME, type: "array", jsonSchema: EXTERNAL_PUMP_RESULT_ARRAY_SCHEMA },
        { name: resultTextRootName, type: "text", jsonSchema: EXTERNAL_PUMP_RESULT_TEXT_SCHEMA },
    ]);
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
    const externalPumpDocumentId = externalPumpDocumentIdFor(taskId);
    if (!externalPumpMap.has(taskId)) {
        externalPumpMap.set(taskId, new Y.Doc({ guid: externalPumpDocumentId }));
        const taskPump = externalPumpMap.get(taskId)
        if (!taskPump) {
            throw new Error("Failed to create task pump doc for taskId: " + taskId);
        }
        waitForDocSync(taskPump).then(() => {
            registerExternalPumpDocumentRoots(externalPumpDocumentId, DEFAULT_RESULT_TEXT_ROOT_NAME);
        });
    }

    const taskPumpDoc = externalPumpMap.get(taskId);
    if (!taskPumpDoc) {
        throw new Error("No task pump doc found for taskId: " + taskId);
    }
    taskPumpDoc.load();
    await waitForDocSync(taskPumpDoc);
    let taskPumpArray: YArray<object> = taskPumpDoc.getArray(RESULT_ARRAY_ROOT_NAME);
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
 * Retrieves a one-shot task's final result text from its default output pump document, rejecting when
 * the task has produced no output.
 *
 * The result is read from the pump's named text root — {@link DEFAULT_RESULT_TEXT_ROOT_NAME} unless
 * the run named another with `output_pump_root`, in which case pass that same name here. A pump
 * written by a host predating the named root is still read, from the document's unnamed text root.
 * @param ydoc - The synapse root document.
 * @param taskId - The unique ID of the task whose result is wanted.
 * @param resultTextRootName - The pump text root to read, defaulting to `resultText`.
 */
export async function getTaskOutputJson(ydoc: Y.Doc, taskId: string, resultTextRootName: string = DEFAULT_RESULT_TEXT_ROOT_NAME): Promise<string> {

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

        const resultText = readPumpResultText(taskPumpDoc, resultTextRootName);
        if (resultText.length > 0) {
            resolve(resultText);
        } else {
            logger.error("No output found for taskId: %s", taskId);
            reject("No output found");
        }
    });
}

/**
 * Returns a pump document's result text, taken from the named text root and falling back to the
 * document's unnamed root, which is where a host predating the named root wrote it.
 */
function readPumpResultText(taskPumpDoc: Y.Doc, resultTextRootName: string): string {
    return taskPumpDoc.getText(resultTextRootName).toString() || taskPumpDoc.getText().toString();
}


async function handleWireSubscription(resolve: { (value: TaskMetadata | PromiseLike<TaskMetadata>): void; (arg0: any): void; }, reject: { (reason?: any): void; (arg0: unknown): void; }, taskID: string) {
    try {
        const wireSubscription = await subscribeToWire(`wireid-${taskID}`);
        wireSubscription.onData((chunk: string, allChunks: string[], error: Error) => {
            if (error) {
                logger.error("Error receiving task metadata: %o", error);
                reject(error);
                wireSubscription.close();
                return;
            }
            if (chunk) {
                const replyText = String(chunk);
                logger.info("Received task metadata chunk: %s", replyText);
                try {
                    if (isLifecycleAnnouncement(replyText, taskID)) {
                        logger.info("Lifecycle announcement %s arrived before the metadata, ignoring it", replyText);
                        return;
                    }
                    const metadata = JSON.parse(replyText);
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

function handleWireStatus(resolve: (status: string) => void, reject: { (reason?: any): void; (arg0: unknown): void; }, wireSubscription: WireSubscription, taskID: string) {
    return function (chunk: string, allChunks: string[], error: Error) {
        if (error) {
            logger.error("Error receiving task status: %o", error);
            reject(error);
            wireSubscription.close();
            return;
        }
        if (chunk) {
            const replyText = String(chunk);
            logger.info("Received task status chunk: %s", replyText);
            if (isLifecycleAnnouncement(replyText, taskID)) {
                logger.info("Lifecycle announcement %s arrived before the status, ignoring it", replyText);
                return;
            }
            resolve(replyText);
            wireSubscription.close();
        }
    };
}

function isLifecycleAnnouncement(chunk: string, taskID: string): boolean {
    return ["task-finished-", "task-stopped-", "task-failed-"].some(prefix => chunk === prefix + taskID);
}

const TASK_CREATION_POLL_INTERVAL_MS = 1000;
const TASK_ASSIGNMENT_POLL_INTERVAL_MS = 500;

function delay(milliseconds: number): Promise<void> {
    return new Promise(resolve => setTimeout(resolve, milliseconds));
}

/**
 * The result of waiting for a task to be assigned.
 *
 * - `"assigned"` — the task was picked up by an agent; `assignment` holds the assignee.
 * - `"task-not-created"` — the task never appeared in the task list within the allotted attempts.
 * - `"assignment-timed-out"` — the task was created but no agent picked it up in time.
 *
 * Read `status` to determine which case occurred before using `assignment`.
 */
export type TaskAssignmentOutcome =
    | { status: "assigned"; assignment: string }
    | { status: "task-not-created"; taskId: string; attempts: number }
    | { status: "assignment-timed-out"; taskId: string; attempts: number };

async function waitForTaskEntryToExist(
    taskList: Y.Map<SignedTaskCredentialWrapper>,
    taskId: string,
    maxAttempts: number
): Promise<SignedTaskCredentialWrapper | undefined> {
    let attempts = 0;
    let taskEntry = taskList.get(taskId);
    while (!taskEntry && attempts < maxAttempts) {
        await delay(TASK_CREATION_POLL_INTERVAL_MS);
        taskEntry = taskList.get(taskId);
        attempts++;
    }
    return taskEntry;
}

const SCHEDULING_DOC_NAMES = ["nodeScheduling", "pythonScheduling"];

/**
 * The host recorded as owner of a task in the scheduling overlay, or null if unassigned or the
 * overlay is not present. Assignment lives at `<taskId>:assigned`, no longer on the task entry.
 */
function readScheduledOwner(rootDoc: Y.Doc, taskId: string): string | null {
    const rootMap: Y.Map<Y.Doc> = rootDoc.getMap(mapname);
    for (const schedulingDocName of SCHEDULING_DOC_NAMES) {
        const schedulingDoc = rootMap.get(schedulingDocName);
        if (!schedulingDoc) continue;
        schedulingDoc.load();
        const owner = schedulingDoc.getMap(mapname).get(`${taskId}:assigned`);
        if (typeof owner === "string" && owner.length > 0) return owner;
    }
    return null;
}

/** The owner of a task, from the scheduling overlay first and the task entry as a fallback. */
function resolveTaskOwner(
    taskEntry: SignedTaskCredentialWrapper | undefined,
    rootDoc: Y.Doc | undefined,
    taskId: string
): string | null {
    if (taskEntry?.assigned) return taskEntry.assigned;
    if (rootDoc) return readScheduledOwner(rootDoc, taskId);
    return null;
}

/**
 * Waits for a task to appear in the task list and then for a host to own it, reporting which of the
 * three outcomes occurred.
 *
 * Pass `rootDoc` whenever the hosts run with `USE_SCHEDULING_DOC=1`, which is required for
 * `target_host`: ownership then lives at `<taskId>:assigned` in the runtime scheduling
 * sub-document and the task entry's own `assigned` field stays null for every task.
 *
 * A targeted task is owned by its target almost immediately, with no claim round trip. If it stays
 * unassigned, the target is offline, unknown, lacking the runtime, or at its concurrency limit — no
 * other host will take over, so this resolves as `"assignment-timed-out"` rather than reassigning.
 */
export async function waitForTaskAssigned(
    taskId: string,
    taskListDoc: Y.Doc,
    maxAttemptsCreation = 5,
    maxAttemptsAssignment = 1000,
    rootDoc?: Y.Doc
): Promise<TaskAssignmentOutcome> {
    taskListDoc.load();
    const taskList: Y.Map<SignedTaskCredentialWrapper> = taskListDoc.getMap(mapname);

    let taskEntry = await waitForTaskEntryToExist(taskList, taskId, maxAttemptsCreation);
    if (!taskEntry) {
        return { status: "task-not-created", taskId, attempts: maxAttemptsCreation };
    }

    let attempts = 0;
    while (!resolveTaskOwner(taskEntry, rootDoc, taskId) && attempts < maxAttemptsAssignment) {
        await delay(TASK_ASSIGNMENT_POLL_INTERVAL_MS);
        taskListDoc.load();
        taskEntry = taskList.get(taskId);
        attempts++;
    }

    const owner = resolveTaskOwner(taskEntry, rootDoc, taskId);
    if (owner) {
        return { status: "assigned", assignment: owner };
    }
    return { status: "assignment-timed-out", taskId, attempts };
}
