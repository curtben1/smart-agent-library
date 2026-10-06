import type { SynapseWriteObject } from "../types/shared.types.js";
import type {
    AdditionalParseOptions,
    Agent,
    AuthoredJsonSchema,
    DeployedFlowAgent,
    FlowDeploymentConnection,
    FlowDeploymentEvent,
    FlowDeploymentOptions,
    FlowDeploymentPlan,
    FlowDeploymentResult,
    FlowDocument,
    FlowTaskList,
    InputSource,
    OutputLocation,
    RootType,
    SemanticFlowExport,
    SynapseDocumentRoot
} from "../types/flow.types.js";
import { taskCredentialPublishers } from "./taskCredentialPublishers.js";
import { watchForTaskListEntry, type PublishedTaskAction } from "./taskListEntryWatching.js";

const DEFAULT_PUBLISH_TIMEOUT_SECONDS = 60;
const DEFAULT_INSTALL_SOURCE = "http";

const SHARED_TYPE_BY_ROOT_TYPE: Record<RootType, SynapseDocumentRoot["type"]> = {
    "Y.Map": "map",
    "Y.Array": "array",
    "Y.Text": "text"
};

const PERMISSIVE_MAP_SCHEMA = { type: "object", additionalProperties: true };

/**
 * Returns what {@link deploySemanticFlow} would do with the same flow and options, and writes
 * nothing. The agents keep the order of `flow.agents`.
 */
export function planSemanticFlowDeployment(
    flow: SemanticFlowExport,
    options: FlowDeploymentOptions = {}
): FlowDeploymentPlan {
    return {
        flow,
        documentsToCreate: documentsToCreate(flow, options),
        agents: flow.agents.map(agent => ({
            name: agent.name,
            taskId: agent.id,
            runtime: agent.runtime,
            executionMode: agent.executionMode
        }))
    };
}

/**
 * Creates the documents of a flow, then installs every agent, then starts every agent. Each agent goes
 * to the task list of its own runtime, under the task id the flow gives it. Each step is reported
 * through `onProgress` once it is on the synapse.
 *
 * Rejects at the first step that fails, with a message that names the step, the document or agent,
 * and the cause. The steps that succeeded before it stay in place.
 */
export async function deploySemanticFlow(
    connection: FlowDeploymentConnection,
    flow: SemanticFlowExport,
    options: FlowDeploymentOptions = {}
): Promise<FlowDeploymentResult> {
    const reportProgress = options.onProgress ?? (() => undefined);
    const createdDocumentIds: string[] = [];

    for (const flowDocument of documentsToCreate(flow, options)) {
        await createFlowDocument(connection, flowDocument, options);
        createdDocumentIds.push(flowDocument.id);
        reportProgress(documentCreatedEvent(flowDocument));
    }
    for (const agent of flow.agents) {
        await installAgent(connection, agent, options);
        reportProgress(agentEvent("agentInstalled", agent));
    }
    for (const agent of flow.agents) {
        await startAgent(connection, agent, options);
        reportProgress(agentEvent("agentStarted", agent));
    }

    return { agents: flow.agents.map(deployedAgent), createdDocumentIds };
}

function documentsToCreate(flow: SemanticFlowExport, options: FlowDeploymentOptions): FlowDocument[] {
    return flow.documents.filter(flowDocument => options.createsEveryDocument || flowDocument.requiresInitialisation);
}

async function createFlowDocument(
    connection: FlowDeploymentConnection,
    flowDocument: FlowDocument,
    options: FlowDeploymentOptions
): Promise<void> {
    await describeFailureAs(`could not create document ${flowDocument.id} on ${flowDocument.synapseId}`, () =>
        connection.registerDocumentRoots(flowDocument.synapseId, flowDocument.id, [
            documentRoot(flowDocument, options.enforcesContentSchema ?? false)
        ]));
}

function documentRoot(flowDocument: FlowDocument, enforcesContentSchema: boolean): SynapseDocumentRoot {
    return {
        name: flowDocument.rootTypeName,
        type: SHARED_TYPE_BY_ROOT_TYPE[flowDocument.rootType],
        jsonSchema: JSON.stringify(rootSchema(flowDocument, enforcesContentSchema))
    };
}

/**
 * Returns the JSON schema to register against the root of a document. A permissive root accepts any
 * entry. Enforcement of the content schema makes the volt refuse an entry that the schema refuses.
 */
function rootSchema(flowDocument: FlowDocument, enforcesContentSchema: boolean): object {
    if (flowDocument.rootType === "Y.Text") return { type: "string" };

    const contentSchema = specifiedContentSchema(flowDocument);
    if (!enforcesContentSchema || !contentSchema) {
        return flowDocument.rootType === "Y.Array" ? { type: "array" } : PERMISSIVE_MAP_SCHEMA;
    }

    return flowDocument.rootType === "Y.Array"
        ? { type: "array", items: contentSchema }
        : { type: "object", additionalProperties: contentSchema };
}

function specifiedContentSchema(flowDocument: FlowDocument): AuthoredJsonSchema | null {
    return flowDocument.contentSchema.state === "specified" ? flowDocument.contentSchema.schema : null;
}

async function installAgent(
    connection: FlowDeploymentConnection,
    agent: Agent,
    options: FlowDeploymentOptions
): Promise<void> {
    await describeFailureAs(`could not install ${agent.name} as task ${agent.id} on the ${agent.runtime} task list`, () => {
        const taskList = connection.taskListFor(agent.runtime);
        return publishAndAwaitEntry(taskList, { taskId: agent.id, action: "new-task" }, options, () =>
            taskCredentialPublishers.installTask({
                taskID: agent.id,
                taskName: agent.name,
                taskLocation: agent.marketplaceLink,
                sourceType: options.installSource ?? DEFAULT_INSTALL_SOURCE,
                taskList: taskList.document,
                ...executeAfterField(options),
                ...targetHostFields(connection, options)
            }));
    });
}

async function startAgent(
    connection: FlowDeploymentConnection,
    agent: Agent,
    options: FlowDeploymentOptions
): Promise<void> {
    await describeFailureAs(`could not start ${agent.name} as task ${agent.id} on the ${agent.runtime} task list`, () => {
        const taskList = connection.taskListFor(agent.runtime);
        return publishAndAwaitEntry(taskList, { taskId: agent.id, action: "run-task" }, options, () =>
            taskCredentialPublishers.startTask({
                taskID: agent.id,
                taskList: taskList.document,
                continuous: agent.executionMode === "continuous",
                ...optionalField("cli_args", options.taskCommandLineArguments),
                ...optionalField("std_in", toStandardInput(agent.inputSource)),
                ...optionalField("synapse_write_path", toSynapseWritePath(agent.outputLocation)),
                ...optionalField("output_pump_root", agent.outputLocation?.outputPumpRoot),
                ...executeAfterField(options)
            }));
    });
}

/**
 * Publishes, and resolves only once the entry for the given action is on the task list. The publish
 * functions return before the volt holds the write, so a caller that stops at the publish can lose it.
 */
async function publishAndAwaitEntry(
    taskList: FlowTaskList,
    publishedAction: PublishedTaskAction,
    options: FlowDeploymentOptions,
    publish: () => Promise<void>
): Promise<void> {
    const entryHasAppeared = watchForTaskListEntry(taskList.entries, publishedAction);
    await publish();
    await rejectAfterDeadline(
        entryHasAppeared,
        options.publishTimeoutSeconds ?? DEFAULT_PUBLISH_TIMEOUT_SECONDS,
        `the ${publishedAction.action} entry to reach the task list`
    );
}

async function rejectAfterDeadline(
    pendingWork: Promise<void>,
    deadlineSeconds: number,
    whatIsBeingWaitedFor: string
): Promise<void> {
    let deadlineTimer: ReturnType<typeof setTimeout> | undefined;
    const deadlineRejection = new Promise<never>((_resolve, reject) => {
        deadlineTimer = setTimeout(
            () => reject(new Error(`timed out after ${deadlineSeconds}s waiting for ${whatIsBeingWaitedFor}`)),
            deadlineSeconds * 1000
        );
    });

    try {
        await Promise.race([pendingWork, deadlineRejection]);
    } finally {
        clearTimeout(deadlineTimer);
    }
}

async function describeFailureAs(failedStep: string, work: () => Promise<void>): Promise<void> {
    try {
        await work();
    } catch (stepFailure) {
        const cause = stepFailure instanceof Error ? stepFailure.message : String(stepFailure);
        throw new Error(`${failedStep}: ${cause}`);
    }
}

function executeAfterField(options: FlowDeploymentOptions): { execute_after_timestamp_ms?: number } {
    if (options.executeAfterSeconds === undefined) return {};
    return { execute_after_timestamp_ms: Date.now() + options.executeAfterSeconds * 1000 };
}

function targetHostFields(
    connection: FlowDeploymentConnection,
    options: FlowDeploymentOptions
): { target_host?: string; validate_target_host?: boolean; rootDoc?: FlowDeploymentConnection["rootDocument"] } {
    if (options.targetHostId === undefined) return {};
    return {
        target_host: options.targetHostId,
        validate_target_host: options.validatesTargetHost ?? false,
        rootDoc: connection.rootDocument
    };
}

function optionalField<FieldName extends string, FieldValue>(
    fieldName: FieldName,
    fieldValue: FieldValue | undefined
): Partial<Record<FieldName, FieldValue>> {
    if (fieldValue === undefined) return {};
    return { [fieldName]: fieldValue } as Record<FieldName, FieldValue>;
}

function toStandardInput(inputSource: InputSource | InputSource[] | null): object | undefined {
    if (inputSource === null) return undefined;
    if (Array.isArray(inputSource)) return inputSource.map(toWatchAddress);
    return toWatchAddress(inputSource);
}

function toWatchAddress(inputSource: InputSource): object {
    return {
        synapse_id: inputSource.synapseId,
        document_id: inputSource.documentId,
        path: inputSource.path
    };
}

function toSynapseWritePath(outputLocation: OutputLocation | null): SynapseWriteObject | undefined {
    if (!outputLocation || outputLocation.path === undefined) return undefined;

    return {
        synapse_id: outputLocation.synapseId,
        document_id: outputLocation.documentId,
        path: outputLocation.path,
        ...toParseOptionsField(outputLocation.additionalParseOptions)
    };
}

function toParseOptionsField(
    parseOptions: AdditionalParseOptions | undefined
): Pick<SynapseWriteObject, "additional_parse_options"> {
    if (!parseOptions) return {};
    if (parseOptions.bodyJsonata === undefined) {
        return { additional_parse_options: { key_jsonata: parseOptions.keyJsonata } };
    }

    return {
        additional_parse_options: {
            key_jsonata: parseOptions.keyJsonata,
            body_jsonata: parseOptions.bodyJsonata,
            new_schema: parseOptions.newSchema
        }
    };
}

function documentCreatedEvent(flowDocument: FlowDocument): FlowDeploymentEvent {
    return {
        kind: "documentCreated",
        synapseId: flowDocument.synapseId,
        documentId: flowDocument.id,
        rootTypeName: flowDocument.rootTypeName
    };
}

function agentEvent(kind: "agentInstalled" | "agentStarted", agent: Agent): FlowDeploymentEvent {
    return { kind, agentName: agent.name, taskId: agent.id, runtime: agent.runtime };
}

function deployedAgent(agent: Agent): DeployedFlowAgent {
    return { name: agent.name, taskId: agent.id, runtime: agent.runtime };
}
