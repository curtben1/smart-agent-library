import type * as Y from "yjs";

export type AgentRuntime = "node" | "python";

export type AuthoredJsonSchema = Record<string, unknown>;

export interface SpecifiedSchemaValue {
    state: "specified";
    schema: AuthoredJsonSchema;
}

export interface UnspecifiedSchemaValue {
    state: "unspecified";
}

export interface UnreadableSchemaValue {
    state: "unreadable";
    problem: string;
}

export type SchemaValue =
    | SpecifiedSchemaValue
    | UnspecifiedSchemaValue
    | UnreadableSchemaValue;

export interface TranslationSchemaEntry {
    producerKey: string;
    consumerKey: string;
}

export type TranslationSchema = TranslationSchemaEntry[];

export type RootType = "Y.Map" | "Y.Array" | "Y.Text";

export type ExecutionMode = "continuous" | "oneShot";

export type SemanticVersion = 2 | 3;

export interface FlowDocument {
    id: string;
    synapseId: string;
    rootType: RootType;
    rootTypeName: string;
    watchPath: string;
    requiresInitialisation: boolean;
    producedExternally: boolean;
    contentSchema: SchemaValue;
}

export interface InputSource {
    synapseId: string;
    documentId: string;
    path: [string, ...string[]];
}

export type AdditionalParseOptions =
    | { keyJsonata: string; bodyJsonata?: never; newSchema?: never }
    | { keyJsonata: string; bodyJsonata: string; newSchema: AuthoredJsonSchema };

export type OutputLocation = {
    synapseId: string;
    documentId: string;
} & (
        | { writeKey?: string; path: string; additionalParseOptions?: never; outputPumpRoot?: never }
        | { writeKey?: never; path: string; additionalParseOptions: AdditionalParseOptions; outputPumpRoot?: never }
        | { writeKey?: never; path?: never; additionalParseOptions?: never; outputPumpRoot?: string }
    );

export interface Agent {
    id: string;
    name: string;
    runtime: AgentRuntime;
    executionMode: ExecutionMode;
    marketplaceLink: string;
    inputSource: InputSource | InputSource[] | null;
    outputLocation: OutputLocation | null;
    inputTranslationSchema: TranslationSchema;
    outputTranslationSchema: TranslationSchema;
}

export interface SemanticFlowExport {
    semanticVersion: SemanticVersion;
    synapseId: string;
    documents: FlowDocument[];
    agents: Agent[];
}

export type AuthoredAgent = Omit<Agent, "id" | "executionMode">
    & { id?: string; executionMode?: ExecutionMode };

export type AuthoredFlowExport = Omit<SemanticFlowExport, "agents"> & { agents: AuthoredAgent[] };

export interface TaskListEntry {
    credential: {
        validFrom?: string;
        credentialSubject: {
            "task-id": string;
            action: string;
            name?: string;
            location?: string;
            target_host?: string;
            output_pump_root?: string;
        };
    };
    assigned?: string | null;
}

export interface SynapseDocumentRoot {
    name: string;
    type: "map" | "array" | "text";
    jsonSchema: string;
}

export interface FlowTaskList {
    document: Y.Doc;
    entries: Y.Map<TaskListEntry>;
}

export interface FlowDeploymentConnection {
    synapseId: string;
    rootDocument: Y.Doc;
    taskListFor(runtime: AgentRuntime): FlowTaskList;
    registerDocumentRoots(synapseId: string, documentId: string, roots: SynapseDocumentRoot[]): Promise<void>;
}

export interface FlowDeploymentOptions {
    createsEveryDocument?: boolean;
    enforcesContentSchema?: boolean;
    publishTimeoutSeconds?: number;
    targetHostId?: string;
    validatesTargetHost?: boolean;
    installSource?: string;
    executeAfterSeconds?: number;
    taskCommandLineArguments?: string;
    onProgress?: (event: FlowDeploymentEvent) => void;
}

export type FlowDeploymentEvent =
    | { kind: "documentCreated"; synapseId: string; documentId: string; rootTypeName: string }
    | { kind: "agentInstalled"; agentName: string; taskId: string; runtime: AgentRuntime }
    | { kind: "agentStarted"; agentName: string; taskId: string; runtime: AgentRuntime };

export interface PlannedFlowAgent {
    name: string;
    taskId: string;
    runtime: AgentRuntime;
    executionMode: ExecutionMode;
}

export interface FlowDeploymentPlan {
    flow: SemanticFlowExport;
    documentsToCreate: FlowDocument[];
    agents: PlannedFlowAgent[];
}

export interface DeployedFlowAgent {
    name: string;
    taskId: string;
    runtime: AgentRuntime;
}

export interface FlowDeploymentResult {
    agents: DeployedFlowAgent[];
    createdDocumentIds: string[];
}
