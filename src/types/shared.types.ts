import { VerifiableCredential } from "verifiable-credential-toolkit";

/**
 * JSONata expressions the host evaluates against a task's parsed output before a `synapse_write_path`
 * write, making the destination per record and optionally reshaping what is written.
 *
 * `key_jsonata` is concatenated onto `path` verbatim, so the expression or the `path` has to supply the
 * separator; the bracketed double-quoted form (`'["' & vessel & '"]'`) is the safe one. Only two shapes
 * are accepted: `key_jsonata` alone, or all three fields — the host refuses a body without a schema and
 * a schema without a body. `new_schema` describes the transformed body only, which is a different shape
 * from the agent's own packaged output schema.
 */
export type SynapseWriteAdditionalParseOptions =
    | { key_jsonata: string; body_jsonata?: never; new_schema?: never }
    | { key_jsonata: string; body_jsonata: string; new_schema: object };

/**
 * A destination on the synapse for a copy of a task's output, written by the host in addition to the
 * default output pump. With `additional_parse_options` the write becomes a keyed collection under
 * `path` rather than a single overwritten key.
 */
export interface SynapseWriteObject {
    synapse_id: string;
    document_id: string;
    path: string;
    additional_parse_options?: SynapseWriteAdditionalParseOptions;
}

export interface SignedTaskCredential extends VerifiableCredential {
    credentialSubject: {
        "task-id": string;
        action: "new-task" | "run-task" | "task-version" | "uninstall-task" | "task-status";
        name?: string;
        location?: string;
        source?: string;
        "task_finished-indicator"?: string;
        cli_args?: string;
        std_in?: object
        [key: string]: any;
        translation_schema?: object;
        continuous?: boolean;
        outer_output_pump_location?: string;
        output_pump_root?: string;
        synapse_write_path?: SynapseWriteObject;
        execute_after_timestamp_ms?: number;
        target_host?: string;
    };
}

/**
 * The outcome of checking a `target_host` against the `agentList` before publishing a targeted task.
 * Only `"usable"` means the named host is live and able to run the task; every other status names a
 * condition that would leave a targeted task queued forever, because no other host will take it.
 */
export type TargetHostValidation =
    | { status: "usable"; targetHost: string }
    | { status: "agent-list-unavailable"; targetHost: string }
    | { status: "unknown-host"; targetHost: string }
    | { status: "stale-host"; targetHost: string; lastSeen?: string; millisecondsSinceLastSeen?: number }
    | {
        status: "runtime-unsupported";
        targetHost: string;
        requiredRuntimes: string[];
        hostRuntimes: string[];
    };

export interface SignedTaskCredentialWrapper {
    credential: SignedTaskCredential;
    assigned?: string;
}

export interface AgentInfo {
    runtimes?: string[];
    lastSeen?: string;
    assignedTasks?: string[];
}

declare module "yjs" {
    interface Doc {
        _syncWaitPromise?: Promise<void>;
    }
}