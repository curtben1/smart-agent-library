import { VerifiableCredential } from "verifiable-credential-toolkit";

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