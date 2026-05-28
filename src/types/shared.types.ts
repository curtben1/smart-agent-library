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
    };
}

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