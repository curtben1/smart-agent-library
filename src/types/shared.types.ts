export interface SignedTaskCredential {
  "@context": string[];
  id: string;
  type: string[];
  issuer: string;
  validFrom: string;
  credentialSubject: {
    "task-id": string;
    action: string;
    name?: string;
    location?: string;
    source?: string;
    "task_finished-indicator"?: string;
    cli_args?: string;
    [key: string]: any;
  };
  proof: {
    type: string;
    created: string;
    proofPurpose: string;
    verificationMethod: string;
    proofValue: string;
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