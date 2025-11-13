export interface TomlConfig {
  capabilities: {
    enabled: string[];
  };
  redis: {
    redis_port: number;
    redis_host: string;
  };
  folders: {
    incoming_path: string;
    execution_path: string;
    outgoing_path: string;
    marketplace_path: string;
  };
  volt: {
    sync_database_id: string;
    volt_config_path: string;
  };
}

export interface TaskCredentialSubject {
  "task-id": string;
  action: "new-task" | "run-task" | "uninstall-task" | "task-status" | "task-metadata";
  location?: string;
  source?: "http" | "https" | "marketplace";
  cli_args?: string[];
}

export interface TaskCredential {
  credentialSubject: TaskCredentialSubject;
}

export interface TaskEntry {
  credential: TaskCredential;
  assigned?: string;
}

export interface AgentInfo {
  runtimes: string[];
  lastSeen: string;
  assignedTasks?: string[];
}

export interface TaskMetadata {
  cli_args?: string;
  pump_info: {
    pump_type: number;
    substring_lines?: number;
    split_string?: string;
    split_index?: number;
    local_path?: string;
  };
}

export const PumpTypes = {
  stdout_whole: 1,
  stdout_substring: 2,
  stdout_split: 3,
  local_files: 4,
  unhandled: 5
} as const;

export type PumpType = typeof PumpTypes[keyof typeof PumpTypes];