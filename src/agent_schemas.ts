// @ts-expect-error - Using JS module without types
import { VoltClient } from "@tdxvolt/volt-client-grpc"


const SYNAPSE_ID = "@agent-synapse-1";

// === SCHEMA DEFINITIONS ===

const TASK_LIST_ROOT_SCHEMA = JSON.stringify({
  type: "object",
  properties: {
    pythonTaskList: {
      type: "object",
      description: "Sub-document reference for Python runtime tasks",
    },
    nodeTaskList: {
      type: "object",
      description: "Sub-document reference for Node.js runtime tasks",
    },
    agentList: {
      type: "object",
      description: "Sub-document reference for registered agent hosts",
    },
    externalPumps: {
      type: "object",
      description: "Sub-document reference for task output pumps",
    },
    taskOutputs: {
      type: "object",
      description: "Sub-document reference for streaming stdout output",
    },
    carriers: {
      type: "object",
      description: "Sub-document reference for load balancer carriers",
    },
  },
  additionalProperties: true,
});

const AGENT_LIST_SCHEMA = JSON.stringify({
  type: "object",
  additionalProperties: {
    type: "object",
    description: "Agent host entry keyed by host_id (UUID)",
    properties: {
      runtimes: {
        type: "array",
        items: { type: "string" },
        description: "Supported runtimes e.g. ['python', 'nodejs']",
      },
      lastSeen: {
        type: "string",
        format: "date-time",
        description: "ISO 8601 heartbeat timestamp",
      },
      assignedTasks: {
        type: "array",
        items: { type: "string" },
        description: "List of assigned task IDs (UUIDs)",
      },
    },
    required: ["runtimes", "lastSeen"],
  },
});

const TASK_LIST_SCHEMA = JSON.stringify({
  type: "object",
  additionalProperties: {
    type: "object",
    description: "Task entry keyed by agentId (UUID)",
    properties: {
      credential: {
        type: "object",
        description: "Signed Verifiable Credential containing the task",
        properties: {
          "@context": {
            type: "array",
            items: { type: "string" },
          },
          id: {
            type: "string",
            description: "urn:uuid:{uuid}",
          },
          type: {
            type: "array",
            items: { type: "string" },
          },
          issuer: {
            type: "string",
            description: "DID of the issuer",
          },
          validFrom: {
            type: "string",
            format: "date-time",
            description: "ISO 8601 issuance timestamp",
          },
          credentialSubject: {
            type: "object",
            properties: {
              "task-id": {
                type: "string",
                description: "Unique task identifier (UUID)",
              },
              action: {
                type: "string",
                enum: [
                  "new-task",
                  "run-task",
                  "uninstall-task",
                  "task-status",
                  "task-metadata",
                ],
                description: "Task action type",
              },
              location: {
                type: "string",
                description:
                  "Download URL or marketplace file ID (for new-task)",
              },
              source: {
                type: "string",
                enum: ["http", "https", "marketplace"],
                description: "Source type (for new-task)",
              },
              std_in: {
                type: "object",
                description: "Structured stdin input (for run-task)",
              },
              cli_args: {
                type: "string",
                description: "CLI arguments string (for run-task)",
              },
              continuous: {
                type: "boolean",
                description:
                  "If true, task runs continuously with streaming output",
              },
              translation_schema: {
                type: "object",
                description: "Schema translation map for input remapping",
              },
              source_schema: {
                type: "object",
                description:
                  "Source JSON schema (used with translation_schema)",
              },
            },
            required: ["task-id", "action"],
          },
          proof: {
            type: "object",
            description: "Cryptographic signature proof",
          },
        },
        required: [
          "@context",
          "id",
          "type",
          "issuer",
          "validFrom",
          "credentialSubject",
          "proof",
        ],
      },
      assigned: {
        type: ["string", "null"],
        description:
          "host_id of the agent that claimed the task, null if unassigned",
      },
    },
    required: ["credential"],
  },
});

const EXTERNAL_PUMPS_SCHEMA = JSON.stringify({
  type: "object",
  additionalProperties: {
    type: "object",
    description: "Per-task output pump sub-document keyed by task_id (UUID)",
  },
});

const EXTERNAL_PUMP_TASK_SCHEMA = JSON.stringify({
  type: "object",
  properties: {
    resultArray: {
      type: "array",
      items: { type: "object" },
      description: "Array of schema-conforming output objects",
    },
  },
  additionalProperties: true,
  description:
    "Per-task pump doc. Default Y.Text holds final output text. resultArray holds structured output objects.",
});

const TASK_OUTPUTS_SCHEMA = JSON.stringify({
  type: "object",
  additionalProperties: {
    type: "object",
    description:
      "Per-task streaming stdout sub-document keyed by task_id (UUID)",
  },
});

const TASK_OUTPUT_TASK_SCHEMA = JSON.stringify({
  type: "object",
  additionalProperties: true,
  description:
    "Per-task output doc. Default Y.Text holds streaming stdout lines.",
});

const CARRIERS_SCHEMA = JSON.stringify({
  type: "object",
  additionalProperties: {
    type: "object",
    description:
      "Per-carrier instruction sub-document keyed by carrier_id (UUID)",
  },
});

const CARRIER_INSTANCE_SCHEMA = JSON.stringify({
  type: "object",
  additionalProperties: {
    type: "string",
    enum: ["add_host", "remove_host"],
    description:
      "Carrier instruction keyed by stringified index. Values: add_host, remove_host, added_host:{host_id}, removed_host:{host_id}",
  },
});

async function writeFieldToSynapseSubdoc(voltClient: VoltClient, field: string, value: object, documentId: string, pathPrefix: string) {
  const jsonPath = pathPrefix ? `$.${pathPrefix}.${field}` : `$.${field}`;

  try {

    const resp = await voltClient.WriteSynapsePath({
      database_id: SYNAPSE_ID,
      document_id: documentId,
      path: jsonPath,
      json: JSON.stringify(value),
    });
    if (resp.status?.code) {
      throw new Error(`WriteSynapsePath failed: ${resp.status.message}`);
    }
    return resp;
  }
  catch (e) {
    console.error(`Error writing to Synapse subdoc ${documentId} field ${field} with content ${JSON.stringify(value)}:`, e);
    throw e;
  }
}

// === SETUP FUNCTION ===

export {
  AGENT_LIST_SCHEMA,
  CARRIER_INSTANCE_SCHEMA,
  CARRIERS_SCHEMA,
  EXTERNAL_PUMP_TASK_SCHEMA,
  EXTERNAL_PUMPS_SCHEMA,
  TASK_LIST_ROOT_SCHEMA,
  TASK_LIST_SCHEMA,
  TASK_OUTPUT_TASK_SCHEMA,
  TASK_OUTPUTS_SCHEMA,
  SYNAPSE_ID,
  writeFieldToSynapseSubdoc
};
