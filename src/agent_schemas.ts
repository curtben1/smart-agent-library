// @ts-expect-error - Using JS module without types
import { VoltClient } from "@tdxvolt/volt-client-grpc"
import { add } from "winston";


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
    description: "Task entry keyed by task ID (UUID)",
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
              name: {
                type: "string",
                description: "Human-readable task name (for new-task)",
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
                additionalProperties: true,
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
              outer_output_pump_location: {
                type: "string",
                description:
                  "Where on the synapse you want the data to be written to. Should be custom path format described in root readme, '.' separated mapname, ':' separated keys'",
              },
              synapse_write_path: {
                type: "object",
                description:
                  "Where to write a copy of the output using WriteSynapsePath",
                properties: {
                  synapse_id: { type: "string" },
                  document_id: { type: "string" },
                  path: { type: "string" },
                  additional_parse_options: {
                    type: "object",
                    description:
                      "JSONata expressions the host evaluates against the parsed output before the write, making the destination per record and optionally reshaping the written body",
                    properties: {
                      key_jsonata: {
                        type: "string",
                        description:
                          "JSONata whose result is appended to path verbatim to form the path written, so it or path must supply the separator. Prefer the bracketed double-quoted form",
                      },
                      body_jsonata: {
                        type: "string",
                        description:
                          "JSONata producing the JSON written in place of the raw output. Requires new_schema",
                      },
                      new_schema: {
                        type: "object",
                        description:
                          "JSON Schema 2020-12 the body_jsonata result is validated against, describing the transform's shape rather than the agent's own output schema",
                      },
                    },
                    required: ["key_jsonata"],
                    additionalProperties: false,
                  },
                },
                required: ["synapse_id", "document_id", "path"],
                additionalProperties: false,
              },
              output_pump_root: {
                type: "string",
                description:
                  "Name of the text root a non-continuous task's final output is written to in its default output pump document, watched at $.<name>. Defaults to resultText",
              },
              execute_after_timestamp_ms: {
                type: "number",
                description:
                  "If set, the task will not be eligible for assignment until the specified Unix timestamp in milliseconds",
              },
              target_host: {
                type: "string",
                description:
                  "host_id of the only host allowed to execute this task. Skips the ranked claim/election: the named host takes the task on sight and every other host ignores the entry. There is no fallback if that host is offline, unknown or lacks the runtime",
              },
            },
            required: ["task-id", "action"],
            additionalProperties: true,
          },
          proof: {
            type: "object",
            description: "Cryptographic signature proof",
            additionalProperties: true,
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
    additionalProperties: true,
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
    "Per-task pump doc. resultText holds a one-shot task's final output text. resultArray holds a continuous task's structured output objects.",
});

const EXTERNAL_PUMP_RESULT_ARRAY_SCHEMA = JSON.stringify({
  type: "array",
  description:
    "Records a continuous task appends to its default output pump, watched at $.resultArray[*]",
});

const EXTERNAL_PUMP_RESULT_TEXT_SCHEMA = JSON.stringify({
  type: "string",
  description:
    "The final output text a non-continuous task writes to its default output pump, watched at $.resultText",
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


// async function writeFieldToSynapseSubdoc(voltClient: VoltClient, field: string, value: object, documentId: string, pathPrefix: string) {
//   // Write each property individually, since WriteSynapsePath expects leaf values
//   const entries = Object.entries(value);

//   for (const [key, val] of entries) {
//     const jsonPath = `$.${pathPrefix}.${field}.${key}`;

//     try {
//       console.log("WriteSynapsePath args:", {
//         database_id: SYNAPSE_ID,
//         document_id: documentId,
//         path: jsonPath,
//         json: JSON.stringify(val),
//       });

//       const resp = await voltClient.WriteSynapsePath({
//         database_id: SYNAPSE_ID,
//         document_id: documentId,
//         path: jsonPath,
//         json: JSON.stringify(val),
//       });
//       if (resp.status?.code) {
//         throw new Error(`WriteSynapsePath failed for path ${jsonPath}: ${resp.status.message}`);
//       }
//     } catch (e) {
//       console.error(`Error writing to Synapse subdoc ${documentId} path ${jsonPath} with content ${JSON.stringify(val)}:`, e);
//       throw e;
//     }
//   }
// }


async function writeFieldToSynapseSubdoc(voltClient: VoltClient, field: string, value: object, documentId: string, pathPrefix: string) {
  const jsonPath = `$.${pathPrefix}.${field}`;

  try {
    console.log("WriteSynapsePath args:", {
      synapse_id: SYNAPSE_ID,
      document_id: documentId,
      path: jsonPath,
      json: JSON.stringify(value),
    });

    const resp = await voltClient.WriteSynapsePath({
      synapse_id: SYNAPSE_ID,
      database_id: SYNAPSE_ID,
      document_id: documentId,
      path: jsonPath,
      json: JSON.stringify(value),
    });
    if (resp.status?.code) {
      throw new Error(`WriteSynapsePath failed for path ${jsonPath}: ${resp.status.message}`);
    }
    return resp;
  } catch (e) {
    console.error(`Error writing to Synapse subdoc ${documentId} path ${jsonPath} with content ${JSON.stringify(value)}:`, e);
    throw e;
  }
}


// === SETUP FUNCTION ===

export {
  AGENT_LIST_SCHEMA,
  CARRIER_INSTANCE_SCHEMA,
  CARRIERS_SCHEMA,
  EXTERNAL_PUMP_RESULT_ARRAY_SCHEMA,
  EXTERNAL_PUMP_RESULT_TEXT_SCHEMA,
  EXTERNAL_PUMP_TASK_SCHEMA,
  EXTERNAL_PUMPS_SCHEMA,
  TASK_LIST_ROOT_SCHEMA,
  TASK_LIST_SCHEMA,
  TASK_OUTPUT_TASK_SCHEMA,
  TASK_OUTPUTS_SCHEMA,
  SYNAPSE_ID,
  writeFieldToSynapseSubdoc
};
