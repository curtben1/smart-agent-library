import { Ajv, type ErrorObject, type ValidateFunction } from "ajv";
import type { AuthoredFlowExport, SemanticVersion } from "../types/flow.types.js";

const ADDRESSABLE_ROOT_NAME_PATTERN = "^[A-Za-z][A-Za-z0-9_-]*$";

const SCHEMA_VALUE_SCHEMA = {
    type: "object",
    required: ["state"],
    oneOf: [
        { properties: { state: { const: "specified" }, schema: { type: "object" } }, required: ["state", "schema"] },
        { properties: { state: { const: "unspecified" } } },
        { properties: { state: { const: "unreadable" }, problem: { type: "string" } }, required: ["state", "problem"] }
    ]
};

const TRANSLATION_SCHEMA_SCHEMA = {
    type: "array",
    items: {
        type: "object",
        required: ["producerKey", "consumerKey"],
        properties: { producerKey: { type: "string" }, consumerKey: { type: "string" } }
    }
};

const FLOW_DOCUMENT_SCHEMA = {
    type: "object",
    required: [
        "id",
        "synapseId",
        "rootType",
        "rootTypeName",
        "watchPath",
        "requiresInitialisation",
        "producedExternally",
        "contentSchema"
    ],
    properties: {
        id: { type: "string", minLength: 1 },
        synapseId: { type: "string", minLength: 1 },
        rootType: { enum: ["Y.Map", "Y.Array", "Y.Text"] },
        rootTypeName: { type: "string" },
        watchPath: { type: "string" },
        requiresInitialisation: { type: "boolean" },
        producedExternally: { type: "boolean" },
        contentSchema: SCHEMA_VALUE_SCHEMA
    }
};

const INPUT_SOURCE_SCHEMA = {
    type: "object",
    required: ["synapseId", "documentId", "path"],
    properties: {
        synapseId: { type: "string", minLength: 1 },
        documentId: { type: "string", minLength: 1 },
        path: { type: "array", minItems: 1, items: { type: "string" } }
    }
};

const ADDITIONAL_PARSE_OPTIONS_SCHEMA = {
    type: "object",
    required: ["keyJsonata"],
    properties: {
        keyJsonata: { type: "string", minLength: 1 },
        bodyJsonata: { type: "string", minLength: 1 },
        newSchema: { type: "object" }
    },
    dependencies: { bodyJsonata: ["newSchema"], newSchema: ["bodyJsonata"] }
};

const OUTPUT_LOCATION_SCHEMA = {
    type: "object",
    required: ["synapseId", "documentId"],
    properties: {
        synapseId: { type: "string", minLength: 1 },
        documentId: { type: "string", minLength: 1 },
        writeKey: { type: "string" },
        path: { type: "string" },
        additionalParseOptions: ADDITIONAL_PARSE_OPTIONS_SCHEMA,
        outputPumpRoot: { type: "string", pattern: ADDRESSABLE_ROOT_NAME_PATTERN }
    },
    dependencies: {
        additionalParseOptions: { required: ["path"], not: { required: ["writeKey"] } }
    }
};

const AGENT_FIELDS_EVERY_VERSION_DECLARES = [
    "name",
    "runtime",
    "marketplaceLink",
    "inputSource",
    "outputLocation",
    "inputTranslationSchema",
    "outputTranslationSchema"
];

const AGENT_SCHEMA = {
    type: "object",
    required: AGENT_FIELDS_EVERY_VERSION_DECLARES,
    properties: {
        id: { type: "string" },
        name: { type: "string", minLength: 1 },
        runtime: { enum: ["node", "python"] },
        executionMode: { enum: ["continuous", "oneShot"] },
        marketplaceLink: { type: "string", minLength: 1 },
        inputSource: {
            oneOf: [
                INPUT_SOURCE_SCHEMA,
                { type: "array", minItems: 1, items: INPUT_SOURCE_SCHEMA },
                { type: "null" }
            ]
        },
        outputLocation: { oneOf: [OUTPUT_LOCATION_SCHEMA, { type: "null" }] },
        inputTranslationSchema: TRANSLATION_SCHEMA_SCHEMA,
        outputTranslationSchema: TRANSLATION_SCHEMA_SCHEMA
    }
};

const VERSION_3_AGENT_SCHEMA = {
    ...AGENT_SCHEMA,
    required: [...AGENT_FIELDS_EVERY_VERSION_DECLARES, "executionMode"]
};

function flowExportSchema(semanticVersion: SemanticVersion, agentSchema: object): object {
    return {
        type: "object",
        required: ["semanticVersion", "synapseId", "documents", "agents"],
        properties: {
            semanticVersion: { const: semanticVersion },
            synapseId: { type: "string", minLength: 1 },
            documents: { type: "array", items: FLOW_DOCUMENT_SCHEMA },
            agents: { type: "array", minItems: 1, items: agentSchema }
        }
    };
}

export const FLOW_EXPORT_SCHEMAS_BY_VERSION: Record<SemanticVersion, object> = {
    2: flowExportSchema(2, AGENT_SCHEMA),
    3: flowExportSchema(3, VERSION_3_AGENT_SCHEMA)
};

const schemaCompiler = new Ajv({ strict: false, allErrors: true });

const validateAgainstVersion = new Map<number, ValidateFunction<AuthoredFlowExport>>(
    Object.entries(FLOW_EXPORT_SCHEMAS_BY_VERSION)
        .map(([semanticVersion, schema]) => [Number(semanticVersion), schemaCompiler.compile<AuthoredFlowExport>(schema)])
);

const SUPPORTED_SEMANTIC_VERSIONS = Array.from(validateAgainstVersion.keys());

/**
 * Returns one readable line for each way the given value fails the flow export schema of the
 * semanticVersion it names. Returns an empty list when the value is a valid flow export.
 */
export function flowExportSchemaProblems(candidate: unknown): string[] {
    const namedVersion = (candidate as { semanticVersion?: unknown } | null)?.semanticVersion;
    const validateAgainstSchema = typeof namedVersion === "number"
        ? validateAgainstVersion.get(namedVersion)
        : undefined;

    if (!validateAgainstSchema) {
        return [
            `the flow must name a semanticVersion of ${SUPPORTED_SEMANTIC_VERSIONS.join(" or ")}, `
            + `got ${JSON.stringify(namedVersion) ?? "nothing"}`
        ];
    }
    if (validateAgainstSchema(candidate)) return [];
    return describeSchemaErrors(validateAgainstSchema.errors);
}

function describeSchemaErrors(schemaErrors: ErrorObject[] | null | undefined): string[] {
    if (!schemaErrors || schemaErrors.length === 0) return ["the flow is invalid, and no reason was reported"];

    return schemaErrors.map(schemaError => `${schemaError.instancePath || "the flow"} ${schemaError.message ?? "is invalid"}`);
}
