import { v4 as uuidv4 } from "uuid";
import type {
    Agent,
    AuthoredAgent,
    AuthoredFlowExport,
    ExecutionMode,
    SemanticFlowExport
} from "../types/flow.types.js";
import { flowExportSchemaProblems } from "./flowSchema.js";

const VERSION_2_EXECUTION_MODE: ExecutionMode = "continuous";

export class FlowValidationError extends Error {
    problems: string[];

    constructor(problems: string[]) {
        super(`the flow does not match the flow export schema:\n${problems.map(problem => `  ${problem}`).join("\n")}`);
        this.name = "FlowValidationError";
        this.problems = problems;
    }
}

/**
 * Checks a value against the flow export schema of the semanticVersion it names, and returns it as a
 * flow ready to deploy. Every agent with a blank id gets a new task id, and a version 2 agent with no
 * execution mode becomes continuous. Throws {@link FlowValidationError} with every schema problem.
 */
export function validateSemanticFlow(flow: unknown): SemanticFlowExport {
    const schemaProblems = flowExportSchemaProblems(flow);
    if (schemaProblems.length > 0) throw new FlowValidationError(schemaProblems);

    const authoredFlow = flow as AuthoredFlowExport;
    return { ...authoredFlow, agents: authoredFlow.agents.map(resolveAgent) };
}

function resolveAgent(authoredAgent: AuthoredAgent): Agent {
    const authoredTaskId = authoredAgent.id?.trim() ?? "";
    return {
        ...authoredAgent,
        id: authoredTaskId === "" ? uuidv4() : authoredTaskId,
        executionMode: authoredAgent.executionMode ?? VERSION_2_EXECUTION_MODE
    };
}
