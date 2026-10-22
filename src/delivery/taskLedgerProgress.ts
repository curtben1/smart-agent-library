import { LEDGER_KEY_PREFIX, type StoredLedgerRecord } from "./controlPlaneGateway.js";

export type TaskEndState = "completed" | "closed" | "failed";

export interface TaskClaim {
    ticket: number;
    hostId: string | null;
    epoch: number;
}

export interface TaskLedgerProgress {
    claimInEpoch: TaskClaim | null;
    latestClaim: TaskClaim | null;
    endState: TaskEndState | null;
    isClosed: boolean;
}

type RecordsByKind = Map<string, Record<string, unknown>>;

function parseTicketAndKind(key: string, taskId: string): { ticket: number; kind: string } | null {
    const taskPrefix = `${LEDGER_KEY_PREFIX}${taskId}/`;
    if (!key.startsWith(taskPrefix)) return null;
    const [ticketText, kind, ...rest] = key.slice(taskPrefix.length).split("/");
    const ticket = Number(ticketText);
    if (rest.length > 0 || kind === undefined || !Number.isInteger(ticket)) return null;
    return { ticket, kind };
}

function groupRecordsByTicket(records: StoredLedgerRecord[], taskId: string): Map<number, RecordsByKind> {
    const recordsByTicket = new Map<number, RecordsByKind>();
    for (const record of records) {
        const ticketAndKind = parseTicketAndKind(record.key, taskId);
        if (!ticketAndKind) continue;
        const recordsOfTicket = recordsByTicket.get(ticketAndKind.ticket) ?? new Map();
        recordsOfTicket.set(ticketAndKind.kind, record.value);
        recordsByTicket.set(ticketAndKind.ticket, recordsOfTicket);
    }
    return recordsByTicket;
}

function readClaim(ticket: number, recordsOfTicket: RecordsByKind): TaskClaim | null {
    const grant = recordsOfTicket.get("grant");
    if (!grant) return null;
    return {
        ticket,
        hostId: typeof grant.hostId === "string" ? grant.hostId : null,
        epoch: typeof grant.epoch === "number" ? grant.epoch : 0
    };
}

function hasExhaustedFailures(recordsByTicket: Map<number, RecordsByKind>): boolean {
    const failedRecords = [...recordsByTicket.values()].flatMap(recordsOfTicket => {
        const failed = recordsOfTicket.get("failed");
        return failed ? [failed] : [];
    });
    const failureLimits = failedRecords
        .map(failed => failed.failureLimit)
        .filter((failureLimit): failureLimit is number => typeof failureLimit === "number");
    return failureLimits.length > 0 && failedRecords.length >= Math.max(...failureLimits);
}

function findEndState(recordsByTicket: Map<number, RecordsByKind>): TaskEndState | null {
    const allRecordsOfTicket = [...recordsByTicket.values()];
    if (allRecordsOfTicket.some(recordsOfTicket => recordsOfTicket.has("completed"))) return "completed";
    if (allRecordsOfTicket.some(recordsOfTicket => recordsOfTicket.has("closed"))) return "closed";
    if (hasExhaustedFailures(recordsByTicket)) return "failed";
    return null;
}

/**
 * Reads how far the fleet has taken one task, from the ticket records that the control plane holds for
 * it. A claim counts for the epoch only when its grant was issued in that epoch or a later one.
 */
export function readTaskLedgerProgress(
    records: StoredLedgerRecord[],
    taskId: string,
    currentEpoch: number
): TaskLedgerProgress {
    const recordsByTicket = groupRecordsByTicket(records, taskId);
    const claims = [...recordsByTicket]
        .map(([ticket, recordsOfTicket]) => readClaim(ticket, recordsOfTicket))
        .filter((claim): claim is TaskClaim => claim !== null)
        .sort((first, second) => first.ticket - second.ticket);

    return {
        claimInEpoch: claims.find(claim => claim.epoch >= currentEpoch) ?? null,
        latestClaim: claims[claims.length - 1] ?? null,
        endState: findEndState(recordsByTicket),
        isClosed: [...recordsByTicket.values()].some(recordsOfTicket => recordsOfTicket.has("closed"))
    };
}
