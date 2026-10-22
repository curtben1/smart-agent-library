import type * as Y from "yjs";

export const CONTROL_PLANE_DOCUMENT_GUID = "control-plane";
const CONTROL_PLANE_MEMBERS_ROOT = "members";
const ACTIVE_VOLT_KEY = "volt/active";
const HOST_KEY_PREFIX = "hosts/";
export const LEDGER_KEY_PREFIX = "ledger/";
const MEMBER_CALL_DEADLINE_MS = 3_000;

export interface ActiveVolt {
    voltDid: string;
    epoch: number;
}

interface ClusterMember {
    name?: string;
    clientURLs?: string[];
    isLearner?: boolean;
}

export interface ControlPlaneStatus {
    voters: string[];
    learners: string[];
    machines: string[];
    answeringVoterCount: number;
    activeVolt: ActiveVolt | null;
}

export interface StoredLedgerRecord {
    key: string;
    value: Record<string, unknown>;
}

/** The client addresses of every member that the control-plane document advertises. */
export function readAdvertisedClientUrls(controlPlaneDocument: Y.Doc): string[] {
    return Array.from(controlPlaneDocument.getMap<{ clientUrl?: unknown }>(CONTROL_PLANE_MEMBERS_ROOT).values())
        .map(advertisement => advertisement?.clientUrl)
        .filter((clientUrl): clientUrl is string => typeof clientUrl === "string");
}

async function postToMember<Answer>(clientUrl: string, gatewayPath: string, body: object): Promise<Answer> {
    const response = await fetch(`${clientUrl}${gatewayPath}`, {
        method: "POST",
        body: JSON.stringify(body),
        signal: AbortSignal.timeout(MEMBER_CALL_DEADLINE_MS)
    });
    if (!response.ok) throw new Error(`${clientUrl} answered HTTP ${response.status}`);
    return (await response.json()) as Answer;
}

async function askFirstAnsweringMember<Answer>(
    clientUrls: string[],
    ask: (clientUrl: string) => Promise<Answer>
): Promise<Answer> {
    const failures: string[] = [];
    for (const clientUrl of clientUrls) {
        try {
            return await ask(clientUrl);
        } catch (failure) {
            failures.push((failure as Error).message);
        }
    }
    throw new Error(`no control-plane member answered: ${failures.join("; ") || "none is advertised"}`);
}

function encodeKey(key: string): string {
    return Buffer.from(key).toString("base64");
}

function decodeBase64(encoded: string): string {
    return Buffer.from(encoded, "base64").toString("utf8");
}

function buildPrefixRangeEnd(prefix: string): string {
    const lastCharacterCode = prefix.charCodeAt(prefix.length - 1);
    return prefix.slice(0, -1) + String.fromCharCode(lastCharacterCode + 1);
}

async function readActiveVoltFrom(clientUrl: string): Promise<ActiveVolt | null> {
    const range = await postToMember<{ kvs?: { value: string; mod_revision: string }[] }>(
        clientUrl,
        "/v3/kv/range",
        { key: encodeKey(ACTIVE_VOLT_KEY) }
    );
    const keyValue = range.kvs?.[0];
    if (!keyValue) return null;
    const record = JSON.parse(decodeBase64(keyValue.value)) as { voltDid?: unknown };
    if (typeof record.voltDid !== "string") throw new Error(`${ACTIVE_VOLT_KEY} names no volt`);
    return { voltDid: record.voltDid, epoch: Number(keyValue.mod_revision) };
}

/** The volt that the control plane names active, with its epoch, or null on clean state. */
export function readActiveVolt(clientUrls: string[]): Promise<ActiveVolt | null> {
    return askFirstAnsweringMember(clientUrls, readActiveVoltFrom);
}

async function readLiveHostIdsFrom(clientUrl: string): Promise<string[]> {
    const range = await postToMember<{ kvs?: { key: string }[] }>(clientUrl, "/v3/kv/range", {
        key: encodeKey(HOST_KEY_PREFIX),
        range_end: encodeKey(buildPrefixRangeEnd(HOST_KEY_PREFIX)),
        keys_only: true
    });
    return (range.kvs ?? []).map(keyValue => decodeBase64(keyValue.key).slice(HOST_KEY_PREFIX.length));
}

/** The ids of the hosts whose control-plane lease is live, which are the hosts that may still act. */
export function readLiveHostIds(clientUrls: string[]): Promise<string[]> {
    return askFirstAnsweringMember(clientUrls, readLiveHostIdsFrom);
}

async function readTaskLedgerRecordsFrom(clientUrl: string, taskId: string): Promise<StoredLedgerRecord[]> {
    const taskPrefix = `${LEDGER_KEY_PREFIX}${taskId}/`;
    const range = await postToMember<{ kvs?: { key: string; value?: string }[] }>(clientUrl, "/v3/kv/range", {
        key: encodeKey(taskPrefix),
        range_end: encodeKey(buildPrefixRangeEnd(taskPrefix))
    });
    return (range.kvs ?? []).map(keyValue => ({
        key: decodeBase64(keyValue.key),
        value: parseRecordValue(keyValue.value)
    }));
}

function parseRecordValue(encodedValue: string | undefined): Record<string, unknown> {
    if (!encodedValue) return {};
    try {
        const parsedValue: unknown = JSON.parse(decodeBase64(encodedValue));
        return typeof parsedValue === "object" && parsedValue !== null ? (parsedValue as Record<string, unknown>) : {};
    } catch {
        return {};
    }
}

/** Every ticket record of one task, as the keys and values that the control plane holds under `ledger/<taskId>/`. */
export function readTaskLedgerRecords(clientUrls: string[], taskId: string): Promise<StoredLedgerRecord[]> {
    return askFirstAnsweringMember(clientUrls, clientUrl => readTaskLedgerRecordsFrom(clientUrl, taskId));
}

async function isMemberAnswering(clientUrl: string): Promise<boolean> {
    return postToMember(clientUrl, "/v3/maintenance/status", {}).then(
        () => true,
        () => false
    );
}

function extractMachineFromMemberName(memberName: string): string {
    return memberName.replace(/-slot-\d+$/, "");
}

/** The members of the control plane, how many of its voters answer, and its active volt. */
export async function readControlPlaneStatus(advertisedClientUrls: string[]): Promise<ControlPlaneStatus> {
    const { members = [] } = await askFirstAnsweringMember(advertisedClientUrls, clientUrl =>
        postToMember<{ members?: ClusterMember[] }>(clientUrl, "/v3/cluster/member/list", {}));
    const voterMembers = members.filter(member => !member.isLearner);
    const answers = await Promise.all(
        voterMembers.map(member => (member.clientURLs?.[0] ? isMemberAnswering(member.clientURLs[0]) : false))
    );
    const memberNames = members.map(member => member.name || "(not started)");
    return {
        voters: voterMembers.map(member => member.name || "(not started)"),
        learners: members.filter(member => member.isLearner).map(member => member.name || "(not started)"),
        machines: [...new Set(memberNames.map(extractMachineFromMemberName))],
        answeringVoterCount: answers.filter(Boolean).length,
        activeVolt: await readActiveVolt(advertisedClientUrls)
    };
}

export function describeControlPlaneStatus(status: ControlPlaneStatus): string {
    const hasMajority = status.answeringVoterCount * 2 > status.voters.length;
    const activeVolt = status.activeVolt
        ? `active volt ${status.activeVolt.voltDid} epoch ${status.activeVolt.epoch}`
        : "no active volt";
    return (
        `control plane: voters=${status.voters.join(",") || "none"}  learners=${status.learners.join(",") || "none"}  `
        + `machines=${status.machines.join(",") || "none"}  `
        + `majority=${hasMajority ? "present" : "missing"} (${status.answeringVoterCount} of ${status.voters.length})  ${activeVolt}`
    );
}
