import { taskCredentialPublishers } from "../flow/taskCredentialPublishers.js";
import { watchForTaskListEntry, type ObservedTaskListEntry, type PublishedTaskAction } from "../flow/taskListEntryWatching.js";
import type { AgentRuntime, FlowTaskList } from "../types/flow.types.js";
import {
    findConfigurationOfVolt,
    readAdvertisedClientUrlsFromAnyVolt,
    readVoltCandidate,
    type VoltCandidate
} from "./activeVoltSelection.js";
import { readActiveVolt, readTaskLedgerRecords, type ActiveVolt } from "./controlPlaneGateway.js";
import { waitWithDeadline, type WaitReporter } from "./deadline.js";
import { readTaskLedgerProgress, type TaskClaim, type TaskLedgerProgress } from "./taskLedgerProgress.js";

export type DeliveryMode = "claimed" | "finished";

export type TaskDeliveryState = "claimed" | "completed" | "closed" | "failed" | "running" | "pending" | "missing";

export interface PublishedTaskEntry extends ObservedTaskListEntry {
    taskId: string;
    action: PublishedTaskAction["action"];
    runtime: AgentRuntime;
}

export interface DeliveryConnection {
    taskListFor(runtime: AgentRuntime): FlowTaskList;
    disconnect(): void;
}

export interface TaskToConfirm {
    taskId: string;
    mode: DeliveryMode;
}

export interface TaskDeliveryRequest<Connection extends DeliveryConnection> {
    tasks: TaskToConfirm[];
    publishedEntries: PublishedTaskEntry[];
    connection: Connection;
    connectedConfigurationPath: string;
    mainConfigurationPath: string;
    peerConfigurationPath?: string | undefined;
    synapseId: string;
    reconnect(configurationPath: string): Promise<Connection>;
    deadlineSeconds: number;
    pollIntervalSeconds?: number | undefined;
    onEvent?: ((event: TaskDeliveryEvent) => void) | undefined;
    reportWait?: WaitReporter | undefined;
    readControlPlaneClientUrls?: (() => Promise<string[]>) | undefined;
}

export interface TaskDeliveryOutcome {
    taskId: string;
    mode: DeliveryMode;
    state: TaskDeliveryState;
    claim: TaskClaim | null;
    republishCount: number;
}

export interface TaskDeliveryResult<Connection extends DeliveryConnection> {
    outcomes: TaskDeliveryOutcome[];
    connection: Connection;
}

export type TaskDeliveryEvent =
    | { kind: "voltChanged"; voltDid: string; epoch: number }
    | { kind: "entryRepublished"; taskId: string; action: string; entryKey: string }
    | { kind: "taskSettled"; outcome: TaskDeliveryOutcome };

interface TrackedTask {
    taskId: string;
    mode: DeliveryMode;
    entries: PublishedTaskEntry[];
    progress: TaskLedgerProgress | null;
    republishCount: number;
    outcome: TaskDeliveryOutcome | null;
}

interface DeliveryState<Connection extends DeliveryConnection> {
    request: TaskDeliveryRequest<Connection>;
    candidates: VoltCandidate[];
    connection: Connection;
    connectedVoltDid: string;
    knownEpoch: number | null;
    clientUrls: string[];
    mustCheckEntries: boolean;
    trackedTasks: TrackedTask[];
}

const DEFAULT_POLL_INTERVAL_SECONDS = 2;
const REPUBLISH_DEADLINE_SECONDS = 20;

/**
 * Follows each published task until it meets the stop condition of its mode, or until the deadline. A
 * `claimed` task stops at a grant issued in the current epoch, and a `finished` task stops at an end
 * state in the ledger. When the control plane names another active volt, it reconnects through
 * `reconnect`, which must also initialise the library's volt client for that volt, and writes again
 * every published entry that the new volt lacks. Resolves with one outcome per task and the connection
 * that is current at the end. Every task the deadline stops is `running`, `pending` or `missing`.
 */
export async function confirmTaskDelivery<Connection extends DeliveryConnection>(
    request: TaskDeliveryRequest<Connection>
): Promise<TaskDeliveryResult<Connection>> {
    const state = createDeliveryState(request);
    const deadline = Date.now() + request.deadlineSeconds * 1000;

    while (true) {
        await followTheActiveVolt(state);
        await readProgressOfUnsettledTasks(state);
        if (state.mustCheckEntries) await republishMissingEntries(state);
        settleTasksThatMetTheirStopCondition(state);

        if (unsettledTasks(state).length === 0 || Date.now() >= deadline) break;
        await waitForNextPoll(request.pollIntervalSeconds ?? DEFAULT_POLL_INTERVAL_SECONDS);
    }

    for (const trackedTask of unsettledTasks(state)) settleAtDeadline(state, trackedTask);
    return { outcomes: state.trackedTasks.map(trackedTask => trackedTask.outcome!), connection: state.connection };
}

function createDeliveryState<Connection extends DeliveryConnection>(
    request: TaskDeliveryRequest<Connection>
): DeliveryState<Connection> {
    const configurationPaths = [request.mainConfigurationPath, request.peerConfigurationPath]
        .filter((configurationPath): configurationPath is string => configurationPath !== undefined);
    return {
        request,
        candidates: configurationPaths.map(readVoltCandidate),
        connection: request.connection,
        connectedVoltDid: readVoltCandidate(request.connectedConfigurationPath).voltDid,
        knownEpoch: null,
        clientUrls: [],
        mustCheckEntries: false,
        trackedTasks: request.tasks.map(task => ({
            taskId: task.taskId,
            mode: task.mode,
            entries: request.publishedEntries.filter(entry => entry.taskId === task.taskId),
            progress: null,
            republishCount: 0,
            outcome: null
        }))
    };
}

function unsettledTasks<Connection extends DeliveryConnection>(state: DeliveryState<Connection>): TrackedTask[] {
    return state.trackedTasks.filter(trackedTask => trackedTask.outcome === null);
}

async function readClientUrls<Connection extends DeliveryConnection>(state: DeliveryState<Connection>): Promise<string[]> {
    if (state.clientUrls.length === 0) {
        const readClientUrlsOfMembers = state.request.readControlPlaneClientUrls
            ?? (() => readAdvertisedClientUrlsFromAnyVolt(state.candidates, state.request.synapseId, state.request.reportWait));
        state.clientUrls = await readClientUrlsOfMembers().catch(() => []);
    }
    return state.clientUrls;
}

async function readActiveVoltOrNull<Connection extends DeliveryConnection>(
    state: DeliveryState<Connection>
): Promise<ActiveVolt | null> {
    const clientUrls = await readClientUrls(state);
    if (clientUrls.length === 0) return null;
    return readActiveVolt(clientUrls).catch(() => {
        state.clientUrls = [];
        return null;
    });
}

async function followTheActiveVolt<Connection extends DeliveryConnection>(state: DeliveryState<Connection>): Promise<void> {
    const activeVolt = await readActiveVoltOrNull(state);
    if (activeVolt === null) return;

    const hasVoltChanged = activeVolt.voltDid !== state.connectedVoltDid;
    const hasEpochChanged = state.knownEpoch !== null && state.knownEpoch !== activeVolt.epoch;
    if (hasVoltChanged) {
        state.connection.disconnect();
        state.connection = await state.request.reconnect(findConfigurationOfVolt(state.candidates, activeVolt.voltDid));
        state.connectedVoltDid = activeVolt.voltDid;
    }
    if (hasVoltChanged || hasEpochChanged) {
        state.mustCheckEntries = true;
        state.request.onEvent?.({ kind: "voltChanged", voltDid: activeVolt.voltDid, epoch: activeVolt.epoch });
    }
    state.knownEpoch = activeVolt.epoch;
}

async function readProgressOfUnsettledTasks<Connection extends DeliveryConnection>(
    state: DeliveryState<Connection>
): Promise<void> {
    const clientUrls = await readClientUrls(state);
    if (clientUrls.length === 0) return;

    for (const trackedTask of unsettledTasks(state)) {
        const records = await readTaskLedgerRecords(clientUrls, trackedTask.taskId).catch(() => null);
        if (records === null) continue;
        trackedTask.progress = readTaskLedgerProgress(records, trackedTask.taskId, state.knownEpoch ?? 0);
    }
}

function isEntryOnTaskList<Connection extends DeliveryConnection>(
    state: DeliveryState<Connection>,
    publishedEntry: PublishedTaskEntry
): boolean {
    try {
        return state.connection.taskListFor(publishedEntry.runtime).entries.has(publishedEntry.entryKey);
    } catch {
        return false;
    }
}

function needsItsEntriesChecked(trackedTask: TrackedTask): boolean {
    return !trackedTask.progress?.isClosed && !trackedTask.progress?.endState;
}

async function republishMissingEntries<Connection extends DeliveryConnection>(state: DeliveryState<Connection>): Promise<void> {
    let everyEntryIsPresent = true;

    for (const trackedTask of unsettledTasks(state).filter(needsItsEntriesChecked)) {
        for (const publishedEntry of trackedTask.entries) {
            if (isEntryOnTaskList(state, publishedEntry)) continue;
            const isNowPresent = await republishAndAwaitEntry(state, publishedEntry);
            trackedTask.republishCount += 1;
            state.request.onEvent?.({
                kind: "entryRepublished",
                taskId: trackedTask.taskId,
                action: publishedEntry.action,
                entryKey: publishedEntry.entryKey
            });
            everyEntryIsPresent &&= isNowPresent;
        }
    }
    state.mustCheckEntries = !everyEntryIsPresent;
}

async function republishAndAwaitEntry<Connection extends DeliveryConnection>(
    state: DeliveryState<Connection>,
    publishedEntry: PublishedTaskEntry
): Promise<boolean> {
    try {
        const taskList = state.connection.taskListFor(publishedEntry.runtime);
        const entryHasAppeared = watchForTaskListEntry(taskList.entries, {
            taskId: publishedEntry.taskId,
            action: publishedEntry.action
        });
        await taskCredentialPublishers.republishTaskListEntry(taskList.document, publishedEntry.entryKey, publishedEntry.entry);
        await waitWithDeadline(
            entryHasAppeared,
            REPUBLISH_DEADLINE_SECONDS,
            `the ${publishedEntry.action} entry of task ${publishedEntry.taskId} to reach the task list again`,
            state.request.reportWait
        );
        return true;
    } catch {
        return false;
    }
}

function settleTask<Connection extends DeliveryConnection>(
    state: DeliveryState<Connection>,
    trackedTask: TrackedTask,
    taskState: TaskDeliveryState,
    claim: TaskClaim | null
): void {
    trackedTask.outcome = {
        taskId: trackedTask.taskId,
        mode: trackedTask.mode,
        state: taskState,
        claim,
        republishCount: trackedTask.republishCount
    };
    state.request.onEvent?.({ kind: "taskSettled", outcome: trackedTask.outcome });
}

function settleTasksThatMetTheirStopCondition<Connection extends DeliveryConnection>(state: DeliveryState<Connection>): void {
    for (const trackedTask of unsettledTasks(state)) {
        const progress = trackedTask.progress;
        if (!progress) continue;
        if (progress.endState) {
            settleTask(state, trackedTask, progress.endState, progress.latestClaim);
        } else if (trackedTask.mode === "claimed" && progress.claimInEpoch) {
            settleTask(state, trackedTask, "claimed", progress.claimInEpoch);
        }
    }
}

function settleAtDeadline<Connection extends DeliveryConnection>(state: DeliveryState<Connection>, trackedTask: TrackedTask): void {
    const claimInEpoch = trackedTask.progress?.claimInEpoch ?? null;
    if (claimInEpoch) {
        settleTask(state, trackedTask, "running", claimInEpoch);
        return;
    }
    const everyEntryIsPresent = trackedTask.entries.every(publishedEntry => isEntryOnTaskList(state, publishedEntry));
    settleTask(state, trackedTask, everyEntryIsPresent ? "pending" : "missing", null);
}

function waitForNextPoll(pollIntervalSeconds: number): Promise<void> {
    return new Promise(resolve => setTimeout(resolve, pollIntervalSeconds * 1000));
}
