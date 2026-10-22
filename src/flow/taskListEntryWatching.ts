import type * as Y from "yjs";
import type { TaskListEntry } from "../types/flow.types.js";

export interface PublishedTaskAction {
    taskId: string;
    action: "new-task" | "run-task" | "stop-task" | "uninstall-task";
}

export interface ObservedTaskListEntry {
    entryKey: string;
    entry: TaskListEntry;
}

/**
 * Resolves with the key and the value of the entry, once a write reaches the task list for an entry
 * that records the given action on the given task. Entries for any other task or action, including ones
 * other publishers write at the same moment, and entries already on the list, leave it waiting.
 */
export function watchForTaskListEntry(
    taskListEntries: Y.Map<TaskListEntry>,
    publishedAction: PublishedTaskAction
): Promise<ObservedTaskListEntry> {
    return new Promise(resolve => {
        const resolveOnceThePublishedEntryIsWritten = (event: Y.YMapEvent<TaskListEntry>) => {
            const writtenEntryKey = Array.from(event.keysChanged).find(entryKey =>
                entryRecordsAction(taskListEntries.get(entryKey), publishedAction));
            if (writtenEntryKey === undefined) return;
            taskListEntries.unobserve(resolveOnceThePublishedEntryIsWritten);
            resolve({ entryKey: writtenEntryKey, entry: taskListEntries.get(writtenEntryKey)! });
        };
        taskListEntries.observe(resolveOnceThePublishedEntryIsWritten);
    });
}

function entryRecordsAction(entry: TaskListEntry | undefined, publishedAction: PublishedTaskAction): boolean {
    const credentialSubject = entry?.credential?.credentialSubject;
    return credentialSubject?.["task-id"] === publishedAction.taskId
        && credentialSubject.action === publishedAction.action;
}
