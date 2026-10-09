import type * as Y from "yjs";
import type { TaskListEntry } from "../types/flow.types.js";

export interface PublishedTaskAction {
    taskId: string;
    action: "new-task" | "run-task" | "stop-task" | "uninstall-task";
}

/**
 * Resolves once a write reaches the task list for an entry that records the given action on the given
 * task. Entries for any other task or action, including ones other publishers write at the same
 * moment, and entries already on the list, leave it waiting.
 */
export function watchForTaskListEntry(
    taskListEntries: Y.Map<TaskListEntry>,
    publishedAction: PublishedTaskAction
): Promise<void> {
    return new Promise(resolve => {
        const resolveOnceThePublishedEntryIsWritten = (event: Y.YMapEvent<TaskListEntry>) => {
            const writtenEntries = Array.from(event.keysChanged, entryKey => taskListEntries.get(entryKey));
            if (!writtenEntries.some(entry => entryRecordsAction(entry, publishedAction))) return;
            taskListEntries.unobserve(resolveOnceThePublishedEntryIsWritten);
            resolve();
        };
        taskListEntries.observe(resolveOnceThePublishedEntryIsWritten);
    });
}

function entryRecordsAction(entry: TaskListEntry | undefined, publishedAction: PublishedTaskAction): boolean {
    const credentialSubject = entry?.credential?.credentialSubject;
    return credentialSubject?.["task-id"] === publishedAction.taskId
        && credentialSubject.action === publishedAction.action;
}
