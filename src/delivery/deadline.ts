export type WaitReporter = <Value>(description: string, pendingWork: Promise<Value>, deadlineSeconds: number) => Promise<Value>;

const reportNothing: WaitReporter = (_description, pendingWork) => pendingWork;

/**
 * Resolves with the given promise, or rejects naming what was waited for once the deadline passes. The
 * work itself is not cancelled. A reporter, when given, sees the wait while it lasts.
 */
export async function waitWithDeadline<Value>(
    pendingWork: Promise<Value>,
    deadlineSeconds: number,
    whatIsBeingWaitedFor: string,
    reportWait: WaitReporter = reportNothing
): Promise<Value> {
    let deadlineTimer: ReturnType<typeof setTimeout> | undefined;
    const deadlineRejection = new Promise<never>((_resolve, reject) => {
        deadlineTimer = setTimeout(
            () => reject(new Error(`timed out after ${deadlineSeconds}s waiting for ${whatIsBeingWaitedFor}`)),
            deadlineSeconds * 1000
        );
    });

    try {
        return await reportWait(
            `waiting for ${whatIsBeingWaitedFor}`,
            Promise.race([pendingWork, deadlineRejection]),
            deadlineSeconds
        );
    } finally {
        clearTimeout(deadlineTimer);
    }
}
