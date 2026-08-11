# Smart Agent Functions

Utilities for managing distributed agent tasks using Yjs and Volt.

## Installation

### From GitHub Release

```bash
npm install https://github.com/your-org/smart-agent-lib/releases/download/v1.0.0/your-org-smart-agent-functions-1.0.0.tgz

```

## Host targeting (`target_host`)

By default every live host that supports a task's runtime competes for it through a ranked
claim/election. Setting `target_host` on a task pins it to one named host: that host takes it on
first sight with no ranking or claim round trip, and every other host ignores the entry.

The value is a **`host_id`** — the key under which the host appears in the `agentList`
sub-document, the same string reported as a task's owner. It is not a hostname, DID or display name.
Hosts must be running with `USE_SCHEDULING_DOC=1`; targeting is not supported on the legacy
task-entry ownership path.

The field is written into the `credentialSubject` before signing, so it is inside the signed payload.

### Publishing a targeted task

```ts
import { installTask, startTask, waitForTaskAssigned } from "@nqminds/smart-agent-functions";

await installTask({
    taskID,
    taskName: "my-agent",
    taskLocation: "https://example.com/task.zip",
    sourceType: "https",
    taskList: nodeTaskList,
    target_host: "host-1a2b3c",
    rootDoc
});

// Follow-up actions omit target_host: the install recorded ownership against the task-id, which
// routes every later action for that task to the same host.
await startTask({ taskID, taskList: nodeTaskList, cli_args: "--once" });
```

`target_host` is accepted by `installTask`, `install_and_launch`, `startTask` (object form),
`uninstallTask`, `getTaskStatus` and `getTaskMetadata`. Repeating it on a follow-up action is
accepted and only necessary when the install itself was never driven by that host.

### Validating the target

A host cannot report a bad target back, so a targeted task aimed at the wrong host sits in the list
forever. Passing `rootDoc` alongside `target_host` validates the target against the `agentList`
before anything is published, and throws `TargetHostUnavailableError` instead of queuing a task no
host would run. Without `rootDoc` the task is published unvalidated and a warning is logged.

The checks are that the `host_id` exists in the `agentList`, that its `lastSeen` heartbeat is within
`TARGET_HOST_STALENESS_THRESHOLD_MS` (90s, the hosts' own staleness threshold), and that it reports
the runtime of the task list being published to.

To check a target without publishing, use `validateTargetHost`, which returns the reason rather than
throwing:

```ts
const validation = await validateTargetHost("host-1a2b3c", nodeTaskList, rootDoc);
if (validation.status !== "usable") {
    // "agent-list-unavailable" | "unknown-host" | "stale-host" | "runtime-unsupported"
    console.error(validation);
}
```

`assertTargetHostUsable` is the throwing equivalent.

### Confirming a targeted task started

With `USE_SCHEDULING_DOC=1` the task entry's `assigned` field stays `null` for every task, targeted
or not; ownership lives at `<taskId>:assigned` in the runtime scheduling sub-document. Pass
`rootDoc` to `waitForTaskAssigned` so it reads that overlay:

```ts
const outcome = await waitForTaskAssigned(taskID, nodeTaskList, 5, 1000, rootDoc);
if (outcome.status === "assigned") console.log(`running on ${outcome.assignment}`);
```

### Failure modes

Capacity is still enforced and there is no fallback host, which shapes what a stuck targeted task
means:

| Symptom                                          | Cause                                                                                    |
| ------------------------------------------------ | ---------------------------------------------------------------------------------------- |
| Task sits with no owner indefinitely             | Target offline, unknown or wrong runtime — no other host will take it                    |
| Task sits, target is healthy                     | Target is at its concurrency limit (default 1); it starts when a slot frees              |
| Task silently ignored by every host              | `target_host` added after signing, or signed with the wrong key                           |
| Follow-up `run-task` never executes              | Its install was never driven by the target host, so publish the install first or target the follow-up explicitly |

The first three are what the pre-publish validation exists to catch.
