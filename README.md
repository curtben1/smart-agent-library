# Smart Agent Functions

Utilities for managing distributed agent tasks using Yjs and Volt.

## Installation

### From GitHub Release

```bash
npm install https://github.com/curtben1/smart-agent-library/releases/download/<version>/nqminds-smart-agent-functions-<version>.tgz

```
get this link by copying the download link for the desired release

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
import { installTask, startTask, waitForTaskAssigned, TargetHostUnavailableError } from "@nqminds/smart-agent-functions";

installTask({
    taskID,
    taskName: "my-agent",
    taskLocation: "https://example.com/task.zip",
    sourceType: "https",
    taskList: nodeTaskList,
    target_host: "host-1a2b3c"
});

// Follow-up actions omit target_host: the install recorded ownership against the task-id, which
// routes every later action for that task to the same host.
startTask({ taskID, taskList: nodeTaskList, cli_args: "--once" });
```

Publishing is set and forget: nothing is read back and nothing is checked, so there is no need to
await these calls unless you opt into validation below.

`target_host` is accepted by `installTask`, `install_and_launch`, `startTask` (object form),
`uninstallTask`, `getTaskStatus` and `getTaskMetadata`. Repeating it on a follow-up action is
accepted and only necessary when the install itself was never driven by that host.

### Validating the target (opt in)

A host cannot report a bad target back, so a targeted task aimed at the wrong host sits in the list
forever. Setting `validate_target_host` with a `rootDoc` checks the target against the `agentList`
before anything is published and throws `TargetHostUnavailableError` instead of queuing a task no
host would run. It is off by default, and turning it on is what makes a publish call worth awaiting:

```ts
try {
    await installTask({ ...installDetails, target_host: "host-1a2b3c", validate_target_host: true, rootDoc });
} catch (error) {
    if (error instanceof TargetHostUnavailableError) console.error(error.validation.status);
}
```

The checks are that the `host_id` exists in the `agentList`, that its `lastSeen` heartbeat is within
`TARGET_HOST_STALENESS_THRESHOLD_MS` (90s, the hosts' own staleness threshold), and that it reports
the runtime of the task list being published to. `validate_target_host` without a `rootDoc` throws,
since the `agentList` cannot be read without it.

On `uninstallTask`, `getTaskStatus` and `getTaskMetadata` these are grouped into one trailing
`TargetHostOptions` argument, e.g.
`uninstallTask(taskID, nodeTaskList, undefined, { target_host, validate_target_host: true, rootDoc })`.

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

The first row is what the opt-in validation exists to catch. Since the default publish path checks
nothing, `waitForTaskAssigned` or `validateTargetHost` are how you tell these apart after the fact.

## Reading a one-shot result (`resultText`)

A non-continuous task's final result lands on the **named** `resultText` text root of its default
output pump document, `external-pump-<task-id>` — not the document's unnamed root, which the synapse
cannot address at all, and where hosts used to write it.

`getTaskOutputJson` reads the named root, falling back to the unnamed one so a pump written by an
older host still reads back:

```ts
const result = await getTaskOutputJson(rootDoc, taskID);
```

Because the root is named, the pump can also be watched over the synapse, which was impossible
before:

```jsonc
{ "document_id": "external-pump-<task-id>", "path": ["$.resultText"] }
```

The watch is changes-only — attach it before the task finishes, or read the backlog from a synced
replica. A continuous task is unchanged: its records go to `resultArray`, watched at
`$.resultArray[*]`.

`taskOutputs.<task-id>` streamed stdout is still the unnamed `Y.Text`, so it remains readable only
through a CRDT replica and cannot be watched by path.

### Naming the root (`output_pump_root`)

`output_pump_root` renames that text root. `startTask` and `install_and_launch` accept it and merge it
into the `credentialSubject` before signing:

```ts
await startTask({ taskID, taskList: nodeTaskList, output_pump_root: "probeResult" });

const result = await getTaskOutputJson(rootDoc, taskID, "probeResult");
```

The name must match `[A-Za-z][A-Za-z0-9_-]*` and must not be `resultArray` or `GENERIC_MAP_NAME`,
which the pump document already holds as other kinds of root. A host refuses anything else *silently*,
in favour of `resultText`; publishing an unusable name logs a warning here so it does not surface as
an empty read later.

It is only worth setting when a specific consumer expects a specific path — the pump document is per
task, so `resultText` never collides with anything, and every in-repo reader looks for it. A consumer
of a renamed root has to be told the name, exactly as it would for a `synapse_write_path`.
