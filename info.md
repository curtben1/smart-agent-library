# Host targeting (`target_host`) — client integration guide

Audience: an agent updating a **client library** that publishes tasks to agent hosts over the
Synapse. This describes the new field, the host behaviour it triggers, and the changes a client
needs. No host-side code changes are required by the client.

## What the feature is

A task credential may now name the host that must execute it. Previously every live host that
supported the task's runtime competed for it through a ranked claim/election; a targeted task skips
that entirely and is executed by the named host only.

## The field

`target_host` goes on the **`credentialSubject`**, alongside `task-id`/`action`, and is therefore
**inside the signed payload** — set it before signing, not on the task-list wrapper.

```jsonc
{
  "task-id": "a1b2c3d4-...",
  "action": "new-task",
  "location": "https://example.com/task.zip",
  "source": "https",
  "target_host": "host-1a2b3c"      // a host_id: the host's key in agentList
}
```

The value is a **`host_id`** — the key under which the host appears in the `agentList` sub-document,
the same string reported as a task's owner. It is not a hostname, DID, or display name.

Publish exactly as before: `taskList.set("<mapKey uuid>", { credential: signedVc, assigned: null })`,
to `pythonTaskList` or `nodeTaskList` per runtime.

## Host behaviour

- The named host takes the task on first sight, with no ranking, staggered wait, or claim/settle
  round trip. Assignment is effectively immediate.
- Every other host ignores the entry outright, for **all** actions including `uninstall-task`.
- **Capacity is still enforced.** A target already running its maximum concurrent tasks (default 1)
  holds the task until a slot frees; it never overflows to another host.
- **There is no fallback.** A target that is offline, unknown to the cluster, or missing the task's
  runtime leaves the task queued indefinitely. The usual takeover of a dead host's work does not
  apply to targeted tasks.
- Hosts must run with `USE_SCHEDULING_DOC=1`. Targeting is not supported on the legacy
  task-entry ownership path.

## Client changes

1. **Types / schemas.** Add `target_host?: string` to the client's `credentialSubject` type. The
   host's JSON Schema accepts it already (`credentialSubject` is `additionalProperties: true`), so no
   schema negotiation is needed — but a client that validates outgoing credentials against its own
   stricter schema must whitelist the field.

2. **Publish API.** Add an optional target-host argument to whatever builds the `credentialSubject`,
   and make sure it is merged in **before** the signing step. A targeted task whose field was added
   post-signing fails signature verification and is silently dropped by the host.

3. **Validate the target before publishing.** The host cannot report a bad target back, so the
   client should check it, reading `agentList`:
   - the `host_id` key exists;
   - `Date.now() - Date.parse(entry.lastSeen) < 90_000` (the hosts' own staleness threshold);
   - the task's runtime appears in `entry.runtimes`, and the task is published to the matching
     runtime task list.

   If any check fails, fail the publish call rather than queuing a task nothing will run.

4. **Follow-up actions: omit the field.** A `run-task`/`task-status`/`uninstall-task` for an
   already-installed targeted task does **not** need `target_host` — the install recorded ownership
   against the `task-id`, and that routes every later action of that task to the same host while
   other hosts skip it. Omitting it is the recommended default. (Repeating it is accepted, and is
   required only if the install itself was never driven by that host.)

5. **Don't wait on the wrapper's `assigned` field.** With `USE_SCHEDULING_DOC=1`, ownership lives in
   the per-runtime scheduling sub-document (`nodeScheduling` / `pythonScheduling`, guids
   `node-scheduling` / `python-scheduling`) under the flat key `<taskId>:assigned`, with the lease at
   `<taskId>:claim:<hostId>`. The wrapper's `assigned` stays `null` in this mode for every task,
   targeted or not. To confirm a targeted task started, watch that scheduling key, the task's wire
   (`@wireid-<taskId>`), or `taskOutputs`.

## Failure modes to surface to callers

| Symptom                                       | Cause                                                                                                                                         |
| --------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------- |
| Task sits in the list, no owner, indefinitely | Target offline / unknown / wrong runtime — no other host will take it                                                                         |
| Task sits, target is healthy                  | Target at its concurrency limit; it will start when a slot frees                                                                              |
| Task silently ignored by all hosts            | `target_host` added after signing, or signed with the wrong key                                                                               |
| Follow-up `run-task` never executes           | Its install was never driven by the target host, so no ownership was recorded — publish the install first, or target the follow-up explicitly |

## Verifying an integration

Publish a targeted `new-task`, then check that `<taskId>:assigned` in the target's runtime scheduling
sub-document becomes the target `host_id` with no intermediate claim churn, and that no other host
logs a claim for the task. Then publish an untargeted `run-task` for the same `task-id` and confirm
the same host executes it.
