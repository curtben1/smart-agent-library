# `target_host` support — change summary (v1.7.0)

Adds host targeting to the client library: a task can name the single host that must execute it,
skipping the ranked claim/election. Requires hosts running `USE_SCHEDULING_DOC=1`.

## API

`target_host` (a `host_id`, as keyed in `agentList`) is accepted by `installTask`,
`install_and_launch`, `startTask` (object form), `uninstallTask`, `getTaskStatus` and
`getTaskMetadata`. It is merged into the `credentialSubject` **before** signing.

Follow-up actions for an already-installed targeted task can omit it — the install records ownership
against the `task-id`, which routes later actions to the same host.

`installTask` gained an object-args overload (`InstallTaskInputArgs`); the positional form still
works. `uninstallTask`, `getTaskStatus` and `getTaskMetadata` take a trailing `TargetHostOptions`
argument.

## Validation is opt in

| Mode | Behaviour |
| --- | --- |
| Default | Set and forget. Credential written with the target, no `agentList` read, nothing to await. A bad target only shows up as a task that never gets an owner. |
| `validate_target_host: true` + `rootDoc` | Checks the target first and throws `TargetHostUnavailableError` instead of publishing. |

Checks performed: `host_id` present in `agentList`; `lastSeen` within
`TARGET_HOST_STALENESS_THRESHOLD_MS` (90s); host reports the runtime of the task list being published
to. The `agentList` sync wait is capped at 5s. Setting the flag without `rootDoc` throws.

Standalone helpers: `validateTargetHost` (returns `TargetHostValidation` — `usable` /
`agent-list-unavailable` / `unknown-host` / `stale-host` / `runtime-unsupported`) and
`assertTargetHostUsable` (throws).

## Other notes

- Publish helpers now return `Promise<void>` where three previously returned `void`; ignoring the
  return is still fine unless you opt into validation.
- `waitForTaskAssigned` documents that a targeted task never falls back to another host — pass
  `rootDoc` so it reads `<taskId>:assigned` from the runtime scheduling sub-document.
- Types: `target_host` added to `SignedTaskCredential["credentialSubject"]` and
  `TaskCredentialSubject`; `target_host` documented in `TASK_LIST_SCHEMA`.
- Full usage docs in [README.md](README.md#host-targeting-target_host).

## Files

`src/smart_agent_functions.ts`, `src/agent_schemas.ts`, `src/types/shared.types.ts`,
`src/types/config.types.ts`, `README.md`, `package.json` (1.6.0 → 1.7.0).
