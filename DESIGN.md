# pi-mqtt — design decisions (v0)

Coordinate independent pi sessions over MQTT. One **orchestrator** (your live, interactive
session) pins tasks to named **workers** (other live pi sessions, one run at a time), and
results come back as a quiet digest that your session can act on.

Everything below is a decision, not a proposal.

## Roles

One package, one extension, role chosen at launch:

```
pi --pi-mqtt-role orchestrator   # your session
pi --pi-mqtt-role worker --pi-mqtt-id laptop-b --cwd ~/work/repo-w-b
```

Config: broker credentials from env (`PI_MQTT_URL`, `PI_MQTT_USER`, `PI_MQTT_PASS`,
`PI_MQTT_CLUSTER`), everything else from `pi-mqtt.json` (worker id, repo allowlist,
`allowModelOverride`). Connect in `session_start`, disconnect in `session_shutdown`
(idempotent).

## Transport

- Mosquitto on Railway, **MQTT 5**, public endpoint, TLS (`8883`, or TCP proxy with TLS).
- **One room password**: a single broker user (`cluster`) carries the room's shared
  secret. Rooms are a name plus that password; membership is the password. The
  superseded per-role design (one user per machine, ACLs keyed on `%u`) is described
  at the end of this document.
- Topic prefix carries the room name, so separate rooms never collide.
- **QoS 1** everywhere; dedupe by `taskId` (see failure policy).
- **Message expiry** on tasks (default 15 min) so a laptop that wakes up 40 minutes later
  does not execute stale work.
- **Retained presence + Last Will** so `list_workers` is correct without polling.

### Topics

```
pi/v1/<cluster>/workers/<id>/presence   retained, LWT → offline
pi/v1/<cluster>/workers/<id>/ctl        orchestrator → worker: task | cancel
pi/v1/<cluster>/workers/<id>/ack        worker → orchestrator: started
pi/v1/<cluster>/results/<taskId>        worker → orchestrator: result
```

No shared subscriptions, no pools: routing is explicit pinning (decision C).

## Messages

```ts
// task (orchestrator → worker/<id>/ctl)
{
  kind: "task", taskId, prompt,
  notify: "silent" | "wake",
  repo?: { baseRef, baseSha, branch? },
  timeoutMs?, maxCostUsd?, model?,
  artifacts?: "none" | "paths"
}

// ack (worker → orchestrator) — the one reliable "I actually started" signal
{ taskId, workerId, startedAt }

// result (worker → orchestrator)
{
  taskId, workerId,
  status: "done" | "error" | "timeout" | "cancelled",
  text,                                  // final assistant text of the run
  artifacts: string[],                   // paths, when the task wrote files
  repo?: { baseSha, headSha, branch, diffstat, patchPath },
  usage, durationMs,
  sessionFile, sessionId                 // lets the orchestrator /resume the run
}

// presence (retained, updated on every state change + 30s heartbeat)
{ workerId, host, cwd, repo, branch, headSha, model,
  state: "idle" | "busy", taskId?, startedAt, version }
```

`sessionFile`/`sessionId` are mandatory: the orchestrator can inspect exactly what a worker
did instead of trusting a one-line summary.

## Orchestrator surface

Tools: `dispatch_task`, `list_workers`, `list_tasks`, `get_result`, `cancel_task`.
Commands: `/workers`, `/tasks`, `/mqtt`.

Task records live in the orchestrator's memory for the session's lifetime: results are not retained on the broker, so a restarted orchestrator cannot recover them. Re-dispatch is the recovery path, which is why `list_tasks` is a tool and not just a command.

`dispatch_task` **waits for the result by default** (`waitTimeoutMs`, default 120s) and returns
the full result in the same tool call; `wait: false` is the fan-out mode, where the result
arrives later as a digest. Fails fast when the target is unknown, stale, or offline.
The task is marked before the wait begins, so a result that lands during the wait is returned
inline and *not* re-delivered as a digest. `ifBusy: "fail" | "wait"` — **wait is implemented in the
orchestrator** as an outbox that fires when presence flips to idle. No queue on the worker,
nothing stale in a broker topic, and every waiting task is visible in the transcript.

## Result delivery

- Default **silent**: `pi.sendMessage({triggerTurn: false})` with a compact digest —
  worker, status, task id, 2–3 line takeaway, artifact paths. Nothing auto-starts a run.
- `notify: "wake"` → `pi.sendUserMessage(digest, {deliverAs: "followUp"})`.
- Results landing within a ~15s window coalesce into one digest.
- Full text, patch, and diffstat are **not** in the digest; `get_result(taskId)` fetches them.
  This keeps a 400-line diff out of the orchestrator's context.

## Worker behavior

1. On `task`: dedupe against seen `taskId`s. If `state !== "idle"` → publish
   `status: "error", error: "busy"`. Otherwise set `state: "busy"`, publish `ack.started`,
   then `pi.sendUserMessage(framed prompt)`.
2. Repo prep before the run: `git fetch && git checkout -B worker/<id> <baseSha>`; refuse if
   the tree is dirty. The repo comes from worker config (allowlisted), never from the broker.
3. Capture assistant text via `message_end` for the duration of the task.
4. On `agent_settled`: commit if the tree changed, produce `format-patch` in the scratch dir,
   publish the result, publish `state: "idle"`. `agent_end` is never used as a completion
   signal.
5. On `cancel`: `ctx.abort()`; the settle handler reports `cancelled` with whatever text the run produced. Partial output is never overwritten by the abort's own error message.
6. On timeout: same abort path, status `timeout`, partial text preserved.
7. Task framing is an explicit transcript delimiter (`--- task <id> ---`), because the session
   is long-lived and a human may type into it between tasks.
8. Cost cap uses the per-message `usage.cost.total` reported by the provider; the run is aborted
   when the accumulated spend for a task exceeds `maxCostUsd`.
9. `dispatch_task` carries prompt guidelines telling the model to use it rather than publishing
   protocol messages with `bash` — an orchestrator without the tool will happily hand-roll one.

## Two sessions, one id

A worker id must belong to exactly one live session. Two sessions under the same id both consume
tasks and their results race, so on startup a worker reads the retained presence for its id and
refuses to start when another session is live (`--pi-mqtt-force` takes over deliberately). Results
are also deduplicated by `taskId` on the orchestrator, because QoS 1 may redeliver and because a
second publisher must never produce a second terminal state.

## Connecting is explicit

A configured install does not reach the network on its own. `session_start` reads the config and
stops there; `/mqtt connect` connects, `/mqtt disconnect` disconnects. A worker is the exception in
practice, not in principle: it must already be listening when a task arrives, so it opts in at
launch with `--pi-mqtt-start` or `"connect": "auto"`.

The reason is that an implicit connection has effects the user cannot see: a socket, a retained
presence entry other sessions read, and (for an orchestrator) five tools that look available.
Tying those to an explicit action keeps "installed" from meaning "reachable", and makes a
misconfigured session inert rather than quietly connected to the wrong broker.

## Failure policy

| Situation | Behavior |
|---|---|
| Target busy | `dispatch_task` → `busy`; caller may use `ifBusy: "wait"` |
| Target offline | instant error from retained presence / LWT |
| No `started` ack within ~5s | safe auto-redispatch (provably no side effects) |
| Worker dies mid-task | `lost`; **manual** re-dispatch, never automatic |
| Timeout | worker-enforced abort, `timeout` + partial text |
| Cancel | cooperative abort, `cancelled` |
| QoS 1 duplicate | worker dedupes by `taskId`; orchestrator ignores a second result for a finished task |
| Worker restart | presence flips offline→online; in-flight task is `lost` by absence |

The orchestrator never retries on its own except the no-ack case. Every other recovery is a
decision your session makes — which only works because `/tasks` shows outstanding work
(`queued`, `running`, `lost`, `timeout`).

## Safety

Laptops are trusted, so workers keep full tools with no approval gate. Two habits retained
because they're nearly free: workers launch with a minimal env (no cloud/API secrets in a
session that runs broker-supplied prompts), and the repo URL comes from local config rather
than presence. Broker access is TLS plus the room password, which is a capability rather than an
identity: any member can read every task and result. The payload therefore carries the
two checks that remain — a worker ignores tasks not addressed to it, and the
orchestrator ignores results whose `workerId` differs from the pinned worker. Task
payloads are treated as untrusted data, not as instructions from the operator.

## Layout

```
package.json            # mqtt, zod-free: types only
pi-package.json         # installable pi package
src/protocol.ts         # topics, message types, guards
src/mqtt.ts             # connect, publish, subscribe, TLS, LWT
extensions/mqtt.ts        # entry: role dispatch, session_start/shutdown
src/orchestrator.ts     # tools, outbox, digest/coalescing, state
src/worker.ts           # task execution, repo prep, abort/timeout, results
src/presence.ts         # heartbeat, retained state
broker/                 # local docker-compose + mosquitto.conf + aclfile
```

## Deferred (explicitly not v0)

Container/VM isolation for workers. Artifact transport beyond paths (S3/Syncthing). Budget
enforcement beyond a best-effort token check at `turn_end`. Multi-cluster, worker tags/pools,
auto-recovery beyond the no-ack case. Shared filesystem anything.

## Build order

1. `protocol.ts` + local mosquitto (docker compose, ACLs) — prove topics by hand with `mosquitto_pub`.
2. Extension connects, presence heartbeat + LWT, `/workers` renders retained presence.
3. `dispatch_task` → worker executes → result published → `get_result`, silent digest only.
4. `notify: wake`, coalescing, `cancel`, `timeout`, `started`-ack redispatch, `/tasks`.
5. Package + Railway deploy + the room credential.

## Superseded: per-machine credentials

The first design gave every machine its own MQTT user, with ACLs keyed on `%u`
(`workers/<id>/ctl` readable only by that user). It gave real isolation — a stolen
worker credential could not read another worker's tasks or forge its results — and it
cost an admin step per machine: edit `PI_MQTT_USERS`, redeploy, keep username equal to
worker id.

It was replaced by rooms because the admin step dominated the workflow. What the
replacement gives up, deliberately:

- the broker cannot distinguish members, so any member can read every task and result,
  dispatch tasks, and publish as any worker id;
- result authenticity rests on the payload (`workerId` compared to the pinned worker),
  which a hostile member can satisfy by claiming the right id;
- uniqueness moved to the broker via the derived client id `<room>-<worker-id>`, so a
  second session evicts the first (the evicted one detects `0x8E` and stops).

Returning to isolation means one credential per device again, most cheaply as client
certificates with `use_identity_as_username` (the ACL `%u` rules keep working).
