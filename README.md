# pi-mqtt

Coordinate independent [pi](https://pi.dev) sessions over MQTT: one **orchestrator**
(your live session) pins tasks to named **workers** (other live pi sessions, one run
at a time).

Design decisions live in [DESIGN.md](./DESIGN.md). This README covers running it.

## Status

| Step | State |
|---|---|
| 1. Protocol + local broker | done |
| 2. Worker presence, LWT, orchestrator registry (`/workers`, `list_workers`) | done, verified |
| 3. `dispatch_task` → worker run → result → `get_result` | done, verified |
| 4. `notify: wake`, digest coalescing, cancel, `/tasks`, `list_tasks` | done, verified — timeout and cost cap implemented, not exercised end to end |
| 4b. `dispatch_task` waits for the result by default; duplicate-id guard | done, verified |
| 5. Packaging + Railway deploy | broker assets written; not deployed |

## Local development

```bash
npm install
npm run broker          # mosquitto on 127.0.0.1:1883, no auth
node scripts/probe.mjs  # prints presence/ack/result traffic
```

Two sessions in the same cluster:

```bash
# worker
PI_MQTT_URL=mqtt://127.0.0.1:1883 \
  pi --extension ./src/extension.ts --pi-mqtt-role worker --pi-mqtt-id lap-b

# orchestrator (another terminal)
PI_MQTT_URL=mqtt://127.0.0.1:1883 \
  pi --extension ./src/extension.ts --pi-mqtt-role orchestrator

# then, in the orchestrator session
/workers
```

A session with no `--pi-mqtt-role` (or no broker URL) stays completely inert: no
socket, no tools, no presence.

## Using it

In the orchestrator session, in plain language:

```
which workers are available?
ask lap-b what time it is
```

Orchestrator tools (active only in an orchestrator session):

| Tool | Behavior |
|---|---|
| `list_workers` | Retained presence: state, repo/branch, model, cwd, heartbeat age |
| `dispatch_task` | Pins a task to one worker. **Waits for the result by default** (`waitTimeoutMs`, 120s) and returns it; `wait: false` fans out and the result arrives later as a digest |
| `get_result` | Full result for a task id: final text, diffstat, patch path, worker session file |
| `list_tasks` | Task states: queued, sent, running, done, timeout, cancelled, lost |
| `cancel_task` | Cooperative abort; the worker reports partial output |

Commands: `/workers`, `/tasks`, `/mqtt` (connection status and recent log lines).

`dispatch_task` options worth knowing: `notify: "silent" | "wake"`, `ifBusy: "fail" | "wait"`,
`timeoutMs`, `maxCostUsd`, and `repo: { baseRef | baseSha, branch }`.

Worker side: one worker id per live session. A second session claiming an id that is
still live is refused (`--pi-mqtt-force` takes it over deliberately), because two
sessions under one id would both consume tasks.

Task transcripts are the worker's own sessions, and each result reports `sessionFile`,
so you can `/resume` a worker run and read exactly what it did. Per-task JSONL logs
land in `~/.pi/agent/pi-mqtt/tasks/<worker>.jsonl`.

## Configuration

Credentials from the environment only: `PI_MQTT_URL`, `PI_MQTT_USERNAME`,
`PI_MQTT_PASSWORD`, `PI_MQTT_CLUSTER`, `PI_MQTT_WORKER_ID`, `PI_MQTT_ROLE`.

Everything else from `pi-mqtt.json` in the working directory, or
`~/.pi/agent/pi-mqtt.json`, or `--pi-mqtt-config <path>`. See
[`pi-mqtt.example.json`](./pi-mqtt.example.json).

Worker ids and cluster ids are `[A-Za-z0-9][A-Za-z0-9_-]{0,63}`; they end up in
topic names.

## Protocol

```
pi/v1/<cluster>/workers/<id>/presence   worker → all, retained, LWT on crash
pi/v1/<cluster>/workers/<id>/ctl        orchestrator → worker (task | cancel)
pi/v1/<cluster>/workers/<id>/ack        worker → orchestrator (task started)
pi/v1/<cluster>/results/<taskId>        worker → orchestrator
```

Presence is retained, so `list_workers` is a local map read rather than a network
round trip. A worker's unpublished state is detected two ways: the Last Will on
crash, and heartbeat age (`3 × heartbeatMs` → shown as `idle?`/`busy?`).

## Testing without Docker

`broker/mosquitto.dev.conf` is a plain mosquitto config:

```bash
brew install mosquitto
npm run broker
```

`npm run broker` runs `scripts/broker.sh`, which resolves the binary from `PATH`
first and then from Homebrew's `sbin` (not on `PATH` by default). Override the
config with `PI_MQTT_BROKER_CONF=./other.conf npm run broker`.

The deployed config lives in [`deploy/`](./deploy/README.md).
