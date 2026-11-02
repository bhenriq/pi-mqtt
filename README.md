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
scripts/dev.sh -h        # what the helper can start
```

Start one piece per shell (`scripts/dev.sh -h` has the details):

```bash
scripts/dev.sh broker           # shell 1: mosquitto on 127.0.0.1:1883
scripts/dev.sh worker           # shell 2: worker-1
scripts/dev.sh orchestrator     # shell 3: the orchestrator
```

Each call runs one process in the foreground, so Ctrl-C stops just that piece. Extra
workers are more shells with distinct ids: `scripts/dev.sh worker -i worker-2`. Common
flags: `-p 1884` for a different broker port, `-c ci` for a separate cluster,
`--dry-run` to see the command without running it. On a non-default port the broker's
WebSocket listener moves to `PORT+7182`. `npm run dev:broker`, `dev:worker`, and
`dev:orchestrator` are the same three commands.

By default the broker serves plain MQTT on `PORT` and **plain** WebSocket on
`PORT+7182` — no TLS. `--tls DIR` adds the two TLS listeners, and the other commands
pick the transport with `-t`:

```bash
openssl req -x509 -newkey rsa:2048 -nodes -keyout certs/key.pem -out certs/cert.pem \
  -days 2 -subj "/CN=127.0.0.1" -addext "subjectAltName=IP:127.0.0.1"

scripts/dev.sh broker --tls ./certs     # + wss://127.0.0.1:9066, mqtts://127.0.0.1:9067
scripts/dev.sh worker -t wss            # certs found automatically via the broker's record
scripts/dev.sh orchestrator -t wss
```

`-t` maps to the local broker's ports and needs no URL:

| `-t` | URL for `PORT=1883` | For another `-p PORT` |
|---|---|---|
| `mqtt` (default) | `mqtt://127.0.0.1:1883` | `mqtt://127.0.0.1:PORT` |
| `ws` | `ws://127.0.0.1:9001` | `ws://127.0.0.1:PORT+7182` |
| `wss` | `wss://127.0.0.1:9066` | `wss://127.0.0.1:PORT+7183` |
| `mqtts` | `mqtts://127.0.0.1:9067` | `mqtts://127.0.0.1:PORT+7184` |

For a remote broker, use `-u URL` instead and add `--ca FILE` when its certificate
is not publicly trusted:

```bash
scripts/dev.sh worker -u wss://broker.example.com/mqtt --ca ./broker-ca.pem
```

Or do it by hand:

```bash
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

Credentials and transport from the environment only: `PI_MQTT_URL`,
`PI_MQTT_USERNAME`, `PI_MQTT_PASSWORD`, `PI_MQTT_CLUSTER`, `PI_MQTT_WORKER_ID`,
`PI_MQTT_ROLE`, `PI_MQTT_CA_FILE` (PEM bundle), `PI_MQTT_REJECT_UNAUTHORIZED=0`
(disables TLS verification — debugging only).

`PI_MQTT_URL` is what selects the transport: `mqtt://`, `mqtts://`, `ws://`, or
`wss://`. Nothing else chooses it, and there is no separate "use websockets" flag.

Everything else from `pi-mqtt.json` in the working directory, or
`~/.pi/agent/pi-mqtt.json`, or `--pi-mqtt-config <path>`. See
[`pi-mqtt.example.json`](./pi-mqtt.example.json).

Worker ids and cluster ids are `[A-Za-z0-9][A-Za-z0-9_-]{0,63}`; they end up in
topic names.

### Transports

| URL | Use |
|---|---|
| `mqtt://` | Plain TCP. Local development only |
| `mqtts://` | TLS on a raw MQTT port (usually 8883) |
| `ws://` | Plain WebSocket, for the local broker's 9001 listener |
| `wss://` | TLS WebSocket. `wss://host/mqtt`, or `wss://host` (mqtt.js uses `/mqtt`) |

The scheme in `PI_MQTT_URL` (or `mqtt.url`) is the only thing that picks the
transport; there is no separate "use websockets" switch.

`wss://` matters where raw TCP is blocked or proxied unreliably — a host that
terminates TLS in front of a normal HTTP port, for instance. Point
`PI_MQTT_CA_FILE` (or `mqtt.caFile`) at a PEM bundle when the broker uses a CA
your system store does not trust. Any other scheme is rejected at startup with a clear error, and a
failed handshake closes the client instead of retrying forever.

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
