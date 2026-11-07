# pi-mqtt

Coordinate independent [pi](https://pi.dev) sessions over MQTT: one **orchestrator**
(your live session) pins tasks to named **workers** (other live pi sessions, one run
at a time).

**First time here? [SETUP.md](./SETUP.md) walks through install → configure → first task.**

Design decisions live in [DESIGN.md](./DESIGN.md). This README covers installing and running it.

## Install

It is a pi package: install once, then no `--extension` flag. Full walkthrough:
[SETUP.md](./SETUP.md).

```bash
pi install git:github.com/bhenriq/pi-mqtt

# or from a clone
pi install ./

# or try it for one session without installing
pi -e ./
```

Then start sessions with a role:

```bash
pi --pi-mqtt-role worker --name my-session
pi --pi-mqtt-role orchestrator
```

The package registers itself as **`mqtt.ts`** in `pi config` (pi shows the file
name; there is no display-name field). Disable or enable it there, and remove the
package with `pi remove git:github.com/bhenriq/pi-mqtt`.

Installed but idle: a session with no `--pi-mqtt-role` opens no socket, publishes
no presence, and registers no tools.

`pi install` fetches the `mqtt` dependency automatically. Upgrade with
`pi update --extensions`.

## Status

| Step | State |
|---|---|
| 1. Protocol + local broker | done |
| 2. Worker presence, LWT, orchestrator registry (`/mqtt status`, `list_workers`) | done, verified |
| 3. `dispatch_task` → worker run → result → `get_result` | done, verified |
| 4. `notify: wake`, digest coalescing, cancel, `/tasks`, `list_tasks` | done, verified — timeout and cost cap implemented, not exercised end to end |
| 4b. `dispatch_task` waits for the result by default; duplicate-id guard | done, verified |
| 5. Public broker on Railway | deployed: `wss://broker-production-0bfd.up.railway.app`, see [deploy/](./deploy/README.md) |

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

If the package is installed (`pi list` shows pi-mqtt), the helper uses it and does
not also pass `--extension` — loading both would register every tool twice.

Each call runs one process in the foreground, so Ctrl-C stops just that piece. Extra
workers are more shells with distinct ids: `scripts/dev.sh worker -i worker-2`. Common
flags: `-p 1884` for a different broker port, `-c ci` for a separate room,
`--dry-run` to see the command without running it. On a non-default port the broker's
WebSocket listener moves to `PORT+7182`. `npm run dev:broker`, `dev:worker`, and
`dev:orchestrator` are the same three commands.

By default the broker serves plain MQTT on `PORT` and **plain** WebSocket on
`PORT+7182` — no TLS. `--tls DIR` adds the two TLS listeners, and the other commands
pick the transport with `-t`:

```bash
scripts/dev.sh broker --tls ./certs --self-signed   # + wss 9066, mqtts 9067
scripts/dev.sh worker -t wss                        # CA found automatically
scripts/dev.sh orchestrator -t wss
```

`--self-signed` creates `certs/cert.pem` and `certs/key.pem` when they are missing
(SAN `127.0.0.1` and `localhost`, valid 2 days). Drop the flag to use your own pair;
`./certs` is gitignored.

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

Two sessions in the same room:

```bash
# worker
PI_MQTT_URL=mqtt://127.0.0.1:1883 \
  pi --extension ./extensions/mqtt.ts --pi-mqtt-role worker --name lap-b

# orchestrator (another terminal)
PI_MQTT_URL=mqtt://127.0.0.1:1883 \
  pi --extension ./extensions/mqtt.ts --pi-mqtt-role orchestrator

# then, in the orchestrator session
/mqtt start
```

`/mqtt status` reports the local state and the whole roster (workers and
orchestrators) in one table.

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

`/mqtt kick [reason]` evicts everyone else: it publishes a room-wide message that every
other session obeys by disconnecting itself. The sender stays. Since a room is one shared
password, any member can do this — the password is the capability, and the kick is
cooperative, not enforced by the broker.

Commands: `/tasks` (dispatched tasks), `/mqtt config`
(interactive editor) and `/mqtt` — bare or `/mqtt status` for local state and counts,
`/mqtt start` / `/mqtt stop` / `/mqtt restart` to connect and disconnect.

`/mqtt config` edits `~/.pi/agent/pi-mqtt.json` (or the project file, or
`--pi-mqtt-config`) without hand-editing JSON: role, broker, room, room password
(masked in the TUI), connect mode, worker id, and an advanced submenu. It
validates as you type, can **test the connection** before saving, shows which values
are overridden by an environment variable, writes atomically with mode 600, and
reconnects on save. Repos stay a file-only setting.

Connecting is explicit: a configured session still opens no socket until `/mqtt start`,
unless it was launched with `--pi-mqtt-start` or its config says `"connect": "auto"`.

Dropped connections are re-established automatically (2s), subscriptions are re-applied, and a
worker re-announces its presence immediately instead of waiting for the next heartbeat.
`/mqtt status` reports `drops: N (last <time>: <source> <code>)` once a connection has been lost,
which distinguishes a normal close (`1000`) from an abnormal one (`1006`, no close frame — usually
an intermediary or a sleeping machine). `keepaliveSec` defaults to 15, so a dead socket is noticed
in about 22s.

`dispatch_task` options worth knowing: `notify: "silent" | "wake"`, `ifBusy: "fail" | "wait"`,
`timeoutMs`, `maxCostUsd`, and `repo: { baseRef | baseSha, branch }`.

Worker side: one worker id per live session. A second session claiming an id that is
still live is refused (`--pi-mqtt-force` takes it over deliberately), because two
sessions under one id would both consume tasks.

Task transcripts are the worker's own sessions, and each result reports `sessionFile`,
so you can `/resume` a worker run and read exactly what it did. Per-task JSONL logs
land in `~/.pi/agent/pi-mqtt/tasks/<worker>.jsonl`.

## Public broker

A broker is deployed on Railway and reachable as
`wss://broker-production-0bfd.up.railway.app` (MQTT over TLS WebSocket, port 443).
Join a room with its name and password:

```bash
export PI_MQTT_URL=wss://broker-production-0bfd.up.railway.app

# orchestrator
pi                                   # config: role orchestrator, room "dev"
/mqtt start

# worker
PI_MQTT_ROOM=dev PI_MQTT_ROOM_PASSWORD=... \
  scripts/dev.sh worker -u "$PI_MQTT_URL" -i worker-1
```

Setup, caveats, and the single broker credential: [deploy/README.md](./deploy/README.md).

## Configuration

One file per machine, then no flags and no environment:

`~/.pi/agent/pi-mqtt.json` — a worker:

```json
{
  "role": "worker",
  "name": "prudential",
  "room": { "name": "dev", "password": "<the room password>" },
  "broker": { "url": "wss://broker-production-0bfd.up.railway.app" }
}
```

Four groups, one each: `role`/`name` (who this session is), `room` (which room and its
password), `broker` (where it lives), and optional `worker` (behaviour such as
`heartbeatMs`). Defaults are omitted, so a minimal file stays minimal.

`name` is the machine's stable base name. The session id is `<name>_<pi session name>`
(`prudential_billing`) — so the pi session must be named to join: `pi --name billing`, or
`/name billing` before `/mqtt start`. An unnamed session is refused rather than being
given a generated id — there is no override: an id is a function of the machine name and
the pi session name, nothing else. Without `name`, the computer name is the base.

An orchestrator is the same file with `"role": "orchestrator"`.

```bash
chmod 600 ~/.pi/agent/pi-mqtt.json   # it holds the room password
pi                                   # already a worker, in room "dev"
/mqtt status                          # role, broker, room, state, peers
```

A **room** is a named namespace plus one shared password: same name and password
means you see each other, anything else and you do not. There are no per-machine
accounts to create — hand someone the room password and they can join.

The **session id** is declared, not authenticated, and it is per *session*: several
sessions can join from one host, and nothing is asked beyond naming the pi session: the id
is `<name>_<session name>` for workers *and* orchestrators. **Collisions are resolved by
takeover, not by renaming** — a second session deriving the same id evicts the first,
which stops with `session "x" was taken over by another session`. Because the id includes
the pi session name, a worker you pin tasks to should keep a stable session name.

Orchestrators announce themselves on `pi/v1/<room>/orchestrators/<id>/presence`, so
you can see who is coordinating: `/mqtt status` lists them and `/mqtt status` shows the roster. They are a different topic from workers by design, so nothing can
dispatch a task to one. The id is the session name (`pi --name demo`), else the hostname.

Because the client id is `<room>-<worker-id>`, the broker permits one live session per
id: if a pinned id is claimed twice, the older session is evicted and stops with
`worker id "x" was taken over by another session`.

Where the file is looked up: `./pi-mqtt.json` in the working directory, then
`~/.pi/agent/pi-mqtt.json`, or `--pi-mqtt-config <path>`. See
[`pi-mqtt.example.json`](./pi-mqtt.example.json).

### Precedence and overrides

CLI flag > environment > `pi-mqtt.json`.

| Setting | Flag | Environment |
|---|---|---|
| role | `--pi-mqtt-role` | `PI_MQTT_ROLE` |
| broker url | — | `PI_MQTT_URL` |
| room | — | `PI_MQTT_ROOM` |
| room password | — | `PI_MQTT_ROOM_PASSWORD` |
| worker name | — | — (file only: `name`) |
| config path | `--pi-mqtt-config` | — |
| CA bundle | — | `PI_MQTT_CA_FILE` |
| skip TLS verify | — | `PI_MQTT_REJECT_UNAUTHORIZED=0` (debugging only) |

Environment wins over the file, so CI and one-off runs can override without
editing it. A session with no role anywhere stays inert: no socket, no presence,
no tools.

Worker ids and room names are `[A-Za-z0-9][A-Za-z0-9_-]{0,63}`; they end up in
topic names. `cluster` and `token` are still accepted as the old names for `room`
and `roomPassword`, so pre-existing files keep working.

### Transports

| URL | Use |
|---|---|
| `mqtt://` | Plain TCP. Local development only |
| `mqtts://` | TLS on a raw MQTT port (usually 8883) |
| `ws://` | Plain WebSocket, for the local broker's 9001 listener |
| `wss://` | TLS WebSocket. `wss://host/mqtt`, or `wss://host` (mqtt.js uses `/mqtt`) |

The scheme in `mqtt.url` (or `PI_MQTT_URL`) is the only thing that picks the
transport; there is no separate "use websockets" switch.

`wss://` matters where raw TCP is blocked or proxied unreliably — a host that
terminates TLS in front of a normal HTTP port, for instance. Point `mqtt.caFile`
(or `PI_MQTT_CA_FILE`) at a PEM bundle when the broker uses a CA your system
store does not trust. Any other scheme is rejected at startup with a clear error,
and a failed handshake closes the client instead of retrying forever.

## Protocol

```
pi/v1/<room>/workers/<id>/presence   worker → all, retained, LWT on crash
pi/v1/<room>/orchestrators/<id>/presence   orchestrator → all, retained
pi/v1/<room>/workers/<id>/ctl        orchestrator → worker (task | cancel)
pi/v1/<room>/workers/<id>/ack        worker → orchestrator (task started)
pi/v1/<room>/results/<taskId>        worker → orchestrator
```

Presence is retained with a 24-hour expiry, so a worker that disappears does not
linger in the roster forever. Retained means `list_workers` is a local map read rather
than a network round trip. A worker's unpublished state is detected two ways: the Last Will on
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
