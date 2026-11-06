# Setup

For a first run: install the extension, write one config file, start talking.

You need **pi** installed and a **broker** to talk through. Either use a broker
someone gave you (a URL plus a username/password) or run one yourself — see
[deploy/README.md](./deploy/README.md) or the [local broker](#local-only-setup)
section at the end.

---

## 1. Install

```bash
pi install git:github.com/bhenriq/pi-mqtt
```

That clones the package, installs its `mqtt` dependency, and records it in
`~/.pi/agent/settings.json`. Check:

```bash
pi list          # shows: git:github.com/bhenriq/pi-mqtt
pi config        # Extensions → mqtt.ts (enabled)
```

The name is `mqtt.ts`: pi names an extension after its file, and there is no
display-name field.

From then on, sessions load it automatically — **no `--extension` flag**. Keep it
updated with `pi update --extensions`, remove it with:

```bash
pi remove git:github.com/bhenriq/pi-mqtt
```

Try it without installing, for one session:

```bash
pi -e ./
```

Installed but unconfigured, the extension does nothing: no socket, no presence, no
tools. So installing it globally is harmless.

---

## 2. Configure

One file per machine, `~/.pi/agent/pi-mqtt.json`. It replaces every flag and every
environment variable.

### A worker (a machine that takes tasks)

```json
{
  "role": "worker",
  "mqtt": {
    "url": "wss://broker-production-0bfd.up.railway.app",
    "cluster": "dev",
    "username": "worker-1",
    "password": "<the password for worker-1>"
  }
}
```

```bash
chmod 600 ~/.pi/agent/pi-mqtt.json    # it holds a password
```

Prefer not to hand-edit? `/mqtt config` in a session opens a menu that writes this
file for you: role, broker, cluster, username, password (masked), connect mode, worker
id, plus a **Test connection** action and an advanced submenu. It reconnects on save.

### An orchestrator (the session you type into)

Same file, two values differ:

```json
{
  "role": "orchestrator",
  "mqtt": {
    "url": "wss://broker-production-0bfd.up.railway.app",
    "cluster": "dev",
    "username": "orch",
    "password": "<the orchestrator password>"
  }
}
```

### The one rule that matters

**The MQTT username is the worker id.** The broker's ACLs grant access with the
username in the topic path, so a session authenticating as `worker-1` may only
read `workers/worker-1/…`. The extension defaults `workerId` to the username for
exactly this reason — if you set them to different values, that session will
publish presence and never receive a task.

So: to use the id `worker-7`, the broker must have a `worker-7` user. Ask the
broker's operator to add one, or add it yourself if it is yours:

```bash
railway variables --set "PI_MQTT_USERS=orch:<pw>,worker-1:<pw>,worker-7:<newpw>" --service broker
railway up -d -y     # from deploy/
```

### Why these values

| Field | Notes |
|---|---|
| `role` | `worker` or `orchestrator`. Without it (here, in `PI_MQTT_ROLE`, or via `--pi-mqtt-role`) the extension stays inert |
| `mqtt.url` | The scheme picks the transport: `wss://` (TLS WebSocket, usual for a hosted broker), `mqtts://` (TLS on a raw port), `mqtt://`/`ws://` (plain, local only) |
| `mqtt.cluster` | Topic prefix. Everyone who should see each other must share it |
| `mqtt.username` / `mqtt.password` | Issued by the broker operator. On a worker the username must equal the worker id |
| `connect` | `"manual"` (default) connects only when you run `/mqtt start`; `"auto"` connects at session start |
| `worker.workerId` | Optional. Defaults to the username; set it only if your broker does not tie the two together |

### Where the file is read from

1. `./pi-mqtt.json` (working directory) — wins, useful for a project override
2. `~/.pi/agent/pi-mqtt.json` — the usual place
3. `--pi-mqtt-config <path>`

Environment overrides the file, and CLI flags override both:
`PI_MQTT_ROLE`, `PI_MQTT_URL`, `PI_MQTT_CLUSTER`, `PI_MQTT_USERNAME`,
`PI_MQTT_PASSWORD`, `PI_MQTT_WORKER_ID`, `PI_MQTT_CA_FILE`,
`PI_MQTT_REJECT_UNAUTHORIZED=0`; flags `--pi-mqtt-role`, `--pi-mqtt-id`,
`--pi-mqtt-config`.

---

## 3. First run

Start the worker first, then the orchestrator.

**On the worker machine:**

```bash
pi                  # starts disconnected
/mqtt start         # connect, publish presence, start taking tasks
```

Or connect at launch, which is what a machine that must be listening wants:

```bash
pi --pi-mqtt-start
# or put "connect": "auto" in pi-mqtt.json
```

In the TUI, `/mqtt status` reports the role, broker, cluster, `state: connected`, and
how many workers are online. From an orchestrator, `/workers` prints the full table. Check from the orchestrator too — `rpi5`-style ids
appear in the worker list.

Start it from the directory tasks should run in: a worker's working directory is
wherever `pi` was started.

**On the orchestrator machine:**

```bash
pi
/mqtt start
which workers are available?
ask worker-1 what time it is
```

`/workers` lists the same thing; `/tasks` shows dispatched tasks and their states.

---

## 4. Once it works

Tools available in an orchestrator session:

| Tool | What it does |
|---|---|
| `list_workers` | Who is online: state, repo/branch, model, cwd, heartbeat age |
| `dispatch_task` | Pins a task to one worker. **Waits for the result by default** and returns it |
| `get_result` | Full result for a task id: text, diffstat, patch path, worker session file |
| `list_tasks` | States: queued, sent, running, done, timeout, cancelled, lost |
| `cancel_task` | Cooperative abort; the worker reports partial output |

`dispatch_task` options: `wait: false` (fan out, results arrive later as digests),
`notify: "wake"` (start a turn when it lands), `ifBusy: "wait"` (hold until the
worker is idle), `timeoutMs`, `maxCostUsd`, `repo: { baseSha | baseRef, branch }`.

---

## Troubleshooting

| Symptom | Cause and fix |
|---|---|
| Nothing happens, no tools, no `/workers` | Not connected. Run `/mqtt start` (connecting is explicit; `--pi-mqtt-start` or `"connect": "auto"` connects at launch) |
| Nothing happens, and `/mqtt` says "not configured" | No role. Add `"role"` to the config, or pass `--pi-mqtt-role` |
| `pi-mqtt inactive: no broker url` | No `mqtt.url` in the file and no `PI_MQTT_URL` |
| `pi-mqtt: cannot reach wss://…: self-signed certificate` | Broker uses a private CA: set `mqtt.caFile` to its PEM bundle |
| `worker id "x" is already live on …` | Two sessions using one id. Stop the old one, use another `-i`/`workerId`, or `--pi-mqtt-force` to take over deliberately |
| `dispatch failed: worker "x" is offline` | No live session with that id, or its heartbeat is older than 3× `heartbeatMs` |
| `dispatch failed: worker "x" is busy with …` | One run at a time per worker. Use `ifBusy: "wait"` or another worker |
| Presence appears, but dispatched tasks fail with auth/model errors | The **worker** needs its own model credentials (`pi` → `/login`). The broker being reachable does not give it a model |
| Task result says `busy with …` | A second task arrived while one ran; it was rejected rather than queued |
| `/workers` empty on the orchestrator but the worker is running | Different cluster (`mqtt.cluster`), or the worker's username/id mismatch |

Permission problems on the broker side — a worker that cannot read its own tasks,
or can read someone else's — are covered in
[deploy/README.md](./deploy/README.md) with a script that checks both directions.

---

## Local-only setup

No broker of your choosing? Run one on your machine, plus both roles:

```bash
npm install
scripts/dev.sh broker           # shell 1: mosquitto on 127.0.0.1:1883
scripts/dev.sh worker           # shell 2
scripts/dev.sh orchestrator     # shell 3
```

That broker has **no authentication** and binds to `127.0.0.1`, so it is for
development only. `scripts/dev.sh -h` lists the rest (other ports, transports,
`wss` with self-signed certs, `--dry-run`). With a local broker running you can
skip the config file entirely, because the helper passes the URL through the
environment.

---

## Security notes for whoever runs the broker

- Only the TLS WebSocket endpoint is public; the plain listeners stay inside the
  container.
- Every client needs a username/password, and ACLs keep a worker to its own
  control topic, its own presence, and results it publishes.
- A worker credential currently **can** publish a result for any task id, so a
  compromised worker could answer a task pinned to a different worker. Validation
  in the orchestrator or per-worker result topics are the fixes; until then, treat
  worker machines as trusted.
- Keep `pi-mqtt.json` at mode `600` and out of any repository. On a worker the
  file contains that machine's broker password, and the broker password is the
  machine's whole identity.
