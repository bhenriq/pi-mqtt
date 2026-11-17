# pi-mqtt for pi on iOS

Drive your pi-mqtt **workers** from the phone: see which machines are in the room,
send one a task, read the result, cancel a run. It is an **orchestrator**, the same
role a desktop session running `pi-mqtt` has — the workers stay where they are.

No dependencies, no build. It talks to the same broker with the same wire protocol
as the rest of this repo, so a phone and a laptop are just two members of one room.

```
iPhone (this extension)  ──wss──►  broker  ◄──wss──  pi --pi-mqtt-role worker --name lap-b
   orchestrator                                               worker
```

## Why this is not just `pi install pi-mqtt`

pi on iOS has no Node `net`/`tls`, so `mqtt.js` cannot open a raw MQTT connection.
WebSocket *is* available, and mosquitto has a WebSocket listener, so this package
implements the handful of MQTT 3.1.1 packets an orchestrator needs directly on top
of `WebSocket` (`src/mqtt-ws.ts`). That is why it is a separate package with zero
dependencies rather than a thin wrapper over the desktop one.

The broker must therefore be reachable over **`ws://` or `wss://`** — the deployed
one is `wss://broker-production-0bfd.up.railway.app` (`/mqtt`, port 443, see
`deploy/`). `mqtt://` and `mqtts://` are refused with an explanation instead of a
socket error.

## Install

On the phone, in a pi session:

```
extension_add github:bhenriq/pi-mqtt/ios
```

Or add this folder while developing:

```
extension_add /path/to/pi-mqtt/ios      # scope "default" makes it every session
```

A desktop session does not need it. Adding it there is harmless but pointless.

## Set up

1. The room password goes in the **Keychain**, never in a config file:

   ```
   /mqtt password
   ```

   (or Settings → Extensions & skills → Secrets, secret `PI_MQTT_ROOM_PASSWORD`,
   which is also the variable name a worker uses.)

2. Broker URL, room and the name this phone shows up under:

   ```
   /mqtt setup
   ```

   Defaults: `wss://broker-production-0bfd.up.railway.app`, room `dev`, name
   `iphone`. Broker user defaults to `cluster` (the single user the deployed
   broker has) and is editable in the app's settings.

3. Join:

   ```
   /mqtt connect
   ```

Settings live in `pi.synced`, so they follow the user across devices. The password
lives in the Keychain and is never shown again.

## Use it

In the MQTT app, tap the antenna in the session's dock (or `/mqtt app`):

- **Workers** — every worker in the room with state, repo@branch, model and
  heartbeat age. Tap **Send a task**, type, send.
- **Tasks** — what this phone dispatched: state, age, output. Tap a card to expand
  the result; cancel while it runs.
- **Log** — the last few transport lines, for when something is not arriving.

The dock badge counts outstanding tasks.

From the transcript, in plain language:

```
which workers are in the room?
ask lap-b to run the test suite and summarise what failed
```

| Tool | What it does |
|---|---|
| `mqtt_workers` | The roster: workers and orchestrators, state, repo, model, cwd, heartbeat age |
| `mqtt_dispatch` | Pin a task to one worker. Waits for the result by default; `wait: false` fans out |
| `mqtt_result` | Full result for a task id: final text, diffstat, patch path, worker session file |
| `mqtt_tasks` | Task states: queued, sent, running, done, error, timeout, cancelled, lost |
| `mqtt_cancel` | Cooperative abort; the worker reports partial output |
| `mqtt_pair` | Worker admission: `pending` codes, `admit` by code, `revoke`, `trusted` |
| `mqtt_status` | Broker, room, device name, link state, reachable workers |

| Command | What it does |
|---|---|
| `/mqtt status` | Configuration and link state |
| `/mqtt connect` / `/mqtt disconnect` | Join or leave the room |
| `/mqtt workers` / `/mqtt tasks` | The roster and this phone's tasks |
| `/mqtt pending` / `/mqtt admit <worker> <code>` | Codes waiting for approval, and approving one |
| `/mqtt trusted` / `/mqtt revoke <worker>` | The approved keys, and dropping one |
| `/mqtt setup` | URL, room, phone name, broker user |
| `/mqtt password` | Store the room password in the Keychain |
| `/mqtt notify on\|off` | Phone notification when a task finishes |
| `/mqtt app` | Open the MQTT app |

`mqtt_dispatch` takes the same options as the desktop tool: `notify: "silent" |
"wake"`, `ifBusy: "fail" | "wait"`, `waitTimeoutMs`, `timeoutMs`, `maxCostUsd`,
`model`, and `repo: { baseRef | baseSha, branch }`.

## Approving workers

A worker joins as **`pending`**: the broker no longer decides *which machine* is answering
to a worker id, so the orchestrator does. The worker advertises a long-term Ed25519 key
and signs a join nonce; this phone answers with a challenge nonce, and both ends derive
the same six-digit code from the two nonces and the worker key.

- the code appears here (the MQTT app, `mqtt_workers`, `/mqtt pending`, and a log line);
  compare it with the one on the worker's screen — a squatter wearing the same id holds
  a different key and shows a different code
- approve it with the **Approve** button in the app, `/mqtt admit <worker> <code>`, or
  `mqtt_pair`
- the approval is pinned to the *key* in `pi.synced` (`trusted:<room>`), so a returning
  worker is admitted without a human: restarts, reloads and sleeps do not turn it back
  into a stranger; `/mqtt revoke <worker>` sends it back to `pending`
- `mqtt_dispatch` refuses a pending or unapproved worker and says which command to run

The proof is checked with WebCrypto Ed25519 (`node:crypto` on iOS has no `verify`), and
the code and fingerprint are byte-for-byte the desktop's algorithms (`src/protocol.ts`).

## Roles are credentials

The broker grants a role per credential (`PI_MQTT_USERS`, e.g. `tribe:…:orchestrator:dev`;
see `deploy/README.md`). This phone must therefore connect with the **orchestrator**
credential: its user name in `/mqtt setup` (or the app's *Broker user*) and its password
in the Keychain (`/mqtt password`). A worker credential is refused at CONNACK, and the
error names the reason instead of showing a silent, frozen roster.

## Protocol parity

Identical to `src/protocol.ts` in the repo root, so nothing on the worker side
changes:

```
pi/v1/<room>/workers/<id>/presence          retained, LWT on crash (worker → all)
pi/v1/<room>/orchestrators/<id>/presence    retained, LWT on crash (this phone)
pi/v1/<room>/workers/<id>/ctl               task | cancel   (phone → worker)
pi/v1/<room>/workers/<id>/ack               task started    (worker → phone)
pi/v1/<room>/results/<taskId>               result          (worker → phone)
pi/v1/<room>/control                        kick            (room-wide)
```

The phone:

- publishes its own presence under `orchestrators/<name>/presence` with a Will, so
  a crashed phone drops out of the roster instead of lingering;
- heartbeats every `heartbeatMs` (30s), re-announces on reconnect;
- re-sends a task once if no ack arrives in 5s (a worker that never acknowledged
  touched nothing);
- ignores results whose `workerId` is not the worker the task was pinned to (the
  shared room credential means the broker cannot enforce that);
- obeys `/mqtt kick` from another member by leaving the room.

## Limits worth knowing

- **Orchestrator only.** The phone dispatches; it does not accept tasks. A worker
  is expected to have the repo and a shell where the work happens.
- **No background connection.** iOS suspends the engine, so the socket and the
  room membership go with it. Opening the app or using a tool reconnects; nothing
  is lost, since tasks and results live on the broker/worker side while it is away.
- **No MQTT 5 message expiry.** Presence is retained and cleared on a clean leave
  or replaced with `offline` by the Will on a crash; a worker that died long ago can
  linger until something overwrites it (the roster marks it stale after three missed
  heartbeats).
- **Roles and keys are the capabilities**, exactly as on the desktop: only an
  orchestrator credential may dispatch or kick, and only a key this phone approved may
  take a task. The worker credential is still shared by the room's workers, so treat it
  like shell access to all those machines, and the orchestrator credential like dispatch
  rights to all of them.

## Troubleshooting

| What you see | What it means |
|---|---|
| `refused: not authorised` | This credential is not an orchestrator credential on the broker (`PI_MQTT_USERS`): check the broker user in `/mqtt setup` and the password with `/mqtt password` |
| `worker "x" is waiting for approval` | A new worker joined as `pending`: compare its code with the worker's screen, then `/mqtt admit x <code>` |
| `the broker must be a WebSocket URL` | Point at `wss://host/mqtt` (the websockets listener), not `mqtts://host:8883` |
| `unknown worker "x"` | Nothing in the roster under that id: check `mqtt_workers`, and that the worker session is named and connected |
| `worker "x" is stale` | Last heartbeat older than 3 × `heartbeatMs` — the machine is asleep or the link there died |
| `the broker closed this connection 5 times in 20s` | Something is systematically closing it (a second session with the same id, or a proxy that will not hold a WebSocket). `/mqtt connect` retries |
| Roster empty but workers exist | The retained presence had not arrived yet: `mqtt_workers` waits for it, `/mqtt status` re-reads |
