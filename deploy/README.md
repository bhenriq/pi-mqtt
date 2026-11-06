# Deploying the broker (Railway)

Stock mosquitto. One shared user (`cluster`) carries the room password; per-machine
users from the earlier design still work while machines migrate.

**Deployed:** project `hello-world` (workspace *Tribe Internal Apps*), service
`broker`, environment `production`.

| Fact | Value |
|---|---|
| Public endpoint | `wss://broker-production-0bfd.up.railway.app` (port 443, path `/mqtt`) |
| Container listeners | `1883` mqtt, `9001` websockets |
| Volume | `broker-volume` mounted at `/mosquitto/data` |
| Env | `PI_MQTT_USERS=orch:<pw>,worker-1:<pw>,cluster:<room-password>` |

Redeploy after changing anything here:

```bash
cd deploy
railway up -d -y          # builds the Dockerfile and rolls the service
railway logs --lines 40   # entrypoint prints "configured user: ..." per user
```

## Client wiring

Join a room: the broker credential is the shared `cluster` user, and the room name
is the topic namespace.

```bash
export PI_MQTT_URL=wss://broker-production-0bfd.up.railway.app

# first machine: create the room by simply using it
PI_MQTT_ROOM=dev PI_MQTT_ROOM_PASSWORD=... \
  pi --pi-mqtt-role orchestrator

# another machine joins with the same room name + password
PI_MQTT_ROOM=dev PI_MQTT_ROOM_PASSWORD=... \
  scripts/dev.sh worker -u "$PI_MQTT_URL" -i worker-1
```

Or put it in `~/.pi/agent/pi-mqtt.json`:

```json
{ "role": "worker", "room": "dev", "roomPassword": "...",
  "mqtt": { "url": "wss://broker-production-0bfd.up.railway.app" } }
```

**What the shared password does and does not do.** It gates the broker: without it
you cannot connect at all. It does *not* separate members — every session uses the
same MQTT user, so any member can read every task and result, publish as any worker
id, and dispatch tasks. Two checks remain in the payload: a worker ignores tasks not
addressed to its id, and the orchestrator ignores results whose `workerId` differs
from the worker the task was pinned to. Uniqueness is enforced by the broker through
the derived client id (`<room>-<worker-id>`): a second session claiming an id evicts
the first, which then stops.

Rooms with *different* passwords need either their own broker, or per-device
credentials as in the sections below.

## Two ways to reach it

| Wiring | Broker listener | Client URL |
|---|---|---|
| HTTP domain (WebSocket) | `9001` protocol websockets | `wss://host/mqtt` (port 443) |
| TCP proxy (raw MQTT) | `1883` plain, or `8883` with certs | `mqtts://host:port` |

The WebSocket route is the sturdier one: it rides the same infrastructure as web
traffic, survives proxy behaviour that breaks long-lived raw TCP connections, and
needs no TCP proxy. Add the public HTTP domain to the service, point it at
container port `9001`, and clients connect with `wss://`.

## Railway specifics that bite

- **No `VOLUME` in the Dockerfile.** Railway rejects it outright
  (`docker VOLUME at Line 10 is not supported, use Railway Volumes`). Use
  `railway volume add -m /mosquitto/data` instead; the build fails otherwise.
- The image runs mosquitto as the `mosquitto` user; the entrypoint chowns config
  and data before exec, and `mosquitto.conf` sets `user mosquitto`.
- The entrypoint rewrites `passwd` from `PI_MQTT_USERS` on every boot, so editing
  the variable and redeploying revokes removed users immediately.
- It refuses to start when `PI_MQTT_USERS` yields no users, rather than serving an
  open broker.

## TLS: verify this on your plan

- **Proxy/edge terminates TLS** → clients use `wss://` (or `mqtts://`) and
  mosquitto keeps plain listeners. This is what is deployed today.
- **Plain pass-through** → enable the `8883`/`9443` listeners with certs mounted at
  `/mosquitto/certs`.

Do not expose a plaintext listener without auth: a leaked password is remote
control of sessions that have `bash`.
