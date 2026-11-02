# Deploying the broker (Railway)

Stock mosquitto with per-role ACLs. Only the auth source is dynamic.

**Deployed:** project `hello-world` (workspace *Tribe Internal Apps*), service
`broker`, environment `production`.

| Fact | Value |
|---|---|
| Public endpoint | `wss://broker-production-0bfd.up.railway.app` (port 443, path `/mqtt`) |
| Container listeners | `1883` mqtt, `9001` websockets |
| Volume | `broker-volume` mounted at `/mosquitto/data` |
| Env | `PI_MQTT_USERS=orch:<pw>,worker-1:<pw>,...` |

Redeploy after changing anything here:

```bash
cd deploy
railway up -d -y          # builds the Dockerfile and rolls the service
railway logs --lines 40   # entrypoint prints "configured user: ..." per user
```

## Client wiring

The MQTT **username must equal the worker id**: the ACL file grants access with
`%u` in the topic path, so `worker-1` may only touch `workers/worker-1/...` and
results. The orchestrator uses the `orch` user.

```bash
export PI_MQTT_URL=wss://broker-production-0bfd.up.railway.app
export PI_MQTT_CLUSTER=dev

# orchestrator
PI_MQTT_USERNAME=orch PI_MQTT_PASSWORD=... pi --extension ./extensions/mqtt.ts --pi-mqtt-role orchestrator

# worker
PI_MQTT_USERNAME=worker-1 PI_MQTT_PASSWORD=... \
  scripts/dev.sh worker -u "$PI_MQTT_URL" -i worker-1
```

Credentials live in `~/.pi/agent/pi-mqtt.json` via `mqtt.caFile`/env, or just
exported per shell. They are **not** in this repo.

Verify the access control after any change to `aclfile`:

```bash
PI_MQTT_URL=wss://broker-production-0bfd.up.railway.app \
PW_ORCH=... PW_WORKER_1=... PW_WORKER_2=... node scripts/acl-check.mjs
```

That checks both directions: a worker cannot publish to another worker's control
topic, cannot read another worker's presence, and cannot read results — while the
orchestrator can. Read checks publish and look for delivery, because mosquitto can
grant a SUBACK and still filter the messages.

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
