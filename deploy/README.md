# Deploying the broker (Railway)

Stock mosquitto with **role-scoped credentials**: one user that may orchestrate, and one
that may only be orchestrated. The broker enforces the difference — a worker credential
cannot dispatch a task, kick the room, or read results.

**Deployed:** project `hello-world` (workspace *Tribe Internal Apps*), service
`broker`, environment `production`.

| Fact | Value |
|---|---|
| Public endpoint | `wss://broker-production-0bfd.up.railway.app` (port 443, path `/mqtt`) |
| Container listeners | `1883` mqtt, `9001` websockets |
| Volume | `broker-volume` mounted at `/mosquitto/data` |
| Env | `PI_MQTT_USERS=tribe:<orchestrator-password>:orchestrator:dev, pru:<worker-password>:worker:dev` |

Redeploy after changing anything here:

```bash
cd deploy
railway up -d -y          # builds the Dockerfile and rolls the service
railway logs --lines 40   # entrypoint prints "configured user: ..."
```

## Credentials

`PI_MQTT_USERS` is a comma-separated list; each entry is one credential:

| Entry | May |
|---|---|
| `name:password` | everything below — **legacy**, reported at boot |
| `name:password:orchestrator:room` | dispatch, kick, read presence/acks/results, write its own presence |
| `name:password:worker:room` | be dispatched to, ack, write its own presence, write results, read kick messages |

Passwords cannot contain `:` or `,`. The entrypoint generates both `passwd` and `acl`
from that list on every boot, so removing an entry revokes access on redeploy. The
generated ACL is the authority; do not hand-edit it in the image.

What the broker now separates:

| | orchestrator credential | worker credential |
|---|---|---|
| write `workers/<id>/ctl` (dispatch) | yes | **no** |
| write `workers/<id>/pair` (admission) | yes | **no** |
| read `workers/<id>/pair` | no | yes |
| write `control` (kick) | yes | **no** |
| read `results/+` | yes | **no** (subscribe is granted, delivery is filtered) |
| read `workers/+/presence`, `workers/+/ack` | yes | no |
| write `workers/<id>/presence`/`ack`, `results/+` | no | yes |

Room names are ACL-scoped: a credential granted for room `dev` cannot read or write
`pi/v1/prod/#`.

What it does **not** separate: a worker credential is shared by the room's workers, so
the broker cannot tell them apart. Any worker can read every worker's control topic
(then ignore tasks not addressed to it), and can overwrite another worker's presence
entry. Per-device credentials need a topic layout where the worker name is its own
level (`workers/<name>/<session>/...`): MQTT filters cannot match a prefix inside a
level, so `workers/pru_+/*` is not a filter and mosquitto rejects it. That is a
protocol change, not an ACL one.

## Client wiring

Each machine carries the credential for its role. `role` in the config still decides
which code runs, but the broker has the final say: a worker credential asking to join
as an orchestrator has its presence refused (`0x87 not authorized`) and the session
reports that instead of pretending to be connected.

```json
{
  "role": "worker",
  "name": "pru",
  "room": { "name": "dev" },
  "mqtt": {
    "url": "wss://broker-production-0bfd.up.railway.app",
    "username": "pru",
    "password": "<worker-password>"
  }
}
```

and the orchestrator's, with `"role": "orchestrator"`, `"username": "tribe"`, and the
orchestrator password. `roomPassword` still works and implies the shared `cluster`
user, which is exactly the legacy grant — leave it out when the credentials are
role-scoped.

**Rotating** is per credential: edit `PI_MQTT_USERS` and redeploy. A machine still
holding an old password is refused with `Connection Refused: not authorised`, which
the extension reports; rotating the worker password does not disturb orchestrators, and
vice versa.

## Checking it

`npm run acl-check` asserts the table above against a running broker, using the
broker's own reason codes rather than absence of errors:

```bash
PI_MQTT_URL=wss://broker-production-0bfd.up.railway.app PI_MQTT_ROOM=dev \
ORCH_CRED=tribe:<orchestrator-password> \
WORKER_CRED=pru:<worker-password> WORKER_ID=pru_misc \
npm run acl-check
```

It is the check that a deployment change did not quietly widen a role.

## Two ways to reach it

| Wiring | Broker listener | Client URL |
|---|---|---|
| HTTP domain (WebSocket) | `9001` protocol websockets | `wss://host/mqtt` (port 443) |
| TCP proxy (raw MQTT) | `1883` plain, or `8883` with certs | `mqtts://host:port` |

The WebSocket route is the sturdier one: it rides the same infrastructure as web
traffic, survives proxy behaviour that breaks long-lived raw TCP connections, and needs
no TCP proxy. Add the public HTTP domain to the service, point it at container port
`9001`, and clients connect with `wss://`.

## Railway specifics that bite

- **No `VOLUME` in the Dockerfile.** Railway rejects it outright
  (`docker VOLUME at Line 10 is not supported, use Railway Volumes`). Use
  `railway volume add -m /mosquitto/data` instead; the build fails otherwise.
- The image runs mosquitto as the `mosquitto` user; the entrypoint chowns config
  and data before exec, and `mosquitto.conf` sets `user mosquitto`.
- The entrypoint rewrites `passwd` **and** the role `acl` from `PI_MQTT_USERS` on every
  boot, so editing the variable and redeploying revokes removed users immediately.
- It refuses to start when `PI_MQTT_USERS` yields no users, rather than serving an
  open broker.

## TLS: verify this on your plan

- **Proxy/edge terminates TLS** → clients use `wss://` (or `mqtts://`) and
  mosquitto keeps plain listeners. This is what is deployed today.
- **Plain pass-through** → enable the `8883`/`9443` listeners with certs mounted at
  `/mosquitto/certs`.

Do not expose a plaintext listener without auth: a leaked password is remote
control of sessions that have `bash`.
