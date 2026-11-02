# Deploying the broker (Railway)

The broker is stock mosquitto with per-role ACLs. Only the auth source is dynamic.

## Railway

1. New service → Deploy from repo → root `deploy/`, or `railway up` from `deploy/`.
2. Variables:
   - `PI_MQTT_USERS` = `orch:<strong-secret>,worker-laptop-b:<other-secret>,worker-mac-mini:<third-secret>`
3. Volume mounted at `/mosquitto/data` (keeps retained presence across deploys).
4. Networking → add a **TCP proxy** to port `1883`. That public `host:port` is
   `PI_MQTT_URL`.
5. Point clients at it:

```bash
export PI_MQTT_URL=mqtts://<proxy-host>:<proxy-port>
export PI_MQTT_USERNAME=worker-laptop-b
export PI_MQTT_PASSWORD=<secret>
export PI_MQTT_CLUSTER=dev
```

## TLS: verify this on your plan

Two shapes, and which one you get decides the URL scheme:

- **Proxy terminates TLS** → clients use `mqtts://` and mosquitto keeps the plain
  1883 listener. Simplest; verify with `openssl s_client -connect host:port`.
- **Proxy is plain TCP passthrough** → enable the `8883` listener in
  `mosquitto.conf`, mount certs at `/mosquitto/certs`, and use `mqtts://`.

Do not run a publicly reachable plaintext MQTT listener with agent workers behind
it: a leaked password becomes remote control of sessions that have `bash`.

## Credential algebra

- One user per role per machine. Never share a worker's credentials between laptops.
- Rotate by editing `PI_MQTT_USERS` and redeploying: the entrypoint rewrites
  `passwd` from scratch, so removed entries lose access immediately.
- The orchestrator's user can publish to `workers/+/ctl` and read everything under
  its cluster. A worker's user can read only its own `ctl` topic and write only
  its own presence/ack/result. That asymmetry is the whole point of the ACL file.

## Local alternative

`npm run broker` runs `broker/mosquitto.dev.conf`: no auth, `127.0.0.1` only, same
protocol and topic behaviour. Use it for development; never as the deployed config.
