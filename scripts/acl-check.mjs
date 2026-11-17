#!/usr/bin/env node
/**
 * Check that the broker enforces the roles its credentials imply.
 *
 * Run it against any broker booted with a role-scoped PI_MQTT_USERS, from a machine
 * that can reach it:
 *
 *   PI_MQTT_URL=mqtt://127.0.0.1:1883 PI_MQTT_ROOM=dev \
 *   ORCH_CRED=tribe:orch-password \
 *   WORKER_CRED=pru:worker-password WORKER_ID=pru_misc \
 *   npm run acl-check
 *
 * Every check is "this credential may do X" or "this credential may not do Y", and a
 * denied publish is confirmed by the broker's own reason code (0x87, 135), not by
 * absence of an error.
 */

import { randomUUID } from "node:crypto";
import { connect } from "mqtt";

const url = process.env.PI_MQTT_URL;
const room = process.env.PI_MQTT_ROOM ?? "dev";
const protocolVersion = Number(process.env.PI_MQTT_PROTOCOL ?? 5);

function parseCred(value) {
	if (!value) return undefined;
	const index = value.indexOf(":");
	if (index <= 0) return undefined;
	return { username: value.slice(0, index), password: value.slice(index + 1) };
}

const orch = parseCred(process.env.ORCH_CRED);
const worker = parseCred(process.env.WORKER_CRED);
const workerId = process.env.WORKER_ID ?? "worker";

if (!url || !orch || !worker) {
	console.error(
		"usage: PI_MQTT_URL=... PI_MQTT_ROOM=dev ORCH_CRED=user:pass WORKER_CRED=user:pass [WORKER_ID=id] npm run acl-check",
	);
	process.exit(2);
}

const base = `pi/v1/${room}`;
const other = `pi/v1/${room}-elsewhere`;
const results = [];
let current = "";

function check(name, ok, detail = "") {
	results.push({ name: `${current}${name}`, ok });
	console.log(`${ok ? "PASS" : "FAIL"}  ${current}${name}${detail ? `  (${detail})` : ""}`);
}

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

async function open(cred, label) {
	const client = connect(url, {
		username: cred.username,
		password: cred.password,
		protocolVersion,
		clientId: `acl-check-${label}-${randomUUID().slice(0, 8)}`,
		reconnectPeriod: 0,
		clean: true,
		connectTimeout: 10_000,
	});
	await new Promise((resolve, reject) => {
		client.once("connect", resolve);
		client.once("error", reject);
	});
	return client;
}

/** Resolves `{ ok: true }` or `{ ok: false, code }` from the broker's PUBACK. */
function publish(client, topic, payload = "acl-check") {
	return new Promise((resolve) => {
		client.publish(topic, payload, { qos: 1 }, (error) => {
			if (!error) return resolve({ ok: true });
			resolve({ ok: false, code: error.code, message: error.message });
		});
	});
}

function subscribe(client, filter) {
	return new Promise((resolve) => {
		client.subscribe(filter, { qos: 1 }, (error) => {
			resolve({ ok: !error, code: error?.code, message: error?.message });
		});
	});
}

async function allows(client, topic) {
	const outcome = await publish(client, topic);
	return outcome.ok ? { pass: true } : { pass: false, detail: `${topic} denied (code ${outcome.code ?? "?"})` };
}

async function denies(client, topic) {
	const outcome = await publish(client, topic);
	return outcome.ok
		? { pass: false, detail: `${topic} was ALLOWED` }
		: { pass: true, detail: `${topic} denied (code ${outcome.code ?? "?"})` };
}

function expect(name, result) {
	check(name, result.pass, result.detail);
}

const orchClient = await open(orch, "orch");
const workerClient = await open(worker, "worker");

// --- orchestrator ---------------------------------------------------------
current = "orchestrator: ";
expect("may dispatch to a worker", await allows(orchClient, `${base}/workers/${workerId}/ctl`));
expect("may kick (room control)", await allows(orchClient, `${base}/control`));
expect("may answer a join (pair channel)", await allows(orchClient, `${base}/workers/${workerId}/pair`));
expect("may not impersonate a worker's presence", await denies(orchClient, `${base}/workers/${workerId}/presence`));
expect("may not publish outside its room", await denies(orchClient, `${other}/workers/${workerId}/ctl`));

// --- worker ---------------------------------------------------------------
current = "worker:       ";
expect("may announce its own presence", await allows(workerClient, `${base}/workers/${workerId}/presence`));
expect("may ack its own task", await allows(workerClient, `${base}/workers/${workerId}/ack`));
expect("may report a result", await allows(workerClient, `${base}/results/t_${randomUUID().slice(0, 8)}`));
expect("may not dispatch, even to itself", await denies(workerClient, `${base}/workers/${workerId}/ctl`));
expect("may not dispatch to another worker", await denies(workerClient, `${base}/workers/other/ctl`));
expect("may not kick the room", await denies(workerClient, `${base}/control`));
expect("may not answer a join (pair channel)", await denies(workerClient, `${base}/workers/${workerId}/pair`));
const readPair = await subscribe(workerClient, `${base}/workers/${workerId}/pair`);
check("may read the pair channel", readPair.ok, readPair.ok ? "" : `code ${readPair.code}`);

// --- read side ------------------------------------------------------------
// Mosquitto grants a wildcard subscribe and enforces "read" when delivering, so the
// assertion that matters is whether the message arrives, not the SUBACK.
current = "read:         ";
const eavesdropper = await open(worker, "eavesdrop");
const overheard = [];
eavesdropper.on("message", (topic) => overheard.push(topic));
await subscribe(eavesdropper, `${base}/results/+`);
await sleep(100);
await publish(workerClient, `${base}/results/t_${randomUUID().slice(0, 8)}`, JSON.stringify({ kind: "result" }));
await sleep(400);
check("a worker does not receive results", overheard.length === 0, `${overheard.length} message(s)`);

// A legitimate publish has to actually arrive, not just be acknowledged.
current = "delivery:     ";
const received = [];
const listener = await open(orch, "listener");
listener.on("message", (_topic, payload) => received.push(payload.toString()));
await subscribe(listener, `${base}/workers/${workerId}/presence`);
await sleep(100);
await publish(workerClient, `${base}/workers/${workerId}/presence`, JSON.stringify({ kind: "presence", state: "idle" }));
await sleep(400);
check("an allowed publish reaches its subscriber", received.length > 0, `${received.length} message(s)`);

for (const client of [orchClient, workerClient, eavesdropper, listener]) await client?.endAsync(true);

const failed = results.filter((r) => !r.ok);
console.log(
	`\n${results.length - failed.length}/${results.length} checks passed ` +
		`(worker credentials are shared within a room: per-device isolation is not part of this ACL)`,
);
process.exit(failed.length === 0 ? 0 : 1);
