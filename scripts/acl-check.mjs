#!/usr/bin/env node
/**
 * Verify the deployed broker's access control: prove that a worker cannot publish
 * tasks, read other workers' traffic, or read results, while the orchestrator can.
 *
 *   PI_MQTT_URL=wss://host PW_ORCH=... PW_WORKER_1=... node scripts/acl-check.mjs
 *
 * Credentials come from the environment: the MQTT username is the worker id, so
 * PW_<USERNAME> (upper-cased, - becomes _) supplies each password.
 */

import { connect } from "mqtt";

const url = process.env.PI_MQTT_URL;
if (!url) {
	console.error("set PI_MQTT_URL");
	process.exit(2);
}

const cluster = process.env.PI_MQTT_CLUSTER ?? "dev";
const base = `pi/v1/${cluster}`;
const otherWorker = process.env.OTHER_WORKER ?? "worker-2";

/** Whoever is allowed to write the topic under test: orch writes ctl, workers write results. */
function publisherFor(topic) {
	return topic.includes("/results/") || topic.endsWith("/results/+") ? "worker-2" : "orch";
}

function passwordFor(user) {
	const key = `PW_${user.toUpperCase().replace(/[^A-Z0-9]/g, "_")}`;
	const value = process.env[key];
	if (!value) throw new Error(`missing ${key}`);
	return value;
}

function attempt(label, user, action) {
	return new Promise((resolve) => {
		const client = connect(url, {
			protocolVersion: 5,
			username: user,
			password: passwordFor(user),
			rejectUnauthorized: true,
			clientId: `acl-${user}-${Math.random().toString(36).slice(2, 8)}`,
			connectTimeout: 10_000,
			reconnectPeriod: 0,
		});
		let settled = false;
		const finish = (verdict) => {
			if (settled) return;
			settled = true;
			clearTimeout(timer);
			client.end(true);
			resolve({ label, verdict });
		};
		const timer = setTimeout(() => finish("TIMEOUT"), 15_000);
		client.on("error", (error) => finish(`connect error: ${error.message}`));
		client.on("connect", () => action(client, finish));
	});
}

/**
 * Subscription permission is not the same as delivery permission: mosquitto can
 * grant a SUBACK and still filter messages. So read checks subscribe, publish
 * from a client that is allowed to, and look at whether anything arrives.
 */
async function readTest(label, user, topic, publisherUser, payload) {
	const reader = connect(url, {
		protocolVersion: 5,
		username: user,
		password: passwordFor(user),
		rejectUnauthorized: true,
		clientId: `acl-read-${Math.random().toString(36).slice(2, 8)}`,
		reconnectPeriod: 0,
	});
	const publisher = connect(url, {
		protocolVersion: 5,
		username: publisherUser,
		password: passwordFor(publisherUser),
		rejectUnauthorized: true,
		clientId: `acl-pub-${Math.random().toString(36).slice(2, 8)}`,
		reconnectPeriod: 0,
	});
	const verdict = await new Promise((resolve) => {
		let received = false;
		const timer = setTimeout(() => resolve(received ? "allowed" : "DENIED"), 4000);
		reader.on("message", () => {
			received = true;
			clearTimeout(timer);
			resolve("allowed");
		});
		reader.on("connect", () => {
			reader.subscribe(topic, { qos: 1 }, () => {
				publisher.on("connect", () => {
					publisher.publish(topic, payload, { qos: 1 }, () => {});
				});
			});
		});
		reader.on("error", () => {});
		publisher.on("error", () => {});
	});
	reader.end(true);
	publisher.end(true);
	return { label, verdict };
}

const checks = [
	["orch publish a worker's ctl", "orch", (c, done) =>
		c.publish(`${base}/workers/${otherWorker}/ctl`, "{}", { qos: 1 }, (e) => done(e ? "DENIED" : "allowed"))],
	["orch subscribe results", "orch", (c, done) =>
		c.subscribe(`${base}/results/+`, { qos: 1 }, (e, granted) => done(e ? "DENIED" : granted?.[0]?.qos === 128 ? "DENIED" : "allowed"))],
	["worker publish its own presence", "worker-1", (c, done) =>
		c.publish(`${base}/workers/worker-1/presence`, "{}", { qos: 1 }, (e) => done(e ? "DENIED" : "allowed"))],
	["worker publish its own result", "worker-1", (c, done) =>
		c.publish(`${base}/results/t_aclcheck`, "{}", { qos: 1 }, (e) => done(e ? "DENIED" : "allowed"))],
	["worker publish another worker's ctl", "worker-1", (c, done) =>
		c.publish(`${base}/workers/${otherWorker}/ctl`, "{}", { qos: 1 }, (e) => done(e ? "DENIED" : "allowed"))],
];

const readChecks = [
	["worker read another worker's presence", "worker-1", `${base}/workers/${otherWorker}/presence`],
	["worker read results", "worker-1", `${base}/results/+`],
	["worker read its own ctl", "worker-1", `${base}/workers/worker-1/ctl`],
];

const expected = new Map([
	["worker read its own ctl", "allowed"],
	["orch publish a worker's ctl", "allowed"],
	["orch subscribe results", "allowed"],
	["worker publish its own presence", "allowed"],
	["worker publish its own result", "allowed"],
	["worker publish another worker's ctl", "DENIED"],
	["worker read another worker's presence", "DENIED"],
	["worker read results", "DENIED"],
]);

const results = [];
for (const [label, user, action] of checks) {
	results.push(await attempt(label, user, action));
}
for (const [label, user, topic] of readChecks) {
	results.push(await readTest(label, user, topic, publisherFor(topic), JSON.stringify({ v: 1, kind: "presence", workerId: otherWorker, state: "idle", updatedAt: Date.now(), host: "acl", cwd: "/", version: "acl" })));
}

let failures = 0;
for (const { label, verdict } of results) {
	const want = expected.get(label);
	const ok = verdict === want;
	if (!ok) failures += 1;
	console.log(`${ok ? "ok  " : "FAIL"}  ${label}: ${verdict}${ok ? "" : ` (expected ${want})`}`);
}

console.log(failures === 0 ? "\nall access-control expectations hold" : `\n${failures} check(s) failed`);
process.exit(failures === 0 ? 0 : 1);
