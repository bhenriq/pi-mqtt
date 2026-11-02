#!/usr/bin/env node
/**
 * Probe the pi-mqtt topic space: prints every presence, ack, and result message.
 *
 *   node scripts/probe.mjs                 # local dev broker, cluster "dev"
 *   PI_MQTT_URL=mqtts://host:8883 PI_MQTT_USERNAME=.. node scripts/probe.mjs
 */

import { connect } from "mqtt";

const url = process.env.PI_MQTT_URL ?? "mqtt://127.0.0.1:1883";
const cluster = process.env.PI_MQTT_CLUSTER ?? "dev";
const base = `pi/v1/${cluster}`;

const client = connect(url, {
	protocolVersion: Number(process.env.PI_MQTT_PROTOCOL ?? 5),
	username: process.env.PI_MQTT_USERNAME,
	password: process.env.PI_MQTT_PASSWORD,
	rejectUnauthorized: process.env.PI_MQTT_INSECURE !== "1",
	clientId: `probe-${Math.random().toString(36).slice(2, 8)}`,
});

client.on("connect", () => {
	console.log(`connected ${url} cluster=${cluster}`);
	for (const filter of [`${base}/workers/+/presence`, `${base}/workers/+/ack`, `${base}/results/+`]) {
		client.subscribe(filter, { qos: 1 }, (error) => {
			console.log(error ? `subscribe ${filter}: ${error.message}` : `subscribed ${filter}`);
		});
	}
});

client.on("message", (topic, payload) => {
	const stamp = new Date().toISOString().slice(11, 23);
	let parsed;
	try {
		parsed = JSON.parse(payload.toString());
	} catch {
		parsed = payload.toString();
	}
	const line =
		typeof parsed === "object" && parsed !== null
			? `${parsed.kind} ${pretty(parsed)}`
			: String(parsed);
	console.log(`${stamp} ${topic}\n    ${line}`);
});

client.on("error", (error) => console.error("error:", error.message));
client.on("close", () => console.log("connection closed"));

function pretty(value) {
	const { v, kind, updatedAt, finishedAt, ...rest } = value;
	const fields = Object.entries(rest)
		.map(([key, item]) => `${key}=${typeof item === "string" ? truncate(item) : JSON.stringify(item)}`)
		.join(" ");
	return fields;
}

function truncate(text) {
	const flat = text.replace(/\s+/g, " ");
	return flat.length > 60 ? `${flat.slice(0, 60)}…` : flat;
}

process.on("SIGINT", () => {
	client.end(true, () => process.exit(0));
});
