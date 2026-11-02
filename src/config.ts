/**
 * Configuration resolution.
 *
 * Order: CLI flags > environment > config file > defaults.
 * Credentials come from the environment only, so they never land in a JSON file
 * that might get committed.
 */

import { readFileSync } from "node:fs";
import { homedir, hostname } from "node:os";
import { isAbsolute, join, resolve } from "node:path";
import { isValidIdentifier, type Role } from "./protocol.ts";

export interface MqttConfig {
	url: string;
	username?: string;
	password?: string;
	cluster: string;
	protocolVersion: 4 | 5;
	keepaliveSec: number;
	reconnectMs: number;
	/** PEM path; when set, the client trusts only this CA. */
	caFile?: string;
	rejectUnauthorized: boolean;
}

export interface WorkerConfig {
	workerId: string;
	/** Allowlisted repo name -> local path. Never taken from the broker. */
	repos: Record<string, string>;
	defaultRepo?: string;
	allowModelOverride: boolean;
	heartbeatMs: number;
	/** Message expiry on dispatched tasks, in seconds. */
	taskExpirySec: number;
}

export interface AppConfig {
	role: Role;
	mqtt: MqttConfig;
	worker: WorkerConfig;
	logDir: string;
}

export type LoadResult = { ok: true; config: AppConfig } | { ok: false; reason: string };

const TRANSPORTS = new Set(["mqtt:", "mqtts:", "ws:", "wss:", "tcp:", "ssl:"]);

/** Reject a URL the client cannot use, before it produces a confusing socket error. */
function validateBrokerUrl(raw: string): string | undefined {
	let parsed: URL;
	try {
		parsed = new URL(raw);
	} catch {
		return `invalid broker url ${JSON.stringify(raw)}`;
	}
	if (!TRANSPORTS.has(parsed.protocol)) {
		return `unsupported broker url scheme ${parsed.protocol} in ${JSON.stringify(raw)}: use mqtt://, mqtts://, ws:// or wss://`;
	}
	if ((parsed.protocol === "ws:" || parsed.protocol === "wss:") && parsed.pathname === "/") {
		// mqtt.js then uses /mqtt, which mosquitto serves. Not an error.
	}
	return undefined;
}

interface FileShape {
	/** Role can live in the file, so a fresh install needs no flags. */
	role?: string;
	mqtt?: Partial<MqttConfig>;
	worker?: Partial<WorkerConfig>;
	logDir?: string;
}

function readConfigFile(path: string): FileShape | undefined {
	let raw: string;
	try {
		raw = readFileSync(path, "utf8");
	} catch {
		return undefined;
	}
	try {
		const parsed: unknown = JSON.parse(raw);
		if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
			throw new Error("config must be a JSON object");
		}
		return parsed as FileShape;
	} catch (error) {
		throw new Error(`pi-mqtt config at ${path} is invalid: ${(error as Error).message}`);
	}
}

function pickConfigPath(explicit: string | undefined, cwd: string): string | undefined {
	if (explicit) return isAbsolute(explicit) ? explicit : resolve(cwd, explicit);
	const candidates = [resolve(cwd, "pi-mqtt.json"), join(homedir(), ".pi", "agent", "pi-mqtt.json")];
	return candidates.find((path) => readConfigFile(path) !== undefined);
}

function defaultWorkerId(): string {
	const cleaned = hostname()
		.toLowerCase()
		.replace(/[^a-z0-9_-]+/g, "-")
		.replace(/^-+|-+$/g, "")
		.slice(0, 48);
	return cleaned || "worker";
}

export interface LoadOptions {
	role?: string;
	workerId?: string;
	configPath?: string;
	cwd: string;
}

function normalizeRole(value: unknown): Role | undefined {
	return value === "orchestrator" || value === "worker" ? value : undefined;
}

export function loadConfig(options: LoadOptions): LoadResult {
	let file: FileShape | undefined;
	try {
		const path = pickConfigPath(options.configPath, options.cwd);
		file = path ? readConfigFile(path) : undefined;
	} catch (error) {
		return { ok: false, reason: (error as Error).message };
	}

	const env = process.env;
	// Precedence: CLI flag > environment > pi-mqtt.json.
	const role = normalizeRole(options.role) ?? normalizeRole(env.PI_MQTT_ROLE) ?? normalizeRole(file?.role);
	if (!role) {
		return {
			ok: false,
			reason: 'no role selected (pass --pi-mqtt-role orchestrator|worker, set PI_MQTT_ROLE, or add "role" to pi-mqtt.json)',
		};
	}
	const url = env.PI_MQTT_URL ?? file?.mqtt?.url;
	if (!url) {
		return { ok: false, reason: "no broker url (set PI_MQTT_URL or mqtt.url in pi-mqtt.json)" };
	}

	const urlError = validateBrokerUrl(url);
	if (urlError) return { ok: false, reason: urlError };

	const cluster = env.PI_MQTT_CLUSTER ?? file?.mqtt?.cluster ?? "dev";
	if (!isValidIdentifier(cluster)) {
		return {
			ok: false,
			reason: `invalid cluster id ${JSON.stringify(cluster)}: use letters, digits, _ or -, starting with a letter or digit`,
		};
	}

	// On a worker the MQTT username *is* the worker id: the broker's ACLs key on it.
	// Falling back to it prevents the classic mismatch where a session announces one
	// id and authenticates as another, and then silently cannot read its own tasks.
	const username = env.PI_MQTT_USERNAME ?? file?.mqtt?.username;
	const workerId =
		options.workerId ??
		env.PI_MQTT_WORKER_ID ??
		file?.worker?.workerId ??
		(role === "worker" ? username : undefined) ??
		defaultWorkerId();
	if (!isValidIdentifier(workerId)) {
		return {
			ok: false,
			reason: `invalid worker id ${JSON.stringify(workerId)}: use letters, digits, _ or -, starting with a letter or digit`,
		};
	}

	const rawVersion = file?.mqtt?.protocolVersion ?? 5;
	if (rawVersion !== 4 && rawVersion !== 5) {
		return { ok: false, reason: `mqtt.protocolVersion must be 4 or 5, got ${String(rawVersion)}` };
	}

	return {
		ok: true,
		config: {
			role,
			mqtt: {
				url,
				// Environment wins; the file is a fallback for a machine you configure
				// once. Keep such a file out of git (pi-mqtt.json is gitignored here).
				username,
				password: env.PI_MQTT_PASSWORD ?? file?.mqtt?.password,
				cluster,
				protocolVersion: rawVersion,
				keepaliveSec: file?.mqtt?.keepaliveSec ?? 30,
				reconnectMs: file?.mqtt?.reconnectMs ?? 2000,
				// Env overrides exist so a launcher can point at a TLS broker without
				// writing a config file.
				caFile: env.PI_MQTT_CA_FILE ?? file?.mqtt?.caFile,
				rejectUnauthorized:
					env.PI_MQTT_REJECT_UNAUTHORIZED === "0" ? false : (file?.mqtt?.rejectUnauthorized ?? true),
			},
			worker: {
				workerId,
				repos: file?.worker?.repos ?? {},
				defaultRepo: file?.worker?.defaultRepo,
				allowModelOverride: file?.worker?.allowModelOverride ?? false,
				heartbeatMs: file?.worker?.heartbeatMs ?? 30_000,
				taskExpirySec: file?.worker?.taskExpirySec ?? 900,
			},
			logDir: file?.logDir ?? join(homedir(), ".pi", "agent", "pi-mqtt"),
		},
	};
}
