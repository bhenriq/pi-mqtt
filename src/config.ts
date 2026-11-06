/**
 * Configuration resolution.
 *
 * Order: CLI flags > environment > config file > defaults.
 * Credentials come from the environment only, so they never land in a JSON file
 * that might get committed.
 */

import { chmodSync, existsSync, mkdirSync, readFileSync, renameSync, writeFileSync } from "node:fs";
import { homedir, hostname } from "node:os";
import { dirname, isAbsolute, join, resolve } from "node:path";
import { isValidIdentifier, type Role } from "./protocol.ts";

export interface MqttConfig {
	url: string;
	/** Shared cluster secret. When set, it is used as the password with one shared username. */
	token?: string;
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
	/** "manual" connects only when asked; "auto" connects at session start. */
	connect: "manual" | "auto";
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
	connect?: string;
	/** Room name and its shared password: the vocabulary people actually use. */
	room?: string;
	roomPassword?: string;
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

export interface ConfigPathInfo {
	path: string;
	origin: "project" | "agent" | "explicit";
	exists: boolean;
}

/** Where a write would land, and how that location was chosen. */
export function resolveConfigPath(options: { explicit?: string; cwd: string }): ConfigPathInfo {
	if (options.explicit) {
		const path = isAbsolute(options.explicit) ? options.explicit : resolve(options.cwd, options.explicit);
		return { path, origin: "explicit", exists: existsSync(path) };
	}
	const project = resolve(options.cwd, "pi-mqtt.json");
	if (existsSync(project)) return { path: project, origin: "project", exists: true };
	const agent = join(homedir(), ".pi", "agent", "pi-mqtt.json");
	return { path: agent, origin: "agent", exists: existsSync(agent) };
}

export type RawConfig = Record<string, unknown>;

export function readConfigObject(path: string): RawConfig {
	if (!existsSync(path)) return {};
	try {
		const parsed: unknown = JSON.parse(readFileSync(path, "utf8"));
		if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) return {};
		return parsed as RawConfig;
	} catch {
		return {};
	}
}

/** Atomic write with owner-only permissions: a half-written or world-readable config is a bug. */
export function writeConfigObject(path: string, value: RawConfig): void {
	mkdirSync(dirname(path), { recursive: true });
	const temporary = `${path}.tmp.${process.pid}`;
	writeFileSync(temporary, `${JSON.stringify(value, null, 2)}\n`, { mode: 0o600 });
	renameSync(temporary, path);
	try {
		chmodSync(path, 0o600);
	} catch {
		// Windows: mode bits are advisory.
	}
}

/** Which environment variable, if any, outranks the file for this field. */
export function envOverrideFor(field: string): string | undefined {
	const map: Record<string, string> = {
		role: "PI_MQTT_ROLE",
		connect: "PI_MQTT_CONNECT",
		url: "PI_MQTT_URL",
		cluster: "PI_MQTT_CLUSTER",
		room: "PI_MQTT_ROOM",
		roomPassword: "PI_MQTT_ROOM_PASSWORD",
		username: "PI_MQTT_USERNAME",
		password: "PI_MQTT_PASSWORD",
		token: "PI_MQTT_TOKEN",
		workerId: "PI_MQTT_WORKER_ID",
		caFile: "PI_MQTT_CA_FILE",
	};
	const name = map[field];
	if (!name) return undefined;
	const value = process.env[name];
	return value && value.trim().length > 0 ? name : undefined;
}

export interface LoadOptions {
	role?: string;
	workerId?: string;
	configPath?: string;
	cwd: string;
}

/**
 * Trim values that come from a file or the environment. A trailing newline in a
 * password is invisible in the error it causes ("Not authorized"), and the same
 * in a URL produces a hostname that cannot resolve.
 */
function tidy(value: string | undefined): string | undefined {
	if (typeof value !== "string") return undefined;
	const trimmed = value.trim();
	return trimmed.length > 0 ? trimmed : undefined;
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
	const url = tidy(env.PI_MQTT_URL) ?? tidy(file?.mqtt?.url);
	if (!url) {
		return { ok: false, reason: "no broker url (set PI_MQTT_URL or mqtt.url in pi-mqtt.json)" };
	}

	const urlError = validateBrokerUrl(url);
	if (urlError) return { ok: false, reason: urlError };

	// A room is just a named namespace plus one shared password. `cluster`/`token`
	// remain accepted so machines configured before the rename keep working.
	const cluster =
		tidy(env.PI_MQTT_ROOM) ?? tidy(file?.room) ?? tidy(env.PI_MQTT_CLUSTER) ?? tidy(file?.mqtt?.cluster) ?? "dev";
	if (!isValidIdentifier(cluster)) {
		return {
			ok: false,
			reason: `invalid cluster id ${JSON.stringify(cluster)}: use letters, digits, _ or -, starting with a letter or digit`,
		};
	}

	// On a worker the MQTT username *is* the worker id: the broker's ACLs key on it.
	// Falling back to it prevents the classic mismatch where a session announces one
	// id and authenticates as another, and then silently cannot read its own tasks.
	// A single shared secret replaces per-machine passwords: one username
	// ("cluster" unless configured), one token. Declared ids stay a protocol concern.
	const token =
		tidy(env.PI_MQTT_ROOM_PASSWORD) ??
		tidy(file?.roomPassword) ??
		tidy(env.PI_MQTT_TOKEN) ??
		tidy(file?.mqtt?.token);
	const configuredUsername = tidy(env.PI_MQTT_USERNAME) ?? tidy(file?.mqtt?.username);
	const username = token ? (configuredUsername ?? "cluster") : configuredUsername;
	const workerId = [
		tidy(options.workerId),
		tidy(env.PI_MQTT_WORKER_ID),
		tidy(file?.worker?.workerId),
		role === "worker" ? configuredUsername : undefined,
	].find((candidate) => candidate !== undefined) ?? defaultWorkerId();
	if (!isValidIdentifier(workerId)) {
		return {
			ok: false,
			reason: `invalid worker id ${JSON.stringify(workerId)}: use letters, digits, _ or -, starting with a letter or digit`,
		};
	}

	const rawConnect = env.PI_MQTT_CONNECT ?? file?.connect ?? "manual";
	if (rawConnect !== "manual" && rawConnect !== "auto") {
		return { ok: false, reason: `connect must be "manual" or "auto", got ${JSON.stringify(rawConnect)}` };
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
				token,
				password: token ?? tidy(env.PI_MQTT_PASSWORD) ?? tidy(file?.mqtt?.password),
				cluster,
				protocolVersion: rawVersion,
				keepaliveSec: file?.mqtt?.keepaliveSec ?? 15,
				reconnectMs: file?.mqtt?.reconnectMs ?? 2000,
				// Env overrides exist so a launcher can point at a TLS broker without
				// writing a config file.
				caFile: tidy(env.PI_MQTT_CA_FILE) ?? tidy(file?.mqtt?.caFile),
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
			logDir: tidy(file?.logDir) ?? join(homedir(), ".pi", "agent", "pi-mqtt"),
			connect: rawConnect,
		},
	};
}
