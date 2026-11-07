/**
 * pi-mqtt: coordinate pi sessions over MQTT.
 *
 * One orchestrator (your live session) pins tasks to named workers. This file is
 * the only entry point: it resolves the role, owns the connection lifecycle, wires
 * the role modules together, and delivers results into the transcript.
 *
 * Nothing opens a socket at load time.
 */

import { rmSync, writeFileSync } from "node:fs";
import { hostname, tmpdir } from "node:os";
import { join } from "node:path";
import { Type } from "@earendil-works/pi-ai";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import {
	loadConfig,
	readConfigObject,
	resolveConfigPath,
	writeConfigObject,
	type AppConfig,
	type RawConfig,
} from "../src/config.ts";
import { runConfigMenu, type Draft } from "../src/menu.ts";
import { readGitInfo } from "../src/git.ts";
import { MqttLink, type LogFn } from "../src/mqtt.ts";
import { Orchestrator, type TaskRecord } from "../src/orchestrator.ts";
import { Presence, buildPresence, probePresence, type PresenceIdentity } from "../src/presence.ts";
import { isKickMessage, topicsFor, type Topics } from "../src/protocol.ts";
import { Worker } from "../src/worker.ts";

const FLAG_ROLE = "pi-mqtt-role";
const FLAG_ID = "pi-mqtt-id";
const FLAG_CONFIG = "pi-mqtt-config";
const FLAG_FORCE = "pi-mqtt-force";
const FLAG_START = "pi-mqtt-start";

const TOOL_LIST_WORKERS = "list_workers";
const TOOL_DISPATCH = "dispatch_task";
const TOOL_RESULT = "get_result";
const TOOL_CANCEL = "cancel_task";
const TOOL_LIST_TASKS = "list_tasks";
const ORCHESTRATOR_TOOLS = [TOOL_LIST_WORKERS, TOOL_DISPATCH, TOOL_RESULT, TOOL_CANCEL, TOOL_LIST_TASKS];

/** Explicit so all three dispatch_task returns share one details type. */
interface DispatchDetails {
	taskId?: string;
	state?: string;
	worker?: string;
	error?: string;
}

const LOG_LIMIT = 100;
const DIGEST_COALESCE_MS = 3_000;
/** How long dispatch_task blocks waiting for the worker by default. */
const DEFAULT_WAIT_MS = 120_000;

interface LogLine {
	at: number;
	level: "info" | "warn" | "error";
	message: string;
}

interface Runtime {
	config: AppConfig;
	topics: Topics;
	link: MqttLink;
	presence?: Presence;
	orchestrator?: Orchestrator;
	worker?: Worker;
	/** Digests waiting for the coalescing window to close. */
	pending: TaskRecord[];
	timer?: NodeJS.Timeout;
}

/** Configured but possibly disconnected: connecting is an explicit act. */
interface Session {
	config: AppConfig;
	topics: Topics;
	log: LogFn;
}

let api: ExtensionAPI | undefined;
/** Worker id chosen for this session at join time (not the file's default). */
let sessionWorkerId: string | undefined;
let session: Session | undefined;
let runtime: Runtime | undefined;
let currentCtx: ExtensionContext | undefined;
let logs: LogLine[] = [];

function makeLog(ctx: ExtensionContext): LogFn {
	return (level, message) => {
		logs.push({ at: Date.now(), level, message });
		if (logs.length > LOG_LIMIT) logs = logs.slice(-LOG_LIMIT);
		if (level !== "info") ctx.ui.notify(`pi-mqtt: ${message}`, level === "warn" ? "warning" : "error");
	};
}

function describeModel(ctx: ExtensionContext): string | undefined {
	const model = ctx.model;
	if (!model) return undefined;
	return `${String(model.provider)}/${String(model.id)}`;
}

function clip(value: string, max: number): string {
	const flat = value.replace(/\s+/g, " ").trim();
	return flat.length > max ? `${flat.slice(0, max - 1)}…` : flat;
}

function describeTask(record: TaskRecord): string {
	const parts = [`task ${record.task.taskId} · ${record.task.to} · ${record.state}`];
	const result = record.result;
	if (!record.finishedAt) {
		const since = record.startedAt ?? record.sentAt ?? record.task.createdAt;
		parts.push(`still running (${Math.round((Date.now() - since) / 1000)}s); the result will arrive as a transcript entry`);
		return parts.join("\n");
	}
	if (record.error) parts.push(`error: ${record.error}`);
	if (result?.error) parts.push(`worker error: ${result.error}`);
	if (result) {
		parts.push(`duration: ${Math.round(result.durationMs / 1000)}s`);
		if (result.text) parts.push(`--- final message ---\n${result.text}`);
		if (result.repo) {
			parts.push(`repo: ${result.repo.branch ?? "-"} ${result.repo.baseSha?.slice(0, 8) ?? "?"}..${result.repo.headSha?.slice(0, 8) ?? "?"}`);
			if (result.repo.diffstat) parts.push(result.repo.diffstat);
			if (result.repo.patchPath) parts.push(`patch: ${result.repo.patchPath}`);
		}
		if (result.artifacts.length) parts.push(`artifacts: ${result.artifacts.join(", ")}`);
		if (result.sessionFile) parts.push(`session: ${result.sessionFile}`);
	}
	return parts.join("\n");
}

function digestText(record: TaskRecord): string {
	const lines = [`${record.task.to} · ${record.state} · ${clip(record.task.taskId, 40)}`];
	const result = record.result;
	if (result) {
		const seconds = Math.round(result.durationMs / 1000);
		lines[0] = `${record.task.to} · ${record.state} in ${seconds}s · ${clip(record.task.taskId, 40)}`;
		if (result.text) lines.push(clip(result.text, 700));
		if (result.error) lines.push(`error: ${result.error}`);
		if (result.repo?.diffstat) lines.push(clip(result.repo.diffstat, 300));
		if (result.repo?.patchPath) lines.push(`patch: ${result.repo.patchPath}`);
		if (result.sessionFile) lines.push(`session: ${result.sessionFile}`);
	} else if (record.error) {
		lines.push(`error: ${record.error}`);
	}
	lines.push(`get_result("${record.task.taskId}") for full output`);
	return lines.join("\n");
}

function flushDigests(pi: ExtensionAPI): void {
	const current = runtime;
	if (!current) return;
	if (current.timer) clearTimeout(current.timer);
	current.timer = undefined;
	const pending = current.pending;
	current.pending = [];
	if (pending.length === 0) return;

	const body = pending.map(digestText).join("\n\n---\n\n");
	pi.sendMessage(
		{
			customType: "pi-mqtt-task",
			content: `pi-mqtt results\n\n${body}`,
			display: true,
			details: { tasks: pending.map((record) => ({ taskId: record.task.taskId, worker: record.task.to, state: record.state })) },
		},
		{ triggerTurn: false },
	);
}

function deliverResult(pi: ExtensionAPI, record: TaskRecord): void {
	const current = runtime;
	if (!current) return;
	if (record.delivered || record.inlineWanted) return;
	if (record.task.notify === "wake") {
		pi.sendUserMessage(`pi-mqtt result\n\n${digestText(record)}`, { deliverAs: "followUp" });
		return;
	}
	current.pending.push(record);
	if (current.timer) return;
	current.timer = setTimeout(() => flushDigests(pi), DIGEST_COALESCE_MS);
	current.timer.unref?.();
}

function autoStartRequested(pi: ExtensionAPI, config: AppConfig): boolean {
	if (pi.getFlag(FLAG_START) === true) return true;
	if (process.env.PI_MQTT_START === "1") return true;
	return config.connect === "auto";
}

/** Read the config and remember it. Connecting is a separate, explicit step. */
async function prepareSession(pi: ExtensionAPI, ctx: ExtensionContext): Promise<void> {
	const role = (pi.getFlag(FLAG_ROLE) as string | undefined) ?? process.env.PI_MQTT_ROLE;
	const workerId = pi.getFlag(FLAG_ID) as string | undefined;
	const configPath = pi.getFlag(FLAG_CONFIG) as string | undefined;

	const loaded = loadConfig({
		role,
		...(workerId ? { workerId } : {}),
		...(configPath ? { configPath } : {}),
		cwd: ctx.cwd,
	});
	if (!loaded.ok) {
		if (role) ctx.ui.notify(`pi-mqtt inactive: ${loaded.reason}`, "warning");
		return;
	}

	const config = loaded.config;
	session = { config, topics: topicsFor(config.mqtt.cluster), log: makeLog(ctx) };

	if (autoStartRequested(pi, config)) {
		await connectRuntime(pi, ctx);
		return;
	}
	ctx.ui.notify(
		`pi-mqtt configured as ${config.role} "${config.worker.workerId}" — run /mqtt start to connect`,
		"info",
	);
}

async function connectRuntime(pi: ExtensionAPI, ctx: ExtensionContext): Promise<boolean> {
	const current = session;
	if (!current) {
		ctx.ui.notify("pi-mqtt is not configured in this session: no role or broker. See SETUP.md.", "warning");
		return false;
	}
	if (runtime) return true;

	const { topics, log } = current;
	let config = current.config;
	const chosen = resolveSessionId(pi, ctx, config);
	sessionWorkerId = chosen;
	config = { ...config, worker: { ...config.worker, workerId: chosen } };
	const git = readGitInfo(ctx.cwd);
	const model = describeModel(ctx);
	const identity: PresenceIdentity = {
		workerId: config.worker.workerId,
		host: hostname(),
		cwd: ctx.cwd,
		...(git.repo ? { repo: git.repo } : {}),
		...(git.branch ? { branch: git.branch } : {}),
		...(git.headSha ? { headSha: git.headSha } : {}),
		...(model ? { model } : {}),
	};

	const isWorker = config.role === "worker";
	// Deterministic client id: one live session per id, and a second one takes the
	// connection over. That is the intended collision policy: no suffixes, no refusals.
	const clientId = `pi-mqtt-${config.mqtt.cluster}-${config.worker.workerId}`;
	const link = new MqttLink(config.mqtt, {
		clientId,
		...(isWorker ? { will: { topic: topics.presence(config.worker.workerId), payload: buildPresence(identity, "offline") } } : {}),
		log,
	});

	try {
		await link.connect();
	} catch (error) {
		await link.close();
		const reason = (error as Error).message;
		const hint = /certificate|self-signed|CA/i.test(reason)
			? " — set mqtt.caFile in pi-mqtt.json to the broker's CA bundle"
			: "";
		ctx.ui.notify(`pi-mqtt: cannot reach ${config.mqtt.url}: ${reason}${hint}`, "error");
		return false;
	}

	// One worker id, one live session: two sessions under the same id both consume
	// tasks and their results race, so take over explicitly or not at all.
	if (isWorker) {
		const existing = await probePresence(link, topics, config.worker.workerId, 1200);
		const fresh = existing && existing.state !== "offline" && Date.now() - existing.updatedAt < config.worker.heartbeatMs * 3;
		if (fresh && pi.getFlag(FLAG_FORCE) !== true) {
			await link.close();
			const age = Math.round((Date.now() - existing.updatedAt) / 1000);
			ctx.ui.notify(
				`pi-mqtt: worker id "${config.worker.workerId}" is already live on ${existing.host} (${existing.cwd}, heartbeat ${age}s ago). Use another --pi-mqtt-id, or --pi-mqtt-force to take it over.`,
				"error",
			);
			return false;
		}
	}

	const connected: Runtime = { config, topics, link, pending: [] };
	if (config.role === "orchestrator") {
		const orchestrator = new Orchestrator(link, topics, {
			heartbeatMs: config.worker.heartbeatMs,
			taskExpirySec: config.worker.taskExpirySec,
			log,
			onFinished: (record) => deliverResult(pi, record),
		});
		orchestrator.start();
		connected.orchestrator = orchestrator;
		const sessionId = config.worker.workerId;
		const presence = new Presence(
			link,
			topics.orchestratorPresence(sessionId),
			{ ...identity, workerId: sessionId },
			config.worker.heartbeatMs,
			log,
			86_400,
			"orchestrator",
		);
		presence.start();
		connected.presence = presence;
		pi.setActiveTools([...new Set([...pi.getActiveTools(), ...ORCHESTRATOR_TOOLS])]);
	} else {
		const presence = new Presence(
			link,
			topics.presence(config.worker.workerId),
			identity,
			config.worker.heartbeatMs,
			log,
		);
		// Reconnect: re-announce immediately instead of waiting for the next heartbeat.
		link.onReconnect = () => presence.republish();
		// Deterministic client id + broker takeover: the older session yields instead of
		// reconnecting and evicting the newcomer in a loop.
		link.onTakenOver = () => {
			void disconnectRuntime();
			ctx.ui.notify(
				`pi-mqtt: session "${config.worker.workerId}" was taken over by another session; this one stopped. Rename the pi session, or set an exact id with --pi-mqtt-id.`,
				"error",
			);
		};
		presence.start();
		connected.presence = presence;
		const worker = new Worker(link, topics, { pi, ctx: () => currentCtx, config, presence, log });
		worker.start();
		connected.worker = worker;
	}

	link.subscribe(topics.roomControl, (_topic, payload) => {
		if (!isKickMessage(payload)) return;
		if (payload.from === config.worker.workerId) return; // our own message
		if (!runtime) return;
		const why = payload.reason ? `: ${payload.reason}` : "";
		ctx.ui.notify(`pi-mqtt: ${payload.from} removed everyone from the room${why}. Disconnected.`, "warning");
		if (runtime.config.role === "worker") currentCtx?.abort();
		void disconnectRuntime();
	});

	runtime = connected;
	ctx.ui.notify(`pi-mqtt ${config.role} "${config.worker.workerId}" connected to ${topics.base}`, "info");
	return true;
}

async function disconnectRuntime(): Promise<void> {
	const current = runtime;
	runtime = undefined;
	if (!current) return;
	if (current.timer) clearTimeout(current.timer);
	current.orchestrator?.stop();
	current.worker?.stop();
	current.presence?.stop();
	await current.link.close();
	if (current.config.role === "orchestrator" && api) {
		const keep = api.getActiveTools().filter((name) => !ORCHESTRATOR_TOOLS.includes(name));
		api.setActiveTools(keep);
	}
}

/** Session end: disconnect (gracefully) and forget the config. */
async function endSession(): Promise<void> {
	await disconnectRuntime();
	session = undefined;
	sessionWorkerId = undefined;
}

/** Local state plus who else is on the broker, as far as this role can see. */
function statusText(): string {
	if (!session) return "pi-mqtt is not configured in this session (no role or broker). See SETUP.md.";

	const { config, topics } = session;
	const lines = [
		`role:    ${config.role}`,
		`session: ${api?.getSessionName() ?? "(unnamed)"}`,
		`session id: ${runtime ? runtime.config.worker.workerId : "(not joined)"}`,
		`broker:  ${config.mqtt.url} (MQTT ${config.mqtt.protocolVersion})`,
		`room:    ${config.mqtt.cluster}`,
		`topics:  ${topics.base}`,
	];

	if (!runtime) {
		lines.push("state:   disconnected — /mqtt start to connect");
		return lines.join("\n");
	}

	lines.push(`auth:    ${config.mqtt.token ? "room password" : `user ${config.mqtt.username ?? "?"}`}`);
	lines.push(`state:   connected (online: ${runtime.link.online})`);
	const reconnects = runtime.link.reconnects;
	if (reconnects > 0) {
		const last = runtime.link.lastClose;
		const when = last ? new Date(last.at).toISOString().slice(11, 19) : "?";
		const why = last
			? `${last.source}${last.code !== undefined ? ` ${last.code}` : ""}${last.reason ? ` ${last.reason}` : ""}`
			: "unknown";
		lines.push(`drops:   ${reconnects} (last ${when}: ${why})`);
	}
	const orchestrator = runtime.orchestrator;
	if (orchestrator) {
		const entries = orchestrator.entries();
		const online = entries.filter((entry) => !entry.stale && entry.presence.state !== "offline");
		const idle = online.filter((entry) => entry.presence.state === "idle");
		const busy = online.filter((entry) => entry.presence.state === "busy");
		const offline = entries.length - online.length;
		lines.push(
			`workers: ${entries.length} known — ${online.length} online (${idle.length} idle, ${busy.length} busy), ${offline} off`,
		);
		const outstanding = orchestrator.outstanding();
		if (outstanding.length > 0) {
			const running = outstanding.filter((record) => record.state === "running");
			const queued = outstanding.length - running.length;
			lines.push(`tasks:   ${outstanding.length} outstanding (${running.length} running, ${queued} queued)`);
		}
		const others = orchestrator.orchestratorEntries();
		lines.push(
			`orchestrators: ${others.length === 0 ? "none in the room" : others.map((entry) => entry.presence.workerId).join(", ")}`,
		);
		lines.push("", orchestrator.combinedTable());
	} else {
		const presence = runtime.worker;
		lines.push(presence ? `worker:  idle, listening for tasks` : "worker:  idle");
		lines.push("peers:   not visible from a worker (the ACLs allow only its own topics)");
	}

	return lines.join("\n");
}

function sanitizeId(value: string): string {
	return value
		.toLowerCase()
		.replace(/[^a-z0-9_-]+/g, "-")
		.replace(/^-+|-+$/g, "")
		.slice(0, 48);
}

/**
 * Pick the worker id for this session.
 *
 * An id set explicitly (flag, env, or `worker.workerId` in the file) is honoured as
 * written. Otherwise the session name is offered as the default, and a live id is
 * suffixed (-2, -3, …) rather than refused: several sessions may join from one host.
 */
/**
 * The session's identity in the room: `<name>_<session>`, derived and never asked for.
 *
 * `name` comes from the config file, else the computer name. The session part is the pi
 * session name, or the first four characters of the pi session id when the session is
 * unnamed, so two windows never collide by accident. An explicitly configured id wins
 * outright. There is no suffixing: if two sessions derive the same id, the second takes
 * the first one's place — the broker evicts it, and the evicted session stops.
 */
function resolveSessionId(pi: ExtensionAPI, ctx: ExtensionContext, config: AppConfig): string {
	if (config.worker.explicitId) return config.worker.workerId;
	if (sessionWorkerId) return sessionWorkerId;

	const base = sanitizeId(config.worker.name ?? "") || config.worker.workerId;
	const named = sanitizeId(pi.getSessionName() ?? "");
	const fromSessionId = (ctx.sessionManager.getSessionId() ?? "")
		.replace(/[^A-Za-z0-9]/g, "")
		.slice(0, 4)
		.toLowerCase();
	return `${base}_${named || fromSessionId || "session"}`;
}

/** Why a tool or command cannot do its job right now. */
function inactiveReason(): string {
	if (!session) return "pi-mqtt is not configured in this session (no role or broker). See SETUP.md.";
	if (!runtime) return "pi-mqtt is not connected — run /mqtt start (or launch with --pi-mqtt-start).";
	return "pi-mqtt is not active as an orchestrator in this session.";
}

function draftFromConfig(config: AppConfig | undefined): Draft {
	const draft: Draft = { mqtt: {}, worker: {} };
	if (!config) return draft;
	draft.role = config.role;
	draft.connect = config.connect;
	draft.id = config.worker.explicitId ? config.worker.workerId : undefined;
	draft.room = config.mqtt.cluster;
	draft.roomPassword = config.mqtt.token;
	draft.mqtt.url = config.mqtt.url;
	draft.mqtt.cluster = config.mqtt.cluster;
	draft.mqtt.username = config.mqtt.username;
	draft.mqtt.password = config.mqtt.password;
	draft.mqtt.token = config.mqtt.token;
	draft.mqtt.caFile = config.mqtt.caFile;
	draft.mqtt.keepaliveSec = config.mqtt.keepaliveSec;
	draft.mqtt.reconnectMs = config.mqtt.reconnectMs;
	draft.mqtt.protocolVersion = config.mqtt.protocolVersion;
	draft.mqtt.rejectUnauthorized = config.mqtt.rejectUnauthorized;
	draft.worker.workerId = config.worker.workerId;
	draft.worker.name = config.worker.name;
	draft.worker.heartbeatMs = config.worker.heartbeatMs;
	draft.worker.taskExpirySec = config.worker.taskExpirySec;
	draft.worker.allowModelOverride = config.worker.allowModelOverride;
	// logDir is only written when the user edits it.
	return draft;
}

/**
 * Serialise the draft into the on-disk structure: identity (`role`, `name`, `id`),
 * membership (`room`), transport (`broker`), behaviour (`worker`). Legacy keys that the
 * loader still accepts are removed on save, so editing through the menu migrates a file.
 */
function mergeDraft(raw: RawConfig, draft: Draft): RawConfig {
	const out: RawConfig = { ...raw };
	delete out.roomPassword;
	delete out.mqtt;

	if (draft.role !== undefined) out.role = draft.role;
	if (draft.connect !== undefined) out.connect = draft.connect;
	if (draft.id !== undefined) {
		if (draft.id) out.id = draft.id;
		else delete out.id;
	}
	if (draft.logDir !== undefined) out.logDir = draft.logDir;

	const existingName = typeof out.name === "string" ? out.name : undefined;
	const name = draft.worker.name ?? existingName;
	if (name) out.name = name;

	const existingRoom: Record<string, unknown> =
		typeof out.room === "object" && out.room !== null ? { ...(out.room as Record<string, unknown>) } : {};
	const previousRoomName =
		typeof out.room === "string"
			? out.room
			: typeof existingRoom.name === "string"
				? existingRoom.name
				: undefined;
	const roomName = draft.room ?? previousRoomName;
	const roomPassword =
		draft.roomPassword ?? (typeof existingRoom.password === "string" ? existingRoom.password : undefined);
	const room: Record<string, unknown> = {};
	if (roomName) room.name = roomName;
	if (roomPassword) room.password = roomPassword;
	if (Object.keys(room).length > 0) out.room = room;

	const existingBroker =
		typeof out.broker === "object" && out.broker !== null ? (out.broker as Record<string, unknown>) : {};
	const broker: Record<string, unknown> = { ...existingBroker };
	if (typeof out.broker === "string") broker.url = out.broker;
	const mqtt = draft.mqtt as Record<string, unknown>;
	const brokerDefaults: Record<string, unknown> = {
		keepaliveSec: 15,
		reconnectMs: 2000,
		protocolVersion: 5,
		rejectUnauthorized: true,
	};
	for (const key of ["url", "caFile", "keepaliveSec", "reconnectMs", "protocolVersion", "rejectUnauthorized"]) {
		const value = mqtt[key];
		if (value === undefined || value === brokerDefaults[key]) continue;
		broker[key] = value;
	}
	if (Object.keys(broker).length > 0) out.broker = broker;

	// Defaults are not written: a config should say what is unusual about this machine.
	const workerDefaults: Record<string, unknown> = {
		heartbeatMs: 30000,
		taskExpirySec: 900,
		allowModelOverride: false,
	};
	const worker: Record<string, unknown> = {};
	for (const key of ["heartbeatMs", "taskExpirySec", "allowModelOverride", "repos", "defaultRepo"]) {
		const value = (draft.worker as Record<string, unknown>)[key];
		if (value === undefined || value === workerDefaults[key]) continue;
		worker[key] = value;
	}
	const existingWorker =
		typeof out.worker === "object" && out.worker !== null ? (out.worker as Record<string, unknown>) : {};
	for (const [key, value] of Object.entries(existingWorker)) {
		if (worker[key] === undefined && !["name", "workerId"].includes(key)) worker[key] = value;
	}
	if (Object.keys(worker).length > 0) out.worker = worker;
	else delete out.worker;

	// Stable order: identity, membership, transport, behaviour, then anything unknown.
	const ordered: RawConfig = {};
	for (const key of ["role", "name", "id", "room", "broker", "worker", "connect", "logDir"]) {
		if (out[key] !== undefined) ordered[key] = out[key];
	}
	for (const [key, value] of Object.entries(out)) {
		if (ordered[key] === undefined) ordered[key] = value;
	}
	return ordered;
}

function buildMenu(pi: ExtensionAPI, ctx: ExtensionContext) {
	const explicit = pi.getFlag(FLAG_CONFIG) as string | undefined;
	const info = resolveConfigPath({ ...(explicit ? { explicit } : {}), cwd: ctx.cwd });
	const raw = readConfigObject(info.path);
	const draft = draftFromConfig(session?.config);

	return {
		ctx,
		path: info.path,
		pathOrigin: info.origin,
		draft,
		raw,
		log: (message: string) => session?.log("info", message),
		busy: () =>
			(runtime?.orchestrator?.outstanding() ?? []).some((record) => record.state === "running") ||
			runtime?.presence?.state === "busy",
		test: async (candidate: Draft): Promise<string | null> => {
			const temporary = join(tmpdir(), `pi-mqtt-config-${process.pid}.json`);
			try {
				writeFileSync(temporary, JSON.stringify(mergeDraft(raw, candidate), null, 2), { mode: 0o600 });
				const loaded = loadConfig({ configPath: temporary, cwd: ctx.cwd });
				if (!loaded.ok) return loaded.reason;
				const link = new MqttLink(loaded.config.mqtt, { clientId: `pi-mqtt-configtest-${Math.random().toString(36).slice(2, 8)}`, log: () => {} });
				try {
					await link.connect();
				} catch (error) {
					return (error as Error).message;
				}
				await link.close();
				return null;
			} finally {
				try {
					rmSync(temporary, { force: true });
				} catch {
					// best effort
				}
			}
		},
		save: async (candidate: Draft, current: RawConfig): Promise<string> => {
			writeConfigObject(info.path, mergeDraft(current, candidate));
			return `saved ${info.path} (${info.origin}); mode 600`;
		},
		apply: async () => {
			const wasConnected = Boolean(runtime);
			await disconnectRuntime();
			session = undefined;
			await prepareSession(pi, ctx);
			if (wasConnected && session) await connectRuntime(pi, ctx);
		},
	};
}

export default function (pi: ExtensionAPI) {
	pi.registerFlag(FLAG_ROLE, { description: "pi-mqtt role: orchestrator | worker", type: "string" });
	pi.registerFlag(FLAG_ID, { description: "pi-mqtt worker id (defaults to hostname)", type: "string" });
	pi.registerFlag(FLAG_CONFIG, { description: "path to pi-mqtt.json", type: "string" });
	pi.registerFlag(FLAG_FORCE, {
		description: "start even if another live session is already using this worker id",
		type: "boolean",
		default: false,
	});
	pi.registerFlag(FLAG_START, {
		description: "connect to the broker at session start instead of waiting for /mqtt start",
		type: "boolean",
		default: false,
	});

	// Registered always, activated only for an orchestrator. That keeps installed
	// packages invisible in unrelated sessions without re-registering on reload.
	pi.registerTool({
		name: TOOL_LIST_WORKERS,
		label: "List workers",
		description:
			"List pi-mqtt workers advertised on the broker: state (idle/busy/offline), repo and branch, model, working directory, and heartbeat age.",
		parameters: Type.Object({}),
		defaultActive: false,
		async execute() {
			const current = runtime;
			if (!current?.orchestrator) {
				return { content: [{ type: "text", text: inactiveReason() }], details: undefined };
			}
			return {
				content: [{ type: "text", text: current.orchestrator.combinedTable() }],
				details: {
					cluster: current.config.mqtt.cluster,
					workers: current.orchestrator.entries().map((entry) => ({
						workerId: entry.presence.workerId,
						state: entry.stale ? "stale" : entry.presence.state,
						repo: entry.presence.repo,
						branch: entry.presence.branch,
						model: entry.presence.model,
						cwd: entry.presence.cwd,
						activeTaskId: entry.presence.activeTaskId,
						ageMs: entry.ageMs,
					})),
				},
			};
		},
	});

	pi.registerTool({
		name: TOOL_DISPATCH,
		label: "Dispatch task",
		description:
			"Send a task to one named pi-mqtt worker and return immediately. The worker runs it in its own session; the result arrives later as a transcript entry. Use this instead of publishing MQTT messages yourself.",
		promptSnippet: "dispatch_task — hand work to a named pi-mqtt worker",
		promptGuidelines: [
			"Hand work to other agents with `dispatch_task`; never publish pi-mqtt protocol messages with bash.",
			"Check `list_workers` before dispatching: dispatch fails fast if the target is unknown, offline, or busy.",
			"`dispatch_task` waits for the result by default and returns it, so do not poll with `get_result` or `sleep`; pass `wait: false` only when fanning out.",
		],
		parameters: Type.Object({
			to: Type.String({ description: "Exact worker id from list_workers, e.g. lap-b" }),
			prompt: Type.String({ description: "The task, in prose. State what 'done' looks like." }),
			notify: Type.Optional(
				Type.Union([Type.Literal("silent"), Type.Literal("wake")], {
					description: "silent (default) drops a digest; wake starts a turn when the result arrives",
				}),
			),
			ifBusy: Type.Optional(
				Type.Union([Type.Literal("fail"), Type.Literal("wait")], {
					description: "fail (default) returns an error; wait holds the task in this session's outbox until the worker is idle",
				}),
			),
			wait: Type.Optional(
				Type.Boolean({
					description:
						"Wait for the result and return it in this tool call (default true). Pass false to fan out and let results arrive as transcript entries.",
				}),
			),
			waitTimeoutMs: Type.Optional(
				Type.Number({ description: `How long to wait for the result before returning. Default ${DEFAULT_WAIT_MS}ms.` }),
			),
			timeoutMs: Type.Optional(Type.Number({ description: "Abort the worker's run after this long. Default 10 minutes." })),
			maxCostUsd: Type.Optional(Type.Number({ description: "Abort the worker's run when its spend exceeds this" })),
			model: Type.Optional(Type.String({ description: "Requested model; honored only if the worker allows overrides" })),
			repo: Type.Optional(
				Type.Object({
					baseRef: Type.Optional(Type.String({ description: "Branch or ref to start from" })),
					baseSha: Type.Optional(Type.String({ description: "Exact commit to start from; preferred over baseRef" })),
					branch: Type.Optional(Type.String({ description: "Branch to create; defaults to worker/<id>" })),
				}),
			),
		}),
		defaultActive: false,
		async execute(_toolCallId, params, signal) {
			const current = runtime;
			if (!current?.orchestrator) {
				return {
					content: [{ type: "text", text: inactiveReason() }],
					details: undefined as DispatchDetails | undefined,
					isError: true,
				};
			}
			const outcome = current.orchestrator.dispatch({
				to: params.to,
				prompt: params.prompt,
				...(params.notify ? { notify: params.notify } : {}),
				...(params.ifBusy ? { ifBusy: params.ifBusy } : {}),
				...(params.timeoutMs !== undefined ? { timeoutMs: params.timeoutMs } : {}),
				...(params.maxCostUsd !== undefined ? { maxCostUsd: params.maxCostUsd } : {}),
				...(params.model ? { model: params.model } : {}),
				...(params.repo ? { repo: params.repo } : {}),
			});
			if (!outcome.ok) {
				return {
					content: [{ type: "text", text: `dispatch failed: ${outcome.error}` }],
					details: { error: outcome.error } as DispatchDetails,
					isError: true,
				};
			}
			const queuedNote = outcome.state === "queued" ? " (queued: worker is busy; it fires when idle)" : "";

			if (params.wait === false) {
				return {
					content: [{ type: "text", text: `dispatched ${outcome.taskId} to ${params.to}${queuedNote}; not waiting` }],
					details: { taskId: outcome.taskId, state: outcome.state, worker: params.to } as DispatchDetails,
				};
			}

			const waitMs = params.waitTimeoutMs ?? DEFAULT_WAIT_MS;
			// Declare the intent first: the result may land while we are waiting, and
			// onFinished runs before this continuation resumes.
			const tracked = current.orchestrator.get(outcome.taskId);
			if (tracked) tracked.inlineWanted = true;

			const finished = await current.orchestrator.waitFor(outcome.taskId, waitMs, signal);
			if (!finished) {
				if (tracked) tracked.inlineWanted = false;
				// Race: it finished while we were giving up, so deliver it normally.
				if (tracked?.finishedAt) deliverResult(pi, tracked);
				// Still running, or the caller's tool call was aborted: either way the
				// worker keeps going and the digest path delivers the result.
				return {
					content: [
						{
							type: "text",
							text: `dispatched ${outcome.taskId} to ${params.to}${queuedNote}; still running after ${Math.round(waitMs / 1000)}s. The result will arrive as a transcript entry.`,
						},
					],
					details: { taskId: outcome.taskId, state: outcome.state, worker: params.to } as DispatchDetails,
				};
			}

			finished.delivered = true;
			return {
				content: [{ type: "text", text: describeTask(finished) }],
				details: { taskId: outcome.taskId, state: finished.state, worker: params.to } as DispatchDetails,
			};
		},
	});

	pi.registerTool({
		name: TOOL_RESULT,
		label: "Get task result",
		description: "Fetch the full result of a dispatched pi-mqtt task: final text, status, diffstat, patch path, session file.",
		parameters: Type.Object({ taskId: Type.String({ description: "Task id returned by dispatch_task" }) }),
		defaultActive: false,
		async execute(_toolCallId, params) {
			const record = runtime?.orchestrator?.get(params.taskId);
			if (!record) {
				return { content: [{ type: "text", text: `unknown task ${params.taskId}` }], details: undefined, isError: true };
			}
			record.delivered = true;
			return {
				content: [{ type: "text", text: describeTask(record) }],
				details: { status: record.state, result: record.result },
			};
		},
	});

	pi.registerTool({
		name: TOOL_LIST_TASKS,
		label: "List tasks",
		description:
			"List pi-mqtt tasks dispatched in this session with their state (queued, sent, running, done, timeout, cancelled, lost). Use it before re-dispatching a lost or timed-out task.",
		parameters: Type.Object({}),
		defaultActive: false,
		async execute() {
			const current = runtime;
			if (!current?.orchestrator) {
				return { content: [{ type: "text", text: inactiveReason() }], details: undefined };
			}
			return {
				content: [{ type: "text", text: current.orchestrator.taskTable() }],
				details: {
					tasks: current.orchestrator.list().map((record) => ({
						taskId: record.task.taskId,
						worker: record.task.to,
						state: record.state,
						attempts: record.attempts,
						error: record.error ?? record.result?.error,
					})),
				},
			};
		},
	});

	pi.registerTool({
		name: TOOL_CANCEL,
		label: "Cancel task",
		description: "Cancel a dispatched pi-mqtt task. Cooperative: the worker aborts its current run and reports the partial result.",
		parameters: Type.Object({ taskId: Type.String({ description: "Task id to cancel" }) }),
		defaultActive: false,
		async execute(_toolCallId, params) {
			const outcome = runtime?.orchestrator?.cancel(params.taskId) ?? { ok: false, error: "pi-mqtt is not active as an orchestrator" };
			return {
				content: [{ type: "text", text: outcome.ok ? `cancelled ${params.taskId}` : `cancel failed: ${outcome.error}` }],
				details: outcome,
				...(outcome.ok ? {} : { isError: true }),
			};
		},
	});

	pi.registerCommand("workers", {
		description: "Show pi-mqtt workers and their state",
		handler: async (_args, ctx) => {
			if (!runtime?.orchestrator) {
				ctx.ui.notify(inactiveReason(), "warning");
				return;
			}
			ctx.ui.notify(runtime.orchestrator.combinedTable(), "info");
		},
	});

	pi.registerCommand("tasks", {
		description: "Show pi-mqtt tasks dispatched in this session",
		handler: async (_args, ctx) => {
			if (!runtime?.orchestrator) {
				ctx.ui.notify(inactiveReason(), "warning");
				return;
			}
			ctx.ui.notify(runtime.orchestrator.taskTable(), "info");
		},
	});

	pi.registerCommand("mqtt", {
		description:
			"pi-mqtt: /mqtt config to edit room and password, /mqtt status, start, stop, restart, kick",
		handler: async (args, ctx) => {
			const arg = args.trim().toLowerCase();

			if (arg === "config") {
				await runConfigMenu(buildMenu(pi, ctx));
				return;
			}
			if (arg === "status") {
				ctx.ui.notify(statusText(), "info");
				return;
			}
			if (arg === "start") {
				await connectRuntime(pi, ctx);
				return;
			}
			if (arg.startsWith("kick")) {
				if (!runtime) {
					ctx.ui.notify("pi-mqtt is not connected.", "info");
					return;
				}
				const reason = args.trim().slice(4).trim() || undefined;
				const from = runtime.config.worker.workerId;
				await runtime.link.publish(
					runtime.topics.roomControl,
					{ v: 1, kind: "kick", from, at: Date.now(), ...(reason ? { reason } : {}) },
					{},
				);
				ctx.ui.notify(
					`pi-mqtt: asked every other session in the room to leave${reason ? ` (${reason})` : ""}.`,
					"info",
				);
				return;
			}
			if (arg === "stop") {
				if (!runtime) {
					ctx.ui.notify("pi-mqtt is not connected.", "info");
					return;
				}
				await disconnectRuntime();
				ctx.ui.notify("pi-mqtt disconnected.", "info");
				return;
			}
			if (arg === "restart") {
				await disconnectRuntime();
				await connectRuntime(pi, ctx);
				return;
			}
			if (arg) {
				ctx.ui.notify("usage: /mqtt [status|config|start|stop|restart|kick]", "warning");
				return;
			}

			const lines = logs
				.slice(-12)
				.map((line) => `${new Date(line.at).toISOString().slice(11, 19)} ${line.level.padEnd(5)} ${line.message}`);
			ctx.ui.notify([statusText(), "", ...lines].join("\n"), "info");
		},
	});

	pi.on("session_start", async (_event, ctx) => {
		currentCtx = ctx;
		api = pi;
		await endSession();
		try {
			await prepareSession(pi, ctx);
		} catch (error) {
			ctx.ui.notify(`pi-mqtt: ${(error as Error).message}`, "error");
		}
	});

	pi.on("session_shutdown", async () => {
		await endSession();
		currentCtx = undefined;
	});

	pi.on("agent_start", async () => {
		if (runtime?.config.role === "orchestrator") runtime.presence?.update({ state: "busy" });
	});

	pi.on("agent_settled", async () => {
		if (runtime?.config.role === "orchestrator") runtime.presence?.update({ state: "idle" });
	});

	pi.on("model_select", async (_event, ctx) => {
		currentCtx = ctx;
		const model = describeModel(ctx);
		if (model) runtime?.presence?.update({ model });
	});

	logs = [];
}
