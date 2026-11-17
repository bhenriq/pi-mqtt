/**
 * pi-mqtt for pi on iOS: drive named pi-mqtt workers from the phone.
 *
 * This is the orchestrator half of https://github.com/bhenriq/pi-mqtt, written for
 * pi on iOS. It speaks the same wire protocol (`pi/v1/<room>/...`, see src/protocol.ts
 * in the repo root), so a phone running this can pin tasks to any worker on the
 * broker, and a desktop session running the normal pi-mqtt package sees the phone
 * like any other orchestrator.
 *
 * Why it does not just depend on the desktop package: pi on iOS has no Node
 * `net`/`tls`, so mqtt.js cannot open a raw MQTT connection. WebSocket is available
 * and mosquitto serves MQTT over WebSocket, so src/mqtt-ws.ts implements MQTT 3.1.1
 * over a WebSocket directly — no dependencies at all.
 *
 * Nothing opens a socket at load time. `/mqtt connect`, any mqtt_* tool, the MQTT
 * app's Connect button, or opening the app when a password is already stored joins
 * the room.
 */

import { Type } from "@earendil-works/pi-ai";
import { MqttWs } from "./src/mqtt-ws.ts";
import { appHtml, type Snapshot } from "./src/page.ts";
import { Pairing } from "./src/pairing.ts";
import {
	formatPairingCode,
	fingerprintOf,
	PROTOCOL_VERSION,
	buildPresence,
	topicsFor,
	type PairMessage,
	type PresenceIdentity,
} from "./src/protocol.ts";
import { Room, type TaskRecord } from "./src/room.ts";
import { TrustStore } from "./src/trust.ts";
import { IDENTIFIER, clip, formatAge, sanitizeId, type LogLine } from "./src/util.ts";

// ---------------------------------------------------------------------------
// Configuration and constants
// ---------------------------------------------------------------------------

/** Name of the Keychain secret that carries the room password. */
const SECRET_NAME = "PI_MQTT_ROOM_PASSWORD";
/** pi.synced key: one record, merged per key across the user's devices. */
const SETTINGS_KEY = "settings";
const DEFAULT_URL = "wss://broker-production-0bfd.up.railway.app";
const DEFAULT_ROOM = "dev";
const DEFAULT_NAME = "iphone";
const DEFAULT_WAIT_MS = 120_000;
const LOG_LIMIT = 60;

export interface Settings {
	url: string;
	room: string;
	name: string;
	/** Broker user; the deployed mosquitto has exactly one, "cluster". */
	username: string;
	heartbeatMs: number;
	/** Send a phone notification when a task finishes in the background. */
	notify: boolean;
}

// ---------------------------------------------------------------------------
// Extension state
// ---------------------------------------------------------------------------

interface CtxLike {
	ui?: any;
	cwd?: string;
	model?: { provider?: unknown; id?: unknown };
}

let api: any;
let settings: Settings;
let client: MqttWs | undefined;
let room: Room | undefined;
let pairing: Pairing | undefined;
let connecting: Promise<void> | undefined;
let appHandle: any;
let lastCtx: CtxLike | undefined;
let logs: LogLine[] = [];
let pushTimer: any;
let passwordKnown = false;

function log(level: "info" | "warn" | "error", message: string): void {
	logs.push({ at: Date.now(), level, message });
	if (logs.length > LOG_LIMIT) logs = logs.slice(-LOG_LIMIT);
	// Only errors interrupt. A dropped WebSocket is normal on a phone — iOS suspends the
	// app, the broker forgets the session, the link comes back — and a popup for each one
	// turns a working extension into a nuisance. Drops stay in the app's log and in
	// mqtt_status; the tools report a frozen roster when it matters.
	if (level === "error") {
		try {
			lastCtx?.ui?.notify(`mqtt: ${message}`, "error");
		} catch {
			// no UI attached (headless run)
		}
	}
	schedulePush();
}

function normalizeSettings(raw: Partial<Settings> | undefined): Settings {
	const name = sanitizeId(String(raw?.name ?? "")) || DEFAULT_NAME;
	const room = String(raw?.room ?? DEFAULT_ROOM).trim() || DEFAULT_ROOM;
	const url = String(raw?.url ?? DEFAULT_URL).trim() || DEFAULT_URL;
	const heartbeat = Number(raw?.heartbeatMs);
	return {
		url,
		room,
		name,
		username: String(raw?.username ?? "cluster").trim() || "cluster",
		heartbeatMs: Number.isFinite(heartbeat) && heartbeat >= 5_000 ? Math.min(300_000, heartbeat) : 30_000,
		notify: raw?.notify === true,
	};
}

function readSettings(): Settings {
	let raw: Partial<Settings> | undefined;
	try {
		raw = api.synced?.get(SETTINGS_KEY) as Partial<Settings> | undefined;
	} catch {
		raw = undefined;
	}
	return normalizeSettings(raw ?? {});
}

function writeSettings(patch: Partial<Settings>): Settings {
	settings = normalizeSettings({ ...settings, ...patch });
	try {
		api.synced?.set(SETTINGS_KEY, settings);
	} catch (error) {
		log("warn", `could not save settings: ${(error as Error).message}`);
	}
	schedulePush();
	return settings;
}

async function readPassword(): Promise<string | undefined> {
	const fromEnv = process.env[SECRET_NAME]?.trim();
	if (fromEnv) return fromEnv;
	try {
		const stored = await api.secrets?.get(SECRET_NAME);
		return typeof stored === "string" && stored.trim().length > 0 ? stored.trim() : undefined;
	} catch {
		return undefined;
	}
}

async function storePassword(value: string): Promise<void> {
	await api.secrets.set(SECRET_NAME, value.trim());
	passwordKnown = true;
	log("info", `room password saved to the Keychain as ${SECRET_NAME}`);
}

function describeModel(ctx: CtxLike | undefined): string | undefined {
	const model = ctx?.model;
	if (!model) return undefined;
	const provider = model.provider === undefined ? "" : String(model.provider);
	const id = model.id === undefined ? "" : String(model.id);
	return provider && id ? `${provider}/${id}` : id || provider || undefined;
}

function presenceIdentity(): PresenceIdentity {
	const model = describeModel(lastCtx);
	return {
		workerId: settings.name,
		host: "iphone",
		cwd: lastCtx?.cwd ?? "~",
		...(model ? { model } : {}),
	};
}

/** Connect if configured. Idempotent, and safe to call from a tool or a button. */
async function ensureConnected(ctx?: CtxLike): Promise<void> {
	if (ctx) lastCtx = ctx;
	if (room && client) {
		// The link re-establishes itself; opening a second socket would only fight it for
		// the connection. Give a reconnect in flight a moment (this is the normal state
		// right after the app was suspended) before letting a caller report it.
		if (!client.online && !(await client.waitOnline(4_000))) {
			log("warn", "the broker link is still down; nothing can go out until it returns");
		}
		return;
	}
	if (connecting) return connecting;

	connecting = (async () => {
		const s = settings;
		if (!/^wss?:\/\//i.test(s.url)) {
			throw new Error(
				`the broker must be a WebSocket URL (ws:// or wss://); ${s.url} needs raw TCP, which pi on iOS cannot open — mosquitto serves WebSocket on its 9001 listener, usually as wss://host/mqtt`,
			);
		}
		if (!IDENTIFIER.test(s.room)) throw new Error(`invalid room id ${JSON.stringify(s.room)}`);
		if (!IDENTIFIER.test(s.name)) throw new Error(`invalid device name ${JSON.stringify(s.name)}`);

		let password = await readPassword();
		passwordKnown = Boolean(password);
		if (!password) {
			const ctxUi = ctx?.ui ?? lastCtx?.ui;
			const entered = ctxUi
				? await ctxUi.input("MQTT room password", "the password every session in the room shares")
				: undefined;
			if (!entered || !entered.trim()) {
				throw new Error(
					`no room password: run /mqtt password, or add the secret ${SECRET_NAME} in Settings → Extensions & skills → Secrets, or set it on the broker side`,
				);
			}
			await storePassword(entered);
			password = entered.trim();
		}

		const t = topicsFor(s.room);
		const identity = presenceIdentity();
		pushNow("connecting");
		const link = new MqttWs({
			url: s.url,
			// Unique per engine instance on purpose. An orchestrator is identified by the
			// presence id, not by the client id, and a reload can leave the previous
			// instance reconnecting for a moment: with a deterministic client id the two
			// would evict each other in a loop (MQTT 3.1.1 closes the loser with no reason
			// code), which reads as "the link keeps dropping".
			clientId: `pi-mqtt-ios-${s.room}-${s.name}-${Math.random().toString(36).slice(2, 8)}`,
			username: s.username,
			password,
			keepaliveSec: 30,
			will: {
				topic: t.orchestratorPresence(s.name),
				payload: JSON.stringify(buildPresence(identity, "offline", "orchestrator")),
				qos: 1,
				retain: true,
			},
			log,
		});
		// Worker admission: a worker joins as `pending` and only becomes dispatchable
		// once this phone approves its key (silently if it is already on file).
		const trust = new TrustStore(api.synced, s.room, log);
		const admission = new Pairing({
			room: s.room,
			orchestratorId: s.name,
			trust,
			log,
			onChange: schedulePush,
			onOffer: (offer) => {
				log(
					"warn",
					`${offer.workerId} wants to join: code ${formatPairingCode(offer.code)} — compare it with the worker's screen, then /mqtt admit ${offer.workerId} ${offer.code}`,
				);
			},
			publish: (message: PairMessage) => {
				void link.publishJson(t.pair(message.workerId), message, { qos: 1 }).catch((error: Error) => {
					log("warn", `pairing message failed: ${error.message}`);
				});
			},
		});
		const next = new Room(
			link,
			t,
			identity,
			s.heartbeatMs,
			log,
			schedulePush,
			deliverResult,
			(from, reason) => {
				const why = reason ? `: ${reason}` : "";
				lastCtx?.ui?.notify(`mqtt: ${from} removed everyone from the room${why}. Left the room.`, "warning");
				void disconnectRoom("removed from the room");
			},
			admission,
		);
		try {
			await link.connect();
		} catch (error) {
			await link.close();
			throw new Error(`cannot reach ${s.url}: ${(error as Error).message}`);
		}
		client = link;
		room = next;
		pairing = admission;
		next.start();
		await next.waitForRoster();
		// A reconnect means the retained presence may have been swept and the filters
		// were lost: re-announce as soon as the socket is back.
		link.onOnline = () => next.republish();
		link.onOffline = (reason) => {
			log("warn", `link down (${reason}); the roster is frozen until it returns`);
		};
		log("info", `joined ${t.base} as orchestrator "${s.name}"`);
		schedulePush();
	})().finally(() => {
		connecting = undefined;
	});

	return connecting;
}

async function disconnectRoom(reason?: string, options: { quiet?: boolean } = {}): Promise<void> {
	const currentRoom = room;
	const currentClient = client;
	room = undefined;
	client = undefined;
	pairing = undefined;
	if (currentRoom) await currentRoom.stop(options);
	if (currentClient) await currentClient.close();
	if (reason) log("info", reason);
	pushNow();
}

function formatRecord(record: TaskRecord): string {
	const parts = [`task ${record.task.taskId} · ${record.task.to} · ${record.state}`];
	if (record.error) parts.push(`error: ${record.error}`);
	const result = record.result;
	if (result) {
		parts.push(`duration: ${Math.round(result.durationMs / 1000)}s`);
		if (result.text) parts.push(`--- final message ---\n${result.text}`);
		if (result.error) parts.push(`worker error: ${result.error}`);
		if (result.repo) {
			parts.push(
				`repo: ${result.repo.branch ?? "-"} ${result.repo.baseSha?.slice(0, 8) ?? "?"}..${result.repo.headSha?.slice(0, 8) ?? "?"}`,
			);
			if (result.repo.diffstat) parts.push(result.repo.diffstat);
			if (result.repo.patchPath) parts.push(`patch: ${result.repo.patchPath}`);
		}
		if (result.artifacts?.length) parts.push(`artifacts: ${result.artifacts.join(", ")}`);
		if (result.sessionFile) parts.push(`session: ${result.sessionFile}`);
	}
	return parts.join("\n");
}

/** A finished task reaches the transcript (and optionally the Lock Screen). */
function deliverResult(record: TaskRecord): void {
	if (record.inlineWanted) return;
	const body = formatRecord(record);
	if (record.task.notify === "wake") {
		api.sendUserMessage(`mqtt result\n\n${body}`, { deliverAs: "followUp" });
	} else {
		api.sendMessage(
			{
				customType: "mqtt-ios-result",
				content: `mqtt results\n\n${body}`,
				display: true,
				details: { taskId: record.task.taskId, worker: record.task.to, state: record.state },
			},
			{ triggerTurn: false },
		);
	}
	if (settings.notify) {
		const text = record.result?.text ?? record.error ?? record.state;
		void Promise.resolve(
			api.notify({
				title: `mqtt · ${record.task.to} · ${record.state}`,
				body: clip(text, 180),
			}),
		).catch(() => {
			// notifications are a nicety; a refusal is not an error
		});
	}
}

// ---------------------------------------------------------------------------
// Snapshot for the app
// ---------------------------------------------------------------------------

function snapshot(statusOverride?: Snapshot["status"]): Snapshot {
	const current = room;
	const now = Date.now();
	const workers = current
		? current.entries().map(({ presence, stale, ageMs }) => ({
				id: presence.workerId,
				state: presence.state,
				stale,
				pending: presence.state === "pending",
				approved: pairing ? pairing.isApproved(presence) : undefined,
				...(presence.state === "pending" ? { code: pairing?.codeFor(presence.workerId) } : {}),
				...(presence.key ? { fingerprint: fingerprintOf(presence.key) } : {}),
				...(presence.repo ? { repo: presence.repo } : {}),
				...(presence.branch ? { branch: presence.branch } : {}),
				...(presence.model ? { model: presence.model } : {}),
				...(presence.cwd ? { cwd: presence.cwd } : {}),
				...(presence.activeTaskId ? { activeTaskId: presence.activeTaskId } : {}),
				age: formatAge(ageMs),
			}))
		: [];
	const orchestrators = current
		? current.orchestratorEntries().map(({ presence, stale, ageMs }) => ({
				id: presence.workerId,
				stale,
				age: formatAge(ageMs),
				self: presence.workerId === settings.name,
			}))
		: [];
	const tasks = current
		? current.list().map((record) => {
				const stamped = record.finishedAt ?? record.startedAt ?? record.sentAt ?? record.task.createdAt;
				const result = record.result;
				const detail = record.error ?? result?.error ?? clip(result?.text ?? record.task.prompt, 120);
				return {
					taskId: record.task.taskId,
					to: record.task.to,
					state: record.state,
					age: formatAge(now - stamped),
					detail,
					...(result?.text ? { text: clip(result.text, 4000) } : {}),
					...(result ? { duration: `${Math.round(result.durationMs / 1000)}s` } : {}),
				};
			})
		: [];
	const status: Snapshot["status"] = statusOverride ?? (client?.online ? "online" : room ? "connecting" : "offline");
	return {
		status,
		room: settings.room,
		url: settings.url,
		me: settings.name,
		hasPassword: passwordKnown,
		settings,
		workers,
		orchestrators,
		tasks,
		log: logs.slice(-25).map((line) => ({ at: line.at, level: line.level, message: line.message })),
		lastError: client?.lastClose ? `${formatAge(Date.now() - client.lastClose.at)} ago: ${client.lastClose.reason}` : undefined,
	};
}

function schedulePush(): void {
	if (pushTimer) return;
	pushTimer = setTimeout(() => {
		pushTimer = undefined;
		pushNow();
	}, 120);
}

function pushNow(statusOverride?: Snapshot["status"]): void {
	const app = appHandle;
	if (!app || app.closed) return;
	const snap = snapshot(statusOverride);
	try {
		app.setState(snap);
		app.post({ type: "snapshot", snapshot: snap });
		const outstanding = snap.tasks.filter((task) => ["queued", "sent", "running"].includes(task.state)).length;
		app.setBadge(outstanding > 0 ? String(outstanding) : "");
	} catch {
		// the app was closed between the check and the post
	}
}

// ---------------------------------------------------------------------------
// Tool and command surface
// ---------------------------------------------------------------------------

function inactiveReason(): string {
	return (
		"mqtt is not connected to a broker. Run /mqtt connect (or tap Connect in the MQTT app). " +
		"If it is not set up yet, /mqtt setup asks for the broker URL, the room and the room password."
	);
}

function workerTable(): string {
	const current = room;
	if (!current) return inactiveReason();
	const entries = current.entries();
	const others = current.orchestratorEntries();
	if (entries.length === 0 && others.length === 0) return `No sessions in ${current.topics.base} yet.`;
	const rows = [
		...entries.map(({ presence, stale, ageMs }) => {
			const repo = [presence.repo, presence.branch].filter(Boolean).join("@") || "-";
			return [
				presence.workerId.padEnd(20),
				("worker").padEnd(13),
				(stale ? `${presence.state}?` : presence.state).padEnd(9),
				repo.padEnd(24),
				(presence.model ?? "-").padEnd(20),
				formatAge(ageMs).padEnd(5),
				presence.cwd ?? "-",
			].join("  ");
		}),
		...others.map(({ presence, stale, ageMs }) => {
			const repo = [presence.repo, presence.branch].filter(Boolean).join("@") || "-";
			return [
				presence.workerId.padEnd(20),
				("orchestrator").padEnd(13),
				(stale ? `${presence.state}?` : presence.state).padEnd(9),
				repo.padEnd(24),
				(presence.model ?? "-").padEnd(20),
				formatAge(ageMs).padEnd(5),
				presence.cwd ?? "-",
			].join("  ");
		}),
	];
	const header = ["ID", "ROLE", "STATE", "REPO", "MODEL", "AGE", "CWD"]
		.map((label, index) => label.padEnd([20, 13, 9, 24, 20, 5, 3][index] ?? 8))
		.join("  ");
	const waiting = (pairing?.list() ?? []).map((offer) => {
		const minutes = Math.max(0, Math.round((offer.expiresAt - Date.now()) / 60_000));
		return `  ${offer.workerId} · code ${formatPairingCode(offer.code)} · expires in ${minutes}m`;
	});
	const approved = (pairing?.trusted() ?? []).map((worker) => `${worker.workerId} (${worker.fingerprint})`);
	const notes: string[] = [];
	if (waiting.length > 0) {
		notes.push(
			[
				"waiting for approval (compare each code with the worker's screen, then /mqtt admit <worker> <code>):",
				...waiting,
			].join("\n"),
		);
	}
	if (approved.length > 0) notes.push(`approved by this phone: ${approved.join(", ")}`);
	return [header, ...rows, ...notes].join("\n");
}

function taskTable(): string {
	const current = room;
	if (!current) return inactiveReason();
	const records = current.list();
	if (records.length === 0) return "No tasks dispatched from this phone yet.";
	return records
		.map((record) => {
			const stamp = record.finishedAt ?? record.startedAt ?? record.sentAt ?? record.task.createdAt;
			const detail = record.error ?? record.result?.error ?? clip(record.result?.text ?? record.task.prompt, 80);
			return `${record.task.taskId}  ${record.task.to}  ${record.state}  ${formatAge(Date.now() - stamp)}  ${detail}`;
		})
		.join("\n");
}

async function toolConnect(ctx: CtxLike): Promise<string | undefined> {
	try {
		await ensureConnected(ctx);
		return undefined;
	} catch (error) {
		return (error as Error).message;
	}
}

/** Delete retained presence for sessions that are gone (zero-length retained publish). */
async function forgetIds(ids: string[]): Promise<{ cleared: string[]; skipped: string[] }> {
	const current = room;
	const link = client;
	if (!current || !link) return { cleared: [], skipped: ids };
	const t = current.topics;
	const cleared: string[] = [];
	for (const id of ids.map((value) => sanitizeId(value)).filter(Boolean)) {
		try {
			await link.publish(t.presence(id), "", { retain: true });
			await link.publish(t.orchestratorPresence(id), "", { retain: true });
			current.workers.delete(id);
			current.orchestrators.delete(id);
			cleared.push(id);
		} catch {
			// unreachable broker: leave it for next time
		}
	}
	if (cleared.length > 0) {
		log("info", `cleared retained presence for ${cleared.join(", ")}`);
		pushNow();
	}
	return { cleared, skipped: ids.filter((id) => !cleared.includes(sanitizeId(id))) };
}

function registerTools(): void {
	api.registerTool({
		name: "mqtt_workers",
		label: "List MQTT workers",
		description:
			"List the pi-mqtt workers and orchestrators in this phone's MQTT room: state (idle/busy/offline/pending), repo and branch, model, working directory, heartbeat age, and the approval codes of workers waiting to be admitted.",
		parameters: Type.Object({}),
		defaultActive: true,
		async execute(_id: string, _params: unknown, _signal: AbortSignal, _onUpdate: unknown, ctx: CtxLike) {
			const failure = await toolConnect(ctx);
			if (failure) return { content: [{ type: "text" as const, text: failure }], isError: true };
			const current = room;
			const notice = current && !current.workers.size ? `No workers in ${current.topics.base} yet — start one with: pi --pi-mqtt-role worker --name <name>` : undefined;
			const text = [workerTable(), notice].filter(Boolean).join("\n\n");
			return { content: [{ type: "text" as const, text }], details: { workers: snapshot().workers } };
		},
	});

	api.registerTool({
		name: "mqtt_dispatch",
		label: "Dispatch MQTT task",
		description:
			"Send a task to one named pi-mqtt worker from this phone. Waits for the result by default and returns it. The worker runs the prompt in its own pi session on its own machine.",
		promptSnippet: "mqtt_dispatch — hand work to a pi worker on another machine, from the phone",
		promptGuidelines: [
			"Use `mqtt_workers` to see which machines are reachable before dispatching; `mqtt_dispatch` fails on unknown, offline, stale or unapproved workers.",
			"A worker that just joined is `pending` until this phone approves its key: `mqtt_pair` lists the code to compare with the worker's screen, and admits it.",
			"`mqtt_dispatch` waits for the worker's final message by default, so do not also poll with `mqtt_result`.",
		],
		parameters: Type.Object({
			to: Type.String({ description: "Exact worker id from mqtt_workers, e.g. lap-b" }),
			prompt: Type.String({ description: "The task, in prose. Say what done looks like." }),
			wait: Type.Optional(Type.Boolean({ description: "Wait for the result and return it (default true); false lets it arrive as a message" })),
			waitTimeoutMs: Type.Optional(Type.Number({ description: `How long to wait before returning, default ${DEFAULT_WAIT_MS}` })),
			notify: Type.Optional(
				Type.Union([Type.Literal("silent"), Type.Literal("wake")], { description: "silent (default) posts a message; wake starts a turn when it lands" }),
			),
			ifBusy: Type.Optional(
				Type.Union([Type.Literal("fail"), Type.Literal("wait")], { description: "fail (default) errors; wait holds the task until the worker is idle" }),
			),
			timeoutMs: Type.Optional(Type.Number({ description: "Abort the worker's run after this long" })),
			maxCostUsd: Type.Optional(Type.Number({ description: "Abort the worker's run above this spend" })),
			model: Type.Optional(Type.String({ description: "Requested model, honoured only if the worker allows overrides" })),
			repo: Type.Optional(
				Type.Object({
					baseRef: Type.Optional(Type.String({ description: "Branch or ref to start from" })),
					baseSha: Type.Optional(Type.String({ description: "Exact commit to start from" })),
					branch: Type.Optional(Type.String({ description: "Branch to create" })),
				}),
			),
		}),
		defaultActive: true,
		async execute(_id: string, params: any, signal: AbortSignal, _onUpdate: unknown, ctx: CtxLike) {
			lastCtx = ctx;
			const failure = await toolConnect(ctx);
			if (failure) return { content: [{ type: "text" as const, text: failure }], isError: true };
			const current = room!;
			const outcome = current.dispatch({
				to: params.to,
				prompt: params.prompt,
				...(params.notify ? { notify: params.notify } : {}),
				...(params.ifBusy ? { ifBusy: params.ifBusy } : {}),
				...(params.timeoutMs !== undefined ? { timeoutMs: params.timeoutMs } : {}),
				...(params.maxCostUsd !== undefined ? { maxCostUsd: params.maxCostUsd } : {}),
				...(params.model ? { model: params.model } : {}),
				...(params.repo ? { repo: params.repo } : {}),
			});
			if (!outcome.ok) return { content: [{ type: "text" as const, text: outcome.error }], isError: true, details: { error: outcome.error } };
			const record = current.get(outcome.taskId)!;
			if (params.wait === false) {
				return {
					content: [{ type: "text" as const, text: `sent ${outcome.taskId} to ${params.to}; the result will arrive as a message.` }],
					details: { taskId: outcome.taskId, state: record.state, worker: params.to },
				};
			}
			record.inlineWanted = true;
			const timeout = typeof params.waitTimeoutMs === "number" ? params.waitTimeoutMs : DEFAULT_WAIT_MS;
			const finished = await current.waitFor(outcome.taskId, timeout, signal);
			record.inlineWanted = false;
			if (signal?.aborted) {
				return { content: [{ type: "text" as const, text: `stopped waiting for ${outcome.taskId}; the worker keeps running and the result will arrive as a message.` }] };
			}
			if (!finished) {
				return {
					content: [
						{
							type: "text" as const,
							text: `${outcome.taskId} is still running on ${params.to} after ${Math.round(timeout / 1000)}s (state ${record.state}); the result will arrive as a message. mqtt_result("${outcome.taskId}") has it later.`,
						},
					],
					details: { taskId: outcome.taskId, state: record.state, worker: params.to },
				};
			}
			return {
				content: [{ type: "text" as const, text: formatRecord(finished) }],
				isError: finished.state === "error" || finished.state === "lost" || finished.state === "timeout",
				details: { taskId: outcome.taskId, state: finished.state, worker: params.to },
			};
		},
	});

	api.registerTool({
		name: "mqtt_result",
		label: "MQTT task result",
		description: "Full result for a task dispatched from this phone: final text, diffstat, patch path, worker session file.",
		parameters: Type.Object({ taskId: Type.String({ description: "Task id, e.g. t_lq3..." }) }),
		defaultActive: true,
		async execute(_id: string, params: any, _signal: AbortSignal, _onUpdate: unknown, ctx: CtxLike) {
			lastCtx = ctx;
			const current = room;
			if (!current) return { content: [{ type: "text" as const, text: inactiveReason() }], isError: true };
			const record = current.get(params.taskId);
			if (!record) return { content: [{ type: "text" as const, text: `unknown task ${params.taskId} (mqtt_tasks lists them; tasks are per session)` }], isError: true };
			return { content: [{ type: "text" as const, text: formatRecord(record) }], details: { state: record.state } };
		},
	});

	api.registerTool({
		name: "mqtt_tasks",
		label: "List MQTT tasks",
		description: "Tasks dispatched from this phone and their states (queued, sent, running, done, error, timeout, cancelled, lost).",
		parameters: Type.Object({}),
		defaultActive: true,
		async execute(_id: string, _params: unknown, _signal: AbortSignal, _onUpdate: unknown, ctx: CtxLike) {
			lastCtx = ctx;
			return { content: [{ type: "text" as const, text: taskTable() }] };
		},
	});

	api.registerTool({
		name: "mqtt_cancel",
		label: "Cancel MQTT task",
		description: "Ask a worker to stop a task dispatched from this phone (cooperative: the worker reports partial output).",
		parameters: Type.Object({ taskId: Type.String({ description: "Task id to cancel" }) }),
		defaultActive: true,
		async execute(_id: string, params: any, _signal: AbortSignal, _onUpdate: unknown, ctx: CtxLike) {
			lastCtx = ctx;
			const current = room;
			if (!current) return { content: [{ type: "text" as const, text: inactiveReason() }], isError: true };
			const outcome = current.cancel(params.taskId);
			return {
				content: [{ type: "text" as const, text: outcome.ok ? `cancel sent for ${params.taskId}` : (outcome.error ?? "cancel failed") }],
				isError: !outcome.ok,
			};
		},
	});

	api.registerTool({
		name: "mqtt_forget",
		label: "Forget MQTT sessions",
		description:
			"Delete the retained presence entries of sessions that are gone (a worker that died days ago, a renamed session). They leave the roster; a live session re-announces itself on its next heartbeat.",
		parameters: Type.Object({
			ids: Type.Array(Type.String({ description: "Worker or orchestrator ids to forget" })),
		}),
		defaultActive: true,
		async execute(_id: string, params: any, _signal: AbortSignal, _onUpdate: unknown, ctx: CtxLike) {
			lastCtx = ctx;
			const failure = await toolConnect(ctx);
			if (failure) return { content: [{ type: "text" as const, text: failure }], isError: true };
			const outcome = await forgetIds(params.ids ?? []);
			const text =
				outcome.cleared.length > 0
					? `forgot ${outcome.cleared.join(", ")}${outcome.skipped.length ? ` (could not clear ${outcome.skipped.join(", ")})` : ""}`
					: "nothing cleared — the broker did not accept the deletions";
			return { content: [{ type: "text" as const, text }], details: outcome };
		},
	});

	api.registerTool({
		name: "mqtt_pair",
		label: "Approve MQTT workers",
		description:
			"Worker admission on this phone's MQTT room. A worker joins as pending and cannot be dispatched to until this phone approves its key: `pending` lists the workers waiting and the six-digit code to compare with the worker's screen, `admit` approves one with that code, `revoke` stops trusting one, `trusted` lists the approved keys.",
		promptSnippet: "mqtt_pair — approve a pi worker's key before dispatching to it",
		promptGuidelines: [
			"A dispatch refused with 'waiting for approval' means mqtt_pair(action: \"pending\") shows a code the user compares with the worker's own screen; then mqtt_pair(action: \"admit\", worker, code).",
			"Never invent a code: without one from the worker's screen, ask the user to compare.",
		],
		parameters: Type.Object({
			action: Type.Union(
				[Type.Literal("pending"), Type.Literal("admit"), Type.Literal("revoke"), Type.Literal("trusted")],
				{ description: "pending (default) lists the codes waiting; admit and revoke act on one worker" },
			),
			worker: Type.Optional(Type.String({ description: "Worker id, e.g. lap-b" })),
			code: Type.Optional(Type.String({ description: "The six digits shown on both screens, e.g. 481902 or '481 902'" })),
		}),
		defaultActive: true,
		async execute(_id: string, params: any, _signal: AbortSignal, _onUpdate: unknown, ctx: CtxLike) {
			lastCtx = ctx;
			const failure = await toolConnect(ctx);
			if (failure) return { content: [{ type: "text" as const, text: failure }], isError: true };
			const current = pairing;
			if (!current) return { content: [{ type: "text" as const, text: "worker admission is not active: /mqtt connect first" }], isError: true };
			const action = params.action ?? "pending";

			if (action === "pending") {
				const offers = current.list();
				if (offers.length === 0) {
					return {
						content: [{ type: "text" as const, text: "No worker is waiting for approval. A worker shows a code here once it joins." }],
						details: { offers: [] },
					};
				}
				const text = offers
					.map((offer) => {
						const minutes = Math.max(0, Math.round((offer.expiresAt - Date.now()) / 60_000));
						return `${offer.workerId} · code ${formatPairingCode(offer.code)} · expires in ${minutes}m`;
					})
					.join("\n");
				return {
					content: [
						{
							type: "text" as const,
							text: `waiting for approval (compare each code with the worker's screen, then mqtt_pair admit):\n${text}`,
						},
					],
					details: { offers: offers.map(({ workerId, code, expiresAt }) => ({ workerId, code, expiresAt })) },
				};
			}

			if (action === "trusted") {
				const trusted = current.trusted();
				const text =
					trusted.length === 0
						? `No worker key is approved by this phone in ${settings.room} yet.`
						: trusted
								.map(
									(worker) =>
										`${worker.workerId} · ${worker.fingerprint} · approved ${formatAge(Date.now() - worker.admittedAt)} ago`,
								)
								.join("\n");
				return { content: [{ type: "text" as const, text }], details: { trusted } };
			}

			const workerId = sanitizeId(String(params.worker ?? ""));
			if (!workerId) return { content: [{ type: "text" as const, text: `which worker? mqtt_pair ${action} needs worker` }], isError: true };
			if (action === "revoke") {
				const outcome = current.revoke(workerId);
				return {
					content: [
						{
							type: "text" as const,
							text: outcome.ok ? `revoked "${workerId}": it goes back to pending on its next heartbeat` : (outcome.error ?? "revoke failed"),
						},
					],
					isError: !outcome.ok,
				};
			}
			const typed = String(params.code ?? "").replace(/\s+/g, "");
			if (!typed) return { content: [{ type: "text" as const, text: "admit needs the code shown on the worker's screen (mqtt_pair pending lists it)" }], isError: true };
			const outcome = current.admit(workerId, typed);
			return {
				content: [
					{
						type: "text" as const,
						text: outcome.ok ? `approved "${workerId}"; it becomes idle within a heartbeat and can take tasks` : (outcome.error ?? "admit failed"),
					},
				],
				isError: !outcome.ok,
			};
		},
	});

	api.registerTool({
		name: "mqtt_status",
		label: "MQTT status",
		description: "This phone's MQTT room connection: broker, room, device name, whether the link is up, and how many workers are reachable.",
		parameters: Type.Object({}),
		defaultActive: true,
		async execute(_id: string, _params: unknown, _signal: AbortSignal, _onUpdate: unknown, ctx: CtxLike) {
			lastCtx = ctx;
			const snap = snapshot();
			const lines = [
				`device:  ${snap.me} (orchestrator)`,
				`broker:  ${snap.url} as ${snap.settings.username}`,
				`room:    ${snap.room}  →  pi/v${PROTOCOL_VERSION}/${snap.room}`,
				`state:   ${snap.status}${snap.lastError ? ` — last drop ${snap.lastError}` : ""}`,
				...(client ? [`link:    ${client.online ? "up" : "down"}, ${client.reconnects} reconnect(s) since launch`] : []),
				`secret:  ${snap.hasPassword ? `${SECRET_NAME} is set` : "no room password yet (/mqtt password)"}`,
				`workers: ${snap.workers.filter((worker) => !worker.stale && worker.state !== "offline" && worker.state !== "pending").length} reachable of ${snap.workers.length} known`,
				...(pairing && pairing.list().length > 0
					? [`waiting: ${pairing.list().map((offer) => `${offer.workerId} ${formatPairingCode(offer.code)}`).join(", ")} — /mqtt admit <worker> <code>`]
					: []),
				`tasks:   ${snap.tasks.filter((task) => ["queued", "sent", "running"].includes(task.state)).length} outstanding`,
			];
			return { content: [{ type: "text" as const, text: lines.join("\n") }], details: snap };
		},
	});
}

async function setupRoom(ctx: CtxLike): Promise<void> {
	const ui = ctx.ui;
	if (!ui) throw new Error("setup needs the app's dialogs");
	const url = (await ui.input("MQTT broker URL (WebSocket)", settings.url)) ?? settings.url;
	const roomName = (await ui.input("Room name", settings.room)) ?? settings.room;
	const name = (await ui.input("Name for this phone in the roster", settings.name)) ?? settings.name;
	const username =
		(await ui.input("Broker user — the one with the orchestrator role", settings.username)) ?? settings.username;
	await applySettings({
		url: String(url).trim(),
		room: String(roomName).trim(),
		name: String(name).trim(),
		username: String(username).trim() || settings.username,
	});
	const password = await ui.input("Room password (stored in the Keychain)", "shared by every session in the room");
	if (password && String(password).trim()) await storePassword(String(password));
	ui.notify(`mqtt: ${settings.name} → ${settings.room} at ${settings.url}`, "info");
	pushNow();
}

function showStatus(ctx: CtxLike): void {
	const snap = snapshot();
	const lines = [
		`device:  ${snap.me}`,
		`broker:  ${snap.url} as ${snap.settings.username}`,
		`room:    ${snap.room}`,
		`state:   ${snap.status}${snap.lastError ? ` (last drop ${snap.lastError})` : ""}`,
		`workers: ${snap.workers.length} known, ${snap.workers.filter((w) => !w.stale && w.state !== "offline" && w.state !== "pending").length} reachable`,
		...(pairing && pairing.list().length > 0
			? [`waiting: ${pairing.list().map((offer) => `${offer.workerId} ${formatPairingCode(offer.code)}`).join(", ")}`]
			: []),
		`tasks:   ${snap.tasks.length} dispatched from this phone`,
	];
	ctx.ui?.notify(lines.join("\n"), "info");
}

/** Saving a different URL, room, user or name means the current link is the wrong one. */
async function applySettings(patch: Partial<Settings>): Promise<void> {
	const before = settings;
	const needsRejoin =
		room !== undefined &&
		((patch.url !== undefined && patch.url.trim() !== before.url) ||
			(patch.room !== undefined && patch.room.trim() !== before.room) ||
			(patch.name !== undefined && patch.name.trim() !== before.name) ||
			(patch.username !== undefined && patch.username.trim() !== before.username));
	writeSettings(patch);
	if (needsRejoin) await disconnectRoom("settings changed; /mqtt connect to rejoin");
}

function registerCommand(): void {
	api.registerCommand("mqtt", {
		description:
			"MQTT on iOS: /mqtt status|connect|disconnect|workers|tasks|pending|admit|revoke|trusted|forget|setup|password|app|notify on|off",
		handler: async (args: string, ctx: CtxLike & { signal?: AbortSignal }) => {
			lastCtx = ctx;
			const [head, ...rest] = args.trim().split(/\s+/);
			const arg = (head ?? "").toLowerCase();
			const tail = rest.join(" ").trim();

			if (arg === "" || arg === "status") {
				showStatus(ctx);
				return;
			}
			if (arg === "connect") {
				try {
					await ensureConnected(ctx);
					ctx.ui?.notify(`mqtt: joined ${settings.room} as ${settings.name}`, "info");
				} catch (error) {
					ctx.ui?.notify(`mqtt: ${(error as Error).message}`, "error");
				}
				return;
			}
			if (arg === "disconnect") {
				await disconnectRoom("left the room (/mqtt disconnect)");
				ctx.ui?.notify("mqtt: disconnected", "info");
				return;
			}
			if (arg === "workers") {
				ctx.ui?.notify(workerTable(), "info");
				return;
			}
			if (arg === "tasks") {
				ctx.ui?.notify(taskTable(), "info");
				return;
			}
			if (arg === "pending") {
				const offers = pairing?.list() ?? [];
				if (offers.length === 0) {
					ctx.ui?.notify("mqtt: no worker is waiting for approval", "info");
					return;
				}
				const text = offers
					.map((offer) => {
						const minutes = Math.max(0, Math.round((offer.expiresAt - Date.now()) / 60_000));
						return `  ${offer.workerId} · code ${formatPairingCode(offer.code)} · expires in ${minutes}m`;
					})
					.join("\n");
				ctx.ui?.notify(
					`waiting for approval (compare each code with the worker's screen, then /mqtt admit <worker> <code>):\n${text}`,
					"warning",
				);
				return;
			}
			if (arg === "admit") {
				const [workerId, code] = tail.split(/\s+/);
				if (!workerId || !code) {
					ctx.ui?.notify("mqtt: /mqtt admit <worker> <code> — codes: /mqtt pending", "warning");
					return;
				}
				if (!pairing) {
					ctx.ui?.notify("mqtt: not connected — /mqtt connect first", "warning");
					return;
				}
				const outcome = pairing.admit(workerId, code);
				ctx.ui?.notify(
					outcome.ok ? `mqtt: approved "${workerId}" — it takes tasks within a heartbeat` : `mqtt: ${outcome.error}`,
					outcome.ok ? "info" : "error",
				);
				pushNow();
				return;
			}
			if (arg === "revoke") {
				const workerId = tail.split(/\s+/)[0];
				if (!workerId) {
					ctx.ui?.notify("mqtt: /mqtt revoke <worker>", "warning");
					return;
				}
				if (!pairing) {
					ctx.ui?.notify("mqtt: not connected — /mqtt connect first", "warning");
					return;
				}
				const outcome = pairing.revoke(workerId);
				ctx.ui?.notify(outcome.ok ? `mqtt: revoked "${workerId}"` : `mqtt: ${outcome.error}`, outcome.ok ? "info" : "error");
				pushNow();
				return;
			}
			if (arg === "trusted") {
				const trusted = pairing?.trusted() ?? [];
				ctx.ui?.notify(
					trusted.length === 0
						? "mqtt: no worker key is approved by this phone yet"
						: `approved:\n${trusted.map((worker) => `  ${worker.workerId} · ${worker.fingerprint}`).join("\n")}`,
					"info",
				);
				return;
			}
			if (arg === "setup") {
				await setupRoom(ctx);
				return;
			}
			if (arg === "password") {
				const entered = await ctx.ui?.input("MQTT room password", "shared by every session in the room");
				if (entered && entered.trim()) {
					await storePassword(entered);
					ctx.ui?.notify("mqtt: password saved; /mqtt connect to (re)join", "info");
				}
				return;
			}
			if (arg === "notify") {
				const on = tail !== "off";
				writeSettings({ notify: on });
				ctx.ui?.notify(`mqtt: notifications ${on ? "on" : "off"}`, "info");
				return;
			}
			if (arg === "forget") {
				const ids = tail.split(/[\s,]+/).filter(Boolean);
				if (ids.length === 0) {
					ctx.ui?.notify("mqtt: /mqtt forget <worker-id>[,<id>…] — clears retained presence of sessions that are gone", "info");
					return;
				}
				if (!room) {
					ctx.ui?.notify("mqtt: not connected — /mqtt connect first", "warning");
					return;
				}
				const outcome = await forgetIds(ids);
				ctx.ui?.notify(
					outcome.cleared.length > 0 ? `mqtt: forgot ${outcome.cleared.join(", ")}` : "mqtt: nothing to forget",
					"info",
				);
				return;
			}
			if (arg === "app") {
				await openApp();
				return;
			}
			ctx.ui?.notify(
				"mqtt: /mqtt status · connect · disconnect · workers · tasks · pending · admit <worker> <code> · revoke <worker> · trusted · forget <id> · setup · password · notify on|off · app",
				"info",
			);
		},
	});
}

// The app page lives in src/page.ts (one document, no libraries).

async function openApp(): Promise<void> {
	if (!appHandle) {
		appHandle = await api.apps.open("mqtt");
		return;
	}
	try {
		appHandle.show();
	} catch {
		appHandle = await api.apps.open("mqtt");
	}
}

function registerApp(): void {
	api.registerApp({
		name: "mqtt",
		title: "MQTT",
		icon: "antenna.radiowaves.left.and.right",
		description: "Workers on the MQTT room, and a place to send them tasks from the phone",
		render: (app: any) => appHtml(app?.state ?? snapshot()),
		onOpen: (app: any) => {
			appHandle = app;
			if (app?.ui) lastCtx = { ui: app.ui };
			pushNow();
			// Opening the app is a request to use the room — but only join silently if the
			// password is already in the Keychain. A dialog on open would be a surprise.
			void readPassword().then((password) => {
				passwordKnown = Boolean(password);
				pushNow();
				if (!password) return;
				ensureConnected({ ui: app?.ui }).catch((error: Error) => {
					try {
						app?.ui?.notify(`mqtt: ${error.message}`, "error");
					} catch {
						// ignore
					}
				});
			});
		},
		onMessage: (data: any, app: any) => {
			if (app?.ui) lastCtx = { ui: app.ui };
			const kind = data?.type;
			if (kind === "connect") {
				ensureConnected({ ui: app?.ui })
					.then(() => app?.post({ type: "toast", text: "joined " + settings.room }))
					.catch((error: Error) => app?.post({ type: "toast", text: error.message }));
				return;
			}
			if (kind === "disconnect") {
				void disconnectRoom("left the room (from the app)").then(() => app?.post({ type: "toast", text: "disconnected" }));
				return;
			}
			if (kind === "dispatch") {
				void (async () => {
					try {
						await ensureConnected({ ui: app?.ui });
						const outcome = room!.dispatch({
							to: String(data.to),
							prompt: String(data.prompt),
							notify: data.wake ? "wake" : "silent",
						});
						app?.post({ type: "toast", text: outcome.ok ? `sent to ${data.to}` : outcome.error });
					} catch (error) {
						app?.post({ type: "toast", text: (error as Error).message });
					}
				})();
				return;
			}
			if (kind === "cancel") {
				const outcome = room?.cancel(String(data.taskId));
				app?.post({ type: "toast", text: outcome?.ok ? "cancel requested" : outcome?.error ?? "not connected" });
				return;
			}
			if (kind === "settings") {
				void applySettings({
					url: String(data.settings?.url ?? settings.url),
					room: String(data.settings?.room ?? settings.room),
					name: String(data.settings?.name ?? settings.name),
					username: String(data.settings?.username ?? settings.username),
					notify: data.settings?.notify === true,
				}).then(() => app?.post({ type: "toast", text: "saved" }));
				return;
			}
			if (kind === "password") {
				void (async () => {
					const entered = await app?.ui?.input("MQTT room password", "shared by every session in the room");
					if (entered && String(entered).trim()) {
						await storePassword(String(entered));
						app?.post({ type: "toast", text: "password saved" });
						pushNow();
					}
				})();
				return;
			}
			if (kind === "admit") {
				const worker = sanitizeId(String(data.worker ?? ""));
				const outcome = pairing?.admit(worker, String(data.code ?? ""));
				app?.post({ type: "toast", text: outcome?.ok ? `approved ${worker}` : outcome?.error ?? "not connected" });
				pushNow();
				return;
			}
			if (kind === "revoke") {
				const worker = sanitizeId(String(data.worker ?? ""));
				const outcome = pairing?.revoke(worker);
				app?.post({ type: "toast", text: outcome?.ok ? `revoked ${worker}` : outcome?.error ?? "not connected" });
				pushNow();
				return;
			}
			if (kind === "refresh") {
				pushNow();
			}
		},
	});
}
// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

export default function (pi: any) {
	// A reload or a re-add replaces this module but not the timers of the instance it
	// replaced: an orphan would keep reconnecting (and, before this, keep reporting every
	// drop). Tear every previous instance down before joining the room again.
	const cleanups: Array<() => void> = ((globalThis as any).__piMqttIosInstances ??= []);
	for (const stop of cleanups.splice(0)) {
		try {
			stop();
		} catch {
			// the previous instance was already gone
		}
	}
	cleanups.push(() => {
		void disconnectRoom(undefined, { quiet: true });
	});

	api = pi;
	settings = readSettings();
	void readPassword().then((password) => {
		passwordKnown = Boolean(password);
	});

	registerTools();
	registerCommand();
	registerApp();


	api.on("session_shutdown", () => {
		void disconnectRoom(undefined, { quiet: true });
		const cleanups: Array<() => void> = ((globalThis as any).__piMqttIosInstances ??= []);
		cleanups.length = 0;
	});

	api.on("session_start", (ctx: any) => {
		if (ctx?.ui) lastCtx = ctx;
		void readPassword().then((password) => {
			passwordKnown = Boolean(password);
			schedulePush();
		});
	});
}
