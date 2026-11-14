/**
 * The orchestrator side of a room: retained presence plus heartbeat, the worker
 * registry, task dispatch with ack retry, and results.
 *
 * All decision state lives here: who was targeted, what was sent, whether it
 * started, what came back. The broker carries messages; it does not remember intent.
 */

import { MqttWs } from "./mqtt-ws.ts";
import {
	PROTOCOL_VERSION,
	buildPresence,
	isAck,
	isKick,
	isPresence,
	isResult,
	parseJson,
	type PresenceIdentity,
	type PresenceMessage,
	type RepoSpec,
	type ResultMessage,
	type TaskMessage,
	type TaskStatus,
	type Topics,
	type WorkerState,
} from "./protocol.ts";
import { formatAge, newTaskId, type LogFn } from "./util.ts";

const STALE_FACTOR = 3;
/** A worker that never acknowledges touched nothing, so a re-send is safe. */
const ACK_TIMEOUT_MS = 5_000;
const MAX_ATTEMPTS = 2;

// ---------------------------------------------------------------------------
// Room: roster, presence heartbeat, task dispatch, results
// ---------------------------------------------------------------------------

export interface TaskRecord {
	task: TaskMessage;
	state: TaskState;
	attempts: number;
	sentAt?: number;
	startedAt?: number;
	finishedAt?: number;
	result?: ResultMessage;
	error?: string;
	waiting?: boolean;
	/** Set before a tool waits, so the result is not also posted as a message. */
	inlineWanted?: boolean;
}

export interface WorkerEntry {
	presence: PresenceMessage;
	stale: boolean;
	ageMs: number;
}

export interface DispatchParams {
	to: string;
	prompt: string;
	notify?: "silent" | "wake";
	ifBusy?: "fail" | "wait";
	repo?: RepoSpec;
	timeoutMs?: number;
	maxCostUsd?: number;
	model?: string;
	artifacts?: "none" | "paths";
}

/** The orchestrator side of a room: what a desktop pi-mqtt orchestrator also does. */
export class Room {
	readonly workers = new Map<string, PresenceMessage>();
	readonly orchestrators = new Map<string, PresenceMessage>();
	readonly tasks = new Map<string, TaskRecord>();
	private readonly unsubscribes: Array<() => void> = [];
	private readonly ackTimers = new Map<string, any>();
	private readonly waiters = new Map<string, Array<(record: TaskRecord) => void>>();
	private heartbeat: any;
	private stopped = false;
	private gotPresence = false;
	private lastPresenceAt = 0;
	/** Leaving the room: unfinished tasks are marked lost without posting messages. */
	private leaving = false;
	private current: PresenceMessage;

	constructor(
		private readonly client: MqttWs,
		readonly topics: Topics,
		private readonly identity: PresenceIdentity,
		private readonly heartbeatMs: number,
		private readonly log: LogFn,
		private readonly onChange: () => void,
		private readonly onFinished: (record: TaskRecord) => void,
		private readonly onKick: (from: string, reason?: string) => void,
	) {
		this.current = buildPresence(identity, "idle", "orchestrator");
	}

	start(): void {
		this.stopped = false;
		this.unsubscribes.push(
			this.client.subscribe(this.topics.presenceFilter, (topic, payload) => {
				if (payload === undefined) {
					const id = idFromPresenceTopic(topic);
					if (id && this.workers.delete(id)) this.onChange();
					return;
				}
				const decoded = parseJson(payload);
				if (!decoded || !isPresence(decoded)) return;
				this.workers.set(decoded.workerId, decoded);
				this.gotPresence = true;
				this.lastPresenceAt = Date.now();
				this.onWorkerState(decoded.workerId);
				this.onChange();
			}),
			this.client.subscribe(this.topics.orchestratorFilter, (topic, payload) => {
				if (payload === undefined) {
					const id = idFromPresenceTopic(topic);
					if (id && this.orchestrators.delete(id)) this.onChange();
					return;
				}
				const decoded = parseJson(payload);
				if (!decoded || !isPresence(decoded)) return;
				this.orchestrators.set(decoded.workerId, decoded);
				this.onChange();
			}),
			this.client.subscribe(this.topics.ackFilter, (_topic, payload) => {
				if (payload === undefined) return;
				const decoded = parseJson(payload);
				if (!decoded || !isAck(decoded)) return;
				const record = this.tasks.get(decoded.taskId);
				if (!record) return;
				record.state = "running";
				record.startedAt = decoded.startedAt ?? Date.now();
				this.clearAckTimer(decoded.taskId);
				this.onChange();
			}),
			this.client.subscribe(this.topics.resultFilter, (_topic, payload) => {
				if (payload === undefined) return;
				const decoded = parseJson(payload);
				if (!decoded || !isResult(decoded)) {
					this.log("warn", "ignoring a malformed result message");
					return;
				}
				const record = this.tasks.get(decoded.taskId);
				if (!record) return;
				// The broker cannot tell sessions apart (one shared credential), so the
				// remaining authenticity check is in the payload.
				if (decoded.workerId !== record.task.to) {
					this.log("warn", `ignoring result for ${decoded.taskId} from ${decoded.workerId}: pinned to ${record.task.to}`);
					return;
				}
				this.finish(record, decoded);
			}),
			this.client.subscribe(this.topics.roomControl, (_topic, payload) => {
				if (payload === undefined) return;
				const decoded = parseJson(payload);
				if (!decoded || !isKick(decoded)) return;
				if (decoded.from === this.identity.workerId) return;
				const why = decoded.reason ? `: ${decoded.reason}` : "";
				this.log("warn", `${decoded.from} removed everyone from the room${why}`);
				this.onKick(decoded.from, decoded.reason);
			}),
		);
		this.publishPresence();
		this.heartbeat = setInterval(() => this.publishPresence(), this.heartbeatMs);
	}

	/**
	 * Give retained presence a moment to arrive.
	 *
	 * A SUBSCRIBE for a retained topic gets its messages a round trip later, so a
	 * roster read taken in the same tick as `connect()` is empty even though workers
	 * are there. Tools await this before reporting "no workers".
	 */
	waitForRoster(ms = 1200): Promise<void> {
		// Retained presence arrives as a burst of separate frames, so "the first one
		// landed" is not the same as "the roster is complete": wait until the traffic has
		// been quiet for a moment, with a hard ceiling so a silent room still returns.
		const started = Date.now();
		return new Promise((resolve) => {
			const tick = () => {
				const quiet = !this.gotPresence || Date.now() - this.lastPresenceAt > 150;
				if ((this.gotPresence && quiet) || Date.now() - started >= ms) {
					resolve();
					return;
				}
				setTimeout(tick, 50);
			};
			tick();
		});
	}


	/** Re-announce now (after a reconnect) instead of waiting for the beat. */
	republish(): void {
		if (this.stopped) return;
		this.publishPresence();
	}

	private publishPresence(): void {
		if (this.stopped || !this.client.online) return;
		this.current = { ...this.current, updatedAt: Date.now() };
		this.client.publishJson(this.topics.orchestratorPresence(this.identity.workerId), this.current, { retain: true }).catch((error: Error) => {
			this.log("warn", `presence publish failed: ${error.message}`);
		});
	}

	/** Leave the room: clear the retained entry, stop the beat, drop waiters. */
	async stop(options: { quiet?: boolean } = {}): Promise<void> {
		if (this.stopped) return;
		this.stopped = true;
		this.leaving = options.quiet === true;
		if (this.heartbeat) clearInterval(this.heartbeat);
		this.heartbeat = undefined;
		for (const unsubscribe of this.unsubscribes) unsubscribe();
		this.unsubscribes.length = 0;
		for (const timer of this.ackTimers.values()) clearTimeout(timer);
		this.ackTimers.clear();
		if (this.client.online) {
			try {
				await this.client.publish(this.topics.orchestratorPresence(this.identity.workerId), "", { retain: true });
			} catch {
				// an unreachable broker will expire the retained entry on its own
			}
		}
		for (const record of this.tasks.values()) {
			if (record.finishedAt) continue;
			this.finishLocally(record, "lost", "left the room");
		}
	}

	entries(): WorkerEntry[] {
		const now = Date.now();
		const staleAfter = this.heartbeatMs * STALE_FACTOR;
		return [...this.workers.values()]
			.map((presence) => {
				const ageMs = now - presence.updatedAt;
				return { presence, ageMs, stale: presence.state !== "offline" && ageMs > staleAfter };
			})
			.sort((a, b) => a.presence.workerId.localeCompare(b.presence.workerId));
	}

	orchestratorEntries(): WorkerEntry[] {
		const now = Date.now();
		const staleAfter = this.heartbeatMs * STALE_FACTOR;
		return [...this.orchestrators.values()]
			.map((presence) => {
				const ageMs = now - presence.updatedAt;
				return { presence, ageMs, stale: ageMs > staleAfter };
			})
			.sort((a, b) => a.presence.workerId.localeCompare(b.presence.workerId));
	}

	list(): TaskRecord[] {
		return [...this.tasks.values()].sort((a, b) => b.task.createdAt - a.task.createdAt);
	}

	get(taskId: string): TaskRecord | undefined {
		return this.tasks.get(taskId);
	}

	dispatch(params: DispatchParams): { ok: true; taskId: string; state: TaskState } | { ok: false; error: string } {
		if (!this.client.online) {
			const dropped = this.client.lastClose;
			const since = dropped ? ` (dropped ${formatAge(Date.now() - dropped.at)} ago: ${dropped.reason})` : "";
			return { ok: false, error: `not connected to the broker${since}: the roster is frozen — /mqtt connect first` };
		}
		const entry = this.entries().find((item) => item.presence.workerId === params.to);
		if (!entry) return { ok: false, error: `unknown worker "${params.to}" (try /mqtt workers)` };
		if (entry.stale) {
			return { ok: false, error: `worker "${params.to}" is stale (last heartbeat ${Math.round(entry.ageMs / 1000)}s ago)` };
		}
		if (entry.presence.state === "offline") return { ok: false, error: `worker "${params.to}" is offline` };

		const task: TaskMessage = {
			v: PROTOCOL_VERSION,
			kind: "task",
			taskId: newTaskId(),
			to: params.to,
			prompt: params.prompt,
			notify: params.notify ?? "silent",
			createdAt: Date.now(),
			...(params.repo ? { repo: params.repo } : {}),
			...(params.timeoutMs !== undefined ? { timeoutMs: params.timeoutMs } : {}),
			...(params.maxCostUsd !== undefined ? { maxCostUsd: params.maxCostUsd } : {}),
			...(params.model ? { model: params.model } : {}),
			...(params.artifacts ? { artifacts: params.artifacts } : {}),
		};
		const record: TaskRecord = { task, state: "queued", attempts: 0 };
		this.tasks.set(task.taskId, record);

		if (entry.presence.state === "busy" && params.ifBusy !== "wait") {
			this.tasks.delete(task.taskId);
			return { ok: false, error: `worker "${params.to}" is busy with ${entry.presence.activeTaskId ?? "a task"}` };
		}
		if (entry.presence.state === "busy") {
			record.waiting = true;
			this.onChange();
			return { ok: true, taskId: task.taskId, state: "queued" };
		}
		this.send(record);
		return { ok: true, taskId: task.taskId, state: record.state };
	}

	private send(record: TaskRecord): void {
		record.attempts += 1;
		record.state = "sent";
		record.sentAt = Date.now();
		record.waiting = false;
		this.onChange();
		this.client.publishJson(this.topics.control(record.task.to), record.task, { qos: 1 }).catch((error: Error) => {
			this.finishLocally(record, "lost", `publish failed: ${error.message}`);
		});

		this.clearAckTimer(record.task.taskId);
		const timer = setTimeout(() => {
			this.ackTimers.delete(record.task.taskId);
			if (record.state !== "sent") return;
			const entry = this.entries().find((item) => item.presence.workerId === record.task.to);
			const reachable = entry && !entry.stale && entry.presence.state !== "offline";
			if (reachable && record.attempts < MAX_ATTEMPTS) {
				this.log("warn", `no ack for ${record.task.taskId}; re-sending`);
				this.send(record);
				return;
			}
			this.finishLocally(record, "lost", `no ack from ${record.task.to} within ${ACK_TIMEOUT_MS / 1000}s`);
		}, ACK_TIMEOUT_MS);
		this.ackTimers.set(record.task.taskId, timer);
	}

	/** A held task goes out the moment its worker is idle again. */
	private onWorkerState(workerId: string): void {
		const entry = this.entries().find((item) => item.presence.workerId === workerId);
		if (!entry || entry.presence.state !== "idle") return;
		for (const record of this.tasks.values()) {
			if (record.task.to !== workerId || !record.waiting) continue;
			this.log("info", `worker ${workerId} is idle; sending queued ${record.task.taskId}`);
			this.send(record);
		}
	}

	cancel(taskId: string): { ok: boolean; error?: string } {
		const record = this.tasks.get(taskId);
		if (!record) return { ok: false, error: `unknown task ${taskId}` };
		if (record.waiting || record.state === "queued") {
			record.waiting = false;
			this.finishLocally(record, "cancelled", "cancelled before dispatch");
			return { ok: true };
		}
		if (["done", "error", "timeout", "cancelled", "lost"].includes(record.state)) {
			return { ok: false, error: `task ${taskId} already finished (${record.state})` };
		}
		this.client
			.publishJson(this.topics.control(record.task.to), { v: PROTOCOL_VERSION, kind: "cancel", taskId, reason: "cancelled from pi on iOS" })
			.catch((error: Error) => this.log("warn", `cancel publish failed: ${error.message}`));
		return { ok: true };
	}

	/** Wait for a terminal state, or for the wait itself to run out. */
	waitFor(taskId: string, timeoutMs: number, signal?: AbortSignal): Promise<TaskRecord | undefined> {
		const record = this.tasks.get(taskId);
		if (!record) return Promise.resolve(undefined);
		if (record.finishedAt) return Promise.resolve(record);

		return new Promise((resolve) => {
			let settled = false;
			const finish = (value: TaskRecord | undefined) => {
				if (settled) return;
				settled = true;
				clearTimeout(timer);
				signal?.removeEventListener("abort", onAbort);
				const list = this.waiters.get(taskId);
				if (list) {
					const index = list.indexOf(waiter);
					if (index >= 0) list.splice(index, 1);
				}
				resolve(value);
			};
			const waiter = (finished: TaskRecord) => finish(finished);
			const onAbort = () => finish(undefined);
			const timer = setTimeout(() => finish(undefined), timeoutMs);
			if (signal?.aborted) {
				finish(undefined);
				return;
			}
			signal?.addEventListener("abort", onAbort, { once: true });
			const list = this.waiters.get(taskId) ?? [];
			list.push(waiter);
			this.waiters.set(taskId, list);
		});
	}

	private finish(record: TaskRecord, result: ResultMessage): void {
		if (record.finishedAt) return; // duplicate (QoS 1) or two sessions under one id
		this.prune();
		this.clearAckTimer(record.task.taskId);
		record.result = result;
		record.state = result.status;
		record.finishedAt = result.finishedAt;
		this.notifyWaiters(record);
		this.onChange();
		this.onFinished(record);
	}

	private finishLocally(record: TaskRecord, state: TaskState, error: string): void {
		this.clearAckTimer(record.task.taskId);
		record.state = state;
		record.error = error;
		record.finishedAt = Date.now();
		this.notifyWaiters(record);
		this.onChange();
		if (!this.leaving) this.onFinished(record);
	}

	/** Keep the last 50 finished tasks; the roster is small, the page is not a database. */
	private prune(): void {
		if (this.tasks.size <= 60) return;
		const finished = [...this.tasks.values()]
			.filter((record) => record.finishedAt)
			.sort((a, b) => (a.finishedAt ?? 0) - (b.finishedAt ?? 0));
		for (const record of finished.slice(0, finished.length - 50)) this.tasks.delete(record.task.taskId);
	}

	private notifyWaiters(record: TaskRecord): void {
		const list = this.waiters.get(record.task.taskId);
		if (!list) return;
		this.waiters.delete(record.task.taskId);
		for (const waiter of list) waiter(record);
	}

	private clearAckTimer(taskId: string): void {
		const timer = this.ackTimers.get(taskId);
		if (!timer) return;
		clearTimeout(timer);
		this.ackTimers.delete(taskId);
	}
}

function idFromPresenceTopic(topic: string): string | undefined {
	const match = topic.match(/\/(?:workers|orchestrators)\/([^/]+)\/presence$/);
	return match?.[1];
}
