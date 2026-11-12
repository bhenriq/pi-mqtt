/**
 * Orchestrator: worker registry plus task dispatch.
 *
 * All decision state lives here rather than on the worker: who was targeted, what
 * was sent, whether it started, what came back. The broker carries the messages;
 * it does not remember the intent.
 */

import type { LogFn, MqttLink } from "./mqtt.ts";
import {
	isAckMessage,
	isPresenceMessage,
	isResultMessage,
	newTaskId,
	type PresenceMessage,
	type RepoSpec,
	type ResultMessage,
	type TaskMessage,
	type TaskStatus,
	type Topics,
} from "./protocol.ts";

export const STALE_FACTOR = 3;
/** A worker that never acknowledges is safe to re-send: it touched nothing. */
export const ACK_TIMEOUT_MS = 5000;
export const MAX_ATTEMPTS = 2;

export type TaskState =
	| "queued"
	| "sent"
	| "running"
	| TaskStatus
	| "lost";

export interface TaskRecord {
	task: TaskMessage;
	state: TaskState;
	attempts: number;
	sentAt?: number;
	startedAt?: number;
	finishedAt?: number;
	result?: ResultMessage;
	error?: string;
	/** Set when the task was held because the worker was busy. */
	waiting?: boolean;
	/** True once the result has been handed to the orchestrator inline. */
	delivered?: boolean;
	/**
	 * Set before waiting, so a result that lands during the wait is not also
	 * queued as a digest. Cleared again if the wait gives up.
	 */
	inlineWanted?: boolean;
}

export interface WorkerEntry {
	presence: PresenceMessage;
	/** True when the heartbeat is overdue, so the shown state may be a lie. */
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

export type DispatchOutcome =
	| { ok: true; taskId: string; state: TaskState }
	| { ok: false; error: string };

export interface OrchestratorOptions {
	heartbeatMs: number;
	taskExpirySec: number;
	/** Called when a task reaches a terminal state, for digest delivery. */
	onFinished: (record: TaskRecord) => void;
	log: LogFn;
}

function clip(value: string, width: number): string {
	const flat = value.replace(/\s+/g, " ");
	return flat.length <= width ? flat.padEnd(width) : `${flat.slice(0, width - 1)}…`;
}

export class Orchestrator {
	/** `/workers/<id>/presence` etc, to drop a registry entry when it is deleted. */
	private static idFromPresenceTopic(topic: string): string | undefined {
		const match = topic.match(/\/(?:workers|orchestrators)\/([^/]+)\/presence$/);
		return match?.[1];
	}

	private readonly workers = new Map<string, PresenceMessage>();
	private readonly orchestrators = new Map<string, PresenceMessage>();
	private readonly tasks = new Map<string, TaskRecord>();
	private readonly unsubscribes: Array<() => void> = [];
	private readonly ackTimers = new Map<string, NodeJS.Timeout>();
	private readonly waiters = new Map<string, Array<(record: TaskRecord) => void>>();

	constructor(
		private readonly link: MqttLink,
		private readonly topics: Topics,
		private readonly options: OrchestratorOptions,
	) {}

	start(): void {
		this.unsubscribes.push(
			this.link.subscribe(this.topics.presenceFilter, (topic, payload) => {
				if (payload === undefined) {
					const id = Orchestrator.idFromPresenceTopic(topic);
					if (id) this.workers.delete(id);
					return;
				}
				if (!isPresenceMessage(payload)) {
					this.options.log("warn", "ignoring malformed presence message");
					return;
				}
				this.workers.set(payload.workerId, payload);
				this.onWorkerState(payload.workerId);
			}),
			this.link.subscribe(this.topics.orchestratorFilter, (topic, payload) => {
				if (payload === undefined) {
					const id = Orchestrator.idFromPresenceTopic(topic);
					if (id) this.orchestrators.delete(id);
					return;
				}
				if (!isPresenceMessage(payload)) return;
				this.orchestrators.set(payload.workerId, payload);
			}),
			this.link.subscribe(this.topics.ackFilter, (_topic, payload) => {
				if (payload === undefined || !isAckMessage(payload)) return;
				const record = this.tasks.get(payload.taskId);
				if (!record) return;
				record.state = "running";
				record.startedAt = payload.startedAt;
				this.clearAckTimer(payload.taskId);
			}),
			this.link.subscribe(this.topics.resultFilter, (_topic, payload) => {
				if (payload === undefined || !isResultMessage(payload)) {
					this.options.log("warn", "ignoring malformed result message");
					return;
				}
				const record = this.tasks.get(payload.taskId);
				if (!record) return;
				// With a shared cluster secret the broker can no longer tell sessions
				// apart, so the only remaining authenticity check is in the payload.
				if (payload.workerId !== record.task.to) {
					this.options.log(
						"warn",
						`ignoring result for ${payload.taskId} from ${payload.workerId}: it was pinned to ${record.task.to}`,
					);
					return;
				}
				this.finish(record, payload);
			}),
		);
	}

	stop(): void {
		for (const unsubscribe of this.unsubscribes) unsubscribe();
		this.unsubscribes.length = 0;
		for (const timer of this.ackTimers.values()) clearTimeout(timer);
		this.ackTimers.clear();
	}

	entries(): WorkerEntry[] {
		const now = Date.now();
		const staleAfter = this.options.heartbeatMs * STALE_FACTOR;
		return [...this.workers.values()]
			.map((presence) => {
				const ageMs = now - presence.updatedAt;
				return { presence, ageMs, stale: presence.state !== "offline" && ageMs > staleAfter };
			})
			.sort((a, b) => a.presence.workerId.localeCompare(b.presence.workerId));
	}

	dispatch(params: DispatchParams): DispatchOutcome {
		// A dead link makes every entry in the roster look stale: say that plainly
		// instead of blaming a worker that may be heartbeating perfectly well.
		if (!this.link.online) {
			const droppedAt = this.link.lastClose?.at;
			const since = droppedAt ? ` (dropped ${formatAge(Date.now() - droppedAt)} ago)` : "";
			return {
				ok: false,
				error: `pi-mqtt is not connected to the broker${since}: the roster is frozen, so nothing can be dispatched — /mqtt disconnect then /mqtt connect to rejoin`,
			};
		}
		const entry = this.entries().find((item) => item.presence.workerId === params.to);
		if (!entry) return { ok: false, error: `unknown worker "${params.to}"` };
		if (entry.stale) {
			return { ok: false, error: `worker "${params.to}" is stale (last heartbeat ${Math.round(entry.ageMs / 1000)}s ago)` };
		}
		if (entry.presence.state === "offline") {
			return { ok: false, error: `worker "${params.to}" is offline` };
		}

		const task: TaskMessage = {
			v: 1,
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

		const busy = entry.presence.state === "busy";
		if (busy && params.ifBusy !== "wait") {
			this.tasks.delete(task.taskId);
			return { ok: false, error: `worker "${params.to}" is busy with ${entry.presence.activeTaskId ?? "a task"}` };
		}
		if (busy) {
			record.waiting = true;
			return { ok: true, taskId: task.taskId, state: "queued" };
		}

		this.send(record);
		return { ok: true, taskId: task.taskId, state: record.state };
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
		void this.link.publish(this.topics.control(record.task.to), {
			v: 1,
			kind: "cancel",
			taskId,
			reason: "cancelled by orchestrator",
		});
		return { ok: true };
	}

	get(taskId: string): TaskRecord | undefined {
		return this.tasks.get(taskId);
	}

	/**
	 * Wait for a task to reach a terminal state, or for the wait itself to time out.
	 *
	 * Resolves `undefined` when the deadline passes or the caller aborts: in both
	 * cases the task keeps running and its result still arrives as a transcript
	 * entry, so the caller must not treat this as a failure.
	 */
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
			timer.unref?.();
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

	list(): TaskRecord[] {
		return [...this.tasks.values()].sort((a, b) => b.task.createdAt - a.task.createdAt);
	}

	outstanding(): TaskRecord[] {
		return this.list().filter((record) => record.state === "queued" || record.state === "sent" || record.state === "running");
	}

	orchestratorEntries(includeOffline = false): WorkerEntry[] {
		const now = Date.now();
		const staleAfter = this.options.heartbeatMs * STALE_FACTOR;
		return [...this.orchestrators.values()]
			.filter((presence) => includeOffline || presence.state !== "offline")
			.map((presence) => {
				const ageMs = now - presence.updatedAt;
				return { presence, ageMs, stale: ageMs > staleAfter };
			})
			.sort((a, b) => a.presence.workerId.localeCompare(b.presence.workerId));
	}


	/** Workers and orchestrators in one table, role as a column. */
	combinedTable(): string {
		const workers = this.entries();
		const others = this.orchestratorEntries();
		if (workers.length === 0 && others.length === 0) return `No sessions on ${this.topics.base} yet.`;

		const header = [
			["ID", 22],
			["ROLE", 14],
			["STATE", 10],
			["REPO", 26],
			["MODEL", 22],
			["AGE", 5],
			["CWD", 3],
		]
			.map(([label, width]) => String(label).padEnd(Number(width)))
			.join("  ");

		const workerRows = workers.map(({ presence, stale, ageMs }) =>
			[
				clip(presence.workerId, 22),
				clip("worker", 14),
				clip(stale ? `${presence.state}?` : presence.state, 10),
				clip([presence.repo, presence.branch].filter(Boolean).join("@") || "-", 26),
				clip(presence.model ?? "-", 22),
				clip(formatAge(ageMs), 5),
				presence.cwd,
			].join("  "),
		);
		const orchestratorRows = others.map(({ presence, stale, ageMs }) =>
			[
				clip(presence.workerId, 22),
				clip("orchestrator", 14),
				clip(stale ? `${presence.state}?` : presence.state, 10),
				clip([presence.repo, presence.branch].filter(Boolean).join("@") || "-", 26),
				clip(presence.model ?? "-", 22),
				clip(formatAge(ageMs), 5),
				presence.cwd,
			].join("  "),
		);

		return [header, ...workerRows, ...orchestratorRows].join("\n");
	}


	taskTable(): string {
		const records = this.list();
		if (records.length === 0) return "No tasks dispatched in this session.";
		const header = (
			[
				["TASK", 26],
				["WORKER", 14],
				["STATE", 10],
				["AGE", 7],
				["DETAIL", 5],
			] as const
		)
			.map(([label, width]) => label.padEnd(width))
			.join("  ");
		const rows = records.map((record) => {
			const age = record.finishedAt ?? record.startedAt ?? record.sentAt ?? record.task.createdAt;
			const detail = record.error ?? record.result?.error ?? record.result?.text.split("\n")[0] ?? "";
			return [
				clip(record.task.taskId, 26),
				clip(record.task.to, 14),
				clip(record.state, 10),
				formatAge(Date.now() - age),
				clip(detail, 60),
			].join("  ");
		});
		return `${header}\n${rows.join("\n")}`;
	}

	// --- internals -------------------------------------------------------

	private send(record: TaskRecord): void {
		record.attempts += 1;
		record.state = "sent";
		record.sentAt = Date.now();
		record.waiting = false;
		void this.link
			.publish(this.topics.control(record.task.to), record.task, { expirySec: this.options.taskExpirySec })
			.catch((error: unknown) => {
				this.options.log("error", `publish task ${record.task.taskId} failed: ${(error as Error).message}`);
				this.finishLocally(record, "lost", `publish failed: ${(error as Error).message}`);
			});

		// No ack in time: the worker never started, so a re-send is safe. We only
		// re-send while the worker looks reachable; otherwise the task is lost.
		this.clearAckTimer(record.task.taskId);
		const timer = setTimeout(() => {
			this.ackTimers.delete(record.task.taskId);
			if (record.state !== "sent") return;
			const entry = this.entries().find((item) => item.presence.workerId === record.task.to);
			const reachable = entry && !entry.stale && entry.presence.state !== "offline";
			if (reachable && record.attempts < MAX_ATTEMPTS) {
				this.options.log("warn", `no ack for ${record.task.taskId}; re-sending (attempt ${record.attempts + 1})`);
				this.send(record);
				return;
			}
			this.finishLocally(record, "lost", `no ack from ${record.task.to} within ${ACK_TIMEOUT_MS}ms`);
		}, ACK_TIMEOUT_MS);
		timer.unref?.();
		this.ackTimers.set(record.task.taskId, timer);
	}

	private onWorkerState(workerId: string): void {
		for (const record of this.tasks.values()) {
			if (record.task.to !== workerId) continue;
			if (!record.waiting) continue;
			const entry = this.entries().find((item) => item.presence.workerId === workerId);
			if (!entry || entry.presence.state !== "idle") continue;
			this.options.log("info", `worker ${workerId} is idle; dispatching queued ${record.task.taskId}`);
			this.send(record);
		}
	}

	private finish(record: TaskRecord, result: ResultMessage): void {
		if (record.finishedAt) {
			// QoS 1 duplicates, or a second session answering under the same worker
			// id: the first terminal state wins.
			this.options.log("warn", `ignoring duplicate result for ${record.task.taskId}`);
			return;
		}
		this.clearAckTimer(record.task.taskId);
		record.result = result;
		record.state = result.status;
		record.finishedAt = result.finishedAt;
		this.notifyWaiters(record);
		this.options.onFinished(record);
	}

	private finishLocally(record: TaskRecord, state: TaskState, error: string): void {
		this.clearAckTimer(record.task.taskId);
		record.state = state;
		record.error = error;
		record.finishedAt = Date.now();
		this.notifyWaiters(record);
		this.options.onFinished(record);
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

export function formatAge(ms: number): string {
	if (ms < 1000) return "now";
	const seconds = Math.round(ms / 1000);
	if (seconds < 60) return `${seconds}s`;
	const minutes = Math.round(seconds / 60);
	if (minutes < 60) return `${minutes}m`;
	const hours = Math.round(minutes / 60);
	if (hours < 48) return `${hours}h`;
	return `${Math.round(hours / 24)}d`;
}
