/**
 * Worker: receives a pinned task, runs it as an agent run in this session, and
 * publishes what happened.
 *
 * One run at a time. Busy tasks are rejected rather than queued: with explicit
 * pinning, a silent queue on a long-lived session is just delayed work.
 */

import { appendFileSync, mkdirSync } from "node:fs";
import { join } from "node:path";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import type { AppConfig } from "./config.ts";
import { collectRepoResult, prepareRepo } from "./git.ts";
import type { LogFn, MqttLink } from "./mqtt.ts";
import type { Presence } from "./presence.ts";
import {
	isCancelMessage,
	isControlMessage,
	taskPromptFrame,
	type ResultMessage,
	type TaskMessage,
	type TaskStatus,
	type Topics,
} from "./protocol.ts";

const DEFAULT_TIMEOUT_MS = 10 * 60 * 1000;
const SEEN_LIMIT = 500;

interface ActiveTask {
	task: TaskMessage;
	startedAt: number;
	baseSha?: string;
	branch?: string;
	text: string;
	/** Provider error for this run, reported only when the run actually failed. */
	lastError?: string;
	costUsd: number;
	timedOut: boolean;
	cancelRequested: boolean;
	timer?: NodeJS.Timeout;
}

export interface WorkerDeps {
	pi: ExtensionAPI;
	/** Resolved lazily: commands and lifecycle handlers may run at different times. */
	ctx: () => ExtensionContext | undefined;
	config: AppConfig;
	presence: Presence;
	log: LogFn;
}

export class Worker {
	private readonly unsubscribes: Array<() => void> = [];
	private readonly seen = new Set<string>();
	private active: ActiveTask | undefined;

	constructor(
		private readonly link: MqttLink,
		private readonly topics: Topics,
		private readonly deps: WorkerDeps,
	) {}

	start(): void {
		const { config, log } = this.deps;
		const controlTopic = this.topics.control(config.worker.workerId);

		this.unsubscribes.push(
			this.link.subscribe(controlTopic, (_topic, payload) => {
				if (!isControlMessage(payload)) {
					log("warn", `ignoring malformed control message on ${controlTopic}`);
					return;
				}
				if (isCancelMessage(payload)) this.handleCancel(payload.taskId);
				else this.handleTask(payload);
			}),
			this.deps.pi.on("message_end", (event) => this.onMessageEnd(event.message)),
			this.deps.pi.on("agent_settled", async () => this.onSettled()),
		);

		log("info", `worker ${config.worker.workerId} listening on ${controlTopic}`);
	}

	stop(): void {
		for (const unsubscribe of this.unsubscribes) unsubscribe();
		this.unsubscribes.length = 0;
		if (this.active?.timer) clearTimeout(this.active.timer);
		this.active = undefined;
	}

	// --- task intake -----------------------------------------------------

	private handleTask(task: TaskMessage): void {
		const { log, config } = this.deps;
		this.taskLog({ direction: "in", taskId: task.taskId, kind: "task", prompt: task.prompt });

		if (this.seen.has(task.taskId)) {
			// QoS 1 may redeliver; the first delivery owns this task id.
			log("info", `duplicate task ${task.taskId} ignored`);
			return;
		}
		this.seen.add(task.taskId);
		if (this.seen.size > SEEN_LIMIT) {
			const oldest = this.seen.values().next().value;
			if (oldest) this.seen.delete(oldest);
		}

		if (this.active) {
			this.publishResult({
				task,
				status: "error",
				error: `busy with ${this.active.task.taskId}`,
				startedAt: Date.now(),
			});
			return;
		}

		let baseSha: string | undefined;
		let branch: string | undefined;
		if (task.repo) {
			const prep = prepareRepo(this.deps.ctx()?.cwd ?? process.cwd(), task.repo, config.worker.workerId);
			if (!prep.ok) {
				this.publishResult({ task, status: "error", error: prep.error ?? "repo preparation failed", startedAt: Date.now() });
				return;
			}
			baseSha = prep.baseSha;
			branch = prep.branch;
		}

		const startedAt = Date.now();
		this.active = { task, startedAt, text: "", costUsd: 0, timedOut: false, cancelRequested: false, baseSha, branch };

		const timeoutMs = task.timeoutMs ?? DEFAULT_TIMEOUT_MS;
		if (timeoutMs > 0) {
			this.active.timer = setTimeout(() => {
				if (!this.active) return;
				this.active.timedOut = true;
				this.deps.log("warn", `task ${task.taskId} exceeded ${timeoutMs}ms; aborting`);
				this.deps.ctx()?.abort();
			}, timeoutMs);
			this.active.timer.unref?.();
		}

		this.deps.presence.update({ state: "busy", activeTaskId: task.taskId, startedAt });
		this.publishAck(task, startedAt);
		log("info", `task ${task.taskId} started`);
		this.deps.pi.sendUserMessage(taskPromptFrame(task));
	}

	private handleCancel(taskId: string): void {
		const active = this.active;
		if (!active || active.task.taskId !== taskId) {
			this.deps.log("info", `cancel for ${taskId} but nothing is running`);
			return;
		}
		active.cancelRequested = true;
		this.deps.log("info", `cancelling ${taskId}`);
		this.deps.ctx()?.abort();
	}

	// --- run observation -------------------------------------------------

	private onMessageEnd(message: unknown): void {
		const active = this.active;
		if (!active) return;
		const candidate = message as {
			role?: string;
			content?: Array<{ type?: string; text?: string }>;
			usage?: { cost?: { total?: number }; totalTokens?: number };
			errorMessage?: string;
			stopReason?: string;
		};
		if (candidate?.role !== "assistant") return;

		const text = (candidate.content ?? [])
			.filter((block) => block?.type === "text" && typeof block.text === "string")
			.map((block) => block.text as string)
			.join("\n")
			.trim();
		if (text) active.text = text;

		const total = candidate.usage?.cost?.total;
		if (typeof total === "number") active.costUsd += total;

		const cap = active.task.maxCostUsd;
		if (cap !== undefined && active.costUsd > cap) {
			this.deps.log("warn", `task ${active.task.taskId} hit its cost cap ($${active.costUsd.toFixed(4)} > $${cap}); aborting`);
			this.deps.ctx()?.abort();
		}
		// Never overwrite partial output: an aborted run still produced text worth
		// returning, and "Request was aborted" is noise, not a result.
		if (candidate.stopReason === "error" || candidate.errorMessage) {
			active.lastError = candidate.errorMessage ?? "model error";
		}
	}

	private async onSettled(): Promise<void> {
		const active = this.active;
		if (!active) return;
		this.active = undefined;
		if (active.timer) clearTimeout(active.timer);

		let status: TaskStatus = "done";
		let error: string | undefined;
		if (active.cancelRequested) status = "cancelled";
		else if (active.timedOut) {
			status = "timeout";
			error = `exceeded ${active.task.timeoutMs ?? DEFAULT_TIMEOUT_MS}ms`;
		} else if (active.task.maxCostUsd !== undefined && active.costUsd > active.task.maxCostUsd) {
			status = "error";
			error = `cost cap exceeded ($${active.costUsd.toFixed(4)} > $${active.task.maxCostUsd})`;
		} else if (active.lastError) {
			status = "error";
			error = active.lastError;
		}

		this.publishResult({ task: active.task, status, error, startedAt: active.startedAt, text: active.text, costUsd: active.costUsd, baseSha: active.baseSha, branch: active.branch });
		this.deps.presence.update({ state: "idle", activeTaskId: undefined, startedAt: undefined });
	}

	// --- publishing ------------------------------------------------------

	private publishAck(task: TaskMessage, startedAt: number): void {
		void this.link.publish(this.topics.ack(this.deps.config.worker.workerId), {
			v: 1,
			kind: "ack",
			taskId: task.taskId,
			workerId: this.deps.config.worker.workerId,
			startedAt,
		});
	}

	private publishResult(input: {
		task: TaskMessage;
		status: TaskStatus;
		error?: string;
		startedAt: number;
		text?: string;
		costUsd?: number;
		baseSha?: string;
		branch?: string;
	}): void {
		const { task } = input;
		const ctx = this.deps.ctx();
		const finishedAt = Date.now();

		let artifacts: string[] = [];
		let repo = undefined;
		if (task.repo && input.status !== "error") {
			repo = collectRepoResult(
				ctx?.cwd ?? process.cwd(),
				input.baseSha,
				input.branch,
				task.taskId,
				join(this.deps.config.logDir, "patches"),
			);
			if (repo.patchPath) artifacts = [repo.patchPath];
		}

		const result: ResultMessage = {
			v: 1,
			kind: "result",
			taskId: task.taskId,
			workerId: this.deps.config.worker.workerId,
			status: input.status,
			text: input.text?.trim() ?? "",
			artifacts,
			durationMs: finishedAt - input.startedAt,
			finishedAt,
			...(input.error ? { error: input.error } : {}),
			...(repo ? { repo } : {}),
			...(ctx?.sessionManager.getSessionFile() ? { sessionFile: ctx.sessionManager.getSessionFile() } : {}),
			...(ctx?.sessionManager.getSessionId() ? { sessionId: ctx.sessionManager.getSessionId() } : {}),
			...(input.costUsd ? { usage: { costUsd: input.costUsd } } : {}),
		};

		this.taskLog({
			direction: "out",
			taskId: task.taskId,
			kind: "result",
			status: result.status,
			error: result.error,
			durationMs: result.durationMs,
			repo: result.repo,
		});
		this.deps.log("info", `task ${task.taskId} finished: ${result.status}`);
		void this.link
			.publish(this.topics.result(task.taskId), result)
			.catch((error: unknown) => this.deps.log("error", `publish result failed: ${(error as Error).message}`));
	}

	private taskLog(entry: Record<string, unknown>): void {
		try {
			const dir = join(this.deps.config.logDir, "tasks");
			mkdirSync(dir, { recursive: true });
			appendFileSync(join(dir, `${this.deps.config.worker.workerId}.jsonl`), `${JSON.stringify({ ts: Date.now(), ...entry })}\n`);
		} catch {
			// Logging must never break task handling.
		}
	}
}
