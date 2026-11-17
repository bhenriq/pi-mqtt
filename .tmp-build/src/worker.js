/**
 * Worker: receives a pinned task, runs it as an agent run in this session, and
 * publishes what happened.
 *
 * One run at a time. Busy tasks are rejected rather than queued: with explicit
 * pinning, a silent queue on a long-lived session is just delayed work.
 */
import { appendFileSync, mkdirSync } from "node:fs";
import { join } from "node:path";
import { collectRepoResult, prepareRepo } from "./git.js";
import { isCancelMessage, isControlMessage, isKickMessage, taskPromptFrame, } from "./protocol.js";
const DEFAULT_TIMEOUT_MS = 10 * 60 * 1000;
const SEEN_LIMIT = 500;
export class Worker {
    link;
    topics;
    deps;
    unsubscribes = [];
    seen = new Set();
    active;
    constructor(link, topics, deps) {
        this.link = link;
        this.topics = topics;
        this.deps = deps;
    }
    start() {
        const { config, log } = this.deps;
        const controlTopic = this.topics.control(config.worker.workerId);
        this.unsubscribes.push(this.link.subscribe(controlTopic, (_topic, payload) => {
            if (payload === undefined)
                return;
            if (!isControlMessage(payload)) {
                log("warn", `ignoring malformed control message on ${controlTopic}`);
                return;
            }
            if (isKickMessage(payload)) {
                // Room-wide kicks arrive on the room control topic, not here.
                return;
            }
            if (isCancelMessage(payload))
                this.handleCancel(payload.taskId);
            else
                this.handleTask(payload);
        }), this.deps.pi.on("message_end", (event) => this.onMessageEnd(event.message)), this.deps.pi.on("agent_settled", async () => this.onSettled()));
        log("info", `worker ${config.worker.workerId} listening on ${controlTopic}`);
    }
    stop() {
        for (const unsubscribe of this.unsubscribes)
            unsubscribe();
        this.unsubscribes.length = 0;
        if (this.active?.timer)
            clearTimeout(this.active.timer);
        this.active = undefined;
    }
    // --- task intake -----------------------------------------------------
    handleTask(task) {
        const { log, config } = this.deps;
        this.taskLog({ direction: "in", taskId: task.taskId, kind: "task", prompt: task.prompt });
        // A shared secret lets any member publish here; only take tasks addressed to us.
        if (task.to !== this.deps.config.worker.workerId) {
            log("warn", `ignoring task ${task.taskId} addressed to ${task.to}`);
            return;
        }
        if (this.seen.has(task.taskId)) {
            // QoS 1 may redeliver; the first delivery owns this task id.
            log("info", `duplicate task ${task.taskId} ignored`);
            return;
        }
        this.seen.add(task.taskId);
        if (this.seen.size > SEEN_LIMIT) {
            const oldest = this.seen.values().next().value;
            if (oldest)
                this.seen.delete(oldest);
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
        let baseSha;
        let branch;
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
                if (!this.active)
                    return;
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
    handleCancel(taskId) {
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
    onMessageEnd(message) {
        const active = this.active;
        if (!active)
            return;
        const candidate = message;
        if (candidate?.role !== "assistant")
            return;
        const text = (candidate.content ?? [])
            .filter((block) => block?.type === "text" && typeof block.text === "string")
            .map((block) => block.text)
            .join("\n")
            .trim();
        if (text)
            active.text = text;
        const total = candidate.usage?.cost?.total;
        if (typeof total === "number")
            active.costUsd += total;
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
    async onSettled() {
        const active = this.active;
        if (!active)
            return;
        this.active = undefined;
        if (active.timer)
            clearTimeout(active.timer);
        let status = "done";
        let error;
        if (active.cancelRequested)
            status = "cancelled";
        else if (active.timedOut) {
            status = "timeout";
            error = `exceeded ${active.task.timeoutMs ?? DEFAULT_TIMEOUT_MS}ms`;
        }
        else if (active.task.maxCostUsd !== undefined && active.costUsd > active.task.maxCostUsd) {
            status = "error";
            error = `cost cap exceeded ($${active.costUsd.toFixed(4)} > $${active.task.maxCostUsd})`;
        }
        else if (active.lastError) {
            status = "error";
            error = active.lastError;
        }
        this.publishResult({ task: active.task, status, error, startedAt: active.startedAt, text: active.text, costUsd: active.costUsd, baseSha: active.baseSha, branch: active.branch });
        this.deps.presence.update({ state: "idle", activeTaskId: undefined, startedAt: undefined });
    }
    // --- publishing ------------------------------------------------------
    publishAck(task, startedAt) {
        void this.link.publish(this.topics.ack(this.deps.config.worker.workerId), {
            v: 1,
            kind: "ack",
            taskId: task.taskId,
            workerId: this.deps.config.worker.workerId,
            startedAt,
        });
    }
    publishResult(input) {
        const { task } = input;
        const ctx = this.deps.ctx();
        const finishedAt = Date.now();
        let artifacts = [];
        let repo = undefined;
        if (task.repo && input.status !== "error") {
            repo = collectRepoResult(ctx?.cwd ?? process.cwd(), input.baseSha, input.branch, task.taskId, join(this.deps.config.logDir, "patches"));
            if (repo.patchPath)
                artifacts = [repo.patchPath];
        }
        const result = {
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
            .catch((error) => this.deps.log("error", `publish result failed: ${error.message}`));
    }
    taskLog(entry) {
        try {
            const dir = join(this.deps.config.logDir, "tasks");
            mkdirSync(dir, { recursive: true });
            appendFileSync(join(dir, `${this.deps.config.worker.workerId}.jsonl`), `${JSON.stringify({ ts: Date.now(), ...entry })}\n`);
        }
        catch {
            // Logging must never break task handling.
        }
    }
}
