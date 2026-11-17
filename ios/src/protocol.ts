/**
 * Wire protocol for pi-mqtt, as an orchestrator sees it.
 *
 * Same topic layout and message shapes as the repo's src/protocol.ts, so a phone
 * running this can talk to workers running the desktop package unchanged. Payloads
 * from a shared broker are untrusted input: nothing is used without passing a guard.
 */

import { IDENTIFIER } from "./util.ts";

export const PROTOCOL_VERSION = 1;
export const CLIENT_VERSION = "0.1.0-ios";

// ---------------------------------------------------------------------------
// Wire protocol (mirrors pi-mqtt's src/protocol.ts)
// ---------------------------------------------------------------------------

export type WorkerState = "idle" | "busy" | "offline";
export type TaskStatus = "done" | "error" | "timeout" | "cancelled";
export type TaskState = "queued" | "sent" | "running" | TaskStatus | "lost";

export interface RepoSpec {
	baseRef?: string;
	baseSha?: string;
	branch?: string;
}

export interface TaskMessage {
	v: number;
	kind: "task";
	taskId: string;
	to: string;
	prompt: string;
	notify: "silent" | "wake";
	repo?: RepoSpec;
	timeoutMs?: number;
	maxCostUsd?: number;
	model?: string;
	artifacts?: "none" | "paths";
	createdAt: number;
}

export interface RepoResult {
	baseSha?: string;
	headSha?: string;
	branch?: string;
	diffstat?: string;
	patchPath?: string;
}

export interface ResultMessage {
	v: number;
	kind: "result";
	taskId: string;
	workerId: string;
	status: TaskStatus;
	text: string;
	error?: string;
	artifacts: string[];
	repo?: RepoResult;
	usage?: { inputTokens?: number; outputTokens?: number; costUsd?: number };
	durationMs: number;
	sessionFile?: string;
	sessionId?: string;
	finishedAt: number;
}

export interface PresenceMessage {
	v: number;
	kind: "presence";
	role?: "worker" | "orchestrator";
	workerId: string;
	host: string;
	cwd: string;
	repo?: string;
	branch?: string;
	headSha?: string;
	model?: string;
	state: WorkerState;
	activeTaskId?: string;
	startedAt?: number;
	version: string;
	updatedAt: number;
}

export interface Topics {
	base: string;
	presence: (workerId: string) => string;
	orchestratorPresence: (id: string) => string;
	control: (workerId: string) => string;
	ack: (workerId: string) => string;
	result: (taskId: string) => string;
	roomControl: string;
	presenceFilter: string;
	orchestratorFilter: string;
	ackFilter: string;
	resultFilter: string;
}

export function topicsFor(room: string): Topics {
	if (!IDENTIFIER.test(room)) {
		throw new Error(`invalid room id ${JSON.stringify(room)}: letters, digits, _ or -, starting with a letter or digit`);
	}
	const base = `pi/v${PROTOCOL_VERSION}/${room}`;
	return {
		base,
		presence: (workerId) => `${base}/workers/${workerId}/presence`,
		orchestratorPresence: (id) => `${base}/orchestrators/${id}/presence`,
		control: (workerId) => `${base}/workers/${workerId}/ctl`,
		ack: (workerId) => `${base}/workers/${workerId}/ack`,
		result: (taskId) => `${base}/results/${taskId}`,
		roomControl: `${base}/control`,
		presenceFilter: `${base}/workers/+/presence`,
		orchestratorFilter: `${base}/orchestrators/+/presence`,
		ackFilter: `${base}/workers/+/ack`,
		resultFilter: `${base}/results/+`,
	};
}

type Json = Record<string, unknown>;

/** Payloads from a shared broker are untrusted input: guard before use. */
export function parseJson(payload: string): Json | undefined {
	try {
		const parsed: unknown = JSON.parse(payload);
		if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) return undefined;
		return parsed as Json;
	} catch {
		return undefined;
	}
}

const isStr = (value: unknown): value is string => typeof value === "string";
const isNum = (value: unknown): value is number => typeof value === "number" && Number.isFinite(value);

export function isPresence(value: Json): value is Json & PresenceMessage {
	return (
		value.v === PROTOCOL_VERSION &&
		value.kind === "presence" &&
		isStr(value.workerId) &&
		isStr(value.state) &&
		["idle", "busy", "offline"].includes(value.state) &&
		isNum(value.updatedAt)
	);
}

export function isAck(value: Json): value is Json & { taskId: string; workerId: string } {
	return value.v === PROTOCOL_VERSION && value.kind === "ack" && isStr(value.taskId) && isStr(value.workerId);
}

export function isResult(value: Json): value is Json & ResultMessage {
	return (
		value.v === PROTOCOL_VERSION &&
		value.kind === "result" &&
		isStr(value.taskId) &&
		isStr(value.workerId) &&
		isStr(value.text) &&
		["done", "error", "timeout", "cancelled"].includes(String(value.status))
	);
}

export function isKick(value: Json): value is Json & { from: string; reason?: string } {
	return value.v === PROTOCOL_VERSION && value.kind === "kick" && isStr(value.from);
}

export interface PresenceIdentity {
	workerId: string;
	host: string;
	cwd: string;
	model?: string;
	repo?: string;
	branch?: string;
}

export function buildPresence(identity: PresenceIdentity, state: WorkerState, role: "worker" | "orchestrator"): PresenceMessage {
	return {
		v: PROTOCOL_VERSION,
		kind: "presence",
		version: CLIENT_VERSION,
		role,
		...identity,
		state,
		updatedAt: Date.now(),
	};
}
