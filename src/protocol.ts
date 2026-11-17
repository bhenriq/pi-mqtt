/**
 * Wire protocol for pi-mqtt.
 *
 * Everything that crosses the broker is defined here: topic layout, message
 * shapes, and the runtime guards used on receive (payloads from a public broker
 * are untrusted input, so nothing is used without passing a guard).
 */

import { createHash } from "node:crypto";

export const PROTOCOL_VERSION = 1;
export const CLIENT_VERSION = "0.1.0";

export type Role = "orchestrator" | "worker";
/** `pending`: joined, but no orchestrator has approved this session yet. */
export type WorkerState = "idle" | "busy" | "offline" | "pending";
export type TaskStatus = "done" | "error" | "timeout" | "cancelled";

export interface RepoSpec {
	/** Branch or ref the orchestrator pinned the task to. */
	baseRef?: string;
	/** Exact commit the task must start from. Preferred over baseRef. */
	baseSha?: string;
	/** Branch the worker should create/check out. Defaults to worker/<id>. */
	branch?: string;
}

export interface TaskMessage {
	v: number;
	kind: "task";
	taskId: string;
	to: string;
	prompt: string;
	/** "silent" drops a digest only; "wake" resumes the orchestrator's run. */
	notify: "silent" | "wake";
	repo?: RepoSpec;
	timeoutMs?: number;
	maxCostUsd?: number;
	model?: string;
	artifacts?: "none" | "paths";
	createdAt: number;
}

/** Room-wide eviction: every session in the room disconnects itself. */
export interface KickMessage {
	v: number;
	kind: "kick";
	/** Session id that asked for it, so the sender ignores its own message. */
	from: string;
	reason?: string;
	at: number;
}

export interface CancelMessage {
	v: number;
	kind: "cancel";
	taskId: string;
	reason?: string;
}

export type ControlMessage = TaskMessage | CancelMessage | KickMessage;

/** Published as soon as a worker actually begins a task. */
export interface AckMessage {
	v: number;
	kind: "ack";
	taskId: string;
	workerId: string;
	startedAt: number;
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
	/** Final assistant text of the run. */
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
	/** Workers can receive tasks; orchestrators only dispatch them. */
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
	/** Long-term public key of this session (base64 DER, Ed25519). */
	key?: string;
	/** Per-join nonce, present while `pending`; makes an approval single-use. */
	nonce?: string;
	/** Ed25519 signature over the join statement, proving the key is ours. */
	proof?: string;
}

const IDENTIFIER = /^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$/;

export function isValidIdentifier(value: string): boolean {
	return IDENTIFIER.test(value);
}

/** Topic layout. Nothing else in the codebase builds topic strings. */
export function topicsFor(cluster: string) {
	if (!isValidIdentifier(cluster)) {
		throw new Error(`Invalid cluster id: ${JSON.stringify(cluster)}`);
	}
	const base = `pi/v${PROTOCOL_VERSION}/${cluster}`;
	return {
		base,
		presence: (workerId: string) => `${base}/workers/${workerId}/presence`,
		orchestratorPresence: (id: string) => `${base}/orchestrators/${id}/presence`,
		control: (workerId: string) => `${base}/workers/${workerId}/ctl`,
		ack: (workerId: string) => `${base}/workers/${workerId}/ack`,
		/** Orchestrator → worker: pairing challenge, admission, revocation. */
		pair: (workerId: string) => `${base}/workers/${workerId}/pair`,
		/** The room's record of who was admitted, written only by orchestrators. */
		admission: (workerId: string) => `${base}/admissions/${workerId}`,
		result: (taskId: string) => `${base}/results/${taskId}`,
		/** Room-wide announcements, currently only "kick". */
		roomControl: `${base}/control`,
		presenceFilter: `${base}/workers/+/presence`,
		orchestratorFilter: `${base}/orchestrators/+/presence`,
		ackFilter: `${base}/workers/+/ack`,
		resultFilter: `${base}/results/+`,
		admissionFilter: `${base}/admissions/+`,
	} as const;
}

export type Topics = ReturnType<typeof topicsFor>;

export function newTaskId(): string {
	return `t_${Date.now().toString(36)}_${crypto.randomUUID().slice(0, 8)}`;
}

/**
 * The exact user message a worker receives for a task.
 *
 * Framed on purpose: worker sessions are long-lived, a human may type into them
 * between tasks, and the prompt arrives from the broker, so the boundary has to
 * be explicit rather than "the last thing in the transcript".
 */
export function taskPromptFrame(task: TaskMessage): string {
	const lines = [`--- pi-mqtt task ${task.taskId} (from orchestrator) ---`, task.prompt.trim(), "--- end task ---"];
	if (task.repo) {
		lines.push("Work only inside the current repository. Leave the result as a commit or as files in the tree.");
	}
	lines.push("Your final assistant message is returned verbatim to the orchestrator. Keep it short and self-contained.");
	return lines.join("\n");
}

/**
 * The approval channel: an orchestrator answers a `pending` join.
 *
 * `challenge` carries a nonce so both ends can derive the same short code over the
 * worker's key; `admit` and `revoke` carry the key fingerprint the decision is about,
 * so a decision cannot be replayed onto a different key.
 */
export interface PairMessage {
	v: number;
	kind: "pair";
	step: "challenge" | "admit" | "revoke";
	workerId: string;
	orchestratorId: string;
	/** challenge: orchestrator nonce, and when this offer stops being usable. */
	nonce?: string;
	expiresAt?: number;
	/** admit/revoke: the worker key this decision applies to. */
	fingerprint?: string;
	at: number;
}

/**
 * The short code a human compares between the worker's screen and the orchestrator's.
 *
 * Derived from the worker key and both nonces: two different keys — a squatter's and
 * the real worker's — cannot produce the same code, and a relay in the middle cannot
 * make the two screens agree. Six digits, because the comparison is deliberate, not a
 * secret to guess.
 */
export function pairingCode(input: {
	room: string;
	workerId: string;
	orchestratorId: string;
	key: string;
	workerNonce: string;
	orchestratorNonce: string;
}): string {
	const canonical = [
		"pi-mqtt-pair",
		input.room,
		input.workerId,
		input.orchestratorId,
		input.key,
		input.workerNonce,
		input.orchestratorNonce,
	].join("\n");
	const digest = createHash("sha256").update(canonical).digest();
	return String(digest.readUInt32BE(0) % 1_000_000).padStart(6, "0");
}

/** "481902" → "481 902", for reading aloud. */
export function formatPairingCode(code: string): string {
	return `${code.slice(0, 3)} ${code.slice(3)}`;
}

/** The statement a worker signs to prove it holds the key it advertises. */
export function joinStatement(input: { room: string; workerId: string; nonce: string; key: string }): string {
	return ["pi-mqtt-join", input.room, input.workerId, input.nonce, input.key].join("\n");
}

/**
 * The room's approval record, retained on `admissions/<workerId>`.
 *
 * Approving a worker is a decision about the room, not about one orchestrator's memory
 * of it, so the record lives in the room. Only an orchestrator credential may write this
 * topic (see the ACL), which is what makes a record trustworthy to an orchestrator that
 * joined later: the broker enforced "an orchestrator said this", and a holder of an
 * orchestrator credential already has room-wide authority.
 *
 * Revocation is a tombstone rather than a deletion: "never admitted" and "admitted, then
 * revoked" are different, and only one of them should be re-offered a code silently.
 */
export interface AdmissionMessage {
	v: number;
	kind: "admission";
	workerId: string;
	state: "admitted" | "revoked";
	/** Present when admitted: the worker key this decision is about. */
	key?: string;
	fingerprint?: string;
	/** Who decided, and when. */
	by: string;
	at: number;
}

export function isAdmissionMessage(value: Json): value is Json & AdmissionMessage {
	return (
		hasEnvelope(value, "admission") &&
		isStr(value.workerId) &&
		(value.state === "admitted" || value.state === "revoked") &&
		isStr(value.by) &&
		isNum(value.at) &&
		(value.key === undefined || isStr(value.key))
	);
}

/** Newlines and control characters make MQTT log/topic inspection painful. */
export function sanitizeText(value: string, max = 4000): string {
	const trimmed = value.replace(/\u0000/g, "").trim();
	return trimmed.length > max ? `${trimmed.slice(0, max)}…` : trimmed;
}

// --- receive-side guards -------------------------------------------------

export type Json = Record<string, unknown>;

export function decodePayload(payload: Uint8Array): Json | undefined {
	try {
		const parsed: unknown = JSON.parse(new TextDecoder().decode(payload));
		if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) return undefined;
		return parsed as Json;
	} catch {
		return undefined;
	}
}

const isStr = (value: unknown): value is string => typeof value === "string";
const isNum = (value: unknown): value is number => typeof value === "number" && Number.isFinite(value);

function hasEnvelope(value: Json, kind: string): boolean {
	return value.v === PROTOCOL_VERSION && value.kind === kind;
}

export function isTaskMessage(value: Json): value is Json & TaskMessage {
	return (
		hasEnvelope(value, "task") &&
		isStr(value.taskId) &&
		isStr(value.to) &&
		isStr(value.prompt) &&
		(value.notify === "silent" || value.notify === "wake") &&
		isNum(value.createdAt)
	);
}

export function isCancelMessage(value: Json): value is Json & CancelMessage {
	return hasEnvelope(value, "cancel") && isStr(value.taskId);
}

export function isKickMessage(value: Json): value is Json & KickMessage {
	return hasEnvelope(value, "kick") && isStr(value.from);
}

export function isControlMessage(value: Json): value is Json & ControlMessage {
	return isTaskMessage(value) || isCancelMessage(value) || isKickMessage(value);
}

export function isPairMessage(value: Json): value is Json & PairMessage {
	const step = value.step;
	return (
		hasEnvelope(value, "pair") &&
		isStr(value.workerId) &&
		isStr(value.orchestratorId) &&
		(step === "challenge" || step === "admit" || step === "revoke") &&
		isNum(value.at)
	);
}

export function isAckMessage(value: Json): value is Json & AckMessage {
	return hasEnvelope(value, "ack") && isStr(value.taskId) && isStr(value.workerId);
}

export function isResultMessage(value: Json): value is Json & ResultMessage {
	return (
		hasEnvelope(value, "result") &&
		isStr(value.taskId) &&
		isStr(value.workerId) &&
		isStr(value.text) &&
		["done", "error", "timeout", "cancelled"].includes(String(value.status))
	);
}

export function isPresenceMessage(value: Json): value is Json & PresenceMessage {
	return (
		hasEnvelope(value, "presence") &&
		isStr(value.workerId) &&
		isStr(value.state) &&
		["idle", "busy", "offline", "pending"].includes(value.state) &&
		isNum(value.updatedAt)
	);
}
