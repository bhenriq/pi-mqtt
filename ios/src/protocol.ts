/**
 * Wire protocol for pi-mqtt, as an orchestrator sees it.
 *
 * Same topic layout and message shapes as the repo's src/protocol.ts, so a phone
 * running this can talk to workers running the desktop package unchanged. Payloads
 * from a shared broker are untrusted input: nothing is used without passing a guard.
 */

import { createHash } from "node:crypto";
import { IDENTIFIER } from "./util.ts";

export const PROTOCOL_VERSION = 1;
export const CLIENT_VERSION = "0.2.0-ios";

// ---------------------------------------------------------------------------
// Wire protocol (mirrors pi-mqtt's src/protocol.ts)
// ---------------------------------------------------------------------------

/** `pending`: joined, but no orchestrator has approved this session's key yet. */
export type WorkerState = "idle" | "busy" | "offline" | "pending";
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
	/** Long-term public key of this session (base64 DER, Ed25519). */
	key?: string;
	/** Per-join nonce, present while `pending`; makes an approval single-use. */
	nonce?: string;
	/** Ed25519 signature over the join statement, proving the key is theirs. */
	proof?: string;
}

export interface Topics {
	base: string;
	presence: (workerId: string) => string;
	orchestratorPresence: (id: string) => string;
	control: (workerId: string) => string;
	ack: (workerId: string) => string;
	/** Orchestrator → worker: pairing challenge, admission, revocation. */
	pair: (workerId: string) => string;
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
		pair: (workerId) => `${base}/workers/${workerId}/pair`,
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
		["idle", "busy", "offline", "pending"].includes(value.state) &&
		isNum(value.updatedAt)
	);
}

export function isPair(value: Json): value is Json & PairMessage {
	const step = value.step;
	return (
		value.v === PROTOCOL_VERSION &&
		value.kind === "pair" &&
		isStr(value.workerId) &&
		isStr(value.orchestratorId) &&
		(step === "challenge" || step === "admit" || step === "revoke") &&
		isNum(value.at)
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

// ---------------------------------------------------------------------------
// Worker admission (pairing)
//
// A worker joins as `pending` with its long-term key, a fresh nonce and a signature
// over both. An orchestrator either recognises the key and admits it silently, or
// answers with a challenge nonce: both ends derive the same six-digit code from the
// worker key and the two nonces, a human compares the two screens, and the code is
// typed back to approve. A squatter wearing the same worker id holds a different
// key, so it produces a different code.
// ---------------------------------------------------------------------------

export interface PairMessage {
	v: number;
	kind: "pair";
	step: "challenge" | "admit" | "revoke";
	workerId: string;
	orchestratorId: string;
	/** challenge: orchestrator nonce, and when the offer stops being usable. */
	nonce?: string;
	expiresAt?: number;
	/** admit/revoke: the worker key this decision applies to (fingerprint). */
	fingerprint?: string;
	at: number;
}

const B64_ALPHABET = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

/** base64 → bytes. No Buffer on iOS, and the keys arrive as base64 DER. */
export function base64ToBytes(value: string): Uint8Array {
	// Accept base64url too: fingerprints are base64url, keys are base64.
	const clean = value.replace(/-/g, "+").replace(/_/g, "/").replace(/[^A-Za-z0-9+/]/g, "");
	const out = new Uint8Array(Math.floor((clean.length * 3) / 4));
	let bits = 0;
	let count = 0;
	let index = 0;
	for (const char of clean) {
		const digit = B64_ALPHABET.indexOf(char);
		if (digit < 0) continue;
		bits = (bits << 6) | digit;
		count += 6;
		if (count >= 8) {
			count -= 8;
			out[index++] = (bits >> count) & 0xff;
		}
	}
	return out.subarray(0, index);
}

/** bytes → base64url without padding, to match the desktop's fingerprints. */
export function bytesToBase64Url(bytes: Uint8Array): string {
	let out = "";
	for (let i = 0; i < bytes.length; i += 3) {
		const a = bytes[i];
		const b = i + 1 < bytes.length ? bytes[i + 1] : undefined;
		const c = i + 2 < bytes.length ? bytes[i + 2] : undefined;
		out += B64_ALPHABET[a >> 2];
		out += B64_ALPHABET[((a & 3) << 4) | ((b ?? 0) >> 4)];
		if (b === undefined) break;
		out += B64_ALPHABET[((b & 15) << 2) | ((c ?? 0) >> 6)];
		if (c === undefined) break;
		out += B64_ALPHABET[c & 63];
	}
	return out.replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
}

const sha256 = (text: string): Uint8Array => createHash("sha256").update(text).digest() as unknown as Uint8Array;

/** Short, human-comparable hash of a worker key, e.g. "k7Q2f1x9" (same as the desktop). */
export function fingerprintOf(publicKey: string): string {
	try {
		const digest = createHash("sha256").update(base64ToBytes(publicKey)).digest() as unknown as Uint8Array;
		return bytesToBase64Url(digest).slice(0, 8);
	} catch {
		return "????????";
	}
}

/** The statement a worker signs to prove it holds the key it advertises. */
export function joinStatement(input: { room: string; workerId: string; nonce: string; key: string }): string {
	return ["pi-mqtt-join", input.room, input.workerId, input.nonce, input.key].join("\n");
}

/** The six digits a human compares between the worker's screen and this one. */
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
	const digest = sha256(canonical);
	const value = ((digest[0] << 24) | (digest[1] << 16) | (digest[2] << 8) | digest[3]) >>> 0;
	return String(value % 1_000_000).padStart(6, "0");
}

/** "481902" → "481 902", for reading aloud. */
export function formatPairingCode(code: string): string {
	return code.length === 6 ? `${code.slice(0, 3)} ${code.slice(3)}` : code;
}

/**
 * Check the worker's Ed25519 proof of its join.
 *
 * node:crypto on iOS has no verify/createPublicKey, so this goes through WebCrypto,
 * which supports Ed25519. Any doubt is a refusal.
 */
export async function verifyJoinProof(publicKey: string, statement: string, signature: string): Promise<boolean> {
	try {
		const subtle = (globalThis.crypto as any)?.subtle;
		if (!subtle) return false;
		// The key crosses the broker as base64 SPKI DER (44 bytes for Ed25519: a
		// 12-byte header and the 32 raw bytes); WebCrypto wants the raw 32.
		const der = base64ToBytes(publicKey);
		const raw = der.length === 44 ? der.subarray(12) : der;
		if (raw.length !== 32) return false;
		const key = await subtle.importKey("raw", raw, { name: "Ed25519" }, false, ["verify"]);
		const ok = await subtle.verify({ name: "Ed25519" }, key, base64ToBytes(signature), new TextEncoder().encode(statement));
		return ok === true;
	} catch {
		return false;
	}
}
