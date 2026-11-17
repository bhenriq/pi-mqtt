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
const IDENTIFIER = /^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$/;
export function isValidIdentifier(value) {
    return IDENTIFIER.test(value);
}
/** Topic layout. Nothing else in the codebase builds topic strings. */
export function topicsFor(cluster) {
    if (!isValidIdentifier(cluster)) {
        throw new Error(`Invalid cluster id: ${JSON.stringify(cluster)}`);
    }
    const base = `pi/v${PROTOCOL_VERSION}/${cluster}`;
    return {
        base,
        presence: (workerId) => `${base}/workers/${workerId}/presence`,
        orchestratorPresence: (id) => `${base}/orchestrators/${id}/presence`,
        control: (workerId) => `${base}/workers/${workerId}/ctl`,
        ack: (workerId) => `${base}/workers/${workerId}/ack`,
        /** Orchestrator → worker: pairing challenge, admission, revocation. */
        pair: (workerId) => `${base}/workers/${workerId}/pair`,
        result: (taskId) => `${base}/results/${taskId}`,
        /** Room-wide announcements, currently only "kick". */
        roomControl: `${base}/control`,
        presenceFilter: `${base}/workers/+/presence`,
        orchestratorFilter: `${base}/orchestrators/+/presence`,
        ackFilter: `${base}/workers/+/ack`,
        resultFilter: `${base}/results/+`,
    };
}
export function newTaskId() {
    return `t_${Date.now().toString(36)}_${crypto.randomUUID().slice(0, 8)}`;
}
/**
 * The exact user message a worker receives for a task.
 *
 * Framed on purpose: worker sessions are long-lived, a human may type into them
 * between tasks, and the prompt arrives from the broker, so the boundary has to
 * be explicit rather than "the last thing in the transcript".
 */
export function taskPromptFrame(task) {
    const lines = [`--- pi-mqtt task ${task.taskId} (from orchestrator) ---`, task.prompt.trim(), "--- end task ---"];
    if (task.repo) {
        lines.push("Work only inside the current repository. Leave the result as a commit or as files in the tree.");
    }
    lines.push("Your final assistant message is returned verbatim to the orchestrator. Keep it short and self-contained.");
    return lines.join("\n");
}
/**
 * The short code a human compares between the worker's screen and the orchestrator's.
 *
 * Derived from the worker key and both nonces: two different keys — a squatter's and
 * the real worker's — cannot produce the same code, and a relay in the middle cannot
 * make the two screens agree. Six digits, because the comparison is deliberate, not a
 * secret to guess.
 */
export function pairingCode(input) {
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
export function formatPairingCode(code) {
    return `${code.slice(0, 3)} ${code.slice(3)}`;
}
/** The statement a worker signs to prove it holds the key it advertises. */
export function joinStatement(input) {
    return ["pi-mqtt-join", input.room, input.workerId, input.nonce, input.key].join("\n");
}
/** Newlines and control characters make MQTT log/topic inspection painful. */
export function sanitizeText(value, max = 4000) {
    const trimmed = value.replace(/\u0000/g, "").trim();
    return trimmed.length > max ? `${trimmed.slice(0, max)}…` : trimmed;
}
export function decodePayload(payload) {
    try {
        const parsed = JSON.parse(new TextDecoder().decode(payload));
        if (!parsed || typeof parsed !== "object" || Array.isArray(parsed))
            return undefined;
        return parsed;
    }
    catch {
        return undefined;
    }
}
const isStr = (value) => typeof value === "string";
const isNum = (value) => typeof value === "number" && Number.isFinite(value);
function hasEnvelope(value, kind) {
    return value.v === PROTOCOL_VERSION && value.kind === kind;
}
export function isTaskMessage(value) {
    return (hasEnvelope(value, "task") &&
        isStr(value.taskId) &&
        isStr(value.to) &&
        isStr(value.prompt) &&
        (value.notify === "silent" || value.notify === "wake") &&
        isNum(value.createdAt));
}
export function isCancelMessage(value) {
    return hasEnvelope(value, "cancel") && isStr(value.taskId);
}
export function isKickMessage(value) {
    return hasEnvelope(value, "kick") && isStr(value.from);
}
export function isControlMessage(value) {
    return isTaskMessage(value) || isCancelMessage(value) || isKickMessage(value);
}
export function isPairMessage(value) {
    const step = value.step;
    return (hasEnvelope(value, "pair") &&
        isStr(value.workerId) &&
        isStr(value.orchestratorId) &&
        (step === "challenge" || step === "admit" || step === "revoke") &&
        isNum(value.at));
}
export function isAckMessage(value) {
    return hasEnvelope(value, "ack") && isStr(value.taskId) && isStr(value.workerId);
}
export function isResultMessage(value) {
    return (hasEnvelope(value, "result") &&
        isStr(value.taskId) &&
        isStr(value.workerId) &&
        isStr(value.text) &&
        ["done", "error", "timeout", "cancelled"].includes(String(value.status)));
}
export function isPresenceMessage(value) {
    return (hasEnvelope(value, "presence") &&
        isStr(value.workerId) &&
        isStr(value.state) &&
        ["idle", "busy", "offline", "pending"].includes(value.state) &&
        isNum(value.updatedAt));
}
