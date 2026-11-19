/**
 * Presence: retained `workers/<id>/presence` messages plus a heartbeat.
 *
 * Retained means `list_workers` is a local map read, not a request/response.
 * The Last Will (built in extension.ts) covers crashes; `stop()` covers orderly
 * shutdown.
 */

import type { MqttLink, LogFn } from "./mqtt.ts";
import {
	CLIENT_VERSION,
	PROTOCOL_VERSION,
	isPresenceMessage,
	type PresenceMessage,
	type Topics,
	type WorkerState,
} from "./protocol.ts";

export interface PresenceIdentity {
	workerId: string;
	host: string;
	cwd: string;
	repo?: string;
	branch?: string;
	headSha?: string;
	model?: string;
	/** This machine's long-term public key: what an orchestrator approves. */
	key?: string;
	/** What this client understands; absent means an older client. */
	capabilities?: string[];
}

/** A heartbeat is not worth four seconds of waiting; the next one is due in 30. */
const HEARTBEAT_TIMEOUT_MS = 2000;

export type PresencePatch = Partial<
	Pick<
		PresenceMessage,
		"state" | "activeTaskId" | "startedAt" | "model" | "repo" | "branch" | "headSha" | "nonce" | "proof"
	>
>;

export function buildPresence(
	identity: PresenceIdentity,
	state: WorkerState,
	patch: PresencePatch = {},
	role: "worker" | "orchestrator" = "worker",
): PresenceMessage {
	return {
		v: PROTOCOL_VERSION,
		kind: "presence",
		version: CLIENT_VERSION,
		role,
		...identity,
		state,
		updatedAt: Date.now(),
		...patch,
	};
}

/**
 * Read the retained presence for one worker id, if any.
 *
 * Used on startup to refuse a second live session claiming an id that is already
 * in use: two sessions under one id both consume tasks, and their results race.
 */
export async function probePresence(
	link: MqttLink,
	topics: Topics,
	workerId: string,
	timeoutMs: number,
): Promise<PresenceMessage | undefined> {
	const topic = topics.presence(workerId);
	return new Promise((resolve) => {
		let settled = false;
		const finish = (value: PresenceMessage | undefined) => {
			if (settled) return;
			settled = true;
			clearTimeout(timer);
			unsubscribe();
			resolve(value);
		};
		const unsubscribe = link.subscribe(topic, (_topic, payload) => {
			// A deleted retained presence means "nobody is using this id".
			finish(payload !== undefined && isPresenceMessage(payload) ? payload : undefined);
		});
		const timer = setTimeout(() => finish(undefined), timeoutMs);
		timer.unref?.();
	});
}

export class Presence {
	private current: PresenceMessage;
	private timer: NodeJS.Timeout | undefined;
	private stopped = false;
	/** Set by `announce()`, so `start()` does not publish the join twice. */
	private announced = false;
	/** Do not stack heartbeats when the broker is not answering. */
	private publishing = false;
	/** Whether the current streak of failed publishes has already been reported. */
	private failing = false;
	/** Heartbeats that failed in a row; two is enough to declare the socket wedged. */
	private consecutiveFailures = 0;
	/** When the next heartbeat was due, so the delay can be reported to a prober. */
	private nextBeatDueAt = 0;
	/** How late the last heartbeat fired. A throttled process shows this. */
	private lastLagMs = 0;

	constructor(
		private readonly link: MqttLink,
		private readonly topic: string,
		identity: PresenceIdentity,
		private readonly heartbeatMs: number,
		private readonly log: LogFn,
		/** Retained presence lifetime: a dead worker's entry disappears on its own. */
		private readonly expirySec = 86_400,
		private readonly role: "worker" | "orchestrator" = "worker",
	) {
		this.current = buildPresence(identity, "idle", {}, role);
	}

	/** Current advertised state, so callers can tell a busy worker from an idle one. */
	get state(): WorkerState {
		return this.current.state;
	}

	/**
	 * Set fields without publishing, for building the state a session *joins* with.
	 *
	 * A worker's first announcement is `pending` plus the nonce and proof of its join; it
	 * must not be published as `idle` first, or an orchestrator would briefly see a
	 * dispatchable worker that has not been approved.
	 */
	seed(patch: PresencePatch): void {
		if (this.stopped) return;
		this.current = { ...this.current, ...patch, updatedAt: Date.now() };
	}

	start(): void {
		if (!this.announced) void this.publish(HEARTBEAT_TIMEOUT_MS);
		this.nextBeatDueAt = Date.now() + this.heartbeatMs;
		this.timer = setInterval(() => {
			const now = Date.now();
			this.lastLagMs = Math.max(0, now - this.nextBeatDueAt);
			this.nextBeatDueAt = now + this.heartbeatMs;
			void this.publish(HEARTBEAT_TIMEOUT_MS);
		}, this.heartbeatMs);
		this.timer.unref?.();
	}

	/** How late the heartbeat timer ran, in ms. Evidence that the process was busy. */
	get heartbeatLagMs(): number {
		return this.lastLagMs;
	}

	/** The task this presence advertises, if any. */
	get activeTaskId(): string | undefined {
		return this.current.activeTaskId;
	}

	/**
	 * Publish presence once and let a failure through, unlike the heartbeat.
	 *
	 * This first publish is also the credential check. A role is a capability on the
	 * broker now: a worker credential writing an orchestrator presence is acknowledged
	 * with 0x87 "not authorized". Swallowing that would leave a session that believes it
	 * joined, hears nothing, and blames the workers — the failure mode this project has
	 * already spent a day on.
	 */
	async announce(): Promise<void> {
		await this.link.publish(
			this.topic,
			{ ...this.current, updatedAt: Date.now() },
			{ retain: true, expirySec: this.expirySec },
		);
		this.announced = true;
	}

	/** Publish a state change immediately (and restart the heartbeat window). */
	update(patch: PresencePatch): void {
		if (this.stopped) return;
		this.current = { ...this.current, ...patch, updatedAt: Date.now() };
		void this.publish();
	}

	/**
	 * Leave the room.
	 *
	 * Deletes the retained presence rather than writing `offline`: a session that
	 * disconnected is gone, and an entry saying "offline" only invites the next reader to
	 * wonder when. A crash still leaves `offline` behind, because the Last Will is fixed at
	 * connect time — that is the honest difference between leaving and dying.
	 */
	async stop(): Promise<void> {
		if (this.stopped) return;
		this.stopped = true;
		if (this.timer) clearInterval(this.timer);
		// Nothing to clear on the broker if we are not on it; trying would open a second
		// connection and wait for it, which is the wrong way to spend a quit.
		if (!this.link.online) return;
		try {
			// Bounded: leaving must not hold up quitting when the broker is unreachable.
			await Promise.race([
				this.link.publishEmpty(this.topic),
				new Promise((resolve) => setTimeout(resolve, 2500)),
			]);
		} catch (error) {
			this.log("warn", `could not clear presence on leave: ${(error as Error).message}`);
		}
	}

	/** Fire-and-forget leave, for paths that cannot await. */
	forget(): void {
		void this.stop();
	}

	/** Republish the current state now (used after a reconnect). */
	republish(): void {
		if (this.stopped) return;
		this.current = { ...this.current, updatedAt: Date.now() };
		void this.publish();
	}

	private async publish(waitMs = HEARTBEAT_TIMEOUT_MS): Promise<void> {
		if (this.stopped || this.publishing || !this.link.online) return;
		this.publishing = true;
		const startedAt = Date.now();
		try {
			await this.link.publish(
				this.topic,
				{ ...this.current, updatedAt: Date.now() },
				{ retain: true, expirySec: this.expirySec },
				waitMs,
			);
			if (this.failing) {
				this.failing = false;
				this.consecutiveFailures = 0;
				this.log("info", "presence is being delivered again");
			}
		} catch (error) {
			// A rejection that arrives long after the timeout is not a slow broker: it is a
			// late timer, i.e. this process was not running for a while (macOS defers timers
			// for backgrounded apps). Treating that as a dead socket is how a throttled laptop
			// ends up recycling a perfectly good connection once a minute.
			const late = Date.now() - startedAt > waitMs + 750;
			if (late) {
				this.log("info", `heartbeat rejected late (${Date.now() - startedAt}ms); the event loop was stalled, not the link`);
				return;
			}
			// A heartbeat that the broker did not acknowledge is one symptom of a link that
			// is already reporting itself; a warning per beat is noise on top of noise, and
			// the link shares one notification window with us.
			this.consecutiveFailures += 1;
			const message = `presence is not being delivered: ${(error as Error).message}`;
			if (!this.failing) {
				this.failing = true;
				this.link.reportTrouble(message);
			} else {
				this.log("info", `presence publish failed again: ${(error as Error).message}`);
			}
			// Two beats with nothing acknowledged: the socket is passing nothing, whatever
			// the keepalive deadline thinks. Recycle it rather than wait to be told.
			if (this.consecutiveFailures === 2) this.link.recycle("heartbeat not acknowledged");
		} finally {
			this.publishing = false;
		}
	}
}
