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
}

export type PresencePatch = Partial<
	Pick<PresenceMessage, "state" | "activeTaskId" | "startedAt" | "model" | "repo" | "branch" | "headSha">
>;

export function buildPresence(
	identity: PresenceIdentity,
	state: WorkerState,
	patch: PresencePatch = {},
): PresenceMessage {
	return {
		v: PROTOCOL_VERSION,
		kind: "presence",
		version: CLIENT_VERSION,
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
			finish(isPresenceMessage(payload) ? payload : undefined);
		});
		const timer = setTimeout(() => finish(undefined), timeoutMs);
		timer.unref?.();
	});
}

export class Presence {
	private current: PresenceMessage;
	private timer: NodeJS.Timeout | undefined;
	private stopped = false;

	constructor(
		private readonly link: MqttLink,
		private readonly topics: Topics,
		identity: PresenceIdentity,
		private readonly heartbeatMs: number,
		private readonly log: LogFn,
	) {
		this.current = buildPresence(identity, "idle");
	}

	start(): void {
		void this.publish();
		this.timer = setInterval(() => void this.publish(), this.heartbeatMs);
		this.timer.unref?.();
	}

	/** Publish a state change immediately (and restart the heartbeat window). */
	update(patch: PresencePatch): void {
		if (this.stopped) return;
		this.current = { ...this.current, ...patch, updatedAt: Date.now() };
		void this.publish();
	}

	/** Republish the current state now (used after a reconnect). */
	republish(): void {
		if (this.stopped) return;
		this.current = { ...this.current, updatedAt: Date.now() };
		void this.publish();
	}

	stop(): void {
		if (this.stopped) return;
		this.stopped = true;
		if (this.timer) clearInterval(this.timer);
		const payload = { ...this.current, state: "offline" as const, updatedAt: Date.now() };
		void this.link.publish(this.topics.presence(this.current.workerId), payload, { retain: true });
	}

	private async publish(): Promise<void> {
		if (this.stopped || !this.link.online) return;
		try {
			await this.link.publish(
				this.topics.presence(this.current.workerId),
				{ ...this.current, updatedAt: Date.now() },
				{ retain: true },
			);
		} catch (error) {
			this.log("warn", `presence publish failed: ${(error as Error).message}`);
		}
	}
}
