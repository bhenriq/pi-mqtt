/**
 * Thin wrapper over mqtt.js: connection, JSON publish, subscription bookkeeping.
 *
 * Transport concerns only. No protocol semantics, no policy.
 */

import { readFileSync } from "node:fs";
import { connect, type IClientOptions, type IClientPublishOptions, type MqttClient } from "mqtt";
import type { MqttConfig } from "./config.ts";
import { decodePayload, type Json } from "./protocol.ts";

const PUBLISH_TIMEOUT_MS = 4000;
/** Ceiling for the reconnect backoff: an unreachable broker is retried twice a minute. */
const MAX_RECONNECT_MS = 30_000;
/** Drops closer together than this are one incident, not one notification each. */
const FLAP_WINDOW_MS = 60_000;

export interface PublishOptions {
	retain?: boolean;
	expirySec?: number;
}

export interface CloseInfo {
	at: number;
	source: "ws" | "mqtt" | "socket";
	code?: number;
	reason?: string;
}

/** Turns a WebSocket close code into something readable in a log line. */
function describeCloseCode(code: number | undefined): string {
	if (code === undefined) return "no code";
	if (code === 1000) return "1000 normal";
	if (code === 1001) return "1001 going away";
	if (code === 1006) return "1006 abnormal (no close frame)";
	return `${code}`;
}

/** `payload` is undefined when a retained message was deleted (zero-length payload). */
export type MessageHandler = (topic: string, payload: Json | undefined) => void;
export type LogFn = (level: "info" | "warn" | "error", message: string) => void;

export interface LinkOptions {
	clientId: string;
	will?: { topic: string; payload: unknown };
	log: LogFn;
	onOnline?: () => void;
}

export class MqttLink {
	private client: MqttClient | undefined;
	private readonly handlers = new Map<string, Set<MessageHandler>>();
	private closed = false;
	/** Suppresses per-retry error spam until the first successful handshake. */
	private everConnected = false;
	private closeInfo: CloseInfo | undefined;
	/** Outages, not attempts: a minute of DNS failure is one drop, not thirty. */
	private reconnectCount = 0;
	/** Recent drops, to tell a long outage from a connection that keeps flapping. */
	private readonly recentDrops: number[] = [];
	/** Own retry loop: mqtt.js's fixed period cannot back off, so it is disabled. */
	private reconnectTimer: NodeJS.Timeout | undefined;
	private reconnectAttempts = 0;
	private offlineSince: number | undefined;
	/** Last reconnect error, so a repeated failure is not logged over and over. */
	private lastAttemptError: string | undefined;
	private readonly trackedStreams = new WeakSet<object>();
	/** Called after every successful (re)connect, including the first. */
	onReconnect?: () => void;
	/** Called when the broker evicted us because another session took the client id. */
	onTakenOver?: () => void;
	private takenOver = false;

	/** Most recent abnormal close, for /mqtt status. */
	get lastClose(): CloseInfo | undefined {
		return this.closeInfo;
	}

	get reconnects(): number {
		return this.reconnectCount;
	}

	constructor(
		private readonly config: MqttConfig,
		private readonly options: LinkOptions,
	) {}

	get online(): boolean {
		return this.client?.connected ?? false;
	}

	connect(): Promise<void> {
		const { config, options } = this;
		const clientOptions: IClientOptions = {
			clientId: options.clientId,
			protocolVersion: config.protocolVersion === 4 ? 4 : 5,
			clean: true,
			keepalive: config.keepaliveSec,
			// mqtt.js retries on a fixed interval, which turns a ten-minute DNS outage into
			// three hundred attempts and as many notifications. Reconnection is driven from
			// here instead, with backoff and one message per outage (see scheduleReconnect).
			reconnectPeriod: 0,
			connectTimeout: 10_000,
			username: config.username,
			password: config.password,
			rejectUnauthorized: config.rejectUnauthorized,
		};
		if (config.caFile) clientOptions.ca = readFileSync(config.caFile);
		if (options.will) {
			clientOptions.will = {
				topic: options.will.topic,
				payload: Buffer.from(JSON.stringify(options.will.payload)),
				qos: 1,
				retain: true,
			};
		}

		const client = connect(config.url, clientOptions);
		this.client = client;

		client.on("connect", () => {
			this.everConnected = true;
			this.clearReconnectTimer();
			const outage = this.offlineSince === undefined ? 0 : Date.now() - this.offlineSince;
			const attempts = this.reconnectAttempts;
			this.offlineSince = undefined;
			this.reconnectAttempts = 0;
			this.lastAttemptError = undefined;
			options.log("info", `mqtt connected to ${config.url} as ${options.clientId}`);
			if (outage > 0) {
				options.log("info", `mqtt reconnected after ${Math.round(outage / 1000)}s (${attempts} attempt${attempts === 1 ? "" : "s"})`);
			}
			// clean:true means the broker forgot our subscriptions; re-apply them.
			this.trackStream(client);
			for (const filter of this.handlers.keys()) {
				client.subscribe(filter, { qos: 1 }, (error) => {
					if (error) options.log("error", `subscribe ${filter} failed: ${error.message}`);
				});
			}
			options.onOnline?.();
			this.onReconnect?.();
		});
		client.on("close", () => {
			if (this.closed || this.takenOver || !this.everConnected) return;
			if (!this.closeInfo) this.closeInfo = { at: Date.now(), source: "socket" };
			const info = this.closeInfo;
			const detail = `${info.source}${info.code !== undefined ? " " + describeCloseCode(info.code) : ""}${info.reason ? `, ${info.reason}` : ""}`;
			// One warning per outage: everything after it is a retry, and the retries belong
			// in the log ring (bare /mqtt), not in the notification area where they would
			// march past a user who can do nothing about them.
			if (this.offlineSince === undefined) {
				this.reconnectCount += 1;
				this.offlineSince = Date.now();
				// One notification a minute, however often the socket dies: a flapping link
				// otherwise resets the backoff each time it reconnects and turns into a
				// notification every few seconds.
				const now = Date.now();
				this.recentDrops.push(now);
				while (this.recentDrops.length > 0 && now - (this.recentDrops[0] ?? now) > FLAP_WINDOW_MS) {
					this.recentDrops.shift();
				}
				if (this.recentDrops.length === 1) options.log("warn", `mqtt connection lost (${detail}); retrying in the background`);
				else options.log("info", `mqtt connection lost again (${detail}); ${this.recentDrops.length} drops in the last minute`);
			} else {
				options.log("info", `mqtt still offline (${detail})`);
			}
			this.scheduleReconnect();
		});
		// MQTT 5 server-initiated DISCONNECT carries a reason code: the broker is
		// telling us why, which a bare socket close never does.
		client.on("disconnect", (packet) => {
			const reasonCode = (packet as { reasonCode?: number } | undefined)?.reasonCode;
			this.closeInfo = { at: Date.now(), source: "mqtt", code: reasonCode, reason: "broker DISCONNECT" };
			// 0x8E "Session taken over": the deterministic client id means another
			// session claimed this id. Reconnecting would evict it and start a fight,
			// so stop and let the newcomer own the id.
			if (reasonCode === 142 && !this.takenOver) {
				this.takenOver = true;
				options.log("warn", `client id taken over by another session; not reconnecting`);
				this.onTakenOver?.();
				client.end(true);
			}
		});

		client.on("error", (error) => {
			// Before the first handshake the connect() rejection reports it once.
			if (!this.everConnected || this.takenOver) return;
			if (this.offlineSince === undefined) {
				// A live connection reporting an error is news.
				options.log("error", `mqtt error: ${error.message}`);
				return;
			}
			// While retrying, the same failure repeating is not news: log it when it changes.
			if (error.message !== this.lastAttemptError) {
				this.lastAttemptError = error.message;
				options.log("info", `mqtt retry failed: ${error.message}`);
			}
		});
		client.on("message", (topic, payload) => {
			// A zero-length payload on a retained topic is a deletion, not garbage:
			// pass it through so registries can drop the entry.
			const decoded = payload.length === 0 ? undefined : decodePayload(payload);
			if (decoded === undefined && payload.length > 0) {
				options.log("warn", `ignoring non-JSON payload on ${topic}`);
				return;
			}
			for (const [filter, set] of this.handlers) {
				if (!topicMatches(filter, topic)) continue;
				for (const handler of set) handler(topic, decoded);
			}
		});

		// A failed handshake must not leave a reconnect loop running: the caller
		// gets one rejection and a closed client, and decides whether to retry.
		//
		// These listeners are removed the moment the handshake settles. Left armed,
		// the error listener would greet every *later* error — a keepalive timeout
		// after a sleeping laptop, say — with `end(true)`, which tears the client
		// down and stops mqtt.js from ever reconnecting: a silently dead link that
		// still reports itself as configured.
		return new Promise<void>((resolve, reject) => {
			let timer: NodeJS.Timeout | undefined;
			const settle = () => {
				clearTimeout(timer);
				timer = undefined;
				client.off("connect", onConnect);
				client.off("error", onError);
			};
			function onConnect(): void {
				settle();
				resolve();
			}
			function onError(error: Error): void {
				settle();
				client.end(true);
				reject(error);
			}
			timer = setTimeout(() => {
				settle();
				client.end(true);
				reject(new Error(`timeout connecting to ${config.url}`));
			}, 15_000);
			client.on("connect", onConnect);
			client.on("error", onError);
		});
	}

	subscribe(filter: string, handler: MessageHandler): () => void {
		let set = this.handlers.get(filter);
		if (!set) {
			set = new Set();
			this.handlers.set(filter, set);
			this.client?.subscribe(filter, { qos: 1 }, (error) => {
				if (error) this.options.log("error", `subscribe ${filter} failed: ${error.message}`);
			});
		}
		set.add(handler);
		return () => {
			const current = this.handlers.get(filter);
			if (!current) return;
			current.delete(handler);
			if (current.size === 0) {
				this.handlers.delete(filter);
				this.client?.unsubscribe(filter);
			}
		};
	}

	publish(topic: string, payload: unknown, options: PublishOptions = {}, waitMs = PUBLISH_TIMEOUT_MS): Promise<void> {
		const client = this.client;
		if (!client) return Promise.reject(new Error("mqtt link is not connected"));
		const publishOptions: IClientPublishOptions = { qos: 1, retain: options.retain ?? false };
		if (options.expirySec !== undefined && this.config.protocolVersion === 5) {
			publishOptions.properties = { messageExpiryInterval: options.expirySec };
		}
		return new Promise((resolve, reject) => {
			// A publish queued while the socket is down never calls back; do not let a
			// caller (or a purge loop) hang on it.
			const timer = setTimeout(() => reject(new Error(`publish to ${topic} timed out`)), waitMs);
			client.publish(topic, Buffer.from(JSON.stringify(payload)), publishOptions, (error) => {
				clearTimeout(timer);
				if (error) reject(error);
				else resolve();
			});
		});
	}

	/**
	 * Delete retained messages, one topic per connection.
	 *
	 * A zero-length retained PUBLISH is the only way to delete a retained message, and
	 * mqtt.js mishandles it: over MQTT 5 mosquitto answers DISCONNECT 130 "protocol
	 * error", and over MQTT 3.1.1 the first deletion works but the connection closes
	 * right after it. So: MQTT 3.1.1, and a fresh connection per topic. Deletions are a
	 * rare, explicit action, so the extra handshakes are acceptable.
	 */
	async deleteRetained(topics: string[]): Promise<{ ok: number; failed: number }> {
		let ok = 0;
		let failed = 0;
		for (const topic of topics) {
			try {
				await this.deleteOneRetained(topic);
				ok += 1;
			} catch {
				failed += 1;
			}
		}
		return { ok, failed };
	}

	private async deleteOneRetained(topic: string): Promise<void> {
		const { config } = this;
		const client = connect(config.url, {
			clientId: `pi-mqtt-delete-${Math.random().toString(36).slice(2, 8)}`,
			protocolVersion: 4,
			clean: true,
			reconnectPeriod: 0,
			connectTimeout: 10_000,
			username: config.username,
			password: config.password,
			rejectUnauthorized: config.rejectUnauthorized,
			...(config.caFile ? { ca: readFileSync(config.caFile) } : {}),
		});
		try {
			await new Promise<void>((resolve, reject) => {
				const timer = setTimeout(() => reject(new Error("delete connect timed out")), 10_000);
				client.once("connect", () => {
					clearTimeout(timer);
					resolve();
				});
				client.once("error", (error) => {
					clearTimeout(timer);
					reject(error);
				});
			});
			await new Promise<void>((resolve, reject) => {
				const timer = setTimeout(() => reject(new Error("delete publish timed out")), PUBLISH_TIMEOUT_MS);
				client.publish(topic, "", { qos: 1, retain: true }, (error) => {
					clearTimeout(timer);
					if (error) reject(error);
					else resolve();
				});
			});
		} finally {
			await new Promise<void>((resolve) => client.end(false, {}, () => resolve()));
		}
	}

	async publishEmpty(topic: string): Promise<void> {
		const { failed } = await this.deleteRetained([topic]);
		if (failed > 0) throw new Error(`could not delete ${topic}`);
	}

	async close(): Promise<void> {
		this.closed = true;
		this.clearReconnectTimer();
		const client = this.client;
		this.client = undefined;
		if (!client) return;
		// A clean goodbye when the socket works, and no waiting when it does not: quitting
		// must not hang behind a half-open connection, which is exactly when people quit.
		let settled = false;
		await new Promise<void>((resolve) => {
			const timer = setTimeout(() => resolve(), client.connected ? 1000 : 50);
			timer.unref?.();
			client.end(false, {}, () => {
				settled = true;
				clearTimeout(timer);
				resolve();
			});
		});
		if (!settled) {
			const stream = (client as unknown as { stream?: { destroy?: () => void } }).stream;
			stream?.destroy?.();
		}
	}

	/**
	 * Retry on our own schedule: reconnectMs, doubling, capped.
	 *
	 * mqtt.js retries on a fixed period, which cannot back off, so its loop is switched
	 * off at connect time and the retries are driven from here. One timer at a time, and
	 * a successful connect resets the count — a blip still costs the configured 2s, while
	 * an unreachable broker settles down to twice a minute instead of hammering DNS.
	 */
	private scheduleReconnect(): void {
		if (this.closed || this.takenOver || this.reconnectTimer) return;
		const client = this.client;
		if (!client) return;
		const delay = Math.min(this.config.reconnectMs * 2 ** this.reconnectAttempts, MAX_RECONNECT_MS);
		this.reconnectAttempts += 1;
		this.reconnectTimer = setTimeout(() => {
			this.reconnectTimer = undefined;
			if (this.closed || this.takenOver) return;
			client.reconnect();
		}, delay);
		this.reconnectTimer.unref?.();
	}

	private clearReconnectTimer(): void {
		if (this.reconnectTimer) clearTimeout(this.reconnectTimer);
		this.reconnectTimer = undefined;
	}

	/** Attach close-code reporting to the transport socket, once per socket. */
	private trackStream(client: MqttClient): void {
		const stream = (client as unknown as { stream?: { on?: (event: string, cb: (...args: unknown[]) => void) => void } }).stream;
		if (!stream || typeof stream.on !== "function") return;
		if (this.trackedStreams.has(stream as object)) return;
		this.trackedStreams.add(stream as object);
		stream.on("close", (code?: unknown, reason?: unknown) => {
			this.closeInfo = {
				at: Date.now(),
				source: "ws",
				code: typeof code === "number" ? code : undefined,
				reason: reason instanceof Uint8Array ? new TextDecoder().decode(reason).slice(0, 60) : undefined,
			};
		});
	}
}

/** MQTT topic filter matching (`+` single level, `#` trailing multi-level). */
export function topicMatches(filter: string, topic: string): boolean {
	const f = filter.split("/");
	const t = topic.split("/");
	for (let i = 0; i < f.length; i += 1) {
		const segment = f[i];
		if (segment === "#") return true;
		if (segment === "+") continue;
		if (segment !== t[i]) return false;
	}
	return f.length === t.length;
}
