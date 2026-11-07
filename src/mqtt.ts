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
	private reconnectCount = 0;
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
			reconnectPeriod: config.reconnectMs,
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
			options.log("info", `mqtt connected to ${config.url} as ${options.clientId}`);
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
		client.on("reconnect", () => options.log("info", "mqtt reconnecting"));
		client.on("close", () => {
			if (this.closed || !this.everConnected) return;
			this.reconnectCount += 1;
			if (!this.closeInfo) this.closeInfo = { at: Date.now(), source: "socket" };
			const info = this.closeInfo;
			options.log(
				"warn",
				`mqtt connection closed (${info.source}${info.code !== undefined ? " " + describeCloseCode(info.code) : ""}${info.reason ? `, ${info.reason}` : ""}), reconnecting`,
			);
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
			if (this.everConnected) options.log("error", `mqtt error: ${error.message}`);
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
		return new Promise((resolve, reject) => {
			const timer = setTimeout(() => {
				client.end(true);
				reject(new Error(`timeout connecting to ${config.url}`));
			}, 15_000);
			client.once("connect", () => {
				clearTimeout(timer);
				resolve();
			});
			client.once("error", (error) => {
				clearTimeout(timer);
				client.end(true);
				reject(error);
			});
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

	publish(topic: string, payload: unknown, options: PublishOptions = {}): Promise<void> {
		const client = this.client;
		if (!client) return Promise.reject(new Error("mqtt link is not connected"));
		const publishOptions: IClientPublishOptions = { qos: 1, retain: options.retain ?? false };
		if (options.expirySec !== undefined && this.config.protocolVersion === 5) {
			publishOptions.properties = { messageExpiryInterval: options.expirySec };
		}
		return new Promise((resolve, reject) => {
			// A publish queued while the socket is down never calls back; do not let a
			// caller (or a purge loop) hang on it.
			const timer = setTimeout(() => reject(new Error(`publish to ${topic} timed out`)), PUBLISH_TIMEOUT_MS);
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
		const client = this.client;
		this.client = undefined;
		if (!client) return;
		await new Promise<void>((resolve) => {
			client.end(false, {}, () => resolve());
		});
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
