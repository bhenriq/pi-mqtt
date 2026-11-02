/**
 * Thin wrapper over mqtt.js: connection, JSON publish, subscription bookkeeping.
 *
 * Transport concerns only. No protocol semantics, no policy.
 */

import { readFileSync } from "node:fs";
import { connect, type IClientOptions, type IClientPublishOptions, type MqttClient } from "mqtt";
import type { MqttConfig } from "./config.ts";
import { decodePayload, type Json } from "./protocol.ts";

export interface PublishOptions {
	retain?: boolean;
	expirySec?: number;
}

export type MessageHandler = (topic: string, payload: Json) => void;
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
			options.log("info", `mqtt connected to ${config.url} as ${options.clientId}`);
			// clean:true means the broker forgot our subscriptions; re-apply them.
			for (const filter of this.handlers.keys()) {
				client.subscribe(filter, { qos: 1 }, (error) => {
					if (error) options.log("error", `subscribe ${filter} failed: ${error.message}`);
				});
			}
			options.onOnline?.();
		});
		client.on("reconnect", () => options.log("info", "mqtt reconnecting"));
		client.on("close", () => {
			if (!this.closed) options.log("warn", "mqtt connection closed");
		});
		client.on("error", (error) => options.log("error", `mqtt error: ${error.message}`));
		client.on("message", (topic, payload) => {
			const decoded = decodePayload(payload);
			if (!decoded) {
				options.log("warn", `ignoring non-JSON payload on ${topic}`);
				return;
			}
			for (const [filter, set] of this.handlers) {
				if (!topicMatches(filter, topic)) continue;
				for (const handler of set) handler(topic, decoded);
			}
		});

		return new Promise((resolve, reject) => {
			const timer = setTimeout(() => reject(new Error(`timeout connecting to ${config.url}`)), 15_000);
			client.once("connect", () => {
				clearTimeout(timer);
				resolve();
			});
			client.once("error", (error) => {
				clearTimeout(timer);
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
			client.publish(topic, Buffer.from(JSON.stringify(payload)), publishOptions, (error) => {
				if (error) reject(error);
				else resolve();
			});
		});
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
