/**
 * MQTT 3.1.1 over WebSocket, for pi on iOS.
 *
 * pi on iOS has no Node `net`/`tls`, so mqtt.js cannot open a raw MQTT connection.
 * WebSocket is available and mosquitto serves MQTT over WebSocket, so this module
 * implements the handful of packets an orchestrator needs: CONNECT (with a Will),
 * SUBSCRIBE, PUBLISH/PUBACK, PINGREQ, DISCONNECT, plus reconnection and
 * subscription bookkeeping.
 *
 * Transport only: no protocol semantics, no policy. Those live in room.ts.
 */

import type { LogFn } from "./util.ts";

// ---------------------------------------------------------------------------
// Byte helpers (no Buffer on iOS)
// ---------------------------------------------------------------------------

const textEncoder = new TextEncoder();
const textDecoder = new TextDecoder();

function concat(parts: Uint8Array[]): Uint8Array {
	let length = 0;
	for (const part of parts) length += part.length;
	const out = new Uint8Array(length);
	let offset = 0;
	for (const part of parts) {
		out.set(part, offset);
		offset += part.length;
	}
	return out;
}

function u16(value: number): Uint8Array {
	return new Uint8Array([(value >> 8) & 0xff, value & 0xff]);
}

function readU16(bytes: Uint8Array, offset: number): number {
	return ((bytes[offset] ?? 0) << 8) | (bytes[offset + 1] ?? 0);
}

/** MQTT UTF-8 string: two-byte length, then the bytes. */
function mqttString(value: string): Uint8Array {
	const bytes = textEncoder.encode(value);
	return concat([u16(bytes.length), bytes]);
}

/** MQTT binary data: same framing as a string. */
function mqttBinary(bytes: Uint8Array): Uint8Array {
	return concat([u16(bytes.length), bytes]);
}

/** MQTT "remaining length" varint. */
function varint(value: number): Uint8Array {
	const out: number[] = [];
	let rest = value;
	do {
		let digit = rest % 128;
		rest = Math.floor(rest / 128);
		if (rest > 0) digit |= 0x80;
		out.push(digit);
	} while (rest > 0);
	return new Uint8Array(out);
}

function readVarint(bytes: Uint8Array, offset: number): { value: number; bytes: number } | undefined {
	let value = 0;
	let multiplier = 1;
	let index = offset;
	for (let i = 0; i < 4; i += 1) {
		if (index >= bytes.length) return undefined;
		const digit = bytes[index]!;
		index += 1;
		value += (digit & 0x7f) * multiplier;
		if ((digit & 0x80) === 0) return { value, bytes: index - offset };
		multiplier *= 128;
	}
	return undefined;
}

function packet(type: number, body: Uint8Array): Uint8Array {
	return concat([new Uint8Array([type]), varint(body.length), body]);
}

/** MQTT topic filter matching (`+` single level, `#` trailing multi-level). */
function topicMatches(filter: string, topic: string): boolean {
	const f = filter.split("/");
	const t = topic.split("/");
	for (let i = 0; i < f.length; i += 1) {
		if (f[i] === "#") return true;
		if (f[i] === "+") continue;
		if (f[i] !== t[i]) return false;
	}
	return f.length === t.length;
}

function describeConnack(code: number): string {
	switch (code) {
		case 0:
			return "accepted";
		case 1:
			return "refused: unacceptable protocol version";
		case 2:
			return "refused: client id rejected";
		case 3:
			return "refused: server unavailable";
		case 4:
			return "refused: bad username or password";
		case 5:
			return "refused: not authorised — the broker grants a role per credential, so this needs an orchestrator credential (room password plus the broker user that carries the orchestrator role, /mqtt setup)";
		default:
			return `refused: CONNACK ${code}`;
	}
}

// ---------------------------------------------------------------------------
// MQTT 3.1.1 over WebSocket
// ---------------------------------------------------------------------------

export type MessageHandler = (topic: string, payload: string | undefined) => void;
export type { LogFn };

export interface WillSpec {
	topic: string;
	payload: string;
	qos?: 0 | 1;
	retain?: boolean;
}

export interface MqttWsOptions {
	url: string;
	clientId: string;
	username?: string;
	password?: string;
	keepaliveSec?: number;
	will?: WillSpec;
	log: LogFn;
}

const WebSocketCtor: any = (globalThis as any).WebSocket;

/**
 * One MQTT connection over one WebSocket.
 *
 * Transport only: CONNECT/SUBSCRIBE/PUBLISH/PUBACK/ping, reconnection and
 * subscription bookkeeping. Protocol semantics live in Room.
 */
export class MqttWs {
	private ws: any;
	private buffer = new Uint8Array(0);
	private nextPacketId = 1;
	private readonly inflight = new Map<number, { resolve: () => void; reject: (error: Error) => void; timer: any }>();
	private readonly subscriptions = new Map<string, Set<MessageHandler>>();
	private connack: { resolve: (code: number) => void; reject: (error: Error) => void } | undefined;
	private pingTimer: any;
	private reconnectTimer: any;
	private lastInbound = 0;
	private attempts = 0;
	/** Timestamps of recent closes, for the "this keeps happening" guard. */
	private recentCloses: number[] = [];
	private stopped = false;
	/** Human-readable reason for the last drop, for status output. */
	lastClose: { at: number; reason: string } | undefined;
	reconnects = 0;
	online = false;
	/** Called after each successful (re)connect, including the first. */
	onOnline?: () => void;
	onOffline?: (reason: string) => void;

	constructor(private readonly options: MqttWsOptions) {}

	get url(): string {
		return this.options.url;
	}

	async connect(): Promise<void> {
		if (typeof WebSocketCtor !== "function") {
			throw new Error("this runtime has no WebSocket: MQTT over WebSocket is unavailable");
		}
		this.stopped = false;
		await this.open();
	}

	/** Open one connection and resolve once CONNACK says yes. */
	private open(): Promise<void> {
		const { options } = this;
		return new Promise<void>((resolve, reject) => {
			let ws: any;
			try {
				ws = new WebSocketCtor(options.url, ["mqtt"]);
			} catch (error) {
				reject(new Error(`cannot open ${options.url}: ${(error as Error).message}`));
				return;
			}
			this.ws = ws;
			ws.binaryType = "arraybuffer";

			let settled = false;
			const settle = (error?: Error) => {
				if (settled) return;
				settled = true;
				clearTimeout(deadline);
				if (error) reject(error);
				else resolve();
			};
			const deadline = setTimeout(() => {
				settle(new Error(`timed out connecting to ${options.url}`));
				try {
					ws.close();
				} catch {
					// already gone
				}
			}, 15_000);

			this.connack = {
				resolve: (code) => {
					if (code !== 0) {
						settle(new Error(describeConnack(code)));
						try {
							ws.close();
						} catch {
							// already gone
						}
						return;
					}
					this.online = true;
					this.lastInbound = Date.now();
					this.attempts = 0;
					this.recentCloses = [];
					this.startPing();
					// clean session: the broker forgot our filters, so re-apply them.
					for (const filter of this.subscriptions.keys()) this.sendSubscribe(filter);
					try {
						this.onOnline?.();
					} catch {
						// a subscriber's bug must not break the link
					}
					options.log("info", `connected to ${options.url} as ${options.clientId}`);
					settle();
				},
				reject: (error) => settle(error),
			};

			ws.onopen = () => {
				this.write(this.connectPacket());
			};
			ws.onmessage = (event: any) => {
				const data = event?.data;
				if (typeof data === "string") return; // the broker only sends binary frames
				if (data instanceof ArrayBuffer) {
					this.feed(new Uint8Array(data));
					return;
				}
				if (data && typeof data.arrayBuffer === "function") {
					data.arrayBuffer().then((buffer: ArrayBuffer) => this.feed(new Uint8Array(buffer)));
					return;
				}
				try {
					this.feed(new Uint8Array(data));
				} catch {
					options.log("warn", "ignoring a frame the transport could not decode");
				}
			};
			ws.onerror = () => {
				// WebSocket gives no detail; onclose follows with a code.
			};
			ws.onclose = (event: any) => {
				const wasOnline = this.online;
				this.online = false;
				this.stopPing();
				this.ws = undefined;
				this.buffer = new Uint8Array(0);
				const reason = `closed${event?.code ? ` ${event.code}` : ""}${event?.reason ? `: ${event.reason}` : ""}`;
				this.lastClose = { at: Date.now(), reason };
				for (const pending of this.inflight.values()) {
					clearTimeout(pending.timer);
					pending.reject(new Error(`connection ${reason}`));
				}
				this.inflight.clear();
				this.connack?.reject(new Error(`connection ${reason}`));
				this.connack = undefined;
				if (!settled) settle(new Error(`connection ${reason}`));
				this.recentCloses.push(Date.now());
				this.recentCloses = this.recentCloses.filter((at) => Date.now() - at < 20_000);
				if (this.recentCloses.length >= 5) {
					// Something is systematically killing the connection (a second session
					// being handed the same id, a proxy that will not hold a WebSocket).
					// Reconnecting forever would hide that; say it once and stop.
					this.stopped = true;
					options.log("error", `the broker closed this connection 5 times in 20s (${reason}); stopped reconnecting — /mqtt connect to try again`);
				}
				if (wasOnline) {
					options.log("warn", `broker connection ${reason}; reconnecting`);
					try {
						this.onOffline?.(reason);
					} catch {
						// ignore
					}
				}
				if (!this.stopped) this.scheduleReconnect();
			};
		});
	}

	private connectPacket(): Uint8Array {
		const { options } = this;
		const keepalive = options.keepaliveSec ?? 30;
		const will = options.will;
		let flags = 0x02; // clean session
		if (will) {
			flags |= 0x04;
			flags |= (will.qos ?? 1) << 3;
			if (will.retain !== false) flags |= 0x20;
		}
		if (options.username !== undefined) flags |= 0x80;
		if (options.password !== undefined) flags |= 0x40;

		const payload: Uint8Array[] = [mqttString(options.clientId)];
		if (will) payload.push(mqttString(will.topic), mqttBinary(textEncoder.encode(will.payload)));
		if (options.username !== undefined) payload.push(mqttString(options.username));
		if (options.password !== undefined) payload.push(mqttBinary(textEncoder.encode(options.password)));

		const variableHeader = concat([
			mqttString("MQTT"),
			new Uint8Array([0x04, flags]),
			u16(keepalive),
		]);
		return packet(0x10, concat([variableHeader, ...payload]));
	}

	private scheduleReconnect(): void {
		if (this.stopped || this.reconnectTimer) return;
		const delay = Math.min(30_000, 2_000 * 2 ** Math.min(this.attempts, 4));
		this.attempts += 1;
		this.reconnects += 1;
		this.reconnectTimer = setTimeout(() => {
			this.reconnectTimer = undefined;
			if (this.stopped) return;
			this.open().catch((error: Error) => {
				this.options.log("warn", `reconnect failed: ${error.message}`);
				this.scheduleReconnect();
			});
		}, delay);
	}

	private startPing(): void {
		this.stopPing();
		const keepalive = (this.options.keepaliveSec ?? 30) * 1000;
		const every = Math.max(5_000, Math.floor(keepalive * 0.6));
		this.pingTimer = setInterval(() => {
			if (!this.online) return;
			if (Date.now() - this.lastInbound > keepalive * 2) {
				this.options.log("warn", "no traffic from the broker; dropping the socket to reconnect");
				try {
					this.ws?.close();
				} catch {
					// already gone
				}
				return;
			}
			this.write(new Uint8Array([0xc0, 0x00])); // PINGREQ
		}, every);
	}

	private stopPing(): void {
		if (!this.pingTimer) return;
		clearInterval(this.pingTimer);
		this.pingTimer = undefined;
	}

	private write(bytes: Uint8Array): void {
		const ws = this.ws;
		if (!ws || ws.readyState !== 1) return;
		// An exact ArrayBuffer: some WebSocket implementations reject views.
		const copy = new Uint8Array(bytes.length);
		copy.set(bytes);
		ws.send(copy.buffer);
	}

	private feed(chunk: Uint8Array): void {
		this.lastInbound = Date.now();
		this.buffer = this.buffer.length === 0 ? chunk : concat([this.buffer, chunk]);
		for (;;) {
			if (this.buffer.length < 2) return;
			const header = this.buffer[0]!;
			const length = readVarint(this.buffer, 1);
			if (!length) return;
			const total = 1 + length.bytes + length.value;
			if (this.buffer.length < total) return;
			const body = this.buffer.slice(1 + length.bytes, total);
			this.buffer = this.buffer.slice(total);
			this.handlePacket(header, body);
		}
	}

	private handlePacket(header: number, body: Uint8Array): void {
		const type = header >> 4;
		switch (type) {
			case 2: {
				// CONNACK
				const code = body[1] ?? 255;
				const pending = this.connack;
				this.connack = undefined;
				pending?.resolve(code);
				return;
			}
			case 3: {
				// PUBLISH
				const qos = (header >> 1) & 0x03;
				const topicLength = readU16(body, 0);
				const topic = textDecoder.decode(body.subarray(2, 2 + topicLength));
				let offset = 2 + topicLength;
				if (qos > 0) {
					const packetId = readU16(body, offset);
					offset += 2;
					this.write(concat([new Uint8Array([0x40]), new Uint8Array([0x02]), u16(packetId)])); // PUBACK
				}
				const text = textDecoder.decode(body.subarray(offset));
				this.dispatch(topic, text.length === 0 ? undefined : text);
				return;
			}
			case 4: {
				// PUBACK
				const packetId = readU16(body, 0);
				const pending = this.inflight.get(packetId);
				if (!pending) return;
				clearTimeout(pending.timer);
				this.inflight.delete(packetId);
				pending.resolve();
				return;
			}
			case 9: {
				// SUBACK: a refusal (0x80) only costs a missing filter, and the filter is
				// already visible in the log; nothing else to do.
				return;
			}
			case 13: {
				// PINGRESP
				return;
			}
			default:
				return;
		}
	}

	private dispatch(topic: string, payload: string | undefined): void {
		for (const [filter, handlers] of this.subscriptions) {
			if (!topicMatches(filter, topic)) continue;
			for (const handler of handlers) {
				try {
					handler(topic, payload);
				} catch (error) {
					this.options.log("error", `handler for ${topic} failed: ${(error as Error).message}`);
				}
			}
		}
	}

	private sendSubscribe(filter: string): void {
		const packetId = this.packetId();
		const body = concat([u16(packetId), mqttString(filter), new Uint8Array([0x01])]);
		this.write(packet(0x82, body));
	}

	private packetId(): number {
		const id = this.nextPacketId;
		this.nextPacketId = this.nextPacketId >= 0xffff ? 1 : this.nextPacketId + 1;
		return id;
	}

	/** Subscribe; the filter is re-applied automatically after a reconnect. */
	subscribe(filter: string, handler: MessageHandler): () => void {
		let handlers = this.subscriptions.get(filter);
		if (!handlers) {
			handlers = new Set();
			this.subscriptions.set(filter, handlers);
			if (this.online) this.sendSubscribe(filter);
		}
		handlers.add(handler);
		return () => {
			const current = this.subscriptions.get(filter);
			if (!current) return;
			current.delete(handler);
			if (current.size === 0) {
				this.subscriptions.delete(filter);
				if (this.online) {
					this.write(packet(0xa2, concat([u16(this.packetId()), mqttString(filter)])));
				}
			}
		};
	}

	/** Publish raw text. An empty payload deletes a retained message. */
	publish(topic: string, payload: string, options: { qos?: 0 | 1; retain?: boolean } = {}): Promise<void> {
		if (!this.online) return Promise.reject(new Error("not connected to the broker"));
		const qos = options.qos ?? 1;
		const header = 0x30 | (qos << 1) | (options.retain ? 0x01 : 0x00);
		const body: Uint8Array[] = [mqttString(topic)];
		const packetId = qos > 0 ? this.packetId() : 0;
		if (qos > 0) body.push(u16(packetId));
		body.push(textEncoder.encode(payload));
		const bytes = packet(header, concat(body));

		if (qos === 0) {
			this.write(bytes);
			return Promise.resolve();
		}
		return new Promise<void>((resolve, reject) => {
			const timer = setTimeout(() => {
				this.inflight.delete(packetId);
				reject(new Error(`publish to ${topic} timed out`));
			}, 10_000);
			this.inflight.set(packetId, { resolve, reject, timer });
			this.write(bytes);
		});
	}

	publishJson(topic: string, payload: unknown, options: { qos?: 0 | 1; retain?: boolean } = {}): Promise<void> {
		return this.publish(topic, JSON.stringify(payload), options);
	}

	/**
	 * Wait for the link to come back.
	 *
	 * iOS suspends the app, which kills the socket; on resume the reconnect needs a
	 * moment. A tool that arrives in that window should wait it out rather than report a
	 * dead room the user has to retry.
	 */
	waitOnline(ms: number): Promise<boolean> {
		if (this.online) return Promise.resolve(true);
		const started = Date.now();
		return new Promise((resolve) => {
			const tick = () => {
				if (this.online) {
					resolve(true);
					return;
				}
				if (Date.now() - started >= ms) {
					resolve(false);
					return;
				}
				setTimeout(tick, 100);
			};
			tick();
		});
	}

	async close(): Promise<void> {
		this.stopped = true;
		if (this.reconnectTimer) {
			clearTimeout(this.reconnectTimer);
			this.reconnectTimer = undefined;
		}
		this.stopPing();
		const ws = this.ws;
		this.ws = undefined;
		const wasOnline = this.online;
		this.online = false;
		if (!ws) return;
		if (wasOnline) {
			try {
				ws.send(new Uint8Array([0xe0, 0x00]).buffer); // DISCONNECT
			} catch {
				// the socket is already gone
			}
		}
		await new Promise<void>((resolve) => {
			let done = false;
			const finish = () => {
				if (done) return;
				done = true;
				resolve();
			};
			setTimeout(finish, 1000);
			try {
				ws.onclose = finish;
				ws.close();
			} catch {
				finish();
			}
		});
	}
}
