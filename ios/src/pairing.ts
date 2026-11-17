/**
 * Worker admission: the orchestrator half of the pairing exchange.
 *
 * A worker joins as `pending` and is not dispatchable until this side approves its
 * key. Two paths:
 *
 * - the key is already approved (pi.synced, per room): admit silently, once per join;
 * - otherwise offer a challenge nonce so both screens derive the same six-digit code,
 *   and wait for a human to compare them and type it back.
 *
 * Dispatch to a `pending` worker is refused with the code to compare, so the phone
 * can never quietly hand work to a session that has not been approved.
 */

import {
	fingerprintOf,
	formatPairingCode,
	joinStatement,
	pairingCode,
	PROTOCOL_VERSION,
	type PairMessage,
	type PresenceMessage,
	verifyJoinProof,
} from "./protocol.ts";
import type { TrustStore, TrustedWorker } from "./trust.ts";
import type { LogFn } from "./util.ts";

/** How long an offered code stays usable. */
const DEFAULT_TTL_MS = 10 * 60_000;

export interface PairingOffer {
	workerId: string;
	code: string;
	expiresAt: number;
	key: string;
	workerNonce: string;
	orchestratorId: string;
}

export interface PairingOptions {
	room: string;
	orchestratorId: string;
	trust: TrustStore;
	publish: (message: PairMessage) => void;
	log: LogFn;
	ttlMs?: number;
	onChange?: () => void;
	/** Called when a new code appears, so the extension can notify and push it. */
	onOffer?: (offer: PairingOffer, admitted: boolean) => void;
}

export class Pairing {
	private readonly offers = new Map<string, PairingOffer>();
	/** Joins already handled: `${workerId}\n${nonce}`, so heartbeats do not re-ask. */
	private readonly handled = new Set<string>();
	private stopped = false;

	constructor(private readonly options: PairingOptions) {}

	/** React to a presence message: only `pending` ones are interesting. */
	consider(presence: PresenceMessage): void {
		if (this.stopped || presence.state !== "pending") return;
		void this.considerAsync(presence).catch((error: Error) =>
			this.options.log("warn", `could not check ${presence.workerId}'s join: ${error.message}`),
		);
	}

	private async considerAsync(presence: PresenceMessage): Promise<void> {
		const { workerId, nonce, key, proof } = presence;
		if (typeof key !== "string" || typeof nonce !== "string" || typeof proof !== "string") {
			const seen = this.handled.has(`${workerId}\nlegacy`);
			if (!seen) {
				this.handled.add(`${workerId}\nlegacy`);
				this.options.log(
					"warn",
					`worker ${workerId} advertises no identity key (older pi-mqtt): it cannot be approved until it is updated`,
				);
			}
			return;
		}
		const joinKey = `${workerId}\n${nonce}`;
		if (this.handled.has(joinKey)) return;
		this.handled.add(joinKey);
		if (this.handled.size > 200) this.handled.clear();

		const statement = joinStatement({ room: this.options.room, workerId, nonce, key });
		if (!(await verifyJoinProof(key, statement, proof))) {
			this.options.log("warn", `worker ${workerId} presented a key it cannot prove it holds; ignoring`);
			return;
		}
		if (this.stopped) return;

		if (this.options.trust.get(workerId, key)) {
			this.publish("admit", workerId, { fingerprint: fingerprintOf(key) });
			this.offers.delete(workerId);
			this.options.log("info", `${workerId} rejoined with an approved key; admitting`);
			this.options.onChange?.();
			return;
		}

		const orchestratorNonce = randomNonce();
		const expiresAt = Date.now() + (this.options.ttlMs ?? DEFAULT_TTL_MS);
		const code = pairingCode({
			room: this.options.room,
			workerId,
			orchestratorId: this.options.orchestratorId,
			key,
			workerNonce: nonce,
			orchestratorNonce,
		});
		const offer: PairingOffer = {
			workerId,
			code,
			expiresAt,
			key,
			workerNonce: nonce,
			orchestratorId: this.options.orchestratorId,
		};
		this.offers.set(workerId, offer);
		this.publish("challenge", workerId, { nonce: orchestratorNonce, expiresAt });
		this.options.onOffer?.(offer, false);
		this.options.onChange?.();
	}

	/** Offers still waiting for a human, oldest first by id. */
	list(): PairingOffer[] {
		const now = Date.now();
		for (const [workerId, offer] of this.offers) {
			if (offer.expiresAt <= now) this.offers.delete(workerId);
		}
		return [...this.offers.values()].sort((a, b) => a.workerId.localeCompare(b.workerId));
	}

	offerFor(workerId: string): PairingOffer | undefined {
		return this.list().find((offer) => offer.workerId === workerId);
	}

	/** The code to show next to a pending worker, if one has been offered. */
	codeFor(workerId: string): string | undefined {
		return this.offerFor(workerId)?.code;
	}

	trusted(): TrustedWorker[] {
		return this.options.trust.entries();
	}

	/** An approval for exactly the key this presence advertises. */
	isApproved(presence: PresenceMessage): boolean {
		return typeof presence.key === "string" && this.options.trust.get(presence.workerId, presence.key) !== undefined;
	}

	/** Confirm a code and remember the worker's key. */
	admit(workerId: string, code: string): { ok: boolean; error?: string } {
		const offer = this.offers.get(workerId);
		if (!offer) {
			const known = this.options.trust.entries().find((worker) => worker.workerId === workerId);
			return {
				ok: false,
				error: known
					? `"${workerId}" is already approved (${known.fingerprint}); nothing is waiting for a code`
					: `nothing is waiting for approval as "${workerId}" (a pending worker shows a code once it joins)`,
			};
		}
		if (offer.expiresAt <= Date.now()) {
			this.offers.delete(workerId);
			this.options.onChange?.();
			return { ok: false, error: `the code for "${workerId}" expired; it is offered again on its next heartbeat` };
		}
		const typed = code.replace(/\s+/g, "");
		if (typed !== offer.code) return { ok: false, error: `wrong code for "${workerId}" (it is ${formatPairingCode(offer.code)})` };
		this.options.trust.admit(workerId, offer.key, this.options.orchestratorId);
		this.offers.delete(workerId);
		this.publish("admit", workerId, { fingerprint: fingerprintOf(offer.key) });
		this.options.log("info", `approved ${workerId} (${fingerprintOf(offer.key)})`);
		this.options.onChange?.();
		return { ok: true };
	}

	/** Stop trusting a worker: it goes back to `pending` on its next heartbeat. */
	revoke(workerId: string): { ok: boolean; error?: string } {
		const offer = this.offers.get(workerId);
		this.offers.delete(workerId);
		const removed = this.options.trust.revoke(workerId);
		if (!removed && !offer) return { ok: false, error: `"${workerId}" is not approved` };
		this.publish("revoke", workerId);
		this.options.log("info", `revoked ${workerId}`);
		this.options.onChange?.();
		return { ok: true };
	}

	stop(): void {
		this.stopped = true;
		this.offers.clear();
		this.handled.clear();
	}

	private publish(step: PairMessage["step"], workerId: string, extra: Partial<PairMessage> = {}): void {
		this.options.publish({
			v: PROTOCOL_VERSION,
			kind: "pair",
			step,
			workerId,
			orchestratorId: this.options.orchestratorId,
			at: Date.now(),
			...extra,
		});
	}
}

function randomNonce(): string {
	const bytes = new Uint8Array(8);
	(globalThis.crypto as any)?.getRandomValues?.(bytes);
	return [...bytes].map((byte) => byte.toString(16).padStart(2, "0")).join("");
}
