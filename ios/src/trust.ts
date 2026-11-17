/**
 * Who this orchestrator has approved, per room.
 *
 * Approval is bound to the worker's *key*, not its id: ids are self-chosen, so a
 * squatter holding the worker credential can wear any name. The record lives in
 * pi.synced (per room) so a restart, a relaunch or a week of sleep does not turn
 * every worker back into a stranger, and so the approvals follow the user to their
 * other devices.
 */

import { fingerprintOf } from "./protocol.ts";
import type { LogFn } from "./util.ts";

export interface TrustedWorker {
	workerId: string;
	key: string;
	fingerprint: string;
	admittedAt: number;
	admittedBy: string;
}

/** The slice of pi.synced this needs; small enough to fake in a test. */
export interface TrustStorage {
	get(key: string): unknown;
	set(key: string, value: unknown): void;
}

export class TrustStore {
	private readonly workers = new Map<string, TrustedWorker>();

	constructor(
		private readonly storage: TrustStorage | undefined,
		private readonly room: string,
		private readonly log?: LogFn,
	) {
		this.load();
	}

	private get storageKey(): string {
		return `trusted:${this.room}`;
	}

	private load(): void {
		try {
			const raw = this.storage?.get(this.storageKey) as { workers?: TrustedWorker[] } | undefined;
			for (const worker of raw?.workers ?? []) {
				if (typeof worker?.workerId === "string" && typeof worker?.key === "string") {
					this.workers.set(worker.workerId, worker);
				}
			}
		} catch (error) {
			this.log?.("warn", `could not read approved workers: ${(error as Error).message}`);
		}
	}

	private save(): void {
		try {
			this.storage?.set(this.storageKey, { room: this.room, workers: [...this.workers.values()] });
		} catch (error) {
			this.log?.("warn", `could not save approved workers: ${(error as Error).message}`);
		}
	}

	/** The approval for this id, if it is for exactly this key. */
	get(workerId: string, key: string): TrustedWorker | undefined {
		const worker = this.workers.get(workerId);
		return worker && worker.key === key ? worker : undefined;
	}

	has(workerId: string): boolean {
		return this.workers.has(workerId);
	}

	entries(): TrustedWorker[] {
		return [...this.workers.values()].sort((a, b) => a.workerId.localeCompare(b.workerId));
	}

	admit(workerId: string, key: string, admittedBy: string): TrustedWorker {
		const worker: TrustedWorker = {
			workerId,
			key,
			fingerprint: fingerprintOf(key),
			admittedAt: Date.now(),
			admittedBy,
		};
		this.workers.set(workerId, worker);
		this.save();
		return worker;
	}

	revoke(workerId: string): boolean {
		const removed = this.workers.delete(workerId);
		if (removed) this.save();
		return removed;
	}
}
