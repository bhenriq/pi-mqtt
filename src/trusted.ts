/**
 * Who this orchestrator has approved, per room.
 *
 * Approval is bound to the worker's *key*, not its id: ids are self-chosen, so a
 * squatter with the worker credential can wear any name. The record lives on disk so a
 * restart, a reload or a week of sleep does not turn every worker back into a stranger.
 */

import { mkdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fingerprintOf } from "./identity.ts";

export interface TrustedWorker {
	workerId: string;
	key: string;
	fingerprint: string;
	admittedAt: number;
	admittedBy: string;
}

export class TrustStore {
	private readonly path: string;
	private readonly workers = new Map<string, TrustedWorker>();

	constructor(
		logDir: string,
		private readonly room: string,
	) {
		this.path = join(logDir, "trusted", `${room}.json`);
		this.load();
	}

	private load(): void {
		try {
			const parsed = JSON.parse(readFileSync(this.path, "utf8")) as { workers?: TrustedWorker[] };
			for (const worker of parsed.workers ?? []) {
				if (typeof worker?.workerId === "string" && typeof worker?.key === "string") {
					this.workers.set(worker.workerId, worker);
				}
			}
		} catch {
			// No file yet, or an unreadable one: an empty list, never a crash at startup.
		}
	}

	private save(): void {
		try {
			mkdirSync(dirname(this.path), { recursive: true });
			const body = { room: this.room, workers: [...this.workers.values()] };
			writeFileSync(this.path, `${JSON.stringify(body, null, 2)}\n`, { mode: 0o600 });
		} catch {
			// Losing the file costs re-approval after a restart, nothing worse.
		}
	}

	/** The approval for this id, if it is for exactly this key. */
	get(workerId: string, key: string): TrustedWorker | undefined {
		const worker = this.workers.get(workerId);
		return worker && worker.key === key ? worker : undefined;
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

	/** Forget everything, for tests and for "start over" moments. */
	clear(): void {
		this.workers.clear();
		try {
			rmSync(this.path, { force: true });
		} catch {
			// Nothing to remove.
		}
	}
}
