/**
 * The approval list that predates room-scoped admissions.
 *
 * Approvals used to live per orchestrator, in `trusted/<room>.json`; they now live in the
 * room, on the retained `admissions/<workerId>` topic (see `AdmissionMessage`). This
 * read-only reader exists so a machine that already approved workers does not have to
 * approve them again: when a worker turns up proving the exact key in the file, and the
 * room has no record for it, the orchestrator writes the room record from here.
 *
 * Deliberately not a store: nothing is read for a *decision* when the room has a record,
 * so a stale file can never undo a revocation.
 */

import { readFileSync } from "node:fs";
import { join } from "node:path";

export interface LegacyApproval {
	workerId: string;
	key: string;
	fingerprint: string;
}

export function readLegacyApprovals(logDir: string, room: string): Map<string, LegacyApproval> {
	const approvals = new Map<string, LegacyApproval>();
	try {
		const parsed = JSON.parse(readFileSync(join(logDir, "trusted", `${room}.json`), "utf8")) as {
			workers?: Array<{ workerId?: unknown; key?: unknown; fingerprint?: unknown }>;
		};
		for (const worker of parsed.workers ?? []) {
			if (typeof worker.workerId === "string" && typeof worker.key === "string") {
				approvals.set(worker.workerId, {
					workerId: worker.workerId,
					key: worker.key,
					fingerprint: typeof worker.fingerprint === "string" ? worker.fingerprint : "",
				});
			}
		}
	} catch {
		// No file: nothing to migrate.
	}
	return approvals;
}
