/**
 * Who this orchestrator has approved, per room.
 *
 * Approval is bound to the worker's *key*, not its id: ids are self-chosen, so a
 * squatter with the worker credential can wear any name. The record lives on disk so a
 * restart, a reload or a week of sleep does not turn every worker back into a stranger.
 */
import { mkdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fingerprintOf } from "./identity.js";
export class TrustStore {
    room;
    path;
    workers = new Map();
    constructor(logDir, room) {
        this.room = room;
        this.path = join(logDir, "trusted", `${room}.json`);
        this.load();
    }
    load() {
        try {
            const parsed = JSON.parse(readFileSync(this.path, "utf8"));
            for (const worker of parsed.workers ?? []) {
                if (typeof worker?.workerId === "string" && typeof worker?.key === "string") {
                    this.workers.set(worker.workerId, worker);
                }
            }
        }
        catch {
            // No file yet, or an unreadable one: an empty list, never a crash at startup.
        }
    }
    save() {
        try {
            mkdirSync(dirname(this.path), { recursive: true });
            const body = { room: this.room, workers: [...this.workers.values()] };
            writeFileSync(this.path, `${JSON.stringify(body, null, 2)}\n`, { mode: 0o600 });
        }
        catch {
            // Losing the file costs re-approval after a restart, nothing worse.
        }
    }
    /** The approval for this id, if it is for exactly this key. */
    get(workerId, key) {
        const worker = this.workers.get(workerId);
        return worker && worker.key === key ? worker : undefined;
    }
    entries() {
        return [...this.workers.values()].sort((a, b) => a.workerId.localeCompare(b.workerId));
    }
    admit(workerId, key, admittedBy) {
        const worker = {
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
    revoke(workerId) {
        const removed = this.workers.delete(workerId);
        if (removed)
            this.save();
        return removed;
    }
    /** Forget everything, for tests and for "start over" moments. */
    clear() {
        this.workers.clear();
        try {
            rmSync(this.path, { force: true });
        }
        catch {
            // Nothing to remove.
        }
    }
}
