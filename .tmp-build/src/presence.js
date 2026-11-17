/**
 * Presence: retained `workers/<id>/presence` messages plus a heartbeat.
 *
 * Retained means `list_workers` is a local map read, not a request/response.
 * The Last Will (built in extension.ts) covers crashes; `stop()` covers orderly
 * shutdown.
 */
import { CLIENT_VERSION, PROTOCOL_VERSION, isPresenceMessage, } from "./protocol.js";
export function buildPresence(identity, state, patch = {}, role = "worker") {
    return {
        v: PROTOCOL_VERSION,
        kind: "presence",
        version: CLIENT_VERSION,
        role,
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
export async function probePresence(link, topics, workerId, timeoutMs) {
    const topic = topics.presence(workerId);
    return new Promise((resolve) => {
        let settled = false;
        const finish = (value) => {
            if (settled)
                return;
            settled = true;
            clearTimeout(timer);
            unsubscribe();
            resolve(value);
        };
        const unsubscribe = link.subscribe(topic, (_topic, payload) => {
            // A deleted retained presence means "nobody is using this id".
            finish(payload !== undefined && isPresenceMessage(payload) ? payload : undefined);
        });
        const timer = setTimeout(() => finish(undefined), timeoutMs);
        timer.unref?.();
    });
}
export class Presence {
    link;
    topic;
    heartbeatMs;
    log;
    expirySec;
    role;
    current;
    timer;
    stopped = false;
    /** Set by `announce()`, so `start()` does not publish the join twice. */
    announced = false;
    constructor(link, topic, identity, heartbeatMs, log, 
    /** Retained presence lifetime: a dead worker's entry disappears on its own. */
    expirySec = 86_400, role = "worker") {
        this.link = link;
        this.topic = topic;
        this.heartbeatMs = heartbeatMs;
        this.log = log;
        this.expirySec = expirySec;
        this.role = role;
        this.current = buildPresence(identity, "idle", {}, role);
    }
    /** Current advertised state, so callers can tell a busy worker from an idle one. */
    get state() {
        return this.current.state;
    }
    /**
     * Set fields without publishing, for building the state a session *joins* with.
     *
     * A worker's first announcement is `pending` plus the nonce and proof of its join; it
     * must not be published as `idle` first, or an orchestrator would briefly see a
     * dispatchable worker that has not been approved.
     */
    seed(patch) {
        if (this.stopped)
            return;
        this.current = { ...this.current, ...patch, updatedAt: Date.now() };
    }
    start() {
        if (!this.announced)
            void this.publish();
        this.timer = setInterval(() => void this.publish(), this.heartbeatMs);
        this.timer.unref?.();
    }
    /**
     * Publish presence once and let a failure through, unlike the heartbeat.
     *
     * This first publish is also the credential check. A role is a capability on the
     * broker now: a worker credential writing an orchestrator presence is acknowledged
     * with 0x87 "not authorized". Swallowing that would leave a session that believes it
     * joined, hears nothing, and blames the workers — the failure mode this project has
     * already spent a day on.
     */
    async announce() {
        await this.link.publish(this.topic, { ...this.current, updatedAt: Date.now() }, { retain: true, expirySec: this.expirySec });
        this.announced = true;
    }
    /** Publish a state change immediately (and restart the heartbeat window). */
    update(patch) {
        if (this.stopped)
            return;
        this.current = { ...this.current, ...patch, updatedAt: Date.now() };
        void this.publish();
    }
    /**
     * Leave the room.
     *
     * Deletes the retained presence rather than writing `offline`: a session that
     * disconnected is gone, and an entry saying "offline" only invites the next reader to
     * wonder when. A crash still leaves `offline` behind, because the Last Will is fixed at
     * connect time — that is the honest difference between leaving and dying.
     */
    async stop() {
        if (this.stopped)
            return;
        this.stopped = true;
        if (this.timer)
            clearInterval(this.timer);
        try {
            // Bounded: leaving must not hold up quitting when the broker is unreachable.
            await Promise.race([
                this.link.publishEmpty(this.topic),
                new Promise((resolve) => setTimeout(resolve, 2500)),
            ]);
        }
        catch (error) {
            this.log("warn", `could not clear presence on leave: ${error.message}`);
        }
    }
    /** Fire-and-forget leave, for paths that cannot await. */
    forget() {
        void this.stop();
    }
    /** Republish the current state now (used after a reconnect). */
    republish() {
        if (this.stopped)
            return;
        this.current = { ...this.current, updatedAt: Date.now() };
        void this.publish();
    }
    async publish() {
        if (this.stopped || !this.link.online)
            return;
        try {
            await this.link.publish(this.topic, { ...this.current, updatedAt: Date.now() }, { retain: true, expirySec: this.expirySec });
        }
        catch (error) {
            this.log("warn", `presence publish failed: ${error.message}`);
        }
    }
}
