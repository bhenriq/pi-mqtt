/**
 * This machine's long-term identity: an Ed25519 keypair kept next to the config.
 *
 * Two jobs. First, a key is an identity a *name* is not: worker ids are self-chosen, so
 * "pru_misc" proves nothing, but a signature over a join nonce proves the session holds
 * a key that was admitted earlier. Second, the key is what an orchestrator pins when it
 * approves a joiner, so approval survives restarts, reloads and sleeps.
 *
 * The private key never leaves the machine; only the public key and its fingerprint go
 * over the broker.
 */
import { createHash, createPrivateKey, createPublicKey, generateKeyPairSync, sign, verify } from "node:crypto";
import { mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
export function fingerprintOf(publicKey) {
    return createHash("sha256").update(Buffer.from(publicKey, "base64")).digest("base64url").slice(0, 8);
}
export function verifySignature(publicKey, statement, signature) {
    try {
        return verify(null, Buffer.from(statement), createPublicKey({ key: Buffer.from(publicKey, "base64"), format: "der", type: "spki" }), Buffer.from(signature, "base64"));
    }
    catch {
        return false;
    }
}
/** Read the identity, creating it on first use. */
export function loadOrCreateIdentity(logDir) {
    const path = join(logDir, "identity.json");
    let stored;
    try {
        stored = JSON.parse(readFileSync(path, "utf8"));
    }
    catch {
        stored = undefined;
    }
    if (!stored || typeof stored.publicKey !== "string" || typeof stored.privateKey !== "string") {
        const { publicKey, privateKey } = generateKeyPairSync("ed25519");
        stored = {
            publicKey: publicKey.export({ format: "der", type: "spki" }).toString("base64"),
            privateKey: privateKey.export({ format: "der", type: "pkcs8" }).toString("base64"),
            createdAt: Date.now(),
        };
        try {
            mkdirSync(dirname(path), { recursive: true });
            writeFileSync(path, `${JSON.stringify(stored, null, 2)}\n`, { mode: 0o600 });
        }
        catch {
            // An identity that cannot be persisted still works for this session; it only
            // means the approval will not be recognised next time.
        }
    }
    const { publicKey, privateKey } = stored;
    const privateKeyObject = createPrivateKey({
        key: Buffer.from(privateKey, "base64"),
        format: "der",
        type: "pkcs8",
    });
    return {
        key: publicKey,
        fingerprint: fingerprintOf(publicKey),
        sign: (statement) => sign(null, Buffer.from(statement), privateKeyObject).toString("base64"),
    };
}
