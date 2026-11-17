/**
 * Admission: a worker joins `pending` and only becomes dispatchable when an
 * orchestrator approves its key, by comparing a code with the worker's screen.
 *
 * Two real extension instances (orchestrator + worker) against a local mosquitto with
 * the generated role ACL, each with its own home directory (so each has its own key and
 * the orchestrator has its own approval list).
 */
const ROOT = "/Users/baptiste/Documents/Dev/pi-mqtt";
const HOME_ORCH = "/tmp/adm-orch";
const HOME_WORKER = "/tmp/adm-worker";

import { mkdirSync, rmSync } from "node:fs";
import { connect } from "mqtt";

for (const home of [HOME_ORCH, HOME_WORKER]) {
	rmSync(home, { recursive: true, force: true });
	mkdirSync(home, { recursive: true });
}

process.env.PI_MQTT_URL = "mqtt://127.0.0.1:18835";
process.env.PI_MQTT_CLUSTER = "dev";
process.env.PI_MQTT_CONNECT = "manual";

const results = [];
function check(name, ok, detail = "") {
	results.push(ok);
	console.log(`${ok ? "PASS" : "FAIL"}  ${name}${detail ? `  (${detail})` : ""}`);
}
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

let instance = 0;
function createApi(notes) {
	const handlers = new Map();
	const commands = new Map();
	const tools = new Map();
	let active = [];
	const pi = {
		registerFlag() {},
		registerTool(def) {
			tools.set(def.name, def);
		},
		registerCommand(name, def) {
			commands.set(name, def);
		},
		on(event, fn) {
			handlers.set(event, [...(handlers.get(event) ?? []), fn]);
			return () => handlers.set(event, (handlers.get(event) ?? []).filter((f) => f !== fn));
		},
		getFlag: () => undefined,
		getSessionName: () => pi.__name,
		getActiveTools: () => active,
		setActiveTools: (next) => {
			active = next;
		},
		sendMessage: () => {},
		sendUserMessage: (text) => notes.userMessages.push(String(text)),
		__name: "x",
	};
	return { pi, handlers, commands, tools, notes };
}

async function load({ role, username, password, home, sessionName, sessionId }) {
	process.env.HOME = home;
	process.env.PI_MQTT_ROLE = role;
	process.env.PI_MQTT_USERNAME = username;
	process.env.PI_MQTT_PASSWORD = password;

	const notes = { notify: [], userMessages: [] };
	const api = createApi(notes);
	api.pi.__name = sessionName;
	const ctx = {
		cwd: "/tmp",
		model: undefined,
		ui: {
			notify: (text, level) => notes.notify.push({ text: String(text), level }),
			addAutocompleteProvider() {},
		},
		sessionManager: { getSessionId: () => sessionId, getSessionFile: () => undefined },
		abort() {},
	};
	const mod = await import(`${ROOT}/.tmp-build/extensions/index.js?i=${instance++}`);
	mod.default(api.pi);
	const fire = async (event, arg) => {
		for (const fn of api.handlers.get(event) ?? []) await fn(arg, ctx);
	};
	await fire("session_start", { reason: "startup" });
	await api.commands.get("mqtt").handler("connect", ctx);
	return { ...api, ctx, fire, notes };
}

const ORCH = { username: "tribe", password: "ORCHPW" };
const WORKER = { username: "pru", password: "WPW" };
const codeOf = (notes) => {
	for (const note of [...notes].reverse()) {
		const match = note.text.match(/\b(\d{6})\b/) ?? note.text.match(/(\d{3}) (\d{3})/);
		if (match) return match[1] + (match[2] ?? "");
	}
	return undefined;
};

// A watcher with the orchestrator credential, to see the worker's advertised state.
const watched = [];
const watcher = connect(process.env.PI_MQTT_URL, {
	username: ORCH.username,
	password: ORCH.password,
	clientId: `watch-${Date.now()}`,
	protocolVersion: 5,
	reconnectPeriod: 0,
	clean: true,
});
await new Promise((r) => watcher.once("connect", r));
watcher.subscribe("pi/v1/dev/workers/+/presence", { qos: 1 });
watcher.on("message", (_t, payload) => {
	if (payload.length === 0) return;
	const m = JSON.parse(payload.toString());
	watched.push({ id: m.workerId, state: m.state });
});
const lastState = (id) => [...watched].reverse().find((w) => w.id === id)?.state;

// --- worker joins, unapproved ---------------------------------------------
const worker = await load({
	role: "worker",
	...WORKER,
	home: HOME_WORKER,
	sessionName: "misc",
	sessionId: "sess-worker",
});
const workerId = worker.notes.notify.length >= 0 ? null : null; // resolved below
await sleep(500);
const joined = watched.find((w) => w.state === "pending");
check("a worker joins as pending", Boolean(joined), joined?.id);
const id = joined?.id ?? "unknown";

// --- orchestrator joins and offers a code ---------------------------------
const orch = await load({
	role: "orchestrator",
	...ORCH,
	home: HOME_ORCH,
	sessionName: "orch",
	sessionId: "sess-orch",
});
await sleep(800);
const orchCode = codeOf(orch.notes.notify.filter((n) => /wants to join/.test(n.text)));
const workerCode = codeOf(worker.notes.notify.filter((n) => /approval code/.test(n.text)));
check("the orchestrator offers a code", Boolean(orchCode), orchCode);
check("the worker displays the same code", Boolean(workerCode) && workerCode === orchCode, `${workerCode} vs ${orchCode}`);

// --- not dispatchable yet -------------------------------------------------
const dispatch = async (params) => {
	const tool = orch.tools.get("dispatch_task");
	return tool.execute("call", { to: id, prompt: "say hello", wait: false }, new AbortController().signal);
};
const refused = await dispatch();
check(
	"dispatch is refused while pending",
	refused.isError === true && /waiting for approval/.test(refused.content[0].text),
	refused.content[0].text.slice(0, 80),
);

// --- wrong code, then right code ------------------------------------------
await orch.commands.get("mqtt").handler(`admit ${id} 000000`, orch.ctx);
await sleep(200);
check(
	"a wrong code is refused",
	orch.notes.notify.some((n) => /wrong code/.test(n.text)),
	orch.notes.notify.at(-1)?.text.slice(0, 60),
);
check("the worker is still pending after a wrong code", lastState(id) === "pending");

await orch.commands.get("mqtt").handler(`admit ${id} ${orchCode}`, orch.ctx);
await sleep(800);
check("approving the right code admits the worker", lastState(id) === "idle", lastState(id));

const accepted = await dispatch();
check("dispatch is accepted once approved", accepted.isError !== true, accepted.content[0].text.slice(0, 60));
await sleep(500);
check(
	"the task reaches the worker",
	worker.notes.userMessages.some((message) => /say hello/.test(message)),
	`${worker.notes.userMessages.length} message(s)`,
);

// --- a known key is admitted again without a human ------------------------
const orchNotesBefore = orch.notes.notify.length;
await worker.fire("session_shutdown", { reason: "quit" });
await sleep(400);
const rejoined = await load({
	role: "worker",
	...WORKER,
	home: HOME_WORKER,
	sessionName: "misc",
	sessionId: "sess-worker-2",
});
await sleep(900);
const newOrchNotes = orch.notes.notify.slice(orchNotesBefore);
check(
	"a returning worker with the approved key is admitted without a code",
	lastState(id) === "idle" && !newOrchNotes.some((n) => /wants to join/.test(n.text)),
	lastState(id),
);
check("...and the worker was told it is approved", rejoined.notes.notify.some((n) => /approved by/.test(n.text)));

// --- revoke ---------------------------------------------------------------
await orch.commands.get("mqtt").handler(`revoke ${id}`, orch.ctx);
await sleep(800);
check("revoking sends the worker back to pending", lastState(id) === "pending", lastState(id));
const afterRevoke = await dispatch();
check("dispatch is refused after revoke", afterRevoke.isError === true);

for (const client of [watcher]) client.end(true);
const failed = results.filter((ok) => !ok).length;
console.log(`\n${results.length - failed}/${results.length} checks passed`);
process.exit(failed === 0 ? 0 : 1);
