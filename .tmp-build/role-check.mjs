const ROOT = "/Users/baptiste/Documents/Dev/pi-mqtt";
const HOME = "/tmp/role-check-home";
import { mkdirSync, rmSync } from "node:fs";
rmSync(HOME, { recursive: true, force: true }); mkdirSync(HOME, { recursive: true });
process.env.HOME = HOME;
process.env.PI_MQTT_URL = "mqtt://127.0.0.1:18835";
process.env.PI_MQTT_CLUSTER = "dev";
process.env.PI_MQTT_CONNECT = "manual";
const results = [];
const check = (n, ok, d = "") => { results.push(ok); console.log(`${ok ? "PASS" : "FAIL"}  ${n}${d ? `  (${d})` : ""}`); };
let i = 0;
async function join(role, username, password) {
	process.env.PI_MQTT_ROLE = role; process.env.PI_MQTT_USERNAME = username; process.env.PI_MQTT_PASSWORD = password;
	const notes = []; const handlers = new Map(), commands = new Map(); let tools = [];
	const pi = { registerFlag(){}, registerTool(){}, registerCommand: (n,d) => commands.set(n,d),
		on: (e,f) => { handlers.set(e, [...(handlers.get(e) ?? []), f]); return () => {}; },
		getFlag: () => undefined, getSessionName: () => "misc", getActiveTools: () => tools,
		setActiveTools: (n) => { tools = n; }, sendMessage(){}, sendUserMessage(){} };
	const ctx = { cwd: "/tmp", model: undefined,
		ui: { notify: (t) => notes.push(String(t)), addAutocompleteProvider(){} },
		sessionManager: { getSessionId: () => `s-${i}`, getSessionFile: () => undefined }, abort(){} };
	const mod = await import(`${ROOT}/.tmp-build/extensions/index.js?i=${i++}`);
	mod.default(pi);
	for (const fn of handlers.get("session_start") ?? []) await fn({ reason: "startup" }, ctx);
	await commands.get("mqtt").handler("connect", ctx);
	return { notes, toolsBeforeShutdown: [...tools] };
}
const okOrch = await join("orchestrator", "tribe", "ORCHPW");
check("orchestrator credential joins as orchestrator", okOrch.toolsBeforeShutdown.includes("dispatch_task"));
const bad = await join("orchestrator", "pru", "WPW");
check("worker credential refused as orchestrator",
	bad.notes.some((n) => /refused this session as orchestrator/.test(n)) && !bad.toolsBeforeShutdown.includes("dispatch_task"));
process.exit(results.every(Boolean) ? 0 : 1);
