/**
 * `/mqtt config` — an interactive editor for pi-mqtt.json.
 *
 * Deliberately narrow: it edits the handful of fields a person actually changes,
 * validates through the same code path the extension uses, can test the connection
 * before saving, and says when an environment variable outranks the file (a saved
 * value that silently does nothing is worse than no editor at all).
 */

import type { Component, TUI } from "@earendil-works/pi-tui";
import { matchesKey, truncateToWidth } from "@earendil-works/pi-tui";
import type { ExtensionContext } from "@earendil-works/pi-coding-agent";
import { envOverrideFor, type RawConfig } from "./config.ts";

export interface Draft {
	role?: string;
	connect?: string;
	mqtt: {
		url?: string;
		cluster?: string;
		username?: string;
		password?: string;
		caFile?: string;
		keepaliveSec?: number;
		reconnectMs?: number;
		protocolVersion?: number;
		rejectUnauthorized?: boolean;
	};
	worker: {
		workerId?: string;
		heartbeatMs?: number;
		taskExpirySec?: number;
		allowModelOverride?: boolean;
	};
	logDir?: string;
}

export interface MenuDeps {
	ctx: ExtensionContext;
	path: string;
	pathOrigin: "project" | "agent" | "explicit";
	draft: Draft;
	/** Values currently in the file, so untouched keys survive a save. */
	raw: RawConfig;
	/** Returns null when the credentials work, or the error text. */
	test: (draft: Draft) => Promise<string | null>;
	/** Persists the draft; returns a one-line summary. */
	save: (draft: Draft, raw: RawConfig) => Promise<string>;
	/** Reconnect with the new values if this session was connected. */
	apply: () => Promise<void>;
	/** True when a task is running, so reconnecting needs a confirm. */
	busy: () => boolean;
	log: (message: string) => void;
}

/** Single-line input that renders bullets instead of the typed password. */
class MaskedInput implements Component {
	private value = "";

	constructor(
		private readonly tui: TUI,
		private readonly title: string,
		private readonly done: (value: string | undefined) => void,
	) {}

	render(width: number): string[] {
		const shown = this.value.length > 0 ? "•".repeat(this.value.length) : "(empty)";
		return [
			truncateToWidth(this.title, width, "…"),
			"",
			truncateToWidth(`  ${shown}▏`, width, "…"),
			"",
			truncateToWidth("enter to accept · esc to cancel", width, "…"),
		];
	}

	invalidate(): void {}

	handleInput(data: string): void {
		if (matchesKey(data, "escape") || matchesKey(data, "ctrl+c")) {
			this.done(undefined);
			return;
		}
		if (matchesKey(data, "return") || data === "\r" || data === "\n") {
			this.done(this.value);
			return;
		}
		if (matchesKey(data, "backspace") || data === "\x7f") {
			this.value = this.value.slice(0, -1);
			this.tui.requestRender();
			return;
		}
		if (data.length === 1 && data >= " " && data <= "~") {
			this.value += data;
			this.tui.requestRender();
		}
	}
}

async function promptSecret(ctx: ExtensionContext, title: string): Promise<string | undefined> {
	if (ctx.mode === "tui" && typeof ctx.ui.custom === "function") {
		return ctx.ui.custom<string | undefined>(
			(tui, _theme, _keybindings, done) => new MaskedInput(tui, title, done),
			{ overlay: true },
		);
	}
	// No custom components outside the TUI: say so rather than pretend.
	return ctx.ui.input(`${title} (typing is visible in ${ctx.mode} mode)`, "");
}

function show(value: string | number | boolean | undefined, secret = false): string {
	if (value === undefined || value === "") return "not set";
	if (typeof value === "boolean") return value ? "yes" : "no";
	if (secret) return `set (${String(value).length} chars)`;
	return String(value);
}

function overrideNote(field: string): string {
	const name = envOverrideFor(field);
	return name ? `  [overridden by ${name}]` : "";
}

async function askText(
	ctx: ExtensionContext,
	title: string,
	current: string | undefined,
): Promise<string | undefined> {
	const answer = await ctx.ui.input(title, current ?? "");
	if (answer === undefined) return undefined;
	const trimmed = answer.trim();
	if (trimmed === "") return current;
	return trimmed;
}

async function askNumber(
	ctx: ExtensionContext,
	title: string,
	current: number | undefined,
	min: number,
	max: number,
): Promise<number | undefined> {
	const answer = await ctx.ui.input(`${title} (${min}-${max})`, String(current ?? ""));
	if (answer === undefined) return undefined;
	const trimmed = answer.trim();
	if (trimmed === "") return current;
	const parsed = Number(trimmed);
	if (!Number.isFinite(parsed) || parsed < min || parsed > max) {
		ctx.ui.notify(`${title} must be a number between ${min} and ${max}`, "warning");
		return current;
	}
	return Math.round(parsed);
}

async function askChoice(
	ctx: ExtensionContext,
	title: string,
	options: string[],
	current: string | undefined,
): Promise<string | undefined> {
	const picked = await ctx.ui.select(`${title} (current: ${current ?? "unset"})`, options);
	return picked ?? undefined;
}

const SET = "set (edit)";
const CLEAR = "clear";

export async function runConfigMenu(deps: MenuDeps): Promise<void> {
	const { ctx, draft, path } = deps;

	if (!ctx.hasUI) {
		ctx.ui.notify(`no interactive UI in this mode; edit ${path}`, "warning");
		return;
	}

	const envNote = (field: string) => overrideNote(field);
	const locked = (field: string) => envOverrideFor(field) !== undefined;

	for (;;) {
		const items = [
			`role          ${show(draft.role)}${envNote("role")}`,
			`broker        ${show(draft.mqtt.url)}${envNote("url")}`,
			`cluster       ${show(draft.mqtt.cluster)}${envNote("cluster")}`,
			`username      ${show(draft.mqtt.username)}${envNote("username")}`,
			`password      ${show(draft.mqtt.password, true)}${envNote("password")}`,
			`connect       ${show(draft.connect)}${envNote("connect")}`,
			`worker id     ${show(draft.worker.workerId ?? draft.mqtt.username)} (from username)`,
			"──────────────────────────────",
			"Test connection",
			"Show effective config",
			"Advanced ▸",
			"Save & reconnect",
			"Cancel",
		];

		const choice = await ctx.ui.select(
			`pi-mqtt configuration — ${path} (${deps.pathOrigin})`,
			items,
		);
		if (choice === undefined || choice === "Cancel") return;

		if (choice.startsWith("role")) {
			if (locked("role")) {
				ctx.ui.notify("role is set by PI_MQTT_ROLE; change the environment instead", "warning");
				continue;
			}
			const picked = await askChoice(ctx, "role", ["worker", "orchestrator"], draft.role);
			if (picked) draft.role = picked;
			continue;
		}

		if (choice.startsWith("broker")) {
			if (locked("url")) {
				ctx.ui.notify("broker url is set by PI_MQTT_URL; change the environment instead", "warning");
				continue;
			}
			const value = await askText(ctx, "broker url (mqtt:// mqtts:// ws:// wss://)", draft.mqtt.url);
			if (value === undefined) continue;
			if (!/^(mqtts?|wss?):\/\/\S+$/i.test(value)) {
				ctx.ui.notify("url must start with mqtt://, mqtts://, ws:// or wss://", "warning");
				continue;
			}
			draft.mqtt.url = value;
			continue;
		}

		if (choice.startsWith("cluster")) {
			if (locked("cluster")) {
				ctx.ui.notify("cluster is set by PI_MQTT_CLUSTER; change the environment instead", "warning");
				continue;
			}
			const value = await askText(ctx, "cluster id", draft.mqtt.cluster);
			if (value === undefined) continue;
			if (!/^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$/.test(value)) {
				ctx.ui.notify("cluster id: letters, digits, _ or -, starting with a letter or digit", "warning");
				continue;
			}
			draft.mqtt.cluster = value;
			continue;
		}

		if (choice.startsWith("username")) {
			if (locked("username")) {
				ctx.ui.notify("username is set by PI_MQTT_USERNAME; change the environment instead", "warning");
				continue;
			}
			const value = await askText(ctx, "MQTT username (= worker id on a worker)", draft.mqtt.username);
			if (value === undefined) continue;
			draft.mqtt.username = value;
			if (draft.role === "worker") draft.worker.workerId = value;
			continue;
		}

		if (choice.startsWith("password")) {
			if (locked("password")) {
				ctx.ui.notify("password is set by PI_MQTT_PASSWORD; change the environment instead", "warning");
				continue;
			}
			const value = await promptSecret(ctx, "MQTT password");
			if (value === undefined) continue;
			if (value.length === 0) {
				ctx.ui.notify("empty password ignored", "warning");
				continue;
			}
			draft.mqtt.password = value;
			continue;
		}

		if (choice.startsWith("connect")) {
			if (locked("connect")) {
				ctx.ui.notify("connect is set by PI_MQTT_CONNECT; change the environment instead", "warning");
				continue;
			}
			const picked = await askChoice(
				ctx,
				"connect at startup?",
				["manual (connect with /mqtt start)", "auto (connect when pi starts)"],
				draft.connect,
			);
			if (picked) draft.connect = picked.startsWith("auto") ? "auto" : "manual";
			continue;
		}

		if (choice.startsWith("worker id")) {
			const value = await askText(
				ctx,
				"worker id (must equal the MQTT username when the broker's ACLs key on it; empty = username)",
				draft.worker.workerId,
			);
			if (value === undefined) continue;
			if (value === draft.mqtt.username) draft.worker.workerId = "";
			else draft.worker.workerId = value;
			continue;
		}

		if (choice === "Test connection") {
			ctx.ui.notify("testing…", "info");
			const error = await deps.test(draft);
			ctx.ui.notify(
				error === null ? "connection ok: TLS, auth and broker all accepted" : `connection failed: ${error}`,
				error === null ? "info" : "error",
			);
			continue;
		}

		if (choice === "Show effective config") {
			ctx.ui.notify(
				[
					`file:     ${path} (${deps.pathOrigin})`,
					`role:     ${show(draft.role)}${overrideNote("role")}`,
					`broker:   ${show(draft.mqtt.url)}${overrideNote("url")}`,
					`cluster:  ${show(draft.mqtt.cluster)}${overrideNote("cluster")}`,
					`username: ${show(draft.mqtt.username)}${overrideNote("username")}`,
					`password: ${show(draft.mqtt.password, true)}${overrideNote("password")}`,
					`connect:  ${show(draft.connect)}${overrideNote("connect")}`,
					`worker:   ${show(draft.worker.workerId ?? draft.mqtt.username)}`,
					`caFile:   ${show(draft.mqtt.caFile)}${overrideNote("caFile")}`,
					`keepalive ${show(draft.mqtt.keepaliveSec ?? 15)}s · heartbeat ${show(
						draft.worker.heartbeatMs ?? 30000,
					)}ms · task expiry ${show(draft.worker.taskExpirySec ?? 900)}s`,
				].join("\n"),
				"info",
			);
			continue;
		}

		if (choice === "Advanced ▸") {
			await advancedMenu(deps);
			continue;
		}

		if (choice === "Save & reconnect") {
			if (deps.busy()) {
				const proceed = await ctx.ui.confirm(
					"Reconnect now?",
					"A task is running. Saving reconnects the broker connection and the task will be reported as lost.",
				);
				if (!proceed) continue;
			}
			const summary = await deps.save(draft, deps.raw);
			ctx.ui.notify(summary, "info");
			await deps.apply();
			deps.log(`configuration saved to ${path}`);
			return;
		}
	}
}

async function advancedMenu(deps: MenuDeps): Promise<void> {
	const { ctx, draft } = deps;
	for (;;) {
		const choice = await ctx.ui.select("advanced", [
			`keepalive     ${show(draft.mqtt.keepaliveSec ?? 15)}s`,
			`heartbeat     ${show(draft.worker.heartbeatMs ?? 30000)}ms`,
			`task expiry   ${show(draft.worker.taskExpirySec ?? 900)}s`,
			`reconnect     ${show(draft.mqtt.reconnectMs ?? 2000)}ms`,
			`model override ${show(draft.worker.allowModelOverride ?? false)}`,
			`CA bundle     ${show(draft.mqtt.caFile)}${overrideNote("caFile")}`,
			`protocol      MQTT ${show(draft.mqtt.protocolVersion ?? 5)}`,
			`TLS verify    ${show(draft.mqtt.rejectUnauthorized ?? true)}`,
			`log dir       ${show(draft.logDir)}`,
			"‹ back",
		]);
		if (choice === undefined || choice === "‹ back") return;

		if (choice.startsWith("keepalive")) {
			draft.mqtt.keepaliveSec = await askNumber(ctx, "keepalive seconds", draft.mqtt.keepaliveSec ?? 15, 5, 600);
			continue;
		}
		if (choice.startsWith("heartbeat")) {
			draft.worker.heartbeatMs = await askNumber(ctx, "heartbeat ms", draft.worker.heartbeatMs ?? 30000, 2000, 600000);
			continue;
		}
		if (choice.startsWith("task expiry")) {
			draft.worker.taskExpirySec = await askNumber(ctx, "task expiry seconds", draft.worker.taskExpirySec ?? 900, 30, 86400);
			continue;
		}
		if (choice.startsWith("reconnect")) {
			draft.mqtt.reconnectMs = await askNumber(ctx, "reconnect ms", draft.mqtt.reconnectMs ?? 2000, 500, 60000);
			continue;
		}
		if (choice.startsWith("model override")) {
			draft.worker.allowModelOverride = !(draft.worker.allowModelOverride ?? false);
			continue;
		}
		if (choice.startsWith("CA bundle")) {
			if (envOverrideFor("caFile")) {
				ctx.ui.notify("caFile is set by PI_MQTT_CA_FILE; change the environment instead", "warning");
				continue;
			}
			const picked = await ctx.ui.select(`CA bundle (current: ${show(draft.mqtt.caFile)})`, [SET, CLEAR]);
			if (picked === CLEAR) {
				draft.mqtt.caFile = "";
				continue;
			}
			if (picked === SET) {
				const value = await askText(ctx, "path to PEM bundle", draft.mqtt.caFile);
				if (value) draft.mqtt.caFile = value;
			}
			continue;
		}
		if (choice.startsWith("protocol")) {
			const picked = await askChoice(ctx, "MQTT protocol version", ["5", "4"], String(draft.mqtt.protocolVersion ?? 5));
			if (picked) draft.mqtt.protocolVersion = picked === "4" ? 4 : 5;
			continue;
		}
		if (choice.startsWith("TLS verify")) {
			const disable = await ctx.ui.confirm(
				"Disable TLS verification?",
				"Anyone able to intercept the connection can then impersonate the broker. Only for a throwaway local test.",
			);
			if (disable) draft.mqtt.rejectUnauthorized = false;
			else draft.mqtt.rejectUnauthorized = true;
			continue;
		}
		if (choice.startsWith("log dir")) {
			const value = await askText(ctx, "log directory", draft.logDir);
			if (value) draft.logDir = value;
			continue;
		}
	}
}
