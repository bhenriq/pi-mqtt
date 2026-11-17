/**
 * Small shared helpers: identifiers, ages, clips. Nothing here knows about MQTT.
 */

export const IDENTIFIER = /^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$/;

export interface LogLine {
	at: number;
	level: "info" | "warn" | "error";
	message: string;
}

export type LogFn = (level: "info" | "warn" | "error", message: string) => void;

export function sanitizeId(value: string): string {
	return value
		.toLowerCase()
		.replace(/[^a-z0-9_-]+/g, "-")
		.replace(/^-+|-+$/g, "")
		.slice(0, 48);
}

export function clip(value: string, max: number): string {
	const flat = value.replace(/\s+/g, " ").trim();
	return flat.length > max ? `${flat.slice(0, max - 1)}…` : flat;
}

export function formatAge(ms: number): string {
	if (ms < 1000) return "now";
	const seconds = Math.round(ms / 1000);
	if (seconds < 60) return `${seconds}s`;
	const minutes = Math.round(seconds / 60);
	if (minutes < 60) return `${minutes}m`;
	const hours = Math.round(minutes / 60);
	if (hours < 48) return `${hours}h`;
	return `${Math.round(hours / 24)}d`;
}

export function newTaskId(): string {
	const random = globalThis.crypto?.randomUUID?.().slice(0, 8) ?? Math.random().toString(16).slice(2, 10);
	return `t_${Date.now().toString(36)}_${random}`;
}
