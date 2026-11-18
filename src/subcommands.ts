/**
 * `/mqtt` subcommand list, and the completion logic for it.
 *
 * Kept dependency-free so it can be exercised without pi: the editor wraps the built-in
 * autocomplete provider with `subcommandSuggestions`, which only needs the text before
 * the cursor.
 */

export interface Subcommand {
	name: string;
	description: string;
}

export const MQTT_SUBCOMMANDS: Subcommand[] = [
	{ name: "status", description: "local state, counts, and who is coordinating" },
	{ name: "workers", description: "the roster: workers and orchestrators" },
	{ name: "tasks", description: "tasks dispatched in this session" },
	{ name: "pending", description: "workers waiting for approval, with their codes" },
	{ name: "admit", description: "approve a joining worker: /mqtt admit <worker> <code>" },
	{ name: "revoke", description: "stop trusting a worker: /mqtt revoke <worker>" },
	{ name: "trusted", description: "workers this orchestrator has approved" },
	{ name: "ping", description: "ask a worker whether it is answering" },
	{ name: "connect", description: "join the room" },
	{ name: "disconnect", description: "leave the room" },
	{ name: "kick", description: "say goodbye to everyone and clear the roster" },
];

export interface Suggestion {
	value: string;
	label: string;
	description: string;
}

/** Subcommands whose first argument is a worker id, and where the candidates come from. */
const WORKER_ARGUMENT_SUBCOMMANDS = new Set(["ping", "admit", "revoke"]);

export interface CompletionContext {
	/** Worker ids to offer as the first argument, per subcommand. */
	workers?: (subcommand: string) => Array<{ id: string; description?: string }>;
}

export interface SubcommandSuggestions {
	items: Suggestion[];
	/** Text the editor replaces, i.e. everything typed for this command so far. */
	prefix: string;
}

/**
 * Suggestions for the line being typed, or undefined when the cursor is not in a
 * subcommand position (so the caller can fall back to the built-in provider).
 */
export function subcommandSuggestions(
	textBeforeCursor: string,
	context: CompletionContext = {},
): SubcommandSuggestions | undefined {
	const match = textBeforeCursor.match(/^(\s*\/mqtt\s+)([a-z]*)$/);
	if (match) {
		const [, head, partial] = match as unknown as [string, string, string];
		const items = MQTT_SUBCOMMANDS.filter((sub) => sub.name.startsWith(partial)).map((sub) => ({
			value: `${head}${sub.name}`,
			label: sub.name,
			description: sub.description,
		}));
		if (items.length === 0) return undefined;
		return { items, prefix: `${head}${partial}` };
	}

	// First argument of a subcommand that names a worker. The *second* argument of `admit`
	// — the code — is deliberately never completed: typing it is what forces the human to
	// compare the two screens, which is the whole point of the code.
	const argument = textBeforeCursor.match(/^(\s*\/mqtt\s+)([a-z]+)(\s+)([^\s]*)$/);
	if (!argument) return undefined;
	const head = argument[1] ?? "";
	const name = argument[2] ?? "";
	const space = argument[3] ?? "";
	const typed = argument[4] ?? "";
	if (!WORKER_ARGUMENT_SUBCOMMANDS.has(name)) return undefined;

	const items = (context.workers?.(name) ?? [])
		.filter((worker) => worker.id.startsWith(typed))
		.map((worker) => ({
			// The finished line, like the subcommand items, so the editor replaces the whole
			// typed prefix with something valid. `admit` gets a trailing space for the code.
			value: `${head}${name} ${worker.id}${name === "admit" ? " " : ""}`,
			label: worker.id,
			description: worker.description ?? "",
		}));
	if (items.length === 0) return undefined;
	return { items, prefix: `${head}${name}${space}${typed}` };
}

/**
 * Replace the typed prefix with the chosen subcommand.
 *
 * Done here rather than by the editor's built-in provider: that one assumes a bare command
 * name and re-adds the leading slash, which turned `/mqtt status` into `//mqtt status`.
 */
export function applySubcommandCompletion(
	line: string,
	cursorCol: number,
	value: string,
	prefix: string,
): { line: string; cursorCol: number } {
	const start = Math.max(0, cursorCol - prefix.length);
	const next = `${line.slice(0, start)}${value}${line.slice(cursorCol)}`;
	return { line: next, cursorCol: start + value.length };
}
