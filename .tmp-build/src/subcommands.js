/**
 * `/mqtt` subcommand list, and the completion logic for it.
 *
 * Kept dependency-free so it can be exercised without pi: the editor wraps the built-in
 * autocomplete provider with `subcommandSuggestions`, which only needs the text before
 * the cursor.
 */
export const MQTT_SUBCOMMANDS = [
    { name: "status", description: "local state, counts, and who is coordinating" },
    { name: "workers", description: "the roster: workers and orchestrators" },
    { name: "tasks", description: "tasks dispatched in this session" },
    { name: "pending", description: "workers waiting for approval, with their codes" },
    { name: "admit", description: "approve a joining worker: /mqtt admit <worker> <code>" },
    { name: "revoke", description: "stop trusting a worker: /mqtt revoke <worker>" },
    { name: "trusted", description: "workers this orchestrator has approved" },
    { name: "connect", description: "join the room" },
    { name: "disconnect", description: "leave the room" },
    { name: "kick", description: "say goodbye to everyone and clear the roster" },
];
/**
 * Suggestions for the line being typed, or undefined when the cursor is not in a
 * subcommand position (so the caller can fall back to the built-in provider).
 */
export function subcommandSuggestions(textBeforeCursor) {
    const match = textBeforeCursor.match(/^(\s*\/mqtt\s+)([a-z]*)$/);
    if (!match)
        return undefined;
    const [, head, partial] = match;
    const items = MQTT_SUBCOMMANDS.filter((sub) => sub.name.startsWith(partial)).map((sub) => ({
        value: `${head}${sub.name}`,
        label: sub.name,
        description: sub.description,
    }));
    if (items.length === 0)
        return undefined;
    return { items, prefix: `${head}${partial}` };
}
/**
 * Replace the typed prefix with the chosen subcommand.
 *
 * Done here rather than by the editor's built-in provider: that one assumes a bare command
 * name and re-adds the leading slash, which turned `/mqtt status` into `//mqtt status`.
 */
export function applySubcommandCompletion(line, cursorCol, value, prefix) {
    const start = Math.max(0, cursorCol - prefix.length);
    const next = `${line.slice(0, start)}${value}${line.slice(cursorCol)}`;
    return { line: next, cursorCol: start + value.length };
}
