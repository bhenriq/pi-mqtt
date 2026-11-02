#!/bin/sh
# Start a local pi-mqtt setup: broker, workers, orchestrator.
#
# Everything runs against a local mosquitto with no auth, so this is for
# development only. Use --manual to print the commands instead of running them.

set -eu

ROOT=$(cd "$(dirname "$0")/.." && pwd)
EXTENSION="$ROOT/src/extension.ts"

PORT=1883
CLUSTER=dev
WORKERS=1
PREFIX=worker
SESSION=pi-mqtt
MODE=auto
ATTACH=1
START_BROKER=1
START_ORCHESTRATOR=1
COMMAND=start

usage() {
	cat <<EOF
Start a local pi-mqtt setup: one broker, N workers, one orchestrator.

Usage:
  $(basename "$0") [options]        start everything
  $(basename "$0") stop             stop what this script started
  $(basename "$0") -h               show this help

Options:
  -p PORT      broker port on 127.0.0.1 (default $PORT). WebSocket moves to
               PORT+7182 when the port is not the default
  -c CLUSTER   pi-mqtt cluster id (default $CLUSTER)
  -n COUNT     number of workers to start (default $WORKERS)
  -i PREFIX    worker id prefix; ids become PREFIX-1..PREFIX-N (default $PREFIX)
  -s SESSION   tmux session name (default $SESSION)
  -m MODE      tmux | manual (default: tmux when available, else manual)
  --no-broker        reuse a broker already listening on the port
  --no-orchestrator  start the broker and workers only
  --no-attach        create the tmux session but do not switch to it

What it does:
  tmux mode    creates a detached session with one window per process
               (broker, worker-1..N, orchestrator) and switches to it
  manual mode  starts the broker in the background and prints the exact
               commands to paste into two more terminals

Then, in the orchestrator session:

  which workers are available?
  ask worker-1 what time is it

Notes:
  - Run \`npm install\` once: the extension imports mqtt from node_modules.
  - Requires \`pi\` on PATH and mosquitto installed (scripts/broker.sh finds it).
  - Worker ids must be unique among live sessions. If a worker refuses to start
    because its id is in use, stop the old session or use -i to pick new ids.
  - \`stop\` kills the tmux session and any broker started by this script.
  - Window names are pinned explicitly; a tmux config with session-created hooks
    may still add its own windows to the new session.
EOF
}

log() { printf '%s\n' "$*" >&2; }
die() { printf 'error: %s\n' "$*" >&2; exit 1; }

is_listening() {
	nc -z 127.0.0.1 "$1" >/dev/null 2>&1
}

wait_for_port() {
	port=$1
	waited=0
	while [ "$waited" -lt 60 ]; do
		if is_listening "$port"; then return 0; fi
		sleep 0.5
		waited=$((waited + 1))
	done
	return 1
}

pid_file() { printf '%s' "$ROOT/.broker-data/dev-$PORT.pid"; }

stop_all() {
	stopped=0
	if command -v tmux >/dev/null 2>&1 && tmux has-session -t "$SESSION" 2>/dev/null; then
		tmux kill-session -t "$SESSION"
		log "stopped tmux session: $SESSION"
		stopped=1
	fi
	file=$(pid_file)
	if [ -f "$file" ]; then
		pid=$(cat "$file")
		if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
			kill "$pid" 2>/dev/null || true
			log "stopped broker (pid $pid)"
			stopped=1
		fi
		rm -f "$file"
	fi
	if [ "$stopped" = 0 ]; then
		log "nothing to stop for session '$SESSION' on port $PORT"
	fi
	if is_listening "$PORT"; then
		log "warning: something is still listening on 127.0.0.1:$PORT (not started by this script?)"
	fi
}

while [ $# -gt 0 ]; do
	case $1 in
	-h | --help) usage; exit 0 ;;
	-p) PORT=${2:-}; shift 2 ;;
	-c) CLUSTER=${2:-}; shift 2 ;;
	-n) WORKERS=${2:-}; shift 2 ;;
	-i) PREFIX=${2:-}; shift 2 ;;
	-s) SESSION=${2:-}; shift 2 ;;
	-m) MODE=${2:-}; shift 2 ;;
	--no-broker) START_BROKER=0; shift ;;
	--no-orchestrator) START_ORCHESTRATOR=0; shift ;;
	--no-attach) ATTACH=0; shift ;;
	stop) COMMAND=stop; shift ;;
	*) die "unknown argument: $1 (try -h)" ;;
	esac
done

case $PORT in
'' | *[!0-9]*) die "port must be a number" ;;
esac
case $WORKERS in
'' | *[!0-9]*) die "worker count must be a number" ;;
esac

if [ "$COMMAND" = stop ]; then
	stop_all
	exit 0
fi

case $MODE in
auto)
	if command -v tmux >/dev/null 2>&1; then MODE=tmux; else MODE=manual; fi
	;;
tmux | manual) ;;
*) die "unknown mode: $MODE (use tmux or manual)" ;;
esac
if [ "$MODE" = tmux ] && ! command -v tmux >/dev/null 2>&1; then
	die "tmux is not installed; use -m manual"
fi

command -v pi >/dev/null 2>&1 || die "pi is not on PATH"
URL="mqtt://127.0.0.1:$PORT"
EXPORTS="PI_MQTT_URL=$URL PI_MQTT_CLUSTER=$CLUSTER"

worker_cmd() {
	printf '%s pi --extension %s --pi-mqtt-role worker --pi-mqtt-id %s' "$EXPORTS" "$EXTENSION" "$1"
}
orchestrator_cmd() {
	printf '%s pi --extension %s --pi-mqtt-role orchestrator' "$EXPORTS" "$EXTENSION"
}

# The committed dev config pins 1883 and 9001. For any other port, derive a
# config with both listeners shifted so the helper never fights another broker.
if [ "$PORT" = 1883 ]; then
	BROKER_CONF="$ROOT/broker/mosquitto.dev.conf"
else
	WS_PORT=$((PORT + 7182))
	mkdir -p "$ROOT/.broker-data"
	BROKER_CONF="$ROOT/.broker-data/dev-$PORT.conf"
	# persistence off: a dev broker that reloads yesterday's retained presence
	# advertises workers that are long gone.
	sed -e "s/^listener 1883 127.0.0.1$/listener $PORT 127.0.0.1/" \
		-e "s/^listener 9001 127.0.0.1$/listener $WS_PORT 127.0.0.1/" \
		-e "s|^persistence true$|persistence false|" \
		-e "s|^persistence_location .*$||" \
		"$ROOT/broker/mosquitto.dev.conf" >"$BROKER_CONF"
fi

broker_running=0
if is_listening "$PORT"; then
	broker_running=1
	if [ "$START_BROKER" = 0 ]; then
		log "reusing broker on 127.0.0.1:$PORT"
	else
		log "port $PORT is already in use; reusing that broker (pass --no-broker to silence this)"
	fi
fi

if [ "$MODE" = manual ]; then
	if [ "$broker_running" = 0 ]; then
		mkdir -p "$ROOT/.broker-data"
		# Redirect at the subshell level: a background broker that inherits the
		# caller's stdout keeps a pipe open, and `dev.sh | tail` would never end.
		(cd "$ROOT" && exec env PI_MQTT_BROKER_CONF="$BROKER_CONF" sh ./scripts/broker.sh) \
			>"$ROOT/.broker-data/broker-$PORT.log" 2>&1 </dev/null &
		echo $! >"$(pid_file)"
		if ! wait_for_port "$PORT"; then
			die "broker did not start; see .broker-data/broker-$PORT.log"
		fi
		log "broker started on 127.0.0.1:$PORT (pid $(cat "$(pid_file)"))"
	fi
	printf '\n# terminal 2\n%s\n' "$(worker_cmd "$PREFIX-1")"
	i=2
	while [ "$i" -le "$WORKERS" ]; do
		printf '\n# terminal %s\n%s\n' "$((i + 1))" "$(worker_cmd "$PREFIX-$i")"
		i=$((i + 1))
	done
	if [ "$START_ORCHESTRATOR" = 1 ]; then
		printf '\n# orchestrator terminal\n%s\n' "$(orchestrator_cmd)"
	fi
	printf '\nStop with: %s stop\n' "$(basename "$0")"
	exit 0
fi

# --- tmux mode ----------------------------------------------------------

if tmux has-session -t "$SESSION" 2>/dev/null; then
	die "tmux session '$SESSION' already exists; run '%s stop' first" "$(basename "$0")"
fi

# Window ids, not names: this tmux runs session-created hooks that rename windows,
# so name lookups fail and indices shift under us.
WINDOW_IDS=""

keep_name() {
	# keep_name <window_id> <name>
	tmux set-window-option -t "$1" automatic-rename off 2>/dev/null || true
	tmux rename-window -t "$1" "$2" 2>/dev/null || true
}

start_window() {
	# start_window <name> <command>
	if ! id=$(tmux new-window -d -P -F '#{window_id}' -t "$SESSION" -n "$1" -c "$ROOT" "$2" 2>/dev/null); then
		die "could not create tmux window $1"
	fi
	WINDOW_IDS="$WINDOW_IDS $id:$1"
	keep_name "$id" "$1"
}

if [ "$broker_running" = 0 ]; then
	tmux new-session -d -s "$SESSION" -n broker -c "$ROOT" "PI_MQTT_BROKER_CONF=$BROKER_CONF sh ./scripts/broker.sh" ||
		die "could not create tmux session $SESSION"
	BROKER_ID=$(tmux list-windows -t "$SESSION" -F '#{window_id}' | head -1)
	keep_name "$BROKER_ID" broker
	if ! wait_for_port "$PORT"; then
		die "broker did not start; check the 'broker' window"
	fi
	# Killing a tmux session does not necessarily kill the broker's process, so
	# record the listener pid: `stop` must leave the port free.
	lsof -nP -iTCP:"$PORT" -sTCP:LISTEN -t 2>/dev/null | head -1 >"$(pid_file)"
	log "broker started in tmux window 'broker'"
else
	tmux new-session -d -s "$SESSION" -n worker-1 -c "$ROOT" "$(worker_cmd "$PREFIX-1")" ||
		die "could not create tmux session $SESSION"
	WINDOW_IDS="$(tmux list-windows -t "$SESSION" -F '#{window_id}' | head -1):worker-1"
	log "worker $PREFIX-1 started in tmux window 'worker-1'"
fi

i=1
if [ "$broker_running" = 0 ]; then first_worker=1; else first_worker=2; fi
while [ "$i" -le "$WORKERS" ]; do
	if [ "$i" -ge "$first_worker" ]; then
		start_window "worker-$i" "$(worker_cmd "$PREFIX-$i")"
		log "worker $PREFIX-$i started in tmux window 'worker-$i'"
	fi
	i=$((i + 1))
done

if [ "$START_ORCHESTRATOR" = 1 ]; then
	start_window orchestrator "$(orchestrator_cmd)"
	log "orchestrator started in tmux window 'orchestrator'"
fi

# Some tmux setups run session-created hooks that add or rename windows. Re-apply
# our names once everything exists so the layout stays readable.
sleep 0.3
for pair in $WINDOW_IDS; do
	keep_name "${pair%%:*}" "${pair#*:}"
done
[ "$broker_running" = 0 ] && keep_name "$BROKER_ID" broker

tmux select-window -t "$SESSION:$( [ "$START_ORCHESTRATOR" = 1 ] && echo orchestrator || echo "worker-$WORKERS")" 2>/dev/null || true

if [ "$ATTACH" = 1 ]; then
	if [ -n "${TMUX:-}" ]; then
		tmux switch-client -t "$SESSION"
	else
		tmux attach -t "$SESSION"
	fi
else
	printf '\nAttach with: tmux attach -t %s\n' "$SESSION"
fi

printf '\nIn the orchestrator session:\n  which workers are available?\n  ask %s-1 what time is it\n\nStop with: %s stop\n' "$PREFIX" "$(basename "$0")"
