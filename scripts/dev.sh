#!/bin/sh
# Start one piece of a local pi-mqtt setup, per shell.
#
#   dev.sh broker        foreground mosquitto on 127.0.0.1:1883
#   dev.sh worker        one worker session in this shell
#   dev.sh orchestrator  the orchestrator session in this shell
#
# Each call runs exactly one process in the foreground, so Ctrl-C stops it and
# three shells (or tmux panes) give you the whole setup. Development only: the
# local broker has no authentication.

set -eu

ROOT=$(cd "$(dirname "$0")/.." && pwd)
EXTENSION="$ROOT/src/extension.ts"

COMMAND=""
PORT=1883
CLUSTER=dev
WORKER_ID=worker-1
DRY_RUN=0

usage() {
	cat <<EOF
Start one piece of a local pi-mqtt setup, per shell.

Usage:
  $(basename "$0") broker       [-p PORT]
  $(basename "$0") worker       [-p PORT] [-c CLUSTER] [-i WORKER_ID]
  $(basename "$0") orchestrator [-p PORT] [-c CLUSTER]
  $(basename "$0") -h

Commands:
  broker        run mosquitto in the foreground on 127.0.0.1:PORT
  worker        run one worker pi session, pinned to WORKER_ID
  orchestrator  run the orchestrator pi session

Options:
  -p PORT        broker port (default $PORT)
  -c CLUSTER     pi-mqtt cluster id (default $CLUSTER)
  -i WORKER_ID   worker id for 'worker' (default $WORKER_ID)
  --dry-run      print the command that would run, then exit

Run them in three shells:

  shell 1:  $(basename "$0") broker
  shell 2:  $(basename "$0") worker
  shell 3:  $(basename "$0") orchestrator

For more workers, run 'worker' in more shells with distinct ids:

  $(basename "$0") worker -i worker-2
  $(basename "$0") worker -i worker-3

Then, in the orchestrator session:

  which workers are available?
  ask worker-1 what time is it

Notes:
  - Run \`npm install\` once: the extension imports mqtt from node_modules.
  - Requires \`pi\` on PATH and mosquitto installed (scripts/broker.sh finds it).
  - 'worker' and 'orchestrator' refuse to start when nothing is listening on the
    port, because a session that cannot reach the broker is inert and confusing.
  - A worker id belongs to one live session. Starting a second worker with an id
    that is already live is refused; use -i to pick another id.
  - On a non-default port the broker's WebSocket listener moves to PORT+7182, and
    a generated config is used so two setups can coexist.
EOF
}

log() { printf '%s\n' "$*" >&2; }
die() { printf 'error: %s\n' "$*" >&2; exit 1; }

is_listening() { nc -z 127.0.0.1 "$1" >/dev/null 2>&1; }

while [ $# -gt 0 ]; do
	case $1 in
	-h | --help) usage; exit 0 ;;
	-p) PORT=${2:-}; shift 2 ;;
	-c) CLUSTER=${2:-}; shift 2 ;;
	-i) WORKER_ID=${2:-}; shift 2 ;;
	--dry-run) DRY_RUN=1; shift ;;
	broker | worker | orchestrator)
		[ -z "$COMMAND" ] || die "pick one command (got '$COMMAND' and '$1')"
		COMMAND=$1
		shift
		;;
	*) die "unknown argument: $1 (try -h)" ;;
	esac
done

[ -n "$COMMAND" ] || { usage; exit 1; }

case $PORT in
'' | *[!0-9]*) die "port must be a number" ;;
esac
case $CLUSTER in
'' | *[!A-Za-z0-9_-]*) die "cluster id may contain letters, digits, _ and -" ;;
esac
case $WORKER_ID in
'' | *[!A-Za-z0-9_-]*) die "worker id may contain letters, digits, _ and -" ;;
esac

URL="mqtt://127.0.0.1:$PORT"

run() {
	# run <description> <command...>
	description=$1
	shift
	log "[$COMMAND] $description"
	if [ "$DRY_RUN" = 1 ]; then
		printf '%s\n' "$*"
		exit 0
	fi
	exec "$@"
}

case $COMMAND in
broker)
	# The committed dev config pins 1883 and 9001. Any other port gets a derived
	# config with both listeners shifted (and persistence off, so a fresh broker
	# does not advertise retained presence from earlier runs).
	if [ "$PORT" = 1883 ]; then
		CONF="$ROOT/broker/mosquitto.dev.conf"
		WS_PORT=9001
	else
		WS_PORT=$((PORT + 7182))
		mkdir -p "$ROOT/.broker-data"
		CONF="$ROOT/.broker-data/dev-$PORT.conf"
		sed -e "s/^listener 1883 127.0.0.1$/listener $PORT 127.0.0.1/" \
			-e "s/^listener 9001 127.0.0.1$/listener $WS_PORT 127.0.0.1/" \
			-e "s|^persistence true$|persistence false|" \
			-e "s|^persistence_location .*$||" \
			"$ROOT/broker/mosquitto.dev.conf" >"$CONF"
	fi

	if is_listening "$PORT"; then
		die "127.0.0.1:$PORT is already in use (another broker?); pick a port with -p"
	fi

	if [ "$DRY_RUN" = 1 ]; then
		log "[broker] 127.0.0.1:$PORT (ws $WS_PORT), config $CONF"
		printf 'PI_MQTT_BROKER_CONF=%s sh %s/scripts/broker.sh\n' "$CONF" "$ROOT"
		exit 0
	fi

	log "[broker] 127.0.0.1:$PORT (WebSocket $WS_PORT)"
	log "Ctrl-C to stop"
	cd "$ROOT"
	PI_MQTT_BROKER_CONF="$CONF" sh ./scripts/broker.sh
	;;

worker)
	[ "$DRY_RUN" = 1 ] || is_listening "$PORT" ||
		die "nothing listening on 127.0.0.1:$PORT; start '$0 broker -p $PORT' first"
	run "$WORKER_ID -> $URL (cluster $CLUSTER)" \
		env "PI_MQTT_URL=$URL" "PI_MQTT_CLUSTER=$CLUSTER" \
		pi --extension "$EXTENSION" --pi-mqtt-role worker --pi-mqtt-id "$WORKER_ID"
	;;

orchestrator)
	[ "$DRY_RUN" = 1 ] || is_listening "$PORT" ||
		die "nothing listening on 127.0.0.1:$PORT; start '$0 broker -p $PORT' first"
	run "-> $URL (cluster $CLUSTER)" \
		env "PI_MQTT_URL=$URL" "PI_MQTT_CLUSTER=$CLUSTER" \
		pi --extension "$EXTENSION" --pi-mqtt-role orchestrator
	;;
esac
