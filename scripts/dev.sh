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
EXTENSION="$ROOT/extensions/mqtt.ts"

COMMAND=""
PORT=1883
CLUSTER=dev
WORKER_ID=worker-1
DRY_RUN=0
URL=""
CA_FILE=""
INSECURE=0
TLS_DIR=""
SELF_SIGNED=0
TRANSPORT=""

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
  -t TRANSPORT   local broker transport: mqtt | ws | wss | mqtts
                 ports: mqtt PORT, ws PORT+7182 (9001 when PORT=1883),
                 wss PORT+7183, mqtts PORT+7184.
                 wss/mqtts reuse the certs recorded by 'broker --tls DIR'.
  -u URL         explicit broker url, for a remote broker:
                 mqtt:// mqtts:// ws:// wss:// (overrides -t)
  --ca FILE      PEM CA bundle for TLS brokers (sets PI_MQTT_CA_FILE)
  --insecure     skip TLS verification (sets PI_MQTT_REJECT_UNAUTHORIZED=0)
  --tls DIR      'broker' only: also serve wss:// and mqtts:// using
                 DIR/cert.pem and DIR/key.pem (PORT+7183, PORT+7184)
  --self-signed  create DIR/cert.pem and DIR/key.pem if they are missing
                 (dev only; SAN 127.0.0.1 + localhost, valid 2 days)
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
	-t | --transport) TRANSPORT=${2:-}; shift 2 ;;
	-u | --url) URL=${2:-}; shift 2 ;;
	--ca) CA_FILE=${2:-}; shift 2 ;;
	--insecure) INSECURE=1; shift ;;
	--tls) TLS_DIR=${2:-}; shift 2 ;;
	--self-signed) SELF_SIGNED=1; shift ;;
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

WS_PORT=$((PORT + 7182))
WSS_PORT=$((PORT + 7183))
MQTTS_PORT=$((PORT + 7184))
TLS_HINT_FILE="$ROOT/.broker-data/dev-$PORT-tls"

case $TRANSPORT in
'') ;;
mqtt) URL="mqtt://127.0.0.1:$PORT" ;;
ws) if [ "$PORT" = 1883 ]; then URL="ws://127.0.0.1:9001"; else URL="ws://127.0.0.1:$WS_PORT"; fi ;;
wss) URL="wss://127.0.0.1:$WSS_PORT" ;;
mqtts) URL="mqtts://127.0.0.1:$MQTTS_PORT" ;;
*) die "unknown transport: $TRANSPORT (use mqtt, ws, wss or mqtts)" ;;
esac

URL_SET=0
if [ -z "$URL" ]; then
	URL="mqtt://127.0.0.1:$PORT"
else
	URL_SET=1
	case $URL in
	mqtt://* | mqtts://* | ws://* | wss://*) ;;
	*) die "url must start with mqtt://, mqtts://, ws:// or wss://" ;;
	esac
fi

# A TLS transport against the local broker: reuse the certs 'broker --tls' recorded,
# so `worker -t wss` needs no extra flags.
case $TRANSPORT in
wss | mqtts)
	if [ -z "$CA_FILE" ] && [ "$INSECURE" = 0 ]; then
		if [ -f "$TLS_HINT_FILE" ]; then
			CA_FILE="$(cat "$TLS_HINT_FILE")/cert.pem"
		elif [ "$DRY_RUN" = 1 ]; then
			log "note: no CA recorded for port $PORT; a real run needs '--ca FILE' or a broker started with '--tls DIR'"
		else
			die "transport $TRANSPORT needs a CA: start the broker with '--tls DIR', pass '--ca FILE', or use --insecure"
		fi
	fi
	;;
esac

# Env passed to the pi sessions: transport comes from the URL, TLS trust from here.
SESSION_ENV() {
	printf '%s\n' "PI_MQTT_URL=$URL" "PI_MQTT_CLUSTER=$CLUSTER"
	[ -n "$CA_FILE" ] && printf '%s\n' "PI_MQTT_CA_FILE=$CA_FILE"
	[ "$INSECURE" = 1 ] && printf '%s\n' "PI_MQTT_REJECT_UNAUTHORIZED=0"
	return 0
}

needs_local_broker() { [ "$URL_SET" = 0 ]; }

# If the package is installed, pi already loads the extension: passing --extension
# as well would register the tools twice and open two connections per session.
if pi list 2>/dev/null | grep -q "pi-mqtt"; then
	EXT_ARGS=""
	EXT_SOURCE="installed package (pi list shows pi-mqtt)"
else
	EXT_ARGS="--extension $EXTENSION"
	EXT_SOURCE="local file $EXTENSION"
fi

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
	# The committed dev config pins mqtt 1883 and ws 9001. Any other port — and any
	# TLS setup, which needs generated cert paths — gets a derived config with both
	# listeners shifted, and persistence off so a fresh broker does not advertise
	# retained presence from earlier runs.
	if [ "$PORT" = 1883 ]; then
		WS_PORT=9001
	else
		WS_PORT=$((PORT + 7182))
	fi
	WSS_PORT=$((PORT + 7183))
	MQTTS_PORT=$((PORT + 7184))

	if [ "$PORT" = 1883 ] && [ -z "$TLS_DIR" ]; then
		CONF="$ROOT/broker/mosquitto.dev.conf"
	else
		mkdir -p "$ROOT/.broker-data"
		CONF="$ROOT/.broker-data/dev-$PORT.conf"
		sed -e "s/^listener 1883 127.0.0.1$/listener $PORT 127.0.0.1/" \
			-e "s/^listener 9001 127.0.0.1$/listener $WS_PORT 127.0.0.1/" \
			-e "s|^persistence true$|persistence false|" \
			-e "s|^persistence_location .*$||" \
			"$ROOT/broker/mosquitto.dev.conf" >"$CONF"
	fi

	rm -f "$TLS_HINT_FILE"
	if [ -n "$TLS_DIR" ]; then
		if [ ! -f "$TLS_DIR/cert.pem" ] || [ ! -f "$TLS_DIR/key.pem" ]; then
			if [ "$SELF_SIGNED" = 1 ] && [ "$DRY_RUN" = 1 ]; then
				log "note: would generate self-signed certs in $TLS_DIR"
			elif [ "$SELF_SIGNED" = 1 ]; then
				command -v openssl >/dev/null 2>&1 || die "openssl not found; cannot create certs"
				mkdir -p "$TLS_DIR"
				openssl req -x509 -newkey rsa:2048 -nodes \
					-keyout "$TLS_DIR/key.pem" -out "$TLS_DIR/cert.pem" \
					-days 2 -subj "/CN=127.0.0.1" \
					-addext "subjectAltName=IP:127.0.0.1,DNS:localhost" >/dev/null 2>&1 ||
					die "openssl failed to create certs in $TLS_DIR"
				chmod 600 "$TLS_DIR/key.pem" 2>/dev/null || true
				chmod 644 "$TLS_DIR/cert.pem" 2>/dev/null || true
				log "[broker] generated self-signed certs in $TLS_DIR (valid 2 days)"
			elif [ "$DRY_RUN" = 1 ]; then
				log "note: $TLS_DIR has no certs; a real run needs --self-signed or your own cert.pem/key.pem"
			else
				die "no certs in $TLS_DIR

  Create a self-signed pair for local testing:
    $(basename "$0") broker --tls $TLS_DIR --self-signed

  Or supply your own:
    $TLS_DIR/cert.pem  (full chain)
    $TLS_DIR/key.pem   (private key)"
			fi
		fi
		{
			printf '\nlistener %s 127.0.0.1\n' "$WSS_PORT"
			printf 'protocol websockets\n'
			printf 'certfile %s/cert.pem\n' "$TLS_DIR"
			printf 'keyfile %s/key.pem\n' "$TLS_DIR"
			printf '\nlistener %s 127.0.0.1\n' "$MQTTS_PORT"
			printf 'protocol mqtt\n'
			printf 'certfile %s/cert.pem\n' "$TLS_DIR"
			printf 'keyfile %s/key.pem\n' "$TLS_DIR"
		} >>"$CONF"
		# Recorded so `worker -t wss` finds the CA without repeating the path.
		printf '%s' "$TLS_DIR" >"$TLS_HINT_FILE"
	fi

	if is_listening "$PORT"; then
		die "127.0.0.1:$PORT is already in use (another broker?); pick a port with -p"
	fi

	if [ "$DRY_RUN" = 1 ]; then
		log "[broker] mqtt://127.0.0.1:$PORT ws://127.0.0.1:$WS_PORT config=$CONF"
		[ -n "$TLS_DIR" ] && log "[broker] wss://127.0.0.1:$WSS_PORT mqtts://127.0.0.1:$MQTTS_PORT"
		printf 'PI_MQTT_BROKER_CONF=%s sh %s/scripts/broker.sh\n' "$CONF" "$ROOT"
		exit 0
	fi

	log "[broker] mqtt://127.0.0.1:$PORT  ws://127.0.0.1:$WS_PORT"
	if [ -n "$TLS_DIR" ]; then
		log "[broker] wss://127.0.0.1:$WSS_PORT  mqtts://127.0.0.1:$MQTTS_PORT (certs $TLS_DIR)"
		log "[broker] other shells: $(basename "$0") worker -p $PORT -t wss"
		log "[broker]               $(basename "$0") orchestrator -p $PORT -t wss"
	fi
	log "Ctrl-C to stop"
	cd "$ROOT"
	PI_MQTT_BROKER_CONF="$CONF" sh ./scripts/broker.sh
	;;

worker)
	[ "$DRY_RUN" = 1 ] || ! needs_local_broker || is_listening "$PORT" ||
		die "nothing listening on 127.0.0.1:$PORT; start '$0 broker -p $PORT' first"
	# shellcheck disable=SC2046
	log "extension source: $EXT_SOURCE"
	run "$WORKER_ID -> $URL (cluster $CLUSTER)" \
		env $(SESSION_ENV) \
		pi $EXT_ARGS --pi-mqtt-start --name "$WORKER_ID" --pi-mqtt-role worker
	;;

orchestrator)
	[ "$DRY_RUN" = 1 ] || ! needs_local_broker || is_listening "$PORT" ||
		die "nothing listening on 127.0.0.1:$PORT; start '$0 broker -p $PORT' first"
	# shellcheck disable=SC2046
	log "extension source: $EXT_SOURCE"
	run "-> $URL (cluster $CLUSTER)" \
		env $(SESSION_ENV) \
		pi $EXT_ARGS --pi-mqtt-start --name orchestrator --pi-mqtt-role orchestrator
	;;
esac
