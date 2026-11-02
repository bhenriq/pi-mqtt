#!/bin/sh
# Starts the local development broker.
#
# mosquitto from Homebrew lands in <prefix>/sbin, which is often not on PATH, so
# resolve it explicitly instead of assuming `mosquitto` is callable.

set -eu

CONF=${PI_MQTT_BROKER_CONF:-./broker/mosquitto.dev.conf}

find_mosquitto() {
	if command -v mosquitto >/dev/null 2>&1; then
		command -v mosquitto
		return
	fi
	for candidate in \
		"$(brew --prefix mosquitto 2>/dev/null || true)/sbin/mosquitto" \
		/opt/homebrew/sbin/mosquitto \
		/usr/local/sbin/mosquitto \
		/usr/sbin/mosquitto; do
		if [ -n "$candidate" ] && [ -x "$candidate" ]; then
			echo "$candidate"
			return
		fi
	done
	return 1
}

if [ ! -f "$CONF" ]; then
	echo "broker config not found: $CONF" >&2
	exit 1
fi

if ! MOSQUITTO=$(find_mosquitto); then
	echo "mosquitto not found. Install it with: brew install mosquitto" >&2
	echo "or set PI_MQTT_BROKER_CONF and put mosquitto on PATH." >&2
	exit 1
fi

echo "broker: $MOSQUITTO -c $CONF"
exec "$MOSQUITTO" -c "$CONF" -v
