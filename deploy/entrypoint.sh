#!/bin/sh
# Builds the password file from PI_MQTT_USERS, then starts mosquitto.
#
#   PI_MQTT_USERS="orch:s3cret,worker-laptop-b:other-secret"
#
# Railway env vars are visible to the service, not to clients. Rotate by editing
# the variable and redeploying; sessions reconnect on their own.

set -eu

: "${PI_MQTT_USERS:?set PI_MQTT_USERS to a comma-separated user:password list}"

# Defaults match the image layout; the overrides exist so this script can be
# exercised on a laptop without Docker.
CONFIG_DIR=${MOSQUITTO_CONFIG_DIR:-/mosquitto/config}
DATA_DIR=${MOSQUITTO_DATA_DIR:-/mosquitto/data}
MOSQUITTO_BIN=${MOSQUITTO_BIN:-mosquitto}
PASSWD_FILE="$CONFIG_DIR/passwd"

mkdir -p "$CONFIG_DIR" "$DATA_DIR"

# Rewrite from scratch so removing an entry in PI_MQTT_USERS revokes access.
# `mosquitto_passwd -c` refuses an existing file, so delete rather than truncate.
rm -f "$PASSWD_FILE"
count=0
old_ifs=$IFS
IFS=','
set -f
for entry in $PI_MQTT_USERS; do
	IFS=$old_ifs
	[ -n "$entry" ] || continue
	case "$entry" in
	*:*:*) echo "ignoring PI_MQTT_USERS entry with more than one colon" >&2; continue ;;
	esac
	user=${entry%%:*}
	password=${entry#*:}
	if [ -z "$user" ] || [ -z "$password" ] || [ "$password" = "$entry" ]; then
		echo "ignoring malformed PI_MQTT_USERS entry" >&2
		continue
	fi
	if [ "$count" = 0 ]; then
		mosquitto_passwd -b -c "$PASSWD_FILE" "$user" "$password"
	else
		mosquitto_passwd -b "$PASSWD_FILE" "$user" "$password"
	fi
	count=$((count + 1))
	echo "configured user: $user"
done
IFS=$old_ifs
set +f

if [ "$count" = 0 ]; then
	echo "PI_MQTT_USERS produced no users; refusing to start an open broker" >&2
	exit 1
fi

# Only meaningful in the image, where this runs as root before dropping privileges.
if [ "$(id -u)" = 0 ]; then
	chown -R mosquitto:mosquitto "$CONFIG_DIR" "$DATA_DIR" 2>/dev/null || true
fi
chmod 600 "$PASSWD_FILE"

exec "$MOSQUITTO_BIN" -c "$CONFIG_DIR/mosquitto.conf"
