FROM eclipse-mosquitto:2

USER root
COPY mosquitto.conf /mosquitto/config/mosquitto.conf
COPY aclfile /mosquitto/config/acl
COPY entrypoint.sh /usr/local/bin/pi-mqtt-entrypoint
RUN chmod +x /usr/local/bin/pi-mqtt-entrypoint

# No VOLUME instruction: Railway rejects it ("docker VOLUME ... is not supported,
# use Railway Volumes") and mounts its own volume at /mosquitto/data.
EXPOSE 1883 9001
ENTRYPOINT ["/usr/local/bin/pi-mqtt-entrypoint"]
