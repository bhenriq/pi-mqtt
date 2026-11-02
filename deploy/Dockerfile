FROM eclipse-mosquitto:2

USER root
COPY mosquitto.conf /mosquitto/config/mosquitto.conf
COPY aclfile /mosquitto/config/acl
COPY entrypoint.sh /usr/local/bin/pi-mqtt-entrypoint
RUN chmod +x /usr/local/bin/pi-mqtt-entrypoint

# Railway mounts a volume here; persistence keeps retained presence across deploys.
VOLUME ["/mosquitto/data"]

EXPOSE 1883
ENTRYPOINT ["/usr/local/bin/pi-mqtt-entrypoint"]
