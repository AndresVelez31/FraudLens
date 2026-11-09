#!/usr/bin/env bash
# Kafka de UN solo broker (KRaft) para pruebas de integración SIN Docker. No es la topología del E1
# (3 brokers, min.insync.replicas=2): esa se levanta con docker-compose.
# Uso: tests/kafka_local.sh start|stop   (requiere Java 17+ y el tgz de Kafka en $KAFKA_TGZ o $KAFKA_HOME ya extraído)
set -euo pipefail
KAFKA_HOME="${KAFKA_HOME:-/tmp/kafka-local/kafka}"
KAFKA_TGZ="${KAFKA_TGZ:-}"
DATA="${KAFKA_DATA:-/tmp/kafka-local/data}"
case "${1:-}" in
  start)
    if [ ! -d "$KAFKA_HOME" ]; then
      mkdir -p "$KAFKA_HOME" && tar -xzf "$KAFKA_TGZ" -C "$KAFKA_HOME" --strip-components=1
    fi
    rm -rf "$DATA"; mkdir -p "$DATA"
    sed "s#^log.dirs=.*#log.dirs=$DATA#" "$KAFKA_HOME/config/kraft/server.properties" > "$DATA/server.properties"
    ID=$("$KAFKA_HOME/bin/kafka-storage.sh" random-uuid)
    "$KAFKA_HOME/bin/kafka-storage.sh" format -t "$ID" -c "$DATA/server.properties" >/dev/null
    nohup "$KAFKA_HOME/bin/kafka-server-start.sh" "$DATA/server.properties" > "$DATA/kafka.log" 2>&1 &
    echo $! > "$DATA/kafka.pid"
    for i in $(seq 1 40); do
      "$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server localhost:9092 --list >/dev/null 2>&1 && break; sleep 1
    done
    "$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server localhost:9092 --create --if-not-exists --topic transacciones --partitions 12 --replication-factor 1
    "$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server localhost:9092 --create --if-not-exists --topic transacciones_dlq --partitions 3 --replication-factor 1
    ;;
  stop) kill "$(cat "$DATA/kafka.pid")" 2>/dev/null || true ;;
  *) echo "uso: $0 start|stop"; exit 1 ;;
esac
