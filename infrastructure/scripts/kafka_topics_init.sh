#!/bin/bash
set -euo pipefail

BOOTSTRAP_SERVER="${KAFKA_BOOTSTRAP_SERVERS:-kafka:29092}"
KAFKA_BIN=/opt/kafka/bin

create_topic() {
  local topic="$1"
  local cleanup="$2"
  local retention_ms="$3"
  local retention_bytes="$4"

  "$KAFKA_BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP_SERVER" \
    --create --if-not-exists --topic "$topic" --partitions 1 --replication-factor 1
  "$KAFKA_BIN/kafka-configs.sh" --bootstrap-server "$BOOTSTRAP_SERVER" \
    --entity-type topics --entity-name "$topic" --alter \
    --add-config "cleanup.policy=$cleanup,retention.ms=$retention_ms,retention.bytes=$retention_bytes"
}

for topic in \
  omni.oltp.public.customers \
  omni.oltp.public.orders \
  omni.oltp.public.payments \
  omni.oltp.transaction \
  __debezium-heartbeat.omni.oltp
do
  create_topic "$topic" delete 604800000 5368709120
done

for topic in omni-connect-configs omni-connect-offsets omni-connect-status
do
  create_topic "$topic" compact -1 -1
done

echo "Kafka CDC and Connect internal topics are reconciled"
