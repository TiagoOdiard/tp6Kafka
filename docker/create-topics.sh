#!/bin/bash
# ============================================================
# Crea los 4 topics de negocio + sus DLQ.
# Todos con 3 particiones y retención de 7 días.
# ============================================================
set -e

BOOTSTRAP="kafka:9092"
RETENTION_MS=604800000 # 7 días

TOPICS=(
  "activation.requested"
  "billing.events"
  "provisioning.events"
  "activation.events"
)

echo "Creando topics en $BOOTSTRAP ..."

for topic in "${TOPICS[@]}"; do
  for name in "$topic" "$topic.dlq"; do
    /opt/kafka/bin/kafka-topics.sh \
      --bootstrap-server "$BOOTSTRAP" \
      --create \
      --if-not-exists \
      --topic "$name" \
      --partitions 3 \
      --replication-factor 1 \
      --config "retention.ms=$RETENTION_MS"
    echo "  OK -> $name (3 particiones)"
  done
done

echo "Topics disponibles:"
/opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --list
