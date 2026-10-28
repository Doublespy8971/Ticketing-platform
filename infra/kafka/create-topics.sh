#!/usr/bin/env bash
# Idempotent topic provisioning (Docker Compose column of docs/arch/03-kafka.md).
set -euo pipefail

KAFKA_BIN="${KAFKA_BIN:-/opt/kafka/bin}"
BOOTSTRAP="${KAFKA_BOOTSTRAP_SERVERS:-kafka:9092}"

SEVEN_DAYS=604800000
THIRTY_DAYS=2592000000

# create_topic <name> <partitions> <retention.ms> [extra --config entries...]
create_topic() {
  local name="$1" partitions="$2" retention="$3"
  shift 3
  echo "Ensuring topic: ${name}"
  "${KAFKA_BIN}/kafka-topics.sh" --bootstrap-server "${BOOTSTRAP}" \
    --create --if-not-exists \
    --topic "${name}" \
    --partitions "${partitions}" \
    --replication-factor 1 \
    --config "retention.ms=${retention}" \
    --config "min.insync.replicas=1" \
    "$@"
}

# Primary domain topics
for topic in inventory.events order.events payment.events; do
  create_topic "${topic}" 12 "${SEVEN_DAYS}" --config "cleanup.policy=delete"
done

# Retry and dead-letter topics: <topic>-<consumer-group>-{retry-0,retry-1,dlt}
RETRY_BASES=(
  "inventory.events-order-service-inventory-cg"
  "order.events-payment-service-order-cg"
  "payment.events-inventory-service-payment-cg"
  "payment.events-order-service-payment-cg"
)
for base in "${RETRY_BASES[@]}"; do
  create_topic "${base}-retry-0" 12 "${SEVEN_DAYS}"
  create_topic "${base}-retry-1" 12 "${SEVEN_DAYS}"
  create_topic "${base}-dlt" 3 "${THIRTY_DAYS}"
done

echo "All topics provisioned."

