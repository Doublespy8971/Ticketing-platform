# Runbook: OutboxStuck

## Symptom
`outbox_oldest_pending_age_seconds > 30` in `inventory-service`, `order-service`, or `payment-service` for >1m.

## Impact
Committed DB changes (holds, checkouts, payments) are not published to Kafka, stalling downstream sagas.

## Diagnose
```bash
docker compose -f infra/docker-compose.yml -f infra/docker-compose.full.yml logs --tail=100 <service> | grep -i outbox
docker compose -f infra/docker-compose.yml exec postgres-order psql -U ticketing -d order_db -c "SELECT count(*), min(created_at) FROM outbox_events WHERE published_at IS NULL;"
docker compose -f infra/docker-compose.yml exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list
```

## Mitigate
Verify Kafka broker health (`KafkaBrokerDown`), check advisory lock contention, and restart affected outbox poller if stalled.

## Related scenario
`S8` (`s8_kafkaBrokerOutageOutboxResilience`).
