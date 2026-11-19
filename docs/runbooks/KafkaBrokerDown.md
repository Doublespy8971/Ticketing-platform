# Runbook: KafkaBrokerDown

## Symptom
`kafka_brokers < 1` or `up{job="kafka-exporter"} == 0` for >30 seconds.

## Impact
Inter-service saga events pause; outbox rows accumulate unpublished in PostgreSQL `outbox_events` tables.

## Diagnose
```bash
docker compose -f infra/docker-compose.yml -f infra/docker-compose.full.yml ps kafka
docker compose -f infra/docker-compose.yml -f infra/docker-compose.full.yml logs --tail=100 kafka kafka-exporter
docker compose -f infra/docker-compose.yml exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list
```

## Mitigate
Restart `kafka` container (`docker compose -f infra/docker-compose.yml up -d kafka`); outbox pollers drain buffered rows automatically on recovery.

## Related scenario
`S8` (`s8_kafkaBrokerOutageOutboxResilience`).
