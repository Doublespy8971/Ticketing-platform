# Runbook: OutboxPendingGrowing

## Symptom
`ticketing_outbox_unpublished_records > 50` and derivative is positive over 5 minutes.

## Impact
Outbox publish throughput lags behind transaction rate, increasing order materialization and payment latency.

## Diagnose
```bash
docker compose -f infra/docker-compose.yml -f infra/docker-compose.full.yml logs --tail=100 <service> | grep -i outbox
docker compose -f infra/docker-compose.yml exec postgres-inventory psql -U ticketing -d inventory_db -c "SELECT count(*) FROM outbox_events WHERE published_at IS NULL;"
```

## Mitigate
Check Kafka broker latency, increase `OUTBOX_BATCH_SIZE` or decrease `OUTBOX_POLL_INTERVAL_MS` if needed.

## Related scenario
`S8` (`s8_kafkaBrokerOutageOutboxResilience`).
