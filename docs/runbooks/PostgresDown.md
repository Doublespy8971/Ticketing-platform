# Runbook: PostgresDown

## Symptom
`pg_up == 0` on one or more PostgreSQL exporters (`postgres-inventory-exporter`, `postgres-order-exporter`, `postgres-payment-exporter`) for >30s.

## Impact
Affected service stops committing reservations/orders/payments, outbox polling fails, and readiness probes turn red.

## Diagnose
```bash
docker compose -f infra/docker-compose.yml -f infra/docker-compose.full.yml ps postgres-inventory postgres-order postgres-payment
docker compose -f infra/docker-compose.yml -f infra/docker-compose.full.yml logs --tail=100 postgres-inventory postgres-order postgres-payment
```

## Mitigate
Restart the failed PostgreSQL container and confirm Flyway/JDBC connection pools reconnect via service `/actuator/health/readiness`.

## Related scenario
Core infrastructure dependency check (`Infra healthcheck`).
