# Runbook: HoldSLOFastBurn

## Symptom
`POST /api/v1/reservations` 5xx error ratio exceeds 14.4x SLO budget (0.72%) across 5m and 1h windows.

## Impact
Flash-sale buyers fail to hold seats (500/503); error budget exhausts rapidly.

## Diagnose
```bash
docker compose -f infra/docker-compose.yml -f infra/docker-compose.full.yml logs --tail=100 inventory-service
docker compose -f infra/docker-compose.yml exec postgres-inventory psql -U ticketing -d inventory_db -c "SELECT count(*) FROM reservations WHERE status='HELD';"
docker compose -f infra/docker-compose.yml exec redis redis-cli ping
```

## Mitigate
Verify Redis and `postgres-inventory` health; scale connections/pool or restart degraded `inventory-service`.

## Related scenario
`S5` (`s5_noOversellUnderHighContentionLoad`), `INV-001`.
