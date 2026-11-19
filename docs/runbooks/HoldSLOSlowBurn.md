# Runbook: HoldSLOSlowBurn

## Symptom
`POST /api/v1/reservations` 5xx error ratio exceeds 6x SLO budget (0.30%) across 30m and 6h windows.

## Impact
Sustained elevated failure rate on seat holds slowly exhausting the rolling error budget.

## Diagnose
```bash
docker compose -f infra/docker-compose.yml -f infra/docker-compose.full.yml logs --tail=150 inventory-service | grep -iE "error|timeout|unavailable"
docker compose -f infra/docker-compose.yml exec postgres-inventory psql -U ticketing -d inventory_db -c "SELECT state, count(*) FROM pg_stat_activity GROUP BY 1;"
```

## Mitigate
Inspect slow queries, Hikari pool saturation, or intermittent Redis timeouts in `inventory-service`.

## Related scenario
`S5` (`s5_noOversellUnderHighContentionLoad`), `INV-001`.
