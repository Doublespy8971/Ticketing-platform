# Runbook: RedisGateUnavailable

## Symptom
`inventory_redis_unavailable_total` rate >0 for 1m (`inventory-service` failing closed with HTTP 503 `SERVICE_UNAVAILABLE`).

## Impact
New seat holds (`POST /api/v1/reservations`) are rejected with HTTP 503 + `Retry-After: 1`.

## Diagnose
```bash
docker compose -f infra/docker-compose.yml -f infra/docker-compose.full.yml logs --tail=100 inventory-service | grep -i redis
docker compose -f infra/docker-compose.yml exec redis redis-cli ping
docker compose -f infra/docker-compose.yml exec redis redis-cli info clients
```

## Mitigate
Restore Redis container health (`RedisDown`), verify network reachability on `ticketing-net`, or increase command timeout if under saturation.

## Related scenario
`S5` (`s5_noOversellUnderHighContentionLoad`), `INV-001`.
