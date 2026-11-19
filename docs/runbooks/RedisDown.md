# Runbook: RedisDown

## Symptom
`redis_up == 0` reported by `redis_exporter` for >30 seconds.

## Impact
Seat hold concurrency gate fails closed (503), rate limiting degrades/blocks, and notification dedup stalls.

## Diagnose
```bash
docker compose -f infra/docker-compose.yml -f infra/docker-compose.full.yml ps redis
docker compose -f infra/docker-compose.yml -f infra/docker-compose.full.yml logs --tail=100 redis
docker compose -f infra/docker-compose.yml exec redis redis-cli ping
```

## Mitigate
Restart `redis` (`docker compose -f infra/docker-compose.yml up -d redis`) and verify `inventory-service` / `notification-service` readiness probes recover.

## Related scenario
`INV-001` (Redis dependency failure).
