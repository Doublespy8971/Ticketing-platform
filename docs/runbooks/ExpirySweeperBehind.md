# Runbook: ExpirySweeperBehind

## Symptom
`inventory_oldest_overdue_hold_seconds > 90` in `inventory-service` for >1 minute.

## Impact
Expired seat holds are not promptly returned to `AVAILABLE`, blocking new buyers from reserving seats.

## Diagnose
```bash
docker compose -f infra/docker-compose.yml -f infra/docker-compose.full.yml logs --tail=100 inventory-service | grep -i sweep
docker compose -f infra/docker-compose.yml exec postgres-inventory psql -U ticketing -d inventory_db -c "SELECT count(*), min(expires_at) FROM reservations WHERE status='HELD' AND expires_at < NOW();"
```

## Mitigate
Verify `HOLD_EXPIRY_SWEEP_ENABLED=true`, inspect DB lock contention, or temporarily raise `HOLD_EXPIRY_SWEEP_MAX_PAGES`.

## Related scenario
`S3` (`s3_holdExpiryReleasesSeatAndCancelsOrder`).
