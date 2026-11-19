# Runbook: NotificationFailures

## Symptom
`notification_events_total{outcome="failed"}` rate >0 for >1 minute.

## Impact
Customers do not receive email/webhook notifications for confirmed or cancelled orders (at-most-once delivery after retries).

## Diagnose
```bash
docker compose -f infra/docker-compose.yml -f infra/docker-compose.full.yml logs --tail=100 notification-service
curl -sf http://localhost:8084/health/readiness
```

## Mitigate
Check downstream notification channel/SMTP availability or Redis dedup claim errors; inspect `notification-service` logs for error cause.

## Related scenario
`S1`, `S2`, `S13` (`notification_failed_total == 0` invariant).
