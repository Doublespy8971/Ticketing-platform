# Runbook: CheckoutSLOFastBurn

## Symptom
`POST /api/v1/orders/{orderId}/checkout` 5xx error ratio exceeds 14.4x SLO budget across 5m and 1h windows.

## Impact
Holders cannot initiate payment checkout; reservations risk expiring before purchase.

## Diagnose
```bash
docker compose -f infra/docker-compose.yml -f infra/docker-compose.full.yml logs --tail=100 order-service
docker compose -f infra/docker-compose.yml exec postgres-order psql -U ticketing -d order_db -c "SELECT status, count(*) FROM orders GROUP BY 1;"
```

## Mitigate
Verify `postgres-order` connectivity and DB locks; restart degraded `order-service` if needed.

## Related scenario
`S7` (`s7_crashRecoveryUnderSigkill`), `S9` (`s9_idempotentCheckoutThroughGateway`).
