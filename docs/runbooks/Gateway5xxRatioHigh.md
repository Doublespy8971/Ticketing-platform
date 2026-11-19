# Runbook: Gateway5xxRatioHigh

## Symptom
`gateway_requests_total{status_class="5xx"}` exceeds 1% of total `gateway-service` traffic over 5m.

## Impact
API callers experience 502 Bad Gateway, 503 Service Unavailable, or 504 Gateway Timeout errors.

## Diagnose
```bash
docker compose -f infra/docker-compose.yml -f infra/docker-compose.full.yml logs --tail=100 gateway-service
curl -sf http://localhost:8080/actuator/health/readiness
docker compose -f infra/docker-compose.yml -f infra/docker-compose.full.yml ps
```

## Mitigate
Identify failing upstream (`inventory-service`, `order-service`, or `redis`) and restore/restart the downstream container.

## Related scenario
`S5`, `S7`, `INV-001`.
