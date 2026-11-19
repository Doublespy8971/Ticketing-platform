# Runbook: Gateway429Spike

## Symptom
`gateway_rate_limited_total` rate exceeds 1 req/s over 2 minutes.

## Impact
Legitimate buyers or bots are receiving HTTP 429 `RATE_LIMIT_EXCEEDED` with `Retry-After: 1`.

## Diagnose
```bash
docker compose -f infra/docker-compose.yml -f infra/docker-compose.full.yml logs --tail=100 gateway-service | grep -i "rate"
docker compose -f infra/docker-compose.yml exec redis redis-cli keys "rl:*"
```

## Mitigate
Verify client traffic pattern vs bot abuse; inspect `X-Forwarded-For` / IP resolver config and Redis rate limiter keys.

## Related scenario
`S12` (`s12_gatewayProtectionRbacHeaderSpoofingAndRateLimitIsolation`).
