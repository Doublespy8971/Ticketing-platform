# Runbook: ServiceDown

## Symptom
Prometheus reports `up == 0` for a service or exporter target for >1m.

## Impact
Users see 502/503 gateway errors (`gateway-service`) or async saga/metrics gaps.

## Diagnose
```bash
docker compose -f infra/docker-compose.yml -f infra/docker-compose.full.yml ps
docker compose -f infra/docker-compose.yml -f infra/docker-compose.full.yml logs --tail=100 <service>
curl -sf http://localhost:8080/actuator/health/readiness
```

## Mitigate
Restart the stopped/crashed container (`docker compose ... up -d <service>`) and check OOM (`docker inspect`) or dependency connectivity.

## Related scenario
`S7` (`s7_crashRecoveryUnderSigkill`), `KAFKA-001`.
