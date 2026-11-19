# Runbook: ConsumerLagHigh

## Symptom
`kafka_consumergroup_lag` exceeds 100 messages for a consumer group/topic for >2 minutes.

## Impact
Async order materialization, payment charges, or customer notifications are delayed.

## Diagnose
```bash
docker compose -f infra/docker-compose.yml exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:9092 --describe --all-groups
docker compose -f infra/docker-compose.yml -f infra/docker-compose.full.yml logs --tail=100 order-service payment-service notification-service
```

## Mitigate
Verify consumers are alive and not rebalancing (`session.timeout.ms=15000`); restart stalled consumers or scale instances.

## Related scenario
`S7` (`s7_crashRecoveryUnderSigkill`), `S8`, `KAFKA-001`.
