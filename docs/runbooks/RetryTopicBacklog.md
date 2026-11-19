# Runbook: RetryTopicBacklog

## Symptom
`kafka_consumergroup_lag` on `*-retry-0` or `*-retry-1` remains >0 for more than 2 minutes.

## Impact
Events are failing transiently (e.g., provider errors or DB contention) and delaying saga completion.

## Diagnose
```bash
docker compose -f infra/docker-compose.yml exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:9092 --describe --group payment-service-order-cg
docker compose -f infra/docker-compose.yml exec kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server kafka:9092 --topic order.events-payment-service-order-cg-retry-0
docker compose -f infra/docker-compose.yml -f infra/docker-compose.full.yml logs --tail=100 payment-service order-service inventory-service
```

## Mitigate
Fix transient downstream failures (provider/DB/Redis) so retries drain before exhausting attempts to DLT.

## Related scenario
`S10` (`s10_technicalProviderErrorRetriesAndSucceeds`).
