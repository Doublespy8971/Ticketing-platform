# Runbook: PaymentProviderErrors

## Symptom
`payment_provider_calls_total{outcome="error"}` rate exceeds 0.05/s over 2 minutes.

## Impact
Charges/refunds fail transiently (`ProviderUnavailableException`) and route through `-retry-0` / `-retry-1` topics.

## Diagnose
```bash
docker compose -f infra/docker-compose.yml -f infra/docker-compose.full.yml logs --tail=100 payment-service | grep -iE "provider|unavailable|retry"
docker compose -f infra/docker-compose.yml exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:9092 --describe --group payment-service-order-cg
docker compose -f infra/docker-compose.yml exec postgres-payment psql -U ticketing -d payment_db -c "SELECT status, count(*) FROM payments GROUP BY 1;"
```

## Mitigate
Inspect external payment gateway health and monitor retry queues (`RetryTopicBacklog`) until provider recovers.

## Related scenario
`S10` (`s10_technicalProviderErrorRetriesAndSucceeds`).
