# Runbook: DeadLetterGrowth

## Symptom
End offset (`kafka_topic_partition_current_offset`) of a `*-dlt` dead-letter topic increased over 5m.

## Impact
A domain or compensation event failed permanently (e.g., declined refund or malformed event), risking stuck saga state.

## Diagnose
```bash
docker compose -f infra/docker-compose.yml exec kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server kafka:9092 --topic order.events-payment-service-order-cg-dlt
docker compose -f infra/docker-compose.yml exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic order.events-payment-service-order-cg-dlt --from-beginning --max-messages 5 --property print.headers=true
docker compose -f infra/docker-compose.yml exec postgres-order psql -U ticketing -d order_db -c "SELECT order_id, status FROM orders WHERE status IN ('REFUND_PENDING','PAYMENT_PENDING');"
```

## Mitigate
Inspect DLT headers (`kafka_dlt-exception-message`), resolve root cause or manually reconcile `REFUND_PENDING` orders.

## Related scenario
`S11` (`s11_refundDeclinedRoutesToDeadLetterTopic`), `PAYMENT-001`.
