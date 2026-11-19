# Runbook: PaymentRefundDeclined

## Symptom
`payment_refund_declined_total` increased over 5m (`increase(...) > 0`).

## Impact
Compensating refund for late payment was declined; payment remains `SUCCEEDED`, order stays `REFUND_PENDING`, event moves to DLT.

## Diagnose
```bash
docker compose -f infra/docker-compose.yml exec postgres-order psql -U ticketing -d order_db -c "SELECT order_id, user_id, status, total_amount_cents FROM orders WHERE status='REFUND_PENDING';"
docker compose -f infra/docker-compose.yml exec postgres-payment psql -U ticketing -d payment_db -c "SELECT order_id, status, failure_code FROM payments WHERE order_id IN (SELECT order_id FROM payments WHERE status='SUCCEEDED');"
docker compose -f infra/docker-compose.yml exec kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server kafka:9092 --topic order.events-payment-service-order-cg-dlt
```

## Mitigate
Inspect the DLT record (`DeadLetterGrowth`), perform manual refund via payment provider portal, and update order/payment state.

## Related scenario
`S11` (`s11_refundDeclinedRoutesToDeadLetterTopic`), `PAYMENT-001`.
