# Runbook: NotificationServiceStalled

## Symptom
`notification-service-order-cg` has lag >0 on `order.events` while zero notifications were sent in 5m.

## Impact
Order confirmation/cancellation events accumulate unconsumed; customers receive delayed or no notifications.

## Diagnose
```bash
docker compose -f infra/docker-compose.yml exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:9092 --describe --group notification-service-order-cg
docker compose -f infra/docker-compose.yml -f infra/docker-compose.full.yml logs --tail=100 notification-service
curl -sf http://localhost:8084/health/readiness
```

## Mitigate
Verify Redis and Kafka connectivity, inspect `notification_dedup_redis_errors_total`, and restart `notification-service` if wedged.

## Related scenario
`S7`, `KAFKA-001`.
