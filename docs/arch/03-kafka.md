## 3. Kafka Design

### 3.1 Broker & Cluster Configuration (KRaft Mode)
- **Kafka Version**: Apache Kafka `3.8.0` running in **KRaft mode** (`process.roles=broker,controller`, zero ZooKeeper dependency).
- **Producer Settings (All Java Outbox Pollers)**:
  - `acks=all`
  - `enable.idempotence=true`
  - `max.in.flight.requests.per.connection=5`
  - `retries=2147483647`
  - `compression.type=lz4`
  - `linger.ms=5`
- **Consumer Settings (Java & Go Services)**:
  - `enable.auto.commit=false` (manual offset acknowledgment only after local DB transaction commits or Redis deduplication succeeds)
  - `isolation.level=read_committed`
  - `auto.offset.reset=earliest`
  - `max.poll.records=100`

---

### 3.2 Primary Domain Topics Table

All domain events belonging to a reservation/order lifecycle use `reservationId` (which equals `orderId`) as the Kafka message key (`StringSerializer` of the UUID string). This guarantees that all events for a single reservation/order land on the exact same partition and are processed in causal order per consumer group.

| Topic Name | Partitions (Docker Compose) | Partitions (Kubernetes) | Replication Factor (Compose / K8s) | `min.insync.replicas` (Compose / K8s) | Partition Key | Retention (`retention.ms`) | Cleanup Policy | Producer Service | Consumer Services (`group.id`) |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `inventory.events` | `12` | `24` | `1` / `3` | `1` / `2` | `reservationId` (`orderId`) | `604800000` (7 days) | `delete` | `inventory-service` | `order-service` (`order-service-inventory-cg`) |
| `order.events` | `12` | `24` | `1` / `3` | `1` / `2` | `orderId` (`reservationId`) | `604800000` (7 days) | `delete` | `order-service` | `payment-service` (`payment-service-order-cg`), `notification-service` (`notification-service-order-cg`) |
| `payment.events` | `12` | `24` | `1` / `3` | `1` / `2` | `orderId` (`reservationId`) | `604800000` (7 days) | `delete` | `payment-service` | `inventory-service` (`inventory-service-payment-cg`), `order-service` (`order-service-payment-cg`) |

---

### 3.3 Retry Topics and Dead-Letter Topic (DLT) Convention

Every stateful Java consumer group (`inventory-service`, `order-service`, `payment-service`) is isolated with **2 retry topic levels (`2s` and `10s` backoff)** plus **1 dead-letter topic (DLT)** using the Spring Kafka `@RetryableTopic` naming convention (`RetryTopicNamesProviderFactory` with consumer group qualifier):

- **Retry Level 1 (2,000 ms delay)**: `<topic>-<consumer-group>-retry-0`
- **Retry Level 2 (10,000 ms delay)**: `<topic>-<consumer-group>-retry-1`
- **Dead-Letter Topic**: `<topic>-<consumer-group>-dlt`

#### Spring Kafka `@RetryableTopic` Configuration Contract & `notification-service` Policy
- `attempts = "3"` (1 initial attempt on main topic + 2 retry attempts on `-retry-0` and `-retry-1`)
- `backoff = @Backoff(delay = 2000, multiplier = 5.0)` (produces exact delays of `2000 ms` for `-retry-0` and `10000 ms` for `-retry-1`)
- `autoCreateTopics = "false"` (all main, retry, and DLT topics are provisioned deterministically at startup via `kafka-topics.sh` / Strimzi `KafkaTopic` manifests)
- `dltStrategy = DltStrategy.FAIL_ON_ERROR`
- Non-retryable exceptions (`JsonProcessingException`, `IllegalArgumentException`, schema validation failures) bypass `-retry-0` and `-retry-1` and route directly to `-dlt`.
- **No Retry or DLT Topics for `notification-service`**: Because customer notifications are non-critical to saga state convergence, `notification-service` does **not** have `-retry-0`, `-retry-1`, or `-dlt` topics. When `notification-service` fails to process or dispatch a notification, it logs the error at `ERROR` level, increments `ticketing_notifications_sent_total{status="FAILED"}`, commits the offset, and does not forward the message to any topic.

#### Complete Retry & Dead-Letter Topics Table

| Topic Name | Backoff Delay | Partitions (Compose / K8s) | Replication Factor (Compose / K8s) | `min.insync.replicas` (Compose / K8s) | Partition Key | Retention (`retention.ms`) | Producer / Forwarder | Consumer Group |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `inventory.events-order-service-inventory-cg-retry-0` | `2000 ms` (2s) | `12` / `24` | `1` / `3` | `1` / `2` | `reservationId` | `604800000` (7d) | `order-service` | `order-service-inventory-cg-retry-0` |
| `inventory.events-order-service-inventory-cg-retry-1` | `10000 ms` (10s) | `12` / `24` | `1` / `3` | `1` / `2` | `reservationId` | `604800000` (7d) | `order-service` | `order-service-inventory-cg-retry-1` |
| `inventory.events-order-service-inventory-cg-dlt` | None (Terminal) | `3` / `6` | `1` / `3` | `1` / `2` | `reservationId` | `2592000000` (30d) | `order-service` | `order-service-inventory-cg-dlt` |
| `order.events-payment-service-order-cg-retry-0` | `2000 ms` (2s) | `12` / `24` | `1` / `3` | `1` / `2` | `orderId` | `604800000` (7d) | `payment-service` | `payment-service-order-cg-retry-0` |
| `order.events-payment-service-order-cg-retry-1` | `10000 ms` (10s) | `12` / `24` | `1` / `3` | `1` / `2` | `orderId` | `604800000` (7d) | `payment-service` | `payment-service-order-cg-retry-1` |
| `order.events-payment-service-order-cg-dlt` | None (Terminal) | `3` / `6` | `1` / `3` | `1` / `2` | `orderId` | `2592000000` (30d) | `payment-service` | `payment-service-order-cg-dlt` |
| `payment.events-inventory-service-payment-cg-retry-0` | `2000 ms` (2s) | `12` / `24` | `1` / `3` | `1` / `2` | `orderId` | `604800000` (7d) | `inventory-service` | `inventory-service-payment-cg-retry-0` |
| `payment.events-inventory-service-payment-cg-retry-1` | `10000 ms` (10s) | `12` / `24` | `1` / `3` | `1` / `2` | `orderId` | `604800000` (7d) | `inventory-service` | `inventory-service-payment-cg-retry-1` |
| `payment.events-inventory-service-payment-cg-dlt` | None (Terminal) | `3` / `6` | `1` / `3` | `1` / `2` | `orderId` | `2592000000` (30d) | `inventory-service` | `inventory-service-payment-cg-dlt` |
| `payment.events-order-service-payment-cg-retry-0` | `2000 ms` (2s) | `12` / `24` | `1` / `3` | `1` / `2` | `orderId` | `604800000` (7d) | `order-service` | `order-service-payment-cg-retry-0` |
| `payment.events-order-service-payment-cg-retry-1` | `10000 ms` (10s) | `12` / `24` | `1` / `3` | `1` / `2` | `orderId` | `604800000` (7d) | `order-service` | `order-service-payment-cg-retry-1` |
| `payment.events-order-service-payment-cg-dlt` | None (Terminal) | `3` / `6` | `1` / `3` | `1` / `2` | `orderId` | `2592000000` (30d) | `order-service` | `order-service-payment-cg-dlt` |

