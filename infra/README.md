# Local Infrastructure

Kafka, three PostgreSQL databases and Redis for local development. No application services yet.

## Ports

| Service | Container | Host port |
| :--- | :--- | :--- |
| Kafka (host listener) | `kafka` | `localhost:29092` |
| Kafka (containers) | `kafka` | `kafka:9092` |
| `inventory_db` | `postgres-inventory` | `5433` |
| `order_db` | `postgres-order` | `5434` |
| `payment_db` | `postgres-payment` | `5435` |
| Redis | `redis` | `6379` |

All containers share the `ticketing-net` network. Credentials live in `infra/.env` (defaults in `infra/.env.example`).

## Start

```bash
make infra-up   # copies infra/.env.example to infra/.env if missing
```

Other targets: `infra-down`, `infra-reset` (wipes volumes), `infra-topics`,
`infra-psql-inventory`, `infra-psql-order`, `infra-psql-payment`, `infra-redis-cli`.

## Verify

```bash
docker compose -f infra/docker-compose.yml ps   # all healthy, kafka-init exited 0
make infra-topics                               # 3 primary + 12 retry/DLT topics
make infra-redis-cli                            # PING -> PONG
make infra-psql-inventory                       # \conninfo
```

Redis keyspace notifications are intentionally not enabled.

## Full stack in Docker

The entire ticketing platform can be run containerized alongside infrastructure using Docker Compose.

### Commands

- **Build all images**:
  ```bash
  make stack-build
  # Or: docker compose -f infra/docker-compose.yml -f infra/docker-compose.full.yml build
  ```
- **Start full stack and wait for health**:
  ```bash
  make stack-up
  # Starts infra and all 5 services, waiting up to 180s until all containers are healthy.
  ```
- **Stop full stack**:
  ```bash
  make stack-down
  ```
- **Reset full stack** (wipes databases, Kafka data, and Redis, then restarts fresh):
  ```bash
  make stack-reset
  ```
- **Follow service logs**:
  ```bash
  make stack-logs
  # Or inspect a single service: docker compose -f infra/docker-compose.yml -f infra/docker-compose.full.yml logs -f <service-name>
  ```
- **Check container status**:
  ```bash
  make stack-ps
  ```
- **Run all end-to-end smoke tests**:
  ```bash
  make smoke-all
  # Runs smoke-inventory, smoke-order, smoke-payment, smoke-notification, and smoke-gateway in sequence.
  ```

### Rebuilding a Single Service

To rebuild and restart an individual service after making code changes:
```bash
docker compose -f infra/docker-compose.yml -f infra/docker-compose.full.yml build <service-name>
docker compose -f infra/docker-compose.yml -f infra/docker-compose.full.yml up -d --no-deps <service-name>
```
Examples:
- Rebuild gateway: `docker compose -f infra/docker-compose.yml -f infra/docker-compose.full.yml build gateway-service`
- Rebuild inventory: `docker compose -f infra/docker-compose.yml -f infra/docker-compose.full.yml build inventory-service`

### Port Map & Reaching Services

All services publish to standard host ports so local smoke scripts and curl commands work identically:

| Service | Container | Host Port | Internal Port | Health / Metrics Endpoint |
| :--- | :--- | :--- | :--- | :--- |
| **API Gateway** | `gateway-service` | `localhost:8080` | `8080` | `http://localhost:8080/actuator/health` |
| **Inventory Service** | `inventory-service` | `localhost:8081` | `8081` | `http://localhost:8081/actuator/health` |
| **Order Service** | `order-service` | `localhost:8082` | `8082` | `http://localhost:8082/actuator/health` |
| **Payment Service** | `payment-service` | `localhost:8083` | `8083` | `http://localhost:8083/actuator/health` |
| **Notification Service** | `notification-service` | `localhost:8084` | `8084` | `http://localhost:8084/health` |
| Kafka (Broker) | `kafka` | `localhost:29092` | `kafka:9092` | Internal cluster bus |
| Postgres Inventory | `postgres-inventory` | `localhost:5433` | `5432` | `inventory_db` |
| Postgres Order | `postgres-order` | `localhost:5434` | `5432` | `order_db` |
| Postgres Payment | `postgres-payment` | `localhost:5435` | `5432` | `payment_db` |
| Redis | `redis` | `localhost:6379` | `6379` | Deduplication & locks |

### Troubleshooting Guide

1. **Port already in use**:
   - *Symptom*: Docker fails to bind port `8080`, `8081`, `8082`, `8083`, `8084`, `5433-5435`, `6379`, or `29092`.
   - *Fix*: Check what process is listening: `lsof -nP -iTCP:<PORT> -sTCP:LISTEN`. Terminate the conflicting local process (e.g., `kill -9 <PID>`) or stop existing Docker containers with `make stack-down`.
2. **Stale process on 8081 (or other service port)**:
   - *Symptom*: Smoke tests fail unexpectedly or connect to old code because a background JVM or Go process was left running on the host.
   - *Fix*: Run `lsof -i :8081` and `kill -9 <PID>` to kill the orphaned host process before running `make stack-up`.
3. **Containers not healthy (`health: starting` timeout or `unhealthy`)**:
   - *Symptom*: `make stack-up` times out after 180 seconds or reports an unhealthy container.
   - *Fix*: Check container logs with `docker compose -f infra/docker-compose.yml -f infra/docker-compose.full.yml logs <service-name>`. Ensure `infra/.env` contains valid credentials.
4. **Kafka topics missing**:
   - *Symptom*: Services crash or log `UnknownTopicOrPartitionException`.
   - *Fix*: Check `kafka-init` container status: `docker compose -f infra/docker-compose.yml ps -a kafka-init`. Ensure `kafka-init` ran `create-topics.sh` and exited with code 0. If needed, manually recreate topics with `docker compose -f infra/docker-compose.yml up kafka-init`.

