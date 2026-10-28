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

