# Platform Scripts

## Inventory Service Smoke Test (`smoke-inventory.sh`)

End-to-end smoke test script for `inventory-service` validating API contracts, concurrent seat hold semantics, idempotency replays and rejections, PostgreSQL anti-overselling invariants, and Redis TTL gates.

### Prerequisites

1. Infrastructure running via `make infra-up` (PostgreSQL 16 on container `postgres-inventory`, Redis on container `redis`).
2. `inventory-service` running on port 8081 (e.g. `mvn spring-boot:run -pl inventory-service`).
3. CLI dependencies: `bash`, `curl`, `jq`, and `docker` (with access to `docker exec`).

### How to Run

#### Using Make

```bash
make smoke-inventory
```

#### Directly via Bash

```bash
bash scripts/smoke-inventory.sh
# or
./scripts/smoke-inventory.sh
```

### Environment Variables

| Variable | Default | Description |
| :--- | :--- | :--- |
| `INVENTORY_URL` | `http://localhost:8081` | Base URL of `inventory-service` |
| `POSTGRES_CONTAINER` | `postgres-inventory` | Docker container name for `inventory_db` |
| `REDIS_CONTAINER` | `redis` | Docker container name for Redis |
| `POSTGRES_USER` | `ticketing` | PostgreSQL username for `psql` queries |

### Test Workflow

The script executes 8 verification stages and outputs one `[PASS]` or `[FAIL]` line per stage:

1. **Service Health Check**: Queries `GET /actuator/health` and verifies `status: "UP"`.
2. **Event Creation & Sale Activation**: Creates an event via `POST /api/v1/admin/events` with `X-User-Roles: ADMIN`, drop time in the past, and event time in the future. As no admin API endpoint exists to transition events to `ON_SALE`, updates status directly in PostgreSQL via `docker exec` and logs a warning.
3. **Bulk Seat Provisioning**: Bulk-creates 20 seats via `POST /api/v1/admin/events/{eventId}/seats/bulk` and captures assigned seat IDs.
4. **Single-Seat Hold & Conflict**: Holds seat 1 as user 1 (`POST /api/v1/reservations`) verifying `201 Created` with full schema; attempts to hold the same seat as user 2 and verifies `409 Conflict` with `SEAT_ALREADY_TAKEN`.
5. **Parallel 50-Request Race**: Fires 50 parallel hold requests from 50 distinct users for seat 2; verifies exactly one `201 Created` and 49 `409 Conflict` responses.
6. **Idempotency Verification**: Replays user 1's request with the same `Idempotency-Key` and body to verify identical `201 Created` response; sends the same key with a modified payload to verify `422 Unprocessable Entity` (`IDEMPOTENCY_KEY_PAYLOAD_MISMATCH`).
7. **PostgreSQL Invariant Verification**: Validates via `docker exec psql`:
   - Active reservation line items equal the number of successful holds (2).
   - No seat has more than one active reservation item.
   - `outbox_events` has exactly one `inventory.seat.reserved` row per successful hold.
   - The two held seats are in status `HELD` and the remaining 18 are `AVAILABLE`.
8. **Redis TTL & Key Verification**: Validates via `docker exec redis-cli` that `seat:{eventId}:{seatId}` keys exist for both held seats with TTLs between 600 and 660 seconds.

