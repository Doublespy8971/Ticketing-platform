## 9. Redis Design

### 9.1 Redis Key Naming, Types, and TTL Contract

| Key Pattern | Redis Type | Value Format | TTL (Seconds) | Owner Service | Purpose |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `event:{eventId}:drop_ts` | `STRING` | Epoch milliseconds (`"1793548800000"`) | `604800` (7 days) | `inventory-service` | Enforces event drop-time gate inside `hold_inventory.lua` without hitting PostgreSQL. |
| `seat:{eventId}:{seatId}` | `STRING` | `"{reservationId}"` (while held) or `"SOLD"` (once confirmed) | **`660`** while `HELD`; **No TTL (never expires)** once `SOLD` | `inventory-service` | Atomic per-seat concurrency lock. **660s TTL while `HELD` is strictly 60s longer than PostgreSQL `expires_at` (600s)** as a safety net so Redis never expires a held seat before PostgreSQL. Once `SOLD`, the key has no TTL and never expires. |
| `ratelimit:user:{userId}` | `HASH` | `tokens` (float), `last_refill_ms` (int64) | `60` | `gateway-service` | Per-user token-bucket rate limiter (10 req/s burst, 5 req/s sustained). |
| `ratelimit:ip:{ipAddress}` | `HASH` | `tokens` (float), `last_refill_ms` (int64) | `60` | `gateway-service` | Per-IP token-bucket rate limiter on unauthenticated endpoints (20 req/s burst, 10 req/s sustained). |
| `notif:dedup:{eventId}` | `STRING` | `"1"` | `604800` (7 days) | `notification-service` | Stateless consumer deduplication key for `order.confirmed` and `order.cancelled`. |

---

### 9.2 Atomic Lua Scripts (`inventory-service` & `gateway-service`)

#### 9.2.1 `hold_inventory.lua` (All-or-Nothing Assigned Seat Hold Acquisition)
Before invoking `hold_inventory.lua`, `inventory-service` sorts `seatIds` lexicographically to maintain deterministic key ordering. All business outcomes are returned as **plain strings** (`"HELD"`, `"EVENT_NOT_CACHED"`, `"EVENT_NOT_OPEN"`, `"SEAT_TAKEN"`):
- **`KEYS[1]`**: `event:{eventId}:drop_ts`
- **`KEYS[2 .. 1+N]`**: `seat:{eventId}:{seatId_1}` .. `seat:{eventId}:{seatId_N}` (for `1 <= N <= 6` assigned seats)
- **`ARGV[1]`**: `reservationId` (UUID string)
- **`ARGV[2]`**: `nowEpochMs` (current epoch milliseconds from `System.currentTimeMillis()`)
- **`ARGV[3]`**: `"660"` (Redis seat lock safety-net TTL in seconds — 60s longer than PostgreSQL `expires_at`)

```lua
-- hold_inventory.lua
local drop_ts_str = redis.call('GET', KEYS[1])
if not drop_ts_str then
    return 'EVENT_NOT_CACHED'
end

local now_ms = tonumber(ARGV[2])
local drop_ms = tonumber(drop_ts_str)
if now_ms < drop_ms then
    return 'EVENT_NOT_OPEN'
end

-- Phase 1: Verify every requested seat is currently unheld and unsold in Redis
for i = 2, #KEYS do
    local current_val = redis.call('GET', KEYS[i])
    if current_val ~= false then
        return 'SEAT_TAKEN'
    end
end

-- Phase 2: All seats are free; acquire all seat locks with 660s safety-net TTL
local reservation_id = ARGV[1]
local seat_ttl_sec = tonumber(ARGV[3])

for i = 2, #KEYS do
    redis.call('SET', KEYS[i], reservation_id, 'EX', seat_ttl_sec)
end

return 'HELD'
```

#### Mapping `hold_inventory.lua` Plain-String Outcomes to HTTP Responses in `inventory-service`
`inventory-service` executes `hold_inventory.lua` via `StringRedisTemplate.execute(DefaultRedisScript<String>, keys, args)` and maps the returned plain string directly:
- `"HELD"`: Proceeds to the synchronous PostgreSQL transaction in `inventory_db` (`seats`, `reservations`, `reservation_items`, `outbox_events`) and returns `201 Created` on commit (or runs `release_inventory.lua` in `finally` on rollback).
- `"EVENT_NOT_CACHED"`: Executes the cache rewarm job from `inventory_db` (Section 9.3.3) and retries `hold_inventory.lua` once; if `eventId` does not exist in `inventory_db`, returns `404 Not Found` (`RESOURCE_NOT_FOUND`).
- `"EVENT_NOT_OPEN"`: Deletes the `IN_PROGRESS` idempotency key claim (so a later retry re-executes) and returns `409 Conflict` with RFC 7807 `errorCode: "EVENT_NOT_OPEN"`.
- `"SEAT_TAKEN"`: Deletes the `IN_PROGRESS` idempotency key claim (so a later retry re-executes) and returns `409 Conflict` with RFC 7807 `errorCode: "SEAT_ALREADY_TAKEN"`.

---

#### 9.2.2 `release_inventory.lua` (Compare-and-Delete Seat Hold Release)
Called in two scenarios: (1) inside the `finally` compensation block if the synchronous PostgreSQL transaction in `POST /api/v1/reservations` fails, or (2) **after** PostgreSQL commits the transition of a reservation to `EXPIRED` or `RELEASED`.
- **`KEYS[1 .. N]`**: `seat:{eventId}:{seatId_1}` .. `seat:{eventId}:{seatId_N}`
- **`ARGV[1]`**: `reservationId` (token check so a delayed cleanup can never delete a seat key re-acquired by another reservation)

```lua
-- release_inventory.lua
local reservation_id = ARGV[1]
local released_count = 0

for i = 1, #KEYS do
    local current_holder = redis.call('GET', KEYS[i])
    if current_holder == reservation_id then
        redis.call('DEL', KEYS[i])
        released_count = released_count + 1
    end
end

return released_count
```

---

#### 9.2.3 `confirm_inventory.lua` (Mark Seats SOLD Permanently on Payment Confirmation)
Called **after** `inventory-service` commits `reservations.status = 'CONFIRMED'` and `seats.status = 'SOLD'` in PostgreSQL upon consuming `payment.succeeded`. Sets each `seat:{eventId}:{seatId}` key to `"SOLD"` **with no TTL** (persisting the key permanently so `SOLD` keys never expire in Redis). Confirm only runs after PostgreSQL has committed `SOLD`, so Redis follows PostgreSQL even if the key is missing, expired or holds another reservation's temporary lock, and a later release by that other reservation cannot delete the `SOLD` marker because the token does not match.
- **`KEYS[1 .. N]`**: `seat:{eventId}:{seatId_1}` .. `seat:{eventId}:{seatId_N}`

```lua
-- confirm_inventory.lua
for i = 1, #KEYS do
    redis.call('SET', KEYS[i], 'SOLD')
end

return 'CONFIRMED'
```

---

#### 9.2.4 `token_bucket_ratelimit.lua` (`gateway-service` Rate Limiting)
- **`KEYS[1]`**: `ratelimit:user:{userId}` or `ratelimit:ip:{ipAddress}`
- **`ARGV[1]`**: `capacity` (`10` for authenticated user, `20` for IP)
- **`ARGV[2]`**: `refillRatePerSec` (`5` for authenticated user, `10` for IP)
- **`ARGV[3]`**: `nowEpochMs`

```lua
-- token_bucket_ratelimit.lua
local capacity = tonumber(ARGV[1])
local refill_rate = tonumber(ARGV[2])
local now_ms = tonumber(ARGV[3])

local bucket = redis.call('HMGET', KEYS[1], 'tokens', 'last_refill_ms')
local tokens = tonumber(bucket[1])
local last_refill_ms = tonumber(bucket[2])

if tokens == nil then
    tokens = capacity
    last_refill_ms = now_ms
else
    local elapsed_sec = math.max(0, (now_ms - last_refill_ms) / 1000.0)
    tokens = math.min(capacity, tokens + (elapsed_sec * refill_rate))
    last_refill_ms = now_ms
end

if tokens < 1.0 then
    redis.call('HMSET', KEYS[1], 'tokens', tokens, 'last_refill_ms', last_refill_ms)
    redis.call('EXPIRE', KEYS[1], 60)
    return 0
end

tokens = tokens - 1.0
redis.call('HMSET', KEYS[1], 'tokens', tokens, 'last_refill_ms', last_refill_ms)
redis.call('EXPIRE', KEYS[1], 60)
return 1
```

---

### 9.3 Interaction Between Redis 7 and the PostgreSQL 16 Source of Truth

#### 1. Why Redis Seat TTL Is `660s` While PostgreSQL `expires_at` Is `600s`
If `seat:{eventId}:{seatId}` in Redis had a 600-second TTL matching PostgreSQL `expires_at`, slight clock skew or a brief delay in the PostgreSQL expiry sweeper would allow Redis to evict `seat:{eventId}:{seatId}` at `t = 600.000s` while PostgreSQL still had the seat in `HELD` status at `t = 600.200s`. A new buyer would pass the Redis Lua gate and then hit a PostgreSQL constraint failure (or worse, race with a late payment confirmation).

By setting **`seat:{eventId}:{seatId}` TTL to `660` seconds** as a safety net and **`inventory_db.reservations.expires_at` to `600` seconds**, Redis **never** releases a seat lock before PostgreSQL has authoritatively transitioned the reservation to `EXPIRED`.

#### 2. PostgreSQL-Authoritative Hold Expiry Sequence (`1000 ms` PostgreSQL Sweeper Only)
There is no Redis keyspace expiry listener and no `reservation:ttl` shadow timer key. Hold expiration is triggered **exclusively by the PostgreSQL sweeper** running every `1000 ms` (`@Scheduled(fixedDelayString = "${HOLD_EXPIRY_SWEEP_INTERVAL_MS:1000}")`) in `inventory-service`:
- **Sweeper Batch Selection (`FOR UPDATE SKIP LOCKED`)**:
  ```sql
  SELECT reservation_id
  FROM reservations
  WHERE status = 'HELD'
    AND expires_at <= NOW()
  ORDER BY expires_at ASC
  LIMIT 500
  FOR UPDATE SKIP LOCKED;
  ```
- **Mandatory Execution Order (PostgreSQL First, `release_inventory.lua` Second)**:
  1. **Step A — Update PostgreSQL First (System of Record)**:
     Inside the `inventory_db` transaction for each expired reservation:
     - Lock and verify the reservation: `SELECT reservation_id, event_id, user_id, status FROM reservations WHERE reservation_id = :reservationId AND status = 'HELD' AND expires_at <= NOW() FOR UPDATE`.
     - Update `reservations SET status = 'EXPIRED', expired_at = NOW(), updated_at = NOW() WHERE reservation_id = :reservationId`.
     - Update `reservation_items SET status = 'EXPIRED', updated_at = NOW() WHERE reservation_id = :reservationId RETURNING seat_id`.
     - Update `seats SET status = 'AVAILABLE', version = version + 1, updated_at = NOW() WHERE event_id = :eventId AND seat_id = ANY(:seatIds) AND status = 'HELD'`.
     - Insert `inventory.reservation.expired` into `outbox_events`.
     - Commit the PostgreSQL transaction.
  2. **Step B — Release Redis Lock Second (`release_inventory.lua`)**:
     Immediately after the PostgreSQL transaction commits (via `TransactionSynchronization.afterCommit`), `inventory-service` invokes `release_inventory.lua` with `reservationId` and all `seat:{eventId}:{seatId}` keys, freeing the seats in Redis ~59 seconds before their 660-second safety-net TTL would have expired.

#### 3. Cache Rewarm After a Redis Flush or Restart (`EVENT_NOT_CACHED`)
If `hold_inventory.lua` returns `"EVENT_NOT_CACHED"` (or when the startup/on-demand rewarm job runs after a Redis flush or restart), `inventory-service` reconstructs Redis state for `eventId` directly from `inventory_db` before retrying `hold_inventory.lua` once:
1. Queries `SELECT drop_time FROM events WHERE event_id = :eventId` and sets `SET event:{eventId}:drop_ts <epochMs> EX 604800`.
2. Restores **active `HELD` seat locks** by querying active held seats joined with their reservation `expires_at` (`SELECT ri.seat_id, r.reservation_id, GREATEST(1, FLOOR(EXTRACT(EPOCH FROM (r.expires_at - NOW()))))::bigint AS remaining_sec FROM reservation_items ri JOIN reservations r ON r.reservation_id = ri.reservation_id WHERE ri.event_id = :eventId AND r.status = 'HELD' AND r.expires_at > NOW()`) and setting each `seat:{eventId}:{seatId}` to `reservationId` (`NX`) with a **TTL equal to seconds remaining until `expires_at` plus `60`** (`EX (remaining_sec + 60)`).
3. Restores **`SOLD` seats** by querying `SELECT seat_id FROM seats WHERE event_id = :eventId AND status = 'SOLD'` and setting each `seat:{eventId}:{seatId}` to `"SOLD"` **with no TTL** (`SET seat:{eventId}:{seatId} "SOLD"`), so `SOLD` keys never expire.

