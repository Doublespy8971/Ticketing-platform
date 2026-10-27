## 7. REST API Contracts per Service

### 7.1 Authentication & Authorization Architecture (`gateway-service`)

#### JWT Verification Contract
- **Algorithm**: `HS256` (HMAC with SHA-256).
- **Signing Secret**: Configured via environment variable `JWT_SECRET` (minimum 32-byte / 256-bit secret shared only by `gateway-service`).
- **Required JWT Claims**:
  - `sub` (`string`, UUID): Authenticated user ID (`"a1b2c3d4-e5f6-4a5b-8c9d-0e1f2a3b4c5d"`).
  - `roles` (`array` of `string`): Granted role identifiers (`["USER"]` or `["ADMIN"]`).
  - `iat` (`integer`, epoch seconds): Token issuance timestamp.
  - `exp` (`integer`, epoch seconds): Token expiration timestamp.
- **Gateway Header Propagation**:
  - On every incoming request, `gateway-service` strips any client-supplied `X-User-Id` and `X-User-Roles` headers.
  - After verifying the `Authorization: Bearer <token>` header, `gateway-service` injects:
    - `X-User-Id: <sub claim>`
    - `X-User-Roles: <comma-joined roles claim>` (`"USER"` or `"ADMIN,USER"`)
    - `X-Correlation-Id: <incoming UUID or newly generated UUIDv4>`
- **Route Access Matrix**:
  - `POST /api/v1/auth/token`: Public in `dev` / `local` profiles only (disabled in `prod`).
  - `GET /api/v1/events/{eventId}/seats`: Public (no JWT required; rate-limited by client IP).
  - `POST /api/v1/reservations`, `POST /api/v1/orders/{orderId}/checkout`, `GET /api/v1/orders/{orderId}`: Requires valid JWT with `USER` or `ADMIN` in `roles`.
  - `POST /api/v1/admin/**`: Requires valid JWT with `ADMIN` in `roles` (enforced both at `gateway-service` and by `inventory-service` checking `X-User-Roles`).

---

#### 7.1.1 Dev-Only Token Issuance Endpoint (`gateway-service` : `8080`)
- **Method & Path**: `POST /api/v1/auth/token`
- **Profile Guard**: `@Profile({"dev", "local"})` — completely unregistered when `SPRING_PROFILES_ACTIVE=prod`.
- **Request Headers**: `Content-Type: application/json`
- **Request Body**:
```json
{
  "userId": "a1b2c3d4-e5f6-4a5b-8c9d-0e1f2a3b4c5d",
  "roles": ["USER"],
  "ttlSeconds": 3600
}
```
- **Response `200 OK` Body**:
```json
{
  "accessToken": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJhMWIyYzNkNC1lNWY2LTRhNWItOGM5ZC0wZTFmMmEzYjRjNWQiLCJyb2xlcyI6WyJVU0VSIl0sImlhdCI6MTc5MzE0ODAwMCwiZXhwIjoxNzkzMTUxNjAwfQ.signature",
  "tokenType": "Bearer",
  "expiresIn": 3600,
  "issuedAt": "2026-10-27T22:00:00Z"
}
```
- **Status Codes**:
  - `200 OK`: Token minted.
  - `400 Bad Request`: Invalid `userId` UUID, empty `roles` array, or `ttlSeconds` outside `60..86400`.

---

### 7.2 Standard Error Response Format (RFC 7807 Problem Details)

All `4xx` and `5xx` responses across `gateway-service`, `inventory-service`, and `order-service` return `Content-Type: application/problem+json` with this exact structure:

```json
{
  "type": "https://api.ticketing.internal/errors/order-not-ready",
  "title": "Order Not Ready",
  "status": 409,
  "detail": "Order 01926b54-1111-7000-8000-000000000001 is still being materialized from reservation 01926b54-1111-7000-8000-000000000001. Retry after 1 second.",
  "instance": "/api/v1/orders/01926b54-1111-7000-8000-000000000001/checkout",
  "errorCode": "ORDER_NOT_READY",
  "correlationId": "c9b1d8e0-2f4a-4b6c-9d1e-3f5a7b9c0d2e",
  "timestamp": "2026-10-27T22:00:00.050000Z",
  "errors": []
}
```

#### Canonical `errorCode` Enum Table
| `errorCode` | HTTP Status | Service | Meaning |
| :--- | :--- | :--- | :--- |
| `VALIDATION_ERROR` | `400` | All | Request body or query parameter failed Bean Validation. |
| `MISSING_IDEMPOTENCY_KEY` | `400` | `inventory-service`, `order-service` | Missing or malformed `Idempotency-Key` UUID header. |
| `UNAUTHORIZED` | `401` | `gateway-service` | Missing, expired, or invalid Bearer JWT. |
| `FORBIDDEN` | `403` | `gateway-service`, `inventory-service`, `order-service` | Caller lacks required `ADMIN` role or does not own the target resource. |
| `RESOURCE_NOT_FOUND` | `404` | `inventory-service`, `order-service` | Event or seat ID does not exist. |
| `EVENT_NOT_OPEN` | `409` | `inventory-service` | Current server time is earlier than the event's `drop_time`. |
| `SEAT_ALREADY_TAKEN` | `409` | `inventory-service` | One or more requested seats are currently `HELD` or `SOLD`. |
| `ORDER_NOT_READY` | `409` | `order-service` | Checkout called before `inventory.seat.reserved` has been consumed (`Retry-After: 1`). |
| `RESERVATION_EXPIRED` | `409` | `order-service` | Order is expired or has fewer than 15 seconds remaining until `expires_at`. |
| `INVALID_ORDER_STATE` | `409` | `order-service` | Order is already in `PAYMENT_PENDING`, `CONFIRMED`, or a `CANCELLED_*` state. |
| `IDEMPOTENCY_REQUEST_IN_PROGRESS` | `409` | `inventory-service`, `order-service` | Concurrent request with the same `Idempotency-Key` is currently executing (`Retry-After: 1`). |
| `IDEMPOTENCY_KEY_PAYLOAD_MISMATCH` | `422` | `inventory-service`, `order-service` | Same `Idempotency-Key` reused with a different request payload. |
| `RATE_LIMIT_EXCEEDED` | `429` | `gateway-service` | Token-bucket rate limit exceeded (`Retry-After: 1`). |
| `INTERNAL_SERVER_ERROR` | `500` | All | Unexpected server error. |

---

### 7.3 `inventory-service` Admin Endpoints (`:8081`, Routed via `:8080`)

#### 7.3.1 Create Event (`ADMIN` Role Only)
- **Method & Path**: `POST /api/v1/admin/events`
- **Required Headers**: `Authorization: Bearer <admin-jwt>`, `Content-Type: application/json`
- **Request Body**:
```json
{
  "name": "Taylor Swift — The Eras Tour Finale",
  "venueName": "MetLife Stadium",
  "eventTime": "2026-12-15T20:00:00Z",
  "dropTime": "2026-11-01T16:00:00Z"
}
```
- **Side Effects**:
  1. Inserts a row into `inventory_db.events` with `status = 'SCHEDULED'`.
  2. Sets Redis key `event:{eventId}:drop_ts` to the epoch millisecond value of `dropTime` (`1793548800000`) with a 7-day TTL (`EX 604800`, refreshed by `inventory-service` while the event is active).
- **Response `201 Created` Body**:
```json
{
  "eventId": "f0e1d2c3-b4a5-4968-8796-a5b4c3d2e1f0",
  "name": "Taylor Swift — The Eras Tour Finale",
  "venueName": "MetLife Stadium",
  "eventTime": "2026-12-15T20:00:00Z",
  "dropTime": "2026-11-01T16:00:00Z",
  "status": "SCHEDULED",
  "createdAt": "2026-10-27T22:00:00Z"
}
```
- **Status Codes**: `201 Created`, `400 Bad Request` (`dropTime >= eventTime`), `401 Unauthorized`, `403 Forbidden`.

---

#### 7.3.2 Bulk-Create Assigned Seats (`ADMIN` Role Only)
- **Method & Path**: `POST /api/v1/admin/events/{eventId}/seats/bulk`
- **Required Headers**: `Authorization: Bearer <admin-jwt>`, `Content-Type: application/json`
- **Request Body** (1 to 5,000 seats per request):
```json
{
  "seats": [
    {
      "sectionCode": "FLOOR-A",
      "rowLabel": "1",
      "seatNumber": "101",
      "priceCents": 15000
    },
    {
      "sectionCode": "FLOOR-A",
      "rowLabel": "1",
      "seatNumber": "102",
      "priceCents": 15000
    }
  ]
}
```
- **Side Effects**:
  - Executes batch `INSERT INTO seats (seat_id, event_id, section_code, row_label, seat_number, price_cents, status, version) VALUES ... ON CONFLICT (event_id, section_code, row_label, seat_number) DO NOTHING` in `inventory_db`.
- **Response `201 Created` Body**:
```json
{
  "eventId": "f0e1d2c3-b4a5-4968-8796-a5b4c3d2e1f0",
  "createdCount": 2,
  "skippedDuplicateCount": 0,
  "seats": [
    {
      "seatId": "11111111-2222-4333-8444-555555555501",
      "sectionCode": "FLOOR-A",
      "rowLabel": "1",
      "seatNumber": "101",
      "priceCents": 15000,
      "status": "AVAILABLE"
    },
    {
      "seatId": "11111111-2222-4333-8444-555555555502",
      "sectionCode": "FLOOR-A",
      "rowLabel": "1",
      "seatNumber": "102",
      "priceCents": 15000,
      "status": "AVAILABLE"
    }
  ]
}
```
- **Status Codes**: `201 Created`, `400 Bad Request`, `401 Unauthorized`, `403 Forbidden`, `404 Not Found` (`eventId` does not exist).

---

### 7.4 `inventory-service` Customer Endpoints (`:8081`, Routed via `:8080`)

#### 7.4.1 List Event Seats
- **Method & Path**: `GET /api/v1/events/{eventId}/seats?sectionCode=FLOOR-A&status=AVAILABLE`
- **Query Parameters**:
  - `sectionCode` (optional `string`): Filter by venue section.
  - `status` (optional `string`, enum: `AVAILABLE`, `HELD`, `SOLD`): Filter by seat status.
- **Response `200 OK` Body**:
```json
{
  "eventId": "f0e1d2c3-b4a5-4968-8796-a5b4c3d2e1f0",
  "dropTime": "2026-10-27T21:00:00Z",
  "seats": [
    {
      "seatId": "11111111-2222-4333-8444-555555555501",
      "sectionCode": "FLOOR-A",
      "rowLabel": "1",
      "seatNumber": "101",
      "priceCents": 15000,
      "currency": "USD",
      "status": "AVAILABLE"
    }
  ]
}
```
- **Status Codes**: `200 OK`, `404 Not Found`.

---

#### 7.4.2 Create Seat Hold (Step 1 of Saga)
- **Method & Path**: `POST /api/v1/reservations`
- **Required Headers**:
  - `Authorization: Bearer <user-jwt>`
  - `Idempotency-Key: <UUID>`
  - `Content-Type: application/json`
- **Request Body**:
```json
{
  "eventId": "f0e1d2c3-b4a5-4968-8796-a5b4c3d2e1f0",
  "seatIds": [
    "11111111-2222-4333-8444-555555555501",
    "11111111-2222-4333-8444-555555555502"
  ]
}
```
- **Validation Rules**:
  - `seatIds` must be non-empty, contain between `1` and `6` distinct UUIDs.
- **Response `201 Created` Body**:
```json
{
  "reservationId": "01926b54-1111-7000-8000-000000000001",
  "orderId": "01926b54-1111-7000-8000-000000000001",
  "eventId": "f0e1d2c3-b4a5-4968-8796-a5b4c3d2e1f0",
  "userId": "a1b2c3d4-e5f6-4a5b-8c9d-0e1f2a3b4c5d",
  "status": "HELD",
  "seats": [
    {
      "seatId": "11111111-2222-4333-8444-555555555501",
      "sectionCode": "FLOOR-A",
      "rowLabel": "1",
      "seatNumber": "101",
      "priceCents": 15000
    },
    {
      "seatId": "11111111-2222-4333-8444-555555555502",
      "sectionCode": "FLOOR-A",
      "rowLabel": "1",
      "seatNumber": "102",
      "priceCents": 15000
    }
  ],
  "totalAmountCents": 30000,
  "currency": "USD",
  "reservedAt": "2026-10-27T22:00:00.000000Z",
  "expiresAt": "2026-10-27T22:10:00.000000Z"
}
```
- **Status Codes**:
  - `201 Created`: Seat hold committed in Redis (`660s` seat lock TTL) and PostgreSQL (`expires_at = reservedAt + 600s`).
  - `400 Bad Request`: Invalid `seatIds` count (`< 1` or `> 6`) or missing `Idempotency-Key`.
  - `401 Unauthorized`: Invalid or missing JWT.
  - `404 Not Found`: `eventId` or any `seatId` does not exist in `inventory_db`.
  - `409 Conflict` (`EVENT_NOT_OPEN`): `hold_inventory.lua` returned `"EVENT_NOT_OPEN"` because current time is before `drop_time`.
  - `409 Conflict` (`SEAT_ALREADY_TAKEN`): `hold_inventory.lua` returned `"SEAT_TAKEN"` (or PostgreSQL optimistic/unique constraint failed) because one or more requested seats are already `HELD` or `SOLD`.
  - `422 Unprocessable Entity` (`IDEMPOTENCY_KEY_PAYLOAD_MISMATCH`): Reused `Idempotency-Key` with different payload.
- **Mapping `hold_inventory.lua` Plain-String Return Values to HTTP Responses**:
  - `"HELD"` -> Proceed to execute the synchronous PostgreSQL transaction on `inventory_db` (`seats`, `reservations`, `reservation_items`, `outbox_events`) and return `201 Created`.
  - `"EVENT_NOT_CACHED"` -> Query `inventory_db` for the event and active seats (`HELD` and `SOLD`), warm Redis (`event:{eventId}:drop_ts`, `HELD` seat locks with TTL `= max(1, seconds_until_expires_at) + 60`, and `SOLD` seat locks with no TTL), and retry `hold_inventory.lua` once; if the event row does not exist in `inventory_db`, return `404 Not Found` (`RESOURCE_NOT_FOUND`).
  - `"EVENT_NOT_OPEN"` -> Return `409 Conflict` with RFC 7807 `errorCode: "EVENT_NOT_OPEN"` without opening a PostgreSQL transaction on `reservations`.
  - `"SEAT_TAKEN"` -> Return `409 Conflict` with RFC 7807 `errorCode: "SEAT_ALREADY_TAKEN"` without opening a PostgreSQL transaction on `reservations`.

---

### 7.5 `order-service` Customer Endpoints (`:8082`, Routed via `:8080`)

#### 7.5.1 Checkout Order (Step 2 of Saga)
- **Method & Path**: `POST /api/v1/orders/{orderId}/checkout` (where `orderId` is equal to `reservationId`)
- **Required Headers**:
  - `Authorization: Bearer <user-jwt>`
  - `Idempotency-Key: <UUID>`
  - `Content-Type: application/json`
- **Request Body**:
```json
{
  "paymentMethodToken": "pm_tok_visa_success"
}
```
- **Response `202 Accepted` Body**:
```json
{
  "orderId": "01926b54-1111-7000-8000-000000000001",
  "reservationId": "01926b54-1111-7000-8000-000000000001",
  "userId": "a1b2c3d4-e5f6-4a5b-8c9d-0e1f2a3b4c5d",
  "eventId": "f0e1d2c3-b4a5-4968-8796-a5b4c3d2e1f0",
  "status": "PAYMENT_PENDING",
  "totalAmountCents": 30000,
  "currency": "USD",
  "checkoutAt": "2026-10-27T22:01:02.000000Z",
  "expiresAt": "2026-10-27T22:10:00.000000Z"
}
```
- **Status Codes**:
  - `202 Accepted`: Order transitioned from `RESERVED` to `PAYMENT_PENDING` and `order.payment.requested` written to `outbox_events`.
  - `400 Bad Request`: Missing `Idempotency-Key` or blank `paymentMethodToken`.
  - `401 Unauthorized`: Invalid or missing JWT.
  - `403 Forbidden`: `orders.user_id` does not match `X-User-Id`.
  - `409 Conflict` (`ORDER_NOT_READY`, with HTTP header `Retry-After: 1`): No row exists in `order_db.orders` for `orderId` because `inventory.seat.reserved` has not yet been consumed.
  - `409 Conflict` (`RESERVATION_EXPIRED`): `orders.status == 'CANCELLED_EXPIRED'` or `expires_at - NOW() < INTERVAL '15 seconds'`.
  - `409 Conflict` (`INVALID_ORDER_STATE`): Order is already in `PAYMENT_PENDING`, `CONFIRMED`, `CANCELLED_PAYMENT_FAILED`, `REFUND_PENDING`, or `CANCELLED_REFUNDED` with a different `Idempotency-Key`.

---

#### 7.5.2 Get Order Status
- **Method & Path**: `GET /api/v1/orders/{orderId}`
- **Required Headers**: `Authorization: Bearer <user-jwt>`
- **Response `200 OK` Body**:
```json
{
  "orderId": "01926b54-1111-7000-8000-000000000001",
  "reservationId": "01926b54-1111-7000-8000-000000000001",
  "userId": "a1b2c3d4-e5f6-4a5b-8c9d-0e1f2a3b4c5d",
  "eventId": "f0e1d2c3-b4a5-4968-8796-a5b4c3d2e1f0",
  "status": "CONFIRMED",
  "paymentId": "01926b55-9999-7000-8000-000000000099",
  "seats": [
    {
      "seatId": "11111111-2222-4333-8444-555555555501",
      "sectionCode": "FLOOR-A",
      "rowLabel": "1",
      "seatNumber": "101",
      "priceCents": 15000
    },
    {
      "seatId": "11111111-2222-4333-8444-555555555502",
      "sectionCode": "FLOOR-A",
      "rowLabel": "1",
      "seatNumber": "102",
      "priceCents": 15000
    }
  ],
  "totalAmountCents": 30000,
  "currency": "USD",
  "reservedAt": "2026-10-27T22:00:00.000000Z",
  "expiresAt": "2026-10-27T22:10:00.000000Z",
  "checkoutAt": "2026-10-27T22:01:02.000000Z",
  "confirmedAt": "2026-10-27T22:01:05.650000Z",
  "cancelledAt": null,
  "cancellationReason": null
}
```
- **Status Codes**:
  - `200 OK`: Order returned.
  - `403 Forbidden`: `orders.user_id` does not match `X-User-Id`.
  - `404 Not Found`: Order does not exist.

