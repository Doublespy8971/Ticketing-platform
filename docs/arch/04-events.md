## 4. Event Contracts

### 4.1 Common Event Envelope
Every Kafka record value across `inventory.events`, `order.events`, and `payment.events` is serialized as UTF-8 JSON conforming to this top-level envelope:

| Field | JSON Type | Format / Constraints | Description |
| :--- | :--- | :--- | :--- |
| `eventId` | `string` | UUIDv7 (`36` chars) | Globally unique identifier for this event instance (used as primary key in `outbox_events` and `processed_events`). |
| `eventType` | `string` | Lowercase dot-separated | Exact event type discriminator (`inventory.seat.reserved`, `order.payment.requested`, and so on). |
| `occurredAt` | `string` | ISO-8601 UTC (`YYYY-MM-DDTHH:mm:ss.SSSSSSZ`) | Timestamp when the event was created inside the producer's database transaction. |
| `correlationId` | `string` | UUID (`36` chars) | End-to-end trace correlation ID propagated from the originating HTTP `X-Correlation-Id` header or background job. |
| `schemaVersion` | `integer` | Positive integer (`1`) | Version of the `payload` schema for forward/backward compatibility. |
| `payload` | `object` | Non-null JSON object | Event-specific payload defined in Section 4.2. |

#### Canonical Envelope Example
```json
{
  "eventId": "01926b54-7c8a-7000-8000-1a2b3c4d5e6f",
  "eventType": "inventory.seat.reserved",
  "occurredAt": "2026-10-27T22:00:00.123456Z",
  "correlationId": "c9b1d8e0-2f4a-4b6c-9d1e-3f5a7b9c0d2e",
  "schemaVersion": 1,
  "payload": {}
}
```

#### Mandatory Kafka Record Headers
Every published Kafka message must include these UTF-8 string headers alongside the JSON body:
- `X-Correlation-Id`: Matches `correlationId` in the envelope.
- `event-type`: Matches `eventType` in the envelope (enables fast filtering by consumers without parsing the JSON body when ignoring unrelated events on shared topics).
- `event-id`: Matches `eventId` in the envelope.
- `schema-version`: `"1"`.

---

### 4.2 Event Payload Specifications (All 12 Events)

All monetary amounts in event payloads are represented in **integer minor currency units (`priceCents` / `totalAmountCents` as `integer` / `int64`)** along with a 3-letter ISO-4217 `currency` string (`"USD"`) to prevent floating-point rounding errors. All inventory items represent discrete **`ASSIGNED`** seats.

---

#### 4.2.1 `inventory.seat.reserved`
- **Topic**: `inventory.events` | **Partition Key**: `reservationId` | **Producer**: `inventory-service` | **Consumer**: `order-service`
- **Payload Fields**:
  - `reservationId` (`string`, UUID): Unique reservation identifier (used by `order-service` as `orderId`).
  - `orderId` (`string`, UUID): Identical to `reservationId`.
  - `userId` (`string`, UUID): Authenticated user who acquired the hold.
  - `eventId` (`string`, UUID): Target event identifier.
  - `seats` (`array` of `object`, min 1, max 6):
    - `seatId` (`string`, UUID): Seat primary key.
    - `sectionCode` (`string`): Venue section code (`"SEC-A"`).
    - `rowLabel` (`string`): Row identifier (`"12"`).
    - `seatNumber` (`string`): Seat number within the row (`"14"`).
    - `priceCents` (`integer`, int64): Price of this seat in minor units (`15000`).
  - `totalAmountCents` (`integer`, int64): Sum of `priceCents` across all reserved seats (`30000`).
  - `currency` (`string`, ISO-4217): `"USD"`.
  - `reservedAt` (`string`, ISO-8601 UTC): Hold creation timestamp.
  - `expiresAt` (`string`, ISO-8601 UTC): Authoritative PostgreSQL hold expiration timestamp (`reservedAt + 600s`).

```json
{
  "reservationId": "01926b54-1111-7000-8000-000000000001",
  "orderId": "01926b54-1111-7000-8000-000000000001",
  "userId": "a1b2c3d4-e5f6-4a5b-8c9d-0e1f2a3b4c5d",
  "eventId": "f0e1d2c3-b4a5-4968-8796-a5b4c3d2e1f0",
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

---

#### 4.2.2 `inventory.seat.released`
- **Topic**: `inventory.events` | **Partition Key**: `reservationId` | **Producer**: `inventory-service` | **Consumer**: `order-service`
- **Payload Fields**:
  - `reservationId` (`string`, UUID)
  - `orderId` (`string`, UUID): Identical to `reservationId`.
  - `eventId` (`string`, UUID)
  - `seatIds` (`array` of `string` UUIDs)
  - `reason` (`string`, enum: `"PAYMENT_FAILED"`)
  - `releasedAt` (`string`, ISO-8601 UTC)

```json
{
  "reservationId": "01926b54-1111-7000-8000-000000000001",
  "orderId": "01926b54-1111-7000-8000-000000000001",
  "eventId": "f0e1d2c3-b4a5-4968-8796-a5b4c3d2e1f0",
  "seatIds": [
    "11111111-2222-4333-8444-555555555501",
    "11111111-2222-4333-8444-555555555502"
  ],
  "reason": "PAYMENT_FAILED",
  "releasedAt": "2026-10-27T22:01:15.200000Z"
}
```

---

#### 4.2.3 `inventory.reservation.expired`
- **Topic**: `inventory.events` | **Partition Key**: `reservationId` | **Producer**: `inventory-service` | **Consumer**: `order-service`
- **Payload Fields**:
  - `reservationId` (`string`, UUID)
  - `orderId` (`string`, UUID): Identical to `reservationId`.
  - `userId` (`string`, UUID)
  - `eventId` (`string`, UUID)
  - `seatIds` (`array` of `string` UUIDs)
  - `expiredAt` (`string`, ISO-8601 UTC)

```json
{
  "reservationId": "01926b54-1111-7000-8000-000000000001",
  "orderId": "01926b54-1111-7000-8000-000000000001",
  "userId": "a1b2c3d4-e5f6-4a5b-8c9d-0e1f2a3b4c5d",
  "eventId": "f0e1d2c3-b4a5-4968-8796-a5b4c3d2e1f0",
  "seatIds": [
    "11111111-2222-4333-8444-555555555501",
    "11111111-2222-4333-8444-555555555502"
  ],
  "expiredAt": "2026-10-27T22:10:00.050000Z"
}
```

---

#### 4.2.4 `inventory.seat.confirmed`
- **Topic**: `inventory.events` | **Partition Key**: `reservationId` | **Producer**: `inventory-service` | **Consumer**: `order-service`
- **Payload Fields**:
  - `reservationId` (`string`, UUID)
  - `orderId` (`string`, UUID): Identical to `reservationId`.
  - `paymentId` (`string`, UUID)
  - `eventId` (`string`, UUID)
  - `seatIds` (`array` of `string` UUIDs)
  - `confirmedAt` (`string`, ISO-8601 UTC)

```json
{
  "reservationId": "01926b54-1111-7000-8000-000000000001",
  "orderId": "01926b54-1111-7000-8000-000000000001",
  "paymentId": "01926b55-9999-7000-8000-000000000099",
  "eventId": "f0e1d2c3-b4a5-4968-8796-a5b4c3d2e1f0",
  "seatIds": [
    "11111111-2222-4333-8444-555555555501",
    "11111111-2222-4333-8444-555555555502"
  ],
  "confirmedAt": "2026-10-27T22:01:05.450000Z"
}
```

---

#### 4.2.5 `inventory.seat.confirmation_rejected`
- **Topic**: `inventory.events` | **Partition Key**: `reservationId` | **Producer**: `inventory-service` | **Consumer**: `order-service`
- **Payload Fields**:
  - `reservationId` (`string`, UUID)
  - `orderId` (`string`, UUID): Identical to `reservationId`.
  - `paymentId` (`string`, UUID)
  - `userId` (`string`, UUID)
  - `eventId` (`string`, UUID)
  - `reason` (`string`, enum: `"RESERVATION_ALREADY_EXPIRED"` | `"RESERVATION_ALREADY_RELEASED"`)
  - `rejectedAt` (`string`, ISO-8601 UTC)

```json
{
  "reservationId": "01926b54-1111-7000-8000-000000000001",
  "orderId": "01926b54-1111-7000-8000-000000000001",
  "paymentId": "01926b55-9999-7000-8000-000000000099",
  "userId": "a1b2c3d4-e5f6-4a5b-8c9d-0e1f2a3b4c5d",
  "eventId": "f0e1d2c3-b4a5-4968-8796-a5b4c3d2e1f0",
  "reason": "RESERVATION_ALREADY_EXPIRED",
  "rejectedAt": "2026-10-27T22:10:01.300000Z"
}
```

---

#### 4.2.6 `order.payment.requested`
- **Topic**: `order.events` | **Partition Key**: `orderId` | **Producer**: `order-service` | **Consumer**: `payment-service` (`notification-service` ignores via `event-type` header)
- **Payload Fields**:
  - `orderId` (`string`, UUID): Identical to `reservationId`.
  - `reservationId` (`string`, UUID): Identical to `orderId`.
  - `userId` (`string`, UUID)
  - `eventId` (`string`, UUID)
  - `amountCents` (`integer`, int64)
  - `currency` (`string`, ISO-4217): `"USD"`
  - `paymentMethodToken` (`string`): Token representing the payment instrument (`"pm_tok_visa_success"`, `"pm_tok_declined"`, `"pm_tok_timeout"`).
  - `requestedAt` (`string`, ISO-8601 UTC)
  - `reservationExpiresAt` (`string`, ISO-8601 UTC)

```json
{
  "orderId": "01926b54-1111-7000-8000-000000000001",
  "reservationId": "01926b54-1111-7000-8000-000000000001",
  "userId": "a1b2c3d4-e5f6-4a5b-8c9d-0e1f2a3b4c5d",
  "eventId": "f0e1d2c3-b4a5-4968-8796-a5b4c3d2e1f0",
  "amountCents": 30000,
  "currency": "USD",
  "paymentMethodToken": "pm_tok_visa_success",
  "requestedAt": "2026-10-27T22:01:02.000000Z",
  "reservationExpiresAt": "2026-10-27T22:10:00.000000Z"
}
```

---

#### 4.2.7 `order.refund.requested`
- **Topic**: `order.events` | **Partition Key**: `orderId` | **Producer**: `order-service` | **Consumer**: `payment-service`
- **Payload Fields**:
  - `orderId` (`string`, UUID)
  - `reservationId` (`string`, UUID)
  - `paymentId` (`string`, UUID)
  - `userId` (`string`, UUID)
  - `amountCents` (`integer`, int64)
  - `currency` (`string`, ISO-4217): `"USD"`
  - `reason` (`string`, enum: `"LATE_PAYMENT_EXPIRED_HOLD"`)
  - `requestedAt` (`string`, ISO-8601 UTC)

```json
{
  "orderId": "01926b54-1111-7000-8000-000000000001",
  "reservationId": "01926b54-1111-7000-8000-000000000001",
  "paymentId": "01926b55-9999-7000-8000-000000000099",
  "userId": "a1b2c3d4-e5f6-4a5b-8c9d-0e1f2a3b4c5d",
  "amountCents": 30000,
  "currency": "USD",
  "reason": "LATE_PAYMENT_EXPIRED_HOLD",
  "requestedAt": "2026-10-27T22:10:01.500000Z"
}
```

---

#### 4.2.8 `order.confirmed`
- **Topic**: `order.events` | **Partition Key**: `orderId` | **Producer**: `order-service` | **Consumer**: `notification-service`
- **Payload Fields**:
  - `orderId` (`string`, UUID)
  - `reservationId` (`string`, UUID)
  - `userId` (`string`, UUID)
  - `eventId` (`string`, UUID)
  - `paymentId` (`string`, UUID)
  - `seats` (`array` of `object` with `seatId`, `sectionCode`, `rowLabel`, `seatNumber`, `priceCents`)
  - `totalAmountCents` (`integer`, int64)
  - `currency` (`string`, ISO-4217): `"USD"`
  - `confirmedAt` (`string`, ISO-8601 UTC)

```json
{
  "orderId": "01926b54-1111-7000-8000-000000000001",
  "reservationId": "01926b54-1111-7000-8000-000000000001",
  "userId": "a1b2c3d4-e5f6-4a5b-8c9d-0e1f2a3b4c5d",
  "eventId": "f0e1d2c3-b4a5-4968-8796-a5b4c3d2e1f0",
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
  "confirmedAt": "2026-10-27T22:01:05.650000Z"
}
```

---

#### 4.2.9 `order.cancelled`
- **Topic**: `order.events` | **Partition Key**: `orderId` | **Producer**: `order-service` | **Consumer**: `notification-service`
- **Payload Fields**:
  - `orderId` (`string`, UUID)
  - `reservationId` (`string`, UUID)
  - `userId` (`string`, UUID)
  - `eventId` (`string`, UUID)
  - `finalStatus` (`string`, enum: `"CANCELLED_PAYMENT_FAILED"` | `"CANCELLED_EXPIRED"` | `"CANCELLED_REFUNDED"`)
  - `cancellationReason` (`string`, enum: `"PAYMENT_FAILED"` | `"RESERVATION_EXPIRED"` | `"LATE_PAYMENT_EXPIRED_HOLD"`)
  - `cancelledAt` (`string`, ISO-8601 UTC)

```json
{
  "orderId": "01926b54-1111-7000-8000-000000000001",
  "reservationId": "01926b54-1111-7000-8000-000000000001",
  "userId": "a1b2c3d4-e5f6-4a5b-8c9d-0e1f2a3b4c5d",
  "eventId": "f0e1d2c3-b4a5-4968-8796-a5b4c3d2e1f0",
  "finalStatus": "CANCELLED_PAYMENT_FAILED",
  "cancellationReason": "PAYMENT_FAILED",
  "cancelledAt": "2026-10-27T22:01:15.350000Z"
}
```

---

#### 4.2.10 `payment.succeeded`
- **Topic**: `payment.events` | **Partition Key**: `orderId` | **Producer**: `payment-service` | **Consumers**: `inventory-service`, `order-service`
- **Payload Fields**:
  - `paymentId` (`string`, UUID)
  - `orderId` (`string`, UUID): Identical to `reservationId`.
  - `reservationId` (`string`, UUID): Identical to `orderId`.
  - `userId` (`string`, UUID)
  - `amountCents` (`integer`, int64)
  - `currency` (`string`, ISO-4217): `"USD"`
  - `providerReference` (`string`): Mock provider transaction ID (`"mock_ch_987654321"`).
  - `paidAt` (`string`, ISO-8601 UTC)

```json
{
  "paymentId": "01926b55-9999-7000-8000-000000000099",
  "orderId": "01926b54-1111-7000-8000-000000000001",
  "reservationId": "01926b54-1111-7000-8000-000000000001",
  "userId": "a1b2c3d4-e5f6-4a5b-8c9d-0e1f2a3b4c5d",
  "amountCents": 30000,
  "currency": "USD",
  "providerReference": "mock_ch_987654321",
  "paidAt": "2026-10-27T22:01:05.100000Z"
}
```

---

#### 4.2.11 `payment.failed`
- **Topic**: `payment.events` | **Partition Key**: `orderId` | **Producer**: `payment-service` | **Consumers**: `inventory-service`, `order-service`
- **Payload Fields**:
  - `paymentId` (`string`, UUID)
  - `orderId` (`string`, UUID): Identical to `reservationId`.
  - `reservationId` (`string`, UUID): Identical to `orderId`.
  - `userId` (`string`, UUID)
  - `amountCents` (`integer`, int64)
  - `currency` (`string`, ISO-4217): `"USD"`
  - `failureCode` (`string`, enum: `"CARD_DECLINED"` | `"INSUFFICIENT_FUNDS"` | `"PROVIDER_TIMEOUT"` | `"FRAUD_REJECTED"`)
  - `failureMessage` (`string`)
  - `failedAt` (`string`, ISO-8601 UTC)

```json
{
  "paymentId": "01926b55-9999-7000-8000-000000000098",
  "orderId": "01926b54-1111-7000-8000-000000000001",
  "reservationId": "01926b54-1111-7000-8000-000000000001",
  "userId": "a1b2c3d4-e5f6-4a5b-8c9d-0e1f2a3b4c5d",
  "amountCents": 30000,
  "currency": "USD",
  "failureCode": "CARD_DECLINED",
  "failureMessage": "Mock payment provider declined the charge.",
  "failedAt": "2026-10-27T22:01:15.050000Z"
}
```

---

#### 4.2.12 `payment.refunded`
- **Topic**: `payment.events` | **Partition Key**: `orderId` | **Producer**: `payment-service` | **Consumer**: `order-service` (`inventory-service` ignores via `event-type` header)
- **Payload Fields**:
  - `refundId` (`string`, UUID)
  - `paymentId` (`string`, UUID)
  - `orderId` (`string`, UUID): Identical to `reservationId`.
  - `reservationId` (`string`, UUID): Identical to `orderId`.
  - `userId` (`string`, UUID)
  - `amountCents` (`integer`, int64)
  - `currency` (`string`, ISO-4217): `"USD"`
  - `reason` (`string`, enum: `"LATE_PAYMENT_EXPIRED_HOLD"`)
  - `refundedAt` (`string`, ISO-8601 UTC)

```json
{
  "refundId": "01926b56-8888-7000-8000-000000000077",
  "paymentId": "01926b55-9999-7000-8000-000000000099",
  "orderId": "01926b54-1111-7000-8000-000000000001",
  "reservationId": "01926b54-1111-7000-8000-000000000001",
  "userId": "a1b2c3d4-e5f6-4a5b-8c9d-0e1f2a3b4c5d",
  "amountCents": 30000,
  "currency": "USD",
  "reason": "LATE_PAYMENT_EXPIRED_HOLD",
  "refundedAt": "2026-10-27T22:10:02.100000Z"
}
```

