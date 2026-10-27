## 8. Database Schema per Service

The platform provisions **three isolated PostgreSQL 16 databases**: `inventory_db`, `order_db`, and `payment_db`. `gateway-service` and `notification-service` are stateless and **do not have PostgreSQL databases**.

### 8.1 `inventory_db` (`inventory-service`)

#### Anti-Overselling Guarantees at the PostgreSQL Level
Overselling is mathematically impossible in `inventory_db` even if Redis were completely bypassed or flushed, enforced by three independent database invariants:
1. **Physical Seat Identity Constraint**: `CONSTRAINT uq_seats_event_location UNIQUE (event_id, section_code, row_label, seat_number)` ensures duplicate physical seat records cannot be created for an event.
2. **Guarded Status Transition with Optimistic Locking**: Seat acquisition executes `UPDATE seats SET status = 'HELD', version = version + 1, updated_at = NOW() WHERE event_id = :eventId AND seat_id = ANY(:seatIds) AND status = 'AVAILABLE'` in deterministic `ORDER BY seat_id ASC` order. If the updated row count does not equal the number of requested `seatIds`, the transaction immediately rolls back.
3. **Partial Unique Index on Active Reservation Line Items**: `CREATE UNIQUE INDEX idx_reservation_items_active_seat ON reservation_items (event_id, seat_id) WHERE status IN ('HELD', 'CONFIRMED')` makes it physically impossible for two active reservations (`HELD` or `CONFIRMED`) to reference the same `(event_id, seat_id)` simultaneously.

```sql
-- 1. Events table
CREATE TABLE events (
    event_id        UUID            PRIMARY KEY,
    name            VARCHAR(255)    NOT NULL,
    venue_name      VARCHAR(255)    NOT NULL,
    event_time      TIMESTAMPTZ     NOT NULL,
    drop_time       TIMESTAMPTZ     NOT NULL,
    status          VARCHAR(32)     NOT NULL DEFAULT 'SCHEDULED',
    created_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_events_status CHECK (status IN ('SCHEDULED', 'ON_SALE', 'SOLD_OUT', 'COMPLETED', 'CANCELLED')),
    CONSTRAINT chk_events_drop_before_event CHECK (drop_time < event_time)
);

-- 2. Assigned Seats table (scoped strictly to discrete numbered seats)
CREATE TABLE seats (
    seat_id         UUID            PRIMARY KEY,
    event_id        UUID            NOT NULL REFERENCES events(event_id),
    section_code    VARCHAR(32)     NOT NULL,
    row_label       VARCHAR(16)     NOT NULL,
    seat_number     VARCHAR(16)     NOT NULL,
    price_cents     BIGINT          NOT NULL,
    currency        CHAR(3)         NOT NULL DEFAULT 'USD',
    status          VARCHAR(16)     NOT NULL DEFAULT 'AVAILABLE',
    version         BIGINT          NOT NULL DEFAULT 0,
    created_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_seats_event_location UNIQUE (event_id, section_code, row_label, seat_number),
    CONSTRAINT chk_seats_price_positive CHECK (price_cents > 0),
    CONSTRAINT chk_seats_status CHECK (status IN ('AVAILABLE', 'HELD', 'SOLD'))
);

CREATE INDEX idx_seats_event_section_status
    ON seats (event_id, section_code, status);

-- 3. Reservations table
CREATE TABLE reservations (
    reservation_id      UUID            PRIMARY KEY,
    event_id            UUID            NOT NULL REFERENCES events(event_id),
    user_id             UUID            NOT NULL,
    status              VARCHAR(16)     NOT NULL DEFAULT 'HELD',
    total_amount_cents  BIGINT          NOT NULL,
    currency            CHAR(3)         NOT NULL DEFAULT 'USD',
    created_at          TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    expires_at          TIMESTAMPTZ     NOT NULL,
    confirmed_at        TIMESTAMPTZ     NULL,
    released_at         TIMESTAMPTZ     NULL,
    expired_at          TIMESTAMPTZ     NULL,
    updated_at          TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_reservations_status CHECK (status IN ('HELD', 'CONFIRMED', 'RELEASED', 'EXPIRED')),
    CONSTRAINT chk_reservations_amount_positive CHECK (total_amount_cents > 0),
    CONSTRAINT chk_reservations_expiry_after_create CHECK (expires_at > created_at)
);

-- Partial index used by the 1000ms PostgreSQL hold expiry sweeper
CREATE INDEX idx_reservations_held_expires_at
    ON reservations (expires_at ASC)
    WHERE status = 'HELD';

CREATE INDEX idx_reservations_user_id
    ON reservations (user_id, created_at DESC);

-- 4. Reservation Items table (maps each reservation to 1..6 assigned seats)
CREATE TABLE reservation_items (
    reservation_item_id UUID            PRIMARY KEY,
    reservation_id      UUID            NOT NULL REFERENCES reservations(reservation_id) ON DELETE CASCADE,
    event_id            UUID            NOT NULL REFERENCES events(event_id),
    seat_id             UUID            NOT NULL REFERENCES seats(seat_id),
    section_code        VARCHAR(32)     NOT NULL,
    row_label           VARCHAR(16)     NOT NULL,
    seat_number         VARCHAR(16)     NOT NULL,
    price_cents         BIGINT          NOT NULL,
    status              VARCHAR(16)     NOT NULL DEFAULT 'HELD',
    created_at          TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_reservation_items_res_seat UNIQUE (reservation_id, seat_id),
    CONSTRAINT chk_reservation_items_status CHECK (status IN ('HELD', 'CONFIRMED', 'RELEASED', 'EXPIRED'))
);

-- Hard DB-level anti-overselling invariant: at most ONE active ('HELD' or 'CONFIRMED')
-- reservation item can ever exist for a given (event_id, seat_id)
CREATE UNIQUE INDEX idx_reservation_items_active_seat
    ON reservation_items (event_id, seat_id)
    WHERE status IN ('HELD', 'CONFIRMED');

-- 5. Transactional Outbox, HTTP Idempotency, and Kafka Processed Events tables
-- (Exact DDL defined in Sections 5.1, 6.1, and 6.2: outbox_events, idempotency_keys, processed_events)
```

---

### 8.2 `order_db` (`order-service`)

```sql
-- 1. Orders table (order_id is strictly equal to reservation_id)
CREATE TABLE orders (
    order_id                UUID            PRIMARY KEY,
    reservation_id          UUID            NOT NULL UNIQUE,
    user_id                 UUID            NOT NULL,
    event_id                UUID            NOT NULL,
    status                  VARCHAR(32)     NOT NULL DEFAULT 'RESERVED',
    total_amount_cents      BIGINT          NOT NULL,
    currency                CHAR(3)         NOT NULL DEFAULT 'USD',
    payment_method_token    VARCHAR(128)    NULL,
    payment_id              UUID            NULL,
    cancellation_reason     VARCHAR(64)     NULL,
    version                 BIGINT          NOT NULL DEFAULT 0,
    reserved_at             TIMESTAMPTZ     NOT NULL,
    expires_at              TIMESTAMPTZ     NOT NULL,
    checkout_at             TIMESTAMPTZ     NULL,
    paid_at                 TIMESTAMPTZ     NULL,
    confirmed_at            TIMESTAMPTZ     NULL,
    cancelled_at            TIMESTAMPTZ     NULL,
    refunded_at             TIMESTAMPTZ     NULL,
    created_at              TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    updated_at              TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_orders_id_equals_reservation CHECK (order_id = reservation_id),
    CONSTRAINT chk_orders_status CHECK (status IN (
        'RESERVED',
        'PAYMENT_PENDING',
        'CONFIRMED',
        'CANCELLED_PAYMENT_FAILED',
        'CANCELLED_EXPIRED',
        'REFUND_PENDING',
        'CANCELLED_REFUNDED'
    )),
    CONSTRAINT chk_orders_total_positive CHECK (total_amount_cents > 0)
);

CREATE INDEX idx_orders_user_id
    ON orders (user_id, created_at DESC);

CREATE INDEX idx_orders_status_expires_at
    ON orders (status, expires_at ASC)
    WHERE status IN ('RESERVED', 'PAYMENT_PENDING');

-- 2. Order Items table
CREATE TABLE order_items (
    order_item_id   UUID            PRIMARY KEY,
    order_id        UUID            NOT NULL REFERENCES orders(order_id) ON DELETE CASCADE,
    seat_id         UUID            NOT NULL,
    section_code    VARCHAR(32)     NOT NULL,
    row_label       VARCHAR(16)     NOT NULL,
    seat_number     VARCHAR(16)     NOT NULL,
    price_cents     BIGINT          NOT NULL,
    CONSTRAINT uq_order_items_order_seat UNIQUE (order_id, seat_id),
    CONSTRAINT chk_order_items_price_positive CHECK (price_cents > 0)
);

-- 3. Transactional Outbox, HTTP Idempotency, and Kafka Processed Events tables
-- (Exact DDL defined in Sections 5.1, 6.1, and 6.2: outbox_events, idempotency_keys, processed_events)
```

---

### 8.3 `payment_db` (`payment-service`)

```sql
-- 1. Payments table (one payment record per order_id, tracking charge and optional refund)
CREATE TABLE payments (
    payment_id          UUID            PRIMARY KEY,
    order_id            UUID            NOT NULL UNIQUE,
    reservation_id      UUID            NOT NULL UNIQUE,
    user_id             UUID            NOT NULL,
    amount_cents        BIGINT          NOT NULL,
    currency            CHAR(3)         NOT NULL DEFAULT 'USD',
    payment_method_token VARCHAR(128)   NOT NULL,
    status              VARCHAR(32)     NOT NULL,
    provider_reference  VARCHAR(128)    NULL,
    failure_code        VARCHAR(64)     NULL,
    failure_message     VARCHAR(512)    NULL,
    refund_id           UUID            NULL UNIQUE,
    refund_reason       VARCHAR(64)     NULL,
    created_at          TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    paid_at             TIMESTAMPTZ     NULL,
    failed_at           TIMESTAMPTZ     NULL,
    refunded_at         TIMESTAMPTZ     NULL,
    updated_at          TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_payments_order_equals_res CHECK (order_id = reservation_id),
    CONSTRAINT chk_payments_status CHECK (status IN ('SUCCEEDED', 'FAILED', 'REFUNDED')),
    CONSTRAINT chk_payments_amount_positive CHECK (amount_cents > 0)
);

CREATE INDEX idx_payments_user_id
    ON payments (user_id, created_at DESC);

-- 2. Transactional Outbox and Kafka Processed Events tables
-- (Exact DDL defined in Sections 5.1 and 6.2: outbox_events, processed_events)
```

---

### 8.4 `notification-service` Database Scope
`notification-service` is **stateless** and owns **zero PostgreSQL databases**. Event deduplication is backed by Redis 7 (`notif:dedup:{eventId}`).

