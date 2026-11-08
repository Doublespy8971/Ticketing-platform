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

