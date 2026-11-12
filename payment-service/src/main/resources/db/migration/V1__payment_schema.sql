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

