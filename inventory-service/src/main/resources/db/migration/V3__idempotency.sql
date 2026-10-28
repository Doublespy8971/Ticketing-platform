CREATE TABLE idempotency_keys (
    user_id         UUID            NOT NULL,
    idempotency_key UUID            NOT NULL,
    endpoint        VARCHAR(128)    NOT NULL,
    request_hash    CHAR(64)        NOT NULL,
    status          VARCHAR(32)     NOT NULL,
    response_status INTEGER         NULL,
    response_body   JSONB           NULL,
    created_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    completed_at    TIMESTAMPTZ     NULL,
    expires_at      TIMESTAMPTZ     NOT NULL DEFAULT (NOW() + INTERVAL '24 hours'),
    PRIMARY KEY (user_id, idempotency_key),
    CONSTRAINT chk_idempotency_status CHECK (status IN ('IN_PROGRESS', 'COMPLETED'))
);

CREATE INDEX idx_idempotency_keys_expires_at
    ON idempotency_keys (expires_at ASC);

CREATE TABLE processed_events (
    event_id        UUID            NOT NULL,
    consumer_group  VARCHAR(128)    NOT NULL,
    event_type      VARCHAR(128)    NOT NULL,
    processed_at    TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    PRIMARY KEY (event_id, consumer_group)
);

CREATE INDEX idx_processed_events_processed_at
    ON processed_events (processed_at ASC);

