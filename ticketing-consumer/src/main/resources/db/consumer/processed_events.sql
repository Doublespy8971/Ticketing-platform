-- Consumer idempotency deduplication table (docs/arch/05-outbox-idempotency.md, section 5.5)
CREATE TABLE IF NOT EXISTS processed_events (
    event_id       UUID        NOT NULL,
    consumer_group VARCHAR(64) NOT NULL,
    event_type     VARCHAR(64) NOT NULL,
    processed_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (event_id, consumer_group)
);

CREATE INDEX IF NOT EXISTS idx_processed_events_processed_at
    ON processed_events (processed_at);

