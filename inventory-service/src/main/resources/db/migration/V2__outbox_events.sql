CREATE TABLE outbox_events (
    event_id        UUID            PRIMARY KEY,
    aggregate_type  VARCHAR(64)     NOT NULL,
    aggregate_id    UUID            NOT NULL,
    topic           VARCHAR(128)    NOT NULL,
    event_type      VARCHAR(128)    NOT NULL,
    correlation_id  UUID            NOT NULL,
    schema_version  INTEGER         NOT NULL DEFAULT 1,
    payload         JSONB           NOT NULL,
    created_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    published_at    TIMESTAMPTZ     NULL,
    CONSTRAINT chk_outbox_schema_version CHECK (schema_version >= 1)
);

-- Partial index scanned by the 200ms polling publisher (only indexes unpublished rows)
CREATE INDEX idx_outbox_events_unpublished
    ON outbox_events (created_at ASC, event_id ASC)
    WHERE published_at IS NULL;

-- Partial index used by the hourly retention cleanup job
CREATE INDEX idx_outbox_events_published_retention
    ON outbox_events (published_at ASC)
    WHERE published_at IS NOT NULL;

