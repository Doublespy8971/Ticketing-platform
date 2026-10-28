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

