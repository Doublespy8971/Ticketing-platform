-- Keyset pagination indexes for public seat availability endpoint:
-- GET /api/v1/events/{eventId}/seats

CREATE INDEX idx_seats_event_keyset
    ON seats (event_id, section_code, row_label, seat_number, seat_id);

CREATE INDEX idx_seats_event_status_keyset
    ON seats (event_id, status, section_code, row_label, seat_number, seat_id);
