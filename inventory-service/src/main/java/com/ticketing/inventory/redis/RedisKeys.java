package com.ticketing.inventory.redis;

import java.util.Objects;
import java.util.UUID;

public final class RedisKeys {

    public static final String SOLD_MARKER = "SOLD";
    public static final long EVENT_DROP_TS_TTL_SECONDS = 604800L;
    public static final long DEFAULT_SEAT_HOLD_TTL_SECONDS = 660L;
    public static final long HOLD_TTL_SAFETY_MARGIN_SECONDS = 60L;

    private RedisKeys() {
    }

    public static String eventDropTsKey(UUID eventId) {
        Objects.requireNonNull(eventId, "eventId must not be null");
        return "event:" + eventId + ":drop_ts";
    }

    public static String seatKey(UUID eventId, UUID seatId) {
        Objects.requireNonNull(eventId, "eventId must not be null");
        Objects.requireNonNull(seatId, "seatId must not be null");
        return "seat:" + eventId + ":" + seatId;
    }
}

