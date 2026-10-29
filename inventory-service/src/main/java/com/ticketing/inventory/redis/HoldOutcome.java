package com.ticketing.inventory.redis;

public enum HoldOutcome {
    HELD,
    EVENT_NOT_CACHED,
    EVENT_NOT_OPEN,
    SEAT_TAKEN;

    public static HoldOutcome fromRaw(String raw) {
        if (raw == null) {
            throw new IllegalArgumentException("Null hold_inventory.lua outcome");
        }
        return HoldOutcome.valueOf(raw);
    }
}

