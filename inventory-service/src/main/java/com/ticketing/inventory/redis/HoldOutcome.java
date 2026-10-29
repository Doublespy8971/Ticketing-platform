package com.ticketing.inventory.redis;

public enum HoldOutcome {
    HELD,
    EVENT_NOT_CACHED,
    EVENT_NOT_OPEN,
    SEAT_TAKEN;

    public static HoldOutcome fromRaw(String raw) {
        if (raw == null) {
            throw new RedisUnavailableException("hold_inventory.lua returned null reply");
        }
        try {
            return HoldOutcome.valueOf(raw);
        } catch (IllegalArgumentException ex) {
            throw new RedisUnavailableException("hold_inventory.lua returned unrecognized outcome: " + raw, ex);
        }
    }
}

