package com.ticketing.inventory.redis;

public enum ConfirmOutcome {
    CONFIRMED;

    public static ConfirmOutcome fromRaw(String raw) {
        if (raw == null) {
            throw new RedisUnavailableException("confirm_inventory.lua returned null reply");
        }
        try {
            return ConfirmOutcome.valueOf(raw);
        } catch (IllegalArgumentException ex) {
            throw new RedisUnavailableException("confirm_inventory.lua returned unrecognized outcome: " + raw, ex);
        }
    }
}

