package com.ticketing.inventory.redis;

public enum ConfirmOutcome {
    CONFIRMED;

    public static ConfirmOutcome fromRaw(String raw) {
        if (raw == null) {
            throw new IllegalArgumentException("Null confirm_inventory.lua outcome");
        }
        return ConfirmOutcome.valueOf(raw);
    }
}

