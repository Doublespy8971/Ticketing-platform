package com.ticketing.outbox;

import java.security.SecureRandom;
import java.util.UUID;

/**
 * RFC 9562 UUIDv7 generator with strictly monotonic 18-bit sub-millisecond counter.
 * Guarantees that every UUIDv7 generated on this JVM is unique and strictly monotonically increasing.
 */
public final class UuidV7 {

    private static final SecureRandom RANDOM = new SecureRandom();

    private static long lastTimestampMs = -1L;
    private static int sequence = 0;

    private UuidV7() {
    }

    public static synchronized UUID generate() {
        long nowMs = System.currentTimeMillis();
        if (nowMs > lastTimestampMs) {
            lastTimestampMs = nowMs;
            // Seed initial 12-bit counter randomly with top bit 0 so plenty of headroom remains within the millisecond
            sequence = RANDOM.nextInt(1 << 11);
        } else {
            sequence++;
            if (sequence >= (1 << 18)) {
                // Overflowed 262,144 IDs in a single millisecond: advance synthetic timestamp by 1 ms
                lastTimestampMs++;
                sequence = 0;
            }
        }

        long ts = lastTimestampMs & 0xFFFFFFFFFFFFL; // 48-bit unix_ts_ms
        long randA = (sequence >>> 6) & 0x0FFFL;      // top 12 bits of sequence go into rand_a
        long randBHigh = sequence & 0x3FL;            // bottom 6 bits of sequence go into top of rand_b
        long randBLow = RANDOM.nextLong() & 0x00FFFFFFFFFFFFFFL; // 56 random bits

        long msb = (ts << 16) | 0x7000L | randA;
        long lsb = 0x8000000000000000L | (randBHigh << 56) | randBLow;
        return new UUID(msb, lsb);
    }
}

