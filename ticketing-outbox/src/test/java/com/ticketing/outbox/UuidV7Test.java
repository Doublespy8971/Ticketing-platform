package com.ticketing.outbox;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class UuidV7Test {

    @Test
    void generatesUniqueAndStrictlyMonotonicallyIncreasingUuids() {
        int count = 10_000;
        List<UUID> uuids = new ArrayList<>(count);
        Set<UUID> unique = new HashSet<>(count);

        for (int i = 0; i < count; i++) {
            UUID id = UuidV7.generate();
            assertThat(id.version()).isEqualTo(7);
            assertThat(id.variant()).isEqualTo(2);
            uuids.add(id);
            unique.add(id);
        }

        assertThat(unique).hasSize(count);
        for (int i = 1; i < count; i++) {
            UUID prev = uuids.get(i - 1);
            UUID curr = uuids.get(i);
            assertThat(curr.toString()).isGreaterThan(prev.toString());
            assertThat(Long.compareUnsigned(curr.getMostSignificantBits(), prev.getMostSignificantBits()))
                    .isGreaterThanOrEqualTo(0);
        }
    }
}

