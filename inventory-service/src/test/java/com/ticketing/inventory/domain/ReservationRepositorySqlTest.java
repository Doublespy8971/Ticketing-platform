package com.ticketing.inventory.domain;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class ReservationRepositorySqlTest {

    @Test
    @DisplayName("None of the expiry and confirm SQL constants in ReservationRepository contains NOW()")
    void reservationRepositoryExpiryAndConfirmSqlConstantsDoNotContainNow() {
        List<String> sqlConstants = List.of(
                ReservationRepository.SELECT_EXPIRED_HELD_IDS_FOR_UPDATE_SKIP_LOCKED_SQL,
                ReservationRepository.LOCK_RESERVATION_FOR_UPDATE_SQL,
                ReservationRepository.LOCK_RESERVATION_FOR_UPDATE_SKIP_LOCKED_SQL,
                ReservationRepository.MARK_EXPIRED_WITH_DB_CLOCK_SQL,
                ReservationRepository.EXPIRE_HELD_ITEMS_SQL,
                ReservationRepository.MARK_CONFIRMED_WITH_DB_CLOCK_SQL,
                ReservationRepository.CONFIRM_HELD_ITEMS_SQL,
                ReservationRepository.CONFIRM_HELD_SEATS_SQL,
                ReservationRepository.OLDEST_OVERDUE_HOLD_SECONDS_SQL
        );

        for (String sql : sqlConstants) {
            assertThat(sql)
                    .doesNotContainIgnoringCase("NOW()")
                    .contains("clock_timestamp()");
        }
    }

    @Test
    @DisplayName("None of the release and confirm SQL constants in SeatRepository contains NOW()")
    void seatRepositoryReleaseAndConfirmSqlConstantsDoNotContainNow() {
        List<String> sqlConstants = List.of(
                SeatRepository.RELEASE_HELD_SEATS_SQL,
                SeatRepository.CONFIRM_HELD_SEATS_SQL
        );

        for (String sql : sqlConstants) {
            assertThat(sql)
                    .doesNotContainIgnoringCase("NOW()")
                    .contains("clock_timestamp()");
        }
    }
}

