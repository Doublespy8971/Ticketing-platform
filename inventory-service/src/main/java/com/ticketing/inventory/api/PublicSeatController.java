package com.ticketing.inventory.api;

import com.ticketing.common.error.ResourceNotFoundException;
import com.ticketing.inventory.api.dto.SeatAvailabilityItem;
import com.ticketing.inventory.api.dto.SeatAvailabilityResponse;
import com.ticketing.inventory.domain.EventRecord;
import com.ticketing.inventory.domain.EventRepository;
import com.ticketing.inventory.domain.SeatRecord;
import com.ticketing.inventory.domain.SeatRepository;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

@RestController
public class PublicSeatController {

    private static final int DEFAULT_LIMIT = 200;
    private static final int MAX_LIMIT = 1000;
    private static final Set<String> ALLOWED_STATUSES = Set.of("AVAILABLE", "HELD", "SOLD");

    private final EventRepository eventRepository;
    private final SeatRepository seatRepository;

    public PublicSeatController(EventRepository eventRepository, SeatRepository seatRepository) {
        this.eventRepository = Objects.requireNonNull(eventRepository, "eventRepository must not be null");
        this.seatRepository = Objects.requireNonNull(seatRepository, "seatRepository must not be null");
    }

    @GetMapping("/api/v1/events/{eventId}/seats")
    public ResponseEntity<SeatAvailabilityResponse> getSeatAvailability(
            @PathVariable UUID eventId,
            @RequestParam(value = "section", required = false) String section,
            @RequestParam(value = "sectionCode", required = false) String sectionCode,
            @RequestParam(value = "status", required = false) String status,
            @RequestParam(value = "limit", required = false) Integer limit,
            @RequestParam(value = "cursor", required = false) String cursor
    ) {
        EventRecord event = eventRepository.findById(eventId)
                .orElseThrow(() -> new ResourceNotFoundException("Event not found: " + eventId));

        String sectionFilter = null;
        if (section != null && !section.isBlank()) {
            sectionFilter = section.trim();
        } else if (sectionCode != null && !sectionCode.isBlank()) {
            sectionFilter = sectionCode.trim();
        }

        String statusFilter = null;
        if (status != null && !status.isBlank()) {
            String upper = status.trim().toUpperCase(Locale.ROOT);
            if (!ALLOWED_STATUSES.contains(upper)) {
                throw new IllegalArgumentException("Invalid status filter: " + status + ". Allowed values: AVAILABLE, HELD, SOLD");
            }
            statusFilter = upper;
        }

        int effectiveLimit = DEFAULT_LIMIT;
        if (limit != null) {
            if (limit <= 0) {
                effectiveLimit = DEFAULT_LIMIT;
            } else if (limit > MAX_LIMIT) {
                effectiveLimit = MAX_LIMIT;
            } else {
                effectiveLimit = limit;
            }
        }

        UUID cursorSeatId = parseCursor(cursor);
        SeatRecord cursorSeat = null;
        if (cursorSeatId != null) {
            cursorSeat = seatRepository.findByIdAndEventId(cursorSeatId, eventId)
                    .orElseThrow(() -> new IllegalArgumentException("Invalid cursor: seat does not exist for event " + eventId));
        }

        List<SeatRecord> records = seatRepository.findSeatsWithKeyset(
                eventId,
                sectionFilter,
                statusFilter,
                cursorSeat,
                effectiveLimit
        );

        boolean hasMore = records.size() > effectiveLimit;
        List<SeatRecord> pageRecords = hasMore ? records.subList(0, effectiveLimit) : records;
        String nextCursor = hasMore ? pageRecords.get(pageRecords.size() - 1).seatId().toString() : null;

        List<SeatAvailabilityItem> items = pageRecords.stream()
                .map(r -> new SeatAvailabilityItem(
                        r.seatId(),
                        r.sectionCode(),
                        r.rowLabel(),
                        r.seatNumber(),
                        r.priceCents(),
                        r.currency(),
                        r.status()
                ))
                .toList();

        SeatAvailabilityResponse response = new SeatAvailabilityResponse(
                event.eventId(),
                event.dropTime(),
                items,
                nextCursor
        );

        return ResponseEntity.ok()
                .header(HttpHeaders.CACHE_CONTROL, "max-age=1")
                .body(response);
    }

    private UUID parseCursor(String cursor) {
        if (cursor == null || cursor.isBlank()) {
            return null;
        }
        String trimmed = cursor.trim();
        try {
            return UUID.fromString(trimmed);
        } catch (IllegalArgumentException e) {
            try {
                byte[] decoded = Base64.getUrlDecoder().decode(trimmed);
                return UUID.fromString(new String(decoded, StandardCharsets.UTF_8).trim());
            } catch (Exception ex) {
                throw new IllegalArgumentException("Invalid cursor format: " + cursor);
            }
        }
    }
}
