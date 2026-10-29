package com.ticketing.inventory.api;

import com.ticketing.inventory.api.dto.BulkCreateSeatsRequest;
import com.ticketing.inventory.api.dto.BulkCreateSeatsResponse;
import com.ticketing.inventory.api.dto.CreateEventRequest;
import com.ticketing.inventory.api.dto.EventResponse;
import com.ticketing.inventory.service.InventoryAdminService;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import java.util.Objects;
import java.util.UUID;

@RestController
@RequestMapping(path = "/api/v1/admin", produces = MediaType.APPLICATION_JSON_VALUE)
public class AdminEventController {

    private final InventoryAdminService adminService;

    public AdminEventController(InventoryAdminService adminService) {
        this.adminService = Objects.requireNonNull(adminService, "adminService must not be null");
    }

    @PostMapping(path = "/events", consumes = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.CREATED)
    public EventResponse createEvent(@Valid @RequestBody CreateEventRequest request) {
        return adminService.createEvent(request);
    }

    @PostMapping(path = "/events/{eventId}/seats/bulk", consumes = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.CREATED)
    public BulkCreateSeatsResponse bulkCreateSeats(
            @PathVariable("eventId") UUID eventId,
            @Valid @RequestBody BulkCreateSeatsRequest request) {
        return adminService.bulkCreateSeats(eventId, request);
    }
}

