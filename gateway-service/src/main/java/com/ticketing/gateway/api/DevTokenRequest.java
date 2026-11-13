package com.ticketing.gateway.api;

import java.util.List;

public record DevTokenRequest(
        String userId,
        List<String> roles,
        Long ttlSeconds
) {
}

