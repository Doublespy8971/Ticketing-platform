package com.ticketing.gateway.auth;

import java.util.Set;

/**
 * Platform security constants.
 */
public final class SecurityConstants {

    private SecurityConstants() {}

    public static final String ROLE_USER = "USER";
    public static final String ROLE_ADMIN = "ADMIN";

    /**
     * Exact upper-case, case-sensitive allowlist of permitted roles.
     */
    public static final Set<String> ALLOWED_ROLES = Set.of(ROLE_USER, ROLE_ADMIN);
}

