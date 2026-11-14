package com.ticketing.gateway.auth;

import java.util.Set;

/**
 * Platform security constants and header normalization helpers.
 */
public final class SecurityConstants {

    private SecurityConstants() {}

    public static final String ROLE_USER = "USER";
    public static final String ROLE_ADMIN = "ADMIN";

    /**
     * Exact upper-case, case-sensitive allowlist of permitted roles.
     */
    public static final Set<String> ALLOWED_ROLES = Set.of(ROLE_USER, ROLE_ADMIN);

    /**
     * Headers that must be stripped on all routes.
     * Evaluated against normalized header name (lower case, underscores replaced by hyphens).
     */
    public static final Set<String> STRIPPED_NORMALIZED_HEADERS = Set.of(
            "x-user-id",
            "x-user-roles",
            "authorization",
            "x-http-method-override",
            "x-http-method",
            "x-method-override"
    );

    /**
     * Normalizes a header name by converting to lower case and replacing underscores with hyphens.
     */
    public static String normalizeHeaderName(String headerName) {
        if (headerName == null) return "";
        return headerName.toLowerCase().replace('_', '-');
    }

    /**
     * Checks case-insensitively whether a raw authorization header has the 'Bearer ' prefix.
     */
    public static boolean hasBearerPrefix(String raw) {
        if (raw == null) return false;
        String leadingTrimmed = raw.stripLeading();
        return leadingTrimmed.toLowerCase().startsWith("bearer ");
    }

    /**
     * Strips 'Bearer ' prefix case-insensitively from a raw token string or header.
     */
    public static String stripBearerPrefix(String raw) {
        if (raw == null) return null;
        String leadingTrimmed = raw.stripLeading();
        if (leadingTrimmed.toLowerCase().startsWith("bearer ")) {
            return leadingTrimmed.substring(7).trim();
        }
        return raw.trim();
    }
}

