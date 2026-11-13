package com.ticketing.gateway.config;

import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;

/**
 * Validates and holds the HS256 secret key.
 *
 * Startup MUST fail if the secret is missing or shorter than 32 bytes (256 bits),
 * unless {@code ticketing.gateway.allow-insecure-dev-secret} is explicitly set to true.
 */
@Component
public class JwtProperties {

    private static final Logger log = LoggerFactory.getLogger(JwtProperties.class);
    public static final String DEV_FALLBACK_SECRET = "dev-only-hs256-secret-key-minimum-32-bytes-long!";

    @Value("${JWT_SECRET:${ticketing.jwt.secret:}}")
    private String rawSecret;

    @Value("${ticketing.gateway.allow-insecure-dev-secret:false}")
    private boolean allowInsecureDevSecret;

    private String effectiveSecret;

    @PostConstruct
    public void validate() {
        if (rawSecret == null || rawSecret.trim().isEmpty() || rawSecret.getBytes(StandardCharsets.UTF_8).length < 32) {
            if (allowInsecureDevSecret) {
                log.warn("Using insecure dev secret key for JWT verification. Do not use in production!");
                this.effectiveSecret = DEV_FALLBACK_SECRET;
            } else {
                throw new IllegalStateException(
                        "JWT_SECRET environment variable is missing or shorter than 32 bytes (256 bits), and ticketing.gateway.allow-insecure-dev-secret is false");
            }
        } else {
            this.effectiveSecret = rawSecret;
        }
    }

    public String getEffectiveSecret() {
        return effectiveSecret;
    }

    public boolean isAllowInsecureDevSecret() {
        return allowInsecureDevSecret;
    }

    public void setRawSecret(String rawSecret) {
        this.rawSecret = rawSecret;
    }

    public void setAllowInsecureDevSecret(boolean allowInsecureDevSecret) {
        this.allowInsecureDevSecret = allowInsecureDevSecret;
    }
}
