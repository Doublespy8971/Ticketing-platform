package com.ticketing.gateway.config;

import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * Validates and holds the HS256 secret key.
 *
 * Startup MUST fail if the secret is missing or shorter than 32 bytes (256 bits),
 * unless {@code ticketing.gateway.allow-insecure-dev-secret} is explicitly set to true.
 * When 'prod' profile is active, allow-insecure-dev-secret MUST NOT be true and secret
 * MUST be at least 32 UTF-8 bytes.
 */
@Component
public class JwtProperties {

    private static final Logger log = LoggerFactory.getLogger(JwtProperties.class);
    public static final String DEV_FALLBACK_SECRET = "dev-only-hs256-secret-key-minimum-32-bytes-long!";

    @Autowired(required = false)
    private Environment environment;

    @Value("${JWT_SECRET:${ticketing.jwt.secret:}}")
    private String rawSecret;

    @Value("${ticketing.gateway.allow-insecure-dev-secret:false}")
    private boolean allowInsecureDevSecret;

    private String effectiveSecret;

    @PostConstruct
    public void validate() {
        boolean isProd = environment != null && (
                Arrays.asList(environment.getActiveProfiles()).contains("prod")
                || environment.matchesProfiles("prod")
        );

        if (isProd) {
            if (allowInsecureDevSecret) {
                throw new IllegalStateException(
                        "ticketing.gateway.allow-insecure-dev-secret must NEVER be true when prod profile is active");
            }
            if (rawSecret == null || rawSecret.trim().isEmpty() || rawSecret.getBytes(StandardCharsets.UTF_8).length < 32) {
                throw new IllegalStateException(
                        "JWT_SECRET must be at least 32 UTF-8 bytes when prod profile is active");
            }
        }

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
