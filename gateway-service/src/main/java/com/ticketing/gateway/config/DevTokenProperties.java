package com.ticketing.gateway.config;

import jakarta.annotation.PostConstruct;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

import java.util.Arrays;

/**
 * Configuration properties for dev-only token issuance endpoint.
 */
@Component
@ConfigurationProperties(prefix = "ticketing.gateway.dev-token")
public class DevTokenProperties {

    private final Environment environment;

    public DevTokenProperties(Environment environment) {
        this.environment = environment;
    }

    /**
     * Whether the dev-only /api/v1/auth/token endpoint is enabled.
     * Defaults to false in application.yml; enabled only via dev/local profiles.
     */
    @org.springframework.beans.factory.annotation.Value("${ticketing.gateway.dev-token.enabled:false}")
    private boolean enabled = false;

    @org.springframework.beans.factory.annotation.Value("${ticketing.gateway.dev-token.allow-admin:false}")
    private boolean allowAdmin = false;

    @PostConstruct
    public void validate() {
        boolean isProd = Arrays.asList(environment.getActiveProfiles()).contains("prod")
                || environment.matchesProfiles("prod");
        if (isProd && enabled) {
            throw new IllegalStateException(
                    "Dev token endpoint (ticketing.gateway.dev-token.enabled) must NEVER be enabled when prod profile is active");
        }
    }

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public boolean isAllowAdmin() {
        return allowAdmin;
    }

    public void setAllowAdmin(boolean allowAdmin) {
        this.allowAdmin = allowAdmin;
    }
}
