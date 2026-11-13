package com.ticketing.gateway.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * Configuration properties for dev-only token issuance endpoint.
 */
@Component
@ConfigurationProperties(prefix = "ticketing.gateway.dev-token")
public class DevTokenProperties {

    /**
     * Whether the dev-only /api/v1/auth/token endpoint is enabled.
     * Default true locally; must never be enabled in production.
     */
    private boolean enabled = true;

    /**
     * Whether requests for the ADMIN role are permitted on the dev token endpoint.
     * Default false.
     */
    private boolean allowAdmin = false;

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

