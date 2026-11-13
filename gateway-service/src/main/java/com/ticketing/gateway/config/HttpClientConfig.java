package com.ticketing.gateway.config;

import org.springframework.cloud.gateway.config.HttpClientCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Customizes the Reactor Netty HTTP client for Spring Cloud Gateway.
 * Disables automatic retry so the gateway never replays requests (especially non-idempotent POSTs).
 * Clients retry explicitly using the Idempotency-Key header.
 */
@Configuration
public class HttpClientConfig {

    @Bean
    public HttpClientCustomizer disableHttpClientRetry() {
        return httpClient -> httpClient.disableRetry(true);
    }
}

