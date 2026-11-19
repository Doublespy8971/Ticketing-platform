package com.ticketing.gateway;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.ReactiveHealthIndicator;
import org.springframework.boot.test.autoconfigure.web.reactive.AutoConfigureWebTestClient;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.reactive.server.WebTestClient;
import reactor.core.publisher.Mono;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "spring.data.redis.host=localhost",
        "spring.data.redis.port=6379"
})
@ActiveProfiles("local")
@AutoConfigureWebTestClient
public class GatewayHealthReadinessTest {

    @TestConfiguration
    static class TestHealthConfig {
        @Bean(name = {"redisHealthIndicator", "redisHealthContributor"})
        @Primary
        public ReactiveHealthIndicator redisHealthIndicator() {
            return () -> Mono.just(Health.down().withDetail("error", "Simulated Redis outage").build());
        }
    }

    @Autowired
    private WebTestClient webTestClient;

    @Test
    @DisplayName("When Redis is DOWN, readiness probe returns 200 UP because rate limiting fails open")
    void readinessStaysUpWhenRedisDown() {
        webTestClient.get()
                .uri("/actuator/health/readiness")
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.status").isEqualTo("UP");
    }

    @Test
    @DisplayName("When Redis is DOWN, liveness probe returns 200 UP")
    void livenessStaysUpWhenRedisDown() {
        webTestClient.get()
                .uri("/actuator/health/liveness")
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.status").isEqualTo("UP");
    }

    @Test
    @DisplayName("When Redis is DOWN, aggregate /actuator/health returns 503 SERVICE_UNAVAILABLE")
    void aggregateHealthReportsDownWhenRedisDown() {
        webTestClient.get()
                .uri("/actuator/health")
                .exchange()
                .expectStatus().isEqualTo(503)
                .expectBody()
                .jsonPath("$.status").isEqualTo("DOWN");
    }
}
