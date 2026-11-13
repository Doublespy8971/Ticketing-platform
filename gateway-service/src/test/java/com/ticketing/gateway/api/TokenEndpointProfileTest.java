package com.ticketing.gateway.api;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.reactive.AutoConfigureWebTestClient;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.reactive.server.WebTestClient;

import java.util.UUID;

class TokenEndpointProfileTest {

    @Nested
    @SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
            properties = {
                    "ticketing.gateway.allow-insecure-dev-secret=true",
                    "spring.data.redis.host=localhost",
                    "spring.data.redis.port=6379"
            })
    @ActiveProfiles("local")
    @AutoConfigureWebTestClient
    class LocalProfileTest {
        @Autowired
        private WebTestClient webTestClient;

        @Test
        @DisplayName("Token endpoint is present (200 OK) under local profile")
        void tokenEndpointPresentUnderLocalProfile() {
            webTestClient.post()
                    .uri("/api/v1/auth/token")
                    .contentType(MediaType.APPLICATION_JSON)
                    .bodyValue("{\"userId\":\"" + UUID.randomUUID() + "\",\"roles\":[\"USER\"],\"ttlSeconds\":1800}")
                    .exchange()
                    .expectStatus().isOk()
                    .expectBody()
                    .jsonPath("$.accessToken").isNotEmpty();
        }
    }

    @Nested
    @SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
            properties = {
                    "JWT_SECRET=this-is-a-valid-production-secret-key-at-least-32-bytes!",
                    "spring.data.redis.host=localhost",
                    "spring.data.redis.port=6379"
            })
    @ActiveProfiles("prod")
    @AutoConfigureWebTestClient
    class ProdProfileTest {
        @Autowired
        private WebTestClient webTestClient;

        @Test
        @DisplayName("Token endpoint is absent (404 Not Found) under prod profile")
        void tokenEndpointAbsentUnderProdProfile() {
            webTestClient.post()
                    .uri("/api/v1/auth/token")
                    .contentType(MediaType.APPLICATION_JSON)
                    .bodyValue("{\"userId\":\"" + UUID.randomUUID() + "\",\"roles\":[\"USER\"],\"ttlSeconds\":1800}")
                    .exchange()
                    .expectStatus().isNotFound()
                    .expectHeader().contentType(MediaType.parseMediaType("application/problem+json"))
                    .expectBody()
                    .jsonPath("$.errorCode").isEqualTo("RESOURCE_NOT_FOUND");
        }
    }

    @Nested
    @SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
            properties = {
                    "JWT_SECRET=this-is-a-valid-production-secret-key-at-least-32-bytes!",
                    "spring.data.redis.host=localhost",
                    "spring.data.redis.port=6379"
            })
    @AutoConfigureWebTestClient
    class DefaultProfileTest {
        @Autowired
        private WebTestClient webTestClient;

        @Test
        @DisplayName("Token endpoint is absent (404 Not Found) without any active profile")
        void tokenEndpointAbsentWithoutProfile() {
            webTestClient.post()
                    .uri("/api/v1/auth/token")
                    .contentType(MediaType.APPLICATION_JSON)
                    .bodyValue("{\"userId\":\"" + UUID.randomUUID() + "\",\"roles\":[\"USER\"],\"ttlSeconds\":1800}")
                    .exchange()
                    .expectStatus().isNotFound()
                    .expectHeader().contentType(MediaType.parseMediaType("application/problem+json"))
                    .expectBody()
                    .jsonPath("$.errorCode").isEqualTo("RESOURCE_NOT_FOUND");
        }
    }
}

