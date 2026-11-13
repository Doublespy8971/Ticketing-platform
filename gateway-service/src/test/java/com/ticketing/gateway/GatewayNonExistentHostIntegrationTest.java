package com.ticketing.gateway;

import com.ticketing.gateway.auth.JwtTokenService;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.reactive.AutoConfigureWebTestClient;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.reactive.server.EntityExchangeResult;
import org.springframework.test.web.reactive.server.WebTestClient;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
                "ORDER_SERVICE_URL=http://nonexistent-host-99999.internal:8082",
                "ticketing.gateway.allow-insecure-dev-secret=true",
                "spring.data.redis.host=localhost",
                "spring.data.redis.port=6379"
        })
@ActiveProfiles("local")
@AutoConfigureWebTestClient
public class GatewayNonExistentHostIntegrationTest {

    @Autowired
    private WebTestClient client;

    @Autowired
    private JwtTokenService tokenService;

    @Test
    @DisplayName("Downstream host that does not exist returns 502 with no hostname in body")
    void nonExistentHostReturns502() {
        String token = tokenService.generateToken(UUID.randomUUID(), List.of("USER"), 3600);

        EntityExchangeResult<byte[]> result = client.post()
                .uri("/api/v1/orders/" + UUID.randomUUID() + "/checkout")
                .header(HttpHeaders.AUTHORIZATION, "Bearer " + token)
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue("{}")
                .exchange()
                .expectStatus().isEqualTo(502)
                .expectHeader().contentType(MediaType.parseMediaType("application/problem+json"))
                .expectBody()
                .jsonPath("$.errorCode").isEqualTo("BAD_GATEWAY")
                .jsonPath("$.detail").isEqualTo("Downstream service is unavailable")
                .returnResult();

        String body = new String(result.getResponseBody(), StandardCharsets.UTF_8);
        assertThat(body).doesNotContain("nonexistent-host-99999");
    }
}

