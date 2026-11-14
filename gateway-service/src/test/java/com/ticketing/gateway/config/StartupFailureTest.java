package com.ticketing.gateway.config;

import com.ticketing.gateway.ratelimit.ClientAddressResolver;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class StartupFailureTest {

    @Test
    @DisplayName("Startup fails when secret is 31 bytes (shorter than 32 bytes) and allow-insecure-dev-secret is false")
    void startupFailsWith31ByteSecret() {
        // Exactly 31 characters / 31 UTF-8 bytes
        String secret31 = "1234567890123456789012345678901";
        try (AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext()) {
            TestPropertyValues.of(
                    "JWT_SECRET=" + secret31,
                    "ticketing.gateway.allow-insecure-dev-secret=false"
            ).applyTo(context);
            context.register(JwtProperties.class);

            assertThatThrownBy(context::refresh)
                    .hasRootCauseInstanceOf(IllegalStateException.class)
                    .rootCause()
                    .hasMessageContaining("shorter than 32 bytes");
        }
    }

    @Test
    @DisplayName("Startup succeeds when secret is exactly 32 bytes")
    void startupSucceedsWith32ByteSecret() {
        // Exactly 32 characters / 32 UTF-8 bytes
        String secret32 = "12345678901234567890123456789012";
        try (AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext()) {
            TestPropertyValues.of(
                    "JWT_SECRET=" + secret32,
                    "ticketing.gateway.allow-insecure-dev-secret=false"
            ).applyTo(context);
            context.register(JwtProperties.class);
            context.refresh();

            JwtProperties props = context.getBean(JwtProperties.class);
            assertThat(props.getEffectiveSecret()).isEqualTo(secret32);
        }
    }

    @Test
    @DisplayName("Startup fails when JWT_SECRET is not set at all and allow-insecure-dev-secret is false")
    void startupFailsWhenSecretNotSetAtAll() {
        try (AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext()) {
            TestPropertyValues.of(
                    "ticketing.gateway.allow-insecure-dev-secret=false"
            ).applyTo(context);
            context.register(JwtProperties.class);

            assertThatThrownBy(context::refresh)
                    .hasRootCauseInstanceOf(IllegalStateException.class)
                    .rootCause()
                    .hasMessageContaining("missing or shorter than 32 bytes");
        }
    }

    @Test
    @DisplayName("Startup fails when JWT_SECRET is empty string and allow-insecure-dev-secret is false")
    void startupFailsWhenSecretIsEmptyString() {
        try (AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext()) {
            TestPropertyValues.of(
                    "JWT_SECRET=",
                    "ticketing.gateway.allow-insecure-dev-secret=false"
            ).applyTo(context);
            context.register(JwtProperties.class);

            assertThatThrownBy(context::refresh)
                    .hasRootCauseInstanceOf(IllegalStateException.class)
                    .rootCause()
                    .hasMessageContaining("missing or shorter than 32 bytes");
        }
    }

    @Test
    @DisplayName("Startup succeeds with dev default when allow-insecure-dev-secret is true")
    void startupSucceedsWithDevFallbackWhenAllowed() {
        try (AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext()) {
            TestPropertyValues.of(
                    "JWT_SECRET=short",
                    "ticketing.gateway.allow-insecure-dev-secret=true"
            ).applyTo(context);
            context.register(JwtProperties.class);
            context.refresh();

            JwtProperties props = context.getBean(JwtProperties.class);
            assertThat(props.getEffectiveSecret()).isEqualTo(JwtProperties.DEV_FALLBACK_SECRET);
        }
    }

    @Test
    @DisplayName("Startup fails when prod profile is active and dev-token.enabled is true")
    void startupFailsWhenProdProfileWithDevTokenEnabled() {
        try (AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext()) {
            context.getEnvironment().setActiveProfiles("prod");
            TestPropertyValues.of(
                    "ticketing.gateway.dev-token.enabled=true"
            ).applyTo(context);
            context.register(DevTokenProperties.class);

            assertThatThrownBy(context::refresh)
                    .hasRootCauseInstanceOf(IllegalStateException.class)
                    .rootCause()
                    .hasMessageContaining("Dev token endpoint");
        }
    }

    @Test
    @DisplayName("Startup fails when prod profile is active and allow-insecure-dev-secret is true")
    void startupFailsWhenProdProfileWithInsecureDevSecretAllowed() {
        try (AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext()) {
            context.getEnvironment().setActiveProfiles("prod");
            TestPropertyValues.of(
                    "JWT_SECRET=12345678901234567890123456789012",
                    "ticketing.gateway.allow-insecure-dev-secret=true"
            ).applyTo(context);
            context.register(JwtProperties.class);

            assertThatThrownBy(context::refresh)
                    .hasRootCauseInstanceOf(IllegalStateException.class)
                    .rootCause()
                    .hasMessageContaining("allow-insecure-dev-secret must NEVER be true when prod profile is active");
        }
    }

    @Test
    @DisplayName("Startup fails when prod profile is active and secret is short")
    void startupFailsWhenProdProfileWithShortSecret() {
        try (AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext()) {
            context.getEnvironment().setActiveProfiles("prod");
            TestPropertyValues.of(
                    "JWT_SECRET=short",
                    "ticketing.gateway.allow-insecure-dev-secret=false"
            ).applyTo(context);
            context.register(JwtProperties.class);

            assertThatThrownBy(context::refresh)
                    .hasRootCauseInstanceOf(IllegalStateException.class)
                    .rootCause()
                    .hasMessageContaining("JWT_SECRET must be at least 32 UTF-8 bytes when prod profile is active");
        }
    }

    @Test
    @DisplayName("Startup fails when trusted-proxies contains invalid CIDRs")
    void invalidCidrEntriesFailStartup() {
        List<String> invalidCidrs = List.of("10.0.0.0/99", "10.0.0.0/-1", "10.0.0.0/8/8", "garbage");
        for (String invalidCidr : invalidCidrs) {
            try (AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext()) {
                TestPropertyValues.of(
                        "ticketing.gateway.trusted-proxies=" + invalidCidr
                ).applyTo(context);
                context.register(ClientAddressResolver.class);

                assertThatThrownBy(context::refresh)
                        .hasRootCauseInstanceOf(IllegalArgumentException.class);
            }
        }
    }
}
