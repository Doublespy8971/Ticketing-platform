package com.ticketing.gateway.config;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class StartupFailureTest {

    @Test
    @DisplayName("Startup fails when secret is shorter than 32 bytes and allow-insecure-dev-secret is false")
    void startupFailsWithShortSecretAndNoInsecureFlag() {
        try (AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext()) {
            TestPropertyValues.of(
                    "JWT_SECRET=too-short-secret",
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
    @DisplayName("Startup fails when secret is missing and allow-insecure-dev-secret is false")
    void startupFailsWithMissingSecretAndNoInsecureFlag() {
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
    @DisplayName("Startup succeeds when secret is at least 32 bytes")
    void startupSucceedsWithValidSecret() {
        try (AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext()) {
            TestPropertyValues.of(
                    "JWT_SECRET=this-is-a-valid-secret-key-that-is-at-least-32-bytes-long",
                    "ticketing.gateway.allow-insecure-dev-secret=false"
            ).applyTo(context);
            context.register(JwtProperties.class);
            context.refresh();

            JwtProperties props = context.getBean(JwtProperties.class);
            assertThat(props.getEffectiveSecret()).isEqualTo("this-is-a-valid-secret-key-that-is-at-least-32-bytes-long");
        }
    }
}
