package com.ticketing.inventory;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.test.web.servlet.MockMvc;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;

import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest(properties = {
        "spring.flyway.enabled=false",
        "ticketing.outbox.enabled=false",
        "ticketing.sweeper.enabled=false",
        "spring.kafka.listener.auto-startup=false"
})
@AutoConfigureMockMvc
public class InventoryHealthReadinessTest {

    @MockBean
    private DataSource dataSource;

    @TestConfiguration
    static class TestHealthConfig {
        @Bean(name = {"redisHealthIndicator", "redisHealthContributor"})
        @Primary
        public HealthIndicator redisHealthIndicator() {
            return () -> Health.down().withDetail("error", "Simulated Redis outage").build();
        }

        @Bean(name = {"dbHealthIndicator", "dbHealthContributor"})
        @Primary
        public HealthIndicator dbHealthIndicator() {
            return () -> Health.up().withDetail("database", "Mock Database").build();
        }
    }

    @Autowired
    private MockMvc mockMvc;

    @Test
    @DisplayName("When Redis is DOWN, inventory readiness probe returns 503 DOWN because hold gate fails closed")
    void readinessBecomesDownWhenRedisDown() throws Exception {
        mockMvc.perform(get("/actuator/health/readiness"))
                .andExpect(status().isServiceUnavailable())
                .andExpect(jsonPath("$.status", is("DOWN")));
    }

    @Test
    @DisplayName("When Redis is DOWN, inventory liveness probe still returns 200 UP")
    void livenessStaysUpWhenRedisDown() throws Exception {
        mockMvc.perform(get("/actuator/health/liveness"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status", is("UP")));
    }

    @Test
    @DisplayName("When Redis is DOWN, inventory aggregate health returns 503 DOWN")
    void aggregateHealthReportsDownWhenRedisDown() throws Exception {
        mockMvc.perform(get("/actuator/health"))
                .andExpect(status().isServiceUnavailable())
                .andExpect(jsonPath("$.status", is("DOWN")));
    }
}
