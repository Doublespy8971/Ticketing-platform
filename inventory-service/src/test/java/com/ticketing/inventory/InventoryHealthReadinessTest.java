package com.ticketing.inventory;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.event.ApplicationEventsTestExecutionListener;
import org.springframework.test.context.event.EventPublishingTestExecutionListener;
import org.springframework.test.context.jdbc.SqlScriptsTestExecutionListener;
import org.springframework.test.context.support.DependencyInjectionTestExecutionListener;
import org.springframework.test.context.support.DirtiesContextBeforeModesTestExecutionListener;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.test.context.transaction.TransactionalTestExecutionListener;
import org.springframework.test.context.web.ServletTestExecutionListener;
import org.springframework.test.web.servlet.MockMvc;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.logging.Logger;

import static org.hamcrest.Matchers.is;
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
@TestExecutionListeners(listeners = {
        ServletTestExecutionListener.class,
        DirtiesContextBeforeModesTestExecutionListener.class,
        ApplicationEventsTestExecutionListener.class,
        DependencyInjectionTestExecutionListener.class,
        DirtiesContextTestExecutionListener.class,
        TransactionalTestExecutionListener.class,
        SqlScriptsTestExecutionListener.class,
        EventPublishingTestExecutionListener.class
})
public class InventoryHealthReadinessTest {

    static class StubDataSource implements DataSource {
        @Override
        public Connection getConnection() throws SQLException {
            return (Connection) Proxy.newProxyInstance(
                    Connection.class.getClassLoader(),
                    new Class<?>[] { Connection.class },
                    (proxy, method, args) -> {
                        if (method.getName().equals("getMetaData")) {
                            return Proxy.newProxyInstance(
                                    DatabaseMetaData.class.getClassLoader(),
                                    new Class<?>[] { DatabaseMetaData.class },
                                    (p2, m2, a2) -> {
                                        if (m2.getName().equals("getDatabaseProductName"))
                                            return "PostgreSQL";
                                        if (m2.getName().equals("getDatabaseProductVersion"))
                                            return "16.0";
                                        if (m2.getReturnType().equals(boolean.class))
                                            return false;
                                        if (m2.getReturnType().equals(int.class))
                                            return 0;
                                        return null;
                                    });
                        }
                        if (method.getReturnType().equals(boolean.class))
                            return false;
                        if (method.getReturnType().equals(int.class))
                            return 0;
                        return null;
                    });
        }

        @Override
        public Connection getConnection(String username, String password) throws SQLException {
            return getConnection();
        }

        @Override
        public PrintWriter getLogWriter() {
            return null;
        }

        @Override
        public void setLogWriter(PrintWriter out) {
        }

        @Override
        public void setLoginTimeout(int seconds) {
        }

        @Override
        public int getLoginTimeout() {
            return 0;
        }

        @Override
        public Logger getParentLogger() {
            return Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);
        }

        @Override
        public <T> T unwrap(Class<T> iface) {
            return null;
        }

        @Override
        public boolean isWrapperFor(Class<?> iface) {
            return false;
        }
    }

    @TestConfiguration
    static class TestHealthConfig {
        @Bean
        @Primary
        public DataSource dataSource() {
            return new StubDataSource();
        }

        @Bean(name = { "redisHealthIndicator", "redisHealthContributor" })
        @Primary
        public HealthIndicator redisHealthIndicator() {
            return () -> Health.down().withDetail("error", "Simulated Redis outage").build();
        }

        @Bean(name = { "dbHealthIndicator", "dbHealthContributor" })
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
