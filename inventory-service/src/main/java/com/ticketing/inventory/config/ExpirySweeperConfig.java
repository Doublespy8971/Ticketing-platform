package com.ticketing.inventory.config;

import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

/**
 * Configures a dedicated single-thread {@link ThreadPoolTaskScheduler} for the reservation
 * hold expiry sweeper, isolated from {@code ticketingOutboxScheduler} so a slow sweep never
 * delays the 200 ms outbox polling publisher.
 */
@Configuration
@EnableConfigurationProperties(ExpirySweeperProperties.class)
public class ExpirySweeperConfig {

    public static final String SCHEDULER_BEAN_NAME = "expirySweeperScheduler";

    @Bean(name = SCHEDULER_BEAN_NAME)
    @ConditionalOnMissingBean(name = SCHEDULER_BEAN_NAME)
    public ThreadPoolTaskScheduler expirySweeperScheduler() {
        ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
        scheduler.setPoolSize(1);
        scheduler.setThreadNamePrefix("expiry-sweeper-");
        scheduler.setDaemon(true);
        return scheduler;
    }

    @Configuration(proxyBeanMethods = false)
    @ConditionalOnProperty(
            prefix = "ticketing.sweeper",
            name = "enabled",
            havingValue = "true",
            matchIfMissing = true
    )
    @EnableScheduling
    static class SchedulingConfiguration {
    }
}

