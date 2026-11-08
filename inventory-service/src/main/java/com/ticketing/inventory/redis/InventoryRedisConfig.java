package com.ticketing.inventory.redis;

import io.lettuce.core.ClientOptions;
import io.lettuce.core.SocketOptions;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.data.redis.LettuceClientConfigurationBuilderCustomizer;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.scripting.support.ResourceScriptSource;

import java.time.Clock;
import java.time.Duration;

@Configuration
@EnableConfigurationProperties(InventoryRedisProperties.class)
public class InventoryRedisConfig {

    private static final Duration SOCKET_CONNECT_TIMEOUT = Duration.ofMillis(500);

    @Bean
    public LettuceClientConfigurationBuilderCustomizer inventoryLettuceCustomizer(
            InventoryRedisProperties properties) {
        return builder -> builder
                .commandTimeout(properties.getCommandTimeout())
                .clientOptions(ClientOptions.builder()
                        .socketOptions(SocketOptions.builder()
                                .connectTimeout(SOCKET_CONNECT_TIMEOUT)
                                .build())
                        .build());
    }

    @Bean
    public DefaultRedisScript<String> holdInventoryScript() {
        DefaultRedisScript<String> script = new DefaultRedisScript<>();
        script.setScriptSource(new ResourceScriptSource(new ClassPathResource("redis/hold_inventory.lua")));
        script.setResultType(String.class);
        return script;
    }

    @Bean
    public DefaultRedisScript<Long> releaseInventoryScript() {
        DefaultRedisScript<Long> script = new DefaultRedisScript<>();
        script.setScriptSource(new ResourceScriptSource(new ClassPathResource("redis/release_inventory.lua")));
        script.setResultType(Long.class);
        return script;
    }

    @Bean
    public DefaultRedisScript<String> confirmInventoryScript() {
        DefaultRedisScript<String> script = new DefaultRedisScript<>();
        script.setScriptSource(new ResourceScriptSource(new ClassPathResource("redis/confirm_inventory.lua")));
        script.setResultType(String.class);
        return script;
    }

    @Bean
    @ConditionalOnMissingBean(Clock.class)
    public Clock inventoryClock() {
        return Clock.systemUTC();
    }
}

