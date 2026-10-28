package com.ticketing.common.autoconfigure;

import com.ticketing.common.correlation.CorrelationRecordInterceptor;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.listener.RecordInterceptor;

/** Registers the correlation interceptor; Spring Boot applies a RecordInterceptor bean to listener containers. */
@AutoConfiguration
@ConditionalOnClass(RecordInterceptor.class)
public class CommonKafkaAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean(RecordInterceptor.class)
    public CorrelationRecordInterceptor<Object, Object> correlationRecordInterceptor() {
        return new CorrelationRecordInterceptor<>();
    }
}
