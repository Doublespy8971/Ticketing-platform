package com.ticketing.inventory.config;

import com.ticketing.inventory.api.AdminRoleInterceptor;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

import java.util.Objects;

@Configuration
public class WebMvcConfig implements WebMvcConfigurer {

    private final AdminRoleInterceptor adminRoleInterceptor;

    public WebMvcConfig(AdminRoleInterceptor adminRoleInterceptor) {
        this.adminRoleInterceptor = Objects.requireNonNull(adminRoleInterceptor, "adminRoleInterceptor must not be null");
    }

    @Override
    public void addInterceptors(InterceptorRegistry registry) {
        registry.addInterceptor(adminRoleInterceptor)
                .addPathPatterns("/api/v1/admin/**");
    }
}

