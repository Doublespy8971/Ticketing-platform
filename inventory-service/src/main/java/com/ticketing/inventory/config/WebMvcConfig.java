package com.ticketing.inventory.config;

import com.ticketing.common.correlation.CorrelationConstants;
import com.ticketing.inventory.api.AdminRoleInterceptor;
import com.ticketing.inventory.api.ReservationController;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.HandlerInterceptor;
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

        registry.addInterceptor(new HandlerInterceptor() {
                    @Override
                    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) {
                        ReservationController.parseAndValidateUserId(
                                request.getHeader(CorrelationConstants.USER_ID_HEADER),
                                request.getHeader(AdminRoleInterceptor.X_USER_ROLES_HEADER)
                        );
                        if ("POST".equalsIgnoreCase(request.getMethod())) {
                            ReservationController.parseAndValidateIdempotencyKey(
                                    request.getHeader(ReservationController.IDEMPOTENCY_KEY_HEADER)
                            );
                        }
                        return true;
                    }
                })
                .addPathPatterns("/api/v1/reservations", "/api/v1/reservations/**");
    }
}


