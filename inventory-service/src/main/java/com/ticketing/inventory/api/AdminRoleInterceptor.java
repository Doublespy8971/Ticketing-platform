package com.ticketing.inventory.api;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.HandlerInterceptor;

import java.util.Arrays;

@Component
public class AdminRoleInterceptor implements HandlerInterceptor {

    public static final String X_USER_ROLES_HEADER = "X-User-Roles";
    public static final String ADMIN_ROLE = "ADMIN";

    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) {
        String rolesHeader = request.getHeader(X_USER_ROLES_HEADER);
        if (rolesHeader == null || rolesHeader.isBlank()) {
            throw new UnauthorizedException("Missing authentication credentials (" + X_USER_ROLES_HEADER + ").");
        }

        boolean hasAdmin = Arrays.stream(rolesHeader.split(","))
                .map(String::trim)
                .anyMatch(role -> role.equalsIgnoreCase(ADMIN_ROLE));

        if (!hasAdmin) {
            throw new ForbiddenException("Caller lacks required " + ADMIN_ROLE + " role.");
        }

        return true;
    }
}

