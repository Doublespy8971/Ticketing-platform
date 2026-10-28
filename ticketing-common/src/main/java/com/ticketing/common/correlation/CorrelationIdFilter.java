package com.ticketing.common.correlation;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.MDC;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.util.UUID;

/**
 * Reads {@code X-Correlation-Id} (generating a UUIDv4 when missing or not a valid UUID), binds it and
 * {@code X-User-Id} to the MDC, and echoes the correlation id on the response.
 */
public class CorrelationIdFilter extends OncePerRequestFilter {

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain chain)
            throws ServletException, IOException {
        String correlationId = resolve(request.getHeader(CorrelationConstants.HTTP_HEADER));
        String userId = request.getHeader(CorrelationConstants.USER_ID_HEADER);

        MDC.put(CorrelationConstants.MDC_KEY, correlationId);
        if (userId != null && !userId.isBlank()) {
            MDC.put(CorrelationConstants.MDC_USER_ID_KEY, userId);
        }
        response.setHeader(CorrelationConstants.HTTP_HEADER, correlationId);
        try {
            chain.doFilter(request, response);
        } finally {
            MDC.remove(CorrelationConstants.MDC_KEY);
            MDC.remove(CorrelationConstants.MDC_USER_ID_KEY);
        }
    }

    static String resolve(String incoming) {
        if (incoming != null) {
            try {
                return UUID.fromString(incoming.trim()).toString();
            } catch (IllegalArgumentException ignored) {
                // fall through and generate a new id
            }
        }
        return UUID.randomUUID().toString();
    }
}
