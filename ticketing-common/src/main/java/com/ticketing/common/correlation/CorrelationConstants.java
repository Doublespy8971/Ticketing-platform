package com.ticketing.common.correlation;

/** Correlation-id names (docs/arch/09-conventions.md, section 10.2). */
public final class CorrelationConstants {

    /** HTTP request/response header carrying the correlation id. */
    public static final String HTTP_HEADER = "X-Correlation-Id";

    /** Kafka record header carrying the correlation id. */
    public static final String KAFKA_HEADER = "X-Correlation-Id";

    /** SLF4J MDC key for the correlation id. */
    public static final String MDC_KEY = "correlationId";

    /** SLF4J MDC key for the authenticated user id. */
    public static final String MDC_USER_ID_KEY = "userId";

    /** HTTP header with the authenticated user id propagated by gateway-service. */
    public static final String USER_ID_HEADER = "X-User-Id";

    private CorrelationConstants() {
    }
}
