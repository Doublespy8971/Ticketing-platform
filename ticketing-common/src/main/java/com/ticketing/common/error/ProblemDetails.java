package com.ticketing.common.error;

import com.fasterxml.jackson.annotation.JsonFormat;

import java.time.Instant;
import java.util.List;

/** RFC 7807 problem details body (docs/arch/06-api.md, section 7.2). */
public record ProblemDetails(
        String type,
        String title,
        int status,
        String detail,
        String instance,
        String errorCode,
        String correlationId,
        @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'", timezone = "UTC")
        Instant timestamp,
        List<FieldViolation> errors) {

    public static final String TYPE_BASE = "https://api.ticketing.internal/errors/";
    public static final String MEDIA_TYPE = "application/problem+json";

    /** A single validation failure. */
    public record FieldViolation(String field, String message) {
    }

    public static ProblemDetails of(ErrorCode code, String detail, String instance, String correlationId,
                                    List<FieldViolation> errors) {
        return new ProblemDetails(TYPE_BASE + code.slug(), code.title(), code.httpStatus(), detail, instance,
                code.name(), correlationId, Instant.now(), errors == null ? List.of() : errors);
    }
}
