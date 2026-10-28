package com.ticketing.common.json;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

/** Creates the ObjectMapper used for all event envelopes (ISO-8601 UTC, microsecond precision). */
public final class ObjectMapperFactory {

    /** {@code YYYY-MM-DDTHH:mm:ss.SSSSSSZ} as required by the event envelope contract. */
    public static final DateTimeFormatter ISO_MICROS_UTC =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'").withZone(ZoneOffset.UTC);

    private ObjectMapperFactory() {
    }

    public static ObjectMapper create() {
        SimpleModule instantModule = new SimpleModule("ticketing-instant");
        instantModule.addSerializer(Instant.class, new JsonSerializer<>() {
            @Override
            public void serialize(Instant value, JsonGenerator gen, SerializerProvider serializers) throws IOException {
                gen.writeString(ISO_MICROS_UTC.format(value));
            }
        });
        return new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .registerModule(instantModule)
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
                .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
    }
}
