package com.ticketing.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ticketing.common.events.EventEnvelope;
import com.ticketing.common.json.ObjectMapperFactory;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;

import java.nio.charset.StandardCharsets;
import java.util.Objects;

/**
 * Parses and validates raw {@code String} Kafka record values into {@link EventEnvelope} instances
 * using the shared {@link ObjectMapperFactory} configuration.
 * <p>
 * Consumer factories use plain {@code StringDeserializer} (never {@code JsonDeserializer} or
 * {@code ErrorHandlingDeserializer}) so that malformed JSON, unknown {@code eventType} values,
 * missing envelope fields, or unsupported {@code schemaVersion != 1} fail inside the listener with
 * {@link NonRetryableEventException} and preserve the original raw {@code String} payload on the
 * dead-letter topic.
 */
public class ConsumerEnvelopeParser {

    public static final int SUPPORTED_SCHEMA_VERSION = 1;
    public static final String EVENT_TYPE_HEADER = "event-type";

    private static final TypeReference<EventEnvelope<JsonNode>> ENVELOPE_JSON_NODE_TYPE =
            new TypeReference<>() {
            };

    private final ObjectMapper objectMapper;

    public ConsumerEnvelopeParser() {
        this(ObjectMapperFactory.create());
    }

    public ConsumerEnvelopeParser(ObjectMapper objectMapper) {
        this.objectMapper = Objects.requireNonNull(objectMapper, "objectMapper must not be null");
    }

    /**
     * Parses a raw JSON string into an {@link EventEnvelope} with a {@link JsonNode} payload, validating
     * that all required envelope fields are present, {@code eventType} is in the {@link ConsumerMetrics#WHITELISTED_EVENT_TYPES}
     * whitelist, and {@code schemaVersion == 1}.
     *
     * @throws NonRetryableEventException if the JSON is malformed or any envelope invariant is violated
     */
    public EventEnvelope<JsonNode> parse(String rawJson) {
        if (rawJson == null || rawJson.isBlank()) {
            throw new NonRetryableEventException("Event envelope JSON payload must not be null or blank");
        }
        final JsonNode root;
        try {
            root = objectMapper.readTree(rawJson);
        } catch (JsonProcessingException e) {
            throw new NonRetryableEventException("Malformed JSON in event envelope: " + e.getOriginalMessage(), e);
        }
        if (root == null || !root.isObject()) {
            throw new NonRetryableEventException("Event envelope JSON root must be a JSON object");
        }

        JsonNode schemaVersionNode = root.get("schemaVersion");
        if (schemaVersionNode == null || !schemaVersionNode.isIntegralNumber()) {
            throw new NonRetryableEventException("Event envelope is missing integral schemaVersion field");
        }
        int schemaVersion = schemaVersionNode.intValue();
        if (schemaVersion != SUPPORTED_SCHEMA_VERSION) {
            throw new NonRetryableEventException(
                    "Unsupported schemaVersion: " + schemaVersion + " (expected " + SUPPORTED_SCHEMA_VERSION + ")");
        }

        JsonNode eventTypeNode = root.get("eventType");
        if (eventTypeNode == null || !eventTypeNode.isTextual() || eventTypeNode.asText().isBlank()) {
            throw new NonRetryableEventException("Event envelope is missing non-blank eventType field");
        }
        String eventType = eventTypeNode.asText();
        if (!ConsumerMetrics.WHITELISTED_EVENT_TYPES.contains(eventType)) {
            throw new NonRetryableEventException("Unrecognized eventType: " + eventType);
        }

        final EventEnvelope<JsonNode> envelope;
        try {
            envelope = objectMapper.convertValue(root, ENVELOPE_JSON_NODE_TYPE);
        } catch (IllegalArgumentException e) {
            throw new NonRetryableEventException("Invalid event envelope field format: " + e.getMessage(), e);
        }

        if (envelope.eventId() == null) {
            throw new NonRetryableEventException("Event envelope is missing required eventId");
        }
        if (envelope.occurredAt() == null) {
            throw new NonRetryableEventException("Event envelope is missing required occurredAt");
        }
        if (envelope.correlationId() == null || envelope.correlationId().isBlank()) {
            throw new NonRetryableEventException("Event envelope is missing required correlationId");
        }
        if (envelope.payload() == null || envelope.payload().isNull() || envelope.payload().isMissingNode()) {
            throw new NonRetryableEventException("Event envelope is missing required payload");
        }
        return envelope;
    }

    /**
     * Parses a raw JSON string into an {@link EventEnvelope} with a typed {@code payloadType}.
     *
     * @throws NonRetryableEventException if envelope validation or payload conversion fails
     */
    public <T> EventEnvelope<T> parse(String rawJson, Class<T> payloadType) {
        Objects.requireNonNull(payloadType, "payloadType must not be null");
        EventEnvelope<JsonNode> rawEnvelope = parse(rawJson);
        final T typedPayload;
        try {
            typedPayload = objectMapper.treeToValue(rawEnvelope.payload(), payloadType);
        } catch (JsonProcessingException | IllegalArgumentException e) {
            throw new NonRetryableEventException(
                    "Failed to deserialize payload for eventType " + rawEnvelope.eventType()
                            + " into " + payloadType.getSimpleName() + ": " + e.getMessage(),
                    e);
        }
        if (typedPayload == null) {
            throw new NonRetryableEventException(
                    "Deserialized payload must not be null for eventType " + rawEnvelope.eventType());
        }
        return new EventEnvelope<>(
                rawEnvelope.eventId(),
                rawEnvelope.eventType(),
                rawEnvelope.occurredAt(),
                rawEnvelope.correlationId(),
                rawEnvelope.schemaVersion(),
                typedPayload
        );
    }

    /**
     * Best-effort extraction of the whitelisted {@code eventType} from a raw JSON string for metric tagging.
     * Never throws; returns {@code "unknown"} if the JSON is malformed or {@code eventType} is not in the whitelist.
     */
    public String extractWhitelistedEventTypeOrUnknown(String rawJson) {
        if (rawJson == null || rawJson.isBlank()) {
            return ConsumerMetrics.UNKNOWN_EVENT_TYPE;
        }
        try {
            JsonNode root = objectMapper.readTree(rawJson);
            if (root != null && root.isObject()) {
                JsonNode eventTypeNode = root.get("eventType");
                if (eventTypeNode != null && eventTypeNode.isTextual()) {
                    return ConsumerMetrics.sanitizeEventType(eventTypeNode.asText());
                }
            }
        } catch (Exception ignored) {
            // Malformed JSON maps to "unknown"
        }
        return ConsumerMetrics.UNKNOWN_EVENT_TYPE;
    }

    /**
     * Best-effort extraction of the whitelisted {@code eventType} from a {@link ConsumerRecord} for metric tagging.
     * Checks the {@code event-type} header first and falls back to inspecting the JSON body.
     * Never throws; returns {@code "unknown"} if unrecognized or unparseable.
     */
    public String extractWhitelistedEventTypeOrUnknown(ConsumerRecord<?, ?> record) {
        if (record == null) {
            return ConsumerMetrics.UNKNOWN_EVENT_TYPE;
        }
        if (record.headers() != null) {
            Header header = record.headers().lastHeader(EVENT_TYPE_HEADER);
            if (header != null && header.value() != null) {
                String headerValue = new String(header.value(), StandardCharsets.UTF_8);
                String sanitized = ConsumerMetrics.sanitizeEventType(headerValue);
                if (!ConsumerMetrics.UNKNOWN_EVENT_TYPE.equals(sanitized)) {
                    return sanitized;
                }
            }
        }
        Object value = record.value();
        if (value instanceof String rawJson) {
            return extractWhitelistedEventTypeOrUnknown(rawJson);
        }
        if (value instanceof byte[] bytes) {
            return extractWhitelistedEventTypeOrUnknown(new String(bytes, StandardCharsets.UTF_8));
        }
        return ConsumerMetrics.UNKNOWN_EVENT_TYPE;
    }

    public ObjectMapper getObjectMapper() {
        return objectMapper;
    }
}

