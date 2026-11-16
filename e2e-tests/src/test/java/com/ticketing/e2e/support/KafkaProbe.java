package com.ticketing.e2e.support;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Kafka inspection and probing helper using {@link AdminClient}, {@link KafkaConsumer}, and {@link KafkaProducer}.
 */
public final class KafkaProbe implements AutoCloseable {

    public static final String TOPIC_INVENTORY_EVENTS = "inventory.events";
    public static final String TOPIC_ORDER_EVENTS = "order.events";
    public static final String TOPIC_PAYMENT_EVENTS = "payment.events";

    public static final List<String> MAIN_TOPICS = List.of(
            TOPIC_INVENTORY_EVENTS,
            TOPIC_ORDER_EVENTS,
            TOPIC_PAYMENT_EVENTS
    );

    public static final String CG_ORDER_INVENTORY = "order-service-inventory-cg";
    public static final String CG_PAYMENT_ORDER = "payment-service-order-cg";
    public static final String CG_NOTIFICATION_ORDER = "notification-service-order-cg";
    public static final String CG_INVENTORY_PAYMENT = "inventory-service-payment-cg";
    public static final String CG_ORDER_PAYMENT = "order-service-payment-cg";

    public static final Map<String, String> GROUP_TO_TOPIC = createGroupToTopicMap();

    public static final List<String> RETRY_TOPICS = List.of(
            "inventory.events-order-service-inventory-cg-retry-0",
            "inventory.events-order-service-inventory-cg-retry-1",
            "order.events-payment-service-order-cg-retry-0",
            "order.events-payment-service-order-cg-retry-1",
            "payment.events-inventory-service-payment-cg-retry-0",
            "payment.events-inventory-service-payment-cg-retry-1",
            "payment.events-order-service-payment-cg-retry-0",
            "payment.events-order-service-payment-cg-retry-1"
    );

    public static final List<String> DLT_TOPICS = List.of(
            "inventory.events-order-service-inventory-cg-dlt",
            "order.events-payment-service-order-cg-dlt",
            "payment.events-inventory-service-payment-cg-dlt",
            "payment.events-order-service-payment-cg-dlt"
    );

    private static Map<String, String> createGroupToTopicMap() {
        Map<String, String> map = new LinkedHashMap<>();
        // Main consumer groups
        map.put(CG_ORDER_INVENTORY, TOPIC_INVENTORY_EVENTS);
        map.put(CG_PAYMENT_ORDER, TOPIC_ORDER_EVENTS);
        map.put(CG_NOTIFICATION_ORDER, TOPIC_ORDER_EVENTS);
        map.put(CG_INVENTORY_PAYMENT, TOPIC_PAYMENT_EVENTS);
        map.put(CG_ORDER_PAYMENT, TOPIC_PAYMENT_EVENTS);
        // Retry consumer groups
        map.put("order-service-inventory-cg-retry-0", "inventory.events-order-service-inventory-cg-retry-0");
        map.put("order-service-inventory-cg-retry-1", "inventory.events-order-service-inventory-cg-retry-1");
        map.put("payment-service-order-cg-retry-0", "order.events-payment-service-order-cg-retry-0");
        map.put("payment-service-order-cg-retry-1", "order.events-payment-service-order-cg-retry-1");
        map.put("inventory-service-payment-cg-retry-0", "payment.events-inventory-service-payment-cg-retry-0");
        map.put("inventory-service-payment-cg-retry-1", "payment.events-inventory-service-payment-cg-retry-1");
        map.put("order-service-payment-cg-retry-0", "payment.events-order-service-payment-cg-retry-0");
        map.put("order-service-payment-cg-retry-1", "payment.events-order-service-payment-cg-retry-1");
        return Collections.unmodifiableMap(map);
    }

    private final E2EConfig config;
    private final ObjectMapper objectMapper;
    private volatile AdminClient adminClient;

    public KafkaProbe(E2EConfig config) {
        this.config = config;
        this.objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
        this.adminClient = createAdminClient();
    }

    private AdminClient createAdminClient() {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, config.kafkaBootstrapServers());
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "5000");
        props.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "8000");
        return AdminClient.create(props);
    }

    public synchronized void resetAdminClient() {
        if (adminClient != null) {
            try {
                adminClient.close(Duration.ofSeconds(1));
            } catch (Exception ignored) {
            }
        }
        adminClient = createAdminClient();
    }

    public void checkConnectivity() {
        try {
            Set<String> topics = adminClient.listTopics().names().get(5, TimeUnit.SECONDS);
            for (String required : MAIN_TOPICS) {
                if (!topics.contains(required)) {
                    throw new AssertionError("Required Kafka topic '" + required + "' does not exist on "
                            + config.kafkaBootstrapServers() + " (found: " + topics + ")");
                }
            }
        } catch (AssertionError e) {
            throw e;
        } catch (Exception e) {
            throw new AssertionError("Kafka broker is unreachable at " + config.kafkaBootstrapServers()
                    + ": " + e.getMessage(), e);
        }
    }

    public record ConsumedEvent(
            String topic,
            int partition,
            long offset,
            String key,
            String rawValue,
            Map<String, String> headers,
            UUID eventId,
            String eventType,
            String correlationId,
            JsonNode payload
    ) {}

    public record GroupLag(
            String groupId,
            String topic,
            long totalLag,
            Map<Integer, Long> partitionLag
    ) {}

    public record DltSnapshot(Map<TopicPartition, Long> offsets) {
        public long totalMessages() {
            return offsets.values().stream().mapToLong(Long::longValue).sum();
        }
    }

    public record DltDiff(
            long totalDelta,
            Map<String, Long> deltaByTopic,
            Map<TopicPartition, Long> deltaByPartition
    ) {}

    public Map<TopicPartition, Long> endOffsets(String topic) {
        return endOffsets(List.of(topic));
    }

    public Map<TopicPartition, Long> endOffsets(Collection<String> topics) {
        try {
            return doEndOffsets(topics);
        } catch (Exception first) {
            resetAdminClient();
            try {
                return doEndOffsets(topics);
            } catch (Exception second) {
                throw new IllegalStateException("Failed to fetch end offsets for topics " + topics, second);
            }
        }
    }

    private Map<TopicPartition, Long> doEndOffsets(Collection<String> topics) throws Exception {
        Map<String, TopicDescription> descriptions = adminClient.describeTopics(topics).allTopicNames().get(6, TimeUnit.SECONDS);
        Map<TopicPartition, OffsetSpec> request = new LinkedHashMap<>();
        for (Map.Entry<String, TopicDescription> entry : descriptions.entrySet()) {
            for (TopicPartitionInfo pInfo : entry.getValue().partitions()) {
                request.put(new TopicPartition(entry.getKey(), pInfo.partition()), OffsetSpec.latest());
            }
        }
        Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> res =
                adminClient.listOffsets(request).all().get(6, TimeUnit.SECONDS);
        Map<TopicPartition, Long> offsets = new LinkedHashMap<>();
        for (Map.Entry<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> e : res.entrySet()) {
            offsets.put(e.getKey(), e.getValue().offset());
        }
        return offsets;
    }

    public long totalEndOffset(String topic) {
        return endOffsets(topic).values().stream().mapToLong(Long::longValue).sum();
    }

    public DltSnapshot snapshotDltOffsets() {
        return new DltSnapshot(endOffsets(DLT_TOPICS));
    }

    public DltDiff diffDltOffsets(DltSnapshot before) {
        Map<TopicPartition, Long> after = endOffsets(DLT_TOPICS);
        Map<String, Long> byTopic = new LinkedHashMap<>();
        Map<TopicPartition, Long> byPartition = new LinkedHashMap<>();
        long total = 0L;

        for (String dlt : DLT_TOPICS) {
            byTopic.put(dlt, 0L);
        }
        for (Map.Entry<TopicPartition, Long> entry : after.entrySet()) {
            TopicPartition tp = entry.getKey();
            long beforeVal = before.offsets().getOrDefault(tp, 0L);
            long delta = Math.max(0L, entry.getValue() - beforeVal);
            if (delta > 0) {
                byPartition.put(tp, delta);
                byTopic.merge(tp.topic(), delta, Long::sum);
                total += delta;
            }
        }
        return new DltDiff(total, byTopic, byPartition);
    }

    /**
     * Reads all records on {@code topic} between {@code startOffsets} (inclusive) and {@code endOffsets} (exclusive).
     */
    public List<ConsumedEvent> readRange(
            String topic,
            Map<TopicPartition, Long> startOffsets,
            Map<TopicPartition, Long> endOffsets) {
        List<TopicPartition> activePartitions = new ArrayList<>();
        for (Map.Entry<TopicPartition, Long> entry : endOffsets.entrySet()) {
            TopicPartition tp = entry.getKey();
            if (!tp.topic().equals(topic)) {
                continue;
            }
            long start = startOffsets != null ? startOffsets.getOrDefault(tp, 0L) : 0L;
            if (entry.getValue() > start) {
                activePartitions.add(tp);
            }
        }
        if (activePartitions.isEmpty()) {
            return List.of();
        }

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.kafkaBootstrapServers());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "500");

        List<ConsumedEvent> results = new ArrayList<>();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.assign(activePartitions);
            for (TopicPartition tp : activePartitions) {
                long start = startOffsets != null ? startOffsets.getOrDefault(tp, 0L) : 0L;
                consumer.seek(tp, start);
            }

            long deadlineMs = System.currentTimeMillis() + 8_000L;
            while (System.currentTimeMillis() < deadlineMs) {
                boolean allDone = true;
                for (TopicPartition tp : activePartitions) {
                    if (consumer.position(tp) < endOffsets.get(tp)) {
                        allDone = false;
                        break;
                    }
                }
                if (allDone) {
                    break;
                }
                ConsumerRecords<String, String> polled = consumer.poll(Duration.ofMillis(200));
                for (ConsumerRecord<String, String> rec : polled) {
                    TopicPartition tp = new TopicPartition(rec.topic(), rec.partition());
                    long targetEnd = endOffsets.getOrDefault(tp, 0L);
                    if (rec.offset() < targetEnd) {
                        results.add(parseRecord(rec));
                    }
                }
            }
        }
        return results;
    }

    public List<ConsumedEvent> readSince(String topic, Map<TopicPartition, Long> startOffsets) {
        Map<TopicPartition, Long> latest = endOffsets(topic);
        return readRange(topic, startOffsets, latest);
    }

    public long countByKey(String topic, Map<TopicPartition, Long> startOffsets, String key, String optionalEventType) {
        return readSince(topic, startOffsets).stream()
                .filter(e -> key.equals(e.key()))
                .filter(e -> optionalEventType == null || optionalEventType.equals(e.eventType()))
                .count();
    }

    public long countByEventId(String topic, Map<TopicPartition, Long> startOffsets, UUID eventId) {
        return readSince(topic, startOffsets).stream()
                .filter(e -> eventId.equals(e.eventId()))
                .count();
    }

    public GroupLag consumerGroupLag(String groupId) {
        String topic = GROUP_TO_TOPIC.get(groupId);
        if (topic == null) {
            throw new IllegalArgumentException("Unknown consumer group: " + groupId);
        }
        return allConsumerGroupsLag().get(groupId);
    }

    public long totalConsumerGroupLag(String groupId) {
        GroupLag gl = consumerGroupLag(groupId);
        return gl != null ? gl.totalLag() : 0L;
    }

    public Map<String, Long> allConsumerGroupLags() {
        Map<String, Long> map = new LinkedHashMap<>();
        allConsumerGroupsLag().forEach((k, v) -> map.put(k, v.totalLag()));
        return map;
    }

    public Map<String, GroupLag> allConsumerGroupsLag() {
        try {
            return doAllConsumerGroupsLag();
        } catch (Exception first) {
            resetAdminClient();
            try {
                return doAllConsumerGroupsLag();
            } catch (Exception second) {
                throw new IllegalStateException("Failed to inspect consumer group lags", second);
            }
        }
    }

    private Map<String, GroupLag> doAllConsumerGroupsLag() throws Exception {
        List<String> allTopics = new ArrayList<>(MAIN_TOPICS);
        allTopics.addAll(RETRY_TOPICS);
        Map<TopicPartition, Long> latestOffsets = doEndOffsets(allTopics);

        Map<String, GroupLag> result = new LinkedHashMap<>();
        for (Map.Entry<String, String> entry : GROUP_TO_TOPIC.entrySet()) {
            String groupId = entry.getKey();
            String topic = entry.getValue();

            ListConsumerGroupOffsetsResult cgResult = adminClient.listConsumerGroupOffsets(groupId);
            Map<TopicPartition, OffsetAndMetadata> committed =
                    cgResult.partitionsToOffsetAndMetadata().get(5, TimeUnit.SECONDS);

            Map<Integer, Long> partitionLag = new LinkedHashMap<>();
            long totalLag = 0L;

            for (Map.Entry<TopicPartition, Long> tpEntry : latestOffsets.entrySet()) {
                TopicPartition tp = tpEntry.getKey();
                if (!tp.topic().equals(topic)) {
                    continue;
                }
                long endOffset = tpEntry.getValue();
                OffsetAndMetadata meta = committed != null ? committed.get(tp) : null;
                long committedOffset = meta != null ? meta.offset() : 0L;
                long lag = Math.max(0L, endOffset - committedOffset);
                partitionLag.put(tp.partition(), lag);
                totalLag += lag;
            }
            result.put(groupId, new GroupLag(groupId, topic, totalLag, partitionLag));
        }
        return result;
    }

    public void produceRaw(String topic, String key, String jsonValue, Map<String, String> headers) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.kafkaBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "10000");
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "5000");

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, jsonValue);
            if (headers != null) {
                for (Map.Entry<String, String> h : headers.entrySet()) {
                    if (h.getKey() != null && h.getValue() != null) {
                        record.headers().add(h.getKey(), h.getValue().getBytes(StandardCharsets.UTF_8));
                    }
                }
            }
            producer.send(record).get(10, TimeUnit.SECONDS);
            producer.flush();
        } catch (Exception e) {
            throw new IllegalStateException("Failed to produce message to topic " + topic + " with key " + key, e);
        }
    }

    private ConsumedEvent parseRecord(ConsumerRecord<String, String> rec) {
        Map<String, String> hdrs = new LinkedHashMap<>();
        for (Header h : rec.headers()) {
            if (h.value() != null) {
                hdrs.put(h.key(), new String(h.value(), StandardCharsets.UTF_8));
            }
        }
        UUID eventId = null;
        String eventType = null;
        String correlationId = null;
        JsonNode payload = null;
        if (rec.value() != null && !rec.value().isBlank()) {
            try {
                JsonNode root = objectMapper.readTree(rec.value());
                if (root != null && root.isObject()) {
                    if (root.hasNonNull("eventId")) {
                        try {
                            eventId = UUID.fromString(root.get("eventId").asText());
                        } catch (Exception ignored) {
                        }
                    }
                    if (root.hasNonNull("eventType")) {
                        eventType = root.get("eventType").asText();
                    }
                    if (root.hasNonNull("correlationId")) {
                        correlationId = root.get("correlationId").asText();
                    }
                    payload = root.get("payload");
                }
            } catch (Exception ignored) {
            }
        }
        return new ConsumedEvent(
                rec.topic(),
                rec.partition(),
                rec.offset(),
                rec.key(),
                rec.value(),
                hdrs,
                eventId,
                eventType,
                correlationId,
                payload
        );
    }

    @Override
    public void close() {
        if (adminClient != null) {
            try {
                adminClient.close(Duration.ofSeconds(2));
            } catch (Exception ignored) {
            }
        }
    }
}
