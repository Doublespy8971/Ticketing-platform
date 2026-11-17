package com.ticketing.e2e.support;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Scrapes {@code notification-service} Prometheus metrics from {@code :8084/metrics}
 * and exposes per-{@code (event_type, outcome)} counter snapshots and diffs.
 */
public final class Notifications implements InvariantAuditor.NotificationAccess {

    private static final Pattern LABEL_PATTERN = Pattern.compile("(\\w+)=\"([^\"]*)\"");

    private final E2EConfig config;
    private final HttpClient httpClient;

    public Notifications(E2EConfig config) {
        this.config = config;
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(3))
                .build();
    }

    public record CounterKey(String eventType, String outcome) {}

    public record NotificationSnapshot(Map<CounterKey, Long> counters) {
        public static NotificationSnapshot empty() {
            return new NotificationSnapshot(Map.of());
        }

        public static NotificationSnapshot ofCounts(long confirmedSent, long cancelledSent, long duplicateCount, long failedCount) {
            Map<CounterKey, Long> map = new LinkedHashMap<>();
            map.put(new CounterKey("order.confirmed", "sent"), confirmedSent);
            map.put(new CounterKey("order.cancelled", "sent"), cancelledSent);
            if (duplicateCount != 0L) {
                map.put(new CounterKey("order.confirmed", "duplicate"), duplicateCount);
            }
            if (failedCount != 0L) {
                map.put(new CounterKey("order.confirmed", "failed"), failedCount);
            }
            return new NotificationSnapshot(Collections.unmodifiableMap(map));
        }

        public long count(String eventType, String outcome) {
            return counters.getOrDefault(new CounterKey(eventType, outcome), 0L);
        }

        public long sent(String eventType) {
            return count(eventType, "sent");
        }

        public long duplicate(String eventType) {
            return count(eventType, "duplicate");
        }

        public long failed(String eventType) {
            return count(eventType, "failed");
        }

        public long totalSent() {
            return sent("order.confirmed") + sent("order.cancelled");
        }

        public long totalDuplicate() {
            return counters.entrySet().stream()
                    .filter(e -> "duplicate".equals(e.getKey().outcome()))
                    .mapToLong(Map.Entry::getValue)
                    .sum();
        }

        public long totalFailed() {
            return counters.entrySet().stream()
                    .filter(e -> "failed".equals(e.getKey().outcome()))
                    .mapToLong(Map.Entry::getValue)
                    .sum();
        }
    }

    @Override
    public NotificationSnapshot snapshot() {
        String url = config.notificationBaseUrl() + "/metrics";
        HttpRequest req = HttpRequest.newBuilder(URI.create(url))
                .timeout(Duration.ofSeconds(5))
                .GET()
                .build();
        try {
            HttpResponse<String> resp = httpClient.send(req, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
            if (resp.statusCode() != 200) {
                throw new IllegalStateException("notification-service /metrics returned HTTP " + resp.statusCode());
            }
            return parsePrometheusText(resp.body());
        } catch (Exception e) {
            throw new IllegalStateException("Failed to fetch notification-service /metrics from " + url, e);
        }
    }

    public NotificationSnapshot diff(NotificationSnapshot before) {
        return diff(before, snapshot());
    }

    /**
     * Computes the signed difference {@code after - before} for every counter key.
     * Never clamps negative values to 0 so counter resets are preserved and detected.
     */
    public static NotificationSnapshot diff(NotificationSnapshot before, NotificationSnapshot after) {
        Set<CounterKey> allKeys = new HashSet<>();
        if (before != null) {
            allKeys.addAll(before.counters().keySet());
        }
        if (after != null) {
            allKeys.addAll(after.counters().keySet());
        }
        Map<CounterKey, Long> deltaMap = new LinkedHashMap<>();
        for (CounterKey key : allKeys) {
            long b = before != null ? before.counters().getOrDefault(key, 0L) : 0L;
            long a = after != null ? after.counters().getOrDefault(key, 0L) : 0L;
            deltaMap.put(key, a - b);
        }
        return new NotificationSnapshot(Collections.unmodifiableMap(deltaMap));
    }

    /**
     * Detects any counter that decreased between {@code before} and {@code after}
     * (indicating an unexpected {@code notification-service} restart or counter reset).
     */
    public static List<String> findCounterResets(NotificationSnapshot before, NotificationSnapshot after) {
        if (before == null || after == null) {
            return List.of();
        }
        List<String> resets = new ArrayList<>();
        for (Map.Entry<CounterKey, Long> entry : before.counters().entrySet()) {
            CounterKey key = entry.getKey();
            long b = entry.getValue() != null ? entry.getValue() : 0L;
            long a = after.counters().getOrDefault(key, 0L);
            if (a < b) {
                resets.add(String.format(
                        "notification-service counter reset detected for {event_type=%s, outcome=%s}: before=%d, after=%d (delta=%d)",
                        key.eventType(), key.outcome(), b, a, a - b));
            }
        }
        return resets;
    }

    public static NotificationSnapshot parsePrometheusText(String body) {
        Map<CounterKey, Long> map = new LinkedHashMap<>();
        if (body == null || body.isBlank()) {
            return new NotificationSnapshot(Collections.unmodifiableMap(map));
        }
        for (String rawLine : body.split("\n")) {
            String line = rawLine.trim();
            if (line.isEmpty() || line.startsWith("#")) {
                continue;
            }
            if (!line.startsWith("notification_events_total{")) {
                continue;
            }
            int openBrace = line.indexOf('{');
            int closeBrace = line.indexOf('}');
            if (openBrace < 0 || closeBrace <= openBrace) {
                continue;
            }
            String labelsPart = line.substring(openBrace + 1, closeBrace);
            String valuePart = line.substring(closeBrace + 1).trim();

            String eventType = null;
            String outcome = null;
            Matcher m = LABEL_PATTERN.matcher(labelsPart);
            while (m.find()) {
                String k = m.group(1);
                String v = m.group(2);
                if ("event_type".equals(k)) {
                    eventType = v;
                } else if ("outcome".equals(k)) {
                    outcome = v;
                }
            }
            if (eventType != null && outcome != null && !valuePart.isEmpty()) {
                String[] tokens = valuePart.split("\\s+");
                try {
                    long val = Math.round(Double.parseDouble(tokens[0]));
                    map.put(new CounterKey(eventType, outcome), val);
                } catch (NumberFormatException ignored) {
                }
            }
        }
        return new NotificationSnapshot(Collections.unmodifiableMap(map));
    }
}
