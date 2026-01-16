package com.example.prices.model;
import java.time.Instant;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;

/**
 * Immutable price record
 */
/**
 * Price record with flexible payload.
 * Payload is a Map<String, Object> for simplicity; could be JSON or a typed class in production.
 */
public final class PriceRecord {
    private final String id;
    private final Instant asOf;
    private final Map<String, Object> payload;

    public PriceRecord(String id, Instant asOf, Map<String, Object> payload) {
        this.id = Objects.requireNonNull(id, "id");
        this.asOf = Objects.requireNonNull(asOf, "asOf");
        this.payload = Collections.unmodifiableMap(Objects.requireNonNull(payload, "payload"));
    }

    public String getId() {
        return id;
    }

    public Instant getAsOf() {
        return asOf;
    }

    public Map<String, Object> getPayload() {
        return payload;
    }

    @Override
    public String toString() {
        return "PriceRecord{" +
                "id='" + id + '\'' +
                ", asOf=" + asOf +
                ", payload=" + payload +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof PriceRecord)) return false;
        PriceRecord that = (PriceRecord) o;
        return id.equals(that.id) &&
               asOf.equals(that.asOf) &&
               payload.equals(that.payload);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, asOf, payload);
    }
}
