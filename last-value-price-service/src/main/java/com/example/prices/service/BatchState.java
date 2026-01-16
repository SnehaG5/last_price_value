package com.example.prices.service;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import com.example.prices.model.PriceRecord;

/**
 * Internal state holder for a batch run.
 *
 * - Each batch has a unique ID.
 * - Records are staged in-memory (not visible to consumers until the batch is completed).
 * - Lifecycle flags (completed/cancelled) are tracked atomically for thread-safety.
 *
 * This class is package-private and intended for internal use by PriceServiceImpl.
 */
final class BatchState {

    /** Unique identifier for this batch instance. */
    private final UUID id;

    /**
     * Staging area for records uploaded during the batch.
     * - Keys: record IDs
     * - Values: latest PriceRecord per ID (by timestamp).
     * - Not thread-safe by itself; intended to be used in a single-threaded batch context.
     */
    private final Map<String, PriceRecord> staging = new HashMap<>();

    /**
     * Flag indicating whether the batch has been marked as completed.
     * - AtomicBoolean ensures visibility across threads.
     */
    private final AtomicBoolean completed = new AtomicBoolean(false);

    /**
     * Flag indicating whether the batch has been marked as cancelled.
     * - AtomicBoolean ensures visibility across threads.
     */
    private final AtomicBoolean cancelled = new AtomicBoolean(false);

    /**
     * Constructs a new BatchState with the given unique ID.
     *
     * @param id unique identifier for this batch
     */
    BatchState(UUID id) {
        this.id = id;
    }

    /** @return the unique batch ID */
    UUID getId() {
        return id;
    }

    /**
     * @return the staging map containing records uploaded so far.
     *         Note: this is a live mutable map; callers should not expose it externally.
     */
    Map<String, PriceRecord> getStaging() {
        return staging;
    }

    /** @return true if the batch has been marked completed */
    boolean isCompleted() {
        return completed.get();
    }

    /** @return true if the batch has been marked cancelled */
    boolean isCancelled() {
        return cancelled.get();
    }

    /** Mark the batch as completed (irreversible). */
    void markCompleted() {
        completed.set(true);
    }

    /** Mark the batch as cancelled (irreversible). */
    void markCancelled() {
        cancelled.set(true);
    }
}