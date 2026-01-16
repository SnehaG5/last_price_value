package com.example.batch;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import com.example.prices.model.PriceRecord;

/**
 * Represents a single batch run in the price ingestion pipeline.
 * 
 * - Each batch has a unique ID and maintains its own set of records.
 * - Records are deduplicated by ID, keeping only the latest (by timestamp).
 * - The batch has a lifecycle state (STARTED, COMPLETED, CANCELLED).
 * - Thread-safe design: ConcurrentHashMap for records, AtomicReference for status.
 */
public class BatchRun {

    /** Possible lifecycle states of a batch. */
    public enum Status { STARTED, COMPLETED, CANCELLED }

    /** Unique identifier for this batch. */
    private final UUID batchId = UUID.randomUUID();

    /**
     * Concurrent map of records keyed by ID.
     * - Ensures thread-safe updates when multiple chunks are uploaded concurrently.
     * - Deduplication logic applied on merge.
     */
    private final ConcurrentHashMap<String, PriceRecord> records = new ConcurrentHashMap<>();

    /**
     * Atomic status reference.
     * - Guarantees visibility of state changes across threads.
     * - Prevents race conditions when checking/updating batch state.
     */
    private final AtomicReference<Status> status = new AtomicReference<>(Status.STARTED);

    /**
     * Adds a chunk of records to this batch.
     * - Only allowed while batch is in STARTED state.
     * - Uses ConcurrentHashMap.merge to deduplicate by ID:
     *   Keeps the record with the latest timestamp (`asOf`).
     *
     * @param chunk list of records to add
     * @throws IllegalStateException if batch is not in STARTED state
     */
    public void addRecords(List<PriceRecord> chunk) {
        if (status.get() != Status.STARTED) {
            throw new IllegalStateException("Batch not in STARTED state");
        }
        for (PriceRecord rec : chunk) {
            records.merge(rec.getId(), rec,
                (existing, incoming) -> incoming.getAsOf().isAfter(existing.getAsOf()) ? incoming : existing
            );
        }
    }

    /**
     * Returns a snapshot of all records in this batch.
     * - Creates a defensive copy to avoid exposing internal concurrent map.
     * - Snapshot is not live; further updates to the batch won't affect this map.
     *
     * @return copy of current records
     */
    public Map<String, PriceRecord> snapshot() {
        return new HashMap<>(records);
    }

    /**
     * Marks the batch as completed.
     * - After completion, no further records can be added.
     */
    public void complete() { status.set(Status.COMPLETED); }

    /**
     * Cancels the batch.
     * - Clears all staged records.
     * - After cancellation, no further records can be added.
     */
    public void cancel() { status.set(Status.CANCELLED); records.clear(); }

    /** @return unique batch ID */
    public UUID getBatchId() { return batchId; }

    /** @return current lifecycle status */
    public Status getStatus() { return status.get(); }
}