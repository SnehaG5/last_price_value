package com.example.prices.service;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import com.example.api.BatchHandle;
import com.example.api.PriceService;
import com.example.batch.BatchRun;
import com.example.exception.BatchException;
import com.example.exception.ValidationException;
import com.example.prices.model.PriceRecord;

/**
 * Implementation of the PriceService API.
 *
 * Responsibilities:
 * - Manage lifecycle of batches (start, upload chunks, complete, cancel).
 * - Maintain a global, atomically visible snapshot of the latest price records.
 * - Deduplicate records by ID, keeping only the most recent (by timestamp).
 * - Ensure thread-safety and atomic visibility under concurrent batch operations.
 */
public class PriceServiceImpl implements PriceService {

    /**
     * Atomic reference holding the immutable snapshot of committed records.
     * - Readers always see a consistent view.
     * - Updated atomically on batch completion to avoid race conditions.
     */
    private final AtomicReference<Map<String, PriceRecord>> storeRef =
            new AtomicReference<>(Collections.unmodifiableMap(new HashMap<>()));

    /**
     * Tracks active batches by their UUID.
     * - ConcurrentHashMap ensures thread-safe access when multiple batches run in parallel.
     */
    private final ConcurrentHashMap<UUID, BatchRun> activeBatches = new ConcurrentHashMap<>();

    /** Maximum allowed chunk size per upload. */
    private static final int MAX_CHUNK_SIZE = 1000;

    /**
     * Starts a new batch and registers it as active.
     *
     * @return handle representing the new batch
     */
    @Override
    public BatchHandle startBatch() {
        BatchRun batch = new BatchRun();
        activeBatches.put(batch.getBatchId(), batch);
        return new BatchHandle(batch.getBatchId());
    }

    /**
     * Uploads a chunk of records into the given batch.
     * - Validates chunk size.
     * - Ensures batch is active and in STARTED state.
     * - Delegates deduplication logic to BatchRun.
     *
     * @param handle batch handle
     * @param records list of price records
     * @throws BatchException if batch is not active or not in STARTED state
     * @throws ValidationException if chunk exceeds MAX_CHUNK_SIZE
     */
    @Override
    public void uploadChunk(BatchHandle handle, List<PriceRecord> records)
            throws BatchException, ValidationException {
        if (records.size() > MAX_CHUNK_SIZE) {
            throw new ValidationException("Chunk too large");
        }
        BatchRun batch = activeBatches.get(handle.getId());
        if (batch == null || batch.getStatus() != BatchRun.Status.STARTED) {
            throw new BatchException("Batch not active: " + handle);
        }
        batch.addRecords(records);
    }

    /**
     * Completes the batch and atomically merges its records into the global store.
     * - Uses AtomicReference.updateAndGet to ensure atomic merge under concurrency.
     * - Deduplicates by timestamp: latest record wins.
     * - Removes batch from active set after completion.
     *
     * @param handle batch handle
     * @throws BatchException if batch not found or not in STARTED state
     */
    @Override
    public void completeBatch(BatchHandle handle) throws BatchException {
        BatchRun batch = activeBatches.get(handle.getId());
        if (batch == null) throw new BatchException("Batch not found");
        if (batch.getStatus() != BatchRun.Status.STARTED) {
            throw new BatchException("Batch not in STARTED state");
        }

        // Atomically merge batch snapshot into global store
        storeRef.updateAndGet(current -> {
            Map<String, PriceRecord> next = new HashMap<>(current);
            for (Map.Entry<String, PriceRecord> e : batch.snapshot().entrySet()) {
                String id = e.getKey();
                PriceRecord incoming = e.getValue();
                PriceRecord existing = next.get(id);
                if (existing == null || incoming.getAsOf().isAfter(existing.getAsOf())) {
                    next.put(id, incoming);
                }
            }
            return Collections.unmodifiableMap(next);
        });

        batch.complete();
        activeBatches.remove(handle.getId());
    }

    /**
     * Cancels the batch and discards its staged records.
     * - Only allowed if batch is in STARTED state.
     * - Removes batch from active set after cancellation.
     *
     * @param handle batch handle
     * @throws BatchException if batch not found or not in STARTED state
     */
    @Override
    public void cancelBatch(BatchHandle handle) throws BatchException {
        BatchRun batch = activeBatches.get(handle.getId());
        if (batch == null) throw new BatchException("Batch not found");
        if (batch.getStatus() != BatchRun.Status.STARTED) {
            throw new BatchException("Batch not in STARTED state");
        }
        batch.cancel();
        activeBatches.remove(handle.getId());
    }

    /**
     * Retrieves the latest committed price record for the given ID.
     *
     * @param id record identifier
     * @return Optional containing the latest PriceRecord, or empty if none exists
     */
    @Override
    public Optional<PriceRecord> getLastPrice(String id) {
        return Optional.ofNullable(storeRef.get().get(id));
    }
}