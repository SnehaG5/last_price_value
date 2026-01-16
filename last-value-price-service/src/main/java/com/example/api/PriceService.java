package com.example.api;
import java.util.List;
import java.util.Optional;

import com.example.exception.BatchException;
import com.example.exception.ValidationException;
import com.example.prices.model.PriceRecord;

/**
 * Main service interface for Last Value Price pattern
 */
public interface PriceService {

	 /**
     * Start a new batch run. Returns a handle to be used for subsequent operations.
     */
    BatchHandle startBatch();

    /**
     * Upload a chunk of up to 1000 records into the batch.
     * Records with the same id are deduplicated by asOf (latest wins within the batch).
     */
    void uploadChunk(BatchHandle handle, List<PriceRecord> records) throws BatchException, ValidationException;

    /**
     * Complete the batch. All records become visible atomically to consumers.
     */
    void completeBatch(BatchHandle handle) throws BatchException;

    /**
     * Cancel the batch. All staged records are discarded.
     */
    void cancelBatch(BatchHandle handle) throws BatchException;

    /**
     * Get the last price for a given instrument id, determined by asOf.
     */
    Optional<PriceRecord> getLastPrice(String id);
}
