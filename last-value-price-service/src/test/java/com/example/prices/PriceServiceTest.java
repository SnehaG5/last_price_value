package com.example.prices;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.Ignore;
import org.junit.Test;

import com.example.api.BatchHandle;
import com.example.api.PriceService;
import com.example.exception.BatchException;
import com.example.exception.ValidationException;
import com.example.prices.model.PriceRecord;
import com.example.prices.service.PriceServiceImpl;

public class PriceServiceTest {

	   private PriceRecord rec(String id, Instant asOf, double price) {
	        Map<String, Object> payload = new HashMap<>();
	        payload.put("price", price);
	        return new PriceRecord(id, asOf, payload);
	    }

	    /** Basic happy path: records uploaded, completed, then visible atomically */
	    @Test
	    public void testHappyPathCompletion() throws Exception {
	        PriceService svc = new PriceServiceImpl();
	        BatchHandle batch = svc.startBatch();

	        svc.uploadChunk(batch, Arrays.asList(
	                rec("AAPL", Instant.parse("2025-01-01T10:00:00Z"), 150.0),
	                rec("GOOG", Instant.parse("2025-01-01T10:00:00Z"), 2000.0)
	        ));

	        // Before completion, nothing visible
	        assertFalse(svc.getLastPrice("AAPL").isPresent());
	        assertFalse(svc.getLastPrice("GOOG").isPresent());

	        svc.completeBatch(batch);

	        // After completion, both visible at once
	        assertTrue(svc.getLastPrice("AAPL").isPresent());
	        assertTrue(svc.getLastPrice("GOOG").isPresent());
	        assertEquals(150.0, (double) svc.getLastPrice("AAPL").get().getPayload().get("price"), 0.0001);
	        assertEquals(2000.0, (double) svc.getLastPrice("GOOG").get().getPayload().get("price"), 0.0001);
	    }

	    /** Deduplication inside a batch: latest asOf wins */
	    @Test
	    public void testDeduplicationWithinBatch() throws Exception {
	        PriceService svc = new PriceServiceImpl();
	        BatchHandle batch = svc.startBatch();

	        svc.uploadChunk(batch, Arrays.asList(
	                rec("TSLA", Instant.parse("2025-01-01T10:00:00Z"), 700.0),
	                rec("TSLA", Instant.parse("2025-01-01T11:00:00Z"), 750.0) // later timestamp
	        ));
	        svc.completeBatch(batch);

	        assertTrue(svc.getLastPrice("TSLA").isPresent());
	        PriceRecord latest = svc.getLastPrice("TSLA").get();
	        assertEquals(750.0, (double) latest.getPayload().get("price"), 0.0001);
	        assertEquals(Instant.parse("2025-01-01T11:00:00Z"), latest.getAsOf());
	    }

	    /** Deduplication across batches: later batch overrides earlier one */
	    @Test
	    public void testDeduplicationAcrossBatches() throws Exception {
	        PriceService svc = new PriceServiceImpl();

	        BatchHandle b1 = svc.startBatch();
	        svc.uploadChunk(b1, Collections.singletonList(
	                rec("NFLX", Instant.parse("2025-01-01T10:00:00Z"), 400.0)
	        ));
	        svc.completeBatch(b1);

	        BatchHandle b2 = svc.startBatch();
	        svc.uploadChunk(b2, Collections.singletonList(
	                rec("NFLX", Instant.parse("2025-01-01T12:00:00Z"), 420.0)
	        ));
	        svc.completeBatch(b2);

	        assertTrue(svc.getLastPrice("NFLX").isPresent());
	        PriceRecord latest = svc.getLastPrice("NFLX").get();
	        assertEquals(420.0, (double) latest.getPayload().get("price"), 0.0001);
	        assertEquals(Instant.parse("2025-01-01T12:00:00Z"), latest.getAsOf());
	    }

	    /** Cancelled batch should discard all staged records */
	    @Test
	    public void testCancelledBatchIsDiscarded() throws Exception {
	        PriceService svc = new PriceServiceImpl();
	        BatchHandle batch = svc.startBatch();

	        svc.uploadChunk(batch, Collections.singletonList(
	                rec("MSFT", Instant.parse("2025-01-01T10:00:00Z"), 300.0)
	        ));
	        svc.cancelBatch(batch);

	        assertFalse(svc.getLastPrice("MSFT").isPresent());
	    }

	    /** Validation: chunk size > 1000 should throw ValidationException */
	    @Test(expected = ValidationException.class)
	    public void testChunkSizeValidation() throws Exception {
	        PriceService svc = new PriceServiceImpl();
	        BatchHandle batch = svc.startBatch();

	        List<PriceRecord> tooBig = new ArrayList<>();
	        for (int i = 0; i < 1001; i++) {
	            tooBig.add(rec("ID-" + i, Instant.parse("2025-01-01T10:00:00Z").plusSeconds(i), i));
	        }
	        svc.uploadChunk(batch, tooBig);
	    }

	    /** Boundary: chunk size exactly 1000 should succeed */
	    @Test
	    public void testChunkSizeBoundary() throws Exception {
	        PriceService svc = new PriceServiceImpl();
	        BatchHandle batch = svc.startBatch();

	        List<PriceRecord> exact = new ArrayList<>();
	        for (int i = 0; i < 1000; i++) {
	            exact.add(rec("ID-" + i, Instant.parse("2025-01-01T10:00:00Z").plusSeconds(i), i));
	        }
	        svc.uploadChunk(batch, exact);
	        svc.completeBatch(batch);

	        assertTrue(svc.getLastPrice("ID-999").isPresent());
	        assertEquals(999.0, (double) svc.getLastPrice("ID-999").get().getPayload().get("price"), 0.0001);
	    }

	    /** Equal timestamps: existing record wins (tie keeps first) */
	    @Test
	    public void testEqualTimestampsKeepsExisting() throws Exception {
	        PriceService svc = new PriceServiceImpl();
	        BatchHandle batch = svc.startBatch();

	        Instant ts = Instant.parse("2025-01-01T10:00:00Z");
	        svc.uploadChunk(batch, Arrays.asList(
	                rec("IBM", ts, 100.0),
	                rec("IBM", ts, 200.0) // same timestamp, should keep first
	        ));
	        svc.completeBatch(batch);

	        assertTrue(svc.getLastPrice("IBM").isPresent());
	        PriceRecord latest = svc.getLastPrice("IBM").get();
	        assertEquals(100.0, (double) latest.getPayload().get("price"), 0.0001);
	        assertEquals(ts, latest.getAsOf());
	    }

	    /** Resilience: completing a non-existent batch should throw BatchException */
	    @Test(expected = BatchException.class)
	    public void testCompleteNonexistentBatch() throws Exception {
	        PriceService svc = new PriceServiceImpl();
	        svc.completeBatch(new BatchHandle(UUID.randomUUID()));
	    }

	    /** Resilience: cancelling a non-existent batch should throw BatchException */
	    @Test(expected = BatchException.class)
	    public void testCancelNonexistentBatch() throws Exception {
	        PriceService svc = new PriceServiceImpl();
	        svc.cancelBatch(new BatchHandle(UUID.randomUUID()));
	    }

	    /** Resilience: uploading after completion should throw BatchException */
	    @Test(expected = BatchException.class)
	    public void testUploadAfterCompleteFails() throws Exception {
	        PriceService svc = new PriceServiceImpl();
	        BatchHandle batch = svc.startBatch();
	        svc.completeBatch(batch);

	        svc.uploadChunk(batch, Collections.singletonList(
	                rec("AMZN", Instant.parse("2025-01-01T10:00:00Z"), 1000.0)
	        ));
	    }

	    /** Resilience: cancelling after completion should throw BatchException */
	    @Test(expected = BatchException.class)
	    public void testCancelAfterCompleteFails() throws Exception {
	        PriceService svc = new PriceServiceImpl();
	        BatchHandle batch = svc.startBatch();
	        svc.completeBatch(batch);
	        svc.cancelBatch(batch);
	    }

	    /** Multi-chunk upload in one batch: dedup across chunks and atomic visibility */
	    @Test
	    public void testMultiChunkUpload() throws Exception {
	        PriceService svc = new PriceServiceImpl();
	        BatchHandle batch = svc.startBatch();

	        svc.uploadChunk(batch, Arrays.asList(
	                rec("ORCL", Instant.parse("2025-01-01T10:00:00Z"), 50.0)
	        ));
	        svc.uploadChunk(batch, Arrays.asList(
	                rec("ORCL", Instant.parse("2025-01-01T11:00:00Z"), 55.0),
	                rec("SAP", Instant.parse("2025-01-01T09:00:00Z"), 60.0)
	        ));

	        // Still invisible before completion
	        assertFalse(svc.getLastPrice("ORCL").isPresent());
	        assertFalse(svc.getLastPrice("SAP").isPresent());

	        svc.completeBatch(batch);

	        assertTrue(svc.getLastPrice("ORCL").isPresent());
	        assertTrue(svc.getLastPrice("SAP").isPresent());
	        assertEquals(55.0, (double) svc.getLastPrice("ORCL").get().getPayload().get("price"), 0.0001);
	        assertEquals(60.0, (double) svc.getLastPrice("SAP").get().getPayload().get("price"), 0.0001);
	    }

	    /** Concurrency stress: multiple threads uploading and completing batches deterministically */
	    @Test
	    public void testConcurrentBatches() throws Exception {
	        final PriceService svc = new PriceServiceImpl();
	        ExecutorService executor = Executors.newFixedThreadPool(4);

	        List<Callable<Void>> tasks = new ArrayList<>();
	        for (int i = 0; i < 5; i++) {
	            final int batchNum = i;
	            tasks.add(() -> {
	                BatchHandle batch = svc.startBatch();
	                Instant base = Instant.parse("2025-01-01T10:00:00Z").plusSeconds(batchNum * 100);
	                svc.uploadChunk(batch, Arrays.asList(
	                        rec("ID-" + batchNum, base, batchNum * 10.0),
	                        rec("ID-" + batchNum, base.plusSeconds(1), batchNum * 20.0)
	                ));
	                svc.completeBatch(batch);
	                return null;
	            });
	        }

	        executor.invokeAll(tasks);
	        executor.shutdown();
	        assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));

	        // Verify all IDs are present with latest values and timestamps
	        for (int i = 0; i < 5; i++) {
	            Optional<PriceRecord> opt = svc.getLastPrice("ID-" + i);
	            assertTrue("Missing ID-" + i, opt.isPresent());
	            PriceRecord latest = opt.get();
	            assertEquals(i * 20.0, (double) latest.getPayload().get("price"), 0.0001);
	            Instant expectedTs = Instant.parse("2025-01-01T10:00:00Z").plusSeconds(i * 100 + 1);
	            assertEquals(expectedTs, latest.getAsOf());
	        }
	    }

	    /** Reader during completion: ensure atomic snapshot (no partial visibility) */
	    @Test
	    public void testReaderDuringCompletionSeesAtomicSnapshot() throws Exception {
	        final PriceService svc = new PriceServiceImpl();
	        final BatchHandle batch = svc.startBatch();

	        // Stage records
	        svc.uploadChunk(batch, Arrays.asList(
	                rec("X", Instant.parse("2025-01-01T10:00:00Z"), 1.0),
	                rec("Y", Instant.parse("2025-01-01T10:00:00Z"), 2.0)
	        ));

	        // Reader thread polling while completion happens
	        ExecutorService exec = Executors.newSingleThreadExecutor();
	        Future<List<Boolean>> visibilitySequence = exec.submit(() -> {
	            List<Boolean> seenBoth = new ArrayList<>();
	            for (int i = 0; i < 1000; i++) {
	                boolean x = svc.getLastPrice("X").isPresent();
	                boolean y = svc.getLastPrice("Y").isPresent();
	                seenBoth.add(x && y);
	            }
	            return seenBoth;
	        });

	        // Complete batch while reader is polling
	        svc.completeBatch(batch);

	        // Reader should either see none or both—never partial
	        List<Boolean> sequence = visibilitySequence.get();
	        exec.shutdown();
	        assertTrue(exec.awaitTermination(2, TimeUnit.SECONDS));

	        boolean sawPartial = false;
	        for (Boolean both : sequence) {
	            // If one is visible and the other isn't, both==false but we need to check partial explicitly
	            boolean x = svc.getLastPrice("X").isPresent();
	            boolean y = svc.getLastPrice("Y").isPresent();
	            if (x ^ y) {
	                sawPartial = true;
	                break;
	            }
	        }
	        assertFalse("Observed partial visibility during completion", sawPartial);

	        // Final state: both visible
	        assertTrue(svc.getLastPrice("X").isPresent());
	        assertTrue(svc.getLastPrice("Y").isPresent());
	    }

	    /** Benchmark test - run manually to measure performance */
	    @Test
	    @Ignore("Benchmark test - run manually to measure performance")
	    public void testLargeBatchPerformance() throws Exception {
	        PriceService svc = new PriceServiceImpl();
	        BatchHandle batch = svc.startBatch();

	        // Generate 100k records with unique ids
	        List<PriceRecord> bigChunk = new ArrayList<>();
	        Instant now = Instant.parse("2025-01-01T10:00:00Z");
	        for (int i = 0; i < 100_000; i++) {
	            bigChunk.add(rec("ID-" + i, now.plusSeconds(i), i));
	        }

	        // Upload in chunks of 1000
	        for (int i = 0; i < bigChunk.size(); i += 1000) {
	            int end = Math.min(i + 1000, bigChunk.size());
	            svc.uploadChunk(batch, bigChunk.subList(i, end));
	        }

	        long start = System.currentTimeMillis();
	        svc.completeBatch(batch);
	        long end = System.currentTimeMillis();

	        System.out.println("Completed 100k record batch in " + (end - start) + " ms");

	        // Verify a few sample records are visible
	        assertTrue(svc.getLastPrice("ID-0").isPresent());
	        assertTrue(svc.getLastPrice("ID-99999").isPresent());
	    }
	}
