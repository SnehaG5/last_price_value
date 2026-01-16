 High Level Design: Last Price Service

 Overview
The Last Price Service provides an in‑memory solution for producers to publish price records in batch runs and for consumers to query the latest price per instrument ID.

1. Key Requirements
- Producers:
  - Start a batch run
  - Upload records in chunks (≤ 1000 records per chunk)
  - Complete or cancel the batch
- Consumers:
  - Query the latest price record for a given ID
- Deduplication:
  - Within a batch: latest asOf timestamp wins
  - Across batches: later batch overrides earlier values
- Atomic visibility:
  - Records become visible only after batch completion
- Resilience:
  - Incorrect method order throws exceptions
  - Cancelled batches discard staged records

2.Architecture
- API Layer: PriceService interface defines producer and consumer operations.
- Service Implementation: PriceServiceImpl manages batches and global store.
-Batch Lifecycle: BatchRun encapsulates records and status (STARTED, COMPLETED, CANCELLED).
- Data Model: PriceRecord holds id, asOf, and flexible payload.

3. Concurrency & Atomicity
- Global store: AtomicReference<Map<String, PriceRecord>> ensures atomic snapshot updates.
- Active batches: ConcurrentHashMap<UUID, BatchRun> tracks batch state safely.
- Deduplication: ConcurrentHashMap.merge inside BatchRun ensures latest record per ID.
- Consumers always see a consistent snapshot (no partial visibility).

4.Error Handling
- ValidationException: chunk size > 1000
- BatchException: invalid lifecycle operations (upload after complete, cancel non‑existent, etc.)






---------------------------------------------------------------------------------------------------------------------
 Low Level Design: Last Price Service

Classes

1.PriceRecord
- Fields:
  - String id
  - Instant asOf
  - Map<String,Object> payload
- Immutable data holder for price information.

2. BatchHandle
- Fields:
  - UUID id
- Lightweight handle returned to producers to reference a batch.

3. BatchRun
- Fields:
  - UUID batchId
  - ConcurrentHashMap<String, PriceRecord> records
  - AtomicReference<Status> status
- Methods:
  - addRecords(List<PriceRecord>): merge records, latest timestamp wins
  - snapshot(): defensive copy of records
  - complete(), cancel(): update status

4.PriceService (interface)
- Methods:
  - BatchHandle startBatch()
  - void uploadChunk(BatchHandle, List<PriceRecord>)
  - void completeBatch(BatchHandle)
  - void cancelBatch(BatchHandle)
  - Optional<PriceRecord> getLastPrice(String id)

5.PriceServiceImpl
- Fields:
  - AtomicReference<Map<String, PriceRecord>> storeRef
  - ConcurrentHashMap<UUID, BatchRun> activeBatches
- Methods:
  - startBatch(): create and register new batch
  - uploadChunk(): validate and add records
  - completeBatch(): atomically merge snapshot into global store
  - cancelBatch(): discard staged records
  - getLastPrice(): return latest committed record

6. Data Flow
1. Producer calls startBatch() → returns BatchHandle.
2. Producer uploads chunks via uploadChunk().
3. Producer calls completeBatch():
   - Merge batch snapshot into global store atomically.
   - Remove batch from active set.
4. Consumer calls getLastPrice(id):
   - Returns latest record from global store snapshot.

----------------------------------------------------------------------------------------------------------------------------------



Overview
This project implements an in‑memory service to track the latest price for financial instruments.  
Producers publish prices in batch runs, consumers query the latest committed price per instrument ID.

1.Features
- Batch lifecycle: start, upload chunks, complete, cancel
- Deduplication by asOf timestamp (latest wins)
- Atomic visibility: records appear only after batch completion
- Resilience against incorrect usage (exceptions thrown)
- Concurrency safe with lock‑free design

2.Tech Stack
- Java 11+
- JUnit 4 for unit tests
- Maven for build and dependency management

3.Project Structure
- com.example.api.PriceService — service interface
- com.example.prices.model.PriceRecord — price data model
- com.example.batch.BatchRun — batch lifecycle state
- com.example.prices.service.PriceServiceImpl — implementation
- com.example.exception- Exception classes
- com.example.prices.PriceServiceTest — unit tests

4.Running Tests

mvn test
