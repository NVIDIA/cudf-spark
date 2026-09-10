---
layout: page
title: RapidsShuffleManager V2 Phase 1 Design
nav_order: 16
parent: Developer Overview
---
# RapidsShuffleManager V2 Phase 1 Design

## 1. Background & Motivation

### The Key Insight: cudf partition() Produces Ordered Output

Before diving into the problems, we must understand a **critical property** that makes 
Phase 1 possible:

When RAPIDS uses cudf's `partition()` function on GPU, the returned table is 
**in ascending partition order from [0, num_partitions)**:

```
┌───────────────────────────────────────────────────────────────────────────┐
│                    GPU Table after cudf::partition()                      │
├─────────────┬─────────────┬─────────────┬───────┬─────────────────────────┤
│ Partition 0 │ Partition 1 │ Partition 2 │  ...  │     Partition N-1       │
│ (all rows)  │ (all rows)  │ (all rows)  │       │     (all rows)          │
├─────────────┼─────────────┼─────────────┼───────┼─────────────────────────┤
│  offset[0]  │  offset[1]  │  offset[2]  │       │     offset[N-1]         │
└─────────────┴─────────────┴─────────────┴───────┴─────────────────────────┘
```

This means when we iterate through the partitioned data, we see **all records for 
partition 0 first, then all records for partition 1, and so on**. This is fundamentally 
different from CPU-based shuffle where records arrive in arbitrary order.

Reference: [cudf partitioning API](https://docs.rapids.ai/api/libcudf/legacy/group__reorder__partition)

### Important Nuance: Per-Batch Ordering, Not Global Ordering

However, there's an important detail: due to `spark.rapids.shuffle.partitioning.maxCpuBatchSize`, 
a single map task may produce **multiple batches**. Each batch is independently ordered, 
but partition IDs "wrap around" between batches:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        Multi-Batch Partition Layout                         │
├─────────────────────────────────────────────────────────────────────────────┤
│ Batch 1: [Part 0][Part 1][Part 2]...[Part N-1]    ← ordered within batch    │
│ Batch 2: [Part 0][Part 1][Part 2]...[Part N-1]    ← partition ID resets!    │
│ Batch 3: [Part 0][Part 1][Part 2]...[Part N-1]    ← again resets            │
└─────────────────────────────────────────────────────────────────────────────┘
```

This is **"locally ordered"** input - each batch is sorted, but globally the partition 
IDs are not monotonically increasing.

Phase 1 handles this by:
1. **Detecting batch boundaries**: When partition ID decreases (e.g., from N-1 back to 0), 
   we know a new batch has started
2. **Independent processing per batch**: Each batch gets its own `BatchMerger` state and
   partial file. Short merger steps run on a shared, bounded executor.
3. **Final merge**: After all batches complete, merge their partial files into one output

This multi-batch mechanism is essential - without it, the pipeline would break when 
partition ID wraps around.

### Problems with the Original Implementation

The original MULTITHREADED shuffle write implementation (`RapidsShuffleThreadedWriter`) 
uses Spark's `BypassMergeSortShuffleWriter` pattern with `DiskBlockObjectWriter`. 
**This approach fails to leverage the ordered partition output from cudf** and has four 
fundamental problems:

#### Problem 1: Write Amplification

Each partition has its own temporary file. At task end, all temporary files are merged 
into a single output file. This means **data is written twice**:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        Original Write Path                                  │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  Records ──► DiskBlockObjectWriter ──► N temp files ──► Merge ──► 1 file    │
│               (per partition)           (on disk)                           │
│                                                                             │
│  Write amplification = 2x (data written to disk twice)                      │
└─────────────────────────────────────────────────────────────────────────────┘
```

#### Problem 2: Serialization & Compression Not Pipelined with Disk Write

The original implementation processes records **serially** in the main thread:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                     Serial Processing (No Pipeline)                         │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  Main Thread:                                                               │
│    for each record:                                                         │
│      1. Serialize ─────────────► blocking                                   │
│      2. Compress  ─────────────► blocking                                   │
│      3. Write to disk ─────────► blocking                                   │
│      4. Move to next record                                                 │
│                                                                             │
│  CPU and Disk are underutilized - they wait for each other                  │
└─────────────────────────────────────────────────────────────────────────────┘
```

#### Problem 3: Hash-Based Approach Does Not Scale (Limited to <= 200 Partitions)

Spark's `BypassMergeSortShuffleWriter` requires:
```
numPartitions <= spark.shuffle.sort.bypassMergeThreshold (default: 200)
```

**Why this limit exists in Spark**: BypassMergeSortShuffleWriter opens one file handle 
per partition simultaneously. With 1000+ partitions, this creates too many open files 
and too much random I/O.

**Why we don't need this limit**: Our data is already partition-ordered from cudf! 
We don't need to hash-distribute records to N different files. We can write sequentially 
to a single output file, processing partition 0, then 1, then 2, etc.

Beyond 200 partitions, Spark falls back to `SortShuffleWriter`, which sorts records 
by partition ID - but cudf already did this for us!

#### Problem 4: Uncontrolled Reliance on OS Page Cache

When writing to `DiskBlockObjectWriter`, data goes through the OS page cache:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    Uncontrolled OS Page Cache                               │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  Application ──► DiskBlockObjectWriter ──► OS Page Cache ──► Disk           │
│                                             (dirty pages)                   │
│                                                  │                          │
│                                                  ▼                          │
│                                         May or may not be flushed           │
│                                         (controlled by OS, not app)         │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

This has several costs:
- **Unpredictable I/O**: OS decides when to flush dirty pages; under memory pressure, 
  pages may be flushed at inconvenient times
- **Page cache pressure**: Dirty pages consume system page cache, affecting other 
  processes and file reads
- **No application control**: Cannot decide which data to keep in memory vs spill
- **Double jeopardy in merge phase**: When merging temp files, the data may have 
  already been evicted from page cache, requiring disk reads

### Phase 1 Goals

**Key enabler**: Leverage the fact that cudf `partition()` output is already sorted by 
partition ID. This allows us to:

| Problem | Solution |
|---------|----------|
| Write amplification | Write to single output file per batch (no temp files per partition) - possible because data arrives in partition order |
| No pipeline | 3-stage pipeline: Main Thread -> Writer Pool -> Merger Pool - Each merger can write partitions sequentially because they arrive in order |
| 200 partition limit | New architecture works with any number of partitions - no need to open N file handles since we write sequentially |
| Uncontrolled page cache reliance | Application-controlled memory management with explicit spill decisions |

## 2. High-Level Design

### Core Idea: Pipelined Architecture + Memory-First Storage

Phase 1 introduces two major changes:

**1. Three-Stage Pipeline**: Decouple record processing, serialization/compression, and
disk I/O across the main thread and two bounded executors.

**2. Memory-First Partial Files**: Replace per-partition temp files with a single 
`SpillablePartialFileHandle` per batch that can reside in memory or on disk.

Together, these changes address all four problems:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                BEFORE (Original Implementation)                             │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  Main Thread (serial):                                                      │
│    Record ──► Serialize ──► Compress ──► Write to Partition File            │
│                                              (via OS page cache)            │
│                                                                             │
│  Problems: No pipeline, 200 partition limit, 2x write amp, uncontrolled I/O │
└─────────────────────────────────────────────────────────────────────────────┘

                                    │
                                    ▼

┌─────────────────────────────────────────────────────────────────────────────┐
│                AFTER (Phase 1 Implementation)                               │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  Main Thread          Writer Pool            Merger Pool                    │
│  (queue tasks)   ──►  (serialize+compress)  ──►  (cooperative steps)        │
│  [non-blocking]       [bounded parallelism]      [bounded, non-blocking]    │
│                                                         │                   │
│                                                         ▼                   │
│                                          SpillablePartialFileHandle         │
│                                          (app-managed memory or disk)       │
│                                                                             │
│  Solves: Pipeline, No partition limit, No write amp, Controlled memory      │
└─────────────────────────────────────────────────────────────────────────────┘
```

### SpillablePartialFileHandle: Two Storage Modes

```
┌─────────────────────────────────────────────────────────────────────────────┐
│               SpillablePartialFileHandle Storage Modes                      │
├───────────────────────────────────┬─────────────────────────────────────────┤
│          FILE_ONLY Mode           │        MEMORY_WITH_SPILL Mode           │
├───────────────────────────────────┼─────────────────────────────────────────┤
│ • Writes directly to disk         │ • Writes to HostMemoryBuffer first      │
│ • Used when memory is scarce      │ • Can expand buffer dynamically         │
│ • Used for final merge writes     │ • Spills to disk when needed            │
│ • No spill framework integration  │ • Integrated with SpillFramework        │
└───────────────────────────────────┴─────────────────────────────────────────┘
```

### Storage Mode Selection

When creating a `SpillablePartialFileHandle`, the system decides which mode to use 
based on current memory pressure:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                      Storage Mode Selection                                 │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  Create SpillablePartialFileHandle                                          │
│                │                                                            │
│                ▼                                                            │
│       ┌────────────────────┐                                                │
│       │ forceFileOnly set? │                                                │
│       └────────┬───────────┘                                                │
│           YES  │  NO                                                        │
│           │    ▼                                                            │
│           │   ┌──────────────────────────────┐                              │
│           │   │ Host memory usage > threshold?│                             │
│           │   └────────────┬─────────────────┘                              │
│           │           YES  │  NO                                            │
│           ▼                ▼                                                │
│    ┌─────────────┐   ┌─────────────────────┐                                │
│    │ FILE_ONLY   │   │ MEMORY_WITH_SPILL   │                                │
│    │    mode     │   │       mode          │                                │
│    └─────────────┘   └─────────────────────┘                                │
└─────────────────────────────────────────────────────────────────────────────┘
```

## 3. Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              EXECUTOR                                       │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌───────────────────────────────────────────────────────────────────────┐  │
│  │               RapidsShuffleThreadedWriter (3-stage pipeline)          │  │
│  │                                                                       │  │
│  │  ┌─────────────┐    ┌─────────────────┐    ┌───────────────────────┐  │  │
│  │  │ Main Thread │ ─► │   Writer Pool   │ ─► │     Merger Pool       │  │  │
│  │  │             │    │    (bounded)    │    │      (bounded)        │  │  │
│  │  │ • Process   │    │                 │    │                       │  │  │
│  │  │   records   │    │ • Serialize     │    │ • Drain ready records  │  │  │
│  │  │ • Inc ref   │    │ • Compress      │    │ • Write in partition   │  │  │
│  │  │ • Queue     │    │ • Write to      │    │   order                │  │  │
│  │  │   tasks     │    │   buffer        │    │ • Yield if not ready   │  │  │
│  │  └─────────────┘    └─────────────────┘    └───────────┬───────────┘  │  │
│  │                                                        │              │  │
│  └────────────────────────────────────────────────────────┼──────────────┘  │
│                                                           │                 │
│                                                           ▼                 │
│  ┌───────────────────────────────────────────────────────────────────────┐  │
│  │                  SpillablePartialFileHandle                           │  │
│  │  ┌─────────────────────────────┐  ┌─────────────────────────────────┐ │  │
│  │  │    HostMemoryBuffer         │  │         Disk File               │ │  │
│  │  │    (expandable)             │  │    (when spilled or FILE_ONLY)  │ │  │
│  │  └─────────────────────────────┘  └─────────────────────────────────┘ │  │
│  └───────────────────────────────────────────────────────────────────────┘  │
│                                                                             │
│  ┌───────────────────────────────────────────────────────────────────────┐  │
│  │                    Spill Framework (Host Store)                       │  │
│  │                                                                       │  │
│  │  • Tracks SpillablePartialFileHandle instances                        │  │
│  │  • Can spill memory buffers to disk under memory pressure             │  │
│  └───────────────────────────────────────────────────────────────────────┘  │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

## 4. Core Components

### 4.1 SpillablePartialFileHandle

**What it is**: A spillable wrapper for partial file data that provides a unified 
write/read interface regardless of whether data is in memory or on disk.

**Why we need it**: The existing spill framework (`SpillableHostBuffer`) is designed 
for read-only access after creation. We need a handle that:
- Supports streaming writes (not just a pre-allocated buffer)
- Can expand capacity dynamically during writes
- Protects data from spill during write phase
- Transitions seamlessly to read phase after write completes

**Key Features**:

1. **Write Phase Protection**: During writes, the handle is marked `protectedFromSpill=true`, 
   preventing the spill framework from evicting partially written data.

2. **Dynamic Buffer Expansion**: When the buffer fills up:
   - Calculate new capacity (double until reaching required size)
   - Check if new capacity exceeds `maxBufferSize` limit
   - Check if host memory usage is below threshold
   - If both conditions pass, allocate new buffer and copy data
   - Otherwise, spill current content to file and switch to file mode

3. **Seamless Mode Transition**: After calling `finishWrite()`:
   - Write protection is removed
   - Buffer becomes spillable
   - Read operations work transparently regardless of storage location

### 4.2 RapidsLocalDiskShuffleDataIO

**What it is**: RAPIDS implementation of Spark's `ShuffleDataIO` interface.

**Why we need it**: To inject our custom `ShuffleExecutorComponents` that creates 
`RapidsLocalDiskShuffleMapOutputWriter` instances instead of Spark's default writers.

### 4.3 RapidsLocalDiskShuffleMapOutputWriter

**What it is**: RAPIDS implementation of `ShuffleMapOutputWriter` that uses 
`SpillablePartialFileHandle` for storage.

**Why we need it**: This is the entry point for all shuffle partition writes. 
By implementing our own writer, we control how partition data is buffered.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│              RapidsLocalDiskShuffleMapOutputWriter                          │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ShuffleMapOutputWriter.getPartitionWriter(partitionId)                     │
│                          │                                                  │
│                          ▼                                                  │
│  ┌───────────────────────────────────────────────────────────────────────┐  │
│  │ • Creates SpillablePartialFileHandle on first write                   │  │
│  │ • Decides FILE_ONLY vs MEMORY_WITH_SPILL mode                         │  │
│  │ • Provides OutputStream/Channel wrapper for writes                    │  │
│  │ • On commit: ensure data is on disk, write index file                 │  │
│  └───────────────────────────────────────────────────────────────────────┘  │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

**Force File-Only Mode**: The writer exposes `setForceFileOnlyMode()` method for 
scenarios where memory buffering doesn't help (e.g., final merge writes in 
multi-batch case where we're just copying data between files).

### 4.4 Rewritten RapidsShuffleThreadedWriter (The Core Change)

**What it is**: Complete rewrite of the threaded shuffle writer with a **three-stage 
pipeline architecture**. This is the most important change in Phase 1.

**Why we rewrote it**: The original implementation had all four problems:
- Serial processing (no pipeline) - CPU and disk wait for each other
- Used `DiskBlockObjectWriter` per partition (200 partition limit, 2x write amplification)
- Blocked on partition writes sequentially
- Always wrote to disk (no memory leverage)

**Why the pipeline works - the critical prerequisite**:

The pipeline depends on cudf's partition output being **ordered by partition ID**:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│           Records from GPU arrive in partition order                        │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  [Part0][Part0][Part0]...[Part1][Part1]...[Part2][Part2]...[PartN-1]...     │
│  ├──────────────────────┤├────────────────┤├────────────────┤├────────┤     │
│     All of Partition 0     All of Part 1     All of Part 2    Part N-1      │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

This ordering enables each `BatchMerger` to:
1. Know that when it sees partition 1 data, ALL of partition 0 data has already arrived
2. Write partitions sequentially to a single output stream (0, then 1, then 2, ...)
3. Never need to "go back" and write more data for an earlier partition

Without this ordering, the pipeline would be impossible - we couldn't know when a 
partition is "complete" and ready to write.

**New Three-Stage Pipeline Architecture**:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    Three-Stage Pipeline Architecture                        │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  Stage 1              Stage 2                    Stage 3                    │
│  ┌─────────────┐      ┌───────────────────┐      ┌─────────────────────┐    │
│  │ Main Thread │      │    Writer Pool    │      │    Merger Pool      │    │
│  │             │      │     (bounded)     │      │     (bounded)       │    │
│  │ 1. Process  │      │                   │      │                     │    │
│  │    records  │ ───► │ 4. Execute        │ ───► │ 6. Drain ready      │    │
│  │ 2. Inc ref  │      │    compression    │      │    records          │    │
│  │    count    │      │    (serialize +   │      │ 7. Write to         │    │
│  │ 3. Queue    │      │     compress)     │      │    OutputStream     │    │
│  │    task     │      │ 5. Write to       │      │    in partition     │    │
│  │             │      │    buffer         │      │    order, then yield│    │
│  └─────────────┘      └───────────────────┘      └──────────┬──────────┘    │
│                                                             │               │
│                                                             ▼               │
│                                               SpillablePartialFileHandle    │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

Each batch owns its `BatchMerger` state, but not a dedicated thread. A merger step drains
all currently ready records and returns when it reaches an unfinished compression future or an
empty queue whose producer is not finished. Record enqueue, compression completion, and batch
finalization reschedule the merger. The merger pool has the same size as the configured writer
pool, so waiting batches do not occupy executor threads and merger concurrency remains bounded.

**Multi-Batch Detection and Handling**:

A single map task may produce multiple batches due to `spark.rapids.shuffle.partitioning.maxCpuBatchSize` 
limiting the size of each batch transferred from GPU to CPU. Each batch is independently 
partition-ordered, but partition IDs "wrap around" between batches.

The writer detects batch boundaries by observing partition ID decrease:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                     Multi-Batch Detection                                   │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  Batch 1: partition 0, 1, 2, 3, 4, ... N-1                                  │
│                                        │                                    │
│                                        ▼                                    │
│  Batch 2: partition 0, 1, 2, ...       ← detected when ID < previous max    │
│                                          (N-1 -> 0 means new batch!)        │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

When multi-batch is detected:
1. Mark current batch as complete (set `maxPartitionIdQueued = numPartitions`)
2. Add current batch state to pending list
3. Create a new batch state and writer; its merger steps use the shared pool - **no blocking!**
4. Continue processing the new batch immediately

After all records processed:
1. Wait for all batch merger futures to complete (bounded merger steps may complete out of order)
2. Extract partial file handles from each batch
3. Merge all partial files into final output

## 5. Data Flow

### 5.1 Single Batch Flow

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         Single Batch Data Flow                              │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │ Step 1: Main Thread (for each record)                               │    │
│  │                                                                     │    │
│  │   1. Get partition ID from key                                      │    │
│  │   2. Inc ref count on ColumnarBatch                                 │    │
│  │   3. Acquire limiter (may block if too much in-flight)              │    │
│  │   4. Queue compression task to writer thread pool                   │    │
│  │   5. Schedule a cooperative merger step (non-blocking)             │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                              │                                              │
│                              ▼                                              │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │ Step 2: Writer Thread Pool (for each task, parallel)                │    │
│  │                                                                     │    │
│  │   1. Get/create buffer for this partition                           │    │
│  │   2. Wrap buffer with SerializerManager (compression + encryption)  │    │
│  │   3. Serialize key + value to buffer                                │    │
│  │   4. Return (uncompressedSize, compressedSize)                      │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                              │                                              │
│                              ▼                                              │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │ Step 3: Shared Merger Pool (for partitionId from 0 to N)            │    │
│  │                                                                     │    │
│  │   1. Drain ready futures from the head of the partition queue       │    │
│  │   2. Incrementally write buffer content to OutputStream             │    │
│  │   3. Release limiter for each completed future                      │    │
│  │   4. Yield when the next future or producer work is not ready       │    │
│  │   5. Reschedule on enqueue, future completion, or finalization      │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                              │                                              │
│                              ▼                                              │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │ Step 4: Commit                                                      │    │
│  │                                                                     │    │
│  │   RapidsLocalDiskShuffleMapOutputWriter.commitAllPartitions():      │    │
│  │   1. Finish write phase on SpillablePartialFileHandle               │    │
│  │   2. If still in memory, force spill to create file                 │    │
│  │   3. Write index file and commit                                    │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 5.2 Multi-Batch Flow

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    Multi-Batch Timeline (Parallel Execution)                │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  Time ────────────────────────────────────────────────────────────────►     │
│                                                                             │
│  Main Thread (processes ALL batches without blocking):                      │
│  ┌───────────────────┬───────────────────┬───────────────────┐              │
│  │ Batch 1 records   │ Batch 2 records   │ Batch 3 records   │              │
│  │ 0->N, detect wrap │ 0->N, detect wrap │ 0->N              │              │
│  └─────────┬─────────┴─────────┬─────────┴─────────┬─────────┘              │
│            │ create new        │ create new        │                        │
│            │ batch state       │ batch state       │                        │
│            ▼                   ▼                   ▼                        │
│                                                                             │
│  Shared Merger Pool (bounded by configured writer thread count):            │
│  ┌───────────────────────────────────────────────────────────────────────┐  │
│  │ Batch 1 steps: ═══► yield ─────────► ═══► Write to PartialFile 1     │  │
│  │ Batch 2 steps:      ═════► yield ─────────► Write to PartialFile 2   │  │
│  │ Batch 3 steps:             ═══════► Write to PartialFile 3           │  │
│  └───────────────────────────────────────────────────────────────────────┘  │
│                                                    │                        │
│                                                    ▼                        │
│  After all batches complete:                                                │
│  ┌───────────────────────────────────────────────────────────────────────┐  │
│  │ 1. Wait for all merger futures (completion order may differ)          │  │
│  │ 2. Extract partial file handles                                       │  │
│  │ 3. Final Merge (into new output file with FILE_ONLY mode):            │  │
│  │    For each partition 0 to N:                                         │  │
│  │      Read from PartialFile 1, 2, 3, ...                               │  │
│  │      Write all to final output file                                   │  │
│  └───────────────────────────────────────────────────────────────────────┘  │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

## 6. Buffer Expansion Logic

```
┌─────────────────────────────────────────────────────────────────────────────┐
│              Buffer Expansion Decision Flow                                 │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  Writing to MEMORY_WITH_SPILL handle, buffer capacity exceeded              │
│                              │                                              │
│                              ▼                                              │
│  ┌───────────────────────────────────────────────────────────────────────┐  │
│  │ Step 1: Calculate newCapacity                                         │  │
│  │         Double current until >= required (capped at maxBufferSize)    │  │
│  └───────────────────────────────────────────────────────────────────────┘  │
│                              │                                              │
│                              ▼                                              │
│  ┌───────────────────────────────────────────────────────────────────────┐  │
│  │ Step 2: newCapacity >= requiredCapacity?                              │  │
│  └───────────────────────────────────────────────────────────────────────┘  │
│                      │                │                                     │
│                     NO               YES                                    │
│                      │                │                                     │
│                      ▼                ▼                                     │
│              ┌──────────────┐  ┌─────────────────────────────────────────┐  │
│              │ SPILL TO     │  │ Step 3: newCapacity <= maxBufferSize?   │  │
│              │ DISK         │  └─────────────────────────────────────────┘  │
│              └──────────────┘         │                │                    │
│                                      NO               YES                   │
│                                       │                │                    │
│                                       ▼                ▼                    │
│                               ┌──────────────┐  ┌───────────────────────┐   │
│                               │ SPILL TO     │  │ Step 4: Host memory   │   │
│                               │ DISK         │  │ usage below threshold?│   │
│                               └──────────────┘  └───────────────────────┘   │
│                                                        │                │   │
│                                                       NO               YES  │
│                                                        │                │   │
│                                                        ▼                ▼   │
│                                                ┌──────────────┐  ┌────────┐ │
│                                                │ SPILL TO     │  │ALLOCATE│ │
│                                                │ DISK         │  │NEW     │ │
│                                                └──────────────┘  │BUFFER  │ │
│                                                                  │& COPY  │ │
│                                                                  └────────┘ │
└─────────────────────────────────────────────────────────────────────────────┘
```

## 7. Configuration

| Config | Default | Description |
|--------|---------|-------------|
| `spark.rapids.memory.host.partialFileBufferInitialSize` | 1GB | Initial buffer size for memory-based mode |
| `spark.rapids.memory.host.partialFileBufferMaxSize` | 8GB | Maximum buffer size before forced spill |
| `spark.rapids.memory.host.partialFileBufferMemoryThreshold` | 0.5 | Host memory usage threshold (0.0-1.0) for using memory mode |

All configs are `.startupOnly()` and `.internal()`.

## 8. Metrics Changes

### Removed Metrics
- `rapidsShuffleSerializationTime` - Replaced by new pipeline model
- `rapidsShuffleWriteTime` - Subsumed by Spark's built-in write time
- `rapidsShuffleCombineTime` - No longer applicable (no file combining)
- `rapidsShuffleWriteIoTime` - Subsumed by Spark's built-in metrics

### New Metrics
- `rapidsThreadedWriterLimiterWaitTime` - Time blocked waiting for limiter
- `rapidsThreadedWriterSerializationWaitTime` - Time waiting for serialization to complete
- `gpuDiskWriteSavedBytes` - Bytes that avoided disk write (kept in memory)

## 9. Files Changed Summary

| File | Change Type | Description |
|------|-------------|-------------|
| `SpillablePartialFileHandle.scala` | New | Memory-backed spillable handle for partial files |
| `RapidsLocalDiskShuffleDataIO.scala` | New | RAPIDS ShuffleDataIO implementation |
| `RapidsLocalDiskShuffleExecutorComponents.scala` | New | RAPIDS ShuffleExecutorComponents |
| `RapidsLocalDiskShuffleMapOutputWriter.scala` | New | RAPIDS ShuffleMapOutputWriter |
| `RapidsShuffleInternalManagerBase.scala` | Modified | Complete rewrite of threaded writer, added bounded writer, reader, and merger pools |
| `RapidsShuffleWriter.scala` | Modified | Changed from DiskBlockObjectWriter to ShuffleMapOutputWriter tracking |
| `SpillableHostBuffer.scala` | Modified | Removed mandatory priority parameter |
| `HostAlloc.scala` | Modified | Added `getUsageRatio()` and `isUsageBelowThreshold()` |
| `GpuShuffleExchangeExecBase.scala` | Modified | Metrics changes |
| `GpuTaskMetrics.scala` | Modified | Added `gpuDiskWriteSavedBytes` |

## 10. How Phase 1 Solves the Four Problems

**Foundation**: All solutions below are enabled by cudf's `partition()` producing 
output in ascending partition order. Without this property, none of these would work.

| Original Problem | Phase 1 Solution | Why cudf Ordering Enables This |
|------------------|------------------|--------------------------------|
| **Write Amplification** | Single output file per batch instead of N temp files. Data written once, not twice. | Can write partition 0, then 1, then 2 sequentially to one file |
| **No Pipeline** | Three-stage pipeline: Main Thread -> Writer Pool -> Merger Pool. Ready work runs in parallel without blocking merger workers. | A merger knows partition N is complete when it sees partition N+1 data |
| **200 Partition Limit** | New architecture works with any number of partitions. | No need to open N file handles; write sequentially to single output |
| **Uncontrolled Page Cache** | SpillablePartialFileHandle gives application control over memory/disk decisions. | Sequential writes enable single buffer instead of N buffers |

### Additional Benefits

1. **Multi-Batch Efficiency**: Multiple GPU batches can be processed in pipeline
   fashion, with cooperative merger steps sharing a bounded pool.

2. **Foundation for Phase 2**: The `SpillablePartialFileHandle` infrastructure 
   enables the skip-merge optimization in Phase 2, where data can stay in memory 
   from write through read without any disk I/O.
