---
layout: page
title: RapidsShuffleManager V2 Phase 2 Skip-Merge Design
nav_order: 17
parent: Developer Overview
---
# RapidsShuffleManager V2 Phase 2: Skip-Merge Design

## 1. Phase 1 Recap

Phase 1 introduced:
- **SpillablePartialFileHandle**: A spillable abstraction for partial shuffle files
- **Rewritten RapidsShuffleThreadedWriter**: Three-stage pipeline (Main Thread -> Writer Threads -> Merger Thread) that leverages cudf's partition-ordered output
- **Memory-first write path**: Data can be written to memory first, spilled to disk only when needed

At the end of Phase 1, partial files are still **merged into a single output file per map task**. This design document describes Phase 2, which **skips the merge** entirely.

## 2. Background & Motivation

In standard Spark shuffle, at the end of a map task:
1. Partial files (one per batch) are merged into a single final file
2. An index file is created pointing to partition offsets in the final file
3. Reducers fetch partition data by reading (offset, length) from the merged file

**The problem**: The merge step adds latency and disk I/O. If data is already in memory (in `SpillablePartialFileHandle`), forcing a merge means:
- Writing data to disk unnecessarily
- Losing the opportunity to serve data directly from memory
- Adding latency to task completion

## 3. Design Goals

Building on Phase 1's foundation:

| Goal | Description |
|------|-------------|
| Skip final merge | Don't merge partial files; keep them separate |
| Serve data from memory | If data is still in host memory, serve it directly |
| Support multi-batch | Map tasks may produce multiple partial files |
| Transparent to reducers | Reducers see normal ManagedBuffer interface |
| Clean resource management | Properly free handles when shuffle is no longer needed |

## 4. Core Idea

Instead of merging partial files at task end, we:
1. **Keep partial files alive** (whether in memory or on disk)
2. **Build an in-memory index** mapping ShuffleBlockId -> [segments in partial files]
3. **Serve reducer requests** by reading from the appropriate segments

```
PartialFile layout:
┌──────────┬──────────┬──────────┬─────┬──────────────┐
│ Part 0   │ Part 1   │ Part 2   │ ... │ Part N-1     │
│ (offset  │ (offset  │ (offset  │     │              │
│  0, len  │  100,    │  250,    │     │              │
│  100)    │  len 150)│  len 80) │     │              │
└──────────┴──────────┴──────────┴─────┴──────────────┘
```

For multi-batch map tasks, multiple partial files exist:
- PartialFile1: partitions 0-N from batch 1
- PartialFile2: partitions 0-N from batch 2
- PartialFile3: partitions 0-N from batch 3

A reducer requesting partition P from map task M needs data from ALL partial files for that map task.

## 5. Architecture Overview

```
┌───────────────────────────────────────────────────────────────────────────┐
│                                EXECUTOR                                   │
│                                                                           │
│  ┌─────────────────────────────────────────────────────────────────────┐  │
│  │                      Map Task (Shuffle Write)                       │  │
│  │                                                                     │  │
│  │  Records ──> RapidsShuffleThreadedWriter                            │  │
│  │                     │                                               │  │
│  │                     ▼                                               │  │
│  │             SpillablePartialFileHandle (per batch)                  │  │
│  │                     │                                               │  │
│  │                     ▼                                               │  │
│  │       ┌───────────────────────────────────┐                         │  │
│  │       │ MultithreadedShuffleBufferCatalog │ ◄── Register partitions │  │
│  │       │                                   │     (offset, length)    │  │
│  │       │  ShuffleBlockId -> [Segments]     │                         │  │
│  │       └───────────────────────────────────┘                         │  │
│  └─────────────────────────────────────────────────────────────────────┘  │
│                                                                           │
│  ┌─────────────────────────────────────────────────────────────────────┐  │
│  │                     Reduce Task (Shuffle Read)                      │  │
│  │                                                                     │  │
│  │  GpuShuffleBlockResolverBase.getBlockData(blockId)                  │  │
│  │                     │                                               │  │
│  │                     ▼                                               │  │
│  │       ┌───────────────────────────────────┐                         │  │
│  │       │ MultithreadedShuffleBufferCatalog │                         │  │
│  │       │         .getMergedBuffer()        │                         │  │
│  │       └───────────────────────────────────┘                         │  │
│  │                     │                                               │  │
│  │                     ▼                                               │  │
│  │           MultiBatchManagedBuffer                                   │  │
│  │           (assembles data from segments)                            │  │
│  └─────────────────────────────────────────────────────────────────────┘  │
│                                                                           │
│  ┌─────────────────────────────────────────────────────────────────────┐  │
│  │                     ShuffleCleanupEndpoint                          │  │
│  │                                                                     │  │
│  │  Polls driver periodically ──> Receives shuffle IDs to clean        │  │
│  │                     │                                               │  │
│  │                     ▼                                               │  │
│  │       MultithreadedShuffleBufferCatalog.unregisterShuffle()         │  │
│  └─────────────────────────────────────────────────────────────────────┘  │
└───────────────────────────────────────────────────────────────────────────┘

┌───────────────────────────────────────────────────────────────────────────┐
│                                 DRIVER                                    │
│                                                                           │
│  ┌─────────────────────────────────────────────┐  ┌─────────────────────┐ │
│  │ ShuffleCleanupListener                      │  │ ShuffleCleanupMgr   │ │
│  │                                             │  │                     │ │
│  │ onJobStart: track shuffleId -> execId       │──│ registerForCleanup  │ │
│  │ onSQLExecutionEnd: trigger cleanup          │  │ handlePoll          │ │
│  └─────────────────────────────────────────────┘  │ handleStats         │ │
│                                                   └─────────────────────┘ │
└───────────────────────────────────────────────────────────────────────────┘
```

## 6. Core Components

### 6.1 SpillablePartialFileHandle (Phase 2 Enhancements)

**Phase 1 functionality**: Write-only handle with memory-first storage and spill support.

**Phase 2 additions**:

1. **`readAt(offset, length): ByteBuffer`**: Read a segment of data from the handle.
   - If data is in memory: Direct slice from `HostMemoryBuffer`
   - If data has been spilled: Read from disk file

2. **Buffer Sizing Optimization**: Predictive buffer expansion based on historical data 
   and memory availability (see Section 9).

### 6.2 MultithreadedShuffleBufferCatalog

**What it is**: An in-memory index that maps shuffle block IDs to their locations 
across partial files.

**Data structure**:
```scala
case class PartitionSegment(
  handle: SpillablePartialFileHandle,
  offset: Long,
  length: Long
)

// Map: ShuffleBlockId -> List of segments (one per batch)
val buffers: ConcurrentHashMap[ShuffleBlockId, ArrayBuffer[PartitionSegment]]
```

**Key methods**:

| Method | Description |
|--------|-------------|
| `addPartition(shuffleId, mapId, partId, handle, offset, length)` | Register a partition segment in the catalog |
| `getMergedBuffer(blockId)` | Return a `ManagedBuffer` that spans all segments for a block |
| `unregisterShuffle(shuffleId)` | Remove all entries for a shuffle, close handles, return stats |

### 6.3 MultiBatchManagedBuffer

**What it is**: A `ManagedBuffer` implementation that transparently concatenates data 
from multiple partial file segments.

**Why we need it**: A reducer requesting `ShuffleBlockId(shuffleId=0, mapId=5, reduceId=3)` 
expects a single contiguous stream of data. But with multiple batches, that data lives 
in multiple locations.

**How it works**:
- Holds a list of `PartitionSegment` references
- `size()`: Sum of all segment lengths
- `createInputStream()`: Returns `MultiSegmentInputStream` that reads segments sequentially
- Reference counting: Each open stream increments ref count on handles

### 6.4 ShuffleCleanupListener (Driver-side)

**What it is**: A Spark listener that tracks shuffle-to-execution mapping and triggers 
cleanup when SQL executions complete.

**Key callbacks**:

| Callback | Action |
|----------|--------|
| `onJobStart` | Extract (shuffleId, executorId) pairs from job, track in cleanupManager |
| `onOtherEvent(SQLExecutionEnd)` | Trigger cleanup for all shuffles associated with this execution |

### 6.5 ShuffleCleanupManager (Driver-side)

**What it is**: Manages the lifecycle of shuffle cleanup requests.

**Key responsibilities**:
- Maintain mapping: shuffleId -> Set[executorId]
- Maintain pending cleanup queue per executor
- Respond to executor poll requests with shuffle IDs to clean
- Receive and log cleanup statistics

### 6.6 ShuffleCleanupEndpoint (Executor-side)

**What it is**: Background thread that periodically polls the driver for cleanup requests.

**Operation**:
1. Every 1 second, send `RapidsShuffleCleanupPollMsg` to driver
2. Receive `RapidsShuffleCleanupResponseMsg` with list of shuffle IDs
3. For each shuffle ID, call `catalog.unregisterShuffle(shuffleId)`
4. Send `RapidsShuffleCleanupStatsMsg` with cleanup statistics

## 7. Data Flow

### 7.1 Write Path

```
┌───────────────────────────────────────────────────────────────────────────┐
│                           Shuffle Write Flow                              │
└───────────────────────────────────────────────────────────────────────────┘

  Input Records
       │
       ▼
  ┌───────────────────────────────────────┐
  │  RapidsShuffleThreadedWriter          │
  │                                       │
  │  for each batch:                      │
  │    1. Create SpillablePartialFile-    │
  │       Handle (memory-backed)          │
  │    2. Write partition data            │
  │    3. Track partition lengths         │
  └───────────────────────────────────────┘
       │
       │ At task end (instead of merging)
       ▼
  ┌───────────────────────────────────────┐
  │  storePartialFilesInCatalog()         │
  │                                       │
  │  for each partial file:               │
  │    for partId in 0..numPartitions:    │
  │      catalog.addPartition(            │
  │        shuffleId, mapId, partId,      │
  │        handle, offset, length)        │
  └───────────────────────────────────────┘
       │
       ▼
  ┌───────────────────────────────────────┐
  │  MultithreadedShuffleBufferCatalog    │
  │                                       │
  │  ShuffleBlockId(0, 0, 0) -> [         │
  │    Segment(handle1, 0, 100)           │
  │  ]                                    │
  │  ShuffleBlockId(0, 0, 1) -> [         │
  │    Segment(handle1, 100, 150)         │
  │  ]                                    │
  │  ...                                  │
  └───────────────────────────────────────┘
```

**Key change from Phase 1**: At task end, instead of calling `mergePartialFiles()` (which would 
force all data to disk), we call `storePartialFilesInCatalog()`. This keeps data in memory and 
builds an index for direct access.

### 7.2 Read Path

```
┌───────────────────────────────────────────────────────────────────────────┐
│                           Shuffle Read Flow                               │
└───────────────────────────────────────────────────────────────────────────┘

  Reducer requests ShuffleBlockId(0, mapId=5, reduceId=3)
       │
       ▼
  ┌───────────────────────────────────────┐
  │  GpuShuffleBlockResolverBase          │
  │  .getBlockData(blockId)               │
  └───────────────────────────────────────┘
       │
       │ Query catalog
       ▼
  ┌───────────────────────────────────────┐
  │  MultithreadedShuffleBufferCatalog    │
  │  .getMergedBuffer(blockId)            │
  │                                       │
  │  Lookup: blockId -> [                 │
  │    Segment(handle1, 500, 100),        │  <- From batch 1
  │    Segment(handle2, 200, 50)          │  <- From batch 2
  │  ]                                    │
  └───────────────────────────────────────┘
       │
       ▼
  ┌───────────────────────────────────────┐
  │  MultiBatchManagedBuffer              │
  │  (wraps the segments)                 │
  │                                       │
  │  .createInputStream() ->              │
  │    MultiSegmentInputStream            │
  │    reads handle1[500..600]            │
  │    then handle2[200..250]             │
  └───────────────────────────────────────┘
       │
       ▼
  Data returned to reducer (150 bytes total)
```

**Key point**: The reducer is unaware that data comes from multiple sources. It just sees a normal `ManagedBuffer`.

## 8. Lifecycle Management: Why We Need Custom Cleanup

### 8.1 The Problem

In the original design, partial files are merged and then closed. With skip-merge, partial files must stay alive until:
- All reducers have finished reading
- The shuffle is no longer needed

Spark's built-in shuffle cleanup (`ContextCleaner.doCleanupShuffle`) is GC-triggered and unreliable:
- Timing is unpredictable (often happens at app shutdown)
- By then, executors may already be shutting down
- For short-running queries, cleanup may never happen

### 8.2 Our Solution: SQL Execution-Based Cleanup

We trigger cleanup when a SQL query completes, not when GC happens:

```
┌───────────────────────────────────────────────────────────────────────────┐
│                             Cleanup Flow                                  │
└───────────────────────────────────────────────────────────────────────────┘

                                DRIVER
  ┌───────────────────────────────────────────────────────────────────────┐
  │                                                                       │
  │  SQL Execution Ends                                                   │
  │        │                                                              │
  │        ▼                                                              │
  │  ShuffleCleanupListener.onSQLExecutionEnd()                           │
  │        │                                                              │
  │        │  Look up shuffle IDs for this execution                      │
  │        ▼                                                              │
  │  ShuffleCleanupManager.registerForCleanup(shuffleId)                  │
  │        │                                                              │
  │        │  Add to pendingCleanup map                                   │
  │        ▼                                                              │
  │  pendingCleanup: {shuffleId -> timestamp}                             │
  │                                                                       │
  └───────────────────────────────────────────────────────────────────────┘
                                  │
                                  │ Executor polls (every 1s)
                                  ▼
                               EXECUTOR
  ┌───────────────────────────────────────────────────────────────────────┐
  │                                                                       │
  │  ShuffleCleanupEndpoint.pollAndCleanup()                              │
  │        │                                                              │
  │        │  RPC: ask(RapidsShuffleCleanupPollMsg)                       │
  │        │  Response: RapidsShuffleCleanupResponseMsg([shuffleIds])     │
  │        ▼                                                              │
  │  MultithreadedShuffleBufferCatalog.unregisterShuffle(shuffleId)       │
  │        │                                                              │
  │        │  - Close all SpillablePartialFileHandles                     │
  │        │  - Collect statistics (bytesFromMemory, bytesFromDisk)       │
  │        ▼                                                              │
  │  RPC: send(RapidsShuffleCleanupStatsMsg)                              │
  │                                                                       │
  └───────────────────────────────────────────────────────────────────────┘
                                  │
                                  ▼
                                DRIVER
  ┌───────────────────────────────────────────────────────────────────────┐
  │  ShuffleCleanupManager.handleStats()                                  │
  │        │                                                              │
  │        ▼                                                              │
  │  Post SparkRapidsShuffleDiskSavingsEvent to event log                 │
  │  (bytesFromMemory = disk I/O saved)                                   │
  └───────────────────────────────────────────────────────────────────────┘
```

### 8.3 Why Polling (Pull) Instead of Push?

**Short answer**: Spark Plugin API only supports executor → driver communication, not the reverse.

The Plugin API provides:
- `PluginContext.send(message)` - executor sends to driver (fire-and-forget)
- `PluginContext.ask(message)` - executor asks driver (request-response)
- `DriverPlugin.receive(message)` - driver responds to executor

There is **no** `driver.sendToExecutor(executorId, message)` API.

**Benefits of pull model**:
- Uses standard Plugin API, no internal Spark dependencies
- Naturally handles executor failures (they just stop polling)
- Driver doesn't need to track executor list
- 1-second polling overhead is negligible

## 9. Buffer Sizing Optimization (Phase 2 Enhancement)

### 9.1 The Challenge

In Phase 1, all partial files start with the same initial buffer size (default 1GB) and use 
simple doubling when expansion is needed. This is inefficient:
- Large initial size: Wastes memory if actual data is small
- Simple doubling: May over-allocate or require multiple expansions
- No prediction: Each expansion is reactive, not proactive

### 9.2 Predictive Buffer Sizing

Phase 2 introduces smarter buffer sizing with two key changes:

**1. Smaller Initial Size (32MB instead of 1GB)**

Since we now have predictive expansion, we can start with a smaller buffer and grow 
intelligently based on actual write patterns.

**2. Partition-Statistics-Based Prediction**

Instead of simple doubling, we predict the total buffer size needed based on 
already-written partitions within the same batch:

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    Predictive Capacity Calculation                      │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  If completedPartitionCount > 0:                                        │
│                                                                         │
│    avgBytesPerPartition = completedPartitionBytes / completedPartitions │
│    estimatedTotal = avgBytesPerPartition × numPartitions × 1.2          │
│                                              (20% safety margin)        │
│    suggestedCapacity = max(estimatedTotal, requiredCapacity)            │
│                                                                         │
│  Else (first partition, no statistics yet):                             │
│    suggestedCapacity = requiredCapacity × 2  (fallback to doubling)     │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

**Example**: Writing 1000 partitions, first 10 partitions average 1MB each:
- Estimated total = 1MB × 1000 × 1.2 = 1.2GB
- Buffer expands to 1.2GB in one step (instead of multiple doublings)

### 9.3 Expansion Strategy

When a buffer needs to grow:

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         Expansion Decision Flow                         │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  Buffer capacity exceeded (need more space)                             │
│                              │                                          │
│                              ▼                                          │
│  ┌───────────────────────────────────────────────────────────────────┐  │
│  │ Calculate new capacity using capacityHintProvider:                │  │
│  │   - Use partition statistics if available (predictive)            │  │
│  │   - Fallback to doubling if no statistics yet                     │  │
│  │   - Cap at maxBufferSize (default 8GB)                            │  │
│  └───────────────────────────────────────────────────────────────────┘  │
│                              │                                          │
│                              ▼                                          │
│  ┌───────────────────────────────────────────────────────────────────┐  │
│  │ Check constraints:                                                │  │
│  │   1. newCapacity >= requiredCapacity?                             │  │
│  │   2. newCapacity <= maxBufferSize?                                │  │
│  │   3. hostMemoryUsage < threshold? (default 50%)                   │  │
│  └───────────────────────────────────────────────────────────────────┘  │
│                              │                                          │
│                    ┌─────────┴─────────┐                                │
│                    ▼                   ▼                                │
│              [ALL PASS]           [ANY FAIL]                            │
│                    │                   │                                │
│                    ▼                   ▼                                │
│             Allocate new         Spill to disk,                         │
│             buffer & copy        switch to FILE_ONLY                    │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

## 10. Special Cases and Edge Handling

### 10.1 Host-Local Fetch Disabled

**Situation**: When two executors are on the same host, Spark normally optimizes by reading shuffle files directly from disk (host-local optimization).

**Problem**: With skip-merge, data may be in the other executor's memory, not on disk. Host-local disk read would fail or read stale data.

**Solution**: When catalog is enabled, we treat host-local blocks as remote. The fetch goes through network to the target executor, which serves data from its catalog.

### 10.2 External Shuffle Service (ESS)

**Limitation**: Skip-merge only works when ESS is **disabled**.

**Why**: ESS runs in a separate JVM process. It cannot access our in-memory catalog. When ESS is enabled, remote shuffle fetches bypass executors entirely and go to the ESS process, which can only read disk files.

**Default behavior**: `spark.rapids.shuffle.multithreaded.skipMerge` defaults to `false`, so the original merge behavior is used by default. Users must explicitly enable skip-merge after ensuring ESS is disabled.

## 11. Configuration

### New Configurations (Phase 2)

| Config | Default | Description |
|--------|---------|-------------|
| `spark.rapids.shuffle.multithreaded.skipMerge` | `false` | Skip merging partial files and serve data from catalog. Requires ESS disabled. Set to `true` for better performance when shuffle data is not reused across SQL queries. |

### Changed Defaults (Phase 2)

| Config | Phase 1 Default | Phase 2 Default | Reason |
|--------|-----------------|-----------------|--------|
| `spark.rapids.memory.host.partialFileBufferInitialSize` | 1GB | **32MB** | Smaller initial size works better with predictive expansion |

## 12. Files Changed Summary (Phase 2 additions)

| File | Change Type | Description |
|------|-------------|-------------|
| `MultithreadedShuffleBufferCatalog.scala` | New | In-memory index for shuffle blocks |
| `MultiBatchManagedBuffer.scala` | New | Multi-segment ManagedBuffer impl |
| `ShuffleCleanupManager.scala` | New | Driver-side cleanup coordinator |
| `ShuffleCleanupListener.scala` | New | Spark listener for SQL execution events |
| `ShuffleCleanupEndpoint.scala` | New | Executor-side cleanup poller |
| `RapidsShuffleInternalManagerBase.scala` | Modified | Add catalog registration path |
| `GpuShuffleBlockResolverBase.scala` | Modified | Query catalog for block data |
| `SpillablePartialFileHandle.scala` | Modified | Add `readAt()` method, add `capacityHintProvider` for predictive sizing |
| `RapidsLocalDiskShuffleMapOutputWriter.scala` | Modified | Implement `capacityHintProvider` using partition statistics |
| `RapidsConf.scala` | Modified | Change `partialFileBufferInitialSize` default from 1GB to 32MB |

## 13. Benefits Summary

| Benefit | Description |
|---------|-------------|
| **Reduced disk I/O** | Data can be served directly from memory |
| **Lower latency** | No merge step at task end |
| **Better memory utilization** | Keep hot data in memory until needed |
| **Observable** | `SparkRapidsShuffleDiskSavingsEvent` shows bytes served from memory |
