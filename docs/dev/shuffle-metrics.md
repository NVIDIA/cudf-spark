---
layout: page
title: Shuffle Metrics
nav_order: 15
parent: Developer Overview
---
# Shuffle Metrics: SparkRapidsShuffleDiskSavingsEvent

When using MULTITHREADED shuffle mode with `spark.rapids.shuffle.multithreaded.skipMerge=true`, the
NVIDIA cuDF plugin for Apache Spark emits `SparkRapidsShuffleDiskSavingsEvent` to the Spark event
log.  This document explains how to interpret and aggregate these events.

## Event Format

Each executor posts its own event when it cleans up shuffle data. A single shuffle may have
multiple events in the eventlog (one per executor that participated in the shuffle write).

Event format in eventlog (JSON):
```json
{"Event":"com.nvidia.spark.rapids.SparkRapidsShuffleDiskSavingsEvent",
 "shuffleId":0,"bytesFromMemory":7868,"bytesFromDisk":0}
```

## Why Custom Events Instead of Task Metrics

Spark task metrics are committed when a task completes. However, shuffle data lifecycle
extends beyond task completion - buffers may be spilled to disk after a task finishes but
before the shuffle data is read. The final `bytesFromMemory` vs `bytesFromDisk` statistics
can only be determined when shuffle cleanup occurs (after the SQL query completes), at
which point task metrics are no longer updatable.

## Field Descriptions

| Field | Description |
|-------|-------------|
| `shuffleId` | The Spark shuffle ID |
| `bytesFromMemory` | Bytes kept in memory and never written to disk (actual disk I/O savings) |
| `bytesFromDisk` | Bytes spilled to disk due to memory pressure |

The sum of `bytesFromMemory` across all events should approximately match the total
"Shuffle Bytes Written" reported in task metrics.

## Aggregating Events

To get application-wide totals from an eventlog:

```bash
grep "SparkRapidsShuffleDiskSavingsEvent" eventlog | \
  jq -s '{
    totalBytesFromMemory: (map(.bytesFromMemory) | add),
    totalBytesFromDisk: (map(.bytesFromDisk) | add),
    diskSavingsBytes: (map(.bytesFromMemory) | add)
  }'
```

## Timing Considerations

The cleanup mechanism uses a polling model where executors poll the driver every 1 second.
For short-running applications or scripts, the session may exit before executors have a
chance to poll and report their statistics.

To ensure all events are captured, add a short delay before exiting:

```scala
// After your last query completes
Thread.sleep(2000)  // Wait for executor cleanup polling
spark.stop()
```

For long-running applications or interactive sessions (spark-shell, notebooks), this is
typically not an issue as there is enough time between queries for cleanup to complete.

# Shuffle Metrics: SparkRapidsShuffleReclaimEvent

When using UCX shuffle mode with `spark.rapids.shuffle.ucx.cleanupOnQueryEnd.enabled=true`, the
plugin reclaims a GPU shuffle once the SQL execution that wrote it has ended and no task that
reads or writes it is still running. A second action on the same DataFrame reruns the map stage
of each reclaimed shuffle. The documentation of that key names the queries that should keep the
reclaim off. The plugin emits one `SparkRapidsShuffleReclaimEvent` to the Spark event log for
each SQL execution that wrote a GPU shuffle. Setting
`spark.rapids.shuffle.ucx.cleanupOnQueryEnd.eventLog.enabled=false` turns the event off and
leaves the reclaim as it is.

## Event Format

The driver posts the event once every shuffle that the execution wrote has an outcome. The event
counts the shuffles that the plugin reclaimed, and lists by reason the ids of the shuffles that it
left to Spark's own cleanup, which removes a shuffle once its dependency is garbage collected.

Event format in eventlog (JSON):
```json
{"Event":"com.nvidia.spark.rapids.SparkRapidsShuffleReclaimEvent",
 "executionId":3,"endedAtMs":1762300000000,"completedAtMs":1762300001250,
 "shuffles":3,"reclaimed":1,"alreadyRemoved":0,
 "skippedShared":[],"skippedUnconsumed":[],"skippedCached":[],"skippedIndeterminate":[4,5],
 "executorsFailed":0,"executorsTimedOut":0,"executorsNeverPolled":0,"executorsDeferred":0,
 "buffersFreed":16,"bytesFreed":7868,"maxWaitMs":12,"maxRemoveMs":1238}
```

## Field Descriptions

| Field | Description |
|-------|-------------|
| `executionId` | The SQL execution ID |
| `endedAtMs` | When the execution ended |
| `completedAtMs` | When the execution's last shuffle got its outcome, and no earlier than `endedAtMs` |
| `shuffles` | GPU shuffles the execution wrote |
| `reclaimed` | Shuffles cleared on the driver and handed to their executors to free |
| `alreadyRemoved` | Shuffles that Spark's own cleanup removed first |
| `skippedShared` | Ids of shuffles that another execution, a job outside SQL or a late job also used, or that the reclaim gave up on, such as after a failed clear |
| `skippedUnconsumed` | Ids of shuffles that nothing in the succeeded execution read, since a lazy iterator may still read them |
| `skippedCached` | Ids of shuffles that a cached DataFrame depends on |
| `skippedIndeterminate` | Ids of shuffles whose map stage Spark marks indeterminate, such as the shuffle of a `repartition(n)` without columns and every shuffle after it |
| `executorsFailed` | Executor reclaims that failed |
| `executorsTimedOut` | Executors that timed out on a shuffle: silent for 10 minutes after its release, or handed it twice with no result |
| `executorsNeverPolled` | Timed-out executors that were never handed the shuffle |
| `executorsDeferred` | Executors that kept the execution's output under `spark.shuffle.useOldFetchProtocol=true`, because a later task wrote to the same shuffle |
| `buffersFreed` | Buffers the executors freed |
| `bytesFreed` | Bytes the executors freed |
| `maxWaitMs` | Longest wait from the execution's end to the decision to reclaim a shuffle, which running tasks cause |
| `maxRemoveMs` | Longest time from the decision to reclaim a shuffle to its outcome: every executor reported, timed out or left, or the reclaim withdrew the shuffle or found it removed |

The shuffles in `skippedShared`, `skippedUnconsumed`, `skippedCached` and `skippedIndeterminate`
wait for Spark's own cleanup, as they do with the reclaim off. The executor counts are summed
over the execution's shuffles. The driver log has one DEBUG line per shuffle from
`com.nvidia.spark.rapids.ShuffleCleanupManager`, and a WARN line for each failure or timeout.

## Aggregating Events

To count the reclaimed shuffles and the shuffles in each skipped list for an application:

```bash
grep "SparkRapidsShuffleReclaimEvent" eventlog | \
  jq -s '{
    executions: length,
    shuffles: (map(.shuffles) | add),
    reclaimed: (map(.reclaimed) | add),
    alreadyRemoved: (map(.alreadyRemoved) | add),
    skippedShared: (map(.skippedShared | length) | add),
    skippedUnconsumed: (map(.skippedUnconsumed | length) | add),
    skippedCached: (map(.skippedCached | length) | add),
    skippedIndeterminate: (map(.skippedIndeterminate | length) | add),
    bytesFreed: (map(.bytesFreed) | add)
  }'
```

Both recipes in this file read an uncompressed event log. Spark 4.0 and later compress event logs
by default, with zstd, so decompress such a log first, for example with `zstd -dc`, or set
`spark.eventLog.compress=false`.

## Timing Considerations

A shuffle's outcome waits for each executor that ran one of its tasks to free it and report. An
executor that stays silent for 10 minutes, or that is handed the shuffle twice with no result,
times out. An application that stops before an execution's last outcome posts no event for that
execution. Once the reclaim turns itself off, which one WARN line reports, it posts no event for
an execution that had not ended by then, or that still had a shuffle waiting on its tasks or
jobs, nor for any later execution.
