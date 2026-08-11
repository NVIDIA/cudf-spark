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
