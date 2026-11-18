---
layout: page
title: RAPIDS Shuffle Manager
parent: Additional Functionality
nav_order: 6
---
# RAPIDS Shuffle Manager

This document describes how to configure and use the RAPIDS Shuffle Manager,
which provides GPU-accelerated shuffle operations for Apache Spark.

## Overview

The RAPIDS Shuffle Manager replaces Spark's default shuffle implementation with
optimized GPU-aware shuffle operations. It offers two operating modes:

| Mode | Description |
|------|-------------|
| **Standard Mode** (skipMerge=false) | Default mode. Partial shuffle files are merged into a single file per map task. Compatible with External Shuffle Service (ESS). |
| **Skip-Merge Mode** (skipMerge=true) | Advanced mode. Skips the merge step and serves data directly from memory. Lower latency but requires specific configuration. |

## Part 1: Standard Mode (skipMerge=false)

This is the default and recommended mode for most deployments.

### Advantages

- **ESS Compatible**: Works with External Shuffle Service, enabling dynamic allocation
- **Simpler Configuration**: Fewer prerequisites to configure
- **Broader Compatibility**: Works in all deployment environments
- **Shuffle Reuse Friendly**: Safe to use with Databricks or other platforms that reuse shuffle data
- **Fault Tolerant**: Shuffle data persisted on disk survives executor restarts

### Disadvantages

- **Merge Overhead**: Data is written to partial files, then merged into a single file
- **Disk I/O**: All shuffle data eventually goes to disk before being served to reducers

### Configuration

```bash
# Required: Enable RAPIDS Shuffle Manager
# Replace spark332 with your Spark version (e.g., spark350, spark400)
--conf spark.shuffle.manager=com.nvidia.spark.rapids.spark332.RapidsShuffleManager

# Optional: Explicitly set mode (this is the default)
--conf spark.rapids.shuffle.mode=MULTITHREADED

# Optional: Tune writer/reader thread counts (defaults are usually good)
--conf spark.rapids.shuffle.multiThreaded.writer.threads=16
--conf spark.rapids.shuffle.multiThreaded.reader.threads=16
```

### Memory Considerations

Standard mode uses a **memory-first** write strategy: shuffle data is initially written to
host memory buffers, then spilled to disk at commit time. Without `offHeapLimit` configured,
the memory threshold check is ineffective (always assumes memory is available), which may
lead to excessive memory usage with many concurrent tasks.

For YARN or Kubernetes deployments, consider setting off-heap limits:

```bash
--conf spark.rapids.memory.host.offHeapLimit.enabled=true
--conf spark.rapids.memory.host.offHeapLimit.size=30g
--conf spark.executor.memoryOverhead=35g
```

### Complete Example

```bash
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --conf spark.plugins=com.nvidia.spark.SQLPlugin \
  --conf spark.executor.memory=20g \
  --conf spark.executor.memoryOverhead=35g \
  --conf spark.shuffle.manager=com.nvidia.spark.rapids.spark332.RapidsShuffleManager \
  --conf spark.rapids.shuffle.mode=MULTITHREADED \
  --conf spark.rapids.memory.host.offHeapLimit.enabled=true \
  --conf spark.rapids.memory.host.offHeapLimit.size=30g \
  your-application.jar
```

---

## Part 2: Skip-Merge Mode (skipMerge=true)

This advanced mode provides better performance by avoiding disk I/O when possible,
but requires additional configuration.

> **Warning**: Skip-merge mode keeps shuffle data in executor memory. If an executor
> crashes and restarts, shuffle data from that executor is lost. While Spark should
> trigger stage retries to recover, this fault tolerance behavior has not been
> thoroughly tested yet (see [#14325](https://github.com/NVIDIA/spark-rapids/issues/14325)).
> **This mode is not recommended for production workloads that require high reliability.**

### Advantages

- **Reduced Disk I/O**: Shuffle data can be served directly from host memory
- **Lower Latency**: No merge step at map task completion
- **Observable Savings**: Emits `SparkRapidsShuffleDiskSavingsEvent` showing bytes served from memory vs disk

### Disadvantages

- **Untested Fault Tolerance**: Shuffle data is lost if executor crashes; recovery behavior needs testing ([#14325](https://github.com/NVIDIA/spark-rapids/issues/14325))
- **No ESS Support**: Cannot use External Shuffle Service (dynamic allocation limited)
- **Memory Requirements**: Requires off-heap memory limit configuration
- **Not for Shuffle Reuse**: Not recommended when shuffle data is reused across queries

### Prerequisites

Before enabling skipMerge, ensure:

1. **External Shuffle Service is DISABLED**
   - `spark.shuffle.service.enabled=false` (this is the default)

2. **Off-heap memory limits are ENABLED**
   - Required to prevent OOM from unbounded buffer growth

### Configuration

```bash
# Required: Enable RAPIDS Shuffle Manager
--conf spark.shuffle.manager=com.nvidia.spark.rapids.spark332.RapidsShuffleManager

# Required: Enable skipMerge
--conf spark.rapids.shuffle.multithreaded.skipMerge=true

# Required: Enable off-heap memory limit
--conf spark.rapids.memory.host.offHeapLimit.enabled=true
--conf spark.rapids.memory.host.offHeapLimit.size=50g

# Required: Disable ESS (should be disabled by default)
--conf spark.shuffle.service.enabled=false
```

### Memory Overhead for YARN and Kubernetes

**Important**: When configuring `offHeapLimit.size`, you must also increase the
executor memory overhead to prevent container OOM kills.

The off-heap memory is allocated outside the JVM heap, so the container needs
additional memory beyond `spark.executor.memory`.

```bash
# Set executor memory overhead to cover off-heap allocation
# Rule: memoryOverhead >= offHeapLimit.size + some buffer (e.g., 5g)
--conf spark.executor.memoryOverhead=55g
```

### Complete Example

```bash
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --conf spark.plugins=com.nvidia.spark.SQLPlugin \
  --conf spark.executor.memory=20g \
  --conf spark.executor.memoryOverhead=55g \
  --conf spark.shuffle.manager=com.nvidia.spark.rapids.spark332.RapidsShuffleManager \
  --conf spark.rapids.shuffle.multithreaded.skipMerge=true \
  --conf spark.rapids.memory.host.offHeapLimit.enabled=true \
  --conf spark.rapids.memory.host.offHeapLimit.size=50g \
  --conf spark.shuffle.service.enabled=false \
  your-application.jar
```

---

## Choosing Between Modes

| Consideration | Standard Mode | Skip-Merge Mode |
|--------------|---------------|-----------------|
| Fault Tolerance | **Yes** (data on disk) | **Untested** (data lost on crash) |
| ESS / Dynamic Allocation | Yes | No |
| Disk I/O | Higher | Lower |
| Configuration Complexity | Simple | More complex |
| Memory Requirements | Lower | Higher |
| Shuffle Reuse (Databricks) | Safe | Not recommended |
| Production Ready | **Yes** | **No** (experimental) |

### When to Use Standard Mode

- **Production workloads** that require reliability
- You need External Shuffle Service for dynamic allocation
- Running on Databricks or platforms with shuffle reuse
- Simpler deployment is preferred
- Memory is constrained

### When to Use Skip-Merge Mode

- **Benchmarking or testing** where fault tolerance is not critical
- Short-running batch jobs on stable clusters
- You want to minimize disk I/O for shuffle-heavy workloads
- Sufficient host memory is available
- You can tolerate job failures if executors crash

---

## Verifying Configuration

### Check if RAPIDS Shuffle Manager is Active

Look for this log message in executor logs:

```
INFO RapidsShuffleInternalManager: RAPIDS Shuffle Manager initialized
```

### Check if skipMerge is Active

For skipMerge mode, look for:

```
INFO MultithreadedShuffleBufferCatalog enabled (ESS disabled, off-heap limits on)
```

If you see warnings like these, skipMerge is NOT active:

```
WARN MultithreadedShuffleBufferCatalog disabled - External Shuffle Service (ESS) is enabled
WARN MultithreadedShuffleBufferCatalog disabled - spark.rapids.memory.host.offHeapLimit.enabled is false
```

### Observing Disk Savings (skipMerge only)

When skipMerge is enabled, `SparkRapidsShuffleDiskSavingsEvent` appears in the event log:

```json
{
  "Event": "SparkRapidsShuffleDiskSavingsEvent",
  "Shuffle Id": 0,
  "Bytes From Memory": 104857600,
  "Bytes From Disk": 0
}
```

- **Bytes From Memory**: Data served directly from memory (disk I/O saved)
- **Bytes From Disk**: Data that had to be read from disk (was spilled)

Note: This event is only emitted when skipMerge=true.

---

## Related Documentation

- [Phase 1 Design Document](../design/rapids_shuffle_manager_v2_phase1_design.md)
- [Phase 2 Design Document](../design/rapids_shuffle_manager_v2_phase2_design.md)
