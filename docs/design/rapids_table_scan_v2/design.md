---
layout: page
title: Cudf-spark Unified Partition Reader Design
nav_order: 18
parent: Developer Overview
---

# Cudf-spark Unified Partition Reader Design

## 1. Background & Motivation

Spark RAPIDS currently maintains three kinds of partition readers — **per-file**,
**multi-threaded**, and **coalescing** — and re-implements each kind for every table format we
support: raw Parquet, Iceberg, and Delta Lake. Code is shared by inheriting from the raw Parquet
readers or embedding them as internal fields. Two problems fall out of this design.

### Problem 1: Combination is decided after IO, by arrival order

Take `com.nvidia.spark.rapids.parquet.MultiFileCloudParquetPartitionReader` as an example. It
submits one buffering job per file to a shared thread pool, and the Spark task thread combines
whichever buffers happen to be complete when it looks:

```
Spark task thread              Shared reader thread pool
─────────────────────────      ─────────────────────────────────────────────
submit one job per file ─────► file A: footer + filter ─► buffer whole file ─┐
                               file B: footer + filter ─► buffer whole file ─┤ completes in
                               file C: footer + filter ─► buffer whole file ─┘ arbitrary order
wait for next buffer    ◄──────────────────────────────────────────────────
combine? decided HERE,
from whichever buffers
happen to be ready
decode on GPU
```

Because the combination decision happens after IO, it depends on the randomness of IO
completion: two combinable files that finish far apart are decoded as two small batches. For raw
Parquet this is a minor issue — combinability is mostly a schema check. For table formats it is
serious, because there are many more reasons two files must not combine:

- when a query projects the `_spec` metadata column, files from different partition specs cannot
  be combined;
- when equality deletes are present, files from different snapshots cannot be combined.

The more constraints a table format adds, the fewer combinations survive arrival-order luck, and
small-file-heavy user environments degrade to per-file-sized GPU batches. This can cause serious
performance issues in production.

### Problem 2: The reader matrix is expensive to maintain

Three reader kinds times three table formats couples scheduling, IO, and decoding together in
nine variants connected by inheritance. Extending one table format's functionality means
navigating raw-Parquet internals it happens to inherit. What actually needs to be shared is much
narrower: the **scheduling logic**, the **file IO layer**, and the **file-format decoding**.

## 2. Goals

1. A new extensible framework for a unified partition reader.
2. Explore the design space of each component.

## 3. Non-Goals

1. File-format-specific algorithms (Parquet/ORC decoding itself).
2. Table-format-specific algorithms (delete handling, schema evolution, etc.).

## 4. Design

We propose one partition-reader framework that splits execution into four explicit phases:

```
     shared pool             task thread              shared pool             task thread
┌───────────────────┐   ┌─────────────────┐   ┌───────────────────────┐   ┌────────────────┐
│ 1. footer fetch   │──►│ 2. incremental  │──►│ 3. subtask IO +       │──►│ 4. decode +    │
│    + filtering    │   │    planning     │   │    combination        │   │    post-       │
│    (parallel)     │   │  (file order)   │   │    (parallel)         │   │    processing  │
└───────────────────┘   └─────────────────┘   └───────────────────────┘   └────────────────┘
```

The partition reader becomes one class that drives these phases; every phase is an extension
point. The key inversion versus today: **combination is planned before IO**, so it is decided by
table-format rules instead of arrival order, and the IO layer materializes each planned
combination directly:

```
Spark task thread               Shared thread pool
──────────────────────────      ────────────────────────────────────────────
submit footer jobs ───────────► footer + filter per file (parallel)
consume footers in file order ◄─
incremental planning:
  emit subtask when full ─────► one subtask job per planned combination:
                                  allocate exact-sized output buffer
                                  read every chunk to its final offset
                                  write header/footer, seal
take next COMPLETED subtask ◄─── publish completed subtask (any order)
decode on GPU
table-format post-processing
```

### 4.1 Parallel footer processing

This phase filters file footers with the predicates pushed down from the scan node. The
filtering itself differs per table format (Iceberg, Delta, Hive, ...) and per file format
(Parquet, ORC, ...); only the parallel execution is shared. The output of this phase is, per
file, the surviving row groups plus whatever per-file state the later phases need.

### 4.2 Planning

Given filtered row groups, a planner decides which row groups from which files are read together
as one **subtask**. This is the most extensible component:

- The perfile, multi-threaded and coalescing readers are, at their core, different planning strategies,
  so this phase unifies them.
- Table formats extend the planner with their combination-compatibility rules (partition specs,
  snapshots, delete files, schema/rebase compatibility) — applied at plan time with complete
  information, not at decode time against whatever arrived.
- To reduce end-to-end latency, planning runs incrementally: footers are consumed in file order
  as they resolve, and a subtask is submitted for execution the moment the planner closes it,
  so phase-3 IO overlaps the remaining footer fetches.

### 4.3 Parallel IO execution

Each planned subtask executes independently on the shared pool. Because the subtask's exact
output layout is known from the plan, every column chunk is read directly to its final offset in
one preallocated, exact-sized output buffer. Compared with combining after IO, this has two
advantages:

1. One less memory copy for in-memory output buffers: combination is just the header/footer
   write around chunks that are already in place.
2. Combination work runs on pool threads instead of the Spark task thread.

Further exploration in this component — file-backed output buffers, a standalone IO thread pool,
GDS for file output buffers — is possible but out of scope for this design.

### 4.4 Decoding

The task thread picks whichever subtask completes first and decodes it — a later subtask is
never blocked behind an earlier one that is slow to finish. Decoding is file-format specific,
and table formats append their own post-processing steps (for example schema evolution, metadata
columns, or delete application) after decode.

### 4.5 Extension points

| Component | Framework provides | File format extends | Table format extends |
| --- | --- | --- | --- |
| Footer processing | parallel execution, ordered consumption | footer parsing, row-group filtering | predicate/delete semantics, per-file state |
| Planning | incremental driver, greedy/coalescing strategies, size targets | row-group sizing inputs | combination-compatibility rules |
| IO execution | subtask scheduling, output buffers, in-place combination | byte ranges per column chunk | file IO (e.g. Iceberg `FileIO`, credentials) |
| Decoding | completion-order dispatch | GPU decode (Parquet, ORC) | post-processing steps |
