# History-backed planning heuristics

Planning decisions that learn from earlier runs. A heuristic reads history when a query plans
and records what the query did when it ends. If history is off, missing, stale or unusable, the
static decision is used unchanged; the two are never blended. The one heuristic today sizes scan
splits for GPU file readers: v1 file sources (catalog tables and path reads, including Delta),
Hive text tables, and DSv2 file scans (Parquet, ORC, CSV, JSON, Avro).

## Enabling

| Key | Default | Meaning |
|---|---|---|
| `spark.rapids.sql.history.metrics.provider` | `none` | any other value enables history-backed planning |
| `spark.rapids.sql.history.planningTimeoutMillis` | 100 | budget for one history lookup while planning |
| `spark.rapids.sql.history.declareTimeoutMillis` | 5000 | budget for declaring each metric family to the store |
| `spark.rapids.sql.history.scanSplit.enabled` | `false` | `true` turns on the scan split heuristic when a provider is set |
| `spark.rapids.sql.history.scanSplit.aggregate` | `latest` | how the readings that a scan finds are combined: `latest` (the newest reading, a bounded read), `mean`, `min`, `max` (the largest ratio gives the smallest split) or `consistent` (the mean when the readings are within 3x of each other, else Spark's split) |
| `spark.rapids.sql.history.scanSplit.maxAgeDays` | 7 | how old a ratio may be; lookup only, capped at the store's 14 day retention |
| `spark.rapids.sql.history.scanSplit.minBytes` | 64 MiB | lower bound of a learned split |
| `spark.rapids.sql.history.scanSplit.maxBytes` | 4 GiB | upper bound of a learned split |

Planning only uses history once a provider store is actually installed. This package talks to
the history metrics API (`MetricStores.current()`) only; the driver plugin's
`HistoryMetricsManager` owns the provider.

## How scan splits are sized

- **Measure**: when a query ends, each scan records its decode-expansion ratio, GPU output batch
  bytes / on-disk bytes. It is recorded only if every planned split was read to the end; a split
  read more than once (task or stage retries) counts once.
- **Key**: three dimensions: `plan` and `literals`, the two parts of a `PlanKey` (the table name,
  else `path:` + the sorted root paths; the columns read; the shape of the filters with literals
  replaced by `?`; and the literals and resolved dynamic pruning keys), each stored as a 64-bit
  hash computed in place (no string is built), and `size`, `floor(log2(listedBytes))`. Known
  limitations:
  - A path read is keyed by its roots as written, so another spelling of the same data (a
    different partition directory, an expanded glob, a new file list) starts with no history.
  - DSv2 file scans are always keyed by path, so one table read through v1 and through DSv2 has
    two histories.
- **Decide**: one lookup in the store with three tiers (the same plan and literals and size; the
  same plan and literals and a neighbouring size; the same plan, any literals, a neighbouring or
  the same size). The first tier with a reading (at most `maxAgeDays`, default 7, old) is combined
  by `aggregate` (default: the latest) and sets
  `split = targetBatchBytes / ratio`, clamped to `[minBytes, min(maxBytes, listedBytes / minPartitionNum)]`
  (defaults 64 MiB and 4 GiB). If `listedBytes / minPartitionNum` is below `minBytes`, the scan
  keeps Spark's split.
- **minPartitionNum**: resolved as Spark does for file scans: `spark.sql.files.minPartitionNum`,
  else `spark.sql.leafNodeDefaultParallelism`, else the SparkContext's default parallelism (the
  cores registered when the query plans, if nothing is configured).

| Step | Where |
|---|---|
| decide / observe (v1) | `GpuFileSourceScanExec.createNonBucketedReadRDD` / `internalDoExecuteColumnar` |
| decide / observe (Hive text) | `GpuHiveTableScanExec.createReadRDDForDirectories` / `internalDoExecuteColumnar` |
| decide / observe (DSv2) | `HistorySizedFileScan.partitions` / `GpuBatchScanExec.internalDoExecuteColumnar` (`registerForExecution`) |

All three register their scan every time the exec runs, so a plan that is executed again is
recorded again.

Driver log lines, kept stable for tooling:

```
scan.split: <k> tier=<n> ratio=<r> targetBatch=<b> rawSplit=<s>
scan.split: <k> listed=<l> -> split=<n> bytes
scan.split: <k> listed=<l> -> spark default
scan.split: <k> not recorded: <reason>
scan.split: not recorded: the scan has no key
```

`<k>` is `<table or last path segment> literals=<v> size=<z> plan=<p> lit=<q>`, with `<p>` and `<q>`
the two hashes in 16 hex digits and `<v>` the literals, capped. At DEBUG, each key is also logged
with its readable text (source, columns, filter shapes, literals; each part capped):
`scan.split: key plan=<p> literals=<q> <text>`.

Store errors are logged once per JVM per reason and the decision falls back to static.

## Adding a heuristic

Code layers: `HistoryMetric` (family contract) -> `MetricHistory` (store access, `HistoryPolicy`)
-> `ScanSplitHeuristic` (decide/observe cycle), driven by `ExecutionEndCallbacks` (runs the
callbacks of a query when it ends) and `HistoryLifecycle` (start, activate, stop).

There is one heuristic, so there is no shared base class yet. A second one is the point to
extract what the two have in common, rather than to guess it now.

1. Define the family: `object X extends HistoryMetric`, with a permanent `id` and `name` and its
   `dimensions`, up to eight named, typed ones, with a value for each in the key. The
   plugin gives every registered family to the history provider as its catalog
   (`HistoryHeuristics.catalog`), which admits only those ids. An id or name is never reused or
   changed, because stored observations are keyed by the id. A clash between two families makes
   building the catalog fail, which leaves history off.
2. Write the heuristic as `ScanSplitHeuristic` is written (key, static decision, formula, bounds,
   observation, and `enabled`, which reads its own `spark.rapids.sql.history.<name>.enabled`
   switch) as a singleton over `MetricStores.current()`, and add it to the list in
   `HistoryHeuristics`, whose lifecycle is typed to `ScanSplitHeuristic` today.
3. Call `decide` and `register` from the planning site.
