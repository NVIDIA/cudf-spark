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
| `spark.rapids.sql.history.scanSplit.enabled` | `true` | `false` turns off only the scan split heuristic |
| `spark.rapids.sql.history.scanSplit.maxAgeDays` | 7 | how old the last ratio may be; lookup only, capped at the store's 14 day retention |
| `spark.rapids.sql.history.scanSplit.minBytes` | 64 MiB | lower bound of a learned split |
| `spark.rapids.sql.history.scanSplit.maxBytes` | 4 GiB | upper bound of a learned split |

Planning only uses history once a provider store is actually installed. This package talks to
the history metrics API (`MetricStores.current()`) only; the driver plugin's
`HistoryMetricsManager` owns the provider.

## How scan splits are sized

- **Measure**: when a query ends, each scan records its decode-expansion ratio, GPU output batch
  bytes / on-disk bytes. It is recorded only if every planned split was read to the end; a split
  read more than once (task or stage retries) counts once.
- **Key**: the table name (catalog or Hive identifier), else `path:` + the
  sorted root paths (user info and trailing `/` dropped). Keys over 253 UTF-8 bytes become a
  readable prefix + `#` + 16 hex of their SHA-256. A file list is keyed on its exact files.
  The key identifies the table, not what a query reads from it. Known limitations:
  - The ratio depends on the columns a scan projects and the filters it pushes down, which are
    not part of the key. Scans of one table that read different columns or filters share one
    key, and the latest recording wins. A fingerprint of the read schema and filters is future
    work.
  - A path read is keyed by its roots as written, so another spelling of the same data (a
    different partition directory, an expanded glob, a new file list) starts with no history.
  - DSv2 file scans are always keyed by path, so one table read through v1 and through DSv2 has
    two histories.
- **Decide**: the next scan of that table reads the latest ratio (at most `maxAgeDays`, default 7, old) and sets
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
scan.split: table=<t> ratio=<r> targetBatch=<b> rawSplit=<s>
scan.split: table=<t> listed=<l> -> split=<n> bytes
scan.split: table=<t> listed=<l> -> no history, spark default
scan.split: table=<t> not recorded: <reason>
```

Store errors are logged once per JVM per reason and the decision falls back to static.

## Adding a heuristic

Code layers: `HistoryMetric` (family contract) -> `MetricHistory` (store access, `HistoryPolicy`)
-> `HistoryHeuristic` (decide/observe cycle, lifecycle) -> `ScanSplitHeuristic`.

1. Register a family id and name in the API's `HistoryMetricCatalog`.
2. Define it: `object X extends HistoryMetric`.
3. Extend `HistoryHeuristic` (key, static decision, formula, bounds, observation, and `enabled`,
   which reads its own `spark.rapids.sql.history.<name>.enabled` switch) as a singleton over
   `MetricStores.current()`, and add it to the list in `HistoryHeuristics`.
4. Call `decide` and `register` from the planning site.
