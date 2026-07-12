# DBR 17.3 native Delta data-file writes

## Design

DBR 17.3 retains ownership of the Delta catalog and transaction control plane. In particular,
`AtomicCreateTableAsSelectExec`, `AtomicReplaceTableAsSelectExec`, staged Delta tables, Unity
Catalog integration, protocol and metadata selection, coordinated commits, the native committer,
history, and cache invalidation remain DBR code.

The accelerator registers the nested `WriteIntoDeltaCommand` as a typed
`DataWritingCommandRule`. Eligible commands execute as `GpuDataWritingCommandExec`, retain the
native command's output specification, Hadoop configuration, options, committer, protocol,
metadata, statistics trackers, and metrics, and use `GpuFileFormatWriter` for data-file creation.
No reflection is used.

DBR planned writes have this shape:

```text
GpuDataWritingCommandExec
+- GpuWriteFilesExec
   +- GPU query plan
```

`GpuWriteFilesExec` intentionally has no output attributes. The adapter maps the native logical
output ordering to the nested GPU data child's output and passes the `GpuWriteFilesExec` wrapper
to `GpuFileFormatWriter`, preserving the planned-write execution contract.

## Eligibility and fallbacks

The GPU path requires the Delta and Parquet writers to be enabled and applies the repository's
normal GPU Parquet schema, encryption, compression, timestamp, rebase, and bloom-filter checks.
It accepts DBR default, explicitly enabled, and explicitly disabled deletion vectors because the
pending command protocol and metadata are used.

The following cases remain on CPU:

- Partitioned writes. DBR's Delta committer requires its private
  `DeltaFileFormatWriter.PartitionedTaskAttemptContextImpl`; the shared GPU file writer supplies a
  Spark `TaskAttemptContextImpl`. Attempting GPU execution would fail while creating partition
  files, so this is an explicit correctness fallback.
- `StatisticsOnLoadJobTracker`. Supporting it requires DBR's Delta analyze/HLL/NDV statistics
  implementation, not omission of those statistics.
- Unknown statistics trackers, static partitions, bucketed writes, non-Delta-Parquet file formats,
  and inputs rejected by standard GPU Parquet write checks.

The outer atomic catalog operation appearing as CPU is expected control-plane work. Eligibility is
determined by the nested `WriteIntoDeltaCommand` and `WriteFilesExec`; both must be reported on GPU
for an accelerated data-file write.

## Coverage matrix

| Behavior | DBR 17.3 API/path | Status |
| --- | --- | --- |
| Fresh managed CTAS/saveAsTable | `AtomicCreateTableAsSelectExec` -> staged Delta V2 -> `WriteIntoDeltaCommand` | Native CPU catalog/commit control plane; GPU query and data-file plane |
| Existing managed RTAS/saveAsTable | `AtomicReplaceTableAsSelectExec` -> staged Delta V2 -> `WriteIntoDeltaCommand` | Native CPU catalog/commit control plane; GPU query and data-file plane |
| SQL CTAS and RTAS | Same nested native command | GPU data-file plane when the command is eligible |
| External unpartitioned table | Native output spec and committer | GPU data-file plane |
| `overwriteSchema` | Native protocol, metadata, and output specification | GPU data-file plane; ordered-schema parity validated |
| Deletion vectors: default/auto, true, false | Pending command protocol and metadata | GPU data-file plane; no requirement to disable deletion vectors |
| AddFile statistics and tags | Native `DeltaJobStatisticsTracker` adapted to the columnar writer | GPU, with CPU/GPU Delta-log parity validation |
| Basic write metrics | Native driver metrics plus write-job metrics | GPU, including task commit time |
| Unity Catalog/catalog-owned/coordinated commit | Native staged table, committer, and outer transaction | Preserved by design; authoritative end-to-end validation requires a UC-enabled job |
| Partitioned writes | Native committer requires private `PartitionedTaskAttemptContextImpl` | CPU fallback; shared GPU writer cannot construct the DBR-private context safely |
| Statistics-on-load/HLL/NDV | `StatisticsOnLoadJobTracker` | CPU fallback; no equivalent GPU tracker currently exists |
| Bucketed/static-partition/non-Parquet writes | Native command variants | CPU fallback; outside the supported Delta Parquet writer contract |
| Unknown statistics tracker | DBR-private tracker | CPU fallback, fail closed to preserve correctness |

## Validation scope

Focused DBR 17.3 validation covers managed fresh CTAS and existing RTAS using the customer writer
chain, `overwriteSchema`, default/true/false deletion vectors, ordered schema, rows, protocol,
metadata, properties, history, AddFile statistics, partition values, stable tags, time travel, and
cache invalidation. It also covers external and SQL CTAS/RTAS paths where the same native command
shape is produced. CPU Parquet `ParquetRecordWriter` must not appear for an eligible boundary.

Unity Catalog ownership and coordinated-commit behavior remain native by design, but require an
actual UC-enabled Databricks job for authoritative validation. Local `spark-submit` does not expose
the `samples` catalog used by the unchanged customer notebooks.

## Performance validation

A warm ABBA benchmark on DBR 17.3 used 5,000,000 rows, eight input/output files, default deletion
vectors, and separate CPU/GPU target tables. Medians and observed ranges were:

| Measurement | CPU | GPU | CPU/GPU speedup |
| --- | ---: | ---: | ---: |
| Query-only scan and aggregate | 1.060 s (0.918-1.242) | 0.867 s (0.789-0.905) | 1.22x |
| RTAS scan plus Delta file write | 6.494 s (6.392-6.897) | 1.930 s (1.798-2.181) | 3.36x |
| 1,000-row commit/control | 0.799 s (0.650-1.158) | 1.215 s (0.908-1.547) | 0.66x |

The much larger end-to-end RTAS speedup than the query-only speedup demonstrates that data-file
generation moved to the GPU. The tiny control shows the remaining native catalog/transaction
overhead and GPU startup cost; small writes are not expected to benefit.
