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

### Output attribute mapping

DBR's native file writer pairs `outputSpec.outputColumns` with the physical child output by
ordinal after requiring equal arity. Catalyst can remove pass-through or no-op aliases, leaving
the native output specification with target ExprIds that no longer occur in the query. The
adapter follows the same positional contract. At each ordinal it accepts either the same ExprId
or, only when that ExprId is absent everywhere in the query, an exact structural match. For
column-mapped tables, the query field must match the pending logical metadata and the native output
field must match the corresponding field returned by Delta's typed
`DeltaColumnMapping.createPhysicalSchema` API. It rejects an ExprId found at another ordinal,
revalidates the physical data child at the same ordinal, and preserves the native output
attribute's physical name, type, nullability, and metadata while replacing only its ExprId.
Ambiguous, reordered, or structurally different output fails closed.

### Delta statistics and column mapping

The command's native `DeltaJobStatisticsTracker` remains the owner of the recorded AddFile
statistics. Its columnar adapter uses the pending protocol and metadata and constructs the table
statistics schema with DBR's typed `DeltaColumnMapping.createPhysicalSchema` API. This preserves
physical names and field IDs for `name` and `id` column-mapping modes and excludes partition
columns in the same way as the established DBR GPU Delta transaction path.

## Eligibility and fallbacks

The GPU path requires the Delta and Parquet writers to be enabled and applies the repository's
normal GPU Parquet schema, encryption, compression, timestamp, rebase, and bloom-filter checks.
It accepts DBR default, explicitly enabled, and explicitly disabled deletion vectors because the
pending command protocol and metadata are used.

The following cases remain on CPU:

- Partitioned writes. A DBR-specific GPU partition task context exists, but this nested command
  path has not yet validated `MaterializePartitionColumns`, `writePartitionColumns`, UniForm,
  partition evolution, and their command-specific commit semantics. It therefore fails closed.
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
| SQL CTAS and RTAS | Same nested native command | Eligible when DBR produces the same command shape; not directly asserted by the focused test |
| External unpartitioned table | Native output spec and committer | Eligible when DBR produces the same command shape; not directly asserted by the focused test |
| `overwriteSchema` | Native protocol, metadata, and output specification | GPU data-file plane; ordered-schema parity validated |
| Deletion vectors: default/auto, true, false | Pending command protocol and metadata | GPU data-file plane; no requirement to disable deletion vectors |
| Delta column mapping: `name`, `id` | Native physical output plus `DeltaColumnMapping.createPhysicalSchema` | GPU for managed unpartitioned CTAS/RTAS; logical schema and normalized AddFile statistics validated |
| AddFile statistics and stable tags | Native `DeltaJobStatisticsTracker` adapted to the columnar writer | CPU/GPU log parity for non-mapped tables; normalized physical-statistics parity for mapped tables |
| Basic write metrics | Native driver metrics plus write-job metrics | GPU, including task commit time |
| Unity Catalog/catalog-owned/coordinated commit | Native staged table, committer, and outer transaction | Preserved by design; authoritative end-to-end validation requires a UC-enabled job |
| Partitioned writes | Native partition materialization and DBR-specific task context | CPU fallback in this command path pending semantic coverage |
| Statistics-on-load/HLL/NDV | `StatisticsOnLoadJobTracker` | CPU fallback; no equivalent GPU tracker currently exists |
| Bucketed/static-partition/non-Parquet writes | Native command variants | CPU fallback; outside the supported Delta Parquet writer contract |
| Unknown statistics tracker | DBR-private tracker | CPU fallback, fail closed to preserve correctness |

## Focused validation

The DBR 17.3 focused test directly validates managed fresh CTAS and existing RTAS for
default/false/true deletion vectors and default-DV `name`/`id` column mapping. Each boundary must
contain both `GpuDataWritingCommandExec` and `GpuWriteFilesExec` and must not contain CPU
`DataWritingCommandExec` or `WriteFilesExec`. The test compares rows, ordered logical schema,
protocol and table properties, version-0 time travel, history, AddFile statistics, partition
values, and stable AddFile tags. For column-mapped tables, random per-table physical names are
mapped back to logical names before statistics comparison.

Cache invalidation, failure atomicity, external and SQL forms, Unity Catalog ownership, and
coordinated commits are not all asserted by this focused test. Unity Catalog behavior remains
native by design, but it still requires an actual UC-enabled Databricks job for authoritative
validation.

## Performance validation

A controlled warm ABBA benchmark on DBR 17.3 used 5,000,000 rows, eight input/output files,
default deletion vectors, and separate CPU/GPU target tables. Medians and observed ranges were:

| Measurement | CPU | GPU | CPU/GPU speedup |
| --- | ---: | ---: | ---: |
| Query-only scan and aggregate | 1.060 s (0.918-1.242) | 0.867 s (0.789-0.905) | 1.22x |
| RTAS scan plus Delta file write | 6.494 s (6.392-6.897) | 1.930 s (1.798-2.181) | 3.36x |
| 1,000-row commit/control | 0.799 s (0.650-1.158) | 1.215 s (0.908-1.547) | 0.66x |

The much larger end-to-end RTAS speedup than the query-only speedup demonstrates that data-file
generation moved to the GPU. The tiny control shows the remaining native catalog/transaction
overhead and GPU startup cost; small writes are not expected to benefit.

## Customer-scale validation

The 14 customer notebook bodies were executed with their SQL transformations unchanged. Only the
`samples.tpcds_sf*` source assignment was replaced by a deterministic, relationship-consistent
synthetic Delta source because the SSH environment had no Unity Catalog workspace credentials or
`samples` catalog. The generated source included 5,000,000 `catalog_sales` rows, 1,666,666
`store_sales` rows, and dimensions up to 500,000 customers and 200,000 items. This is not TPC-DS
SF1000 data despite retaining the notebook scale label.

The run exercised 15 write boundaries per pass: 14 final tables plus the
`fact_salesinvoice_loadtemp` boundary. Across fresh and existing-target GPU passes, conversion
auditing recorded 30 `WriteIntoDeltaCommand` and 30 `WriteFilesExec` boundaries as GPU eligible,
with zero corresponding CPU fallbacks and zero CPU Parquet-MR writer markers. Focused captured-plan
tests verify that this eligible shape executes as `GpuDataWritingCommandExec` plus
`GpuWriteFilesExec`. The five notebook `DELETE` statements per pass remained separate CPU Delta
mutations and are not counted as CTAS/RTAS writes.

| Target state | GPU | CPU | Observed CPU/GPU ratio |
| --- | ---: | ---: | ---: |
| Fresh | 84.412 s | 228.672 s | 2.71x |
| Existing/replace | 72.033 s | 217.953 s | 3.03x |

Before the final positional attribute mapping, only 20 of 30 boundaries selected the GPU writer
and the corresponding GPU passes took 115.320 s and 97.210 s. Reaching 30 of 30 reduced those GPU
times by 26.8% and 25.9%, respectively. One previously missed boundary, fresh
`fact_loadshipment`, fell from 10.084 s to 2.925 s.

All 14 final output tables had matching ordered schemas and row counts. Seven had exact
fingerprints. The remaining differences were limited to SQL constructs without a deterministic
bitwise result: `NTILE(100) ORDER BY net_profit` has no tie-breaker, `collect_list` has unspecified
element order, and floating aggregates can vary by reduction order. The final persisted outputs
had zero sorted-list multiset mismatches, zero NTILE bucket-cardinality mismatches, and a maximum
floating-point absolute difference of `4.55e-13`; deterministic columns matched exactly.

The customer timings are two paired observations run in `GPU, CPU, GPU, CPU` order: the first pair
used fresh targets and the second existing targets. They demonstrate a strong large-scale signal
but are not repeated medians, and CPU always following GPU leaves possible order/cache bias. A
release-quality claim should repeat each target state with multiple alternating warm CPU/GPU runs
and report medians and ranges.

This local run is not authoritative Unity Catalog validation. It did not exercise the real
`samples.tpcds_sf1000` catalog, catalog ownership, UC-applied table features, or coordinated-commit
services. Those remain native in the architecture but must be validated in a UC-enabled workspace
before claiming unchanged customer deployment coverage.
