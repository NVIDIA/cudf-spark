---
layout: page
title: Delta Lake catalog-managed tables
parent: Additional Functionality
nav_order: 12
---

# Delta Lake catalog-managed tables

The NVIDIA cuDF plugin for Apache Spark supports GPU reads and writes for catalog-managed
(catalog-owned) Delta Lake 4.2 tables. Catalog-managed tables differ from
path-based and session-catalog Delta tables because the catalog allocates the
table location and identity, vends storage credentials, and participates in
the staged create or replace operation. The plugin retains that catalog
context while accelerating the data operation.

## Supported environment

This support is limited to the following open-source combination:

- Delta Lake 4.2.0 (or 4.3.0, see below) with the Scala 2.13 artifact;
- Apache Spark 4.0.1 or 4.1.1; and
- the Unity Catalog 0.6.0 `UCSingleCatalog` staging protocol.

The plugin validates the catalog's expected staging shape before replacing
the Delta delegate. If the catalog is incompatible or uses an unsupported
server-side replacement plan, the operation falls back before a GPU Delta
transaction begins. Fully qualified catalog, namespace, and table identifiers
are supported.

## Accelerated operations

The supported catalog-managed paths include:

- reads, including deletion-vector reads, time travel, and Change Data Feed;
- `CREATE TABLE` and CTAS, plus data-only `REPLACE TABLE`, RTAS, and
  create-or-replace paths;
- V1 and V2 append, insert, overwrite, `replaceWhere`, and dynamic partition
  overwrite;
- partitioned and clustered table writes and qualified optimized writes; and
- copy-on-write `DELETE`, `UPDATE`, and `MERGE`.

Create and replace operations keep the catalog-provided location and
credentials. A data-only replacement also keeps the catalog table identity and
the existing Delta metadata ID. Catalog-visible schema and properties are
updated only after the Delta commit succeeds.

## Delta Lake 4.3 Unity Catalog REST catalogs

Delta Lake 4.3.0 talks to Unity Catalog through the Delta REST API. The plugin leaves that
control plane to the initialized CPU `DeltaCatalog` and its REST client: staging, table and
property preparation, validation, commit, and abort. Only the query and file writes run on the
GPU, so the catalog-assigned table ID and location, storage credentials, server-advertised
features, intent-based property and domain-metadata updates, and server-side commit rejections
behave as they do on the CPU. A rejected commit leaves the existing table unchanged.

On Spark 4.0.1 and 4.1.1 the following run on the GPU when Delta routes them through its REST
client (a managed Unity Catalog Delta table without `LOCATION`, `EXTERNAL`, or a path
identifier): CTAS, RTAS, create-or-replace, appends, overwrites, dynamic partition overwrite,
and copy-on-write `DELETE` and `MERGE`, with the optimized-write and row-tracking exceptions
below. Schema-only `CREATE` and `REPLACE`, external
tables, path-based tables, non-Delta tables, and Unity Catalog without the Delta REST API stay on
the CPU.

A write to a partitioned table runs on the GPU only when optimized write is enabled, through
`spark.databricks.delta.optimizeWrite.enabled` or the `delta.autoOptimize.optimizeWrite` table
property. This covers partitioned CTAS and RTAS, appends and overwrites of an existing partitioned
table, and dynamic partition overwrite. Without optimized write each task writes a file for every
partition it holds, and the GPU Parquet writer's per-file cost made such writes slower than the
CPU in cluster tests (about half as fast for a 64-partition CTAS, and 1.9x slower for a partitioned
append). They stay on the CPU, and the plan explains why. Optimized write consolidates files per
partition and removes that cost. Enabling it only through DataFrameWriter options is not detected,
so those writes also stay on the CPU.

## Current limitations

- Catalog-managed `DELETE`, `UPDATE`, `MERGE`, and dynamic partition overwrite
  fall back to the CPU when the operation is configured to create persistent
  deletion vectors. Deletion-vector reads remain accelerated. Persistent-DV
  mutation support is tracked separately.
- Unity Catalog managed tables have Delta row tracking, and the GPU Parquet scan does not support the
  hidden metadata columns it needs. The file scan in the rewrite step of `DELETE` and `MERGE`
  therefore runs on the CPU and its output is copied to the GPU. These operations still run on the
  GPU, but the scan gains nothing. For `UPDATE` that copy made the GPU rewrite slower than the CPU
  rewrite in cluster tests, so `UPDATE` on a catalog-managed table with row tracking stays on the
  CPU, and the plan explains why.
- Delta 4.2 rejects catalog-managed `OPTIMIZE` and `REORG TABLE` with
  `DELTA_UNSUPPORTED_CATALOG_MANAGED_TABLE_OPERATION` on both CPU and GPU.
- Metadata-changing replacements follow the Delta CPU behavior and are rejected
  without committing a partial replacement.
- Server-side planned table replacement is not accelerated.
- The validated Unity Catalog 0.6.0 path predates an active coordinated-commit
  implementation. The plugin preserves catalog-owned and coordinated-commit
  properties when they are present, but recovery against a newer catalog
  coordinator is outside this compatibility scope.

No additional RAPIDS configuration is required beyond enabling the existing
Delta Lake read and write support. See the
[integration-test documentation](https://github.com/NVIDIA/cudf-spark/blob/main/integration_tests/README.md#enabling-unity-catalog-catalog-managed-table-tests)
for the pinned local Unity Catalog fixture and its CI entry points.
