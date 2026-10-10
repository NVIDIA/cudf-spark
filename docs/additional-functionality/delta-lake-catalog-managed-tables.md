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

- Delta Lake 4.2.0 with the Scala 2.13 artifact;
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

## Current limitations

- Catalog-managed `DELETE`, `UPDATE`, `MERGE`, and dynamic partition overwrite
  fall back to the CPU when the operation is configured to create persistent
  deletion vectors. Deletion-vector reads remain accelerated. Persistent-DV
  mutation support is tracked separately.
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
