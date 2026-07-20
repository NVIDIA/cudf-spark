---
layout: page
title: Iceberg session-level scan options
parent: Additional Functionality
nav_order: 11
---

# Iceberg session-level scan options

Iceberg's split-planning knobs (`read.split.target-size`,
`read.split.planning-lookback`, `read.split.open-file-cost`) are normally tuned
either by editing the table properties at write time or by passing
`.option(...)` at every DataFrame read site. Both are awkward when an admin
wants to tune scan behavior across an existing Spark session — for example
before a benchmark run, or to try different split sizes against existing tables
without modifying them.

The RAPIDS Accelerator ships a thin Iceberg session-catalog wrapper that lets
these options be set via session conf at three scopes — per-table, per-catalog,
and global. The wrapper is opt-in and a pure pass-through for any table whose
conf is not set at any scope, so enabling it does not change behavior for
tables you do not explicitly tune.

## When to use it

Use this when you want to:

- Override Iceberg scan options for a specific table without modifying the
  table itself or recompiling the application; or
- A/B test split-size / lookback tunings across runs by toggling a session
  conf instead of rewriting tables.

If you are happy with the Iceberg defaults, or you set scan options at the
DataFrame level via `.option(...)`, you do not need this feature.

## Enabling

Replace Iceberg's session catalog with the RAPIDS drop-in. For the default
catalog (`spark_catalog`):

```
--conf spark.sql.catalog.spark_catalog=com.nvidia.spark.rapids.iceberg.spark.RapidsSparkSessionCatalog
--conf spark.sql.catalog.spark_catalog.type=hadoop
--conf spark.sql.catalog.spark_catalog.warehouse=s3://my-bucket/warehouse/
--conf spark.sql.catalog.spark_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO
```

The `io-impl=org.apache.iceberg.aws.s3.S3FileIO` line is required when the
warehouse lives on S3 — without it, Iceberg falls back to the Hadoop
`FileSystem` API and you have to deal with `s3a://` URI rewrites yourself.
Drop the line for non-S3 warehouses.

For a non-default Iceberg catalog (e.g. one you have configured under
`spark.sql.catalog.<name>`), replace the catalog class with
`com.nvidia.spark.rapids.iceberg.spark.RapidsSparkCatalog` instead:

```
--conf spark.sql.catalog.my_catalog=com.nvidia.spark.rapids.iceberg.spark.RapidsSparkCatalog
--conf spark.sql.catalog.my_catalog.type=hadoop
--conf spark.sql.catalog.my_catalog.warehouse=s3://my-bucket/warehouse/
--conf spark.sql.catalog.my_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO
```

## Setting options

Overrides use the prefix `spark.rapids.iceberg.` followed by a scope marker
(`table-setting` / `catalog-setting` / `global-setting`):

| Scope     | Key shape                                                                                 | Applies to                                  |
| --------- | ----------------------------------------------------------------------------------------- | ------------------------------------------- |
| table     | `spark.rapids.iceberg.table-setting.<catalog>.<namespace>.<table>.<suffix>`               | one specific table                          |
| catalog   | `spark.rapids.iceberg.catalog-setting.<catalog>.<suffix>`                                 | every table under `<catalog>`               |
| global    | `spark.rapids.iceberg.global-setting.<suffix>`                                            | every table the wrapper handles             |

Three suffixes are recognized at every scope:

| Session-conf suffix             | Iceberg read option                    | Iceberg `TableProperties` key   | Type        |
| ------------------------------- | -------------------------------------- | ------------------------------- | ----------- |
| `read-split-target-size`        | `SparkReadOptions.SPLIT_SIZE`          | `read.split.target-size`        | bytes (long)|
| `read-split-planning-lookback`  | `SparkReadOptions.LOOKBACK`            | `read.split.planning-lookback`  | int         |
| `read-split-open-file-cost`     | `SparkReadOptions.FILE_OPEN_COST`      | `read.split.open-file-cost`     | bytes (long)|

The suffix names mirror the Iceberg `TableProperties` keys (the token after
`read.split.`) with `-` instead of `.`, so the dot-separated session-conf
path remains unambiguous.

`<catalog>` is the Spark catalog name, `<namespace>` is the Iceberg namespace
(`default` for Hadoop catalogs that have not been organized into namespaces),
and `<table>` is the Iceberg table name.

### Example

Set a global default, override it at the catalog scope for one catalog, and
override it again for one specific hot table:

```
# Global default for every table the wrapper handles.
--conf spark.rapids.iceberg.global-setting.read-split-target-size=536870912

# Catalog-scoped override for all tables in spark_catalog.
--conf spark.rapids.iceberg.catalog-setting.spark_catalog.read-split-target-size=1073741824

# Per-table override for one table in spark_catalog.default.
--conf spark.rapids.iceberg.table-setting.spark_catalog.default.store_sales.read-split-target-size=2147483648
--conf spark.rapids.iceberg.table-setting.spark_catalog.default.store_sales.read-split-planning-lookback=1000
```

## Precedence

For a given table, each suffix is resolved by walking the priority list below
and picking the first value that is set. The four priorities are, from highest
to lowest:

1. **Session table** — per-table session conf
   (`spark.rapids.iceberg.table-setting.<catalog>.<namespace>.<table>.<suffix>`)
2. **Catalog** — catalog-scoped session conf
   (`spark.rapids.iceberg.catalog-setting.<catalog>.<suffix>`)
3. **Global** — global session conf
   (`spark.rapids.iceberg.global-setting.<suffix>`)
4. **Table itself** — Iceberg `TBLPROPERTIES`
   (e.g. `read.split.target-size`, set via `ALTER TABLE … SET TBLPROPERTIES`)

If none of the four is set, Iceberg's built-in default applies. An explicit
DataFrame `.option(...)` call wins over all four priorities:

```scala
spark.read.format("iceberg")
  .option(SparkReadOptions.SPLIT_SIZE, 268435456L)  // wins; 256 MiB
  .load("default.store_sales")
```

Tables for which no `spark.rapids.iceberg.<…>.*` conf is set at any scope and
no matching `TBLPROPERTIES` is configured behave exactly as if the RAPIDS
catalog wrapper were not in use.

## Caveats

- Suffixes other than the three listed above are rejected with an
  `IllegalArgumentException` on the first scan of the matching table.
  Mistyping a key (e.g. `read.split.target-size` with dots, or `target-size`
  without the `read-split-` prefix) is therefore caught loudly rather than
  being a silent no-op.
- A table-scoped key joins catalog, namespace, and table with `.`, so a `.`
  inside any of those identifiers makes the table-scoped prefix ambiguous
  (`catalog=hadoop.prod` + `namespace=[ns]` + `table=tbl` produces the same
  prefix as `catalog=hadoop` + `namespace=[prod, ns]` + `table=tbl`). Tables
  with such identifiers stay as a pure pass-through as long as no
  table-scoped `spark.rapids.iceberg.table-setting.*` conf is set for them.
  Catalog-scoped and global-scoped confs and `TBLPROPERTIES` still apply,
  since they don't span multiple identifier components. If a table-scoped
  conf is set against such an identifier, the wrapper throws an
  `IllegalArgumentException` from `RapidsSparkTable` rather than silently
  picking the wrong override; in that case, either rename the identifier,
  switch to a catalog- or global-scoped conf, or set the Iceberg
  `read.split.*` table property directly.
- The wrapper applies to scans only. Writes go through the underlying Iceberg
  `SparkTable.newWriteBuilder` unchanged.
- The session-conf lookup happens once per `newScanBuilder(options)` call (a
  single `SparkSession.conf` read per option key) and is cheap; setting many
  per-table overrides has no measurable cost on the scan path.
