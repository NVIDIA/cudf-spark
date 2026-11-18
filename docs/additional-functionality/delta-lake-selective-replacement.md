---
layout: page
title: Delta Lake selective replacement
parent: Additional Functionality
nav_order: 13
---

# Delta Lake selective replacement

The NVIDIA cuDF plugin for Apache Spark accelerates Delta Lake 4.3 selective
replacement with `replaceUsing` and `replaceOn`. These operations atomically delete
matching target rows and insert the source rows. They support partitioned tables,
Change Data Feed (CDF), and targets with persistent deletion vectors (DVs).

## Supported environment

Use open-source Delta Lake 4.3.0 with Scala 2.13 and Apache Spark 4.0.1 or 4.1.1
for path-based and Spark session-catalog tables. This support does not extend to
similarly named Databricks APIs.

Enable the plugin and configure Delta Lake:

```text
spark.plugins=com.nvidia.spark.SQLPlugin
spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension
spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog
```

Delta GPU writes are enabled by default through
`spark.rapids.sql.format.delta.write.enabled=true`. If this setting was disabled,
re-enable it to accelerate writes.

Install compatible Delta dependencies on the driver and executors. See the
[user guide](https://docs.nvidia.com/spark-rapids/user-guide/latest/index.html) for plugin setup.

## Replace rows using matching columns

Selective replacement requires `mode("overwrite")` for both `replaceUsing` and
`replaceOn`. In append mode, Delta ignores these options and appends all source
rows, including duplicates.

Use `replaceUsing` with a comma-separated list of column names. The target must
already exist. For example, replace target rows whose `id` occurs in `updates`,
and insert every row from `updates`:

```python
(updates.write.format("delta")
 .mode("overwrite")
 .option("replaceUsing", "id")
 .save("/data/events"))
```

Multiple columns, such as `"account_id,event_date"`, form a composite match.
Matching uses ordinary equality: a null key does not match another null key.
Target rows without a match remain in the table. Duplicate source keys can match
one target row, but all source rows are inserted; this operation does not
remove duplicates from the source.

## Replace rows using an expression

Use `replaceOn` for an explicit matching expression. Give the source a DataFrame
alias and identify the target with `targetAlias` to avoid ambiguous references:

```python
(updates.alias("source").write.format("delta")
 .mode("overwrite")
 .option("targetAlias", "target")
 .option("replaceOn", "target.id = source.id")
 .save("/data/events"))
```

Use Spark's null-safe equality operator, `<=>`, if null keys should match.
The expression determines which target rows to delete; all source rows are
inserted. With an empty source there are no matching target rows to delete.
Delta's expression resolution, validation, constraints, source materialization,
and retry behavior also apply when GPU execution is enabled.

## Change Data Feed and deletion vectors

CDF records deleted and inserted rows when enabled on the target. Existing DVs
are respected, and matching can create or update persistent DVs when Delta
selects that execution path.

## Write APIs

V1 `save` and `saveAsTable` resolve source columns by name. V1 `insertInto`
resolves columns by position; keep source columns in target order. Delta rejects
`replaceUsing` inserts with misaligned matching columns.

For example, if the existing `events` table has columns `id`, `event_date`, and
`payload` in that order:

```python
(updates.select("id", "event_date", "payload").write.format("delta")
 .mode("overwrite")
 .option("replaceUsing", "id")
 .insertInto("events"))
```

Explicitly use `.format("delta")` with `insertInto` to preserve the replacement
options. With the default Parquet writer format, Spark takes a different insert
path that drops these options; overwrite mode can then replace the entire table,
including non-matching rows, without an error.

For partitioned tables, `insertInto` with selective replacement is rejected when
`spark.sql.sources.partitionOverwriteMode=dynamic`. V1 `save` ignores this session
setting for selective replacement. Passing `partitionOverwriteMode` as a writer
option is rejected for both `replaceOn` and `replaceUsing`.

Use these V1 APIs for selective replacement. V2 `writeTo(...)` and SQL
`INSERT OVERWRITE ... WITH (...)` are not supported entry points for these
options and can reject them rather than perform selective replacement.

## Limitations

- Use one selective replacement option per write. `replaceWhere`, `replaceOn`, and
  `replaceUsing` have distinct semantics and cannot be combined. Delta also
  rejects incompatible options such as `overwriteSchema=true`, `dataChange=false`,
  and `partitionOverwriteMode` with `replaceOn` or `replaceUsing`.
- Delta's `spark.databricks.delta.replaceOn.dataframe.writer.enabled` and
  `spark.databricks.delta.replaceUsing.dataframe.writer.enabled` feature flags
  remain effective. Disabling the corresponding flag causes the same validation
  error as CPU Delta.
- GPU support follows the plugin's supported expressions, types, and operators.
  An unsupported operator inside matching can run on the CPU while supported
  parts remain accelerated. Disabling Delta GPU writes runs the command on the
  CPU. Use `spark.rapids.sql.explain=NOT_ON_GPU` to inspect fallback reasons.
- User subqueries follow upstream Delta's validation and GPU fallback rules;
  enabling selective replacement does not enable arbitrary subqueries in
  `replaceWhere` or ordinary `DELETE` commands.
- With a shuffled (non-broadcast) matching join against a target without DVs,
  upstream Delta 4.3 can fail with `DELTA_FILE_TO_OVERWRITE_NOT_FOUND`, regardless
  of source materialization. This limitation affects CPU and GPU execution; the
  failed replacement leaves the target unchanged. Enable DVs or use a broadcast
  matching join to avoid this path. A source too large to broadcast, or disabled
  broadcast joins, can cause shuffled matching.
- Delta 4.3's `replaceUsing` helper does not resolve a quoted column name containing
  a dot as a single matching column. Use a supported identifier, or use
  `replaceOn` with appropriately quoted references.

Without DVs, matching a small fraction of rows can still rewrite most target
files when those rows are spread across the table; performance depends on the
file layout as well as the number of matching rows.
