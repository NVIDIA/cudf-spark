---
layout: page
title: The Local Replay Framework
nav_order: 13
parent: Developer Overview
---

# Local Replay Framework

## Overview

LORE (the local replay framework) is a tool that allows developer to replay the execution of a 
gpu operator in local environment, so that developer could debug and profile the operator for 
performance analysis. In high level it works as follows:

1. Each gpu operator will be assigned a LORE id, which is a unique identifier for the operator. 
   This id is guaranteed to be unique within the same query, and guaranteed to be same when two 
   sql executions have same sql, same configuration, and same data. 
2. In the first run of the query, developer could found the LORE id of the operator they are 
   interested in by checking spark ui, where LORE id usually appears in the arguments of operator.
3. In the second run of the query, developer needs to configure the LORE ids of the operators they 
   are interested in, and LORE will dump the input data of the operator to given path.
4. Developer could copy the dumped data to local environment, and replay the operator in local 
   environment.

## Configuration

By default, LORE id will always be generated for operators, but user could disable this behavior 
by setting `spark.rapids.sql.lore.tag.enabled` to `false`. 

To tell LORE the LORE ids of the operators you are interested in, you need to set 
`spark.rapids.sql.lore.idsToDump`. For example, you could set it to "1[\*], 2[\*], 3[\*]" to tell 
LORE to dump all partitions of input data of operators with id 1, 2, or 3. You can also only dump 
some partition of the operator's input by appending partition numbers to lore ids. For example, 
"1[0 4-6 7], 2[\*]" tell LORE to dump operator with LORE id 1, but only dump partition 0, 4, 5, 
and 7, e.g. the end of the range is exclusive. But for operator with LORE id 2, it will dump all 
partitions. 

You also need to set `spark.rapids.sql.lore.dumpPath` to tell LORE where to dump the data, the 
value of which should point to a directory. All dumped data of a query will live in this 
directory. By default the directory must either not exist (it will be created) or be empty.
If the directory exists and contains files, an `IllegalArgumentException` will be thrown to prevent overwriting existing data unless you explicitly enable non-strict mode (see below).

### Non-strict mode

Set `spark.rapids.sql.lore.nonStrictMode.enabled=true` to make LORE more tolerant when dumping multiple lore ids. When this flag is enabled:

* Any lore id that fails to dump because of unsupported operators or misconfigurations is skipped with a warning while the rest of the query continues.
* Existing data underneath `spark.rapids.sql.lore.dumpPath` is preserved. LORE logs a warning if the path is non-empty but still dumps any remaining lore ids into their own subdirectories.
### Preserve original schema names in Parquet

By default, LORE writes Parquet files using the original Spark schema names (including nested
field names for structures, arrays, and maps, following Spark-compat naming such as `element`,
`key`, and `value` where applicable).

You can disable this behavior if you prefer auto-generated type-based names by setting:

```
spark.rapids.sql.lore.parquet.useOriginalSchemaNames = false
```

When enabled, LORE writes Parquet files with the original Spark schema names (including nested
field names for structures, arrays, and maps, following Spark-compat naming such as `element`,
`key`, and `value` where applicable). Replay functionality still relies on LORE metadata to
reconstruct precise types; the Parquet files are now also directly readable with expected names.


A typical directory hierarchy would look like this:

```console
+ loreId-10/
  - plan.meta
  + input-0/
    - rdd.meta
    + partition-0/
      - partition.meta
      - batch-0.parquet
      - batch-1.parquet
    + partition-1/
      - partition.meta
      - batch-0.parquet
  + input-1/
    - rdd.meta
    + partition-0/
      - partition.meta
      - batch-0.parquet
      - batch-1.parquet
 
+ loreId-15/
  - plan.meta
  + input-0/
    - rdd.meta
    + partition-0/
      - partition.meta
      - batch-0.parquet
```

## How to replay dumped data

Currently we don't provide a tool to replay the dumped data, but it's simple to replay the 
dumped data with our api. Here is an example:

```scala
GpuColumnarToRowExec(
   GpuLore.restoreGpuExec(
      new Path(s"${TEST_FILES_ROOT.getAbsolutePath}/loreId-$loreId"), 
      spark))
        .executeCollect()
        .foreach(println)
```

Above code will replay the operator with LORE id `loreId` in local environment, and print the 
restored data to console.



# Limitations

1. Currently, the LORE id is missed when the RDD of a `DataFrame` is used directly.
2. Not all operators are supported by LORE. For example, shuffle related operator (e.g.
   `GpuShuffleExchangeExec`), leaf operator (e.g. `GpuFileSourceScanExec`) are not supported.
3. **GpuDataWritingCommandExec is not supported for LORE dump on certain Spark versions**.
   The following versions are not supported due to compatibility issues with `GpuWriteFiles`:
   - Spark 3.4.0 (`340`)
   - Spark 3.4.1 (`341`)
   - Spark 3.4.2 (`342`)
   - Spark 3.4.3 (`343`)
   - Spark 3.4.4 (`344`)
   - Spark 3.5.0 (`350`)
   - Spark 3.5.0 Databricks 14.3 (`350db143`)
   - Spark 3.5.1 (`351`)
   - Spark 3.5.2 (`352`)
   - Spark 3.5.3 (`353`)
   - Spark 3.5.4 (`354`)
   - Spark 3.5.5 (`355`)
   - Spark 3.5.6 (`356`)
   - Spark 3.5.7 (`357`)
   - Spark 4.0.0 (`400`)
   - Spark 4.0.0 Databricks 17.3 (`400db173`)
   - Spark 4.0.1 (`401`)

   When attempting to dump a `GpuDataWritingCommandExec` on these versions, LORE will throw an
   `UnsupportedOperationException` with a clear error message indicating the unsupported version.
