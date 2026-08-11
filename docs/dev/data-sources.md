---
layout: page
title: Working with Spark Data Sources
nav_order: 2
parent: Developer Overview
---

# Working with Spark Data Sources

## Data Source API Versions

Spark has two major versions of its data source APIs, simply known as "v1" and "v2". There is a configuration
property `spark.sql.sources.useV1SourceList` which determines which API version is used when reading from data
sources such as CSV, Orc, and Parquet. The default value for this configuration option (as of Spark 3.4.0)
is `"avro,csv,json,kafka,orc,parquet,text"`, meaning that all of these data sources fall back to v1 by default.

When using Spark SQL (including the DataFrame API), the representation of a read in the physical plan will be
different depending on the API version being used, and in the plugin we therefore have different code paths
for tagging and replacing these operations.

## V1 API

In the v1 API, a read from a file-based data source is represented by a `FileSourceScanExec`, which wraps
a `HadoopFsRelation`.

`HadoopFsRelation` is an important component in Apache Spark. It represents a relation based on data stored in the
Hadoop FileSystem. When we talk about the Hadoop FileSystem in this context, it encompasses various distributed
storage systems that are Hadoop-compatible, such as HDFS (Hadoop Distributed FileSystem), Amazon S3, and others.

`HadoopFsRelation` is not tied to a specific file format. Instead, it relies on implementations of the `FileFormat`
interface to read and write data.

This means that various file formats like CSV, Parquet, and ORC can have their implementations of the `FileFormat`
interface, and `HadoopFsRelation` will be able to work with any of them.

When overriding `FileSourceScanExec` in the plugin, there are a number of different places where tagging code can be
placed, depending on the file format. We start in GpuOverrides with a map entry `GpuOverrides.exec[FileSourceScanExec]`,
and then the hierarchical flow is typically as follows, although it may vary between shim versions:

```
FileSourceScanExecMeta.tagPlanForGpu
  ScanExecShims.tagGpuFileSourceScanExecSupport
    GpuFileSourceScanExec.tagSupport
```

`GpuFileSourceScanExec.tagSupport` will inspect the `FileFormat` and then call into one of the following:

- `GpuReadCSVFileFormat.tagSupport`, which calls `GpuCSVScan.tagSupport`
- `GpuReadOrcFileFormat.tagSupport`, which calls `GpuOrcScan.tagSupport`
- `GpuReadParquetFileFormat.tagSupport`, which calls `GpuParquetScan.tagSupport`

The classes `GpuCSVScan`, `GpuParquetScan`, `GpuOrcScan`, and `GpuJsonScan` are also called
from the v2 API, so this is a good place to put code that is not specific to either API
version. These scan classes also call into `FileFormatChecks.tag`.

## V2 API

When using the v2 API, the physical plan will contain a `BatchScanExec`, which wraps a scan that implements
the `org.apache.spark.sql.connector.read.Scan` trait. The scan implementations include `CsvScan`, `ParquetScan`,
and `OrcScan`. These are the same scan implementations used in the v1 API, and the plugin tagging code can be
placed in one of the following methods:

- `GpuCSVScan.tagSupport`
- `GpuOrcScan.tagSupport`
- `GpuParquetScan.tagSupport`

When overriding v2 operators in the plugin, we can override both `BatchScanExec` and the individual scans, such
as `CsvScanExec`.
