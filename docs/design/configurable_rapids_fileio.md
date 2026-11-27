---
layout: page
title: Configurable RapidsFileIO
parent: Developer Overview
---

# RFC for configurable RapidsFileIO

**Status** Proposed for review. [Feature request #17025](https://github.com/NVIDIA/cudf-spark/issues/17025)

## Background

`RapidsFileIO` is the interface RAPIDS GPU readers and writers use to create input and output
files. Today `HadoopFileIO` backs every non-Iceberg data-file path through this interface.
These paths include Parquet and ORC file-source reads, writes through the shared GPU file-format
writer, and reads of on-disk deletion vectors in open-source Delta Lake. `HadoopFileIO` uses
the job's Hadoop configuration to access multiple filesystems and can select built-in optimized
readers.

Iceberg table reads and writes use `IcebergFileIO`, which wraps the table's own `FileIO`.

## Problem

An application may need to use its own storage backend for some files and the job's configured
Hadoop filesystems for others. Supplying a `RapidsFileIO` for that backend currently requires
changing the plugin.

## Proposed design

Let a Spark SQL session select a `RapidsFileIO` with `spark.sql.rapids.fileio.impl`. It can
choose one class for these reads and writes or select `ResolvingRapidsFileIO` to route each
file by URI scheme. Custom implementations receive their own options as a map of strings.
`HadoopFileIO` is the default and handles unconfigured schemes in the resolver.

## How it works

### Configuring one implementation

```properties
spark.sql.rapids.fileio.impl=com.example.StorageFileIO
spark.sql.rapids.fileio.key1=value1
spark.sql.rapids.fileio.key2=value2
```

Supply the implementation JAR to the driver and executors, for example with `spark-submit --jars`.
The class implements `com.nvidia.spark.rapids.jni.fileio.RapidsFileIO` and exposes a public
constructor with this signature in Java or its JVM equivalent.

```java
public StorageFileIO(java.util.Map<String, String> options)
```

RAPIDS strips `spark.sql.rapids.fileio.` and consumes the exact key `impl` to select the class.
Here the read-only map contains exactly `key1=value1` and `key2=value2`. Key spelling, dots in
option names, empty strings, and original string values are preserved. The implementation owns
option parsing and can load additional settings itself.

### Configuring multiple stores

```properties
spark.sql.rapids.fileio.impl=ResolvingRapidsFileIO
spark.sql.rapids.fileio.s3.impl=com.example.S3FileIO
spark.sql.rapids.fileio.s3.key1=value1
spark.sql.rapids.fileio.s3.key2=value2
```

The resolver receives `s3.impl`, `s3.key1`, and `s3.key2`. A single scan can span stores, so
the resolver looks up `<scheme>.impl` on each `newInputFile` or `newOutputFile` call. It strips
the scheme prefix and consumes the selector before passing options to the selected class.

| File location | Selected implementation | Options or Hadoop settings |
| --- | --- | --- |
| `s3://analytics/result` | `com.example.S3FileIO` | `key1=value1`, `key2=value2` |
| `hdfs://nameservice/events` | `HadoopFileIO` | The job's Hadoop configuration |
| `/warehouse/result` with `fs.defaultFS=s3://analytics` | `com.example.S3FileIO` | `key1=value1`, `key2=value2` |

Paths without a scheme use `fs.defaultFS`. Write scheme keys in lowercase. URI schemes match
case-insensitively and exactly, so `s3` and `s3a` need separate entries. The top-level setting
also accepts the full class name `com.nvidia.spark.rapids.fileio.ResolvingRapidsFileIO`.

### From planning to file access

1. When the driver plans a scan or prepares a write, it captures the session settings in
   `FileIOConfig` and checks the top-level custom class and constructor signature. Later session
   changes apply to newly planned operations.
2. Each executor-side reader or writer creates its FileIO on first use. `HadoopFileIO` uses the
   scan's broadcast Hadoop configuration or the writer's prepared job configuration. Delta scans
   open on-disk deletion vectors through the FileIO selected for the scan.
3. Each resolver creates a delegate on the first open for its scheme and shares it across
   concurrent opens. A bad class or a failed constructor for an explicit route raises an error
   naming the selecting key and class.

## Breaking changes and compatibility

- **Existing jobs and files.** With `spark.sql.rapids.fileio.impl` unset, jobs use
  `HadoopFileIO` with their existing Hadoop settings and credentials. They can upgrade without
  changing configuration or rewriting data. Jobs that already set this previously unused key
  will now load the named class.
- **Extension code.** The `RapidsFileIO` methods keep their signatures. Custom classes need the
  public `Map` constructor to opt into configuration-based loading. Direct callers of plugin
  internals must pass `FileIOConfig` to the `GpuWriteJobDescription` constructor or replace
  `RapidsDeletionVectorStore.createInstance(fileIO)` with `new RapidsDeletionVectorStore(fileIO)`,
  as applicable.
- **Spark filesystem operations.** Spark handles path discovery, schema inference, and output
  commits. Its filesystem and committer must recognize the job's locations even when RAPIDS
  opens the data through a custom client.
- **Iceberg tables.** Iceberg table I/O is outside this proposal. GPU reads and writes wrap
  `Table.io()` in `IcebergFileIO` and use the table's storage settings, including any credentials
  supplied by the catalog. An Iceberg table and a direct Parquet scan can therefore use different
  clients for the same URI scheme in one session.
- **Filtered Parquet scans.** A custom FileIO or resolver uses the selected input file for
  dictionary and Bloom filter pruning. This can read the footer a second time, including when
  the resolver falls back to Hadoop. The default `HadoopFileIO` path can reuse the parsed footer.
