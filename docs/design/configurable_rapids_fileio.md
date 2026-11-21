---
layout: page
title: Configurable RapidsFileIO
parent: Developer Overview
---

# RFC for configurable RapidsFileIO

**Status** Proposed for review. [Feature request #17025](https://github.com/NVIDIA/cudf-spark/issues/17025)

An application may read a Parquet table from HDFS and write its results to S3 in the same Spark
session. The application should be able to supply its S3 client while keeping the job's Hadoop
configuration and credentials for HDFS. A session can select one implementation for all files or
use the built-in `ResolvingRapidsFileIO` to select an implementation for each file it opens.

## Configuring one implementation

```properties
spark.sql.rapids.fileio.impl=com.example.StorageFileIO
spark.sql.rapids.fileio.key1=value1
spark.sql.rapids.fileio.key2=value2
```

Supply the implementation JAR to the driver and executors, for example with `spark-submit --jars`.
The class implements `com.nvidia.spark.rapids.jni.fileio.RapidsFileIO` and exposes this public
constructor in Java or its JVM equivalent.

```java
public StorageFileIO(java.util.Map<String, String> options)
```

In this example the map contains exactly `key1=value1` and `key2=value2`. RAPIDS removes the
namespace and consumes the exact key `impl` to select the class. The map is read-only. Key spelling,
dots inside option names, empty strings, and the original string values are preserved. The
implementation owns option parsing and can use these options to load additional settings itself.
Omitting the top-level `impl` selects the existing `HadoopFileIO` with the job's Hadoop configuration.

## Configuring multiple stores

```properties
spark.sql.rapids.fileio.impl=ResolvingRapidsFileIO
spark.sql.rapids.fileio.s3.impl=com.example.S3FileIO
spark.sql.rapids.fileio.s3.key1=value1
spark.sql.rapids.fileio.s3.key2=value2
```

The fully qualified built-in name is `com.nvidia.spark.rapids.fileio.ResolvingRapidsFileIO`.
The resolver receives `s3.impl`, `s3.key1`, and `s3.key2`. It interprets the scheme namespace when
creating an input or output file. With the settings above, one session routes files as follows.

| Location | Implementation | Constructor options or Hadoop settings |
| --- | --- | --- |
| `s3://analytics/result` | `com.example.S3FileIO` | `key1=value1`, `key2=value2` |
| `hdfs://nameservice/events` | `HadoopFileIO` | The job's Hadoop configuration |
| `/warehouse/events` | Implementation for `fs.defaultFS` | The options for that scheme, or the Hadoop fallback |

Use a lowercase URI scheme in the configuration. URI scheme matching is case-insensitive and
exact. To route both `s3://` and `s3a://` to the same implementation, configure an entry for
each. Each delegate receives only its own options. An invalid configured class or a failed
constructor raises an error identifying the selecting key and class. Hadoop takes over only
when the scheme has no configured implementation.

## Implementation plan

1. Capture the settings in `FileIOConfig` when the driver plans a scan or prepares a write.
   Read the current Spark session's settings and check the top-level class and constructor
   signature. Later session changes apply to newly planned operations.
2. Construct the FileIO lazily in each executor-side reader or writer. The built-in implementation
   uses the reader's broadcast Hadoop configuration or the writer's prepared job configuration.
   Custom constructors receive their option map. Only the settings travel from the driver.
3. Resolve each URI in `newInputFile` and `newOutputFile`. Create each configured delegate
   on first use and share it across concurrent opens for that scheme within the resolver.
   Unregistered schemes share one Hadoop fallback.

The configuration covers RAPIDS Parquet and ORC data reads and writes through the shared GPU
file-format writer. OSS Delta on-disk deletion vectors use the scan's FileIO. Spark still owns
path discovery, schema inference, and output commit operations, so its filesystem and committer
must recognize the job's locations. Iceberg scans continue to use the catalog-provided FileIO
and its credentials.

Parquet dictionary and Bloom filter pruning can also read file data. For a configured custom
FileIO or resolver, these reads use Parquet's input-file API. This path can read the footer a
second time on filtered scans. The built-in Hadoop reader can reuse the already parsed footer.

## Verification

Exercise direct construction and per-scheme dispatch with different values for the same key,
including a scheme handled by Hadoop. Verify that a malformed explicit route fails when opened.
For GPU Parquet and ORC, write through a recording implementation, compare the GPU and CPU reads,
and check that both per-file and multi-file readers opened the data through that implementation.
Exercise filtered scans against a Hadoop filesystem that permits discovery and rejects data opens.
Load a separate Java implementation with `--jars` and read two URI schemes in one Spark scan.
