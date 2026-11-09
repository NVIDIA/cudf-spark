# RAPIDS Accelerator for Apache Spark Iceberg Support

The Iceberg support is organized into multiple Maven projects, one per Iceberg minor
version that is supported. This allows each submodule to build against the Iceberg minor
version it supports.

# Iceberg Submodules

The following table shows which Iceberg integration modules are packaged for each Spark
version and the directory that contains the corresponding support code.

| Iceberg Version | Spark Version              | Directory         |
|-----------------|----------------------------|-------------------|
| 1.6.x           | Spark 3.5.0-3.5.3          | `iceberg-1-6-x`  |
| 1.9.x           | Spark 3.5.4-3.5.9          | `iceberg-1-9-x`  |
| 1.10.x          | Spark 3.5.4-3.5.9, 4.0.x  | `iceberg-1-10-x` |
| 1.11.x          | Spark 4.0.2+, 4.1.x        | `iceberg-1-11-x` |

Iceberg GPU acceleration is currently supported on Spark 3.5.x, 4.0.x, and 4.1.x.
The integration-test source of truth is maintained in
[`scripts/iceberg-versions.json`](../scripts/iceberg-versions.json).

Iceberg tables configured with `encryption.key-id` fall back to CPU for scans and writes,
including when experimental v3 support is enabled. Encryption inspection failures also fall back
to CPU. Iceberg's CPU implementation remains responsible for encryption and key management.

For Spark 3.5.4+, both `iceberg-1-9-x` and `iceberg-1-10-x` modules are compiled into the
build. The correct version-specific implementation is selected at runtime by probing the
`iceberg-spark-runtime` jar on the classpath. Version-specific code lives in distinct
sub-packages (`iceberg19x`, `iceberg110x`, `iceberg111x`) to avoid class conflicts, and the
common `ShimUtils` dispatcher delegates to the appropriate implementation.

For Spark 4.0.0-4.0.1, only `iceberg-1-10-x` is compiled during the build. For Spark
4.0.2+, both `iceberg-1-10-x` and `iceberg-1-11-x` are compiled, and the correct
implementation is selected at runtime.

For Spark 4.1.x, only `iceberg-1-11-x` is compiled during the build. Apache Iceberg
publishes the `iceberg-spark-runtime-4.1` artifact starting at version 1.11.0, so earlier
Iceberg releases cannot be used with Spark 4.1.

## Variant Reads

Iceberg Variant reads require a supported Spark/Iceberg runtime,
`spark.rapids.sql.format.iceberg.v3.enabled=true`,
`spark.sql.variant.allowReadingShredded=false` and
`spark.sql.variant.pushVariantIntoScan=false`. Allowing shredded reads keeps Variant
scans and their consumers on CPU. GPU Variant writes remain unsupported.

The GPU execution reader reuses the Raw Parquet physical-layout predicate to validate
every projected Variant leaf: exactly two `REQUIRED BINARY` children named `value` and
`metadata`. Checks preserve complete Variant groups and field-ID/name-mapping resolution
through structs, lists and maps. They use the schema already loaded by the execution
reader, with no additional file opens and no planning-time data-file inspection.

Shredded or incompatible input throws `UnsupportedOperationException` with
`GPU Iceberg reader does not support shredded or incompatible Variant input` and
instructions to set `spark.rapids.sql.format.iceberg.read.enabled=false` and rerun with
the CPU Iceberg reader. Spark may wrap this in a task/stage failure. No automatic CPU
retry occurs after GPU execution starts. Raw reads, extraction, imported files and
mixed snapshots use the same validation. Queries that do not project Variant columns
remain eligible for GPU scans.

## Code Shared Between Modules

The `common` directory contains code that is shared across some or all of the Iceberg
submodules. It is not built directly as a Maven submodule but simply houses common code
that is picked up by the Iceberg submodules via the Maven build helper plugin.

| Directory                         | Description                              |
|-----------------------------------|------------------------------------------|
| `common/src/main/scala`           | Scala code shared across all versions    |
| `common/src/main/java`            | Java code shared across all versions     |
| `common/src/main/spark35x/java`   | Java code for Spark 3.5.x only           |
