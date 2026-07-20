# RAPIDS Accelerator for Apache Spark Iceberg Support

The Iceberg support is organized into multiple Maven projects, one per Iceberg minor
version that is supported. This allows each submodule to build against the Iceberg minor
version it supports.

# Iceberg Submodules

The following table shows the mapping of Iceberg versions to their supported Spark version
and the directory that contains the corresponding support code.

| Iceberg Version | Spark Version              | Directory         |
|-----------------|----------------------------|-------------------|
| 1.6.x           | Spark 3.5.0-3.5.3          | `iceberg-1-6-x`  |
| 1.9.x           | Spark 3.5.4-3.5.8          | `iceberg-1-9-x`  |
| 1.10.x          | Spark 3.5.4-3.5.8, 4.0.x  | `iceberg-1-10-x` |
| 1.11.x          | Spark 4.0.2+, 4.1.x        | `iceberg-1-11-x` |

Iceberg GPU acceleration is currently supported on Spark 3.5.x, 4.0.x, and 4.1.x.

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

## Code Shared Between Modules

The `common` directory contains code that is shared across some or all of the Iceberg
submodules. It is not built directly as a Maven submodule but simply houses common code
that is picked up by the Iceberg submodules via the Maven build helper plugin.

| Directory                         | Description                              |
|-----------------------------------|------------------------------------------|
| `common/src/main/scala`           | Scala code shared across all versions    |
| `common/src/main/java`            | Java code shared across all versions     |
| `common/src/main/spark35x/java`   | Java code for Spark 3.5.x only           |
