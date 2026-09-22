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
The integration-test matrix is maintained in
[`scripts/iceberg-versions.json`](../scripts/iceberg-versions.json) and read by
[`scripts/get_iceberg_versions.py`](../scripts/get_iceberg_versions.py).

Each matrix entry describes one Apache Iceberg runtime version tested by cudf-spark. For that
Iceberg release, `upstream_minimums` is copied from the Spark versions in Apache Iceberg's
`gradle/libs.versions.toml`. Each key is a Spark major/minor family, and its value is the patch
release that Iceberg builds and tests against. As specified in
[issue #15875](https://github.com/NVIDIA/cudf-spark/issues/15875), cudf-spark treats that patch
as the minimum baseline for this test matrix. This is a cudf-spark test policy; the upstream
dependency pin does not itself declare a minimum compatible Spark patch.

The `spark_versions` list is computed from the `spark*.version` properties in
`scala2.13/pom.xml`.
For each family in `upstream_minimums`, it contains every cudf-spark shim whose patch version is
greater than or equal to the baseline. A shim is marked as supported when cudf-spark
packages the corresponding Iceberg integration module. Shims at or above the baseline that
are not packaged remain in the list with `supported` set to `false` and an explanation in
`reason`. Maven release profiles determine packaging; packaging alone does not select a
combination for this test matrix.

Consequently, the following exclusions are intentional:

| Spark version | Matrix selection | Reason |
|---------------|------------------|--------|
| 3.5.0 | None | Packaged Iceberg 1.6.1 has a 3.5.1 test baseline. |
| 3.5.4 | None | Packaged Iceberg 1.9.2 and 1.10.1 have 3.5.5 and 3.5.6 test baselines. |
| 3.5.5 | Iceberg 1.9.2 | Packaged Iceberg 1.10.1 has a 3.5.6 test baseline. |
| 4.2.0 | None | The release profile uses the Iceberg stub, and the matrix has no 4.2 baseline. |

An empty selection succeeds and reports the test policy in the CLI diagnostic and CI skip
message. Missing matrix entries for shims at or above a baseline are validation errors,
so the callers fail instead of silently skipping those tests. Explicitly requested Iceberg
versions must also satisfy the baseline and packaging policy.

For Spark 3.5.4+, both `iceberg-1-9-x` and `iceberg-1-10-x` modules are compiled into the
build. The correct version-specific implementation is
selected at runtime by probing the `iceberg-spark-runtime` jar on the classpath.
Version-specific code lives in distinct sub-packages (`iceberg19x`, `iceberg110x`,
`iceberg111x`) to avoid class conflicts, and the common `ShimUtils` dispatcher delegates to
the appropriate implementation.

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
