# RAPIDS Accelerator for Apache Spark Delta Lake Support

The Delta Lake support is organized into multiple Maven projects, one per Delta Lake minor
version that is supported. This allows each submodule to build against the Delta Lake minor
version it supports.

# Delta Lake Submodules

The following table shows the mapping of Delta Lake versions to their supported Spark version
and directory contains the corresponding support code.

| Delta Lake Version | Spark Version   | Directory          |
|--------------------|-----------------|--------------------|
| 2.0.x              | Spark 3.2.x     | `delta-20x`        |
| 2.1.x              | Spark 3.3.x     | `delta-21x`        |
| 2.2.x              | Spark 3.3.x     | `delta-22x`        |
| 2.3.x              | Spark 3.3.x     | `delta-23x`        |
| 2.4.x              | Spark 3.4.x     | `delta-24x`        |
| 3.3.x              | Spark 3.5.[3-]  | `delta-33x`        |
| 4.0.x              | Spark 4.0.x     | `delta-40x`        |
| 4.1.x              | Spark 4.1.x     | `delta-41x`        |
| Databricks 13.3    | Databricks 13.3 | `delta-spark341db` |
| Databricks 14.3    | Databricks 14.3 | `delta-spark350db143` |


Delta Lake is not supported on all Spark versions, and for Spark versions where it is not
supported the `delta-stub` project is used.

## Code Shared Between Modules

The `common` directory contains code that is shared across some or all of the Delta Lake
submodules depending on the particular subdirectory within `common`. It is not built directly
as a Maven submodule but simply houses common code that is picked up by the Delta Lake
submodules via the Maven build helper plugin.

The following table details how the common subdirectories map to Delta Lake versions.

| Directory                    | Applicable to Delta Lake Versions          |
|------------------------------|--------------------------------------------|
| `common/src/main/scala`      | All supported                              |
| `common/src/main/databricks` | Delta Lake on all Databricks platforms     |
| `common/src/main/delta-io`   | Delta Lake on all non-Databricks platforms |
| `delta-lake/src/main/delta*` | Directories supporting various version ranges |
