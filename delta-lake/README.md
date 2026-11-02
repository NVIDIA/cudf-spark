# NVIDIA cuDF plugin for Apache Spark Delta Lake Support

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
| 4.1.0              | Spark 4.1.0, 4.1.1 | `delta-41x`     |
| 4.2.0              | Spark 4.0.1, 4.1.1 | `delta-42x`     |
| 4.3.0              | Spark 4.0.1, 4.1.1 | `delta-43x`     |
| Databricks 14.3    | Databricks 14.3 | `delta-spark350db143` |
| Databricks 17.3    | Databricks 17.3 | `delta-spark400db173` |
| Databricks 18.3 | Databricks 18.3 | `delta-spark410db183` |

Delta Lake is not supported on all Spark versions, and for Spark versions where it is not
supported the `delta-stub` project is used.

The Delta Lake 4.1.0 restriction to Spark 4.1.0 and 4.1.1 is caused by an upstream binary
incompatibility in open source Delta Lake with Spark 4.1.2 and later, not by the NVIDIA cuDF
plugin for Apache Spark.

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
| `common/src/main/db-350db143-400db173/scala` | DBR 14.3 and 17.3 |
| `common/src/main/db-350db143-410db183/scala` | DBR 14.3, 17.3 and 18.3 |
| `common/src/main/db-400db173-410db183/{scala,java}` | DBR 17.3 and 18.3 |

These directories provide source roots, not independently built modules. The full
`db-400db173-410db183` source set requires `db-350db143-410db183`; their command and
base-class implementations reference one another.

- `db-400db173-410db183` needs compatible `GpuOptimisticTransaction`,
  `GpuWriteIntoDeltaBase`, and `GpuCreateDeltaTableCommandBase` implementations.
  Its commands use `GpuDeleteCommandBase`, `GpuUpdateCommandBase`,
  `GpuOptimizeExecutorBase`, and `PartitionedBin` from `db-350db143-410db183`.
- `db-350db143-410db183` needs compatible `GpuOptimisticTransactionBase`,
  `GpuCreateDeltaTableCommand`, `GpuDeleteCommand`, `GpuWriteIntoDelta`, and
  `UpdateCommandShims` implementations. DBR 17.3 and 18.3 obtain the latter four
  from `db-400db173-410db183`; DBR 14.3 supplies its own implementations.

DBR 17.3 and 18.3 share the native deletion-vector reader core, legacy Parquet reader,
low-shuffle MERGE command, provider base, write commands, MERGE command metadata shim,
descriptor-based bitmap utilities, and reader eligibility helpers.
Runtime wrappers derive low-shuffle discovery settings and the collision-safe row-index name
from scan options. The shared bitmap lookup prefers row-index providers and keeps descriptor
objects intact, avoiding the string encoding limit for large temporary deletion vectors.

The DBR 18.3 release profile compiles the reader, transaction, catalog, and write layers
and packages the Delta module in the plugin distribution. Databricks integration-test
enablement is tracked separately.
