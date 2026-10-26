# Databricks 18.3 Delta Lake implementation

This module contains the DBR 18.3 reader, transaction, catalog, and write implementations.
The default DBR 18.3 release profile continues to use `delta-stub` until build activation.

The provider base, write commands, low-shuffle MERGE command, and Parquet reader
implementations are shared with DBR 17.3. Native staged-write handling remains runtime specific.
Both classic and Edge MERGE commands select low-shuffle MERGE when enabled. Discovery and
rewrite scans retain the established reader-mode mapping and classic GPU MERGE fallbacks
for unsupported scan settings, row-tracking tables, and existing deletion vectors.

Runtime-specific transaction hooks preserve DBR 18.3 CDC, eager-clustering, and column-mapping
removal semantics through native fallback. Explicit clustering columns alone do not require
fallback: their native use selects optional Photon Parquet page indexes, which the GPU writer
does not emit.

Build with Java 17 on a DBR 18.3 node. First generate the Databricks dependency POM using
[build/gen-spark-home-deps.py](../../build/gen-spark-home-deps.py), as shown in
[jenkins/databricks/build.sh](../../jenkins/databricks/build.sh).
The generated POM references the native runtime JARs under `/databricks/jars` as system-scoped
dependencies instead of installing them into the local Maven repository.
Then install the SQL-plugin and Databricks shim dependencies from the repository root:

```bash
mvn -B -f scala2.13/pom.xml -pl sql-plugin,shim-deps/databricks -am \
  -Ddatabricks -Dbuildver=410db183 -DskipTests -Dmaven.test.skip=true \
  -Dmaven.scaladoc.skip install
```

The Delta module is not in the root reactor while the release profile uses `delta-stub`.
Build and install it separately:

```bash
mvn -B -f scala2.13/delta-lake/delta-spark410db183/pom.xml \
  -Ddatabricks -Dbuildver=410db183 -DskipTests -Dmaven.test.skip=true \
  -Dmaven.scaladoc.skip clean install
```

Runtime integration coverage and feature parity are tracked in NVIDIA/cudf-spark#16282.
The DBR 18.3 pre-merge integration-test guard remains unchanged.
