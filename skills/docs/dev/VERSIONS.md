# Version Update Guide

For published releases, verify that the configured artifact ID and version publish a POM, an unclassified JAR, and a JAR for the selected CUDA classifier in Maven Central. For locally built snapshots, install the artifacts in the local Maven repository before testing.

## Files To Update

### udf-gen-test Maven template

File: `skills/udf-gen-test/templates/scala/pom.xml`

Update these properties together:

- `<scala.binary.version>`
- `<scala.version>`
- `<spark.version>`
- `<rapids4spark.artifactId>`
- `<rapids4spark.version>`
- `<cuda.version>` if the RAPIDS artifact classifier changes
- `<cudf.git.branch>`
- `<rapids.cmake.branch>`

The template defaults to `26.06.0` with `rapids-4-spark_${scala.binary.version}`. When selecting `26.12.0-SNAPSHOT` built from the renamed sources, also set `rapids4spark.artifactId` to `cudf-spark_${scala.binary.version}` in the generated POM. Changing only `rapids4spark.version` does not change the Maven artifact ID.

For example, to test a locally installed Scala 2.12 snapshot:

```bash
mvn test -Dsuites=com.udf.UnitTest \
  -Drapids4spark.version=26.12.0-SNAPSHOT \
  -Drapids4spark.artifactId=cudf-spark_2.12
```

For Scala 2.13, set the matching Scala binary/full versions and use `cudf-spark_2.13`. Native CUDA builds require the same artifact-ID override with `-Pcuda-native-udf` and matching CUDA, cuDF, and rapids-cmake versions. Keep the legacy artifact ID for older releases.

### Native CUDA build image

File: `skills/udf-convert-to-cuda/templates/cuda/Dockerfile`

Update this default value:

- `CUDA_VERSION` must match the CUDA toolkit version spark-rapids is built against (the same version the native build uses on the host).

**Note:** The pre-merge CI image `skills/ci/Dockerfile.pre-merge` is a mirror of this toolchain, plus python dependencies. Keep its CUDA base image tag, `CMAKE_VERSION`, `TOOLSET_VERSION`, `MAVEN_VERSION`, and `CCACHE_VERSION` in sync with this file.

Keep these native build requirements aligned when their versions change:

- The CMake minimum in `skills/udf-convert-to-cuda/templates/cuda/native/src/main/cpp/CMakeLists.txt`, `skills/udf-convert-to-cuda/SKILL.md`, and `skills/udf-convert-to-cuda/references/NATIVE_BUILD_ENV.md`
- The JDK version in the Maven compiler properties, both CUDA Dockerfiles, and `skills/udf-convert-to-cuda/references/NATIVE_BUILD_ENV.md`

### Native CUDA dependency extraction

File: `skills/udf-convert-to-cuda/templates/cuda/native/scripts/extract-cudf-libs.sh`

Update these default values:

- `SCALA_VERSION`
- `RAPIDS4SPARK_ARTIFACT_ID`
- `RAPIDS4SPARK_VERSION`
- `CUDA_VERSION` if the RAPIDS artifact classifier changes
- `CUDF_BRANCH`

### Native CUDA CMake template

File: `skills/udf-convert-to-cuda/templates/cuda/native/src/main/cpp/CMakeLists.txt`

Update these values:

- `RAPIDS_CMAKE_BRANCH`
- `project(RAPIDSUDFJNI VERSION ...)`
- `rapids_cpm_find(cudf ...)`

`RAPIDS_CMAKE_BRANCH` should generally match the RAPIDS/cuDF branch or tag used by the Maven templates and `extract-cudf-libs.sh`. The `rapids_cpm_find(cudf...)` version should use the RAPIDS major/minor CPM version, for example `26.04.00` for `26.04.0`.
