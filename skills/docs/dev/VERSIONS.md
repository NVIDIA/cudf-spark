# Version Update Guide

## Files To Update

### udf-gen-test Maven template

File: `skills/udf-gen-test/templates/scala/pom.xml`

Update these properties together:

- `<scala.binary.version>`
- `<scala.version>`
- `<spark.version>`
- `<rapids4spark.version>`
- `<cuda.version>` if the RAPIDS artifact classifier changes
- `<cudf.git.branch>`
- `<rapids.cmake.branch>`

### Native CUDA build image

File: `skills/udf-convert-to-cuda/templates/cuda/Dockerfile`

Update this default value:

- `CUDA_VERSION` must match the CUDA toolkit version spark-rapids is built against (the same version the native build uses on the host).

**Note:** The pre-merge CI image `skills/ci/Dockerfile.pre-merge` is a mirror of this toolchain, plus python dependencies. Keep its `CUDA_VERSION`, `CMAKE_VERSION`, `TOOLSET_VERSION`, `MAVEN_VERSION`, and `CCACHE_VERSION` in sync with this file.

After changing `skills/ci/Dockerfile.pre-merge`, **rebuild and publish** the CI image so pre-merge jobs can pull the updated environment.

### Native CUDA dependency extraction

File: `skills/udf-convert-to-cuda/templates/cuda/native/scripts/extract-cudf-libs.sh`

Update these default values:

- `SCALA_VERSION`
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
