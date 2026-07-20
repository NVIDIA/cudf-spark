<!--
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: CC-BY-4.0
-->

# Native CUDA UDF Build Environment

## Dependency Model

The native build uses the RAPIDS JAR already resolved by Maven. The `cuda-native-udf` profile asks Maven to copy `rapids-4-spark_<scala>-<version>-<cuda>.jar` and `rapids-4-spark_<scala>-<version>.jar` into `target/rapids-jar`. The `native/scripts/extract-cudf-libs.sh` script then extracts `libcudf.so*` and `libnvcomp.so*`, clones matching cuDF headers, builds `librapidsudfjni.so`, and packages it in the UDF JAR for `NativeDepsLoader`.

No separate manual JAR download is required. Maven should resolve the RAPIDS dependency declared in `pom.xml`; the native profile reuses the same coordinates and copies the resolved JAR into `target/rapids-jar`.

The profile first tries the CUDA-classified artifact (`-cuda12`) and then the unclassified artifact. If extraction fails, the selected JAR probably does not contain Linux native CUDA libraries or the Maven cache/repository is inconsistent with the generated version properties.

## Required Tools

- CUDA toolkit matching spark-rapids build and a compatible NVIDIA driver
- CMake 3.30.4+
- C++ compiler compatible with the selected CUDA toolkit
- JDK 17
- Maven
- `git`
- `unzip`

## CUDA Toolkit Version

The native build compiles against the prebuilt libcudf in the spark-rapids jar, so the local CUDA toolkit must match the version spark-rapids was built against.

1. Get the CUDA version(s) spark-rapids is built against:

```bash
curl -fsSL https://nvidia.github.io/cudf-spark/docs/download.html \
  | grep -Eo '[^<>]*built against CUDA[^<>]*'
```

2. Check the active toolkit (`nvcc --version`). CMake uses `$CUDACXX`, else `nvcc` on `PATH`, else `$CUDAToolkit_ROOT/bin/nvcc` — the default `PATH` `nvcc` may not be the one you want.

3. If it doesn't match, point the build at a matching toolkit that's already installed; otherwise install one that matches:

```bash
export CUDACXX=/usr/local/cuda-<major.minor>/bin/nvcc
export CUDAToolkit_ROOT=/usr/local/cuda-<major.minor>
export PATH="$CUDAToolkit_ROOT/bin:$PATH"
```

Docker is optional. Use it when local compiler/CMake/CUDA versions drift or when the build needs to be reproducible across machines.

The provided Dockerfile installs JDK 17 and sets it via `/etc/profile.d/java17.sh`. If a modified Dockerfile or alternate entrypoint bypasses the login shell and `mvn` reports Java 8, export `JAVA_HOME=/usr/lib/jvm/java-17-openjdk` and prepend `$JAVA_HOME/bin` to `PATH` explicitly.

Use the full Docker command listed in SKILL.md. It runs as the calling user to avoid root-owned artifacts, mounts the project and Maven cache, and uses a Docker-specific native build path so CMake cache paths do not conflict with host builds.

If a previous root container run already wrote `target/` artifacts, fix ownership or clean them before rerunning as a non-root user.

CMake stores absolute source and build paths in `CMakeCache.txt`. A host-generated `target/native-build` cannot be reused from `/workspace/target/native-build` inside Docker. Use `mvn clean`, remove the stale native build directory, or pass a Docker-specific path such as `-Dnative.build.path=/workspace/target/native-build-docker`.

## Version Alignment

Keep these values aligned:
- Spark version
- Scala binary version
- `rapids4spark.version`
- `cuda.version`
- `cudf.git.branch`
- `rapids.cmake.branch`
- JDK version

The generated template maps RAPIDS `<major>.<minor>.<patch>` to the `v<major>.<minor>.00` cuDF and rapids-cmake tags. If building a snapshot, a custom RAPIDS JAR, or a patch release with known native ABI changes, verify the matching cuDF/RMM/CCCL versions with the user.

## Fast Rebuilds and Verification

After the first successful extraction, use `-DskipCudfExtraction=true` while iterating on Java/JNI/CUDA source:

```bash
mvn package -Pcuda-native-udf -DskipCudfExtraction=true -DskipTests
```

Verify deployable packaging with:

```bash
jar tf target/*.jar | grep librapidsudfjni.so
```

## Build Modes

Default: `USE_PREBUILT_CUDF=ON`.

This extracts `libcudf` from the RAPIDS JAR and builds only the UDF JNI/CUDA library. This is the stable, fast path.

Escape hatch: `-DUSE_PREBUILT_CUDF=OFF`.

This builds cuDF from source through RAPIDS CMake/CPM. It is slow and more sensitive to branch drift; ask the user before using it.
