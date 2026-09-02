---
name: udf-convert-to-cuda
description: Assists with converting a non-aggregating Apache Spark UDF to a native CUDA RapidsUDF using JNI and libcudf. This is step 2 of 3 in the UDF conversion workflow (udf-gen-test -> udf-convert-to-cuda -> udf-benchmark). Use this skill when you have a CPU UDF with a unit test and need to convert it to a native CUDA implementation. Prefer udf-convert-to-cudf unless a CUDA implementation is necessary for performance or correctness, or if requested by the user.
license: CC-BY-4.0 AND Apache-2.0
metadata:
  spdx-file-copyright-text: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
model: inherit
---

# Convert UDF to Native CUDA RapidsUDF

## Workflow

- [ ] Step 1: Copy CUDA add-on templates into the udf-gen-test project
- [ ] Step 2: Create the Java RapidsUDF/JNI bridge
- [ ] Step 3: Implement the CUDA/libcudf native function
- [ ] Step 4: Build with the `cuda-native-udf` Maven profile
- [ ] Step 5: Fill in the comparison test and iterate
- [ ] Step 6: Run judge subagent if requested
- [ ] Step 7: Review conversion

**Before making any edits, create a visible TODO checklist for every workflow step in this skill and keep it updated.** Do not produce a final answer until every required checklist item is marked complete.

## Prerequisites

- Project directory from Step 1 (`udf-gen-test`) with a passing unit test
  - If the user provides their own tests, inline it into the project template's test harness.
- Native build tools: CMake 3.30.4+, a CUDA-compatible C++ compiler, `git`, and `unzip`
- Docker is optional, but can be used for a stable native build environment

Derive `<CamelName>` and `<snake_name>` from the UDF class name.

> **Note:** Commands require access to `/tmp` (Spark temp storage) and `/dev` (GPU device). If commands fail due to sandbox restrictions, re-run them unsandboxed.

## Step 1: Copy CUDA Add-On Templates

Copy this skill's CUDA templates into the existing project:
```bash
cp -r templates/cuda/* <project_root>/<CamelName>/
chmod +x <project_root>/<CamelName>/native/scripts/extract-cudf-libs.sh
```

The `udf-gen-test` Maven template already contains an inactive `cuda-native-udf` profile. The native profile is activated only when you build with `-Pcuda-native-udf`.

Read [NATIVE_BUILD_ENV.md](references/NATIVE_BUILD_ENV.md) before changing build configuration.
Read `examples/` for native RapidsUDF examples.

## Step 2: Create the RapidsUDF/JNI Bridge

Use `src/main/java/com/udf/PlaceholderUDFNameNativeRapidsUDF.java` as a starting point:

1. Rename it to `<CamelName>NativeRapidsUDF.java`.
2. Rename the class to `<CamelName>NativeRapidsUDF`.
3. Copy the original CPU UDF interface and row-by-row method onto the class.
4. Implement `evaluateColumnar` to validate column count/types and call the native method.
5. Rename the native method to a descriptive operation name, e.g. `cosineSimilarityNative`.

The project joint-compiles Java, so keep this Java wrapper under `src/main/java/com/udf/` and register it from the Scala test. JNI can be used from Scala, but the Java wrapper keeps native symbol names and examples simpler.
If the Java wrapper's CPU fallback needs to call a Scala object, direct references can fail before `scala-maven-plugin` compiles the Scala classes; use reflection in the row-by-row fallback only, and keep `evaluateColumnar` on the normal JNI path.

Read [JNI_CUDA_GUIDE.md](references/JNI_CUDA_GUIDE.md) for the `evaluateColumnar` contract, type mapping, pointer ownership, `NativeDepsLoader`, and native memory rules.
**Note:** memory allocations must use the active RMM resource; avoid direct usage of ad hoc CUDA or Thrust allocators.

## Step 3: Implement Native CUDA Code

Rename and edit:
- `native/src/main/cpp/src/PlaceholderUDFNameJni.cpp`
- `native/src/main/cpp/src/placeholder_udf_name.cu`
- `native/src/main/cpp/src/placeholder_udf_name.hpp`

Update `native/src/main/cpp/CMakeLists.txt` `SOURCE_FILES` to match the renamed files. If libcudf ABI/version compatibility is unclear, defer to the user.

Read [JNI_CUDA_GUIDE.md](references/JNI_CUDA_GUIDE.md) before writing kernels.

Verify cuDF header names before choosing includes or APIs. After dependency extraction, the active header tree will be cloned under `target/cudf-repo/cpp/include`.

### Critical Requirements

- **NEVER use `copyToHost()` or native methods that copy inputs from GPU to CPU.** This defeats the purpose of GPU acceleration
- **Do NOT hardcode test values.** The RapidsUDF must implement actual business logic for ANY potential input

## Step 4: Build

The native Maven profile uses the RAPIDS dependency already declared in `pom.xml`.

```bash
mvn package -Pcuda-native-udf -DskipTests
```

To use the Docker build environment:
```bash
docker build -t cuda-udf-build .
mkdir -p "$HOME/.m2"
docker run --rm --gpus all \
  --user "$(id -u):$(id -g)" \
  -e HOME=/workspace \
  -v "$PWD":/workspace \
  -v "$HOME/.m2":/workspace/.m2 \
  -w /workspace \
  cuda-udf-build \
  -c "mvn -B -Dmaven.repo.local=/workspace/.m2/repository package -Pcuda-native-udf -DskipTests -Dnative.build.path=/workspace/target/native-build-docker"
```

If the build fails while resolving cuDF headers or RAPIDS CMake, check network access and the generated `cudf.git.branch` / `rapids.cmake.branch` properties. These properties may contain either a branch or a tag.

## Step 5: Build and Test

Fill in the target-specific TODOs in `src/test/scala/com/udf/CudfComparisonTest.scala`:
- Register `<CamelName>NativeRapidsUDF` as the GPU implementation
- Replace placeholder UDF names

Run:
```bash
mvn test -Dsuites=com.udf.CudfComparisonTest -Pcuda-native-udf
```

To run the tests inside the Docker build environment:

```bash
docker run --rm --gpus all \
  --user "$(id -u):$(id -g)" \
  -e HOME=/workspace \
  -v "$PWD":/workspace \
  -v "$HOME/.m2":/workspace/.m2 \
  -v /etc/passwd:/etc/passwd:ro \
  -v /etc/group:/etc/group:ro \
  -w /workspace \
  cuda-udf-build \
  -c "mvn -B -Dmaven.repo.local=/workspace/.m2/repository test -Dsuites=com.udf.CudfComparisonTest -Pcuda-native-udf -Dnative.build.path=/workspace/target/native-build-docker -DskipCudfExtraction=true"
```

If tests fail, iterate on the Java bridge or native implementation.

### Difficult Test Failures

Treat the unit test as the CPU behavior specification. Do not weaken or remove test cases silently.

- Tests that check for CPU errors may not be directly applicable to a columnar implementation: the GPU path typically evaluates a whole column and may produce nulls for invalid rows instead of throwing row-level exceptions. If this causes an unavoidable mismatch, add a clear comment in the test and a `TODO/NOTE` in the implementation explaining the mismatch.
- If a test case does not pass because of inherent CUDA/libcudf/API limitations or low-level GPU/CPU semantic differences, comment out the conflicting assertion/test only after documenting how you tried to make the behavior match and why those attempts failed. Add a note to the user.
- If the behavior is important, common, or part of the documented input domain, **always prefer fixing the implementation** over commenting out the test case. The exception is a performance-vs-correctness tradeoff that the user explicitly approves.

## Step 6: Run Judge Subagent If Requested

If the user explicitly asked for the judge, a judge subagent, or a review agent, treat that as an explicit request for delegation: you **MUST** launch a separate subagent with `model: inherit` and instruct it to use the **udf-judge-conversion** skill. Ask it to review the `UnitTest`, `CudfComparisonTest`, Java bridge, and JNI/CUDA sources.

If the user did not request a judge/review agent, mark this step as skipped and continue to Step 7. If a required judge subagent is blocked by tool policy, stop and tell the user that explicit permission/instruction is needed.

If you run the judge, wait for it to complete and review its report. If the judge finds any issues, 1) fix the issues, 2) re-run the tests, and 3) re-run the judge subagent.

## Step 7: Review Conversion

Review your own work to ensure:
- The test runs on the GPU and directly compares CPU-GPU outputs
- The implementation does not overfit to test cases
- No `copyToHost()` or row-by-row GPU-to-CPU copying is used for computation
- No debug statements (e.g., `TableDebug.get().debug(...)`) remain in final output

## Output

Upon successful completion:
- Native RapidsUDF file at `src/main/java/com/udf/<CamelName>NativeRapidsUDF.java`
- JNI/CUDA sources under `native/src/main/cpp/src/`
- Packaged native library in the generated UDF JAR
- Comparison test passes

These outputs are required for **Step 3: Benchmark**.
