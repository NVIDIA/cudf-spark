---
name: udf-convert-to-cudf
description: Assists with converting an Apache Spark UDF to a GPU-accelerated RapidsUDF using cuDF Java APIs. This is step 2 of 3 in the UDF conversion workflow (udf-gen-test -> udf-convert-to-cudf -> udf-benchmark). Use this skill when you have a CPU UDF with a unit test and need to convert it to a RapidsUDF.
license: CC-BY-4.0 AND Apache-2.0
metadata:
  spdx-file-copyright-text: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
model: inherit
---

# Convert UDF to cuDF RapidsUDF

## Workflow

- [ ] Step 1: Create the RapidsUDF file
- [ ] Step 2: Implement the `evaluateColumnar` method
- [ ] Step 3: Build and test
- [ ] Step 4: Check for memory leaks
- [ ] Step 5: Run judge subagent if requested
- [ ] Step 6: Review conversion

**Before making any edits, create a visible TODO checklist for every workflow step in this skill and keep it updated.** Do not produce a final answer until every required checklist item is marked complete.

## Prerequisites

- Project directory from Step 1 (udf-gen-test) with passing unit test

Derive `<CamelName>` and `<snake_name>` from the UDF class name.

> **Note:** Commands require access to `/tmp` (Spark temp storage) and `/dev` (GPU device). If commands fail due to sandbox restrictions, re-run them unsandboxed.

## Step 1: Create the RapidsUDF File

Create a copy of the original UDF file in the same source directory (`src/main/<java|scala>/com/udf/`), then modify it:

1. Add imports:
    Java: `import ai.rapids.cudf.*;`, `import com.nvidia.spark.RapidsUDF;`
    Scala: `import ai.rapids.cudf._`, `import com.nvidia.spark.RapidsUDF`, `import Arm.{withResource, closeOnExcept}`
2. Add `implements RapidsUDF` to the class declaration
3. Add the `evaluateColumnar` method stub:
    Java: `public ColumnVector evaluateColumnar(int numRows, ColumnVector... args) { }`
    Scala: `def evaluateColumnar(numRows: Int, args: ColumnVector*): ColumnVector = { }`
4. Rename the class and the file to `<CamelName>RapidsUDF`

## Step 2: Implement the `evaluateColumnar` method

### Background

**Read `references/RAPIDS_UDF.md`** for detailed background on:
- How RapidsUDF and `evaluateColumnar` work
- Input ColumnVector types and output type mapping
- Debugging techniques and GPU memory management

**Read `examples/` for example RapidsUDF implementations for the target language.**

### Implementation

1. Clone https://github.com/rapidsai/cudf (branch matching spark-rapids version) to `~/.cache/aether_agent/` if not already present. Explore `java/src/<main|test>/java/ai/rapids/cudf` for relevant methods and usage patterns.
2. Implement the `evaluateColumnar` method using cuDF APIs.

### Critical Requirements

- **NEVER use `copyToHost()` or methods that copy data GPU→CPU.** This defeats the purpose of GPU acceleration
- **Do NOT hardcode test values.** The RapidsUDF must implement actual business logic for ANY potential input

## Step 3: Build and Test

Fill in the target-specific TODOs in `src/test/scala/com/udf/CudfComparisonTest.scala`:
- Implement `registerRapidsUDF` to register the new RapidsUDF class.
- Replace placeholders with the actual camel/snake UDF name

Then run the test:
```bash
mvn test -Dsuites=com.udf.CudfComparisonTest
```

If the test fails, analyze the error and iterate on the RapidsUDF implementation.

### Difficult Test Failures

Treat the unit test as the CPU behavior specification. Do not weaken or remove test cases silently.

- Tests that check for CPU errors may not be directly applicable to a columnar implementation: the GPU path typically evaluates a whole column and may produce nulls for invalid rows instead of throwing row-level exceptions. If this causes an unavoidable mismatch, add a clear comment in the test and a `TODO/NOTE` in the implementation explaining the mismatch.
- If a test case does not pass because of inherent cuDF/libcudf/API limitations or low-level GPU/CPU semantic differences, comment out the conflicting assertion/test only after documenting how you tried to make the behavior match and why those attempts failed. Add a note to the user.
- If the behavior is important, common, or part of the documented input domain, **always prefer fixing the implementation** over commenting out the test case. The exception is a performance-vs-correctness tradeoff that the user explicitly approves.

## Step 4: Memory Leak Check

Re-run with memory leak detection:
```bash
mvn test -Dsuites=com.udf.CudfComparisonTest -Ddebug.memory.leaks=true > /tmp/memleak.log 2>&1

# Check for leaks
grep "LEAKED" /tmp/memleak.log | head -5
```

If leaks are found, ensure all GPU objects are properly closed.

## Step 5: Run Judge Subagent If Requested

If the user explicitly asked for the judge, a judge subagent, or a review agent, treat that as an explicit request for delegation: you **MUST** launch a separate subagent with `model: inherit` and instruct it to use the **udf-judge-conversion** skill. Ask it to review the `UnitTest`, `CudfComparisonTest`, and RapidsUDF implementation.

If the user did not request a judge/review agent, mark this step as skipped and continue to Step 6. If a required judge subagent is blocked by tool policy, stop and tell the user that explicit permission/instruction is needed.

If you run the judge, wait for it to complete and review its report. If the judge finds any issues, 1) fix the issues, 2) re-run the tests and leak checks, and 3) re-run the judge subagent.

## Step 6: Review Conversion

Review your own work to ensure:
- The test runs on the GPU and directly compares CPU-GPU outputs
- The implementation does not overfit to test cases
- No `copyToHost()` or row-by-row GPU-to-CPU copying is used for computation
- No debug statements (e.g., `TableDebug.get().debug(...)`) remain in final output

## Output

Upon successful completion:
- RapidsUDF file at `src/main/<java|scala>/com/udf/<CamelName>RapidsUDF.<java|scala>`
- Comparison test passes with no memory leaks

These outputs are required for **Step 3: Benchmark**.
