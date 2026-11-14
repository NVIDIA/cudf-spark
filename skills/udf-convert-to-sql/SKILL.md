---
name: udf-convert-to-sql
description: Assists with converting an Apache Spark UDF to a functionally equivalent Spark SQL expression. This is step 2 of 3 in the UDF conversion workflow (udf-gen-test -> udf-convert-to-sql -> udf-benchmark). Use this skill when you have a CPU UDF with a unit test and need to convert it to SQL for GPU acceleration.
license: CC-BY-4.0 AND Apache-2.0
metadata:
  spdx-file-copyright-text: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
model: inherit
---

# Convert UDF to Spark SQL

## Workflow

- [ ] Step 1: Implement the SQL expression
- [ ] Step 2: Fill in the comparison test and iterate
- [ ] Step 3: Run judge subagent if requested
- [ ] Step 4: Review conversion

**Before making any edits, create a visible TODO checklist for every workflow step in this skill and keep it updated.** Do not produce a final answer until every required checklist item is marked complete.

## Prerequisites

- Project directory from Step 1 (`udf-gen-test`) with a passing unit test
  - If the user provides their own tests, inline it into the project template's test harness.

Derive `<CamelName>` and `<snake_name>` from the UDF class name.

> **Note:** Commands require access to `/tmp` (Spark temp storage) and `/dev` (GPU device). If commands fail due to sandbox restrictions, re-run them unsandboxed.

## Step 1: Implement the SQL Expression

Implement the SQL expression in a file at `src/main/resources/<snake_name>.sql`.

**Read `examples/` for example UDF-to-SQL conversions for the target language.**

### Guidelines

- Focus on correctness FIRST, then GPU compatibility — the test will report which operators are not GPU-compatible
- Avoid expensive joins; prefer window functions, CTEs, and built-in array/map functions over explode-and-aggregate patterns

**Do NOT hardcode test sample values or outputs.** The SQL expression must work correctly for ANY potential input.

## Step 2: Fill in test and iterate

Update `src/test/scala/com/udf/SqlComparisonTest.scala`:
- Update the SQL file path to point to your `src/main/resources/<snake_name>.sql` file
- Replace placeholders with the actual camel/snake UDF name

Then run the test:
```bash
mvn test -Dsuites=com.udf.SqlComparisonTest
```

If the test fails, analyze the error and iterate on the SQL expression.

### Difficult Test Failures

Treat the unit test as the CPU behavior specification. Do not weaken or remove test cases silently.

- Tests that check for CPU errors may not be directly applicable to SQL operators: Spark RAPIDS typically evaluates a whole column/batch and may produce nulls for invalid rows instead of throwing one row-level exception. Make an explicit judgment call about the UDF contract. Add a clear comment in the test and a `TODO/NOTE` in the SQL statement explaining the mismatch.
- In rare cases, the Spark RAPIDS Plugin has known discrepancies in certain SQL operators. If a test case does not pass because of these discrepancies, notify the user and comment out the conflicting assertion/test only after documenting how you tried to make the behavior match and why those attempts failed.
- If the behavior is important, common, or part of the documented input domain, **always prefer fixing the SQL expression** over commenting out the test case. The exception is a performance-vs-correctness tradeoff that the user explicitly approves.

## Step 3: Run Judge Subagent If Requested

If the user explicitly asked for the judge, a judge subagent, or a review agent, treat that as an explicit request for delegation: you **MUST** launch a separate subagent with `model: inherit` and instruct it to use the **udf-judge-conversion** skill. Ask it to review the `UnitTest`, `SqlComparisonTest`, and SQL expression.

If the user did not request a judge/review agent, mark this step as skipped and continue to Step 4. If a required judge subagent is blocked by tool policy, stop and tell the user that explicit permission/instruction is needed.

If you run the judge, wait for it to complete and review its report. If the judge finds any issues, 1) fix the issues, 2) re-run the tests, and 3) re-run the judge subagent.

## Step 4: Review Conversion

Review your own work to ensure:
- The test runs on the GPU and directly compares CPU-SQL outputs
- The implementation does not overfit to test cases

## Output

Upon successful completion:
- SQL file at `src/main/resources/<snake_name>.sql`
- Comparison test passes

These outputs are required for **Step 3: Benchmark**.
