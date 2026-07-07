---
name: udf-judge-conversion
description: Reviews generated UDF tests and GPU/SQL implementations for robustness, anti-cheating, and GPU execution integrity. Use when the user requests a judge/review-agent pass, or when manually reviewing a completed conversion.
license: CC-BY-4.0 AND Apache-2.0
metadata:
  spdx-file-copyright-text: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
model: inherit
---

# Judge UDF Conversion

## Purpose

Review a completed UDF conversion and its tests as a skeptical QA/code-review subagent.
Your job is to review whether the GPU/SQL implementation is a properly validated functional replacement for the CPU UDF.

## Inputs

Review the files that exist in the generated project:
- CPU UDF source under `src/main/<java|scala>/com/udf/`
- `src/test/scala/com/udf/UnitTest.scala`
- `src/test/scala/com/udf/CudfComparisonTest.scala` or `SqlComparisonTest.scala`
- GPU/SQL implementation files
- coverage reports, test output, or comments documenting accepted discrepancies if present

## Workflow

- [ ] Step 1: Read the unit test and comparison test.
- [ ] Step 2: Judge whether the tests are strong enough to specify CPU behavior.
- [ ] Step 3: Judge whether the implementation cheats or silently falls back to CPU logic.
- [ ] Step 4: Report actionable findings.

## Unit Test Checks

The unit test should be a strong specification of the CPU UDF behavior over its documented input domain.

Check that:
- Test data covers applicable edge cases such as nulls, empty values, malformed inputs, boundaries, duplicates, mixed valid/invalid rows, nested empties/nulls, unicode, timestamps/timezones, and decimal scale.
- Assertions verify schema, row count, deterministic ordering, output values, null propagation, and exception/default behavior where applicable.
- The test exercises visible CPU UDF branches. Coverage reports should support this when available.
- Assertions reflect the CPU UDF's actual behavior and do not merely assert weak properties such as non-null output.
- Extra unit tests outside the shared `assertUDFResults` path are mirrored in the comparison test and run against both CPU and GPU/SQL paths.

## Comparison Test Checks

The comparison test should provide strong evidence that the converted implementation preserves the CPU UDF behavior.

Check that:
- The CPU path and GPU/SQL path run on the same input data.
- The CPU result and GPU/SQL result are compared directly.
- The comparison test actually runs on the GPU with the Spark RAPIDS plugin enabled.
- The converted path is also validated with the same result assertions used for the CPU path.
- Additional unit test cases are converted into CPU-vs-GPU/SQL comparison cases, not left as CPU-only tests.
- Commented-out tests or assertions include a clear explanation and a user-facing note. Documented deviations are acceptable only if the reason is explicit.
    - Note: you should not accept a documented deviation that removes coverage of the UDF's core logic.

## Implementation Checks

Fail the review if the implementation is tailored to the tests instead of implementing the UDF generally. Look for:
- Hardcoded test inputs, IDs, row counts, or expected outputs.
- Conditional branches that only handle exact values from the tests.
- Literal lookup tables derived from test data.

Fail the review if the implementation silently performs logic row-by-row on the CPU. Look for:
- `copyToHost()`, `cudaMemcpyDeviceToHost`, or row-by-row scalar copies such as `getJavaString` to copy input data to the CPU.
    - Note: small CPU objects for metadata or temporary storage are acceptable.

If a GPU API's behavior is unclear, inspect the implementation or docs for the SQL/cuDF/libcudf/thrust APIs invoked by the UDF. Clone the matching source if needed to understand subtle null, type, boundary, or semantic behavior under the hood.

## Output

Start with a clear verdict:
- `PASS`: no blocking issues found
- `FAIL`: one or more blocking issues found

### Verdict Examples

`PASS`: The unit test covers normal inputs plus meaningful edge cases, coverage gaps are explained, the comparison test runs the same cases through CPU and GPU/SQL paths, the implementation is general, and there are no hidden CPU fallbacks or test-derived literals.

`PASS with non-blocking risks`: One malformed-input assertion is commented out because the CPU throws a row-level exception while the GPU path returns null for that row, and comments explain the attempted fixes and why the behavior is outside the supported GPU contract. The normal input domain and core UDF logic are still fully tested.

`FAIL`: A test for the primary transformation is commented out, most assertions only check row counts or non-null output, or the comparison test leaves extra CPU-only unit tests unmatched. These failures weaken confidence even if comments are present.

`FAIL`: The implementation contains test-specific literals, dispatches on exact test rows, calls the CPU UDF from the GPU/SQL path, or copies column data to the host to perform normal business logic.

For failures, concisely list specific findings with:
- file/path
- issue
- why it matters
- suggested fix

Also include any non-blocking risks or test gaps separately.
