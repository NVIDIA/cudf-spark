---
name: udf-gen-test
description: Assists with generating a unit test for an Apache Spark UDF. This is step 1 of 3 in the UDF conversion workflow (udf-gen-test -> udf-convert-to-* -> udf-benchmark). Use this skill when you have a CPU UDF and need to create a unit test for the UDF before converting it into a GPU-compatible implementation.
license: CC-BY-4.0 AND Apache-2.0
metadata:
  spdx-file-copyright-text: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
model: inherit
---

# UDF Unit Test Generation

## Workflow

- [ ] Step 1: Set up project (copy template, add UDF source)
- [ ] Step 2: Implement the unit test (fill in TODO methods)
- [ ] Step 3: Compile and test until passing
- [ ] Step 4: Run coverage and inspect gaps
- [ ] Step 5: Verify outputs

**Before making any edits, create a visible TODO checklist for every workflow step in this skill and keep it updated.** Do not produce a final answer until every required checklist item is marked complete.

## Prerequisites

- Path to the input UDF file (Java or Scala)

Derive `<CamelName>` and `<snake_name>` from the UDF class name.

> **Note:** Commands require access to `/tmp` (Spark temp storage) and `/dev` (GPU device). If commands fail due to sandbox restrictions, re-run them unsandboxed.

## Step 1: Set Up the Project

### 1a. Copy the template project

The project can be found under this skill's templates directory.
```bash
cp -r templates/scala <project_root>/<CamelName>/
```

This provides a complete Maven project with all test and benchmark infrastructure.

### 1b. Copy or extract the UDF source

Before copying code, decide whether the input UDF is already self-contained:
- If the UDF file contains only the target UDF and local helpers it directly needs, copy it as-is.
- If the UDF is part of a larger project or a file containing unrelated UDFs/classes, extract only the target UDF class/object and all local helper classes/methods required for that UDF to compile and run (modifying package declarations as needed).

The template project should contain the smallest self-contained implementation of the target CPU UDF.

Place the resulting source file(s) in the source directory:
- Java: `<CamelName>/src/main/java/com/udf/`
- Scala: `<CamelName>/src/main/scala/com/udf/`

Set the package declaration to `com.udf`:
- Java: `package com.udf;`
- Scala: `package com.udf`

## Step 2: Implement the Unit Test

Read `src/test/scala/com/udf/UnitTest.scala`. Replace placeholders with the actual camel/snake UDF name.

Fill in the TODO methods following the docstrings. Include diverse edge cases in `createTestData` (nulls, empty strings, malformed inputs, varying lengths).

### Test Data Coverage

The generated tests should serve as a strong specification of the CPU UDF behavior over a documented input domain, and are intended to prove that a GPU or SQL implementation preserves the CPU UDF behavior.
For each input type and visible UDF branch, include applicable examples from these coverage dimensions:
- null inputs and null elements
- empty strings, arrays, maps, or structs
- malformed or unparsable inputs
- edges of input boundaries, such as min/max valid values, string length, or array length
- numeric sign/identity cases, such as negative, zero, and positive values
- string variety, such as unicode, ASCII, and encoding-sensitive inputs
- date/time boundaries, such as epoch, end-of-day/month/year, leap day, and DST/timezone transitions
- decimal precision and scale
- duplicate rows and repeated values
- mixed valid/invalid rows in the same DataFrame
- nested empty and nested null values

Assertions should verify schema, row count, deterministic ordering, output values, null propagation, and exception/default behavior. Every visible UDF branch should be covered by the unit test or explicitly documented as out of scope.

### Critical Requirements

- Do NOT hardcode the UDF name; use the provided `udfName` argument. This ensures the correct registered UDF is exercised.
- Assume the user's UDF implementation is correct; the assertions should reflect its actual behavior.

## Step 3: Compile and Test

```bash
mvn test -Dsuites=com.udf.UnitTest
```

If it fails, analyze the error output (stdout/stderr) and fix the test code. Continue iterating until the test passes.

## Step 4: Coverage Report

The template projects use JaCoCo (Java) / scoverage (Scala) code coverage tools.

```bash
# Java UDF
mvn -Pcoverage test jacoco:report -Dsuites=com.udf.UnitTest

# Scala UDF
mvn -Pcoverage scoverage:report -Dsuites=com.udf.UnitTest
```

For a Java UDF, read `target/site/jacoco/jacoco.csv` and inspect LINE, BRANCH, and METHOD counters for the target CPU UDF class and local helper classes. In `jacoco.xml`, counters appear as `<counter type="...">` elements, and source-line misses appear under `<sourcefile><line nr="..." mi="..." ci="..." mb="..." cb="...">`.

For a Scala UDF, read `target/scoverage.xml` and inspect statement, branch, and method-level coverage for the target CPU UDF class/object and local helper classes/objects. scoverage XML stores package/class/method `statement-rate` and `branch-rate` attributes, and each executable statement has `line`, `branch`, and `invocation-count` attributes.

Use the coverage report as actionable feedback:
1. Inspect missed Java line, branch, and method coverage, or missed Scala statement, branch, and method-level coverage.
2. Add test cases and assertions that exercise those paths.
3. Re-run the unit test and coverage report.
4. Repeat until important CPU UDF branches are covered.

If a missed line, statement, branch, or method path cannot or should not be tested, add a clear comment explaining why. Examples include:
- unreachable defensive code
- unsupported input domains
- unrelated template infrastructure

Report the relevant counters for the target CPU UDF and local helper classes/objects:
- Java: LINE, BRANCH, and METHOD counters from JaCoCo.
- Scala: statement and branch coverage from scoverage, plus method-level statement/branch rates from `<method>` elements.

NOTE: JaCoCo and scoverage will not track source-level coverage in external JARs. If the UDF relies on external JAR business logic, make a note of this residual coverage gap.

## Step 5: Verify Outputs

After the test passes, verify that:
1. The test data covers various edge cases and reflects realistic input formats
2. The assertions reflect actual UDF behavior (no "cheating" by hardcoding values)
3. The coverage report shows strong coverage of the target CPU UDF and local helper logic
4. Any uncovered lines, branches, or methods are explicitly explained
5. Any external JAR logic invoked by the UDF is called out as outside the coverage scope

If any quality checks fail, revise the test code and re-run.

## Output

Upon successful completion:
- Project directory: `<project_root>/<CamelName>/`
- Unit test: `src/test/scala/com/udf/UnitTest.scala`

These outputs are required for **Step 2: Convert UDF**.
