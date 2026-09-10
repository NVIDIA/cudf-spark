---
name: udf-benchmark
description: Assists with benchmarking and profiling the performance of an Apache Spark UDF on the GPU. This is step 3 of 3 in the UDF conversion workflow (udf-gen-test -> udf-convert-to-* -> udf-benchmark). Use this skill when you have a CPU UDF and a RapidsUDF or SQL implementation, and need to benchmark the performance of the CPU UDF against the GPU implementation.
license: CC-BY-4.0 AND Apache-2.0
metadata:
  spdx-file-copyright-text: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
model: inherit
---

# UDF Benchmark

## Workflow

- [ ] Step 1: Implement BenchUtils (fill in TODO methods)
- [ ] Step 2: Validate with a small dataset
- [ ] Step 3: Generate full benchmark data and run benchmarks
- [ ] Step 4: cuDF microbenchmarks (skip for SQL targets)

**Before making any edits, create a visible TODO checklist for every workflow step in this skill and keep it updated.** Do not produce a final answer until every required checklist item is marked complete.

## Prerequisites

- Project directory from Steps 1-2 (udf-gen-test, udf-convert-to-*) with passing tests

Derive `<CamelName>` and `<snake_name>` from the UDF class name.

> **Note:** Commands require access to `/tmp` (Spark temp storage) and `/dev` (GPU device). If commands fail due to sandbox restrictions, re-run them unsandboxed.

## Step 1: Implement BenchUtils

Read `src/main/scala/com/udf/bench/BenchUtils.scala`. Replace placeholders with the actual camel/snake UDF name.

Fill in the TODO methods following the docstrings. For variable-length inputs, generate sizable rows representative of enterprise-scale data. Refer to the unit test for schema and example data.

## Step 2: Validate

Make scripts executable:
```bash
chmod +x *.sh
```

Run validation mode to test with a small dataset:
```bash
./run_gen_data.sh --rows 1000 --validate
```

This runs both the CPU and GPU implementations on the dataset.
If validation fails, analyze the error and fix the BenchUtils implementation.

## Step 3: Generate Data and Run Benchmarks

The scripts set the default heap size to 16g in `.mvn/jvm.config`; adjust depending on data size.

### Generate benchmark data (10M rows):
```bash
./run_gen_data.sh --rows 10000000
```

### Run benchmarks:
```bash
# CPU benchmark
./run_spark_benchmark.sh --mode cpu --data-path data/bench_data_10000000_rows.parquet

# GPU benchmark
./run_spark_benchmark.sh --mode gpu --data-path data/bench_data_10000000_rows.parquet
```

Results are saved to the `results/` directory as JSON files.

## Step 4: cuDF Microbenchmarks

> Skip this step for SQL targets. This only applies to cuDF RapidsUDF conversions.

Follow [CUDF_MICROBENCHMARKS.md](CUDF_MICROBENCHMARKS.md) to implement and run in-memory microbenchmarks.

## Output

Upon successful completion:
- Benchmark utilities: `src/main/scala/com/udf/bench/BenchUtils.scala`
- Microbenchmarks (cuDF): `src/main/scala/com/udf/bench/MicroBenchRunner.scala`
- Generated data: `data/`
- Benchmark results: `results/`
