---
name: udf-optimize-cudf
description: Iteratively optimizes a cuDF RapidsUDF implementation for GPU performance. Use after testing and benchmarking with udf-benchmark. Runs a loop of profiling, optimizing, testing, and benchmarking until performance converges or the iteration budget is exhausted.
license: CC-BY-4.0 AND Apache-2.0
metadata:
  spdx-file-copyright-text: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
model: inherit
---

# Optimize cuDF RapidsUDF

## Workflow

- [ ] Step 0: Create backup and establish baseline
- [ ] Steps 1-4: Iterative optimization loop (repeat up to N iterations)
  - [ ] Step 1: Profile with nsys
  - [ ] Step 2: Implement one targeted change
  - [ ] Step 3: Run unit tests (fail &rarr; discard, retry)
  - [ ] Step 4: Run microbenchmarks (no improvement &rarr; discard, retry)
- [ ] Final Step 1: Run judge subagent if requested
- [ ] Final Step 2: Review optimized implementation and report results

## Prerequisites

- Project directory with passing unit tests and cuDF comparison test
- MicroBenchRunner implemented and working (from the **udf-benchmark** skill)
- Benchmark data generated (reuse from the benchmark step)

Derive `<CamelName>` and `<snake_name>` from the UDF class name.

> **Note:** Commands require access to `/tmp` (Spark temp storage) and `/dev` (GPU device). If commands fail due to sandbox restrictions, re-run them unsandboxed.

## Step 0: Create Backup and Establish Baseline

1. Create a backup of the current RapidsUDF implementation:
```bash
cp src/main/<java|scala>/com/udf/<CamelName>RapidsUDF.<java|scala> \
   src/main/<java|scala>/com/udf/<CamelName>RapidsUDF.<java|scala>.bak
```

2. If no `.orig.bak` exists yet, save the original unoptimized implementation:
```bash
cp src/main/<java|scala>/com/udf/<CamelName>RapidsUDF.<java|scala> \
   src/main/<java|scala>/com/udf/<CamelName>RapidsUDF.<java|scala>.orig.bak
```
This file is never overwritten; it preserves the pre-optimization baseline.

3. If no baseline microbenchmark results exist, run the baseline now:
```bash
./run_micro_benchmark.sh --mode all --data-path data/bench_data_<rows>_rows.parquet --rows <rows>
```

Record the baseline GPU time and speedup. This is the number to beat.

## Iterative Optimization Loop

Repeat the following steps up to **N iterations** (default: 10). Also stop early if no improvement is found after **3 consecutive failed attempts**.

Maintain an **optimization log** throughout the loop: for each iteration, record what change was attempted and whether it improved, regressed, or had no effect. This prevents repeating failed approaches and feeds the final report.

### Step 1: Profile with nsys

Profile the current implementation to identify bottlenecks:
```bash
./run_micro_benchmark.sh --mode gpu --data-path data/bench_data_<rows>_rows.parquet --rows <rows> --profile
```

Summarize libcudf kernel stats:
```bash
nsys stats --report nvtx_sum --format csv -o rapidsudf results/<report>.nsys-rep
```

Consult **references/OPTIMIZATION_PATTERNS.md** for interpreting profiler output and identifying optimization opportunities.

> **Tip:** Profiling frequently is strongly recommended. Without profiler data, optimization changes are guesses. Try using other `nsys stats` commands as needed.

### Step 2: Implement One Targeted Change

Based on profiling insights (or optimization patterns from the reference), make **one targeted change** to the RapidsUDF implementation. Isolating changes one at a time makes it possible to attribute performance impact.

### Step 3: Run Unit Tests

```bash
mvn test -Dsuites=com.udf.CudfComparisonTest
```

- **Tests pass** &rarr; proceed to Step 4
- **Tests fail** &rarr; analyze the failure. If it is an ordinary implementation bug, fix it and rerun the test. If the targeted optimization introduces a CPU/GPU semantic mismatch that cannot be resolved, discard changes by restoring from backup:
  ```bash
  cp src/main/<java|scala>/com/udf/<CamelName>RapidsUDF.<java|scala>.bak \
     src/main/<java|scala>/com/udf/<CamelName>RapidsUDF.<java|scala>
  ```
  Log the failure reason, then return to Step 1.

Sometimes, matching a certain edge case is impossible without a major performance tradeoff. If so, document the attempted fix, the benchmark evidence, and the exact behavior difference, then ask the user whether the performance-vs-correctness tradeoff is acceptable.
Do not comment out tests or accept a correctness difference during optimization unless the user explicitly approves that tradeoff.

### Step 4: Run Microbenchmarks

```bash
./run_micro_benchmark.sh --mode all --data-path data/bench_data_<rows>_rows.parquet --rows <rows>
```

Compare the GPU time against the current best (from the last checkpoint).

- **Performance improved** &rarr; create a new checkpoint:
  ```bash
  cp src/main/<java|scala>/com/udf/<CamelName>RapidsUDF.<java|scala> \
     src/main/<java|scala>/com/udf/<CamelName>RapidsUDF.<java|scala>.bak
  ```
  Record the new best GPU time. Reset the consecutive-failure counter. Return to Step 1.

- **Performance did NOT improve** &rarr; discard changes:
  ```bash
  cp src/main/<java|scala>/com/udf/<CamelName>RapidsUDF.<java|scala>.bak \
     src/main/<java|scala>/com/udf/<CamelName>RapidsUDF.<java|scala>
  ```
  Increment the consecutive-failure counter. Return to Step 1.

## Final Step 1: Run Judge Subagent If Requested

If the user explicitly asked for the judge, a judge subagent, or a review agent, treat that as an explicit request for delegation: you **MUST** launch a separate subagent with `model: inherit` and instruct it to use the **udf-judge-conversion** skill. Ask it to review the `UnitTest`, `CudfComparisonTest`, optimized RapidsUDF implementation, and optimization log as a cuDF conversion.

If the user did not request a judge/review agent, mark this step as skipped and continue to Final Step 2. If a required judge subagent is blocked by tool policy, stop and tell the user that explicit permission/instruction is needed.

If you run the judge, include the judge verdict in the final report. If there are any blocking issues, fix them or report the last known-good checkpoint.

## Final Step 2: Review Optimized Implementation and Report Results

After completing all iterations (or early-stopping), review your own work to ensure the optimization did not weaken correctness, introduce hardcoded test behavior, hide CPU fallback logic, or comment out core test coverage.

After completing all iterations (or early-stopping), report:
1. **Baseline**: starting GPU time and speedup
2. **Final**: best GPU time and speedup (from the last checkpoint)
3. **Successful optimizations**: what changes improved performance and by how much
4. **Failed optimizations**: what was attempted but did not help
5. **Review result**: self-review summary, or judge PASS/failures if the judge was requested

## Output

Upon successful completion:
- Optimized RapidsUDF: `src/main/<java|scala>/com/udf/<CamelName>RapidsUDF.<java|scala>`
- Backup of best version: `src/main/<java|scala>/com/udf/<CamelName>RapidsUDF.<java|scala>.bak`
- Original unoptimized version: `src/main/<java|scala>/com/udf/<CamelName>RapidsUDF.<java|scala>.orig.bak`
- Benchmark results: `results/`
