---
layout: page
title: NVTX Profiling
nav_order: 4
parent: Developer Overview
---
# Using NVTX Ranges with the NVIDIA cuDF plugin for Apache Spark
NVTX ranges are typically used to profile applications that use the GPU. Such NVTX profiles,
once captured can be visually analyzed using
[NVIDIA NSight Systems](https://developer.nvidia.com/nsight-systems).
This document is specific to cuDF plugin profiling.

### STEPS:

We need to pass a flag to the spark executors / driver in order to enable NVTX collection.
This can be done for spark shell by adding the following configuration keys:
```
--conf spark.driver.extraJavaOptions=-Dai.rapids.cudf.nvtx.enabled=true
--conf spark.executor.extraJavaOptions=-Dai.rapids.cudf.nvtx.enabled=true
```
For java based profile tests add this to `JAVA_OPTS`
```
export JAVA_OPTS=”-Dai.rapids.cudf.nvtx.enabled=true”
```
To capture the process’ profile run: `nsys profile <command>` where command can be your Spark shell 
/ Java program etc.  This works typically in non-distributed mode.

To make it run in Spark’s distributed mode, start the worker with `nsys profile` in front of the
worker start command.

Here is an example that starts up a worker in standalone mode, profiles it and the shell
until the shell exits (using Ctrl+D) while stopping the worker process at the end.
```
nsys profile bash -c " \
CUDA_VISIBLE_DEVICES=0 ${SPARK_HOME}/sbin/start-worker.sh $master_url & \
$SPARK_HOME/bin/spark-shell; \
${SPARK_HOME}/sbin/stop-worker.sh"

```
If you need to kill the worker process that is being traced, do not use `kill -9`.

You should have a *.qdrep file once the trace completes. This can now be opened in NSight UI.

## How to add NVTX ranges to your code?

If you are in Java or Scala land you can do the following:

```
val nvtxRange = new NvtxRangeWithDoc(<NvtxId>, NvtxColor.YELLOW)
try {
  // the code you want to profile
} finally {
  nvtxRange.close()
}
```
See [nvtx_ranges.md](nvtx_ranges.md) for documentation on existing ranges and registering a new range.

In C++ land:
```
gdf_nvtx_range_push_hex("write_orc_all", 0xffff0000);
// the code you want to profile
gdf_nvtx_range_pop();
```

To use CPU profiling features, run the following command before running `nsys profile`:
```
sudo sh -c 'echo [level] >/proc/sys/kernel/perf_event_paranoid'
```
where valid values are { 1, 2 }. Refer to
[NVIDIA Nsight Systems documentation](https://docs.nvidia.com/nsight-systems/)
for further details.

## Project AST JIT diagnostics

For an AST-JIT-enabled Project, `spark.rapids.sql.explain=ALL` logs the final bound
tiers and their JIT execution groups. Input ordinals refer to the current tier's
input batch; output positions refer to its output batch. The log identifies
forwarded outputs and computed outputs materialized for the next tier.
`plugin_unique_ops` and `plugin_shared_ops` count operations using plugin expression
equivalence, not the final cuDF Row IR or GPU instructions. Grouping honors the
multi-output execution setting. These are final physical tiers, not the original
planner's dependency-wave identifiers.

With `spark.rapids.sql.metrics.level=DEBUG`, `GpuProjectExec` exposes:

| Metric key | Meaning |
|---|---|
| `astJitProgramBuildAttempts` | Calls to construct a schema-bound native program, including failed attempts. |
| `astJitProgramCacheHits` | Reuse of the owning expression's existing schema-compatible program. |
| `astJitProgramBuildTime` | Host elapsed time constructing programs, including failures, native cache lookup, and any compilation/waiting inside that call. |
| `astJitEvalAttempts` | Group evaluation calls, including failed attempts. A multi-output group counts once. |
| `astJitEvalRows` | Input rows summed over group evaluation attempts, including retries and repeated processing by different groups. |
| `astJitEvalTime` | Host elapsed time of group evaluation, including failed attempts and output wrapping; excludes explicit program construction. |

These counters measure attempted work, not committed output. In particular, a
batch processed by two groups contributes twice its row count. Program reuse is
local to the bound expression; a program build can still hit the native kernel
cache and perform no NVRTC compilation. Neither counter measures native cache
hits or misses. Projections evaluated within joins only receive these counters
if their owning operator supplies them.

The timing metrics are task-accumulated host times, not query wall time or isolated
GPU kernel time. Their work also contributes to the existing operator timing;
do not add them to operator time. Use native NVTX/compiler tracing to distinguish
actual NVRTC compilation from program construction and waiting.
