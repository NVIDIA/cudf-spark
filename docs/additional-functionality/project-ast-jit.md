---
layout: page
title: Project AST JIT
parent: Additional Functionality
nav_order: 6
---

# Project AST JIT

Project AST row intermediate representation (row-IR) just-in-time (JIT) compilation is
experimental, disabled by default, and controlled by internal configuration options. It should
currently be enabled only for evaluation on workloads where its performance and compatibility have
been measured.

## Execution model

When Project AST is enabled, an eligible Spark `Project` is replaced by `GpuProjectAstExec`. Each
computed output expression uses one of two cuDF AST backends:

- The legacy AST backend calls `CompiledExpression.computeColumn`.
- Expressions that require row-IR semantics call the explicit
  `CompiledExpression.computeColumnJit` API.

Each distinct computed output is evaluated separately. Project AST JIT does not compile the entire
Project output list into one kernel, although reusable equivalent outputs can be computed once.

The two backends can be used by different outputs in the same project. The row-IR backend lowers an
expression tree to CUDA source and compiles or loads its kernel on the executor. Its first call pays
for row-IR generation, cache lookup or loading, source compilation, and CUDA module loading as
needed. Later calls can reuse the executor-process memory cache, and later executor processes can
reuse a persistent disk cache when they use the same compatible cache path.

Project AST is selected only when every expression is supported and every output has a fixed-width
type. If AST selection fails, expressions that are otherwise supported on the GPU use the ordinary
`GpuProjectExec` path. Expressions whose GPU support depends on row-IR JIT follow the standard CPU
fallback behavior when row-IR JIT is disabled.

## Configuration

Set these options when the Spark application is submitted so the executor initializes the JIT
runtime before executing a query:

```text
--conf spark.rapids.sql.projectAstEnabled=true
--conf spark.rapids.sql.projectAstRowIrEnabled=true
```

The options have separate responsibilities:

| Option | Default | Purpose |
| --- | --- | --- |
| `spark.rapids.sql.projectAstEnabled` | `false` | Allows eligible projects to use `GpuProjectAstExec`. |
| `spark.rapids.sql.projectAstRowIrEnabled` | `false` | Allows safe row-IR JIT operations, including operations such as `try_*` arithmetic that return null for evaluation errors. |
| `spark.rapids.sql.projectAstAnsiArithmeticEnabled` | `false` | Allows row-IR arithmetic that propagates Spark ANSI errors. It has no effect unless row-IR JIT is also enabled. |

The row-IR option does not enable Project AST by itself. The master and row-IR options must both be
set for the explicit JIT path.

To evaluate ANSI error-propagating arithmetic, add:

```text
--conf spark.sql.ansi.enabled=true
--conf spark.rapids.sql.projectAstAnsiArithmeticEnabled=true
```

Keeping the row-IR and ANSI options separate allows safe or nullifying row-IR operations to be
evaluated without enabling fallible ANSI semantics.

## Executor runtime requirements

Each executor needs a compatible CUDA driver, `libnvrtc`, and `libnvJitLink`. When Project AST and
row-IR JIT are enabled, the plugin initializes the libcudf JIT runtime during executor GPU
initialization and fails before the first JIT kernel is executed if these runtime libraries cannot
be loaded.

`LIBCUDF_JIT_ENABLED` is not required for this feature. It is a process-wide libcudf backend
selector, while the plugin calls `computeColumnJit` explicitly for selected expressions. Do not
enable it for this feature. Enabling it overrides libcudf calls that the plugin selected for the
legacy backend and also changes Parquet filter paths. Leave it unset or set it to `OFF`; use `ON`
only for isolated testing of process-wide libcudf JIT behavior.

## Persistent kernel cache

libcudf automatically maintains an in-memory and on-disk JIT cache. cudf-spark does not need a
separate JNI API or plugin-specific cache option to activate it. Use Spark's executor environment
configuration to select a persistent cache root:

```text
--conf spark.executorEnv.LIBCUDF_KERNEL_CACHE_PATH=/var/cache/libcudf
```

For local mode, set the environment variable before starting the Spark JVM instead:

```bash
export LIBCUDF_KERNEL_CACHE_PATH=/var/cache/libcudf
```

The directory must be visible and readable, writable, and searchable by the executor process. Use a
persistent node-local directory or a persistent container volume if cache reuse across executor
restarts is desired. Do not make the directory writable by untrusted users. The cache is not
automatically shared across nodes. Keep the same absolute cache-root path across executor restarts;
the current source-JIT cache key includes the absolute path of the bundled JIT source.

If `LIBCUDF_KERNEL_CACHE_PATH` is not set or is unusable, libcudf tries these locations in order:

1. `${XDG_CACHE_HOME}/libcudf`
2. `${HOME}/.cache/libcudf`
3. `${TMPDIR}/libcudf`
4. `/tmp/libcudf`

libcudf creates `bundle`, `rtcx_cache`, `pch`, and `tmp` subdirectories under the selected root.
Compiled kernels are keyed by generated code, CUDA runtime and driver versions, GPU architecture,
and the libcudf JIT bundle. An incompatible entry is not reused after one of those inputs changes,
but old disk entries are not removed automatically.

The following optional executor environment variables control cache behavior. They are read once
during libcudf global-context initialization, so they must be set before the executor JVM starts.

| Variable | Default | Behavior |
| --- | --- | --- |
| `LIBCUDF_KERNEL_CACHE_PRELOAD` | `OFF` | Loads existing disk entries into the process cache during initialization. It does not compile missing kernels and can increase executor startup time. Disk entries are still loaded on demand when this is off. |
| `LIBCUDF_KERNEL_CACHE_LIMIT_PER_PROCESS` | `16384` | Limits entries in each in-memory artifact cache. It is an entry count, not a byte limit, and does not limit disk usage. |
| `LIBCUDF_KERNEL_CACHE_CLEAR` | `OFF` | Clears the RTCX memory and disk stores once during initialization. Caching resumes afterward. Use only for testing or recovery, and do not leave it enabled for executors sharing a directory. |
| `LIBCUDF_KERNEL_CACHE_DISABLED` | `OFF` | Bypasses cache reuse and forces JIT requests to compile again. It does not disable JIT and must not be used as a guarantee that no cache files are written. |

There is currently no built-in disk-size limit. A persistent deployment must monitor the cache and
apply an external quota or retention policy. Preloading is useful only when a reasonably small cache
contains kernels that the executor is likely to use; it is not query-aware precompilation.

## Performance and observability

The first use of a source-JIT kernel can be substantially slower than an ordinary GPU project.
Disk-warm execution avoids source compilation but still pays for cache lookup, artifact loading, and
module setup. The strongest benefit is expected when the same expression shapes are reused enough
to reach memory-cache hits. Keep the feature disabled for workloads where the cold-start cost is not
amortized.

The plugin does not currently precompile all kernels for a SQL query or compile them in a background
executor thread. A persistent cache is the available mechanism for carrying compiled artifacts
across executor restarts.

`GpuProjectAstExec` in the executed plan confirms that Project AST was selected, but it does not
identify which output expressions used the row-IR JIT backend. When the Project AST master option is
enabled, the default `spark.rapids.sql.explain=NOT_ON_GPU` setting logs AST rejection reasons. Use
`ALL` to include the complete expression eligibility tree. Neither setting reports the per-output
backend for a successfully selected AST project.

With `spark.rapids.sql.metrics.level=DEBUG`, `GpuProjectAstExec` reports these metrics:

- `compileAstsTime` measures construction of native AST objects. It does not measure CUDA source
  compilation.
- `computeAstsTime` includes row-IR generation, cache lookup or loading, source compilation, CUDA
  module loading, and execution. On a cold first call it therefore includes both compilation and
  kernel execution. It also includes legacy AST evaluation for outputs that use that backend.

There is not yet a separate memory-hit, disk-hit, or compiled status metric. In a source checkout,
use `scripts/run_ansi_jit_project_bench.sh` to compare CPU, ordinary GPU Project, cold JIT, disk-warm
JIT, and process-hot JIT modes.

## Compatibility and rollback

Safe row-IR and ANSI fallible semantics are separate gates. If exact ANSI error messages and query
context are required, review the current
[Project AST JIT ANSI error-reporting limitation](../compatibility.md#project-ast-jit-ansi-error-reporting)
before enabling `spark.rapids.sql.projectAstAnsiArithmeticEnabled`.

There is no runtime backend fallback after `GpuProjectAstExec` is selected. Non-retryable JIT
initialization, compilation, or evaluation errors propagate instead of switching to legacy AST,
ordinary GPU Project, or CPU execution. Normal GPU out-of-memory retry and row splitting still
apply.

To stop using source JIT while retaining legacy Project AST where it is eligible, disable only
`spark.rapids.sql.projectAstRowIrEnabled`. Expressions that require row-IR JIT may then use the
standard CPU fallback. Disable `spark.rapids.sql.projectAstEnabled` to disable Project AST selection
entirely.

## Developer verification

From a source checkout with a built plugin, this command runs the JIT-focused tests and requires the
test-runner process to load the required CUDA libraries instead of skipping them. The availability
probe is not a cluster-wide executor compatibility check.

```bash
LIBCUDF_JIT_ENABLED=0 \
TESTS=ast_test.py TEST_PARALLEL=0 \
./integration_tests/run_pyspark_from_build.sh -s \
  --libcudf_jit_mode=required -k 'jit or row_ir'
```
