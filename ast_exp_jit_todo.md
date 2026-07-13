# AST JIT follow-up work

## Long term: hide source-JIT cold-start latency

Status: follow-up work; this does not block the initial opt-in merge.

Current behavior and deployment gaps:

- `compileAstsTime` measures Java/native AST construction, not RTC kernel
  compilation. Row-IR code generation, cache lookup or loading, NVRTC
  compilation, CUDA module loading, and execution are currently combined in
  `computeAstsTime`.
- The libcudf RTC cache is process-global and deduplicates concurrent requests
  for the same kernel within an executor. Its disk store can survive a process
  only when the selected cache directory is persistent.
- libcudf honors `LIBCUDF_KERNEL_CACHE_PATH`, but cudf-spark does not
  automatically configure a durable executor cache location. Deployment
  guidance is documented in
  `docs/additional-functionality/project-ast-jit.md`; operators must still
  provide a persistent executor path and retention policy. Premerge CI uses
  `/tmp/.cudf`, and the benchmark scripts use run-specific paths; neither is a
  production persistence policy.
- `LIBCUDF_KERNEL_CACHE_PRELOAD=1` loads existing disk entries into the process
  cache during initialization. It does not compile missing kernels and can add
  executor startup cost when the cache is large.
- `LIBCUDF_KERNEL_CACHE_LIMIT_PER_PROCESS` limits in-memory entries, not the
  persistent disk store. A durable deployment also needs disk usage monitoring,
  retention, or an external quota because old version- or architecture-specific
  entries remain on disk even when their cache keys no longer match.
- AQE determines the final GPU plan one query stage at a time, so eagerly
  compiling every kernel from the initial SQL plan can waste work on expressions
  that are later replanned or pruned.

Target design:

- Add an executor-local, bounded JIT prewarm manager with one compiler worker.
- Submit exact bound expression and input-type recipes from
  `GpuProjectAstExec.buildRetryableAstIterator` after output planning and before
  the first `input.next()`. This allows compilation to overlap scan, decode, or
  shuffle work and naturally follows AQE stage decisions.
- Deduplicate recipes by expression structure and physical input types. A
  foreground request for a kernel already being prepared must wait on the same
  cache future instead of compiling it again.
- Use a time and kernel-count budget, avoid starting new speculative work when
  executor CPU capacity is busy, and never rely on an interrupt to stop an
  NVRTC compilation already in progress.
- Keep prewarming disabled by default until measurements show that hidden
  critical-path time exceeds CPU contention and unused-kernel cost.

Native API follow-up:

- Add a synchronous schema-only libcudf prepare API, with a thin Java/JNI
  binding, that performs type resolution, row-IR code generation, cache lookup
  or loading, compilation, and CUDA module loading without allocating output
  columns or launching a kernel.
- Keep asynchronous scheduling in cudf-spark rather than in libcudf/JNI.
- Start with one expression and cache population. A long-lived prepared handle
  or multi-output execution should remain a separate design because it changes
  lifetime, schema-validation, and ANSI error-ordering contracts.
- If a stable contract is available, return `MEMORY_HIT`, `DISK_HIT`, or
  `COMPILED` plus lookup, load, compile, and module-load timings. Do not infer
  these states from the existing aggregate `computeAstsTime` metric.
- Treat a JNI-only zero-row `computeColumnJit` wrapper as an experimental
  measurement tool, not the production prepare API: it still touches RMM,
  launches a kernel, synchronizes a stream, and lacks Spark task/retry ownership
  on a background thread.
- Make RTC cache compilation failures remove or complete the reserved cache
  future with the original exception so a speculative failure cannot leave a
  broken entry for later foreground execution.

Phased work:

1. Add accurate cache/compile observability. Keep the documented persistent
   executor cache, retention, preload, and startup-cost guidance current.
2. Measure a default-off, stage-local zero-row prewarm prototype using the
   existing `computeColumnJit` API.
3. If the prototype hides meaningful critical-path time, add the schema-only
   libcudf prepare API and Java/JNI binding, then replace zero-row execution.
4. Consider an incremental, AQE-aware whole-query manifest only if stage-local
   lead time is insufficient. Do not add an auxiliary or barrier warmup job.

Acceptance criteria:

- Cold, disk-warm, and memory-hit costs are reported separately.
- A prewarmed foreground call does not compile the same kernel a second time.
- Prewarming does not allocate task output, consume retry OOM injection, or
  bypass GPU semaphore and RMM ownership.
- Query cancellation, executor shutdown, dynamic allocation, and prepare
  failures do not leak native state or fail an unused query branch.
- Benchmarks report critical-path savings, compiler CPU time, foreground wait
  time, prepared-kernel reuse, and unused prepared kernels.
- Persistent cache disk usage is bounded or has an explicit cleanup policy.

## Low priority: structured ANSI error identity

Status: deferred while project AST JIT remains experimental and disabled by default.

cuDF currently reports the JIT failure category, such as `OVERFLOW` or
`DIVISION_BY_ZERO`, but not the specific failing row-IR node. When a fused
expression contains multiple fallible operations, cudf-spark must infer the
Spark operator and query context from the first compatible error site. The
error category is preserved, but the reported operator or query context can
differ from Spark CPU.

Follow-up work:

- Add a structured cuDF/JNI error code instead of matching exception text.
- Return a stable failing-node or site identifier from row-IR execution.
- Carry that identifier through the Java bindings and map it to the exact
  `AstJitErrorSite` in cudf-spark.
- Remove the fallback first-compatible-site inference and the corresponding
  compatibility limitation.

Relevant code:

- `sql-plugin/src/main/scala/com/nvidia/spark/rapids/basicPhysicalOperators.scala`
  (`GpuProjectAstExec.translateAstJitError`)
- `docs/compatibility.md` (`Project AST JIT ANSI error reporting`)

Acceptance criteria:

- Nested and sibling fallible operations report the same Spark error class,
  operator, and query context as CPU execution.
- Error translation does not depend on the full cuDF exception message.
