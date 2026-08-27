# GitHub Copilot Review Instructions for spark-rapids

cudf-spark is a GPU acceleration plugin for Apache Spark.
For full project context, coding conventions, and code examples,
see [AGENTS.md](../AGENTS.md).

## Cross-repo References

When reviewing shim code or GPU operator implementations, cross-reference
these upstream repositories to verify correctness:
- **apache/spark** — verify GPU behavior matches the CPU implementation in the target Spark version (expression evaluation semantics, null handling, type coercion, catalog/partition behavior)
- **rapidsai/cudf** — verify cuDF API usage (column operations, memory allocation semantics, null handling)
- **NVIDIA/cudf-spark-jni** — verify JNI binding signatures, memory ownership rules, error codes

## Review Checklist

- [ ] C1: Resource leaks — AutoCloseable not closed on exception paths; use withResource/closeOnExcept/safeClose, never bare .close() (see AGENTS.md § Resource management)
- [ ] C2: OOM retry — GPU-allocating code not in withRetry/withRetryNoSplit; retry function must be idempotent (see AGENTS.md § OOM retry)
- [ ] C3: Data correctness — GPU vs CPU divergence: nulls, NaN, overflow, decimal precision/scale, TIMESTAMP_NTZ vs LTZ, empty result handling
- [ ] C4: Shim consistency — shim change not adjusted across all Spark versions (see AGENTS.md § Shim Layer)
- [ ] C5: Resource lifecycle — SpillableColumnarBatch used after close or without retry handling
- [ ] H1: Performance — unnecessary host-device copies, redundant materializations, avoidable data serialization
- [ ] H2: Concurrency — missing GpuSemaphore.acquireIfNecessary(context), nested locks without ordering
- [ ] H3: Fallback gaps — new replacement rule without a fallback declaration or test
- [ ] H4: Test quality — no GPU execution verification, hardcoded sleeps, unseeded random data; GPU resource cleanup in afterAll/afterEach
- [ ] H5: Configuration — new RapidsConf without docs/defaults; should use .internal() if not user-visible; new features default off
- [ ] H6: Magic numbers — unexplained numeric literals without named constants or comments
- [ ] H7: Pre-merge CI gaps — only selected shims run unit tests; [databricks] needed for DB CI; feature-gated tests need explicit enable; limited Scala 2.13 coverage
- [ ] H8: Upstream dependencies — SNAPSHOT changes from cudf-spark-jni/cudf may break; verify API usage against upstream repos
- [ ] H10: Performance checklist — report `Performance: Not required` as a high-severity finding unless the PR is documentation-only or test-only, or its description gives a verifiable reason the change cannot affect runtime performance. A bug-fix label, small diff, or rarely used path is not by itself an exemption. When uncertain, flag

## Do NOT Comment On

- Code formatting or import ordering (handled by scalastyle + pre-commit)
- Naming preferences (unless actively misleading)
- Self-explanatory documentation suggestions
- Minor refactoring preferences
