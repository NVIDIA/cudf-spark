# spark-rapids Code Review Rules

For full coding conventions, build commands, code examples, and
shim layer architecture, see `AGENTS.md` at the repo root.

## Review Checklist

- [ ] C1: Resource leaks — AutoCloseable not closed on exception paths; use withResource/closeOnExcept/safeClose, never bare .close() (see AGENTS.md § Resource management)
- [ ] C2: OOM retry — GPU-allocating code not in withRetry/withRetryNoSplit; retry function must be idempotent (see AGENTS.md § OOM retry)
- [ ] C3: Data correctness — GPU vs CPU divergence: nulls, NaN, overflow, decimal precision/scale, TIMESTAMP_NTZ vs LTZ, empty result handling
- [ ] C4: Shim consistency — shim change not adjusted across all Spark versions (see AGENTS.md § Shim Layer)
- [ ] C5: Resource lifecycle — SpillableColumnarBatch used after close or without retry handling
- [ ] H1: Performance — unnecessary host-device copies, redundant materializations, avoidable data serialization
- [ ] H2: Concurrency — missing GpuSemaphore.acquireIfNecessary(context), nested locks without ordering
- [ ] H3: Fallback gaps — new operator in GpuOverrides without fallback declaration or test
- [ ] H4: Test quality — no GPU execution verification, hardcoded sleeps, unseeded random data; GPU resource cleanup in afterAll/afterEach
- [ ] H5: Configuration — new RapidsConf without docs/defaults; should use .internal() if not user-visible; new features default off
- [ ] H6: Magic numbers — unexplained numeric literals without named constants or comments
- [ ] H7: Pre-merge CI gaps — only selected shims run unit tests; feature-gated tests need explicit enable; limited Scala 2.13 coverage
- [ ] H8: Upstream dependencies — SNAPSHOT changes from spark-rapids-jni/cudf may break; verify API usage against upstream repos
- [ ] H9: Databricks coverage — change may behave differently on Databricks (filesystem/path semantics, plan-string/optimizer assertions, DBR Spark-fork runtime) but the PR title lacks [databricks] and the diff doesn't touch a *db* shim path or a path containing `databricks` (either auto-triggers DB CI). Recommend [databricks] or confirm DB validation (see AGENTS.md § PR title tags)
