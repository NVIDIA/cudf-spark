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
- [ ] H8: Upstream dependencies — SNAPSHOT changes from cudf-spark-jni/cudf may break; verify API usage against upstream repos
- [ ] H9: Databricks coverage — integration-test behavior may differ on Databricks, but the title lacks `[databricks]` and the complete PR diff does not auto-trigger DB CI through `databricksTestNeeded()` in `jenkins/Jenkinsfile-blossom.premerge`; check numbered DBR shim directories, `delta-lake` paths, and paths containing `databricks` before recommending the tag or equivalent validation
- [ ] H10: Performance checklist — report `Performance: Not required` as a high-severity finding unless the PR is documentation-only or test-only, or its description gives a verifiable reason the change cannot affect runtime performance. A bug-fix label, small diff, or rarely used path is not by itself an exemption. When uncertain, flag

## CI Title Suggestions

When the complete PR diff supports a faster CI mode, include at most one optional
suggestion in the review summary, giving the proposed full title and a short reason.
This advice is not an inline defect, an approval requirement, or a reason to lower
the confidence score; report actual correctness and coverage findings separately.
Do not repeat a suggestion already applied or declined in the PR discussion.

Check `CONTRIBUTING.md#blossom-ci` for policy and the PR's effective
`jenkins/Jenkinsfile-blossom.premerge` for supported tags and job selection; account
for the target branch and any pipeline changes in the PR instead of assuming that
every release branch supports the current defaults.

- For documentation-only changes, suggest `[skip ci]` when it is absent; executable
  scripts, build files, CI configuration, and tests are not documentation-only.
- For low-risk CI, build, script, or other localized changes, suggest `[reduced-it]`
  only when the diff shows that parameter interactions are not needed to validate
  the change and the selected job actually runs integration tests; it has no benefit
  for a skills-only pipeline, an already skipped build, or a title with the tag.
- Reduced IT covers each parameter value, not their interactions; follow
  `integration_tests/README.md` and retain full combinations when correctness may
  depend on types, formats, time zones, ANSI mode, code generation, or shims, including
  test-only changes that exercise those behaviors.
- Parallel Scala unit tests are already the premerge default; do not suggest
  `[fast-ut]`, and preserve `[serial-ut]` or `[serial ut]` when used for debugging
  concurrency-sensitive failures, as described in `tests/README.md`.
- Preserve required `[databricks]` coverage and all other valid title tags and
  category prefixes; append a missing CI tag at the end, separated by one space,
  without promising a fixed runtime improvement.

If an existing `[reduced-it]` tag omits combinations needed by the change, identify
the affected test and interaction as a coverage finding and recommend full IT.
