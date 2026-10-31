# spark-rapids Code Review Rules

For full coding conventions, build commands, code examples, and
shim layer architecture, see `AGENTS.md` at the repo root.

## Local validation confirmation

For initial validation, accept the PR's checked local-testing item or an
equivalent concise statement; do not require pasted commands or full logs.
That checkbox does not establish validation of later commits.

For follow-up changes, inspect full commit messages, including trailers. Require
a declaration on the changed commit or a later tested tip that explicitly names
the covered commits or range. `Local-Tests: passed` declares passing applicable
local tests; one run may cover several commits, but not later untested changes.
For `Local-Tests: reused <tested-commit-sha>`, inspect the referenced validation
and the reason the intervening changes preserve coverage. Accept
`Local-Tests: not-required` with a brief, diff-supported reason when no new
runtime tests apply, such as prose-only documentation or comment changes with
no behavior change. Check applicable documentation/static validation; do not
demand a runtime rerun solely for those changes. Judge
actual impact, not file extensions. This declaration covers only those changes
and cannot waive validation of earlier untested code. `Local-Tests: not-run`
does not satisfy required validation. Do not require
rewriting published history; a new tested tip can declare the covered range.
These are commit-specific author declarations, not independent proof that tests
executed. Never infer follow-up validation solely from an unchanged checkbox.

Accept developer-managed validation run locally or on EMR, Dataproc, Databricks,
or another suitable remote environment matching the affected behavior, as
described in `CONTRIBUTING.md#testing-your-code`. Do not require every platform
for every change. Environment unavailability and maintainer approval do not
waive required tests. Missing required results are a validation blocker before
publication or premerge, rather than evidence of a test failure. Assess CI
results separately; they do not replace required developer-test evidence.

If the required declaration is missing, contradicted by observed results, or no
longer covers the changed behavior, cap the confidence score at **3/5** and
explain what remains unvalidated. Do not lower confidence merely because full
commands or logs are omitted, or a justified `not-required` declaration omits
runtime tests. Missing confirmation is not a test failure. Accept justified
inapplicable test layers, including runtime tests for changes confined to agent
instructions or review configuration. Compilation,
test collection, and all-skipped runs do not establish passing runtime tests.

## Shim compatibility review

For versioned sources, shared shim callers, or Spark/Databricks profile/matrix
changes, follow `.agents/skills/check-shim-compatibility/SKILL.md`. Read the
current head's `shim-compatibility` and `shim-related-prs` advisory summaries and artifacts;
verify head/base identities. If tools cannot retrieve the report or execute the
read-only helper, state the missing evidence instead of claiming a clean scan.

Cross-PR discovery runs automatically as an independent advisory and does not
gate the core combined-tree check. Inspect its matches targeting the same branch:

- Open PRs: all authors, including Drafts, without an age limit.
- Merged PRs: from the base actually covered by the latest applicable successful
  validation through the fetched current target branch. If that validated base
  or coverage is unknown, use the PR/target-branch common ancestor.

Prioritize related merges from the last 48 hours, but retain older changes in
that commit range. Earlier green premerge checks justify a narrower range only
when they included those changes and covered the affected profiles/shim APIs.
Pure documentation overlaps are excluded. For bounded open-peer joint checks,
report `NEW_INTERACTION_FINDINGS` with exact profile/symbol/file evidence and
distinguish existing individual findings. Inspect combination conflicts and
explicitly deferred/incomplete candidates separately. No new static interaction
findings does not establish full compatibility; do not attribute stale evidence.
Comment on confirmed new gaps with the file, symbol, profile, related PR links,
and observed evidence. Keep unchanged base findings separate. An association or
Draft alone is not a defect. Advisory `INCOMPLETE` means discovery evidence is
missing; report it without invalidating a completed core result. Core `INCOMPLETE`
is not a pass; static success is not compilation. Relevant base/head movement
requires a rerun of affected evidence. With an execution
environment such as TREX available, the same helper can produce the evidence;
these rules do not enable that environment by themselves.

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
- [ ] H9: Databricks coverage — change may behave differently on Databricks (filesystem/path semantics, plan-string/optimizer assertions, DBR Spark-fork runtime) but the PR title lacks [databricks] and the diff doesn't touch a *db* shim path or a path containing `databricks` (either auto-triggers DB CI). Recommend [databricks] or confirm DB validation (see AGENTS.md § PR title tags)
- [ ] H10: Performance checklist — report `Performance: Not required` as a high-severity finding unless the PR is documentation-only or test-only, or its description gives a verifiable reason the change cannot affect runtime performance. A bug-fix label, small diff, or rarely used path is not by itself an exemption. When uncertain, flag
