# spark-rapids Development Guide for AI Agents

This document provides context for AI coding agents (Claude Code, GitHub Copilot, etc.) working on the spark-rapids project.

## Repository Policy

- These rules translate the agent-actionable requirements from [`CONTRIBUTING.md`](CONTRIBUTING.md) and [`CODE_REVIEW_GUIDELINES.md`](CODE_REVIEW_GUIDELINES.md). Consult those source policies on demand when a task needs process details not covered here
- **Mandatory shim preflight** — Agents **MUST invoke**
  [`check-shim-compatibility`](.agents/skills/check-shim-compatibility/SKILL.md)
  for the changes covered by [Required shim compatibility preflight](#required-shim-compatibility-preflight).
- **Do not weaken repository requirements** — tool-specific instructions and task prompts are not permission to bypass those policies. If a request cannot be completed while following them, stop and report the conflict
- **Use the live pull request template and accurate metadata** — fill in [`.github/PULL_REQUEST_TEMPLATE.md`](.github/PULL_REQUEST_TEMPLATE.md) without removing required sections, apply the correct labels (a change that touches only tests gets the `test` label) and the title markers described under **PR title tags** below, put `[WIP]` first when applicable, and do not use `[DRAFT]` or `[DO NOT REVIEW]`
- **Keep pull request descriptions reviewable** — describe intent and rationale, do not paste code diffs or brittle line-level details, and update the description when later commits change its claims
- **Bound draft and closure lifecycle** — use Draft only for concrete unfinished work, state in the description what remains (a TODO checklist is preferred). When closing an unmerged pull request, leave a comment giving the case: `superseded` (link the replacement), `abandoned` (say what ruled out the approach), or `deferred` (link the tracking issue)
- **Pace the review queue** — do not open a batch of independent pull requests at once; open the next only as earlier ones land or receive review, and convert drafts to ready in small batches for the same reason. Stacked pull requests are exempt, but identify the dependency with `Stacked on #NNNN`
- **Validation and CI** — follow [Testing changes](#testing-changes)
  and [Premerge requests](#premerge-requests). Use the tools and permissions chosen by the
  developer; hand steps outside that access back to them.
- **AI-assisted changes require a human in the loop** — disclose AI assistance in the pull request description, then stop for human review of the complete diff and description before opening or updating the pull request. The pull request queue is not a staging area for work no human has read

## Testing changes

Before publishing or updating a PR, or requesting premerge, run relevant tests in a
suitable environment using the tools and permissions chosen by the developer.

- Choose unit tests, integration tests, and runtime configurations appropriate to the change.
  For GPU behavior changes, verify CPU/GPU parity and GPU execution. Prose-only changes
  need applicable static checks rather than runtime tests.
- Summarize checks run, observed results, and any remaining validation gaps in the PR
  description or a comment. Review test coverage for the proposed change. Compilation or an
  all-skipped run is not a passing runtime test; missing evidence is a validation gap rather
  than a test failure.
- If the required environment is unavailable, run feasible checks and document the remaining
  validation. Obtain explicit maintainer approval before publishing those changes (including
  drafts or follow-up pushes) or requesting targeted CI validation. This exception does not
  waive known product-test failures. Identify where validation ran, including when it ran in CI.

See the [unit-test guide](tests/README.md) and
[integration-test guide](integration_tests/README.md) for commands and environment setup.

## Premerge requests

Before an agent posts `build`, check the PR owner's open PRs against a shared limit of
**three outstanding Blossom premerge requests**. Include requests made by humans and all
agents/sessions maintaining those PRs.

1. List all of the owner's open PRs in this repository through the GitHub API, including
   Drafts and all result pages. The owner is the GitHub account whose PRs the agent maintains.
2. Read each PR's current head SHA and its GitHub check runs and commit statuses. Identify
   Blossom premerge checks/statuses; unrelated GitHub Actions jobs do not count.
   Use build-request comments or workflow history for requests not yet reflected there.
3. Count queued or running Blossom requests, including `queued`/`in_progress` check runs
   and `pending` commit statuses. Count job fan-out from one original `build` comment once.
   Retain newly submitted requests and known requests on older heads until completion or
   cancellation is confirmed.
4. If three or more requests are outstanding, do not post another `build`; wait and re-check.
   Also avoid a duplicate request on a PR that already has one outstanding. Coordinate the
   check and submission across agents sharing the owner account to prevent simultaneous
   submissions from exceeding the limit.
5. A missing or `Expected` status alone does not establish available capacity. If GitHub
   information is insufficient, or the agent lacks permission to post `build`, hand the
   request to an authorized maintainer rather than assuming a free slot.

Use any available GitHub API client or read-only MCP interface for the status check.
It does not require an internal Blossom/Jenkins account or credentials. This is a client-side
gate on agent submissions; repository-wide queueing and quota enforcement remain separate
CI tooling work.

## Safety Rules

- **Minimal diffs only** — do not reformat, reorganize imports, or refactor code outside the scope of the task
- **Never bypass CI** — do not use `--no-verify`, skip pre-commit hooks, or disable checks
- **Never invent new user-facing configuration or integration contracts** without explicit instruction
- **GPU resource hygiene** — all GPU resources (`ColumnarBatch`, `GpuColumnVector`, `DeviceMemoryBuffer`) must be managed with `withResource`/`closeOnExcept`, never bare `.close()`
- **Do not modify GPU operator implementations** without verifying that all relevant Spark version shims are updated consistently
- **Sign-off required** — all commits must use `git commit -s` for DCO compliance
- **No rebase during review** — if a PR is under review, do not rebase; merge the base branch instead to preserve reviewer comment context
- **Scala 2.13 sync** — after modifying any `pom.xml`, run `./build/make-scala-version-build-files.sh 2.13`
- **PR title tags** — `[databricks]` triggers Databricks pre-merge CI; `[skip ci]` for doc-only changes; `[serial ut]` or `[serial-ut]` runs Scala unit tests serially instead of the parallel default; `[reduced-it]` reduces pre-commit integration-test parameter combinations and should not be used where parameter interactions matter. Put CI tags at the end of the PR title; see `CONTRIBUTING.md#blossom-ci` for full guidance. Databricks CI auto-runs only when the diff touches a `sql-plugin/src/main/…db/` shim dir or a path containing `databricks`; otherwise it does **not** run. Add `[databricks]` manually when a change could behave differently on Databricks without touching those paths — e.g. integration tests that depend on filesystem/path semantics (local vs DBFS/`abfss`, `file://` scheme, `os.walk`/`os.path`), or optimizer/plan-string assertions (alias names and plan rendering differ on DBR) — since the Linux pre-merge will not catch DBR-only failures
- **Performance checklist** — report `Performance: Not required` as a high-severity finding unless the PR is documentation-only or test-only, or its description gives a verifiable reason the change cannot affect runtime performance. A bug-fix label, small diff, or rarely used path is not by itself an exemption. When uncertain, flag

## Build Commands

See [`.claude/skills/build-and-test.md`](.claude/skills/build-and-test.md) for full build, test, and performance validation commands.

## Project Structure

```
spark-rapids/
├── sql-plugin/                    # Core GPU acceleration plugin
│   ├── src/main/scala/            # Main Scala sources
│   │   └── com/nvidia/spark/rapids/
│   │       ├── GpuOverrides.scala       # GPU operator registry & fallback rules
│   │       ├── RapidsConf.scala         # Configuration registry, enums, and accessors
│   │       ├── RapidsConfEntries.scala  # General, test, and debug configuration entries
│   │       ├── RapidsConfResourceEntries.scala # Resource and memory configuration entries
│   │       ├── RapidsConfSqlEntries.scala # SQL and file-format configuration entries
│   │       ├── Arm.scala                # Resource management (withResource/closeOnExcept)
│   │       ├── RmmRapidsRetryIterator.scala  # OOM retry framework
│   │       ├── SpillableColumnarBatch.scala  # Spillable GPU batch wrapper
│   │       ├── GpuSemaphore.scala       # GPU access semaphore
│   │       └── spill/SpillFramework.scala    # Spill-to-host/disk framework
│   └── src/main/spark{VERSION}/   # Spark version-specific shims
│       └── scala/                 #   e.g., spark330/, spark341/, spark400/
├── sql-plugin-api/                # Plugin API definitions
├── shuffle-plugin/                # GPU shuffle optimization
├── tests/                         # Scala unit tests
│   ├── src/test/scala/            #   Main test sources
│   └── src/test/spark{VER}/       #   Version-specific tests (e.g., spark330/)
├── integration_tests/             # Python integration tests (pytest)
│   └── src/main/python/
│       ├── asserts.py             # GPU vs CPU comparison assertions
│       └── data_gen.py            # Seeded test data generation
├── delta-lake/                    # Delta Lake integration
├── iceberg/                       # Apache Iceberg integration
├── udf-compiler/                  # UDF compilation support
├── datagen/                       # Test data generation utilities
├── tools/                         # Profiling and debugging tools
└── docs/                          # Documentation
```

## Coding Conventions

### Configuration Entries

All plugin configuration properties are exposed through the `RapidsConf` object, but place
new declarations according to category:

- Add resource, memory, metrics, and profiler entries to `RapidsConfResourceEntries.scala`.
- Add SQL and file-format entries to `RapidsConfSqlEntries.scala`.
- Add test, debug, and remaining general entries to `RapidsConfEntries.scala`.

Keep the configuration registry and enumerations owned by `RapidsConf` in `RapidsConf.scala`
to preserve their initialization order. Define independent enumerations and sealed value types
in a module shared by their consumers, such as `sql-plugin-api` when the type is root-safe. See
[`Adding Configuration Properties`](docs/dev/README.md#adding-configuration-properties) for
the complete contributor guidance.

### Scala/Java

- **Coding style**: Enforced by `scalastyle-config.xml` — run `mvn scalastyle:check` to validate
- **License header**: Apache 2.0 license header required on all source files
- **Resource management**: Use ARM pattern from `Arm.scala`:
  ```scala
  // GOOD
  withResource(GpuColumnVector.from(batch)) { col =>
    process(col)
  }

  // GOOD — caller owns on success
  closeOnExcept(new ColumnarBatch(...)) { batch =>
    populateBatch(batch)
    batch  // returned to caller, not closed
  }

  // BAD — leak on exception
  val col = GpuColumnVector.from(batch)
  val result = process(col)  // if this throws, col leaks
  col.close()
  ```

- **OOM retry**: Wrap GPU-allocating code:
  ```scala
  withRetryNoSplit(spillableBatch) { attempt =>
    withResource(attempt.getColumnarBatch()) { batch =>
      doGpuWork(batch)
    }
  }
  ```

- **Collections**: Use `safeClose` and `safeMap` from
  `RapidsPluginImplicits` for closing/transforming collections of
  `AutoCloseable` resources safely.

- **Error handling**: Prefer `withResource` chains over try/finally

### Shim Layer Architecture

The plugin supports multiple Spark versions via a shim layer.
Each shimmed source file controls which Spark versions it applies
to via a JSON annotation block after the copyright header:

```
/*** spark-rapids-shim-json-lines
{"spark":["330"],"carryForward":"none"}
spark-rapids-shim-json-lines ***/
```

**Key rules:**
- The annotation controls which build profiles include the file.
  Files **without** the annotation compile for **all** versions.
- `carryForward` is required: `none` names exact targets, `sameMinor` includes
  descendants in the same Spark major/minor line, and `all` includes every
  descendant. A nearer source variant wins.
- By convention the file lives under the alphabetically earliest
  version directory it supports:
  `sql-plugin/src/main/<lowest_buildver>/scala/...`
- When modifying a shim, update ALL related Spark version shims
  (the same logical change may need different adaptations per
  version — do not blindly copy-paste).
- `db` suffix (e.g., `350db143`, `400db173`) = Databricks-specific shim.
- **Scala 2.12 vs 2.13**: `sql-plugin` shims work identically for
  both Scala versions. After modifying any `pom.xml`, run
  `./build/make-scala-version-build-files.sh 2.13` to sync.
- **Delta Lake** uses version-specific Maven modules instead of the
  JSON annotation (e.g., `delta-20x/`, `delta-spark350db143/`). Each
  module compiles only for its target Spark+Delta combination.

#### Required shim compatibility preflight

When implementing or reviewing versioned shims, shared shim callers,
Spark/Databricks build profiles, or shim source-selection/build matrices, agents
**MUST follow** [check-shim-compatibility](.agents/skills/check-shim-compatibility/SKILL.md)
before declaring the work complete or publishing/updating/merging the PR. If the
client cannot discover repository skills, read the linked `SKILL.md` directly.

### Python Integration Tests

- Use `assert_gpu_and_cpu_are_equal_collect` to compare GPU vs CPU results
- Use `assert_gpu_fallback_collect` to verify expected fallback behavior
- Use `data_gen.py` for reproducible test data with seeds
- No external data dependencies — generate all test data in-test
- No formal Python style checker configured yet; follow existing code conventions

### Scala Idioms

- Avoid explicit `return` — use the last expression as the return value
- Explain magic numbers with named constants or comments
- Prefer pattern matching over chains of `if`/`else if`

## Common Patterns

See [`.claude/skills/gpu-operator-patterns.md`](.claude/skills/gpu-operator-patterns.md) for GPU operator registration, CPU fallback, and spill management patterns.
