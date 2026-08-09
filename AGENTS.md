# spark-rapids Development Guide for AI Agents

This document provides context for AI coding agents (Claude Code, GitHub Copilot, etc.) working on the spark-rapids project.

## Safety Rules

- **Minimal diffs only** — do not reformat, reorganize imports, or refactor code outside the scope of the task
- **Never bypass CI** — do not use `--no-verify`, skip pre-commit hooks, or disable checks
- **Never invent new user-facing configuration or integration contracts** without explicit instruction
- **GPU resource hygiene** — all GPU resources (`ColumnarBatch`, `GpuColumnVector`, `DeviceMemoryBuffer`) must be managed with `withResource`/`closeOnExcept`, never bare `.close()`
- **Do not modify GPU operator implementations** without verifying that all relevant Spark version shims are updated consistently
- **Sign-off required** — all commits must use `git commit -s` for DCO compliance
- **No rebase during review** — if a PR is under review, do not rebase; merge the base branch instead to preserve reviewer comment context
- **Scala 2.13 sync** — after modifying any `pom.xml`, run `./build/make-scala-version-build-files.sh 2.13`
- **PR title tags** — `[databricks]` triggers Databricks pre-merge CI; `[skip ci]` for doc-only changes. Databricks CI auto-runs only when the diff touches a `sql-plugin/src/main/…db/` shim dir or a path containing `databricks`; otherwise it does **not** run. Add `[databricks]` manually when a change could behave differently on Databricks without touching those paths — e.g. integration tests that depend on filesystem/path semantics (local vs DBFS/`abfss`, `file://` scheme, `os.walk`/`os.path`), or optimizer/plan-string assertions (alias names and plan rendering differ on DBR) — since the Linux pre-merge will not catch DBR-only failures

## Build Commands

See [`.claude/skills/build-and-test.md`](.claude/skills/build-and-test.md) for full build, test, and performance validation commands.

## Project Structure

```
spark-rapids/
├── sql-plugin/                    # Core GPU acceleration plugin
│   ├── src/main/scala/            # Main Scala sources
│   │   └── com/nvidia/spark/rapids/
│   │       ├── GpuOverrides.scala       # GPU operator registry & fallback rules
│   │       ├── RapidsConf.scala         # Configuration keys & defaults
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
{"spark": "330"}
{"spark": "330db"}
spark-rapids-shim-json-lines ***/
```

**Key rules:**
- The annotation controls which build profiles include the file.
  Files **without** the annotation compile for **all** versions.
- By convention the file lives under the alphabetically earliest
  version directory it supports:
  `sql-plugin/src/main/<lowest_buildver>/scala/...`
- When modifying a shim, update ALL related Spark version shims
  (the same logical change may need different adaptations per
  version — do not blindly copy-paste).
- `db` suffix (e.g., `330db`, `350db143`) = Databricks-specific shim.
- **Scala 2.12 vs 2.13**: `sql-plugin` shims work identically for
  both Scala versions. After modifying any `pom.xml`, run
  `./build/make-scala-version-build-files.sh 2.13` to sync.
- **Delta Lake** uses version-specific Maven modules instead of the
  JSON annotation (e.g., `delta-20x/`, `delta-spark330db/`). Each
  module compiles only for its target Spark+Delta combination.

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
