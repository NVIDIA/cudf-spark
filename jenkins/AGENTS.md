# Jenkins CI

- **Dockerfiles:** Edits to registered recipes are detected automatically. For a
  new or renamed Dockerfile, or changed local `COPY`/`ADD` inputs, update the
  [image catalog](../.github/workflows/ci-images/suites.json) (including `recipe-inputs`).
  Keep build arguments and Maven URL overrides compatible with both CI systems.
  See the [image guide](../.github/workflows/ci-images/AGENTS.md).
- **Standalone tests:** Ordinary tests need no registration. Change the
  [suite catalog](../.github/gpu-ci-suites.json) for shim or shard assignments
  and the [impact rules](../.github/workflows/gpu-ci/impact-rules.json) for
  UT-only or IT-only launchers. When the standalone Spark distribution changes,
  update the [archive manifest](../.github/actions/spark-archive-cache/releases.json).
- **Databricks:** Update the [path rules](../.github/workflows/databricks-ci/paths.json)
  when a script should select DB tests. Only DB-only scripts belong in
  `cluster.exclusive_files`.
