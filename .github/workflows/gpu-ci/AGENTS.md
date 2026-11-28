# Maintaining standalone GPU tests

- Edit [gpu-ci-suites.json](../../gpu-ci-suites.json) for runner architecture,
  Scala/shim assignments and IT shard overrides. Balance using measured durations.
- [impact-rules.json](impact-rules.json) lists eligible Scala suite bases and
  pytest decorators. Ordinary tests need no entry. Add an exact path to
  `file_owners` only if changes need the full UT **or** IT suite, not both.
  For DB-only scripts, use `cluster.exclusive_files` in the
  [Databricks rules](../databricks-ci/paths.json).
- A rule change takes effect after it lands on the target branch. Missing or
  invalid rules, shared fixtures, and mixed product/test edits keep full
  standalone coverage. These rules never suppress Databricks.
- Keep worker timeouts, queue limits and the reporting gate consistent. On
  a failed-jobs rerun, a cancelled group is not a passing result.
- IT consumes the [standalone Spark archive](../../actions/spark-archive-cache/AGENTS.md).
  Never restore an AMD64 Maven cache on ARM64.
