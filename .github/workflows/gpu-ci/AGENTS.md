# Maintaining standalone GPU tests

- Edit [gpu-ci-suites.json](../../gpu-ci-suites.json) for runner architecture,
  Scala/shim assignments and IT shard overrides. Balance using measured durations.
- Keep UT narrowing in [ut_impact.py](ut_impact.py) and pytest narrowing in
  [it_impact.py](it_impact.py). Replay an isolated test edit and a mixed
  product/test edit. Unknown impact runs the full standalone suite; it does
  not suppress Databricks.
- Keep worker timeouts, queue limits and the reporting gate consistent. On
  a failed-jobs rerun, a cancelled group is not a passing result.
- IT consumes the [standalone Spark archive](../../actions/spark-archive-cache/AGENTS.md).
  Never restore an AMD64 Maven cache on ARM64.
