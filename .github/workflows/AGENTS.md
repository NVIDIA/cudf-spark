# Maintaining GitHub Actions

- **CPU CI:** [cpu-ci.yml](cpu-ci.yml) gates the selected Maven and image checks.
  Keep Maven path policy in [mvn-verify-paths.json](mvn-verify-paths.json) and
  its matrices non-fail-fast. Keep existing check names until branch rules change.
- [Standalone GPU tests](gpu-ci/AGENTS.md) and [Databricks](databricks-ci/AGENTS.md)
  share one GPU result. Keep cloud credentials out of standalone workers.
- For cache keys and inputs, see [Maven dependencies](../actions/maven-cache/AGENTS.md)
  or [standalone Spark archives](../actions/spark-archive-cache/AGENTS.md).
- For Docker recipes, publication and cleanup, see [ci-images/AGENTS.md](ci-images/AGENTS.md).
