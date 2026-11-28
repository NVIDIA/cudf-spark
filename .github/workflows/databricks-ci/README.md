# Databricks in GPU CI

Selected Databricks cells run alongside standalone UT and IT.
A `[databricks]` title or a Databricks path match
selects both test parts for each supported DBR. `[reduced-it]` changes the tests, not
selection; `[skip ci]` wins. A verified no-impact change needs no cluster.
The combined **GPU CI gate** reports on the PR.

[paths.json](paths.json) separates the CPU helper-test paths from the cluster
selection rules. The controller reads the cluster rules from trusted `main`.

Each cell tries **aws2, aws, then azure3**, at most twice per profile. Only
classified infrastructure failures can advance, and only after confirmed
termination and cleanup. Build failures, product-test failures and ambiguous
cluster creation stop the cell. If creation or teardown is unconfirmed,
reconcile the cluster by the CI build tag before retrying. An always-run
cleanup step and the cluster idle timeout are safeguards, not proof of
cleanup after a runner is lost. An independent [hourly cleanup](../databricks-cleanup.yml)
rechecks the cluster's CI creator, ownership tags, name, profile and completed
GitHub attempt before deleting it. It leaves active and unrelated clusters alone.

Use **Re-run failed jobs** in the run linked from the gate. If a required cancelled group
is not picked up on a failed-jobs rerun, use **Re-run all jobs**.
If a previous cell's cleanup is unconfirmed, wait for a successful hourly
reconciliation before retrying it.

## Configuration

- Use a protected GitHub environment named `databricks-ci`. Allow deployments
  from the exact `main` branch only, not tags or copied PR branches. Do not
  put these tokens in repository or organization secrets.
- Store one token per workspace in that environment: `DATABRICKS_AWS2_TOKEN`,
  `DATABRICKS_AWS_TOKEN` and `DATABRICKS_AZURE3_TOKEN`. Its
  `DATABRICKS_PROFILES_JSON` variable maps each profile to its HTTPS `host`,
  `driver`, `worker` and dedicated CI `creator_user_name`.
- Use isolated CI Databricks workspaces and compute identities: PR code runs
  on the driver. Do not expose shared production storage, credentials or
  instance permissions there. PR compute must not be able to modify binaries
  shared with trusted workloads. Provide a read-only preseeded cache or an
  isolated CI cache. The runner uses the workspace HTTPS API; no SSH or
  bastion credentials are needed.
- Set `DATABRICKS_CI_RUNNER_GROUP` to a dedicated ephemeral Linux runner
  group restricted to [the DB cell](../databricks-ci-cell.yml),
  [cache preparation](../cache.yml) and [hourly cleanup](../databricks-cleanup.yml)
  on `main`. Install Git, GitHub CLI, Bash, curl, Python 3.9+ and Requests. Set
  `DATABRICKS_CI_ENABLED=true` only after these controls are in place.
  Missing configuration fails a required Databricks selection before allocation.
  The cache publisher also needs a JDK, Maven, and HTTPS access to public Maven repositories.

After each phase, the Actions console shows stdout and stderr tails. Failed
phases also show bounded diagnostic excerpts. No cloud credentials are passed
to the source under test.

Fresh drivers start with prewarmed third-party Maven dependencies when available.
A missing or corrupt seed leaves Maven free to download dependencies normally;
JNI and private SNAPSHOTs are never supplied by this cache.
