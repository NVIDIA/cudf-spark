# GitHub Actions workflows

## CPU CI

PR CPU and GPU checks start from a `copy-pr-bot` push after `/ok to test`,
not from opening or updating a PR. Use `/ok to test <full-head-SHA>` for a new
revision. Scheduled and post-merge jobs keep their own triggers.

[CPU CI](cpu-ci.yml) gates the selected [Maven Verify](mvn-verify-check.yml)
and [Docker image](ci-images/README.md) checks on PRs to `main` or `release/YY.MM`.
Standalone pytest-only changes need a license check instead of the Maven build
matrix. Documentation used by Maven still gets verified. The Docker title
skip does not bypass Maven checks. A Maven failure does not cancel its siblings.

## GPU CI

[GPU CI](gpu-ci.yml) runs on PRs targeting `main` or `release/27.02` and later.
Documentation-only or Skills-only changes and a `[skip ci]` title skip these
jobs. The [UT selector](gpu-ci/ut_impact.py) runs just the changed ScalaTest
suites on the configured UT shims; the [IT selector](gpu-ci/it_impact.py)
runs just the affected pytest cases. Each narrows coverage only when it
can establish that the changes are isolated. Shared or uncertain changes run
all eight groups. The selection job lists the suites or cases. These reductions
apply only to standalone Spark. The same GPU gate requires
[Databricks](databricks-ci/README.md) when the title includes `[databricks]`
or the Databricks detector selects the change; otherwise it allocates no clusters.

[Selected UT, IT and Databricks groups](gpu-ci/AGENTS.md) run in parallel as
runners and clusters become available. A terminal failure cancels outstanding
groups; cluster cleanup still runs.

## Shared dependency caches

[Cache preparation](cache.yml) warms Maven dependencies and standalone IT Spark
archives on relevant pushes to `main` and release branches. A daily check on
`main` also picks up upstream SNAPSHOTs. Databricks smoke tests use the
workspace cache, not the standalone archive cache.
