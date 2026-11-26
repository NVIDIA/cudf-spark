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

[GPU CI](gpu-ci.yml) tests the approved PR head merged with `main` or
`release/27.02` and later.
Documentation-only or Skills-only changes and a `[skip ci]` title skip these
jobs. The [UT selector](gpu-ci/ut_impact.py) runs just the changed ScalaTest
suites on the configured UT shims; the [IT selector](gpu-ci/it_impact.py)
runs just the affected pytest cases. Each narrows coverage only when it
can establish that the changes are isolated. Shared or uncertain changes run
all eight groups. The selection job lists the suites or cases. These reductions
apply only to standalone Spark. The same GPU gate requires
[Databricks](databricks-ci/README.md) when the title includes `[databricks]`
or the Databricks detector selects the change; otherwise it allocates no clusters.

[GPU CI suites](../gpu-ci-suites.json) assigns four unit-test runners by Scala
version and Spark shim. Groups default to AMD64; set `"arch": "arm64"` on an
individual group to use a native ARM64 worker. Each Scala version also has two
integration-test runners for the Spark distribution
in the [archive catalog](../actions/spark-archive-cache/releases.json).
The first UT or IT failure cancels outstanding groups and prevents Databricks
from starting. Rebalance from measured group times, not the number of shims.

## Shared dependency caches

[Cache preparation](cache.yml) seeds Maven dependencies for [Maven Verify](mvn-verify-check.yml)
and standalone GPU CI using the [Maven cache action](../actions/maven-cache/action.yml).
The seed includes the Scala compiler bridge and standalone integration-test dependencies.
When no current seed is available, Verify can save a smaller cache on the PR.
Later commits on that PR reuse it; if dependencies change, the previous cache is a starting point.
Hosted Maven jobs use [public settings](../actions/maven-cache/settings.xml) for Central,
including transitive and plugin dependencies. Local builds are unchanged. To opt in for
one command, pass `-gs .github/actions/maven-cache/settings.xml` from the repo root.
Relevant pushes warm `main` and release branches; a daily check on `main` also
detects upstream SNAPSHOTs published without a repository change.

The key separates branch, platform and Scala version. JNI, private and history-metrics
SNAPSHOTs track their own publications; fixed versions use their coordinates.
PRs can reuse their target branch's cache. A copied-branch workflow can pass
`base-branch` to compute the same key, but GitHub's cache visibility still applies:
a push on a sibling branch cannot read a release-only cache by naming it.
GPU workers restore the exact Maven key and build normally on a miss. IT requests
the full runtime seed; a smaller Verify cache is not a complete IT seed.
Do not save PR-built artifacts to a branch cache.

The same warmer seeds the standalone [Apache Spark distributions](../actions/spark-archive-cache/releases.json).
Integration-test workers use the [archive action](../actions/spark-archive-cache/action.yml)
to restore a checksum-verified tarball by Scala version. Extract its `path` output
instead of downloading Spark again. When a standalone lane changes Spark version,
update its archive version and checksum together. The warmer does not cache locally
built plugin JARs, test outputs or native objects.
