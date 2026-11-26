# GitHub Actions workflows

## CPU CI

[CPU CI](cpu-ci.yml) gates the selected [Maven Verify](mvn-verify-check.yml)
and [Docker image](ci-images/README.md) checks on PRs to `main` or `release/YY.MM`.
Standalone pytest-only changes need a license check instead of the Maven build
matrix. Documentation used by Maven still gets verified. The Docker title
skip does not bypass Maven checks.

## GPU CI

[GPU CI](gpu-ci.yml) tests the approved PR head merged with its target.
Documentation-only or Skills-only changes and a `[skip ci]` title skip these
jobs. The [impact selector](gpu-ci/impact.py) runs just the changed ScalaTest
suites on the configured UT shims, or just the affected pytest cases, when it
can establish that the changes are isolated. Shared or uncertain changes run
all eight groups. The selection job lists the suites or cases. These reductions
apply only to standalone Spark, not to Databricks.

[GPU CI suites](../gpu-ci-suites.json) assigns four unit-test runners by Scala
version and Spark shim. Groups default to AMD64; set `"arch": "arm64"` on an
individual group to use a native ARM64 worker. Each Scala version also has two
integration-test runners for the Spark distribution
in the [archive catalog](../actions/spark-archive-cache/releases.json).
Each group has its own result, so a failed group can be rerun without repeating
successful groups. Rebalance from measured group times, not the number of shims.

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
GPU workers testing a PR should restore the exact Maven key as the Verify matrix does,
and build normally on a miss. `premerge: true` is for the branch warmer: an exact hit
checks that the cache exists without downloading it. Do not save PR-built artifacts to a branch cache.

The same warmer seeds the standalone [Apache Spark distributions](../actions/spark-archive-cache/releases.json).
Integration-test workers use the [archive action](../actions/spark-archive-cache/action.yml)
to restore a checksum-verified tarball by Scala version. Extract its `path` output
instead of downloading Spark again. When a standalone lane changes Spark version,
update its archive version and checksum together. The warmer does not cache locally
built plugin JARs, test outputs or native objects.
