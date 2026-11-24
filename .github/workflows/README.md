# GitHub Actions workflows

## CPU CI

[CPU CI](cpu-ci.yml) runs the checks affected by a PR to `main` or
`release/YY.MM`. The gate passes when selection and all selected checks pass.
Currently it checks [Docker images](ci-images/README.md). Markdown-only PRs
and PRs titled with `[skip ci]` pass without building images.

## Shared dependency caches

[Cache preparation](cache.yml) seeds Maven dependencies for [Maven Verify](mvn-verify-check.yml)
and future standalone premerge jobs using the [Maven cache action](../actions/maven-cache/action.yml).
The seed includes the Scala compiler bridge and standalone integration-test dependencies.
When no current seed is available, Verify can save a smaller cache on the PR.
Later commits on that PR reuse it; if dependencies change, the previous cache is a starting point.
Hosted Maven jobs use [public settings](../actions/maven-cache/settings.xml) for Central,
including transitive and plugin dependencies. Local builds are unchanged. To opt in for
one command, pass `-gs .github/actions/maven-cache/settings.xml` from the repo root.
Relevant pushes warm `main` and release branches; a daily check on `main` also
detects upstream SNAPSHOTs published without a repository change.

The key separates branch, platform, compiler JDK and Scala version. Changing only
the test JVM does not require warming Maven again. JNI, private and history-metrics
SNAPSHOTs track their own publications; fixed versions use their coordinates.
PRs can reuse their target branch's cache. A copied-branch workflow can pass
`base-branch` to compute the same key, but GitHub's cache visibility still applies:
a push on a sibling branch cannot read a release-only cache by naming it.
GPU workers testing a PR should restore the exact Maven key as the Verify matrix does,
and build normally on a miss. `premerge: true` is for the branch warmer: an exact hit
checks that the cache exists without downloading it. Do not save PR-built artifacts to a branch cache.

The same warmer seeds the standalone [Apache Spark distributions](../actions/spark-archive-cache/releases.json).
Future GHA premerge workers can use the [archive action](../actions/spark-archive-cache/action.yml)
to restore a checksum-verified tarball by Scala version. Extract its `path` output
instead of downloading Spark again. When a standalone lane changes Spark version,
update its archive version and checksum together. The warmer does not cache locally
built plugin JARs, test outputs or native objects.
