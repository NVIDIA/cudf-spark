# GitHub Actions workflows

## CPU CI

[CPU CI](cpu-ci.yml) runs the checks affected by a PR to `main` or
`release/YY.MM`. The gate passes when selection and all selected checks pass.
Currently it checks [Docker images](ci-images/README.md). Markdown-only PRs
and PRs titled with `[skip ci]` pass without building images.

## Premerge caches

[Maven Verify](mvn-verify-check.yml) and the [branch warmer](mvn-verify-cache.yml)
use the same [Maven cache action](mvn-verify-check/action.yml). A branch seed
includes the Scala compiler bridge and standalone integration-test dependencies.
On a cold PR, Verify can prepare a smaller cache without those extra downloads.
Relevant pushes warm `main` and release branches; a daily check on `main` also
detects upstream SNAPSHOTs published without a repository change.

The key separates branch, platform, JDK and Scala version. JNI, private and history-metrics SNAPSHOTs
track the published classifier versions; fixed versions use their coordinates.
PRs can reuse their target branch's cache. A copied-branch workflow can pass
`base-branch` to compute the same key, but GitHub's cache visibility still applies:
a push on a sibling branch cannot read a release-only cache by naming it.
Workers dispatched on a trusted branch to test PR code should use restore-only,
as the Verify matrix does; do not save PR-produced data into a branch cache.

The same warmer seeds the two standalone [Apache Spark distributions](premerge-spark-cache/releases.json).
Future GHA premerge workers can use the [archive action](premerge-spark-cache/action.yml)
to restore a checksum-verified tarball by Scala version. It is independent of Maven
pins. When a standalone lane changes Spark version, update its archive version and
checksum together. The warmer does not cache locally built plugin JARs, test outputs or native objects.
