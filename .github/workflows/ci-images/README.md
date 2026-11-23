# Docker image CI

[`suites.json`](suites.json) assigns every tracked Dockerfile to one build and smoke test.
Pull requests build only the recipes affected by the full PR diff. For example,
editing one shuffle Dockerfile builds that example only; editing
`integration_tests/requirements.txt` builds the three toolchains that copy it.
Markdown changes do not build images. Register new Dockerfiles in the catalog;
when adding a local `COPY` or `ADD`, list its source in `recipe-inputs` so a
later input-only change selects the right build and changes its recipe fingerprint.
The `CPU CI gate` passes when image selection succeeds and every selected build
and smoke test passes. For an unrelated change, only selection needs to succeed.

| Suite | On a pull request | On a matching branch push |
| --- | --- | --- |
| CI toolchains: Spark, Skills, Ubuntu and Rocky integration | Build and smoke-test | Build, smoke-test and publish |
| Shuffle examples: Ubuntu/Rocky, with/without RDMA | Build and smoke-test the changed examples | No build |
| UDF CUDA template | Build and exercise the template toolchain | No build |

The standard Ubuntu Spark image builds on native AMD64 and ARM64 runners; the
branch tag contains both smoke-tested platforms. Other recipes target AMD64.
The standard Spark and integration toolchains include JDK 21 for Spark 5.0
alongside their existing JDKs. Jobs opt in by setting `JAVA_HOME` and using
`$JAVA_HOME/bin`; the default JDK does not change.
Each recipe and architecture has its own best-effort Docker layer cache.
A trusted publish builds fresh and exports layers for later validation. Only CI
toolchains publish to `ghcr.io/nvidia/cudf-spark/<image>`: main owns `:main`
and `release/YY.MM` owns `:YY.MM`. Main refreshes daily; a new release branch
bootstraps its toolchains. Manual publication runs on the selected supported branch.
For a new GHCR package, an admin may need to enable public reads after the first
candidate is pushed. To retry multi-platform publication, start a fresh manual
run on the target branch: a rerun of an old workflow cannot promote its saved
platform digests over a newer tag.

Daily cleanup removes up to 20 old temporary `build-*` tags or unreferenced
versions per CI package after seven days. It preserves `:main`, release tags,
and the platform images they reference. Cleanup shares the publication lock;
manual cleanup defaults to a dry run.

These checks build images and run CPU-only smoke tests. They do not replace
GPU/RDMA tests, Maven Verify, or the existing Jenkins images.
