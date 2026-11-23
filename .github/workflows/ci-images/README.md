# CI images

The **CPU CI** workflow builds and smoke-tests Docker images affected by a pull
request. Its **CPU CI gate** requires successful selection and every selected
build. A Markdown-only PR passes after selection. Add `[skip ci]` to a PR title
to skip image builds and pass the gate. Title and description edits do not start
CPU CI. Choose **Re-run all jobs** to apply the current title on the same commit.

Register each Dockerfile in [suites.json](suites.json). List local `COPY` and
`ADD` inputs in `recipe-inputs` so changes to them trigger the build and update
its fingerprint. Changing a shuffle Dockerfile selects just that example.
The four shuffle examples and the UDF template run only on pull requests.

## Published toolchains

The **Docker image CI** workflow publishes to `ghcr.io/nvidia/cudf-spark/`
on main and release branch pushes. Main also refreshes daily. Each image
is smoke-tested before its tag moves.

| Package | Platforms | Tag after the branch or release prefix |
| --- | --- | --- |
| `spark-build` | AMD64 and ARM64, one index | `-cuda12.0.1-ubuntu22.04` |
| `integration-ubuntu` | AMD64 | `-cuda12.8.0-ubuntu22.04`, `-cuda12.8.0-ubuntu24.04` |
| `integration-rocky` | AMD64 | `-cuda12.8.0-rockylinux8`, `-cuda12.8.0-rockylinux9` |
| `skills-build` | AMD64 | No suffix |

For example, main publishes `integration-ubuntu:main-cuda12.8.0-ubuntu24.04`.
Before the matching release branch exists, the publisher also gives that
digest the release prefix from the root POM, such as `26.12`. Once
`release/26.12` exists, it owns the `26.12` tags. The next main POM version
starts the next line automatically. Skills uses the branch prefix alone.

The Spark and integration images include an opt-in JDK 21. Set `JAVA_HOME`
and use its `bin` directory for Java 21 jobs.

New GHCR packages may need public read access enabled for anonymous verification.
To retry multi-platform publication, dispatch a fresh **Docker image CI** run
on the target branch. Daily cleanup keeps stable tags and their platform
manifests and removes up to 20 temporary or unreferenced versions per package
after seven days. Manual cleanup defaults to a dry run.
