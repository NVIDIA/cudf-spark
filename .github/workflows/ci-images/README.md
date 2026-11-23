# CI images

The **CPU CI** workflow builds and smoke-tests Docker images affected by a pull
request. Its **CPU CI gate** requires successful selection and every selected
build. A Markdown-only PR passes after selection. Add `[skip ci]` to a PR title
to skip image builds and pass the gate. Title and description edits do not start
CPU CI. Choose **Re-run all jobs** on the latest commit to apply the current title.

Follow the [image maintenance guide](AGENTS.md) when changing a recipe or workflow.
Register each Dockerfile in [suites.json](suites.json). List local `COPY` and
`ADD` inputs in `recipe-inputs` so changes to them trigger the build and update
its fingerprint. Changing a shuffle Dockerfile selects just that example.
The shuffle examples and the UDF template run only on pull requests.
Toolchain PR builds reuse image caches written by the branch publishers.
The examples and template cache their final image layers for use within the same PR.

## Published toolchains

The **Docker image CI** workflow publishes to `ghcr.io/nvidia/cudf-spark/`
on main and release branch pushes and refreshes main on a schedule.
See the `ci-toolchains` entries in [suites.json](suites.json) for image names,
platforms, and build arguments. Each image is smoke-tested before its tag moves.

Choose the package by job, not by CUDA classifier. Pull from
`ghcr.io/nvidia/cudf-spark/` with these package and tag names:

| Job | Package and tag |
| --- | --- |
| Build the plugin or run its Maven unit tests | `spark-build:<line>-ubuntu22.04` |
| Run integration tests on Ubuntu | `integration-ubuntu:<line>-ubuntu22.04` or `<line>-ubuntu24.04` |
| Run integration tests on Rocky | `integration-rocky:<line>-rockylinux8` or `<line>-rockylinux9` |
| Build or test Skills | `skills-build:<line>` (AMD64 only) |

Use `main` for a main-line job and `YY.MM` for a release-line job, even when
checking out a commit SHA. Main seeds versioned Spark and integration tags until
the matching release branch exists; after branch-out that branch owns them.
For example, both AMD64 and ARM64 workers use
`docker pull ghcr.io/nvidia/cudf-spark/integration-ubuntu:main-ubuntu22.04`.
A registry mirror must preserve the multi-platform index, not just one worker's image.

To select an alternate JDK, set `JAVA_HOME` and put its `bin` directory first on
`PATH`.

New GHCR packages may need public read access enabled for anonymous verification.
To retry multi-platform publication, dispatch a fresh **Docker image CI** run
on the target branch. The [cleanup workflow](../ci-image-cleanup.yml) defines
retention and deletion limits and preserves published tags and their platform
manifests. Manual cleanup defaults to a dry run.
