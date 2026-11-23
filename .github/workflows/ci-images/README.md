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

Spark and integration tags include the branch or release prefix, CUDA version,
and OS variant. Main seeds the release tag from the root POM until the matching
release branch exists. After the branch is cut, it owns that release tag.
Skills uses the branch prefix alone.

To select an alternate JDK, set `JAVA_HOME` and put its `bin` directory first on
`PATH`.

New GHCR packages may need public read access enabled for anonymous verification.
To retry multi-platform publication, dispatch a fresh **Docker image CI** run
on the target branch. The [cleanup workflow](../ci-image-cleanup.yml) defines
retention and deletion limits and preserves published tags and their platform
manifests. Manual cleanup defaults to a dry run.
