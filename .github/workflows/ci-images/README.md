# CI images

For PR triggers, skips, and gate behavior, see [CPU CI](../README.md#cpu-ci).

Follow the [image maintenance guide](AGENTS.md) when changing a recipe or workflow.
Register each Dockerfile in [suites.json](suites.json). List local `COPY` and
`ADD` inputs in `recipe-inputs` so changes to them trigger the build and update
its fingerprint. Changing a shuffle Dockerfile selects just that example.
The shuffle examples and the UDF template run only on pull requests.
Toolchain PR builds reuse image caches written by the branch publishers.
The examples and template cache their final image layers for use within the same PR.

## Published toolchains

The **Docker image CI** workflow publishes to `ghcr.io/nvidia/cudf-spark/`
on relevant main and release branch pushes. Scheduled builds refresh main at
02:27 UTC on Tuesday, Thursday, and Saturday. Manual runs on either branch
can publish all toolchains (the default) or one image family and all its variants.
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
Skills gets its versioned tag from the release publisher, not before branch-out.
For example, both AMD64 and ARM64 workers use
`docker pull ghcr.io/nvidia/cudf-spark/integration-ubuntu:main-ubuntu22.04`.
A registry mirror must preserve the multi-platform index, not just one worker's image.

For CI jobs that verify images against the source checkout, use the same recipe
helper as the publisher. Check the bootstrap job's Python version with
`python3 --version`, then run the selector from the clean source checkout.
This mode does not need Git or registry credentials:

```sh
CI_IMAGE_METADATA_HELPER=/path/to/spark-rapids-common/container-image/metadata.py \
  python3 .github/workflows/ci-images/select.py --resolve \
    spark-build/ubuntu22.04/amd64 integration-ubuntu/ubuntu22.04/arm64
```

Resolve in a bootstrap job before allocating the build or test workers. The
JSON map gives the image, platform, expected recipe version, and tags for each
request. The default is the versioned tag from the checked-out POM, including
on a detached commit. Development jobs can pass `--allow-main-fallback` and
enable fallback in their registry resolver to try the matching main tag second.
Use the digest returned after the platform and recipe label match.

For an approved PR, a worker can build the catalog recipe locally if the image
is missing or its fingerprint differs. The CPU validation image stays on its
runner. Registry access errors are not a reason to rebuild. Keep PR-controlled
code away from registry write and internal credentials. For nightly builds,
pass the source SHA and each platform's resolved digest to downstream jobs.

To select an alternate JDK, set `JAVA_HOME` and put its `bin` directory first on
`PATH`.

New GHCR packages may need public read access enabled for anonymous verification.
Use **Re-run failed jobs** to retry an image without rebuilding successful
images. For example, a failed Spark AMD64 build can reuse the same run's
verified ARM64 digest. If the records have expired or the tag has since moved
to a different image, dispatch **Docker image CI** for just that image family.
The [cleanup workflow](../ci-image-cleanup.yml) defines
retention and deletion limits and preserves published tags and their platform
manifests. Cleanup runs at 04:57 and 16:57 UTC daily; manual cleanup defaults
to a dry run.
