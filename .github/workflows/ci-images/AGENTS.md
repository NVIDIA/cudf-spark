# Maintaining Docker image CI

| When you change | What to update |
| --- | --- |
| A Dockerfile, build context, or local `COPY` or `ADD` input | Keep one entry per Dockerfile in [suites.json](suites.json). List each copied repository file in `recipe-inputs` so it triggers a rebuild and changes the image fingerprint. |
| An OS, architecture, or installed tool | Keep the catalog arguments and platforms aligned with the Dockerfile. Update the matching [smoke test](smoke) to exercise the tool offline. Preserve the defaults used by existing jobs. |
| Files that trigger CI | Edit shared trigger paths and unit-test includes/excludes in [paths.json](paths.json). Keep image-specific inputs in [suites.json](suites.json). |
| Selection logic or tag formats | Update [select.py](select.py) and its [tests](tests). For a tag change, check the formats accepted by [platform promotion](promote-platforms.sh) and [tag publication](publish-tag.sh). Document the tag format in the [README](README.md). |

The workflows read the catalog. Add a new OS to the existing Dockerfile's `variants` list.
Change the [CPU gate](../cpu-ci.yml), [builder](../build-docker-image.yml),
[publisher](../docker-images.yml), or [cleanup](../ci-image-cleanup.yml) when
that workflow's behavior changes. Start each workflow and script with a one-line
summary of its purpose. Link to configuration for versions, platforms, and limits.

PRs validate images. Branch publishers own
GHCR tags, and publication and cleanup share a per-package lock.

Use synthetic release lines in test fixtures so a project version bump needs no test edits.
From the repository root, run `python3 -m unittest discover -s .github/workflows/ci-images/tests -v`.
Check the selected variants and smoke results in **CPU CI**. Before switching
a consumer, verify its tag and platform in the first post-merge publication.

Title edits do not start CPU CI. Choose **Re-run all jobs** on the latest
commit to apply the current title. After changing the target branch, push a
commit or reopen the PR to validate against the new base.
