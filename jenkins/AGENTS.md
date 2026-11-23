# Jenkins Dockerfiles

When changing a Dockerfile in this directory, follow the
[Docker image CI guide](../.github/workflows/ci-images/AGENTS.md) for catalog
entries, build inputs, and smoke tests. Keep build arguments compatible with
both Jenkins and GitHub Actions, including the Maven download URL overrides.
