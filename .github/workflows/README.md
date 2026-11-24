# GitHub Actions workflows

## CPU CI

[CPU CI](cpu-ci.yml) runs the checks affected by a PR to `main` or
`release/YY.MM`. The gate passes when selection and all selected checks pass.
Currently it checks [Docker images](ci-images/README.md). Markdown-only PRs
and PRs titled with `[skip ci]` pass without building images.
