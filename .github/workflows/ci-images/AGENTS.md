# Maintaining Docker image CI

| Change | Related configuration |
| --- | --- |
| Dockerfile or build context | Keep one entry per Dockerfile in [suites.json](suites.json). Local `COPY` and `ADD` sources belong in `recipe-inputs`: they trigger rebuilds and affect the image fingerprint. |
| OS, architecture, or toolchain | Update the recipe's `variants`, platform arguments, or [smoke test](smoke) as applicable. |
| Trigger paths | [paths.json](paths.json) holds shared validation, publication, and unit-test triggers. Per-image inputs stay in [suites.json](suites.json). |
| Tag format | Keep the [selector](select.py), [platform promotion](promote-platforms.sh), [tag publisher](publish-tag.sh), [tests](tests), and [consumer guide](README.md#published-toolchains) in sync. |

Only `ci-toolchains` entries are published and eligible for [cleanup](../ci-image-cleanup.yml).
Keep the publisher's manual `image` choices in sync with those entries.
The [publisher](../docker-images.yml) and cleanup must use the same per-package concurrency group.
