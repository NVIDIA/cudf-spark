#!/usr/bin/env bash
# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail
case "${1:?expected verify or warm}" in
    verify)
        # A cancelled extractor may still write to the old directory. Leave it out of the build.
        repository=$(mktemp -d "${RUNNER_TEMP:?}/maven-cold.XXXXXXXX")
        echo "::warning::Maven cache unavailable; continuing with a clean local repository."
        printf 'MAVEN_OPTS=%s -Dmaven.repo.local=%s\nMAVEN_REPOSITORY=%s\n' \
            "${MAVEN_OPTS:-}" "$repository" "$repository" >> "${GITHUB_ENV:?}"
        ;;
    warm)
        # The restore has returned. Only the disposable producer rewrites the path it will save.
        if [[ ${RUNNER_ENVIRONMENT:-} == github-hosted && -n ${HOME:-} && $HOME != / &&
              -d "$HOME/.m2/repository" && ! -L "$HOME/.m2" ]]; then
            rm -rf -- "$HOME/.m2/repository"
        fi
        ;;
    *) echo "Unknown cache recovery mode: $1" >&2; exit 2 ;;
esac
