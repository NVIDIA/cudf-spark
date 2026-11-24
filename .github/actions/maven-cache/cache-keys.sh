#!/usr/bin/env bash
# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

scope=$BRANCH
if [[ $scope == release/[0-9][0-9].[0-9][0-9] ]]; then
    scope=${scope#release/}
fi
prefix="spark-maven-${RUNNER_OS}-${RUNNER_ARCH}-${SCALA_VER}-${scope}-"
snapshot_prefix="${prefix}${1}-"
stem="${snapshot_prefix}${INPUT_HASH}"
spark=$(jq -er --arg scala "$SCALA_VER" '.[$scala].version' .github/actions/spark-archive-cache/releases.json)
printf 'prefix=%s\nsnapshot-prefix=%s\nstem=%s\nfull=%s\n' \
    "$prefix" "$snapshot_prefix" "$stem" "$stem-premerge-$spark-$PREMERGE_HASH"
