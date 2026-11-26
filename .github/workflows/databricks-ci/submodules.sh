#!/usr/bin/env bash
# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# Candidate files are data: never follow a PR-controlled submodule URL on the controller.
set -euo pipefail
candidate=${1:?expected candidate checkout}
git -C "$candidate" ls-tree -r HEAD | awk '$1 == "160000" {print $4}' |
  diff -u <(printf '%s\n' thirdparty/parquet-testing thirdparty/spark-rapids-common) -
for entry in thirdparty/parquet-testing thirdparty/spark-rapids-common; do
  case "$entry" in
    thirdparty/parquet-testing) url=https://github.com/apache/parquet-testing.git ;;
    thirdparty/spark-rapids-common) url=https://github.com/NVIDIA/spark-rapids-common.git ;;
  esac
  declared=$(git -C "$candidate" config --no-includes --blob HEAD:.gitmodules --get "submodule.$entry.url")
  [[ ${declared%.git} == "${url%.git}" ]]
  path="$candidate/$entry"
  [[ -d $path && ! -L $path && ! -L $candidate/thirdparty ]]
  pin=$(git -C "$candidate" rev-parse "HEAD:$entry")
  git -C "$path" init --quiet
  git -C "$path" -c protocol.allow=never -c protocol.https.allow=always fetch --quiet --no-tags --depth=1 "$url" "$pin"
  git -C "$path" checkout --quiet --detach FETCH_HEAD
  [[ $(git -C "$path" rev-parse HEAD) == "$pin" ]]
done
