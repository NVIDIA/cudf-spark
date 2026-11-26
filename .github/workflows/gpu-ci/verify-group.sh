#!/usr/bin/env bash
# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail
pinned=$(git rev-parse HEAD:thirdparty/parquet-testing)
if [[ "$(git -C thirdparty/parquet-testing rev-parse HEAD)" != "$pinned" ]]; then
  echo "::error::Parquet test data checkout does not match the pinned submodule."
  exit 1
fi
compgen -G 'thirdparty/parquet-testing/data/*.parquet' >/dev/null

suites=$(jq -cn '[env.KIND]')
row=$(jq -ce '
  if .kind == "unit" then {kind, group, scala, shims}
  elif .kind == "integration" then {kind, group, scala, shim, shard}
  else error("Unknown GPU workload kind") end | .arch = env.GPU_ARCH' <<< "$WORKLOAD")
if ! python3 .github/workflows/gpu-ci/groups.py "$suites" | jq -e --argjson row "$row" '
  any(.include[]; ({arch: "amd64"} + .) == $row)' >/dev/null; then
  echo "::error::Dispatched group does not match the $GPU_ARCH merge catalog or the catalog could not be read."
  exit 1
fi
if jq -e 'has("targets")' <<< "$WORKLOAD" >/dev/null; then
  targets=$(jq -ce 'if (.targets | type == "array" and length > 0 and all(.[]; type == "string"))
    then .targets else error("Expected nonempty suite or case targets") end' <<< "$WORKLOAD")
  if ! python3 .github/workflows/gpu-ci/groups.py "$suites" --base "$BASE_SHA" --merge "$MERGE_SHA" |
    jq -e --argjson row "$row" --argjson targets "$targets" '
      any(.include[]; . == ($row + {targets: $targets}))' >/dev/null; then
    echo "::error::Could not verify targets for $GROUP against $BASE_SHA..$MERGE_SHA."
    exit 1
  fi
  printf 'targets=%s\n' "$targets" >> "$GITHUB_OUTPUT"
fi
