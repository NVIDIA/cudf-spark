#!/usr/bin/env bash
# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
set -euo pipefail

candidate=${1:?candidate checkout}
base=${2:?base SHA}
merge=${3:?merge SHA}
if [[ ! $base =~ ^[0-9a-f]{40}$ || ! $merge =~ ^[0-9a-f]{40}$ || $(git -C "$candidate" rev-parse HEAD) != "$merge" ]]; then
  echo "GPU selection does not match the candidate checkout" >&2
  exit 1
fi
control=$(cd "$(dirname "$0")" && pwd)
catalog="$candidate/.github/gpu-ci-suites.json"
selector="${GITHUB_WORKSPACE:?}/common/workflow-suite-selector/index.js"
# Use both paths of a rename; only read candidate files, never load candidate modules.
if suites=$(git -C "$candidate" diff --no-renames --no-ext-diff --no-textconv --ignore-submodules=none \
    --name-only -z "$base" "$merge" -- | node -e '
      const fs = require("node:fs"), select = require(process.argv[1]).selectSuites;
      const paths = fs.readFileSync(0, "utf8").split("\0").filter(Boolean);
      console.log(JSON.stringify(select(paths.map(filename => ({filename})), JSON.parse(fs.readFileSync(process.argv[2])))));
    ' "$selector" "$catalog"); then
  set -- --base "$base" --merge "$merge"
else
  echo "Could not establish changed-file scope; selecting all enabled GPU suites" >&2
  suites=$(jq -ce '[.suites | to_entries[] | select(.value.enabled != false) | .key]' "$catalog")
  set --
fi
if [[ $suites == '[]' ]]; then
  echo '{"include":[]}'
else
  python3 -I "$control/groups.py" "$suites" --repo "$candidate" "$@"
fi
