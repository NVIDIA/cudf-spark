#!/usr/bin/env bash
#
# Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
submodule=thirdparty/spark-rapids-common
generator="$repo_root/$submodule/spark-home-deps/gen-spark-home-deps.py"

# A CI job may copy prepared source to a node without its Git metadata.
if [[ ! -e "$repo_root/.git" ]]; then
    if [[ -f "$generator" ]]; then
        exit 0
    fi
    echo "Missing common tooling in a source copy without Git metadata" >&2
    exit 1
fi

pinned=$(git -C "$repo_root" ls-files --stage -- "$submodule" | awk '$1 == "160000" { print $2 }')
if [[ -z "$pinned" ]]; then
    echo "No pinned common submodule in this checkout" >&2
    exit 1
fi

current=$(git -C "$repo_root/$submodule" rev-parse HEAD 2>/dev/null || true)
if [[ "$current" != "$pinned" || ! -f "$generator" ]]; then
    git -C "$repo_root" submodule update --init -- "$submodule"
fi

if [[ ! -f "$generator" ]]; then
    echo "Missing generator in pinned common submodule: $generator" >&2
    exit 1
fi
