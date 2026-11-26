#!/usr/bin/env bash
# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
set -euo pipefail

revisions=(--base "$BASE_SHA" --merge "$MERGE_SHA")
if [[ $SELECT_OUTCOME != success || -z $SUITES ]]; then
  echo "::warning::Could not establish changed-file scope; running all configured GPU suites"
  SUITES=$(jq -ce '[.suites | to_entries[] | select(.value.enabled != false) | .key] | select(length > 0)' .github/gpu-ci-suites.json)
  revisions=()
fi
if [[ $SUITES == '[]' ]]; then
  # A docs-only result must belong to the original merge, even if the base moved.
  gh api "repos/$GITHUB_REPOSITORY/pulls/${GITHUB_REF_NAME#pull-request/}" |
    jq -e '.state == "open" and .head.sha == env.GITHUB_SHA and .base.ref == env.BASE and
      .base.sha == env.BASE_SHA and .merge_commit_sha == env.MERGE_SHA' >/dev/null ||
    { echo "::error::Cannot confirm the current PR merge for the no-test decision"; exit 1; }
  exit 0
fi
matrix=$(python3 .github/workflows/gpu-ci/groups.py "$SUITES" ${revisions[@]+"${revisions[@]}"})
jq -er '.include[] | .group + " (" + .arch + "): " + ((.targets // ["full group"]) | join(", "))' \
  <<< "$matrix" >> "$GITHUB_STEP_SUMMARY"
echo "matrix=$matrix" >> "$GITHUB_OUTPUT"
for scala in 2.12 2.13; do
  mode=$(jq -er --arg scala "$scala" '[.include[] | select(.scala == $scala and .arch == "amd64") | .kind]
    | if length == 0 then "none" elif index("integration") then "integration" else "unit" end' <<< "$matrix")
  echo "cache${scala/./}=$mode" >> "$GITHUB_OUTPUT"
done
