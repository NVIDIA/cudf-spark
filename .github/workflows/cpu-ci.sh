#!/usr/bin/env bash
# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# Small operations used by the CPU selector and the reusable Maven result job.
set -euo pipefail

case "${1:?missing CPU CI operation}" in
  db-tests)
    config=.github/workflows/databricks-ci/paths.json
    paths=$(jq -er '.helper_tests | select(type == "array" and length > 0 and
      all(.[]; type == "string" and length > 0 and (test("[\r\n]") | not))) | .[]' "$config") ||
      { echo "::error::Invalid helper_tests path list in $config"; exit 1; }
    pathspecs=()
    while IFS= read -r path; do pathspecs+=("$path"); done <<< "$paths"
    if git --literal-pathspecs diff --quiet HEAD^1 HEAD -- "${pathspecs[@]}"; then
      exit 0
    fi
    python3 -B -m unittest discover -s .github/workflows/databricks-ci
    python3 -B -m unittest discover -s jenkins/databricks/tests -p '*test*.py'
    ;;
  required)
    jobs=$(jq -cne --argjson images "$VALIDATE" --arg maven "$MAVEN" '
      if ($images | type) != "array" or (["full","rat","skip"] | index($maven)) == null
      then error("Invalid CPU selection")
      else ["select", (if ($images | length) > 0 then "validate" else empty end),
           (if $maven == "full" then "maven" else empty end)] end')
    echo "jobs=$jobs" >> "$GITHUB_OUTPUT"
    ;;
  maven-contexts)
    # These are the original planned matrices, including successful siblings of a retried job.
    contexts=$(jq -cn --argjson a "$PACKAGES_212" --argjson b "$PACKAGES_213" --argjson c "$VERIFY_213" '
      [($a.include[] | select(.isSnapshot == false) | "package-tests (\(."spark-version"), false)"),
       ($b.include[] | select(.isSnapshot == false) | "package-tests-scala213 (\(."spark-version"), false)"),
       ($c.include[] | "verify-213-modules (\(."spark-version"), \(."java-version"))")]')
    echo "contexts=$contexts" >> "$GITHUB_OUTPUT"
    ;;
  *)
    echo "Unknown CPU CI operation: $1" >&2
    exit 1
    ;;
esac
