#!/usr/bin/env bash
# Copyright (c) 2026, NVIDIA CORPORATION.
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

set -euo pipefail

if [[ $# -ne 1 || -z ${SPARK_HOME:-} || ! -x "$SPARK_HOME/bin/spark-shell" ]]; then
  echo "Usage: SPARK_HOME=/path/to/spark $0 /path/to/rapids-4-spark.jar" >&2
  exit 2
fi

plugin_jar=$(realpath "$1")
if [[ ! -f "$plugin_jar" ]]; then
  echo "Distribution JAR not found: $plugin_jar" >&2
  exit 2
fi

test_dir=$(cd "$(dirname "$0")" && pwd)
run_log=$(mktemp)
trap 'rm -f "$run_log"' EXIT
printf ':load %s/spark-cache-root-safety.scala\n' "$test_dir" |
  "$SPARK_HOME/bin/spark-shell" --master 'local[2]' --driver-memory 6g \
    --jars "$plugin_jar" \
    --conf spark.sql.cache.serializer=com.nvidia.spark.ParquetCachedBatchSerializer \
    --conf spark.rapids.sql.enabled=false \
    --conf spark.sql.adaptive.enabled=false \
    --conf spark.ui.enabled=false | tee "$run_log"
grep -q 'ROOT_SAFETY_PASS:' "$run_log"
