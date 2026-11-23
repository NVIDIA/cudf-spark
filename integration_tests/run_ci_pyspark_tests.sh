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

set -eu
launcher="$(cd "$(dirname "$0")" && pwd)/run_pyspark_from_build.sh"
work_dir=$(mktemp -d)
trap 'rm -rf -- "$work_dir"' EXIT
export RAPIDS_TEST_UTF8_PENDING="$(cd "$work_dir" && pwd)/utf8-tests"
unset RAPIDS_TEST_UTF8_ONLY
# Both pytest processes must generate the same cases, data and OOM injection choices.
export SPARK_RAPIDS_TEST_DATAGEN_SEED=${SPARK_RAPIDS_TEST_DATAGEN_SEED:-${DATAGEN_SEED:-$(date +%s)}}
export SPARK_RAPIDS_TEST_INJECT_OOM_SEED=${SPARK_RAPIDS_TEST_INJECT_OOM_SEED:-$(date +%s)}

# Pytest signals whether any of the selected tests need a separate Spark application.
bash "$launcher" "$@"
if [[ ! -f "$RAPIDS_TEST_UTF8_PENDING" ]]; then
    exit 0
fi

# A standalone Worker's environment is independent of the test driver.
echo "Running selected Unicode path tests with a UTF-8 driver and executor locale"
LC_ALL=C.UTF-8 PYSP_TEST_spark_executorEnv_LC_ALL=C.UTF-8 RAPIDS_TEST_UTF8_ONLY=1 \
    bash "$launcher" "$@"
