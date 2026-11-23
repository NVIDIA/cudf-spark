#!/bin/bash
#
# Copyright (c) 2022-2023, NVIDIA CORPORATION. All rights reserved.
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
# Run integration testing individually by setting environment variable:
#   TEST=xxx
# or
#   TEST_TAGS=xxx
# More details please refer to './integration_tests/run_pyspark_from_build.sh'.
# Note, 'setup.sh' should be executed first to setup proper environment.
#
# This file runs pytests with Jenkins parallel jobs.

set -xe

# 'setup.sh' already be executed before running this script
db_script_path="$( cd "$(dirname "$0")" >/dev/null 2>&1 ; pwd -P )"
# Init common variables like SPARK_HOME, spark configs
source $db_script_path/common_vars.sh

# Disable parallel test as multiple tests would be executed by leveraging external parallelism, e.g. Jenkins parallelism
export TEST_PARALLEL=${TEST_PARALLEL:-0}

set +e
# Run integration testing
./integration_tests/run_pyspark_from_build.sh --runtime_env='databricks' --test_type=$TEST_TYPE
ret=$?
set -e
if [ "$ret" = 5 ]; then
  # avoid exit script w/ code 5 when the cases are skipped in specific test
  echo "Suppress Exit code 5: No tests were collected"
  exit 0
fi
exit "$ret"
