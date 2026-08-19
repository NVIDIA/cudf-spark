#!/bin/bash
#
# Copyright (c) 2023-2025, NVIDIA CORPORATION. All rights reserved.
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

# This script sets the environment to run cudf_udf tests of RAPIDS Accelerator for Apache Spark on DB.
# cudf python packages need to be installed in advance, please refer to
#   './jenkins/databricks/init_cudf_udf.sh' to install.
# All the environments can be overwritten by shell variables:
#   LOCAL_JAR_PATH: Location of the RAPIDS jars
#   SPARK_CONF: Spark configuration parameters

# Usage:
# - Running tests on Databricks:
#       `./jenkins/databricks/cudf-udf-test.sh`
# To add support of a new runtime:
#   1. Check if any more dependencies need to be added to the apt/conda/pip install commands.
#   2. If you had to go beyond the above steps to support the new runtime, then update the
#      instructions accordingly.
set -ex

# Try to use "cudf-udf" conda/pip environment for the python cudf-udf tests.
CUDF_PY_ENV=${CUDF_PY_ENV:-$(echo /databricks/*/envs/cudf-udf)}
if [ ! -d "${CUDF_PY_ENV}" ]; then
    echo "Error not found cudf-py packages! Please refer to './jenkins/databricks/init_cudf_udf.sh' to install."
    exit 255
fi
# Set the path of python site-packages.
PYTHON_SITE_PACKAGES=$(echo -n ${CUDF_PY_ENV}/*/lib/site-packages)
[ -d "${CUDF_PY_ENV}/bin" ] && export PATH=${CUDF_PY_ENV}/bin:$PATH

SOURCE_PATH="/home/ubuntu/spark-rapids"
[[ -d "$LOCAL_JAR_PATH" ]] && cd $LOCAL_JAR_PATH || cd $SOURCE_PATH

# 'init_cudf_udf.sh' already be executed to install required python packages
# Init common variables like SPARK_HOME, spark configs
source jenkins/databricks/common_vars.sh

sudo ln -sf /databricks/jars/ $SPARK_HOME/jars
sudo chmod -R 777 /databricks/data/logs/

CUDF_UDF_TEST_ARGS="--conf spark.python.daemon.module=rapids.daemon_databricks \
    --conf spark.rapids.memory.gpu.minAllocFraction=0 \
    --conf spark.rapids.memory.gpu.allocFraction=0.1 \
    --conf spark.rapids.python.memory.gpu.allocFraction=0.1 \
    --conf spark.rapids.python.concurrentPythonWorkers=2"

# Enable event log for qualification & profiling tools testing
export PYSP_TEST_spark_eventLog_enabled=true
mkdir -p /tmp/spark-events

CUDF_UDF_TEST_ARGS="$CUDF_UDF_TEST_ARGS --conf spark.executorEnv.PYTHONPATH=`ls $PWD/rapids-4-spark_*.jar | grep -v 'tests.jar'`"

SPARK_SUBMIT_FLAGS="$SPARK_CONF $CUDF_UDF_TEST_ARGS" TEST_PARALLEL=1 \
    bash integration_tests/run_pyspark_from_build.sh --runtime_env="databricks" -m "cudf_udf" --cudf_udf --test_type=$TEST_TYPE
