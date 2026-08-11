#!/bin/bash
#
# Copyright (c) 2025, NVIDIA CORPORATION.
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

# Common shuffle test utilities shared between spark-premerge-build.sh and spark-tests.sh

# Get the shuffle shim name for a given Spark version
# Arguments:
#   $1 - Spark version (e.g., "3.3.0", "4.0.1"). Defaults to $SPARK_VER if not provided.
# Output:
#   Prints the shim name (e.g., "spark330", "spark401")
# Note: For non-Apache Spark flavors (databricks, cloudera, etc.), the version string
#       may need special handling - set SHUFFLE_SPARK_SHIM explicitly in those cases.
get_shuffle_shim() {
    local spark_ver="${1:-$SPARK_VER}"
    local shim="spark${spark_ver//./}"
    # Remove -SNAPSHOT suffix if present
    echo "${shim//\-SNAPSHOT/}"
}

# Run shuffle integration test
# Arguments:
#   $1 - shuffle mode: "UCX" or "MULTITHREADED"
#   $2 - path to run_pyspark_from_build.sh (e.g., "./run_pyspark_from_build.sh" or "./integration_tests/run_pyspark_from_build.sh")
#   $3 - (optional) "premerge" to use premerge-specific configs (memory limits, TEST_PARALLEL=0, etc.)
#   $4 - (optional) Spark version for shuffle shim. Defaults to $SPARK_VER.
invoke_shuffle_integration_test() {
    local shuffle_mode="$1"
    local pyspark_script="${2:-./run_pyspark_from_build.sh}"
    local run_mode="${3:-}"
    local spark_ver="${4:-$SPARK_VER}"

    # Get the shuffle shim for this Spark version
    local shuffle_shim=$(get_shuffle_shim "$spark_ver")

    # check out what else is on the GPU
    nvidia-smi

    # Build environment variables for the command
    local env_vars=""

    # Premerge-specific configs
    if [[ "$run_mode" == "premerge" ]]; then
        env_vars+="TEST_PARALLEL=0 "
        env_vars+="PYSP_TEST_spark_master=$SPARK_MASTER "
        env_vars+="PYSP_TEST_spark_cores_max=2 "
        env_vars+="PYSP_TEST_spark_executor_cores=1 "
        env_vars+="PYSP_TEST_spark_rapids_memory_gpu_minAllocFraction=0 "
        env_vars+="PYSP_TEST_spark_rapids_memory_gpu_maxAllocFraction=0.1 "
        env_vars+="PYSP_TEST_spark_rapids_memory_gpu_allocFraction=0.1 "
    else
        env_vars+="SPARK_SUBMIT_FLAGS=\"$SPARK_CONF\" "
    fi

    # Common shuffle manager
    env_vars+="PYSP_TEST_spark_shuffle_manager=com.nvidia.spark.rapids.$shuffle_shim.RapidsShuffleManager "

    # Mode-specific configs
    if [[ "$shuffle_mode" == "UCX" ]]; then
        # The UCX_TLS=^posix config removes posix from the list of memory transports
        # so that IPC regions are obtained using SysV API instead. This was done because of
        # intermittent test failures. See: https://github.com/NVIDIA/spark-rapids/issues/6572
        env_vars+="PYSP_TEST_spark_rapids_shuffle_mode=UCX "
        env_vars+="PYSP_TEST_spark_executorEnv_UCX_ERROR_SIGNALS=\"\" "
        env_vars+="PYSP_TEST_spark_executorEnv_UCX_TLS=\"^posix\" "
    elif [[ "$shuffle_mode" == "MULTITHREADED" ]]; then
        env_vars+="PYSP_TEST_spark_rapids_shuffle_mode=MULTITHREADED "
        env_vars+="PYSP_TEST_spark_rapids_shuffle_multiThreaded_writer_threads=2 "
        env_vars+="PYSP_TEST_spark_rapids_shuffle_multiThreaded_reader_threads=2 "
    else
        echo "ERROR: Unknown shuffle mode: $shuffle_mode (expected UCX or MULTITHREADED)"
        return 1
    fi

    # Execute the test
    eval "$env_vars $pyspark_script -m shuffle_test"
}
