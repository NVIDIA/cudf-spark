#!/bin/bash
# Copyright (c) 2020-2026, NVIDIA CORPORATION.
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

# Documentation generated using a generative AI tool
# ============================================================================
# run_pyspark_from_build.sh
# ============================================================================
#
# Description:
#   This script runs PySpark integration tests from a build environment.
#
# Usage:
#   To run this script, ensure you have Apache Spark installed and configured
#   properly, along with the necessary dependencies (e.g., findspark, xdist).
#
# Parameters and Environment Variables:
#   - SPARK_HOME: Path to your Apache Spark installation.
#   - SKIP_TESTS: If set to true, skips running the Python integration tests.
#   - INCLUDE_SPARK_AVRO_JAR: If set to true, includes Avro tests.
#   - TEST: Specifies a specific test to run.
#   - TEST_TAGS: Allows filtering tests based on tags.
#   - TEST_TYPE: Specifies the type of tests to run.
#   - LOCAL_JAR_PATH: Path to local jars if not building from source.
#   - PLUGIN_JAR: Path to a built spark-rapids plugin jar, the default points to the target directory
#   - INTEGRATION_TEST_VERSION_OVERRIDE: Overrides the auto-detected shim version.
#   - ICEBERG_EXTRA_CLASSPATH: Colon- or comma-separated local Iceberg jars to use with
#     spark.driver.extraClassPath and spark.executor.extraClassPath instead of --jars/--packages.
#
# Script Flow:
#   1. Setup and Checks: Validates environment and detects Spark/Scala versions.
#   2. Jars and Dependencies: Identifies necessary jars based on Spark version.
#   3. Test Configuration: Determines parallelism and checks for necessary tools.
#   4. Running Tests: Executes tests using runtests.py with specified options.
#
# Example Usage:
#   To run all tests, including Avro tests:
#     INCLUDE_SPARK_AVRO_JAR=true ./run_pyspark_from_build.sh
#
#   To run a specific test:
#     TEST=my_test ./run_pyspark_from_build.sh
#
# Troubleshooting:
#   - Ensure SPARK_HOME is correctly set.
#   - Check for missing dependencies like findspark or xdist.
#   - Verify that the necessary jars are correctly linked.
#
# ============================================================================

set -ex

SCRIPTPATH="$( cd "$(dirname "$0")" >/dev/null 2>&1 ; pwd -P )"
cd "$SCRIPTPATH"

# No failure message truncation in the test summary by default
# https://github.com/NVIDIA/spark-rapids/issues/12043
export CI=${CI:-true}

if [[ $( echo ${SKIP_TESTS} | tr '[:upper:]' '[:lower:]' ) == "true" ]];
then
    echo "PYTHON INTEGRATION TESTS SKIPPED..."
elif [[ -z "$SPARK_HOME" ]];
then
    >&2 echo "SPARK_HOME IS NOT SET CANNOT RUN PYTHON INTEGRATION TESTS..."
else
    echo "WILL RUN TESTS WITH SPARK_HOME: ${SPARK_HOME}"
    [[ ! -x "$(command -v zip)" ]] && { echo "fail to find zip command in $PATH"; exit 1; }
    PY4J_TMP=("${SPARK_HOME}"/python/lib/py4j-*-src.zip)
    PY4J_FILE=${PY4J_TMP[0]}
    # PySpark uses ".dev0" for "-SNAPSHOT" and either ".dev" for "preview" or ".devN" for "previewN"
    # https://github.com/apache/spark/blob/66f25e314032d562567620806057fcecc8b71f08/dev/create-release/release-build.sh#L267
    VERSION_STRING=$(PYTHONPATH=${SPARK_HOME}/python:${PY4J_FILE} python -c \
        "import pyspark, re; print(re.sub(r'\.dev[012]?$', '', pyspark.__version__))"
    )
    SCALA_VERSION=`$SPARK_HOME/bin/pyspark --version 2>&1| grep Scala | awk '{split($4,v,"."); printf "%s.%s", v[1], v[2]}'`

    [[ -z $VERSION_STRING ]] && { echo "Unable to detect the Spark version at $SPARK_HOME"; exit 1; }
    [[ -z $SCALA_VERSION ]] && { echo "Unable to detect the Scala version at $SPARK_HOME"; exit 1; }
    [[ -z $SPARK_SHIM_VER ]] && { SPARK_SHIM_VER="spark${VERSION_STRING//./}"; }

    echo "Detected Spark version $VERSION_STRING (shim version: $SPARK_SHIM_VER) (Scala version: $SCALA_VERSION)"

    INTEGRATION_TEST_VERSION=$SPARK_SHIM_VER

    if [[ -n $INTEGRATION_TEST_VERSION_OVERRIDE ]]; then
        # Override auto detected shim version in case of non-standard version string, e.g. `spark3113172702000-53`
        INTEGRATION_TEST_VERSION=$INTEGRATION_TEST_VERSION_OVERRIDE
    fi

    TARGET_DIR="$SCRIPTPATH"/target
    # support alternate local jars NOT building from the source code
    if [ -d "$LOCAL_JAR_PATH" ]; then
        AVRO_JARS=$(echo "$LOCAL_JAR_PATH"/spark-avro*.jar)
        PLUGIN_JAR=$(echo "$LOCAL_JAR_PATH"/rapids-4-spark_*.jar)
        if [ -f $(echo $LOCAL_JAR_PATH/parquet-hadoop*.jar) ]; then
            export INCLUDE_PARQUET_HADOOP_TEST_JAR=true
            PARQUET_HADOOP_TESTS=$(echo $LOCAL_JAR_PATH/parquet-hadoop*.jar)
            # remove the log4j.properties file so it doesn't conflict with ours, ignore errors
            # if it isn't present or already removed
            zip -d $PARQUET_HADOOP_TESTS log4j.properties || true
        else
            export INCLUDE_PARQUET_HADOOP_TEST_JAR=false
            PARQUET_HADOOP_TESTS=
        fi
        # the integration-test-spark3xx.jar, should not include the integration-test-spark3xxtest.jar
        TEST_JARS=$(echo "$LOCAL_JAR_PATH"/rapids-4-spark-integration-tests*-$INTEGRATION_TEST_VERSION.jar)
    else
        [[ "$SCALA_VERSION" != "2.12"  ]] && TARGET_DIR=${TARGET_DIR/integration_tests/scala$SCALA_VERSION\/integration_tests}
        AVRO_JARS=$(echo "$TARGET_DIR"/dependency/spark-avro*.jar)
        PARQUET_HADOOP_TESTS=$(echo "$TARGET_DIR"/dependency/parquet-hadoop*.jar)
        # remove the log4j.properties file so it doesn't conflict with ours, ignore errors
        # if it isn't present or already removed
        zip -d $PARQUET_HADOOP_TESTS log4j.properties || true
        MIN_PARQUET_JAR="$TARGET_DIR"/dependency/parquet-hadoop-1.12.0-tests.jar
        # Make sure we have Parquet version >= 1.12 in the dependency
        LOWEST_PARQUET_JAR=$(echo -e "$MIN_PARQUET_JAR\n$PARQUET_HADOOP_TESTS" | sort -V | head -1)
        export INCLUDE_PARQUET_HADOOP_TEST_JAR=$([[ "$LOWEST_PARQUET_JAR" == "$MIN_PARQUET_JAR" ]] && echo true || echo false)
        PLUGIN_JAR=${PLUGIN_JAR:-$(echo "$TARGET_DIR"/../../dist/target/rapids-4-spark_*.jar)}
        # the integration-test-spark3xx.jar, should not include the integration-test-spark3xxtest.jar
        TEST_JARS=$(echo "$TARGET_DIR"/rapids-4-spark-integration-tests*-$INTEGRATION_TEST_VERSION.jar)
    fi

    # `./run_pyspark_from_build.sh` runs all the tests excluding the avro tests in 'avro_test.py'.
    #
    # `INCLUDE_SPARK_AVRO_JAR=true ./run_pyspark_from_build.sh` runs all the tests, including the tests
    #                                                           in 'avro_test.py'.
    if [[ $( echo ${INCLUDE_SPARK_AVRO_JAR} | tr '[:upper:]' '[:lower:]' ) == "true" ]];
    then
        export INCLUDE_SPARK_AVRO_JAR=true
    else
        export INCLUDE_SPARK_AVRO_JAR=false
        AVRO_JARS=""
    fi

    # ALL_JARS includes dist.jar integration-test.jar avro.jar parquet.jar if they exist
    # Remove non-existing paths and canonicalize the paths including get rid of links and `..`
    ALL_JARS=$(readlink -e $PLUGIN_JAR $TEST_JARS $AVRO_JARS $PARQUET_HADOOP_TESTS || true)
    # `:` separated jars
    ALL_JARS="${ALL_JARS//$'\n'/:}"

    echo "AND PLUGIN JARS: $ALL_JARS"
    if [[ "${TEST}" != "" ]];
    then
        TEST_ARGS="-k $TEST"
    fi
    if [[ "${TEST_TAGS}" != "" ]];
    then
        TEST_TAGS="-m $TEST_TAGS"
    fi
    if [[ "${TEST_PARALLEL}" == "" ]];
    then
        # For integration tests we want to have at least
        #  - 1.5 GiB of GPU memory for the tests and 750 MiB for loading CUDF + CUDA.
        #    From profiling we saw that tests allocated under 200 MiB of GPU memory and
        #    1.5 GiB felt like it gave us plenty of room to grow.
        #  - 8 GiB of host memory. In testing with a limited number of tasks (4) we saw
        #    the amount of host memory not go above 5.5 GiB so 8 felt like a good number
        #    for future growth.
        #  - 1 CPU core
        # per Spark application. We reserve 2 GiB of GPU memory for general overhead also.

        # For now just assume that we are going to use the GPU on the system with the most
        # free memory. We use free memory to try and avoid issues if the GPU also is working
        # on graphics, which happens with many workstation GPUs. We also reserve 2 GiB for
        # CUDA/CUDF overhead which can be used because of JIT or launching large kernels.

        # If you need to increase the amount of GPU memory you need to change it here and
        # below where the processes are launched.
        GPU_MEM_PARALLEL=`nvidia-smi --query-gpu=memory.free --format=csv,noheader | awk '{if (MAX < $1){ MAX = $1}} END {print int((MAX - 2 * 1024) / ((1.5 * 1024) + 750))}'`
        CPU_CORES=`nproc`
        HOST_MEM_PARALLEL=$(awk '/MemAvailable/ {print int($2 / (8 * 1024 * 1024))}' /proc/meminfo)
        TMP_PARALLEL=$(( GPU_MEM_PARALLEL > CPU_CORES ? CPU_CORES : GPU_MEM_PARALLEL ))
        TMP_PARALLEL=$(( TMP_PARALLEL > HOST_MEM_PARALLEL ? HOST_MEM_PARALLEL : TMP_PARALLEL ))

        # Account for intra-Spark parallelism
        numGpuJVM=1
        if [[ "$NUM_LOCAL_EXECS" != "" ]]; then
            numGpuJVM=$NUM_LOCAL_EXECS
        elif [[ "$PYSP_TEST_spark_cores_max" != "" && "$PYSP_TEST_spark_executor_cores" != "" ]]; then
            numGpuJVM=$(( PYSP_TEST_spark_cores_max / PYSP_TEST_spark_executor_cores ))
        fi
        TMP_PARALLEL=$(( TMP_PARALLEL / numGpuJVM ))

        if  (( TMP_PARALLEL <= 1 )); then
            TEST_PARALLEL=1
        else
            TEST_PARALLEL=$TMP_PARALLEL
        fi

        echo "AUTO DETECTED PARALLELISM OF $TEST_PARALLEL"
    fi
    if python -c 'import findspark';
    then
        echo "FOUND findspark"
    else
        TEST_PARALLEL=0
        echo "findspark not installed cannot run tests in parallel"
    fi
    if python -c 'import xdist.plugin';
    then
        echo "FOUND xdist"
    else
        TEST_PARALLEL=0
        echo "xdist not installed cannot run tests in parallel"
    fi

    TEST_TYPE_PARAM=""
    if [[ "${TEST_TYPE}" != "" ]];
    then
        TEST_TYPE_PARAM="--test_type=$TEST_TYPE"
    fi

    # We found that when parallelism > 8, as it increases, the test speed will become slower and slower. So we set the default maximum parallelism to 8.
    # Note that MAX_PARALLEL varies with the hardware, OS, and test case. Please overwrite it with an appropriate value if needed.
    MAX_PARALLEL=${MAX_PARALLEL:-8}
    if [[ ${TEST_PARALLEL} -lt 2 ]];
    then
        # With xdist 0 and 1 are the same parallelism but
        # 0 is more efficient
        TEST_PARALLEL_OPTS=()
    elif [[ ${TEST_PARALLEL} -gt ${MAX_PARALLEL} ]]; then
        TEST_PARALLEL_OPTS=("-n" "$MAX_PARALLEL" "--dist" "worksteal")
    else
        TEST_PARALLEL_OPTS=("-n" "$TEST_PARALLEL" "--dist" "worksteal")
    fi

    mkdir -p "$TARGET_DIR"

    while true; do
      # to avoid hit spark bug https://issues.apache.org/jira/browse/SPARK-44242
      # do not dry-run to provide a safe directory name
      temp_rundir=$(mktemp -p "${TARGET_DIR}" -d "run_dir-$(date +%Y%m%d%H%M%S)-XXXX")
      if [[ ! "${temp_rundir}" =~ [xX][mM][xXsS] ]]; then
        echo "run_dir: ${temp_rundir}"
        break
      fi
      echo "invalid ${temp_rundir}, regenerating..."
      rmdir "$temp_rundir"
    done

    RUN_DIR=${RUN_DIR-"${temp_rundir}"}
    mkdir -p "$RUN_DIR"
    cd "$RUN_DIR"

    ## Under cloud environment, overwrite the '--rootdir' param to point to the working directory of each excutor
    LOCAL_ROOTDIR=${LOCAL_ROOTDIR:-"$SCRIPTPATH"}
    ## Under cloud environment, overwrite the '--std_input_path' param to point to the distributed file path
    INPUT_PATH=${INPUT_PATH:-"$SCRIPTPATH"}

    RUN_TESTS_COMMAND=(
        "$SCRIPTPATH"/runtests.py
        --rootdir "$LOCAL_ROOTDIR"
    )
    if [[ "${TESTS}" == "" ]]; then
        RUN_TESTS_COMMAND+=("${LOCAL_ROOTDIR}/src/main/python")
    else
        read -ra RAW_TESTS <<< "${TESTS}"
        for raw_test in "${RAW_TESTS[@]}"; do
            RUN_TESTS_COMMAND+=("${LOCAL_ROOTDIR}/src/main/python/${raw_test}")
        done
    fi

    REPORT_CHARS=${REPORT_CHARS:="fE"} # default as (f)ailed, (E)rror
    STD_INPUT_PATH="$INPUT_PATH"/src/test/resources
    TEST_COMMON_OPTS=(-v
          -r"$REPORT_CHARS"
          "$TEST_TAGS"
          --std_input_path="$STD_INPUT_PATH"
          --color=yes
          "$TEST_TYPE_PARAM"
          "$TEST_ARGS"
          "$RUN_TEST_PARAMS"
          --junitxml=TEST-pytest-`date +%s%N`.xml
          "$@")

    NUM_LOCAL_EXECS=${NUM_LOCAL_EXECS:-0}
    MB_PER_EXEC=${MB_PER_EXEC:-1536}
    CORES_PER_EXEC=${CORES_PER_EXEC:-1}

    SPARK_TASK_MAXFAILURES=${SPARK_TASK_MAXFAILURES:-1}

    ICEBERG_EXTRA_CLASSPATH_COMMA="${ICEBERG_EXTRA_CLASSPATH//:/,}"
    if [[ -n "${ICEBERG_EXTRA_CLASSPATH_COMMA}" ]]; then
        if [[ -n "${PYSP_TEST_spark_jars_packages}" ]]; then
            >&2 echo "ICEBERG_EXTRA_CLASSPATH cannot be used with PYSP_TEST_spark_jars_packages."
            >&2 echo "Use local Iceberg jar paths in ICEBERG_EXTRA_CLASSPATH instead of Maven coordinates."
            exit 1
        fi
        if [[ -n "${PYSP_TEST_spark_jars}" ]]; then
            PYSP_TEST_spark_jars="${PYSP_TEST_spark_jars},${ICEBERG_EXTRA_CLASSPATH_COMMA}"
        else
            PYSP_TEST_spark_jars="${ICEBERG_EXTRA_CLASSPATH_COMMA}"
        fi
    fi

    if [[ "${PYSP_TEST_spark_shuffle_manager}" =~ "RapidsShuffleManager" ||
          -n "${ICEBERG_EXTRA_CLASSPATH_COMMA}" ]]; then
        # The RAPIDS shuffle manager and Iceberg package-private access tests need the plugin
        # and dependency jars on extraClassPath instead of spark.jars/spark.jars.packages.
        EXTRA_CLASSPATH="${ALL_JARS}"
        if [[ -n "${PYSP_TEST_spark_jars}" ]]; then
            EXTRA_CLASSPATH="${EXTRA_CLASSPATH}:${PYSP_TEST_spark_jars//,/:}"
        fi
        if [[ -n "${PYSP_TEST_spark_driver_extraClassPath}" ]]; then
            EXTRA_CLASSPATH="${PYSP_TEST_spark_driver_extraClassPath}:${EXTRA_CLASSPATH}"
        fi
        EXECUTOR_EXTRA_CLASSPATH="${EXTRA_CLASSPATH}"
        if [[ -n "${PYSP_TEST_spark_executor_extraClassPath}" ]]; then
            EXECUTOR_EXTRA_CLASSPATH="${PYSP_TEST_spark_executor_extraClassPath}:${EXECUTOR_EXTRA_CLASSPATH}"
        fi
        export PYSP_TEST_spark_driver_extraClassPath="${EXTRA_CLASSPATH}"
        export PYSP_TEST_spark_executor_extraClassPath="${EXECUTOR_EXTRA_CLASSPATH}"
        unset PYSP_TEST_spark_jars
    else
        export PYSP_TEST_spark_jars="${ALL_JARS//:/,}"
    fi

    # time zone will be tested; use export TZ=time_zone_name before run this script
    export TZ=${TZ:-UTC}

    # Disable Spark UI by default since it is not needed for tests, and Spark can fail to start
    # due to Spark UI port collisions, especially in a parallel test setup.
    export PYSP_TEST_spark_ui_enabled=${PYSP_TEST_spark_ui_enabled:-false}

    # Set the Delta log cache size to prevent the driver from caching every Delta log indefinitely
    export PYSP_TEST_spark_databricks_delta_delta_log_cacheSize=${PYSP_TEST_spark_databricks_delta_delta_log_cacheSize:-10}
    deltaCacheSize=$PYSP_TEST_spark_databricks_delta_delta_log_cacheSize
    # currently the only test feature this enables is OOM injection
    # we enable the java property in the driver and executor, in case the tests are running in 
    # local mode or in standalone mode.
    ENABLE_TEST_FEATURES="-Dcom.nvidia.spark.rapids.runningTests=true"
    # Opt-in Spark testing mode (driver only). With -Dspark.testing=true Spark's Utils.isTesting
    # turns on internal-contract guards that are silent in production, e.g. the DSv2
    # computeStats-before-pushdown check. See NVIDIA/spark-rapids#14950 and #14927. Driver-only is
    # enough for the planning/optimizer guards and keeps the blast radius off the executors.
    SPARK_TESTING_OPTS=""
    if [[ "${SPARK_TESTING_ENABLED:-0}" == "1" ]]; then
        SPARK_TESTING_OPTS="-Dspark.testing=true"
        [[ -n "${SPARK_HOME:-}" ]] && SPARK_TESTING_OPTS="$SPARK_TESTING_OPTS -Dspark.test.home=$SPARK_HOME"
    fi
    DRIVER_EXTRA_JAVA_OPTIONS="-ea -Duser.timezone=$TZ -Ddelta.log.cacheSize=$deltaCacheSize $SPARK_TESTING_OPTS"
    export PYSP_TEST_spark_driver_extraJavaOptions="$DRIVER_EXTRA_JAVA_OPTIONS $COVERAGE_SUBMIT_FLAGS $ENABLE_TEST_FEATURES"
    export PYSP_TEST_spark_executor_extraJavaOptions="-ea -Duser.timezone=$TZ $ENABLE_TEST_FEATURES"

    # TODO: https://github.com/NVIDIA/spark-rapids/issues/10940
    export PYSP_TEST_spark_driver_memory=${PYSP_TEST_spark_driver_memory:-"${MB_PER_EXEC}m"}
    # Set driver memory to speed up tests such as deltalake
    if [[ -n "${DRIVER_MEMORY}" ]]; then
        export PYSP_TEST_spark_driver_memory="${DRIVER_MEMORY}"
    fi

    export PYSP_TEST_spark_ui_showConsoleProgress='false'
    export PYSP_TEST_spark_sql_session_timeZone=$TZ
    export PYSP_TEST_spark_sql_shuffle_partitions='4'
    # prevent cluster shape to change
    export PYSP_TEST_spark_dynamicAllocation_enabled='false'
    export PYSP_TEST_spark_rapids_memory_host_spillStorageSize='100m'
    # Not the default 2G but should be large enough for a single batch for all data (we found
    # 200 MiB being allocated by a single test at most, and we typically have 4 tasks.
    export PYSP_TEST_spark_rapids_sql_batchSizeBytes='100m'

    export PYSP_TEST_spark_hadoop_hive_exec_scratchdir="$RUN_DIR/hive"


    # Set spark.task.maxFailures for most schedulers.
    #
    # Local (non-cluster) mode is the exception and does not work with `spark.task.maxFailures`.
    # It requires two arguments to the master specification "local[N, K]" where
    # N is the number of threads, and K is the maxFailures (otherwise this is hardcoded to 1,
    # see https://issues.apache.org/jira/browse/SPARK-2083).
    export PYSP_TEST_spark_task_maxFailures="$SPARK_TASK_MAXFAILURES"

    if ((NUM_LOCAL_EXECS > 0)); then
      export PYSP_TEST_spark_master="local-cluster[$NUM_LOCAL_EXECS,$CORES_PER_EXEC,$MB_PER_EXEC]"
    else
      # If a master is not specified, use "local[cores, $SPARK_TASK_MAXFAILURES]"
      if [ -z "${PYSP_TEST_spark_master}" ] && [[ "$SPARK_SUBMIT_FLAGS" != *"--master"* ]]; then
        CPU_CORES=`nproc`
        # We are limiting the number of tasks in local mode to 4 because it helps to reduce the
        # total memory usage, especially host memory usage because when copying data to the GPU
        # buffers as large as batchSizeBytes can be allocated, and the fewer of them we have the better.
        LOCAL_PARALLEL=$(( CPU_CORES > 4 ? 4 : CPU_CORES ))
        export PYSP_TEST_spark_master="local[$LOCAL_PARALLEL,$SPARK_TASK_MAXFAILURES]"
      fi
    fi
    if [[ "$SPARK_SUBMIT_FLAGS" == *"--master local"* || "$PYSP_TEST_spark_master" == "local"* ]]; then
        # The only case where we want worker logs is in local mode so we set the value here explicitly
        # We can't use the PYSP_TEST_spark_master as it's not always set e.g. when using --master
        export USE_WORKER_LOGS=1
    fi
    # Set a seed to be used in the tests, for datagen
    export SPARK_RAPIDS_TEST_DATAGEN_SEED=${SPARK_RAPIDS_TEST_DATAGEN_SEED:-${DATAGEN_SEED:-`date +%s`}}
    echo "SPARK_RAPIDS_TEST_DATAGEN_SEED used: $SPARK_RAPIDS_TEST_DATAGEN_SEED"

    # Set a seed to be used to pick random tests to inject with OOM
    export SPARK_RAPIDS_TEST_INJECT_OOM_SEED=${SPARK_RAPIDS_TEST_INJECT_OOM_SEED:-`date +%s`}
    echo "SPARK_RAPIDS_TEST_INJECT_OOM_SEED used: $SPARK_RAPIDS_TEST_INJECT_OOM_SEED"
    if [[ -n "${RANDOM_SELECT}" ]]; then
        if [[ -n "${RANDOM_SELECT_SEED}" ]]; then
            echo "RANDOM_SELECT configured: value=${RANDOM_SELECT}, seed=${RANDOM_SELECT_SEED}"
        else
            echo "RANDOM_SELECT configured: value=${RANDOM_SELECT}, seed=default(0)"
        fi
    else
        echo "RANDOM_SELECT not set"
    fi

    # If you want to change the amount of GPU memory allocated you have to change it here
    # and where TEST_PARALLEL is calculated
    if [[ -n "${PYSP_TEST_spark_rapids_memory_gpu_allocSize}" ]]; then
       >&2 echo "#### WARNING: using externally set" \
                "PYSP_TEST_spark_rapids_memory_gpu_allocSize" \
                "${PYSP_TEST_spark_rapids_memory_gpu_allocSize}." \
                "If needed permanently in CI please file an issue to accommodate" \
                "for new GPU memory requirements ####"
    fi
    export PYSP_TEST_spark_rapids_memory_gpu_allocSize=${PYSP_TEST_spark_rapids_memory_gpu_allocSize:-'1536m'}

    # Retry coverage tracking - detect memory allocations not covered by withRetry.
    # Enable by setting SPARK_RAPIDS_RETRY_COVERAGE_TRACKING=true before running tests.
    # See AllocationRetryCoverageTracker.scala and https://github.com/NVIDIA/spark-rapids/issues/13672
    if [[ -n "${SPARK_RAPIDS_RETRY_COVERAGE_TRACKING}" ]]; then
        export PYSP_TEST_spark_executorEnv_SPARK_RAPIDS_RETRY_COVERAGE_TRACKING="${SPARK_RAPIDS_RETRY_COVERAGE_TRACKING}"
    fi

    # Turns on $LOAD_HYBRID_BACKEND and setup the filepath of hybrid backend jars, to activate the
    # hybrid backend while running subsequent integration tests.
    if [[ "$LOAD_HYBRID_BACKEND" -eq 1 ]]; then
      if [ -z "${HYBRID_BACKEND_JARS}" ]; then
        echo "Error: Environment HYBRID_BACKEND_JARS is not set."
        exit 1
      fi
      export PYSP_TEST_spark_jars="${PYSP_TEST_spark_jars},${HYBRID_BACKEND_JARS//:/,}"
      export PYSP_TEST_spark_rapids_sql_hybrid_loadBackend=true
      export PYSP_TEST_spark_memory_offHeap_enabled=true
      export PYSP_TEST_spark_memory_offHeap_size=512M
      export PYSP_TEST_spark_gluten_loadLibFromJar=true
    fi

    SPARK_SHELL_SMOKE_TEST="${SPARK_SHELL_SMOKE_TEST:-0}"
    EXPLAIN_ONLY_CPU_SMOKE_TEST="${EXPLAIN_ONLY_CPU_SMOKE_TEST:-0}"
    SPARK_CONNECT_SMOKE_TEST="${SPARK_CONNECT_SMOKE_TEST:-0}"
    if [[ "${SPARK_SHELL_SMOKE_TEST}" != "0" ]]; then
        echo "Running spark-shell smoke test..."
        SPARK_SHELL_ARGS_ARR=(
            --master local-cluster[1,2,1024]
            --conf spark.plugins=com.nvidia.spark.SQLPlugin
            --conf spark.deploy.maxExecutorRetries=0
        )
        if [[ "${PYSP_TEST_spark_shuffle_manager}" != "" ]]; then
            SPARK_SHELL_ARGS_ARR+=(
                --conf spark.shuffle.manager="${PYSP_TEST_spark_shuffle_manager}"
            )
        fi
        if [[ -n "$PYSP_TEST_spark_driver_extraClassPath" ]]; then
            SPARK_SHELL_ARGS_ARR+=(
                --driver-class-path "${PYSP_TEST_spark_driver_extraClassPath}"
                --conf spark.executor.extraClassPath="${PYSP_TEST_spark_executor_extraClassPath:-$PYSP_TEST_spark_driver_extraClassPath}"
            )
        elif [[ -n "$PYSP_TEST_spark_jars_packages" ]]; then
            SPARK_SHELL_ARGS_ARR+=(--packages "${PYSP_TEST_spark_jars_packages}")
        else
            SPARK_SHELL_ARGS_ARR+=(--jars "${PYSP_TEST_spark_jars}")
        fi

        if [[ -n "$PYSP_TEST_spark_jars_repositories" ]]; then
            SPARK_SHELL_ARGS_ARR+=(--repositories "${PYSP_TEST_spark_jars_repositories}")
        fi

        if [[ -n "$PYSP_TEST_spark_jars_ivySettings" ]]; then
            SPARK_SHELL_ARGS_ARR+=(--conf "spark.jars.ivySettings=${PYSP_TEST_spark_jars_ivySettings}")
        fi

        # NOTE grep is used not only for checking the output but also
        # to workaround the fact that spark-shell catches all failures.
        # In this test it exits not because of the failure but because it encounters
        # an EOF on stdin and injects a ":quit" command. Without a grep check
        # the exit code would be success 0 regardless of the exceptions.
        #
        # Capture combined stdout/stderr to a log so we can dump diagnostics
        # on failure. Previously stderr was discarded, which made CI failures
        # in this step opaque (the only signal was a non-zero grep exit).
        smoke_log=$(mktemp -t spark-shell-smoke.XXXXXX.log)
        if <<< 'spark.range(100).agg(Map("id" -> "sum")).collect()' \
                "${SPARK_HOME}"/bin/spark-shell "${SPARK_SHELL_ARGS_ARR[@]}" 2>&1 \
                | tee "$smoke_log" \
                | grep -F 'res0: Array[org.apache.spark.sql.Row] = Array([4950])'; then
            rm -f "$smoke_log"
            echo "SUCCESS spark-shell smoke test"
        else
            shell_status=${PIPESTATUS[0]}
            grep_status=${PIPESTATUS[2]}
            echo "FAILED spark-shell smoke test (spark-shell exit=${shell_status}, grep exit=${grep_status})"
            echo "----- spark-shell combined stdout/stderr (tail -n 300 of $smoke_log) -----"
            tail -n 300 "$smoke_log" || true
            echo "----- spark-shell executor stderr (tail -n 200 each) -----"
            for f in "${SPARK_HOME}"/work/*/*/stderr; do
                [[ -f "$f" ]] || continue
                echo "=== $f ==="
                tail -n 200 "$f" || true
            done
            rm -f "$smoke_log"
            exit 1
        fi
    elif [[ "${EXPLAIN_ONLY_CPU_SMOKE_TEST}" != "0" ]]; then
        echo "Running explainOnly mode on CPU smoke test..."
        SPARK_SHELL_ARGS_ARR=(
            --master local[2]
            --conf spark.plugins=com.nvidia.spark.SQLPlugin
            --conf spark.deploy.maxExecutorRetries=0
            --conf spark.rapids.sql.mode=explainOnly
        )
        if [[ -n "$PYSP_TEST_spark_driver_extraClassPath" ]]; then
            SPARK_SHELL_ARGS_ARR+=(
                --driver-class-path "${PYSP_TEST_spark_driver_extraClassPath}"
                --conf spark.executor.extraClassPath="${PYSP_TEST_spark_executor_extraClassPath:-$PYSP_TEST_spark_driver_extraClassPath}"
            )
        else
            SPARK_SHELL_ARGS_ARR+=(--jars "${PYSP_TEST_spark_jars}")
        fi
        output=$(<<< 'spark.range(100).agg(Map("id" -> "sum")).collect()' \
            CUDA_VISIBLE_DEVICES="" "${SPARK_HOME}"/bin/spark-shell "${SPARK_SHELL_ARGS_ARR[@]}" 2>&1)
        grep 'WARN RapidsPluginUtils: RAPIDS Accelerator is in explain only mode' <<< "$output"
        grep -F 'res0: Array[org.apache.spark.sql.Row] = Array([4950])' <<< "$output"
        echo "SUCCESS explainOnly mode on CPU smoke test"
    elif [[ "${SPARK_CONNECT_SMOKE_TEST}" != "0" ]]; then
        echo "Running Spark Connect smoke test..."
        # Gate on Spark version (3.5.6+ has Connect support with external jars. Example:plugin jar)
        # https://github.com/apache/spark/pull/50475
        # Version-aware compare: skip if VERSION_STRING < 3.5.6
        if ! printf '%s\n' "$VERSION_STRING" "3.5.6" | sort -V | head -1 | grep -qx "3.5.6"; then
            echo "SKIPPING Spark Connect smoke test - requires Spark 3.5.6+ but found $VERSION_STRING"
            exit 0
        fi

        # Build Connect packages and server-side jars (RAPIDS plugin)
        CONNECT_PACKAGES="org.apache.spark:spark-connect_${SCALA_VERSION}:${VERSION_STRING}"
        SERVER_JARS=""
        if [[ -n "$PYSP_TEST_spark_jars" ]]; then
            SERVER_JARS="$PYSP_TEST_spark_jars"
        elif [[ -n "$ALL_JARS" ]]; then
            SERVER_JARS="${ALL_JARS//:/,}"
        fi

        SPARK_SHELL_ARGS_ARR=(
            --master local-cluster[1,2,1024]
            --conf spark.plugins=com.nvidia.spark.SQLPlugin
            --packages "$CONNECT_PACKAGES"
            ${SERVER_JARS:+--jars "$SERVER_JARS"}
        )
        if [[ -n "$PYSP_TEST_spark_jars_ivySettings" ]]; then
            SPARK_SHELL_ARGS_ARR+=(--conf "spark.jars.ivySettings=${PYSP_TEST_spark_jars_ivySettings}")
        fi

        # Helper: check if port is listening
        check_port() {
            local port=$1
            if command -v ss >/dev/null 2>&1; then
                ss -ltn 2>/dev/null | grep -q ":${port} "
            else
                netstat -ltn 2>/dev/null | grep -q ":${port} "
            fi
        }

        # Pick a free localhost port
        pick_free_port() {
            local port
            for i in $(seq 1 50); do
                port=$(( (RANDOM % 10000) + 20000 ))
                if ! check_port "$port"; then
                    echo "$port"
                    return 0
                fi
            done
            echo 15002
        }

        # Prefer default Connect port; if busy, fall back to a free ephemeral port
        CONNECT_HOST="${CONNECT_HOST:-127.0.0.1}"
        CONNECT_PORT=15002
        if check_port "$CONNECT_PORT"; then
            CONNECT_PORT=$(pick_free_port)
        fi
        CONNECT_SERVER_URL="sc://${CONNECT_HOST}:${CONNECT_PORT}"

        cleanup_connect_server() {
            # dump connect server logs for troubleshooting
            local connect_server_logs=("$SPARK_HOME"/logs/*SparkConnectServer*.out)
            if [[ -f "${connect_server_logs[0]}" ]]; then
                for f in "${connect_server_logs[@]}"; do
                    echo "=== $f ==="
                    cat "$f" || true
                done
            else
                echo "No Spark Connect server .out log found in $SPARK_HOME/logs"
            fi
            if [[ -f "${SPARK_HOME}/sbin/stop-connect-server.sh" ]]; then
                timeout 20 "${SPARK_HOME}/sbin/stop-connect-server.sh" || true
            fi
            pkill -f "org.apache.spark.sql.connect.service.SparkConnectServer" || true
            pkill -f "spark-shell.*--remote" || true
        }
        trap cleanup_connect_server EXIT

        if ! start_output=$("${SPARK_HOME}/sbin/start-connect-server.sh" \
            "${SPARK_SHELL_ARGS_ARR[@]}" 2>&1); then
          echo "ERROR: Spark Connect server failed to launch"
          printf "%s\n" "$start_output" | tail -n 200
          exit 1
        fi

        # Wait for Connect server to listen
        service_ready=0
        for i in $(seq 1 60); do
            if timeout 1 bash -c "</dev/tcp/${CONNECT_HOST}/${CONNECT_PORT}" 2>/dev/null; then
                service_ready=1
                break
            fi
            sleep 10
        done
        if (( service_ready != 1 )); then
            echo "ERROR: Connect server failed to start on ${CONNECT_HOST}:${CONNECT_PORT}"
            exit 1
        fi

        case $VERSION_STRING in
          3.5.*)
            CONNECT_PIP_PACKAGE="pyspark[connect]"
            ;;
          
          4.*)
            # Create a venv and install only pyspark-client to ensure a pure Python client
            # See: https://spark.apache.org/docs/latest/api/python/getting_started/install.html#python-spark-connect-client
            CONNECT_PIP_PACKAGE="pyspark-client"
            ;;
        esac

        CONNECT_CLIENT_VENV="${RUN_DIR}/connect_client_venv"
        python -m venv "$CONNECT_CLIENT_VENV"
        "$CONNECT_CLIENT_VENV/bin/python" -m pip install --upgrade pip >/dev/null
        "$CONNECT_CLIENT_VENV/bin/python" -m pip install --no-cache-dir "$CONNECT_PIP_PACKAGE==${VERSION_STRING}" > /dev/null

        # Run a simple query using the Connect client and assert expected result and GPU operator in the plan
        output=$(CONNECT_URL="$CONNECT_SERVER_URL" \
            timeout 120s "$CONNECT_CLIENT_VENV/bin/python" - <<'PY'
import os
from pyspark.sql import SparkSession
url = os.environ["CONNECT_URL"]
spark = SparkSession.builder.remote(url).getOrCreate()
spark.range(100).explain(True)
res = spark.range(100).selectExpr('sum(id) as s').collect()[0].s
print(f'SC_RESULT={res}')
spark.stop()
PY
)
        client_rc=$?
        if (( client_rc != 0 )); then
            # Exit due to client timeout/failure from the 120s timeout wrapper
            echo "ERROR: Spark Connect client timed out after 120s"
            exit 1
        fi
        # Verify numeric result marker and GPU plan element
        if ! grep -Fq 'SC_RESULT=4950' <<< "$output"; then
            echo "ERROR: Expected result SC_RESULT=4950 not found in Connect output"
            exit 1
        fi
        if ! grep -Fq 'GpuRange' <<< "$output"; then
            echo "ERROR: Connect physical plan does not contain GpuRange"
            exit 1
        fi
        echo "SUCCESS Spark Connect smoke test"
    elif ((${#TEST_PARALLEL_OPTS[@]} > 0));
    then
        exec python "${RUN_TESTS_COMMAND[@]}" "${TEST_PARALLEL_OPTS[@]}" "${TEST_COMMON_OPTS[@]}"
    else
        if [[ "$USE_WORKER_LOGS" == "1" ]]; then
          # Setting the extraJavaOptions again to set the log4j confs that will be needed for writing logs in the expected location
          # We have to export it again because we want to be able to let the user override these confs by setting them on the
          # command-line using the COVERAGE_SUBMIT_FLAGS which won't be possible if we were to just say
          # export $PYSP_TEST_spark_driver_extraJavaOptions = "$PYSP_TEST_spark_driver_extraJavaOptions $LOG4J_CONF"
          LOG4J_CONF="-Dlog4j.configuration=file://$STD_INPUT_PATH/pytest_log4j.properties -Dlogfile=$RUN_DIR/gw0_worker_logs.log"
          export PYSP_TEST_spark_driver_extraJavaOptions="$DRIVER_EXTRA_JAVA_OPTIONS $LOG4J_CONF $COVERAGE_SUBMIT_FLAGS $ENABLE_TEST_FEATURES"
        fi

        # We set the GPU memory size to be a constant value even if only running with a parallelism of 1
        # because it helps us have consistent test runs.
        jarOpts=()
        if [[ -n "$PYSP_TEST_spark_jars" ]]; then
            jarOpts+=(--jars "${PYSP_TEST_spark_jars}")
        fi

        if [[ -n "$PYSP_TEST_spark_jars_packages" ]]; then
            jarOpts+=(--packages "${PYSP_TEST_spark_jars_packages}")
        fi

        if [[ -n "$PYSP_TEST_spark_jars_repositories" ]]; then
            jarOpts+=(--repositories "${PYSP_TEST_spark_jars_repositories}")
        fi

        if [[ -n "$PYSP_TEST_spark_driver_extraClassPath" ]]; then
            jarOpts+=(--driver-class-path "${PYSP_TEST_spark_driver_extraClassPath}")
        fi

        driverJavaOpts="$PYSP_TEST_spark_driver_extraJavaOptions"
        gpuAllocSize="$PYSP_TEST_spark_rapids_memory_gpu_allocSize"

        # avoid double processing of variables passed to spark in
        # spark_conf_init
        unset PYSP_TEST_spark_databricks_delta_delta_log_cacheSize
        unset PYSP_TEST_spark_driver_extraClassPath
        unset PYSP_TEST_spark_driver_extraJavaOptions
        unset PYSP_TEST_spark_jars
        unset PYSP_TEST_spark_jars_packages
        unset PYSP_TEST_spark_jars_repositories
        unset PYSP_TEST_spark_rapids_memory_gpu_allocSize


        # Comment this out if you want to run remote debug this local mode spark process
        # Don't forget to set TEST_PARALLEL=1 to ensure local mode spark 
        # export SPARK_SUBMIT_OPTS="-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005"

        exec "$SPARK_HOME"/bin/spark-submit "${jarOpts[@]}" \
            --driver-java-options "$driverJavaOpts" \
            $SPARK_SUBMIT_FLAGS \
            --conf 'spark.rapids.memory.gpu.allocSize='"$gpuAllocSize" \
            --conf 'spark.databricks.delta.delta.log.cacheSize='"$deltaCacheSize" \
            "${RUN_TESTS_COMMAND[@]}" "${TEST_COMMON_OPTS[@]}"
    fi
fi
