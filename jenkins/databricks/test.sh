#!/bin/bash
#
# Copyright (c) 2020-2026, NVIDIA CORPORATION. All rights reserved.
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

# This script sets the  environment to run tests of RAPIDS Accelerator for Apache Spark on DB.
# All the environments can be overwritten by shell variables:
#   LOCAL_JAR_PATH: Location of the RAPIDS jars
#   SPARK_CONF: Spark configuration parameters
#   BASE_SPARK_VERSION: Spark version [3.2.1, 3.3.0]. Default is pulled from current instance.
#   SHUFFLE_SPARK_SHIM: Set the default value for the shuffle shim. For Databricks
#                       versions, use the matching buildver, such as spark350db143
#   TEST_MODE: Can be one of the following (`DEFAULT` is the default value):
#       - DEFAULT: all tests except cudf_udf tests
#       - DELTA_LAKE_ONLY: delta_lake tests only
#       - MULTITHREADED_SHUFFLE: shuffle tests only
# Usage:
# - Running tests on DB10.4/Spark 3.2.1:
#       `BASE_SPARK_VERSION=3.2.1 ./jenkins/databricks/test.sh`
# To add support of a new runtime:
#   1. Check if any more dependencies need to be added to the apt/pip install commands.
#   2. If you had to go beyond the above steps to support the new runtime, then update the
#      instructions accordingly.
#
# This file parallely runs pytests with python-xdist(e.g., TEST_PARALLEL=4).

set -ex

## 'foo=abc,bar=123,...' to 'export foo=abc bar=123 ...'
[[ -n "$EXTRA_ENVS" ]] && export ${EXTRA_ENVS//','/' '}
# TEST_MODE
# - DEFAULT: all tests except cudf_udf tests
# - DELTA_LAKE_ONLY: delta_lake tests only
# - MULTITHREADED_SHUFFLE: shuffle tests only
# - PYARROW_ONLY: pyarrow tests only
# - CI_PART1 or CI_PART2 : part1 or part2 of the tests run in parallel from CI.
#   CI_PART2 includes nightly resource-consuming tests to keep the split runtimes closer.
TEST_MODE=${TEST_MODE:-'DEFAULT'}

# CI_PART2 untars the spark-rapids tgz built by C1_PART1 instead of rebuilding it
PLUGIN_BUILT_TGZ=${PLUGIN_BUILT_TGZ:-"$1"}
if [[ "$TEST_MODE" == "CI_PART2"  && -z "$LOCAL_JAR_PATH" && -f "$PLUGIN_BUILT_TGZ" ]]; then
    tar -zxf $PLUGIN_BUILT_TGZ
fi

SOURCE_PATH="/home/ubuntu/spark-rapids"
[[ -d "$LOCAL_JAR_PATH" ]] && cd $LOCAL_JAR_PATH || cd $SOURCE_PATH

# Install python packages for integration tests
source jenkins/databricks/setup.sh
# Init common variables like SPARK_HOME, spark configs
source jenkins/databricks/common_vars.sh

# The startup hook identifies CI callers; manual runs keep their historical behavior.
# Enable native DBR 18.3 product tests while Delta Lake CI support is tracked in #16282.
DBR183_NON_DELTA_CI=0
if [[ -n "${SPARK_STARTUP_CHECK:-}" &&
      "${PYSP_TEST_spark_databricks_clusterUsageTags_sparkVersion:-}" == 18.3.* &&
      ("$TEST_MODE" == "DEFAULT" || "$TEST_MODE" == "CI_PART1" || "$TEST_MODE" == "CI_PART2") ]]; then
    DBR183_NON_DELTA_CI=1
fi
SPARK_READINESS_ONLY=${SPARK_READINESS_ONLY:-0}
if [[ "$SPARK_READINESS_ONLY" == "1" ]]; then
    if [[ -z "${SPARK_STARTUP_CHECK:-}" ]]; then
        echo "Readiness-only mode requires the CI Spark readiness hook" >&2
        exit 1
    fi
    case "$TEST_MODE" in
        DEFAULT|CI_PART1|CI_PART2) ;;
        *) echo "Readiness-only mode does not support TEST_MODE=$TEST_MODE" >&2; exit 1 ;;
    esac
fi

BASE_SPARK_VERSION=${BASE_SPARK_VERSION:-$(< /databricks/spark/VERSION)}

# For Spark 4.x (Scala 2.13), the upstream base shim is 350 (Spark 3.5.0).
# For Spark 3.x (Scala 2.12), the upstream base shim is 330 (Spark 3.3.0).
if [[ "$BASE_SPARK_VERSION" == 4.* ]]; then
    # TODO: Revisit UPSTREAM_SHIM_VER when new Spark 4.x Databricks runtimes are introduced
    # that may pair with a different upstream shim (e.g., spark360).
    UPSTREAM_SPARK_VERSION="3.5.0"
    UPSTREAM_SHIM_VER="spark350"
    UPSTREAM_SPARK_SCALA_SUFFIX="-scala2.13"
else
    UPSTREAM_SPARK_VERSION="3.3.0"
    UPSTREAM_SHIM_VER="spark330"
    UPSTREAM_SPARK_SCALA_SUFFIX=""
fi

WITH_DEFAULT_UPSTREAM_SHIM=${WITH_DEFAULT_UPSTREAM_SHIM:-1}

# CI-only hook installed by fallback.groovy, not a prerequisite for local build/test commands.
# Leave SPARK_STARTUP_CHECK unset for local usage; ordinary callers keep their existing behavior.
if [[ -n "${SPARK_STARTUP_CHECK:-}" ]]; then
    bash "$SPARK_STARTUP_CHECK" databricks
fi

if [[ "$SPARK_READINESS_ONLY" == "1" &&
      ("$TEST_MODE" == "CI_PART2" || "$WITH_DEFAULT_UPSTREAM_SHIM" == "0") ]]; then
    echo "Required Spark readiness checks completed; product tests remain disabled"
    exit 0
fi

IS_SPARK_321_OR_LATER=0
[[ "$(printf '%s\n' "3.2.1" "$BASE_SPARK_VERSION" | sort -V | head -n1)" = "3.2.1" ]] && IS_SPARK_321_OR_LATER=1

# Classloader config is here to work around classloader issues with
# --packages in distributed setups, should be fixed by
# https://github.com/NVIDIA/spark-rapids/pull/5646
rapids_shuffle_smoke_test() {
    echo "Run rapids_shuffle_smoke_test..."

    # using MULTITHREADED shuffle
    TEST_PARALLEL=0 \
    PYSP_TEST_spark_rapids_shuffle_mode=MULTITHREADED \
    PYSP_TEST_spark_rapids_shuffle_multiThreaded_writer_threads=2 \
    PYSP_TEST_spark_rapids_shuffle_multiThreaded_reader_threads=2 \
    PYSP_TEST_spark_shuffle_manager=com.nvidia.spark.rapids.$SPARK_SHIM_VER.RapidsShuffleManager \
    SPARK_SUBMIT_FLAGS="$SPARK_CONF" \
    bash integration_tests/run_pyspark_from_build.sh -m shuffle_test --runtime_env="databricks" --test_type=$TEST_TYPE
}

run_pyarrow_tests() {
    SPARK_SUBMIT_FLAGS="$SPARK_CONF" \
        bash integration_tests/run_pyspark_from_build.sh -m pyarrow_test --pyarrow_test --runtime_env="databricks" --test_type=$TEST_TYPE
}

# Move selected tests to CI_PART2 so the split CI jobs finish closer together.
# Keep DEFAULT unchanged so manual/default runs still cover everything in the historical order.
# Include files whose names also match the CI_PART1 substring expression.
CI_PART2_BALANCED_TESTS="json_test get_json_test orc_test parquet_test parquet_testing_test sort_test window_function_test"
CI_PART2_BALANCED_TEST_EXPR="${CI_PART2_BALANCED_TESTS// / or }"

## Separate the integration tests into "CI_PART1" and "CI_PART2", run each part in parallel on separate Databricks clusters to speed up the testing process.
if [[ $TEST_MODE == "DEFAULT" || $TEST_MODE == "CI_PART1" ]]; then
    # Run two-shim smoke test with the base Spark build
    if [[ "$WITH_DEFAULT_UPSTREAM_SHIM" != "0" ]]; then
        if [[ ! -d $HOME/spark-${UPSTREAM_SPARK_VERSION}-bin-hadoop3${UPSTREAM_SPARK_SCALA_SUFFIX} ]]; then
            # DBFS cache for Spark
            DBFS_CACHE_DIR=${DBFS_CACHE_DIR:-"/dbfs/cached_jars"}
            JAR_FILE_NAME=${JAR_FILE_NAME:-"spark-${UPSTREAM_SPARK_VERSION}-bin-hadoop3${UPSTREAM_SPARK_SCALA_SUFFIX}.tgz"}
            SPARK_CACHE_FILE=${SPARK_CACHE_FILE:-"$DBFS_CACHE_DIR/$JAR_FILE_NAME"}
            SPARK_URL=${SPARK_URL:-"https://archive.apache.org/dist/spark/spark-${UPSTREAM_SPARK_VERSION}/$JAR_FILE_NAME"}
            # Create cache directory if it doesn't exist
            mkdir -p "$DBFS_CACHE_DIR"
            # Check if file exists in DBFS cache
            if [[ -f "$SPARK_CACHE_FILE" ]]; then
                echo "Found Spark in DBFS cache, copying to /tmp..."
                cp "$SPARK_CACHE_FILE" "/tmp/$JAR_FILE_NAME"
            else
                echo "Spark not found in DBFS cache, downloading from archive.apache.org..."
                if wget "$SPARK_URL" -P /tmp; then
                    echo "Download successful, caching to DBFS..."
                    cp "/tmp/$JAR_FILE_NAME" "$SPARK_CACHE_FILE" || true
                else
                    echo "Download failed"
                    exit 1
                fi
            fi
            tar xf "/tmp/$JAR_FILE_NAME" -C $HOME
            rm -f "/tmp/$JAR_FILE_NAME"
        fi
        # Spark 3.5.0 bundles Scala 2.13.8, whose compiler is incompatible with JDK 21.
        # Keep JDK 17 scoped to upstream processes; native Databricks uses its own JDK.
        upstream_java_env=()
        if [[ "$UPSTREAM_SPARK_VERSION" == "3.5.0" ]]; then
            upstream_java_home=""
            for candidate in "${JAVA_HOME:-}" /usr/lib/jvm/zulu17 /usr/lib/jvm/java-17-openjdk-*; do
                if [[ -n "$candidate" && -x "$candidate/bin/java" ]]; then
                    candidate_version=$("$candidate/bin/java" -XshowSettings:properties -version 2>&1) || continue
                    if [[ "$candidate_version" =~ java\.specification\.version[[:space:]]*=[[:space:]]*17([[:space:]]|$) ]]; then
                        upstream_java_home="$candidate"
                        break
                    fi
                fi
            done
            if [[ -z "$upstream_java_home" ]]; then
                echo "Upstream Spark 3.5.0 requires an installed JDK 17" >&2
                exit 1
            fi
            upstream_java_env=("JAVA_HOME=$upstream_java_home" "PATH=$upstream_java_home/bin:$PATH")
        fi
        # The same CI-only hook checks upstream Spark; local runs leave it unset.
        if [[ -n "${SPARK_STARTUP_CHECK:-}" ]]; then
            env "${upstream_java_env[@]}" \
            "SPARK_HOME=$HOME/spark-${UPSTREAM_SPARK_VERSION}-bin-hadoop3${UPSTREAM_SPARK_SCALA_SUFFIX}" \
                bash "$SPARK_STARTUP_CHECK" upstream
        fi
        if [[ "$SPARK_READINESS_ONLY" == "1" ]]; then
            echo "Required Spark readiness checks completed; product tests remain disabled"
            exit 0
        fi
        env "${upstream_java_env[@]}" \
        "SPARK_HOME=$HOME/spark-${UPSTREAM_SPARK_VERSION}-bin-hadoop3${UPSTREAM_SPARK_SCALA_SUFFIX}" \
        SPARK_SHELL_SMOKE_TEST=1 \
        PYSP_TEST_spark_shuffle_manager=com.nvidia.spark.rapids.${UPSTREAM_SHIM_VER}.RapidsShuffleManager \
            bash integration_tests/run_pyspark_from_build.sh
    fi
    if [[ "$TEST_MODE" == "CI_PART1" ]]; then
        bash integration_tests/run_pyspark_from_build.sh --runtime_env="databricks" \
            --test_type=$TEST_TYPE -k "not ($CI_PART2_BALANCED_TEST_EXPR)"
    else
        bash integration_tests/run_pyspark_from_build.sh --runtime_env="databricks" --test_type=$TEST_TYPE
    fi
fi

## Run tests with jars building from the spark-rapids source code
if [[ "$(pwd)" == "$SOURCE_PATH" ]]; then
    if [[ "$TEST_MODE" == "CI_PART2" ]]; then
        ## Run the balanced integration tests moved out of CI_PART1
        TESTS="${CI_PART2_BALANCED_TESTS// /.py }.py" \
            bash integration_tests/run_pyspark_from_build.sh --runtime_env="databricks" \
                --test_type=$TEST_TYPE
    fi

    ## Run cache tests
    if [[ "$IS_SPARK_321_OR_LATER" -eq "1" && ("$TEST_MODE" == "DEFAULT" || $TEST_MODE == "CI_PART2") ]]; then
        PYSP_TEST_spark_sql_cache_serializer=${PCBS_CONF} \
            bash integration_tests/run_pyspark_from_build.sh --runtime_env="databricks" --test_type=$TEST_TYPE -k cache_test
    fi

    if [[ "$TEST_MODE" == "DEFAULT" || $TEST_MODE == "CI_PART2" || "$TEST_MODE" == "DELTA_LAKE_ONLY" ]]; then
        if [[ "$DBR183_NON_DELTA_CI" == "1" ]]; then
            echo "DBR 18.3 CI: Delta Lake integration tests remain disabled (#16282)"
        else
            DRIVER_MEMORY="4g" \
                bash integration_tests/run_pyspark_from_build.sh --runtime_env="databricks" \
                    -m "delta_lake" --delta_lake --test_type=$TEST_TYPE
        fi
    fi

    if [[ "$TEST_MODE" == "DEFAULT" || $TEST_MODE == "CI_PART2" || "$TEST_MODE" == "MULTITHREADED_SHUFFLE" ]]; then
        ## Mutithreaded Shuffle test
        rapids_shuffle_smoke_test
    fi
    if [[ "$TEST_MODE" == "DEFAULT" || $TEST_MODE == "CI_PART2" || "$TEST_MODE" == "PYARROW_ONLY" ]]; then
      # Pyarrow tests
      run_pyarrow_tests
    fi
fi
