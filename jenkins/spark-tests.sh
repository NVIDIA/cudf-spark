#!/bin/bash
#
# Copyright (c) 2019-2026, NVIDIA CORPORATION. All rights reserved.
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

set -ex

nvidia-smi

. jenkins/version-def.sh
. jenkins/shuffle-common.sh

# Get the shuffle shim for the current Spark version
SHUFFLE_SPARK_SHIM=$(get_shuffle_shim)

# if run in jenkins WORKSPACE refers to rapids root path; if not run in jenkins just use current pwd(contains jenkins dirs)
WORKSPACE=${WORKSPACE:-`pwd`}

ARTF_ROOT="$WORKSPACE/jars"
WGET_CMD="wget -q -P $ARTF_ROOT -t 3"
PROJECT_REPO_HOST=$(sed -E 's#^(.*://)?([^/@]*@)?([^/:]+).*#\3#' <<< "$PROJECT_REPO")

rm -rf $ARTF_ROOT && mkdir -p $ARTF_ROOT
$WGET_CMD $PROJECT_TEST_REPO/com/nvidia/rapids-4-spark-integration-tests_$SCALA_BINARY_VER/$PROJECT_TEST_VER/rapids-4-spark-integration-tests_$SCALA_BINARY_VER-$PROJECT_TEST_VER-${SHUFFLE_SPARK_SHIM}.jar

CLASSIFIER=${CLASSIFIER:-"$CUDA_CLASSIFIER"} # default as CUDA_CLASSIFIER for compatibility
if [ "$CLASSIFIER"x == x ];then
    $WGET_CMD $PROJECT_REPO/com/nvidia/rapids-4-spark_$SCALA_BINARY_VER/$PROJECT_VER/rapids-4-spark_$SCALA_BINARY_VER-${PROJECT_VER}.jar
    export RAPIDS_PLUGIN_JAR=$ARTF_ROOT/rapids-4-spark_${SCALA_BINARY_VER}-${PROJECT_VER}.jar
else
    $WGET_CMD $PROJECT_REPO/com/nvidia/rapids-4-spark_$SCALA_BINARY_VER/$PROJECT_VER/rapids-4-spark_$SCALA_BINARY_VER-$PROJECT_VER-${CLASSIFIER}.jar
    export RAPIDS_PLUGIN_JAR="$ARTF_ROOT/rapids-4-spark_${SCALA_BINARY_VER}-$PROJECT_VER-${CLASSIFIER}.jar"
fi
RAPIDS_TEST_JAR="$ARTF_ROOT/rapids-4-spark-integration-tests_${SCALA_BINARY_VER}-$PROJECT_TEST_VER-$SHUFFLE_SPARK_SHIM.jar"

export INCLUDE_SPARK_AVRO_JAR=${INCLUDE_SPARK_AVRO_JAR:-"true"}
if [[ "${INCLUDE_SPARK_AVRO_JAR}" == "true" ]]; then
  $WGET_CMD $SPARK_REPO/org/apache/spark/spark-avro_$SCALA_BINARY_VER/$SPARK_VER/spark-avro_$SCALA_BINARY_VER-${SPARK_VER}.jar
fi

$WGET_CMD $PROJECT_TEST_REPO/com/nvidia/rapids-4-spark-integration-tests_$SCALA_BINARY_VER/$PROJECT_TEST_VER/rapids-4-spark-integration-tests_$SCALA_BINARY_VER-$PROJECT_TEST_VER-pytest.tar.gz

RAPIDS_INT_TESTS_HOME="$ARTF_ROOT/integration_tests/"
# The version of pytest.tar.gz that is uploaded is the one built against spark330 but its being pushed without classifier for now
RAPIDS_INT_TESTS_TGZ="$ARTF_ROOT/rapids-4-spark-integration-tests_${SCALA_BINARY_VER}-$PROJECT_TEST_VER-pytest.tar.gz"

tmp_info=${TMP_INFO_FILE:-'/tmp/artifacts-build.info'}
rm -rf "$tmp_info"
TEE_CMD="tee -a $tmp_info"
GREP_CMD="grep revision"
AWK_CMD=(awk -F'=' '{print $2}')
getRevision() {
  local file=$1
  local properties=$2
  local revision
  if [[ $file == *.jar || $file == *.zip ]]; then
    revision=$(unzip -p "$file" "$properties" | $TEE_CMD | $GREP_CMD | "${AWK_CMD[@]}" || true)
  elif [[ $file == *.tgz || $file == *.tar.gz ]]; then
    revision=$(tar -xzf "$file" --to-command=cat "$properties" | $TEE_CMD | $GREP_CMD | "${AWK_CMD[@]}" || true)
  fi
  echo "$revision"
}

set +x
echo -e "\n==================== ARTIFACTS BUILD INFO ====================\n" >> "$tmp_info"
echo "-------------------- rapids-4-spark BUILD INFO --------------------" >> "$tmp_info"
p_ver=$(getRevision $RAPIDS_PLUGIN_JAR rapids4spark-version-info.properties)
echo "-------------------- rapids-4-spark-integration-tests BUILD INFO --------------------" >> "$tmp_info"
it_ver=$(getRevision $RAPIDS_TEST_JAR rapids4spark-version-info.properties)
echo "-------------------- rapids-4-spark-integration-tests pytest BUILD INFO --------------------" >> "$tmp_info"
pt_ver=$(getRevision $RAPIDS_INT_TESTS_TGZ integration_tests/rapids4spark-version-info.properties)
echo -e "\n==================== ARTIFACTS BUILD INFO ====================\n" >> "$tmp_info"
set -x
cat "$tmp_info" || true

SKIP_REVISION_CHECK=${SKIP_REVISION_CHECK:-'false'}
if [[ "$SKIP_REVISION_CHECK" != "true" && (-z "$p_ver"|| \
      "$p_ver" != "$it_ver" || "$p_ver" != "$pt_ver") ]]; then
  echo "Artifacts revisions are inconsistent!"
  exit 1
fi

tar xzf "$RAPIDS_INT_TESTS_TGZ" -C $ARTF_ROOT && rm -f "$RAPIDS_INT_TESTS_TGZ"

. jenkins/hadoop-def.sh $SPARK_VER ${SCALA_BINARY_VER}
$WGET_CMD $SPARK_REPO/org/apache/spark/$SPARK_VER/spark-$SPARK_VER-$BIN_HADOOP_VER.tgz

export SPARK_HOME="$ARTF_ROOT/spark-$SPARK_VER-$BIN_HADOOP_VER"
export PATH="$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH"
tar zxf $SPARK_HOME.tgz -C $ARTF_ROOT && \
    rm -f $SPARK_HOME.tgz

# Download parquet-hadoop jar for parquet-read encryption tests
PARQUET_HADOOP_JAR=$(find "$SPARK_HOME/jars" -maxdepth 1 -type f -name 'parquet-hadoop-[0-9]*.jar' | head -n1)
PARQUET_HADOOP_VER=${PARQUET_HADOOP_JAR##*/parquet-hadoop-}
PARQUET_HADOOP_VER=${PARQUET_HADOOP_VER%.jar}
if [[ -n "$PARQUET_HADOOP_VER" && "$(printf '%s\n' "1.12.0" "$PARQUET_HADOOP_VER" | sort -V | head -n1)" = "1.12.0" ]]; then
  $WGET_CMD $SPARK_REPO/org/apache/parquet/parquet-hadoop/$PARQUET_HADOOP_VER/parquet-hadoop-$PARQUET_HADOOP_VER-tests.jar
fi

# copy python path libs to container /tmp instead of workspace to avoid ephemeral PVC issue
TMP_PYTHON=/tmp/$(date +"%Y%m%d")
rm -rf $TMP_PYTHON && mkdir -p $TMP_PYTHON && cp -r $SPARK_HOME/python $TMP_PYTHON
# Get the correct py4j file.
PY4J_FILE=$(find $TMP_PYTHON/python/lib -type f -iname "py4j*.zip")
export PYTHONPATH=$TMP_PYTHON/python:$TMP_PYTHON/python/pyspark/:$PY4J_FILE

# Extract 'value' from conda config string 'key: value'
CONDA_ROOT=`conda config --show root_prefix | cut -d ' ' -f2`
if [[ -n "$CONDA_ROOT" ]]; then
  # Put conda package path ahead of the env 'PYTHONPATH',
  # to import the right pandas from conda instead of spark binary path.
  PYTHON_VER=`conda config --show default_python | cut -d ' ' -f2`
  export PYTHONPATH="$CONDA_ROOT/lib/python$PYTHON_VER/site-packages:$PYTHONPATH"
else
  # if no conda, then try with default python
  DEFAULT_SITE_PACKAGES=$(python -c "import site; print(site.getsitepackages()[0])")
  export PYTHONPATH="$DEFAULT_SITE_PACKAGES:$PYTHONPATH"
fi


echo "----------------------------START TEST------------------------------------"
pushd $RAPIDS_INT_TESTS_HOME
export TEST_TYPE="nightly"
export LOCAL_JAR_PATH=$ARTF_ROOT

export SPARK_TASK_MAXFAILURES=1
export PATH="$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH"
# enable worker cleanup to avoid "out of space" issue
# if failed, we abort the test instantly, so the failed executor log should still be left there for debugging
export SPARK_WORKER_OPTS="$SPARK_WORKER_OPTS -Dspark.worker.cleanup.enabled=true -Dspark.worker.cleanup.interval=120 -Dspark.worker.cleanup.appDataTtl=60"
#stop and restart SPARK ETL
stop-worker.sh
stop-master.sh
start-master.sh
start-worker.sh spark://$HOSTNAME:7077
jps

# BASE spark test configs
export PYSP_TEST_spark_master=spark://$HOSTNAME:7077
export PYSP_TEST_spark_sql_shuffle_partitions=12
export PYSP_TEST_spark_task_maxFailures=$SPARK_TASK_MAXFAILURES
export PYSP_TEST_spark_dynamicAllocation_enabled=false
export PYSP_TEST_spark_driver_extraJavaOptions=-Duser.timezone=UTC
export PYSP_TEST_spark_executor_extraJavaOptions=-Duser.timezone=UTC
export PYSP_TEST_spark_sql_session_timeZone=UTC

# Run the nightly integration tests with Spark testing mode so that Spark enforces its internal
# contract guards (e.g. DSv2 computeStats-before-pushdown) that are silent in production.
# See NVIDIA/spark-rapids#14950. Export SPARK_TESTING_ENABLED=0 on a job to opt out.
export SPARK_TESTING_ENABLED=${SPARK_TESTING_ENABLED:-1}

# PARALLEL or non-PARALLEL specific configs
if [[ $PARALLEL_TEST == "true" ]]; then
  export PYSP_TEST_spark_cores_max=1
  export PYSP_TEST_spark_executor_memory=4g
  export PYSP_TEST_spark_executor_cores=1
  export PYSP_TEST_spark_task_cores=1
  export PYSP_TEST_spark_rapids_sql_concurrentGpuTasks=1
  export PYSP_TEST_spark_rapids_memory_gpu_minAllocFraction=0

  USER_SET_PARALLELISM="false"
  if [[ -n "${PARALLELISM}" ]]; then
    USER_SET_PARALLELISM="true"
  fi

  if [[ "${PARALLELISM}" == "" ]]; then
    PARALLELISM=$(nvidia-smi --query-gpu=memory.free --format=csv,noheader | \
      awk '{if (MAX < $1){ MAX = $1}} END {print int(MAX / (2 * 1024))}')
  fi
  # parallelism > 5 could slow down the whole process, so we have a limitation for it
  # this is based on our CI gpu types, so we do not put it into the run_pyspark_from_build.sh
  # 1. If TEST_MODE is DEFAULT: Always limit to 5.
  # 2. If TEST_MODE is NOT DEFAULT:
  #    a. If user set PARALLELISM: Use it directly (no limit).
  #    b. If user did NOT set PARALLELISM (calculated): Limit to 5.
  if [[ "${TEST_MODE:-DEFAULT}" == "DEFAULT" || "${USER_SET_PARALLELISM}" == "false" ]]; then
    [[ ${PARALLELISM} -gt 5 ]] && PARALLELISM=5
  fi
  MEMORY_FRACTION=$(python -c "print(1/($PARALLELISM + 0.1))")

  export TEST_PARALLEL=${PARALLELISM}
  export PYSP_TEST_spark_rapids_memory_gpu_allocFraction=${MEMORY_FRACTION}
  export PYSP_TEST_spark_rapids_memory_gpu_maxAllocFraction=${MEMORY_FRACTION}
else
  export PYSP_TEST_spark_cores_max=6
  export PYSP_TEST_spark_executor_memory=16g
  export TEST_PARALLEL=0
fi

export SCRIPT_PATH="$(pwd -P)"
export TARGET_DIR="$SCRIPT_PATH/target"
mkdir -p $TARGET_DIR

run_delta_lake_tests() {
  echo "run_delta_lake_tests SPARK_VER = $SPARK_VER, SCALA_BINARY_VER = $SCALA_BINARY_VER"
  SPARK_32X_PATTERN="(3\.2\.[0-9])"
  SPARK_33X_PATTERN="(3\.3\.[0-9])"
  SPARK_34X_PATTERN="(3\.4\.[0-9])"
  SPARK_35X_PATTERN="(3\.5\.[3-9])"
  SPARK_40X_PATTERN="(4\.0\.[0-9])"

  if [[ $SPARK_VER =~ $SPARK_32X_PATTERN ]]; then
    # There are multiple versions of deltalake that support SPARK 3.2.X
    # but for zorder tests to work we need 2.0.0+
    DELTA_LAKE_VERSIONS="2.0.1"
  fi

  if [[ $SPARK_VER =~ $SPARK_33X_PATTERN ]]; then
    DELTA_LAKE_VERSIONS="2.1.1 2.2.0 2.3.0"
  fi

  if [[ $SPARK_VER =~ $SPARK_34X_PATTERN ]]; then
    DELTA_LAKE_VERSIONS="2.4.0"
  fi

  if [[ $SPARK_VER =~ $SPARK_35X_PATTERN ]]; then
    DELTA_LAKE_VERSIONS="3.3.0"
  fi

  if [[ $SPARK_VER =~ $SPARK_40X_PATTERN ]]; then
    # Delta 4.0.x only supports Scala 2.13 (Spark 4.0 requirement)
    if [[ "$SCALA_BINARY_VER" == "2.13" ]]; then
      DELTA_LAKE_VERSIONS="4.0.0"
    else
      echo "Skipping Delta Lake 4.0.x tests for Scala $SCALA_BINARY_VER (requires Scala 2.13)"
    fi
  fi

  if [ -z "$DELTA_LAKE_VERSIONS" ]; then
    echo "Skipping Delta Lake tests. $SPARK_VER"
  else
    for v in $DELTA_LAKE_VERSIONS; do
      echo "Running Delta Lake tests for Delta Lake version $v"
      if [[ "$v" == "3.3.0" || "$v" == "4.0.0" ]]; then
        DELTA_MAIN_JAR="io.delta:delta-spark_${SCALA_BINARY_VER}:$v"
      else
        DELTA_MAIN_JAR="io.delta:delta-core_${SCALA_BINARY_VER}:$v"
      fi
      # Delta Lake 1.2+ moved LogStore implementations into delta-storage.
      # All versions tested here are 2.0+, so include it explicitly.
      DELTA_JAR="${DELTA_MAIN_JAR},io.delta:delta-storage:$v"
      HOST_NAME=$PROJECT_REPO_HOST \
        PYSP_TEST_spark_jars_packages=${DELTA_JAR} \
        PYSP_TEST_spark_jars_ivySettings="${WORKSPACE}/jenkins/ivysettings.xml" \
        PYSP_TEST_spark_sql_extensions="io.delta.sql.DeltaSparkSessionExtension" \
        PYSP_TEST_spark_sql_catalog_spark__catalog="org.apache.spark.sql.delta.catalog.DeltaCatalog" \
        ./run_pyspark_from_build.sh -m delta_lake --delta_lake
    done
  fi
}

run_iceberg_tests() {
  # get the major/minor version of Spark
  ICEBERG_SPARK_VER=$(echo "$SPARK_VER" | cut -d. -f1,2)
  # get the patch version of Spark
  SPARK_PATCH_VER=$(echo "$SPARK_VER" | cut -d. -f3)

  if [[ "$ICEBERG_SPARK_VER" != "3.5" && "$ICEBERG_SPARK_VER" != "4.0" \
        && "$ICEBERG_SPARK_VER" != "4.1" ]]; then
    echo "!!!! Skipping Iceberg tests. GPU acceleration of Iceberg is not supported on $ICEBERG_SPARK_VER"
    return 0
  fi

  # Supported Iceberg versions per Spark patch version:
  # Spark 3.5.0-3.5.3 -> Iceberg 1.6.1
  # Spark 3.5.4+       -> Iceberg 1.9.2, 1.10.1
  # Spark 4.0.0-4.0.1 -> Iceberg 1.10.1
  # Spark 4.0.2+       -> Iceberg 1.10.1, 1.11.0
  # Spark 4.1.x        -> Iceberg 1.11.0
  local supported_versions
  if [[ "$ICEBERG_SPARK_VER" == "4.1" ]]; then
    if [[ "$SCALA_BINARY_VER" != "2.13" ]]; then
      echo "!!!! Skipping Iceberg tests. Spark 4.1 Iceberg tests require Scala 2.13"
      return 0
    fi
    supported_versions="1.11.0"
  elif [[ "$ICEBERG_SPARK_VER" == "4.0" ]]; then
    if [[ "$SCALA_BINARY_VER" != "2.13" ]]; then
      echo "!!!! Skipping Iceberg tests. Spark 4.0 Iceberg tests require Scala 2.13"
      return 0
    fi
    if [[ "$SPARK_PATCH_VER" -ge 2 ]]; then
      supported_versions="1.10.1 1.11.0"
    else
      supported_versions="1.10.1"
    fi
  elif [[ "$SPARK_PATCH_VER" -le 3 ]]; then
    supported_versions="1.6.1"
  else
    supported_versions="1.9.2 1.10.1"
  fi

  local test_type=${1:-'default'}

  if [[ -n "$ICEBERG_VERSIONS" ]]; then
    for ver in $ICEBERG_VERSIONS; do
      if ! echo "$supported_versions" | grep -qw "$ver"; then
        echo "!!!! Error: Iceberg version $ver is not supported on Spark $SPARK_VER (supported: $supported_versions)"
        return 1
      fi
    done
    echo "Using user-specified ICEBERG_VERSIONS=$ICEBERG_VERSIONS"
  else
    # Default: test one representative version per Spark patch range
    if [[ "$ICEBERG_SPARK_VER" == "4.1" ]]; then
      ICEBERG_VERSIONS="1.11.0"
    elif [[ "$ICEBERG_SPARK_VER" == "4.0" ]]; then
      ICEBERG_VERSIONS="1.10.1"
    elif [[ "$SPARK_PATCH_VER" -le 3 ]]; then
      ICEBERG_VERSIONS="1.6.1"
    elif [[ "$SPARK_PATCH_VER" -le 6 ]]; then
      ICEBERG_VERSIONS="1.9.2"
    else
      ICEBERG_VERSIONS="1.10.1"
    fi
  fi
  for ICEBERG_VERSION in $ICEBERG_VERSIONS; do
    echo "Running Iceberg tests for Iceberg version $ICEBERG_VERSION"
    if [[ "$test_type" == "default" ]]; then
      echo "!!! Running iceberg tests"
      env \
        HOST_NAME=$PROJECT_REPO_HOST \
        EXPECTED_ICEBERG_VERSION=${ICEBERG_VERSION} \
        PYSP_TEST_spark_driver_memory=1G \
        PYSP_TEST_spark_executor_memory=2G \
        PYSP_TEST_spark_jars_packages=org.apache.iceberg:iceberg-spark-runtime-${ICEBERG_SPARK_VER}_${SCALA_BINARY_VER}:${ICEBERG_VERSION} \
        PYSP_TEST_spark_jars_ivySettings="${WORKSPACE}/jenkins/ivysettings.xml" \
        PYSP_TEST_spark_sql_extensions="org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions" \
        PYSP_TEST_spark_sql_catalog_spark__catalog="org.apache.iceberg.spark.SparkSessionCatalog" \
        PYSP_TEST_spark_sql_catalog_spark__catalog_type="hadoop" \
        PYSP_TEST_spark_sql_catalog_spark__catalog_warehouse="/tmp/spark-warehouse-$RANDOM" \
        ./run_pyspark_from_build.sh -m iceberg --iceberg
    elif [[ "$test_type" == "rest" ]]; then
      echo "!!! Running iceberg tests with rest catalog"
      ICEBERG_REST_JARS="org.apache.iceberg:iceberg-spark-runtime-${ICEBERG_SPARK_VER}_${SCALA_BINARY_VER}:${ICEBERG_VERSION},\
org.apache.iceberg:iceberg-aws-bundle:${ICEBERG_VERSION}"
          # filecache.enabled is a startup-only config, so it must be set here via
          # PYSP_TEST_ env var rather than as a session-level Spark config, because
          # FileCacheManager is initialized at executor startup time.
          # Some REST catalog defaults resolve Iceberg Parquet writes to gzip, which
          # is not supported by the GPU writer, so use a supported default codec.
          env \
            HOST_NAME=$PROJECT_REPO_HOST \
            EXPECTED_ICEBERG_VERSION=${ICEBERG_VERSION} \
            ICEBERG_TEST_CATALOG_TYPE="rest" \
            ICEBERG_TEST_REMOTE_CATALOG=1 \
            PYSP_TEST_spark_driver_memory=1G \
            PYSP_TEST_spark_executor_memory=2G \
            PYSP_TEST_spark_rapids_filecache_enabled=true \
            PYSP_TEST_spark_jars_packages="${ICEBERG_REST_JARS}" \
            PYSP_TEST_spark_jars_ivySettings="${WORKSPACE}/jenkins/ivysettings.xml" \
            PYSP_TEST_spark_sql_extensions="org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions" \
            PYSP_TEST_spark_sql_catalog_spark__catalog="org.apache.iceberg.spark.SparkSessionCatalog" \
            "PYSP_TEST_spark_sql_catalog_spark__catalog_catalog-impl=org.apache.iceberg.rest.RESTCatalog" \
            PYSP_TEST_spark_sql_catalog_spark__catalog_uri="${ICEBERG_REST_CATALOG_URI:-http://localhost:8181/catalog/}" \
            PYSP_TEST_spark_sql_catalog_spark__catalog_credential="${ICEBERG_REST_CREDENTIAL}" \
            "PYSP_TEST_spark_sql_catalog_spark__catalog_oauth2-server-uri=${ICEBERG_REST_OAUTH2_SERVER_URI:-http://localhost:8080/realms/iceberg/protocol/openid-connect/token}" \
            PYSP_TEST_spark_sql_catalog_spark__catalog_scope="${ICEBERG_REST_SCOPE:-lakekeeper}" \
            PYSP_TEST_spark_sql_catalog_spark__catalog_warehouse="${ICEBERG_REST_WAREHOUSE:-demo}" \
            "PYSP_TEST_spark_sql_catalog_spark__catalog_table-default_write_parquet_compression-codec=zstd" \
            "PYSP_TEST_spark_sql_catalog_spark__catalog_table-default_write_delete_parquet_compression-codec=zstd" \
            ./run_pyspark_from_build.sh -m iceberg --iceberg
    elif [[ "$test_type" == "s3tables" ]]; then
      echo "!!! Running iceberg tests with s3tables"
      # AWS deps versions for Spark 3.5.x and 4.0.x
      AWS_SDK_VERSION=${AWS_SDK_VERSION:-"2.29.26"}
      HADOOP_AWS_VERSION=${HADOOP_AWS_VERSION:-"3.3.4"}
      AWS_SDK_BUNDLE_VERSION=${AWS_SDK_BUNDLE_VERSION:-"1.12.709"}
      S3TABLES_CATALOG_VERSION=${S3TABLES_CATALOG_VERSION:-"0.1.6"}

      ICEBERG_S3TABLES_JARS="org.apache.iceberg:iceberg-spark-runtime-${ICEBERG_SPARK_VER}_${SCALA_BINARY_VER}:${ICEBERG_VERSION},\
software.amazon.s3tables:s3-tables-catalog-for-iceberg-runtime:${S3TABLES_CATALOG_VERSION},\
software.amazon.awssdk:apache-client:${AWS_SDK_VERSION},\
software.amazon.awssdk:aws-core:${AWS_SDK_VERSION},\
software.amazon.awssdk:dynamodb:${AWS_SDK_VERSION},\
software.amazon.awssdk:glue:${AWS_SDK_VERSION},\
software.amazon.awssdk:http-client-spi:${AWS_SDK_VERSION},\
software.amazon.awssdk:kms:${AWS_SDK_VERSION},\
software.amazon.awssdk:s3:${AWS_SDK_VERSION},\
software.amazon.awssdk:sdk-core:${AWS_SDK_VERSION},\
software.amazon.awssdk:sts:${AWS_SDK_VERSION},\
software.amazon.awssdk:url-connection-client:${AWS_SDK_VERSION},\
software.amazon.awssdk:s3tables:${AWS_SDK_VERSION},\
org.apache.hadoop:hadoop-aws:${HADOOP_AWS_VERSION},\
com.amazonaws:aws-java-sdk-bundle:${AWS_SDK_BUNDLE_VERSION}"

      # Requires to setup s3 buckets and namespaces to run iceberg s3tables tests.
      # These steps are included in the test pipeline.
      # Please refer to integration_tests/README.md#run-apache-iceberg-s3tables-tests
      # filecache.enabled is a startup-only config, so it must be set here via
      # PYSP_TEST_ env var rather than as a session-level Spark config, because
      # FileCacheManager is initialized at executor startup time.
      env \
        HOST_NAME=$PROJECT_REPO_HOST \
        EXPECTED_ICEBERG_VERSION=${ICEBERG_VERSION} \
        ICEBERG_TEST_REMOTE_CATALOG=1 \
        PYSP_TEST_spark_driver_memory=1G \
        PYSP_TEST_spark_executor_memory=2G \
        PYSP_TEST_spark_rapids_filecache_enabled=true \
        PYSP_TEST_spark_jars_packages="${ICEBERG_S3TABLES_JARS}" \
        PYSP_TEST_spark_jars_ivySettings="${WORKSPACE}/jenkins/ivysettings.xml" \
        PYSP_TEST_spark_sql_extensions="org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions" \
        PYSP_TEST_spark_sql_catalog_spark__catalog="org.apache.iceberg.spark.SparkSessionCatalog" \
        "PYSP_TEST_spark_sql_catalog_spark__catalog_catalog-impl=software.amazon.s3tables.iceberg.S3TablesCatalog" \
        PYSP_TEST_spark_sql_catalog_spark__catalog_warehouse="${S3TABLES_BUCKET_ARN}" \
        ./run_pyspark_from_build.sh -s -m iceberg --iceberg
    fi
  done
}

run_avro_tests() {
  # Workaround to avoid appending avro jar file by '--jars',
  # which would be addressed by https://github.com/NVIDIA/spark-rapids/issues/6532
  # Adding the Apache snapshots repository since we may be running with a snapshot
  # version of Apache Spark which requires accessing the snapshot repository to
  # fetch the spark-avro jar.
  rm -vf $LOCAL_JAR_PATH/spark-avro*.jar
  HOST_NAME=$PROJECT_REPO_HOST \
    PYSP_TEST_spark_jars_packages="org.apache.spark:spark-avro_${SCALA_BINARY_VER}:${SPARK_VER}" \
    PYSP_TEST_spark_jars_repositories="https://repository.apache.org/snapshots" \
    PYSP_TEST_spark_jars_ivySettings="${WORKSPACE}/jenkins/ivysettings.xml" \
    ./run_pyspark_from_build.sh -k avro
}

run_pyarrow_tests() {
  ./run_pyspark_from_build.sh -m pyarrow_test --pyarrow_test
}

run_other_join_modes_tests() {
  echo "HASH WITH POST JOIN TESTS"
  export PYSP_TEST_spark_rapids_sql_join_strategy=INNER_HASH_WITH_POST
  ./run_pyspark_from_build.sh -k 'join'

  echo "SORT MERGE JOIN TESTS"
  export PYSP_TEST_spark_rapids_sql_join_strategy=INNER_SORT_WITH_POST
  ./run_pyspark_from_build.sh -k 'join'
  # reset the config to the default in case other tests run with a join
  export PYSP_TEST_spark_rapids_sql_join_strategy=AUTO
}

run_non_utc_time_zone_tests() {
  # select one time zone according to current day of week
  source "${WORKSPACE}/jenkins/test-timezones.sh"
  time_zones_length=${#time_zones_test_cases[@]}
  # get day of week, Sunday is represented by 0 and Saturday by 6
  current_date=$(date +%w)
  echo "Current day of week is: ${current_date}"
  time_zone_index=$((current_date % time_zones_length))
  time_zone="${time_zones_test_cases[${time_zone_index}]}"
  echo "Run Non-UTC tests, time zone is ${time_zone}"

  # run tests
  TZ=${time_zone} ./run_pyspark_from_build.sh
}

# TEST_MODE
# - DEFAULT: all tests except cudf_udf tests
# - DELTA_LAKE_ONLY: Delta Lake tests only
# - ICEBERG_ONLY: iceberg tests only
# - ICEBERG_S3TABLES_ONLY: iceberg s3tables tests only
# - ICEBERG_REST_CATALOG_ONLY: iceberg rest catalog tests only
# - AVRO_ONLY: avro tests only (with --packages option instead of --jars)
# - CUDF_UDF_ONLY: cudf_udf tests only, requires extra conda cudf-py lib
# - MULTITHREADED_SHUFFLE: shuffle tests only using MULTITHREADED shuffle mode
# - UCX_SHUFFLE: shuffle tests only using UCX shuffle mode
# - NON_UTC_TZ: test all tests in a non-UTC time zone which is selected according to current day of week.
TEST_MODE=${TEST_MODE:-'DEFAULT'}
if [[ $TEST_MODE == "DEFAULT" ]]; then
  ./run_pyspark_from_build.sh

  SPARK_SHELL_SMOKE_TEST=1 \
  PYSP_TEST_spark_shuffle_manager=com.nvidia.spark.rapids.${SHUFFLE_SPARK_SHIM}.RapidsShuffleManager \
    ./run_pyspark_from_build.sh

  EXPLAIN_ONLY_CPU_SMOKE_TEST=1 \
    ./run_pyspark_from_build.sh

  # Spark Connect smoke test (available in Spark 3.5.6+)
  if printf '%s\n' "3.5.6" "$SPARK_VER" | sort -V | head -1 | grep -q "3.5.6"; then
    SPARK_CONNECT_SMOKE_TEST=1 \
      HOST_NAME=$PROJECT_REPO_HOST \
      PYSP_TEST_spark_jars_ivySettings=${WORKSPACE}/jenkins/ivysettings.xml \
      ./run_pyspark_from_build.sh
  fi

  # As '--packages' only works on the default cuda12 jar, it does not support classifiers
  # refer to issue : https://issues.apache.org/jira/browse/SPARK-20075
  # "$CLASSIFIER" == ''" is usually for the case run by developers,
  # while "$CLASSIFIER" == "cuda12" is for the case running on CI.
  # We expect to run packages test for both cases
  SKIP_PACKAGES_TESTS=${SKIP_PACKAGES_TESTS:-"false"}
  if { [[ "$CLASSIFIER" == "" || "$CLASSIFIER" == "cuda12" ]]; } && [[ "$SKIP_PACKAGES_TESTS" == "false" ]]; then
    # Add the ivysettings.xml file to support --packages downloads from Artifactory using credentials
    # Set the HOST_NAME variable for ivysettings.xml (e.g., from https://usr:psw@HOST_NAME/path/to/repo)
    SPARK_SHELL_SMOKE_TEST=1 HOST_NAME=$PROJECT_REPO_HOST \
    PYSP_TEST_spark_jars_packages=com.nvidia:rapids-4-spark_${SCALA_BINARY_VER}:${PROJECT_VER} \
    PYSP_TEST_spark_jars_repositories=${PROJECT_REPO} \
    PYSP_TEST_spark_jars_ivySettings=${WORKSPACE}/jenkins/ivysettings.xml \
      ./run_pyspark_from_build.sh
  fi

  # ParquetCachedBatchSerializer cache_test
  PYSP_TEST_spark_sql_cache_serializer=com.nvidia.spark.ParquetCachedBatchSerializer \
    ./run_pyspark_from_build.sh -k cache_test
fi

# Delta Lake tests
if [[ "$TEST_MODE" == "DEFAULT" || "$TEST_MODE" == "DELTA_LAKE_ONLY" ]]; then
  run_delta_lake_tests
fi

# Iceberg tests
# TODO: https://github.com/NVIDIA/spark-rapids/issues/13885
if [[ "$TEST_MODE" == "ICEBERG_ONLY" ]]; then
  run_iceberg_tests
fi

# Iceberg s3tables tests
if [[ "$TEST_MODE" == "ICEBERG_S3TABLES_ONLY" ]]; then
  run_iceberg_tests 's3tables'
fi

# Iceberg rest tests
if [[ "$TEST_MODE" == "ICEBERG_REST_CATALOG_ONLY" ]]; then
  run_iceberg_tests 'rest'
fi

# Avro tests
if [[ "$TEST_MODE" == "DEFAULT" || "$TEST_MODE" == "AVRO_ONLY" ]]; then
  run_avro_tests
fi

# Rapids Shuffle Manager smoke test.
# Note the TEST_MODE is either DEFAULT or MULTITHREADED_SHUFFLE. The later is
# a misleading, it actually tests the Rapids Shuffle Manager with both UCX and
# MULTITHREADED shuffle modes, but was kept to not break possible CI that is
# using it.
if [[ "$TEST_MODE" == "DEFAULT" || "$TEST_MODE" == "MULTITHREADED_SHUFFLE" ]]; then
  invoke_shuffle_integration_test MULTITHREADED ./run_pyspark_from_build.sh
fi

if [[ "$TEST_MODE" == "UCX_SHUFFLE" ]]; then
  invoke_shuffle_integration_test UCX ./run_pyspark_from_build.sh
fi

# cudf_udf test: this mostly depends on cudf-py, so we run it into an independent CI
if [[ "$TEST_MODE" == "CUDF_UDF_ONLY" ]]; then
  # Create a separate conda env for cudf-udf tests to avoid affecting the base env (python 3.10)
  CUDF_UDF_ENV="cudf_udf_$(date +"%Y%m%d")"
  CUDF_UDF_PYTHON_VER="3.12"  # since 26.04, python 3.12+ is required for cudf-py
  CUDF_VER=$(echo "${PROJECT_VER}" | cut -d '.' -f 1,2)
  CUDA_VER_FOR_CUDF=${CUDA_VER_FOR_CUDF:-'12.9'}

  conda create -y -n ${CUDF_UDF_ENV} -c rapidsai-nightly -c nvidia -c conda-forge -c defaults \
    python=${CUDF_UDF_PYTHON_VER} pip cudf=${CUDF_VER} cuda-version=${CUDA_VER_FOR_CUDF}

  # Activate the cudf_udf env and reset PYTHONPATH to use the new env's site-packages
  source activate ${CUDF_UDF_ENV}
  CUDF_UDF_PYTHON="${CONDA_ROOT}/envs/${CUDF_UDF_ENV}/bin/python"
  # Save original PYTHONPATH and reset for cudf_udf env
  ORIG_PYTHONPATH=${PYTHONPATH}
  CUDF_UDF_SITE_PACKAGES="${CONDA_ROOT}/envs/${CUDF_UDF_ENV}/lib/python${CUDF_UDF_PYTHON_VER}/site-packages"
  export PYTHONPATH="${CUDF_UDF_SITE_PACKAGES}:${TMP_PYTHON}/python:${TMP_PYTHON}/python/pyspark/:${PY4J_FILE}"
  # Set PySpark to use the cudf_udf Python for both driver and workers
  export PYSPARK_PYTHON=${CUDF_UDF_PYTHON}
  export PYSPARK_DRIVER_PYTHON=${CUDF_UDF_PYTHON}
  python -m pip install -r ./requirements.txt

  # hardcode config
  [[ ${TEST_PARALLEL} -gt 2 ]] && export TEST_PARALLEL=2
  PYSP_TEST_spark_rapids_memory_gpu_allocFraction=0.1 \
    PYSP_TEST_spark_rapids_memory_gpu_minAllocFraction=0 \
    PYSP_TEST_spark_rapids_python_memory_gpu_allocFraction=0.1 \
    PYSP_TEST_spark_rapids_python_concurrentPythonWorkers=2 \
    PYSP_TEST_spark_executorEnv_PYTHONPATH=${RAPIDS_PLUGIN_JAR} \
    ./run_pyspark_from_build.sh -m cudf_udf --cudf_udf

  # Deactivate and restore original environment
  conda deactivate
  export PYTHONPATH=${ORIG_PYTHONPATH}
  unset PYSPARK_PYTHON PYSPARK_DRIVER_PYTHON
fi

# Pyarrow tests
if [[ "$TEST_MODE" == "DEFAULT" || "$TEST_MODE" == "PYARROW_ONLY" ]]; then
  run_pyarrow_tests
fi

# TODO: https://github.com/NVIDIA/spark-rapids/issues/13854
if [[ "$TEST_MODE" == "EXTRA_JOIN_ONLY" ]]; then
  run_other_join_modes_tests
fi

# Non-UTC time zone tests
if [[ "$TEST_MODE" == "NON_UTC_TZ" ]]; then
  run_non_utc_time_zone_tests
fi

# hybrid execution tests
if [[ "$TEST_MODE" == "DEFAULT" || "$TEST_MODE" == "HYBRID_EXECUTION" ]]; then
  source "${WORKSPACE}/jenkins/hybrid_execution.sh"
  if hybrid_prepare ; then
    LOAD_HYBRID_BACKEND=1 ./run_pyspark_from_build.sh -m hybrid_test
  fi
fi

popd
stop-worker.sh
stop-master.sh
