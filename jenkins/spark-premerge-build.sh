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

set -ex

BUILD_TYPE=all

if [[ $# -eq 1 ]]; then
    BUILD_TYPE=$1

elif [[ $# -gt 1 ]]; then
    >&2 echo "ERROR: too many parameters are provided"
    exit 1
fi

CUDA_CLASSIFIER=${CUDA_CLASSIFIER:-'cuda12'}
CLASSIFIER=${CLASSIFIER:-"$CUDA_CLASSIFIER"} # default as CUDA_CLASSIFIER for compatibility
MVN_SETTINGS=${MVN_SETTINGS:-"jenkins/settings.xml"}
MVN=${MVN:-"mvn -s $MVN_SETTINGS -Dmaven.wagon.http.retryHandler.count=3"}
MVN_BUILD_ARGS="-Drat.skip=true -Dmaven.scaladoc.skip -Dmaven.scalastyle.skip=true -Dcuda.version=$CLASSIFIER"

mvn_verify() {
    echo "Run mvn verify..."
    export JAVA_HOME=$(echo /usr/lib/jvm/java-1.17.0-*)
    update-java-alternatives --set $JAVA_HOME
    java -version

    # Download a Scala 2.12 build of spark
    prepare_spark $SPARK_VER 2.12

    # get merge BASE from merged pull request. Log message e.g. "Merge HEAD into BASE"
    BASE_REF=$(git --no-pager log --oneline -1 | awk '{ print $NF }')
    # file size check for pull request. The size of a committed file should be less than 1.5MiB
    pre-commit run check-added-large-files --from-ref $BASE_REF --to-ref HEAD

    MVN_INSTALL_CMD="env -u SPARK_HOME $MVN -U -B $MVN_URM_MIRROR clean install $MVN_BUILD_ARGS -DskipTests -pl aggregator -am"

    # For snapshot versions, we do mvn[compile,RAT,scalastyle,docgen] CI in github actions
    for version in "${SPARK_SHIM_VERSIONS_NOSNAPSHOTS_TAIL[@]}"
    do
        echo "Spark version: $version"
        # build and run unit tests on one specific version for each sub-version (e.g. 330) except base version
        # separate the versions to two ci stages (mvn_verify, ci_2) for balancing the duration
        match=1
        for element in "${SPARK_SHIM_VERSIONS_PREMERGE_UT_1[@]}"; do
            if [[ "$element" == "$version" ]]; then
                match=0
                break
            fi
        done
        if [[ $match == 0 ]]; then
            env -u SPARK_HOME \
              $MVN -U -B $MVN_URM_MIRROR -Dbuildver=$version clean install $MVN_BUILD_ARGS -Dpytest.TEST_TAGS=''
            # Run filecache tests
            env -u SPARK_HOME SPARK_CONF=spark.rapids.filecache.enabled=true \
              $MVN -B $MVN_URM_MIRROR -Dbuildver=$version test -rf tests $MVN_BUILD_ARGS -Dpytest.TEST_TAGS='' \
              -DwildcardSuites=org.apache.spark.sql.rapids.filecache.FileCacheIntegrationSuite
        # build only for other versions
        # elif [[ "${SPARK_SHIM_VERSIONS_NOSNAPSHOTS_TAIL[@]}" =~ "$version" ]]; then
        #     $MVN_INSTALL_CMD -DskipTests -Dbuildver=$version
        fi
    done
    # build base shim version for following test step with PREMERGE_PROFILES
    $MVN_INSTALL_CMD -DskipTests -Dbuildver=$SPARK_BASE_SHIM_VERSION

    # enable UTF-8 for regular expression tests
    for version in "${SPARK_SHIM_VERSIONS_PREMERGE_UTF8[@]}"
    do
        echo "Spark version (regex): $version"
        env -u SPARK_HOME LC_ALL="en_US.UTF-8" $MVN $MVN_URM_MIRROR -Dbuildver=$version package $MVN_BUILD_ARGS \
          -Dpytest.TEST_TAGS='' \
          -DwildcardSuites=com.nvidia.spark.rapids.ConditionalsSuite,com.nvidia.spark.rapids.RegularExpressionSuite,com.nvidia.spark.rapids.RegularExpressionTranspilerSuite
    done

    # Here run Python integration tests tagged with 'premerge_ci_1' only, that would help balance test duration and memory
    # consumption from two k8s pods running in parallel, which executes 'mvn_verify()' and 'ci_2()' respectively.
    $MVN -B $MVN_URM_MIRROR $PREMERGE_PROFILES clean verify -Dpytest.TEST_TAGS="premerge_ci_1" \
        -Dpytest.TEST_TYPE="pre-commit" -Dcuda.version=$CLASSIFIER

    # The jacoco coverage should have been collected, but because of how the shade plugin
    # works and jacoco we need to clean some things up so jacoco will only report for the
    # things we care about
    SPK_VER=${JACOCO_SPARK_VER:-"330"}
    mkdir -p target/jacoco_classes/
    FILE=$(ls dist/target/rapids-4-spark_2.12-*.jar | grep -v test | xargs readlink -f)
    UDF_JAR=$(ls ./udf-compiler/target/spark${SPK_VER}/rapids-4-spark-udf_2.12-*-spark${SPK_VER}.jar | grep -v test | xargs readlink -f)
    pushd target/jacoco_classes/
    jar xf $FILE com org rapids spark-shared "spark${JACOCO_SPARK_VER:-330}/"
    # extract the .class files in udf jar and replace the existing ones in spark3xx-ommon and spark$SPK_VER
    # because the class files in udf jar will be modified in aggregator's shade phase
    jar xf "$UDF_JAR" com/nvidia/spark/udf
    rm -rf com/nvidia/shaded/ \
      org/openucx/ spark-shared/com/nvidia/spark/udf/ \
      spark${SPK_VER}/com/nvidia/spark/udf/ \
      spark${SPK_VER}/META-INF/versions/*
    popd

    # Triggering here until we change the jenkins file
    rapids_shuffle_smoke_test

    # Test a portion of cases for non-UTC time zone because of limited GPU resources.
    # Here testing: parquet scan, orc scan, csv scan, cast, TimeZoneAwareExpression, FromUTCTimestamp
    # Nightly CIs will cover all the cases.
    source "$(dirname "$0")"/test-timezones.sh
    for tz in "${time_zones_test_cases[@]}"
    do
        TZ=$tz ./integration_tests/run_pyspark_from_build.sh -m tz_sensitive_test
    done

    # test Hybrid feature
    source "${WORKSPACE}/jenkins/hybrid_execution.sh"
    if hybrid_prepare ; then
        LOAD_HYBRID_BACKEND=1 ./integration_tests/run_pyspark_from_build.sh -m hybrid_test
    fi
}

rapids_shuffle_smoke_test() {
    local SPARK_VER=${1:-${SPARK_VER}}
    echo "Run rapids_shuffle_smoke_test for Spark version: $SPARK_VER..."

    # basic ucx check
    ucx_info -d

    # run in standalone mode
    export SPARK_MASTER_HOST=localhost
    export SPARK_MASTER=spark://$SPARK_MASTER_HOST:7077
    $SPARK_HOME/sbin/start-master.sh -h $SPARK_MASTER_HOST
    $SPARK_HOME/sbin/spark-daemon.sh start org.apache.spark.deploy.worker.Worker 1 $SPARK_MASTER

    # using UCX shuffle
    invoke_shuffle_integration_test UCX ./integration_tests/run_pyspark_from_build.sh premerge $SPARK_VER

    # using MULTITHREADED shuffle
    invoke_shuffle_integration_test MULTITHREADED ./integration_tests/run_pyspark_from_build.sh premerge $SPARK_VER

    $SPARK_HOME/sbin/spark-daemon.sh stop org.apache.spark.deploy.worker.Worker 1
    $SPARK_HOME/sbin/stop-master.sh
}

run_iceberg_version_detect_tests() {
    local spark_ver=${1:?'spark_ver is required'}
    local scala_ver=${2:?'scala_ver is required'}
    echo "Running Iceberg version detection tests for Spark $spark_ver (Scala $scala_ver)..."

    local iceberg_spark_ver
    iceberg_spark_ver=$(echo "$spark_ver" | cut -d. -f1,2)
    local spark_patch_ver
    spark_patch_ver=$(echo "$spark_ver" | cut -d. -f3)

    if [[ "$iceberg_spark_ver" != "3.5" && "$iceberg_spark_ver" != "4.0" \
          && "$iceberg_spark_ver" != "4.1" ]]; then
        echo "!!!! Skipping Iceberg version detection. Not supported on Spark $iceberg_spark_ver"
        return 0
    fi

    # Supported Iceberg versions per Spark version. The 3.5.x / 4.0.x rows mirror
    # run_iceberg_tests() in spark-tests.sh. The Spark 4.1 -> 1.11.0 row is kept here
    # for callers that explicitly test Spark 4.1, while the regular pre-merge job
    # below runs on Spark 4.0.1. Spark 4.1 is covered by nightly run_iceberg_tests().
    local iceberg_versions
    if [[ "$iceberg_spark_ver" == "4.1" ]]; then
        iceberg_versions="1.11.0"
    elif [[ "$iceberg_spark_ver" == "4.0" ]]; then
        if [[ "$spark_patch_ver" -ge 2 ]]; then
            iceberg_versions="1.10.1 1.11.0"
        else
            iceberg_versions="1.10.1"
        fi
    elif [[ "$spark_patch_ver" -le 3 ]]; then
        iceberg_versions="1.6.1"
    else
        iceberg_versions="1.9.2 1.10.1"
    fi

    for ICEBERG_VERSION in $iceberg_versions; do
        echo "!!! Running iceberg version detection test for Iceberg $ICEBERG_VERSION"
        EXPECTED_ICEBERG_VERSION=${ICEBERG_VERSION} \
        PYSP_TEST_spark_jars_packages=org.apache.iceberg:iceberg-spark-runtime-${iceberg_spark_ver}_${scala_ver}:${ICEBERG_VERSION} \
            PYSP_TEST_spark_sql_extensions="org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions" \
            PYSP_TEST_spark_sql_catalog_spark__catalog="org.apache.iceberg.spark.SparkSessionCatalog" \
            PYSP_TEST_spark_sql_catalog_spark__catalog_type="hadoop" \
            PYSP_TEST_spark_sql_catalog_spark__catalog_warehouse="/tmp/spark-warehouse-$RANDOM" \
            ./integration_tests/run_pyspark_from_build.sh -m iceberg --iceberg -k test_iceberg_version_detection
    done
}

ci_2() {
    echo "Run premerge ci 2 testings..."
    export JAVA_HOME=$(echo /usr/lib/jvm/java-1.17.0-*)
    update-java-alternatives --set $JAVA_HOME
    java -version

    $MVN -U -B $MVN_URM_MIRROR clean package $MVN_BUILD_ARGS -DskipTests=true
    export TEST_TAGS="not premerge_ci_1"
    export TEST_TYPE="pre-commit"

    # Download a Scala 2.12 build of spark
    prepare_spark $SPARK_VER 2.12
    ./integration_tests/run_pyspark_from_build.sh

    # enable avro test separately
    INCLUDE_SPARK_AVRO_JAR=true TEST='avro_test.py' ./integration_tests/run_pyspark_from_build.sh
    # export 'LC_ALL' to set locale with UTF-8 so regular expressions are enabled
    LC_ALL="en_US.UTF-8" TEST="regexp_test.py" ./integration_tests/run_pyspark_from_build.sh

    # put some mvn tests here to balance durations of parallel stages
    echo "Run mvn package..."
    for version in "${SPARK_SHIM_VERSIONS_PREMERGE_UT_2[@]}"
    do
        env -u SPARK_HOME $MVN -U -B $MVN_URM_MIRROR -Dbuildver=$version clean package $MVN_BUILD_ARGS \
          -Dpytest.TEST_TAGS=''
    done
}

ci_scala213() {
    echo "Run premerge ci (Scala 2.13) testing..."
    export JAVA_HOME=$(echo /usr/lib/jvm/java-1.17.0-*)
    update-java-alternatives --set $JAVA_HOME
    java -version

    # build Scala 2.13 versions
    for version in "${SPARK_SHIM_VERSIONS_PREMERGE_SCALA213[@]}"
    do
        echo "Spark version (Scala 2.13): $version"
        env -u SPARK_HOME \
            $MVN -f scala2.13/ -U -B $MVN_URM_MIRROR -Dbuildver=$version clean install $MVN_BUILD_ARGS -Dpytest.TEST_TAGS=''
        # Run filecache tests
        env -u SPARK_HOME SPARK_CONF=spark.rapids.filecache.enabled=true \
            $MVN -f scala2.13/ -B $MVN_URM_MIRROR -Dbuildver=$version test -rf tests $MVN_BUILD_ARGS -Dpytest.TEST_TAGS='' \
            -DwildcardSuites=org.apache.spark.sql.rapids.filecache.FileCacheIntegrationSuite
    done

    # Download a Scala 2.13 version of Spark (use Spark 4.0.1 for Spark 4 shuffle testing)
    # don't leak the new version here, it's only for use within this function
    local SPARK_VER=4.0.1
    local buildver="${SPARK_VER//./}"
    prepare_spark $SPARK_VER 2.13

    # We are going to run integration tests against Spark 4.0.1
    $MVN -f scala2.13/ -U -B $MVN_URM_MIRROR -Dbuildver=$buildver clean package $MVN_BUILD_ARGS -DskipTests=true

    export TEST_TAGS="not premerge_ci_1"
    export TEST_TYPE="pre-commit"
    # SPARK_HOME (and related) must be set to a Spark built with Scala 2.13
    SPARK_HOME=$SPARK_HOME PYTHONPATH=$PYTHONPATH \
        ./integration_tests/run_pyspark_from_build.sh
    # enable avro test separately
    SPARK_HOME=$SPARK_HOME PYTHONPATH=$PYTHONPATH \
        INCLUDE_SPARK_AVRO_JAR=true TEST='avro_test.py' ./integration_tests/run_pyspark_from_build.sh
    # export 'LC_ALL' to set locale with UTF-8 so regular expressions are enabled
    SPARK_HOME=$SPARK_HOME PYTHONPATH=$PYTHONPATH \
        LC_ALL="en_US.UTF-8" TEST="regexp_test.py" ./integration_tests/run_pyspark_from_build.sh

    # Trigger the RapidsShuffleManager tests for scala 2.13
    rapids_shuffle_smoke_test $SPARK_VER

    # Iceberg version detection (requires JDK 17, available in this stage).
    # Moved out of spark-tests.sh DEFAULT mode where JDK 8 causes
    # UnsupportedClassVersionError for Iceberg 1.9+ runtime JARs.
    run_iceberg_version_detect_tests $SPARK_VER 2.13

}

prepare_spark() {
    spark_version=${1:-'3.3.0'}
    scala_ver=${2:-'2.12'}

    ARTF_ROOT="$(pwd)/.download"
    rm -rf $ARTF_ROOT && mkdir -p $ARTF_ROOT
    # Download a full version of spark
    . jenkins/hadoop-def.sh $spark_version $scala_ver
    wget -P $ARTF_ROOT $SPARK_REPO/org/apache/spark/$spark_version/spark-$spark_version-$BIN_HADOOP_VER.tgz

    export SPARK_HOME="$ARTF_ROOT/spark-$spark_version-$BIN_HADOOP_VER"
    export PATH="$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH"
    tar zxf $SPARK_HOME.tgz -C $ARTF_ROOT && rm -f $SPARK_HOME.tgz
    # copy python path libs to container /tmp instead of workspace to avoid ephemeral PVC issue
    TMP_PYTHON=/tmp/$(date +"%Y%m%d")
    rm -rf $TMP_PYTHON && cp -r $SPARK_HOME/python $TMP_PYTHON
    export PYTHONPATH=$TMP_PYTHON/python:$TMP_PYTHON/python/pyspark/:$(echo -n $TMP_PYTHON/python/lib/py4j-*-src.zip)
}

nvidia-smi

. jenkins/version-def.sh
. jenkins/shuffle-common.sh

PREMERGE_PROFILES="-Ppre-merge"

case $BUILD_TYPE in

    all)
        echo "Run all testings..."
        mvn_verify
        ci_2
        ci_scala213
        ;;

    mvn_verify)
        mvn_verify
        ;;

    ci_2 )
        ci_2
        ;;

    ci_scala213 )
        ci_scala213
        ;;

    *)
        echo "ERROR: unknown parameter: $BUILD_TYPE"
        ;;
esac
