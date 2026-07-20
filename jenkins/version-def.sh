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

set -e

# Split abc=123 from $OVERWRITE_PARAMS
# $OVERWRITE_PARAMS patten 'abc=123;def=456;'
PRE_IFS=$IFS
IFS=";"
for VAR in $OVERWRITE_PARAMS; do
    echo $VAR && export $VAR
done
IFS=$PRE_IFS

if [[ -z "${MVN:-}" && -n "${MVN_SETTINGS:-}" ]]; then
    MVN="mvn -s $MVN_SETTINGS"
else
    MVN=${MVN:-"mvn"}
fi

GLUTEN_VERSION=${GLUTEN_VERSION:-"1.2.0"}
# TODO: https://github.com/NVIDIA/spark-rapids/issues/12278
SUPPORTED_HYBRID_SHIMS=${SUPPORTED_HYBRID_SHIMS:-"3.2 3.3 3.4"}
CUDA_CLASSIFIER=${CUDA_CLASSIFIER:-"cuda12"}
CLASSIFIER=${CLASSIFIER:-"$CUDA_CLASSIFIER"} # default as CUDA_CLASSIFIER for compatibility
PROJECT_VER=${PROJECT_VER:-"26.08.0-SNAPSHOT"}
PROJECT_TEST_VER=${PROJECT_TEST_VER:-"26.08.0-SNAPSHOT"}
SPARK_VER=${SPARK_VER:-"3.3.0"}
SPARK_VER_213=${SPARK_VER_213:-"3.5.0"}
SCALA_BINARY_VER=${SCALA_BINARY_VER:-"2.12"}
SERVER_ID=${SERVER_ID:-"snapshots"}

PROJECT_REPO=${PROJECT_REPO:-"$ART_URL"}
PROJECT_TEST_REPO=${PROJECT_TEST_REPO:-"$ART_URL"}
SPARK_REPO=${SPARK_REPO:-"$ART_URL"}

echo "CUDA_CLASSIFIER: $CUDA_CLASSIFIER, CLASSIFIER: $CLASSIFIER, PROJECT_VER: $PROJECT_VER \
    SPARK_VER: $SPARK_VER, SCALA_BINARY_VER: $SCALA_BINARY_VER"

# Spark shim versions
# get Spark shim versions from pom
function set_env_var_SPARK_SHIM_VERSIONS_ARR() {
    PROFILE_OPT=$1
    SPARK_SHIM_VERSIONS_STR=$(echo -n $($MVN -B help:evaluate -q -pl dist $PROFILE_OPT -Dexpression=included_buildvers -DforceStdout))
    IFS=", " <<< $SPARK_SHIM_VERSIONS_STR read -r -a SPARK_SHIM_VERSIONS_ARR
}

function set_env_var_SPARK_SHIM_VERSIONS_ARR_FROM_PROFILES() {
   versionStr=$(python build/get_buildvers.py $1 $2)
   SPARK_SHIM_VERSIONS_STR=$(echo -n $versionStr)
   IFS=", " <<< $SPARK_SHIM_VERSIONS_STR read -r -a SPARK_SHIM_VERSIONS_ARR
}

pom="pom.xml"
if [[ $SCALA_BINARY_VER == "2.13" ]]; then
    pom="scala2.13/pom.xml"
fi

# snapshots: snapshots + noSnapshots
set_env_var_SPARK_SHIM_VERSIONS_ARR_FROM_PROFILES "snap_and_no_snap" "$pom"
SPARK_SHIM_VERSIONS_SNAPSHOTS=("${SPARK_SHIM_VERSIONS_ARR[@]}")
# noSnapshots: noSnapshots only
set_env_var_SPARK_SHIM_VERSIONS_ARR_FROM_PROFILES "no_snapshots" "$pom"
SPARK_SHIM_VERSIONS_NOSNAPSHOTS=("${SPARK_SHIM_VERSIONS_ARR[@]}")
# snapshotOnly : snapshots only
set_env_var_SPARK_SHIM_VERSIONS_ARR_FROM_PROFILES "snapshots" "$pom"
SPARK_SHIM_VERSIONS_SNAPSHOTS_ONLY=("${SPARK_SHIM_VERSIONS_ARR[@]}")

# PHASE_TYPE: CICD phase at which the script is called, to specify Spark shim versions.
# regular: noSnapshots + snapshots
# pre-release: noSnapshots only
# *: shim versions to build, e.g., PHASE_TYPE="330 331"
PHASE_TYPE=${PHASE_TYPE:-"regular"}
case $PHASE_TYPE in
    # SPARK_SHIM_VERSIONS will be used for nightly artifact build
    pre-release)
        SPARK_SHIM_VERSIONS=("${SPARK_SHIM_VERSIONS_NOSNAPSHOTS[@]}")
        ;;

    regular)
        SPARK_SHIM_VERSIONS=("${SPARK_SHIM_VERSIONS_SNAPSHOTS[@]}")
        ;;

    *)
        IFS=" " read -ra SPARK_SHIM_VERSIONS <<< $PHASE_TYPE
        ;;
esac
# base version
SPARK_BASE_SHIM_VERSION=${SPARK_SHIM_VERSIONS[0]}
# tail snapshots
SPARK_SHIM_VERSIONS_SNAPSHOTS_TAIL=("${SPARK_SHIM_VERSIONS_SNAPSHOTS[@]:1}")
# tail noSnapshots
SPARK_SHIM_VERSIONS_NOSNAPSHOTS_TAIL=("${SPARK_SHIM_VERSIONS_NOSNAPSHOTS[@]:1}")
# build and run unit tests on one specific version for each sub-version (e.g. 330, 340)
# separate the versions to two parts (premergeUT1, premergeUT2) for balancing the duration
set_env_var_SPARK_SHIM_VERSIONS_ARR -PpremergeUT1
SPARK_SHIM_VERSIONS_PREMERGE_UT_1=("${SPARK_SHIM_VERSIONS_ARR[@]}")
set_env_var_SPARK_SHIM_VERSIONS_ARR -PpremergeUT2
SPARK_SHIM_VERSIONS_PREMERGE_UT_2=("${SPARK_SHIM_VERSIONS_ARR[@]}")
# utf-8 cases
set_env_var_SPARK_SHIM_VERSIONS_ARR -PpremergeUTF8
SPARK_SHIM_VERSIONS_PREMERGE_UTF8=("${SPARK_SHIM_VERSIONS_ARR[@]}")
# scala 2.13 cases
set_env_var_SPARK_SHIM_VERSIONS_ARR -PpremergeScala213
SPARK_SHIM_VERSIONS_PREMERGE_SCALA213=("${SPARK_SHIM_VERSIONS_ARR[@]}")
# jdk11 cases
set_env_var_SPARK_SHIM_VERSIONS_ARR -Pjdk11-test
SPARK_SHIM_VERSIONS_JDK11=("${SPARK_SHIM_VERSIONS_ARR[@]}")
# jdk17 cases
set_env_var_SPARK_SHIM_VERSIONS_ARR -Pjdk17-test
SPARK_SHIM_VERSIONS_JDK17=("${SPARK_SHIM_VERSIONS_ARR[@]}")
# jdk17 scala213 cases
set_env_var_SPARK_SHIM_VERSIONS_ARR -Pjdk17-scala213-test
SPARK_SHIM_VERSIONS_JDK17_SCALA213=("${SPARK_SHIM_VERSIONS_ARR[@]}")
# databricks shims
set_env_var_SPARK_SHIM_VERSIONS_ARR_FROM_PROFILES "databricks" "$pom"
SPARK_SHIM_VERSIONS_DATABRICKS=("${SPARK_SHIM_VERSIONS_ARR[@]}")

echo "SPARK_BASE_SHIM_VERSION: $SPARK_BASE_SHIM_VERSION"
