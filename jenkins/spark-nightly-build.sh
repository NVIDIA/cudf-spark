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

## MVN_OPT : maven options environment, e.g. MVN_OPT='-Dspark-rapids-jni.version=xxx' to specify spark-rapids-jni dependency's version.
MVN_SETTINGS=${MVN_SETTINGS:-"jenkins/settings.xml"}
export MVN="mvn -s $MVN_SETTINGS -Dmaven.wagon.http.retryHandler.count=3 -DretryFailedDeploymentCount=3 ${MVN_OPT} -Psource-javadoc"

. jenkins/version-def.sh

# Set JDK 17 as the default for nightly builds across both:
# scala2.12 and scala2.13 (with maven.compiler.source as 1.8)
export JAVA_HOME=$(echo /usr/lib/jvm/java-1.17.0-*)
update-java-alternatives --set $JAVA_HOME
$MVN -version

DIST_PL="dist"
DIST_PATH="$DIST_PL" # The path of the dist module is used only outside of the mvn cmd
SCALA_BINARY_VER=${SCALA_BINARY_VER:-"2.12"}
if [ $SCALA_BINARY_VER == "2.13" ]; then
    export MVN="$MVN -f scala2.13/"
    DIST_PATH="scala2.13/$DIST_PL"
fi

export WORKSPACE=${WORKSPACE:-$(pwd)}
## export 'M2DIR' so that shims can get the correct Spark dependency info
export M2DIR=${M2DIR:-"$WORKSPACE/.m2"}
## DEV_MODE: if true, copy M2DIR to SHIM_M2DIR for dev CI job
export DEV_MODE=${DEV_MODE:-'false'}

function mvnEval {
    $MVN help:evaluate -q -pl $DIST_PL $MVN_URM_MIRROR -Prelease330 -Dmaven.repo.local=$M2DIR -DforceStdout -Dexpression=$1
}

ART_ID=$(mvnEval project.artifactId)
ART_GROUP_ID=$(mvnEval project.groupId)
ART_VER=$(mvnEval project.version)
export DEFAULT_CUDA_CLASSIFIER=${DEFAULT_CUDA_CLASSIFIER:-$(mvnEval cuda.version)} # default cuda version
CUDA_CLASSIFIERS=${CUDA_CLASSIFIERS:-"$DEFAULT_CUDA_CLASSIFIER"} # e.g. cuda12
CLASSIFIERS=${CLASSIFIERS:-"$CUDA_CLASSIFIERS"}  # default as CUDA_CLASSIFIERS for compatibility
IFS=',' read -ra CLASSIFIERS_ARR <<< "$CLASSIFIERS"

export TMP_PATH="/tmp/$(date '+%Y%m%d')-$$"
mkdir -p ${TMP_PATH}

DIST_FPATH="$DIST_PL/target/$ART_ID-$ART_VER-$DEFAULT_CUDA_CLASSIFIER"
DIST_POM_FPATH="$DIST_PL/target/parallel-world/META-INF/maven/$ART_GROUP_ID/$ART_ID/pom.xml"

DIST_PROFILE_OPT=-Dincluded_buildvers=$(IFS=,; echo "${SPARK_SHIM_VERSIONS[*]}")
DIST_INCLUDES_DATABRICKS=${DIST_INCLUDES_DATABRICKS:-"true"}
if [[ "$DIST_INCLUDES_DATABRICKS" == "true" ]] && [[ -n ${SPARK_SHIM_VERSIONS_DATABRICKS[*]} ]]; then
    DIST_PROFILE_OPT="$DIST_PROFILE_OPT,"$(IFS=,; echo "${SPARK_SHIM_VERSIONS_DATABRICKS[*]}")
fi

# Make sure that the local m2 repo on the build machine has the same pom
# installed as the one being pushed to the remote repo. This to prevent
# discrepancies between the build machines regardless of how the local repo was populated.
function distWithReducedPom {
    cmd="$1"

    case $cmd in

        install)
            mvnCmd="install:install-file"
            mvnExtraFlags="-Dpackaging=jar"
            ;;
        *)
            echo "Unknown command: $cmd"
            ;;
    esac

    $MVN -B $mvnCmd $MVN_URM_MIRROR \
        -Dcuda.version=$DEFAULT_CUDA_CLASSIFIER \
        -Dmaven.repo.local=$M2DIR \
        -Dfile="${DIST_FPATH}.jar" \
        -DpomFile="${DIST_POM_FPATH}" \
        -DgroupId="${ART_GROUP_ID}" \
        -DartifactId="${ART_ID}" \
        -Dversion="${ART_VER}" \
        $mvnExtraFlags
}

function build_shim() {
  local BUILD_VER=$1
  local SLOT_ID=$2
  # reuse slot workspace and maven cache to save time
  local SHIM_WORKSPACE="${TMP_PATH}/shim/${SLOT_ID}"
  local SHIM_M2DIR="${SHIM_WORKSPACE}/.m2"

  local CODE_PATH="${SHIM_WORKSPACE}/workspace"
  if [ ! -d "${CODE_PATH}" ]; then
    mkdir -p "${SHIM_WORKSPACE}"
    cp -r "${WORKSPACE}/" "${CODE_PATH}"
  fi

  # update SHIM_M2DIR for dev CI job
  if [[ "$DEV_MODE" == "true" ]]; then
    SHIM_M2DIR=${CODE_PATH}/.m2
  fi
  
  cd "${CODE_PATH}"
  echo "Workspace at ${CODE_PATH}..."

  local BUILD_CMD="$MVN -U -B clean install $MVN_URM_MIRROR -Dmaven.repo.local=$SHIM_M2DIR \
    -Dcuda.version=$DEFAULT_CUDA_CLASSIFIER \
    -DskipTests -Drat.skip=true -Djava.io.tmpdir=${SHIM_WORKSPACE} \
    -Dbuildver=${BUILD_VER}"

  echo "Building ${BUILD_VER}"
  BUILD_LOG="${SHIM_WORKSPACE}/build-${BUILD_VER}.log"

  $BUILD_CMD > "${BUILD_LOG}" 2>&1
  CODE="$?"
  if [[ $CODE != "0" ]]; then
    tail -1000 "${BUILD_LOG}" || true
    return $CODE
  fi

  ( # use flock to prevent maven local repository contention across all parallel builds
    flock -x -w 300 200 || { echo "Lock acquisition failed"; exit 1; }

    echo "Copying sql-plugin-api,aggregator to .m2 repo..."
    for mod in \
      "rapids-4-spark-sql-plugin-api_${SCALA_BINARY_VER}" "rapids-4-spark-aggregator_${SCALA_BINARY_VER}"; do
      SRC_DIR="${SHIM_M2DIR}/com/nvidia/${mod}/${ART_VER}"
      DEST_DIR="${M2DIR}/com/nvidia/${mod}/${ART_VER}"

      if [[ -d "$SRC_DIR" ]]; then
        mkdir -p "$DEST_DIR"
        rsync -av --checksum --ignore-existing "${SRC_DIR}/" "${DEST_DIR}/"
      fi
    done
  ) 200>"${M2DIR}/.copy.lock"

  if [[ $SKIP_DEPLOY != 'true' ]]; then
    local DEPLOY_CMD="$MVN -B deploy -pl $DEPLOY_SUBMODULES $MVN_URM_MIRROR \
          -Dmaven.repo.local=$SHIM_M2DIR \
          -Dcuda.version=$DEFAULT_CUDA_CLASSIFIER \
          -DskipTests -Drat.skip=true -Djava.io.tmpdir=${SHIM_WORKSPACE} \
          -Dmaven.scaladoc.skip -Dmaven.scalastyle.skip=true \
          -Dbuildver=${BUILD_VER}"

    echo "Deploying ${BUILD_VER}"
    DEPLOY_LOG="${SHIM_WORKSPACE}/deploy-${BUILD_VER}.log"

    $DEPLOY_CMD > "${DEPLOY_LOG}" 2>&1
    CODE="$?"
    if [[ $CODE != "0" ]]; then
      tail -1000 "${DEPLOY_LOG}" || true
      return $CODE
    fi
  fi

  echo "${BUILD_VER} done."
}
export -f build_shim

# build, install, and deploy all the versions we support, but skip deploy of individual dist module since we
# only want the combined jar to be pushed.
# Note this does not run any integration tests
# Deploy jars unless SKIP_DEPLOY is 'true'

set +H # turn off history expansion
export DEPLOY_SUBMODULES=${DEPLOY_SUBMODULES:-"integration_tests"}
if ! command -v parallel &> /dev/null; then
  # the script still supports building shims sequentially, but not recommended
  for buildver in "${SPARK_SHIM_VERSIONS[@]:1}"; do
      build_shim "${buildver}"
  done
else
  # The default is derived from the average perf of cpu, mem, disk and network on internal CI machines.
  BUILD_PARALLELISM=${BUILD_PARALLELISM:-'6'}
  # Ignore SIGTTOU to allow background processes to write to the terminal
  # this is a workaround for cdh shims build, otherwise it will hang forever with parallel
  trap '' SIGTTOU
  parallel --ungroup --halt "now,fail=1" -j"${BUILD_PARALLELISM}" 'build_shim {1} {%}' ::: "${SPARK_SHIM_VERSIONS[@]:1}"
  trap - SIGTTOU
fi

installDistArtifact() {
  local cuda_version="$1"
  local opt="$2"
  $MVN -B clean install \
      $opt \
      $DIST_PROFILE_OPT \
      -Dbuildver=$SPARK_BASE_SHIM_VERSION \
      $MVN_URM_MIRROR \
      -Dmaven.repo.local=$M2DIR \
      -Dcuda.version=$cuda_version \
      -DskipTests
}

# TODO: parallel build for different cuda classifiers
# build extra cuda classifiers
if (( ${#CLASSIFIERS_ARR[@]} > 1 )); then
  for classifier in "${CLASSIFIERS_ARR[@]}"; do
    if [ "${classifier}" == "${DEFAULT_CUDA_CLASSIFIER}" ]; then
      echo "skip default: ${DEFAULT_CUDA_CLASSIFIER} in build extra cuda classifiers step..."
      continue
    fi

    opt=""
    if [[ "${classifier}" == *"-arm64" ]]; then
      opt="-Parm64"
    fi
    # pass cuda version and extra opt
    installDistArtifact ${classifier%%-*} ${opt}

    # move artifacts to temp for deployment later
    artifactFile="${ART_ID}-${ART_VER}-${classifier}.jar"
    mv ${DIST_PATH}/target/${artifactFile} ${TMP_PATH}/
  done
fi
# build dist w/ default cuda classifier
installDistArtifact ${DEFAULT_CUDA_CLASSIFIER}

distWithReducedPom "install"

if [[ $SKIP_DEPLOY != 'true' ]]; then
    # this deploys selected submodules that is unconditionally built with default Spark shim
    $MVN -B deploy -pl "!${DIST_PL}" \
        -Dbuildver=$SPARK_BASE_SHIM_VERSION \
        -DskipTests -Drat.skip=true \
        -Dmaven.scaladoc.skip -Dmaven.scalastyle.skip=true \
        $MVN_URM_MIRROR -Dmaven.repo.local=$M2DIR \
        -Dcuda.version=$DEFAULT_CUDA_CLASSIFIER

    # try move tmp artifacts back to target folder for simplifying separate release process
    if (( ${#CLASSIFIERS_ARR[@]} > 1 )); then
        mv ${TMP_PATH}/${ART_ID}-${ART_VER}-*.jar ${DIST_PATH}/target/
    fi
    # Deploy dist jars in the final step to ensure that the POM files are not overwritten
    SERVER_URL=${SERVER_URL:-"$ART_URL"} SERVER_ID=${SERVER_ID:-"snapshots"} jenkins/deploy.sh
fi

# Parse Spark files from local mvn repo
jenkins/printJarVersion.sh "SPARKVersion" "$M2DIR/org/apache/spark/spark-core_2.12/${SPARK_VER}" "spark-core_2.12-${SPARK_VER}" ".jar" $SERVER_ID
