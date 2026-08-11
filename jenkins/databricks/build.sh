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

# This script installs dependencies required to build RAPIDS Accelerator for Apache Spark on DB.
# All the environments can be overwritten by shell variables:
#   SPARKSRCTGZ: Archive file location of the plugin repository. Default is empty.
#   BASE_SPARK_VERSION: Spark version [3.2.1, 3.3.0]. Default is pulled from current instance.
#   BASE_SPARK_VERSION_TO_INSTALL_DATABRICKS_JARS: The version of Spark used when we install the
#       Databricks jars in .m2. Default is {BASE_SPARK_VERSION}.
#   MVN_OPT: Options to be passed to the MVN commands. Note that "-DskipTests" is hardcoded in the
#       build command.
#   SKIP_DEP_INSTALL: Skips installation of dependencies when set to 1. Default is 0.
# Usage:
# - build for DB10.4/Spark 3.2.1:
#       `BASE_SPARK_VERSION=3.2.1 ./jenkins/databricks/build.sh`
# - Build without dependency installation:
#       `BASE_SPARK_VERSION=3.2.1 SKIP_DEP_INSTALL=1 ./jenkins/databricks/build.sh`
# To add support of new runtime:
#   1. Review `install_deps.py` to make sure that the prefix of the jar files is set
#      correctly. If not, then add a new if-else block to set the variables as necessary.
#   2. The jar files and their artifacts are defined in `install_deps.py`.
#      You may need to add another conditional block because some runtimes may require special
#      handling.
#      For example, "3.1.2" had different patterns for a few JARs (i.e., HIVE).
#   3. If you had to go beyond the above steps to support the new runtime, then update the
#      instructions accordingly.

set -ex

# Map of software versions for each dependency.
declare -A sw_versions
# Map of jar file locations of all dependencies
declare -A dep_jars
# Map of string arrays to hold the groupId and the artifactId for each JAR
declare -A artifacts

# Initializes the scripts and the variables based on teh arguments passed to the script.
initialize()
{
    # Print DB runtime version details
    if [[ -f /databricks/BUILDINFO ]]; then
        echo "DB runtime version details:"
        cat /databricks/BUILDINFO
    else
        echo "No /databricks/BUILDINFO file found"
    fi

    # ubuntu22
    sudo sed -i -e 's|http://archive.ubuntu.com/ubuntu|https://archive.ubuntu.com/ubuntu|g' \
      -e 's|http://security.ubuntu.com/ubuntu|https://security.ubuntu.com/ubuntu|g' \
      /etc/apt/sources.list
    # ubuntu24
    sudo find /etc/apt/sources.list.d/ -name '*.sources' -exec sed -i \
      -e "s|http://archive.ubuntu.com/ubuntu|https://archive.ubuntu.com/ubuntu|g" \
      -e "s|http://security.ubuntu.com/ubuntu|https://security.ubuntu.com/ubuntu|g" {} +

    # force cache refresh
    sudo apt-get clean
    sudo rm -rf /var/lib/apt/lists/*
    sudo apt-get update
    # install rsync to be used for copying onto the databricks nodes
    sudo apt install -y rsync openjdk-17-jdk
    if [[ ! -d $HOME/apache-maven-3.6.3 ]]; then
        # DBFS cache for Maven
        DBFS_CACHE_DIR=${DBFS_CACHE_DIR:-"/dbfs/cached_jars"}
        JAR_FILE_NAME=${JAR_FILE_NAME:-"apache-maven-3.6.3-bin.tar.gz"}
        MAVEN_CACHE_FILE=${MAVEN_CACHE_FILE:-"$DBFS_CACHE_DIR/$JAR_FILE_NAME"}
        MAVEN_URL=${MAVEN_URL:-"https://archive.apache.org/dist/maven/maven-3/3.6.3/binaries/$JAR_FILE_NAME"}
        # Create cache directory if it doesn't exist
        mkdir -p "$DBFS_CACHE_DIR"        
        # Check if file exists in DBFS cache
        if [[ -f "$MAVEN_CACHE_FILE" ]]; then
            echo "Found Maven in DBFS cache, copying to /tmp..."
            cp "$MAVEN_CACHE_FILE" "/tmp/$JAR_FILE_NAME"
        else
            echo "Maven not found in DBFS cache, downloading from archive.apache.org..."
            if wget "$MAVEN_URL" -P /tmp; then
                echo "Download successful, caching to DBFS..."
                cp "/tmp/$JAR_FILE_NAME" "$MAVEN_CACHE_FILE" || true
            else
                echo "Download failed"
                exit 1
            fi
        fi
        
        tar xf "/tmp/$JAR_FILE_NAME" -C $HOME
        rm -f "/tmp/$JAR_FILE_NAME"
        sudo ln -s $HOME/apache-maven-3.6.3/bin/mvn /usr/local/bin/mvn
    fi

    # Set JDK 17 as the default for nightly builds across both:
    # scala2.12 and scala2.13 (with maven.compiler.source as 1.8)
    export JAVA_HOME=$(echo /usr/lib/jvm/java-1.17.0-*)
    mvn -version

    # Archive file location of the plugin repository
    SPARKSRCTGZ=${SPARKSRCTGZ:-''}

    # Version of Apache Spark we are building against
    BASE_SPARK_VERSION=${BASE_SPARK_VERSION:-$(< /databricks/spark/VERSION)}

    ## '-Pfoo=1,-Dbar=2,...' to '-Pfoo=1 -Dbar=2 ...'
    MVN_OPT=${MVN_OPT//','/' '}
    BUILDVER=$(echo ${BASE_SPARK_VERSION} | sed 's/\.//g')db
    # the version of Spark used when we install the Databricks jars in .m2
    BASE_SPARK_VERSION_TO_INSTALL_DATABRICKS_JARS=${BASE_SPARK_VERSION_TO_INSTALL_DATABRICKS_JARS:-$BASE_SPARK_VERSION}
    SPARK_VERSION_TO_INSTALL_DATABRICKS_JARS=${BASE_SPARK_VERSION_TO_INSTALL_DATABRICKS_JARS}-databricks

     # Determine Scala version based on Spark version
    # Spark 4.0+ uses Scala 2.13, earlier versions use 2.12
    if [[ "$BASE_SPARK_VERSION" == 4.* ]]; then
        export SCALA_BINARY_VER=2.13
    fi

    DBR_VER=$(cat /databricks/DBR_VERSION)
    if [ $DBR_VER == '14.3' ] || [ $DBR_VER == '17.3' ]; then
        DBR_VER=$(echo $DBR_VER | sed 's/\.//g')
        # We are appending 143 in addition to the base spark version because Databricks 14.3
        # and Databricks 15.4 are both based on spark version 3.5.0. Similarly, we are appending 173
        # for Databricks 17.3 based on Spark 4.0.0.
        BUILDVER="$BUILDVER$DBR_VER"
        SPARK_VERSION_TO_INSTALL_DATABRICKS_JARS="$SPARK_VERSION_TO_INSTALL_DATABRICKS_JARS-$DBR_VER"
    fi

    # pull normal Spark artifacts and ignore errors then install databricks jars, then build again.
    # this should match the databricks init script.
    JARDIR=/databricks/jars

    if [[ -n $SPARKSRCTGZ ]]
    then
        rm -rf spark-rapids
        mkdir spark-rapids
        echo  "tar -zxf $SPARKSRCTGZ -C spark-rapids"
        tar -zxf $SPARKSRCTGZ -C spark-rapids
        cd spark-rapids
    fi

    # Now, we can set the WORKSPACE
    export WORKSPACE=$PWD
    # set the retry count for mvn commands
    MVN_CMD="mvn -Dmaven.wagon.http.retryHandler.count=3"

    # Determine which pom to use based on Scala version
    if [[ "$SCALA_BINARY_VER" == "2.13" ]]; then
        POM_FILE="scala2.13/pom.xml"
    else
        POM_FILE="pom.xml"
    fi

    # getting the versions of CUDA, SCALA and SPARK_PLUGIN
    SPARK_PLUGIN_JAR_VERSION=$($MVN_CMD help:evaluate -q -f $POM_FILE -pl dist -Dexpression=project.version -DforceStdout)
    SCALA_VERSION=$($MVN_CMD help:evaluate -q -f $POM_FILE -pl dist -Dexpression=scala.binary.version -DforceStdout)
    CUDA_VERSION=$($MVN_CMD help:evaluate -q -f $POM_FILE -pl dist -Dexpression=cuda.version -DforceStdout)
    RAPIDS_BUILT_JAR=rapids-4-spark_$SCALA_VERSION-$SPARK_PLUGIN_JAR_VERSION.jar
    # If set to 1, skips installing dependencies into mvn repo.
    SKIP_DEP_INSTALL=${SKIP_DEP_INSTALL:-'0'}
    # export 'M2DIR' so that shims can get the correct Spark dependency info
    export M2DIR=/home/ubuntu/.m2/repository
    # whether to build a two-shim jar with the lowest supported upstream Spark version
    WITH_DEFAULT_UPSTREAM_SHIM=${WITH_DEFAULT_UPSTREAM_SHIM:-1}


    # Print a banner of the build configurations.
    printf '+ %*s +\n' 100 '' | tr ' ' =
    echo "Initializing build for Databricks:"
    echo
    echo "tgz                                           : ${SPARKSRCTGZ}"
    echo "Base Spark version                            : ${BASE_SPARK_VERSION}"
    echo "maven options                                 : ${MVN_OPT}"
    echo "BASE_SPARK_VERSION_TO_INSTALL_DATABRICKS_JARS : ${BASE_SPARK_VERSION_TO_INSTALL_DATABRICKS_JARS}"
    echo "workspace                                     : ${WORKSPACE}"
    echo "Scala version                                 : ${SCALA_VERSION}"
    echo "CUDA version                                  : ${CUDA_VERSION}"
    echo "Rapids build jar                              : ${RAPIDS_BUILT_JAR}"
    echo "Build Version                                 : ${BUILDVER}"
    echo "Skip Dependencies                             : ${SKIP_DEP_INSTALL}"
    echo "Include Default Spark Shim                    : ${WITH_DEFAULT_UPSTREAM_SHIM}"
    echo "Extra environments                            : ${EXTRA_ENVS}"
    printf '+ %*s +\n' 100 '' | tr ' ' =
}

# Install dependency jars to MVN repository.
install_dependencies()
{
    local depsPomXml="$(mktemp /tmp/install-databricks-deps-XXXXXX-pom.xml)"

    python jenkins/databricks/install_deps.py "${BASE_SPARK_VERSION}" "${SPARK_VERSION_TO_INSTALL_DATABRICKS_JARS}" "${SCALA_VERSION}" "${M2DIR}" "${JARDIR}" "${depsPomXml}"

    $MVN_CMD -f ${depsPomXml} initialize
    echo "Done with installation of Databricks dependencies, removing ${depsPomXml}"
    rm ${depsPomXml}
}

##########################
# Main script starts here
##########################
## 'foo=abc,bar=123,...' to 'export foo=abc bar=123 ...'
if [ -n "$EXTRA_ENVS" ]; then
    export ${EXTRA_ENVS//','/' '}
fi

initialize
if [[ $SKIP_DEP_INSTALL == "1" ]]
then
    echo "!!!! SKIP_DEP_INSTALL is set to $SKIP_DEP_INSTALL. Skipping install-file for dependencies."
else
    echo "!!!! Installing dependendecies. Set SKIP_DEP_INSTALL=1 to speed up reruns of build.sh"# Install required dependencies.
    install_dependencies
fi

if [[ "$WITH_BLOOP" == "1" ]]; then
    MVN_OPT="-DbloopInstall $MVN_OPT"
    MVN_PHASES="clean install"
    for jdk_ver in 17 11 8; do
      if [[ $jdk_ver == 8 ]]; then
        echo "WARNING: could not find an 11+ JDK. Bloop Project might not be fully functional" >&2
        exit 1
      fi

      jdk_home="/usr/lib/jvm/zulu${jdk_ver}"
      if ls $jdk_home > /dev/null; then
        export JAVA_HOME=$jdk_home
        break
      fi
    done

    WITH_DEFAULT_UPSTREAM_SHIM=0
else
    MVN_PHASES="clean package"
fi

# Build the RAPIDS plugin by running package command for databricks
$MVN_CMD -B -f $POM_FILE -Ddatabricks -Dbuildver=$BUILDVER $MVN_PHASES -DskipTests $MVN_OPT

if [[ "$WITH_DEFAULT_UPSTREAM_SHIM" != "0" ]]; then
    echo "Building the default Spark shim and creating a two-shim dist jar"
    UPSTREAM_BUILDVER=$($MVN_CMD help:evaluate -q -f $POM_FILE -pl dist -Dexpression=buildver -DforceStdout)
    $MVN_CMD -B -f $POM_FILE -Dbuildver=$UPSTREAM_BUILDVER package -pl dist -am -DskipTests -Dmaven.scaladoc.skip $MVN_OPT \
        -Dincluded_buildvers=$UPSTREAM_BUILDVER,$BUILDVER
fi

# "Delete the unused object files to reduce the size of the Spark Rapids built tar."
# Determine the correct dist target directory based on which POM was used
if [[ "$SCALA_BINARY_VER" == "2.13" ]]; then
    DIST_TARGET="scala2.13/dist/target"
else
    DIST_TARGET="dist/target"
fi
rm -rf $DIST_TARGET/jni-deps/
find $DIST_TARGET/parallel-world/ -mindepth 1 -maxdepth 1 ! -name META-INF -exec rm -rf {} +

cd /home/ubuntu
tar -zcf spark-rapids-built.tgz spark-rapids

# Back up spark rapids built jars for the CI_PART2 job to run integration tests
TEST_MODE=${TEST_MODE:-'DEFAULT'}
PLUGIN_BUILT_TGZ=${PLUGIN_BUILT_TGZ:-"$1"}
if [[ "$TEST_MODE" == "CI_PART1"  && -n "$PLUGIN_BUILT_TGZ" ]]; then
   mkdir -p $(dirname $PLUGIN_BUILT_TGZ)
   cp spark-rapids-built.tgz $PLUGIN_BUILT_TGZ
fi
