#!/bin/bash
#
# Copyright (c) 2025-2026, NVIDIA CORPORATION. All rights reserved.
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

# Explicitly export HYBRID_BACKEND_JARS for hybrid execution tests.
export HYBRID_BACKEND_JARS

hybrid_prepare(){
    echo "Checking hybrid exeicution tests environmet..."
    local cup_arch=$(uname -m)
    local os_version=$(source /etc/os-release > /dev/null 2>&1 && echo ${ID}_${VERSION_ID})
    local spark_prefix="${SPARK_VER:0:3}" # get prefix from SPARK_VER, e.g.: 3.2, 3.3 ... 3.5
    echo "cup_arch=$cup_arch, os_version=$os_version, SCALA_BINARY_VER=$SCALA_BINARY_VER, spark_prefix=$spark_prefix"
    if [[ ! ( "$SUPPORTED_HYBRID_SHIMS" == *"$spark_prefix"* && "$cup_arch" == "x86_64" && ("$os_version" == "ubuntu_20.04" || "$os_version" == "ubuntu_22.04") && "$SCALA_BINARY_VER" == "2.12") ]]; then
        echo "SKIP! Hybrid only supports shims $SUPPORTED_HYBRID_SHIMS running Scala 2.12 tests on an x86_64 processor under Ubuntu 20.04 or 22.04."
        return 1
    fi

    echo "Downloading hybrid execution dependency jars..."
    # Read spark-rapids-hybrid.version directly from pom.xml; avoids an mvn call (and Maven Central plugin downloads).
    # Assumes the property is a literal value, not a Maven property reference.
    RAPIDS_HYBRID_VER=${RAPIDS_HYBRID_VER:-$(sed -n 's|.*<spark-rapids-hybrid\.version>\([^<]*\)</spark-rapids-hybrid\.version>.*|\1|p' "$WORKSPACE/pom.xml")}
    RAPIDS_HYBRID_URL=${RAPIDS_HYBRID_URL:-$ART_URL}
    GLUTEN_BUNDLE_JAR="gluten-velox-bundle-${GLUTEN_VERSION}-spark${spark_prefix}_${SCALA_BINARY_VER}-${os_version}_${cup_arch}.jar"
    HYBRID_JAR="rapids-4-spark-hybrid_${SCALA_BINARY_VER}-${RAPIDS_HYBRID_VER}.jar"
    GLUTEN_THIRDPARTY_JAR="gluten-thirdparty-lib-${GLUTEN_VERSION}-${os_version}-${cup_arch}.jar"
    wget -q -O /tmp/$GLUTEN_BUNDLE_JAR $ART_URL/com/nvidia/gluten-velox-bundle/$GLUTEN_VERSION/$GLUTEN_BUNDLE_JAR
    wget -q -O /tmp/$HYBRID_JAR $RAPIDS_HYBRID_URL/com/nvidia/rapids-4-spark-hybrid_${SCALA_BINARY_VER}/$RAPIDS_HYBRID_VER/$HYBRID_JAR
    wget -q -O /tmp/$GLUTEN_THIRDPARTY_JAR  $ART_URL/com/nvidia/gluten-thirdparty-lib/$GLUTEN_VERSION/$GLUTEN_THIRDPARTY_JAR
    HYBRID_BACKEND_JARS=/tmp/${HYBRID_JAR},/tmp/${GLUTEN_BUNDLE_JAR},/tmp/${GLUTEN_THIRDPARTY_JAR}
}
