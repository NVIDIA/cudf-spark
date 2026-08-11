#!/bin/bash

# Copyright (c) 2024-2025, NVIDIA CORPORATION.
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

set -e

scala_ver=${1:-"2.12"}
base_URL="https://central.sonatype.com/repository/maven-snapshots/com/nvidia"
project_jni="spark-rapids-jni"
project_private="rapids-4-spark-private_${scala_ver}"
project_hybrid="rapids-4-spark-hybrid_${scala_ver}"

jni_ver=$(mvn help:evaluate -q -pl dist -Dexpression=spark-rapids-jni.version -DforceStdout)
private_ver=$(mvn help:evaluate -q -pl dist -Dexpression=spark-rapids-private.version -DforceStdout)
hybrid_ver=$(mvn help:evaluate -q -pl dist -Dexpression=spark-rapids-hybrid.version -DforceStdout)

get_latest_snapshot_version() {
  local project_URL="$1"
  if [ -z "$project_URL" ]; then
    return 0
  fi
  local latest_ver
  latest_ver=$(curl -s "${project_URL}/maven-metadata.xml" 2>/dev/null | tr -d '\n' | 
      grep -o '<snapshotVersion>.*<extension>jar</extension>.*<value>[^<]*</value>' | 
      sed -n 's/.*<value>\([^<]*\)<\/value>.*/\1/p' | 
      head -1) || latest_ver=""
  echo "$latest_ver"
}


if [[ $jni_ver == *SNAPSHOT* ]]; then
  jni_URL="${base_URL}/${project_jni}/${jni_ver}"
  jni_latest_ver=$(get_latest_snapshot_version "${jni_URL}")
  jni_sha1=$(curl -sf "${jni_URL}/${project_jni}-${jni_latest_ver}.jar.sha1" 2>/dev/null) \
    || jni_sha1=$(date +'%Y-%m-%d')
else
  jni_sha1=$jni_ver
fi

if [[ $private_ver == *SNAPSHOT* ]]; then
  private_URL="${base_URL}/${project_private}/${private_ver}"
  private_latest_ver=$(get_latest_snapshot_version "${private_URL}")
  private_sha1=$(curl -sf "${private_URL}/${project_private}-${private_latest_ver}.jar.sha1" 2>/dev/null) \
    || private_sha1=$(date +'%Y-%m-%d')
else
  private_sha1=$private_ver
fi

if [[ $hybrid_ver == *SNAPSHOT* ]]; then
  hybrid_URL="${base_URL}/${project_hybrid}/${hybrid_ver}"
  hybrid_latest_ver=$(get_latest_snapshot_version "${hybrid_URL}")
  hybrid_sha1=$(curl -sf "${hybrid_URL}/${project_hybrid}-${hybrid_latest_ver}.jar.sha1" 2>/dev/null) \
    || hybrid_sha1=$(date +'%Y-%m-%d')
else
  hybrid_sha1=$hybrid_ver
fi

echo -n "${jni_sha1}_${private_sha1}_${hybrid_sha1}"
