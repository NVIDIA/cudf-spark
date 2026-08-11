#!/bin/bash
#
# Copyright (c) 2020-2022, NVIDIA CORPORATION. All rights reserved.
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

function print_ver(){
    TAG=$1
    REPO=$2
    VERSION=$3
    SUFFIX=$4
    SERVER_ID=$5

    # Collect snapshot dependency info only in Jenkins build
    # In dev build, print 'SNAPSHOT' tag without time stamp, e.g.: rapids-4-spark-<version>-SNAPSHOT.jar
    if [[ "$VERSION" == *"-SNAPSHOT" && -n "$JENKINS_URL" ]]; then
        PREFIX=${VERSION%-SNAPSHOT}
        # List the latest SNAPSHOT jar file in the maven repo
        echo $TAG=`ls -t $REPO/$PREFIX-[0-9]*$SUFFIX | head -1 | xargs basename`
    else
        echo $TAG=$VERSION$SUFFIX
    fi
}

print_ver $1 $2 $3 $4 $5
