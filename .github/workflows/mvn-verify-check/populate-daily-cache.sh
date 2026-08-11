#!/bin/bash

# Copyright (c) 2024, NVIDIA CORPORATION.
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
set -o pipefail

if [[ $SCALA_VER == '2.12' ]]; then
    pom='pom.xml'
elif [[ $SCALA_VER == '2.13' ]]; then
    pom='scala2.13/pom.xml'
fi

max_retry=3; delay=30; i=1
while true; do
    buildvers=($(python build/get_buildvers.py no_snapshots $pom | tr -d ',')) &&
    {
        for buildver in "${buildvers[@]}"; do
            mvn $COMMON_MVN_FLAGS --file $pom -Dbuildver=$buildver de.qaware.maven:go-offline-maven-plugin:resolve-dependencies
        done
    } && {
        # compile base versions to cache scala compiler and compiler bridge
        mvn $COMMON_MVN_FLAGS --file $pom process-test-resources -pl sql-plugin-api -am
    } && break || {
    if [[ $i -le $max_retry ]]; then
        echo "mvn command failed. Retry $i/$max_retry."; ((i++)); sleep $delay; ((delay=delay*2))
    else
        echo "mvn command failed. Exit 1"; exit 1
    fi
}
done
