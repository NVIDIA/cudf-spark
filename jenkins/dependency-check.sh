#!/bin/bash
#
# Copyright (c) 2024-2026, NVIDIA CORPORATION. All rights reserved.
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
# This file checks whether all the dependency jar or pom files for the specified
# artifacts defined in the file "$ARTIFACT_FILE" are available
# in the "$SERVER_ID::default::$SERVER_URL" maven repo


# Argument(s):
#   ARTIFACT_FILE :  Artifact(groupId:artifactId:version:[[packaging]:classifier]) list file
#
# Used environment(s):
#   SERVER_ID:      The repository id for this deployment.
#   SERVER_URL:     The url where to deploy artifacts.
#   M2_CACHE:       Maven local repo
###

set -ex

ARTIFACT_FILE=${1:-"/tmp/artifacts-list"}
SERVER_ID=${SERVER_ID:-"local"}
SERVER_URL=${SERVER_URL:-"file:/tmp/local-release-repo"}
M2_CACHE=${M2_CACHE:-"/tmp/m2-cache"}
DEST_PATH=${DEST_PATH:-"/tmp/test-get-dest"}
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MVN_SETTINGS=${MVN_SETTINGS:-"$SCRIPT_DIR/settings.xml"}
MVN=${MVN:-"mvn -s $MVN_SETTINGS"}
rm -rf $DEST_PATH && mkdir -p $DEST_PATH

# Warm up $M2_CACHE by downloading the Maven Dependency Plugin using -s $MVN_SETTINGS to fix intermittent mvn failures caused by timeouts
# or HTTP 429 errors while do wnloading Maven plugins from Maven Central. See https://github.com/NVIDIA/spark-rapids/pull/14727.
$MVN -B dependency:get -Dmaven.repo.local=$M2_CACHE -Dartifact=org.apache.maven.plugins:maven-dependency-plugin:2.8
remote_maven_repo=$SERVER_ID::default::$SERVER_URL
while read -r line; do
    artifact=$line # artifact=groupId:artifactId:version:[[packaging]:classifier]
    # Clean up $M2_CACHE to avoid side-effect of previous dependency:get
    rm -rf $M2_CACHE/com/nvidia
    # Dependency checks should run without -s $MVN_SETTINGS, because we do not want to download temporary or test JARs from the internal Maven repository.
    # These internal JARs will not be released, and we only want to check dependency issues for release JARs. Using -s $MVN_SETTINGS may interfere with the check.
    mvn -B dependency:get -DremoteRepositories=$remote_maven_repo -Dmaven.repo.local=$M2_CACHE -Dartifact=$artifact -Ddest=$DEST_PATH
done < $ARTIFACT_FILE

ls -l $DEST_PATH
