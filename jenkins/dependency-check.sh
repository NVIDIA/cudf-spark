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
#   WARMUP_REPO:    Maven local repo populated through the internal settings
###

set -ex

ARTIFACT_FILE=${1:-"/tmp/artifacts-list"}
SERVER_ID=${SERVER_ID:-"local"}
SERVER_URL=${SERVER_URL:-"file:/tmp/local-release-repo"}
M2_CACHE=${M2_CACHE:-"/tmp/m2-cache"}
WARMUP_REPO=${WARMUP_REPO:-"${M2_CACHE}-warmup"}
DEST_PATH=${DEST_PATH:-"/tmp/test-get-dest"}
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MVN_SETTINGS=${MVN_SETTINGS:-"$SCRIPT_DIR/settings.xml"}
MVN=${MVN:-"mvn -s $MVN_SETTINGS"}
rm -rf $DEST_PATH && mkdir -p $DEST_PATH

# This flow addresses two related problems:
# 1. Plain-Maven dependency checks can fail with HTTP 429 while downloading from Maven Central.
#    The original fix used the internal settings to warm up dependencies directly in $M2_CACHE.
# 2. That simple warmup is not sufficient because Maven records the source repository of each
#    cached artifact. The warmup records internal repository IDs, but plain Maven later requests
#    the same dependencies from Central. Maven then ignores the cached entries from the different
#    source repository and downloads from Central again, which reintroduces the HTTP 429 failure.
#
# More issue details: https://github.com/NVIDIA/cudf-spark/issues/15747
#
# Current fix is to keep the internally downloaded dependencies in a separate warmup repository and expose that
# cache as a file-based remote repository during validation.
#
# Repository roles:
# - remote_maven_repo: contains the artifacts under test at SERVER_URL.
# - warmup_maven_repo: exposes external dependencies fetched through internal Artifactory as a
#   file-based remote repository.
# - validation_maven_repos: combines the repository under test and the warmup repository for the
#   final plain-Maven dependency checks.
remote_maven_repo=$SERVER_ID::default::$SERVER_URL
warmup_maven_repo=warmup-cache::default::file://$WARMUP_REPO
validation_maven_repos=$remote_maven_repo,$warmup_maven_repo

# Step 1: Initialize $M2_CACHE by downloading the Maven Dependency Plugin through the internal
# settings.
# The final plain-Maven command cannot use warmup_maven_repo until this plugin has been loaded.
$MVN -B dependency:get \
    -Dmaven.repo.local="$M2_CACHE" \
    -Dartifact=org.apache.maven.plugins:maven-dependency-plugin:2.8

# Step 2: Resolve the artifact POM and its parent/dependency graph transitively through the original
# settings, downloading external dependencies from internal Artifactory into $WARMUP_REPO.
# All entries in $ARTIFACT_FILE share this POM, so resolving it transitively also caches common
# dependencies through the internal Maven repository before validation runs with plain Maven.
IFS=: read -r group_id artifact_id version _ < "$ARTIFACT_FILE"
pom_artifact="$group_id:$artifact_id:$version:pom"
$MVN -B dependency:get \
    -DremoteRepositories="$remote_maven_repo" \
    -Dmaven.repo.local="$WARMUP_REPO" \
    -Dartifact="$pom_artifact"

# Step 3: Remove NVIDIA artifacts downloaded during warmup. This keeps external dependencies
# available without allowing artifacts under test to resolve from the internal repository.
rm -rf "$WARMUP_REPO/com/nvidia"

# Step 4: Run the actual dependency checks with plain Maven against two remote repositories: the
# repository under test and the file-based warmup repository. Do not use $MVN_SETTINGS here,
# because temporary or test NVIDIA artifacts from internal Artifactory must not satisfy validation.
while read -r line; do
    artifact=$line # artifact=groupId:artifactId:version:[[packaging]:classifier]
    # Clean up $M2_CACHE to avoid side-effect of previous dependency:get
    rm -rf $M2_CACHE/com/nvidia
    # Dependency checks should run without -s $MVN_SETTINGS, because we do not want to download temporary or test JARs from the internal Maven repository.
    # These internal JARs will not be released, and we only want to check dependency issues for release JARs. Using -s $MVN_SETTINGS may interfere with the check.
    mvn -B dependency:get -DremoteRepositories=$validation_maven_repos -Dmaven.repo.local=$M2_CACHE -Dartifact=$artifact -Ddest=$DEST_PATH
done < $ARTIFACT_FILE

ls -l $DEST_PATH
