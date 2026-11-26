#!/usr/bin/env bash
# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
set -euo pipefail

# Resolve the same effective POM and upstream SNAPSHOTs as the producer. Never warm or save here.
name="spark-ci-key-${GITHUB_RUN_ID:?}-${GITHUB_RUN_ATTEMPT:?}"
trap 'timeout 10s docker rm -f "$name" >/dev/null 2>&1 || :' EXIT
# Expand HOME, JAVA_HOME and SCALA inside the build image.
# shellcheck disable=SC2016
snapshot=$(timeout -k 5s 5m docker run --rm --init --name "$name" --pull=never --platform "linux/$GPU_ARCH" \
  --user "$(id -u):$(id -g)" --mount "type=bind,src=$PWD,dst=/repo,readonly" --workdir /repo \
  -e HOME=/tmp/ci -e SCALA -e JAVA_HOME="/usr/lib/jvm/java-17-openjdk-$GPU_ARCH" \
  -e MAVEN_OPTS=-Duser.home=/tmp/ci --entrypoint /bin/bash "$IMAGE" -ec '
    mkdir -p "$HOME/.m2"
    cp .github/actions/maven-cache/settings.xml "$HOME/.m2/settings.xml"
    export PATH="$JAVA_HOME/bin:$PATH"
    exec python3 .github/actions/maven-cache/dependency-key.py "$SCALA"')
if [[ ! $snapshot =~ ^s1(\.[0-9a-f]{64})+$ || -z $INPUT_HASH || -z $PREMERGE_HASH ]]; then
  echo "Could not determine the Maven cache identity; continuing cold" >&2
  exit 1
fi
SCALA_VER=$SCALA bash .github/actions/maven-cache/cache-keys.sh "$snapshot"
