#!/usr/bin/env bash
# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
set -euo pipefail

root=$(cd "$(dirname "$0")/../../.." && pwd)
scratch=$(mktemp -d)
trap 'rm -rf -- "$scratch"' EXIT
cd "$scratch"
printf '%s\n' '<project><modelVersion>4.0.0</modelVersion><groupId>ci</groupId><artifactId>seed</artifactId><version>1</version></project>' > pom.xml
flags=(-B -ntp -s "$root/jenkins/databricks/maven-settings.xml")

# Keep wrapper bootstrap artifacts outside the seed. Only the driver CLI primes the restored repo.
mvn "${flags[@]}" "-Dmaven.repo.local=$PWD/bootstrap" org.apache.maven.plugins:maven-wrapper-plugin:3.3.4:wrapper \
    -Dmaven=3.8.1 -DdistributionSha256Sum=ba1517f73c5c22cf39afa0d570c998e6e024f37c75569f5c5524a69ff00a7f1b
export MAVEN_USER_HOME="$PWD/wrapper" MVNW_REPOURL=https://maven-central.storage-download.googleapis.com/maven2
# The generated wrapper downloads the ZIP itself, outside Maven's repository fallback.
if ! ./mvnw --version >/dev/null; then
    export MVNW_REPOURL=https://repo.maven.apache.org/maven2
fi
./mvnw "${flags[@]}" "-Dmaven.repo.local=$HOME/.m2/repository" \
    org.apache.maven.plugins:maven-clean-plugin:3.2.0:clean
