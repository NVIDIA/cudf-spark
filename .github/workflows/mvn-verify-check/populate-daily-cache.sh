#!/bin/bash

# Copyright (c) 2024-2026, NVIDIA CORPORATION.
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

# Warm Maven dependencies for Verify and premerge without running their test suites.
set -euo pipefail

case "$SCALA_VER" in
    2.12)
        pom=pom.xml
        premerge_profiles=(premergeUT1 premergeUT2 premergeUTF8)
        ;;
    2.13)
        pom=scala2.13/pom.xml
        premerge_profiles=(premergeScala213)
        ;;
    *) echo "Unsupported Scala binary version: $SCALA_VER" >&2; exit 1 ;;
esac

maven_flags=(
    --batch-mode --update-snapshots --file "$pom"
    -Ddist.jar.compress=false -DskipTests -Dmaven.artifact.threads=10
    -Dmaven.wagon.http.retryHandler.count=3 -Dmaven.wagon.httpconnectionManager.ttlSeconds=30
    -Daether.connector.http.connectionMaxTtl=30
    "-DsecondaryCacheDir=$HOME/.m2/repository/.sbt/1.0/zinc/org.scala-sbt"
)
versions=$(python3 build/get_buildvers.py no_snapshots "$pom")
read -r -a buildvers <<< "${versions//,/ }"
(( ${#buildvers[@]} > 0 )) || { echo "No build versions found in $pom" >&2; exit 1; }

warm_cli_plugins() (
    # dist/build/package-parallel-worlds.py pins the dependency plugin for its standalone get goal.
    goals=(help:evaluate -Dexpression=project.version
           org.apache.maven.plugins:maven-dependency-plugin:2.10:help)
    if [[ "$SCALA_VER" == 2.12 ]]; then
        # The install matrix generates a wrapper. Running the goal also resolves its scripts ZIP.
        wrapper_dir=$(mktemp -d "${TMPDIR:-/tmp}/maven-wrapper.XXXXXXXX") || exit $?
        trap 'rm -rf "$wrapper_dir"' EXIT
        cp "$pom" "$wrapper_dir/pom.xml" || exit $?
        cd "$wrapper_dir" || exit $?
        goals+=(wrapper:wrapper)
    else
        # Verify uses source-javadoc, and the Iceberg matrix pins its effective-POM reader.
        goals+=(source:help org.apache.maven.plugins:maven-help-plugin:3.5.2:help)
    fi
    mvn "${maven_flags[@]}" -N -q "${goals[@]}"
)

warm_premerge_dependencies() {
    local buildver profile selected spark_version delta_property delta_version
    local premerge_shims=" ${buildvers[0]} "
    for profile in "${premerge_profiles[@]}"; do
        selected=$(mvn "${maven_flags[@]}" -q -pl dist "-P$profile" help:evaluate \
            -Dexpression=included_buildvers -DforceStdout) || return $?
        premerge_shims+="$(tr ',[:space:]' ' ' <<< "$selected") "
    done

    for buildver in "${buildvers[@]}"; do
        if [[ "$premerge_shims" == *" $buildver "* ]]; then
            # Packaging copies external test JARs that are absent from the dependency graph.
            mvn "${maven_flags[@]}" "-Dbuildver=$buildver" -pl integration_tests \
                dependency:copy@copy dependency:copy@copy-spark-avro \
                dependency:copy@copy-spark-protobuf || return $?
        fi
    done

    if [[ "$SCALA_VER" == 2.13 ]]; then
        spark_version=$(jq -er --arg scala "$SCALA_VER" '.[$scala].version' \
            .github/workflows/premerge-spark-cache/releases.json) || return $?
        for delta_property in delta42x.version delta43x.version; do
            delta_version=$(mvn "${maven_flags[@]}" -N -q help:evaluate \
                "-Dexpression=$delta_property" -DforceStdout) || return $?
            SPARK_VER="$spark_version" SCALA_BINARY_VER="$SCALA_VER" \
                integration_tests/run_unity_catalog_server.sh --resolve-only \
                --delta-version "$delta_version" || return $?
        done
    fi
}

warm_dependencies() {
    local buildver
    warm_cli_plugins || return $?
    for buildver in "${buildvers[@]}"; do
        mvn "${maven_flags[@]}" "-Dbuildver=$buildver" \
            de.qaware.maven:go-offline-maven-plugin:resolve-dependencies || return $?
    done
    if [[ "${WARM_PREMERGE:-false}" == true ]]; then
        warm_premerge_dependencies || return $?
    fi
    mvn "${maven_flags[@]}" process-test-resources -pl sql-plugin-api -am
}

for delay in 30 60 120; do
    warm_dependencies && exit 0
    echo "Maven dependency warm-up failed. Retrying in ${delay}s." >&2
    sleep "$delay"
done
warm_dependencies
