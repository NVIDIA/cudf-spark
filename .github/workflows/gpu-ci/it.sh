#!/usr/bin/env bash
# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail
it=.github/workflows/gpu-ci/it.py
files=$(python3 "$it" files "${IT_SHARD:?Expected shard 1 or 2}")
read -r product SPARK_VER _ < "${SPARK_HOME:?}/RELEASE"
[[ $product == Spark && "${SPARK_VER//./}" == "${SPARK_SHIM:?}" && -x $SPARK_HOME/bin/spark-submit ]]
export SPARK_VER TEST_TYPE=pre-commit
export PATH="$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH"
py4j=("$SPARK_HOME"/python/lib/py4j-*-src.zip)
[[ ${#py4j[@]} == 1 && -f ${py4j[0]} ]]
export PYTHONPATH="$SPARK_HOME/python:${py4j[0]}${PYTHONPATH:+:$PYTHONPATH}"

case "$SCALA_BINARY_VER" in
  2.12) project=. ;;
  2.13) project=scala2.13 ;;
  *) echo "Unsupported Scala: $SCALA_BINARY_VER" >&2; exit 2 ;;
esac

# Build exactly the plugin and test artifacts for this distribution; UT runs in other jobs.
settings=${MVN_SETTINGS:?Missing hosted Maven settings}
mvn -f "$project/pom.xml" -s "$settings" -U -B -Dbuildver="$SPARK_SHIM" \
  -Dcuda.version="${CLASSIFIER:?}" -Drat.skip=true -Dmaven.scaladoc.skip \
  -Dmaven.scalastyle.skip=true -DskipTests=true clean package

# Publish only new JUnit output, including a failed command's partial reports.
target=$(cd "$project/integration_tests/target" && pwd -P)
out="$PWD/target/gpu-it-junit/$SCALA_BINARY_VER-$IT_SHARD"
mkdir -p "$out"
before=$(mktemp)
active=
finish() {
  local status=$?
  trap - EXIT
  if [[ -n $active ]]; then
    python3 "$it" reports "$target" "$out" "$active" "$before" || :
  fi
  rm -f "$before"
  exit "$status"
}
trap finish EXIT
run_phase() {
  local phase=$1 report_status=0
  shift
  find "$target" -maxdepth 2 -type f -name 'TEST-pytest-*.xml' > "$before"
  active=$phase
  "$@"
  active=
  python3 "$it" reports "$target" "$out" "$phase" "$before" || report_status=$?
  if [[ $phase == targeted && $report_status == 42 ]]; then
    needs_full_shard=true
  else
    return "$report_status"
  fi
}

unset TEST TESTS TEST_TAGS RUN_DIR
launcher=./integration_tests/run_pyspark_from_build.sh
if [[ -n ${TARGETS_JSON+x} ]]; then
  # Do not sample explicit node IDs. A runtime-specific skip falls back to this shard.
  needs_full_shard=false
  run_phase targeted env REDUCED_IT=false RANDOM_SELECT='' TESTS="$files" TEST_TAGS='' "$launcher"
  if [[ $needs_full_shard == false ]]; then exit 0; fi
  unset TARGETS_JSON
  files=$(python3 "$it" files "$IT_SHARD")
fi
tags=''
if [[ $SCALA_BINARY_VER == 2.13 ]]; then tags='not premerge_ci_1'; fi
run_phase baseline env TESTS="$files" TEST_TAGS="$tags" "$launcher"

source jenkins/spark-premerge-build.sh
source jenkins/shuffle-common.sh
MVN="mvn -s $settings -Dmaven.wagon.http.retryHandler.count=3"
if [[ $IT_SHARD == 1 ]]; then
  run_phase avro env TESTS=avro_test.py INCLUDE_SPARK_AVRO_JAR=true \
    TEST_TAGS='not premerge_ci_1' "$launcher"
  run_phase regexp env TESTS=regexp_test.py LC_ALL=en_US.UTF-8 \
    TEST_TAGS='not premerge_ci_1' "$launcher"
  if [[ $SCALA_BINARY_VER == 2.12 ]]; then
    source jenkins/test-timezones.sh
    for zone in "${time_zones_test_cases[@]}"; do
      run_phase "tz-${zone//\//-}" env TZ="$zone" "$launcher" -m tz_sensitive_test
    done
  else
    run_phase unity-catalog run_premerge_unity_catalog_tests "$SPARK_VER"
  fi
else
  run_phase shuffle rapids_shuffle_smoke_test "$SPARK_VER"
  if [[ $SCALA_BINARY_VER == 2.13 ]]; then
    artifact="iceberg-spark-runtime-${SPARK_VER%.*}_$SCALA_BINARY_VER"
    version=1.10.1
    jar="$HOME/.m2/repository/org/apache/iceberg/$artifact/$version/$artifact-$version.jar"
    if [[ ! -s $jar || ! -s ${jar%.jar}.pom ]]; then
      mvn -N -B -s "$settings" org.apache.maven.plugins:maven-dependency-plugin:2.10:get \
        -Dartifact="org.apache.iceberg:$artifact:$version"
    fi
    # Both spark-submit and parallel PySpark read this job's extracted Spark configuration.
    # Ivy does not read Maven settings; use the staged Maven repository before the public mirror.
    printf '\nspark.jars.ivySettings %s\n' "$PWD/.github/actions/maven-cache/ivysettings.xml" \
      >> "$SPARK_HOME/conf/spark-defaults.conf"
    run_phase iceberg-versions run_iceberg_version_detect_tests "$SPARK_VER" "$SCALA_BINARY_VER"
    run_phase iceberg-classpath run_iceberg_extra_classpath_tests \
      "$SPARK_VER" "$SCALA_BINARY_VER" "$version" "$jar"
  fi
fi
