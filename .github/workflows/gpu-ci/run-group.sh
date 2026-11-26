#!/usr/bin/env bash
# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail
arch=${GPU_ARCH-amd64}
case "$arch" in
  amd64|arm64) ;;
  *) echo "::error::Unsupported GPU architecture: $arch"; exit 2 ;;
esac
scratch=$(mktemp -d "${RUNNER_TEMP:?}/gpu-group.XXXXXXXX")
home="$scratch/home"
name="spark-ci-${GITHUB_RUN_ID:?}-${GITHUB_RUN_ATTEMPT:?}"
cleanup() {
  local status=$?
  trap - EXIT
  if [[ -f "$scratch/cid" ]]; then
    # A timed-out Docker client does not guarantee the workload stopped.
    timeout -k 5s 25s docker rm -f "$(<"$scratch/cid")" >/dev/null 2>&1 || :
  fi
  rm -rf -- "$scratch" || echo "::warning::Unable to remove the temporary test home"
  exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

mkdir -p "$home/.m2"
# ARC's Docker daemon shares RUNNER_TEMP, not necessarily the agent's HOME.
# A failed/partial cache restore must never become the test container's Maven repository.
if [[ ${CACHE_HIT:-false} == true ]]; then
  if ! timeout -k 5s 5m cp -a --reflink=auto "$HOME/.m2/repository" "$home/.m2/repository"; then
    echo "::warning::Could not stage the restored Maven cache; continuing cold"
    rm -rf -- "$home" || echo "::warning::Could not reclaim all partial cache staging space"
    home="$scratch/cold"
  fi
fi
mkdir -p "$home/.m2/repository"
cp .github/actions/maven-cache/settings.xml "$home/.m2/settings.xml"
if [[ $arch == arm64 ]]; then
  # Both Scala POMs use the arm64 profile for native JNI and UCX artifacts.
  python3 - "$home/.m2/settings.xml" <<'PY'
import sys
from xml.etree import ElementTree as ET

path = sys.argv[1]
tree = ET.parse(path)
root = tree.getroot()
ns = "http://maven.apache.org/SETTINGS/1.0.0"
ET.register_namespace("", ns)
profiles = root.find(f"{{{ns}}}activeProfiles")
if profiles is None:
    profiles = ET.SubElement(root, f"{{{ns}}}activeProfiles")
ET.SubElement(profiles, f"{{{ns}}}activeProfile").text = "arm64"
tree.write(path, encoding="utf-8", xml_declaration=True)
PY
fi

# Hadoop resolves the process identity through passwd; do not depend on the daemon's UID map.
uid=$(id -u)
gid=$(id -g)
printf 'sparkci:x:%s:%s::/home/ci:/bin/bash\n' "$uid" "$gid" > "$scratch/passwd"
printf 'sparkci:x:%s:\n' "$gid" > "$scratch/group"
run_in_image() {
  local remaining
  local -a workload_env=()
  if [[ $KIND == integration && -n ${TARGETS_JSON:-} ]]; then
    workload_env=(-e TARGETS_JSON)
  elif [[ $KIND == unit && -n ${unit_locale:-} ]]; then
    workload_env=(-e "LC_ALL=$unit_locale")
  fi
  remaining=$(( GPU_DEADLINE - $(date +%s) ))
  if ((remaining <= 0)); then
    echo "::error::GPU worker time budget exhausted before Spark $SPARK_SHIM"
    return 124
  fi
  echo "Running $GROUP: Scala $SCALA, Spark $SPARK_SHIM; ${remaining}s remaining"
  rm -f -- "$scratch/cid"
  # PATH is expanded in the image, not on the runner.
  # shellcheck disable=SC2016
  timeout -k 30s "${remaining}s" docker run --rm --init --pull=never --platform "linux/$arch" \
    --gpus all --shm-size=2g --name "$name" --cidfile "$scratch/cid" --user "$uid:$gid" \
    --mount "type=bind,src=$PWD,dst=/workspace" --workdir /workspace \
    --mount "type=bind,src=$home,dst=/home/ci" \
    --mount "type=bind,src=$scratch/passwd,dst=/etc/passwd,readonly" \
    --mount "type=bind,src=$scratch/group,dst=/etc/group,readonly" \
    -e HOME=/home/ci -e WORKSPACE=/workspace -e SPARK_LOCAL_IP=127.0.0.1 \
    -e JAVA_HOME="/usr/lib/jvm/java-17-openjdk-$arch" -e CLASSIFIER=cuda12 \
    -e SCALA_BINARY_VER="$SCALA" -e SPARK_SHIM -e UNIT_TEST_MODE -e PARALLEL_UT \
    -e SPARK_HOME -e IT_SHARD -e REDUCED_IT "${workload_env[@]}" \
    -e MVN_SETTINGS=/home/ci/.m2/settings.xml \
    -e 'MAVEN_OPTS=-Duser.home=/home/ci -Dmaven.repo.local=/home/ci/.m2/repository -DsecondaryCacheDir=/home/ci/.m2/repository/.sbt/1.0/zinc/org.scala-sbt' \
    --entrypoint /bin/bash "$IMAGE" -c 'export PATH="$JAVA_HOME/bin:$PATH"; exec bash "$@"' bash "$@"
}

export SPARK_SHIM UNIT_TEST_MODE IT_SHARD
unset SPARK_HOME
if [[ $KIND == integration ]]; then
  case $SHARD in
    1|2) IT_SHARD=$SHARD ;;
    *) echo "::error::Unsupported integration shard $SHARD"; exit 1 ;;
  esac
  distribution=$(basename "${SPARK_TARBALL:?Missing verified Spark archive}" .tgz)
  tar -xzf "$SPARK_TARBALL" -C "$home"
  [[ -x "$home/$distribution/bin/spark-submit" ]]
  export SPARK_HOME="/home/ci/$distribution"
  run_in_image .github/workflows/gpu-ci/it.sh
  exit
fi

suites=()
if [[ -n ${TARGETS_JSON:-} ]]; then
  suite_csv=$(jq -er '
    if type == "array" and length > 0 and
       all(.[]; type == "string" and test("\\A([A-Za-z_][A-Za-z0-9_]*\\.)+[A-Za-z_][A-Za-z0-9_]*\\z"))
    then join(",") else error("Expected fully qualified ScalaTest suite names") end' <<< "$TARGETS_JSON")
  IFS=, read -r -a suites <<< "$suite_csv"
fi

run_selected_suites() {
  local project
  case "$SCALA" in
    2.12) project=. ;;
    2.13) project=scala2.13 ;;
    *) echo "::error::Unsupported Scala version: $SCALA"; return 1 ;;
  esac
  echo "Selected ScalaTest suites: $suite_csv"
  # Build the classified reactor dependencies first. Only the tests module has these suites.
  # Its serial ScalaTest plugin accepts exact names; parallelUnitTests uses prefix matching.
  # shellcheck disable=SC2016
  run_in_image -c '
    set -e
    nvidia-smi
    suites=$1
    shift
    set -- "$@" "-Dcuda.version=$CLASSIFIER"
    mvn "$@" -U -am clean install -DskipTests
    mvn "$@" test "-Dsuites=$suites"
  ' bash "$suite_csv" -B -f "$project/pom.xml" -pl tests -Dbuildver="$SPARK_SHIM" \
    -Drat.skip=true -Dmaven.scaladoc.skip -Dmaven.scalastyle.skip=true \
    -Dmaven.wagon.http.retryHandler.count=3
}

jq -er '.[] | [.shim, ."unit-group", (.locale // "")] | @tsv' <<< "${SHIMS:?}" > "$scratch/shims"
[[ -s "$scratch/shims" ]]
while read -r SPARK_SHIM unit_group unit_locale; do
  case "$unit_group" in
    ut2) UNIT_TEST_MODE=package ;;
    ut1|scala213) UNIT_TEST_MODE=filecache ;;
    *) echo "::error::Unsupported unit group $unit_group"; exit 1 ;;
  esac
  status=0
  evidence=0
  check=(python3 .github/workflows/gpu-ci/check-reports.py "$SCALA" "$SPARK_SHIM" "$unit_group")
  if ((${#suites[@]})); then
    run_selected_suites || status=$?
    "${check[@]}" "${suites[@]}" || evidence=$?
  fi
  if (( ${#suites[@]} == 0 || (status == 0 && evidence == 3) )); then
    if ((evidence == 3)); then
      echo "Targeted suite skipped all cases; running the full group for Spark $SPARK_SHIM"
    fi
    run_in_image jenkins/spark-premerge-build.sh ci_unit || status=$?
    evidence=0
    "${check[@]}" || evidence=$?
  fi

  # Keep partial results on failure, and never replace the original Maven/timeout exit.
  if ((status || evidence)); then
    echo "::error::Scala $SCALA / Spark $SPARK_SHIM: workload exit $status, JUnit check exit $evidence"
    if ((status)); then exit "$status"; fi
    exit "$evidence"
  fi
done < "$scratch/shims"
