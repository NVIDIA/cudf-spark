#!/usr/bin/env bash
#
# Copyright (c) 2023-2026, NVIDIA CORPORATION. All rights reserved.
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

trap_func() {
  rv=$?
  if [[ $rv == 0 ]]; then
    echo DONE scala2.13 poms generated: exit code = $rv
  else
    echo ERROR generating scala2.13 poms, re-execute with:
    echo "  bash -x $*"
    echo to inspect the error output
    exit $rv
  fi
}

trap "trap_func" EXIT

VALID_VERSIONS=( 2.13 )
declare -A DEFAULT_SPARK
DEFAULT_SPARK[2.12]="spark330"
DEFAULT_SPARK[2.13]="spark350"

usage() {
  echo "Usage: $(basename $0) [-h|--help] <version>
where :
  -h| --help Display this help text
  valid version values : ${VALID_VERSIONS[*]}
" 1>&2
  exit 1
}

if [[ ($# -ne 1) || ( $1 == "--help") ||  $1 == "-h" ]]; then
  usage
fi

TO_VERSION=$1

check_scala_version() {
  for i in "${VALID_VERSIONS[@]}"; do [ $i = "$1" ] && return 0; done
  echo "Invalid Scala version: $1. Valid versions: ${VALID_VERSIONS[*]}" 1>&2
  exit 1
}

check_scala_version "$TO_VERSION"

sed_i() {
  sed -e "$1" "$2" > "$2.tmp" && mv "$2.tmp" "$2"
}

BASEDIR=$(dirname $0)/..
if [ $TO_VERSION = "2.13" ]; then
  FROM_VERSION="2.12"
  SUBDIR="scala2.13"
fi

TO_DIR="$BASEDIR/$SUBDIR"
mkdir -p $TO_DIR

for f in $(git ls-files '**pom.xml'); do
  if [[ $f ==  $SUBDIR* ]]; then
    echo "Skipping $f"
    continue
  fi
  # Skills package their own pom.xml templates. Ignore those.
  if [[ $f == skills/* ]]; then
    echo "Skipping $f"
    continue
  fi
  echo $f
  tof="$TO_DIR/$f"
  mkdir -p $(dirname $tof)
  cp -f $f $tof
  echo $tof
  sed_i 's/\(artifactId.*\)_'$FROM_VERSION'/\1_'$TO_VERSION'/g' $tof
  sed_i 's/^\([[:space:]]*<!-- #if scala-'$TO_VERSION' -->\)<!--/\1/' $tof
  sed_i 's/^\([[:space:]]*\)-->\(<!-- #endif scala-'$TO_VERSION' -->\)/\1\2/' $tof
  sed_i 's/^\([[:space:]]*<!-- #if scala-'$FROM_VERSION' -->\)$/\1<!--/' $tof
  sed_i 's/^\([[:space:]]*\)\(<!-- #endif scala-'$FROM_VERSION' -->\)/\1-->\2/' $tof
done

# Update spark.version to spark350.version for Scala 2.13
SPARK_VERSION=${DEFAULT_SPARK[$TO_VERSION]}
sed_i '/<java\.major\.version>/,/<spark\.version>\${spark[0-9]\+\.version}</s/<spark\.version>\${spark[0-9]\+\.version}</<spark.version>\${'$SPARK_VERSION'.version}</' \
  "$TO_DIR/pom.xml"

sed_i '/<java\.major\.version>/,/<spark\.version>\${spark[0-9]\+\.version}</s/<spark\.version>\${spark[0-9]\+\.version}</<spark.version>\${'$SPARK_VERSION'.version}</' \
  "$TO_DIR/pom.xml"

# Update <scala.binary.version> in parent POM
# Match any scala binary version to ensure idempotency
sed_i '/<spark\-rapids\-jni\.version>/,/<scala\.binary\.version>[0-9]*\.[0-9]*</s/<scala\.binary\.version>[0-9]*\.[0-9]*</<scala.binary.version>'$TO_VERSION'</' \
  "$TO_DIR/pom.xml"


# Update <scala.version> in parent POM
# Match any scala version to ensure idempotency
SCALA_VERSION=$(awk -v profile="scala-${TO_VERSION}" '
  /<profile>/ { in_profile = 1; found = 0 }
  in_profile && index($0, "<id>" profile "</id>") { found = 1 }
  found && /<scala[.]version>/ {
    gsub(/.*<scala[.]version>|<\/scala[.]version>.*/, "")
    print
    exit
  }
  /<\/profile>/ { in_profile = 0; found = 0 }
' "$BASEDIR/pom.xml")
if [[ -z "$SCALA_VERSION" ]]; then
  echo "Could not find scala.version for profile scala-${TO_VERSION}" >&2
  exit 1
fi

sed_i '/<spark\-rapids\-jni\.version>/,/<scala.version>[0-9]*\.[0-9]*\.[0-9]*</s/<scala\.version>[0-9]*\.[0-9]*\.[0-9]*</<scala.version>'$SCALA_VERSION'</' \
  "$TO_DIR/pom.xml"
