#!/bin/bash
# Copyright (c) 2026, NVIDIA CORPORATION.
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

# Starts an OSS Unity Catalog server for the Delta Lake catalog-managed table tests in
# delta_lake_catalog_managed_test.py, backed by a fake S3 bucket on local disk.
#
# Usage:
#   run_unity_catalog_server.sh [options]              # server stays up until Ctrl-C
#   run_unity_catalog_server.sh [options] -- COMMAND   # run COMMAND against the server
#
# Options:
#   --port N          client port, the server binds its REST API at N+1 (default 18080)
#   --uc-version V    Unity Catalog version; this suite is pinned to 0.6.0 (default 0.6.0)
#   --run-dir DIR     parent for the server's scratch directory (default $TMPDIR or /tmp)
#   --refresh         re-resolve the cached classpaths before starting
#
# Without a command the server runs in the foreground and prints an env file to source from
# another terminal, which keeps one server alive across repeated test runs. With a command the
# server is started, the command is run against it, and the server is stopped again.
#
# Example:
#   ./integration_tests/run_unity_catalog_server.sh -- \
#     ./integration_tests/run_pyspark_from_build.sh -m unity_catalog --delta_lake --unity_catalog

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

UC_VERSION=${UNITY_CATALOG_VERSION:-'0.6.0'}
DELTA_VERSION='4.2.0'
UC_PORT=${UNITY_CATALOG_PORT:-'18080'}
RUN_DIR_BASE=${TMPDIR:-/tmp}
REFRESH=0
COMMAND=()

# The fake credentials the server vends and CredentialTestFileSystem asserts on.
S3_BUCKET='test-bucket0'
S3_ACCESS_KEY='accessKey0'
S3_SECRET_KEY='secretKey0'
S3_SESSION_TOKEN='sessionToken0'

die() {
  >&2 echo "ERROR: $*"
  exit 1
}

http_get_succeeds() {
  local url=$1
  local quiet=${2:-1}
  case "$HTTP_CLIENT" in
    curl)
      if [[ "$quiet" -eq 1 ]]; then
        curl -s -f -o /dev/null -m 5 "$url"
      else
        curl -sS -f -o /dev/null -m 5 "$url"
      fi
      ;;
    wget)
      if [[ "$quiet" -eq 1 ]]; then
        wget -q -T 5 -t 1 -O /dev/null "$url"
      else
        wget -S -T 5 -t 1 -O /dev/null "$url"
      fi
      ;;
  esac
}

usage() {
  cat <<'EOF'
Usage:
  run_unity_catalog_server.sh [options]              # server stays up until Ctrl-C
  run_unity_catalog_server.sh [options] -- COMMAND   # run COMMAND against the server

Options:
  --port N          client port, the server binds its REST API at N+1 (default 18080)
  --uc-version V    Unity Catalog version; this suite is pinned to 0.6.0 (default 0.6.0)
  --run-dir DIR     parent for the server's scratch directory (default $TMPDIR or /tmp)
  --refresh         re-resolve the cached classpaths before starting
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --port) UC_PORT="$2"; shift 2 ;;
    --uc-version) UC_VERSION="$2"; shift 2 ;;
    --run-dir) RUN_DIR_BASE="$2"; shift 2 ;;
    --refresh) REFRESH=1; shift ;;
    -h|--help) usage; exit 0 ;;
    --) shift; COMMAND=("$@"); break ;;
    *) die "unknown option '$1', see --help" ;;
  esac
done

if command -v curl >/dev/null 2>&1; then
  HTTP_CLIENT='curl'
elif command -v wget >/dev/null 2>&1; then
  HTTP_CLIENT='wget'
else
  die "curl or wget is required to check Unity Catalog server readiness"
fi

# ---------------------------------------------------------------------------
# Versions
# ---------------------------------------------------------------------------

# SPARK_VER and SCALA_BINARY_VER are set by the CI environment. Outside CI they are derived from
# the Spark installation, using the jar name rather than `pyspark --version` to avoid a JVM start.
if [[ -z "$SPARK_VER" || -z "$SCALA_BINARY_VER" ]]; then
  [[ -n "$SPARK_HOME" ]] || die "SPARK_HOME is not set and SPARK_VER/SCALA_BINARY_VER are empty"
  spark_core_jar=$(find "$SPARK_HOME/jars" -maxdepth 1 -name 'spark-core_*.jar' | head -n 1)
  [[ -n "$spark_core_jar" ]] || die "no spark-core jar found under $SPARK_HOME/jars"
  # spark-core_2.13-4.1.1.jar -> 2.13-4.1.1
  detected=$(basename "$spark_core_jar" .jar)
  detected=${detected#spark-core_}
  SCALA_BINARY_VER=${SCALA_BINARY_VER:-${detected%%-*}}
  SPARK_VER=${SPARK_VER:-${detected#*-}}
fi
SPARK_LINE=${SPARK_VER%.*}

# Unsupported combinations fail here rather than being skipped: this script only runs when
# somebody asked for it explicitly, so silence would be the wrong answer.
[[ "$SCALA_BINARY_VER" == "2.13" ]] ||
  die "Delta Lake $DELTA_VERSION requires Scala 2.13, found $SCALA_BINARY_VER"
case "$SPARK_VER" in
  4.0.1|4.1.1) ;;
  *) die "Delta Lake $DELTA_VERSION is only tested against Spark 4.0.1 and 4.1.1, found $SPARK_VER" ;;
esac
# The implementation relies on the UCSingleCatalog 0.6.0 staging shape. Keep the test dependency
# pinned so a connector refactor cannot silently turn compatibility fallback coverage into an
# unreviewed support claim.
[[ "$UC_VERSION" == "0.6.0" ]] ||
  die "Unity Catalog $UC_VERSION is not supported; this suite is pinned to 0.6.0"

# ---------------------------------------------------------------------------
# Classpaths
# ---------------------------------------------------------------------------

CACHE_DIR="$SCRIPT_DIR/target/unity-catalog"
[[ "$REFRESH" -eq 0 ]] || rm -rf "$CACHE_DIR"
mkdir -p "$CACHE_DIR"

# Resolves $2 (comma separated Maven coordinates) into the classpath file $1, excluding the
# comma separated group ids in $3. Results are cached because resolution is slow.
resolve_classpath() {
  local out_file=$1
  local coordinates=$2
  local exclude_group_ids=${3:-}
  if [[ -s "$out_file" ]]; then
    return 0
  fi

  local work_dir
  work_dir=$(mktemp -d "$CACHE_DIR/resolve-XXXXXX")
  {
    echo '<project xmlns="http://maven.apache.org/POM/4.0.0">'
    echo '  <modelVersion>4.0.0</modelVersion>'
    echo '  <groupId>com.nvidia.spark.rapids.tests</groupId>'
    echo '  <artifactId>unity-catalog-classpath</artifactId>'
    echo '  <version>1.0</version>'
    echo '  <packaging>pom</packaging>'
    echo '  <repositories>'
    # The id must not be "central". A settings.xml that redefines the "central" id shadows this
    # declaration, sending every request to that mirror instead of Maven Central; mirrors that do
    # not carry io.unitycatalog then fail the build with an opaque HTTP error.
    echo '    <repository>'
    echo '      <id>unity-catalog-maven-central</id>'
    echo '      <url>https://repo1.maven.org/maven2</url>'
    echo '    </repository>'
    echo '  </repositories>'
    echo '  <dependencies>'
    local coordinate group_id artifact_id version
    local -a coordinates_array
    IFS=',' read -ra coordinates_array <<< "$coordinates"
    for coordinate in "${coordinates_array[@]}"; do
      IFS=':' read -r group_id artifact_id version <<< "$coordinate"
      echo '    <dependency>'
      echo "      <groupId>$group_id</groupId>"
      echo "      <artifactId>$artifact_id</artifactId>"
      echo "      <version>$version</version>"
      echo '    </dependency>'
    done
    echo '  </dependencies>'
    echo '</project>'
  } > "$work_dir/pom.xml"

  # Only outputFile carries the "mdep." prefix; includeScope and excludeGroupIds do not, and a
  # prefixed name is silently ignored rather than rejected. See the plugin descriptor.
  local -a mvn_args=(
    -B -q -f "$work_dir/pom.xml" dependency:build-classpath
    -DincludeScope=runtime
    "-Dmdep.outputFile=$out_file"
  )
  if [[ -n "$exclude_group_ids" ]]; then
    mvn_args+=("-DexcludeGroupIds=$exclude_group_ids")
  fi
  # Jenkins supplies MVN as a command string with repository settings and retry options, for
  # example "mvn -s jenkins/settings.xml -Dmaven.wagon.http.retryHandler.count=3". Split that
  # conventional value into an argv array instead of quoting it as one executable name.
  local -a mvn_command
  read -r -a mvn_command <<< "${MVN:-mvn}"
  [[ ${#mvn_command[@]} -gt 0 ]] || die "MVN resolved to an empty command"
  "${mvn_command[@]}" "${mvn_args[@]}" >&2 || die "failed to resolve $coordinates"
  rm -rf "$work_dir"
  [[ -s "$out_file" ]] || die "no classpath resolved for $coordinates"
}

echo "Resolving Unity Catalog $UC_VERSION jars for Spark $SPARK_VER, Scala $SCALA_BINARY_VER"

SERVER_CP_FILE="$CACHE_DIR/server-$UC_VERSION.classpath"
resolve_classpath "$SERVER_CP_FILE" "io.unitycatalog:unitycatalog-server:$UC_VERSION"

# The Spark session only needs the Delta and Unity Catalog connector jars. Jackson and Hadoop come
# from Spark itself and the GCS connector is unused, so those groups are excluded rather than
# pinning Unity Catalog's transitive versions by hand.
SPARK_CP_FILE="$CACHE_DIR/spark-$UC_VERSION-$SPARK_LINE-$SCALA_BINARY_VER.classpath"
# unitycatalog-client is declared explicitly because delta-storage depends on 0.4.1 at the same
# depth as unitycatalog-spark depends on the current one. Maven breaks a depth tie by declaration
# order, so without this the old client wins and classes added since 0.4.1 are missing at runtime.
spark_coordinates="io.unitycatalog:unitycatalog-client:$UC_VERSION"
spark_coordinates+=",io.delta:delta-spark_${SPARK_LINE}_${SCALA_BINARY_VER}:$DELTA_VERSION"
spark_coordinates+=",io.delta:delta-storage:$DELTA_VERSION"
spark_coordinates+=",io.unitycatalog:unitycatalog-spark_${SPARK_LINE}_${SCALA_BINARY_VER}:$UC_VERSION"
spark_excludes="com.fasterxml.jackson.core,com.fasterxml.jackson.module"
spark_excludes+=",com.fasterxml.jackson.dataformat,com.fasterxml.jackson.datatype"
spark_excludes+=",org.apache.hadoop,com.google.cloud.bigdataoss"
resolve_classpath "$SPARK_CP_FILE" "$spark_coordinates" "$spark_excludes"

# ---------------------------------------------------------------------------
# Server
# ---------------------------------------------------------------------------

port_in_use() {
  (exec 3<>"/dev/tcp/127.0.0.1/$1") 2>/dev/null
}

for port in "$UC_PORT" $((UC_PORT + 1)); do
  ! port_in_use "$port" || die "port $port is already in use, pass --port to pick another pair"
done

mkdir -p "$RUN_DIR_BASE"
RUN_DIR=$(mktemp -d "$RUN_DIR_BASE/rapids-unity-catalog-XXXXXX")
STORAGE_ROOT="$RUN_DIR/storage"
mkdir -p "$STORAGE_ROOT" "$RUN_DIR/vertx-cache"
UC_URI="http://localhost:${UC_PORT}/"

cleanup() {
  local status=$?
  [[ -z "$UC_PID" ]] || kill "$UC_PID" 2>/dev/null || true
  if [[ $status -eq 0 ]]; then
    rm -rf "$RUN_DIR"
  else
    >&2 echo "Unity Catalog server directory kept for inspection: $RUN_DIR"
  fi
}
UC_PID=""
trap cleanup EXIT

# Every server setting can be supplied as a system property, so no server.properties is needed.
# vertx.cacheDirBase is redirected because Vert.x otherwise writes to /tmp.
java -Dvertx.cacheDirBase="$RUN_DIR/vertx-cache" \
  -Dserver.env=test \
  -Dserver.managed-table.enabled=true \
  -Dstorage-root.tables="s3://${S3_BUCKET}${STORAGE_ROOT}" \
  -Ds3.bucketPath.0="s3://${S3_BUCKET}" \
  -Ds3.accessKey.0="$S3_ACCESS_KEY" \
  -Ds3.secretKey.0="$S3_SECRET_KEY" \
  -Ds3.sessionToken.0="$S3_SESSION_TOKEN" \
  -cp "$(cat "$SERVER_CP_FILE")" io.unitycatalog.server.UnityCatalogServer --port "$UC_PORT" \
  > "$RUN_DIR/server.log" 2>&1 &
UC_PID=$!

catalogs_url="${UC_URI}api/2.1/unity-catalog/catalogs"
server_ready=0
for _ in $(seq 1 60); do
  if http_get_succeeds "$catalogs_url" 2>/dev/null; then
    server_ready=1
    break
  fi
  kill -0 "$UC_PID" 2>/dev/null || break
  sleep 1
done
if [[ "$server_ready" -eq 0 ]] && http_get_succeeds "$catalogs_url" 0; then
  server_ready=1
fi
if [[ "$server_ready" -eq 0 ]]; then
  >&2 echo "Unity Catalog server did not become ready using $HTTP_CLIENT"
  for port in "$UC_PORT" $((UC_PORT + 1)); do
    if port_in_use "$port"; then
      >&2 echo "Port $port accepts TCP connections"
    else
      >&2 echo "Port $port does not accept TCP connections"
    fi
  done
  >&2 echo "Unity Catalog server log follows"
  >&2 cat "$RUN_DIR/server.log"
  exit 1
fi

# ---------------------------------------------------------------------------
# Environment for the tests
# ---------------------------------------------------------------------------

# CredentialTestFileSystem maps the fake s3 bucket onto local disk and asserts that the
# catalog-vended credentials reached the filesystem, so path-only access cannot pass. The RAPIDS
# S3 reader is disabled because that bucket is not a real S3 endpoint.
cat > "$RUN_DIR/env.sh" <<EOF
export DELTA_UC_URI='$UC_URI'
export DELTA_UC_STORAGE_ROOT='$STORAGE_ROOT'
export EXTRA_MAVEN_CLASSPATH='$(cat "$SPARK_CP_FILE")'
export PYSP_TEST_spark_sql_extensions='io.delta.sql.DeltaSparkSessionExtension'
export PYSP_TEST_spark_sql_catalog_spark__catalog='org.apache.spark.sql.delta.catalog.DeltaCatalog'
export PYSP_TEST_spark_hadoop_fs_s3_impl='com.nvidia.spark.rapids.tests.delta.CredentialTestFileSystem'
export PYSP_TEST_spark_rapids_perfio_s3_enabled='false'
EOF

# shellcheck source=/dev/null
source "$RUN_DIR/env.sh"

if [[ ${#COMMAND[@]} -gt 0 ]]; then
  echo "Unity Catalog $UC_VERSION ready at $UC_URI"
  status=0
  "${COMMAND[@]}" || status=$?
  exit $status
fi

cat <<EOF

Unity Catalog $UC_VERSION is ready at $UC_URI
Storage root: $STORAGE_ROOT

To run the tests from another terminal:

  source $RUN_DIR/env.sh
  ./integration_tests/run_pyspark_from_build.sh -m unity_catalog --delta_lake --unity_catalog

Press Ctrl-C to stop the server.
EOF

# Ctrl-C reaches the whole foreground process group, so the server exits with it. Treat that as a
# normal shutdown rather than a failure, otherwise every interactive run would keep $RUN_DIR.
trap 'exit 0' INT TERM
wait "$UC_PID" || true
