#!/usr/bin/env bash
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

set -euo pipefail

backend=${1:?Usage: run_path_character_tests.sh BACKEND TMP_PATH [pytest options]}
tmp_path=${2:?A writable temporary path is required}
shift 2

# These settings are startup-only: every invocation launches a new Spark application.
export PYSP_TEST_spark_rapids_filecache_enabled=false
export PYSP_TEST_spark_rapids_perfio_s3_enabled=false
export PYSP_TEST_spark_rapids_perfio_gcs_enabled=false
case "$backend" in
    hadoop) ;;
    s3-netty|s3-crt)
        [[ "$tmp_path" == s3://* || "$tmp_path" == s3a://* ]] || {
            echo 'S3 backends require an s3:// or s3a:// temporary path' >&2; exit 2;
        }
        export PYSP_TEST_spark_rapids_perfio_s3_enabled=true
        client=${backend#s3-}
        export PYSP_TEST_spark_rapids_perfio_s3_httpClient=${client^^}
        ;;
    gcs-http)
        [[ "$tmp_path" == gs://* || "$tmp_path" == gcs://* ]] || {
            echo 'GCS backends require a gs:// or gcs:// temporary path' >&2; exit 2;
        }
        export PYSP_TEST_spark_rapids_perfio_gcs_enabled=true
        ;;
    *) echo "Unknown path-test backend: $backend" >&2; exit 2 ;;
esac

export TESTS=${TESTS:-path_character_test.py}
script_dir=$(cd "$(dirname "$0")" && pwd)
exec bash "$script_dir/run_pyspark_from_build.sh" \
    --path_backend="$backend" --tmp_path="$tmp_path" "$@"
