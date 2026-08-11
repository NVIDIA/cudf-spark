#!/bin/bash

#
# Copyright (c) 2021-2022, NVIDIA CORPORATION. All rights reserved.
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
#
# check the revisions of each shims, they should be equal
# $1 is included_buildvers
function check-shims-revisions() {
  included_buildvers="$1"
  # PWD should be spark-rapids root path
  parallel_dir=${PWD}/parallel-world
  pre_revision=""
  pre_shim_version_path=""

  IFS=","
  for shim in ${included_buildvers}; do
    # trim
    shim=$(echo "${shim}" | xargs)
    shim_version_path="${parallel_dir}/spark${shim}/rapids4spark-version-info.properties"
    if [[ -f "$shim_version_path" ]] ; then
      curr_revision=$(grep "revision=" "${shim_version_path}" | cut -d'=' -f2)
      if [ -n "$pre_revision" ] && [[ "$curr_revision" != "$pre_revision" ]] ; then
        echo >&2 "Check Failed: git revisions between shims are not equal"
        echo >&2 "Please check the revisions of each shim to see which one is inconsistent. Note, if building with Databricks those jars are built separately."

        echo >&2 "-------------"
        echo >&2 "Version file ${pre_shim_version_path}, revision is ${pre_revision}"
        echo >&2 "Version file ${shim_version_path}, revision is ${curr_revision}"
        echo >&2 "------------"

        exit 1
      fi
      pre_revision="${curr_revision}"
      pre_shim_version_path="${shim_version_path}"
    else
      echo >&2 "Error: version file missing: ${shim_version_path}"
      exit 1
    fi
  done
}

check-shims-revisions "$1"
