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

profile=parent
settings_file=
output=
while [[ $# -gt 0 ]]; do
  case "$1" in
    -s|--settings)
      settings_file=$2
      shift 2
      ;;
    -P)
      profile=$2
      shift 2
      ;;
    -Doutput=*)
      output=${1#*=}
      shift
      ;;
    *)
      shift
      ;;
  esac
done

printf '%s|%s\n' "$PWD" "$settings_file" >> "$MAVEN_LOG"
if [[ ! -f "$settings_file" ]]; then
  echo "missing Maven settings: $settings_file" >&2
  exit 1
fi
cp "$MAVEN_FIXTURES/effective-$profile.xml" "$output"
