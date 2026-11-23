#!/usr/bin/env bash
# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
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
image=${1:?Image reference required}
platform=${2:?Platform required}
if [[ "$platform" != linux/amd64 && "$platform" != linux/arm64 ]]; then
  echo "Unsupported smoke platform: $platform (expected linux/amd64 or linux/arm64)" >&2
  exit 1
fi

docker run --rm -i --pull=never --platform "$platform" --network none \
  --entrypoint /bin/bash "$image" -s -- "${platform#linux/}" <<'SMOKE'
set -euo pipefail
arch=$(dpkg --print-architecture)
if [[ "$arch" != "$1" ]]; then
  echo "Expected a $1 image, found $arch" >&2
  exit 1
fi
java -version
for jdk in 8 11 17; do
  "/usr/lib/jvm/java-${jdk}-openjdk-${arch}/bin/javac" -version
done
mvn --version
python3 -c 'import pytest, pandas, pyarrow, sre_yield'
ucx_info -v
dpkg-query -W -f='${Status}\n' "ucx-cuda:$arch" | grep -Fx 'install ok installed'
SMOKE
