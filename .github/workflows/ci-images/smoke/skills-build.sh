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

# Check the Skills build toolchain and compile a CUDA kernel without a GPU.
set -euo pipefail
image=${1:?Image reference required}
platform=${2:?Platform required}
if [[ "$platform" != linux/amd64 ]]; then
  echo "Unsupported smoke platform: $platform (expected linux/amd64)" >&2
  exit 1
fi

docker run --rm -i --pull=never --platform "$platform" --network none \
  --entrypoint /bin/bash "$image" -s <<'SMOKE'
set -euo pipefail
java -version
javac -version
mvn --version
python3 -m pip --version
gcc --version
g++ --version
gcc_version=$(gcc -dumpfullversion)
if [[ "${gcc_version%%.*}" != "${TOOLSET_VERSION:?}" ]]; then
  echo "Expected GCC toolset $TOOLSET_VERSION on PATH, found $gcc_version" >&2
  exit 1
fi
cmake --version
ccache --version
nvcc --version

# Compile a CUDA kernel without loading a driver or allocating a GPU.
cd /tmp
printf '__global__ void smoke(int* x) { x[0] = 1; }\n' > smoke.cu
nvcc -c smoke.cu -o smoke.o
SMOKE
