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
if [[ $# -ne 3 || ( $3 != rdma && $3 != no-rdma ) ]]; then
  echo "Usage: $0 IMAGE PLATFORM {rdma|no-rdma}" >&2
  exit 2
fi

image=$1
platform=$2
mode=$3

docker run --rm -i --pull=never --platform "$platform" --network none \
  --entrypoint /bin/bash "$image" -s -- "$mode" <<'SMOKE'
set -euo pipefail
ucx_info -v
devices=$(ucx_info -d)
if ! grep -Eq 'Transport:[[:space:]]+self([[:space:]]|$)' <<< "$devices"; then
  echo "UCX did not report the CPU self transport" >&2
  exit 1
fi

# GPU drivers are supplied by the host. Check the image's CUDA runtime and UCX module.
ls -L /usr/local/cuda/lib64/libcudart.so.*
module_dir=/usr/lib/ucx
if [[ -d /usr/lib64/ucx ]]; then
  module_dir=/usr/lib64/ucx
fi
ls -L "$module_dir/libuct_cuda.so.0"

if [[ $1 == rdma ]]; then
  # Check image contents only; this CPU runner cannot exercise an RDMA transport.
  ls -L "$module_dir/libuct_ib.so.0" "$module_dir/libuct_rdmacm.so.0"
fi
SMOKE
