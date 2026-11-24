#!/usr/bin/env bash
# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# Verify that the UDF template can compile CUDA code using its default login shell.
set -euo pipefail
image=${1:?Image reference required}
platform=${2:?Platform required}
if [[ "$platform" != linux/amd64 ]]; then
  echo "Unsupported smoke platform: $platform (expected linux/amd64)" >&2
  exit 1
fi

# The template's default login shell enables the host C++ compiler used by nvcc.
docker run --rm -i --pull=never --platform "$platform" --network none "$image" -s <<'SMOKE'
set -euo pipefail
javac -version
mvn --version
cmake --version
cd /tmp
printf '__global__ void smoke() {}\n' > smoke.cu
ccache nvcc -c smoke.cu -o smoke.o
[[ -s smoke.o ]]
SMOKE
