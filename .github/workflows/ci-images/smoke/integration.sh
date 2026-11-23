#!/usr/bin/env bash
# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

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
javac -version
mvn --version
python -c 'import pandas, pyarrow, pytest, sre_yield, xdist'
SMOKE
