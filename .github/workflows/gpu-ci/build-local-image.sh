#!/usr/bin/env bash
# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail
home=$(mktemp -d "$RUNNER_TEMP/gpu-image.XXXXXXXX")
trap 'rm -rf -- "$home"' EXIT
env -i PATH="$PATH" HOME="$home" DOCKER_CONFIG="$home" DOCKER_HOST="${DOCKER_HOST:-unix:///var/run/docker.sock}" \
  IMAGE_CONFIG="$IMAGE_CONFIG" PUBLISH_IMAGE=false bash .ci-common/container-image/image.sh
image=$(docker image inspect "image-validation:$(jq -er .tag <<< "$IMAGE_CONFIG")" |
  jq -er --arg arch "$GPU_ARCH" '.[0] | select(.Os == "linux" and .Architecture == $arch) | .Id')
if [[ ! $image =~ ^sha256:[0-9a-f]{64}$ ]]; then
  echo "::error::Local Spark build did not return a valid image ID for linux/$GPU_ARCH."
  exit 1
fi
echo "image=$image" >> "$GITHUB_OUTPUT"
