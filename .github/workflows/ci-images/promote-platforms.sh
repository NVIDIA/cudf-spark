#!/usr/bin/env bash
# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail
die() { echo "Image promotion failed: $*" >&2; exit 1; }
[[ $# == 3 ]] || die 'expected BASENAME TAG RECORD_DIR'
base=$1
tag=$2
records=$3
: "${GITHUB_REPOSITORY:?}" "${GITHUB_SHA:?}" "${GITHUB_RUN_ID:?}" "${GITHUB_RUN_ATTEMPT:?}" "${GITHUB_STEP_SUMMARY:?}"
[[ $base =~ ^[a-z0-9]+(-[a-z0-9]+)*$ ]] || die 'invalid image basename'
[[ $tag =~ ^(main|[0-9]{2}\.[0-9]{2})(-[a-z0-9][a-z0-9.-]*)?$ ]] ||
  die 'expected a main or YY.MM tag with an optional toolchain suffix'
[[ $GITHUB_SHA =~ ^[0-9a-f]{40}$ && $GITHUB_RUN_ID =~ ^[0-9]+$ && $GITHUB_RUN_ATTEMPT =~ ^[0-9]+$ ]] ||
  die 'invalid workflow source or run identity'
# A rerun can reuse old native records even after another run published the same source SHA.
[[ $GITHUB_RUN_ATTEMPT == 1 ]] ||
  die "run $GITHUB_RUN_ID attempt $GITHUB_RUN_ATTEMPT cannot promote $base:$tag; dispatch a new 'Docker image CI' run on the target branch"

image="ghcr.io/$(printf '%s' "$GITHUB_REPOSITORY" | tr '[:upper:]' '[:lower:]')/$base"
sources=()
for arch in amd64 arm64; do
  digest=$(jq -ser --arg image "$image" --arg tag "$tag" --arg platform "linux/$arch" \
    --arg source "$GITHUB_SHA" '
      select(length == 1) | .[0] |
      select(.image == $image and .tag == $tag and .platform == $platform and .source == $source) |
      .digest | select(type == "string" and test("^sha256:[0-9a-f]{64}\\z"))
    ' "$records/$arch.json") || die "missing or invalid $arch record for $image:$tag at $GITHUB_SHA"
  sources+=("$image@$digest")
done
[[ ${sources[0]} != "${sources[1]}" ]] || die 'expected two different native image digests'

scratch=$(mktemp -d)
trap 'rm -rf "$scratch"' EXIT
# Buildx merges the complete immutable indexes, including provenance attestations.
docker buildx imagetools create --dry-run "${sources[@]}" > "$scratch/index.json"
jq -e '[.manifests[].platform | select(.os != "unknown" or .architecture != "unknown") |
  [.os, .architecture]] | sort == [["linux","amd64"],["linux","arm64"]]' \
  "$scratch/index.json" >/dev/null || die 'combined image does not have exactly linux/amd64 and linux/arm64'

candidate="$image:build-$tag-$GITHUB_RUN_ID-$GITHUB_RUN_ATTEMPT"
docker buildx imagetools create --metadata-file "$scratch/candidate.json" \
  --tag "$candidate" "${sources[@]}"
digest=$(jq -er '."containerimage.descriptor".digest | select(test("^sha256:[0-9a-f]{64}\\z"))' \
  "$scratch/candidate.json") || die "missing published digest for $candidate"
reference="$image@$digest"

# Native workers already pulled the layers anonymously. Read just the published
# index with an isolated config; only the CLI plugin executables are shared.
mkdir "$scratch/anonymous"
plugins=${DOCKER_CONFIG:-"$HOME/.docker"}/cli-plugins
[[ ! -d "$plugins" ]] || ln -s "$(cd "$plugins" && pwd -P)" "$scratch/anonymous/cli-plugins"
printf '%s\n' '{"auths":{"ghcr.io":{}}}' > "$scratch/anonymous/config.json"
(
  unset DOCKER_AUTH_CONFIG BUILDX_CONFIG BUILDX_BUILDER
  docker --config "$scratch/anonymous" buildx imagetools inspect --raw "$reference"
) > "$scratch/resolved.json" ||
  die "anonymous index lookup failed for $reference; $image:$tag was not updated"
jq -e --slurpfile planned "$scratch/index.json" '(.manifests | sort_by(.digest)) ==
  ($planned[0].manifests | sort_by(.digest))' "$scratch/resolved.json" >/dev/null ||
  die "published platform or attestation descriptors differ from the verified inputs: $reference"

docker buildx imagetools create --prefer-index=false --tag "$image:$tag" "$reference"
if [[ -n ${GITHUB_OUTPUT:-} ]]; then
  printf 'digest=%s\n' "$digest" >> "$GITHUB_OUTPUT"
fi
printf 'Published %s:%s\nDigest: %s\nSource: %s\nVerified platforms: linux/amd64, linux/arm64\n' \
  "$image" "$tag" "$digest" "$GITHUB_SHA" | tee -a "$GITHUB_STEP_SUMMARY"
