#!/usr/bin/env bash
# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail
die() { echo "Image release tag: $*" >&2; exit 1; }
[[ $# == 4 ]] || die 'expected PACKAGE MAIN_TAG RELEASE_TAG VERIFIED_DIGEST'
package=$1
main_tag=$2
release_tag=$3
digest=$4
[[ ${GITHUB_REF:?} == refs/heads/main ]] || die "expected main, got $GITHUB_REF"
[[ $package =~ ^[a-z0-9]+(-[a-z0-9]+)*$ ]] || die "invalid package: $package"
[[ $release_tag =~ ^([0-9]{2}\.[0-9]{2})(-[a-z0-9][a-z0-9.-]*)?$ ]] || die "invalid release tag: $release_tag"
release_ref="refs/heads/release/${BASH_REMATCH[1]}"
[[ $main_tag == "main${BASH_REMATCH[2]:-}" ]] || die "toolchain mismatch: $main_tag and $release_tag"
[[ $digest =~ ^sha256:[0-9a-f]{64}$ ]] || die "invalid verified digest for $package"

# The caller holds the package lock. Main may advance while the image builds.
scratch=$(mktemp -d)
trap 'rm -rf "$scratch"' EXIT
GITHUB_OUTPUT="$scratch/current" python3 .github/workflows/ci-images/select.py --current "$package"
case "$(cat "$scratch/current")" in
  current=true) ;;
  current=false) echo "Skipping $release_tag because the main recipe changed."; exit 0 ;;
  *) die "invalid recipe check for $package" ;;
esac

if git ls-remote --exit-code --refs origin "$release_ref" >/dev/null; then
  echo "Skipping $release_tag because $release_ref owns it."
  exit 0
else
  status=$?
  [[ $status == 2 ]] || die "cannot check $release_ref on origin (git exit $status)"
fi

image="ghcr.io/$(printf '%s' "${GITHUB_REPOSITORY:?}" | tr '[:upper:]' '[:lower:]')/$package"
# Use the verified digest directly. A lone manifest must remain a lone manifest.
docker buildx imagetools create --prefer-index=false --metadata-file "$scratch/tag.json" \
  --tag "$image:$release_tag" "$image@$digest"
jq -e --arg digest "$digest" '."containerimage.descriptor".digest == $digest' "$scratch/tag.json" >/dev/null ||
  die "registry did not confirm $image:$release_tag at $digest"
echo "Seeded $image:$release_tag at $digest" | tee -a "${GITHUB_STEP_SUMMARY:?}"
