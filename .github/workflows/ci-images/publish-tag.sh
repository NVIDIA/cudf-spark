#!/usr/bin/env bash
# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# Move a consumer tag to a verified digest while its source recipe still owns it.
set -euo pipefail
die() { echo "Image publication: $*" >&2; exit 1; }
[[ $# == 4 ]] || die 'expected PACKAGE BRANCH_TAG TARGET_TAG VERIFIED_DIGEST'
package=$1
branch_tag=$2
tag=$3
digest=$4
[[ $package =~ ^[a-z0-9]+(-[a-z0-9]+)*$ ]] || die "invalid package: $package"
[[ $digest =~ ^sha256:[0-9a-f]{64}$ ]] || die "invalid verified digest for $package"
if [[ ${GITHUB_REF:?} == refs/heads/main ]]; then
  owner=main
elif [[ $GITHUB_REF =~ ^refs/heads/release/([0-9]{2}\.[0-9]{2})$ ]]; then
  owner=${BASH_REMATCH[1]}
else
  die "not a publication branch: $GITHUB_REF"
fi
[[ $branch_tag =~ ^(main|[0-9]{2}\.[0-9]{2})(-[a-z0-9][a-z0-9.-]*)?$ && ${BASH_REMATCH[1]} == "$owner" ]] ||
  die "$GITHUB_REF does not own branch tag $branch_tag"
release_ref=
if [[ $tag != "$branch_tag" ]]; then
  [[ $owner == main && $tag =~ ^([0-9]{2}\.[0-9]{2})(-[a-z0-9][a-z0-9.-]*)?$ ]] ||
    die "invalid release alias $tag for $branch_tag"
  release_ref="refs/heads/release/${BASH_REMATCH[1]}"
  [[ $branch_tag == "main${BASH_REMATCH[2]:-}" ]] || die "toolchain mismatch: $branch_tag and $tag"
fi

# The package lock serializes tag writes, not Git branch updates during a build.
scratch=$(mktemp -d)
trap 'rm -rf "$scratch"' EXIT
GITHUB_OUTPUT="$scratch/current" python3 .github/workflows/ci-images/select.py --current "$package"
case "$(cat "$scratch/current")" in
  current=true) ;;
  current=false) echo "Skipping $tag because the $owner recipe changed."; exit 0 ;;
  *) die "invalid recipe check for $package" ;;
esac

if [[ -n $release_ref ]]; then
  if git ls-remote --exit-code --refs origin "$release_ref" >/dev/null; then
    echo "Skipping $tag because $release_ref owns it."
    exit 0
  else
    status=$?
    [[ $status == 2 ]] || die "cannot check $release_ref on origin (git exit $status)"
  fi
fi

image="ghcr.io/$(printf '%s' "${GITHUB_REPOSITORY:?}" | tr '[:upper:]' '[:lower:]')/$package"
if [[ -n ${PLATFORM_RECORDS:-} && ${GITHUB_RUN_ATTEMPT:?} != 1 ]]; then
  # The recipe can still be current even if a newer rebuild moved this particular tag.
  python3 -I .github/workflows/ci-images/tag-baseline.py check "$image" "$tag" "$digest" "$PLATFORM_RECORDS"
fi
# Use the verified digest directly. A lone manifest must remain a lone manifest.
docker buildx imagetools create --prefer-index=false --metadata-file "$scratch/tag.json" \
  --tag "$image:$tag" "$image@$digest"
jq -e --arg digest "$digest" '."containerimage.descriptor".digest == $digest' "$scratch/tag.json" >/dev/null ||
  die "registry did not confirm $image:$tag at $digest"
if [[ -n ${GITHUB_OUTPUT:-} ]]; then
  printf 'digest=%s\n' "$digest" >> "$GITHUB_OUTPUT"
fi
echo "Published $image:$tag at $digest" | tee -a "${GITHUB_STEP_SUMMARY:?}"
