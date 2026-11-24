#!/usr/bin/env bash
# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -eu
if [[ ${1:-} == --after-save ]]; then
    if [[ -n $TARGET && $MATCH == "$TARGET" ]]; then
        echo "key=$TARGET"
    elif [[ $PREMERGE == true ]]; then
        echo "::error::Could not confirm Maven cache $TARGET after save." >&2
        exit 1
    else
        echo "::warning::Could not confirm Maven cache $TARGET after save; Verify will continue without it." >&2
    fi
    exit 0
fi

key="$STEM-verify"
if [[ $PREMERGE == true ]]; then
    key="$FULL"
elif [[ $MATCH == "$STEM-premerge-"?* ]]; then
    # Verify does not depend on which runtime fixtures were included.
    key="$MATCH"
fi
echo "key=$key"
if [[ $MATCH == "$key" ]]; then echo "ready=true"; fi
