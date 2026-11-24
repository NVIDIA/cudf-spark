#!/usr/bin/env bash
# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -eu
key="$STEM-verify"
if [[ $PREMERGE == true ]]; then
    key="$FULL"
elif [[ $MATCH == "$STEM-premerge-"* ]]; then
    # Verify does not depend on which runtime fixtures were included.
    key="$MATCH"
fi
echo "key=$key"
if [[ $MATCH == "$key" ]]; then echo "ready=true"; fi
