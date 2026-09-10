#!/bin/bash
# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# Run in-memory microbenchmark for RapidsUDFs.

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

print_usage() {
    echo "Usage: $0 --mode cpu|gpu|all --data-path PATH [--rows N] [--warmup N] [--measured N] [--pool-fraction F] [--profile] [--mvn-arg ARG]..."
}

MODE=""
DATA_PATH=""
PROFILE=""
MAVEN_ARGS=()
RUNNER_ARGS=()

while [[ $# -gt 0 ]]; do
    case $1 in
        --mode) MODE="$2"; RUNNER_ARGS+=("$1" "$2"); shift 2;;
        --data-path) DATA_PATH="$2"; RUNNER_ARGS+=("$1" "$2"); shift 2;;
        --profile) PROFILE="true"; RUNNER_ARGS+=("$1"); shift;;
        --mvn-arg) MAVEN_ARGS+=("$2"); shift 2;;
        *) RUNNER_ARGS+=("$1"); shift;;
    esac
done

if [ -z "$MODE" ] || [ -z "$DATA_PATH" ]; then
    echo "Error: --mode and --data-path are required"
    print_usage
    exit 1
fi

MVN_CMD=(
    mvn "${MAVEN_ARGS[@]}" compile exec:java
    -Dexec.mainClass=com.udf.bench.MicroBenchRunner
    -Dexec.classpathScope=compile
    "-Dexec.args=${RUNNER_ARGS[*]}"
)

if [ -n "$PROFILE" ]; then
    REPORT_PATH="results/microbench_$(date +%Y%m%d_%H%M%S)"
    mkdir -p results
    echo "Running microbenchmark (mode=$MODE) on $DATA_PATH with nsys profiling..."
    echo "nsys report will be saved to: ${REPORT_PATH}.nsys-rep"
    nsys profile \
        -c cudaProfilerApi \
        --capture-range-end=stop \
        --trace=cuda,nvtx \
        --nvtx-domain-include="libcudf" \
        -o "$REPORT_PATH" \
        "${MVN_CMD[@]}"
else
    echo "Running microbenchmark (mode=$MODE) on $DATA_PATH..."
    "${MVN_CMD[@]}"
fi
