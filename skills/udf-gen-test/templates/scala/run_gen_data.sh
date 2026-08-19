#!/bin/bash
# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# Generate or validate benchmark data

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

print_usage() {
    echo "Usage: $0 --rows NUM [--validate] [--output-path PATH] [--mvn-arg ARG]..."
}

ROWS=""
VALIDATE=""
OUTPUT_PATH=""
MAVEN_ARGS=()

while [[ $# -gt 0 ]]; do
    case $1 in
        --rows) ROWS="$2"; shift 2;;
        --validate) VALIDATE="true"; shift;;
        --output-path) OUTPUT_PATH="$2"; shift 2;;
        --mvn-arg) MAVEN_ARGS+=("$2"); shift 2;;
        *)
            echo "Unknown option: $1"
            print_usage
            exit 1
            ;;
    esac
done

if [ -z "$ROWS" ]; then
    echo "Error: --rows is required"
    print_usage
    exit 1
fi

SPARK_CONFS=(
    --spark-conf spark.master="local[8]"
    --spark-conf spark.rapids.sql.enabled="true"
    --spark-conf spark.plugins="com.nvidia.spark.SQLPlugin"
    --spark-conf spark.locality.wait="0s"
    --spark-conf spark.sql.cache.serializer="com.nvidia.spark.ParquetCachedBatchSerializer"
    --spark-conf spark.rapids.sql.format.parquet.reader.type="MULTITHREADED"
    --spark-conf spark.rapids.sql.reader.batchSizeBytes="1000MB"
    --spark-conf spark.sql.files.maxPartitionBytes="512MB"
    --spark-conf spark.rapids.sql.metrics.level="DEBUG"
)

EXEC_ARGS="--rows $ROWS --partitions 32"
for arg in "${SPARK_CONFS[@]}"; do
    EXEC_ARGS="$EXEC_ARGS $arg"
done

if [ -n "$VALIDATE" ]; then
    EXEC_ARGS="$EXEC_ARGS --validate"
    echo "Running GenData in validation mode with $ROWS rows..."
else
    if [ -z "$OUTPUT_PATH" ]; then
        OUTPUT_PATH="data/bench_data_${ROWS}_rows.parquet"
    fi
    EXEC_ARGS="$EXEC_ARGS --output-path $OUTPUT_PATH"
    echo "Running GenData to generate $ROWS rows -> $OUTPUT_PATH..."
fi

mvn "${MAVEN_ARGS[@]}" compile exec:java \
    -Dexec.mainClass="com.udf.bench.GenData" \
    -Dexec.classpathScope=compile \
    -Dexec.args="$EXEC_ARGS"
