#!/bin/bash
# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# Run CPU or GPU Spark benchmark.

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

print_usage() {
    echo "Usage: $0 --mode cpu|gpu --data-path PATH [--result-path PATH] [--mvn-arg ARG]..."
}

MODE=""
DATA_PATH=""
RESULT_PATH=""
MAVEN_ARGS=()

while [[ $# -gt 0 ]]; do
    case $1 in
        --mode) MODE="$2"; shift 2;;
        --data-path) DATA_PATH="$2"; shift 2;;
        --result-path) RESULT_PATH="$2"; shift 2;;
        --mvn-arg) MAVEN_ARGS+=("$2"); shift 2;;
        *)
            echo "Unknown option: $1"
            print_usage
            exit 1
            ;;
    esac
done

if [ -z "$MODE" ] || [ -z "$DATA_PATH" ]; then
    echo "Error: --mode and --data-path are required"
    print_usage
    exit 1
fi

DATA_BASENAME=$(basename "$DATA_PATH" .parquet)
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
if [ -z "$RESULT_PATH" ]; then
    RESULT_PATH="results/${MODE}_${DATA_BASENAME}_${TIMESTAMP}_result.json"
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

EXEC_ARGS="--mode $MODE --data-path $DATA_PATH --result-path $RESULT_PATH"
for arg in "${SPARK_CONFS[@]}"; do
    EXEC_ARGS="$EXEC_ARGS $arg"
done
EXEC_ARGS="$EXEC_ARGS --spark-conf spark.app.name=${MODE}_${DATA_BASENAME}_${TIMESTAMP}"

echo "Running $MODE benchmark on $DATA_PATH..."
mvn "${MAVEN_ARGS[@]}" compile exec:java \
    -Dexec.mainClass="com.udf.bench.SparkBenchRunner" \
    -Dexec.classpathScope=compile \
    -Dexec.args="$EXEC_ARGS"
