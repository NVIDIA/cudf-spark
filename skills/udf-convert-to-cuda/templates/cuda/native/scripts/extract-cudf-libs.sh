#!/bin/bash
# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
TARGET_DIR="${TARGET_DIR:-${PROJECT_DIR}/target}"
NATIVE_DEPS_DIR="${TARGET_DIR}/native-deps"
CUDF_REPO_DIR="${TARGET_DIR}/cudf-repo"
RAPIDS_JAR_DIR="${TARGET_DIR}/rapids-jar"

SCALA_VERSION="${SCALA_VERSION:-2.12}"
RAPIDS4SPARK_VERSION="${RAPIDS4SPARK_VERSION:-26.04.0}"
CUDA_VERSION="${CUDA_VERSION:-cuda12}"
CUDF_BRANCH="${CUDF_BRANCH:-v26.04.00}"

mkdir -p "${NATIVE_DEPS_DIR}" "${CUDF_REPO_DIR}"

choose_rapids_jar() {
  local candidates=(
    "${RAPIDS_JAR_DIR}/rapids-4-spark_${SCALA_VERSION}-${RAPIDS4SPARK_VERSION}-${CUDA_VERSION}.jar"
    "${RAPIDS_JAR_DIR}/rapids-4-spark_${SCALA_VERSION}-${RAPIDS4SPARK_VERSION}.jar"
    "${HOME}/.m2/repository/com/nvidia/rapids-4-spark_${SCALA_VERSION}/${RAPIDS4SPARK_VERSION}/rapids-4-spark_${SCALA_VERSION}-${RAPIDS4SPARK_VERSION}-${CUDA_VERSION}.jar"
    "${HOME}/.m2/repository/com/nvidia/rapids-4-spark_${SCALA_VERSION}/${RAPIDS4SPARK_VERSION}/rapids-4-spark_${SCALA_VERSION}-${RAPIDS4SPARK_VERSION}.jar"
  )

  for candidate in "${candidates[@]}"; do
    if [[ -f "${candidate}" ]]; then
      echo "${candidate}"
      return 0
    fi
  done

  echo "ERROR: Could not find a rapids-4-spark jar." >&2
  echo "Tried target/rapids-jar and ~/.m2 for version ${RAPIDS4SPARK_VERSION} (${CUDA_VERSION})." >&2
  echo "Run the build through Maven with -Pcuda-native-udf so the profile can copy the RAPIDS dependency first." >&2
  return 1
}

JAR_PATH="$(choose_rapids_jar)"

echo "Using RAPIDS jar: ${JAR_PATH}"
echo "Using cuDF header ref: ${CUDF_BRANCH}"

TEMP_DIR="${TARGET_DIR}/cudf-extract"
rm -rf "${TEMP_DIR}"
mkdir -p "${TEMP_DIR}"

if ! unzip -o "${JAR_PATH}" "*/libcudf.so*" "*/libnvcomp.so*" -d "${TEMP_DIR}"; then
  echo "ERROR: Failed to extract libcudf/libnvcomp from ${JAR_PATH}" >&2
  echo "The selected RAPIDS jar may not include native Linux CUDA libraries." >&2
  rm -rf "${TEMP_DIR}"
  exit 1
fi

while IFS= read -r source_file; do
  cp -f "${source_file}" "${NATIVE_DEPS_DIR}/$(basename "${source_file}")"
done < <(find "${TEMP_DIR}" -name "*.so*")
rm -rf "${TEMP_DIR}"

if [[ ! -f "${NATIVE_DEPS_DIR}/libcudf.so" ]]; then
  echo "ERROR: libcudf.so was not extracted into ${NATIVE_DEPS_DIR}" >&2
  exit 1
fi

if [[ ! -d "${CUDF_REPO_DIR}/.git" ]]; then
  git clone --depth 1 --branch "${CUDF_BRANCH}" https://github.com/rapidsai/cudf.git "${CUDF_REPO_DIR}"
else
  echo "Using existing cuDF headers at ${CUDF_REPO_DIR}"
fi

if [[ ! -d "${CUDF_REPO_DIR}/cpp/include" ]]; then
  echo "ERROR: cuDF headers not found at ${CUDF_REPO_DIR}/cpp/include" >&2
  exit 1
fi

echo "Native dependencies ready:"
echo "  Libraries: ${NATIVE_DEPS_DIR}"
echo "  Headers:   ${CUDF_REPO_DIR}/cpp/include"
