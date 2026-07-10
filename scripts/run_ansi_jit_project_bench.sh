#!/usr/bin/env bash
#
# Copyright (c) 2026, NVIDIA CORPORATION.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
BENCH_SCALA="${REPO_DIR}/scripts/ansi_jit_project_bench.scala"

BENCH_BASE_PATH="${BENCH_BASE_PATH:-/tmp/ansi_jit_project_bench_${USER:-unknown}}"
BENCH_DATA_PATH="${BENCH_DATA_PATH:-}"
BENCH_EXPR_SUITES="${BENCH_EXPR_SUITES:-mixed}"
BENCH_EXPR_COUNTS="${BENCH_EXPR_COUNTS:-8}"
BENCH_EXPR_DEPTHS="${BENCH_EXPR_DEPTHS:-1,4,8}"
BENCH_MODES="${BENCH_MODES:-CPU,GPU_PROJECT,GPU_AST_JIT_COLD,GPU_AST_JIT_DISK_WARM,GPU_AST_JIT_PCH_WARM_KERNEL_COLD,GPU_AST_JIT_HOT}"
BENCH_APP_REPEATS="${BENCH_APP_REPEATS:-1}"
BENCH_WARMUPS="${BENCH_WARMUPS:-1}"
BENCH_ITERS="${BENCH_ITERS:-3}"
BENCH_REGENERATE_DATA="${BENCH_REGENERATE_DATA:-false}"
BENCH_SUMMARIZE_ONLY="${BENCH_SUMMARIZE_ONLY:-false}"
BENCH_SKIP_EXISTING="${BENCH_SKIP_EXISTING:-false}"
BENCH_CONSUME_MODE="${BENCH_CONSUME_MODE:-aggregate}"
BENCH_PRINT_PLAN="${BENCH_PRINT_PLAN:-false}"
BENCH_KERNEL_CACHE_BASE="${BENCH_KERNEL_CACHE_BASE:-${BENCH_BASE_PATH}/jit_cache}"
BENCH_RESULTS_DIR="${BENCH_RESULTS_DIR:-${BENCH_BASE_PATH}/fresh_app_results}"
BENCH_MARKDOWN_PATH="${BENCH_MARKDOWN_PATH:-${BENCH_RESULTS_DIR}/summary.md}"
BENCH_COMBINED_CSV_PATH="${BENCH_COMBINED_CSV_PATH:-${BENCH_RESULTS_DIR}/summary.csv}"
LIBCUDF_JIT_ENABLED="${LIBCUDF_JIT_ENABLED:-0}"
LIBCUDF_JIT_VERBOSE="${LIBCUDF_JIT_VERBOSE:-0}"
LIBCUDF_JIT_DUMP_CODEGEN="${LIBCUDF_JIT_DUMP_CODEGEN:-0}"
BENCH_PROFILE_TOOL="${BENCH_PROFILE_TOOL:-none}"
BENCH_PROFILE_TOOL="${BENCH_PROFILE_TOOL,,}"
BENCH_PROFILE_OUTPUT_DIR="${BENCH_PROFILE_OUTPUT_DIR:-${BENCH_RESULTS_DIR}/profiles}"
BENCH_CUDA_HOME="${BENCH_CUDA_HOME:-${CUDA_HOME:-/usr/local/cuda}}"
BENCH_NSYS_BIN="${BENCH_NSYS_BIN:-${BENCH_CUDA_HOME}/bin/nsys}"
BENCH_NCU_BIN="${BENCH_NCU_BIN:-${BENCH_CUDA_HOME}/bin/ncu}"
BENCH_NCU_SET="${BENCH_NCU_SET:-detailed}"
BENCH_NCU_KERNEL_NAME_BASE="${BENCH_NCU_KERNEL_NAME_BASE:-function}"
BENCH_NCU_KERNEL_NAME="${BENCH_NCU_KERNEL_NAME:-regex:^cudf_kernel_entry$}"
BENCH_NCU_LAUNCH_SKIP="${BENCH_NCU_LAUNCH_SKIP:-0}"
BENCH_NCU_LAUNCH_COUNT="${BENCH_NCU_LAUNCH_COUNT:-1}"
BENCH_PROFILE_POSTPROCESS="${BENCH_PROFILE_POSTPROCESS:-true}"

case "${BENCH_PROFILE_TOOL}" in
  none) ;;
  nsys)
    [[ -x "${BENCH_NSYS_BIN}" ]] || {
      echo "NSYS executable not found: ${BENCH_NSYS_BIN}" >&2
      exit 1
    }
    ;;
  ncu)
    [[ -x "${BENCH_NCU_BIN}" ]] || {
      echo "NCU executable not found: ${BENCH_NCU_BIN}" >&2
      exit 1
    }
    ;;
  *)
    echo "Unknown BENCH_PROFILE_TOOL=${BENCH_PROFILE_TOOL}; expected none, nsys, or ncu" >&2
    exit 1
    ;;
esac

if [[ "${BENCH_SUMMARIZE_ONLY}" != "true" ]]; then
  : "${SPARK_HOME:?SPARK_HOME must be set}"

  if [[ -z "${RAPIDS_JAR:-}" ]]; then
    for candidate in "${REPO_DIR}"/dist/target/rapids-4-spark_*-cuda*.jar \
        "${REPO_DIR}"/dist/target/rapids-4-spark_*.jar; do
      if [[ -f "${candidate}" && "${candidate}" != *sources.jar &&
          "${candidate}" != *javadoc.jar ]]; then
        RAPIDS_JAR="${candidate}"
        break
      fi
    done
  fi

  : "${RAPIDS_JAR:?RAPIDS_JAR must be set or a dist/target RAPIDS jar must exist}"
fi

mkdir -p "${BENCH_RESULTS_DIR}" "${BENCH_KERNEL_CACHE_BASE}"
if [[ "${BENCH_PROFILE_TOOL}" != "none" ]]; then
  mkdir -p "${BENCH_PROFILE_OUTPUT_DIR}"
  echo "Profiler enabled (${BENCH_PROFILE_TOOL}); benchmark timings are not comparable to normal runs."
fi

split_csv() {
  local value="$1"
  local -n out_array="$2"
  IFS=',' read -r -a out_array <<< "${value}"
}

safe_name() {
  printf '%s' "$1" | tr -c 'A-Za-z0-9_' '_'
}

is_ast_mode() {
  [[ "$1" == GPU_AST_JIT* || "$1" == GPU_AST_JIT ]]
}

report_pch_activity() {
  local mode="$1"
  local log_path="$2"
  case "${LIBCUDF_JIT_VERBOSE,,}" in
    1|true|on) ;;
    *) return ;;
  esac
  if ! is_ast_mode "${mode}"; then
    return
  fi

  local created used
  created="$(grep -ci 'creating precompiled header' "${log_path}" || true)"
  used="$(grep -ci 'using precompiled header' "${log_path}" || true)"
  echo "PCH activity mode=${mode}: created=${created} used=${used}"
  if [[ "${created}" == "0" && "${used}" == "0" ]]; then
    echo "WARNING: no automatic PCH create/use messages found in ${log_path}" >&2
  fi
}

postprocess_profile() {
  local profile_base="$1"
  local run_log="$2"
  local run_cache="$3"

  if [[ "${BENCH_PROFILE_TOOL}" == "none" || "${BENCH_PROFILE_POSTPROCESS}" != "true" ]]; then
    return
  fi

  {
    echo "Spark log: ${run_log}"
    echo "RTCX cache and embedded CUDA sources: ${run_cache}"
    echo "Generated row-IR UDFs: search the Spark log for 'Generated code for transform:'"
  } > "${profile_base}_sources.txt"

  case "${BENCH_PROFILE_TOOL}" in
    nsys)
      local nsys_report="${profile_base}.nsys-rep"
      if [[ ! -s "${nsys_report}" ]]; then
        echo "WARNING: NSYS report not found: ${nsys_report}" >&2
        return
      fi
      if ! "${BENCH_NSYS_BIN}" stats \
          --report cuda_gpu_kern_sum,cuda_api_sum,nvtx_sum \
          "${nsys_report}" > "${profile_base}_stats.txt" 2>&1; then
        echo "WARNING: failed to generate NSYS stats for ${nsys_report}" >&2
      fi
      echo "NSYS report: ${nsys_report}"
      echo "NSYS stats:  ${profile_base}_stats.txt"
      ;;
    ncu)
      local ncu_report="${profile_base}.ncu-rep"
      if [[ ! -s "${ncu_report}" ]]; then
        echo "WARNING: NCU report not found: ${ncu_report}" >&2
        return
      fi
      if ! "${BENCH_NCU_BIN}" --import "${ncu_report}" --page source \
          --print-source ptx > "${profile_base}_ptx.txt" 2>&1; then
        echo "WARNING: failed to export NCU PTX source view for ${ncu_report}" >&2
      fi
      if ! "${BENCH_NCU_BIN}" --import "${ncu_report}" --page source \
          --print-source cuda,sass > "${profile_base}_cuda_sass.txt" 2>&1; then
        echo "WARNING: failed to export NCU CUDA/SASS source view for ${ncu_report}" >&2
      fi
      if ! "${BENCH_NCU_BIN}" --import "${ncu_report}" \
          --page details > "${profile_base}_details.txt" 2>&1; then
        echo "WARNING: failed to export NCU details for ${ncu_report}" >&2
      fi
      echo "NCU report:     ${ncu_report}"
      echo "NCU PTX:        ${profile_base}_ptx.txt"
      echo "NCU CUDA/SASS:  ${profile_base}_cuda_sass.txt"
      echo "NCU details:    ${profile_base}_details.txt"
      ;;
  esac
}

declare -a result_csvs

run_benchmarks() {
if [[ "${BENCH_SUMMARIZE_ONLY}" == "true" ]]; then
  mapfile -t result_csvs < <(
    find "${BENCH_RESULTS_DIR}" -maxdepth 1 -name '*.csv' ! -name 'summary.csv' \
      -printf '%p\n' | sort -V
  )
  summarize_results
else
  declare -a suites counts depths modes
  split_csv "${BENCH_EXPR_SUITES}" suites
  split_csv "${BENCH_EXPR_COUNTS}" counts
  split_csv "${BENCH_EXPR_DEPTHS}" depths
  split_csv "${BENCH_MODES}" modes

  spark_args=(
    --jars "${RAPIDS_JAR}"
    --conf spark.plugins=com.nvidia.spark.SQLPlugin
    --conf "spark.executorEnv.LIBCUDF_JIT_ENABLED=${LIBCUDF_JIT_ENABLED}"
    --conf "spark.executorEnv.LIBCUDF_JIT_VERBOSE=${LIBCUDF_JIT_VERBOSE}"
    --conf "spark.executorEnv.LIBCUDF_JIT_DUMP_CODEGEN=${LIBCUDF_JIT_DUMP_CODEGEN}"
  )

  if [[ -n "${LD_LIBRARY_PATH:-}" ]]; then
    spark_args+=(--conf "spark.executorEnv.LD_LIBRARY_PATH=${LD_LIBRARY_PATH}")
  fi
  if [[ -n "${SPARK_MASTER:-}" ]]; then
    spark_args+=(--master "${SPARK_MASTER}")
  fi
  if [[ -n "${BENCH_SPARK_ARGS:-}" ]]; then
    # shellcheck disable=SC2206
    extra_spark_args=(${BENCH_SPARK_ARGS})
    spark_args+=("${extra_spark_args[@]}")
  fi

  run_index=0
  regenerate_next="${BENCH_REGENERATE_DATA}"

  for suite in "${suites[@]}"; do
    suite="${suite//[[:space:]]/}"
    for count in "${counts[@]}"; do
      count="${count//[[:space:]]/}"
      for depth in "${depths[@]}"; do
        depth="${depth//[[:space:]]/}"
        for mode in "${modes[@]}"; do
          mode="${mode//[[:space:]]/}"
          for repeat in $(seq 1 "${BENCH_APP_REPEATS}"); do
            run_index=$((run_index + 1))
            safe_suite="$(safe_name "${suite}")"
            safe_mode="$(safe_name "${mode}")"
            run_name="${run_index}_${safe_suite}_c${count}_d${depth}_${safe_mode}_r${repeat}"
            run_csv="${BENCH_RESULTS_DIR}/${run_name}.csv"
            run_log="${BENCH_RESULTS_DIR}/${run_name}.log"
            profile_base="${BENCH_PROFILE_OUTPUT_DIR}/${run_name}"
            cache_pair_name="${safe_suite}_c${count}_d${depth}_r${repeat}"
            if [[ "${mode}" == "GPU_AST_JIT_COLD" ]]; then
              run_cache="${BENCH_KERNEL_CACHE_BASE}/${cache_pair_name}"
              reset_run_cache=true
            elif [[ "${mode}" == "GPU_AST_JIT_DISK_WARM" ]]; then
              run_cache="${BENCH_KERNEL_CACHE_BASE}/${cache_pair_name}"
              reset_run_cache=false
            else
              run_cache="${BENCH_KERNEL_CACHE_BASE}/${run_name}"
              reset_run_cache=true
            fi

            if [[ "${BENCH_SKIP_EXISTING}" == "true" && -s "${run_csv}" ]]; then
              echo "[$run_index] skip existing suite=${suite} count=${count} depth=${depth} " \
                "mode=${mode} repeat=${repeat}"
              result_csvs+=("${run_csv}")
              regenerate_next=false
              continue
            fi

            if is_ast_mode "${mode}"; then
              case "$(realpath -m "${run_cache}")" in
                "$(realpath -m "${BENCH_KERNEL_CACHE_BASE}")"/*) ;;
                *)
                  echo "Refusing to clear cache outside BENCH_KERNEL_CACHE_BASE: ${run_cache}" >&2
                  exit 1
                  ;;
              esac
              if [[ "${reset_run_cache}" == "true" ]]; then
                rm -rf "${run_cache}"
                mkdir -p "${run_cache}"
              elif [[ ! -d "${run_cache}" ||
                  -z "$(find "${run_cache}" -mindepth 1 -print -quit)" ]]; then
                echo "Disk-warm mode requires a populated matching cold cache: ${run_cache}" >&2
                echo "Run GPU_AST_JIT_COLD before GPU_AST_JIT_DISK_WARM with the same shape and repeat." >&2
                exit 1
              fi
            fi

            run_spark_args=("${spark_args[@]}")
            if is_ast_mode "${mode}"; then
              run_spark_args+=(
                --conf "spark.executorEnv.LIBCUDF_KERNEL_CACHE_PATH=${run_cache}"
              )
            fi

            echo "[$run_index] suite=${suite} count=${count} depth=${depth} " \
              "mode=${mode} repeat=${repeat}"
            spark_command=(
              "${SPARK_HOME}/bin/spark-shell"
              "${run_spark_args[@]}"
              -i "${BENCH_SCALA}"
            )
            case "${BENCH_PROFILE_TOOL}" in
              none)
                run_command=("${spark_command[@]}")
                ;;
              nsys)
                run_command=(
                  "${BENCH_NSYS_BIN}" profile
                  "--trace=cuda,nvtx,osrt"
                  --sample=none
                  --cpuctxsw=none
                  --force-overwrite=true
                  --output="${profile_base}"
                  "${spark_command[@]}"
                )
                ;;
              ncu)
                run_command=(
                  "${BENCH_NCU_BIN}"
                  --target-processes all
                  --set "${BENCH_NCU_SET}"
                  --kernel-name-base "${BENCH_NCU_KERNEL_NAME_BASE}"
                  --kernel-name "${BENCH_NCU_KERNEL_NAME}"
                  --launch-skip "${BENCH_NCU_LAUNCH_SKIP}"
                  --launch-count "${BENCH_NCU_LAUNCH_COUNT}"
                  --import-source yes
                  --source-folders "${run_cache}"
                  --force-overwrite
                  --export "${profile_base}"
                  "${spark_command[@]}"
                )
                ;;
            esac
            if (
              export BENCH_BASE_PATH
              export BENCH_DATA_PATH
              export BENCH_EXPR_SUITES="${suite}"
              export BENCH_EXPR_COUNTS="${count}"
              export BENCH_EXPR_DEPTHS="${depth}"
              export BENCH_MODES="${mode}"
              export BENCH_RESULT_CSV_PATH="${run_csv}"
              export BENCH_REGENERATE_DATA="${regenerate_next}"
              export BENCH_WARMUPS
              export BENCH_ITERS
              export BENCH_CONSUME_MODE
              export BENCH_PRINT_PLAN
              export BENCH_FRESH_APP_MODE=true
              export BENCH_CLEAR_JIT_CACHE_FOR_COLD=true
              export LIBCUDF_JIT_ENABLED
              export LIBCUDF_JIT_VERBOSE
              export LIBCUDF_JIT_DUMP_CODEGEN
              if is_ast_mode "${mode}"; then
                export LIBCUDF_KERNEL_CACHE_PATH="${run_cache}"
              fi
              "${run_command[@]}" < /dev/null > "${run_log}" 2>&1
            ); then
              :
            else
              run_status=$?
              echo "Spark/profiler command failed with status ${run_status}." >&2
              echo "See log: ${run_log}" >&2
              exit "${run_status}"
            fi
            if [[ ! -s "${run_csv}" ]]; then
              echo "Spark app did not write result CSV: ${run_csv}" >&2
              echo "See log: ${run_log}" >&2
              exit 1
            fi
            report_pch_activity "${mode}" "${run_log}"
            postprocess_profile "${profile_base}" "${run_log}" "${run_cache}"
            result_csvs+=("${run_csv}")
            regenerate_next=false
          done
        done
      done
    done
    summarize_results
  done
fi
}

summarize_results() {
python3 - "${BENCH_MARKDOWN_PATH}" "${BENCH_COMBINED_CSV_PATH}" "${BENCH_EXPR_SUITES}" \
  "${result_csvs[@]}" <<'PY'
import csv
import math
import os
import statistics
import sys
from collections import defaultdict

markdown_path = sys.argv[1]
combined_csv_path = sys.argv[2]
suite_order_arg = sys.argv[3]
csv_paths = sys.argv[4:]

mode_order = {
    "CPU": 0,
    "GPU_PROJECT": 1,
    "GPU_AST_JIT_COLD": 2,
    "GPU_AST_JIT_DISK_WARM": 3,
    "GPU_AST_JIT_PCH_WARM_KERNEL_COLD": 4,
    "GPU_AST_JIT_HOT": 5,
    "GPU_AST_JIT": 5,
}
suite_order = {
    ("mixed" if suite.strip().lower() == "all"
     else "casts" if suite.strip().lower() == "decimal_cast"
     else suite.strip().lower()): idx
    for idx, suite in enumerate(suite_order_arg.split(","))
    if suite.strip()
}

def to_float(value):
    if value is None or value == "" or value.lower() == "nan":
        return math.nan
    return float(value)

def fmt(value, places=1):
    if value is None or math.isnan(value):
        return "n/a"
    return f"{value:.{places}f}"

def mean(values):
    values = [v for v in values if not math.isnan(v)]
    if not values:
        return math.nan
    return statistics.fmean(values)

def field_float(row, name, fallback_name=None):
    value = row.get(name)
    if (value is None or value == "") and fallback_name:
        value = row.get(fallback_name)
    return to_float(value)

rows = []
for path in csv_paths:
    with open(path, newline="") as fh:
        for row in csv.DictReader(fh):
            if row["kind"] == "summary":
                rows.append(row)

groups = defaultdict(list)
for row in rows:
    key = (
        row["expr_suite"],
        int(row["expr_count"]),
        int(row["expr_depth"]),
        row["mode"],
        row["jit_cache_state"],
    )
    groups[key].append(row)

summary = []
for key, group_rows in groups.items():
    suite, expr_count, expr_depth, mode, cache_state = key
    item = {
        "expr_suite": suite,
        "expr_count": expr_count,
        "expr_depth": expr_depth,
        "mode": mode,
        "jit_cache_state": cache_state,
        "app_runs": len(group_rows),
        "avg_wall_ms": mean([field_float(r, "wall_ms") for r in group_rows]),
        "min_wall_ms": min([field_float(r, "min_wall_ms", "wall_ms") for r in group_rows]),
        "project_op_ms": mean([field_float(r, "project_op_ms") for r in group_rows]),
        "compile_asts_ms": mean([field_float(r, "compile_asts_ms") for r in group_rows]),
        "compute_asts_ms": mean([field_float(r, "compute_asts_ms") for r in group_rows]),
        "project_no_compile_ms": mean(
            [field_float(r, "project_no_compile_ms") for r in group_rows]),
        "project_other_ms": mean([field_float(r, "project_other_ms") for r in group_rows]),
        "all_gpu_op_ms": mean([field_float(r, "all_gpu_op_ms") for r in group_rows]),
    }
    summary.append(item)

cpu_wall = {
    (r["expr_suite"], r["expr_count"], r["expr_depth"]): r["avg_wall_ms"]
    for r in summary if r["mode"] == "CPU"
}
for item in summary:
    base = cpu_wall.get((item["expr_suite"], item["expr_count"], item["expr_depth"]))
    item["speedup_vs_cpu"] = (
        base / item["avg_wall_ms"]
        if base and not math.isnan(base) and not math.isnan(item["avg_wall_ms"])
        else math.nan
    )

summary.sort(key=lambda r: (
    suite_order.get(r["expr_suite"], 1000),
    r["expr_suite"],
    r["expr_count"],
    r["expr_depth"],
    mode_order.get(r["mode"], 100),
    r["mode"],
))

fieldnames = [
    "expr_suite",
    "expr_count",
    "expr_depth",
    "mode",
    "jit_cache_state",
    "app_runs",
    "avg_wall_ms",
    "min_wall_ms",
    "speedup_vs_cpu",
    "project_op_ms",
    "compile_asts_ms",
    "compute_asts_ms",
    "project_no_compile_ms",
    "project_other_ms",
    "all_gpu_op_ms",
]
os.makedirs(os.path.dirname(combined_csv_path), exist_ok=True)
with open(combined_csv_path, "w", newline="") as out:
    writer = csv.DictWriter(out, fieldnames=fieldnames)
    writer.writeheader()
    for item in summary:
        writer.writerow({k: item[k] for k in fieldnames})

os.makedirs(os.path.dirname(markdown_path), exist_ok=True)
with open(markdown_path, "w") as out:
    out.write("# ANSI JIT Project Benchmark\n\n")
    out.write(f"- rows: `{os.environ.get('BENCH_ROWS', '100000000')}`\n")
    out.write(f"- partitions: `{os.environ.get('BENCH_PARTITIONS', 'default')}`\n")
    out.write(f"- app repeats: `{os.environ.get('BENCH_APP_REPEATS', '1')}`\n")
    out.write(f"- warmups: `{os.environ.get('BENCH_WARMUPS', '1')}`\n")
    out.write(f"- hot iterations per app: `{os.environ.get('BENCH_ITERS', '3')}`\n")
    out.write(f"- completed app csvs: `{len(csv_paths)}`\n")
    out.write(f"- combined csv: `{combined_csv_path}`\n\n")

    suites = []
    for item in summary:
        if item["expr_suite"] not in suites:
            suites.append(item["expr_suite"])

    for suite in suites:
        out.write(f"## {suite}\n\n")
        out.write("| exprs | depth | mode | cache | apps | avg wall ms | min wall ms | "
                  "speedup vs CPU | project op ms | compile ASTs ms | "
                  "compute ASTs ms | project other ms | all GPU op ms |\n")
        out.write("|---:|---:|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|\n")
        for item in summary:
            if item["expr_suite"] != suite:
                continue
            out.write(
                f"| {item['expr_count']} | {item['expr_depth']} | {item['mode']} | "
                f"{item['jit_cache_state']} | {item['app_runs']} | "
                f"{fmt(item['avg_wall_ms'])} | {fmt(item['min_wall_ms'])} | "
                f"{fmt(item['speedup_vs_cpu'], 2)} | {fmt(item['project_op_ms'])} | "
                f"{fmt(item['compile_asts_ms'])} | {fmt(item['compute_asts_ms'])} | "
                f"{fmt(item['project_other_ms'])} | "
                f"{fmt(item['all_gpu_op_ms'])} |\n"
            )
        out.write("\n")

print(markdown_path)
PY

echo "Markdown summary: ${BENCH_MARKDOWN_PATH}"
echo "Combined CSV: ${BENCH_COMBINED_CSV_PATH}"
}

run_benchmarks
