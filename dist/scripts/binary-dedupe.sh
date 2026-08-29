#!/bin/bash

# Copyright (c) 2021-2026, NVIDIA CORPORATION.
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


# PWD should be dist/target
set -e

start_time=$(date +%s)

[[ "${SKIP_BINARY_DEDUPE:-0}" == "1" ]] && {
  echo "Skipping binary-dedupe. Unset SKIP_BINARY_DEDUPE to activate binary-dedupe"
  exit 0
}
case "$OSTYPE" in
  darwin*)
    export SHASUM="shasum -b"
    ;;
  *)
    export SHASUM="sha1sum -b"
    ;;
esac

STEP=0
export SPARK_SHARED_TXT="$PWD/spark-shared.txt"
export SPARK_SHARED_CLASSES_TXT="$PWD/spark-shared-classes.txt"
export SPARK_SHARED_COPY_LIST="$PWD/spark-shared-copy-list.txt"
export DELETE_DUPLICATES_TXT="$PWD/delete-duplicates.txt"
export SPARK_SHARED_DIR="$PWD/spark-shared"
export UNSHIMMED_FROM_SPARK_SHARED_COPY_LIST="$PWD/unshimmed-from-spark-shared-copy-list.txt"
export ROOT_SAFE_SPARK_SHARED_TXT="$PWD/root-safe-spark-shared.txt"
export DEFAULT_UNSHIMMED_SPARK_SHARED_TXT="$PWD/default-unshimmed-spark-shared.txt"
export UNSHIMMED_NEED_SHARED_TXT="$PWD/unshimmed-need-shared.txt"
export UNSHIMMED_MISSING_SHARED_TXT="$PWD/unshimmed-missing-shared.txt"
KEEP_IN_SPARK_SHARED_PATTERNS=()
KEEP_IN_SPARK_SHARED_PATTERNS_LOADED=0
KEEP_IN_SPARK_SHIM_DIRS_PATTERNS=()
KEEP_IN_SPARK_SHIM_DIRS_PATTERNS_LOADED=0

SPARK_SHIM_DIRS=()
if [[ "${UNSHIM_FAST:-0}" == "1" ]]; then
  while IFS= read -r shim_dir; do
    SPARK_SHIM_DIRS+=("$shim_dir")
  done < <(find ./parallel-world -maxdepth 1 -mindepth 1 -type d -name 'spark[345]*' | sort)
fi

DEDUPE_CACHE_DIR="${UNSHIM_DEDUPE_CACHE_DIR:-}"
DEDUPE_CACHE_SPARK_SHARED_TXT=""
DEDUPE_CACHE_SHA1_FILES_TXT=""
DEDUPE_CACHE_SHIM_SHA_PACKAGE_FILES_TXT=""
DEDUPE_CACHE_COUNT_SHIM_SHA_PACKAGE_FILES_TXT=""
if [[ -n "$DEDUPE_CACHE_DIR" ]]; then
  DEDUPE_CACHE_SPARK_SHARED_TXT="$DEDUPE_CACHE_DIR/spark-shared.txt"
  DEDUPE_CACHE_SHA1_FILES_TXT="$DEDUPE_CACHE_DIR/tmp-sha1-files.txt"
  DEDUPE_CACHE_SHIM_SHA_PACKAGE_FILES_TXT="$DEDUPE_CACHE_DIR/tmp-shim-sha-package-files.txt"
  DEDUPE_CACHE_COUNT_SHIM_SHA_PACKAGE_FILES_TXT="$DEDUPE_CACHE_DIR/tmp-count-shim-sha-package-files.txt"
fi

# This script de-duplicates .class files at the binary level.
# We could also diff classes using scalap / javap outputs.
# However, with observed warnings in the output we have no guarantee that the
# output is complete, and that the complete output would not exhibit diffs.
# We compute and compare checksum signatures of same-named classes

# The following pipeline determines identical classes across shims in this build.
# - checksum all class files
# - move the varying-prefix sparkxyz to the left so it can be easily skipped for uniq and sort
# - sort by path, secondary sort by checksum, print one line per group
# - produce uniq count for paths
# - filter the paths with count=1, the class files without diverging checksums
# - put the path starting with /sparkxyz back together for the final list
echo "Retrieving class files hashing to a single value ..."

CACHE_HIT=0
if [[ -n "$DEDUPE_CACHE_SPARK_SHARED_TXT" && \
      -f "$DEDUPE_CACHE_SPARK_SHARED_TXT" && \
      -f "$DEDUPE_CACHE_SHA1_FILES_TXT" && \
      -f "$DEDUPE_CACHE_SHIM_SHA_PACKAGE_FILES_TXT" && \
      -f "$DEDUPE_CACHE_COUNT_SHIM_SHA_PACKAGE_FILES_TXT" ]]; then
  echo "$((++STEP))/ reusing cached files with unique sha1 > $SPARK_SHARED_TXT"
  cp "$DEDUPE_CACHE_SPARK_SHARED_TXT" "$SPARK_SHARED_TXT"
  cp "$DEDUPE_CACHE_SHA1_FILES_TXT" tmp-sha1-files.txt
  cp "$DEDUPE_CACHE_SHIM_SHA_PACKAGE_FILES_TXT" tmp-shim-sha-package-files.txt
  cp "$DEDUPE_CACHE_COUNT_SHIM_SHA_PACKAGE_FILES_TXT" tmp-count-shim-sha-package-files.txt
  CACHE_HIT=1
# With one shim there is no cross-shim identity proof to perform; every
# non-META file is the sole representative for its path.
elif [[ "${UNSHIM_FAST:-0}" == "1" && "${#SPARK_SHIM_DIRS[@]}" == "1" ]]; then
  echo "$((++STEP))/ single shim fast path; listing files > $SPARK_SHARED_TXT"
  : > tmp-sha1-files.txt
  : > tmp-shim-sha-package-files.txt
  : > tmp-count-shim-sha-package-files.txt
  find "${SPARK_SHIM_DIRS[0]}" -name META-INF -prune -o -name webapps -prune -o \( -type f -print \) | \
    sort | sed 's|^\./parallel-world||' > "$SPARK_SHARED_TXT"
else
  echo "$((++STEP))/ SHA1 of all non-META files > tmp-sha1-files.txt"
  find ./parallel-world/spark[345]* -name META-INF -prune -o -name webapps -prune -o \( -type f -print0 \) | \
    xargs --null $SHASUM > tmp-sha1-files.txt

  echo "$((++STEP))/ make shim column 1 > tmp-shim-sha-package-files.txt"
  < tmp-sha1-files.txt awk -F/ '$1=$1' | \
    awk '{checksum=$1; shim=$4; $1=shim; $2=$3=""; $4=checksum;  print $0}' | \
    tr -s  ' ' > tmp-shim-sha-package-files.txt

  echo "$((++STEP))/ sort by path, sha1; output first from each group > tmp-count-shim-sha-package-files.txt"
  sort -k3 -k2,2 -u tmp-shim-sha-package-files.txt | \
    uniq -f 2 -c > tmp-count-shim-sha-package-files.txt

  echo "$((++STEP))/ files with unique sha1 > $SPARK_SHARED_TXT"
  grep '^\s\+1 .*' tmp-count-shim-sha-package-files.txt | \
    awk '{$1=""; $3=""; print $0 }' | \
    tr -s ' ' | sed 's/\ /\//g' > "$SPARK_SHARED_TXT"
fi

if [[ "$CACHE_HIT" == "0" && -n "$DEDUPE_CACHE_SPARK_SHARED_TXT" ]]; then
  mkdir -p "$DEDUPE_CACHE_DIR"
  cp "$SPARK_SHARED_TXT" "$DEDUPE_CACHE_SPARK_SHARED_TXT"
  cp tmp-sha1-files.txt "$DEDUPE_CACHE_SHA1_FILES_TXT"
  cp tmp-shim-sha-package-files.txt "$DEDUPE_CACHE_SHIM_SHA_PACKAGE_FILES_TXT"
  cp tmp-count-shim-sha-package-files.txt "$DEDUPE_CACHE_COUNT_SHIM_SHA_PACKAGE_FILES_TXT"
fi

function load_keep_in_spark_shim_dirs_patterns() {
  set -e
  [[ "$KEEP_IN_SPARK_SHIM_DIRS_PATTERNS_LOADED" == "0" ]] || return 0
  KEEP_IN_SPARK_SHIM_DIRS_PATTERNS_LOADED=1

  local keep_patterns_txt="${KEEP_IN_SPARK_SHIM_DIRS_TXT:-}"
  [[ -n "$keep_patterns_txt" ]] || return 0
  [[ -f "$keep_patterns_txt" ]] || {
    echo >&2 "Keep-in-spark-shim-dirs list does not exist: $keep_patterns_txt"
    exit 255
  }

  local pattern
  while IFS= read -r pattern; do
    [[ -n "$pattern" ]] || continue
    [[ "$pattern" =~ ^[[:space:]]*# ]] && continue
    KEEP_IN_SPARK_SHIM_DIRS_PATTERNS+=("$pattern")
  done < "$keep_patterns_txt"
}

function keep_in_spark_shim_dirs() {
  set -e
  local class_file="$1"
  local pattern
  for pattern in "${KEEP_IN_SPARK_SHIM_DIRS_PATTERNS[@]}"; do
    # shellcheck disable=SC2053
    if [[ "$class_file" == $pattern ]]; then
      return 0
    fi
  done
  return 1
}

function filter_keep_in_spark_shim_dirs() {
  set -e
  load_keep_in_spark_shim_dirs_patterns
  [[ "${#KEEP_IN_SPARK_SHIM_DIRS_PATTERNS[@]}" -gt 0 ]] || return 0

  local tmp_txt="$SPARK_SHARED_TXT.tmp"
  local class_resource
  local path_without_leading_slash
  local class_file

  echo "$((++STEP))/ retaining selected classes in Spark shim directories"
  : > "$tmp_txt"
  while IFS= read -r class_resource; do
    [[ -n "$class_resource" ]] || continue
    path_without_leading_slash="${class_resource#/}"
    class_file="${path_without_leading_slash#*/}"
    if keep_in_spark_shim_dirs "$class_file"; then
      # Root-layout classes were explicitly selected by the packager and must
      # remain eligible for shared-identity validation so the later root
      # de-duplication can remove their shim copies.
      if [[ -f "./parallel-world/$class_file" ]]; then
        echo "$class_resource"
      fi
      continue
    fi
    echo "$class_resource"
  done < "$SPARK_SHARED_TXT" > "$tmp_txt"
  mv "$tmp_txt" "$SPARK_SHARED_TXT"
}

filter_keep_in_spark_shim_dirs

function retain_single_copy() {
  set -e
  class_resource="$1"
  # example input: /spark320/com/nvidia/spark/udf/Repr$UnknownCapturedArg$.class

  IFS='/' read -ra path_parts <<< "$class_resource"
  # declare -p path_parts
  # declare -a path_parts='([0]="" [1]="spark320" [2]="com" [3]="nvidia" [4]="spark" [5]="udf" [6]="Repr\$UnknownCapturedArg\$.class")'
  shim="${path_parts[1]}"

  package_class_parts=("${path_parts[@]:2}")

  package_class_with_spaces="${package_class_parts[*]}"
  # com/nvidia/spark/udf/Repr\$UnknownCapturedArg\$.class
  package_class="${package_class_with_spaces// //}"

  # get the reference copy out of the way
  echo "$package_class" >> "from-$shim-to-spark-shared.txt"
  # expanding directories separately because full path
  # glob is broken for class file name including the "$" character
  for pw in ./parallel-world/spark[345]* ; do
    delete_path="$pw/$package_class"
    [[ -f "$delete_path" ]] && echo "$delete_path" || true
  done >> "$DELETE_DUPLICATES_TXT" || exit 255
}

function append_matching_spark_shared_patterns() {
  set -e
  local unshimmed_patterns_txt="$1"
  local output_txt="$2"

  [[ -n "$unshimmed_patterns_txt" ]] || return 0
  [[ -f "$unshimmed_patterns_txt" ]] || {
    echo >&2 "Unshimmed common list does not exist: $unshimmed_patterns_txt"
    exit 255
  }

  local shared_dir="./parallel-world/spark-shared"
  local pattern
  while IFS= read -r pattern; do
    [[ -n "$pattern" ]] || continue
    [[ "$pattern" =~ ^[[:space:]]*# ]] && continue
    case "$pattern" in
      *[\*\?\[]*)
        find "$shared_dir" -type f -path "$shared_dir/$pattern" |
          sed "s|^\./parallel-world/spark-shared/||" >> "$output_txt"
        ;;
      *)
        if [[ -f "$shared_dir/$pattern" ]]; then
          echo "$pattern" >> "$output_txt"
        fi
        ;;
    esac
  done < "$unshimmed_patterns_txt"
}

function write_root_safe_spark_shared_classes() {
  set -e
  local analyzer_script="${UNSHIM_ANALYZER_SCRIPT:-}"
  if [[ -z "$analyzer_script" && -n "${UNSHIMMED_COMMON_FROM_SINGLE_SHIM_TXT:-}" ]]; then
    analyzer_script="$(dirname "$UNSHIMMED_COMMON_FROM_SINGLE_SHIM_TXT")/scripts/analyze-parallel-world-deps.py"
  fi
  [[ -n "$analyzer_script" && -f "$analyzer_script" ]] || {
    echo >&2 "WARNING: cannot locate analyze-parallel-world-deps.py; skipping diagnostic default unshim analysis"
    : > "$ROOT_SAFE_SPARK_SHARED_TXT"
    return 0
  }

  echo "$((++STEP))/ analyzing spark-shared dependency paths > $ROOT_SAFE_SPARK_SHARED_TXT"
  if ! python3 "$analyzer_script" ./parallel-world \
      --write-safe-paths "$ROOT_SAFE_SPARK_SHARED_TXT"; then
    echo >&2 "WARNING: spark-shared dependency analysis failed; continuing because it is diagnostic"
    : > "$ROOT_SAFE_SPARK_SHARED_TXT"
  fi
}

function write_default_unshimmed_spark_shared_classes() {
  set -e
  echo "$((++STEP))/ selecting all bitwise-identical spark-shared classes > $DEFAULT_UNSHIMMED_SPARK_SHARED_TXT"
  sed -E "s|^/spark[^/]*/||" "$SPARK_SHARED_TXT" | \
    grep '\.class$' | sort -u > "$DEFAULT_UNSHIMMED_SPARK_SHARED_TXT"
}

function load_keep_in_spark_shared_patterns() {
  set -e
  [[ "$KEEP_IN_SPARK_SHARED_PATTERNS_LOADED" == "0" ]] || return 0
  KEEP_IN_SPARK_SHARED_PATTERNS_LOADED=1

  local keep_patterns_txt="${KEEP_IN_SPARK_SHARED_TXT:-}"
  [[ -n "$keep_patterns_txt" ]] || return 0
  [[ -f "$keep_patterns_txt" ]] || {
    echo >&2 "Keep-in-spark-shared list does not exist: $keep_patterns_txt"
    exit 255
  }

  local pattern
  while IFS= read -r pattern; do
    [[ -n "$pattern" ]] || continue
    [[ "$pattern" =~ ^[[:space:]]*# ]] && continue
    KEEP_IN_SPARK_SHARED_PATTERNS+=("$pattern")
  done < "$keep_patterns_txt"
}

function keep_in_spark_shared() {
  set -e
  local class_file="$1"
  local pattern
  for pattern in "${KEEP_IN_SPARK_SHARED_PATTERNS[@]}"; do
    # shellcheck disable=SC2053
    if [[ "$class_file" == $pattern ]]; then
      return 0
    fi
  done
  return 1
}

function filter_keep_in_spark_shared() {
  set -e
  local input_txt="$1"
  local output_txt="$2"
  local class_file
  load_keep_in_spark_shared_patterns
  : > "$output_txt"
  while IFS= read -r class_file; do
    [[ -n "$class_file" ]] || continue
    if keep_in_spark_shared "$class_file"; then
      continue
    fi
    echo "$class_file"
  done < "$input_txt" > "$output_txt.tmp"
  mv "$output_txt.tmp" "$output_txt"
}

function copy_unshimmed_from_spark_shared() {
  set -e
  local raw_copy_list="$UNSHIMMED_FROM_SPARK_SHARED_COPY_LIST.raw"
  local sorted_copy_list="$UNSHIMMED_FROM_SPARK_SHARED_COPY_LIST.sorted"

  : > "$raw_copy_list"
  local promote_default_spark_shared=0
  if [[ "${UNSHIM_PROMOTE_DEFAULT_SPARK_SHARED_CLASSES:-0}" == "1" ||
        "${UNSHIM_PROMOTE_DEFAULT_SPARK_SHARED_CLASSES:-0}" == "true" ]]; then
    promote_default_spark_shared=1
  fi

  # The dependency analysis is diagnostic-only. Keep it for fast unshim analysis
  # and promotion experiments, but do not put it on the normal packaging hot path.
  if [[ "$promote_default_spark_shared" == "1" || "${UNSHIM_FAST:-0}" == "1" ]]; then
    write_root_safe_spark_shared_classes
  else
    echo "$((++STEP))/ skipping diagnostic spark-shared dependency analysis"
    : > "$ROOT_SAFE_SPARK_SHARED_TXT"
  fi

  if [[ "$promote_default_spark_shared" == "1" ]]; then
    write_default_unshimmed_spark_shared_classes
    cat "$DEFAULT_UNSHIMMED_SPARK_SHARED_TXT" >> "$raw_copy_list"
  else
    echo "$((++STEP))/ default spark-shared class promotion disabled"
    : > "$DEFAULT_UNSHIMMED_SPARK_SHARED_TXT"
  fi
  append_matching_spark_shared_patterns \
    "${UNSHIMMED_COMMON_FROM_SINGLE_SHIM_TXT:-}" "$raw_copy_list"

  sort -u "$raw_copy_list" > "$sorted_copy_list"
  filter_keep_in_spark_shared "$sorted_copy_list" "$UNSHIMMED_FROM_SPARK_SHARED_COPY_LIST"
  if [[ -s "$UNSHIMMED_FROM_SPARK_SHARED_COPY_LIST" ]]; then
    echo "Promoting root-layout files from spark-shared"
    rsync --files-from="$UNSHIMMED_FROM_SPARK_SHARED_COPY_LIST" \
      ./parallel-world/spark-shared ./parallel-world
  fi
}

# this belongs into maven initialize phase, left in here for easier
# standalone debugging
# truncate incremental files
: > "$DELETE_DUPLICATES_TXT"
rm -f from-spark[345]*-to-spark-shared.txt
rm -rf "$SPARK_SHARED_DIR"
mkdir -p "$SPARK_SHARED_DIR"

echo "$((++STEP))/ retaining a single copy of spark-shared classes"
awk -F/ "
  NF >= 3 {
    shim = \$2
    package_class = \$0
    sub(\"^/spark[345][^/]*/\", \"\", package_class)
    print package_class >> (\"from-\" shim \"-to-spark-shared.txt\")
  }
" "$SPARK_SHARED_TXT"
for pw in ./parallel-world/spark[345]* ; do
  awk -v pw="$pw" "
    {
      package_class = \$0
      sub(\"^/spark[345][^/]*/\", \"\", package_class)
      print pw \"/\" package_class
    }
  " "$SPARK_SHARED_TXT"
done >> "$DELETE_DUPLICATES_TXT"

echo "$((++STEP))/ rsyncing common classes to $SPARK_SHARED_DIR"
for copy_list in from-spark[345]*-to-spark-shared.txt; do
  echo Initializing rsync of "$copy_list"
  IFS='-' read -ra copy_list_parts <<< "$copy_list"
  # declare -p copy_list_parts
  shim="${copy_list_parts[1]}"
  # use rsync to reduce process forking
  rsync --files-from="$copy_list" ./parallel-world/"$shim" "$SPARK_SHARED_DIR"
done

mv "$SPARK_SHARED_DIR" parallel-world/

echo "$((++STEP))/ promoting default spark-shared files to root layout"
copy_unshimmed_from_spark_shared

# Verify that all class files in the conventional jar location are bitwise
# identical regardless of the Spark-version-specific jar.
#
# At this point the duplicate classes have not been removed from version-specific jar
# locations such as parallel-world/spark321.
# For each unshimmed class file look for all of its copies inside /spark[345]* and
# and count the number of distinct checksums. There are two representative cases
# 1) The class is contributed to the unshimmed location via the unshimmed-from-each-spark345 list. These are classes
#    carrying the shim classifier in their package name such as
#    com.nvidia.spark.rapids.spark321.RapidsShuffleManager. They are unique by construction,
#    and will have zero copies in any non-spark321 shims. Although such classes are currently excluded from
#    being copied to the /spark321 Parallel World we keep the algorithm below general without assuming this.
#
# 2) The class is contributed to the unshimmed location via unshimmed-common. These are classes that
#    that have the same package and class name across all parallel worlds.
#
#  So if the number of distinct class files per class in the unshimmed location is < 2, the jar
#  is content is as expected
#
#  If we find an unshimmed class file occurring > 1  we fail the build and the code must be refactored
#  until bitwise-identity of each unshimmed class is restored.

# Determine the list of unshimmed class files
UNSHIMMED_LIST_TXT=unshimmed-result.txt
echo "$((++STEP))/ creating sorted list of root-layout unshimmed classes > $UNSHIMMED_LIST_TXT"
find ./parallel-world -name '*.class' \
  -not -path './parallel-world/spark[345-]*' \
  -not -path './parallel-world/spark-shared/*' | \
  cut -d/ -f 3- | sort > "$UNSHIMMED_LIST_TXT"

echo "$((++STEP))/ creating sorted list of spark-shared classes > $SPARK_SHARED_CLASSES_TXT"
sed -E "s|^/spark[^/]*/||" "$SPARK_SHARED_TXT" | sort -u > "$SPARK_SHARED_CLASSES_TXT"

function unshimmed_class_needs_shared_identity() {
  set -e
  class_file="$1"

  # Most root-layout classes with the same FQCN must be bitwise-identical across
  # the selected shim jars. This function preserves only root-visible legacy
  # exceptions that predate default unshimming. These classes have compatible
  # executable bytecode for their supported runtime paths, but differ in Scala
  # metadata, debug attributes, or Spark-dependency-shaped signatures.
  # SparkRapidsBuildInfoEvent is root-loaded during plugin initialization along
  # with root-level build-info resources; Databricks shim metadata can differ.
  #
  # Keep this list narrow. Do not add a class here when it can stay in
  # spark-shared without being referenced from root-loaded code.
  class_file_quoted=$(printf "%q" "$class_file")
  if [[ "$class_file_quoted" =~ com/nvidia/spark/rapids/spark[345].*/.*ShuffleManager.class || \
          "$class_file_quoted" == "com/nvidia/spark/ParquetCachedBatchSerializer.class" || \
          "$class_file_quoted" =~ org/apache/spark/sql/rapids/ProxyRapidsShuffleInternalManagerBase || \
          "$class_file_quoted" =~ com/nvidia/spark/rapids/SparkRapidsBuildInfoEvent.*\.class || \
          "$class_file_quoted" =~ org/apache/spark/sql/rapids/execution/TrampolineUtil.*\.class || \
          "$class_file_quoted" =~ com/nvidia/spark/rapids/shims/GpuBroadcastJoinMeta.*\.class || \
          "$class_file_quoted" == "org/apache/spark/sql/rapids/GpuShuffleDependency.class" || \
          "$class_file_quoted" == "com/nvidia/spark/rapids/parquet/CloseableColumnBatchIterator.class" || \
          "$class_file_quoted" == "com/nvidia/spark/rapids/GpuReadCSVFileFormat.class" || \
          "$class_file_quoted" == "org/apache/spark/sql/catalyst/json/rapids/GpuReadJsonFileFormat.class" || \
          "$class_file_quoted" == "com/nvidia/spark/rapids/shims/PythonMapInArrowExecShims.class" || \
          "$class_file_quoted" == "org/apache/spark/sql/rapids/execution/python/shims/PythonArgumentUtils.class" || \
          "$class_file_quoted" == "com/nvidia/spark/rapids/shims/GpuUnionExecShim.class" || \
          "$class_file_quoted" == "org/apache/spark/sql/rapids/GpuStringTrim.class" || \
          "$class_file_quoted" == "org/apache/spark/sql/rapids/GpuStringTrimLeft.class" || \
          "$class_file_quoted" == "org/apache/spark/sql/rapids/GpuStringTrimRight.class" || \
          "$class_file" == "org/apache/spark/sql/execution/datasources/v2/rapids/GpuAtomicCreateTableAsSelectExec$.class" || \
          "$class_file_quoted" == "org/apache/spark/sql/rapids/shims/RapidsErrorUtils.class" || \
          "$class_file_quoted" == "org/apache/spark/sql/rapids/execution/python/shims/WindowInPandasExecTypeShim.class" ]]; then
      return 1
  fi
  return 0
}

echo "$((++STEP))/ filtering unshimmed classes that require shared identity > $UNSHIMMED_NEED_SHARED_TXT"
while read -r unshimmed_class; do
  if unshimmed_class_needs_shared_identity "$unshimmed_class"; then
    echo "$unshimmed_class"
  fi
done < "$UNSHIMMED_LIST_TXT" | sort -u > "$UNSHIMMED_NEED_SHARED_TXT"

echo "$((++STEP))/ verifying unshimmed classes have unique sha1 across shims"
comm -23 "$UNSHIMMED_NEED_SHARED_TXT" "$SPARK_SHARED_CLASSES_TXT" > "$UNSHIMMED_MISSING_SHARED_TXT"
if [[ -s "$UNSHIMMED_MISSING_SHARED_TXT" ]]; then
  read -r missing_unshimmed_class < "$UNSHIMMED_MISSING_SHARED_TXT"
  echo >&2 "$missing_unshimmed_class is not bitwise-identical across shims"
  exit 255
fi

# Remove unshimmed classes from parallel worlds
# TODO rework with low priority, only a few classes.
echo "$((++STEP))/ removing duplicates of unshimmed classes"
{
  sed "s|^|./parallel-world/spark-shared/|" "$UNSHIMMED_LIST_TXT"
  for pw in ./parallel-world/spark[345-]* ; do
    awk -v pw="$pw" "{ print pw \"/\" \$0 }" "$UNSHIMMED_LIST_TXT"
  done
} >> "$DELETE_DUPLICATES_TXT"

echo "$((++STEP))/ deleting all class files listed in $DELETE_DUPLICATES_TXT"
< "$DELETE_DUPLICATES_TXT" sort -u | xargs rm -f

end_time=$(date +%s)
echo "binary-dedupe completed in $((end_time - start_time)) seconds"
