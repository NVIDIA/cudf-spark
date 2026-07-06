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
export SPARK_SHARED_COPY_LIST="$PWD/spark-shared-copy-list.txt"
export DELETE_DUPLICATES_TXT="$PWD/delete-duplicates.txt"
export SPARK_SHARED_DIR="$PWD/spark-shared"
export UNSHIMMED_FROM_SPARK_SHARED_COPY_LIST="$PWD/unshimmed-from-spark-shared-copy-list.txt"

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


echo "$((++STEP))/ SHA1 of all non-META files > tmp-sha1-files.txt"
find ./parallel-world/spark[34]* -name META-INF -prune -o -name webapps -prune -o \( -type f -print0 \) | \
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

function retain_single_copy() {
  set -e
  class_resource="$1"
  # example input: /spark320/com/nvidia/spark/udf/Repr$UnknownCapturedArg$.class

  IFS='/' <<< "$class_resource" read -ra path_parts
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
  for pw in ./parallel-world/spark[34]* ; do
    delete_path="$pw/$package_class"
    [[ -f "$delete_path" ]] && echo "$delete_path" || true
  done >> "$DELETE_DUPLICATES_TXT" || exit 255
}

function copy_unshimmed_from_spark_shared() {
  set -e
  local unshimmed_patterns_txt="${UNSHIMMED_COMMON_FROM_SINGLE_SHIM_TXT:-}"

  [[ -n "$unshimmed_patterns_txt" ]] || return 0
  [[ -f "$unshimmed_patterns_txt" ]] || {
    echo >&2 "Unshimmed common list does not exist: $unshimmed_patterns_txt"
    exit 255
  }

  : > "$UNSHIMMED_FROM_SPARK_SHARED_COPY_LIST"
  while read -r shared_path; do
    local rel_path="${shared_path#./parallel-world/spark-shared/}"
    local pattern
    while read -r pattern; do
      [[ -n "$pattern" ]] || continue
      [[ "$pattern" =~ ^[[:space:]]*# ]] && continue
      # shellcheck disable=SC2053
      if [[ "$rel_path" == $pattern ]]; then
        echo "$rel_path" >> "$UNSHIMMED_FROM_SPARK_SHARED_COPY_LIST"
        break
      fi
    done < "$unshimmed_patterns_txt"
  done < <(find ./parallel-world/spark-shared -type f)

  if [[ -s "$UNSHIMMED_FROM_SPARK_SHARED_COPY_LIST" ]]; then
    echo "Promoting root-layout files from spark-shared via $unshimmed_patterns_txt"
    rsync --files-from="$UNSHIMMED_FROM_SPARK_SHARED_COPY_LIST" \
      ./parallel-world/spark-shared ./parallel-world
  fi
}

# this belongs into maven initialize phase, left in here for easier
# standalone debugging
# truncate incremental files
: > "$DELETE_DUPLICATES_TXT"
rm -f from-spark[34]*-to-spark-shared.txt
rm -rf "$SPARK_SHARED_DIR"
mkdir -p "$SPARK_SHARED_DIR"

echo "$((++STEP))/ retaining a single copy of spark-shared classes"
while read -r spark_common_class; do
  retain_single_copy "$spark_common_class"
done < "$SPARK_SHARED_TXT"

echo "$((++STEP))/ rsyncing common classes to $SPARK_SHARED_DIR"
for copy_list in from-spark[34]*-to-spark-shared.txt; do
  echo Initializing rsync of "$copy_list"
  IFS='-' <<< "$copy_list" read -ra copy_list_parts
  # declare -p copy_list_parts
  shim="${copy_list_parts[1]}"
  # use rsync to reduce process forking
  rsync --files-from="$copy_list" ./parallel-world/"$shim" "$SPARK_SHARED_DIR"
done

mv "$SPARK_SHARED_DIR" parallel-world/

echo "$((++STEP))/ promoting allowlisted spark-shared files to root layout"
copy_unshimmed_from_spark_shared

# Verify that all class files in the conventional jar location are bitwise
# identical regardless of the Spark-version-specific jar.
#
# At this point the duplicate classes have not been removed from version-specific jar
# locations such as parallel-world/spark321.
# For each unshimmed class file look for all of its copies inside /spark[34]* and
# and count the number of distinct checksums. There are two representative cases
# 1) The class is contributed to the unshimmed location via the unshimmed-from-each-spark34 list. These are classes
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
echo "$((++STEP))/ creating sorted list of unshimmed classes > $UNSHIMMED_LIST_TXT"
find ./parallel-world -name '*.class' -not -path './parallel-world/spark[34-]*' | \
  cut -d/ -f 3- | sort > "$UNSHIMMED_LIST_TXT"

function verify_same_sha_for_unshimmed() {
  set -e
  class_file="$1"

  # the raw spark-shared.txt file list contains all single-sha1 classes
  # including the ones that are unshimmed. Instead of expensively recomputing
  # sha1 look up if there is an entry with the unshimmed class as a suffix

  class_file_quoted=$(printf '%q' "$class_file")
  # TODO currently RapidsShuffleManager is "removed" from /spark* by construction in
  # dist pom.xml via ant. We could delegate this logic to this script
  # and make both simmpler
  #
  # TODO ParquetCachedBatchSerializer is not bitwise-identical after 411, 
  # but it is compatible with previous versions because it merely adds a new method.
  # we might need to replace this strict check with MiMa
  # https://github.com/apache/spark/blob/7011706a0a8dbec6adb5b5b121921b29b314335f/sql/core/src/main/scala/org/apache/spark/sql/columnar/CachedBatchSerializer.scala#L75-L95
  # ProxyRapidsShuffleInternalManagerBase is not bitwise-identical when
  # DB 17.3 is included because ShuffleManager.getReader signature differs
  # (8-param with prismMapStatusEnabled vs 7-param). This is safe because
  # the class provides concrete implementations for ALL getReader variants,
  # so the JVM resolves the correct one at runtime regardless of which
  # ShuffleManager version the class was compiled against.
  if [[ ! "$class_file_quoted" =~ com/nvidia/spark/rapids/spark[34].*/.*ShuffleManager.class && \
          "$class_file_quoted" != "com/nvidia/spark/ParquetCachedBatchSerializer.class" && \
          ! "$class_file_quoted" =~ org/apache/spark/sql/rapids/ProxyRapidsShuffleInternalManagerBase ]]; then
      if ! grep -q "/spark.\+/$class_file_quoted" "$SPARK_SHARED_TXT"; then
        echo >&2 "$class_file is not bitwise-identical across shims"
        exit 255
      fi
  fi
}

echo "$((++STEP))/ verifying unshimmed classes have unique sha1 across shims"
while read -r unshimmed_class; do
  verify_same_sha_for_unshimmed "$unshimmed_class"
done < "$UNSHIMMED_LIST_TXT"

# Remove unshimmed classes from parallel worlds
# TODO rework with low priority, only a few classes.
echo "$((++STEP))/ removing duplicates of unshimmed classes"

while read -r unshimmed_class; do
  for pw in ./parallel-world/spark[34-]* ; do
    unshimmed_path="$pw/$unshimmed_class"
    [[ -f "$unshimmed_path" ]] && echo "$unshimmed_path" || true
  done >> "$DELETE_DUPLICATES_TXT"
done < "$UNSHIMMED_LIST_TXT"

echo "$((++STEP))/ deleting all class files listed in $DELETE_DUPLICATES_TXT"
< "$DELETE_DUPLICATES_TXT" sort -u | xargs rm

end_time=$(date +%s)
echo "binary-dedupe completed in $((end_time - start_time)) seconds"
