#!/bin/bash

# Copyright (c) 2021-2025, NVIDIA CORPORATION.
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


# This script generates the commits that went in Apache Spark for audit.
# Audit is required to evaluate if the code needs to be updated based
# on new commits merged in Apache Spark. This currently audits changes for
# Spark master branch by default. The script can be run for different branches by providing
# the argument -b along with the branch name.
# Arguments:
#   lastcommit - File which contains the latest commit hash when this script ran last.
#   basebranch - branch in Apache Spark for which commits needs to be audited.
#                Currently it's Apache Spark's master branch(Spark-3.3-SNAPSHOT).
#   tag        - tag until which the commits are audited 


set -ex
ABSOLUTE_PATH=$(cd $(dirname $0) && pwd)
lastcommit=""
basebranch="master"
tag="branch-3.2"
commonancestor=""
REF=${REF:-"main"}
REF=main
while getopts v:b:t:c: flag
do
  case "${flag}" in
      v) lastcommit=${OPTARG};;
      b) basebranch=${OPTARG};;
      t) tag=${OPTARG};;
      c) commonancestor=${OPTARG};;
      *) echo "Unknown option: $flag"; exit 1;;
  esac
done

SPARK_TREE="$WORKSPACE/spark"
if [ -e ${SPARK_TREE} ]; then
 rm -rf $SPARK_TREE 
fi
git clone https://github.com/apache/spark.git $SPARK_TREE

if [ -f "$lastcommit" ]; then
    cd ${SPARK_TREE}
    latestcommit=`cat ${lastcommit}`
    git checkout $basebranch
    git log --oneline HEAD...$latestcommit -- sql/core/src/main sql/catalyst/src/main \
        core/src/main/scala/org/apache/spark/shuffle core/src/main/scala/org/apache/spark/storage \
        sql/hive/src/main | tee ${COMMIT_DIFF_LOG}
    git log HEAD -n 1 --pretty="%h" > ${lastcommit}

    cd $WORKSPACE
    set +ex
    COMMIT_UPDATE=`git diff ${lastcommit}`
    set -ex
    if [ -n "$COMMIT_UPDATE" ]; then
        git config --global user.name blossom
        git config --global user.email blossom@nvidia.com
        git add ${lastcommit}
        git commit -m "Update latest commit-id for org.apache.spark branch ${basebranch}"
        git push origin HEAD:$REF
    else
        echo "No commit update"
    fi
else
    ## Below sequence of commands were used to get the initial list of commits to audit branch-3.3-SNAPSHOT(which is currently `master` branch)
    ## It filters out all the commits that were audited until branch-3.2
    ## There wasn't easy way to get the list of commits to audit for branch-3.3-SNAPSHOT.
    ## Spark release works in this way -  Once the release branch is cut, PR's are merged into master and then cherry-picked to release branches.
    ## This causes different commit ids for the same PR in different branches(master & release branch).
    ## We need to find the common parent before branch-3.2 was cut. In this case commit id 79a6e00b7621bb is the one.
    ## So we get all commits from master and branch-3.2 until the common parent commit and then filter it based on commit header message i.e
    ## if the commit header is same, it means it is cherry-picked to branch-3.2(implying that commit is already audited).
    echo "file $lastcommit not found"
    cd ${SPARK_TREE}

    # if common ancestor is not provided, then provide the default commit id.
    if [ -z "$commonancestor" ]; then
        commonancestor="79a6e00b7621bb"
    fi

    ## Get all the commits from TOT branch-3.2 to common ancestor
    git checkout $tag
    git log --oneline HEAD...$commonancestor -- sql/core/src/main sql/catalyst/src/main  > previousVersion.log

    ## Get all the commits from TOT master to common ancestor
    git checkout $basebranch
    git log --oneline HEAD...$commonancestor -- sql/core/src/main sql/catalyst/src/main  > currentVersion.log

    ## Below steps filter commit header messages, sorts and saves only uniq commits that needs to be audited in commits.to.audit.3.3 file
    awk '{$1=""; print}' previousVersion.log > previousVersion.filter.log
    awk '{$1=""; print}' currentVersion.log > currentVersion.filter.log
    sort previousVersion.filter.log currentVersion.filter.log | uniq -c | sort | awk '{$1=$1; print}' > uniqsort.log
    awk '$1=="1" {$1=""; print}' uniqsort.log > uniqcommits.log
    sort previousVersion.filter.log > previousVersion.filter.sorted.log
    sort currentVersion.filter.log > currentVersion.filter.sorted.log
    sort uniqcommits.log > uniqcommits.sorted.log

    comm -12 previousVersion.filter.sorted.log uniqcommits.sorted.log | wc -l
    comm -12 currentVersion.filter.sorted.log uniqcommits.sorted.log > commits.to.audit.currentVersion
    sed -i 's/\[/\\[/g' commits.to.audit.currentVersion
    sed -i 's/\]/\\]/g' commits.to.audit.currentVersion

    filename=commits.to.audit.currentVersion
    while read -r line; do
      echo "1"
      git log --grep="$line" --pretty="%h %s" >> ${COMMIT_DIFF_LOG}
    done < $filename
    git log HEAD -n 1 --pretty="%h"
fi
cd ${ABSOLUTE_PATH}/../ 
. scripts/prioritize-commits.sh
