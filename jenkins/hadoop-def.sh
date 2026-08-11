#!/bin/bash
#
# Copyright (c) 2023-2025, NVIDIA CORPORATION. All rights reserved.
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
#
# Argument(s):
#   1 SPARK_VER: spark version. e.g. 3.3.0
#

set -e

spark_version=${1:-"3.3.0"}
scala_version=${2:-"2.12"}
# Split spark version into base version (e.g. 3.3.0) and suffix (e.g. SNAPSHOT)
PRE_IFS=$IFS
IFS="-" read -r -a spark_version <<< "$1"
IFS=$PRE_IFS
# From spark 3.3.0, the pre-built binary tar starts to use new name pattern
if [[ `echo -e "${spark_version[0]}\n4.0.0" | sort -V | head -n 1` == "4.0.0" ]]; then
    BIN_HADOOP_VER="bin-hadoop3"
elif [[ `echo -e "${spark_version[0]}\n3.3.0" | sort -V | head -n 1` == "3.3.0" ]]; then
    if [ "$scala_version" == "2.12" ]; then
        BIN_HADOOP_VER="bin-hadoop3"
    else
        BIN_HADOOP_VER="bin-hadoop3-scala$scala_version"
    fi
# For spark version <3.3.0 and SNAPSHOT version, use previous name pattern
else
    BIN_HADOOP_VER="bin-hadoop3.2"
fi
