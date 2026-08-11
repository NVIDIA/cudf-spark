#!/bin/bash
#
# Copyright (c) 2022-2025, NVIDIA CORPORATION. All rights reserved.
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
# Setup Spark local environment for integration testing with Databricks cluster

set -xe

sudo apt-get update
sudo apt-get install -y zip

# Configure spark environment on Databricks
export SPARK_HOME=$DB_HOME/spark

SCRIPTPATH="$(cd -P "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Workaround to support local spark job
sudo ln -sf $DB_HOME/jars/ $SPARK_HOME/jars

# Set $SPARK_LOCAL_DIRS writable for ordinary user
if [ -f $SPARK_HOME/conf/spark-env.sh ]; then
    # Sample output: export SPARK_LOCAL_DIRS='/local_disk0'
    local_dir=`grep SPARK_LOCAL_DIRS $SPARK_HOME/conf/spark-env.sh`
    local_dir=${local_dir##*=}

    sudo chmod 777 `echo $local_dir | xargs`
fi

# Set PYSPARK_PYTHON to keep the version of driver/workers python consistent.
export PYSPARK_PYTHON=${PYSPARK_PYTHON:-"$(which python)"}
# Get Python version (major.minor). i.e., python3.8 for DB10.4 and python3.9 for DB12.2
PYTHON_VERSION=$(${PYSPARK_PYTHON} -c 'import sys; print("{}.{}".format(sys.version_info.major, sys.version_info.minor))')
[[ $(printf "%s\n" "3.9" "$PYTHON_VERSION" | sort -V | head -n1) = "3.9" ]] && IS_PY39_OR_LATER=1 || IS_PY39_OR_LATER=0
# Install if python pip does not exist.
if [ -z "$($PYSPARK_PYTHON -m pip --version || true)" ]; then
    if [ "$IS_PY39_OR_LATER" == 1 ]; then
        curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py
    else
        curl https://bootstrap.pypa.io/pip/$PYTHON_VERSION/get-pip.py -o get-pip.py
    fi
    $PYSPARK_PYTHON get-pip.py && rm get-pip.py
fi

# Set the path of python site-packages, and install packages here.
PYTHON_SITE_PACKAGES="$HOME/.local/lib/python${PYTHON_VERSION}/site-packages"
# Use "python -m pip install" to make sure pip matches with python.
$PYSPARK_PYTHON -m pip install --target $PYTHON_SITE_PACKAGES -r ${SCRIPTPATH}/../../integration_tests/requirements.txt
$PYSPARK_PYTHON -m pip install --target $PYTHON_SITE_PACKAGES requests pytest-order

