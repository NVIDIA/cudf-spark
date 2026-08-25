#!/bin/bash
#
# Copyright (c) 2023-2026, NVIDIA CORPORATION. All rights reserved.
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

## 'foo=abc,bar=123,...' to 'export foo=abc bar=123 ...'
if [ -n "$EXTRA_ENVS" ]; then
    export ${EXTRA_ENVS//','/' '}
fi

SPARK_VER=${SPARK_VER:-$(< /databricks/spark/VERSION)}

# Print DB runtime version details
if [[ -f /databricks/BUILDINFO ]]; then
    echo "DB runtime version details:"
    cat /databricks/BUILDINFO
else
    echo "No /databricks/BUILDINFO file found"
fi

# Extract Databricks version from deployed configs.
# spark.databricks.clusterUsageTags.sparkVersion is set automatically on Databricks
# notebooks but not when running Spark manually.
#
# At the OS level the DBR version can be obtailed via
# 1. DATABRICKS_RUNTIME_VERSION environment set by Databricks, e.g., 12.2
# 2. File at /databricks/DBR_VERSION created by Databricks, e.g., 12.2
# 3. The value for Spark conf in file /databricks/common/conf/deploy.conf created by Databricks,
#    e.g. 12.2.x-gpu-ml-scala2.12
#
# For cases 1 and 2 append '.' for version matching in 3XYdb SparkShimServiceProvider
#
DBR_VERSION=/databricks/DBR_VERSION
DB_DEPLOY_CONF=/databricks/common/conf/deploy.conf
if [[ -n "${DATABRICKS_RUNTIME_VERSION}" ]]; then
  export PYSP_TEST_spark_databricks_clusterUsageTags_sparkVersion="${DATABRICKS_RUNTIME_VERSION}."
elif [[ -f $DBR_VERSION || -f $DB_DEPLOY_CONF ]]; then
  DB_VER="$(< ${DBR_VERSION})." || \
    DB_VER=$(grep spark.databricks.clusterUsageTags.sparkVersion $DB_DEPLOY_CONF | sed -e 's/.*"\(.*\)".*/\1/')
  # if we did not error out on reads we should have at least four characters "x.y."
  if (( ${#DB_VER} < 4 )); then
      echo >&2 "Unable to determine Databricks version, unexpected length of: ${DB_VER}"
      exit 1
  fi
  export PYSP_TEST_spark_databricks_clusterUsageTags_sparkVersion=$DB_VER
else
  cat << EOF
This node does not define
- DATABRICKS_RUNTIME_VERSION environment,
- Files containing version information: $DBR_VERSION, $DB_DEPLOY_CONF

Proceeding assuming a non-Databricks environment.
EOF

fi

# TODO make this standard going forward
if [[ "$SPARK_VER" == '3.5.0' || "$SPARK_VER" == '4.0.0' ]]; then
    DB_VER_SUFFIX="${PYSP_TEST_spark_databricks_clusterUsageTags_sparkVersion//./}"
else
    DB_VER_SUFFIX=""
fi

export SPARK_SHIM_VER=${SPARK_SHIM_VER:-"spark${SPARK_VER//.}db${DB_VER_SUFFIX}"}

# On Databricks with Spark 4.x, the built-in SparkConnectPlugin starts a gRPC
# service on port 15002 which is already occupied by the cluster's own SparkConnect.
# The bind failure leaves non-daemon Netty threads that block JVM exit.
# Fix: use an ephemeral port so the service starts and stops cleanly.
if [[ "$SPARK_VER" == 4.* ]]; then
    export PYSP_TEST_spark_connect_grpc_binding_port=0
fi

# Setup SPARK_HOME if need
if [[ -z "$SPARK_HOME" ]]; then
    # Configure spark environment on Databricks
    export SPARK_HOME=$DB_HOME/spark
fi

# Set PYSPARK_PYTHON to keep the version of driver/workers python consistent.
export PYSPARK_PYTHON=${PYSPARK_PYTHON:-"$(which python)"}
# Get Python version (major.minor). i.e., python3.8 for DB10.4 and python3.9 for DB12.2
PYTHON_VERSION=$(${PYSPARK_PYTHON} -c 'import sys; print("python{}.{}".format(sys.version_info.major, sys.version_info.minor))')
# Set the path of python site-packages, packages were installed here by 'jenkins/databricks/setup.sh'.
PYTHON_SITE_PACKAGES=${PYTHON_SITE_PACKAGES:-"$HOME/.local/lib/${PYTHON_VERSION}/site-packages"}

# Get the correct py4j file.
PY4J_FILE=$(find $SPARK_HOME/python/lib -type f -iname "py4j*.zip")
# Databricks Koalas can conflict with the actual Pandas version, so put site packages first.
# Note that Koala is deprecated for DB10.4+ and it is recommended to use Pandas API on Spark instead.
export PYTHONPATH=$PYTHON_SITE_PACKAGES:$SPARK_HOME/python:$SPARK_HOME/python/pyspark/:$PY4J_FILE
export PCBS_CONF="com.nvidia.spark.ParquetCachedBatchSerializer"
if [[ "$TEST" == "cache_test" || "$TEST" == "cache_test.py" ]]; then
    export PYSP_TEST_spark_sql_cache_serializer="$PCBS_CONF"
fi

export TEST_TYPE=${TEST_TYPE:-"nightly"}

if [[ -n "$LOCAL_JAR_PATH" ]]; then
    export LOCAL_JAR_PATH=$LOCAL_JAR_PATH
fi

## 'spark.foo=1,spark.bar=2,...' to 'export PYSP_TEST_spark_foo=1 export PYSP_TEST_spark_bar=2'
if [ -n "$SPARK_CONF" ]; then
    CONF_LIST=${SPARK_CONF//','/' '}
    for CONF in ${CONF_LIST}; do
        KEY=${CONF%%=*}
        VALUE=${CONF#*=}
        ## run_pyspark_from_build.sh requires 'export PYSP_TEST_spark_foo=1' as the spark configs
        export PYSP_TEST_${KEY//'.'/'_'}=$VALUE
    done

    ## 'spark.foo=1,spark.bar=2,...' to '--conf spark.foo=1 --conf spark.bar=2 --conf ...'
    export SPARK_CONF="--conf ${SPARK_CONF/','/' --conf '}"
fi
