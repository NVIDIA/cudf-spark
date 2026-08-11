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

import pytest

from asserts import assert_gpu_and_cpu_are_equal_collect, assert_gpu_and_cpu_row_counts_equal
from data_gen import non_utc_allow, copy_and_update
from marks import *

columnarClass = 'com.nvidia.spark.rapids.tests.datasourcev2.parquet.ArrowColumnarDataSourceV2'

# Disable AQE temporarily until https://github.com/NVIDIA/spark-rapids/issues/14319 is resolved.
aqe_disabled = {"spark.sql.adaptive.enabled": "false"}

def readTable(types, classToUse):
    return lambda spark: spark.read\
        .option("arrowTypes", types)\
        .format(classToUse).load()\
        .orderBy("col1")

@allow_non_gpu('BatchScanExec')
@validate_execs_in_gpu_plan('HostColumnarToGpu')
def test_read_int():
    assert_gpu_and_cpu_are_equal_collect(readTable("int", columnarClass), conf=aqe_disabled)

@validate_execs_in_gpu_plan('HostColumnarToGpu')
@allow_non_gpu('BatchScanExec', *non_utc_allow)
def test_read_strings():
    assert_gpu_and_cpu_are_equal_collect(readTable("string", columnarClass), conf=aqe_disabled)

@allow_non_gpu('BatchScanExec')
@validate_execs_in_gpu_plan('HostColumnarToGpu')
def test_read_all_types():
    conf = copy_and_update(aqe_disabled, {'spark.rapids.sql.castFloatToString.enabled': 'true'})
    assert_gpu_and_cpu_are_equal_collect(
       readTable("int,bool,byte,short,long,string,float,double,date,timestamp", columnarClass),
            conf=conf)


@allow_non_gpu('BatchScanExec')
@validate_execs_in_gpu_plan('HostColumnarToGpu')
def test_read_all_types_count():
    conf = copy_and_update(aqe_disabled, {'spark.rapids.sql.castFloatToString.enabled': 'true'})
    assert_gpu_and_cpu_row_counts_equal(
       readTable("int,bool,byte,short,long,string,float,double,date,timestamp", columnarClass),
            conf=conf)


@allow_non_gpu('BatchScanExec')
@validate_execs_in_gpu_plan('HostColumnarToGpu')
def test_read_arrow_off():
    conf = copy_and_update(aqe_disabled, {'spark.rapids.arrowCopyOptimizationEnabled': 'false',
                                     'spark.rapids.sql.castFloatToString.enabled': 'true'})
    assert_gpu_and_cpu_are_equal_collect(
        readTable("int,bool,byte,short,long,string,float,double,date,timestamp", columnarClass),
            conf=conf)
