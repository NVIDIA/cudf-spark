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
import pyspark.sql.functions as f
from pyspark.sql.types import IntegerType

from asserts import (
    assert_cpu_and_gpu_are_equal_collect_with_capture,
    assert_gpu_and_cpu_are_equal_collect,
    assert_gpu_and_cpu_row_counts_equal)
from data_gen import non_utc_allow, copy_and_update
from marks import *
from spark_session import is_spark_420_or_later

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


arrow_udf_conf = copy_and_update(aqe_disabled, {
    'spark.sql.execution.arrow.pyspark.enabled': 'true',
})


def _arrow_int_df(spark):
    return spark.read.option("arrowTypes", "int").format(columnarClass).load()


@allow_non_gpu('BatchScanExec')
def test_arrow_source_pandas_udf():
    pytest.importorskip('pandas')
    pytest.importorskip('pyarrow')

    def add_one(a):
        return a + 1

    my_udf = f.pandas_udf(add_one, returnType=IntegerType())

    def do_it(spark):
        return _arrow_int_df(spark).select(
            f.col('col1'), my_udf(f.col('col1')).alias('u')).orderBy('col1')

    # Spark 4.2 SPARK-56350 can skip ColumnarToRow for Arrow-backed CPU input.
    # That CPU path is what required the test Arrow source to keep batches alive.
    # The GPU path does not use Spark's Arrow pass-through; it still ingests
    # Arrow via HostColumnarToGpu and evaluates the UDF with GpuArrowEvalPythonExec.
    assert_cpu_and_gpu_are_equal_collect_with_capture(
        do_it,
        exist_classes='HostColumnarToGpu,GpuArrowEvalPythonExec',
        conf=arrow_udf_conf)


@allow_non_gpu('BatchScanExec', 'PythonUDF')
@pytest.mark.skipif(not is_spark_420_or_later(),
                    reason='Arrow-optimized regular Python UDFs use ArrowEvalPythonExec from Spark 4.2')
def test_arrow_source_regular_udf():
    pytest.importorskip('pandas')
    pytest.importorskip('pyarrow')

    def add_one(a):
        return a + 1

    my_udf = f.udf(add_one, returnType=IntegerType())

    def do_it(spark):
        return _arrow_int_df(spark).select(
            f.col('col1'), my_udf(f.col('col1')).alias('u')).orderBy('col1')

    # SPARK-58241 is a Spark CPU bug on 4.2.0: evalType=101 hangs when Arrow
    # columnar input is enabled. It is not a GPU bug. Disable that CPU path
    # so this test compares GpuArrowEvalPythonExec against a stable row-based
    # CPU ArrowEvalPythonExec baseline.
    conf = copy_and_update(arrow_udf_conf, {
        'spark.sql.execution.arrow.pythonUDF.columnarInput.enabled': 'false',
    })
    assert_cpu_and_gpu_are_equal_collect_with_capture(
        do_it,
        exist_classes='HostColumnarToGpu,GpuArrowEvalPythonExec',
        conf=conf)
