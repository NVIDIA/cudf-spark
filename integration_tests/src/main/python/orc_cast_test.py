# Copyright (c) 2020-2026, NVIDIA CORPORATION.
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

from asserts import assert_gpu_and_cpu_are_equal_collect, assert_gpu_and_cpu_error
from conftest import is_not_utc
from data_gen import *
from marks import (allow_non_gpu, datagen_overrides, tz_sensitive_test,
                   validate_execs_in_gpu_plan)
from pyspark.sql.types import *
from spark_session import with_cpu_session
from orc_test import reader_opt_confs


def create_orc(data_gen_list, data_path):
    # generate ORC dataframe, and dump it to local file 'data_path'
    with_cpu_session(
        lambda spark: gen_df(spark, data_gen_list).write.orc(data_path)
    )


@pytest.mark.parametrize('reader_confs', reader_opt_confs, ids=idfn)
@pytest.mark.parametrize('v1_enabled_list', ["", "orc"])
@pytest.mark.parametrize('to_type', ['boolean', 'tinyint', 'smallint', 'int', 'bigint'])
def test_casting_among_integer_types(spark_tmp_path, reader_confs, v1_enabled_list, to_type):
    # cast integral types to another integral types
    int_gens = [boolean_gen] + integral_gens
    gen_list = [('c' + str(i), gen) for i, gen in enumerate(int_gens)]
    data_path = spark_tmp_path + '/ORC_DATA'
    create_orc(gen_list, data_path)

    # generate schema string like "c0 to_type, c1 to_type, ..., c4 to_type"
    schema_str = " {}, ".join([x[0] for x in gen_list]) + " {}"
    schema_str = schema_str.format(*([to_type] * len(gen_list)))
    all_confs = copy_and_update(reader_confs,
                                {'spark.sql.sources.useV1SourceList': v1_enabled_list})
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.schema(schema_str).orc(data_path),
        conf=all_confs)

non_utc_allow_orc_scan=['ColumnarToRowExec', 'FileScan', 'FileSourceScanExec'] if is_not_utc() else []

@pytest.mark.parametrize('to_type', ['float', 'double', 'string', 'timestamp'])
@allow_non_gpu(*non_utc_allow_orc_scan)
def test_casting_from_integer(spark_tmp_path, to_type):
    orc_path = spark_tmp_path + '/orc_cast_integer'
    # The Python 'datetime' module only supports a max-year of 10000, so we set the Long type max
    # to '1e11'. If the long-value is out of this range, pytest will throw an exception.
    data_gen = [('boolean_col', boolean_gen), ('tinyint_col', byte_gen),
                ('smallint_col', ShortGen(min_val=BYTE_MAX + 1)),
                ('int_col', IntegerGen(min_val=SHORT_MAX + 1)),
                ('bigint_col', LongGen(min_val=INT_MAX + 1, max_val=int(1e11))),
                ('negint_col', IntegerGen(max_val=-1))]
    create_orc(data_gen, orc_path)

    schema_str = "boolean_col {}, tinyint_col {}, smallint_col {}, int_col {}, bigint_col {}, negint_col {}"
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.schema(
            schema_str.format(*([to_type] * len(data_gen)))).orc(orc_path)
    )

non_utc_allow_for_test_casting_from_overflow_long = ['FileSourceScanExec', 'ColumnarToRowExec', ] if is_not_utc() else []
@pytest.mark.parametrize('overflow_long_gen', [LongGen(min_val=int(1e16)),
                                               LongGen(max_val=int(-1e16))])
@pytest.mark.parametrize('to_type', ['timestamp'])
@allow_non_gpu(*non_utc_allow_for_test_casting_from_overflow_long)
def test_casting_from_overflow_long(spark_tmp_path, overflow_long_gen,to_type):
    # Timestamp(micro-seconds) is actually type of int64, when casting long(int64) to timestamp,
    # we need to multiply 1e6 (or 1e3), and it may cause overflow. This function aims to test
    # whether if 'ArithmeticException' is caught.
    orc_path = spark_tmp_path + '/orc_cast_overflow_long'
    create_orc([('long_column', overflow_long_gen)], orc_path)
    schema_str = "long_column {}".format(to_type)
    assert_gpu_and_cpu_error(
        df_fun=lambda spark: spark.read.schema(schema_str).orc(orc_path).collect(),
        conf={},
        error_message="ArithmeticException"
    )


# When casting float/double to double/float, we need to compare values of GPU with CPU
# in an approximate way.
@pytest.mark.approximate_float
@pytest.mark.parametrize('to_type', ['float', 'double', 'boolean', 'tinyint', 'smallint', 'int', 'bigint'])
def test_casting_from_float_and_double(spark_tmp_path, to_type):
    orc_path = spark_tmp_path + '/orc_casting_from_float_and_double'
    data_gen = [('float_column', float_gen), ('double_column', double_gen)]
    create_orc(data_gen, orc_path)
    schema_str = "float_column {}, double_column {}".format(to_type, to_type)
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.schema(schema_str).orc(orc_path)
    )

@pytest.mark.parametrize('data_gen', [DoubleGen(max_exp=32, special_cases=None),
                                      DoubleGen(max_exp=32, special_cases=[8.88e9, 9.99e10, 1.314e11])])
@allow_non_gpu(*non_utc_allow_orc_scan)
def test_casting_from_double_to_timestamp(spark_tmp_path, data_gen):
    # ORC will assume the original double value in seconds, we need to convert them to
    # timestamp(INT64 in micro-seconds).
    #
    # The 'datetime' module in python requires 0 <= year < 10000, and UTC timestamp is start from 1970/1/1.
    # That is, the minimum valid negative number is -1970 * 365 * 24 * 3600 = -62125920000 -> 6e10 -> 2^32.
    # So we set max_exp = 32 in DoubleGen.
    #
    # The maximum valid positive number is (10000 - 1970) * 365 * 24 * 3600 = 253234080000 -> 2e11 -> 2^37,
    # so we add some special cases from 2^33 - 2^37 (8e9 ~ 1e11).
    #
    # In DoubleGen, special_case=None will generate some NaN, INF corner cases.

    orc_path = spark_tmp_path + '/orc_casting_from_double_to_timestamp'
    with_cpu_session(
        lambda spark: unary_op_df(spark, data_gen).write.orc(orc_path)
    )
    # the name of unique column is 'a', cast it into timestamp type
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.schema("a timestamp").orc(orc_path)
    )


@tz_sensitive_test
@pytest.mark.skipif(not is_not_utc(), reason="non-UTC ORC timestamp regression test")
@allow_non_gpu(*non_utc_allow_orc_scan)
@validate_execs_in_gpu_plan('GpuFileSourceScanExec')
def test_non_utc_timestamp_regressions(spark_tmp_path):
    integer_path = spark_tmp_path + '/orc_integer_timestamp_regression'
    double_path = spark_tmp_path + '/orc_double_timestamp_regression'
    physical_path = spark_tmp_path + '/orc_physical_timestamp_regression'

    with_cpu_session(
        lambda spark: spark.createDataFrame([(514952012,)], "a long")
            .write.orc(integer_path)
    )
    with_cpu_session(
        lambda spark: spark.createDataFrame(
            [(0.0,),
             (float('nan'),),
             (float('inf'),),
             (float('-inf'),),
             (-8589934591.999999,),
             (-7953731124.723491,),
             (-0.0015,),
             (-0.0005,),
             (0.0005,),
             (0.0015,)],
            "a double")
            .write.orc(double_path)
    )
    with_cpu_session(
        lambda spark: spark.createDataFrame(
            [(-2957649381472612,), (-3649379812521628,)], "a long")
            .selectExpr("timestamp_micros(a) AS a")
            .write.orc(physical_path)
    )

    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.schema("a timestamp").orc(integer_path)
    )
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.schema("a timestamp").orc(double_path)
    )
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.orc(physical_path)
    )


@allow_non_gpu(*non_utc_allow_for_test_casting_from_overflow_long)
# Keep both signs beyond the Long microsecond range after any timezone correction.
@pytest.mark.parametrize("overflow_value", [9223372123255.0, -9223372123255.0])
def test_casting_from_overflow_double_to_timestamp(spark_tmp_path, overflow_value):
    orc_path = spark_tmp_path + '/orc_casting_from_overflow_double_to_timestamp'
    with_cpu_session(
        lambda spark: spark.createDataFrame(
            [(overflow_value,)], "a double")
            .write.orc(orc_path)
    )
    assert_gpu_and_cpu_error(
        df_fun=lambda spark: spark.read.schema("a timestamp").orc(orc_path).collect(),
        conf={},
        error_message="ArithmeticException"
    )


@pytest.mark.parametrize('data_gen', [DecimalGen(precision=9, scale=2),
                                      DecimalGen(precision=18, scale=4),
                                      DecimalGen(precision=38, scale=6)])
@pytest.mark.parametrize('read_type', ["DECIMAL(9,2)",
                                       "DECIMAL(18,4)",
                                       "DECIMAL(38,6)"])
@pytest.mark.parametrize('ansi_mode', ["false", "true"])
def test_casting_decimal_to_decimal(spark_tmp_path, data_gen, read_type, ansi_mode):
    """
    Tests that ORC files with decimal columns written with one set of
    precision and scale are readable with different precision/scale.
    """
    orc_path = spark_tmp_path + '/orc_casting_from_decimal_to_decimal'
    with_cpu_session(
        lambda spark: unary_op_df(spark, data_gen).write.orc(orc_path)
    )
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.schema("a " + read_type).orc(orc_path),
        conf={'spark.sql.ansi.enabled': ansi_mode}
    )
