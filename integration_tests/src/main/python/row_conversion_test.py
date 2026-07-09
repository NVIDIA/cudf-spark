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

from asserts import assert_gpu_and_cpu_are_equal_collect
from data_gen import *
from marks import allow_non_gpu, approximate_float, incompat
from pyspark.sql.types import *
from spark_session import with_cpu_session


# This is one of the most basic tests where we verify that we can
# move data onto and off of the GPU without messing up. All data
# that comes from data_gen is row formatted, with how pyspark
# currently works and when we do a collect all of that data has
# to be brought back to the CPU (rows) to be returned.
# So we just need a very simple operation in the middle that
# can be done on the GPU.
def test_row_conversions():
    gens = [["a", byte_gen], ["b", short_gen], ["c", int_gen], ["d", long_gen],
            ["e", float_gen], ["f", double_gen], ["g", string_gen], ["h", boolean_gen],
            ["i", timestamp_gen], ["j", date_gen], ["k", ArrayGen(byte_gen)],
            ["l", ArrayGen(string_gen)], ["m", ArrayGen(float_gen)],
            ["n", ArrayGen(boolean_gen)], ["o", ArrayGen(ArrayGen(short_gen))],
            ["p", StructGen([["c0", byte_gen], ["c1", ArrayGen(byte_gen)]])],
            ["q", simple_string_to_string_map_gen],
            ["r", MapGen(BooleanGen(nullable=False), ArrayGen(boolean_gen), max_length=2)],
            ["s", null_gen], ["t", decimal_gen_64bit], ["u", decimal_gen_32bit],
            ["v", decimal_gen_128bit]]
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : gen_df(spark, gens).selectExpr("*", "a as a_again"))

def test_row_conversions_fixed_width():
    gens = [["a", byte_gen], ["b", short_gen], ["c", int_gen], ["d", long_gen],
            ["e", float_gen], ["f", double_gen], ["h", boolean_gen],
            ["i", timestamp_gen], ["j", date_gen], ["k", decimal_gen_64bit],
            ["l", decimal_gen_32bit]]
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : gen_df(spark, gens).selectExpr("*", "a as a_again"))

def test_row_conversions_fixed_width_wide():
    gens = [["a{}".format(i), ByteGen(nullable=True)] for i in range(10)] + \
           [["b{}".format(i), ShortGen(nullable=True)] for i in range(10)] + \
           [["c{}".format(i), IntegerGen(nullable=True)] for i in range(10)] + \
           [["d{}".format(i), LongGen(nullable=True)] for i in range(10)] + \
           [["e{}".format(i), FloatGen(nullable=True)] for i in range(10)] + \
           [["f{}".format(i), DoubleGen(nullable=True)] for i in range(10)] + \
           [["h{}".format(i), BooleanGen(nullable=True)] for i in range(10)] + \
           [["i{}".format(i), TimestampGen(nullable=True)] for i in range(10)] + \
           [["j{}".format(i), DateGen(nullable=True)] for i in range(10)] + \
           [["k{}".format(i), DecimalGen(precision=12, scale=2, nullable=True)] for i in range(10)] + \
           [["l{}".format(i), DecimalGen(precision=7, scale=3, nullable=True)] for i in range(10)]
    def do_it(spark):
        df=gen_df(spark, gens, length=1).selectExpr("*", "a0 as a_again")
        debug_df(df)
        return df
    assert_gpu_and_cpu_are_equal_collect(do_it)

# Wide fixed-width + STRING + DECIMAL128 round trip; toggles the accelerated path.
@pytest.mark.parametrize('use_fast_path', ['true', 'false'], ids=['fast', 'slow'])
def test_accelerated_c2r_wide_with_string_and_dec128(use_fast_path):
    gens = [["l{}".format(i), LongGen(nullable=True)]    for i in range(10)] + \
           [["d{}".format(i), DoubleGen(nullable=True)]  for i in range(5)]  + \
           [["i{}".format(i), IntegerGen(nullable=True)] for i in range(5)]  + \
           [["s{}".format(i), StringGen(nullable=True)]  for i in range(3)]  + \
           [["dec{}".format(i),
             DecimalGen(precision=30, scale=4, nullable=True)] for i in range(3)]
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: gen_df(spark, gens).selectExpr("*", "l0 as l_again"),
        conf={'spark.rapids.sql.acceleratedColumnarToRow.enabled': use_fast_path})


# DECIMAL precision boundaries around DECIMAL64 -> DECIMAL128 (= 18 -> 19).
@pytest.mark.parametrize('use_fast_path', ['true', 'false'], ids=['fast', 'slow'])
@pytest.mark.parametrize('precision,scale', [(18, 4), (19, 4), (30, 8), (38, 10)])
def test_accelerated_c2r_decimal128(use_fast_path, precision, scale):
    gens = [["dec{}".format(i),
             DecimalGen(precision=precision, scale=scale, nullable=True)] for i in range(8)] + \
           [["l{}".format(i), LongGen(nullable=True)] for i in range(4)]
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: gen_df(spark, gens).selectExpr("*"),
        conf={'spark.rapids.sql.acceleratedColumnarToRow.enabled': use_fast_path})


@pytest.mark.parametrize('use_fast_path', ['true', 'false'], ids=['fast', 'slow'])
def test_accelerated_c2r_strings(use_fast_path):
    gens = [["s{}".format(i), StringGen(nullable=True)] for i in range(6)] + \
           [["l{}".format(i), LongGen(nullable=True)]   for i in range(4)]
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: gen_df(spark, gens).selectExpr("*"),
        conf={'spark.rapids.sql.acceleratedColumnarToRow.enabled': use_fast_path})


# Multi-tile regression for jni #4590 (tile-boundary write race).
@pytest.mark.parametrize('use_fast_path', ['true', 'false'], ids=['fast', 'slow'])
def test_accelerated_c2r_wide_int32(use_fast_path):
    gens = [["c{}".format(i), IntegerGen(nullable=False)] for i in range(500)]
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: gen_df(spark, gens, length=256).selectExpr("*"),
        conf={'spark.rapids.sql.acceleratedColumnarToRow.enabled': use_fast_path})


# Hand-picked schemas that exercise alignment / tile-boundary edge cases random data_gen
# is unlikely to hit.
@pytest.mark.parametrize('schema', [
    pytest.param([['c0', ByteGen()]] + [['c{}'.format(i), IntegerGen()] for i in range(1, 100)],
                 id='byte_then_99_int'),
    pytest.param([['b{}'.format(i), ByteGen()] for i in range(50)] +
                 [['i{}'.format(i), IntegerGen()] for i in range(50)],
                 id='50_byte_50_int'),
    pytest.param([['s{}'.format(i), ShortGen()] for i in range(200)],
                 id='200_short'),
    pytest.param([['d', DecimalGen(precision=30, scale=4)]] +
                 [['i{}'.format(i), IntegerGen()] for i in range(99)],
                 id='dec128_then_99_int'),
    pytest.param([['i{}'.format(i), IntegerGen()] for i in range(383)],
                 id='383_int'),
])
def test_accelerated_c2r_dangerous_schemas(schema):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: gen_df(spark, schema, length=256).selectExpr("*"))


# Nested types should fall back through the slow path even with the fast-path conf on.
def test_accelerated_c2r_falls_back_for_nested():
    gens = [["s", StringGen(nullable=True)],
            ["a", ArrayGen(int_gen)],
            ["st", StructGen([["x", int_gen], ["y", string_gen]])],
            ["l", LongGen(nullable=True)],
            ["dec", DecimalGen(precision=30, scale=4, nullable=True)]]
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: gen_df(spark, gens).selectExpr("*"),
        conf={'spark.rapids.sql.acceleratedColumnarToRow.enabled': 'true'})


# Test handling of transitions when the data is already columnar on the host
# Note that Apache Spark will automatically convert a load of nested types to rows, so
# the nested types will not test a host columnar transition in that case.
# Databricks does support returning nested types as columnar data on the host, and that
# is where we would expect any problems with handling nested types in host columnar form to appear.
@pytest.mark.parametrize('data_gen', [
    int_gen,
    string_gen,
    decimal_gen_64bit,
    decimal_gen_128bit,
    ArrayGen(string_gen, max_length=10),
    ArrayGen(decimal_gen_128bit, max_length=10),
    StructGen([('a', string_gen)]) ] + map_string_string_gen, ids=idfn)
@allow_non_gpu('ColumnarToRowExec', 'FileSourceScanExec')
def test_host_columnar_transition(spark_tmp_path, data_gen):
    data_path = spark_tmp_path + '/PARQUET_DATA'
    with_cpu_session(lambda spark : unary_op_df(spark, data_gen).write.parquet(data_path))
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.parquet(data_path).filter("a IS NOT NULL"),
        conf={ 'spark.rapids.sql.exec.FileSourceScanExec' : 'false'})


# BinaryType is intentionally NOT in the gen list: CudfRowTransitions
# .isC2RSupportedType has no BinaryType branch, so including it would route
# the scan through the fallback (non-Cudf) ColumnarToRowIterator and
# contribute 0 LC to the target class.
def test_cudf_unsafe_row_primitive_sweep(spark_tmp_path):
    data_path = spark_tmp_path + "/cudf_unsafe_row"
    gen_list = [(f'c{i}', gen) for i, gen in enumerate(all_gen)]
    with_cpu_session(lambda spark:
        gen_df(spark, gen_list, length=200).write.mode("overwrite").parquet(data_path),
        conf={
            'spark.sql.parquet.datetimeRebaseModeInWrite': 'CORRECTED',
            'spark.sql.parquet.int96RebaseModeInWrite': 'CORRECTED',
        })
    # Pin acceleratedColumnarToRow so a future default flip or local conf
    # override cannot silently route the test through the slow CPU
    # ColumnarToRowIterator (which would still pass the equality check but
    # contribute 0 LC to CudfUnsafeRowBase).
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.parquet(data_path),
        conf={'spark.rapids.sql.acceleratedColumnarToRow.enabled': 'true'})
