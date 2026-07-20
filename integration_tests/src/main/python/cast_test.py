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

from asserts import *
from conftest import is_not_utc, is_supported_time_zone, is_dataproc_serverless_runtime
from data_gen import *
from spark_session import *
from marks import allow_non_gpu, approximate_float, datagen_overrides, disable_ansi_mode, tz_sensitive_test
from pyspark.sql.types import *
from spark_init_internal import spark_version
from datetime import date, datetime, timedelta
import math
import pytz

_decimal_gen_36_5 = DecimalGen(precision=36, scale=5)


def test_cast_empty_string_to_int_ansi_off():
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, StringGen(pattern="")).selectExpr(
                'CAST(a as BYTE)',
                'CAST(a as SHORT)',
                'CAST(a as INTEGER)',
                'CAST(a as LONG)'),
                conf=ansi_disabled_conf)


@pytest.mark.parametrize('to_type', ['BYTE', 'SHORT', 'INTEGER', 'LONG'])
def test_cast_empty_string_to_int_ansi_on(to_type):
    err_mess = "cannot be cast to "
    assert_gpu_and_cpu_error(
        lambda spark : unary_op_df(spark, StringGen(pattern="")).selectExpr(
            'CAST(a as {})'.format(to_type)).collect(),
        conf=ansi_enabled_conf,
        error_message=err_mess)


valid_bool_strings = ['true', 'false', 't', 'f', 'y', 'n', 'yes', 'no', '1', '0',
                      'TRUE', 'FALSE', 'True', 'False', ' true ', ' false ']
invalid_bool_strings = ['maybe', 'invalid', '2', '-1', 'on', 'off',
                        '1.0', '0.0', 'tr', 'fa', '', 'null']

def test_cast_string_to_boolean_ansi_off():
    data_rows = [(s,) for s in valid_bool_strings + invalid_bool_strings]
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.createDataFrame(data_rows, "str_col string")
                           .selectExpr("str_col", "CAST(str_col AS BOOLEAN) as bool_col"),
        conf=ansi_disabled_conf)

def test_cast_string_to_boolean_valid_ansi_on():
    data_rows = [(s,) for s in valid_bool_strings]
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.createDataFrame(data_rows, "str_col string")
                           .selectExpr("str_col", "CAST(str_col AS BOOLEAN) as bool_col"),
        conf=ansi_enabled_conf)


@pytest.mark.parametrize('invalid_value', invalid_bool_strings)
def test_cast_string_to_boolean_invalid_ansi_on(invalid_value):
    assert_gpu_and_cpu_error(
        lambda spark: spark.createDataFrame([(invalid_value,)], "str_col string")
                           .selectExpr("CAST(str_col AS BOOLEAN) as bool_col").collect(),
        conf=ansi_enabled_conf,
        error_message="SparkRuntimeException")

# These tests are not intended to be exhaustive. The scala test CastOpSuite should cover
# just about everything for non-nested values. This is intended to check that the
# recursive code in nested type checks, like arrays, is working properly. So we are going
# pick child types that are simple to cast. Upcasting integer values and casting them to strings
@pytest.mark.parametrize('data_gen,to_type', [
    (ArrayGen(byte_gen), ArrayType(IntegerType())),
    (ArrayGen(_decimal_gen_36_5), ArrayType(DecimalType(38, 5))),
    (ArrayGen(StringGen('[0-9]{1,5}')), ArrayType(IntegerType())),
    (ArrayGen(byte_gen), ArrayType(StringType())),
    (ArrayGen(byte_gen), ArrayType(DecimalType(6, 2))),
    (ArrayGen(ArrayGen(byte_gen)), ArrayType(ArrayType(IntegerType()))),
    (ArrayGen(ArrayGen(byte_gen)), ArrayType(ArrayType(StringType()))),
    (ArrayGen(ArrayGen(byte_gen)), ArrayType(ArrayType(DecimalType(6, 2)))),
    (StructGen([('a', byte_gen)]), StructType([StructField('a', IntegerType())])),
    (StructGen([('a', _decimal_gen_36_5)]), StructType([StructField('a', DecimalType(38, 5))])),
    (StructGen([('a', byte_gen), ('c', short_gen)]), StructType([StructField('b', IntegerType()), StructField('c', ShortType())])),
    (StructGen([('a', ArrayGen(byte_gen)), ('c', short_gen)]), StructType([StructField('a', ArrayType(IntegerType())), StructField('c', LongType())])),
    (ArrayGen(StructGen([('a', byte_gen), ('b', byte_gen)])), ArrayType(StringType())),
    (MapGen(ByteGen(nullable=False), byte_gen), MapType(StringType(), StringType())),
    (MapGen(ByteGen(nullable=False), _decimal_gen_36_5), MapType(StringType(), DecimalType(38, 5))),
    (MapGen(ShortGen(nullable=False), ArrayGen(byte_gen)), MapType(IntegerType(), ArrayType(ShortType()))),
    (MapGen(ShortGen(nullable=False), ArrayGen(StructGen([('a', byte_gen)]))), MapType(IntegerType(), ArrayType(StructType([StructField('b', ShortType())]))))
    ], ids=idfn)
def test_cast_nested(data_gen, to_type):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).select(f.col('a').cast(to_type)))

def test_cast_string_date_ansi_off():
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: unary_op_df(spark, StringGen(date_start_1_1_1)).selectExpr("cast(a as date)"),
        conf=ansi_disabled_conf)


invalid_values_string_to_date = ['200', ' 1970A', '1970 A', '1970T',  # not conform to "yyyy" after trim
                                 '1970 T', ' 1970-01T', '1970-01 A',  # not conform to "yyyy-[M]M" after trim
                                 # not conform to 'yyyy-[M]M-[d]d', "yyyy-[M]M-[d]d *" or "yyyy-[M]M-[d]d T*" after trim
                                 '1970-01-01A',
                                 '2022-02-29',  # nonexistent day
                                 '200-1-1',  # 200 not conform to 'YYYY'
                                 '2001-13-1',  # nonexistent month
                                 '2001-1-32',  # nonexistent day
                                 'not numbers',
                                 '666666666' # year has more than 7 digits.
                                 ]
valid_values_string_to_date = ['2001', ' 2001 ', '1970-01', ' 1970-1 ',
                               '1970-1-01', ' 1970-10-5 ', ' 2001-10-16 ',  # 'yyyy-[M]M-[d]d' after trim
                               '1970-01-01T', '1970-01-01T-no_impact',  # "yyyy-[M]M-[d]d T*" after trim
                               ' 1970-01-01 A', '1970-01-01 B '  # "yyyy-[M]M-[d]d *" after trim
                               ]
values_string_to_data = invalid_values_string_to_date + valid_values_string_to_date

def test_cast_string_date_ansi_off_special_strings():
    data_rows = [(v,) for v in values_string_to_data]
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.createDataFrame(data_rows, "a string").selectExpr("cast(a as date)"),
        conf=ansi_disabled_conf)

def test_cast_string_date_valid_values_ansi_on():
    data_rows = [(v,) for v in valid_values_string_to_date]
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.createDataFrame(data_rows, "a string").selectExpr("cast(a as date)"),
        conf = ansi_disabled_conf)

# test ANSI mode, invalid input
@pytest.mark.parametrize('invalid', invalid_values_string_to_date)
def test_cast_string_date_invalid_values_ansi_on(invalid):
    assert_gpu_and_cpu_error(
        lambda spark: spark.createDataFrame([(invalid,)], "a string").selectExpr("cast(a as date)").collect(),
        conf = ansi_enabled_conf,
        error_message="DateTimeException")

# test try_cast in Spark versions >= 320 and < 340
@pytest.mark.skipif(is_before_spark_320() or is_spark_340_or_later() or is_databricks113_or_later(), reason="try_cast only in Spark 3.2+")
@allow_non_gpu('ProjectExec', 'TryCast')
@pytest.mark.parametrize('invalid', invalid_values_string_to_date)
def test_try_cast_string_date_fallback(invalid):
    assert_gpu_fallback_collect(
        lambda spark: spark.createDataFrame([(invalid,)], "a string").selectExpr("try_cast(a as date)"),
        'TryCast',
        conf = ansi_enabled_conf)

# test try_cast in Spark versions >= 340
@pytest.mark.skipif(not (is_spark_340_or_later() or is_databricks113_or_later()), reason="Cast with EvalMode only in Spark 3.4+")
@allow_non_gpu('ProjectExec','Cast')
@pytest.mark.parametrize('invalid', invalid_values_string_to_date)
def test_try_cast_string_date_fallback_340(invalid):
    assert_gpu_fallback_collect(
        lambda spark: spark.createDataFrame([(invalid,)], "a string").selectExpr("try_cast(a as date)"),
        'Cast',
        conf = ansi_enabled_conf)

@pytest.mark.parametrize('data_gen', [StringGen(date_start_1_1_1),
                                      StringGen(date_start_1_1_1 + '[ T][0-3][0-9]:[0-6][0-9]:[0-6][0-9]'),
                                      StringGen(date_start_1_1_1 + '[ T][0-3][0-9]:[0-6][0-9]:[0-6][0-9]\.[0-9]{0,6}Z?'),
                                      # year < 2200, this is testing running on GPU when default TZ is DST
                                      # The above 3 gens have more possibilities to have year > 2200
                                      StringGen('[0-2][0-1][0-9]{2,2}-[0-9]{1,2}-[0-9]{1,2}[ T][0-9]{1,2}:[0-9]{1,2}:[0-9]{1,2}'),
                                      ],
                        ids=idfn)
@tz_sensitive_test
def test_cast_string_to_timestamp_valid_format_ansi_off(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).selectExpr("cast(a as timestamp)"),
            conf = ansi_disabled_conf)

@disable_ansi_mode  # In ANSI mode, there are restrictions to casting DECIMAL to other types.
                    # ANSI mode behaviour is tested in test_ansi_cast_decimal_to.
@approximate_float
@pytest.mark.parametrize('data_gen', [
    decimal_gen_32bit,
    pytest.param(decimal_gen_32bit_neg_scale, marks=
        pytest.mark.skipif(is_dataproc_serverless_runtime(),
                           reason="Dataproc Serverless does not support negative scale for Decimal cast")),
    DecimalGen(precision=7, scale=7),
    decimal_gen_64bit, decimal_gen_128bit, DecimalGen(precision=30, scale=2),
    DecimalGen(precision=36, scale=5), DecimalGen(precision=38, scale=0),
    DecimalGen(precision=38, scale=10), DecimalGen(precision=36, scale=-5),
    DecimalGen(precision=38, scale=-10)], ids=meta_idfn('from:'))
@pytest.mark.parametrize('to_type', [ByteType(), ShortType(), IntegerType(), LongType(), FloatType(), DoubleType(), StringType()], ids=meta_idfn('to:'))
def test_with_ansi_disabled_cast_decimal_to(data_gen, to_type):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).select(f.col('a').cast(to_type), f.col('a')),
            conf = {'spark.rapids.sql.castDecimalToFloat.enabled': True})

@approximate_float
@pytest.mark.parametrize('data_gen', [
    decimal_gen_32bit, decimal_gen_32bit_neg_scale, DecimalGen(precision=7, scale=7),
    decimal_gen_64bit, decimal_gen_128bit, DecimalGen(precision=30, scale=2),
    DecimalGen(precision=36, scale=5), DecimalGen(precision=38, scale=0),
    DecimalGen(precision=38, scale=10), DecimalGen(precision=36, scale=-5),
    DecimalGen(precision=38, scale=-10)], ids=meta_idfn('from:'))
@pytest.mark.parametrize('to_type', [FloatType(), DoubleType(), StringType()], ids=meta_idfn('to:'))
def test_ansi_cast_decimal_to(data_gen, to_type):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).select(f.col('a').cast(to_type), f.col('a')),
            conf = {'spark.rapids.sql.castDecimalToFloat.enabled': True,
                'spark.sql.ansi.enabled': True})


@disable_ansi_mode  # With ANSI enabled, casting from wider to narrower types will fail.
@datagen_overrides(seed=0, reason='https://github.com/NVIDIA/spark-rapids/issues/10050')
@pytest.mark.parametrize('data_gen', [
    DecimalGen(7, 1),
    DecimalGen(9, 9),
    DecimalGen(15, 2),
    DecimalGen(15, 15),
    DecimalGen(30, 3),
    DecimalGen(5, -3),
    DecimalGen(3, 0)], ids=meta_idfn('from:'))
@pytest.mark.parametrize('to_type', [
    DecimalType(9, 0),
    DecimalType(17, 2),
    DecimalType(35, 4),
    DecimalType(30, -4),
    DecimalType(38, -10),
    DecimalType(1, -1)], ids=meta_idfn('to:'))
def test_with_ansi_disabled_cast_decimal_to_decimal(data_gen, to_type):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).select(f.col('a').cast(to_type), f.col('a')))


@datagen_overrides(seed=0, reason='https://github.com/NVIDIA/spark-rapids/issues/10050')
@pytest.mark.parametrize('data_gen', [
    DecimalGen(3, 0)], ids=meta_idfn('from:'))
@pytest.mark.parametrize('to_type', [
    DecimalType(1, -1)], ids=meta_idfn('to:'))
def test_ansi_cast_failures_decimal_to_decimal(data_gen, to_type):
    assert_gpu_and_cpu_error(
        lambda spark : unary_op_df(spark, data_gen).select(f.col('a').cast(to_type), f.col('a')).collect(),
        conf=ansi_enabled_conf,
        error_message="cannot be represented as Decimal")


@pytest.mark.parametrize('data_gen', [byte_gen, short_gen, int_gen, long_gen], ids=idfn)
@pytest.mark.parametrize('to_type', [
    DecimalType(2, 0),
    DecimalType(3, 0),
    DecimalType(5, 0),
    DecimalType(7, 2),
    DecimalType(10, 0),
    DecimalType(10, 2),
    DecimalType(18, 0),
    DecimalType(18, 2)], ids=idfn)
def test_cast_integral_to_decimal_ansi_off(data_gen, to_type):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : unary_op_df(spark, data_gen).select(
            f.col('a').cast(to_type)),
        conf=ansi_disabled_conf)


@pytest.mark.parametrize('data_gen', [LongGen(min_val=100)], ids=idfn)
@pytest.mark.parametrize('to_type', [DecimalType(2, 0)], ids=idfn)
def test_cast_integral_to_decimal_ansi_on(data_gen, to_type):
    assert_gpu_and_cpu_error(
        lambda spark: unary_op_df(spark, data_gen).select(f.col('a').cast(to_type)).collect(),
        conf=ansi_enabled_conf,
        error_message="cannot be represented as Decimal")


def test_cast_byte_to_decimal_overflow():
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : unary_op_df(spark, byte_gen).select(
            f.col('a').cast(DecimalType(2, -1))))

def test_cast_short_to_decimal_overflow():
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : unary_op_df(spark, short_gen).select(
            f.col('a').cast(DecimalType(4, -1))))

def test_cast_int_to_decimal_overflow():
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : unary_op_df(spark, int_gen).select(
            f.col('a').cast(DecimalType(9, -1))))

def test_cast_long_to_decimal_overflow():
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : unary_op_df(spark, long_gen).select(
            f.col('a').cast(DecimalType(18, -1))))


_float_special_cases = [(float("inf"), 5.0), (float("-inf"), 5.0), (float("nan"), 5.0)]
@pytest.mark.parametrize('data_gen', [FloatGen(special_cases=_float_special_cases),
                                      DoubleGen(special_cases=_float_special_cases)],
                         ids=idfn)
@pytest.mark.parametrize('to_type', [
    DecimalType(7, 1),
    DecimalType(9, 9),
    DecimalType(15, 2),
    DecimalType(15, 15),
    DecimalType(30, 3),
    DecimalType(5, -3),
    DecimalType(3, 0)], ids=idfn)
def test_cast_floating_point_to_decimal_ansi_off(data_gen, to_type):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : unary_op_df(spark, data_gen).select(
            f.col('a'), f.col('a').cast(to_type)),
        conf=copy_and_update(
               ansi_disabled_conf,
               {'spark.rapids.sql.castFloatToDecimal.enabled': True}))


@pytest.mark.parametrize('d_gen', [
    SetValuesGen(FloatType(), [float('nan'), float('-inf'), float('inf')]),
    SetValuesGen(DoubleType(), [float('nan'), float('-inf'), float('inf')])])
def test_cast_floating_point_to_decimal_specials_ansi_on(d_gen):
    # Spark always returns a null for a NaN, an inf, or an -inf.
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: unary_op_df(spark, d_gen).select(
            f.col('a'), f.col('a').cast(DecimalType(7, 1))),
        conf=copy_and_update(
            ansi_enabled_conf,
            {'spark.rapids.sql.castFloatToDecimal.enabled': True}))


@pytest.mark.parametrize('data_gen', [FloatGen(special_cases=_float_special_cases)])
@pytest.mark.parametrize('to_type', [DecimalType(7, 1)])
def test_cast_floating_point_to_decimal_ansi_on(data_gen, to_type):
    assert_gpu_and_cpu_error(
        lambda spark : unary_op_df(spark, data_gen).select(
                         f.col('a'),
                         f.col('a').cast(to_type)).collect(),
        conf=copy_and_update(
            ansi_enabled_conf,
            {'spark.rapids.sql.castFloatToDecimal.enabled': True}),
        error_message="cannot be represented as Decimal")


# casting these types to string should be passed
basic_gens_for_cast_to_string = [ByteGen, ShortGen, IntegerGen, LongGen, StringGen, BooleanGen, DateGen, TimestampGen]
basic_array_struct_gens_for_cast_to_string = [f() for f in basic_gens_for_cast_to_string] + [null_gen] + decimal_gens

# We currently do not generate the exact string as Spark for some decimal values of zero
# https://github.com/NVIDIA/spark-rapids/issues/6339
basic_map_gens_for_cast_to_string = [
    MapGen(f(nullable=False), f()) for f in basic_gens_for_cast_to_string] + [
    MapGen(DecimalGen(nullable=False),
           DecimalGen(precision=7, scale=3)),
    MapGen(DecimalGen(precision=7, scale=7, nullable=False),
           DecimalGen(precision=12, scale=2))]

# GPU does not match CPU to casting these types to string, marked as xfail when testing
not_matched_gens_for_cast_to_string = [FloatGen, DoubleGen]
not_matched_struct_array_gens_for_cast_to_string = [f() for f in not_matched_gens_for_cast_to_string] + [decimal_gen_32bit_neg_scale]
not_matched_map_gens_for_cast_to_string = [MapGen(f(nullable = False), f()) for f in not_matched_gens_for_cast_to_string] + [MapGen(DecimalGen(precision=7, scale=-3, nullable=False), DecimalGen())]

single_level_array_gens_for_cast_to_string = [ArrayGen(sub_gen) for sub_gen in basic_array_struct_gens_for_cast_to_string]
nested_array_gens_for_cast_to_string = [
    ArrayGen(ArrayGen(short_gen, max_length=10), max_length=10),
    ArrayGen(ArrayGen(null_gen, max_length=10), max_length=10),
    ArrayGen(MapGen(ByteGen(nullable=False), DateGen()), max_length=10),
    ArrayGen(StructGen([['child0', byte_gen], ['child1', string_gen], ['child2', date_gen]]))
    ]

all_array_gens_for_cast_to_string = single_level_array_gens_for_cast_to_string + nested_array_gens_for_cast_to_string

def _assert_cast_to_string_equal (data_gen, conf):
    """
    helper function for casting to string of supported type
    """
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: unary_op_df(spark, data_gen).select(f.col('a').cast("STRING")),
        conf
    )


@pytest.mark.parametrize('data_gen', all_array_gens_for_cast_to_string, ids=idfn)
@pytest.mark.parametrize('legacy', [True, False])
@allow_non_gpu(*non_utc_allow)
def test_cast_array_to_string(data_gen, legacy):
    _assert_cast_to_string_equal(
        data_gen,
        {"spark.sql.legacy.castComplexTypesToString.enabled": legacy})

def test_cast_float_to_string():
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: unary_op_df(spark, FloatGen()).selectExpr("cast(cast(a as string) as float)"),
        conf = {"spark.rapids.sql.castStringToFloat.enabled": True,
                "spark.rapids.sql.castFloatToString.enabled": True})

def test_cast_double_to_string():
    conf = {"spark.rapids.sql.castFloatToString.enabled": True}
    cast_func = lambda spark: unary_op_df(spark, DoubleGen()).selectExpr("cast(a as string)").collect()
    from_cpu = with_cpu_session(cast_func, conf)
    from_gpu = with_gpu_session(cast_func, conf)
    cast_to_float_func = lambda row: row.a if row.a is None or row.a == 'NaN' else float(row.a)
    from_cpu_float = list(map(cast_to_float_func, from_cpu))
    from_gpu_float = list(map(cast_to_float_func, from_gpu))
    assert from_cpu_float == from_gpu_float

@pytest.mark.parametrize('data_gen', [ArrayGen(sub) for sub in not_matched_struct_array_gens_for_cast_to_string], ids=idfn)
@pytest.mark.parametrize('legacy', [True, False])
@pytest.mark.xfail(reason='casting this type to string is not exact match')
def test_cast_array_with_unmatched_element_to_string(data_gen, legacy):
    _assert_cast_to_string_equal(
        data_gen,
        {"spark.rapids.sql.castFloatToString.enabled"       : True,
         "spark.sql.legacy.castComplexTypesToString.enabled": legacy}
    )


@pytest.mark.parametrize('data_gen', basic_map_gens_for_cast_to_string, ids=idfn)
@pytest.mark.parametrize('legacy', [True, False])
@allow_non_gpu(*non_utc_allow)
def test_cast_map_to_string(data_gen, legacy):
    _assert_cast_to_string_equal(
        data_gen,
        {"spark.sql.legacy.castComplexTypesToString.enabled": legacy})


@pytest.mark.parametrize('data_gen', not_matched_map_gens_for_cast_to_string, ids=idfn)
@pytest.mark.parametrize('legacy', [True, False])
@pytest.mark.xfail(reason='casting this type to string is not exact match')
def test_cast_map_with_unmatched_element_to_string(data_gen, legacy):
    _assert_cast_to_string_equal(
        data_gen,
        {"spark.rapids.sql.castFloatToString.enabled"       : True,
         "spark.sql.legacy.castComplexTypesToString.enabled": legacy}
    )


@pytest.mark.parametrize('data_gen', [StructGen([[str(i), gen] for i, gen in enumerate(basic_array_struct_gens_for_cast_to_string)] + [["map", MapGen(ByteGen(nullable=False), null_gen)]])], ids=idfn)
@pytest.mark.parametrize('legacy', [True, False])
@allow_non_gpu(*non_utc_allow)
def test_cast_struct_to_string(data_gen, legacy):
    _assert_cast_to_string_equal(
        data_gen,
        {"spark.sql.legacy.castComplexTypesToString.enabled": legacy}
    )

# https://github.com/NVIDIA/spark-rapids/issues/2309
@pytest.mark.parametrize('cast_conf', ['LEGACY', 'SPARK311+'])
def test_one_nested_null_field_legacy_cast(cast_conf):
    def was_broken_for_nested_null(spark):
        data = [
            (('foo',),),
            ((None,),),
            (None,)
        ]
        df = spark.createDataFrame(data)
        return df.select(df._1.cast(StringType()))

    assert_gpu_and_cpu_are_equal_collect(
        was_broken_for_nested_null,
        {"spark.sql.legacy.castComplexTypesToString.enabled": True if cast_conf == 'LEGACY' else False}
    )

# https://github.com/NVIDIA/spark-rapids/issues/2315
@pytest.mark.parametrize('cast_conf', ['LEGACY', 'SPARK311+'])
def test_two_col_struct_legacy_cast(cast_conf):
    def broken_df(spark):
        key_data_gen = StructGen([
            ('a', IntegerGen(min_val=0, max_val=4)),
            ('b', IntegerGen(min_val=5, max_val=9)),
        ], nullable=False)
        val_data_gen = IntegerGen()
        df = two_col_df(spark, key_data_gen, val_data_gen)
        return df.select(df.a.cast(StringType())).filter(df.b > 1)

    assert_gpu_and_cpu_are_equal_collect(
        broken_df,
        {"spark.sql.legacy.castComplexTypesToString.enabled": True if cast_conf == 'LEGACY' else False}
    )

@pytest.mark.parametrize('data_gen', [StructGen([["first", element_gen]]) for element_gen in not_matched_struct_array_gens_for_cast_to_string], ids=idfn)
@pytest.mark.parametrize('legacy', [True, False])
@pytest.mark.xfail(reason='casting this type to string is not an exact match')
def test_cast_struct_with_unmatched_element_to_string(data_gen, legacy):
    _assert_cast_to_string_equal(
        data_gen,
        {"spark.rapids.sql.castFloatToString.enabled"       : True,
         "spark.sql.legacy.castComplexTypesToString.enabled": legacy}
    )


# The bug SPARK-37451 only affects the following versions
def is_neg_dec_scale_bug_version():
    return "3.2.0" <= spark_version() < "3.2.1"

@pytest.mark.skipif(is_neg_dec_scale_bug_version(), reason="RAPIDS doesn't support casting string to decimal for negative scale decimal in this version of Spark because of SPARK-37451")
def test_cast_string_to_negative_scale_decimal():
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: unary_op_df(spark, StringGen("[0-9]{9}")).select(
            f.col('a').cast(DecimalType(8, -3))))


@pytest.mark.skipif(is_neg_dec_scale_bug_version(),
                    reason="Negative scale isn't supported in this Spark version due to SPARK-37451")
@pytest.mark.parametrize('ansi_on', [True, False], ids=idfn)
def test_cast_string_to_negative_scale_decimal_overflow(ansi_on):
    def test_overflow(spark):
        return unary_op_df(spark, StringGen("[0-9]{9}")).select(
            f.col('a').cast(DecimalType(4, -3)))

    if ansi_on:
        assert_gpu_and_cpu_error(lambda spark: test_overflow(spark).collect(),
                                 conf=ansi_enabled_conf,
                                 error_message="cannot be represented as Decimal")
    else:
        assert_gpu_and_cpu_are_equal_collect(test_overflow,
                                             conf=ansi_disabled_conf)


@pytest.mark.parametrize('type', [DoubleType(), FloatType()], ids=idfn)
@pytest.mark.parametrize('invalid_value', [float("inf"), float("-inf"), float("nan")])
@allow_non_gpu(*non_utc_allow)
def test_cast_float_to_timestamp_ansi_for_nan_inf(type, invalid_value):
    def fun(spark):
        data = [invalid_value]
        df = spark.createDataFrame(data, type)
        return df.select(f.col('value').cast(TimestampType())).collect()

    assert_gpu_and_cpu_error(fun, {"spark.sql.ansi.enabled": True},
                             error_message="SparkDateTimeException"
                             if is_before_spark_400() else "DateTimeException")

# if float.floor > Long.max or float.ceil < Long.min, throw exception
@pytest.mark.parametrize('type', [DoubleType(), FloatType()], ids=idfn)
@pytest.mark.parametrize('invalid_value', [float(LONG_MAX) + 100, float(LONG_MIN) - 100])
@allow_non_gpu(*non_utc_allow)
def test_cast_float_to_timestamp_ansi_overflow(type, invalid_value):
    def fun(spark):
        data = [invalid_value]
        df = spark.createDataFrame(data, type)
        return df.select(f.col('value').cast(TimestampType())).collect()
    assert_gpu_and_cpu_error(fun, {"spark.sql.ansi.enabled": True}, "ArithmeticException")

@allow_non_gpu(*non_utc_allow)
def test_cast_float_to_timestamp_side_effect():
    def getDf(spark):
        data = [(True, float(LONG_MAX) + 100), (False, float(1))]
        distData = spark.sparkContext.parallelize(data, 1)
        return spark.createDataFrame(distData, "c_b boolean, c_f float")
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: getDf(spark).selectExpr("if(c_b, cast(0 as timestamp), cast(c_f as timestamp))"),
        conf=ansi_enabled_conf)

# non ansi mode, will get null
@pytest.mark.parametrize('type', [DoubleType(), FloatType()], ids=idfn)
@allow_non_gpu(*non_utc_allow)
def test_with_ansi_off_cast_float_to_timestamp_for_nan_inf(type):
    """
    Tests the behaviour of floats when cast to timestamp, with ANSI disabled.
    ANSI mode tests are covered in test_cast_float_to_timestamp_ansi_for_nan_inf.
    """
    def fun(spark):
        data = [(float("inf"),), (float("-inf"),), (float("nan"),)]
        schema = StructType([StructField("value", type, True)])
        df = spark.createDataFrame(data, schema)
        return df.select(f.col('value').cast(TimestampType()))
    assert_gpu_and_cpu_are_equal_collect(fun, conf=ansi_disabled_conf)

# gen for casting long to timestamp, range is about in [0000, 9999]
long_gen_to_timestamp = LongGen(max_val=math.floor((9999-1970) * 365 * 86400),
                                min_val=-math.floor(1970 * 365 * 86400))

# the overflow case of `cast(long to timestamp)` is move to `TimestampSuite`
@pytest.mark.parametrize('ansi_enabled', [True, False], ids=['ANSI_ON', 'ANSI_OFF'])
@pytest.mark.parametrize('gen', [
    byte_gen,
    short_gen,
    int_gen,
    long_gen_to_timestamp], ids=idfn)
def test_cast_integral_to_timestamp(gen, ansi_enabled):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: unary_op_df(spark, gen).selectExpr("cast(a as timestamp)"),
        conf={"spark.sql.ansi.enabled": ansi_enabled})

@pytest.mark.parametrize('ansi_enabled', [True, False], ids=['ANSI_ON', 'ANSI_OFF'])
@allow_non_gpu(*non_utc_allow)
def test_cast_float_to_timestamp(ansi_enabled):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: unary_op_df(spark, long_gen_to_timestamp)
            .selectExpr("cast(cast(a as float) as timestamp)"),
        conf={"spark.sql.ansi.enabled": ansi_enabled})

@pytest.mark.parametrize('ansi_enabled', [True, False], ids=['ANSI_ON', 'ANSI_OFF'])
@allow_non_gpu(*non_utc_allow)
def test_cast_double_to_timestamp(ansi_enabled):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: unary_op_df(spark, long_gen_to_timestamp)
            .selectExpr("cast(cast(a as double) as timestamp)"),
        conf={"spark.sql.ansi.enabled": ansi_enabled})

@pytest.mark.parametrize('invalid_and_type', [
    (BYTE_MAX + 1, ByteType()),
    (BYTE_MIN  - 1, ByteType()),
    (SHORT_MAX + 1, ShortType()),
    (SHORT_MIN - 1, ShortType()),
    (INT_MAX + 1, IntegerType()),
    (INT_MIN - 1, IntegerType()),
], ids=idfn)
def test_cast_timestamp_to_integral_ansi_overflow(invalid_and_type):
    (invalid, to_type) = invalid_and_type
    assert_gpu_and_cpu_error(
        # pass seconds to `datetime.fromtimestamp`
        lambda spark: spark.createDataFrame([datetime.fromtimestamp(invalid)], TimestampType())
            .select(f.col("value").cast(to_type)).collect(),
        conf=ansi_enabled_conf,
        error_message="overflow")

def test_cast_timestamp_to_numeric_ansi_no_overflow():
    data = [datetime.fromtimestamp(i) for i in range(BYTE_MIN, BYTE_MAX + 1)]
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.createDataFrame(data, TimestampType())
            .selectExpr("cast(value as byte)", "cast(value as short)", "cast(value as int)", "cast(value as long)",
                        "cast(value as float)", "cast(value as double)"),
        conf=ansi_enabled_conf)


def test_cast_timestamp_to_numeric_non_ansi():
    """
    Test timestamp->numeric conversions with ANSI off.
    """
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: unary_op_df(spark, timestamp_gen)
            .selectExpr("cast(a as byte)", "cast(a as short)", "cast(a as int)", "cast(a as long)",
                        "cast(a as float)", "cast(a as double)"),
        conf=ansi_disabled_conf)

@allow_non_gpu(*non_utc_allow)
def test_cast_timestamp_to_string():
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: unary_op_df(spark, timestamp_gen)
            .selectExpr("cast(a as string)"))

@tz_sensitive_test
@allow_non_gpu(*non_supported_tz_allow)
def test_cast_timestamp_to_date():
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: unary_op_df(spark, timestamp_gen)
            .selectExpr("cast(a as date)"))

def test_cast_day_time_interval_to_string():
    _assert_cast_to_string_equal(DayTimeIntervalGen(start_field='day', end_field='day', special_cases=[MIN_DAY_TIME_INTERVAL, MAX_DAY_TIME_INTERVAL, timedelta(seconds=0)]), {})
    _assert_cast_to_string_equal(DayTimeIntervalGen(start_field='day', end_field='hour', special_cases=[MIN_DAY_TIME_INTERVAL, MAX_DAY_TIME_INTERVAL, timedelta(seconds=0)]), {})
    _assert_cast_to_string_equal(DayTimeIntervalGen(start_field='day', end_field='minute', special_cases=[MIN_DAY_TIME_INTERVAL, MAX_DAY_TIME_INTERVAL, timedelta(seconds=0)]), {})
    _assert_cast_to_string_equal(DayTimeIntervalGen(start_field='day', end_field='second', special_cases=[MIN_DAY_TIME_INTERVAL, MAX_DAY_TIME_INTERVAL, timedelta(seconds=0)]), {})
    _assert_cast_to_string_equal(DayTimeIntervalGen(start_field='hour', end_field='hour', special_cases=[MIN_DAY_TIME_INTERVAL, MAX_DAY_TIME_INTERVAL, timedelta(seconds=0)]), {})
    _assert_cast_to_string_equal(DayTimeIntervalGen(start_field='hour', end_field='minute', special_cases=[MIN_DAY_TIME_INTERVAL, MAX_DAY_TIME_INTERVAL, timedelta(seconds=0)]), {})
    _assert_cast_to_string_equal(DayTimeIntervalGen(start_field='hour', end_field='second', special_cases=[MIN_DAY_TIME_INTERVAL, MAX_DAY_TIME_INTERVAL, timedelta(seconds=0)]), {})
    _assert_cast_to_string_equal(DayTimeIntervalGen(start_field='minute', end_field='minute', special_cases=[MIN_DAY_TIME_INTERVAL, MAX_DAY_TIME_INTERVAL, timedelta(seconds=0)]), {})
    _assert_cast_to_string_equal(DayTimeIntervalGen(start_field='minute', end_field='second', special_cases=[MIN_DAY_TIME_INTERVAL, MAX_DAY_TIME_INTERVAL, timedelta(seconds=0)]), {})
    _assert_cast_to_string_equal(DayTimeIntervalGen(start_field='second', end_field='second', special_cases=[MIN_DAY_TIME_INTERVAL, MAX_DAY_TIME_INTERVAL, timedelta(seconds=0)]), {})

def test_cast_string_to_day_time_interval():
    gen = DayTimeIntervalGen(start_field='day', end_field='second', special_cases=[MIN_DAY_TIME_INTERVAL, MAX_DAY_TIME_INTERVAL, timedelta(seconds=0)])
    dtType = DayTimeIntervalType(0, 3) # 0 is day; 3 is second
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: unary_op_df(spark, gen).select(f.col('a').cast(StringType()).cast(dtType)))

    gen = DayTimeIntervalGen(start_field='hour', end_field='second', special_cases=[MIN_DAY_TIME_INTERVAL, MAX_DAY_TIME_INTERVAL, timedelta(seconds=0)])
    dtType = DayTimeIntervalType(1, 3) # 1 is hour; 3 is second
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: unary_op_df(spark, gen).select(f.col('a').cast(StringType()).cast(dtType)))

@pytest.mark.parametrize('invalid_string', [
    "INTERVAL 'xxx' DAY TO SECOND", # invalid format
    "-999999999 04:00:54.775808000" # exceeds min value, min value is "-106751991 04:00:54.775808000"
])
def test_cast_string_to_day_time_interval_exception(invalid_string):
    dtType = DayTimeIntervalType(0, 3)
    def fun(spark):
        data=[invalid_string]
        df = spark.createDataFrame(data, StringType())
        return df.select(f.col('value').cast(dtType)).collect()
    assert_gpu_and_cpu_error(fun, {}, "IllegalArgumentException")

def test_cast_day_time_interval_to_integral_no_overflow():
    second_dt_gen = DayTimeIntervalGen(start_field='second', end_field='second', min_value=timedelta(seconds=-128), max_value=timedelta(seconds=127), nullable=False)
    gen = StructGen([('a', DayTimeIntervalGen(start_field='day', end_field='day', min_value=timedelta(seconds=-128 * 86400), max_value=timedelta(seconds=127 * 86400))),
                     ('b', DayTimeIntervalGen(start_field='hour', end_field='hour', min_value=timedelta(seconds=-128 * 3600), max_value=timedelta(seconds=127 * 3600))),
                     ('c', DayTimeIntervalGen(start_field='minute', end_field='minute', min_value=timedelta(seconds=-128 * 60), max_value=timedelta(seconds=127 * 60))),
                     ('d', second_dt_gen),
                     ('c_array', ArrayGen(second_dt_gen)),
                     ('c_struct', StructGen([("a", second_dt_gen), ("b", second_dt_gen)])),
                     ('c_map', MapGen(second_dt_gen, second_dt_gen))
                     ], nullable=False)
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: gen_df(spark, gen).select(f.col('a').cast(ByteType()), f.col('a').cast(ShortType()), f.col('a').cast(IntegerType()), f.col('a').cast(LongType()),
                                               f.col('b').cast(ByteType()), f.col('b').cast(ShortType()), f.col('b').cast(IntegerType()), f.col('b').cast(LongType()),
                                               f.col('c').cast(ByteType()), f.col('c').cast(ShortType()), f.col('c').cast(IntegerType()), f.col('c').cast(LongType()),
                                               f.col('d').cast(ByteType()), f.col('d').cast(ShortType()), f.col('d').cast(IntegerType()), f.col('d').cast(LongType()),
                                               f.col('c_array').cast(ArrayType(ByteType())),
                                               f.col('c_struct').cast(StructType([StructField('a', ShortType()), StructField('b', ShortType())])),
                                               f.col('c_map').cast(MapType(IntegerType(), IntegerType()))
                                                ))

integral_gens_no_overflow = [
    LongGen(min_val=math.ceil(LONG_MIN / 86400 / 1000000), max_val=math.floor(LONG_MAX / 86400 / 1000000), special_cases=[0, 1, -1]),
    IntegerGen(min_val=math.ceil(INT_MIN / 86400 / 1000000), max_val=math.floor(INT_MAX / 86400 / 1000000), special_cases=[0, 1, -1]),
    ShortGen(),
    ByteGen(),
    # StructGen([("a", ShortGen()), ("b", ByteGen())])
]
def test_cast_integral_to_day_time_interval_no_overflow():
    long_gen = IntegerGen(min_val=math.ceil(INT_MIN / 86400 / 1000000), max_val=math.floor(INT_MAX / 86400 / 1000000), special_cases=[0, 1, -1])
    int_gen = LongGen(min_val=math.ceil(LONG_MIN / 86400 / 1000000), max_val=math.floor(LONG_MAX / 86400 / 1000000), special_cases=[0, 1, -1], nullable=False)
    gen = StructGen([("a", long_gen),
                     ("b", int_gen),
                     ("c", ShortGen()),
                     ("d", ByteGen()),
                     ("c_struct", StructGen([("a", long_gen), ("b", int_gen)], nullable=False)),
                     ('c_array', ArrayGen(int_gen)),
                     ('c_map', MapGen(int_gen, long_gen))], nullable=False)
    # day_time_field: 0 is day, 1 is hour, 2 is minute, 3 is second
    day_type = DayTimeIntervalType(0, 0)
    hour_type = DayTimeIntervalType(1, 1)
    minute_type = DayTimeIntervalType(2, 2)
    second_type = DayTimeIntervalType(3, 3)

    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: gen_df(spark, gen).select(
            f.col('a').cast(day_type), f.col('a').cast(hour_type), f.col('a').cast(minute_type), f.col('a').cast(second_type),
            f.col('b').cast(day_type), f.col('b').cast(hour_type), f.col('b').cast(minute_type), f.col('b').cast(second_type),
            f.col('c').cast(day_type), f.col('c').cast(hour_type), f.col('c').cast(minute_type), f.col('c').cast(second_type),
            f.col('d').cast(day_type), f.col('d').cast(hour_type), f.col('d').cast(minute_type), f.col('d').cast(second_type),
            f.col('c_struct').cast(StructType([StructField('a', day_type), StructField('b', hour_type)])),
            f.col('c_array').cast(ArrayType(hour_type)),
            f.col('c_map').cast(MapType(minute_type, second_type)),
        ))

cast_day_time_to_inregral_overflow_pairs = [
    (INT_MIN - 1, IntegerType()),
    (INT_MAX + 1, IntegerType()),
    (SHORT_MIN - 1, ShortType()),
    (SHORT_MAX + 1, ShortType()),
    (BYTE_MIN - 1, ByteType()),
    (BYTE_MAX + 1, ByteType())
]
@pytest.mark.parametrize('large_second, integral_type', cast_day_time_to_inregral_overflow_pairs)
def test_cast_day_time_interval_to_integral_overflow(large_second, integral_type):
    def getDf(spark):
        return spark.createDataFrame([timedelta(seconds=large_second)], DayTimeIntervalType(DayTimeIntervalType.SECOND, DayTimeIntervalType.SECOND))
    assert_gpu_and_cpu_error(
        lambda spark: getDf(spark).select(f.col('value').cast(integral_type)).collect(),
        conf={},
        error_message="overflow")

day_time_interval_max_day = math.floor(LONG_MAX / (86400 * 1000000))
large_days_overflow_pairs = [
    (-day_time_interval_max_day - 1, LongType()),
    (+day_time_interval_max_day + 1, LongType()),
    (-day_time_interval_max_day - 1, IntegerType()),
    (+day_time_interval_max_day + 1, IntegerType())
]
@pytest.mark.parametrize('large_day,integral_type', large_days_overflow_pairs)
def test_cast_integral_to_day_time_interval_overflow(large_day, integral_type):
    def getDf(spark):
        return spark.createDataFrame([large_day], integral_type)
    assert_gpu_and_cpu_error(
        lambda spark: getDf(spark).select(f.col('value').cast(DayTimeIntervalType(DayTimeIntervalType.DAY, DayTimeIntervalType.DAY))).collect(),
        conf={},
        error_message="overflow")

def test_cast_integral_to_day_time_side_effect():
    # Disable AQE temporarily until https://github.com/NVIDIA/spark-rapids/issues/14319 is resolved.
    conf = {'spark.sql.adaptive.enabled': 'false'}
    def getDf(spark):
        # INT_MAX > 106751991 (max value of interval day)
        return spark.createDataFrame([(True, INT_MAX, LONG_MAX), (False, 0, 0)], "c_b boolean, c_i int, c_l long").repartition(1)
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: getDf(spark).selectExpr("if(c_b, interval 0 day, cast(c_i as interval day))", "if(c_b, interval 0 second, cast(c_l as interval second))"),
        conf=conf)

def test_cast_day_time_to_integral_side_effect():
    # Disable AQE temporarily until https://github.com/NVIDIA/spark-rapids/issues/14319 is resolved.
    conf = {'spark.sql.adaptive.enabled': 'false'}
    def getDf(spark):
        # 106751991 > Byte.MaxValue
        return spark.createDataFrame([(True, MAX_DAY_TIME_INTERVAL), (False, (timedelta(microseconds=0)))], "c_b boolean, c_dt interval day to second").repartition(1)
    assert_gpu_and_cpu_are_equal_collect(lambda spark: getDf(spark).selectExpr("if(c_b, 0, cast(c_dt as byte))", "if(c_b, 0, cast(c_dt as short))", "if(c_b, 0, cast(c_dt as int))"),
                                         conf=conf)

def test_cast_binary_to_string():
    assert_gpu_and_cpu_are_equal_collect(lambda spark: unary_op_df(spark, binary_gen).selectExpr("a", "CAST(a AS STRING) as str"))

def test_cast_int_to_string_not_UTC():
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: unary_op_df(spark, int_gen, 100).selectExpr("a", "CAST(a AS STRING) as str"),
        {"spark.sql.session.timeZone": "+08"})

not_utc_fallback_test_params = [(timestamp_gen, 'STRING')]

@allow_non_gpu('Cast')
@pytest.mark.parametrize('from_gen, to_type', not_utc_fallback_test_params, ids=idfn)
def test_cast_fallback_not_UTC(from_gen, to_type):
    assert_gpu_fallback_collect(
        lambda spark: unary_op_df(spark, from_gen).selectExpr("CAST(a AS {}) as casted".format(to_type)),
        "Cast",
        {"spark.sql.session.timeZone": "+08"})

def test_cast_string_to_timestamp_in_non_utc():
    gen = SetValuesGen(StringType(), ['2023-03-20 10:38:50', '2023-03-20 10:39:02'])
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: unary_op_df(spark, gen).selectExpr("CAST(a AS TIMESTAMP) AS casted"),
        {"spark.sql.session.timeZone": "+08"})

def test_cast_date_integral_and_fp_ansi_off():
    """
    This tests that a date column can be cast to different numeric/floating-point types.
    This needs to be tested with ANSI disabled, because none of these conversions are
    ANSI-compliant.
    """
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: unary_op_df(spark, date_gen).selectExpr(
            "cast(a as boolean)", "cast(a as byte)", "cast(a as short)", "cast(a as int)", "cast(a as long)", "cast(a as float)", "cast(a as double)"),
            conf=ansi_disabled_conf)

# ToPrettyString is generated by df.show(), and there is no ToPrettyString function in pyspark.sql.functions package
# Here use df.show() to test ToPrettyString on date column. If fall back occurs, `with_gpu_session` will report error:
#   Part of the plan is not columnar class
@allow_non_gpu('CollectLimitExec')
def test_to_pretty_string_for_date_and_test_cast_between_date_string():
    def _query(spark):
        spark.createDataFrame([('2025-01-02',), ('2025-01-03',)], 'str_col string').selectExpr(
            "CAST(str_col as date)", "CAST(CAST(str_col as date) as string)").show()

    with_gpu_session(
        lambda spark: _query(spark),
        {'spark.rapids.sql.hasExtendedYearValues': False,
         'spark.sql.session.timeZone': 'America/Los_Angeles'})


def test_cast_string_to_timestamp_valid():
    def _gen_df(spark):
        return spark.createDataFrame(
            [
                ("   2023-11-05T03:04:55 +00:00",),  # leading spaces
                ("2023-11-05 03:04:55 +01:02   ",),  # tailing spaces
                ("   2023-11-05 03:04:55 +01:02   ",), # both leading/ tailing spaces
                ("2023-11-05 03:04:55 -01:02",),
                ("2023-11-05 03:04:55 +01:02",),
                ("2023-11-05 03:04:55 +10:59",),
                ("2023-11-05 03:04:55 +10:59:03",),
                ("2023-11-05 03:04:55 +105903",),
                ("2023-11-05 03:04:55 +1059",),
                ("2023-11-05 03:04:55 +10",),
                ("2023-11-05T03:04:55 UT+00:00",),
                ("2023-11-05 03:04:55 UT+01:02",),
                ("2023-11-05 03:04:55 UT+01:02",),
                ("2023-11-05 03:04:55 UT+01:02",),
                ("2023-11-05 03:04:55 UT+01:02",),
                ("2023-11-05 03:04:55 UT+10:59",),
                ("2023-11-05 03:04:55 UT-10:59:03",),
                ("2023-11-05 03:04:55 UT+105903",),
                ("2023-11-05 03:04:55 UT+1059",),
                ("2023-11-05 03:04:55 UT+10",),
                ("2023-11-05T03:04:55 UTC+00:00",),
                ("2023-11-05 03:04:55 UTC+01:02",),
                ("2023-11-05 03:04:55 UTC+01:02",),
                ("2023-11-05 03:04:55 UTC+01:02",),
                ("2023-11-05 03:04:55 UTC+01:02",),
                ("2023-11-05 03:04:55 UTC+10:59",),
                ("2023-11-05 03:04:55 UTC+10:59:03",),
                ("2023-11-05 03:04:55 UTC+105903",),
                ("2023-11-05 03:04:55 UTC+1059",),
                ("2023-11-05 03:04:55    UTC-10",), # there are spaces before timezone
                ("2023-11-05T03:04:55 GMT+00:00",),
                ("2023-11-05 03:04:55 GMT+01:02",),
                ("2023-11-05 03:04:55 GMT+01:02",),
                ("2023-11-05 03:04:55 GMT-01:02",),
                ("2023-11-05 03:04:55 GMT+01:02",),
                ("2023-11-05 03:04:55 GMT+10:59",),
                ("2023-11-05 03:04:55 GMT+10:59:03",),
                ("2023-11-05 03:04:55 GMT+105903",),
                ("2023-11-05 03:04:55 GMT+1059",),
                ("2023-11-05 03:04:55 GMT+10",),
                ("2023-11-05T03:04:55.123456789 PST",),
                ("2023-11-05 03:04:55.123456 PST",),
                ("2023-11-05T03:04:55 CTT",),
                ("2023-11-05 03:04:55 JST",),
                ("2023-11-05 03:04:55 PST",),
                ("2023-11-05 03:04:55",),
                ("2023-11-05",),
                ("2023-11",),
                ("2023",),
                ("12345",),
                ("2023-1-1",),
                ("2023-1-01",),
                ("2023-01-1",),
                ("2023-01-01",),
                ("2028-02-29",),
                ("2023-01-01 00:00:00Z",),
                ("2023-01-01 00:00:00 Z",),
                ("2023-01-01 00:00:00 GMT0",),
                ("   2023-11-05 03:04:55 +1:02   ",),  # both leading/ tailing spaces
                ("2023-11-05 03:04:55 -01:2",),
                ("2023-11-05 03:04:55 +1:2",),
                ("2023-11-05 03:04:55 UT+1:02",),
                ("2023-11-05 03:04:55 UT+01:2",),
                ("2023-11-05 03:04:55 UT+1:2",),
                ("2023-11-05 03:04:55 UTC+1:02",),
                ("2023-11-05 03:04:55 UTC+01:2",),
                ("2023-11-05 03:04:55 UTC+1:2",),
                ("2023-11-05 03:04:55 GMT+1:02",),
                ("2023-11-05 03:04:55 GMT-01:2",),
                ("2023-11-05 03:04:55 GMT+1:2",),
            ],
            'str_col string')
    def _query(spark):
        # depends on the timezone info in `GpuTimeZoneDB`, load first
        spark._jvm.com.nvidia.spark.rapids.jni.GpuTimeZoneDB.cacheDatabase()
        return _gen_df(spark).selectExpr("cast(str_col as timestamp)")
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: _query(spark))

# Disable ANSI for Spark 4.0
@disable_ansi_mode
def test_cast_string_to_timestamp_just_time_ANSI_OFF():
    # For the just time strings, will get current date to fill the missing date.
    # E.g.: "T00:00:00" will be "2025-05-23T00:00:00"
    # This test case is sensitive to the current date, and may cause diff between CPU and GPU
    # E.g.: CPU get date: 2025-05-23 and GPU runs later and the date becomes 2025-05-24
    # The following code checks the date is the same in 5 minutes, and assumes this case will
    # finish in 5 minutes.
    current_time = datetime.now()
    current_day = current_time.date().day
    day_of_five_minutes_later = (current_time + timedelta(minutes=5)).date().day
    if current_day != day_of_five_minutes_later:
        return

    def _gen_df(spark):
        return spark.createDataFrame(
            [
                ("T23:17:50",),
                ("T23:17:50",),
                ("T23:17:50",),

                # This is invalid value for Spark 4.0
                # For details, refer to https://github.com/NVIDIA/spark-rapids-jni/issues/3401
                (" \r\n\tT23:17:50",),

                ("T23:17:50 \r\n\t",),
                ("T00",),
                ("T1:2",),
                ("T01:2",),
                ("T1:02",),
                ("T01:02",),
                ("T01:02:03",),
                ("T1:2:3",),
                ("T01:02:03 UTC",),
                ("1:2",),
                ("1:02",),
                ("01:2",),
                ("01:02",),
                ("1:2:3",),
                ("01:02:03",),
                ("01:02:03.123456",),
                ("1:2:3.123456",),
            ],
            'str_col string')

    def _query(spark):
        # depends on the timezone info in `GpuTimeZoneDB`, load first
        spark._jvm.com.nvidia.spark.rapids.jni.GpuTimeZoneDB.cacheDatabase()
        return _gen_df(spark).selectExpr("cast(str_col as timestamp)")

    assert_gpu_and_cpu_are_equal_collect(lambda spark: _query(spark))


def test_cast_string_to_timestamp_valid_just_time_with_timezone():
    # For the just time strings, will get current date to fill the missing date.
    # E.g.: "T00:00:00" will be "2025-05-23T00:00:00"
    # This test case is sensitive to the current date, and may cause diff between CPU and GPU
    # E.g.: CPU get date: 2025-05-23 and GPU runs later and the date becomes 2025-05-24
    # The following code checks the date is the same in 5 minutes, and assumes this case will
    # finish in 5 minutes.
    pst_tz = pytz.timezone('America/Los_Angeles')
    current_time = datetime.now(pst_tz)
    current_day = current_time.date().day
    day_of_five_minutes_later = (current_time + timedelta(minutes=5)).date().day
    if current_day != day_of_five_minutes_later:
        return

    def _gen_df(spark):
        return spark.createDataFrame(
            [
                # PST = America/Los_Angeles
                ("T23:17:50.201567 PST",),
                ("T23:17:50.201567 PST",),
                ("T23:17:50.201567 PST",),
                ("T23:17:50 PST",),
                ("T23:17:50 PST",),
                ("T23:17:50 PST",),
            ],
            'str_col string')

    def _query(spark):
        # depends on the timezone info in `GpuTimeZoneDB`, load first
        spark._jvm.com.nvidia.spark.rapids.jni.GpuTimeZoneDB.cacheDatabase()
        return _gen_df(spark).selectExpr("cast(str_col as timestamp)")

    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: _query(spark))


_cast_string_to_timestamp_invalid = [
    # empty after strip
    ("",),
    ("  ",),

    # test non leap year and day is 29
    (" -2025-2-29 ",),

    # non-existence tz, result is invalid, but keeps tz type is other
    (" 2023-11-05 03:04:55 non-existence-tz ",),

    # invalid month
    ("-2025-13-1",),
    ("-2025-99-1",),

    # invalid day
    ("-2025-01-32",),
    ("-2025-01-99",),

    # invalid hour
    ("2000-01-01 24:00:00",),
    ("2000-01-01 99:00:00",),

    # invalid minute
    ("2000-01-01 00:60:00",),
    ("2000-01-01 00:61:00",),

    # invalid second
    ("2000-01-01 00:00:60",),
    ("2000-01-01 00:00:61",),

    # invalid date
    ("x2025",),
    ("12",),
    ("123",),
    ("1234567",),
    ("2200x",),
    ("2200-",),
    ("2200-x",),
    ("2200-123",),
    ("2200-12x",),
    ("2200-01-",),
    ("2200-01-x",),
    ("2200-01-11x",),
    ("2200-01-113",),
    ("2200-03-25T",),
    ("2200-03-25 x",),
    ("2200-03-25Tx",),

    # invalid time
    ("Tx",),
    ("T00x",),
    ("T00:",),
    ("T00:x",),
    ("T000:00",),
    ("T00:022",),
    ("T00:02:",),
    ("T00:02:x",),
    ("T00:02:003",),
    ("T123",),
    ("T12345",),
    ("T00:02:003",),

    # invalid TZs
    ("2000-01-01 00:00:00 +",),
    ("2000-01-01 00:00:00 -X",),
    ("2000-01-01 00:00:00 +07:",),
    ("2000-01-01 00:00:00 +09:x",),
    ("2000-01-01 00:00:00 +15:07x",),
    ("2000-01-01 00:00:00 +15:07:",),
    ("2000-01-01 00:00:00 +15:07:x",),
    ("2000-01-01 00:00:00 +15:07:1",),
    ("2000-01-01 00:00:00 +15:07:12x",),
    ("2000-01-01 00:00:00 +01x",),
    ("2000-01-01 00:00:00 +0102x",),
    ("2000-01-01 00:00:00 +010203x",),
    ("2000-01-01 00:00:00 +111",),
    ("2000-01-01 00:00:00 +11111",),
    ("2000-01-01 00:00:00 +1x",),

    # exceeds the max value 18 hours
    ("2000-01-01 00:00:00 +180001",),
    ("2000-01-01 00:00:00 -180001",),
    ("2000-01-01 00:00:00 -08:1:08",),
    ("2000-01-01 00:00:00 -8:1:08",),

    # U is invalid tz
    ("2000-01-01 00:00:00 U",),

    # Result is invalid, although ts is not zero
    ("2023-11-05 03:04:55 Ux",),
    ("2023-11-05 03:04:55 UTx",),
    ("2023-11-05 03:04:55 UTCx",),
    ("2023-11-05 03:04:55 UT+",),
    ("2023-11-05 03:04:55 UT-08:1:08",),
    ("2023-11-05 03:04:55 UT-8:1:08",),
    ("2023-11-05 03:04:55 Gx",),
    ("2023-11-05 03:04:55 GMTx",),
    ("2023-11-05 03:04:55 GMT+",),
    ("2023-11-05 03:04:55 GMT-08:1:08",),
    ("2023-11-05 03:04:55 GMT-8:1:08",),

    # invalid year: e.g. abs(year) > 30,000
    ("300001-01-01",),
    ("-300001-01-01",),

    # invalid just time
    ("+00:00:00",),
    ("-00:00:00",),
    ("+T00:00:00",),
    ("-T00:00:00",),
]

# Disable ANSI for Spark 4.0
@disable_ansi_mode
def test_cast_string_to_timestamp_invalid_ANSI_OFF():
    def _gen_df(spark):
        return spark.createDataFrame(
            _cast_string_to_timestamp_invalid,
            'str_col string')
    def _query(spark):
        # depends on the timezone info in `GpuTimeZoneDB`, load first
        spark._jvm.com.nvidia.spark.rapids.jni.GpuTimeZoneDB.cacheDatabase()
        return _gen_df(spark).selectExpr("cast(str_col as timestamp)")

    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: _query(spark))

# for each invalid item, will cause an exception.
@pytest.mark.parametrize('invalid_item', _cast_string_to_timestamp_invalid)
def test_cast_string_to_timestamp_invalid_ansi_enabled(invalid_item):
    def _gen_df(spark):
        return spark.createDataFrame(
            [invalid_item],
            'str_col string')
    def _query(spark):
        # depends on the timezone info in `GpuTimeZoneDB`, load first
        spark._jvm.com.nvidia.spark.rapids.jni.GpuTimeZoneDB.cacheDatabase()
        return _gen_df(spark).selectExpr("cast(str_col as timestamp)")

    assert_gpu_and_cpu_error(
        lambda spark: _query(spark).collect(),
        conf=ansi_enabled_conf,
        error_message="DateTimeException")

# Disable ANSI for Spark 4.0
@disable_ansi_mode
@pytest.mark.parametrize(
    'pattern',
    [
        # has no timezone, 4 years, only date, will run on GPU
        pytest.param(r'[0-9]{4,4}-[0-9]{1,2}-[0-9]{1,2}', id='yyyy-mm-dd'),

        # has no timezone, 4-6 years, only date, will run on GPU
        pytest.param(r'[0-9]{4,6}-[0-9]{1,2}-[0-9]{1,2}', id='yyyy[y][y]-mm-dd'),

        # has no timezone, 4 years, will run on GPU
        pytest.param(r'[0-9]{4,4}-[0-9]{1,2}-[0-9]{1,2}[ T][0-9]{1,2}:[0-9]{1,2}:[0-9]{1,2}\.[0-9]{0,6}Z?', id='yyyy-mm-dd hh:mm:ss'),

        # has no timezone, 4-6 years, will run on GPU
        pytest.param(r'[0-9]{4,6}-[0-9]{1,2}-[0-9]{1,2}[ T][0-9]{1,2}:[0-9]{1,2}:[0-9]{1,2}\.[0-9]{0,6}Z?', id='yyyy[y][y]-mm-dd hh:mm:ss'),

        # CTT is non-DST, will run on GPU
        pytest.param(
            r'[0-9]{4,4}-[0-9]{1,2}-[0-9]{1,2}[ T][0-9]{1,2}:[0-9]{1,2}:[0-9]{1,2} CTT', id='yyyy-mm-dd hh:mm:ss CTT'),

        # PST is DST, year is less than 2200, run on GPU
        pytest.param(
            r'[0-2][0-1][0-9]{2,2}-[0-9]{1,2}-[0-9]{1,2}[ T][0-9]{1,2}:[0-9]{1,2}:[0-9]{1,2} PST',
            id='yyyy-mm-dd hh:mm:ss PST, ts < 2200'),

        # PST is DST, and year is larger than 2200, run on CPU
        pytest.param(
            r'3[0-9]{3,3}-[0-9]{1,2}-[0-9]{1,2}[ T][0-9]{1,2}:[0-9]{1,2}:[0-9]{1,2} PST',
            id='yyyy-mm-dd[ T]hh:mm:ss PST, ts > 2200')
    ])
def test_cast_string_to_timestamp_const_format_ANSI_OFF(pattern):
    gen = [("str_col", StringGen(pattern))]
    def _query(spark):
        # depends on the timezone info in `GpuTimeZoneDB`, load first
        spark._jvm.com.nvidia.spark.rapids.jni.GpuTimeZoneDB.cacheDatabase()
        return gen_df(spark, gen).selectExpr("cast(str_col as timestamp)")

    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: _query(spark))
