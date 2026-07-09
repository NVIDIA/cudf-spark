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

from logging import exception
import pytest

from asserts import assert_gpu_and_cpu_are_equal_collect, assert_gpu_and_cpu_error, assert_gpu_fallback_collect, assert_gpu_and_cpu_are_equal_sql
from data_gen import *
from marks import ignore_order, incompat, approximate_float, allow_non_gpu, allow_non_gpu_conditional, datagen_overrides, disable_ansi_mode
from pyspark.sql.types import *
from pyspark.sql.types import IntegralType
from spark_session import *
import pyspark.sql.functions as f
import pyspark.sql.utils
from datetime import timedelta

_arithmetic_exception_string = 'org.apache.spark.SparkArithmeticException' if is_before_spark_400() else \
        'pyspark.errors.exceptions.captured.ArithmeticException'

# No overflow gens here because we just focus on verifying the fallback to CPU when
# enabling ANSI mode. But overflows will fail the tests because CPU runs raise
# exceptions.
_no_overflow_multiply_gens_for_fallback = [
    ByteGen(min_val = 1, max_val = 10, special_cases=[]),
    ShortGen(min_val = 1, max_val = 100, special_cases=[]),
    IntegerGen(min_val = 1, max_val = 1000, special_cases=[]),
    LongGen(min_val = 1, max_val = 3000, special_cases=[])]


_no_overflow_multiply_gens = _no_overflow_multiply_gens_for_fallback + [
    DecimalGen(10, 0),
    DecimalGen(19, 0)]

_decimal_gen_7_7 = DecimalGen(precision=7, scale=7)
_decimal_gen_18_0 = DecimalGen(precision=18, scale=0)
_decimal_gen_18_3 = DecimalGen(precision=18, scale=3)
_decimal_gen_30_2 = DecimalGen(precision=30, scale=2)
_decimal_gen_36_5 = DecimalGen(precision=36, scale=5)
_decimal_gen_36_neg5 = DecimalGen(precision=36, scale=-5)
_decimal_gen_38_0 = DecimalGen(precision=38, scale=0)
_decimal_gen_38_10 = DecimalGen(precision=38, scale=10)
_decimal_gen_38_neg10 = DecimalGen(precision=38, scale=-10)

_arith_data_gens_diff_precision_scale_and_no_neg_scale_no_38_0_no_38_10_no_36_5 = [
    decimal_gen_32bit, decimal_gen_64bit, _decimal_gen_18_0, decimal_gen_128bit,
    _decimal_gen_30_2
]

_arith_data_gens_diff_precision_scale_and_no_neg_scale_no_38_0 = \
    _arith_data_gens_diff_precision_scale_and_no_neg_scale_no_38_0_no_38_10_no_36_5 + \
    [_decimal_gen_36_5, _decimal_gen_38_10]

_arith_decimal_gens_high_precision_no_neg_scale = [_decimal_gen_36_5, _decimal_gen_38_0, _decimal_gen_38_10]

_arith_decimal_gens_high_precision = _arith_decimal_gens_high_precision_no_neg_scale + [
    _decimal_gen_36_neg5, _decimal_gen_38_neg10
]

# Gens covering the full accepted input-type set for math-unary functions
# (asinh/atanh/cbrt/...): Spark auto-casts FLOAT/DECIMAL to DOUBLE before
# applying these, but the cast+op code path is worth exercising end-to-end.
_math_unary_input_gens = [double_gen, FloatGen(), decimal_gen_64bit]

# DECIMAL input requires GPU decimal->double cast (off by default for
# precision-loss reasons).
_math_unary_conf = {'spark.rapids.sql.castDecimalToFloat.enabled': 'true'}

_arith_data_gens_diff_precision_scale_and_no_neg_scale = \
    _arith_data_gens_diff_precision_scale_and_no_neg_scale_no_38_0 + [_decimal_gen_38_0]

_arith_decimal_gens_no_neg_scale = _arith_data_gens_diff_precision_scale_and_no_neg_scale + [_decimal_gen_7_7]

_arith_decimal_gens = _arith_decimal_gens_no_neg_scale + [
    decimal_gen_32bit_neg_scale, _decimal_gen_36_neg5, _decimal_gen_38_neg10
]

_arith_decimal_gens_low_precision = \
    _arith_data_gens_diff_precision_scale_and_no_neg_scale_no_38_0_no_38_10_no_36_5 + \
    [decimal_gen_32bit_neg_scale, _decimal_gen_7_7]


_arith_data_gens = numeric_gens + _arith_decimal_gens

_arith_data_gens_no_neg_scale = numeric_gens + _arith_decimal_gens_no_neg_scale

_arith_decimal_gens_no_neg_scale_38_0_overflow = \
    _arith_data_gens_diff_precision_scale_and_no_neg_scale_no_38_0 + [
        _decimal_gen_7_7,
        pytest.param(_decimal_gen_38_0, marks=pytest.mark.skipif(
            is_spark_330_or_later(), reason='This case overflows in Spark 3.3.0+'))]

def _get_overflow_df(spark, data, data_type, expr):
    return spark.createDataFrame(
        SparkContext.getOrCreate().parallelize([data]),
        StructType([StructField('a', data_type)])
    ).selectExpr(expr)

@pytest.mark.parametrize('data_gen', _arith_data_gens, ids=idfn)
@disable_ansi_mode
def test_addition(data_gen):
    data_type = data_gen.data_type
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : binary_op_df(spark, data_gen).select(
                f.col('a') + f.lit(100).cast(data_type),
                f.lit(-12).cast(data_type) + f.col('b'),
                f.lit(None).cast(data_type) + f.col('a'),
                f.col('b') + f.lit(None).cast(data_type),
                f.col('a') + f.col('b')))

# If it will not overflow for multiply it is good for add too
@pytest.mark.parametrize('data_gen', _no_overflow_multiply_gens, ids=idfn)
def test_addition_ansi_no_overflow(data_gen):
    data_type = data_gen.data_type
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : binary_op_df(spark, data_gen).select(
                f.col('a') + f.lit(100).cast(data_type),
                f.lit(-12).cast(data_type) + f.col('b'),
                f.lit(None).cast(data_type) + f.col('a'),
                f.col('b') + f.lit(None).cast(data_type),
                f.col('a') + f.col('b')),
            conf=ansi_enabled_conf)

@pytest.mark.parametrize('data_gen', _arith_data_gens, ids=idfn)
@disable_ansi_mode
def test_subtraction(data_gen):
    data_type = data_gen.data_type
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : binary_op_df(spark, data_gen).select(
                f.col('a') - f.lit(100).cast(data_type),
                f.lit(-12).cast(data_type) - f.col('b'),
                f.lit(None).cast(data_type) - f.col('a'),
                f.col('b') - f.lit(None).cast(data_type),
                f.col('a') - f.col('b')))

@pytest.mark.parametrize('lhs', [byte_gen, short_gen, int_gen, long_gen, DecimalGen(6, 5),
    DecimalGen(6, 4), DecimalGen(5, 4), DecimalGen(5, 3), DecimalGen(4, 2), DecimalGen(3, -2),
    DecimalGen(16, 7), DecimalGen(19, 0), DecimalGen(30, 10)], ids=idfn)
@pytest.mark.parametrize('rhs', [byte_gen, short_gen, int_gen, long_gen, DecimalGen(6, 3),
    DecimalGen(10, -2), DecimalGen(15, 3), DecimalGen(30, 12), DecimalGen(3, -3),
    DecimalGen(27, 7), DecimalGen(20, -3)], ids=idfn)
@pytest.mark.parametrize('addOrSub', ['+', '-'])
@disable_ansi_mode
def test_addition_subtraction_mixed(lhs, rhs, addOrSub):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : two_col_df(spark, lhs, rhs).selectExpr(f"a {addOrSub} b")
    )

# If it will not overflow for multiply it is good for subtract too
@pytest.mark.parametrize('data_gen', _no_overflow_multiply_gens, ids=idfn)
def test_subtraction_ansi_no_overflow(data_gen):
    data_type = data_gen.data_type
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : binary_op_df(spark, data_gen).select(
                f.col('a') - f.lit(100).cast(data_type),
                f.lit(-12).cast(data_type) - f.col('b'),
                f.lit(None).cast(data_type) - f.col('a'),
                f.col('b') - f.lit(None).cast(data_type),
                f.col('a') - f.col('b')),
            conf=ansi_enabled_conf)

@pytest.mark.parametrize('data_gen', numeric_gens + [
    decimal_gen_32bit_neg_scale, decimal_gen_32bit, _decimal_gen_7_7,
    DecimalGen(precision=8, scale=8), decimal_gen_64bit, _decimal_gen_18_3,
    _decimal_gen_38_10,
    _decimal_gen_38_neg10
    ], ids=idfn)
@disable_ansi_mode
def test_multiplication(data_gen):
    data_type = data_gen.data_type
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : binary_op_df(spark, data_gen).select(
                f.col('a'), f.col('b'),
                f.col('a') * f.lit(100).cast(data_type),
                f.lit(-12).cast(data_type) * f.col('b'),
                f.lit(None).cast(data_type) * f.col('a'),
                f.col('b') * f.lit(None).cast(data_type),
                f.col('a') * f.col('b')
                ))

@pytest.mark.parametrize('data_gen', _no_overflow_multiply_gens_for_fallback, ids=idfn)
def test_multiplication_no_overflow_and_ansi_enabled(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : binary_op_df(spark, data_gen).select(
                f.col('a') * f.col('b')),
            conf=ansi_enabled_conf)

@pytest.mark.parametrize('data_gen', [float_gen, double_gen, decimal_gen_32bit, DecimalGen(19, 0)], ids=idfn)
def test_multiplication_ansi_enabled(data_gen):
    data_type = data_gen.data_type
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : binary_op_df(spark, data_gen).select(
                f.col('a') * f.lit(100).cast(data_type),
                f.col('a') * f.col('b')),
            conf=ansi_enabled_conf)

def test_multiplication_ansi_overflow_for_decimal():
    exception_str = 'ArithmeticException'
    assert_gpu_and_cpu_error(
        lambda spark : unary_op_df(spark, DecimalGen(38, 0)).selectExpr("a * " + "9"*38 + " as ret").collect(),
        ansi_enabled_conf,
        exception_str)

_gen_gen_lit_tuple_list_overflow_for_multiply = [
    (ByteGen(nullable=False, min_val=30, max_val=50, special_cases=[]), ByteGen(nullable=False, min_val=50, max_val=127, special_cases=[]), -50),  # 30 * 50 > 127 (BYTE_MAX)
    (ShortGen(nullable=False, min_val=SHORT_MIN, max_val=-30000, special_cases=[]), ShortGen(nullable=False, min_val=2, max_val=SHORT_MAX, special_cases=[]), 2), # 30000 * 2 > 32767 (SHORT_MAX)
    (IntegerGen(nullable=False, min_val=INT_MIN, max_val=INT_MIN//2, special_cases=[]), IntegerGen(nullable=False, min_val=3, max_val=INT_MAX, special_cases=[]), 3) , # INT_MIN//2 * 3 < INT_MIN
    (LongGen(nullable=False, min_val=LONG_MAX//2, max_val=LONG_MAX, special_cases=[]), LongGen(nullable=False, min_val=3, max_val=LONG_MAX, special_cases=[]), 3)] # LONG_MAX//2 * 3 > LONG_MAX
@pytest.mark.parametrize('gen_gen_lit', _gen_gen_lit_tuple_list_overflow_for_multiply, ids=idfn)
def test_multiplication_ansi_overflow_for_integer(gen_gen_lit):
    (gen1, gen2, lit) = gen_gen_lit
    data_type = gen1.data_type
    exception_str = 'ArithmeticException'
    assert_gpu_and_cpu_error(
        lambda spark : two_col_df(spark, gen1, gen2, length=4).select(
            f.lit(lit).cast(data_type) * f.col('a'),
            f.col('a') * f.lit(lit).cast(data_type),
            f.col('a') * f.col('b')).collect(),
        ansi_enabled_conf,
        exception_str)

_gen_lit_pair_list_without_overflow_for_multiply = [
    (ByteGen(min_val=-11, max_val=11, special_cases=[]), 11),  # 11 * 11 < 127 (BYTE_MAX)
    (ShortGen(min_val=-181, max_val=181, special_cases=[]), 181), # 181 * 181 < 32767 (SHORT_MAX)
    (IntegerGen(min_val=-46340, max_val=46340, special_cases=[]), 46340) , # 46340 * 46340 < 2147483647 (INT_MAX)
    (LongGen(min_val=-3037000499, max_val=3037000499, special_cases=[]), 3037000499), # 3037000499 * 3037000499 < 9223372036854775807(LONG_MAX)
    (float_gen, 12),
    (double_gen, 12)]
@pytest.mark.parametrize('data_gen_lit_pair', _gen_lit_pair_list_without_overflow_for_multiply, ids=idfn)
@pytest.mark.parametrize('ansi_enabled', [True, False])
def test_multiplication_without_overflow_for_numeric_ansi_on_off(data_gen_lit_pair, ansi_enabled):
    (data_gen, lit) = data_gen_lit_pair
    data_type = data_gen.data_type
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : binary_op_df(spark, data_gen).select(
            f.lit(lit).cast(data_type) * f.col('a'),
            f.col('a') * f.lit(lit).cast(data_type),
            f.col('a') * f.col('b')),
        conf={'spark.sql.ansi.enabled': ansi_enabled})

@pytest.mark.parametrize('lhs', [byte_gen, short_gen, int_gen, long_gen, DecimalGen(6, 5),
    DecimalGen(6, 4), DecimalGen(5, 4), DecimalGen(5, 3), DecimalGen(4, 2), DecimalGen(3, -2),
    DecimalGen(16, 7), DecimalGen(19, 0), DecimalGen(30, 10)], ids=idfn)
@pytest.mark.parametrize('rhs', [byte_gen, short_gen, int_gen, long_gen, DecimalGen(6, 3),
    DecimalGen(10, -2), DecimalGen(15, 3), DecimalGen(30, 12), DecimalGen(3, -3),
    DecimalGen(27, 7), DecimalGen(20, -3)], ids=idfn)
@disable_ansi_mode
def test_multiplication_mixed(lhs, rhs):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : two_col_df(spark, lhs, rhs).select(
                f.col('a') * f.col('b')))

@approximate_float # we should get the perfectly correct answer for floats except when casting a decimal to a float in some corner cases.
@pytest.mark.parametrize('lhs', [float_gen, double_gen], ids=idfn)
@pytest.mark.parametrize('rhs', [DecimalGen(6, 3), DecimalGen(10, -2), DecimalGen(15, 3)], ids=idfn)
def test_float_multiplication_mixed(lhs, rhs):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : two_col_df(spark, lhs, rhs).select(
                f.col('a') * f.col('b')),
            conf={'spark.rapids.sql.castDecimalToFloat.enabled': 'true'})

@pytest.mark.parametrize('data_gen', [double_gen, decimal_gen_32bit_neg_scale, DecimalGen(6, 3),
 DecimalGen(5, 5), DecimalGen(6, 0), DecimalGen(7, 4), DecimalGen(15, 0), DecimalGen(18, 0),
 DecimalGen(17, 2), DecimalGen(16, 4), DecimalGen(38, 21), DecimalGen(21, 17), DecimalGen(3, -2)], ids=idfn)
@disable_ansi_mode
def test_division(data_gen):
    data_type = data_gen.data_type
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : binary_op_df(spark, data_gen).select(
                f.col('a') / f.lit(100).cast(data_type),
                f.lit(-12).cast(data_type) / f.col('b'),
                f.lit(None).cast(data_type) / f.col('a'),
                f.col('b') / f.lit(None).cast(data_type),
                f.col('a') / f.col('b')))

@pytest.mark.parametrize('rhs', [byte_gen, short_gen, int_gen, long_gen, DecimalGen(4, 1), DecimalGen(5, 0), DecimalGen(5, 1), DecimalGen(10, 5)], ids=idfn)
@pytest.mark.parametrize('lhs', [byte_gen, short_gen, int_gen, long_gen, DecimalGen(5, 3), DecimalGen(4, 2), DecimalGen(1, -2), DecimalGen(16, 1)], ids=idfn)
@disable_ansi_mode
def test_division_mixed(lhs, rhs):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : two_col_df(spark, lhs, rhs).select(
                f.col('a'), f.col('b'),
                f.col('a') / f.col('b')))

# Spark has some problems with some decimal operations where it can try to generate a type that is invalid (scale > precision) which results in an error
# instead of increasing the precision. So we have a second test that deals with a few of these use cases
@pytest.mark.parametrize('rhs', [DecimalGen(30, 10), DecimalGen(28, 18)], ids=idfn)
@pytest.mark.parametrize('lhs', [DecimalGen(27, 7), DecimalGen(20, -3)], ids=idfn)
@disable_ansi_mode
def test_division_mixed_larger_dec(lhs, rhs):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : two_col_df(spark, lhs, rhs).select(
                f.col('a'), f.col('b'),
                f.col('a') / f.col('b')))

@disable_ansi_mode
def test_special_decimal_division():
    for precision in range(1, 39):
        for scale in range(-3, precision + 1):
            print("PRECISION " + str(precision) + " SCALE " + str(scale))
            data_gen = DecimalGen(precision, scale)
            assert_gpu_and_cpu_are_equal_collect(
                    lambda spark : two_col_df(spark, data_gen, data_gen).select(
                        f.col('a') / f.col('b')))

@approximate_float # we should get the perfectly correct answer for floats except when casting a decimal to a float in some corner cases.
@pytest.mark.parametrize('rhs', [float_gen, double_gen], ids=idfn)
@pytest.mark.parametrize('lhs', [DecimalGen(5, 3), DecimalGen(4, 2), DecimalGen(1, -2), DecimalGen(16, 1)], ids=idfn)
@disable_ansi_mode
def test_float_division_mixed(lhs, rhs):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : two_col_df(spark, lhs, rhs).select(
                f.col('a') / f.col('b')),
            conf={'spark.rapids.sql.castDecimalToFloat.enabled': 'true'})

@pytest.mark.parametrize('data_gen', integral_gens + [
    decimal_gen_32bit, decimal_gen_64bit, _decimal_gen_7_7, _decimal_gen_18_3, _decimal_gen_30_2,
    _decimal_gen_36_5, _decimal_gen_38_0], ids=idfn)
@disable_ansi_mode
def test_int_division(data_gen):
    string_type = to_cast_string(data_gen.data_type)
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : binary_op_df(spark, data_gen).selectExpr(
                'a DIV cast(100 as {})'.format(string_type),
                'cast(-12 as {}) DIV b'.format(string_type),
                'cast(null as {}) DIV a'.format(string_type),
                'b DIV cast(null as {})'.format(string_type),
                'a DIV b'))

@pytest.mark.parametrize('lhs', [DecimalGen(6, 5), DecimalGen(5, 4), DecimalGen(3, -2), _decimal_gen_30_2], ids=idfn)
@pytest.mark.parametrize('rhs', [DecimalGen(13, 2), DecimalGen(6, 3), _decimal_gen_38_0,
                                 pytest.param(_decimal_gen_36_neg5, marks=pytest.mark.skipif(not is_before_spark_340() or is_databricks113_or_later(), reason='SPARK-41207'))], ids=idfn)
@disable_ansi_mode
def test_int_division_mixed(lhs, rhs):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : two_col_df(spark, lhs, rhs).selectExpr(
                'a DIV b'))

@pytest.mark.parametrize('data_gen', _arith_data_gens, ids=idfn)
@disable_ansi_mode
def test_mod(data_gen):
    data_type = data_gen.data_type
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : binary_op_df(spark, data_gen).select(
                f.col('a') % f.lit(100).cast(data_type),
                f.lit(-12).cast(data_type) % f.col('b'),
                f.lit(None).cast(data_type) % f.col('a'),
                f.col('b') % f.lit(None).cast(data_type),
                f.col('a') % f.col('b')))

# pmod currently falls back for Decimal(precision=38)
# https://github.com/NVIDIA/spark-rapids/issues/6336
# only testing numeric_gens because of https://github.com/NVIDIA/spark-rapids/issues/7553
_pmod_gens = numeric_gens
test_pmod_fallback_decimal_gens = [ decimal_gen_32bit, decimal_gen_64bit, _decimal_gen_18_0, decimal_gen_128bit,
                              _decimal_gen_30_2, _decimal_gen_36_5,
                              DecimalGen(precision=37, scale=0), DecimalGen(precision=37, scale=10),
                              _decimal_gen_7_7]

@pytest.mark.parametrize('data_gen', _pmod_gens, ids=idfn)
@disable_ansi_mode
def test_pmod(data_gen):
    string_type = to_cast_string(data_gen.data_type)
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : binary_op_df(spark, data_gen).selectExpr(
                'pmod(a, cast(100 as {}))'.format(string_type),
                'pmod(cast(-12 as {}), b)'.format(string_type),
                'pmod(cast(null as {}), a)'.format(string_type),
                'pmod(b, cast(null as {}))'.format(string_type),
                'pmod(a, b)'))


# Spark 3.3 non-DB wraps decimal arithmetic in CheckOverflow. CheckOverflow blocks
# CPU bridge optimization, so decimal pmod falls back at ProjectExec on those builds.
@allow_non_gpu_conditional(is_before_spark_340() and not is_databricks_runtime(), "ProjectExec")
@allow_non_gpu("Pmod", "PromotePrecision")
@pytest.mark.parametrize('data_gen', test_pmod_fallback_decimal_gens + [_decimal_gen_38_0, _decimal_gen_38_10], ids=idfn)
@disable_ansi_mode
def test_pmod_fallback(data_gen):
    string_type = to_cast_string(data_gen.data_type)
    assert_gpu_fallback_collect(
        lambda spark : binary_op_df(spark, data_gen).selectExpr(
            'pmod(a, cast(100 as {}))'.format(string_type),
            'pmod(cast(-12 as {}), b)'.format(string_type),
            'pmod(cast(null as {}), a)'.format(string_type),
            'pmod(b, cast(null as {}))'.format(string_type),
            'pmod(a, b)'),
        "Pmod")

# test pmod(Long.MinValue, -1) = 0 and Long.MinValue % -1 = 0, should not throw
def test_mod_pmod_long_min_value():
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : spark.createDataFrame([(-9223372036854775808,)], ["a"]).selectExpr(
            'pmod(a, -1L)',
            'a % -1L'),
        ansi_enabled_conf)

# pmod currently falls back for Decimal(precision=38)
# https://github.com/NVIDIA/spark-rapids/issues/6336
@pytest.mark.xfail(reason='Decimals type disabled https://github.com/NVIDIA/spark-rapids/issues/7553')
@pytest.mark.parametrize('data_gen', [decimal_gen_32bit, decimal_gen_64bit, _decimal_gen_18_0,
                                      decimal_gen_128bit, _decimal_gen_30_2, _decimal_gen_36_5], ids=idfn)
@pytest.mark.parametrize('overflow_exp', [
    'pmod(a, cast(0 as {}))',
    'pmod(cast(-12 as {}), cast(0 as {}))',
    'a % (cast(0 as {}))',
    'cast(-12 as {}) % cast(0 as {})'], ids=idfn)
def test_mod_pmod_by_zero(data_gen, overflow_exp):
    string_type = to_cast_string(data_gen.data_type)
    if is_before_spark_320():
        exception_str = 'java.lang.ArithmeticException: divide by zero'
    elif is_before_spark_340() and not is_databricks113_or_later():
        exception_str = 'SparkArithmeticException: Division by zero'
    else:
        exception_str = 'SparkArithmeticException: [DIVIDE_BY_ZERO] Division by zero'

    assert_gpu_and_cpu_error(
        lambda spark : unary_op_df(spark, data_gen).selectExpr(
            overflow_exp.format(string_type, string_type)).collect(),
        ansi_enabled_conf,
        exception_str)

def test_cast_neg_to_decimal_err():
    # -12 cannot be represented as decimal(7,7)
    data_gen = _decimal_gen_7_7
    assert_gpu_and_cpu_error(
        lambda spark : unary_op_df(spark, data_gen).selectExpr(
            'cast(-12 as {})'.format(to_cast_string(data_gen.data_type))).collect(),
        ansi_enabled_conf,
        error_message='cannot be represented as Decimal(7, 7)')

@pytest.mark.parametrize('data_gen', _pmod_gens, ids=idfn)
def test_mod_pmod_by_zero_not_ansi(data_gen):
    string_type = to_cast_string(data_gen.data_type)
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : unary_op_df(spark, data_gen).selectExpr(
            'pmod(a, cast(0 as {}))'.format(string_type),
            'pmod(cast(-12 as {}), cast(0 as {}))'.format(string_type, string_type)),
        {'spark.sql.ansi.enabled': 'false'})
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : unary_op_df(spark, data_gen).selectExpr(
            'a % (cast(0 as {}))'.format(string_type),
            'cast(-12 as {}) % cast(0 as {})'.format(string_type, string_type)),
        {'spark.sql.ansi.enabled': 'false'})

@pytest.mark.parametrize('lhs', [byte_gen, short_gen, int_gen, long_gen, DecimalGen(6, 5),
    DecimalGen(6, 4), DecimalGen(5, 4), DecimalGen(5, 3), DecimalGen(4, 2), DecimalGen(3, -2),
    DecimalGen(16, 7), DecimalGen(19, 0), DecimalGen(30, 10)], ids=idfn)
@pytest.mark.parametrize('rhs', [byte_gen, short_gen, int_gen, long_gen, DecimalGen(6, 3),
    DecimalGen(10, -2), DecimalGen(15, 3), DecimalGen(30, 12), DecimalGen(3, -3),
    DecimalGen(27, 7), DecimalGen(20, -3)], ids=idfn)
@disable_ansi_mode
def test_mod_mixed(lhs, rhs):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : two_col_df(spark, lhs, rhs).selectExpr(f"a % b"))

# @pytest.mark.skipif(not is_databricks113_or_later() and not is_spark_340_or_later(), reason="https://github.com/NVIDIA/spark-rapids/issues/8330")
@pytest.mark.parametrize('lhs', [DecimalGen(38,0), DecimalGen(37,2), DecimalGen(38,5), DecimalGen(38,-10), DecimalGen(38,7)], ids=idfn)
@pytest.mark.parametrize('rhs', [DecimalGen(27,7), DecimalGen(30,10), DecimalGen(38,1), DecimalGen(36,0), DecimalGen(28,-7)], ids=idfn)
@disable_ansi_mode
def test_mod_mixed_decimal128(lhs, rhs):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : two_col_df(spark, lhs, rhs).selectExpr("a", "b", f"a % b"))

# Split into 4 tests to permute https://github.com/NVIDIA/spark-rapids/issues/7553 failures
@pytest.mark.parametrize('lhs', [byte_gen, short_gen, int_gen, long_gen], ids=idfn)
@pytest.mark.parametrize('rhs', [byte_gen, short_gen, int_gen, long_gen], ids=idfn)
@disable_ansi_mode
def test_pmod_mixed_numeric(lhs, rhs):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : two_col_df(spark, lhs, rhs).selectExpr(f"pmod(a, b)"))

# Decimal pmod mixed-type plans also contain CheckOverflow on Spark 3.3 non-DB.
@allow_non_gpu_conditional(is_before_spark_340() and not is_databricks_runtime(), "ProjectExec")
@allow_non_gpu("Pmod", "Cast", "PromotePrecision")
@pytest.mark.parametrize('lhs', [DecimalGen(6, 5), DecimalGen(6, 4), DecimalGen(5, 4), DecimalGen(5, 3),
    DecimalGen(4, 2), DecimalGen(3, -2), DecimalGen(16, 7), DecimalGen(19, 0), DecimalGen(30, 10)
    ], ids=idfn)
@pytest.mark.parametrize('rhs', [byte_gen, short_gen, int_gen, long_gen], ids=idfn)
@disable_ansi_mode
def test_pmod_mixed_decimal_lhs(lhs, rhs):
    assert_gpu_fallback_collect(
        lambda spark : two_col_df(spark, lhs, rhs).selectExpr(f"pmod(a, b)"),
        "Pmod")

@allow_non_gpu_conditional(is_before_spark_340() and not is_databricks_runtime(), "ProjectExec")
@allow_non_gpu("Pmod", "Cast", "PromotePrecision")
@pytest.mark.parametrize('lhs', [byte_gen, short_gen, int_gen, long_gen], ids=idfn)
@pytest.mark.parametrize('rhs', [DecimalGen(6, 3), DecimalGen(10, -2), DecimalGen(15, 3),
    DecimalGen(30, 12), DecimalGen(3, -3), DecimalGen(27, 7), DecimalGen(20, -3)
    ], ids=idfn)
@disable_ansi_mode
def test_pmod_mixed_decimal_rhs(lhs, rhs):
    assert_gpu_fallback_collect(
        lambda spark : two_col_df(spark, lhs, rhs).selectExpr(f"pmod(a, b)"),
        "Pmod")

@allow_non_gpu_conditional(is_before_spark_340() and not is_databricks_runtime(), "ProjectExec")
@allow_non_gpu("Pmod", "PromotePrecision", "Cast")
@pytest.mark.parametrize('lhs', [DecimalGen(6, 5), DecimalGen(6, 4), DecimalGen(5, 4), DecimalGen(5, 3),
    DecimalGen(4, 2), DecimalGen(3, -2), DecimalGen(16, 7), DecimalGen(19, 0), DecimalGen(30, 10)
    ], ids=idfn)
@pytest.mark.parametrize('rhs', [DecimalGen(6, 3), DecimalGen(10, -2), DecimalGen(15, 3),
    DecimalGen(30, 12), DecimalGen(3, -3), DecimalGen(27, 7), DecimalGen(20, -3)
    ], ids=idfn)
@disable_ansi_mode
def test_pmod_mixed_decimal(lhs, rhs):
    assert_gpu_fallback_collect(
        lambda spark : two_col_df(spark, lhs, rhs).selectExpr("pmod(a, b)"),
        "Pmod")

@pytest.mark.parametrize('data_gen', double_gens, ids=idfn)
def test_signum(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).selectExpr('signum(a)'))

@pytest.mark.parametrize('data_gen', numeric_gens + _arith_decimal_gens_low_precision, ids=idfn)
@disable_ansi_mode
def test_unary_minus(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).selectExpr('-a'))

@pytest.mark.parametrize('data_gen', _arith_decimal_gens_high_precision, ids=idfn)
@pytest.mark.skipif(is_scala213(), reason="Apache Spark built with Scala 2.13 produces inconsistent results at high precision (SPARK-45438)")
def test_unary_minus_decimal128(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).selectExpr('-a'))

@pytest.mark.parametrize('data_gen', _no_overflow_multiply_gens + [float_gen, double_gen] +
    _arith_decimal_gens_low_precision, ids=idfn)
def test_unary_minus_ansi_no_overflow(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).selectExpr('-a'),
            conf=ansi_enabled_conf)

@pytest.mark.parametrize('data_gen', _arith_decimal_gens_high_precision, ids=idfn)
@pytest.mark.skipif(is_scala213(), reason="Apache Spark built with Scala 2.13 produces inconsistent results at high precision (SPARK-45438)")
def test_unary_minus_ansi_no_overflow_decimal128(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).selectExpr('-a'),
            conf=ansi_enabled_conf)

@pytest.mark.parametrize('data_type,value', [
    (LongType(), LONG_MIN),
    (IntegerType(), INT_MIN),
    (ShortType(), SHORT_MIN),
    (ByteType(), BYTE_MIN)], ids=idfn)
def test_unary_minus_ansi_overflow(data_type, value):
    """
    We don't check the error messages because they are different on CPU and GPU.
    CPU: {name of the data type} overflow.
    GPU: One or more rows overflow for {name of the operation} operation.
    """
    assert_gpu_and_cpu_error(
            df_fun=lambda spark: _get_overflow_df(spark, [value], data_type, '-a').collect(),
            conf=ansi_enabled_conf,
            error_message=_arithmetic_exception_string)

# This just ends up being a pass through.  There is no good way to force
# a unary positive into a plan, because it gets optimized out, but this
# verifies that we can handle it.
@pytest.mark.parametrize('data_gen', _arith_data_gens, ids=idfn)
def test_unary_positive(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : unary_op_df(spark, data_gen).selectExpr('+a'))

@pytest.mark.parametrize('data_gen', numeric_gens + _arith_decimal_gens_low_precision, ids=idfn)
@disable_ansi_mode
def test_abs(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).selectExpr('abs(a)'))

@pytest.mark.parametrize('data_gen', _arith_decimal_gens_high_precision, ids=idfn)
@pytest.mark.skipif(is_scala213(), reason="Apache Spark built with Scala 2.13 produces inconsistent results at high precision (SPARK-45438)")
def test_abs_decimal128(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).selectExpr('abs(a)'))

# ANSI is ignored for abs prior to 3.2.0, but still okay to test it a little more.
@pytest.mark.parametrize('data_gen', _no_overflow_multiply_gens + [float_gen, double_gen] +
    _arith_decimal_gens_low_precision, ids=idfn)
def test_abs_ansi_no_overflow(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).selectExpr('abs(a)'),
            conf=ansi_enabled_conf)

@pytest.mark.parametrize('data_gen', _arith_decimal_gens_high_precision, ids=idfn)
@pytest.mark.skipif(is_scala213(), reason="Apache Spark built with Scala 2.13 produces inconsistent results at high precision")
def test_abs_ansi_no_overflow_decimal128(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).selectExpr('a','abs(a)'),
            conf=ansi_enabled_conf)

# Only run this test for Spark v3.2.0 and later to verify abs will
# throw exceptions for overflow when ANSI mode is enabled.
@pytest.mark.skipif(is_before_spark_320(), reason='SPARK-33275')
@pytest.mark.parametrize('data_type,value', [
    (LongType(), LONG_MIN),
    (IntegerType(), INT_MIN),
    (ShortType(), SHORT_MIN),
    (ByteType(), BYTE_MIN)], ids=idfn)
def test_abs_ansi_overflow(data_type, value):
    """
    We don't check the error messages because they are different on CPU and GPU.
    CPU: {name of the data type} overflow.
    GPU: One or more rows overflow for abs operation.
    """
    assert_gpu_and_cpu_error(
        df_fun=lambda spark: _get_overflow_df(spark, [value], data_type, 'abs(a)').collect(),
        conf=ansi_enabled_conf,
        error_message=_arithmetic_exception_string)

@approximate_float
@pytest.mark.parametrize('data_gen', double_gens, ids=idfn)
def test_asin(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).selectExpr('asin(a)'))

@pytest.mark.parametrize('data_gen', double_gens, ids=idfn)
def test_sqrt(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).selectExpr('sqrt(a)'))

@datagen_overrides(seed=0, reason='https://github.com/NVIDIA/spark-rapids/issues/9744')
@approximate_float
@pytest.mark.parametrize('data_gen', double_gens, ids=idfn)
def test_hypot(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : binary_op_df(spark, data_gen).selectExpr(
            'hypot(a, b)',
        ))

@pytest.mark.parametrize('data_gen', double_n_long_gens + _arith_decimal_gens_no_neg_scale + [DecimalGen(30, 15)], ids=idfn)
def test_floor(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).selectExpr('floor(a)'))

@pytest.mark.parametrize('data_gen', [long_gen] + _arith_decimal_gens_no_neg_scale, ids=idfn)
def test_floor_scale_zero(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).selectExpr('floor(a, 0)'))

@allow_non_gpu('RoundFloor', 'Cast')
@pytest.mark.parametrize('data_gen', [long_gen] + _arith_decimal_gens_no_neg_scale_38_0_overflow, ids=idfn)
def test_floor_scale_nonzero(data_gen):
    assert_gpu_fallback_collect(
            lambda spark : unary_op_df(spark, data_gen).selectExpr('floor(a, -1)'), 'RoundFloor')

@pytest.mark.parametrize('data_gen', double_n_long_gens + _arith_decimal_gens_no_neg_scale + [DecimalGen(30, 15)], ids=idfn)
def test_ceil(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).selectExpr('ceil(a)'))

@pytest.mark.parametrize('data_gen', [long_gen] + _arith_decimal_gens_no_neg_scale, ids=idfn)
def test_ceil_scale_zero(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).selectExpr('ceil(a, 0)'))

@pytest.mark.parametrize('data_gen', [_decimal_gen_36_neg5, _decimal_gen_38_neg10], ids=idfn)
def test_floor_ceil_overflow(data_gen):
    exception_type = "SparkArithmeticException" if is_before_spark_400() else \
        "pyspark.errors.exceptions.captured.ArithmeticException: [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION]"
    assert_gpu_and_cpu_error(
        lambda spark: unary_op_df(spark, data_gen).selectExpr('floor(a)').collect(),
        conf={},
        error_message=exception_type)
    assert_gpu_and_cpu_error(
        lambda spark: unary_op_df(spark, data_gen).selectExpr('ceil(a)').collect(),
        conf={},
        error_message=exception_type)

@pytest.mark.parametrize('data_gen', double_gens, ids=idfn)
def test_rint(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).selectExpr('rint(a)'))

@pytest.mark.parametrize('data_gen', int_n_long_gens, ids=idfn)
def test_shift_left(data_gen):
    string_type = to_cast_string(data_gen.data_type)
    assert_gpu_and_cpu_are_equal_collect(
            # The version of shiftLeft exposed to dataFrame does not take a column for num bits
            lambda spark : two_col_df(spark, data_gen, IntegerGen()).selectExpr(
                'shiftleft(a, cast(12 as INT))',
                'shiftleft(cast(-12 as {}), b)'.format(string_type),
                'shiftleft(cast(null as {}), b)'.format(string_type),
                'shiftleft(a, cast(null as INT))',
                'shiftleft(a, b)'))

@pytest.mark.parametrize('data_gen', int_n_long_gens, ids=idfn)
def test_shift_right(data_gen):
    string_type = to_cast_string(data_gen.data_type)
    assert_gpu_and_cpu_are_equal_collect(
            # The version of shiftRight exposed to dataFrame does not take a column for num bits
            lambda spark : two_col_df(spark, data_gen, IntegerGen()).selectExpr(
                'shiftright(a, cast(12 as INT))',
                'shiftright(cast(-12 as {}), b)'.format(string_type),
                'shiftright(cast(null as {}), b)'.format(string_type),
                'shiftright(a, cast(null as INT))',
                'shiftright(a, b)'))

@pytest.mark.parametrize('data_gen', int_n_long_gens, ids=idfn)
def test_shift_right_unsigned(data_gen):
    string_type = to_cast_string(data_gen.data_type)
    assert_gpu_and_cpu_are_equal_collect(
            # The version of shiftRightUnsigned exposed to dataFrame does not take a column for num bits
            lambda spark : two_col_df(spark, data_gen, IntegerGen()).selectExpr(
                'shiftrightunsigned(a, cast(12 as INT))',
                'shiftrightunsigned(cast(-12 as {}), b)'.format(string_type),
                'shiftrightunsigned(cast(null as {}), b)'.format(string_type),
                'shiftrightunsigned(a, cast(null as INT))',
                'shiftrightunsigned(a, b)'))

_arith_data_gens_for_round = numeric_gens + _arith_decimal_gens_no_neg_scale_38_0_overflow + [
    decimal_gen_32bit_neg_scale,
    DecimalGen(precision=15, scale=-8),
    DecimalGen(precision=30, scale=-5),
    pytest.param(_decimal_gen_36_neg5, marks=pytest.mark.skipif(
        is_spark_330_or_later(), reason='This case overflows in Spark 3.3.0+')),
    pytest.param(_decimal_gen_38_neg10, marks=pytest.mark.skipif(
        is_spark_330_or_later(), reason='This case overflows in Spark 3.3.0+'))
]

@incompat
@approximate_float
@datagen_overrides(seed=0, reason="https://github.com/NVIDIA/spark-rapids/issues/9350")
@pytest.mark.parametrize('data_gen', _arith_data_gens_for_round, ids=idfn)
@disable_ansi_mode
def test_decimal_bround(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: unary_op_df(spark, data_gen).selectExpr(
                'bround(a)',
                'bround(1.234, 2)',
                'bround(a, -1)',
                'bround(a, 1)',
                'bround(a, 2)',
                'bround(a, 10)'))

@incompat
@approximate_float
@datagen_overrides(seed=0, reason="https://github.com/NVIDIA/spark-rapids/issues/9847")
@pytest.mark.parametrize('data_gen', _arith_data_gens_for_round, ids=idfn)
@disable_ansi_mode
def test_decimal_round(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: unary_op_df(spark, data_gen).selectExpr(
                'round(a)',
                'round(1.234, 2)',
                'round(a, -1)',
                'round(a, 1)',
                'round(a, 2)',
                'round(a, 10)'))


@incompat
@approximate_float
@pytest.mark.parametrize('data_gen', [int_gen], ids=idfn)
def test_illegal_args_round(data_gen):
    def check_analysis_exception(spark, sql_text):
        try:
            gen_df(spark, [("a", data_gen), ("b", int_gen)], length=10).selectExpr(sql_text)
            raise Exception("round/bround should not plan with invalid arguments %s" % sql_text)
        except pyspark.sql.utils.AnalysisException as e:
            pass

    def doit(spark):
        check_analysis_exception(spark, "round(1.2345, b)")
        check_analysis_exception(spark, "round(a, b)")
        check_analysis_exception(spark, "bround(1.2345, b)")
        check_analysis_exception(spark, "bround(a, b)")

    with_cpu_session(lambda spark: doit(spark))
    with_gpu_session(lambda spark: doit(spark))


@incompat
@approximate_float
@disable_ansi_mode
def test_non_decimal_round_overflow():
    gen = StructGen([('byte_c', byte_gen), ('short_c', short_gen),
                     ('int_c', int_gen), ('long_c', long_gen),
                     ('float_c', float_gen), ('double_c', double_gen)], nullable=False)
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: gen_df(spark, gen).selectExpr(
            'round(byte_c, -2)', 'round(byte_c, -3)',
            'round(short_c, -4)', 'round(short_c, -5)',
            'round(int_c, -9)', 'round(int_c, -10)',
            'round(long_c, -19)', 'round(long_c, -20)',
            'round(float_c, -38)', 'round(float_c, -39)',
            'round(float_c, 38)', 'round(float_c, 39)',
            'round(double_c, -308)', 'round(double_c, -309)',
            'round(double_c, 308)', 'round(double_c, 309)',
            'bround(byte_c, -2)', 'bround(byte_c, -3)',
            'bround(short_c, -4)', 'bround(short_c, -5)',
            'bround(int_c, -9)', 'bround(int_c, -10)',
            'bround(long_c, -19)', 'bround(long_c, -20)',
            'bround(float_c, -38)', 'bround(float_c, -39)',
            'bround(float_c, 38)', 'bround(float_c, 39)',
            'bround(double_c, -308)', 'bround(double_c, -309)',
            'bround(double_c, 308)', 'bround(double_c, 309)'))

# ANSI mode tests for round/bround - no overflow cases
@pytest.mark.parametrize('data_gen', [
    ByteGen(min_val=-120, max_val=120, special_cases=[]),
    ShortGen(min_val=-30000, max_val=30000, special_cases=[]),
    IntegerGen(min_val=-2000000000, max_val=2000000000, special_cases=[]),
    LongGen(min_val=-9000000000000000000, max_val=9000000000000000000, special_cases=[])
], ids=idfn)
def test_round_ansi_no_overflow_integral(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: unary_op_df(spark, data_gen).selectExpr(
            'round(a, -1)',
            'round(a, -2)'),
        conf=ansi_enabled_conf)

@pytest.mark.parametrize('data_gen', [
    ByteGen(min_val=-120, max_val=120, special_cases=[]),
    ShortGen(min_val=-30000, max_val=30000, special_cases=[]),
    IntegerGen(min_val=-2000000000, max_val=2000000000, special_cases=[]),
    LongGen(min_val=-9000000000000000000, max_val=9000000000000000000, special_cases=[])
], ids=idfn)
def test_bround_ansi_no_overflow_integral(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: unary_op_df(spark, data_gen).selectExpr(
            'bround(a, -1)',
            'bround(a, -2)'),
        conf=ansi_enabled_conf)

# ANSI mode tests - overflow cases
@pytest.mark.skipif(is_before_spark_340(), reason='ANSI mode for round/bround is only supported in Spark 3.4.0+')
@pytest.mark.parametrize('data_gen,scale', [
    (ByteGen(min_val=125, max_val=127, special_cases=[]), -1),    # 125-127 overflow at scale -1 for HALF_UP
    (ShortGen(min_val=32760, max_val=32767, special_cases=[]), -1),  # near Short.MAX_VALUE
    (IntegerGen(min_val=2147483640, max_val=2147483647, special_cases=[]), -1),  # near Int.MAX_VALUE
    (LongGen(min_val=9223372036854775800, max_val=9223372036854775807, special_cases=[]), -1),  # near Long.MAX_VALUE
], ids=idfn)
def test_round_ansi_overflow_integral(data_gen, scale):
    assert_gpu_and_cpu_error(
        lambda spark: unary_op_df(spark, data_gen).selectExpr(f'round(a, {scale})').collect(),
        conf=ansi_enabled_conf,
        error_message="ArithmeticException")

@pytest.mark.skipif(is_before_spark_340(), reason='ANSI mode for round/bround is only supported in Spark 3.4.0+')
@pytest.mark.parametrize('data_gen,scale', [
    (ByteGen(min_val=125, max_val=127, special_cases=[]), -1),    # 126-127 overflow at scale -1 for HALF_EVEN
    (ShortGen(min_val=32765, max_val=32767, special_cases=[]), -1),  # near Short.MAX_VALUE
    (IntegerGen(min_val=2147483645, max_val=2147483647, special_cases=[]), -1),  # near Int.MAX_VALUE
    (LongGen(min_val=9223372036854775805, max_val=9223372036854775807, special_cases=[]), -1),  # near Long.MAX_VALUE
], ids=idfn)
def test_bround_ansi_overflow_integral(data_gen, scale):
    assert_gpu_and_cpu_error(
        lambda spark: unary_op_df(spark, data_gen).selectExpr(f'bround(a, {scale})').collect(),
        conf=ansi_enabled_conf,
        error_message="ArithmeticException")

@approximate_float
@pytest.mark.parametrize('data_gen', _math_unary_input_gens, ids=idfn)
def test_cbrt(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).selectExpr('cbrt(a)'),
            conf=_math_unary_conf)

@pytest.mark.parametrize('data_gen', integral_gens, ids=idfn)
def test_bit_and(data_gen):
    string_type = to_cast_string(data_gen.data_type)
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : binary_op_df(spark, data_gen).selectExpr(
                'a & cast(100 as {})'.format(string_type),
                'cast(-12 as {}) & b'.format(string_type),
                'cast(null as {}) & a'.format(string_type),
                'b & cast(null as {})'.format(string_type),
                'a & b'))

@pytest.mark.parametrize('data_gen', integral_gens, ids=idfn)
def test_bit_or(data_gen):
    string_type = to_cast_string(data_gen.data_type)
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : binary_op_df(spark, data_gen).selectExpr(
                'a | cast(100 as {})'.format(string_type),
                'cast(-12 as {}) | b'.format(string_type),
                'cast(null as {}) | a'.format(string_type),
                'b | cast(null as {})'.format(string_type),
                'a | b'))

@pytest.mark.parametrize('data_gen', integral_gens, ids=idfn)
def test_bit_xor(data_gen):
    string_type = to_cast_string(data_gen.data_type)
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : binary_op_df(spark, data_gen).selectExpr(
                'a ^ cast(100 as {})'.format(string_type),
                'cast(-12 as {}) ^ b'.format(string_type),
                'cast(null as {}) ^ a'.format(string_type),
                'b ^ cast(null as {})'.format(string_type),
                'a ^ b'))

@pytest.mark.parametrize('data_gen', integral_gens, ids=idfn)
def test_bit_not(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).selectExpr('~a'))


# SPARK-48128: bit_count(boolean) CPU whole-stage codegen is broken before Spark 4.0.0 and only
# fails when spark.testing disables the codegen->interpreted fallback (see #15022, #15097).
@pytest.mark.parametrize('data_gen', integral_gens + [pytest.param(boolean_gen,
    marks=pytest.mark.skipif(is_before_spark_400() and is_spark_testing_enabled(),
        reason='SPARK-48128 bit_count(boolean) codegen bug exposed by spark.testing; fixed in Spark 4.0.0'))],
    ids=idfn)
def test_bit_count(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: unary_op_df(spark, data_gen).selectExpr('bit_count(a)'))

@approximate_float
@pytest.mark.parametrize('data_gen', double_gens, ids=idfn)
def test_radians(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).selectExpr('radians(a)'))

# Spark's degrees will overflow on large values in jdk 8 or below
@approximate_float
@pytest.mark.skipif(get_java_major_version() <= 8, reason="requires jdk 9 or higher")
@pytest.mark.parametrize('data_gen', double_gens, ids=idfn)
def test_degrees(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).selectExpr('degrees(a)'))

@approximate_float
@pytest.mark.parametrize('data_gen', [float_gen], ids=idfn)
def test_degrees_small(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).selectExpr('degrees(a)'))

@pytest.mark.parametrize('data_gen', [long_gen], ids=idfn)
def test_bin_long(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).selectExpr('bin(a)'))

@pytest.mark.parametrize('data_gen', [long_gen, StringGen(),
    StringGen('[\x00-\xff]{0,10}'), BinaryGen()], ids=idfn)
def test_hex(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).selectExpr('hex(a)'))

@approximate_float
@pytest.mark.parametrize('data_gen', double_gens, ids=idfn)
def test_cos(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).selectExpr('cos(a)'))

@approximate_float
@pytest.mark.parametrize('data_gen', double_gens, ids=idfn)
def test_acos(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).selectExpr('acos(a)'))

@approximate_float
@pytest.mark.parametrize('data_gen', double_gens, ids=idfn)
def test_cosh(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).selectExpr('cosh(a)'))

@approximate_float
@pytest.mark.parametrize('data_gen', double_gens, ids=idfn)
def test_acosh(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).selectExpr('acosh(a)'))

# The default approximate is 1e-6 or 1 in a million
# in some cases we need to adjust this because the algorithm is different
@approximate_float(rel=1e-4, abs=1e-12)
# Because spark will overflow on large exponents drop to something well below
# what it fails at, note this is binary exponent, not base 10
@pytest.mark.parametrize('data_gen', [DoubleGen(min_exp=-20, max_exp=20)], ids=idfn)
def test_columnar_acosh_improved(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).selectExpr('acosh(a)'),
            {'spark.rapids.sql.improvedFloatOps.enabled': 'true'})

@approximate_float
@pytest.mark.parametrize('data_gen', double_gens, ids=idfn)
def test_sin(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).selectExpr('sin(a)'))

@approximate_float
@pytest.mark.parametrize('data_gen', double_gens, ids=idfn)
def test_sinh(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).selectExpr('sinh(a)'))

@approximate_float
@pytest.mark.parametrize('data_gen', double_gens, ids=idfn)
def test_asin(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).selectExpr('asin(a)'))

@approximate_float
@pytest.mark.parametrize('data_gen', _math_unary_input_gens, ids=idfn)
def test_asinh(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).selectExpr('asinh(a)'),
            conf=_math_unary_conf)

# The default approximate is 1e-6 or 1 in a million
# in some cases we need to adjust this because the algorithm is different
@approximate_float(rel=1e-4, abs=1e-12)
# Because spark will overflow on large exponents drop to something well below
# what it fails at, note this is binary exponent, not base 10
@pytest.mark.parametrize('data_gen', [DoubleGen(min_exp=-20, max_exp=20)], ids=idfn)
def test_columnar_asinh_improved(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).selectExpr('asinh(a)'),
            {'spark.rapids.sql.improvedFloatOps.enabled': 'true'})

@approximate_float
@pytest.mark.parametrize('data_gen', double_gens, ids=idfn)
def test_tan(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).selectExpr('tan(a)'))

@approximate_float
@pytest.mark.parametrize('data_gen', double_gens, ids=idfn)
def test_atan(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).selectExpr('atan(a)'))

@approximate_float
@pytest.mark.parametrize('data_gen', _math_unary_input_gens, ids=idfn)
def test_atanh(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).selectExpr('atanh(a)'),
            conf=_math_unary_conf)

@approximate_float
@pytest.mark.parametrize('data_gen', double_gens, ids=idfn)
def test_tanh(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).selectExpr('tanh(a)'))

@approximate_float
@pytest.mark.parametrize('data_gen', double_gens, ids=idfn)
def test_cot(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).selectExpr('cot(a)'))

@approximate_float
@pytest.mark.parametrize('data_gen', double_gens, ids=idfn)
def test_exp(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).selectExpr('exp(a)'))

@approximate_float
@pytest.mark.parametrize('data_gen', double_gens, ids=idfn)
def test_expm1(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).selectExpr('expm1(a)'))

@approximate_float
@pytest.mark.parametrize('data_gen', double_gens, ids=idfn)
def test_log(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).selectExpr('log(a)'))

@approximate_float
@pytest.mark.parametrize('data_gen', double_gens, ids=idfn)
def test_log1p(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).selectExpr('log1p(a)'))

@approximate_float
@pytest.mark.parametrize('data_gen', double_gens, ids=idfn)
def test_log2(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).selectExpr('log2(a)'))

@approximate_float
@pytest.mark.parametrize('data_gen', double_gens, ids=idfn)
def test_log10(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).selectExpr('log10(a)'))

@approximate_float
def test_logarithm():
    # For the 'b' field include a lot more values that we would expect customers to use as a part of a log
    data_gen = [('a', DoubleGen()),('b', DoubleGen().with_special_case(lambda rand: float(rand.randint(-16, 16)), weight=100.0))]
    string_type = 'DOUBLE'
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : gen_df(spark, data_gen).selectExpr(
                'log(a, cast(100 as {}))'.format(string_type),
                'log(cast(-12 as {}), b)'.format(string_type),
                'log(cast(null as {}), b)'.format(string_type),
                'log(a, cast(null as {}))'.format(string_type),
                'log(a, b)'))

@approximate_float
def test_scalar_pow():
    # For the 'b' field include a lot more values that we would expect customers to use as a part of a pow
    data_gen = [('a', DoubleGen()),('b', DoubleGen().with_special_case(lambda rand: float(rand.randint(-16, 16)), weight=100.0))]
    string_type = 'DOUBLE'
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : gen_df(spark, data_gen).selectExpr(
                'pow(a, cast(7 as {}))'.format(string_type),
                'pow(cast(-12 as {}), b)'.format(string_type),
                'pow(cast(null as {}), a)'.format(string_type),
                'pow(b, cast(null as {}))'.format(string_type)))

@approximate_float
@pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/89')
@pytest.mark.parametrize('data_gen', double_gens, ids=idfn)
def test_columnar_pow(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : binary_op_df(spark, data_gen).selectExpr('pow(a, b)'))

@pytest.mark.parametrize('data_gen', all_basic_gens + _arith_decimal_gens, ids=idfn)
def test_least(data_gen):
    num_cols = 20
    s1 = with_cpu_session(
        lambda spark: gen_scalar(data_gen, force_no_nulls=not isinstance(data_gen, NullGen)))
    # we want lots of nulls
    gen = StructGen([('_c' + str(x), data_gen.copy_special_case(None, weight=100.0))
        for x in range(0, num_cols)], nullable=False)

    command_args = [f.col('_c' + str(x)) for x in range(0, num_cols)]
    command_args.append(s1)
    data_type = data_gen.data_type
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : gen_df(spark, gen).select(
                f.least(*command_args)))

@pytest.mark.parametrize('data_gen', all_basic_gens + _arith_decimal_gens, ids=idfn)
def test_greatest(data_gen):
    num_cols = 20
    s1 = with_cpu_session(
        lambda spark: gen_scalar(data_gen, force_no_nulls=not isinstance(data_gen, NullGen)))
    # we want lots of nulls
    gen = StructGen([('_c' + str(x), data_gen.copy_special_case(None, weight=100.0))
        for x in range(0, num_cols)], nullable=False)
    command_args = [f.col('_c' + str(x)) for x in range(0, num_cols)]
    command_args.append(s1)
    data_type = data_gen.data_type
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : gen_df(spark, gen).select(
                f.greatest(*command_args)))


def _test_div_by_zero(ansi_mode, expr, is_lit=False):
    ansi_conf = {'spark.sql.ansi.enabled': ansi_mode == 'ansi'}
    data_gen = lambda spark: two_col_df(spark, IntegerGen(), IntegerGen(min_val=0, max_val=0), length=1)
    div_by_zero_func = lambda spark: data_gen(spark).selectExpr(expr)
    if is_before_spark_320():
        err_message = 'java.lang.ArithmeticException: divide by zero'
    elif is_before_spark_340() and not is_databricks113_or_later():
        err_message = 'SparkArithmeticException: Division by zero'
    else:
        exception_type = 'SparkArithmeticException: ' \
            if not is_lit else "pyspark.errors.exceptions.captured.ArithmeticException: "
        err_message = exception_type + "[DIVIDE_BY_ZERO] Division by zero"

    if ansi_mode == 'ansi':
        assert_gpu_and_cpu_error(df_fun=lambda spark: div_by_zero_func(spark).collect(),
                                 conf=ansi_conf,
                                 error_message=err_message)
    else:
        assert_gpu_and_cpu_are_equal_collect(div_by_zero_func, ansi_conf)


@pytest.mark.parametrize('expr', ['a/0', 'a/b'])
@pytest.mark.parametrize('ansi', [True, False])
def test_div_by_zero(expr, ansi):
    _test_div_by_zero(ansi_mode=ansi, expr=expr)

# We want to test literals separate from expressions because Spark 3.4 throws different exceptions
@pytest.mark.parametrize('ansi', [True, False])
def test_div_by_zero_literal(ansi):
    _test_div_by_zero(ansi_mode=ansi, expr='1/0', is_lit=True)

@pytest.mark.parametrize('ansi_on', [True, False])
def test_null_div_by_zero(ansi_on):
    null_dividend_gen = SetValuesGen(IntegerType(), [None])
    zero_int_gen = IntegerGen(min_val=0, max_val=0)
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: two_col_df(spark, null_dividend_gen, zero_int_gen).selectExpr(
            'null/0', 'null/b', 'a/b', 'a/0'),
        conf={'spark.sql.ansi.enabled': ansi_on})

def _get_div_overflow_df(spark, expr):
    return spark.createDataFrame(
        [(LONG_MIN, -1)],
        ['a', 'b']
    ).selectExpr(expr)

def _div_overflow_exception_when(expr, ansi_enabled, is_lit=False):
    ansi_conf = {'spark.sql.ansi.enabled': ansi_enabled}
    if ansi_enabled:
        assert_gpu_and_cpu_error(
            df_fun=lambda spark: _get_div_overflow_df(spark, expr).collect(),
            conf=ansi_conf,
            error_message='Overflow in integral divide')
    else:
        assert_gpu_and_cpu_are_equal_collect(
            func=lambda spark: _get_div_overflow_df(spark, expr),
            conf=ansi_conf)

# Only run this test for Spark v3.2.0 and later to verify IntegralDivide will
# throw exceptions for overflow when ANSI mode is enabled.
@pytest.mark.skipif(is_before_spark_320(), reason='https://github.com/apache/spark/pull/32260')
@pytest.mark.parametrize('expr', ['a DIV CAST(-1 AS INT)', 'a DIV b'])
@pytest.mark.parametrize('ansi_enabled', [False, True])
def test_div_overflow_exception_when_ansi(expr, ansi_enabled):
    _div_overflow_exception_when(expr, ansi_enabled)

# Only run this test for Spark v3.2.0 and later to verify IntegralDivide will
# throw exceptions for overflow when ANSI mode is enabled.
# We have split this test from test_div_overflow_exception_when_ansi because Spark 3.4
# throws a different exception for literals
@pytest.mark.skipif(is_before_spark_320(), reason='https://github.com/apache/spark/pull/32260')
@pytest.mark.parametrize('expr', ['CAST(-9223372036854775808L as LONG) DIV -1'])
@pytest.mark.parametrize('ansi_enabled', [False, True])
def test_div_overflow_exception_when_ansi_literal(expr, ansi_enabled):
    _div_overflow_exception_when(expr, ansi_enabled, is_lit=True)

# Only run this test before Spark v3.2.0 to verify IntegralDivide will NOT
# throw exceptions for overflow even ANSI mode is enabled.
@pytest.mark.skipif(not is_before_spark_320(), reason='https://github.com/apache/spark/pull/32260')
@pytest.mark.parametrize('expr', ['CAST(-9223372036854775808L as LONG) DIV -1', 'a DIV CAST(-1 AS INT)', 'a DIV b'])
@pytest.mark.parametrize('ansi_enabled', ['false', 'true'])
def test_div_overflow_no_exception_when_ansi(expr, ansi_enabled):
    assert_gpu_and_cpu_are_equal_collect(
        func=lambda spark: _get_div_overflow_df(spark, expr),
        conf={'spark.sql.ansi.enabled': ansi_enabled})

_data_type_expr_for_add_overflow = [
    ([127], ByteType(), 'a + 1Y'),
    ([-128], ByteType(), '-1Y + a'),
    ([32767], ShortType(), 'a + 1S'),
    ([-32768], ShortType(), '-1S + a'),
    ([2147483647], IntegerType(), 'a + 1'),
    ([-2147483648], IntegerType(), '-1 + a'),
    ([9223372036854775807],  LongType(), 'a + 1L'),
    ([-9223372036854775808], LongType(), '-1L + a'),
    ([3.4028235E38],  FloatType(), 'a + a'),
    ([-3.4028235E38], FloatType(), 'a + a'),
    ([1.7976931348623157E308],  DoubleType(), 'a + a'),
    ([-1.7976931348623157E308], DoubleType(), 'a + a'),
    ([Decimal('-' + '9' * 38)], DecimalType(38,0), 'a + -1'),
    ([Decimal('-' + '9' * 38)], DecimalType(38,0), 'a + a'),
    ([Decimal('9' * 38)], DecimalType(38,0), 'a + 1'),
    ([Decimal('9' * 38)], DecimalType(38,0), 'a + 1')]

@pytest.mark.parametrize('data,tp,expr', _data_type_expr_for_add_overflow, ids=idfn)
def test_add_overflow_with_ansi_enabled(data, tp, expr):
    if isinstance(tp, IntegralType):
        assert_gpu_and_cpu_error(
            lambda spark: _get_overflow_df(spark, data, tp, expr).collect(),
            conf=ansi_enabled_conf,
            error_message=_arithmetic_exception_string)
    elif isinstance(tp, DecimalType):
        assert_gpu_and_cpu_error(
            lambda spark: _get_overflow_df(spark, data, tp, expr).collect(),
            conf=ansi_enabled_conf,
            error_message='')
    else:
        assert_gpu_and_cpu_are_equal_collect(
            func=lambda spark: _get_overflow_df(spark, data, tp, expr),
            conf=ansi_enabled_conf)


_data_type_expr_for_sub_overflow = [
    ([-128], ByteType(), 'a - 1Y'),
    ([-32768], ShortType(), 'a -1S'),
    ([-2147483648], IntegerType(), 'a - 1'),
    ([-9223372036854775808], LongType(), 'a - 1L'),
    ([-3.4028235E38], FloatType(), 'a - cast(1.0 as float)'),
    ([-1.7976931348623157E308], DoubleType(), 'a - 1.0'),
    ([Decimal('-' + '9' * 38)], DecimalType(38,0), 'a - 1'),
    ([Decimal('-' + '9' * 38)], DecimalType(38,0), 'a - (-a)')
    ]

@pytest.mark.parametrize('data,tp,expr', _data_type_expr_for_sub_overflow, ids=idfn)
def test_subtraction_overflow_with_ansi_enabled(data, tp, expr):
    if isinstance(tp, IntegralType):
        assert_gpu_and_cpu_error(
            lambda spark: _get_overflow_df(spark, data, tp, expr).collect(),
            conf=ansi_enabled_conf,
            error_message='SparkArithmeticException' \
            if is_before_spark_400() else "pyspark.errors.exceptions.captured.ArithmeticException:")
    elif isinstance(tp, DecimalType):
        assert_gpu_and_cpu_error(
            lambda spark: _get_overflow_df(spark, data, tp, expr).collect(),
            conf=ansi_enabled_conf,
            error_message='')
    else:
        assert_gpu_and_cpu_are_equal_collect(
            func=lambda spark: _get_overflow_df(spark, data, tp, expr),
            conf=ansi_enabled_conf)


@pytest.mark.parametrize('ansi_enabled', ['false', 'true'])
def test_unary_minus_day_time_interval(ansi_enabled):
    DAY_TIME_GEN_NO_OVER_FLOW = DayTimeIntervalGen(min_value=timedelta(days=-2000*365), max_value=timedelta(days=3000*365))
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: unary_op_df(spark, DAY_TIME_GEN_NO_OVER_FLOW).selectExpr('-a'),
        conf={'spark.sql.ansi.enabled': ansi_enabled})

@pytest.mark.parametrize('ansi_enabled', ['false', 'true'])
def test_unary_minus_ansi_overflow_day_time_interval(ansi_enabled):
    """
    We don't check the error messages because they are different on CPU and GPU.
    CPU: long overflow.
    GPU: One or more rows overflow for minus operation.
    """
    assert_gpu_and_cpu_error(
        df_fun=lambda spark: _get_overflow_df(spark, [timedelta(microseconds=LONG_MIN)], DayTimeIntervalType(), '-a').collect(),
        conf={'spark.sql.ansi.enabled': ansi_enabled},
        error_message='SparkArithmeticException' if is_before_spark_400() else "ArithmeticException")

@pytest.mark.parametrize('ansi_enabled', ['false', 'true'])
def test_abs_ansi_no_overflow_day_time_interval(ansi_enabled):
    DAY_TIME_GEN_NO_OVER_FLOW = DayTimeIntervalGen(min_value=timedelta(days=-2000*365), max_value=timedelta(days=3000*365))
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: unary_op_df(spark, DAY_TIME_GEN_NO_OVER_FLOW).selectExpr('abs(a)'),
        conf={'spark.sql.ansi.enabled': ansi_enabled})

@pytest.mark.parametrize('ansi_enabled', ['false', 'true'])
def test_abs_ansi_overflow_day_time_interval(ansi_enabled):
    """
    Check the error message only when ANSI mode is false because they are different on CPU and GPU.
    CPU: long overflow.
    GPU: One or more rows overflow for abs operation.
    """
    assert_gpu_and_cpu_error(
        df_fun=lambda spark: _get_overflow_df(spark, [timedelta(microseconds=LONG_MIN)], DayTimeIntervalType(), 'abs(a)').collect(),
        conf={'spark.sql.ansi.enabled': ansi_enabled},
        error_message='' if ansi_enabled else 'SparkArithmeticException')

@pytest.mark.parametrize('ansi_enabled', ['false', 'true'])
def test_addition_day_time_interval(ansi_enabled):
    DAY_TIME_GEN_NO_OVER_FLOW = DayTimeIntervalGen(min_value=timedelta(days=-2000*365), max_value=timedelta(days=3000*365))
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: two_col_df(spark, DAY_TIME_GEN_NO_OVER_FLOW, DAY_TIME_GEN_NO_OVER_FLOW).select(
            f.col('a') + f.col('b')),
        conf={'spark.sql.ansi.enabled': ansi_enabled})

@pytest.mark.parametrize('ansi_enabled', ['false', 'true'])
def test_add_overflow_with_ansi_enabled_day_time_interval(ansi_enabled):
    assert_gpu_and_cpu_error(
        df_fun=lambda spark: spark.createDataFrame(
            SparkContext.getOrCreate().parallelize([(timedelta(microseconds=LONG_MAX), timedelta(microseconds=10)),]),
            StructType([StructField('a', DayTimeIntervalType()), StructField('b', DayTimeIntervalType())])
        ).selectExpr('a + b').collect(),
        conf={'spark.sql.ansi.enabled': ansi_enabled},
        error_message=_arithmetic_exception_string)

@pytest.mark.parametrize('ansi_enabled', ['false', 'true'])
def test_subtraction_day_time_interval(ansi_enabled):
    DAY_TIME_GEN_NO_OVER_FLOW = DayTimeIntervalGen(min_value=timedelta(days=-2000*365), max_value=timedelta(days=3000*365))
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: two_col_df(spark, DAY_TIME_GEN_NO_OVER_FLOW, DAY_TIME_GEN_NO_OVER_FLOW).select(
            f.col('a') - f.col('b')),
        conf={'spark.sql.ansi.enabled': ansi_enabled})

@pytest.mark.parametrize('ansi_enabled', ['false', 'true'])
def test_subtraction_overflow_with_ansi_enabled_day_time_interval(ansi_enabled):
    assert_gpu_and_cpu_error(
        df_fun=lambda spark: spark.createDataFrame(
            SparkContext.getOrCreate().parallelize([(timedelta(microseconds=LONG_MIN), timedelta(microseconds=10)),]),
            StructType([StructField('a', DayTimeIntervalType()), StructField('b', DayTimeIntervalType())])
        ).selectExpr('a - b').collect(),
        conf={'spark.sql.ansi.enabled': ansi_enabled},
        error_message='SparkArithmeticException' if is_before_spark_400() else "ArithmeticException")

def test_unary_positive_day_time_interval():
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: unary_op_df(spark, DayTimeIntervalGen()).selectExpr('+a'))

@pytest.mark.parametrize('data_gen', _no_overflow_multiply_gens_for_fallback + [DoubleGen(min_exp=-3, max_exp=5, special_cases=[0.0])], ids=idfn)
def test_day_time_interval_multiply_number(data_gen):
    gen_list = [('_c1', DayTimeIntervalGen(min_value=timedelta(seconds=-20 * 86400), max_value=timedelta(seconds=20 * 86400))),
                ('_c2', data_gen)]
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: gen_df(spark, gen_list).selectExpr("_c1 * _c2"))

@pytest.mark.parametrize('data_gen', _no_overflow_multiply_gens_for_fallback + [DoubleGen(min_exp=0, max_exp=5, special_cases=[])], ids=idfn)
def test_day_time_interval_division_number_no_overflow1(data_gen):
    gen_list = [('_c1', DayTimeIntervalGen(min_value=timedelta(seconds=-5000 * 365 * 86400), max_value=timedelta(seconds=5000 * 365 * 86400))),
                ('_c2', data_gen)]
    assert_gpu_and_cpu_are_equal_collect(
        # avoid dividing by 0
        lambda spark: gen_df(spark, gen_list).selectExpr("_c1 / case when _c2 = 0 then cast(1 as {}) else _c2 end".format(to_cast_string(data_gen.data_type))))

@pytest.mark.parametrize('data_gen', _no_overflow_multiply_gens_for_fallback + [DoubleGen(min_exp=-5, max_exp=0, special_cases=[])], ids=idfn)
def test_day_time_interval_division_number_no_overflow2(data_gen):
    gen_list = [('_c1', DayTimeIntervalGen(min_value=timedelta(seconds=-20 * 86400), max_value=timedelta(seconds=20 * 86400))),
                ('_c2', data_gen)]
    assert_gpu_and_cpu_are_equal_collect(
        # avoid dividing by 0
        lambda spark: gen_df(spark, gen_list).selectExpr("_c1 / case when _c2 = 0 then cast(1 as {}) else _c2 end".format(to_cast_string(data_gen.data_type))))

@pytest.mark.parametrize('data_gen', _no_overflow_multiply_gens_for_fallback + [DoubleGen(min_exp=-3, max_exp=5, special_cases=[0.0])], ids=idfn)
def test_year_month_interval_multiply_number(data_gen):
    # Cast result to BIGINT (months count) so the final schema is LongType.
    # YearMonthIntervalType schema is not deserializable by PySpark 3.3.0
    # client-side, and CAST(YM AS STRING) is not GPU-supported (would force
    # the multiply onto CPU). CAST(YM AS BIGINT) is GPU-supported.
    gen_list = [('_c2', data_gen)]
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: gen_df(spark, gen_list).selectExpr(
            "CAST(INTERVAL '0-3' YEAR TO MONTH * _c2 AS BIGINT)",
            "CAST(INTERVAL '5-7' YEAR TO MONTH * _c2 AS BIGINT)",
            "CAST(_c2 * INTERVAL '100-0' YEAR TO MONTH AS BIGINT)"))

@pytest.mark.parametrize('data_gen', _no_overflow_multiply_gens_for_fallback + [DoubleGen(min_exp=0, max_exp=5, special_cases=[])], ids=idfn)
def test_year_month_interval_division_number_no_overflow(data_gen):
    # Cast result to BIGINT (same reason as test_year_month_interval_multiply_number).
    gen_list = [('_c2', data_gen)]
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: gen_df(spark, gen_list).selectExpr(
            "CAST(INTERVAL '5-3' YEAR TO MONTH / _c2 AS BIGINT)",
            "CAST(INTERVAL '100-0' YEAR TO MONTH / _c2 AS BIGINT)"))

def _get_overflow_df_1col(spark, data_type, value, expr):
    return spark.createDataFrame(
        SparkContext.getOrCreate().parallelize([value]),
        StructType([
            StructField('a', data_type)
        ])
    ).selectExpr(expr)

def _get_overflow_df_2cols(spark, data_types, values, expr):
    return spark.createDataFrame(
        SparkContext.getOrCreate().parallelize([values]),
        StructType([
            StructField('a', data_types[0]),
            StructField('b', data_types[1])
        ])
    ).selectExpr(expr)

# test interval division overflow, such as interval / 0, Long.MinValue / -1 ...
@pytest.mark.parametrize('data_type,value_pair', [
    (LongType(), [MIN_DAY_TIME_INTERVAL, -1]),
    (IntegerType(), [timedelta(microseconds=LONG_MIN), -1])
], ids=idfn)
def test_day_time_interval_division_overflow(data_type, value_pair):
    exception_message = "SparkArithmeticException: Overflow in integral divide." \
        if is_before_spark_340() and not is_databricks113_or_later() else \
        "SparkArithmeticException: [ARITHMETIC_OVERFLOW] Overflow in integral divide." if is_before_spark_400() else \
            "ArithmeticException: [ARITHMETIC_OVERFLOW] Overflow in integral divide."
    assert_gpu_and_cpu_error(
        df_fun=lambda spark: _get_overflow_df_2cols(spark, [DayTimeIntervalType(), data_type], value_pair, 'a / b').collect(),
        conf={},
        error_message=exception_message)

@pytest.mark.parametrize('data_type,value_pair', [
    (FloatType(), [MAX_DAY_TIME_INTERVAL, 0.1]),
    (DoubleType(), [MAX_DAY_TIME_INTERVAL, 0.1]),
    (FloatType(), [MIN_DAY_TIME_INTERVAL, 0.1]),
    (DoubleType(), [MIN_DAY_TIME_INTERVAL, 0.1]),
], ids=idfn)
def test_day_time_interval_division_round_overflow(data_type, value_pair):
    assert_gpu_and_cpu_error(
        df_fun=lambda spark: _get_overflow_df_2cols(spark, [DayTimeIntervalType(), data_type], value_pair, 'a / b').collect(),
        conf={},
        error_message='java.lang.ArithmeticException')

@pytest.mark.parametrize('data_type,value_pair', [
    (ByteType(), [timedelta(seconds=1), 0]),
    (ShortType(), [timedelta(seconds=1), 0]),
    (IntegerType(), [timedelta(seconds=1), 0]),
    (LongType(), [timedelta(seconds=1), 0]),
    (FloatType(), [timedelta(seconds=1), 0.0]),
    (FloatType(), [timedelta(seconds=1), -0.0]),
    (DoubleType(), [timedelta(seconds=1), 0.0]),
    (DoubleType(), [timedelta(seconds=1), -0.0]),
    (FloatType(), [timedelta(seconds=0), 0.0]),   # 0 / 0 = NaN
    (DoubleType(), [timedelta(seconds=0), 0.0]),  # 0 / 0 = NaN
], ids=idfn)
def test_day_time_interval_divided_by_zero(data_type, value_pair):
    exception_message = "SparkArithmeticException: Division by zero." \
        if is_before_spark_340() and not is_databricks113_or_later() else \
        "SparkArithmeticException: [INTERVAL_DIVIDED_BY_ZERO] Division by zero" if is_before_spark_400() else \
        "ArithmeticException: [INTERVAL_DIVIDED_BY_ZERO] Division by zero"
    assert_gpu_and_cpu_error(
        df_fun=lambda spark: _get_overflow_df_2cols(spark, [DayTimeIntervalType(), data_type], value_pair, 'a / b').collect(),
        conf={},
        error_message=exception_message)

@pytest.mark.parametrize('zero_literal', ['0', '0.0f', '-0.0f'], ids=idfn)
def test_day_time_interval_divided_by_zero_scalar(zero_literal):
    exception_message = "SparkArithmeticException: Division by zero." \
        if is_before_spark_340() and not is_databricks113_or_later() else \
        "SparkArithmeticException: [INTERVAL_DIVIDED_BY_ZERO] Division by zero." if is_before_spark_400() else \
            "ArithmeticException: [INTERVAL_DIVIDED_BY_ZERO] Division by zero"
    assert_gpu_and_cpu_error(
        df_fun=lambda spark: _get_overflow_df_1col(spark, DayTimeIntervalType(), [timedelta(seconds=1)], 'a / ' + zero_literal).collect(),
        conf={},
        error_message=exception_message)

@pytest.mark.parametrize('data_type,value', [
    (ByteType(), 0),
    (ShortType(), 0),
    (IntegerType(), 0),
    (LongType(), 0),
    (FloatType(), 0.0),
    (FloatType(), -0.0),
    (DoubleType(), 0.0),
    (DoubleType(), -0.0),
], ids=idfn)
def test_day_time_interval_scalar_divided_by_zero(data_type, value):
    exception_message = "SparkArithmeticException: Division by zero." \
        if is_before_spark_340() and not is_databricks113_or_later() else \
        "SparkArithmeticException: [INTERVAL_DIVIDED_BY_ZERO] Division by zero." if is_before_spark_400() else \
            "ArithmeticException: [INTERVAL_DIVIDED_BY_ZERO] Division by zero"
    assert_gpu_and_cpu_error(
        df_fun=lambda spark: _get_overflow_df_1col(spark, data_type, [value], 'INTERVAL 1 SECOND / a').collect(),
        conf={},
        error_message=exception_message)

@pytest.mark.parametrize('data_type,value_pair', [
    (FloatType(), [timedelta(seconds=1), float('NaN')]),
    (DoubleType(), [timedelta(seconds=1), float('NaN')]),
], ids=idfn)
def test_day_time_interval_division_nan(data_type, value_pair):
    assert_gpu_and_cpu_error(
        df_fun=lambda spark: _get_overflow_df_2cols(spark, [DayTimeIntervalType(), data_type], value_pair, 'a / b').collect(),
        conf={},
        error_message='java.lang.ArithmeticException')


@pytest.mark.parametrize('op_str', ['+', '- -', '*'], ids=['Add', 'Subtract', 'Multiply'])
def test_decimal_nullability_of_overflow_for_binary_ops(op_str):
    def test_func(spark):
        return spark.range(20).selectExpr("CAST(id as DECIMAL(38,0)) as dec_num")\
            .selectExpr("99999999999999999999999999999999999991" + op_str + "dec_num as dec_over")
    # Have to disable ansi, otherwise both CPU and GPU will throw exceptions, which is
    # not we want for this test.
    conf_no_ansi = {"spark.sql.ansi.enabled": "false"}
    assert_gpu_and_cpu_are_equal_collect(test_func, conf = conf_no_ansi)


# Spark 3.3 non-DB represents try_* arithmetic with TryEval. TryEval needs its
# child expression tree on CPU to catch CPU exceptions and convert them to null,
# so the enclosing ProjectExec falls back on those builds.
@allow_non_gpu_conditional(is_before_spark_340() and not is_databricks_runtime(), "ProjectExec")
@allow_non_gpu('Add')
@pytest.mark.parametrize('data_gen', integral_gens, ids=idfn)
def test_try_add_fallback_to_cpu(data_gen):
    assert_gpu_fallback_collect(
        lambda spark: binary_op_df(spark, data_gen).selectExpr(
            "try_add(a, b) as result"), "Add")

@allow_non_gpu_conditional(is_before_spark_340() and not is_databricks_runtime(), "ProjectExec")
@allow_non_gpu('Divide', 'Cast')
@pytest.mark.parametrize('data_gen', numeric_gens, ids=idfn)
def test_try_divide_fallback_to_cpu(data_gen):
    assert_gpu_fallback_collect(
        lambda spark: binary_op_df(spark, data_gen).selectExpr(
            "try_divide(a, b) as result"), "Divide")

@pytest.mark.skipif(is_before_spark_400(), reason="try_mod is not supported before Spark 4.0.0")
@allow_non_gpu('Remainder')
@pytest.mark.parametrize('data_gen', numeric_gens, ids=idfn)
def test_try_mod_fallback_to_cpu(data_gen):
    assert_gpu_fallback_collect(
        lambda spark: binary_op_df(spark, data_gen).selectExpr(
            "try_mod(a, b) as result"), "Remainder")

@allow_non_gpu_conditional(is_before_spark_340() and not is_databricks_runtime(), "ProjectExec")
@allow_non_gpu('Subtract')
@pytest.mark.parametrize('data_gen', numeric_gens, ids=idfn)
def test_try_subtract_fallback_to_cpu(data_gen):
    assert_gpu_fallback_collect(
        lambda spark: binary_op_df(spark, data_gen).selectExpr(
            "try_subtract(a, b) as result"), "Subtract")

@allow_non_gpu_conditional(is_before_spark_340() and not is_databricks_runtime(), "ProjectExec")
@allow_non_gpu('Multiply')
@pytest.mark.parametrize('data_gen', numeric_gens, ids=idfn)
def test_try_multiply_fallback_to_cpu(data_gen):
    assert_gpu_fallback_collect(
        lambda spark: binary_op_df(spark, data_gen).selectExpr(
            "try_multiply(a, b) as result"), "Multiply")
