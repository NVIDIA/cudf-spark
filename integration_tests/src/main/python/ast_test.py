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

import os
import re

import pytest

from asserts import (
    assert_cpu_and_gpu_are_equal_collect_with_capture,
    assert_gpu_and_cpu_are_equal_collect,
    assert_gpu_and_cpu_error,
    assert_gpu_fallback_collect,
    assert_spark_exception)
from data_gen import *
from marks import (allow_non_gpu, approximate_float, datagen_overrides, disable_ansi_mode,
                   ignore_order, inject_oom, validate_execs_in_gpu_plan)
from spark_session import (
    with_cpu_session, with_gpu_session, is_before_spark_330, is_before_spark_340,
    is_before_spark_400, is_databricks113_or_later, is_spark_403,
    is_spark_412_or_later)
from conftest import is_libcudf_jit_available, get_libcudf_jit_unavailable_reason
import pyspark.sql.functions as f
from pyspark.sql.types import (BooleanType, ByteType, DecimalType, DoubleType, FloatType,
                               IntegerType, LongType, ShortType, StructField, StructType)

# Each descriptor contains a list of data generators and a corresponding boolean
# indicating whether that data type is supported by the AST
ast_integral_descrs = [
    (byte_gen, False),  # AST implicitly upcasts to INT32, need AST cast to support
    (short_gen, False), # AST implicitly upcasts to INT32, need AST cast to support
    (int_gen, True),
    (long_gen, True)
]

ast_arithmetic_descrs = ast_integral_descrs + [(float_gen, True), (double_gen, True)]

# cudf AST cannot support comparing floating point until it is expressive enough to handle NaNs
ast_comparable_descrs = [
    (boolean_gen, True),
    (byte_gen, True),
    (short_gen, True),
    (int_gen, True),
    (long_gen, True),
    (float_gen, False),
    (double_gen, False),
    (timestamp_gen, True),
    (date_gen, True),
    (string_gen, True)
]

ast_descrs = [
    (boolean_gen, True),
    (byte_gen, True),
    (short_gen, True),
    (int_gen, True),
    (long_gen, True),
    (float_gen, True),
    (double_gen, True),
    (timestamp_gen, True),
    (date_gen, True),
    (string_gen, True)
]

ast_boolean_descr = [(boolean_gen, True)]
ast_double_descr = [(double_gen, True)]
# AST is not expressive enough to support the ACOSH Spark emulation expression in Spark 4.0.3
# and Spark 4.1.2+.
ast_acosh_descr = [(double_gen, not (is_spark_403() or is_spark_412_or_later()))]

_project_ast_enabled_conf = {"spark.rapids.sql.projectAstEnabled": "true"}
_jit_ast_enabled_conf = {"spark.rapids.sql.projectAstRowIrEnabled": "true"}
_ansi_jit_ast_enabled_conf = copy_and_update(
    ansi_enabled_conf,
    _jit_ast_enabled_conf,
    {"spark.rapids.sql.projectAstAnsiArithmeticEnabled": "true"})
_ansi_safe_row_ir_ast_enabled_conf = copy_and_update(ansi_enabled_conf, _jit_ast_enabled_conf)
_non_ansi_jit_ast_enabled_conf = copy_and_update(ansi_disabled_conf, _jit_ast_enabled_conf)
_requires_libcudf_jit = pytest.mark.skipif(
    not is_libcudf_jit_available(),
    reason="Project AST JIT requires libcudf JIT runtime: " +
           get_libcudf_jit_unavailable_reason())
_requires_global_jit_disabled = pytest.mark.skipif(
    os.environ.get("LIBCUDF_JIT_ENABLED") != "0",
    reason="explicit JIT API coverage requires LIBCUDF_JIT_ENABLED=0 at process startup")

def assert_gpu_ast(is_supported, func, conf={}):
    exist = "GpuProjectAstExec"
    non_exist = "GpuProjectExec"
    if not is_supported:
        exist = "GpuProjectExec"
        non_exist = "GpuProjectAstExec"
    ast_conf = copy_and_update(conf, _project_ast_enabled_conf)
    assert_cpu_and_gpu_are_equal_collect_with_capture(
        func,
        exist_classes=exist,
        non_exist_classes=non_exist,
        conf=ast_conf)

def assert_unary_ast(data_descr, func, conf={}):
    (data_gen, is_supported) = data_descr
    assert_gpu_ast(is_supported, lambda spark: func(unary_op_df(spark, data_gen)), conf=conf)

def assert_binary_ast(data_descr, func, conf={}):
    (data_gen, is_supported) = data_descr
    assert_gpu_ast(is_supported, lambda spark: func(binary_op_df(spark, data_gen)), conf=conf)

@pytest.mark.parametrize('data_gen', [boolean_gen, byte_gen, short_gen, int_gen, long_gen, float_gen, double_gen, timestamp_gen, date_gen], ids=idfn)
def test_literal(spark_tmp_path, data_gen):
    # Write data to Parquet so Spark generates a plan using just the count of the data.
    data_path = spark_tmp_path + '/AST_TEST_DATA'
    with_cpu_session(lambda spark: gen_df(spark, [("a", IntegerGen())]).write.parquet(data_path))
    scalar = with_cpu_session(lambda spark: gen_scalar(data_gen, force_no_nulls=True))
    assert_gpu_ast(is_supported=True,
                   func=lambda spark: spark.read.parquet(data_path).select(scalar))

@pytest.mark.parametrize('data_gen', [boolean_gen, byte_gen, short_gen, int_gen, long_gen, float_gen, double_gen, timestamp_gen, date_gen], ids=idfn)
def test_null_literal(spark_tmp_path, data_gen):
    # Write data to Parquet so Spark generates a plan using just the count of the data.
    data_path = spark_tmp_path + '/AST_TEST_DATA'
    with_cpu_session(lambda spark: gen_df(spark, [("a", IntegerGen())]).write.parquet(data_path))
    data_type = data_gen.data_type
    assert_gpu_ast(is_supported=True,
                   func=lambda spark: spark.read.parquet(data_path).select(f.lit(None).cast(data_type)))


def test_ast_project_pass_through_reorder_and_duplicates():
    assert_gpu_ast(
        is_supported=True,
        func=lambda spark: spark.createDataFrame(
            [(1, 2), (None, 3), (4, None)], 'a INT, b INT').selectExpr(
                'b AS x', 'a AS y', 'b AS z'))


@_requires_libcudf_jit
def test_jit_project_mixed_pass_through_and_computed_outputs():
    assert_gpu_ast(
        is_supported=True,
        func=lambda spark: spark.createDataFrame(
            [(1, 2), (None, 3), (4, None)], 'a INT, b INT').selectExpr(
                'b AS x', 'try_add(a, 1) AS sum', 'a AS y', '7 AS literal',
                '7 AS duplicate_literal', 'a * b AS product',
                'try_add(1, a) AS sum_again', 'b AS z'),
        conf=_ansi_jit_ast_enabled_conf)


@_requires_libcudf_jit
def test_jit_decimal_literals(spark_tmp_path):
    data_path = spark_tmp_path + '/AST_TEST_DATA'
    with_cpu_session(lambda spark: gen_df(spark, [("a", IntegerGen())]).write.parquet(data_path))
    assert_gpu_ast(is_supported=True,
                   func=lambda spark: spark.read.parquet(data_path).selectExpr(
                       'cast(12.34 as DECIMAL(7, 2)) AS dec32_positive',
                       'cast(0 as DECIMAL(9, 0)) AS dec32_zero_scale',
                       'cast(null as DECIMAL(9, 2)) AS dec32_null',
                       'cast(-12345678901234.5678 as DECIMAL(18, 4)) AS dec64_negative',
                       'cast(null as DECIMAL(18, 4)) AS dec64_null',
                       'cast(1234567890123456789012345678.1234567890 as '
                       'DECIMAL(38, 10)) AS dec128_positive',
                       'cast(0.00000000000000000000000000000000000001 as '
                       'DECIMAL(38, 38)) AS dec128_max_scale',
                       'cast(null as DECIMAL(38, 10)) AS dec128_null'),
                   conf=_ansi_jit_ast_enabled_conf)

def test_decimal_literal_falls_back_without_jit(spark_tmp_path):
    data_path = spark_tmp_path + '/AST_TEST_DATA'
    with_cpu_session(lambda spark: gen_df(spark, [("a", IntegerGen())]).write.parquet(data_path))
    assert_gpu_ast(is_supported=False,
                   func=lambda spark: spark.read.parquet(data_path).selectExpr(
                       'cast(12.34 as DECIMAL(7, 2))'),
                   conf={"spark.rapids.sql.projectAstRowIrEnabled": "false"})

@pytest.mark.parametrize('data_descr', ast_descrs, ids=idfn)
def test_isnull(data_descr):
    assert_unary_ast(data_descr, lambda df: df.selectExpr('isnull(a)'))

@pytest.mark.parametrize('data_descr', ast_descrs, ids=idfn)
def test_isnotnull(data_descr):
    assert_unary_ast(data_descr, lambda df: df.selectExpr('isnotnull(a)'))

@pytest.mark.parametrize('data_descr', ast_integral_descrs, ids=idfn)
def test_bitwise_not(data_descr):
    assert_unary_ast(data_descr, lambda df: df.selectExpr('~a'))

# This just ends up being a pass through.  There is no good way to force
# a unary positive into a plan, because it gets optimized out, but this
# verifies that we can handle it.
@pytest.mark.parametrize('data_descr', [
    (byte_gen, True),
    (short_gen, True),
    (int_gen, True),
    (long_gen, True),
    (float_gen, True),
    (double_gen, True)], ids=idfn)
def test_unary_positive(data_descr):
    assert_unary_ast(data_descr, lambda df: df.selectExpr('+a'))

@pytest.mark.skipif(is_before_spark_330(), reason='DayTimeInterval is not supported before Pyspark 3.3.0')
def test_unary_positive_for_daytime_interval():
    data_descr = (DayTimeIntervalGen(), True)
    assert_unary_ast(data_descr, lambda df: df.selectExpr('+a'))

@pytest.mark.parametrize('data_descr', ast_arithmetic_descrs, ids=idfn)
@disable_ansi_mode
def test_unary_minus(data_descr):
    assert_unary_ast(data_descr, lambda df: df.selectExpr('-a'))

@pytest.mark.parametrize('data_descr', ast_arithmetic_descrs, ids=idfn)
@disable_ansi_mode
def test_abs(data_descr):
    assert_unary_ast(data_descr, lambda df: df.selectExpr('abs(a)'))

@approximate_float
@pytest.mark.parametrize('data_descr', ast_double_descr, ids=idfn)
def test_cbrt(data_descr):
    assert_unary_ast(data_descr, lambda df: df.selectExpr('cbrt(a)'))

@pytest.mark.parametrize('data_descr', ast_boolean_descr, ids=idfn)
def test_not(data_descr):
    assert_unary_ast(data_descr, lambda df: df.selectExpr('!a'))

@pytest.mark.parametrize('data_descr', ast_double_descr, ids=idfn)
def test_rint(data_descr):
    assert_unary_ast(data_descr, lambda df: df.selectExpr('rint(a)'))

@approximate_float
@pytest.mark.parametrize('data_descr', ast_double_descr, ids=idfn)
def test_sqrt(data_descr):
    assert_unary_ast(data_descr, lambda df: df.selectExpr('sqrt(a)'))

@approximate_float
@pytest.mark.parametrize('data_descr', ast_double_descr, ids=idfn)
def test_sin(data_descr):
    assert_unary_ast(data_descr, lambda df: df.selectExpr('sin(a)'))

@approximate_float
@pytest.mark.parametrize('data_descr', ast_double_descr, ids=idfn)
def test_cos(data_descr):
    assert_unary_ast(data_descr, lambda df: df.selectExpr('cos(a)'))

@approximate_float
@pytest.mark.parametrize('data_descr', ast_double_descr, ids=idfn)
def test_tan(data_descr):
    assert_unary_ast(data_descr, lambda df: df.selectExpr('tan(a)'))

@approximate_float
@pytest.mark.parametrize('data_descr', ast_double_descr, ids=idfn)
def test_cot(data_descr):
    assert_unary_ast(data_descr, lambda df: df.selectExpr('cot(a)'))

@approximate_float
@pytest.mark.parametrize('data_descr', ast_double_descr, ids=idfn)
def test_sinh(data_descr):
    assert_unary_ast(data_descr, lambda df: df.selectExpr('sinh(a)'))

@approximate_float
@pytest.mark.parametrize('data_descr', ast_double_descr, ids=idfn)
def test_cosh(data_descr):
    assert_unary_ast(data_descr, lambda df: df.selectExpr('cosh(a)'))

@approximate_float
@pytest.mark.parametrize('data_descr', ast_double_descr, ids=idfn)
def test_tanh(data_descr):
    assert_unary_ast(data_descr, lambda df: df.selectExpr('tanh(a)'))

@approximate_float
@pytest.mark.parametrize('data_descr', ast_double_descr, ids=idfn)
def test_asin(data_descr):
    assert_unary_ast(data_descr, lambda df: df.selectExpr('asin(a)'))

@approximate_float
@pytest.mark.parametrize('data_descr', ast_double_descr, ids=idfn)
def test_acos(data_descr):
    assert_unary_ast(data_descr, lambda df: df.selectExpr('acos(a)'))

@approximate_float
@pytest.mark.parametrize('data_descr', ast_double_descr, ids=idfn)
def test_atan(data_descr):
    assert_unary_ast(data_descr, lambda df: df.selectExpr('atan(a)'))

# AST is not expressive enough to support the ASINH Spark emulation expression
@approximate_float
@pytest.mark.parametrize('data_descr', [(double_gen, False)], ids=idfn)
def test_asinh(data_descr):
    assert_unary_ast(data_descr, lambda df: df.selectExpr('asinh(a)'))

@approximate_float
@pytest.mark.parametrize('data_descr', ast_acosh_descr, ids=idfn)
def test_acosh(data_descr):
    assert_unary_ast(data_descr, lambda df: df.selectExpr('acosh(a)'))

@approximate_float
@pytest.mark.parametrize('data_descr', ast_double_descr, ids=idfn)
def test_atanh(data_descr):
    assert_unary_ast(data_descr, lambda df: df.selectExpr('atanh(a)'))

# The default approximate is 1e-6 or 1 in a million
# in some cases we need to adjust this because the algorithm is different
@approximate_float(rel=1e-4, abs=1e-12)
# Because Spark will overflow on large exponents drop to something well below
# what it fails at, note this is binary exponent, not base 10
@pytest.mark.parametrize('data_descr', [(DoubleGen(min_exp=-20, max_exp=20), True)], ids=idfn)
def test_asinh_improved(data_descr):
    assert_unary_ast(data_descr, lambda df: df.selectExpr('asinh(a)'),
        conf={'spark.rapids.sql.improvedFloatOps.enabled': 'true'})

# The default approximate is 1e-6 or 1 in a million
# in some cases we need to adjust this because the algorithm is different
@approximate_float(rel=1e-4, abs=1e-12)
# Because Spark will overflow on large exponents drop to something well below
# what it fails at, note this is binary exponent, not base 10
@pytest.mark.parametrize('data_descr', [(DoubleGen(min_exp=-20, max_exp=20), True)], ids=idfn)
def test_acosh_improved(data_descr):
    assert_unary_ast(data_descr, lambda df: df.selectExpr('acosh(a)'),
        conf={'spark.rapids.sql.improvedFloatOps.enabled': 'true'})

@approximate_float
@pytest.mark.parametrize('data_descr', ast_double_descr, ids=idfn)
def test_exp(data_descr):
    assert_unary_ast(data_descr, lambda df: df.selectExpr('exp(a)'))

@approximate_float
@pytest.mark.parametrize('data_descr', ast_double_descr, ids=idfn)
def test_expm1(data_descr):
    assert_unary_ast(data_descr, lambda df: df.selectExpr('expm1(a)'))

@pytest.mark.parametrize('data_descr', ast_comparable_descrs, ids=idfn)
def test_eq(data_descr):
    (s1, s2) = with_cpu_session(lambda spark: gen_scalars(data_descr[0], 2))
    assert_binary_ast(data_descr,
        lambda df: df.select(
            f.col('a') == s1,
            s2 == f.col('b'),
            f.col('a') == f.col('b')))

@pytest.mark.parametrize('data_descr', ast_comparable_descrs, ids=idfn)
def test_eq_null_safe(data_descr):
    data_gen, _ = data_descr
    (s1, s2) = with_cpu_session(lambda spark: gen_scalars(data_gen, 2))
    data_type = data_gen.data_type
    assert_binary_ast(data_descr,
        lambda df: df.select(
            f.col('a').eqNullSafe(s1),
            s2.eqNullSafe(f.col('b')),
            f.lit(None).cast(data_type).eqNullSafe(f.col('a')),
            f.col('b').eqNullSafe(f.lit(None).cast(data_type)),
            f.col('a').eqNullSafe(f.col('b'))))

@pytest.mark.parametrize('data_gen', [
    StructGen([('child', IntegerGen())])
], ids=idfn)
def test_eq_null_safe_unsupported_ast_type_falls_back(data_gen):
    assert_binary_ast((data_gen, False),
        lambda df: df.select(f.col('a').eqNullSafe(f.col('b'))))

@_requires_libcudf_jit
def test_eq_null_safe_with_jit_child():
    def project(spark):
        result = spark.createDataFrame([
            (1, 1),
            (1, 2),
            (None, None),
            (None, 1),
            (1, None),
            (INT_MAX, INT_MAX)
        ] * 8, 'a INT, b LONG').selectExpr(
            'CAST(a AS BIGINT) <=> b AS same')
        assert result.schema['same'].nullable is False
        return result

    assert_gpu_ast(True, project, conf=_ansi_jit_ast_enabled_conf)

@pytest.mark.parametrize('data_descr', ast_comparable_descrs, ids=idfn)
def test_ne(data_descr):
    (s1, s2) = with_cpu_session(lambda spark: gen_scalars(data_descr[0], 2))
    assert_binary_ast(data_descr,
        lambda df: df.select(
            f.col('a') != s1,
            s2 != f.col('b'),
            f.col('a') != f.col('b')))

@pytest.mark.parametrize('data_descr', ast_comparable_descrs, ids=idfn)
def test_lt(data_descr):
    (s1, s2) = with_cpu_session(lambda spark: gen_scalars(data_descr[0], 2))
    assert_binary_ast(data_descr,
        lambda df: df.select(
            f.col('a') < s1,
            s2 < f.col('b'),
            f.col('a') < f.col('b')))

@pytest.mark.parametrize('data_descr', ast_comparable_descrs, ids=idfn)
def test_lte(data_descr):
    (s1, s2) = with_cpu_session(lambda spark: gen_scalars(data_descr[0], 2))
    assert_binary_ast(data_descr,
        lambda df: df.select(
            f.col('a') <= s1,
            s2 <= f.col('b'),
            f.col('a') <= f.col('b')))

@pytest.mark.parametrize('data_descr', ast_comparable_descrs, ids=idfn)
def test_gt(data_descr):
    (s1, s2) = with_cpu_session(lambda spark: gen_scalars(data_descr[0], 2))
    assert_binary_ast(data_descr,
        lambda df: df.select(
            f.col('a') > s1,
            s2 > f.col('b'),
            f.col('a') > f.col('b')))

@pytest.mark.parametrize('data_descr', ast_comparable_descrs, ids=idfn)
def test_gte(data_descr):
    (s1, s2) = with_cpu_session(lambda spark: gen_scalars(data_descr[0], 2))
    assert_binary_ast(data_descr,
        lambda df: df.select(
            f.col('a') >= s1,
            s2 >= f.col('b'),
            f.col('a') >= f.col('b')))

@pytest.mark.parametrize('data_descr', ast_integral_descrs, ids=idfn)
def test_bitwise_and(data_descr):
    data_type = data_descr[0].data_type
    assert_binary_ast(data_descr,
        lambda df: df.select(
            f.col('a').bitwiseAND(f.lit(100).cast(data_type)),
            f.lit(-12).cast(data_type).bitwiseAND(f.col('b')),
            f.col('a').bitwiseAND(f.col('b'))))

@pytest.mark.parametrize('data_descr', ast_integral_descrs, ids=idfn)
def test_bitwise_or(data_descr):
    data_type = data_descr[0].data_type
    assert_binary_ast(data_descr,
        lambda df: df.select(
            f.col('a').bitwiseOR(f.lit(100).cast(data_type)),
            f.lit(-12).cast(data_type).bitwiseOR(f.col('b')),
            f.col('a').bitwiseOR(f.col('b'))))

@pytest.mark.parametrize('data_descr', ast_integral_descrs, ids=idfn)
def test_bitwise_xor(data_descr):
    data_type = data_descr[0].data_type
    assert_binary_ast(data_descr,
        lambda df: df.select(
            f.col('a').bitwiseXOR(f.lit(100).cast(data_type)),
            f.lit(-12).cast(data_type).bitwiseXOR(f.col('b')),
            f.col('a').bitwiseXOR(f.col('b'))))

_ast_coalesce_descrs = [
    (boolean_gen, True),
    (byte_gen, True),
    (short_gen, True),
    (int_gen, True),
    (long_gen, True),
    (float_gen, True),
    (double_gen, True),
    (timestamp_gen, True),
    (date_gen, True),
    (string_gen, False)
]

@_requires_libcudf_jit
@pytest.mark.parametrize('data_descr', _ast_coalesce_descrs, ids=idfn)
def test_jit_coalesce(data_descr):
    data_gen, is_supported = data_descr
    scalar = with_cpu_session(
        lambda spark: gen_scalar(data_gen, force_no_nulls=True))
    gen = StructGen([
        ('a', data_gen.copy_special_case(None, weight=1000.0)),
        ('b', data_gen.copy_special_case(None, weight=1000.0)),
        ('c', data_gen.copy_special_case(None, weight=1000.0))],
        nullable=False)
    assert_gpu_ast(is_supported,
        lambda spark: gen_df(spark, gen).select(
            f.coalesce(f.col('a'), f.col('b')),
            f.coalesce(f.col('a'), f.col('b'), f.col('c'), scalar)),
        conf=_ansi_jit_ast_enabled_conf)

@_requires_libcudf_jit
def test_ansi_jit_coalesce_fallible_fallback_falls_back_from_ast():
    assert_gpu_ast(False,
        lambda spark: spark.createDataFrame(
            [(1, INT_MAX)] * 8,
            'a INT, b INT').selectExpr('COALESCE(a, b + 2)'),
        conf=_ansi_jit_ast_enabled_conf)

def test_ansi_coalesce_fallible_later_child_is_lazy():
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.createDataFrame(
            [(1, INT_MAX), (None, 0)] * 4,
            'a INT, b INT').selectExpr('COALESCE(a, b + 2)'),
        conf=ansi_enabled_conf)

def test_ansi_coalesce_recomputes_unresolved_rows():
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.createDataFrame(
            [(1, INT_MAX, INT_MAX), (None, 0, INT_MAX), (None, None, 0)] * 4,
            'a INT, b INT, c INT').selectExpr('COALESCE(a, b + 2, c + 2)'),
        conf=ansi_enabled_conf)

def test_ansi_coalesce_unmasked_fallible_child_errors():
    conf = copy_and_update(
        ansi_enabled_conf,
        {"spark.rapids.sql.test.injectRetryOOM": "false"})
    assert_gpu_and_cpu_error(
        lambda spark: spark.createDataFrame(
            [(None, INT_MAX)] * 8,
            'a INT, b INT').selectExpr('COALESCE(a, b + 2)').collect(),
        conf,
        'overflow')

_ast_nullify_if_descrs = _ast_coalesce_descrs

@_requires_libcudf_jit
@pytest.mark.parametrize('data_descr', _ast_nullify_if_descrs, ids=idfn)
def test_jit_if_nullify(data_descr):
    data_gen, is_supported = data_descr
    data_type = to_cast_string(data_gen.data_type)
    assert_gpu_ast(is_supported,
        lambda spark: binary_op_df(spark, data_gen).selectExpr(
            'if(isnull(a), cast(null as {}), b)'.format(data_type),
            'if(isnotnull(a), b, cast(null as {}))'.format(data_type),
            'if(cast(null as BOOLEAN), cast(null as {}), b)'.format(data_type)),
        conf=_ansi_jit_ast_enabled_conf)

_ast_if_else_descrs = _ast_coalesce_descrs

@_requires_libcudf_jit
@pytest.mark.parametrize('data_descr', _ast_if_else_descrs, ids=idfn)
def test_jit_if_else(data_descr):
    data_gen, is_supported = data_descr
    assert_gpu_ast(is_supported,
        lambda spark: binary_op_df(spark, data_gen).selectExpr(
            'if(isnull(a), b, a)',
            'if(isnotnull(a), a, b)',
            'if(cast(null as BOOLEAN), a, b)'),
        conf=_ansi_jit_ast_enabled_conf)

@_requires_libcudf_jit
def test_jit_if_else_boolean_condition():
    gen = StructGen([
        ('a', boolean_gen),
        ('b', boolean_gen),
        ('c', boolean_gen)],
        nullable=False)
    assert_gpu_ast(True,
        lambda spark: gen_df(spark, gen).selectExpr('if(c, a, b)'),
        conf=_ansi_jit_ast_enabled_conf)

_ast_decimal_conditional_types = [
    DecimalType(9, 2),
    DecimalType(18, 6),
    DecimalType(38, 18)
]

def _decimal_conditional_df(spark, decimal_type):
    rows = [
        (True, Decimal('1.25'), Decimal('2.50'), None, Decimal('1.25')),
        (False, Decimal('-3.75'), Decimal('4.00'), Decimal('0.50'), None),
        (None, Decimal('5.00'), Decimal('-6.25'), None, None),
        (True, Decimal('0.00'), Decimal('0.00'), Decimal('0.00'), Decimal('0.00'))
    ]
    schema = StructType([
        StructField('p', BooleanType(), True),
        StructField('a', decimal_type, False),
        StructField('b', decimal_type, False),
        StructField('n', decimal_type, True),
        StructField('m', decimal_type, True)
    ])
    return spark.createDataFrame(rows * 8, schema)

@_requires_libcudf_jit
@pytest.mark.parametrize('decimal_type', _ast_decimal_conditional_types, ids=idfn)
def test_jit_decimal_conditionals(decimal_type):
    def project(spark):
        result = _decimal_conditional_df(spark, decimal_type).selectExpr(
            'if(p, a, b) AS if_non_null',
            'if(p, n, a) AS if_nullable',
            'coalesce(n, a) AS coalesce_non_null',
            'coalesce(n, m) AS coalesce_nullable')
        assert all(field.dataType == decimal_type for field in result.schema)
        assert [field.nullable for field in result.schema] == [False, True, False, True]
        return result

    assert_gpu_ast(True, project, conf=_ansi_jit_ast_enabled_conf)

@_requires_libcudf_jit
@pytest.mark.parametrize('data_gen', decimal_gens, ids=idfn)
def test_jit_decimal_comparisons(data_gen):
    def project(spark):
        result = binary_op_df(spark, data_gen).select(
            (f.col('a') == f.col('b')).alias('eq'),
            (f.col('a') != f.col('b')).alias('ne'),
            (f.col('a') < f.col('b')).alias('lt'),
            (f.col('a') <= f.col('b')).alias('lte'),
            (f.col('a') > f.col('b')).alias('gt'),
            (f.col('a') >= f.col('b')).alias('gte'),
            f.col('a').eqNullSafe(f.col('b')).alias('eq_null_safe'))
        assert all(field.dataType == BooleanType() for field in result.schema)
        assert result.schema['eq_null_safe'].nullable is False
        return result

    assert_gpu_ast(True, project, conf=_ansi_jit_ast_enabled_conf)

@pytest.mark.parametrize('expression', [
    'if(p, a, b)',
    'coalesce(n, a)',
    'a < b'
], ids=['if', 'coalesce', 'comparison'])
def test_decimal_conditionals_and_comparison_require_jit(expression):
    assert_gpu_ast(False,
        lambda spark: _decimal_conditional_df(spark, DecimalType(9, 2)).selectExpr(expression),
        conf={
            "spark.rapids.sql.projectAstRowIrEnabled": "false"
        })

_ast_decimal_add_sub_cases = [
    ('decimal32', DecimalType(4, 2), DecimalType(4, 2), DecimalType(5, 2)),
    ('decimal32_to_64', DecimalType(9, 2), DecimalType(9, 2), DecimalType(10, 2)),
    ('decimal64_to_128', DecimalType(18, 6), DecimalType(18, 6), DecimalType(19, 6)),
    ('decimal128', DecimalType(37, 18), DecimalType(37, 18), DecimalType(38, 18)),
    ('mixed_scale', DecimalType(7, 2), DecimalType(9, 4), DecimalType(10, 4))
]
_requires_direct_decimal_arithmetic = pytest.mark.skipif(
    is_before_spark_340() and not is_databricks113_or_later(),
    reason='Spark 3.3 decimal arithmetic uses CheckOverflow wrappers')

def _decimal_binary_df(spark, lhs_type, rhs_type):
    rows = [
        (Decimal('1.25'), Decimal('2.50')),
        (Decimal('-3.75'), Decimal('4.00')),
        (None, Decimal('0.50')),
        (Decimal('5.00'), None),
        (Decimal('0.00'), Decimal('0.00'))
    ]
    schema = StructType([
        StructField('a', lhs_type, True),
        StructField('b', rhs_type, True)
    ])
    return spark.createDataFrame(rows * 8, schema)

@_requires_libcudf_jit
@_requires_direct_decimal_arithmetic
@pytest.mark.parametrize(
    'conf', [_ansi_jit_ast_enabled_conf, _non_ansi_jit_ast_enabled_conf],
    ids=['ansi', 'non-ansi'])
@pytest.mark.parametrize('case', _ast_decimal_add_sub_cases, ids=lambda case: case[0])
def test_jit_decimal_add_sub_exact(case, conf):
    _, lhs_type, rhs_type, expected_type = case

    def project(spark):
        result = _decimal_binary_df(spark, lhs_type, rhs_type).selectExpr(
            'a + b AS added',
            'a - b AS subtracted')
        assert [field.dataType for field in result.schema] == [expected_type, expected_type]
        return result

    assert_gpu_ast(True, project, conf=conf)

@_requires_libcudf_jit
@_requires_direct_decimal_arithmetic
@pytest.mark.parametrize(
    'allow_precision_loss, expected_type', [
        ('true', DecimalType(38, 17)),
        ('false', DecimalType(38, 18))
    ], ids=['scale_reduction', 'precision_cap'])
def test_jit_decimal_add_sub_inexact_result_falls_back(allow_precision_loss, expected_type):
    def project(spark):
        result = _decimal_binary_df(
            spark, DecimalType(38, 18), DecimalType(38, 18)).selectExpr(
                'a + b AS added',
                'a - b AS subtracted')
        assert all(field.dataType == expected_type for field in result.schema)
        return result

    assert_gpu_ast(False, project,
        conf=copy_and_update(_non_ansi_jit_ast_enabled_conf, {
            'spark.sql.decimalOperations.allowPrecisionLoss': allow_precision_loss
        }))

@_requires_direct_decimal_arithmetic
def test_decimal_add_sub_requires_jit():
    assert_gpu_ast(False,
        lambda spark: _decimal_binary_df(
            spark, DecimalType(9, 2), DecimalType(9, 2)).selectExpr('a + b', 'a - b'),
        conf=ansi_disabled_conf)

_ast_decimal_unary_gens = [
    DecimalGen(9, 2),
    DecimalGen(18, 6),
    DecimalGen(38, 18),
    DecimalGen(18, -2)
]

@_requires_libcudf_jit
@pytest.mark.parametrize(
    'conf', [_ansi_jit_ast_enabled_conf, _non_ansi_jit_ast_enabled_conf],
    ids=['ansi', 'non-ansi'])
@pytest.mark.parametrize('data_gen', _ast_decimal_unary_gens, ids=idfn)
def test_jit_decimal_unary_minus_abs(data_gen, conf):
    def project(spark):
        result = unary_op_df(spark, data_gen).selectExpr(
            '-a AS negated',
            'abs(a) AS absolute')
        assert all(field.dataType == data_gen.data_type for field in result.schema)
        return result

    assert_gpu_ast(True, project, conf=conf)

def test_decimal_unary_minus_abs_require_jit():
    assert_gpu_ast(False,
        lambda spark: unary_op_df(spark, DecimalGen(9, 2)).selectExpr('-a', 'abs(a)'),
        conf=ansi_disabled_conf)

_ast_decimal_multiply_cases = [
    ('decimal32', DecimalType(4, 2), DecimalType(4, 2), DecimalType(9, 4)),
    ('decimal64', DecimalType(7, 2), DecimalType(7, 2), DecimalType(15, 4)),
    ('decimal128', DecimalType(18, 4), DecimalType(18, 4), DecimalType(37, 8)),
    ('raw_precision_38', DecimalType(18, 4), DecimalType(19, 4), DecimalType(38, 8)),
    ('mixed_scale', DecimalType(7, 2), DecimalType(8, 4), DecimalType(16, 6))
]

@_requires_libcudf_jit
@_requires_direct_decimal_arithmetic
@pytest.mark.parametrize(
    'conf', [_ansi_jit_ast_enabled_conf, _non_ansi_jit_ast_enabled_conf],
    ids=['ansi', 'non-ansi'])
@pytest.mark.parametrize('case', _ast_decimal_multiply_cases, ids=lambda case: case[0])
def test_jit_decimal_multiply_exact(case, conf):
    _, lhs_type, rhs_type, expected_type = case

    def project(spark):
        result = _decimal_binary_df(spark, lhs_type, rhs_type).selectExpr('a * b AS multiplied')
        assert result.schema['multiplied'].dataType == expected_type
        return result

    assert_gpu_ast(True, project, conf=conf)

@_requires_libcudf_jit
@_requires_direct_decimal_arithmetic
@pytest.mark.parametrize(
    'allow_precision_loss, expected_type', [
        ('true', DecimalType(38, 17)),
        ('false', DecimalType(38, 38))
    ], ids=['scale_reduction', 'precision_cap'])
def test_jit_decimal_multiply_inexact_result_falls_back(
        allow_precision_loss, expected_type):
    def project(spark):
        result = _decimal_binary_df(
            spark, DecimalType(30, 20), DecimalType(30, 20)).selectExpr(
                'a * b AS multiplied')
        assert result.schema['multiplied'].dataType == expected_type
        return result

    assert_gpu_ast(False, project,
        conf=copy_and_update(_non_ansi_jit_ast_enabled_conf, {
            'spark.sql.decimalOperations.allowPrecisionLoss': allow_precision_loss
        }))

@_requires_direct_decimal_arithmetic
def test_decimal_multiply_requires_jit():
    assert_gpu_ast(False,
        lambda spark: _decimal_binary_df(
            spark, DecimalType(7, 2), DecimalType(7, 2)).selectExpr('a * b'),
        conf=ansi_disabled_conf)

_ast_decimal_remainder_types = [
    DecimalType(9, 2),
    DecimalType(18, 6),
    DecimalType(38, 18)
]

def _decimal_div_mod_df(spark, decimal_type, rows=None):
    if rows is None:
        rows = [
            (Decimal('500'), Decimal('200')),
            (Decimal('-500'), Decimal('200')),
            (Decimal('500'), Decimal('-200')),
            (Decimal('5.75'), Decimal('2.10')),
            (Decimal('-5.75'), Decimal('2.10')),
            (Decimal('5.75'), Decimal('-2.10')),
            (Decimal('-5.75'), Decimal('-2.10')),
            (None, Decimal('200')),
            (Decimal('500'), None)
        ]
    schema = StructType([
        StructField('a', decimal_type, True),
        StructField('b', decimal_type, True)
    ])
    return spark.createDataFrame(rows * 8, schema)

@_requires_libcudf_jit
@_requires_direct_decimal_arithmetic
@pytest.mark.parametrize(
    'conf', [_ansi_jit_ast_enabled_conf, _non_ansi_jit_ast_enabled_conf],
    ids=['ansi', 'non-ansi'])
@pytest.mark.parametrize('decimal_type', _ast_decimal_remainder_types, ids=str)
def test_jit_decimal_remainder_same_type(decimal_type, conf):
    def project(spark):
        result = _decimal_div_mod_df(spark, decimal_type).selectExpr('a % b AS remainder')
        assert result.schema['remainder'].dataType == decimal_type
        return result

    assert_gpu_ast(True, project, conf=conf)

@_requires_libcudf_jit
@_requires_direct_decimal_arithmetic
@pytest.mark.parametrize(
    'conf', [_ansi_jit_ast_enabled_conf, _non_ansi_jit_ast_enabled_conf],
    ids=['ansi', 'non-ansi'])
def test_jit_decimal_remainder_negative_scale(conf):
    decimal_type = DecimalType(18, -2)
    rows = [
        (Decimal('12300'), Decimal('200')),
        (Decimal('-12300'), Decimal('200')),
        (Decimal('12300'), Decimal('-200')),
        (None, Decimal('200')),
        (Decimal('12300'), None)
    ]

    def project(spark):
        result = _decimal_div_mod_df(spark, decimal_type, rows).selectExpr(
            'a % b AS remainder')
        assert result.schema['remainder'].dataType == decimal_type
        return result

    assert_gpu_ast(True, project, conf=conf)

@_requires_libcudf_jit
@_requires_direct_decimal_arithmetic
def test_non_ansi_jit_decimal_remainder_by_zero_is_null():
    rows = [
        (Decimal('500'), Decimal('0')),
        (None, Decimal('0')),
        (Decimal('500'), None)
    ]
    assert_gpu_ast(True,
        lambda spark: _decimal_div_mod_df(
            spark, DecimalType(18, 2), rows).selectExpr('a % b'),
        conf=_non_ansi_jit_ast_enabled_conf)

@_requires_libcudf_jit
@_requires_direct_decimal_arithmetic
def test_ansi_jit_decimal_remainder_null_dividend_zero_divisor():
    assert_gpu_ast(True,
        lambda spark: _decimal_div_mod_df(
            spark, DecimalType(18, 2), [(None, Decimal('0'))]).selectExpr('a % b'),
        conf=_ansi_jit_ast_enabled_conf)

@_requires_direct_decimal_arithmetic
def test_ansi_decimal_remainder_null_dividend_zero_divisor_gpu_project():
    assert_gpu_ast(False,
        lambda spark: _decimal_div_mod_df(
            spark, DecimalType(18, 2), [(None, Decimal('0'))]).selectExpr('a % b'),
        conf=ansi_enabled_conf)

@_requires_libcudf_jit
@_requires_direct_decimal_arithmetic
@validate_execs_in_gpu_plan('GpuProjectAstExec')
def test_ansi_jit_decimal_remainder_by_zero_errors():
    ast_conf = copy_and_update(
        _ansi_jit_ast_enabled_conf,
        _project_ast_enabled_conf,
        {'spark.rapids.sql.test.injectRetryOOM': 'false'})
    df_fun = lambda spark: _decimal_div_mod_df(
        spark, DecimalType(18, 2), [(Decimal('500'), Decimal('0'))]).selectExpr(
            'a % b').collect()
    assert_spark_exception(
        lambda: with_cpu_session(df_fun, ast_conf),
        'Division by zero')
    gpu_error_pattern = re.compile(
        r'(?s)(?=.*Division by zero)(?=.*a % b)',
        re.IGNORECASE)
    assert_spark_exception(
        lambda: with_gpu_session(df_fun, ast_conf),
        gpu_error_pattern)

@_requires_libcudf_jit
@_requires_direct_decimal_arithmetic
def test_jit_decimal_remainder_mixed_type_falls_back():
    assert_gpu_ast(False,
        lambda spark: _decimal_binary_df(
            spark, DecimalType(7, 2), DecimalType(8, 4)).selectExpr('a % b'),
        conf=_non_ansi_jit_ast_enabled_conf)

@_requires_direct_decimal_arithmetic
def test_decimal_remainder_requires_jit():
    assert_gpu_ast(False,
        lambda spark: _decimal_div_mod_df(
            spark, DecimalType(9, 2)).selectExpr('a % b'),
        conf=ansi_disabled_conf)

_ast_decimal_integral_divide_types = [
    DecimalType(9, 2),
    DecimalType(18, 6),
    DecimalType(18, -2)
]

@_requires_libcudf_jit
@_requires_direct_decimal_arithmetic
@pytest.mark.parametrize(
    'conf', [_ansi_jit_ast_enabled_conf, _non_ansi_jit_ast_enabled_conf],
    ids=['ansi', 'non-ansi'])
@pytest.mark.parametrize('decimal_type', _ast_decimal_integral_divide_types, ids=str)
def test_jit_decimal_integral_divide_same_type(decimal_type, conf):
    rows = None
    if decimal_type == DecimalType(18, 6):
        rows = [
            (Decimal('999999999999.999999'), Decimal('0.000001')),
            (Decimal('-999999999999.999999'), Decimal('0.000001')),
            (Decimal('999999999999.999999'), Decimal('-0.000001')),
            (Decimal('-999999999999.999999'), Decimal('-0.000001')),
            (Decimal('5.75'), Decimal('2.10')),
            (None, Decimal('2.10')),
            (Decimal('5.75'), None)
        ]
    elif decimal_type.scale < 0:
        rows = [
            (Decimal('12300'), Decimal('200')),
            (Decimal('-12300'), Decimal('200')),
            (Decimal('12300'), Decimal('-200')),
            (None, Decimal('200')),
            (Decimal('12300'), None)
        ]

    def project(spark):
        result = _decimal_div_mod_df(spark, decimal_type, rows).selectExpr(
            'a DIV b AS quotient')
        assert result.schema['quotient'].dataType == LongType()
        assert result.schema['quotient'].nullable
        return result

    assert_gpu_ast(True, project, conf=conf)

@_requires_libcudf_jit
@_requires_direct_decimal_arithmetic
def test_non_ansi_jit_decimal_integral_divide_by_zero_is_null():
    rows = [
        (Decimal('500'), Decimal('0')),
        (None, Decimal('0')),
        (Decimal('500'), None)
    ]
    assert_gpu_ast(True,
        lambda spark: _decimal_div_mod_df(
            spark, DecimalType(18, 2), rows).selectExpr('a DIV b'),
        conf=_non_ansi_jit_ast_enabled_conf)

@_requires_libcudf_jit
@_requires_direct_decimal_arithmetic
def test_ansi_jit_decimal_integral_divide_null_dividend_zero_divisor():
    assert_gpu_ast(True,
        lambda spark: _decimal_div_mod_df(
            spark, DecimalType(18, 2), [(None, Decimal('0'))]).selectExpr('a DIV b'),
        conf=_ansi_jit_ast_enabled_conf)

@_requires_libcudf_jit
@_requires_direct_decimal_arithmetic
@validate_execs_in_gpu_plan('GpuProjectAstExec')
def test_ansi_jit_decimal_integral_divide_by_zero_errors():
    ast_conf = copy_and_update(
        _ansi_jit_ast_enabled_conf,
        _project_ast_enabled_conf,
        {'spark.rapids.sql.test.injectRetryOOM': 'false'})
    df_fun = lambda spark: _decimal_div_mod_df(
        spark, DecimalType(18, 2), [(Decimal('500'), Decimal('0'))]).selectExpr(
            'a DIV b').collect()
    assert_spark_exception(
        lambda: with_cpu_session(df_fun, ast_conf),
        'Division by zero')
    gpu_error_pattern = re.compile(
        r'(?s)(?=.*Division by zero)(?=.*a DIV b)',
        re.IGNORECASE)
    assert_spark_exception(
        lambda: with_gpu_session(df_fun, ast_conf),
        gpu_error_pattern)

@_requires_libcudf_jit
@_requires_direct_decimal_arithmetic
@pytest.mark.parametrize('lhs_type,rhs_type', [
    (DecimalType(19, 2), DecimalType(19, 2)),
    (DecimalType(9, 2), DecimalType(9, 4))
], ids=['precision_19', 'mixed_scale'])
def test_jit_decimal_integral_divide_unsupported_type_falls_back(lhs_type, rhs_type):
    assert_gpu_ast(False,
        lambda spark: _decimal_binary_df(spark, lhs_type, rhs_type).selectExpr('a DIV b'),
        conf=_non_ansi_jit_ast_enabled_conf)

@_requires_direct_decimal_arithmetic
def test_decimal_integral_divide_requires_jit():
    assert_gpu_ast(False,
        lambda spark: _decimal_div_mod_df(
            spark, DecimalType(9, 2)).selectExpr('a DIV b'),
        conf=ansi_disabled_conf)

_ast_nullif_descrs = [
    (boolean_gen, True),
    (byte_gen, True),
    (short_gen, True),
    (int_gen, True),
    (long_gen, True),
    (timestamp_gen, True),
    (date_gen, True),
    (string_gen, False)
]

@_requires_libcudf_jit
@pytest.mark.parametrize('data_descr', _ast_nullif_descrs, ids=idfn)
def test_jit_nullif(data_descr):
    data_gen, is_supported = data_descr
    assert_gpu_ast(is_supported,
        lambda spark: binary_op_df(spark, data_gen).selectExpr('nullif(a, b)'),
        conf=_ansi_jit_ast_enabled_conf)

@_requires_libcudf_jit
@pytest.mark.parametrize('data_gen', [
    IntegerGen(nullable=False, min_val=-100, max_val=100, special_cases=[]),
    LongGen(nullable=False, min_val=-100, max_val=100, special_cases=[])
], ids=idfn)
def test_jit_if_pure_complex_branches(data_gen):
    data_type = to_cast_string(data_gen.data_type)
    gen = StructGen([
        ('p', RepeatSeqGen([True, False, None], data_type=BooleanType())),
        ('a', data_gen),
        ('b', data_gen)
    ], nullable=False)

    def project(spark):
        result = gen_df(spark, gen).selectExpr(
            f'if(p, (a + b) * cast(2 as {data_type}), '
            f'(a - b) + cast(3 as {data_type})) AS arithmetic',
            f'if(p, (a + cast(1 as {data_type})) < b, '
            f'a >= (b - cast(1 as {data_type}))) AS comparison',
            f'if(p, if(a < b, a + cast(1 as {data_type}), '
            f'b + cast(1 as {data_type})), '
            f'if(a >= b, a - cast(1 as {data_type}), '
            f'b - cast(1 as {data_type}))) AS nested_if')
        assert result.schema['arithmetic'].dataType == data_gen.data_type
        assert result.schema['arithmetic'].nullable is False
        assert result.schema['comparison'].dataType == BooleanType()
        assert result.schema['comparison'].nullable is False
        assert result.schema['nested_if'].dataType == data_gen.data_type
        assert result.schema['nested_if'].nullable is False
        return result

    assert_gpu_ast(True, project, conf=_non_ansi_jit_ast_enabled_conf)

@_requires_libcudf_jit
def test_jit_if_side_effecting_branch_falls_back():
    assert_gpu_ast(False,
        lambda spark: spark.createDataFrame(
            [(INT_MAX,), (0,), (None,)] * 8, 'a INT').selectExpr(
                f'if(a < {INT_MAX}, a + 1, a)',
                f'if(a >= {INT_MAX}, a, a + 1)'),
        conf=_ansi_jit_ast_enabled_conf)

@_requires_libcudf_jit
def test_jit_if_try_branches():
    rows = [
        (True, INT_MAX, 1),
        (False, INT_MIN, 1),
        (True, 0, INT_MIN),
        (False, 0, INT_MAX),
        (None, 7, 3),
        (True, None, 1),
        (False, 1, None)
    ]

    def project(spark):
        result = spark.createDataFrame(rows * 8, 'p BOOLEAN, a INT, b INT').selectExpr(
            'if(p, try_add(a, b), try_subtract(a, b)) AS result')
        assert result.schema['result'].dataType == IntegerType()
        assert result.schema['result'].nullable
        return result

    assert_gpu_ast(True, project, conf=_ansi_jit_ast_enabled_conf)

@_requires_libcudf_jit
def test_jit_case_when_pure_branches():
    rows = [
        (True, False, 3, 1),
        (True, True, 3, 1),
        (False, True, 3, 1),
        (False, False, 3, 1),
        (None, True, 3, 1),
        (None, None, 3, 1)
    ]
    schema = StructType([
        StructField('p', BooleanType(), True),
        StructField('q', BooleanType(), True),
        StructField('a', IntegerType(), False),
        StructField('b', IntegerType(), False)
    ])

    def project(spark):
        result = spark.createDataFrame(rows * 8, schema).selectExpr(
            'CASE WHEN p THEN (a + b) * 2 '
            'WHEN q THEN a - b ELSE a + 3 END AS value',
            'CASE WHEN p THEN a < b WHEN q THEN a >= b END AS no_else')
        assert result.schema['value'].dataType == IntegerType()
        assert result.schema['value'].nullable is False
        assert result.schema['no_else'].dataType == BooleanType()
        assert result.schema['no_else'].nullable
        return result

    assert_gpu_ast(True, project, conf=_non_ansi_jit_ast_enabled_conf)

@_requires_libcudf_jit
def test_jit_case_when_side_effecting_branches_fall_back():
    rows = [
        (True, False, INT_MAX),
        (False, True, 0),
        (False, False, INT_MAX),
        (None, True, 1)
    ]
    assert_gpu_ast(False,
        lambda spark: spark.createDataFrame(
            rows * 8, 'p BOOLEAN, q BOOLEAN, a INT').selectExpr(
                'CASE WHEN p THEN a WHEN q THEN a + 1 ELSE a END'),
        conf=_ansi_jit_ast_enabled_conf)

@_requires_libcudf_jit
def test_jit_case_when_later_side_effecting_predicate_is_lazy():
    rows = [
        (True, INT_MAX),
        (False, 0),
        (False, -2),
        (None, 1)
    ]
    assert_gpu_ast(False,
        lambda spark: spark.createDataFrame(
            rows * 8, 'p BOOLEAN, a INT').selectExpr(
                'CASE WHEN p THEN a WHEN a + 1 > 0 THEN a ELSE 0 END'),
        conf=_ansi_jit_ast_enabled_conf)

_ast_decimal_cast_descrs = [
    (DecimalGen(7, 2, special_cases=[]), DecimalType(9, 2), True),
    (DecimalGen(7, 2, special_cases=[]), DecimalType(9, 4), True),
    (DecimalGen(7, 2, special_cases=[]), DecimalType(18, 4), True),
    (DecimalGen(18, 2, special_cases=[]), DecimalType(30, 4), True),
    (DecimalGen(7, 4, special_cases=[]), DecimalType(9, 2), False)
]

@_requires_libcudf_jit
@pytest.mark.parametrize('data_descr', _ast_decimal_cast_descrs, ids=idfn)
def test_jit_decimal_cast(data_descr):
    data_gen, to_type, is_supported = data_descr
    assert_gpu_ast(is_supported,
        lambda spark: unary_op_df(spark, data_gen).select(f.col('a').cast(to_type)),
        conf=_ansi_jit_ast_enabled_conf)

@_requires_libcudf_jit
def test_jit_decimal_cast_precision_check_ansi_disabled():
    data_gen = DecimalGen(18, 2)
    assert_gpu_ast(True,
        lambda spark: unary_op_df(spark, data_gen).select(f.col('a').cast(DecimalType(9, 2))),
        conf=_non_ansi_jit_ast_enabled_conf)

@_requires_libcudf_jit
def test_jit_decimal_cast_scale_up_precision_check_ansi_disabled():
    data_gen = DecimalGen(18, 0)
    assert_gpu_ast(False,
        lambda spark: unary_op_df(spark, data_gen).select(f.col('a').cast(DecimalType(18, 2))),
        conf=_non_ansi_jit_ast_enabled_conf)

_ast_safe_numeric_cast_byte_gen = ByteGen(
    special_cases=[BYTE_MIN, BYTE_MAX, 0, 1, -1])
_ast_safe_numeric_cast_descrs = [
    (_ast_safe_numeric_cast_byte_gen, ShortType()),
    (_ast_safe_numeric_cast_byte_gen, IntegerType()),
    (_ast_safe_numeric_cast_byte_gen, LongType()),
    (short_gen, IntegerType()),
    (short_gen, LongType()),
    (int_gen, LongType()),
    (float_gen, DoubleType())
]

@_requires_libcudf_jit
@pytest.mark.parametrize(
    'conf', [_ansi_jit_ast_enabled_conf, _non_ansi_jit_ast_enabled_conf],
    ids=['ansi', 'non-ansi'])
@pytest.mark.parametrize('data_descr', _ast_safe_numeric_cast_descrs, ids=idfn)
def test_jit_safe_numeric_cast(data_descr, conf):
    data_gen, to_type = data_descr
    assert_gpu_ast(True,
        lambda spark: unary_op_df(spark, data_gen).select(f.col('a').cast(to_type)),
        conf=conf)

_ast_safe_primitive_cast_descrs = [
    (_ast_safe_numeric_cast_byte_gen, BooleanType()),
    (short_gen, BooleanType()),
    (int_gen, BooleanType()),
    (long_gen, BooleanType()),
    (float_gen, BooleanType()),
    (double_gen, BooleanType()),
    (boolean_gen, ByteType()),
    (boolean_gen, ShortType()),
    (boolean_gen, IntegerType()),
    (boolean_gen, LongType()),
    (boolean_gen, FloatType()),
    (boolean_gen, DoubleType()),
    (double_gen, FloatType())
]

@_requires_libcudf_jit
@pytest.mark.parametrize(
    'conf', [_ansi_jit_ast_enabled_conf, _non_ansi_jit_ast_enabled_conf],
    ids=['ansi', 'non-ansi'])
@pytest.mark.parametrize('data_descr', _ast_safe_primitive_cast_descrs, ids=idfn)
def test_jit_safe_primitive_cast(data_descr, conf):
    data_gen, to_type = data_descr
    assert_gpu_ast(True,
        lambda spark: unary_op_df(spark, data_gen).select(f.col('a').cast(to_type)),
        conf=conf)

_jit_join_conf = copy_and_update(
    _jit_ast_enabled_conf,
    _project_ast_enabled_conf,
    {'spark.sql.adaptive.enabled': 'false'})

@_requires_libcudf_jit
@ignore_order(local=True)
@validate_execs_in_gpu_plan('GpuBroadcastHashJoinExec')
def test_jit_cast_in_broadcast_hash_join_condition():
    def do_join(spark):
        left = spark.createDataFrame(
            [('cat1', 100), ('cat2', 300)],
            'category STRING, range_start LONG')
        right = spark.createDataFrame(
            [('cat1', 120), ('cat2', 200)],
            'category STRING, range_end INT')
        condition = ((left.category == right.category) &
                     (left.range_start < right.range_end.cast('long')))
        return left.join(f.broadcast(right), condition)

    assert_gpu_and_cpu_are_equal_collect(do_join, conf=_jit_join_conf)

@_requires_libcudf_jit
@ignore_order(local=True)
@validate_execs_in_gpu_plan('GpuBroadcastNestedLoopJoinExec')
def test_jit_cast_in_broadcast_nested_loop_join_condition():
    def do_join(spark):
        left = spark.createDataFrame([(100,), (300,)], 'range_start LONG')
        right = spark.createDataFrame([(120,), (200,)], 'range_end INT')
        return left.join(
            f.broadcast(right),
            left.range_start < right.range_end.cast('long'))

    assert_gpu_and_cpu_are_equal_collect(do_join, conf=_jit_join_conf)

@_requires_libcudf_jit
@ignore_order(local=True)
@validate_execs_in_gpu_plan('GpuBroadcastHashJoinExec', 'GpuFilterExec')
def test_jit_fallible_expression_is_not_precomputed_before_join():
    def do_join(spark):
        left = spark.createDataFrame([('match', 10)], 'category STRING, range_start INT')
        right = spark.createDataFrame(
            [('match', 1), ('unmatched', INT_MAX)],
            'category STRING, value INT')
        condition = ((left.category == right.category) &
                     (left.range_start > right.value + 1))
        return left.join(f.broadcast(right), condition)

    conf = copy_and_update(_jit_join_conf, ansi_enabled_conf)
    assert_gpu_and_cpu_are_equal_collect(do_join, conf=conf)

_ast_shift_descrs = [(int_gen, True), (long_gen, True)]
_ast_shift_amount_gen = IntegerGen(
    min_val=-80,
    max_val=80,
    special_cases=[-65, -64, -63, -33, -32, -31, -1, 0, 1, 31, 32, 33, 63, 64, 65])

@_requires_libcudf_jit
@pytest.mark.parametrize('data_descr', _ast_shift_descrs, ids=idfn)
def test_jit_shift_left(data_descr):
    data_gen, is_supported = data_descr
    string_type = to_cast_string(data_gen.data_type)
    assert_gpu_ast(is_supported,
        lambda spark: two_col_df(spark, data_gen, _ast_shift_amount_gen).selectExpr(
            'shiftleft(a, cast(12 as INT))',
            'shiftleft(a, cast(40 as INT))',
            'shiftleft(a, cast(-1 as INT))',
            'shiftleft(cast(-12 as {}), b)'.format(string_type),
            'shiftleft(cast(null as {}), b)'.format(string_type),
            'shiftleft(a, cast(null as INT))',
            'shiftleft(a, b)'),
        conf=_ansi_jit_ast_enabled_conf)

@_requires_libcudf_jit
@pytest.mark.parametrize('data_descr', _ast_shift_descrs, ids=idfn)
def test_jit_shift_right(data_descr):
    data_gen, is_supported = data_descr
    string_type = to_cast_string(data_gen.data_type)
    assert_gpu_ast(is_supported,
        lambda spark: two_col_df(spark, data_gen, _ast_shift_amount_gen).selectExpr(
            'shiftright(a, cast(12 as INT))',
            'shiftright(a, cast(40 as INT))',
            'shiftright(a, cast(-1 as INT))',
            'shiftright(cast(-12 as {}), b)'.format(string_type),
            'shiftright(cast(null as {}), b)'.format(string_type),
            'shiftright(a, cast(null as INT))',
            'shiftright(a, b)'),
        conf=_ansi_jit_ast_enabled_conf)

@_requires_libcudf_jit
@pytest.mark.parametrize('data_descr', _ast_shift_descrs, ids=idfn)
def test_jit_shift_right_unsigned(data_descr):
    data_gen, is_supported = data_descr
    string_type = to_cast_string(data_gen.data_type)
    assert_gpu_ast(is_supported,
        lambda spark: two_col_df(spark, data_gen, _ast_shift_amount_gen).selectExpr(
            'shiftrightunsigned(a, cast(12 as INT))',
            'shiftrightunsigned(a, cast(40 as INT))',
            'shiftrightunsigned(a, cast(-1 as INT))',
            'shiftrightunsigned(cast(-12 as {}), b)'.format(string_type),
            'shiftrightunsigned(cast(null as {}), b)'.format(string_type),
            'shiftrightunsigned(a, cast(null as INT))',
            'shiftrightunsigned(a, b)'),
        conf=_ansi_jit_ast_enabled_conf)

@pytest.mark.parametrize('data_descr', ast_arithmetic_descrs, ids=idfn)
@disable_ansi_mode
def test_addition(data_descr):
    data_type = data_descr[0].data_type
    assert_binary_ast(data_descr,
        lambda df: df.select(
            f.col('a') + f.lit(100).cast(data_type),
            f.lit(-12).cast(data_type) + f.col('b'),
            f.col('a') + f.col('b')))

@pytest.mark.parametrize('data_descr', ast_arithmetic_descrs, ids=idfn)
@disable_ansi_mode
def test_subtraction(data_descr):
    data_type = data_descr[0].data_type
    assert_binary_ast(data_descr,
        lambda df: df.select(
            f.col('a') - f.lit(100).cast(data_type),
            f.lit(-12).cast(data_type) - f.col('b'),
            f.col('a') - f.col('b')))

@pytest.mark.parametrize('data_descr', ast_arithmetic_descrs, ids=idfn)
@disable_ansi_mode
def test_multiplication(data_descr):
    data_type = data_descr[0].data_type
    assert_binary_ast(data_descr,
        lambda df: df.select(
            f.col('a') * f.lit(100).cast(data_type),
            f.lit(-12).cast(data_type) * f.col('b'),
            f.col('a') * f.col('b')))

# Each descriptor contains a list of data generators and a corresponding boolean
# indicating whether that data type is supported by the AST
# all the below desc are not supported by the AST because ANSI mode is on
_ast_integral_desc_list_for_ansi_on = [
    (ByteGen(min_val=-11, max_val=11, special_cases=[]), False),  # 11 * 11 < 127 (Byte.MaxValue)
    (ShortGen(min_val=-181, max_val=181, special_cases=[]), False), # 181 * 181 < 32767 (Short.MaxValue)
    (IntegerGen(min_val=-46340, max_val=46340, special_cases=[]), False) , # 46340 * 46340 < 2147483647 (Int.MaxValue)
    (LongGen(min_val=-3037000499, max_val=3037000499, special_cases=[]), False)] # 3037000499 * 3037000499 < 9223372036854775807(Long.MaxValue)
@pytest.mark.parametrize('data_desc', _ast_integral_desc_list_for_ansi_on, ids=idfn)
def test_multiplication_for_integer_ansi_on(data_desc):
    data_type = data_desc[0].data_type
    assert_binary_ast(data_desc,
                      lambda df: df.select(f.col('a') * f.col('b')),
                      conf=ansi_enabled_conf)

_ast_integral_desc_list_for_ansi_jit_on = [
    (ByteGen(min_val=-11, max_val=11, special_cases=[]), True),
    (ShortGen(min_val=-181, max_val=181, special_cases=[]), True),
    (IntegerGen(min_val=-46340, max_val=46340, special_cases=[]), True),
    (LongGen(min_val=-3037000499, max_val=3037000499, special_cases=[]), True)]

_ansi_jit_narrow_fallback_cases = [
    ('byte_ansi_feature_off', 'a TINYINT, b TINYINT', [(11, 2), (-11, 2), (None, 1)],
     ansi_enabled_conf),
    ('short_ansi_feature_off', 'a SMALLINT, b SMALLINT', [(181, 2), (-181, 2), (None, 1)],
     ansi_enabled_conf),
    ('byte_non_ansi_jit_on', 'a TINYINT, b TINYINT', [(11, 2), (-11, 2), (None, 1)],
     _non_ansi_jit_ast_enabled_conf),
    ('short_non_ansi_jit_on', 'a SMALLINT, b SMALLINT', [(181, 2), (-181, 2), (None, 1)],
     _non_ansi_jit_ast_enabled_conf),
    ('int_ansi_fallible_off', 'a INT, b INT', [(46340, 2), (-46340, 2), (None, 1)],
     _ansi_safe_row_ir_ast_enabled_conf)]

@_requires_libcudf_jit
@pytest.mark.parametrize(
    'case', _ansi_jit_narrow_fallback_cases, ids=lambda case: case[0])
def test_narrow_arithmetic_requires_ansi_row_ir_jit(case):
    _, schema, rows, conf = case
    assert_gpu_ast(False,
                   lambda spark: spark.createDataFrame(rows * 8, schema).selectExpr(
                       'a + b', 'a - b', 'a * b', '-a', 'abs(a)'),
                   conf=conf)

@_requires_libcudf_jit
@_requires_global_jit_disabled
def test_safe_row_ir_does_not_require_ansi_fallible_semantics():
    assert_gpu_ast(True,
                   lambda spark: spark.createDataFrame(
                       [(1,), (-1,), (None,)] * 8, 'a INT').selectExpr(
                           'if(a > 0, shiftleft(a, 1), shiftright(a, 1))'),
                   conf=_ansi_safe_row_ir_ast_enabled_conf)

@_requires_libcudf_jit
def test_ansi_jit_byte_add_exact_width():
    rows = [(127, 0), (-128, 0), (126, 1), (-127, -1), (None, 1)] * 8
    assert_gpu_ast(True,
                   lambda spark: spark.createDataFrame(
                       rows, 'a TINYINT, b TINYINT').selectExpr('a + b'),
                   conf=_ansi_jit_ast_enabled_conf)

@_requires_libcudf_jit
def test_ansi_jit_short_add_exact_width():
    rows = [(32767, 0), (-32768, 0), (32766, 1), (-32767, -1), (None, 1)] * 8
    assert_gpu_ast(True,
                   lambda spark: spark.createDataFrame(
                       rows, 'a SMALLINT, b SMALLINT').selectExpr('a + b'),
                   conf=_ansi_jit_ast_enabled_conf)

@_requires_libcudf_jit
def test_ansi_jit_byte_subtract_exact_width():
    rows = [(127, 0), (-128, 0), (127, 1), (-128, -1), (None, 1)] * 8
    assert_gpu_ast(True,
                   lambda spark: spark.createDataFrame(
                       rows, 'a TINYINT, b TINYINT').selectExpr('a - b'),
                   conf=_ansi_jit_ast_enabled_conf)

@_requires_libcudf_jit
def test_ansi_jit_short_subtract_exact_width():
    rows = [(32767, 0), (-32768, 0), (32767, 1), (-32768, -1), (None, 1)] * 8
    assert_gpu_ast(True,
                   lambda spark: spark.createDataFrame(
                       rows, 'a SMALLINT, b SMALLINT').selectExpr('a - b'),
                   conf=_ansi_jit_ast_enabled_conf)

@_requires_libcudf_jit
def test_ansi_jit_byte_multiply_exact_width():
    rows = [(127, 1), (-128, 1), (63, 2), (-64, 2), (0, 127), (None, 1)] * 8
    assert_gpu_ast(True,
                   lambda spark: spark.createDataFrame(
                       rows, 'a TINYINT, b TINYINT').selectExpr('a * b'),
                   conf=_ansi_jit_ast_enabled_conf)

@_requires_libcudf_jit
def test_ansi_jit_short_multiply_exact_width():
    rows = [
        (32767, 1), (-32768, 1), (16383, 2), (-16384, 2), (0, 32767), (None, 1)
    ] * 8
    assert_gpu_ast(True,
                   lambda spark: spark.createDataFrame(
                       rows, 'a SMALLINT, b SMALLINT').selectExpr('a * b'),
                   conf=_ansi_jit_ast_enabled_conf)

@_requires_libcudf_jit
def test_ansi_jit_byte_unary_minus_exact_width():
    rows = [(127,), (-127,), (1,), (-1,), (0,), (None,)] * 8
    assert_gpu_ast(True,
                   lambda spark: spark.createDataFrame(
                       rows, 'a TINYINT').selectExpr('-a'),
                   conf=_ansi_jit_ast_enabled_conf)

@_requires_libcudf_jit
def test_ansi_jit_short_unary_minus_exact_width():
    rows = [(32767,), (-32767,), (1,), (-1,), (0,), (None,)] * 8
    assert_gpu_ast(True,
                   lambda spark: spark.createDataFrame(
                       rows, 'a SMALLINT').selectExpr('-a'),
                   conf=_ansi_jit_ast_enabled_conf)

@_requires_libcudf_jit
def test_ansi_jit_byte_abs_exact_width():
    rows = [(127,), (-127,), (1,), (-1,), (0,), (None,)] * 8
    assert_gpu_ast(True,
                   lambda spark: spark.createDataFrame(
                       rows, 'a TINYINT').selectExpr('abs(a)'),
                   conf=_ansi_jit_ast_enabled_conf)

@_requires_libcudf_jit
def test_ansi_jit_short_abs_exact_width():
    rows = [(32767,), (-32767,), (1,), (-1,), (0,), (None,)] * 8
    assert_gpu_ast(True,
                   lambda spark: spark.createDataFrame(
                       rows, 'a SMALLINT').selectExpr('abs(a)'),
                   conf=_ansi_jit_ast_enabled_conf)

_ansi_jit_narrow_binary_overflow_cases = [
    ('byte_add_positive', 'a TINYINT, b TINYINT', (BYTE_MAX, 1),
     'a + b', 'Add operation', r'a \+ b'),
    ('byte_add_negative', 'a TINYINT, b TINYINT', (BYTE_MIN, -1),
     'a + b', 'Add operation', r'a \+ b'),
    ('short_add_positive', 'a SMALLINT, b SMALLINT', (SHORT_MAX, 1),
     'a + b', 'Add operation', r'a \+ b'),
    ('short_add_negative', 'a SMALLINT, b SMALLINT', (SHORT_MIN, -1),
     'a + b', 'Add operation', r'a \+ b'),
    ('byte_subtract_positive', 'a TINYINT, b TINYINT', (BYTE_MAX, -1),
     'a - b', 'Subtract operation', r'a - b'),
    ('byte_subtract_negative', 'a TINYINT, b TINYINT', (BYTE_MIN, 1),
     'a - b', 'Subtract operation', r'a - b'),
    ('short_subtract_positive', 'a SMALLINT, b SMALLINT', (SHORT_MAX, -1),
     'a - b', 'Subtract operation', r'a - b'),
    ('short_subtract_negative', 'a SMALLINT, b SMALLINT', (SHORT_MIN, 1),
     'a - b', 'Subtract operation', r'a - b'),
    ('byte_multiply_positive', 'a TINYINT, b TINYINT', (64, 2),
     'a * b', 'Multiply operation', r'a \* b'),
    ('byte_multiply_negative', 'a TINYINT, b TINYINT', (BYTE_MIN, 2),
     'a * b', 'Multiply operation', r'a \* b'),
    ('short_multiply_positive', 'a SMALLINT, b SMALLINT', (16384, 2),
     'a * b', 'Multiply operation', r'a \* b'),
    ('short_multiply_negative', 'a SMALLINT, b SMALLINT', (SHORT_MIN, 2),
     'a * b', 'Multiply operation', r'a \* b')]

@_requires_libcudf_jit
@validate_execs_in_gpu_plan('GpuProjectAstExec')
@pytest.mark.parametrize(
    'case', _ansi_jit_narrow_binary_overflow_cases, ids=lambda case: case[0])
def test_ansi_jit_narrow_binary_overflow(case):
    _, schema, row, expression, gpu_message, query_fragment = case
    ast_conf = copy_and_update(
        _ansi_jit_ast_enabled_conf,
        _project_ast_enabled_conf,
        {"spark.rapids.sql.test.injectRetryOOM": "false"})
    df_fun = lambda spark: spark.createDataFrame(
        [row] * 8, schema).selectExpr(expression).collect()
    assert_spark_exception(
        lambda: with_cpu_session(df_fun, ast_conf),
        re.compile(r'\[(?:BINARY_)?ARITHMETIC_OVERFLOW\]'))
    gpu_error_pattern = re.compile(
        rf'(?s)(?=.*{gpu_message})(?=.*{query_fragment})', re.IGNORECASE)
    assert_spark_exception(
        lambda: with_gpu_session(df_fun, ast_conf),
        gpu_error_pattern)

_ansi_jit_narrow_unary_overflow_cases = [
    ('byte_negate', 'a TINYINT', BYTE_MIN, '-a', 'minus operation', r'-a'),
    ('short_negate', 'a SMALLINT', SHORT_MIN, '-a', 'minus operation', r'-a'),
    ('byte_abs', 'a TINYINT', BYTE_MIN, 'abs(a)', 'abs operation', r'abs\(a\)'),
    ('short_abs', 'a SMALLINT', SHORT_MIN, 'abs(a)', 'abs operation', r'abs\(a\)')]

@_requires_libcudf_jit
@validate_execs_in_gpu_plan('GpuProjectAstExec')
@pytest.mark.parametrize(
    'case', _ansi_jit_narrow_unary_overflow_cases, ids=lambda case: case[0])
def test_ansi_jit_narrow_unary_overflow(case):
    _, schema, value, expression, gpu_message, query_fragment = case
    ast_conf = copy_and_update(
        _ansi_jit_ast_enabled_conf,
        _project_ast_enabled_conf,
        {"spark.rapids.sql.test.injectRetryOOM": "false"})
    df_fun = lambda spark: spark.createDataFrame(
        [(value,)] * 8, schema).selectExpr(expression).collect()
    assert_spark_exception(
        lambda: with_cpu_session(df_fun, ast_conf),
        re.compile(r'caused overflow', re.IGNORECASE))
    gpu_error_pattern = re.compile(
        rf'(?s)(?=.*{gpu_message})(?=.*{query_fragment})', re.IGNORECASE)
    assert_spark_exception(
        lambda: with_gpu_session(df_fun, ast_conf),
        gpu_error_pattern)

@_requires_libcudf_jit
@pytest.mark.parametrize('data_desc', _ast_integral_desc_list_for_ansi_jit_on, ids=idfn)
def test_ansi_jit_arithmetic_for_integer_ansi_on(data_desc):
    assert_binary_ast(data_desc,
                      lambda df: df.select(
                          f.col('a') + f.col('b'),
                          f.col('a') - f.col('b'),
                          f.col('a') * f.col('b')),
                      conf=_ansi_jit_ast_enabled_conf)

@_requires_libcudf_jit
@inject_oom("num_ooms=2,skip=0,type=GPU")
def test_ansi_jit_compile_oom_with_literal():
    assert_gpu_ast(True,
        lambda spark: spark.createDataFrame(
            [(1,), (2,)] * 4,
            'a INT').selectExpr('a + 1'),
        conf=_ansi_jit_ast_enabled_conf)

@_requires_libcudf_jit
@pytest.mark.parametrize('data_desc', _ast_integral_desc_list_for_ansi_jit_on, ids=idfn)
def test_ansi_jit_unary_arithmetic_for_integer_ansi_on(data_desc):
    assert_unary_ast(data_desc,
                     lambda df: df.select(-f.col('a'), f.abs(f.col('a'))),
                     conf=_ansi_jit_ast_enabled_conf)

_ansi_jit_overflow_error_cases = [
    ('add', 'a INT, b INT', [(INT_MAX, 1)], 'a + b', 'Add operation', r'a \+ b'),
    ('subtract', 'a INT, b INT', [(INT_MIN, 1)], 'a - b', 'Subtract operation', r'a - b'),
    ('multiply', 'a INT, b INT', [(INT_MAX, 2)], 'a * b', 'Multiply operation', r'a \* b'),
    ('negate', 'a INT', [(INT_MIN,)], '-a', 'minus operation', r'-a'),
    ('abs', 'a INT', [(INT_MIN,)], 'abs(a)', 'abs operation', r'abs\(a\)')]

@_requires_libcudf_jit
@validate_execs_in_gpu_plan('GpuProjectAstExec')
@pytest.mark.parametrize('case', _ansi_jit_overflow_error_cases, ids=lambda case: case[0])
def test_ansi_jit_arithmetic_overflow_errors(case):
    _, schema, rows, expression, gpu_message, query_fragment = case
    ast_conf = copy_and_update(
        _ansi_jit_ast_enabled_conf,
        _project_ast_enabled_conf,
        {"spark.rapids.sql.test.injectRetryOOM": "false"})
    df_fun = lambda spark: spark.createDataFrame(
        rows * 8, schema).selectExpr(expression).collect()
    assert_spark_exception(
        lambda: with_cpu_session(df_fun, ast_conf),
        '[ARITHMETIC_OVERFLOW]')
    gpu_error_pattern = re.compile(
        rf'(?s)(?=.*{gpu_message})(?=.*{query_fragment})',
        re.IGNORECASE)
    assert_spark_exception(
        lambda: with_gpu_session(df_fun, ast_conf),
        gpu_error_pattern)

@_requires_libcudf_jit
@inject_oom("num_ooms=1,skip=0,type=GPU")
@validate_execs_in_gpu_plan('GpuProjectAstExec')
def test_ansi_jit_later_output_error_after_oom():
    ast_conf = copy_and_update(_ansi_jit_ast_enabled_conf, _project_ast_enabled_conf)
    df_fun = lambda spark: spark.createDataFrame(
        spark.sparkContext.parallelize([(1, INT_MAX)] * 8, 1),
        'a INT, b INT').selectExpr('a AS safe', 'b + 1 AS fail').collect()
    assert_spark_exception(
        lambda: with_cpu_session(df_fun, ast_conf),
        '[ARITHMETIC_OVERFLOW]')
    gpu_error_pattern = re.compile(
        r'(?s)(?=.*Add operation)(?=.*b \+ 1)',
        re.IGNORECASE)
    assert_spark_exception(
        lambda: with_gpu_session(df_fun, ast_conf),
        gpu_error_pattern)

@_requires_libcudf_jit
@validate_execs_in_gpu_plan('GpuProjectAstExec')
def test_ansi_jit_nested_fallible_nodes_report_overflow():
    ast_conf = copy_and_update(
        _ansi_jit_ast_enabled_conf,
        _project_ast_enabled_conf,
        {"spark.rapids.sql.test.injectRetryOOM": "false"})
    df_fun = lambda spark: spark.createDataFrame(
        [(INT_MAX, 1, 1)] * 8,
        'a INT, b INT, c INT').selectExpr('(a + b) * c').collect()
    assert_spark_exception(
        lambda: with_cpu_session(df_fun, ast_conf),
        '[ARITHMETIC_OVERFLOW]')
    assert_spark_exception(
        lambda: with_gpu_session(df_fun, ast_conf),
        '[ARITHMETIC_OVERFLOW]')

@_requires_libcudf_jit
@validate_execs_in_gpu_plan('GpuProjectAstExec')
def test_ansi_jit_nested_fallible_nodes_report_division_by_zero():
    ast_conf = copy_and_update(
        _ansi_jit_ast_enabled_conf,
        _project_ast_enabled_conf,
        {"spark.rapids.sql.test.injectRetryOOM": "false"})
    df_fun = lambda spark: spark.createDataFrame(
        [(1, 2, 0)] * 8,
        'a INT, b INT, c INT').selectExpr('(a + b) DIV c').collect()
    assert_spark_exception(
        lambda: with_cpu_session(df_fun, ast_conf),
        '[DIVIDE_BY_ZERO]')
    assert_spark_exception(
        lambda: with_gpu_session(df_fun, ast_conf),
        '[DIVIDE_BY_ZERO]')

@_requires_libcudf_jit
@validate_execs_in_gpu_plan('GpuProjectExec')
def test_ansi_jit_decimal_precision_error_falls_back_from_ast():
    ast_conf = copy_and_update(
        _ansi_jit_ast_enabled_conf,
        _project_ast_enabled_conf,
        {"spark.rapids.sql.test.injectRetryOOM": "false"})
    error_pattern = re.compile(
        r'(?s)(?=.*NUMERIC_VALUE_OUT_OF_RANGE)(?=.*CAST\(a AS DECIMAL\(9,\s*2\)\))')
    assert_gpu_and_cpu_error(
        lambda spark: spark.createDataFrame(
            [(Decimal('12345678.90'),)] * 8,
            'a DECIMAL(18, 2)').selectExpr('CAST(a AS DECIMAL(9, 2))').collect(),
        ast_conf,
        error_pattern)

_ast_div_mod_desc_list_for_ansi_jit_on = [
    (IntegerGen(min_val=-1000, max_val=1000, special_cases=[]),
        SetValuesGen(IntegerType(), [-13, -7, -3, -1, 1, 3, 7, 13, None])),
    (LongGen(min_val=-1000000, max_val=1000000, special_cases=[]),
        SetValuesGen(LongType(), [-13, -7, -3, -1, 1, 3, 7, 13, None]))]

_non_ansi_jit_div_mod_cases = [
    ('int', 'a INT, b INT', IntegerType(), [
        (13, 3), (-13, 3), (13, -3), (-13, -3),
        (INT_MIN, -1), (7, 0), (None, 0), (7, None),
        (None, -1), (INT_MIN, None)]),
    ('long', 'a LONG, b LONG', LongType(), [
        (13, 3), (-13, 3), (13, -3), (-13, -3),
        (LONG_MIN, -1), (7, 0), (None, 0), (7, None),
        (None, -1), (LONG_MIN, None)])]

_primitive_remainder_cases = [
    ('byte', 'a TINYINT, b TINYINT', ByteType(), [
        (13, 3), (-13, 3), (13, -3), (-13, -3),
        (BYTE_MIN, -1), (None, 0), (7, None)], [
        (7, 0), (-7, 0)]),
    ('short', 'a SMALLINT, b SMALLINT', ShortType(), [
        (13, 3), (-13, 3), (13, -3), (-13, -3),
        (SHORT_MIN, -1), (None, 0), (7, None)], [
        (7, 0), (-7, 0)]),
    ('float', 'a FLOAT, b FLOAT', FloatType(), [
        (13.5, 3.0), (-13.5, 3.0), (13.5, -3.0), (-13.5, -3.0),
        (float('nan'), 3.0), (float('inf'), 3.0), (-float('inf'), 3.0),
        (13.5, float('inf')), (None, 0.0), (7.0, None)], [
        (7.0, 0.0), (7.0, -0.0), (-7.0, 0.0), (-7.0, -0.0)]),
    ('double', 'a DOUBLE, b DOUBLE', DoubleType(), [
        (13.5, 3.0), (-13.5, 3.0), (13.5, -3.0), (-13.5, -3.0),
        (float('nan'), 3.0), (float('inf'), 3.0), (-float('inf'), 3.0),
        (13.5, float('inf')), (None, 0.0), (7.0, None)], [
        (7.0, 0.0), (7.0, -0.0), (-7.0, 0.0), (-7.0, -0.0)])]

_primitive_remainder_plan_cases = [
    _primitive_remainder_cases[0],
    _primitive_remainder_cases[-1]]


@_requires_libcudf_jit
@approximate_float
@pytest.mark.parametrize(
    'conf,include_zero',
    [(_ansi_jit_ast_enabled_conf, False), (_non_ansi_jit_ast_enabled_conf, True)],
    ids=['ansi_on', 'ansi_off'])
@pytest.mark.parametrize('case', _primitive_remainder_cases, ids=lambda case: case[0])
def test_jit_primitive_remainder(case, conf, include_zero):
    _, schema, result_type, safe_rows, zero_rows = case
    rows = safe_rows + zero_rows if include_zero else safe_rows

    def project(spark):
        result = spark.createDataFrame(rows * 8, schema).selectExpr('a % b AS remainder')
        assert result.schema['remainder'].dataType == result_type
        assert result.schema['remainder'].nullable
        return result

    assert_gpu_ast(True, project, conf=conf)


@_requires_libcudf_jit
@validate_execs_in_gpu_plan('GpuProjectAstExec')
@pytest.mark.parametrize('case', _primitive_remainder_cases, ids=lambda case: case[0])
def test_ansi_jit_primitive_remainder_by_zero_errors(case):
    _, schema, _, _, zero_rows = case
    ast_conf = copy_and_update(
        _ansi_jit_ast_enabled_conf,
        _project_ast_enabled_conf,
        {"spark.rapids.sql.test.injectRetryOOM": "false"})
    df_fun = lambda spark: spark.createDataFrame(
        [zero_rows[0]] * 8, schema).selectExpr('a % b').collect()
    assert_spark_exception(
        lambda: with_cpu_session(df_fun, ast_conf),
        'Division by zero')
    gpu_error_pattern = re.compile(
        r'(?s)(?=.*Division by zero)(?=.*a % b)',
        re.IGNORECASE)
    assert_spark_exception(
        lambda: with_gpu_session(df_fun, ast_conf),
        gpu_error_pattern)

_try_jit_integral_arithmetic_cases = [
    ('byte', 'a TINYINT, b TINYINT', ByteType(), -128, 127),
    ('short', 'a SMALLINT, b SMALLINT', ShortType(), -32768, 32767),
    ('int', 'a INT, b INT', IntegerType(), INT_MIN, INT_MAX),
    ('long', 'a LONG, b LONG', LongType(), LONG_MIN, LONG_MAX)]

@_requires_libcudf_jit
@pytest.mark.parametrize(
    'conf', [_ansi_jit_ast_enabled_conf, _non_ansi_jit_ast_enabled_conf],
    ids=['ansi_on', 'ansi_off'])
@pytest.mark.parametrize(
    'case', _try_jit_integral_arithmetic_cases, ids=lambda case: case[0])
def test_try_jit_integral_arithmetic(case, conf):
    _, schema, result_type, min_value, max_value = case
    rows = [
        (max_value, 1), (min_value, -1),
        (min_value, 1), (max_value, -1),
        (max_value, 2), (min_value, 2),
        (7, -3), (None, 1), (1, None)]

    def project(spark):
        result = spark.createDataFrame(rows * 8, schema).selectExpr(
            'try_add(a, b)', 'try_subtract(a, b)', 'try_multiply(a, b)')
        assert all(field.dataType == result_type for field in result.schema)
        assert all(field.nullable for field in result.schema)
        return result

    assert_gpu_ast(True, project, conf=conf)

@allow_non_gpu('Add', 'Subtract', 'Multiply')
def test_try_integral_arithmetic_falls_back_without_row_ir_jit():
    assert_gpu_fallback_collect(
        lambda spark: spark.createDataFrame(
            [(INT_MAX, 1), (INT_MIN, -1), (None, 1)] * 8,
            'a INT, b INT').selectExpr(
                'try_add(a, b)', 'try_subtract(a, b)', 'try_multiply(a, b)'),
        'Add', conf=ansi_enabled_conf)

@_requires_libcudf_jit
def test_try_integral_arithmetic_regular_gpu_project():
    assert_gpu_ast(False,
        lambda spark: spark.createDataFrame(
            [(INT_MAX, 1), (INT_MIN, -1), (1, INT_MAX), (1, INT_MIN),
             (2, INT_MAX), (2, INT_MIN), (None, 1), (1, None)] * 8,
            'a INT, b INT').selectExpr(
                'try_add(a, b)', 'try_subtract(a, b)', 'try_multiply(a, b)',
                'try_add(a, cast(1 as INT))', 'try_add(cast(1 as INT), b)',
                'try_subtract(a, cast(1 as INT))', 'try_subtract(cast(1 as INT), b)',
                'try_multiply(a, cast(2 as INT))', 'try_multiply(cast(2 as INT), b)',
                'cast(a as STRING)'),
        conf=_ansi_jit_ast_enabled_conf)

@_requires_libcudf_jit
def test_try_integral_arithmetic_tiered_project_preserves_grouping():
    conf = copy_and_update(
        _ansi_jit_ast_enabled_conf,
        {"spark.rapids.sql.tiered.project.enabled": "true",
         "spark.sql.subexpressionElimination.enabled": "false"})
    assert_gpu_ast(False,
        lambda spark: spark.createDataFrame(
            [(INT_MAX, 1, -1), (INT_MAX, 2, 0), (None, 1, -1)] * 8,
            'a INT, b INT, c INT').selectExpr(
                'try_add(try_add(a, b), c)',
                'try_add(a, try_add(b, c))',
                'try_multiply(try_multiply(a, b), c)',
                'try_multiply(a, try_multiply(b, c))',
                'cast(a as STRING)'),
        conf=conf)

@pytest.mark.skipif(is_before_spark_400(), reason="try_mod is not supported before Spark 4.0.0")
@_requires_libcudf_jit
@pytest.mark.parametrize(
    'conf', [_ansi_jit_ast_enabled_conf, _non_ansi_jit_ast_enabled_conf],
    ids=['ansi_on', 'ansi_off'])
@pytest.mark.parametrize(
    'case', _non_ansi_jit_div_mod_cases, ids=lambda case: case[0])
def test_try_jit_integral_mod(case, conf):
    _, schema, result_type, rows = case

    def project(spark):
        result = spark.createDataFrame(rows * 8, schema).selectExpr('try_mod(a, b)')
        assert result.schema[0].dataType == result_type
        assert result.schema[0].nullable
        return result

    assert_gpu_ast(True, project, conf=conf)

@pytest.mark.skipif(is_before_spark_400(), reason="try_mod is not supported before Spark 4.0.0")
@_requires_libcudf_jit
@approximate_float
@pytest.mark.parametrize(
    'conf', [_ansi_jit_ast_enabled_conf, _non_ansi_jit_ast_enabled_conf],
    ids=['ansi_on', 'ansi_off'])
@pytest.mark.parametrize('case', _primitive_remainder_cases, ids=lambda case: case[0])
def test_try_jit_primitive_mod(case, conf):
    _, schema, result_type, safe_rows, zero_rows = case

    def project(spark):
        result = spark.createDataFrame(
            (safe_rows + zero_rows) * 8, schema).selectExpr('try_mod(a, b) AS remainder')
        assert result.schema['remainder'].dataType == result_type
        assert result.schema['remainder'].nullable
        return result

    assert_gpu_ast(True, project, conf=conf)


@pytest.mark.skipif(is_before_spark_400(), reason="try_mod is not supported before Spark 4.0.0")
@_requires_libcudf_jit
@allow_non_gpu('Remainder', 'Add')
def test_try_mod_side_effecting_lhs_falls_back():
    assert_gpu_fallback_collect(
        lambda spark: spark.createDataFrame(
            [(INT_MAX, 0), (INT_MAX, None)] * 8,
            'a INT, b INT').selectExpr('try_mod(a + 1, b)'),
        'Remainder', conf=_ansi_jit_ast_enabled_conf)


@pytest.mark.skipif(is_before_spark_400(), reason="try_mod is not supported before Spark 4.0.0")
@_requires_libcudf_jit
@allow_non_gpu('Remainder')
def test_try_jit_decimal_mod_falls_back():
    assert_gpu_fallback_collect(
        lambda spark: binary_op_df(spark, DecimalGen(10, 2)).selectExpr('try_mod(a, b)'),
        'Remainder', conf=_ansi_jit_ast_enabled_conf)

@pytest.mark.skipif(is_before_spark_400(), reason="try_mod is not supported before Spark 4.0.0")
@_requires_libcudf_jit
@pytest.mark.parametrize(
    'case', _non_ansi_jit_div_mod_cases, ids=lambda case: case[0])
def test_try_integral_mod_regular_gpu_project(case):
    _, schema, _, rows = case
    assert_gpu_ast(False,
        lambda spark: spark.createDataFrame(rows * 8, schema).selectExpr(
            'try_mod(a, b)', 'cast(a as STRING)'),
        conf=_ansi_jit_ast_enabled_conf)

@pytest.mark.skipif(is_before_spark_400(), reason="try_mod is not supported before Spark 4.0.0")
@_requires_libcudf_jit
@approximate_float
@pytest.mark.parametrize(
    'case', _primitive_remainder_plan_cases, ids=lambda case: case[0])
def test_try_primitive_mod_regular_gpu_project(case):
    _, schema, _, safe_rows, zero_rows = case
    assert_gpu_ast(False,
        lambda spark: spark.createDataFrame(
            (safe_rows + zero_rows) * 8, schema).selectExpr(
                'try_mod(a, b)', 'cast(null as STRING)'),
        conf=_ansi_jit_ast_enabled_conf)

@pytest.mark.skipif(is_before_spark_400(), reason="try_mod is not supported before Spark 4.0.0")
@allow_non_gpu('Remainder')
@approximate_float
@pytest.mark.parametrize(
    'case', _primitive_remainder_plan_cases, ids=lambda case: case[0])
def test_try_primitive_mod_falls_back_without_row_ir_jit(case):
    _, schema, _, safe_rows, zero_rows = case
    assert_gpu_fallback_collect(
        lambda spark: spark.createDataFrame(
            (safe_rows + zero_rows) * 8, schema).selectExpr('try_mod(a, b)'),
        'Remainder', conf=ansi_enabled_conf)

@approximate_float
@pytest.mark.parametrize(
    'case', _primitive_remainder_plan_cases, ids=lambda case: case[0])
def test_primitive_remainder_regular_gpu_project_without_row_ir_jit(case):
    _, schema, _, safe_rows, zero_rows = case
    assert_gpu_ast(False,
        lambda spark: spark.createDataFrame(
            (safe_rows + zero_rows) * 8, schema).selectExpr('a % b'),
        conf=ansi_disabled_conf)

@_requires_libcudf_jit
@pytest.mark.parametrize('case', _non_ansi_jit_div_mod_cases, ids=lambda case: case[0])
def test_non_ansi_jit_integral_div_mod(case):
    _, schema, mod_type, rows = case

    def project(spark):
        result = spark.createDataFrame(rows * 8, schema).selectExpr('a DIV b', 'a % b')
        assert result.schema[0].dataType == LongType()
        assert result.schema[1].dataType == mod_type
        return result

    assert_gpu_ast(True, project, conf=_non_ansi_jit_ast_enabled_conf)

def test_non_ansi_integral_div_mod_requires_row_ir_jit():
    assert_gpu_ast(False,
        lambda spark: spark.createDataFrame(
            [(13, 3), (7, 0), (None, 0)] * 8,
            'a INT, b INT').selectExpr('a DIV b', 'a % b'),
        conf=ansi_disabled_conf)

_true_divide_rows = [
    (13.5, 3.0), (-13.5, 3.0), (13.5, -3.0), (-13.5, -3.0),
    (0.0, 3.0), (-0.0, 3.0), (7.0, 0.0), (7.0, -0.0),
    (None, 0.0), (7.0, None),
    (float('nan'), 3.0), (float('inf'), 3.0), (-float('inf'), 3.0),
    (13.5, float('inf')), (float('inf'), float('inf'))]

_true_divide_schemas = ['a FLOAT, b FLOAT', 'a DOUBLE, b DOUBLE']

@_requires_libcudf_jit
@approximate_float
@pytest.mark.parametrize('schema', _true_divide_schemas, ids=['float', 'double'])
def test_non_ansi_jit_true_divide(schema):
    def project(spark):
        result = spark.createDataFrame(_true_divide_rows * 8, schema).selectExpr('a / b')
        assert result.schema[0].dataType == DoubleType()
        assert result.schema[0].nullable
        return result

    assert_gpu_ast(True, project, conf=_non_ansi_jit_ast_enabled_conf)

@_requires_libcudf_jit
@approximate_float
@pytest.mark.parametrize('schema', _true_divide_schemas, ids=['float', 'double'])
def test_try_jit_true_divide(schema):
    def project(spark):
        result = spark.createDataFrame(_true_divide_rows * 8, schema).selectExpr(
            'try_divide(a, b)')
        assert result.schema[0].dataType == DoubleType()
        assert result.schema[0].nullable
        return result

    assert_gpu_ast(True, project, conf=_ansi_jit_ast_enabled_conf)

@_requires_libcudf_jit
@approximate_float
def test_ansi_jit_true_divide_falls_back_from_ast():
    assert_gpu_ast(False,
        lambda spark: spark.createDataFrame(
            [(13.5, 3.0), (-13.5, 3.0), (None, 1.0)] * 8,
            'a DOUBLE, b DOUBLE').selectExpr('a / b'),
        conf=_ansi_jit_ast_enabled_conf)

@_requires_libcudf_jit
@approximate_float
def test_true_divide_integral_inputs_fall_back_from_ast():
    assert_gpu_ast(False,
        lambda spark: spark.createDataFrame(
            [(13, 3), (-13, 3), (None, 1)] * 8,
            'a LONG, b LONG').selectExpr('a / b'),
        conf=_non_ansi_jit_ast_enabled_conf)

@_requires_libcudf_jit
@approximate_float
def test_try_true_divide_regular_gpu_project():
    assert_gpu_ast(False,
        lambda spark: spark.createDataFrame(
            _true_divide_rows * 8, 'a DOUBLE, b DOUBLE').selectExpr(
                'try_divide(a, b)', 'cast(null as STRING)'),
        conf=_ansi_jit_ast_enabled_conf)

@allow_non_gpu('Divide')
@approximate_float
def test_try_true_divide_falls_back_without_row_ir_jit():
    assert_gpu_fallback_collect(
        lambda spark: spark.createDataFrame(
            _true_divide_rows * 8, 'a DOUBLE, b DOUBLE').selectExpr(
                'try_divide(a, b)'),
        'Divide', conf=ansi_enabled_conf)

@_requires_libcudf_jit
@allow_non_gpu('Divide', 'Cast', 'Add')
def test_try_divide_side_effecting_lhs_falls_back():
    assert_gpu_fallback_collect(
        lambda spark: spark.createDataFrame(
            [(INT_MAX, 0.0), (INT_MAX, None)] * 8,
            'a INT, b DOUBLE').selectExpr(
                'try_divide(CAST(a + 1 AS DOUBLE), b)'),
        'Divide', conf=_ansi_jit_ast_enabled_conf)

@_requires_libcudf_jit
@allow_non_gpu('ProjectExec')
@pytest.mark.skipif(
    not is_before_spark_340() or is_databricks113_or_later(),
    reason='Spark 3.3 OSS represents try_divide with an outer TryEval')
def test_spark_330_try_divide_side_effecting_rhs_falls_back():
    assert_gpu_fallback_collect(
        lambda spark: spark.createDataFrame(
            [(1.0, INT_MAX), (None, INT_MAX)] * 8,
            'a DOUBLE, b INT').selectExpr(
                'try_divide(a, CAST(b + 1 AS DOUBLE))'),
        'TryEval', conf=_ansi_jit_ast_enabled_conf)

@_requires_libcudf_jit
@pytest.mark.parametrize('data_desc', _ast_div_mod_desc_list_for_ansi_jit_on, ids=idfn)
def test_ansi_jit_integral_div_mod_for_integer_ansi_on(data_desc):
    lhs_gen, rhs_gen = data_desc
    assert_gpu_ast(True,
                   lambda spark: two_col_df(spark, lhs_gen, rhs_gen).selectExpr(
                       'a DIV b',
                       'a % b'),
                   conf=_ansi_jit_ast_enabled_conf)

@_requires_libcudf_jit
def test_ansi_jit_integral_mod_sign_for_integer_ansi_on():
    assert_gpu_ast(True,
                   lambda spark: spark.createDataFrame(
                       spark.sparkContext.parallelize(
                           [(-5, 3), (5, -3), (-5, -3), (5, 3), (None, 3), (5, None)] * 8,
                           1),
                       'a INT, b INT').selectExpr('a % b'),
                   conf=_ansi_jit_ast_enabled_conf)

@_requires_libcudf_jit
@validate_execs_in_gpu_plan('GpuProjectAstExec')
@pytest.mark.parametrize('expression', ['a DIV b', 'a % b'], ids=['div', 'mod'])
def test_ansi_jit_integral_div_by_zero_errors(expression):
    ast_conf = copy_and_update(
        _ansi_jit_ast_enabled_conf,
        _project_ast_enabled_conf)
    df_fun = lambda spark: two_col_df(
        spark,
        LongGen(nullable=False, min_val=-100, max_val=100, special_cases=[]),
        SetValuesGen(LongType(), [0]),
        length=8,
        num_slices=1).selectExpr(expression).collect()
    assert_spark_exception(
        lambda: with_cpu_session(df_fun, ast_conf),
        'Division by zero')
    gpu_error_pattern = re.compile(
        rf'(?s)(?=.*Division by zero)(?=.*{re.escape(expression)})',
        re.IGNORECASE)
    assert_spark_exception(
        lambda: with_gpu_session(df_fun, ast_conf),
        gpu_error_pattern)

@_requires_libcudf_jit
@validate_execs_in_gpu_plan('GpuProjectAstExec')
def test_ansi_jit_integral_div_overflow_errors():
    ast_conf = copy_and_update(
        _ansi_jit_ast_enabled_conf,
        _project_ast_enabled_conf)
    df_fun = lambda spark: spark.createDataFrame(
        spark.sparkContext.parallelize([(LONG_MIN, -1)] * 8, 1),
        'a LONG, b LONG').selectExpr('a DIV b').collect()
    assert_spark_exception(
        lambda: with_cpu_session(df_fun, ast_conf),
        'Overflow')
    gpu_error_pattern = re.compile(
        r'(?s)(?=.*Overflow)(?=.*a DIV b)',
        re.IGNORECASE)
    assert_spark_exception(
        lambda: with_gpu_session(df_fun, ast_conf),
        gpu_error_pattern)

@_requires_libcudf_jit
def test_ansi_jit_integral_div_mod_null_dividend_zero_divisor():
    assert_gpu_ast(True,
        lambda spark: spark.createDataFrame(
            [(None, 0)] * 8,
            'a LONG, b LONG').selectExpr(
                'a DIV b',
                'a % b'),
        conf=_ansi_jit_ast_enabled_conf)

@_requires_libcudf_jit
@pytest.mark.parametrize('schema,min_value', [
    ('a INT, b INT', INT_MIN),
    ('a LONG, b LONG', LONG_MIN)], ids=['int', 'long'])
def test_ansi_jit_integral_mod_min_value_by_minus_one(schema, min_value):
    assert_gpu_ast(True,
        lambda spark: spark.createDataFrame(
            [(min_value, -1)] * 8,
            schema).selectExpr('a % b'),
        conf=_ansi_jit_ast_enabled_conf)

@approximate_float
def test_scalar_pow():
    # For the 'b' field include a lot more values that we would expect customers to use as a part of a pow
    data_gen = [('a', DoubleGen()),('b', DoubleGen().with_special_case(lambda rand: float(rand.randint(-16, 16)), weight=100.0))]
    assert_gpu_ast(is_supported=True,
        func=lambda spark: gen_df(spark, data_gen).selectExpr(
            'pow(a, 7.0)',
            'pow(-12.0, b)'))

@approximate_float
@pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/89')
@pytest.mark.parametrize('data_descr', ast_double_descr, ids=idfn)
def test_columnar_pow(data_descr):
    assert_binary_ast(data_descr, lambda df: df.selectExpr('pow(a, b)'))

@pytest.mark.parametrize('data_gen', boolean_gens, ids=idfn)
def test_and(data_gen):
    data_type = data_gen.data_type
    assert_gpu_ast(is_supported=True,
        func=lambda spark: binary_op_df(spark, data_gen).select(
            f.col('a') & f.lit(True),
            f.lit(False) & f.col('b'),
            f.col('a') & f.col('b')))

@_requires_libcudf_jit
def test_ansi_jit_and_fallible_rhs_falls_back_from_ast():
    assert_gpu_ast(False,
        lambda spark: spark.createDataFrame(
            [(False, INT_MAX)] * 8,
            'a BOOLEAN, b INT').selectExpr('a AND (b + 2 > 0)'),
        conf=_ansi_jit_ast_enabled_conf)

@pytest.mark.parametrize('data_gen', boolean_gens, ids=idfn)
def test_or(data_gen):
    data_type = data_gen.data_type
    assert_gpu_ast(is_supported=True,
                   func=lambda spark: binary_op_df(spark, data_gen).select(
                       f.col('a') | f.lit(True),
                       f.lit(False) | f.col('b'),
                       f.col('a') | f.col('b')))

@_requires_libcudf_jit
def test_ansi_jit_or_fallible_rhs_falls_back_from_ast():
    assert_gpu_ast(False,
        lambda spark: spark.createDataFrame(
            [(True, INT_MAX)] * 8,
            'a BOOLEAN, b INT').selectExpr('a OR (b + 2 > 0)'),
        conf=_ansi_jit_ast_enabled_conf)

@_requires_libcudf_jit
def test_ansi_jit_logical_fallible_lhs_uses_ast():
    assert_gpu_ast(True,
        lambda spark: spark.createDataFrame(
            [(False, 0), (True, 1)] * 4,
            'a BOOLEAN, b INT').selectExpr('(b + 2 > 0) AND a'),
        conf=_ansi_jit_ast_enabled_conf)

@ignore_order
@disable_ansi_mode
def test_multi_tier_ast():
    assert_gpu_ast(
        is_supported=True,
        # repartition is here to avoid Spark simplifying the expression
        func=lambda spark: spark.range(10).withColumn("x", f.col("id")).repartition(1)\
            .selectExpr("x", "(id < x) == (id < (id + x))"))


# MUST NOT use GPU AST when project refers to string type(non-fixed-width),
# or cudf::compute_column will throw error: Invalid, non-fixed-width type
# ANSI mode is disabled here due to an overflow issue with integer multiplication on Spark 4.0.0.
@disable_ansi_mode
@ignore_order(local=True)
def test_refer_to_non_fixed_width_column():
    gens = [('col_int', int_gen), ('col_string', string_gen)]
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: gen_df(spark, gens).selectExpr("col_int * col_int", "col_string"),
        conf=_project_ast_enabled_conf)
