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

from asserts import assert_gpu_and_cpu_are_equal_collect, assert_gpu_and_cpu_are_equal_sql, assert_cpu_and_gpu_are_equal_collect_with_capture
from data_gen import *
from spark_session import is_before_spark_320, is_jvm_charset_utf8, is_before_spark_400
from pyspark.sql.types import *
from marks import datagen_overrides, allow_non_gpu, disable_ansi_mode
import pyspark.sql.functions as f

# mark this test as ci_1 for mvn verify sanity check in pre-merge CI
pytestmark = pytest.mark.premerge_ci_1

def mk_str_gen(pattern):
    return StringGen(pattern).with_special_case('').with_special_pattern('.{0,10}')

all_gens = all_gen + [NullGen(), binary_gen]
all_nested_gens = array_gens_sample + [ArrayGen(BinaryGen(max_length=10), max_length=10)] + struct_gens_sample + map_gens_sample
all_nested_gens_nonempty_struct = array_gens_sample + nonempty_struct_gens_sample

# Create dedicated data gens of nested type for 'if' tests here with two exclusions:
#   1) Excludes the nested 'NullGen' because it seems to be impossible to convert the
#      'NullType' to a SQL type string. But the top level NullGen is handled specially
#      in 'gen_scalars_for_sql'.
#   2) Excludes the empty struct gen 'Struct()' because it leads to an error as below
#      in both cpu and gpu runs.
#      E: java.lang.AssertionError: assertion failed: each serializer expression should contain\
#         at least one `BoundReference`
if_array_gens_sample = [ArrayGen(sub_gen) for sub_gen in all_gen] + nested_array_gens_sample
if_struct_gen = StructGen([['child'+str(ind), sub_gen] for ind, sub_gen in enumerate(all_gen)])
if_struct_gens_sample = [if_struct_gen,
        StructGen([['child0', byte_gen], ['child1', if_struct_gen]]),
        StructGen([['child0', ArrayGen(short_gen)], ['child1', double_gen]])]
if_nested_gens = if_array_gens_sample + if_struct_gens_sample

@pytest.mark.parametrize('data_gen', all_gens + if_nested_gens, ids=idfn)
# https://github.com/NVIDIA/spark-rapids/issues/12019
@disable_ansi_mode
def test_if_else(data_gen):
    (s1, s2) = with_cpu_session(
        lambda spark: gen_scalars_for_sql(data_gen, 2, force_no_nulls=not isinstance(data_gen, NullGen)))
    null_lit = get_null_lit_string(data_gen.data_type)
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : three_col_df(spark, boolean_gen, data_gen, data_gen).selectExpr(
                'IF(TRUE, b, c)',
                'IF(TRUE, {}, {})'.format(s1, null_lit),
                'IF(FALSE, {}, {})'.format(s1, null_lit),
                'IF(a, b, c)',
                'IF(a, {}, c)'.format(s1),
                'IF(a, b, {})'.format(s2),
                'IF(a, {}, {})'.format(s1, s2),
                'IF(a, b, {})'.format(null_lit),
                'IF(a, {}, c)'.format(null_lit)))

# Maps scalars are not really supported by Spark from python without jumping through a lot of hoops
# so for now we are going to skip them
@pytest.mark.parametrize('data_gen', map_gens_sample, ids=idfn)
def test_if_else_map(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : three_col_df(spark, boolean_gen, data_gen, data_gen).selectExpr(
                'IF(TRUE, b, c)',
                'IF(a, b, c)'))

@pytest.mark.order(1) # at the head of xdist worker queue if pytest-order is installed
@pytest.mark.parametrize('data_gen', all_gens + all_nested_gens, ids=idfn)
def test_case_when(data_gen):
    num_cmps = 20
    s1 = with_cpu_session(
        lambda spark: gen_scalar(data_gen, force_no_nulls=not isinstance(data_gen, NullGen)))
    # we want lots of false
    bool_gen = BooleanGen().with_special_case(False, weight=1000.0)
    gen_cols = [('_b' + str(x), bool_gen) for x in range(0, num_cmps)]
    gen_cols = gen_cols + [('_c' + str(x), data_gen) for x in range(0, num_cmps)]
    gen = StructGen(gen_cols, nullable=False)
    command = f.when(f.col('_b0'), f.col('_c0'))
    for x in range(1, num_cmps):
        command = command.when(f.col('_b'+ str(x)), f.col('_c' + str(x)))
    command = command.otherwise(s1)
    data_type = data_gen.data_type
    # `command` covers the case of (column, scalar) for values, so the following 3 ones
    # are for
    #    (scalar, scalar)  -> the default `otherwise` is a scalar.
    #    (column, column)
    #    (scalar, column)
    # in sequence.
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : gen_df(spark, gen).select(command,
                f.when(f.col('_b0'), s1),
                f.when(f.col('_b0'), f.col('_c0')).otherwise(f.col('_c1')),
                f.when(f.col('_b0'), s1).otherwise(f.col('_c0')),
                f.when(f.col('_b0'), s1).when(f.lit(False), f.col('_c0')),
                f.when(f.col('_b0'), s1).when(f.lit(True), f.col('_c0')),
                f.when(f.col('_b0'), f.lit(None).cast(data_type)).otherwise(f.col('_c0')),
                f.when(f.lit(False), f.col('_c0'))))

@pytest.mark.parametrize('data_gen', [float_gen, double_gen], ids=idfn)
def test_nanvl(data_gen):
    s1 = with_cpu_session(
        lambda spark: gen_scalar(data_gen, force_no_nulls=not isinstance(data_gen, NullGen)))
    data_type = data_gen.data_type
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : binary_op_df(spark, data_gen).select(
                f.nanvl(f.col('a'), f.col('b')),
                f.nanvl(f.col('a'), s1.cast(data_type)),
                f.nanvl(f.lit(None).cast(data_type), f.col('b')),
                f.nanvl(f.lit(float('nan')).cast(data_type), f.col('b'))))

@pytest.mark.parametrize('data_gen', all_basic_gens + decimal_gens, ids=idfn)
# https://github.com/NVIDIA/spark-rapids/issues/12019
@disable_ansi_mode
def test_nvl(data_gen):
    (s1, s2) = with_cpu_session(
        lambda spark: gen_scalars_for_sql(data_gen, 2, force_no_nulls=not isinstance(data_gen, NullGen)))
    null_lit = get_null_lit_string(data_gen.data_type)
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : binary_op_df(spark, data_gen).selectExpr(
                'nvl(a, b)',
                'nvl(a, {})'.format(s2),
                'nvl({}, b)'.format(s1),
                'nvl({}, b)'.format(null_lit),
                'nvl(a, {})'.format(null_lit)))

#nvl is translated into a 2 param version of coalesce
# Exclude the empty struct gen 'Struct()' because it leads to an error as below
# in both cpu and gpu runs.
#      E: java.lang.AssertionError: assertion failed: each serializer expression should contain\
#         at least one `BoundReference`
@pytest.mark.parametrize('data_gen', all_gens + all_nested_gens_nonempty_struct + map_gens_sample, ids=idfn)
def test_coalesce(data_gen):
    num_cols = 20
    s1 = with_cpu_session(
        lambda spark: gen_scalar(data_gen, force_no_nulls=not isinstance(data_gen, NullGen)))
    # we want lots of nulls
    gen = StructGen([('_c' + str(x), data_gen.copy_special_case(None, weight=1000.0))
        for x in range(0, num_cols)], nullable=False)
    command_args = [f.col('_c' + str(x)) for x in range(0, num_cols)]
    command_args.append(s1)
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : gen_df(spark, gen).select(
                f.coalesce(*command_args)))

def test_coalesce_constant_output():
    # Coalesce can allow a constant value as output. Technically Spark should mark this
    # as foldable and turn it into a constant, but it does not, so make sure our code
    # can deal with it.  (This means something like + will get two constant scalar values)
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : spark.range(1, 100).selectExpr("4 + coalesce(5, id) as nine"))

def test_coalesce_foldable_non_literal():
    # A foldable coalesce(cast(null as T), lit) can reach the planner unfolded (e.g. when AQE
    # regenerates it; the AQE-time optimizer runs neither ConstantFolding nor NullPropagation).
    # Reproduced deterministically by excluding both rules so the coalesce survives to tagging.
    # It must still run on GPU rather than forcing the project onto the CPU.
    conf = {
        'spark.sql.optimizer.excludedRules':
            'org.apache.spark.sql.catalyst.optimizer.ConstantFolding,'
            'org.apache.spark.sql.catalyst.optimizer.NullPropagation',
        'spark.sql.adaptive.enabled': 'false',  # deterministic plan capture
    }
    assert_cpu_and_gpu_are_equal_collect_with_capture(
            lambda spark : spark.range(0, 100).selectExpr(
                'coalesce(cast(null as bigint), -1001) as c0',
                'coalesce(cast(null as int), 100) as c1',
                'id'),
            exist_classes='GpuCoalesce',
            conf=conf)

@pytest.mark.parametrize('data_gen', all_basic_gens + decimal_gens, ids=idfn)
# https://github.com/NVIDIA/spark-rapids/issues/12019
@disable_ansi_mode
def test_nvl2(data_gen):
    (s1, s2) = with_cpu_session(
        lambda spark: gen_scalars_for_sql(data_gen, 2, force_no_nulls=not isinstance(data_gen, NullGen)))
    null_lit = get_null_lit_string(data_gen.data_type)
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : three_col_df(spark, data_gen, data_gen, data_gen).selectExpr(
                'nvl2(a, b, c)',
                'nvl2(a, b, {})'.format(s2),
                'nvl2({}, b, c)'.format(s1),
                'nvl2({}, b, c)'.format(null_lit),
                'nvl2(a, {}, c)'.format(null_lit)))

@pytest.mark.parametrize('data_gen', eq_gens_with_decimal_gen, ids=idfn)
# https://github.com/NVIDIA/spark-rapids/issues/12019
@disable_ansi_mode
def test_nullif(data_gen):
    (s1, s2) = with_cpu_session(
        lambda spark: gen_scalars_for_sql(data_gen, 2, force_no_nulls=not isinstance(data_gen, NullGen)))
    null_lit = get_null_lit_string(data_gen.data_type)
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : binary_op_df(spark, data_gen).selectExpr(
                'nullif(a, b)',
                'nullif(a, {})'.format(s2),
                'nullif({}, b)'.format(s1),
                'nullif({}, b)'.format(null_lit),
                'nullif(a, {})'.format(null_lit)))

@pytest.mark.parametrize('data_gen', eq_gens_with_decimal_gen, ids=idfn)
# https://github.com/NVIDIA/spark-rapids/issues/12019
@disable_ansi_mode
def test_ifnull(data_gen):
    (s1, s2) = with_cpu_session(
        lambda spark: gen_scalars_for_sql(data_gen, 2, force_no_nulls=not isinstance(data_gen, NullGen)))
    null_lit = get_null_lit_string(data_gen.data_type)
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : binary_op_df(spark, data_gen).selectExpr(
                'ifnull(a, b)',
                'ifnull(a, {})'.format(s2),
                'ifnull({}, b)'.format(s1),
                'ifnull({}, b)'.format(null_lit),
                'ifnull(a, {})'.format(null_lit)))

@pytest.mark.parametrize('data_gen', [IntegerGen().with_special_case(2147483647)], ids=idfn)
def test_conditional_with_side_effects_col_col(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).selectExpr(
                'IF(a < 2147483647, a + 1, a)'),
            conf = ansi_enabled_conf)

@pytest.mark.parametrize('data_gen', [IntegerGen().with_special_case(2147483647)], ids=idfn)
def test_conditional_with_side_effects_col_scalar(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).selectExpr(
                'IF(a < 2147483647, a + 1, 2147483647)',
                'IF(a >= 2147483646, 2147483647, a + 1)'),
            conf = ansi_enabled_conf)

@pytest.mark.parametrize('data_gen', [mk_str_gen('[0-9]{1,20}')], ids=idfn)
@pytest.mark.skipif(not is_jvm_charset_utf8(), reason="regular expressions require UTF-8")
def test_conditional_with_side_effects_cast(data_gen):
    test_conf=copy_and_update(
        ansi_enabled_conf, {'spark.rapids.sql.regexp.enabled': True})
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).selectExpr(
                'IF(a RLIKE "^[0-9]{5,}", CAST(SUBSTR(a, 0, 5) AS INT), 0)'),
            conf = test_conf)

@pytest.mark.parametrize('data_gen', [mk_str_gen('[0-9]{1,9}')], ids=idfn)
@pytest.mark.skipif(not is_jvm_charset_utf8(), reason="regular expressions require UTF-8")
def test_conditional_with_side_effects_case_when(data_gen):
    test_conf=copy_and_update(
        ansi_enabled_conf, {'spark.rapids.sql.regexp.enabled': True})
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).selectExpr(
                'CASE \
                WHEN a RLIKE "^[0-9]{6}" THEN CAST(SUBSTR(a, 0, 6) AS INT) + 123 \
                WHEN a RLIKE "^[0-9]{3}" THEN CAST(SUBSTR(a, 0, 3) AS INT) \
                ELSE -1 END'),
                conf = test_conf)

@pytest.mark.parametrize('data_gen', [mk_str_gen('[a-z]{0,3}')], ids=idfn)
@allow_non_gpu(*non_utc_allow)
def test_conditional_with_side_effects_sequence(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : unary_op_df(spark, data_gen).selectExpr(
            'CASE \
            WHEN length(a) > 0 THEN sequence(1, length(a), 1) \
            ELSE null END'),
        conf = ansi_enabled_conf)

@pytest.mark.skipif(is_before_spark_320(), reason='Earlier versions of Spark cannot cast sequence to string')
@pytest.mark.parametrize('data_gen', [mk_str_gen('[a-z]{0,3}')], ids=idfn)
@allow_non_gpu(*non_utc_allow)
def test_conditional_with_side_effects_sequence_cast(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : unary_op_df(spark, data_gen).selectExpr(
            'CASE \
            WHEN length(a) > 0 THEN CAST(sequence(1, length(a), 1) AS STRING) \
            ELSE null END'),
        conf = ansi_enabled_conf)

@pytest.mark.parametrize('data_gen', [ArrayGen(mk_str_gen('[a-z]{0,3}'))], ids=idfn)
@pytest.mark.parametrize('ansi_enabled', ['true', 'false'])
def test_conditional_with_side_effects_element_at(data_gen, ansi_enabled):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : unary_op_df(spark, data_gen).selectExpr(
            'CASE WHEN size(a) > 1 THEN element_at(a, 2) ELSE null END'),
        conf = {'spark.sql.ansi.enabled': ansi_enabled})

@pytest.mark.parametrize('data_gen', [ArrayGen(mk_str_gen('[a-z]{0,3}'))], ids=idfn)
@pytest.mark.parametrize('ansi_enabled', ['true', 'false'])
def test_conditional_with_side_effects_array_index(data_gen, ansi_enabled):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : unary_op_df(spark, data_gen).selectExpr(
            'CASE WHEN size(a) > 1 THEN a[1] ELSE null END'),
        conf = {'spark.sql.ansi.enabled': ansi_enabled})

@pytest.mark.parametrize('map_gen',
                         [MapGen(StringGen(pattern='key_[0-9]', nullable=False),
                                 mk_str_gen('[a-z]{0,3}'), max_length=6)])
@pytest.mark.parametrize('data_gen', [StringGen(pattern='neverempty_[0-9]', nullable=False)])
@pytest.mark.parametrize('ansi_enabled', ['true', 'false'])
def test_conditional_with_side_effects_map_key_not_found(map_gen, data_gen, ansi_enabled):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: two_col_df(spark, map_gen, data_gen).selectExpr(
            'CASE WHEN length(b) = 0 THEN a["not_found"] ELSE null END'),
        conf = {'spark.sql.ansi.enabled': ansi_enabled})

@pytest.mark.parametrize('data_gen', [ShortGen().with_special_case(SHORT_MIN)], ids=idfn)
@pytest.mark.parametrize('ansi_enabled', ['true', 'false'])
def test_conditional_with_side_effects_abs(data_gen, ansi_enabled):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : unary_op_df(spark, data_gen).selectExpr(
            'CASE WHEN a > -32768 THEN abs(a) ELSE null END'),
        conf = {'spark.sql.ansi.enabled': ansi_enabled})

@pytest.mark.parametrize('data_gen', [ShortGen().with_special_case(SHORT_MIN)], ids=idfn)
@pytest.mark.parametrize('ansi_enabled', ['true', 'false'])
def test_conditional_with_side_effects_unary_minus(data_gen, ansi_enabled):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : unary_op_df(spark, data_gen).selectExpr(
            'CASE WHEN a > -32768 THEN -a ELSE null END'),
        conf = {'spark.sql.ansi.enabled': ansi_enabled})

_case_when_scalars = [
    ['True', 'False', 'null', 'True', 'False'],
    ['CAST(1 AS TINYINT)', 'CAST(2 AS TINYINT)', 'CAST(3 AS TINYINT)', 'CAST(4 AS TINYINT)', 'CAST(5 AS TINYINT)'],
    ['CAST(1 AS SMALLINT)', 'CAST(2 AS SMALLINT)', 'CAST(3 AS SMALLINT)', 'CAST(4 AS SMALLINT)', 'CAST(5 AS SMALLINT)'],
    ['1', '2', '3', '4', '5'],
    ['CAST(1 AS BIGINT)',          'CAST(2 AS BIGINT)',          'CAST(3 AS BIGINT)',          'CAST(4 AS BIGINT)',          'CAST(5 AS BIGINT)'],
    ['CAST(1.1 AS FLOAT)',         'CAST(2.2 AS FLOAT)',         'CAST(3.3 AS FLOAT)',         'CAST(4.4 AS FLOAT)',         'CAST(5.5 AS FLOAT)'],
    ['CAST(1.1 AS DOUBLE)',        'CAST(2.2 AS DOUBLE)',        'CAST(3.3 AS DOUBLE)',        'CAST(4.4 AS DOUBLE)',        'CAST(5.5 AS DOUBLE)'],
    ["'str_value1'",               "'str_value2'",               "'str_value3'",               "'str_value4'",               "'str_else'"],
    ['null',  'CAST(2.2 AS DECIMAL(7,3))',  'CAST(3.3 AS DECIMAL(7,3))',  'CAST(4.4 AS DECIMAL(7,3))',  'CAST(5.5 AS DECIMAL(7,3))'], # null and decimal(7)
    ['null',  'CAST(2.2 AS DECIMAL(12,2))',  'CAST(3.3 AS DECIMAL(7,3))',  'CAST(4.4 AS DECIMAL(7,3))',  'CAST(5.5 AS DECIMAL(7,3))'], # decimal(7) and decimal(12)
    ['CAST(1.1 AS DECIMAL(12,2))', 'CAST(2.2 AS DECIMAL(12,2))', 'CAST(3.3 AS DECIMAL(20,2))', 'CAST(4.4 AS DECIMAL(12,2))', 'CAST(5.5 AS DECIMAL(12,2))'], # decimal(12) and decimal(20)
    ['CAST(1.1 AS DECIMAL(20,2))', 'CAST(2.2 AS DECIMAL(20,2))', 'CAST(3.3 AS DECIMAL(20,2))', 'CAST(4.4 AS DECIMAL(20,2))', 'CAST(5.5 AS DECIMAL(20,2))'], # decimal(20)
]
@pytest.mark.parametrize('case_when_scalars', _case_when_scalars, ids=idfn)
def test_case_when_all_then_values_are_scalars(case_when_scalars):
    data_gen = [
        ("a", boolean_gen),
        ("b", boolean_gen),
        ("c", boolean_gen),
        ("d", boolean_gen),
        ("e", boolean_gen)
    ]
    sql =  """
            select case
                when a then {}
                when b then {}
                when c then {}
                when d then {}
                else {}
            end
            from tab
            """
    assert_gpu_and_cpu_are_equal_sql(
        lambda spark : gen_df(spark, data_gen),
        "tab",
        sql.format(case_when_scalars[0], case_when_scalars[1], case_when_scalars[2], case_when_scalars[3], case_when_scalars[4]),
        conf = {'spark.rapids.sql.case_when.fuse': 'true'})

# test corner cases:
#  - when exprs has nulls
#  - else expr is null
def test_case_when_all_then_values_are_scalars_with_nulls():
    bool_rows = [(True, False, False, None),
                 (False, True, True, None), # the second true will enable `when b then null` branch
                 (False, False, None, None),
                 (None, None, True, False),
                 (False, False, False, False),
                 (None, None, None, None)]
    sql =  """
            select case 
                when a then 'aaa' 
                when b then null
                when c then 'ccc' 
                when d then 'ddd' 
                else {}
            end
            from tab
            """
    sql_without_else =  """
            select case 
                when a then cast(1.1 as decimal(7,2))
                when b then null
                when c then cast(3.3 as decimal(7,2))
                when d then cast(4.4 as decimal(7,2))
            end
            from tab
            """
    assert_gpu_and_cpu_are_equal_sql(
        lambda spark: spark.createDataFrame(bool_rows, "a boolean, b boolean, c boolean, d boolean"),
        "tab",
        sql.format("'unknown'"),
        conf = {'spark.rapids.sql.case_when.fuse': 'true'})
    assert_gpu_and_cpu_are_equal_sql(
        lambda spark: spark.createDataFrame(bool_rows, "a boolean, b boolean, c boolean, d boolean"),
        "tab",
        sql.format("null"), # set else as null
        conf = {'spark.rapids.sql.case_when.fuse': 'true'})
    assert_gpu_and_cpu_are_equal_sql(
        lambda spark: spark.createDataFrame(bool_rows, "a boolean, b boolean, c boolean, d boolean"),
        "tab",
        sql_without_else,
        conf = {'spark.rapids.sql.case_when.fuse': 'true'})

@pytest.mark.parametrize('combine_string_contains_enabled', [True, False])
def test_combine_string_contains_in_case_when(combine_string_contains_enabled):
    data_gen = [("c1", string_gen)]
    sql =  """
            SELECT
                CASE
                     WHEN INSTR(c1, 'a') > 0 THEN 'a'
                     WHEN INSTR(c1, 'b') > 0 THEN 'b'
                     WHEN INSTR(c1, 'c') > 0 THEN 'c'
                     ELSE ''
                END as output_1,
                CASE
                     WHEN INSTR(c1, 'c') > 0 THEN 'c'
                     WHEN INSTR(c1, 'd') > 0 THEN 'd'
                     WHEN INSTR(c1, 'e') > 0 THEN 'e'
                     ELSE ''
                END as output_2
            from tab
            """
    # spark.rapids.sql.combined.expressions.enabled is true by default
    assert_gpu_and_cpu_are_equal_sql(
        lambda spark : gen_df(spark, data_gen),
        "tab",
        sql,
        { "spark.rapids.sql.expression.combined.GpuContains" : combine_string_contains_enabled}
    )


def test_case_when_with_side_effect_in_else():
    sql = """
        SELECT
            a,
            CASE
                WHEN size(a) > 0 THEN NULL
                ELSE element_at(a, 0)
            END
        FROM else_side_effect
    """

    assert_gpu_and_cpu_are_equal_sql(
        lambda spark: unary_op_df(spark, SetValuesGen(ArrayType(IntegerType()), [[1, 2, 3]]), 10),
        "else_side_effect",
        sql
    )

_data_gen_for_between = [("c1", string_gen), ("c2", byte_gen), ("c3", short_gen), ("c4", int_gen),
                         ("c5", long_gen), ("c6", float_gen), ("c7", double_gen)]

# test between expr:  between(x, min, max)
@pytest.mark.skipif(is_before_spark_400(), reason="Only supports Spark versions: 400 and 400+")
def test_between_expr():
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : gen_df(spark, _data_gen_for_between).selectExpr(
            "BETWEEN(c1, 'a', NULL)",
            "BETWEEN(c1, 'a', 'Z')",
            "BETWEEN(c2, CAST(1 as byte), cast(127 as byte))",
            "BETWEEN(c3, cast(-1 as short), cast(1024 as short))",
            "BETWEEN(c4, 2048, 4096)",
            "BETWEEN(c5, cast(4096 as long), cast(1000000 as long))",
            "BETWEEN(c6, CAST(1.1 as float), CAST(90000.02 as float))",
            "BETWEEN(c7, 1.2, 1234576.11)"))

# test between sql:  x between min and max
def test_between_sql():
    sql =  """
        SELECT
            c1 BETWEEN 'a' AND NULL,
            c1 BETWEEN 'a' AND 'Z',
            c2 BETWEEN CAST(1 as byte) AND cast(127 as byte),
            c3 BETWEEN cast(-1 as short) AND cast(1024 as short),
            c4 BETWEEN 2048 AND 4096,
            c5 BETWEEN cast(4096 as long) AND cast(1000000 as long),
            c6 BETWEEN CAST(1.1 as float) AND CAST(90000.02 as float),
            c7 BETWEEN 1.2 AND 1234576.11
        from tab
    """
    assert_gpu_and_cpu_are_equal_sql(
        lambda spark : gen_df(spark, _data_gen_for_between),
        "tab",
        sql)
