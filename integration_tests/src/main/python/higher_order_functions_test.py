# Copyright (c) 2023-2026, NVIDIA CORPORATION.
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

from asserts import assert_gpu_and_cpu_are_equal_collect, assert_gpu_fallback_collect
from data_gen import *
from marks import allow_non_gpu, allow_non_gpu_conditional, disable_ansi_mode, ignore_order
from spark_session import is_before_spark_340, is_databricks_runtime


@ignore_order(local=True)
def test_tiered_project_with_complex_transform():
    confs = {"spark.rapids.sql.tiered.project.enabled": "true"}
    def do_project(spark):
        df = spark.createDataFrame(
            [
                (1, "a", [(0, "z"), (1, "y")]),
                (2, "b", [(2, "x")])
            ],
            "a int, b string, c array<struct<x: int, y: string>>").repartition(2)
        return df.selectExpr(
            "transform(c, (v, i) -> named_struct('x', c[i].x, 'y', c[i].y)) AS t")
    assert_gpu_and_cpu_are_equal_collect(do_project, conf=confs)


@pytest.mark.parametrize('lambda_sql, init_sql, gen_max', [
    ('(acc, x) -> acc + CAST(x as BIGINT)', '0L', 100),
    ('(acc, x) -> acc * CAST(x as BIGINT)', '1L', 3),
    ('(acc, x) -> greatest(acc, CAST(x as BIGINT))', '-9223372036854775808L', 100),
    ('(acc, x) -> least(acc, CAST(x as BIGINT))', '9223372036854775807L', 100),
], ids=['sum', 'product', 'max', 'min'])
@disable_ansi_mode
def test_array_aggregate_numeric_ops(lambda_sql, init_sql, gen_max):
    gen = IntegerGen(min_val=-gen_max, max_val=gen_max)
    def do_it(spark):
        return unary_op_df(spark, ArrayGen(gen, max_length=8)).selectExpr(
            f'aggregate(a, {init_sql}, {lambda_sql}) as res')
    assert_gpu_and_cpu_are_equal_collect(do_it)


@pytest.mark.parametrize('gen, lambda_sql, init_sql', [
    (IntegerGen(min_val=-100, max_val=100), '(acc, x) -> acc + x', '0'),
    (LongGen(min_val=-100, max_val=100), '(acc, x) -> acc + x', '0L'),
    (IntegerGen(min_val=-100, max_val=100),
        '(acc, x) -> greatest(acc, x)', 'CAST(-9999 as INT)'),
    (LongGen(min_val=-100, max_val=100),
        '(acc, x) -> least(acc, x)', '9223372036854775807L'),
], ids=['int-sum', 'long-sum', 'int-max', 'long-min'])
@disable_ansi_mode
def test_array_aggregate_native_integer_ops(gen, lambda_sql, init_sql):
    def do_it(spark):
        return unary_op_df(spark, ArrayGen(gen, max_length=8)).selectExpr(
            f'aggregate(a, {init_sql}, {lambda_sql}) as res')
    assert_gpu_and_cpu_are_equal_collect(do_it)


# Elements are non-null because the tag-time guard falls back to CPU when the element type
# is nullable. ALL/ANY don't overflow so ANSI mode doesn't change behaviour.
@pytest.mark.parametrize('lambda_sql, init_sql', [
    ('(acc, x) -> acc AND x', 'true'),
    ('(acc, x) -> acc OR x', 'false'),
], ids=['all', 'any'])
def test_array_aggregate_boolean_ops(lambda_sql, init_sql):
    non_null_bool = BooleanGen(nullable=False)
    def do_it(spark):
        return unary_op_df(spark, ArrayGen(non_null_bool, max_length=8)).selectExpr(
            f'aggregate(a, {init_sql}, {lambda_sql}) as res')
    assert_gpu_and_cpu_are_equal_collect(do_it)


def test_array_aggregate_boolean_ops_nullable_zero():
    def do_it(spark):
        return spark.sql("""
            SELECT
              aggregate(array(false), CAST(NULL AS BOOLEAN), (acc, x) -> acc AND x) as all_false,
              aggregate(array(true), CAST(NULL AS BOOLEAN), (acc, x) -> acc AND x) as all_true,
              aggregate(array(true), CAST(NULL AS BOOLEAN), (acc, x) -> acc OR x) as any_true,
              aggregate(array(false), CAST(NULL AS BOOLEAN), (acc, x) -> acc OR x) as any_false
            """)
    assert_gpu_and_cpu_are_equal_collect(do_it)


@pytest.mark.parametrize('lambda_sql, init_sql', [
    ('(acc, x) -> acc AND x', 'true'),
    ('(acc, x) -> acc OR x', 'false'),
], ids=['all', 'any'])
@allow_non_gpu('ArrayAggregate', 'LambdaFunction', 'NamedLambdaVariable', 'And', 'Or')
def test_array_aggregate_boolean_ops_nullable_elements_fallback(lambda_sql, init_sql):
    assert_gpu_fallback_collect(
        lambda spark: unary_op_df(spark, ArrayGen(boolean_gen, max_length=8)).selectExpr(
            f'aggregate(a, {init_sql}, {lambda_sql}) as res'),
        'ArrayAggregate')


@pytest.mark.parametrize('lambda_sql, init_sql', [
    ('''(acc, x) -> acc AND
          CASE WHEN x THEN CAST(NULL AS BOOLEAN) ELSE false END''', 'true'),
    ('''(acc, x) -> acc OR
          CASE WHEN x THEN CAST(NULL AS BOOLEAN) ELSE true END''', 'false'),
], ids=['all', 'any'])
@allow_non_gpu('ArrayAggregate', 'LambdaFunction', 'NamedLambdaVariable', 'And', 'Or', 'CaseWhen')
def test_array_aggregate_boolean_ops_nullable_g_fallback(lambda_sql, init_sql):
    non_null_bool = BooleanGen(nullable=False)
    assert_gpu_fallback_collect(
        lambda spark: unary_op_df(spark, ArrayGen(non_null_bool, max_length=8)).selectExpr(
            f'aggregate(a, {init_sql}, {lambda_sql}) as res'),
        'ArrayAggregate')


@disable_ansi_mode
def test_array_aggregate_count_if_int():
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: unary_op_df(spark, ArrayGen(int_gen, max_length=15)).selectExpr(
            'aggregate(a, 0, (acc, x) -> acc + CASE WHEN x > 0 THEN 1 ELSE 0 END) as pos_cnt',
            'aggregate(a, 0L, (acc, x) -> acc + CAST(CASE WHEN x IS NULL THEN 1 ELSE 0 END as BIGINT)) as null_cnt'))


@disable_ansi_mode
def test_array_heterogeneous_elementwise_hof_mixed_project():
    data_gen = ArrayGen(IntegerGen(min_val=-10, max_val=10), max_length=8)
    def do_it(spark):
        outer_gen = IntegerGen(min_val=-5, max_val=5)
        return three_col_df(spark, data_gen, outer_gen, outer_gen).selectExpr(
            'a',
            'b',
            'c',
            'transform(a, item -> item + b) as plus_b',
            'transform(a, item -> item + c) as plus_c',
            'filter(a, item -> item is not null and item + b >= c) as filtered_b_ge_c',
            'exists(a, item -> item is not null and item + c < b) as has_c_less_b')

    assert_gpu_and_cpu_are_equal_collect(do_it)


@disable_ansi_mode
def test_array_hof_project_with_disjoint_outer_column_groups():
    data_gen = ArrayGen(IntegerGen(min_val=-10, max_val=10), max_length=8)
    def do_it(spark):
        outer_gen = IntegerGen(min_val=-5, max_val=5)
        return three_col_df(spark, data_gen, outer_gen, outer_gen).selectExpr(
            'transform(a, item -> item + b) as plus_b',
            'transform(a, item -> item + c) as plus_c',
            'filter(a, item -> item is not null and item + b >= 0) as non_negative_b',
            'exists(a, item -> item is not null and item + c < 0) as has_negative_c')

    assert_gpu_and_cpu_are_equal_collect(do_it)


@disable_ansi_mode
def test_array_hof_mixed_project_with_aggregate():
    data_gen = ArrayGen(IntegerGen(min_val=-10, max_val=10), max_length=8)
    def do_it(spark):
        return unary_op_df(spark, data_gen).selectExpr(
            'transform(a, x -> x + 1) as plus_one',
            'filter(a, x -> x is not null and x >= 0) as non_negative',
            'exists(a, x -> x is not null and x < 0) as has_negative',
            '''aggregate(a, 0L,
                 (acc, x) -> acc + CAST(CASE WHEN x IS NULL THEN 0 ELSE x END AS BIGINT))
               as sum_or_zero''')

    assert_gpu_and_cpu_are_equal_collect(do_it)


@disable_ansi_mode
def test_array_hof_mixed_project_with_aggregate_outer_state():
    data_gen = ArrayGen(IntegerGen(min_val=-10, max_val=10), max_length=8)
    outer_gen = LongGen(min_val=-3, max_val=3, nullable=False)
    def do_it(spark):
        return two_col_df(spark, data_gen, outer_gen).selectExpr(
            'transform(a, x -> coalesce(x, 0) + CAST(b AS INT)) as plus_b',
            '''aggregate(a, b, (acc, x) -> acc +
                 CAST(coalesce(x, 0) + CAST(b AS INT) AS BIGINT)) as sum_plus_b''')

    assert_gpu_and_cpu_are_equal_collect(do_it)


@disable_ansi_mode
def test_array_hof_mixed_project_with_indexed_lambdas():
    data_gen = ArrayGen(IntegerGen(min_val=-10, max_val=10), max_length=8)
    outer_gen = IntegerGen(min_val=-3, max_val=3, nullable=False)
    def do_it(spark):
        return two_col_df(spark, data_gen, outer_gen).selectExpr(
            'transform(a, (x, i) -> coalesce(x, 0) + i + b) as indexed_add',
            'filter(a, (x, i) -> x is not null and x + i + b >= 0) as indexed_filter',
            'transform(a, (x, i) -> i - coalesce(x, 0)) as index_minus_value')

    assert_gpu_and_cpu_are_equal_collect(do_it)


# `if(cond, acc + t, acc)` shape — branches lifted via op identity. Same count-if
# pattern as above but written naturally instead of using `CASE WHEN ... THEN 1 ELSE 0`.
@disable_ansi_mode
def test_array_aggregate_if_count():
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: unary_op_df(spark, ArrayGen(int_gen, max_length=15)).selectExpr(
            'aggregate(a, 0L, (acc, x) -> if(x > 0, acc + 1L, acc)) as pos_cnt',
            'aggregate(a, 0L, (acc, x) -> if(x is null, acc, acc + 1L)) as nonnull_cnt'))


@disable_ansi_mode
def test_array_aggregate_if_mixed_acc_sides():
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: unary_op_df(spark, ArrayGen(int_gen, max_length=15)).selectExpr(
            '''aggregate(a, 0L,
                 (acc, x) -> if(x > 0, acc + CAST(x AS BIGINT), 1L + acc)) as res'''))


# CaseWhen with several acc+t branches and a bare-acc else.
@disable_ansi_mode
def test_array_aggregate_casewhen_multi_branch():
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: unary_op_df(spark, ArrayGen(int_gen, max_length=15)).selectExpr(
            '''aggregate(a, 0L,
                 (acc, x) -> CASE
                   WHEN x > 100 THEN acc + 10L
                   WHEN x > 0 THEN acc + 1L
                   ELSE acc
                 END) as weighted_cnt'''))


@disable_ansi_mode
def test_array_aggregate_with_filter_and_split():
    field_gen = StringGen('[a-z]{2}')
    def do_it(spark):
        df = unary_op_df(spark, ArrayGen(field_gen, max_length=5))
        return df.selectExpr("""
            aggregate(
              filter(transform(a, x -> concat_ws('    ', x, x, x, x, x)), z -> z != ''),
              0L,
              (acc, z) -> acc + CAST(CASE WHEN (
                size(split(z, '    ', -1)) > 2
                AND split(z, '    ', -1)[2] IN ('aa', 'bb')
                AND NOT split(z, '    ', -1)[1] IN ('xx', 'yy')
              ) THEN 1 ELSE 0 END as BIGINT),
              id -> id
            ) as cnt""")
    assert_gpu_and_cpu_are_equal_collect(do_it)


@disable_ansi_mode
def test_array_aggregate_filtered_struct_with_nested_array_children():
    element_gen = StructGen([
        ('product_id', IntegerGen(nullable=False, min_val=0, max_val=2, special_cases=[0, 1, 2])),
        ('score', IntegerGen(nullable=False, min_val=-100, max_val=100)),
        ('nums', ArrayGen(IntegerGen(nullable=False), max_length=5, nullable=False)),
        ('labels', ArrayGen(StringGen('[a-z]{1,3}', nullable=False),
            max_length=5, nullable=False))
    ], nullable=False)
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: unary_op_df(spark, ArrayGen(element_gen, max_length=8)).selectExpr(
            '''aggregate(
                 filter(a, ad -> ad.product_id = 1),
                 0L,
                 (acc, ad) -> acc + CAST(ad.score AS BIGINT),
                 id -> id) as total_score'''))


@disable_ansi_mode
def test_array_aggregate_filtered_struct_with_nested_map_children():
    element_gen = StructGen([
        ('product_id', IntegerGen(nullable=False, min_val=0, max_val=2, special_cases=[0, 1, 2])),
        ('score', IntegerGen(nullable=False, min_val=-100, max_val=100)),
        ('attrs', MapGen(StringGen('key_[0-9]', nullable=False),
            IntegerGen(nullable=False), max_length=5, nullable=False))
    ], nullable=False)
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: unary_op_df(spark, ArrayGen(element_gen, max_length=8)).selectExpr(
            '''aggregate(
                 filter(a, ad -> ad.product_id = 1),
                 0L,
                 (acc, ad) -> acc +
                   (CAST(ad.score AS BIGINT) + CAST(size(ad.attrs) AS BIGINT)),
                 id -> id) as score_and_attr_count'''))


@pytest.mark.parametrize('element_gen', [
    ArrayGen(IntegerGen(nullable=False), max_length=5, nullable=False),
    MapGen(StringGen('key_[0-9]', nullable=False),
        IntegerGen(nullable=False), max_length=5, nullable=False)
], ids=['array-element', 'map-element'])
@disable_ansi_mode
def test_array_aggregate_direct_nested_collection_elements(element_gen):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: unary_op_df(spark, ArrayGen(element_gen, max_length=8)).selectExpr(
            'aggregate(a, 0L, (acc, x) -> acc + CAST(size(x) AS BIGINT), id -> id) '
            'as total_size'))


@disable_ansi_mode
def test_array_aggregate_nested_filter_and_aggregate_over_struct_array_field():
    charge_info_gen = StringGen(
        '[0-9]{1,2}\t[a-z]{2}\t(IGN_ZTC_CPA_CPC|MISS|-)', nullable=False
    ).with_special_case('', weight=5.0)
    element_gen = StructGen([
        ('product_id', IntegerGen(nullable=False, min_val=0, max_val=2, special_cases=[0, 1, 2])),
        ('im_ad_res_field', StructGen([
            ('charge_info', ArrayGen(charge_info_gen, max_length=5, nullable=False))
        ])),
        ('unused_ids', ArrayGen(IntegerGen(nullable=False), max_length=5, nullable=False))
    ], nullable=False)
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: unary_op_df(spark, ArrayGen(element_gen, max_length=8)).selectExpr("""
            aggregate(
              filter(a, ad -> ad.product_id = 1 AND ad.im_ad_res_field IS NOT NULL),
              0L,
              (acc, ad) -> acc + coalesce(
                aggregate(
                  filter(ad.im_ad_res_field.charge_info, z -> z != ''),
                  0L,
                  (acc2, z) -> acc2 + CASE WHEN (
                    size(split(z, '\t', -1)) > 2
                    AND split(z, '\t', -1)[2] IN ('-', 'IGN_ZTC_CPA_CPC')
                  ) THEN CAST(split(z, '\t', -1)[0] AS BIGINT) ELSE 0L END,
                  id -> id),
                0L),
              id -> id
            ) as charge_sum"""))


@disable_ansi_mode
def test_array_aggregate_non_zero_init():
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: unary_op_df(spark, ArrayGen(int_gen, max_length=10)).selectExpr(
            'aggregate(a, 100L, (acc, x) -> acc + CAST(x as BIGINT)) as sum_with_init'))


@disable_ansi_mode
def test_array_aggregate_null_array():
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: unary_op_df(spark, ArrayGen(int_gen, all_null=True)).selectExpr(
            'aggregate(a, 0L, (acc, x) -> acc + CAST(x as BIGINT)) as should_be_null'))


@disable_ansi_mode
def test_array_aggregate_empty_array():
    def do_it(spark):
        return spark.createDataFrame(
            [([1, 2, 3],), ([],), ([7],), ([],)],
            'a array<int>').selectExpr(
                'aggregate(a, 42L, (acc, x) -> acc + CAST(x as BIGINT)) as sum_with_empty')
    assert_gpu_and_cpu_are_equal_collect(do_it)


def test_array_aggregate_extremum_nullable_zero_no_contribution():
    def do_it(spark):
        return spark.createDataFrame(
            [([1, None],), ([],), ([None],), (None,)],
            'a array<int>').selectExpr(
                'aggregate(a, CAST(NULL AS INT), (acc, x) -> greatest(acc, x)) as max_res',
                'aggregate(a, CAST(NULL AS INT), (acc, x) -> least(acc, x)) as min_res')
    assert_gpu_and_cpu_are_equal_collect(do_it)


@allow_non_gpu('ArrayAggregate', 'LambdaFunction', 'NamedLambdaVariable', 'If', 'GreaterThan', 'Greatest', 'LessThan', 'Least')
def test_array_aggregate_extremum_nullable_zero_bare_acc_fallback():
    def do_it(spark):
        return spark.createDataFrame(
            [([-1, -2],), ([1, -2],), ([],), ([None],)],
            'a array<int>').selectExpr(
                '''aggregate(a, CAST(NULL AS INT),
                     (acc, x) -> if(x > 0, greatest(acc, x), acc)) as max_res''',
                '''aggregate(a, CAST(NULL AS INT),
                     (acc, x) -> if(x < 0, least(acc, x), acc)) as min_res''')
    assert_gpu_fallback_collect(do_it, 'ArrayAggregate')


@disable_ansi_mode
def test_array_aggregate_lambda_refs_outer_column():
    def do_it(spark):
        return two_col_df(spark, ArrayGen(int_gen, max_length=10), int_gen).selectExpr(
            'aggregate(a, 0L, (acc, x) -> acc + CAST(x + b as BIGINT)) as sum_with_outer')
    assert_gpu_and_cpu_are_equal_collect(do_it)


@disable_ansi_mode
def test_array_aggregate_zero_is_outer_column():
    def do_it(spark):
        return two_col_df(spark, ArrayGen(int_gen, max_length=10), long_gen).selectExpr(
            'aggregate(a, b, (acc, x) -> acc + CAST(x as BIGINT)) as sum_from_col')
    assert_gpu_and_cpu_are_equal_collect(do_it)


@disable_ansi_mode
def test_array_aggregate_over_struct_field():
    def do_it(spark):
        elem_gen = StructGen([['i', int_gen]], nullable=False)
        return unary_op_df(spark, ArrayGen(elem_gen, max_length=10)).selectExpr(
            'aggregate(a, 0L, (acc, s) -> acc + CAST(s.i as BIGINT)) as sum_field')
    assert_gpu_and_cpu_are_equal_collect(do_it)


@disable_ansi_mode
def test_array_aggregate_over_binary():
    # GpuLength only accepts STRING, so we hex(binary) → string first to keep the
    # whole lambda on the GPU. Result: 2 × byte count of each element, summed.
    def do_it(spark):
        return unary_op_df(spark, ArrayGen(BinaryGen(max_length=10), max_length=8)).selectExpr(
            'aggregate(a, 0L, (acc, x) -> acc + CAST(length(hex(x)) as BIGINT)) as total_hex_len')
    assert_gpu_and_cpu_are_equal_collect(do_it)


@disable_ansi_mode
def test_array_aggregate_deeper_g_body():
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: unary_op_df(spark, ArrayGen(int_gen, max_length=10)).selectExpr(
            'aggregate(a, 0L, (acc, x) -> acc + CAST(x * 2 + 1 as BIGINT)) as sum_poly'))


# Long overflow wraps in non-ANSI mode on both Spark SUM and cuDF SUM.
@disable_ansi_mode
def test_array_aggregate_long_overflow_wraps():
    def do_it(spark):
        big = LongGen(min_val=9223372036854775000, max_val=9223372036854775700, nullable=False)
        return unary_op_df(spark, ArrayGen(big, min_length=5, max_length=15)).selectExpr(
            'aggregate(a, 0L, (acc, x) -> acc + x) as wrapped_sum')
    assert_gpu_and_cpu_are_equal_collect(do_it)


@disable_ansi_mode
@allow_non_gpu('ArrayAggregate', 'LambdaFunction', 'NamedLambdaVariable', 'Add', 'Ascii')
def test_array_aggregate_cpu_only_g_fallback():
    str_gen = StringGen(pattern='[A-Za-z]{1,5}', nullable=False)
    assert_gpu_fallback_collect(
        lambda spark: unary_op_df(spark, ArrayGen(str_gen, max_length=5)).selectExpr(
            'aggregate(a, 0, (acc, x) -> acc + ascii(x)) as res'),
        'ArrayAggregate')


# Spark 3.3 non-DB wraps decimal lambda arithmetic in CheckOverflow. CheckOverflow
# blocks CPU bridge optimization, so the containing ProjectExec falls back there.
@disable_ansi_mode
@allow_non_gpu_conditional(is_before_spark_340() and not is_databricks_runtime(), 'ProjectExec')
@allow_non_gpu('ArrayAggregate', 'LambdaFunction', 'NamedLambdaVariable', 'Add')
def test_array_aggregate_decimal_sum_overflow_fallback():
    def do_it(spark):
        return spark.sql("""
            SELECT array(
              CAST('99999999999999999999999999999999999999' AS DECIMAL(38,0)),
              CAST('1' AS DECIMAL(38,0))
            ) AS a
            """).selectExpr(
                'aggregate(a, CAST(0 as DECIMAL(38,0)), (acc, x) -> acc + x) as dec_sum')
    assert_gpu_fallback_collect(do_it, 'ArrayAggregate')


@pytest.mark.parametrize('lambda_sql, init_sql', [
    ('(acc, x) -> acc - CAST(x as BIGINT)', '0L'),
    ('(acc, x) -> CAST(acc / CAST(x + 1 as BIGINT) as BIGINT)', '1L'),
    ('(acc, x) -> greatest(acc, CAST(x as BIGINT), CAST(x * 2 as BIGINT))', '-999L'),
    ('(acc, x) -> acc + acc * CAST(x as BIGINT)', '0L'),
], ids=['subtract', 'divide', 'greatest-3ary', 'g-refs-acc'])
@disable_ansi_mode
@allow_non_gpu('ArrayAggregate', 'LambdaFunction', 'NamedLambdaVariable', 'Add', 'Subtract', 'Multiply', 'Divide', 'Greatest', 'Cast')
def test_array_aggregate_fallback_shapes(lambda_sql, init_sql):
    assert_gpu_fallback_collect(
        lambda spark: unary_op_df(spark, ArrayGen(int_gen, max_length=5)).selectExpr(
            f'aggregate(a, {init_sql}, {lambda_sql}) as res'),
        'ArrayAggregate')


@allow_non_gpu('ArrayAggregate', 'LambdaFunction', 'NamedLambdaVariable', 'Add', 'Multiply', 'Cast')
def test_array_aggregate_non_identity_finish_fallback():
    assert_gpu_fallback_collect(
        lambda spark: unary_op_df(spark, ArrayGen(int_gen, max_length=5)).selectExpr(
            'aggregate(a, 0L, (acc, x) -> acc + CAST(x as BIGINT), acc -> acc * 2) as doubled'),
        'ArrayAggregate')


@disable_ansi_mode
@allow_non_gpu('ArrayAggregate', 'LambdaFunction', 'NamedLambdaVariable', 'Add', 'Cast')
def test_array_aggregate_finish_cast_fallback():
    assert_gpu_fallback_collect(
        lambda spark: unary_op_df(
            spark, ArrayGen(IntegerGen(min_val=-10, max_val=10), max_length=5)).selectExpr(
            'aggregate(a, 0, (acc, x) -> acc + x, acc -> CAST(acc AS BIGINT)) as res'),
        'ArrayAggregate')


@pytest.mark.parametrize('lambda_sql, init_sql', [
    ('(acc, x) -> greatest(acc, x)', 'CAST("-Infinity" as DOUBLE)'),
    ('(acc, x) -> least(acc, x)', 'CAST("Infinity" as DOUBLE)'),
], ids=['max', 'min'])
@allow_non_gpu('ArrayAggregate', 'LambdaFunction', 'NamedLambdaVariable', 'Greatest', 'Least')
def test_array_aggregate_double_extremum_fallback(lambda_sql, init_sql):
    assert_gpu_fallback_collect(
        lambda spark: unary_op_df(spark, ArrayGen(double_gen, max_length=5)).selectExpr(
            f'aggregate(a, {init_sql}, {lambda_sql}) as res'),
        'ArrayAggregate')


# SUM / PRODUCT on FLOAT and DOUBLE: cuDF's parallel tree-reduction sums in a different
# order than Spark's sequential left-fold, so GPU vs CPU can differ in the low bits. Gated
# by `spark.rapids.sql.variableFloatAgg.enabled` (default true, same as scalar GpuSum) —
# we only verify the fallback path here, since the GPU path under default conf accepts
# minor numeric divergence and cannot use strict-equality assertions.
@pytest.mark.parametrize('elem_gen, lambda_sql, init_sql', [
    (float_gen, '(acc, x) -> acc + x', 'CAST(0 as FLOAT)'),
    (double_gen, '(acc, x) -> acc + x', 'CAST(0 as DOUBLE)'),
    (float_gen, '(acc, x) -> acc * x', 'CAST(1 as FLOAT)'),
    (double_gen, '(acc, x) -> acc * x', 'CAST(1 as DOUBLE)'),
], ids=['float-sum', 'double-sum', 'float-product', 'double-product'])
@allow_non_gpu('ArrayAggregate', 'LambdaFunction', 'NamedLambdaVariable', 'Add', 'Multiply')
def test_array_aggregate_float_sum_product_fallback_when_variable_float_agg_disabled(
        elem_gen, lambda_sql, init_sql):
    assert_gpu_fallback_collect(
        lambda spark: unary_op_df(spark, ArrayGen(elem_gen, max_length=5)).selectExpr(
            f'aggregate(a, {init_sql}, {lambda_sql}) as res'),
        'ArrayAggregate',
        conf={'spark.rapids.sql.variableFloatAgg.enabled': 'false'})


# ANSI mode + integer SUM/PRODUCT must fall back: Spark requires overflow detection
# and type-specific failure behaviour, while cuDF segmented reduce wraps on overflow.
# Decimal SUM is included because decimal segmented reductions are unsupported in all modes.
# MAX/MIN/ALL/ANY don't overflow and stay on GPU under ANSI.
@pytest.mark.parametrize('elem_gen, lambda_sql, init_sql', [
    (IntegerGen(min_val=-100, max_val=100, nullable=False),
        '(acc, x) -> acc + CAST(x as BIGINT)', '0L'),
    (LongGen(min_val=-100, max_val=100, nullable=False),
        '(acc, x) -> acc + x', '0L'),
    (LongGen(min_val=-100, max_val=100, nullable=False),
        '(acc, x) -> acc * x', '1L'),
    (DecimalGen(precision=10, scale=2, nullable=False),
        '(acc, x) -> acc + cast(x as decimal(38,2))', 'cast(0 as decimal(38,2))'),
], ids=['int-to-long-sum', 'long-sum', 'long-product', 'decimal-sum'])
@allow_non_gpu_conditional(is_before_spark_340() and not is_databricks_runtime(), 'ProjectExec')
@allow_non_gpu('ArrayAggregate', 'LambdaFunction', 'NamedLambdaVariable', 'Add', 'Multiply', 'Cast')
def test_array_aggregate_ansi_sum_product_fallback(elem_gen, lambda_sql, init_sql):
    assert_gpu_fallback_collect(
        lambda spark: unary_op_df(spark, ArrayGen(elem_gen, max_length=5)).selectExpr(
            f'aggregate(a, {init_sql}, {lambda_sql}) as res'),
        'ArrayAggregate',
        conf=ansi_enabled_conf)
