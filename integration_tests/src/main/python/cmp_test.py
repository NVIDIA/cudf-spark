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

from decimal import Decimal

import pytest

from asserts import assert_cpu_and_gpu_are_equal_collect_with_capture, \
    assert_gpu_and_cpu_are_equal_collect, assert_gpu_fallback_collect
from conftest import is_not_utc
from data_gen import *
from spark_session import with_cpu_session, is_before_spark_313
from pyspark.sql.types import *
from marks import datagen_overrides, allow_non_gpu
import pyspark.sql.functions as f

@pytest.mark.parametrize('data_gen', eq_gens_with_decimal_gen + struct_gens_sample_with_decimal128_no_list, ids=idfn)
def test_eq(data_gen):
    (s1, s2) = with_cpu_session(
        lambda spark: gen_scalars(data_gen, 2, force_no_nulls=not isinstance(data_gen, NullGen)))
    data_type = data_gen.data_type
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : binary_op_df(spark, data_gen).select(
                f.col('a') == s1,
                s2 == f.col('b'),
                f.lit(None).cast(data_type) == f.col('a'),
                f.col('b') == f.lit(None).cast(data_type),
                f.col('a') == f.col('b')))

def test_eq_for_interval():
    def test_func(data_gen):
        (s1, s2) = with_cpu_session(
        lambda spark: gen_scalars(data_gen, 2, force_no_nulls=not isinstance(data_gen, NullGen)))
        data_type = data_gen.data_type
        assert_gpu_and_cpu_are_equal_collect(
            lambda spark : binary_op_df(spark, data_gen).select(
                f.col('a') == s1,
                s2 == f.col('b'),
                f.lit(None).cast(data_type) == f.col('a'),
                f.col('b') == f.lit(None).cast(data_type),
                f.col('a') == f.col('b')))
    # DayTimeIntervalType not supported inside Structs -- issue #6184
    # data_gens = [DayTimeIntervalGen(),
    # StructGen([['child0', StructGen([['child2', DayTimeIntervalGen()]])], ['child1', short_gen]])]
    data_gens = [DayTimeIntervalGen()]
    for data_gen in data_gens:
        test_func(data_gen)

@pytest.mark.parametrize('data_gen', eq_gens_with_decimal_gen + struct_gens_sample_with_decimal128_no_list, ids=idfn)
def test_eq_ns(data_gen):
    (s1, s2) = with_cpu_session(
        lambda spark: gen_scalars(data_gen, 2, force_no_nulls=not isinstance(data_gen, NullGen)))
    data_type = data_gen.data_type
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : binary_op_df(spark, data_gen).select(
                f.col('a').eqNullSafe(s1),
                s2.eqNullSafe(f.col('b')),
                f.lit(None).cast(data_type).eqNullSafe(f.col('a')),
                f.col('b').eqNullSafe(f.lit(None).cast(data_type)),
                f.col('a').eqNullSafe(f.col('b'))))

def test_eq_ns_for_interval():
    data_gen = DayTimeIntervalGen()
    (s1, s2) = with_cpu_session(
        lambda spark: gen_scalars(data_gen, 2, force_no_nulls=not isinstance(data_gen, NullGen)))
    data_type = data_gen.data_type
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : binary_op_df(spark, data_gen).select(
            f.col('a').eqNullSafe(s1),
            s2.eqNullSafe(f.col('b')),
            f.lit(None).cast(data_type).eqNullSafe(f.col('a')),
            f.col('b').eqNullSafe(f.lit(None).cast(data_type)),
            f.col('a').eqNullSafe(f.col('b'))))

@pytest.mark.parametrize('data_gen', eq_gens_with_decimal_gen + struct_gens_sample_with_decimal128_no_list, ids=idfn)
def test_ne(data_gen):
    (s1, s2) = with_cpu_session(
        lambda spark: gen_scalars(data_gen, 2, force_no_nulls=not isinstance(data_gen, NullGen)))
    data_type = data_gen.data_type
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : binary_op_df(spark, data_gen).select(
                f.col('a') != s1,
                s2 != f.col('b'),
                f.lit(None).cast(data_type) != f.col('a'),
                f.col('b') != f.lit(None).cast(data_type),
                f.col('a') != f.col('b')))

def test_ne_for_interval():
    def test_func(data_gen):
        (s1, s2) = with_cpu_session(
        lambda spark: gen_scalars(data_gen, 2, force_no_nulls=not isinstance(data_gen, NullGen)))
        data_type = data_gen.data_type
        assert_gpu_and_cpu_are_equal_collect(
            lambda spark : binary_op_df(spark, data_gen).select(
                f.col('a') != s1,
                s2 != f.col('b'),
                f.lit(None).cast(data_type) != f.col('a'),
                f.col('b') != f.lit(None).cast(data_type),
                f.col('a') != f.col('b')))
    # DayTimeIntervalType not supported inside Structs -- issue #6184
    # data_gens = [DayTimeIntervalGen(),
    # StructGen([['child0', StructGen([['child2', DayTimeIntervalGen()]])], ['child1', short_gen]])]
    data_gens = [DayTimeIntervalGen()]
    for data_gen in data_gens:
        test_func(data_gen)

@pytest.mark.parametrize('data_gen', orderable_gens + struct_gens_sample_with_decimal128_no_list, ids=idfn)
def test_lt(data_gen):
    (s1, s2) = with_cpu_session(
        lambda spark: gen_scalars(data_gen, 2, force_no_nulls=not isinstance(data_gen, NullGen)))
    data_type = data_gen.data_type
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : binary_op_df(spark, data_gen).select(
                f.col('a') < s1,
                s2 < f.col('b'),
                f.lit(None).cast(data_type) < f.col('a'),
                f.col('b') < f.lit(None).cast(data_type),
                f.col('a') < f.col('b')))

def test_lt_for_interval():
    def test_func(data_gen):
        (s1, s2) = with_cpu_session(
        lambda spark: gen_scalars(data_gen, 2, force_no_nulls=not isinstance(data_gen, NullGen)))
        data_type = data_gen.data_type
        assert_gpu_and_cpu_are_equal_collect(
            lambda spark : binary_op_df(spark, data_gen).select(
                f.col('a') < s1,
                s2 < f.col('b'),
                f.lit(None).cast(data_type) < f.col('a'),
                f.col('b') < f.lit(None).cast(data_type),
                f.col('a') < f.col('b')))
    # DayTimeIntervalType not supported inside Structs -- issue #6184
    # data_gens = [DayTimeIntervalGen(),
    # StructGen([['child0', StructGen([['child2', DayTimeIntervalGen()]])], ['child1', short_gen]])]
    data_gens = [DayTimeIntervalGen()]
    for data_gen in data_gens:
        test_func(data_gen)

@pytest.mark.parametrize('data_gen', orderable_gens + struct_gens_sample_with_decimal128_no_list, ids=idfn)
def test_lte(data_gen):
    (s1, s2) = with_cpu_session(
        lambda spark: gen_scalars(data_gen, 2, force_no_nulls=not isinstance(data_gen, NullGen)))
    data_type = data_gen.data_type
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : binary_op_df(spark, data_gen).select(
                f.col('a') <= s1,
                s2 <= f.col('b'),
                f.col('b') <= s2,
                f.lit(None).cast(data_type) <= f.col('a'),
                f.col('b') <= f.lit(None).cast(data_type),
                f.col('a') <= f.col('b')))

def test_lte_for_interval():
    def test_func(data_gen):
        (s1, s2) = with_cpu_session(
        lambda spark: gen_scalars(data_gen, 2, force_no_nulls=not isinstance(data_gen, NullGen)))
        data_type = data_gen.data_type
        assert_gpu_and_cpu_are_equal_collect(
            lambda spark : binary_op_df(spark, data_gen).select(
                f.col('a') <= s1,
                s2 <= f.col('b'),
                f.lit(None).cast(data_type) <= f.col('a'),
                f.col('b') <= f.lit(None).cast(data_type),
                f.col('a') <= f.col('b')))
    # DayTimeIntervalType not supported inside Structs -- issue #6184
    # data_gens = [DayTimeIntervalGen(),
    # StructGen([['child0', StructGen([['child2', DayTimeIntervalGen()]])], ['child1', short_gen]])]
    data_gens = [DayTimeIntervalGen()]
    for data_gen in data_gens:
        test_func(data_gen)

@pytest.mark.parametrize('data_gen', orderable_gens, ids=idfn)
def test_gt(data_gen):
    (s1, s2) = with_cpu_session(
        lambda spark: gen_scalars(data_gen, 2, force_no_nulls=not isinstance(data_gen, NullGen)))
    data_type = data_gen.data_type
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : binary_op_df(spark, data_gen).select(
                f.col('a') > s1,
                s2 > f.col('b'),
                f.col('b') > s2,
                f.lit(None).cast(data_type) > f.col('a'),
                f.col('b') > f.lit(None).cast(data_type),
                f.col('a') > f.col('b')))

def test_gt_interval():
    def test_func(data_gen):
        (s1, s2) = with_cpu_session(
        lambda spark: gen_scalars(data_gen, 2, force_no_nulls=not isinstance(data_gen, NullGen)))
        data_type = data_gen.data_type
        assert_gpu_and_cpu_are_equal_collect(
            lambda spark : binary_op_df(spark, data_gen).select(
                f.col('a') > s1,
                s2 > f.col('b'),
                f.lit(None).cast(data_type) > f.col('a'),
                f.col('b') > f.lit(None).cast(data_type),
                f.col('a') > f.col('b')))
    # DayTimeIntervalType not supported inside Structs -- issue #6184
    # data_gens = [DayTimeIntervalGen(),
    # StructGen([['child0', StructGen([['child2', DayTimeIntervalGen()]])], ['child1', short_gen]])]
    data_gens = [DayTimeIntervalGen()]
    for data_gen in data_gens:
        test_func(data_gen)

@pytest.mark.parametrize('data_gen', orderable_gens + struct_gens_sample_with_decimal128_no_list, ids=idfn)
def test_gte(data_gen):
    (s1, s2) = with_cpu_session(
        lambda spark: gen_scalars(data_gen, 2, force_no_nulls=not isinstance(data_gen, NullGen)))
    data_type = data_gen.data_type
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : binary_op_df(spark, data_gen).select(
                f.col('a') >= s1,
                s2 >= f.col('b'),
                f.col('b') >= s2,
                f.lit(None).cast(data_type) >= f.col('a'),
                f.col('b') >= f.lit(None).cast(data_type),
                f.col('a') >= f.col('b')))

def test_gte_for_interval():
    def test_func(data_gen):
        (s1, s2) = with_cpu_session(
        lambda spark: gen_scalars(data_gen, 2, force_no_nulls=not isinstance(data_gen, NullGen)))
        data_type = data_gen.data_type
        assert_gpu_and_cpu_are_equal_collect(
            lambda spark : binary_op_df(spark, data_gen).select(
                f.col('a') >= s1,
                s2 >= f.col('b'),
                f.lit(None).cast(data_type) >= f.col('a'),
                f.col('b') >= f.lit(None).cast(data_type),
                f.col('a') >= f.col('b')))
    # DayTimeIntervalType not supported inside Structs -- issue #6184
    # data_gens = [DayTimeIntervalGen(),
    # StructGen([['child0', StructGen([['child2', DayTimeIntervalGen()]])], ['child1', short_gen]])]
    data_gens = [DayTimeIntervalGen()]
    for data_gen in data_gens:
        test_func(data_gen)

@pytest.mark.parametrize('data_gen', eq_gens_with_decimal_gen + [binary_gen] + array_gens_sample + struct_gens_sample + map_gens_sample, ids=idfn)
def test_isnull(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).select(
                f.isnull(f.col('a'))))

def test_isnull_for_interval():
    data_gen = DayTimeIntervalGen()
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : unary_op_df(spark, data_gen).select(
            f.isnull(f.col('a'))))

@pytest.mark.parametrize('data_gen', [FloatGen(), DoubleGen()], ids=idfn)
def test_isnan(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).select(
                f.isnan(f.col('a'))))

@pytest.mark.parametrize('data_gen', eq_gens_with_decimal_gen + [binary_gen] + array_gens_sample + struct_gens_sample + map_gens_sample, ids=idfn)
def test_dropna_any(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : binary_op_df(spark, data_gen).dropna())

@pytest.mark.parametrize('data_gen', eq_gens_with_decimal_gen + [binary_gen] + array_gens_sample + struct_gens_sample + map_gens_sample, ids=idfn)
def test_dropna_all(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : binary_op_df(spark, data_gen).dropna(how='all'))

#dropna is really a filter along with a test for null, but lets do an explicit filter test too
@pytest.mark.parametrize('data_gen', eq_gens_with_decimal_gen + array_gens_sample + struct_gens_sample + map_gens_sample, ids=idfn)
def test_filter(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : three_col_df(spark, BooleanGen(), data_gen, data_gen).filter(f.col('a')))

# coalesce batch happens after a filter, but only if something else happens on the GPU after that
@pytest.mark.parametrize('data_gen', eq_gens_with_decimal_gen + array_gens_sample + struct_gens_sample + map_gens_sample, ids=idfn)
def test_filter_with_project(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : two_col_df(spark, BooleanGen(), data_gen).filter(f.col('a')).selectExpr('*', 'a as a2'))

# DateAddInterval is a time zone aware expression
non_utc_allow_for_date_add_interval = ['ProjectExec', 'FilterExec'] if is_not_utc() else []
# It takes quite a bit to get filter to have a column it can filter on, but
# no columns to actually filter. We are making it happen here with a sub-query
# and some constants that then make it so all we need is the number of rows
# of input.
@pytest.mark.parametrize('op', ['>', '<'])
@allow_non_gpu(*non_utc_allow_for_date_add_interval)
def test_empty_filter(op, spark_tmp_path):
    # Disable AQE temporarily until https://github.com/NVIDIA/spark-rapids/issues/14319 is resolved.
    conf = {'spark.sql.adaptive.enabled': 'false'}

    def do_it(spark):
        df = spark.createDataFrame([(14, "Tom"), (23, "Alice"), (16, "Bob")], ["age", "name"])
        # we repartition the data to 1 because for some reason Spark can write 4 files for 3 rows.
        # In this case that causes a race condition with the last aggregation which can result
        # in a null being returned. For some reason this happens a lot on the GPU in local mode
        # and not on the CPU in local mode.
        df.repartition(1).write.mode("overwrite").parquet(spark_tmp_path)
        df = spark.read.parquet(spark_tmp_path)
        curDate = df.withColumn("current_date", f.current_date())
        curDate.createOrReplaceTempView("empty_filter_test_curDate")
        spark.sql("select current_date, ((select last(current_date) from empty_filter_test_curDate) + interval 1 day) as test from empty_filter_test_curDate").createOrReplaceTempView("empty_filter_test2")
        return spark.sql(f"select * from empty_filter_test2 where test {op} current_date")
    assert_gpu_and_cpu_are_equal_collect(do_it, conf=conf)

def test_nondeterministic_filter():
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, LongGen(), 1).filter(f.rand(0) > 0.5))

@pytest.mark.parametrize('expr', [f.lit(True), f.lit(False), f.lit(None).cast('boolean')], ids=idfn)
def test_filter_with_lit(expr):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, LongGen()).filter(expr))

# Spark supports two different versions of 'IN', and it depends on the spark.sql.optimizer.inSetConversionThreshold conf
# This is to test entries under that value.
@pytest.mark.parametrize('data_gen', eq_gens_with_decimal_gen, ids=idfn)
def test_in(data_gen):
    # nulls are not supported for in on the GPU yet
    num_entries = int(with_cpu_session(lambda spark: spark.conf.get('spark.sql.optimizer.inSetConversionThreshold'))) - 1
    # we have to make the scalars in a session so negative scales in decimals are supported
    scalars = with_cpu_session(lambda spark: list(gen_scalars(data_gen, num_entries, force_no_nulls=not isinstance(data_gen, NullGen))))
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).select(f.col('a').isin(scalars)))

@pytest.mark.parametrize('data_type, rows', [
    ('int', [
        (1, 1, 7),
        (2, 1, 2),
        (3, None, 4),
        (3, None, None),
        (None, 1, 2),
    ]),
    ('float', [
        (float('nan'), 1.0, float('nan')),
        (1.0, float('nan'), 2.0),
        (3.0, None, 4.0),
        (None, 1.0, 2.0),
    ]),
    ('double', [
        (float('nan'), float('nan'), 1.0),
        # In returns NULL when NaN misses every candidate but the list contains NULL.
        (float('nan'), 2.0, 3.0),
        (2.0, 1.0, 2.0),
        (3.0, None, 4.0),
        (None, 1.0, 2.0),
    ]),
    ('decimal(10, 2)', [
        (Decimal('1.00'), Decimal('1.00'), Decimal('7.00')),
        (Decimal('2.00'), Decimal('1.00'), Decimal('2.00')),
        (Decimal('3.00'), None, Decimal('4.00')),
        (None, Decimal('1.00'), Decimal('2.00')),
    ]),
], ids=['int', 'float', 'double', 'decimal'])
def test_dynamic_in(data_type, rows):
    def do_it(spark):
        return spark.createDataFrame(rows, f'a {data_type}, b {data_type}, c {data_type}') \
            .selectExpr(
                'a IN (b) AS single_result',
                'a IN (b, c) AS dynamic_result',
                'a IN (NULL, 1) AS literal_result',
                'a IN (-10, -9, -8, -7, -6, -5, -4, -3, -2, -1, 1, b, NULL, c) '
                'AS mixed_result')

    assert_gpu_and_cpu_are_equal_collect(do_it)


def test_dynamic_in_mixed_ast_support():
    # Integral equality can be fused into the AST, while floating-point equality must be
    # materialized first to preserve Spark's NaN semantics.
    def do_it(spark):
        return spark.createDataFrame([
            (1, 1, 7, 1.0, 1.0, 7.0),
            (2, 1, 2, float('nan'), 1.0, float('nan')),
            (3, None, 4, 3.0, None, 4.0),
            (None, 1, 2, None, 1.0, 2.0),
        ], 'a int, b int, c int, x float, y float, z float').selectExpr(
            'a IN (b, c) AS ast_result',
            'x IN (y, z) AS regular_result')

    assert_cpu_and_gpu_are_equal_collect_with_capture(
        do_it,
        exist_classes='GpuIn')


def test_dynamic_in_allows_nondeterministic_value():
    # The value is evaluated once before the list, so its nondeterminism does not affect ordering.
    def do_it(spark):
        return spark.range(10).selectExpr(
            'rand(1) IN (id + 2, id + 3) AS result')

    assert_cpu_and_gpu_are_equal_collect_with_capture(
        do_it,
        exist_classes='GpuIn')


def test_dynamic_in_allows_side_effecting_value():
    def do_it(spark):
        return spark.createDataFrame([(1, 1, '1')], 'a int, b int, c string') \
            .selectExpr('CAST(c AS INT) IN (b, a) AS result')

    assert_cpu_and_gpu_are_equal_collect_with_capture(
        do_it,
        exist_classes='GpuIn',
        conf={'spark.sql.ansi.enabled': 'true'})


@allow_non_gpu('ProjectExec')
def test_nondeterministic_dynamic_in_fallback():
    # GpuIn groups literals and eagerly projects dynamic candidates, so it cannot preserve
    # Spark's evaluation order for nondeterministic expressions.
    def do_it(spark):
        return spark.createDataFrame([(1.0, 2.0)], 'a double, b double') \
            .selectExpr('a IN (b, rand(1)) AS result')

    assert_gpu_fallback_collect(
        do_it,
        'ProjectExec')


@allow_non_gpu('In', 'Add')
def test_large_dynamic_in_fallback():
    def do_it(spark):
        # One candidate past the stack-safety limit must remain on CPU.
        candidate_count = 257
        candidates = ', '.join(f'id + {i}' for i in range(1, candidate_count + 1))
        return spark.range(1).selectExpr(f'id IN ({candidates}) AS result')

    assert_cpu_and_gpu_are_equal_collect_with_capture(
        do_it,
        exist_classes='GpuCpuBridgeExpression',
        non_exist_classes='GpuIn')


@allow_non_gpu('In', 'Cast')
def test_ansi_side_effecting_dynamic_in_fallback():
    def do_it(spark):
        return spark.createDataFrame([(1, 1, 'invalid')], 'a int, b int, c string') \
            .selectExpr('a IN (b, CAST(c AS INT)) AS result')

    assert_cpu_and_gpu_are_equal_collect_with_capture(
        do_it,
        exist_classes='GpuCpuBridgeExpression',
        conf={'spark.sql.ansi.enabled': 'true'})


@allow_non_gpu('In', 'Cast')
def test_bridge_only_dynamic_in_candidate_fallback():
    def do_it(spark):
        return spark.createDataFrame([(1, 1, 'invalid')], 'a int, b int, c string') \
            .selectExpr('a IN (b, CAST(c AS INT)) AS result')

    assert_cpu_and_gpu_are_equal_collect_with_capture(
        do_it,
        exist_classes='GpuCpuBridgeExpression',
        non_exist_classes='GpuIn',
        conf={
            'spark.sql.ansi.enabled': 'true',
            'spark.rapids.sql.expression.Cast': 'false'
        })


@allow_non_gpu('ProjectExec', 'In', 'Divide', 'Cast')
def test_ansi_decimal_side_effecting_dynamic_in_fallback():
    def do_it(spark):
        rows = [(Decimal('1.00'), Decimal('1.00'), Decimal('1.00'), Decimal('0.00'))]
        schema = 'a decimal(10, 2), b decimal(10, 2), c decimal(10, 2), d decimal(10, 2)'
        return spark.createDataFrame(rows, schema).selectExpr(
            'a IN (b, c / d) AS result')

    assert_gpu_and_cpu_are_equal_collect(
        do_it,
        conf={'spark.sql.ansi.enabled': 'true'})


@allow_non_gpu('ProjectExec', 'In', 'Multiply', 'Cast')
def test_ansi_decimal_multiply_side_effecting_dynamic_in_fallback():
    def do_it(spark):
        # Spark stops after b matches; eager evaluation of c * d would overflow in ANSI mode.
        rows = [(Decimal('1'), Decimal('1'), Decimal('9' * 38), Decimal('9'))]
        schema = 'a decimal(38, 0), b decimal(38, 0), c decimal(38, 0), d decimal(38, 0)'
        return spark.createDataFrame(rows, schema).selectExpr(
            'a IN (b, c * d) AS result')

    assert_gpu_and_cpu_are_equal_collect(
        do_it,
        conf={'spark.sql.ansi.enabled': 'true'})

# We avoid testing inset with NaN in Spark < 3.1.3 since it has issue with NaN comparisons.
# See https://github.com/NVIDIA/spark-rapids/issues/9687.
test_inset_data_gen = [gen for gen in eq_gens_with_decimal_gen if gen != float_gen if gen != double_gen] + \
                                   [FloatGen(no_nans=True), DoubleGen(no_nans=True)] \
                      if is_before_spark_313() else eq_gens_with_decimal_gen

# Spark supports two different versions of 'IN', and it depends on the spark.sql.optimizer.inSetConversionThreshold conf
# This is to test entries over that value.
@allow_non_gpu(*non_utc_allow)
@pytest.mark.parametrize('data_gen', test_inset_data_gen, ids=idfn)
def test_in_set(data_gen):
    # nulls are not supported for in on the GPU yet
    num_entries = int(with_cpu_session(lambda spark: spark.conf.get('spark.sql.optimizer.inSetConversionThreshold'))) + 1
    # we have to make the scalars in a session so negative scales in decimals are supported
    scalars = with_cpu_session(lambda spark: list(gen_scalars(data_gen, num_entries, force_no_nulls=not isinstance(data_gen, NullGen))))
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).select(f.col('a').isin(scalars)))
