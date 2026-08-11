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

from asserts import assert_gpu_and_cpu_are_equal_collect, assert_gpu_fallback_collect
from data_gen import *
from marks import allow_non_gpu, ignore_order
from pyspark.sql.types import *
import pyspark.sql.functions as f

pytestmark = pytest.mark.nightly_resource_consuming_test

explode_gens = all_gen + [binary_gen]
arrays_with_binary = [ArrayGen(BinaryGen(max_length=5))]
maps_with_binary = [MapGen(IntegerGen(nullable=False), BinaryGen(max_length=5))]

def four_op_df(spark, gen, length=2048):
    return gen_df(spark, StructGen([
        ('a', gen),
        ('b', gen),
        ('c', gen),
        ('d', gen)], nullable=False), length=length)

#sort locally because of https://github.com/NVIDIA/spark-rapids/issues/84
# After 3.1.0 is the min spark version we can drop this
@ignore_order(local=True)
@pytest.mark.parametrize('data_gen', explode_gens, ids=idfn)
def test_explode_makearray(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : four_op_df(spark, data_gen).selectExpr('a', 'explode(array(b, c, d))'))

#sort locally because of https://github.com/NVIDIA/spark-rapids/issues/84
# After 3.1.0 is the min spark version we can drop this
@ignore_order(local=True)
@pytest.mark.parametrize('data_gen', explode_gens, ids=idfn)
def test_explode_litarray(data_gen):
    array_lit = with_cpu_session(
        lambda spark: gen_scalar(ArrayGen(data_gen, min_length=3, max_length=3, nullable=False)))
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : four_op_df(spark, data_gen).select(f.col('a'), f.col('b'), f.col('c'), 
                f.explode(array_lit)))

# use a small `spark.rapids.sql.batchSizeBytes` to enforce input batches splitting up during explode
conf_to_enforce_split_input = {'spark.rapids.sql.batchSizeBytes': '8192'}

@ignore_order(local=True)
@pytest.mark.order(1) # at the head of xdist worker queue if pytest-order is installed
@pytest.mark.parametrize('data_gen', explode_gens + struct_gens_sample_with_decimal128 +
                         array_gens_sample + map_gens_sample + arrays_with_binary + maps_with_binary,
                         ids=idfn)
def test_explode_array_data(data_gen):
    data_gen = [int_gen, ArrayGen(data_gen)]
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: two_col_df(spark, *data_gen).selectExpr('a', 'explode(b)'),
        conf=conf_to_enforce_split_input)

#sort locally because of https://github.com/NVIDIA/spark-rapids/issues/84
# After 3.1.0 is the min spark version we can drop this
@ignore_order(local=True)
@pytest.mark.parametrize('map_gen', map_gens_sample + decimal_128_map_gens + maps_with_binary, ids=idfn)
def test_explode_map_data(map_gen):
    data_gen = [int_gen, map_gen]
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: two_col_df(spark, *data_gen).selectExpr('a', 'explode(b)'),
        conf=conf_to_enforce_split_input)

#sort locally because of https://github.com/NVIDIA/spark-rapids/issues/84
# After 3.1.0 is the min spark version we can drop this
@ignore_order(local=True)
@pytest.mark.parametrize('data_gen', explode_gens, ids=idfn)
def test_explode_nested_array_data(data_gen):
    data_gen = [int_gen, ArrayGen(ArrayGen(data_gen))]
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: two_col_df(spark, *data_gen).selectExpr(
            'a', 'explode(b) as c').selectExpr('a', 'explode(c)'),
        conf=conf_to_enforce_split_input)

#sort locally because of https://github.com/NVIDIA/spark-rapids/issues/84
# After 3.1.0 is the min spark version we can drop this
@ignore_order(local=True)
@pytest.mark.order(1) # at the head of xdist worker queue if pytest-order is installed
@pytest.mark.parametrize('data_gen', explode_gens + struct_gens_sample_with_decimal128 +
                         array_gens_sample + arrays_with_binary + map_gens_sample + maps_with_binary,
                         ids=idfn)
def test_explode_outer_array_data(data_gen):
    data_gen = [int_gen, ArrayGen(data_gen)]
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: two_col_df(spark, *data_gen).selectExpr('a', 'explode_outer(b)'),
        conf=conf_to_enforce_split_input)

#sort locally because of https://github.com/NVIDIA/spark-rapids/issues/84
# After 3.1.0 is the min spark version we can drop this
@ignore_order(local=True)
@pytest.mark.parametrize('map_gen', map_gens_sample + decimal_128_map_gens + maps_with_binary, ids=idfn)
def test_explode_outer_map_data(map_gen):
    data_gen = [int_gen, map_gen]
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: two_col_df(spark, *data_gen).selectExpr('a', 'explode_outer(b)'),
        conf=conf_to_enforce_split_input)

#sort locally because of https://github.com/NVIDIA/spark-rapids/issues/84
# After 3.1.0 is the min spark version we can drop this
@ignore_order(local=True)
@pytest.mark.parametrize('data_gen', explode_gens, ids=idfn)
@allow_non_gpu(*non_utc_allow)
def test_explode_outer_nested_array_data(data_gen):
    data_gen = [int_gen, ArrayGen(ArrayGen(data_gen))]
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: two_col_df(spark, *data_gen).selectExpr(
            'a', 'explode_outer(b) as c').selectExpr('a', 'explode_outer(c)'),
        conf=conf_to_enforce_split_input)

#sort locally because of https://github.com/NVIDIA/spark-rapids/issues/84
# After 3.1.0 is the min spark version we can drop this
@ignore_order(local=True)
@pytest.mark.parametrize('data_gen', explode_gens, ids=idfn)
@allow_non_gpu(*non_utc_allow)
def test_posexplode_makearray(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : four_op_df(spark, data_gen).selectExpr('posexplode(array(b, c, d))', 'a'))

#sort locally because of https://github.com/NVIDIA/spark-rapids/issues/84
# After 3.1.0 is the min spark version we can drop this
@ignore_order(local=True)
@pytest.mark.parametrize('data_gen', explode_gens, ids=idfn)
@allow_non_gpu(*non_utc_allow)
def test_posexplode_litarray(data_gen):
    array_lit = with_cpu_session(
        lambda spark: gen_scalar(ArrayGen(data_gen, min_length=3, max_length=3, nullable=False)))
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : four_op_df(spark, data_gen).select(f.col('a'), f.col('b'), f.col('c'),
                f.posexplode(array_lit)))

#sort locally because of https://github.com/NVIDIA/spark-rapids/issues/84
# After 3.1.0 is the min spark version we can drop this
@ignore_order(local=True)
@pytest.mark.order(1) # at the head of xdist worker queue if pytest-order is installed
@pytest.mark.parametrize('data_gen', explode_gens + struct_gens_sample_with_decimal128 +
                         array_gens_sample + arrays_with_binary + map_gens_sample + maps_with_binary,
                         ids=idfn)
@allow_non_gpu(*non_utc_allow)
def test_posexplode_array_data(data_gen):
    data_gen = [int_gen, ArrayGen(data_gen)]
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: two_col_df(spark, *data_gen).selectExpr('a', 'posexplode(b)'),
        conf=conf_to_enforce_split_input)

#sort locally because of https://github.com/NVIDIA/spark-rapids/issues/84
# After 3.1.0 is the min spark version we can drop this
@ignore_order(local=True)
@pytest.mark.parametrize('map_gen', map_gens_sample + decimal_128_map_gens + maps_with_binary, ids=idfn)
@allow_non_gpu(*non_utc_allow)
def test_posexplode_map_data(map_gen):
    data_gen = [int_gen, map_gen]
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: two_col_df(spark, *data_gen).selectExpr('a', 'posexplode(b)'),
        conf=conf_to_enforce_split_input)

#sort locally because of https://github.com/NVIDIA/spark-rapids/issues/84
# After 3.1.0 is the min spark version we can drop this
@ignore_order(local=True)
@pytest.mark.parametrize('data_gen', explode_gens, ids=idfn)
@allow_non_gpu(*non_utc_allow)
def test_posexplode_nested_array_data(data_gen):
    data_gen = [int_gen, ArrayGen(ArrayGen(data_gen))]
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: two_col_df(spark, *data_gen).selectExpr(
            'a', 'posexplode(b) as (pos, c)').selectExpr('a', 'pos', 'posexplode(c)'),
        conf=conf_to_enforce_split_input)

#sort locally because of https://github.com/NVIDIA/spark-rapids/issues/84
# After 3.1.0 is the min spark version we can drop this
@ignore_order(local=True)
@pytest.mark.order(1) # at the head of xdist worker queue if pytest-order is installed
@pytest.mark.parametrize('data_gen', explode_gens + struct_gens_sample_with_decimal128 +
                         array_gens_sample + arrays_with_binary + map_gens_sample + maps_with_binary,
                         ids=idfn)
@allow_non_gpu(*non_utc_allow)
def test_posexplode_outer_array_data(data_gen):
    data_gen = [int_gen, ArrayGen(data_gen)]
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: two_col_df(spark, *data_gen).selectExpr('a', 'posexplode_outer(b)'),
        conf=conf_to_enforce_split_input)

#sort locally because of https://github.com/NVIDIA/spark-rapids/issues/84
# After 3.1.0 is the min spark version we can drop this
@ignore_order(local=True)
@pytest.mark.parametrize('map_gen', map_gens_sample + decimal_128_map_gens + maps_with_binary, ids=idfn)
@allow_non_gpu(*non_utc_allow)
def test_posexplode_outer_map_data(map_gen):
    data_gen = [int_gen, map_gen]
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: two_col_df(spark, *data_gen).selectExpr('a', 'posexplode_outer(b)'),
        conf=conf_to_enforce_split_input)

#sort locally because of https://github.com/NVIDIA/spark-rapids/issues/84
# After 3.1.0 is the min spark version we can drop this
@ignore_order(local=True)
@pytest.mark.parametrize('data_gen', explode_gens, ids=idfn)
@allow_non_gpu(*non_utc_allow)
def test_posexplode_nested_outer_array_data(data_gen):
    data_gen = [int_gen, ArrayGen(ArrayGen(data_gen))]
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: two_col_df(spark, *data_gen).selectExpr(
            'a', 'posexplode_outer(b) as (pos, c)').selectExpr(
            'a', 'pos', 'posexplode_outer(c)'),
        conf=conf_to_enforce_split_input)

@allow_non_gpu("GenerateExec", "ShuffleExchangeExec")
@ignore_order(local=True)
def test_generate_outer_fallback():
    assert_gpu_fallback_collect(
        lambda spark: spark.sql("SELECT array(struct(1, 'a'), struct(2, 'b')) as x")\
            .repartition(1).selectExpr("inline_outer(x)"),
        "GenerateExec",
        # Disable AQE temporarily until https://github.com/NVIDIA/spark-rapids/issues/14319 is resolved.
        conf={'spark.sql.adaptive.enabled': 'false'})

# gpu stack not guarantee to produce the same output order as Spark does
@ignore_order(local=True) 
def test_stack():
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : spark.range(100).selectExpr('*', 'stack(3, id, 2L, 3L, 4L, 5L, 6L)'))

# gpu stack not guarantee to produce the same output order as Spark does
@ignore_order(local=True)
@allow_non_gpu(*non_utc_allow)
def test_stack_mixed_types():
    base_gens = [byte_gen, short_gen, int_gen, long_gen, float_gen, double_gen, string_gen, 
                  boolean_gen, date_gen, timestamp_gen, null_gen, DecimalGen(precision=7, scale=3),
                  DecimalGen(precision=12, scale=2), DecimalGen(precision=20, scale=2)]
    data_gen = StructGen([['child'+str(ind), sub_gen] for ind, sub_gen in 
                          enumerate(base_gens)], nullable=False)
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : gen_df(spark, data_gen, length=100)
                .selectExpr('*', 'stack(2, child1, child2, child3, child4, child5, child6, ' + 
                            'child7, child8, child9, child10, child11, child12, child13, ' + 
                            '1Y, 2S, 3, 4L, 5.0f, 6.0d, "7", false, to_date("2009-01-01"), ' + 
                            'to_timestamp("2010-01-01 00:00:00"), null, 1234.567, ' + 
                            '1234567890.12, 123456789012345678.90)'))

# gpu stack not guarantee to produce the same output order as Spark does
@ignore_order(local=True)
def test_stack_nested_types():
    data_gen = StructGen([['array', ArrayGen(IntegerGen(nullable=False))], 
                         ['map', MapGen(IntegerGen(nullable=False), StringGen(nullable=False))],
                         ['struct', StructGen([['col1', IntegerGen(nullable=False)], 
                                               ['col2', StringGen(nullable=False)]])]
                         ], nullable=False)
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : gen_df(spark, data_gen, length=100)
                .selectExpr('*', 'stack(2, map, array, struct, ' + 
                            'map(1, "a", 2, "b"), array(1, 2, 3), struct(1, "a"))'))

