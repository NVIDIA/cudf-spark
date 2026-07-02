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

from asserts import assert_gpu_and_cpu_are_equal_collect, assert_gpu_and_cpu_error, assert_gpu_fallback_collect
from spark_session import is_before_spark_320, is_before_spark_330
from conftest import is_not_utc
from data_gen import *
from marks import ignore_order, allow_non_gpu
import pyspark.sql.functions as f

# 4 level nested struct
# each level has a different number of children to avoid a bug in spark < 3.1
nested_struct = StructGen([
    ['child0', StructGen([
        ['child0', StructGen([
            ['child0', StructGen([
                ['child0', DecimalGen(7, 2)],
                ['child1', BooleanGen()],
                ['child2', BooleanGen()],
                ['child3', BooleanGen()]
            ])],
            ['child1', BooleanGen()],
            ['child2', BooleanGen()]
        ])],
        ['child1', BooleanGen()]
    ])]])

# map generators without ArrayType value, since Union on ArrayType is not supported
map_gens = [simple_string_to_string_map_gen,
            MapGen(RepeatSeqGen(IntegerGen(nullable=False), 10), long_gen, max_length=10),
            MapGen(BooleanGen(nullable=False), boolean_gen, max_length=2),
            MapGen(StringGen(pattern='key_[0-9]', nullable=False), simple_string_to_string_map_gen),
            MapGen(
                LongGen(nullable=False),
                MapGen(
                    DecimalGen(7, 2, nullable=False),
                    MapGen(
                        IntegerGen(nullable=False),
                        StringGen(pattern='value_[0-9]', nullable=False),
                        max_length=4),
                    max_length=7),
                max_length=5)]

struct_of_maps = StructGen([['child0', BooleanGen()]] + [
    ['child%d' % (i + 1), gen] for i, gen in enumerate(map_gens)])

@pytest.mark.parametrize('data_gen', [pytest.param((StructGen([['child0', DecimalGen(7, 2)]]),
                                                    StructGen([['child1', IntegerGen()]]))),
                                      # left_struct(child0 = 4 level nested struct, child1 = Int)
                                      # right_struct(child0 = 4 level nested struct, child1 = missing)
                                      (StructGen([['child0', StructGen([['child0', StructGen([['child0', StructGen([['child0',
                                                             StructGen([['child0', DecimalGen(7, 2)]])]])]])]])], ['child1', IntegerGen()]], nullable=False),
                                       StructGen([['child0', StructGen([['child0', StructGen([['child0', StructGen([['child0',
                                                            StructGen([['child0', DecimalGen(7, 2)]])]])]])]])]], nullable=False)),
                                      # left_struct(child0 = 4 level nested struct, child1=missing)
                                      # right_struct(child0 = missing struct, child1 = Int)
                                      (StructGen([['child0', StructGen([['child0', StructGen([['child0', StructGen([['child0',
                                                             StructGen([['child0', DecimalGen(7, 2)]])]])]])]])]], nullable=False),
                                       StructGen([['child1', IntegerGen()]], nullable=False)),
                                      (StructGen([['child0', DecimalGen(7, 2)]], nullable=False),
                                       StructGen([['child1', IntegerGen()]], nullable=False)),
                                      # left_struct(child0 = Map[String, String], child1 = missing map)
                                      # right_struct(child0 = missing map, child1 = Map[Boolean, Boolean])
                                      (StructGen([['child0', simple_string_to_string_map_gen]], nullable=False),
                                       StructGen([['child1', MapGen(BooleanGen(nullable=False), boolean_gen)]], nullable=False))], ids=idfn)
# This tests the union of DF of structs with different types of cols as long as the struct itself
# isn't null. This is a limitation in cudf because we don't support nested types as literals
def test_union_struct_missing_children(data_gen):
    left_gen, right_gen = data_gen
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : binary_op_df(spark, left_gen).unionByName(binary_op_df(
            spark, right_gen), True))

@pytest.mark.parametrize('data_gen', all_gen + map_gens + array_gens_sample +
                                     [all_basic_struct_gen,
                                      StructGen([['child0', DecimalGen(7, 2)]]),
                                      nested_struct,
                                      struct_of_maps], ids=idfn)
# This tests union of two DFs of two cols each. The types of the left col and right col is the same
def test_union(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : binary_op_df(spark, data_gen).union(binary_op_df(spark, data_gen)))

@pytest.mark.parametrize('data_gen', all_gen + map_gens + array_gens_sample +
                                     [all_basic_struct_gen,
                                      StructGen([['child0', DecimalGen(7, 2)]]),
                                      nested_struct,
                                      struct_of_maps], ids=idfn)
# This tests union of two DFs of two cols each. The types of the left col and right col is the same
def test_unionAll(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : binary_op_df(spark, data_gen).unionAll(binary_op_df(spark, data_gen)))

@pytest.mark.parametrize('data_gen', all_gen + map_gens + array_gens_sample +
                                     [all_basic_struct_gen,
                                      pytest.param(all_basic_struct_gen),
                                      pytest.param(StructGen([[ 'child0', DecimalGen(7, 2)]])),
                                      nested_struct,
                                      StructGen([['child0', StructGen([['child0', StructGen([['child0', StructGen([['child0',
                                                            StructGen([['child0', DecimalGen(7, 2)]])]])]])]])], ['child1', IntegerGen()]]),
                                      struct_of_maps], ids=idfn)
# This tests the union of two DFs of structs with missing child column names. The missing child
# column will be replaced by nulls in the output DF. This is a feature added in 3.1+
def test_union_by_missing_col_name(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : binary_op_df(spark, data_gen).withColumnRenamed("a", "x")
                                .unionByName(binary_op_df(spark, data_gen).withColumnRenamed("a",
                                                                                             "y"), True))


# the first number ('1' and '2') is the nest level
# the second number ('one' and 'two') is the fields number in the struct
base_one = (ArrayGen(StructGen([["ba", StringGen()]]), 1, 1), ArrayGen(StructGen([["bb", StringGen()]]), 1, 1))
base_two = (ArrayGen(StructGen([["ba", StringGen()], ["bb", StringGen()]]), 1, 1), ArrayGen(StructGen([["bb", StringGen()], ["ba", StringGen()]]), 1, 1))
nest_1_one = (StructGen([('b', base_one[0])]), StructGen([('b', base_one[1])]))
nest_1_two = (StructGen([('b', base_two[0])]), StructGen([('b', base_two[1])]))
nest_2_one = (StructGen([('b', ArrayGen(base_one[0], 1, 1))]), StructGen([('b', ArrayGen(base_one[1],1,1))]))
nest_2_two = (StructGen([('b', ArrayGen(base_two[0], 1, 1))]), StructGen([('b', ArrayGen(base_two[1],1,1))]))

@pytest.mark.parametrize('gen_pair', [base_one,   base_two,
                                      nest_1_one, nest_1_two,
                                      nest_2_one, nest_2_two])
@pytest.mark.skipif(is_before_spark_330(), reason="This is supported only in Spark 3.3.0+")
def test_union_by_missing_field_name_in_arrays_structs(gen_pair):
    """
    This tests the union of two DFs of arrays of structs with missing field names.
    The missing field will be replaced be nulls in the output DF. This is a feature added in 3.3+
    This test is for https://github.com/NVIDIA/spark-rapids/issues/3953
    Test cases are copies from https://github.com/apache/spark/commit/5241d98800 
    """
    def assert_union_equal(gen1, gen2):
        assert_gpu_and_cpu_are_equal_collect(
            lambda spark: unary_op_df(spark, gen1).unionByName(unary_op_df(spark, gen2), True))
    
    assert_union_equal(gen_pair[0], gen_pair[1])
    assert_union_equal(gen_pair[1], gen_pair[0])



@pytest.mark.parametrize('data_gen', all_gen + map_gens + array_gens_sample +
                                     [all_basic_struct_gen,
                                      StructGen([['child0', DecimalGen(7, 2)]]),
                                      nested_struct,
                                      struct_of_maps], ids=idfn)
def test_union_by_name(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : binary_op_df(spark, data_gen).unionByName(binary_op_df(spark,
                                                                                  data_gen)))


@pytest.mark.parametrize('data_gen', [
    pytest.param([('basic' + str(i), gen) for i, gen in enumerate(all_basic_gens + decimal_gens + [binary_gen])]),
    pytest.param([('struct' + str(i), gen) for i, gen in enumerate(struct_gens_sample)]),
    pytest.param([('array' + str(i), gen) for i, gen in enumerate(array_gens_sample + [ArrayGen(BinaryGen(max_length=5), max_length=5)])]),
    pytest.param([('map' + str(i), gen) for i, gen in enumerate(map_gens_sample)]),
], ids=idfn)
@allow_non_gpu(*non_utc_allow)
def test_coalesce_types(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: gen_df(spark, data_gen).coalesce(2))

@pytest.mark.parametrize('num_parts', [1, 10, 100, 1000, 2000], ids=idfn)
@pytest.mark.parametrize('length', [0, 2048, 4096], ids=idfn)
@allow_non_gpu(*non_utc_allow)
def test_coalesce_df(num_parts, length):
    #This should change eventually to be more than just the basic gens
    gen_list = [('_c' + str(i), gen) for i, gen in enumerate(all_basic_gens + decimal_gens + [binary_gen])]
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : gen_df(spark, gen_list, length=length).coalesce(num_parts))

@pytest.mark.parametrize('data_gen', [
    pytest.param([('_c' + str(i), gen) for i, gen in enumerate(all_basic_gens + decimal_gens + [binary_gen])]),
    pytest.param([('s', StructGen([['child0', all_basic_struct_gen]]))]),
    pytest.param([('a', ArrayGen(string_gen))]),
    pytest.param([('m', simple_string_to_string_map_gen)]),
], ids=idfn)
@pytest.mark.parametrize('num_parts', [1, 10, 2345], ids=idfn)
@pytest.mark.parametrize('length', [0, 2048, 4096], ids=idfn)
@ignore_order(local=True) # To avoid extra data shuffle by 'sort on Spark' for this repartition test.
@allow_non_gpu(*non_utc_allow)
def test_repartition_df(data_gen, num_parts, length):
    from pyspark.sql.functions import lit
    assert_gpu_and_cpu_are_equal_collect(
            # Add a computed column to avoid shuffle being optimized back to a CPU shuffle
            lambda spark : gen_df(spark, data_gen, length=length).withColumn('x', lit(1)).repartition(num_parts),
            # disable sort before shuffle so round robin works for maps
            conf = {'spark.sql.execution.sortBeforeRepartition': 'false'})

@pytest.mark.parametrize('data_gen', [
    pytest.param([('_c' + str(i), gen) for i, gen in enumerate(all_basic_gens + decimal_gens)]),
    pytest.param([('s', StructGen([['child0', all_basic_struct_gen]]))]),
    pytest.param([('_c' + str(i), ArrayGen(gen)) for i, gen in enumerate(all_basic_gens + decimal_gens)]),
], ids=idfn)
@pytest.mark.parametrize('num_parts', [1, 10, 2345], ids=idfn)
@pytest.mark.parametrize('length', [0, 2048, 4096], ids=idfn)
@ignore_order(local=True) # To avoid extra data shuffle by 'sort on Spark' for this repartition test.
@allow_non_gpu(*non_utc_allow)
def test_repartition_df_for_round_robin(data_gen, num_parts, length):
    from pyspark.sql.functions import lit
    assert_gpu_and_cpu_are_equal_collect(
        # Add a computed column to avoid shuffle being optimized back to a CPU shuffle
        lambda spark : gen_df(spark, data_gen, length=length).withColumn('x', lit(1)).repartition(num_parts),
        # Enable sort for round robin partition
        conf = {'spark.sql.execution.sortBeforeRepartition': 'true'}) # default is true

@allow_non_gpu('ShuffleExchangeExec', 'RoundRobinPartitioning')
@pytest.mark.parametrize('data_gen', [[('a', simple_string_to_string_map_gen)]], ids=idfn)
@ignore_order(local=True) # To avoid extra data shuffle by 'sort on Spark' for this repartition test.
def test_round_robin_sort_fallback(data_gen):
    from pyspark.sql.functions import lit
    # Disable AQE as it can change the shuffle type
    assert_gpu_fallback_collect(
            # Add a computed column to avoid shuffle being optimized back to a CPU shuffle like in test_repartition_df
            lambda spark : gen_df(spark, data_gen).withColumn('extra', lit(1)).repartition(13),
            'ShuffleExchangeExec',
        conf = {'spark.sql.adaptive.enabled': 'false'})

@allow_non_gpu("ProjectExec", "ShuffleExchangeExec")
@ignore_order(local=True) # To avoid extra data shuffle by 'sort on Spark' for this repartition test.
@pytest.mark.parametrize('num_parts', [2, 10, 17, 19, 32], ids=idfn)
@pytest.mark.parametrize('gen', [([('ag', ArrayGen(StructGen([('b1', long_gen)])))], ['ag'])], ids=idfn)
def test_hash_repartition_exact_fallback(gen, num_parts):
    data_gen = gen[0]
    part_on = gen[1]
    # Disable AQE as it can change the shuffle type
    assert_gpu_fallback_collect(
        lambda spark : gen_df(spark, data_gen, length=1024) \
            .repartition(num_parts, *part_on) \
            .withColumn('id', f.spark_partition_id()) \
            .selectExpr('*'), "ShuffleExchangeExec",
        # Force to use murmur3
        conf = {'spark.rapids.sql.partitioning.hashFunction.enabled': False,
                'spark.sql.adaptive.enabled': 'false'})

@allow_non_gpu("Murmur3Hash")
@pytest.mark.parametrize('data_gen', [ArrayGen(StructGen([('b1', long_gen)]))], ids=idfn)
def test_hash_fallback_bridge(data_gen):
    assert_gpu_fallback_collect(
        lambda spark : unary_op_df(spark, data_gen, length=1024) \
            .selectExpr('*', 'hash(a) as h'), "Murmur3Hash",
        conf = {"spark.rapids.sql.expression.cpuBridge.enabled": True})

@allow_non_gpu("ProjectExec")
@pytest.mark.parametrize('data_gen', [ArrayGen(StructGen([('b1', long_gen)]))], ids=idfn)
def test_hash_fallback(data_gen):
    assert_gpu_fallback_collect(
        lambda spark : unary_op_df(spark, data_gen, length=1024) \
            .selectExpr('*', 'hash(a) as h'), "Murmur3Hash",
        conf = {"spark.rapids.sql.expression.cpuBridge.enabled": False})

@ignore_order(local=True) # To avoid extra data shuffle by 'sort on Spark' for this repartition test.
@pytest.mark.parametrize('num_parts', [1, 2, 10, 17, 19, 32], ids=idfn)
@pytest.mark.parametrize('gen', [
    ([('a', boolean_gen)], ['a']),
    ([('a', byte_gen)], ['a']), 
    ([('a', short_gen)], ['a']),
    ([('a', int_gen)], ['a']),
    ([('a', long_gen)], ['a']),
    ([('a', float_gen)], ['a']),
    ([('a', double_gen)], ['a']),
    ([('a', timestamp_gen)], ['a']),
    ([('a', date_gen)], ['a']),
    ([('a', decimal_gen_32bit)], ['a']),
    ([('a', decimal_gen_64bit)], ['a']),
    ([('a', decimal_gen_128bit)], ['a']),
    ([('a', string_gen)], ['a']),
    ([('a', null_gen)], ['a']),
    ([('a', StructGen([('c0', boolean_gen), ('c1', StructGen([('c1_0', byte_gen), ('c1_1', string_gen), ('c1_2', boolean_gen)]))]))], ['a']), 
    ([('a', long_gen), ('b', StructGen([('b1', long_gen)]))], ['a']),
    ([('a', long_gen), ('b', ArrayGen(long_gen, max_length=2))], ['a']),
    ([('a', byte_gen)], [f.col('a') - 5]), 
    ([('a', ArrayGen(long_gen, max_length=2)), ('b', long_gen)], ['a']),
    ([('a', StructGen([('aa', ArrayGen(long_gen, max_length=2))])), ('b', long_gen)], ['a']),
    ([('a', byte_gen), ('b', boolean_gen)], ['a', 'b']),
    ([('a', short_gen), ('b', string_gen)], ['a', 'b']),
    ([('a', int_gen), ('b', byte_gen)], ['a', 'b']),
    ([('a', long_gen), ('b', null_gen)], ['a', 'b']),
    ([('a', byte_gen), ('b', boolean_gen), ('c', short_gen)], ['a', 'b', 'c']),
    ([('a', float_gen), ('b', double_gen), ('c', short_gen)], ['a', 'b', 'c']),
    ([('a', timestamp_gen), ('b', date_gen), ('c', int_gen)], ['a', 'b', 'c']),
    ([('a', short_gen), ('b', string_gen), ('c', int_gen)], ['a', 'b', 'c']),
    ([('a', decimal_gen_64bit), ('b', decimal_gen_64bit), ('c', decimal_gen_64bit)], ['a', 'b', 'c']),
    ([('a', decimal_gen_128bit), ('b', decimal_gen_128bit), ('c', decimal_gen_128bit)], ['a', 'b', 'c']),
    ], ids=idfn)
@allow_non_gpu(*non_utc_allow)
def test_hash_repartition_exact(gen, num_parts):
    data_gen = gen[0]
    part_on = gen[1]
    # Disable AQE as it can change the shuffle type
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : gen_df(spark, data_gen, length=1024)\
                    .repartition(num_parts, *part_on)\
                    .withColumn('id', f.spark_partition_id())\
                    .withColumn('hashed', f.hash(*part_on))\
                    .selectExpr('*', 'pmod(hashed, {})'.format(num_parts)),
        conf = {'spark.sql.adaptive.enabled': 'false'})


@ignore_order(local=True)  # To avoid extra data shuffle by 'sort on Spark' for this repartition test.
@pytest.mark.parametrize('num_parts', [1, 2, 10, 17, 19, 32], ids=idfn)
@pytest.mark.parametrize('is_ansi_mode', [False, True], ids=idfn)
@allow_non_gpu(*non_utc_allow)
def test_hash_repartition_exact_longs_no_overflow(num_parts, is_ansi_mode):
    gen = LongGen(min_val=-1000, max_val=1000, special_cases=[]) if is_ansi_mode else long_gen
    data_gen = [('a', gen)]
    part_on = [f.col('a') + 15]
    # Disable AQE as it can change the shuffle type
    conf = {'spark.sql.ansi.enabled': is_ansi_mode, 'spark.sql.adaptive.enabled': 'false'}

    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: gen_df(spark, data_gen, length=1024)
                        .repartition(num_parts, *part_on)
                        .withColumn('id', f.spark_partition_id())
                        .withColumn('hashed', f.hash(*part_on))
                        .selectExpr('*', 'pmod(hashed, {})'.format(num_parts)), conf=conf)


@ignore_order(local=True)  # To avoid extra data shuffle by 'sort on Spark' for this repartition test.
@pytest.mark.parametrize('num_parts', [17], ids=idfn)
@allow_non_gpu(*non_utc_allow)
def test_hash_repartition_long_overflow_ansi_exception(num_parts):
    conf = copy_and_update(ansi_enabled_conf, {})

    def test_function(spark):
        df = gen_df(spark, [('a', long_gen)], length=1024)
        maxVal = df.selectExpr("max(a) as m").head()['m']
        overflowVal = (1 << 63) - maxVal
        return df.withColumn('plus', f.col('a') + overflowVal) \
            .repartition(num_parts, f.col('plus')) \
            .withColumn('id', f.spark_partition_id()) \
            .withColumn('hashed', f.hash(f.col('a') + overflowVal)) \
            .selectExpr('*', 'pmod(hashed, {})'.format(num_parts))

    assert_gpu_and_cpu_error(
        lambda spark: test_function(spark).collect(),
        conf=conf, error_message="ArithmeticException")


# Test a query that should cause Spark to leverage getShuffleRDD
@ignore_order(local=True)
def test_union_with_filter():
    def doit(spark):
        dfa = spark.range(1, 100).withColumn("id2", f.col("id"))
        dfb = dfa.groupBy("id").agg(f.size(f.collect_set("id2")).alias("idc"))
        dfc = dfb.filter(f.col("idc") == 1).select("id")
        return dfc.union(dfc)
    conf = { "spark.sql.adaptive.enabled": "true"}
    assert_gpu_and_cpu_are_equal_collect(doit, conf)
