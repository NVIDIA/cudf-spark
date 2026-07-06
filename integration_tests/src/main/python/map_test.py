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

from asserts import *
from conftest import is_not_utc
from data_gen import *
from conftest import is_databricks_runtime
from marks import allow_non_gpu, datagen_overrides, disable_ansi_mode, ignore_order
from spark_session import *
from pyspark.sql.functions import create_map, col, lit, row_number
from pyspark.sql.types import *
from pyspark.sql.types import IntegralType
from pyspark.sql.window import Window


basic_struct_gen = StructGen([
    ['child' + str(ind), sub_gen]
    for ind, sub_gen in enumerate([StringGen(), ByteGen(), ShortGen(), IntegerGen(), LongGen(),
                                   BooleanGen(), DateGen(), TimestampGen(), null_gen] + decimal_gens)],
    nullable=False)

maps_with_binary_value = [MapGen(IntegerGen(nullable=False), BinaryGen(max_length=5))]
# we need to fix https://github.com/NVIDIA/spark-rapids/issues/8985 and add to
# map_keys, map_values, and map_entries tests
maps_with_binary_key = [MapGen(BinaryGen(nullable=False), BinaryGen(max_length=5))]
maps_with_array_key = [
    MapGen(ArrayGen(IntegerGen(), nullable=False, max_length=5, convert_to_tuple=True),
           IntegerGen())]
maps_with_struct_key = [
    MapGen(StructGen([['child0', IntegerGen()],
                      ['child1', IntegerGen()]], nullable=False),
           IntegerGen())]

supported_key_map_gens = \
    map_gens_sample + \
    maps_with_binary_value + \
    decimal_64_map_gens + \
    decimal_128_map_gens

not_supported_get_map_value_keys_map_gens = \
    maps_with_binary_key + \
    maps_with_array_key + \
    maps_with_struct_key


@pytest.mark.parametrize('data_gen', supported_key_map_gens, ids=idfn)
@allow_non_gpu(*non_utc_allow)
def test_map_keys(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: unary_op_df(spark, data_gen).selectExpr(
                # Technically the order of the keys could change, and be different and still correct
                # but it works this way for now so lets see if we can maintain it.
                # Good thing too, because we cannot support sorting all of the types that could be
                # in here yet, and would need some special case code for checking equality
                'map_keys(a)'))


@pytest.mark.parametrize('data_gen', supported_key_map_gens, ids=idfn)
@allow_non_gpu(*non_utc_allow)
def test_map_values(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: unary_op_df(spark, data_gen).selectExpr(
                # Technically the order of the values could change, and be different and still correct
                # but it works this way for now so lets see if we can maintain it.
                # Good thing too, because we cannot support sorting all of the types that could be
                # in here yet, and would need some special case code for checking equality
                'map_values(a)'))


@pytest.mark.parametrize('data_gen', supported_key_map_gens, ids=idfn)
def test_map_entries(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: unary_op_df(spark, data_gen).selectExpr(
                # Technically the order of the values could change, and be different and still correct
                # but it works this way for now so lets see if we can maintain it.
                # Good thing too, because we cannot support sorting all of the types that could be
                # in here yet, and would need some special case code for checking equality
                'map_entries(a)'))


@pytest.mark.parametrize('data_gen', supported_key_map_gens, ids=idfn)
def test_map_from_entries(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: unary_op_df(spark, data_gen).selectExpr(
                'map_from_entries(map_entries(a))'),
            conf={'spark.sql.mapKeyDedupPolicy': 'LAST_WIN'})


@pytest.mark.parametrize('key_gen,value_gen', [
    (IntegerGen(nullable=False), IntegerGen()),
    (StringGen(nullable=False), StringGen()),
    (LongGen(nullable=False), DoubleGen()),
    (IntegerGen(nullable=False), ArrayGen(IntegerGen())),
    (StringGen(nullable=False), StructGen([['child', IntegerGen()]])),
], ids=idfn)
def test_map_from_entries_basic(key_gen, value_gen):
    # Create an array of struct<key, value> and convert to map
    struct_gen = StructGen([
        ('key', key_gen),
        ('value', value_gen)
    ], nullable=False)
    array_gen = ArrayGen(struct_gen, max_length=10)
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: unary_op_df(spark, array_gen).selectExpr(
                'map_from_entries(a)'),
            conf={'spark.sql.mapKeyDedupPolicy': 'LAST_WIN'})


@pytest.mark.parametrize('key_gen,value_gen', [
    (IntegerGen(nullable=False), IntegerGen()),
    (StringGen(nullable=False), StringGen()),
    (LongGen(nullable=False), DoubleGen()),
    (IntegerGen(nullable=False), ArrayGen(IntegerGen())),
    (StringGen(nullable=False), StructGen([['child', IntegerGen()]])),
], ids=idfn)
def test_map_from_entries_null_struct_entries(key_gen, value_gen):
    struct_gen = StructGen([
        ('key', key_gen),
        ('value', value_gen)
    ], nullable=True)
    array_gen = ArrayGen(struct_gen, max_length=10)
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: unary_op_df(spark, array_gen).selectExpr(
                'map_from_entries(a)'),
            conf={'spark.sql.mapKeyDedupPolicy': 'LAST_WIN'})


@pytest.mark.parametrize('key_gen,value_gen', [
    (IntegerGen(nullable=True), IntegerGen()),
    (StringGen(nullable=True), StringGen()),
    (LongGen(nullable=True), DoubleGen()),
    (IntegerGen(nullable=True), ArrayGen(IntegerGen())),
    (StringGen(nullable=True), StructGen([['child', IntegerGen()]])),
], ids=idfn)
def test_map_from_entries_null_keys(key_gen, value_gen):
    # Create an array of struct<key, value> and convert to map
    struct_gen = StructGen([
        ('key', key_gen),
        ('value', value_gen)
    ], nullable=False)
    array_gen = ArrayGen(struct_gen, max_length=10)
    assert_gpu_and_cpu_error(
            lambda spark: unary_op_df(spark, array_gen).selectExpr(
                'map_from_entries(a)').collect(),
            conf={'spark.sql.mapKeyDedupPolicy': 'LAST_WIN'},
            error_message='Cannot use null as map key')


def test_map_from_entries_dup_exception():
    # Test that duplicate keys throw an exception with EXCEPTION policy
    struct_gen = RepeatSeqGen(StructGen([
        ('key', IntegerGen(nullable=False)),
        ('value', IntegerGen())
    ], nullable=False), length=2)
    array_gen = ArrayGen(struct_gen, min_length=2, max_length=10)
    def do_it(spark):
        df = unary_op_df(spark, array_gen)
        result = df.selectExpr(
                'map_from_entries(a)').collect()
        return result

    assert_gpu_and_cpu_error(
            do_it,
            conf={'spark.sql.mapKeyDedupPolicy': 'EXCEPTION'},
            error_message='Duplicate map key')


def get_map_value_gens(precision=18, scale=0):
    def simple_struct_value_gen():
        return StructGen([["child", IntegerGen()]])

    def nested_struct_value_gen():
        return StructGen([["child", simple_struct_value_gen()]])

    def nested_map_value_gen():
        return MapGen(StringGen(pattern='key_[0-9]', nullable=False), IntegerGen(), max_length=6)

    def array_value_gen():
        return ArrayGen(IntegerGen(), max_length=6)

    def decimal_value_gen():
        return DecimalGen(precision, scale)

    return [ByteGen, ShortGen, IntegerGen, LongGen, FloatGen, DoubleGen,
            StringGen, DateGen, TimestampGen, decimal_value_gen, BinaryGen,
            simple_struct_value_gen, nested_struct_value_gen, nested_map_value_gen, array_value_gen]


@pytest.mark.parametrize('data_gen',
                         [MapGen(StringGen(pattern='key_[0-9]', nullable=False), value(), max_length=6)
                          for value in get_map_value_gens()],
                         ids=idfn)
def test_get_map_value_string_keys(data_gen):
    index_gen = StringGen()
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: gen_df(spark, [("a", data_gen), ("ix", index_gen)]).selectExpr(
                'a[ix]',
                'a["key_0"]',
                'a["key_1"]',
                'a[null]',
                'a["key_9"]',
                'a["NOT_FOUND"]',
                'a["key_5"]'))


numeric_key_gens = [
    key(nullable=False) if key in [FloatGen, DoubleGen, DecimalGen]
    else key(nullable=False, min_val=0, max_val=100)
    for key in [ByteGen, ShortGen, IntegerGen, LongGen, FloatGen, DoubleGen, DecimalGen]]

numeric_key_map_gens = [MapGen(key, value(), max_length=6)
                        for key in numeric_key_gens for value in get_map_value_gens()]


@disable_ansi_mode  # ANSI mode failures are tested separately.
@pytest.mark.parametrize('data_gen', numeric_key_map_gens, ids=idfn)
def test_get_map_value_numeric_keys(data_gen):
    key_gen = data_gen._key_gen
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: gen_df(spark, [("a", data_gen), ("ix", key_gen)]).selectExpr(
            'a[ix]',
            'a[0]',
            'a[1]',
            'a[null]',
            'a[-9]',
            'a[999]'))


@disable_ansi_mode  # ANSI mode failures are tested separately.
@pytest.mark.parametrize('data_gen', supported_key_map_gens, ids=idfn)
@allow_non_gpu(*non_utc_allow)
def test_get_map_value_supported_keys(data_gen):
    key_gen = data_gen._key_gen
    # first expression is not guaranteed to hit
    # the second expression with map_keys will hit on the first key, or null
    # on an empty dictionary generated in `a`
    assert_cpu_and_gpu_are_equal_collect_with_capture(
        lambda spark: gen_df(spark, [("a", data_gen), ("ix", key_gen)]) \
            .selectExpr('a[ix]', 'a[map_keys(a)[0]]'),
        exist_classes="GpuGetMapValue,GpuMapKeys")


@allow_non_gpu("GetMapValue")
@pytest.mark.parametrize('data_gen', not_supported_get_map_value_keys_map_gens, ids=idfn)
def test_get_map_value_fallback_keys(data_gen):
    key_gen = data_gen._key_gen
    assert_gpu_fallback_collect(
        lambda spark: gen_df(spark, [("a", data_gen), ("ix", key_gen)]) \
            .selectExpr('a[ix]'),
        cpu_fallback_class_name="GetMapValue")


@disable_ansi_mode  # ANSI mode failures are tested separately.
@pytest.mark.parametrize('key_gen', numeric_key_gens, ids=idfn)
def test_basic_scalar_map_get_map_value(key_gen):
    def query_map_scalar(spark):
        return unary_op_df(spark, key_gen).selectExpr('map(0, "zero", 1, "one")[a]')
    assert_cpu_and_gpu_are_equal_collect_with_capture(
        query_map_scalar,
        # check that GpuGetMapValue wasn't optimized out
        exist_classes="GpuGetMapValue",
        conf = {"spark.rapids.sql.explain": "NONE",
                # this is set to True so we don't fall back due to float/double -> int
                # casting (because the keys of the scalar map are integers)
                "spark.rapids.sql.castFloatToIntegralTypes.enabled": True})


@allow_non_gpu('WindowLocalExec')
@datagen_overrides(seed=0, condition=is_before_spark_314()
                             or (not is_before_spark_320() and is_before_spark_323())
                             or (not is_before_spark_330() and is_before_spark_331()), reason="https://issues.apache.org/jira/browse/SPARK-40089")
@pytest.mark.parametrize('data_gen', supported_key_map_gens, ids=idfn)
@allow_non_gpu(*non_utc_allow)
def test_map_scalars_supported_key_types(data_gen):
    key_gen = data_gen._key_gen
    def query_map_scalar(spark):
        key_df = gen_df(spark, [("key", key_gen)], length=100).orderBy(col("key"))\
            .select(col("key"),
                    row_number().over(Window().orderBy(col('key')))\
                        .alias("row_num"))
        key_df.select(col("key").alias("key_at_ix"))\
            .where(col("row_num") == 5)\
            .repartition(100)\
            .createOrReplaceTempView("single_key_tbl")
        key_df.select(col("key").alias("key_at_ix_next")) \
            .where(col("row_num") == 6) \
            .repartition(100) \
            .createOrReplaceTempView("single_key_tbl_next")
        # There will be a single row in single_key_tbl (the row that matched key_ix).
        # We repartition this table to create several empty tables to test empty partitions,
        # and also because the window operation put everything into a one partition prior.
        # Because this is a single key, first(ignore_nulls = true) will be deterministic.
        return spark.sql(
            "select key_at_ix, " +
            "       (select first(map(key_at_ix, 'value'), true) " +
            "        from single_key_tbl)[key_at_ix], " +
            # this one is on purpose using `key_at_ix` to guarantee we won't match the key
            "       (select first(map(key_at_ix_next, 'value'), true) " +
            "        from single_key_tbl_next)[key_at_ix] " +
            "from single_key_tbl")
    # Disable AQE temporarily until https://github.com/NVIDIA/spark-rapids/issues/14319 is resolved.
    assert_cpu_and_gpu_are_equal_collect_with_capture(
        query_map_scalar,
        # check that GpuGetMapValue wasn't optimized out
        exist_classes="GpuGetMapValue",
        conf = {"spark.rapids.sql.explain": "NONE", 'spark.sql.adaptive.enabled': 'false'})


@pytest.mark.parametrize('data_gen',
                         [MapGen(DateGen(nullable=False), value(), max_length=6)
                          for value in get_map_value_gens()], ids=idfn)
@allow_non_gpu(*non_utc_allow)
def test_get_map_value_date_keys(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: unary_op_df(spark, data_gen).selectExpr(
            'a[date "1997"]',
            'a[date "2022-01-01"]',
            'a[null]'))


@pytest.mark.parametrize('data_gen',
                         [MapGen(TimestampGen(nullable=False), value(), max_length=6)
                          for value in get_map_value_gens()], ids=idfn)
@allow_non_gpu(*non_utc_allow)
def test_get_map_value_timestamp_keys(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: unary_op_df(spark, data_gen).selectExpr(
            'a[timestamp "1997"]',
            'a[timestamp "2022-01-01"]',
            'a[null]'))


def test_map_side_effects():
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: spark.range(10).selectExpr(
                'id',
                'if(id == 0, null, map(id, id, id DIV 2, id)) as m'),
            conf={'spark.sql.mapKeyDedupPolicy': 'EXCEPTION'})


@pytest.mark.parametrize('key_gen', [StringGen(nullable=False), IntegerGen(nullable=False), basic_struct_gen], ids=idfn)
@pytest.mark.parametrize('value_gen', [StringGen(nullable=True), IntegerGen(nullable=True), basic_struct_gen], ids=idfn)
@allow_non_gpu(*non_utc_allow)
def test_single_entry_map(key_gen, value_gen):
    data_gen = [('a', key_gen), ('b', value_gen)]
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: gen_df(spark, data_gen).selectExpr(
                'map("literal_key", b) as map1',
                'map(a, b) as map2'))


def test_map_expr_no_pairs():
    data_gen = [('a', StringGen(nullable=False)), ('b', StringGen(nullable=False))]
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: gen_df(spark, data_gen).selectExpr(
                'map() as m1'))


def test_map_expr_multiple_pairs():
    # we don't hit duplicate keys in this test due to the high cardinality of the generated strings
    data_gen = [('a', StringGen(nullable=False)), ('b', StringGen(nullable=False))]
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: gen_df(spark, data_gen).selectExpr(
                'map("key1", b, "key2", a) as m1',
                'map(a, b, b, a) as m2'))


def test_map_expr_expr_keys_dupe_last_win():
    data_gen = [('a', StringGen(nullable=False)), ('b', StringGen(nullable=False))]
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: gen_df(spark, data_gen).selectExpr(
                'map(a, b, a, b) as m2'),
            conf={'spark.sql.mapKeyDedupPolicy':'LAST_WIN'})


def test_map_expr_expr_keys_dupe_exception():
    data_gen = [('a', StringGen(nullable=False)), ('b', StringGen(nullable=False))]
    assert_gpu_and_cpu_error(
            lambda spark: gen_df(spark, data_gen).selectExpr(
                'map(a, b, a, b) as m2').collect(),
            conf={'spark.sql.mapKeyDedupPolicy':'EXCEPTION'},
            error_message = "Duplicate map key")

def test_map_keys_null_exception():
    assert_gpu_and_cpu_error(
            lambda spark: spark.sql(
                "select map(x, -1) from (select explode(array(1,null)) as x)").collect(),
            conf = {},
            error_message = "Cannot use null as map key")

def test_map_expr_literal_keys_dupe_last_win():
    data_gen = [('a', StringGen(nullable=False)), ('b', StringGen(nullable=False))]
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: gen_df(spark, data_gen).selectExpr(
                'map("key1", b, "key1", a) as m1'),
            conf={'spark.sql.mapKeyDedupPolicy':'LAST_WIN'})


@pytest.mark.parametrize('map_expr',['map("key1", b, "key1", a) as m1', 
                                     'map(double("NaN"), b, double("NaN"), a) as m1'], ids=idfn)
def test_map_expr_literal_keys_dupe_exception(map_expr):
    data_gen = [('a', StringGen(nullable=False)), ('b', StringGen(nullable=False))]
    assert_gpu_and_cpu_error(
            lambda spark: gen_df(spark, data_gen).selectExpr(map_expr).collect(),
            conf={'spark.sql.mapKeyDedupPolicy':'EXCEPTION'},
            error_message = "Duplicate map key")


def test_map_expr_multi_non_literal_keys():
    data_gen = [('a', StringGen(nullable=False)), ('b', StringGen(nullable=False))]
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: gen_df(spark, data_gen).selectExpr(
                'map(a, b, b, a) as m1'))


def test_map_scalar_project():
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: spark.range(2).selectExpr(
                "map(1, 2, 3, 4) as i",
                "map('a', 'b', 'c', 'd') as s",
                "map('a', named_struct('foo', 10, 'bar', 'bar')) as st"
                "id"))


def test_str_to_map_expr_fixed_pattern_input():
    # Test pattern "key1:val1,key2:val2".
    # In order to prevent duplicate keys, the first key starts with a number [0-9] and the second
    # key start with a letter [a-zA-Z].
    data_gen = [('a', StringGen(pattern='[0-9][^:,]{0,10}:[^:,]{0,10},[a-zA-Z][^:,]{0,10}:[^:,]{0,10}',
                                nullable=True))]
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: gen_df(spark, data_gen).selectExpr(
            'str_to_map(a) as m1',
            'str_to_map(a, ",") as m2',
            'str_to_map(a, ",", ":") as m3'))


def test_str_to_map_expr_fixed_delimiters():
    data_gen = [('a', StringGen(pattern='[0-9a-zA-Z:,]{0,100}', nullable=True)
                 .with_special_pattern('[abc]:.{0,20},[abc]:.{0,20}', weight=100))]
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: gen_df(spark, data_gen).selectExpr(
            'str_to_map(a) as m1',
            'str_to_map(a, ",") as m2',
            'str_to_map(a, ",", ":") as m3'
        ), conf={'spark.sql.mapKeyDedupPolicy': 'LAST_WIN'})


def test_str_to_map_expr_random_delimiters():
    class UniqueStringGen(StringGen):
        """Generate unique strings"""
        def __init__(self, pattern, nullable):
            super().__init__(pattern=pattern, nullable=nullable)
            self.previous_values = set()

        def start(self, rand):
            super().start(rand)
            self.previous_values = set()

        def gen(self, force_no_nulls=False):
            v = super().gen(force_no_nulls=force_no_nulls)
            while v in self.previous_values:
                v = super().gen(force_no_nulls=force_no_nulls)
            self.previous_values.add(v)
            return v
    data_gen = [('a', StringGen(pattern='[0-9a-z:,]{0,100}', nullable=True)
                 .with_special_pattern('[abc]:.{0,20},[abc]:.{0,20}', weight=100))]
    delim_gen = UniqueStringGen(pattern='[0-9a-z :,]', nullable=False)
    (pair_delim, keyval_delim) = with_cpu_session(
        lambda spark: gen_scalars_for_sql(delim_gen, 2, force_no_nulls=True))
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: gen_df(spark, data_gen).selectExpr(
            'str_to_map(a) as m1',
            'str_to_map(a, {}) as m2'.format(pair_delim),
            'str_to_map(a, {}, {}) as m3'.format(pair_delim, keyval_delim)
        ), conf={'spark.sql.mapKeyDedupPolicy': 'LAST_WIN'})


def test_str_to_map_expr_input_no_delimiter():
    # Test input strings that contain either one delimiter or do not contain delimiters at all.
    data_gen = [('a', StringGen(pattern='[0-9:,]{0,100}', nullable=True)
                 .with_special_pattern('[abc]:.{0,20},[abc]:.{0,20}', weight=100))]
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: gen_df(spark, data_gen).selectExpr(
            'str_to_map(a, "A", ":") as m1',  # input doesn't contain pair delimiter
            'str_to_map(a, ",", "A") as m2',  # input doesn't contain key-value delimiter
            'str_to_map(a, "A", "A") as m3'   # input doesn't contain any delimiter
        ), conf={'spark.sql.mapKeyDedupPolicy': 'LAST_WIN'})


def test_str_to_map_expr_with_regex_and_non_regex_delimiters():
    data_gen = [('a', StringGen(pattern='(([bf]:{0,5}){1,7},{0,5}[0-9]{1,10}){0,10}',
                                nullable=True))]
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: gen_df(spark, data_gen).selectExpr(
            'str_to_map(a, "[,]") as m1',
            'str_to_map(a, "[,]{1,5}") as m2',
            'str_to_map(a, "[,b]") as m3',
            'str_to_map(a, ",", "[:]") as m4',
            'str_to_map(a, ",", "[:f]") as m5',
            'str_to_map(a, ",", "[:]{1,10}") as m6'
        ), conf={'spark.sql.mapKeyDedupPolicy': 'LAST_WIN'})


def test_str_to_map_expr_with_all_regex_delimiters():
    data_gen = [('a', StringGen(pattern='(([bf]:{0,5}){1,7},{0,5}[0-9]{1,10}){0,10}',
                                nullable=True))]
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: gen_df(spark, data_gen).selectExpr(
            'str_to_map(a, "[,]") as m1',
            'str_to_map(a, "[,]", "[:]") as m2',
            'str_to_map(a, "[,b]", "[:f]") as m3',
            'str_to_map(a, "[,]", "[:]{1,10}") as m4',
            'str_to_map(a, "[,]{1,10}", "[:]{1,10}") as m5'
        ), conf={'spark.sql.mapKeyDedupPolicy': 'LAST_WIN'})


@pytest.mark.parametrize('empty_type', all_empty_string_types)
def test_str_to_map_input_all_empty(empty_type):
    data_gen = mk_empty_str_gen(empty_type)
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : unary_op_df(spark, data_gen).selectExpr(
            'str_to_map(a) as m0',
            'str_to_map(a, ",") as m1',
            'str_to_map(a, ",", ":") as m2'
        ), conf={'spark.sql.mapKeyDedupPolicy': 'LAST_WIN'})

@pytest.mark.skipif(not is_before_spark_330(),
                    reason="Only in Spark 3.1.1+ (< 3.3.0) + ANSI mode, map key throws on no such element")
@pytest.mark.parametrize('data_gen', [simple_string_to_string_map_gen], ids=idfn)
def test_simple_get_map_value_ansi_fail(data_gen):
    message = "org.apache.spark.SparkNoSuchElementException" if is_databricks104_or_later() else "java.util.NoSuchElementException"
    assert_gpu_and_cpu_error(
            lambda spark: unary_op_df(spark, data_gen).selectExpr(
                'a["NOT_FOUND"]').collect(),
            conf=ansi_enabled_conf,
            error_message=message)

@pytest.mark.skipif(is_before_spark_340() and not is_databricks113_or_later(),
                    reason="Only in Spark 3.4+ with ANSI mode, map key returns null on no such element")
@pytest.mark.parametrize('data_gen', [simple_string_to_string_map_gen], ids=idfn)
def test_simple_get_map_value_ansi_null(data_gen):
        assert_gpu_and_cpu_are_equal_collect(
                lambda spark: unary_op_df(spark, data_gen).selectExpr(
                        'a["NOT_FOUND"]'),
                conf=ansi_enabled_conf)

@pytest.mark.skipif(not is_spark_33X() or is_databricks_runtime(),
                    reason="Only in Spark 3.3.X + ANSI mode + Strict Index, map key throws on no such element")
@pytest.mark.parametrize('strict_index', ['true', 'false'])
@pytest.mark.parametrize('data_gen', [simple_string_to_string_map_gen], ids=idfn)
def test_simple_get_map_value_with_strict_index(strict_index, data_gen):
    message = "org.apache.spark.SparkNoSuchElementException"
    test_conf = copy_and_update(ansi_enabled_conf, {'spark.sql.ansi.strictIndexOperator': strict_index})
    if strict_index == 'true':
        assert_gpu_and_cpu_error(
                lambda spark: unary_op_df(spark, data_gen).selectExpr(
                        'a["NOT_FOUND"]').collect(),
                conf=test_conf,
                error_message=message)
    else:
        assert_gpu_and_cpu_are_equal_collect(
                lambda spark: unary_op_df(spark, data_gen).selectExpr(
                        'a["NOT_FOUND"]'),
                conf=test_conf)


@pytest.mark.parametrize('data_gen',
                         [MapGen(StringGen(pattern='key_[0-9]', nullable=False), value(), max_length=6)
                          for value in get_map_value_gens()],
                         ids=idfn)
@allow_non_gpu(*non_utc_allow)
def test_element_at_map_string_keys(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: unary_op_df(spark, data_gen).selectExpr(
                'element_at(a, "key_0")',
                'element_at(a, "key_1")',
                'element_at(a, "null")',
                'element_at(a, "key_9")',
                'element_at(a, "NOT_FOUND")',
                'element_at(a, "key_5")'),
            conf={'spark.sql.ansi.enabled': False})


@pytest.mark.parametrize('data_gen', numeric_key_map_gens, ids=idfn)
@allow_non_gpu(*non_utc_allow)
def test_element_at_map_numeric_keys(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: unary_op_df(spark, data_gen).selectExpr(
            'element_at(a, 0)',
            'element_at(a, 1)',
            'element_at(a, null)',
            'element_at(a, -9)',
            'element_at(a, 999)'),
        conf={'spark.sql.ansi.enabled': False})


@pytest.mark.parametrize('data_gen',
                         [MapGen(DecimalGen(precision=35, scale=2, nullable=False), value(), max_length=6)
                          for value in get_map_value_gens(precision=37, scale=0)],
                         ids=idfn)
@allow_non_gpu(*non_utc_allow)
def test_get_map_value_element_at_map_dec_col_keys(data_gen):
    keys = DecimalGen(precision=35, scale=2)
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: two_col_df(spark, data_gen, keys).selectExpr(
            'element_at(a, b)', 'a[b]'),
        conf={'spark.sql.ansi.enabled': False})


@pytest.mark.parametrize('data_gen',
                         [MapGen(StringGen(pattern='key', nullable=False),
                                 IntegerGen(nullable=False), max_length=1, min_length=1, nullable=False)],
                         ids=idfn)
@pytest.mark.parametrize('ansi', [True, False], ids=idfn)
def test_get_map_value_element_at_map_string_col_keys_ansi(data_gen, ansi):
    keys = StringGen(pattern='key', nullable=False)
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: two_col_df(spark, data_gen, keys).selectExpr(
            'element_at(a, b)', 'a[b]'),
        conf={'spark.sql.ansi.enabled': ansi})


@pytest.mark.parametrize('data_gen',
                         [MapGen(StringGen(pattern='key_[0-9]', nullable=False), value(), max_length=6)
                          for value in get_map_value_gens(precision=37, scale=0)],
                         ids=idfn)
@allow_non_gpu(*non_utc_allow)
def test_get_map_value_element_at_map_string_col_keys(data_gen):
    keys = StringGen(pattern='key_[0-9]')
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: two_col_df(spark, data_gen, keys).selectExpr(
            'element_at(a, b)', 'a[b]'),
        conf={'spark.sql.ansi.enabled': False})

@pytest.mark.parametrize('data_gen', [simple_string_to_string_map_gen], ids=idfn)
@pytest.mark.skipif(is_spark_340_or_later() or is_databricks113_or_later(),
                    reason="Since Spark3.4 and DB11.3, null will always be returned on invalid access to map")
def test_element_at_map_string_col_keys_ansi_fail(data_gen):
    keys = StringGen(pattern='NOT_FOUND')
    message = "org.apache.spark.SparkNoSuchElementException" if (not is_before_spark_330() or is_databricks104_or_later()) else "java.util.NoSuchElementException"
    # For 3.3.X strictIndexOperator should not affect element_at
    test_conf = copy_and_update(ansi_enabled_conf, {'spark.sql.ansi.strictIndexOperator': 'false'})
    assert_gpu_and_cpu_error(
        lambda spark: two_col_df(spark, data_gen, keys).selectExpr(
            'element_at(a, b)').collect(),
        conf=test_conf,
        error_message=message)

@pytest.mark.skipif(is_before_spark_340() and not is_databricks113_or_later(),
                    reason="Only in Spark 3.4 + with ANSI mode, map key returns null on no such element")
@pytest.mark.parametrize('data_gen', [simple_string_to_string_map_gen], ids=idfn)
def test_element_at_map_string_col_keys_ansi_null(data_gen):
    keys = StringGen(pattern='NOT_FOUND')
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: two_col_df(spark, data_gen, keys).selectExpr(
            'element_at(a, b)'),
        conf=ansi_enabled_conf)

@pytest.mark.parametrize('data_gen', [simple_string_to_string_map_gen], ids=idfn)
@pytest.mark.skipif(is_spark_340_or_later() or is_databricks113_or_later(),
                    reason="Since Spark3.4 and DB11.3, null will always be returned on invalid access to map")
def test_get_map_value_string_col_keys_ansi_fail(data_gen):
    keys = StringGen(pattern='NOT_FOUND')
    message = "org.apache.spark.SparkNoSuchElementException" if (not is_before_spark_330() or is_databricks104_or_later()) else "java.util.NoSuchElementException"
    assert_gpu_and_cpu_error(
        lambda spark: two_col_df(spark, data_gen, keys).selectExpr(
            'a[b]').collect(),
        conf=ansi_enabled_conf,
        error_message=message)

@pytest.mark.skipif(is_before_spark_340() and not is_databricks113_or_later(),
                    reason="Only in Spark 3.4 + ANSI mode, map key returns null on no such element")
@pytest.mark.parametrize('data_gen', [simple_string_to_string_map_gen], ids=idfn)
def test_get_map_value_string_col_keys_ansi_null(data_gen):
    keys = StringGen(pattern='NOT_FOUND')
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: two_col_df(spark, data_gen, keys).selectExpr(
            'a[b]'),
        conf=ansi_enabled_conf)

@pytest.mark.parametrize('data_gen',
                         [MapGen(DateGen(nullable=False), value(), max_length=6)
                          for value in get_map_value_gens()], ids=idfn)
@allow_non_gpu(*non_utc_allow)
def test_element_at_map_date_keys(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: unary_op_df(spark, data_gen).selectExpr(
            'element_at(a, date "1997")',
            'element_at(a, date "2022-01-01")',
            'element_at(a, null)'),
        conf={'spark.sql.ansi.enabled': False})


@pytest.mark.parametrize('data_gen',
                         [MapGen(TimestampGen(nullable=False), value(), max_length=6)
                          for value in get_map_value_gens()],
                         ids=idfn)
@allow_non_gpu(*non_utc_allow)
def test_element_at_map_timestamp_keys(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: unary_op_df(spark, data_gen).selectExpr(
            'element_at(a, timestamp "1997")',
            'element_at(a, timestamp "2022-01-01")',
            'element_at(a, null)'),
        conf={'spark.sql.ansi.enabled': False})

@pytest.mark.parametrize('data_gen', [simple_string_to_string_map_gen], ids=idfn)
@pytest.mark.skipif(is_spark_340_or_later() or is_databricks113_or_later(),
                    reason="Since Spark3.4 and DB11.3, null will always be returned on invalid access to map")
def test_map_element_at_ansi_fail(data_gen):
    message = "org.apache.spark.SparkNoSuchElementException" if (not is_before_spark_330() or is_databricks104_or_later()) else "java.util.NoSuchElementException"
    # For 3.3.0+ strictIndexOperator should not affect element_at
    test_conf = copy_and_update(ansi_enabled_conf, {'spark.sql.ansi.strictIndexOperator': 'false'})
    assert_gpu_and_cpu_error(
            lambda spark: unary_op_df(spark, data_gen).selectExpr(
                'element_at(a, "NOT_FOUND")').collect(),
            conf=test_conf,
            error_message=message)

@pytest.mark.skipif(is_before_spark_340() and not is_databricks113_or_later(),
                    reason="Only in Spark 3.4 + ANSI mode, map key returns null on no such element")
@pytest.mark.parametrize('data_gen', [simple_string_to_string_map_gen], ids=idfn)
def test_map_element_at_ansi_null(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: unary_op_df(spark, data_gen).selectExpr(
                'element_at(a, "NOT_FOUND")'),
            conf=ansi_enabled_conf)


@disable_ansi_mode  # ANSI mode failures are tested separately.
@pytest.mark.parametrize('data_gen', map_gens_sample + maps_with_binary_value +
    decimal_128_map_gens + decimal_64_map_gens, ids=idfn)
@allow_non_gpu(*non_utc_allow)
def test_transform_values(data_gen):
    def do_it(spark):
        columns = ['a', 'b',
                   'transform_values(a, (key, value) -> value) as ident',
                   'transform_values(a, (key, value) -> null) as n',
                   'transform_values(a, (key, value) -> 1) as one',
                   'transform_values(a, (key, value) -> key) as indexed',
                   'transform_values(a, (key, value) -> b) as b_val']
        value_type = data_gen.data_type.valueType
        # Arithmetic expansions below can overflow decimal precision, so they
        # run only on IntegralType values.
        if isinstance(value_type, IntegralType):
            columns.extend([
                'transform_values(a, (key, value) -> value + 1) as add',
                'transform_values(a, (key, value) -> value + value) as mul',
                'transform_values(a, (key, value) -> value + b) as all_add'])

        if isinstance(value_type, StringType):
            columns.extend(['transform_values(a, (key, value) -> concat(value, "-test")) as con'])

        if isinstance(value_type, ArrayType):
            columns.extend([
                'transform_values(a, '
                '                 (key, value) -> transform(value, sub_entry -> 1)) as sub_one',
                'transform_values(a, '
                '                 (key, value) -> transform(value, (sub_entry, sub_index) -> sub_index)) as sub_index',
                'transform_values(a, '
                '                 (key, value) -> transform(value, (sub_entry, sub_index) -> sub_index + b)) as add_indexes'])

        if isinstance(value_type, MapType):
            columns.extend([
                'transform_values(a, (key, value) -> transform_values(value, (sub_key, sub_value) -> 1)) as sub_one'])

        if isinstance(value_type, BinaryType):
            columns.extend([
                'transform_values(a, (key, value) -> hex(value)) as hex_val'])

        return two_col_df(spark, data_gen, byte_gen).selectExpr(columns)

    assert_gpu_and_cpu_are_equal_collect(do_it)


@pytest.mark.parametrize('data_gen', map_gens_sample + maps_with_binary_value +
    decimal_128_map_gens + decimal_64_map_gens, ids=idfn)
@allow_non_gpu(*non_utc_allow)
def test_transform_keys(data_gen):
    # The processing here is very limited, because we need to be sure we do not create duplicate keys.
    # This can happen because of integer overflow, round off errors in floating point, etc. So for now
    # we really are only looking at a very basic transformation.
    def do_it(spark):
        columns = ['a', 'transform_keys(a, (key, value) -> key) as ident']
        key_type = data_gen.data_type.keyType
        if isinstance(key_type, StringType):
            columns.extend(['transform_keys(a, (key, value) -> concat(key, "-test")) as con'])

        return unary_op_df(spark, data_gen).selectExpr(columns)

    assert_gpu_and_cpu_are_equal_collect(do_it)


@pytest.mark.parametrize('data_gen', [simple_string_to_string_map_gen], ids=idfn)
def test_transform_keys_null_fail(data_gen):
    assert_gpu_and_cpu_error(
            lambda spark: unary_op_df(spark, data_gen).selectExpr(
                'transform_keys(a, (key, value) -> CAST(null as INT))').collect(),
            conf={},
            error_message='Cannot use null as map key')


@pytest.mark.parametrize('data_gen', [simple_string_to_string_map_gen], ids=idfn)
def test_transform_keys_duplicate_fail(data_gen):
    assert_gpu_and_cpu_error(
            lambda spark: unary_op_df(spark, data_gen).selectExpr(
                'transform_keys(a, (key, value) -> 1)').collect(),
            conf={},
            error_message='Duplicate map key')


@pytest.mark.parametrize('data_gen', [simple_string_to_string_map_gen], ids=idfn)
def test_transform_keys_last_win(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: unary_op_df(spark, data_gen).selectExpr('transform_keys(a, (key, value) -> 1)'),
            conf={'spark.sql.mapKeyDedupPolicy': 'LAST_WIN'})


@pytest.mark.parametrize('data_gen', [MapGen(IntegerGen(nullable=False), long_gen)], ids=idfn)
def test_transform_keys_last_win2(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: unary_op_df(spark, data_gen).selectExpr('transform_keys(a, (key, value) -> key % 2)'),
        conf={'spark.sql.mapKeyDedupPolicy': 'LAST_WIN'})


# We add in several types of processing for foldable functions because the output
# can be different types.
@pytest.mark.parametrize('query', [
    'map_from_arrays(sequence(1, 5), sequence(1, 5)) as m_a',
    'map("a", "a", "b", "c") as m',
    'map(1, sequence(1, 5)) as m'], ids=idfn)
def test_sql_map_scalars(query):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: spark.sql('SELECT {}'.format(query)))


@pytest.mark.parametrize('data_gen', map_gens_sample + maps_with_binary_value \
                         + [MapGen(f(nullable=False, min_val=-10, max_val=10), f(), min_length=10) for f in [ByteGen, ShortGen, IntegerGen, LongGen]] \
                         + [MapGen(StringGen(pattern='key_[0-9]', nullable=False), StringGen(), min_length=10)], ids=idfn)
@allow_non_gpu(*non_utc_allow)
def test_map_zip_with(data_gen):
    def do_it(spark):
        columns = ['a', 'b',
                    'map_zip_with(a, b,  (key, value1, value2) -> value1) as ident1',
                    'map_zip_with(a, b,  (key, value1, value2) -> value2) as ident2',
                    'map_zip_with(a, b,  (key, value1, value2) -> null) as n',
                    'map_zip_with(a, b,  (key, value1, value2) -> 1) as one',
                    'map_zip_with(a, b,  (key, value1, value2) -> key) as indexed',]
        value_type = data_gen.data_type.valueType
        if isinstance(value_type, IntegralType):
            columns.extend([
                    'map_zip_with(a, b,  (key, value1, value2) -> coalesce(value1, 0) + coalesce(value2, 0)) as add',
                    'map_zip_with(a, b,  (key, value1, value2) -> coalesce(value1, 0) * coalesce(value2, 0)) as mul',
                    'map_zip_with(a, b,  (key, value1, value2) -> coalesce(value1, 0) - coalesce(value2, 0)) as sub',
                    'map_zip_with(a, b,  (key, value1, value2) -> coalesce(value1, 1) / if(coalesce(value2, 1) == 0, 1, coalesce(value2, 1))) as div',])
        if isinstance(value_type, StringType):
            columns.extend([
                    'map_zip_with(a, b,  (key, value1, value2) -> concat(coalesce(value1, ""), "-test-", coalesce(value2, ""))) as string_concat',])
        if isinstance(value_type, ArrayType):
            columns.extend([
                    'map_zip_with(a, b,  (key, value1, value2) -> concat(value1, value2)) as array_concat',])
        if isinstance(value_type, BinaryType):
            columns.extend([
                    'map_zip_with(a, b,  (key, value1, value2) -> hex(value1)) as hex_val',])
        df = two_col_df(spark, data_gen, data_gen)
        return df.selectExpr(columns)
    # ANSI mode is disabled since this test verifies the behaviour of map_zip_with and the evaluation of the associated lambda. 
    # Exceptions during overflow conditions are tested in the arithmetic-ops tests.
    # Not using @disable_ansi_mode because of https://github.com/NVIDIA/spark-rapids/issues/13214.  Using explicit setting instead.
    assert_gpu_and_cpu_are_equal_collect(do_it, conf={'spark.sql.ansi.enabled': False})

@pytest.mark.parametrize('data_gen', [MapGen(IntegerGen(False, min_val=-5, max_val=5), ArrayGen(int_gen, max_length=5), min_length=7)], ids=idfn)
@allow_non_gpu(*non_utc_allow)
def test_map_zip_with_lambda(data_gen):
    def do_it(spark):
        columns = ['a', 'b',
                    'map_zip_with(a, b, (k, arr, v2) -> exists(arr, v1 -> v1 = v2)) as lambdatest',]
        b_gen = MapGen(IntegerGen(False), IntegerGen())
        df = two_col_df(spark, data_gen, b_gen)
        return df.selectExpr(columns)
    assert_gpu_and_cpu_are_equal_collect(do_it)

@pytest.mark.parametrize('data_gen', [MapGen(IntegerGen(False, min_val=-15, max_val=-5), IntegerGen(), min_length=7)], ids=idfn)
@allow_non_gpu(*non_utc_allow)
def test_map_zip_with_bind(data_gen):
    def do_it(spark):
        columns = ['a', 'b', 'c',
                    'map_zip_with(a, b, (k, v1, v2) -> (v1 | v2) & c) as bindtest',]
        df = three_col_df(spark, data_gen, data_gen, int_gen)
        return df.selectExpr(columns)
    assert_gpu_and_cpu_are_equal_collect(do_it)

@pytest.mark.parametrize('data_gen', [MapGen(IntegerGen(False, min_val=5, max_val=15), IntegerGen(), min_length=7)], ids=idfn)
@allow_non_gpu(*non_utc_allow)
def test_map_zip_with_mismatch_keys(data_gen):
    def do_it(spark):
        columns = ['a', 'b',
                    'map_zip_with(a, b, (k, v1, v2) -> v1 + v2) as mismatch',]
        b_gen = MapGen(ByteGen(False), ByteGen())
        df = two_col_df(spark, data_gen, b_gen)
        return df.selectExpr(columns)
    # ANSI mode is disabled since this test verifies the behaviour of map_zip_with and the evaluation of the associated lambda. 
    # Exceptions during overflow conditions are tested in the arithmetic-ops tests.
    # Not using @disable_ansi_mode because of https://github.com/NVIDIA/spark-rapids/issues/13214.  Using explicit setting instead.
    assert_gpu_and_cpu_are_equal_collect(do_it, conf={'spark.sql.ansi.enabled': False})

@pytest.mark.parametrize('data_gen', map_gens_sample + maps_with_binary_value, ids=idfn)
@allow_non_gpu(*non_utc_allow)
def test_map_filter(data_gen):
    columns = ['map_filter(a, (key, value) -> isnotnull(value) )',
               'map_filter(a, (key, value) -> isnull(value) )',
               'map_filter(a, (key, value) -> isnull(key) or isnotnull(value) )',
               'map_filter(a, (key, value) -> isnotnull(key) and isnull(value) )']
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: unary_op_df(spark, data_gen).selectExpr(columns))

@pytest.mark.skipif(is_before_spark_330(), reason="try_element_at is not supported before Spark 3.3.0")
@pytest.mark.parametrize('data_gen', numeric_key_map_gens, ids=idfn)
def test_try_element_at_map_numeric_keys(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: unary_op_df(spark, data_gen).selectExpr(
            'try_element_at(a, 0)',
            'try_element_at(a, 1)',
            'try_element_at(a, null)',
            'try_element_at(a, -9)',
            'try_element_at(a, 999)'))


@pytest.mark.skipif(is_before_spark_330(), reason="try_element_at is not supported before Spark 3.3.0")
@pytest.mark.parametrize('data_gen', [simple_string_to_string_map_gen], ids=idfn)
def test_try_element_at_map_missing_keys(data_gen):
    missing_keys = StringGen(pattern='MISSING_KEY')

    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: two_col_df(spark, data_gen, missing_keys).selectExpr(
            'try_element_at(a, b)'),
        conf=ansi_disabled_conf)

    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: two_col_df(spark, data_gen, missing_keys).selectExpr(
            'try_element_at(a, b)'),
        conf=ansi_enabled_conf)
