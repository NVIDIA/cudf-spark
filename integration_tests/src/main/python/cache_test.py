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

from asserts import assert_gpu_and_cpu_are_equal_collect, assert_equal
from conftest import is_not_utc
from data_gen import *
from pyspark import StorageLevel
import pyspark.sql.functions as f
from spark_session import with_cpu_session, with_gpu_session, is_spark_350_or_351
from join_test import create_df
from marks import incompat, allow_non_gpu, allow_non_gpu_conditional, ignore_order, disable_ansi_mode
import pyspark.mllib.linalg as mllib
import pyspark.ml.linalg as ml

enable_vectorized_confs = [{"spark.sql.inMemoryColumnarStorage.enableVectorizedReader": "true"},
                           {"spark.sql.inMemoryColumnarStorage.enableVectorizedReader": "false"}]

# Many tests sort the results, so use a sortable decimal generator as many Spark versions
# fail to sort some large decimals properly.
_cache_decimal_gens = [decimal_gen_32bit, decimal_gen_64bit, orderable_decimal_gen_128bit]
_cache_single_array_gens_no_null = [ArrayGen(gen) for gen in all_basic_gens_no_null + _cache_decimal_gens]

decimal_struct_gen= StructGen([['child0', sub_gen] for ind, sub_gen in enumerate(_cache_decimal_gens)])

@pytest.mark.parametrize('enable_vectorized_conf', enable_vectorized_confs, ids=idfn)
@allow_non_gpu('CollectLimitExec')
def test_passing_gpuExpr_as_Expr(enable_vectorized_conf):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : unary_op_df(spark, string_gen)
            .select(f.col("a")).na.drop()
            .groupBy(f.col("a"))
            .agg(f.count(f.col("a")).alias("count_a"))
            .orderBy(f.col("count_a").desc(), f.col("a"))
            .cache()
            .limit(50), enable_vectorized_conf)

# creating special cases to just remove -0.0 because of https://github.com/NVIDIA/spark-rapids/issues/84
# After 3.1.0 is the min Spark version we can drop this
double_special_cases = [
    DoubleGen.make_from(1, DOUBLE_MAX_EXP, DOUBLE_MAX_FRACTION),
    DoubleGen.make_from(0, DOUBLE_MAX_EXP, DOUBLE_MAX_FRACTION),
    DoubleGen.make_from(1, DOUBLE_MIN_EXP, DOUBLE_MAX_FRACTION),
    DoubleGen.make_from(0, DOUBLE_MIN_EXP, DOUBLE_MAX_FRACTION),
    0.0, 1.0, -1.0, float('inf'), float('-inf'), float('nan'),
    NEG_DOUBLE_NAN_MAX_VALUE
]

all_gen = [StringGen(), ByteGen(), ShortGen(), IntegerGen(), LongGen(),
           pytest.param(FloatGen(special_cases=[FLOAT_MIN, FLOAT_MAX, 0.0, 1.0, -1.0]), marks=[incompat]),
           pytest.param(DoubleGen(special_cases=double_special_cases), marks=[incompat]),
           BooleanGen(), DateGen(), TimestampGen()] + _cache_decimal_gens

@pytest.mark.parametrize('data_gen', all_gen, ids=idfn)
@pytest.mark.parametrize('enable_vectorized_conf', enable_vectorized_confs, ids=idfn)
@ignore_order
def test_cache_join(data_gen, enable_vectorized_conf):
    # Disable AQE temporarily until https://github.com/NVIDIA/spark-rapids/issues/14319 is resolved.
    conf=copy_and_update(enable_vectorized_conf, {"spark.sql.adaptive.enabled": "false"})
    def do_join(spark):
        left, right = create_df(spark, data_gen, 500, 500)
        cached = left.join(right, left.a == right.r_a, 'Inner').cache()
        cached.count() # populates cache
        return cached
    assert_gpu_and_cpu_are_equal_collect(do_join, conf=conf)

@pytest.mark.parametrize('data_gen', all_gen, ids=idfn)
@pytest.mark.parametrize('enable_vectorized_conf', enable_vectorized_confs, ids=idfn)
# We are OK running everything on CPU until we complete 'https://github.com/NVIDIA/spark-rapids/issues/360'
# because we have an explicit check in our code that disallows InMemoryTableScan to have anything other than
# AttributeReference
@allow_non_gpu(any=True)
@ignore_order
def test_cached_join_filter(data_gen, enable_vectorized_conf):
    data = data_gen
    def do_join(spark):
        left, right = create_df(spark, data, 500, 500)
        cached = left.join(right, left.a == right.r_a, 'Inner').cache()
        cached.count() #populates the cache
        return cached.filter("a is not null")
    assert_gpu_and_cpu_are_equal_collect(do_join, conf=enable_vectorized_conf)

@pytest.mark.parametrize('data_gen', all_gen, ids=idfn)
@pytest.mark.parametrize('enable_vectorized_conf', enable_vectorized_confs, ids=idfn)
@ignore_order
def test_cache_expand_exec(data_gen, enable_vectorized_conf):
    def op_df(spark, length=2048):
        cached = gen_df(spark, StructGen([
            ('a', data_gen),
            ('b', IntegerGen())], nullable=False), length=length).cache()
        cached.count() # populate the cache
        return cached.rollup(f.col("a"), f.col("b")).agg(f.col("b"))

    assert_gpu_and_cpu_are_equal_collect(op_df, conf=enable_vectorized_conf)

@pytest.mark.parametrize('data_gen', [all_basic_struct_gen, StructGen([['child0', StructGen([['child1', byte_gen]])]]),
                                      ArrayGen(
                                          StructGen([['child0', StringGen()],
                                                     ['child1',
                                                      StructGen([['child0', IntegerGen()]])]])),
                                      decimal_struct_gen] + _cache_single_array_gens_no_null + all_gen, ids=idfn)
@pytest.mark.parametrize('enable_vectorized_conf', enable_vectorized_confs, ids=idfn)
@allow_non_gpu('CollectLimitExec')
def test_cache_partial_load(data_gen, enable_vectorized_conf):
    def partial_return(col):
        def partial_return_cache(spark):
            return two_col_df(spark, data_gen, string_gen).select(f.col("a"), f.col("b")).cache().limit(50).select(col)
        return partial_return_cache
    assert_gpu_and_cpu_are_equal_collect(partial_return(f.col("a")), conf=enable_vectorized_conf)
    assert_gpu_and_cpu_are_equal_collect(partial_return(f.col("b")), conf=enable_vectorized_conf)

@pytest.mark.parametrize('enable_vectorized_conf', enable_vectorized_confs, ids=idfn)
@allow_non_gpu('CollectLimitExec')
@ignore_order
def test_cache_reverse_order(enable_vectorized_conf):
    # Disable AQE temporarily until https://github.com/NVIDIA/spark-rapids/issues/14319 is resolved.
    conf=copy_and_update(enable_vectorized_conf, {"spark.sql.adaptive.enabled": "false"})
    col0 = StructGen([['child0', StructGen([['child1', byte_gen]])]])
    col1 = StructGen([['child0', byte_gen]])
    def partial_return():
        def partial_return_cache(spark):
            return two_col_df(spark, col0, col1).select(f.col("a"), f.col("b")).cache().select(f.col("b"), f.col("a"))
        return partial_return_cache
    assert_gpu_and_cpu_are_equal_collect(partial_return(), conf=conf)

@allow_non_gpu('CollectLimitExec')
def test_cache_diff_req_order(spark_tmp_path):
    def n_fold(spark):
        data_path_cpu = spark_tmp_path + '/PARQUET_DATA/{}/{}'
        data = spark.range(100).selectExpr(
            "cast(id as double) as col0",
            "cast(id - 100 as double) as col1",
            "cast(id * 2 as double) as col2",
            "rand(100) as col3",
            "rand(200) as col4")

        num_buckets = 10
        with_random = data.selectExpr("*", "cast(rand(0) * {} as int) as BUCKET".format(num_buckets)).cache()
        for test_bucket in range(0, num_buckets):
            with_random.filter(with_random.BUCKET == test_bucket).drop("BUCKET") \
                .write.parquet(data_path_cpu.format("test_data", test_bucket))
            with_random.filter(with_random.BUCKET != test_bucket).drop("BUCKET") \
                .write.parquet(data_path_cpu.format("train_data", test_bucket))

    with_cpu_session(n_fold)

# allow non gpu when time zone is non-UTC because of https://github.com/NVIDIA/spark-rapids/issues/9653'
non_utc_orc_save_table_allow = ['DataWritingCommandExec', 'WriteFilesExec'] if is_not_utc() else []

# This test doesn't allow negative scale for Decimals as ` df.write.mode('overwrite').parquet(data_path)`
# writes parquet which doesn't allow negative decimals
@pytest.mark.parametrize('data_gen', [StringGen(), ByteGen(), ShortGen(), IntegerGen(), LongGen(),
                                      ArrayGen(
                                          StructGen([['child0', StringGen()],
                                                     ['child1',
                                                      StructGen([['child0', IntegerGen()]])]])),
                                     pytest.param(FloatGen(special_cases=[FLOAT_MIN, FLOAT_MAX, 0.0, 1.0, -1.0]), marks=[incompat]),
                                     pytest.param(DoubleGen(special_cases=double_special_cases), marks=[incompat]),
                                     BooleanGen(), DateGen(), TimestampGen(), decimal_gen_32bit, decimal_gen_64bit,
                                     orderable_decimal_gen_128bit] + _cache_single_array_gens_no_null, ids=idfn)
@pytest.mark.parametrize('ts_write', ['TIMESTAMP_MICROS', 'TIMESTAMP_MILLIS'])
@pytest.mark.parametrize('enable_vectorized', ['true', 'false'], ids=idfn)
@ignore_order
@allow_non_gpu("SortExec", "ShuffleExchangeExec", "RangePartitioning", *non_utc_orc_save_table_allow)
def test_cache_columnar(spark_tmp_path, data_gen, enable_vectorized, ts_write):
    data_path_gpu = spark_tmp_path + '/PARQUET_DATA'
    def read_parquet_cached(data_path):
        def write_read_parquet_cached(spark):
            df = unary_op_df(spark, data_gen)
            df.write.mode('overwrite').parquet(data_path)
            cached = spark.read.parquet(data_path).cache()
            cached.count()
            return cached.select(f.col("a"))
        return write_read_parquet_cached
    # rapids-spark doesn't support LEGACY read for parquet
    conf={'spark.sql.legacy.parquet.datetimeRebaseModeInWrite': 'CORRECTED',
          'spark.sql.legacy.parquet.datetimeRebaseModeInRead' : 'CORRECTED',
          # set the int96 rebase mode values because its LEGACY in databricks which will preclude this op from running on GPU
          'spark.sql.legacy.parquet.int96RebaseModeInWrite' : 'CORRECTED',
          'spark.sql.legacy.parquet.int96RebaseModeInRead' : 'CORRECTED',
          'spark.sql.inMemoryColumnarStorage.enableVectorizedReader' : enable_vectorized,
          'spark.sql.parquet.outputTimestampType': ts_write}

    assert_gpu_and_cpu_are_equal_collect(read_parquet_cached(data_path_gpu), conf)

@pytest.mark.parametrize('data_gen', [all_basic_struct_gen, StructGen([['child0', StructGen([['child1', byte_gen]])]]),
                                      decimal_struct_gen,
                                      ArrayGen(
                                          StructGen([['child0', StringGen()],
                                                     ['child1',
                                                      StructGen([['child0', IntegerGen()]])]]))] + _cache_single_array_gens_no_null + all_gen, ids=idfn)
@pytest.mark.parametrize('enable_vectorized_conf', enable_vectorized_confs, ids=idfn)
def test_cache_cpu_gpu_mixed(data_gen, enable_vectorized_conf):
    def func(spark):
        df = unary_op_df(spark, data_gen)
        df.cache().count()
        enabled = spark.conf.get("spark.rapids.sql.enabled")
        spark.conf.set("spark.rapids.sql.enabled", not enabled)

        return df.selectExpr("a")

    assert_gpu_and_cpu_are_equal_collect(func, conf=enable_vectorized_conf)

@pytest.mark.parametrize('enable_vectorized', ['false', 'true'], ids=idfn)
@pytest.mark.parametrize('with_x_session', [with_gpu_session, with_cpu_session])
# make_interval materializes CalendarIntervalType before this test extracts its fields.
# ProjectExec does not support CalendarIntervalType outputs, and this test intentionally
# allows that operator fallback rather than adding CalendarIntervalType output support.
@allow_non_gpu("ProjectExec", "Alias", "DateAddInterval", "MakeInterval", "Cast",
               "ExtractIntervalYears", "Year", "Month", "Second", "ExtractIntervalMonths",
               "ExtractIntervalSeconds", "SecondWithFraction", "ColumnarToRowExec")
@pytest.mark.parametrize('select_expr', [("NULL as d", "d"),
                                        # In order to compare the results, since pyspark doesn't
                                        # know how to parse interval types, we need to "extract"
                                        # values from the interval. NOTE, "extract" is a misnomer
                                        # because we are actually coverting the value to the
                                        # requested time precision, which is not actually extraction
                                        # i.e. 'extract(years from d) will actually convert the
                                        # entire interval to year
                                        ("make_interval(y,m,w,d,h,min,s) as d", ["cast(extract(years from d) as long)", "extract(months from d)", "extract(seconds from d)"])])
# https://github.com/NVIDIA/spark-rapids/issues/12700
@disable_ansi_mode
def test_cache_additional_types(enable_vectorized, with_x_session, select_expr):
    def with_cache(cache):
        select_expr_df, select_expr_project = select_expr

        def helper(spark):
            # the goal is to just get a DF of CalendarIntervalType, therefore limiting the values
            # so when we do get the individual parts of the interval, it doesn't overflow
            df = gen_df(spark, StructGen([('m', IntegerGen(min_val=-1000, max_val=1000, nullable=False)),
                                          ('y', IntegerGen(min_val=-10000, max_val=10000, nullable=False)),
                                          ('w', IntegerGen(min_val=-10000, max_val=10000, nullable=False)),
                                          ('h', IntegerGen(min_val=-10000, max_val=10000, nullable=False)),
                                          ('min', IntegerGen(min_val=-10000, max_val=10000, nullable=False)),
                                          ('d', IntegerGen(min_val=-10000, max_val=10000, nullable=False)),
                                          ('s', IntegerGen(min_val=-10000, max_val=10000, nullable=False))],
                                         nullable=False), seed=1)
            duration_df = df.selectExpr(select_expr_df)
            if (cache):
                duration_df.cache()
            duration_df.count
            df_1 = duration_df.selectExpr(select_expr_project)
            return df_1.collect()

        return helper

    cached_result = with_x_session(with_cache(True),
                                   conf={'spark.sql.legacy.parquet.datetimeRebaseModeInWrite': 'CORRECTED',
                                         'spark.sql.legacy.parquet.datetimeRebaseModeInRead': 'CORRECTED',
                                         'spark.sql.inMemoryColumnarStorage.enableVectorizedReader': enable_vectorized})
    reg_result = with_x_session(with_cache(False),
                                conf={'spark.sql.legacy.parquet.datetimeRebaseModeInWrite': 'CORRECTED',
                                      'spark.sql.legacy.parquet.datetimeRebaseModeInRead': 'CORRECTED',
                                      'spark.sql.inMemoryColumnarStorage.enableVectorizedReader': enable_vectorized})

    # NOTE: we aren't comparing cpu and gpu results, we are comparing the cached and non-cached results.
    assert_equal(reg_result, cached_result)

def generate_data_and_test_func_on_cached_df(with_x_session, func, data_gen, test_conf):
    df = lambda spark: unary_op_df(spark, data_gen)
    function_to_test_on_df(with_x_session, df, func, test_conf)

def function_to_test_on_df(with_x_session, df_gen, func_on_df, test_conf):
    def with_cache(cached):
        def helper(spark):
            _df = df_gen(spark)
            if cached:
                _df.cache().count()
            return func_on_df(_df)
        return helper

    reg_result = with_x_session(with_cache(False), test_conf)
    cached_result = with_x_session(with_cache(True), test_conf)

    assert_equal(reg_result, cached_result)

@pytest.mark.parametrize('data_gen', all_gen, ids=idfn)
@pytest.mark.parametrize('with_x_session', [with_gpu_session, with_cpu_session])
@pytest.mark.parametrize('enable_vectorized_conf', enable_vectorized_confs, ids=idfn)
@pytest.mark.parametrize('batch_size', [{"spark.rapids.sql.batchSizeBytes": "100"}, {}], ids=idfn)
@ignore_order
def test_cache_count(data_gen, with_x_session, enable_vectorized_conf, batch_size):
    test_conf = copy_and_update(enable_vectorized_conf, batch_size)
    generate_data_and_test_func_on_cached_df(with_x_session, lambda df: df.count(), data_gen, test_conf)

@pytest.mark.parametrize('data_gen', all_gen, ids=idfn)
@pytest.mark.parametrize('with_x_session', [with_cpu_session, with_gpu_session])
@pytest.mark.parametrize('enable_vectorized_conf', enable_vectorized_confs, ids=idfn)
@pytest.mark.parametrize('batch_size', [{"spark.rapids.sql.batchSizeBytes": "100"}, {}], ids=idfn)
@ignore_order
# This tests the cached and uncached values returned by collect on the CPU and GPU.
# When running on the GPU with the DefaultCachedBatchSerializer, to project the results Spark adds a ColumnarToRowExec
# to be able to show the results which will cause this test to throw an exception as it's not on the GPU so we have to
# add that case to the `allowed` list. As of now there is no way for us to limit the scope of allow_non_gpu based on a
# condition therefore we must allow it in all cases
@allow_non_gpu('ColumnarToRowExec')
def test_cache_multi_batch(data_gen, with_x_session, enable_vectorized_conf, batch_size):
    test_conf = copy_and_update(enable_vectorized_conf, batch_size)
    generate_data_and_test_func_on_cached_df(with_x_session, lambda df: df.collect(), data_gen, test_conf)

@pytest.mark.parametrize('data_gen', all_basic_map_gens + _cache_single_array_gens_no_null, ids=idfn)
@pytest.mark.parametrize('enable_vectorized', enable_vectorized_confs, ids=idfn)
def test_cache_map_and_array(data_gen, enable_vectorized):
    def helper(spark):
        df = gen_df(spark, StructGen([['a', data_gen]], nullable=False))
        df.persist()
        return df.selectExpr("a")

    assert_gpu_and_cpu_are_equal_collect(helper)

def test_cache_udt():
    def fun(spark):
        df = spark.sparkContext.parallelize([
            (mllib.DenseVector([1, ]), ml.DenseVector([1, ])),
            (mllib.SparseVector(1, [0, ], [1, ]), ml.SparseVector(1, [0, ], [1, ]))
        ]).toDF(["mllib_v", "ml_v"])
        df.cache().count()
        return df.selectExpr("mllib_v", "ml_v").collect()
    cpu_result = with_cpu_session(fun)
    gpu_result = with_gpu_session(fun)
    # assert_gpu_and_cpu_are_equal_collect method doesn't handle UDT so we just write a single
    # statement here to compare
    assert cpu_result == gpu_result, "not equal"

@pytest.mark.parametrize('enable_vectorized_conf', enable_vectorized_confs, ids=idfn)
@ignore_order(local=True)
def test_cache_daytimeinterval(enable_vectorized_conf):
    def test_func(spark):
        df = two_col_df(spark, DayTimeIntervalGen(), int_gen)
        df.cache().count()
        return df.selectExpr("b", "a")
    assert_gpu_and_cpu_are_equal_collect(test_func, enable_vectorized_conf)

# For AQE, test the computeStats(...) implementation in GpuInMemoryTableScanExec
# NOTE: this test is here because the necessary cache configuration is only 
# available when this test file is used
@ignore_order(local=True)
@allow_non_gpu("ShuffleExchangeExec", "ColumnarToRowExec")
@pytest.mark.parametrize("data_gen", integral_gens, ids=idfn)
def test_aqe_cache_join(data_gen):
    conf = {'spark.sql.adaptive.enabled': 'true'}
    def do_it(spark):
        df1 = unary_op_df(spark, data_gen).orderBy('a').cache()
        df2 = df1.alias('df2')
        return df1.join(df2, df1.a == df2.a, 'Outer')
    assert_gpu_and_cpu_are_equal_collect(do_it, conf=conf)

# TODO: remove this test after timezone support is added.
# This test is testing cache when InMemoryTableScanExec fallsback to the CPU which causes the HostColumnarToGPU
# to be pushed to the query plan which can cause ArrayIndexOutOfBoundsException
@ignore_order
@allow_non_gpu("InMemoryTableScanExec", "ProjectExec", "ColumnarToRowExec")
def test_inmem_cache_count():
    conf={"spark.sql.session.timeZone": "America/Los_Angeles"}
    function_to_test_on_df(with_gpu_session, lambda spark: unary_op_df(spark, int_gen).selectExpr("cast(a as timestamp)"), lambda df: df.count(), test_conf=conf)

@pytest.mark.parametrize('with_x_session', [with_gpu_session, with_cpu_session])
def test_batch_no_cols(with_x_session):
    function_to_test_on_df(with_x_session, lambda spark: unary_op_df(spark, int_gen).drop("a"), lambda df: df.count(), test_conf={})

@ignore_order(local=True)
@allow_non_gpu("ShuffleExchangeExec", "ColumnarToRowExec")
@allow_non_gpu_conditional(is_spark_350_or_351(), "InMemoryTableScanExec")
@pytest.mark.parametrize("data_gen", integral_gens, ids=idfn)
@pytest.mark.parametrize('enable_vectorized_conf', enable_vectorized_confs, ids=idfn)
def test_aqe_cache_version_specific_behavior(data_gen, enable_vectorized_conf):
    """
    Test InMemoryTableScan + AQE behavior across Spark versions.
    - Spark 3.2.0-3.4.x: InMemoryTableScan works on GPU
    - Spark 3.5.0-3.5.1: InMemoryTableScan disabled by default due to missing InMemoryTableScanLike trait
    - Spark 3.5.2+: InMemoryTableScan works on GPU with proper trait support
    """

    def do_it(spark):
        df1 = unary_op_df(spark, data_gen).orderBy('a').cache()
        df2 = unary_op_df(spark, data_gen).withColumnRenamed("a", "r_a").cache()
        df1.count()
        df2.count()
        return df1.join(df2, df1.a == df2.r_a, 'Outer')

    assert_gpu_and_cpu_are_equal_collect(do_it, conf=enable_vectorized_conf)

@ignore_order(local=True)
@allow_non_gpu("CollectLimitExec", "ShuffleExchangeExec", "ColumnarToRowExec")
@pytest.mark.parametrize('enable_vectorized_conf', enable_vectorized_confs, ids=idfn)
@allow_non_gpu_conditional(is_spark_350_or_351(), "InMemoryTableScanExec")
def test_persist_with_groupby_join_version_specific(enable_vectorized_conf):
    """
    Expected behavior:
    - Spark 3.2.0-3.4.x: InMemoryTableScan works on GPU
    - Spark 3.5.0-3.5.1: InMemoryTableScan falls back to CPU due to missing InMemoryTableScanLike trait
    - Spark 3.5.2+: InMemoryTableScan works on GPU with proper trait support
    """

    def do_it(spark):
        df = spark.range(0, 1000, 1, 2).select(
            f.col("id").alias("_1"),
            f.col("id").alias("_2")
        )

        ee = df.select(
            f.col("_1").alias("src"),
            f.col("_2").alias("dst")
        ).persist(StorageLevel.MEMORY_AND_DISK)
        ee.count()

        minNbrs1 = ee.groupBy("src").agg(
            f.min(f.col("dst")).alias("min_number")
        ).persist(StorageLevel.MEMORY_AND_DISK)
        minNbrs1.count()

        ee.join(minNbrs1, "src")

        return ee.join(minNbrs1, "src")

    assert_gpu_and_cpu_are_equal_collect(do_it, conf=enable_vectorized_conf)

@ignore_order(local=True)
# Ensure base allow list is a tuple to satisfy pytest hook concatenation
@allow_non_gpu("")
@pytest.mark.parametrize('enable_vectorized_conf', enable_vectorized_confs, ids=idfn)
@allow_non_gpu_conditional(is_spark_350_or_351(), "InMemoryTableScanExec")
def test_cached_groupby_sum_version_specific(enable_vectorized_conf):
    """
    Validate aggregation on a cached query works without errors across Spark versions.
    Expected behavior:
    - Spark 3.5.0-3.5.1: InMemoryTableScan falls back to CPU by default
    - Spark 3.5.2+: InMemoryTableScan works on GPU
    """

    def do_it(spark):
        spark.sql(
            """
            SELECT id, value FROM VALUES (1, 10.0), (2, 20.0) AS t(id, value)
            """).createOrReplaceTempView("t_simple")

        df = spark.sql("""SELECT SUM(value) AS s FROM t_simple GROUP BY id""").cache()

        # Populate the cache
        df.count()
        return df

    assert_gpu_and_cpu_are_equal_collect(do_it, conf=enable_vectorized_conf)
