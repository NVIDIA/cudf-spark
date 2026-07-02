# Copyright (c) 2024-2026, NVIDIA CORPORATION.
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
import re

from asserts import *
from data_gen import *
from marks import *
from parquet_test import rebase_write_corrected_conf
from spark_session import *
import pyspark.sql.functions as f

"""
Hybrid Scan unsupported types:
1. Decimal with negative scale is NOT supported
2. Decimal128 inside nested types is NOT supported
3. BinaryType is NOT supported
4. MapType wrapped by NestedType (Struct of Map/Array of Map/Map of Map) is NOT fully supported
"""
parquet_gens_list = [
    [byte_gen, short_gen, int_gen, long_gen, float_gen, double_gen,
     string_gen, boolean_gen, date_gen,
     TimestampGen(start=datetime(1900, 1, 1, tzinfo=timezone.utc)), ArrayGen(byte_gen),
     ArrayGen(long_gen), ArrayGen(string_gen), ArrayGen(date_gen),
     ArrayGen(TimestampGen(start=datetime(1900, 1, 1, tzinfo=timezone.utc))),
     ArrayGen(decimal_gen_64bit),
     ArrayGen(ArrayGen(byte_gen)),
     StructGen([['child0', ArrayGen(byte_gen)],
                ['child1', byte_gen],
                ['child2', float_gen],
                ['child3', decimal_gen_64bit]]),
     ArrayGen(StructGen([['child0', string_gen],
                         ['child1', double_gen],
                         ['child2', int_gen]]))
     ],
    [MapGen(f(nullable=False), f()) for f in [
        BooleanGen, ByteGen, ShortGen, IntegerGen, LongGen, FloatGen, DoubleGen, DateGen,
        lambda nullable=True: TimestampGen(start=datetime(1900, 1, 1, tzinfo=timezone.utc), nullable=nullable)]
     ],
    [simple_string_to_string_map_gen,
     MapGen(StringGen(pattern='key_[0-9]', nullable=False), ArrayGen(string_gen), max_length=10),
     MapGen(RepeatSeqGen(IntegerGen(nullable=False), 10), long_gen, max_length=10),
     ],
    [decimal_gen_32bit, decimal_gen_64bit],
]

parquet_gens_fallback_lists = [
    # Decimal128 is NOT supported
    [decimal_gen_128bit],
    # Decimal with negative scale is NOT supported
    pytest.param([DecimalGen(precision=10, scale=-3)],
                 marks=pytest.mark.xfail(
                     reason='GpuParquetScan cannot read decimal with negative scale')),
    # BinaryType is NOT supported
    [BinaryGen()],
    # MapType wrapped by NestedType is NOT fully supported
    [MapGen(StringGen(pattern='key_[0-9]', nullable=False), simple_string_to_string_map_gen)],
    [ArrayGen(simple_string_to_string_map_gen)],
    [ArrayGen(ArrayGen(simple_string_to_string_map_gen))],
    [ArrayGen(StructGen([["c0", simple_string_to_string_map_gen]]))],
    [StructGen([["c0", simple_string_to_string_map_gen]])],
    [StructGen([["c0", ArrayGen(simple_string_to_string_map_gen)]])],
    [StructGen([["c0", StructGen([["cc0", simple_string_to_string_map_gen]])]])],
    # empty schema is NOT supported (select count(1))
    [],
]


@pytest.mark.skipif(is_databricks_runtime(), reason="Hybrid feature does not support Databricks currently")
@pytest.mark.skipif(not is_hybrid_backend_loaded(), reason="HybridScan specialized tests")
@pytest.mark.parametrize('parquet_gens', parquet_gens_list, ids=idfn)
@pytest.mark.parametrize('gen_rows', [20, 100, 512, 1024, 4096], ids=idfn)
@hybrid_test
def test_hybrid_parquet_read_round_trip(spark_tmp_path, parquet_gens, gen_rows):
    gen_list = [('_c' + str(i), gen) for i, gen in enumerate(parquet_gens)]
    data_path = spark_tmp_path + '/PARQUET_DATA'
    with_cpu_session(
        lambda spark: gen_df(spark, gen_list, length=gen_rows).write.parquet(data_path),
        conf=rebase_write_corrected_conf)

    assert_cpu_and_gpu_are_equal_collect_with_capture(
        lambda spark: spark.read.parquet(data_path),
        exist_classes='HybridFileSourceScanExec',
        non_exist_classes='GpuFileSourceScanExec',
        conf={
            'spark.sql.sources.useV1SourceList': 'parquet',
            'spark.rapids.sql.hybrid.parquet.enabled': 'true',
        })


# Creating scenarios in which CoalesceConverter will coalesce several input batches by adjusting
# reader_batch_size and coalesced_batch_size, tests if the CoalesceConverter functions correctly
# when coalescing is needed.
@pytest.mark.skipif(is_databricks_runtime(), reason="Hybrid feature does not support Databricks currently")
@pytest.mark.skipif(not is_hybrid_backend_loaded(), reason="HybridScan specialized tests")
@pytest.mark.parametrize('parquet_gens', parquet_gens_list, ids=idfn)
@pytest.mark.parametrize('reader_batch_size', [512, 1024, 2048], ids=idfn)
@pytest.mark.parametrize('coalesced_batch_size', [1 << 25, 1 << 27], ids=idfn)
@pytest.mark.parametrize('gen_rows', [8192, 10000], ids=idfn)
@hybrid_test
def test_hybrid_parquet_read_round_trip_multiple_batches(spark_tmp_path,
                                                         parquet_gens,
                                                         reader_batch_size,
                                                         coalesced_batch_size,
                                                         gen_rows):
    gen_list = [('_c' + str(i), gen) for i, gen in enumerate(parquet_gens)]
    data_path = spark_tmp_path + '/PARQUET_DATA'
    with_cpu_session(
        lambda spark: gen_df(spark, gen_list, length=gen_rows).write.parquet(data_path),
        conf=rebase_write_corrected_conf)

    assert_cpu_and_gpu_are_equal_collect_with_capture(
        lambda spark: spark.read.parquet(data_path),
        exist_classes='HybridFileSourceScanExec',
        non_exist_classes='GpuFileSourceScanExec',
        conf={
            'spark.sql.sources.useV1SourceList': 'parquet',
            'spark.rapids.sql.hybrid.parquet.enabled': 'true',
            'spark.gluten.sql.columnar.maxBatchSize': reader_batch_size,
            'spark.rapids.sql.batchSizeBytes': coalesced_batch_size,
        })


# HybridScan shall NOT be enabled over unsupported data types. Instead, fallbacks to GpuScan.
@pytest.mark.skipif(is_databricks_runtime(), reason="Hybrid feature does not support Databricks currently")
@pytest.mark.skipif(not is_hybrid_backend_loaded(), reason="HybridScan specialized tests")
@pytest.mark.parametrize('parquet_gens', parquet_gens_fallback_lists, ids=idfn)
@hybrid_test
def test_hybrid_parquet_read_fallback_to_gpu(spark_tmp_path, parquet_gens):
    data_path = spark_tmp_path + '/PARQUET_DATA'
    # check the fallback over empty schema(`SELECT COUNT(1)`) within the same case
    if len(parquet_gens) == 0:
        with_cpu_session(
            lambda spark: gen_df(spark, [('a', int_gen)], length=512).write.parquet(data_path),
            conf=rebase_write_corrected_conf)
        read_fn = lambda spark: spark.read.parquet(data_path).selectExpr('count(1)')
    else:
        gen_list = [('_c' + str(i), gen) for i, gen in enumerate(parquet_gens)]
        with_cpu_session(
            lambda spark: gen_df(spark, gen_list, length=512).write.parquet(data_path),
            conf=rebase_write_corrected_conf)
        read_fn = lambda spark: spark.read.parquet(data_path)
    assert_cpu_and_gpu_are_equal_collect_with_capture(
        read_fn,
        exist_classes='GpuFileSourceScanExec',
        non_exist_classes='HybridFileSourceScanExec',
        conf={
            'spark.sql.sources.useV1SourceList': 'parquet',
            'spark.rapids.sql.hybrid.parquet.enabled': 'true',
        })

@pytest.mark.skipif(is_databricks_runtime(), reason="Hybrid feature does not support Databricks currently")
@pytest.mark.skipif(not is_hybrid_backend_loaded(), reason="HybridScan specialized tests")
@pytest.mark.skipif(is_before_spark_330(), reason='DayTimeInterval is not supported before Pyspark 3.3.0')
@hybrid_test
def test_hybrid_parquet_read_daytime_interval_fallback_to_gpu(spark_tmp_path):
    data_path = spark_tmp_path + '/PARQUET_DATA'
    with_cpu_session(
        lambda spark: gen_df(spark, [('a', DayTimeIntervalGen())]).write.parquet(data_path),
        conf=rebase_write_corrected_conf)
    assert_cpu_and_gpu_are_equal_collect_with_capture(
        lambda spark: spark.read.parquet(data_path),
        exist_classes='GpuFileSourceScanExec',
        non_exist_classes='HybridFileSourceScanExec',
        conf={
            'spark.sql.sources.useV1SourceList': 'parquet',
            'spark.rapids.sql.hybrid.parquet.enabled': 'true',
        })

# Test the preloading feature with extreme tiny target batch size (and source batch size), creating
# scenarios in which multiple target batches will be generated.
@pytest.mark.skipif(is_databricks_runtime(), reason="Hybrid feature does not support Databricks currently")
@pytest.mark.skipif(not is_hybrid_backend_loaded(), reason="HybridScan specialized tests")
@pytest.mark.parametrize('coalesced_batch_size', [1 << 17, 1 << 20], ids=idfn)
@pytest.mark.parametrize('preloaded_batches', [1, 3, 5], ids=idfn)
@hybrid_test
def test_hybrid_parquet_preloading(spark_tmp_path, coalesced_batch_size, preloaded_batches):
    parquet_gens = parquet_gens_list[0].copy()
    parquet_gens.extend(parquet_gens_list[1])
    gen_list = [('_c' + str(i), gen) for i, gen in enumerate(parquet_gens)]
    data_path = spark_tmp_path + '/PARQUET_DATA'
    with_cpu_session(
        lambda spark: gen_df(spark, gen_list, length=4096).write.parquet(data_path),
        conf=rebase_write_corrected_conf)

    assert_cpu_and_gpu_are_equal_collect_with_capture(
        lambda spark: spark.read.parquet(data_path),
        exist_classes='HybridFileSourceScanExec',
        non_exist_classes='GpuFileSourceScanExec',
        conf={
            'spark.sql.sources.useV1SourceList': 'parquet',
            'spark.rapids.sql.hybrid.parquet.enabled': 'true',
            'spark.gluten.sql.columnar.maxBatchSize': 16,
            'spark.rapids.sql.batchSizeBytes': coalesced_batch_size,
            'spark.rapids.sql.hybrid.parquet.numPreloadedBatches': preloaded_batches,
        })


filter_split_conf = {
    'spark.sql.sources.useV1SourceList': 'parquet',
    'spark.rapids.sql.hybrid.parquet.enabled': 'true',
    'spark.rapids.sql.hybrid.parquet.filterPushDown': 'CPU',
    'spark.rapids.sql.expression.Ascii': False,
    'spark.rapids.sql.expression.StartsWith': False,
    'spark.rapids.sql.hybrid.whitelistExprs': 'StartsWith'
}

def check_filter_pushdown(plan, pushed_exprs, not_pushed_exprs):
    plan = str(plan)
    filter_part, scan_part = plan.split("Scan parquet")
    for expr in pushed_exprs:
        assert expr in scan_part
    for expr in not_pushed_exprs:
        assert expr in filter_part

@pytest.mark.skipif(is_databricks_runtime(), reason="Hybrid feature does not support Databricks currently")
@pytest.mark.skipif(not is_hybrid_backend_loaded(), reason="HybridScan specialized tests")
@hybrid_test
def test_hybrid_parquet_filter_pushdown_gpu(spark_tmp_path):
    data_path = spark_tmp_path + '/PARQUET_DATA'
    def add(a, b):
        return a + b
    my_udf = f.pandas_udf(add, returnType=LongType())
    with_cpu_session(
        lambda spark: gen_df(spark, [('a', long_gen)]).write.parquet(data_path),
        conf=rebase_write_corrected_conf)
    conf = filter_split_conf.copy()
    conf.update({
        'spark.rapids.sql.hybrid.parquet.filterPushDown': 'GPU'
    })
    # filter conditions should remain on the GPU
    plan = with_gpu_session(
        lambda spark: spark.read.parquet(data_path).filter(my_udf(f.col('a'), f.col('a')) > 0)._jdf.queryExecution().executedPlan(),
        conf=conf)
    check_filter_pushdown(plan, pushed_exprs=[], not_pushed_exprs=['pythonUDF'])
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.parquet(data_path).filter(my_udf(f.col('a'), f.col('a')) > 0),
        conf=conf)

@pytest.mark.skipif(is_databricks_runtime(), reason="Hybrid feature does not support Databricks currently")
@pytest.mark.skipif(not is_hybrid_backend_loaded(), reason="HybridScan specialized tests")
@hybrid_test
def test_hybrid_parquet_filter_pushdown_cpu(spark_tmp_path):
    data_path = spark_tmp_path + '/PARQUET_DATA'
    with_cpu_session(
        lambda spark: gen_df(spark, [('a', StringGen(pattern='[0-9]{1,5}'))]).write.parquet(data_path),
        conf=rebase_write_corrected_conf)
    # filter conditions should be pushed down to the CPU, so the ascii will not fall back to CPU in the FilterExec
    # use f.startWith because sql function startswith is from spark 3.5.0
    plan = with_gpu_session(
        lambda spark: spark.read.parquet(data_path).filter(f.col("a").startswith('1') & (f.ascii(f.col("a")) >= 50) & (f.col("a") < '1000'))._jdf.queryExecution().executedPlan(),
        conf=filter_split_conf)
    check_filter_pushdown(plan, pushed_exprs=['ascii', 'StartsWith', 'isnotnull'], not_pushed_exprs=[])
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.parquet(data_path).filter(f.col("a").startswith('1') & (f.ascii(f.col("a")) >= 50) & (f.col("a") < '1000')),
        conf=filter_split_conf)

@allow_non_gpu('FilterExec', 'BatchEvalPythonExec', 'PythonUDF')
@pytest.mark.skipif(is_databricks_runtime(), reason="Hybrid feature does not support Databricks currently")
@pytest.mark.skipif(not is_hybrid_backend_loaded(), reason="HybridScan specialized tests")
@hybrid_test
def test_hybrid_parquet_filter_pushdown_unsupported(spark_tmp_path):
    data_path = spark_tmp_path + '/PARQUET_DATA'
    with_cpu_session(
        lambda spark: gen_df(spark, [('a', StringGen(pattern='[0-9]{1,5}'))]).write.parquet(data_path),
        conf=rebase_write_corrected_conf)
    # UDf is not supported by GPU, so it should fallback to CPU in the FilterExec    
    def udf_fallback(s):
        return f'udf_{s}'
    
    with_cpu_session(lambda spark: spark.udf.register("udf_fallback", udf_fallback))
    plan = with_gpu_session(
        lambda spark: spark.read.parquet(data_path).filter("ascii(a) >= 50 and udf_fallback(a) = 'udf_100'")._jdf.queryExecution().executedPlan(),
        conf=filter_split_conf)
    check_filter_pushdown(plan, pushed_exprs=['ascii', 'isnotnull'], not_pushed_exprs=['udf_fallback'])
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.parquet(data_path).filter("ascii(a) >= 50 and udf_fallback(a) = 'udf_100'"),
        conf=filter_split_conf)

@pytest.mark.skipif(is_databricks_runtime(), reason="Hybrid feature does not support Databricks currently")
@pytest.mark.skipif(not is_hybrid_backend_loaded(), reason="HybridScan specialized tests")
@hybrid_test
@allow_non_gpu(*non_utc_allow)
def test_hybrid_parquet_filter_pushdown_timestamp(spark_tmp_path):
    data_path = spark_tmp_path + '/PARQUET_DATA'
    with_cpu_session(
        lambda spark: gen_df(spark, [('a', TimestampGen(start=datetime(1900, 1, 1, tzinfo=timezone.utc)))]).write.parquet(data_path),
        conf=rebase_write_corrected_conf)

    # Timestamp is not fully supported in Hybrid Filter, so it should remain on the GPU
    plan = with_gpu_session(
        lambda spark: spark.read.parquet(data_path).filter(f.col("a") > f.lit(datetime(2024, 1, 1, tzinfo=timezone.utc)))._jdf.queryExecution().executedPlan(),
        conf=filter_split_conf)
    check_filter_pushdown(plan, pushed_exprs=[], not_pushed_exprs=['isnotnull', '>'])
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.parquet(data_path).filter(f.col("a") > f.lit(datetime(2024, 1, 1, tzinfo=timezone.utc))),
        conf=filter_split_conf)

filter_pushdown_gen_list = [('bool1', BooleanGen()),
           ('bool2', BooleanGen()),
           ('double1', DoubleGen(min_exp=-5, max_exp=5)),
           ('double2', DoubleGen()),
           ('double3', DoubleGen()),
           ('int1', IntegerGen(min_val=-16, max_val=16)),
           ('int2', IntegerGen(min_val=-16, max_val=16)),
           ('int3', IntegerGen(min_val=1, max_val=2)),
           ('int4', IntegerGen()),
           ('int_not_null1', IntegerGen(nullable=False, min_val=-16, max_val=16)),
           ('int_not_null2', IntegerGen(nullable=False, min_val=-16, max_val=16)),
           ('date1', DateGen(start=date(2009, 12, 25), end=date(2010, 1, 5))),
           ('date2', DateGen(start=date(2009, 12, 25), end=date(2010, 1, 5))),
           ('date3', DateGen(start=date(2010, 1, 25), end=date(2010, 2, 5))), 
           ('timestamp', TimestampGen(start=datetime(1900, 1, 1, tzinfo=timezone.utc))),
           ('array_int_1', ArrayGen(IntegerGen(min_val=1, max_val=2), min_length=1, max_length=2)),
           ('array_int_2', ArrayGen(IntegerGen(min_val=1, max_val=2), min_length=1, max_length=2)),
           ('array_array_int_1', ArrayGen(ArrayGen(IntegerGen(min_val=1, max_val=2),max_length=2))),
           ('array_str_1', ArrayGen(StringGen(pattern='[a-z]{1,3}'))),
           ('str1', StringGen(pattern='[a-dA-D ]{1,3}')),
           ('str2', StringGen(pattern='[a-dA-D ]{1,3}')),
           ('long', LongGen()),
           ('comma_separated_str', StringGen(pattern='([0-9],){1,5}')),
           ('json_str', StringGen(pattern=r'\{"a": "[a-c]{1,2}"\}')),
]

condition_list = [
    # Boolean:
    # Not
    "not bool1",
    # Boolean, Boolean:
    # And, Or
    "(bool1 and bool2)",
    "(bool1 or bool2)",
    # Double:
    # Acos, Asin, Atan, Atan2, Atanh, Ceil, Cos, Cosh, IsNaN, Log, Log10, Log2, Rint, Exp, Expm1, Floor
    "(acos(double1) > 0.5)", 
    "(asin(double1) > 0.2)", 
    "(atan(double1) > 0.3)", 
    "(atan2(double1, double2) > 0.4)", 
    "(atanh(double1) < 0.6)",
    "(ceil(double1) == 3)", 
    "(cos(double1) < 0.7)", 
    "(cosh(double1) > 0.9)",
    "(isnan(double1)) ",
    "(log(double1) > 1.0)", 
    "(log10(double1) < 1.2)", 
    "(log2(double1) > 1.4)", 
    "(rint(double1) == 2)",
    "(exp(double1) < 1.0)",
    "(expm1(double1) < 1.0)",
    "(floor(double1) == 1)",
    # Double, Int:
    # Round,ArrayRepeat
    "(round(double1) == 3)",
    # Double, Double:
    # NaNvl,Pow
    "(nanvl(double1, 1) == 1)",
    "(pow(double1, double2) < 1.0)",
    # Double, Long:
    # Round
    "(round(double1, 1) == 3.1)",
    # Date:
    # LastDay, WeekOfYear,DayOfMonth, DayOfWeek, DayOfYear, Month, Quarter, Second, UnixMicros, UnixMillis, UnixSeconds, WeekDay, Year
    "(last_day(date1) == date1)",
    "(weekofyear(date1) == 1)",
    "(dayofmonth(date1) == 1)",
    "(dayofweek(date1) == 1)",
    "(dayofyear(date1) == 1)",
    "(month(date1) == 1)",
    # Date, Int
    # AddMonths
    "(add_months(date1, 1) == date3)",
    # Date, Date:
    # DateDiff
    pytest.param("(date_diff(date1, date2) == 1)", marks=pytest.mark.skipif(is_before_spark_340(), reason='date_diff is not supported before Pyspark 3.4.0')),
    # unit, Int, Date:
    # DateAdd,DateSub
    "(date_add(date1, 1) == date2)",
    "(date_sub(date1, 1) == date2)",
    # Array[Int], lambda:
    # ArrayExists,ArrayForAll
    "(exists(array_int_1, x -> x == 1))",
    "(forall(array_int_1, x -> x == 1))",
    # Array[Int], Int:
    # ArrayContains,ArrayPosition,ArrayRemove,ElementAt
    "(array_contains(array_int_1, 1))",
    "(array_position(array_int_1, 1) == 1)",
    "(array_remove(array_int_1, 1) == array_int_1)",
    "(element_at(array_int_1, 1) == 1)",
    # Array[Int]:
    # ArrayDistinct,ArrayMax,ArrayMin,ArraySort,Size
    "(array_distinct(array_int_1) == array_int_1)",
    "(array_max(array_int_1) == 1)",
    "(array_min(array_int_1) == 1)",
    "(sort_array(array_int_1) == array_int_1)",
    "(size(array_int_1) == 1)",
    # Array[Int], Array[Int]:
    # ArrayExcept,ArraysZip
    "(array_except(array_int_1, array_int_2) == array_int_1)",
    "(arrays_zip(array_int_1, array_int_2) == " + 
    "array(struct(1 as array_int_1, 1 as array_int_2), struct(2 as array_int_1, 2 as array_int_2)))",
    # Array[Array[Int]]:
    # Flatten
    "(flatten(array_array_int_1) == array(1,2,1,2))",
    # Array[Int], Array[Int], lambda:
    # ZipWith
    "(zip_with(array_int_1, array_int_2, (x, y) -> x + y) == array(2,4))",
    # String:
    # Ascii,BitLength,Hex,Length,Lower,Reverse,SoundEx,StringTrim,StringTrimLeft,StringTrimRight,Upper
    "(ascii(str1) == 97)",
    "(bit_length(str1) == 16)",
    "(hex(str1) == '616263')", # abc -> 616263
    "(length(str1) == 2)",
    "(lower(str1) == 'abc')",
    "(reverse(str1) == 'cba')",
    "(soundex(str1) == 'A120')", # abc -> A120
    "(trim(str1) == 'abc')",
    "(ltrim(str1) == 'abc')",
    "(rtrim(str1) == 'abc')",
    "(upper(str1) == 'ABC')",
    # Int, Int:
    # ArrayRepeat,BitwiseAnd,BitwiseOr,BitwiseXor,EqualNullSafe,EqualTo,GreaterThan,GreaterThanOrEqual,
    # Greatest,Least,LessThan,LessThanOrEqual,Remainder,ShiftLeft,ShiftRight
    "(array_repeat(int3, 2) == array_int_1)",
    "(int1 & int2 == 0)",
    "(int1 | int2 == 16)",
    "(int1 ^ int2 == 16)",
    "(int1 <=> int2)",
    "(int1 == int2)",
    "(int1 > int2)",
    "(int1 >= int2)",
    "(greatest(int1, int2) > 15)",
    "(least(int1, int2) < -10)",
    "(int1 < int2)",
    "(int1 <= int2)",
    "(int1 % int2 == 1)",
    "(shiftleft(int1, 1) >= 8)",
    "(shiftright(int1, 1) < 8)",
    # Int:
    # DateFromUnixDate,IsNotNull,IsNull,UnaryPositive
    "(date_from_unix_date(int1) == '1970-01-02')",
    "(isnotnull(int1))",
    "(isnull(int1))",
    "(positive(int1) == 1)",
    # String, String:
    # Concat,Levenshtein,StringInstr
    "(concat(str1, str1) == 'aaaa')",
    pytest.param("(levenshtein(str1, str2) == 4)", marks=pytest.mark.xfail(reason='not supported by gluten')),
    "(instr(str1, 'b') == 1)",
    # String, String, String:
    # StringReplace
    "(replace(str1, 'c', 'a') == 'aba')",
    # String, Int:
    # Left,StringLPad,StringRPad,Substring
    "(left(str1, 2) == 'ab')",
    "(lpad(str1, 3, 'a') == 'aab')",
    "(rpad(str1, 3, 'a') == 'aba')",
    "(substring(str1, 1, 2) == 'ab')",
    # Long:
    # Chr
    "(chr(int1 + 95) == 'd')",
    # No args:
    # MonotonicallyIncreasingID,Pi,SparkPartitionID
    "(monotonically_increasing_id() > int1)",
    "(pi() == 3.141592653589793 + double1)",
    "(spark_partition_id() > int1)",
    # Special:
    # FindInSet,GetJsonObject,If,Like,
    # MapFromArrays,MapKeys,MapValues,MapZipWith,NextDay,Overlay,StringToMap,
    # SubstringIndex,Unhex
    "(find_in_set('1', comma_separated_str) == 1)",
    "(get_json_object(json_str, '$.a') == 'ab')",
    "(if(bool1, 1, 2) == 1)",
    "(like(str1, '_a_'))",
    "(map_keys(map_from_arrays(array(int_not_null1), array(str1))) == array(1))",
    "(map_keys(map(int_not_null1, str1)) == array(1))",
    "(map_values(map(int_not_null1, str1)) == array('ab'))",
    "(map_keys(map_zip_with(map(int_not_null1, str1), map(int_not_null2, str2), (k, v1, v2) -> if(k == int_not_null2, v2, v1))) == array(1))",
    "(next_day(date1, 'monday') == date2)",
    "(overlay(str1, 'a', 1) == 'aab')",
    "(map_keys(str_to_map(str1)) == array('ab'))",
    "(substring_index(str1, 'b', 2) == 'ab')",
    "(unhex('616263') == str1)",
    # Ansi false only:
    # Multiply, Add, Subtract, Divide, Abs, Pmod
    "(int1 * int2 == 24)",
    "(int1 + int2 == 5)",
    "(int1 - int2 == 1)",
    "(int1 / int2 == 2)",
    "(abs(int1) == 1)",
    "(pmod(double1, double2) == 1)",
]

unsupported_condition_list = [
    "(acosh(double1) < 1.5)",
    "(asinh(double1) < 0.8)", 
    "(sin(double1) > 0.1)",
    "(sqrt(double1) < 1.6)",
    "(tan(double1) > 0.8)",
    "(tanh(double1) < 0.4)",
    "(cbrt(double1) == 3.0)",
    "(degrees(double1) == 0)",
    "(radians(double1) == 0)",
    "(hour(timestamp) == 1)",
    "(minute(timestamp) == 1)",
    "(sha1(str1) == 'b10a8db164e0754105b7a99be72e3fe5')",
    "(levenshtein(str1, str2) == 4)",
    "(sha2(str1, 256) == 'b10a8db164e0754105b7a99be72e3fe5')",
    "(repeat(str1, 2) == 'hellohello')",
    "(crc32(str1) == 1041237462)",
    "(md5(str1) == 'b10a8db164e0754105b7a99be72e3fe5')",
    "(array_join(array_str_1, ',') == '1,2,3')",
    "(1 in(int1, int2, int3))",
    "(json_array_length(json_str) == 1)",
    "(to_unix_timestamp(timestamp) == 1717171717)",
]

def adaptive_select_datagen(condition, gen_list):
    # parse the condition to get the column names
    get_column_names = re.findall(r'[\w\d_]+', condition)
    # filter the gen_list to only include the columns that are in the condition
    return [gen for gen in gen_list if gen[0] in get_column_names]

def check_filtered_length(data_path, condition, conf, lower_bound, upper_bound):
    cpu_df_count = with_cpu_session(lambda spark: spark.read.parquet(data_path).filter(condition).count(), conf=conf)
    
    assert cpu_df_count <= upper_bound
    assert cpu_df_count >= lower_bound

@pytest.mark.skipif(is_databricks_runtime(), reason="Hybrid feature does not support Databricks currently")
@pytest.mark.skipif(not is_hybrid_backend_loaded(), reason="HybridScan specialized tests")
@hybrid_test
@pytest.mark.parametrize('condition', condition_list, ids=idfn)
def test_hybrid_parquet_filter_pushdown_more_exprs(spark_tmp_path, condition):
    data_path = spark_tmp_path + '/PARQUET_DATA'
    gen_list = adaptive_select_datagen(condition, filter_pushdown_gen_list)
    with_cpu_session(
        lambda spark: gen_df(spark, gen_list).write.parquet(data_path),
        conf=rebase_write_corrected_conf)

    conf = {
        'spark.sql.sources.useV1SourceList': 'parquet',
        'spark.rapids.sql.hybrid.parquet.enabled': 'true',
        'spark.rapids.sql.hybrid.parquet.filterPushDown': 'CPU',
        'spark.rapids.sql.exec.FilterExec': False,
        'spark.sql.ansi.enabled': 'false'
    }

    assert_cpu_and_gpu_are_equal_collect_with_capture(
        lambda spark: spark.read.parquet(data_path).filter(condition),
        exist_classes='HybridFileSourceScanExec',
        non_exist_classes='GpuFileSourceScanExec',
        conf=conf)

@allow_non_gpu('FilterExec', 'Sha2', 'Cast', 'Levenshtein', 
  'Crc32', 'LengthOfJsonArray', 'In')
@pytest.mark.skipif(is_databricks_runtime(), reason="Hybrid feature does not support Databricks currently")
@pytest.mark.skipif(not is_hybrid_backend_loaded(), reason="HybridScan specialized tests")
@hybrid_test
@pytest.mark.parametrize('condition', unsupported_condition_list, ids=idfn)
def test_hybrid_parquet_filter_pushdown_more_exprs_unsupported(spark_tmp_path, condition):
    data_path = spark_tmp_path + '/PARQUET_DATA'
    gen_list = adaptive_select_datagen(condition, filter_pushdown_gen_list)
    with_cpu_session(
        lambda spark: gen_df(spark, gen_list).write.parquet(data_path),
        conf=rebase_write_corrected_conf)

    # check if the condition is supported by hybrid execution when cannot push down
    conf = {
        'spark.sql.sources.useV1SourceList': 'parquet',
        'spark.rapids.sql.hybrid.parquet.enabled': 'true',
        'spark.rapids.sql.hybrid.parquet.filterPushDown': 'CPU',
        'spark.sql.ansi.enabled': 'false'
    }

    assert_cpu_and_gpu_are_equal_collect_with_capture(
        lambda spark: spark.read.parquet(data_path).filter(condition),
        exist_classes='HybridFileSourceScanExec',
        non_exist_classes='GpuFileSourceScanExec',
        conf=conf)

cast_condition_list = [
    # Boolean to:
    # Byte, Short, Int, Long, Float, Double, String
    "(cast(bool1 as byte) == 1)",
    "(cast(bool1 as short) == 1)",
    "(cast(bool1 as int) == 1)",
    "(cast(bool1 as long) == 1)",
    "(cast(bool1 as float) == 1.0)",
    "(cast(bool1 as double) == 1.0)",
    "(cast(bool1 as string) == 'true')",
    # Byte to:
    # Boolean, Short, Int, Long, Float, Double, String, Decimal
    pytest.param("(cast(byte1 as boolean) == true)", marks=pytest.mark.xfail(reason='not supported by gluten')),
    "(cast(byte1 as short) == 1)",
    "(cast(byte1 as int) == 1)",
    "(cast(byte1 as long) == 1)",
    "(cast(byte1 as float) == 1.0)",
    "(cast(byte1 as double) == 1.0)",
    "(cast(byte1 as string) == '1')",
    "(cast(byte1 as decimal) == 1)",
    # Short to:
    # Boolean, Byte, Int, Long, Float, Double, String, Decimal
    pytest.param("(cast(short1 as boolean) == true)", marks=pytest.mark.xfail(reason='not supported by gluten')),
    "(cast(short1 as byte) == 1)",
    "(cast(short1 as int) == 1)",
    "(cast(short1 as long) == 1)",
    "(cast(short1 as float) == 1.0)",
    "(cast(short1 as double) == 1.0)",
    "(cast(short1 as string) == '1')",
    "(cast(short1 as decimal) == 1)",
    # Int to:
    # Boolean, Byte, Short, Long, Float, Double, String, Decimal
    pytest.param("(cast(int1 as boolean) == true)", marks=pytest.mark.xfail(reason='not supported by gluten')),
    "(cast(int1 as byte) == 1)",
    "(cast(int1 as short) == 1)",
    "(cast(int1 as long) == 1)",
    "(cast(int1 as float) == 1.0)",
    "(cast(int1 as double) == 1.0)",
    "(cast(int1 as string) == '1')",
    "(cast(int1 as decimal) == 1)",
    # Long to:
    # Boolean, Byte, Short, Int, Float, Double, String, Decimal
    pytest.param("(cast(long1 as boolean) == true)", marks=pytest.mark.xfail(reason='not supported by gluten')),
    "(cast(long1 as byte) == 1)",
    "(cast(long1 as short) == 1)",
    "(cast(long1 as int) == 1)",
    "(cast(long1 as float) == 1.0)",
    "(cast(long1 as double) == 1.0)",
    "(cast(long1 as string) == '1')",
    "(cast(long1 as decimal) == 1)",
    # Float to:
    # Boolean, Byte, Short, Int, Long, Double, String, Decimal
    pytest.param("(cast(float1 as boolean) == true)", marks=pytest.mark.xfail(reason='not supported by gluten')),
    "(cast(float1 as byte) == 1)",
    "(cast(float1 as short) == 1)",
    "(cast(float1 as int) == 1)",
    "(cast(float1 as long) == 1)",
    pytest.param("(cast(float1 as double) == 1.0)", marks=pytest.mark.xfail(reason='not supported by gluten')),
    "(cast(float1 as string) == '0.0')",
    "(cast(float1 as decimal) == 1)",
    # Double to:
    # Boolean, Byte, Short, Int, Long, Float, String, Decimal
    pytest.param("(cast(double1 as boolean) == true)", marks=pytest.mark.xfail(reason='not supported by gluten')),
    "(cast(double1 as byte) == 1)",
    "(cast(double1 as short) == 1)",
    "(cast(double1 as int) == 1)",
    "(cast(double1 as long) == 1)",
    "(cast(double1 as float) == 0)",
    "(cast(double1 as string) == '0.0')",
    "(cast(double1 as decimal) == 1)",
    # Date to:
    # Boolean, Byte, Short, Int, Long, Float, Double, String, Decimal
    pytest.param("(cast(date1 as boolean) == true)", marks=pytest.mark.xfail(reason='not supported by gluten')),
    pytest.param("(cast(date1 as byte) == 1)", marks=pytest.mark.xfail(reason='not supported by gluten')),
    pytest.param("(cast(date1 as short) == 1)", marks=pytest.mark.xfail(reason='not supported by gluten')),
    pytest.param("(cast(date1 as int) == 1)", marks=pytest.mark.xfail(reason='not supported by gluten')),
    pytest.param("(cast(date1 as long) == 1)", marks=pytest.mark.xfail(reason='not supported by gluten')),
    pytest.param("(cast(date1 as float) == 1.0)", marks=pytest.mark.xfail(reason='not supported by gluten')),
    pytest.param("(cast(date1 as double) == 1.0)", marks=pytest.mark.xfail(reason='not supported by gluten')),
    "(substring(cast(date1 as string), 1, 3) == '200')",
    pytest.param("(cast(date1 as decimal) == 1)", marks=pytest.mark.xfail(reason='not supported by gluten')),
    # String to:
    # Boolean, Byte, Short, Int, Long, Float, Double, Decimal
    pytest.param("(cast(str1 as boolean) == true)", marks=pytest.mark.xfail(reason='not supported by gluten')),
    "(cast(string_digit as byte) == 1)",
    "(cast(string_digit as short) == 1)",
    "(cast(string_digit as int) == 1)",
    "(cast(string_digit as long) == 1)",
    "(cast(string_digit as float) == 0.0)",
    "(cast(string_digit as double) == 0.0)",
    "(cast(string_digit as decimal) == 1)",
    # Decimal to:
    # Boolean, Byte, Short, Int, Long, Float, Double, String
    pytest.param("(cast(decimal1 as boolean) == true)", marks=pytest.mark.xfail(reason='not supported by gluten')),
    "(cast(decimal1 as byte) == 0)",
    "(cast(decimal1 as short) == 0)",
    "(cast(decimal1 as int) == 0)",
    "(cast(decimal1 as long) == 0)",
    "(cast(decimal1 as float) == 0.0)",
    "(cast(decimal1 as double) == 0.0)",
    "(cast(decimal1 as string) == '0')",
    # Array to:
    # String, Array
    pytest.param("(cast(array_int1 as string) == '[1, 2, 3]')", marks=pytest.mark.xfail(reason='not supported by gluten')),
    pytest.param("(cast(array_int1 as array<long>) == array(1, 2, 3))", marks=pytest.mark.xfail(reason='not supported by hybrid execution')),
    # Map to:
    # String, Map
    pytest.param("(cast(map_int_str1 as string) == '{{1 -> a, 2 -> b, 3 -> c}}')", marks=pytest.mark.xfail(reason='not supported by gluten')),
    pytest.param("(isnotnull(cast(map_int_str1 as map<long, string>)))", marks=pytest.mark.xfail(reason='not supported by hybrid execution')),
    # Struct to:
    # String, Struct
    pytest.param("(cast(struct_int_str1 as string) == '{{int1 -> 1, str1 -> a}}')", marks=pytest.mark.xfail(reason='not supported by gluten')),
    pytest.param("(cast(struct_int_str1 as struct<col1 long, col2 string>) == struct(1, 'a'))", marks=pytest.mark.xfail(reason='not supported by gluten')),
]

filter_pushdown_cast_gen_list = [
    ('bool1', BooleanGen()),
    ('byte1', ByteGen()),
    ('short1', ShortGen()),
    ('int1', IntegerGen()),
    ('long1', LongGen()),
    ('float1', FloatGen()),
    ('double1', DoubleGen()),
    ('date1', DateGen()),
    # Timestamp is not supported in filter pushdown yet
    ('string1', StringGen()),
    ('string_digit', StringGen(pattern=r'[0-9]{1,2}(\.[0-9]{1,2})?')),
    ('decimal1', DecimalGen()),
    # Null is not supported in parquet
    # Binary is not supported in hybrid execution yet
    # CalendarInterval is not supported in hybrid execution yet
    ('array_int1', ArrayGen(IntegerGen())),
    ('map_int_str1', MapGen(IntegerGen(nullable=False), StringGen())),
    ('struct_int_str1', StructGen([('int1', IntegerGen()), ('str1', StringGen())])),
    # UDT is not supported in hybrid execution yet
    # DayTimeIntervalType is not supported in hybrid execution yet
    # YearMonthIntervalType is not supported in hybrid execution yet
]

@pytest.mark.skipif(is_databricks_runtime(), reason="Hybrid feature does not support Databricks currently")
@pytest.mark.skipif(not is_hybrid_backend_loaded(), reason="HybridScan specialized tests")
@hybrid_test
@pytest.mark.parametrize('condition', cast_condition_list, ids=idfn)
def test_hybrid_parquet_filter_pushdown_cast(spark_tmp_path, condition):
    data_path = spark_tmp_path + '/PARQUET_DATA'
    gen_list = adaptive_select_datagen(condition, filter_pushdown_cast_gen_list)
    with_cpu_session(
        lambda spark: gen_df(spark, gen_list).write.parquet(data_path),
        conf=rebase_write_corrected_conf)

    conf = {
        'spark.sql.sources.useV1SourceList': 'parquet',
        'spark.rapids.sql.hybrid.parquet.enabled': 'true',
        'spark.rapids.sql.hybrid.parquet.filterPushDown': 'CPU',
        'spark.rapids.sql.exec.FilterExec': False,
        'spark.sql.ansi.enabled': 'false',
    }

    assert_cpu_and_gpu_are_equal_collect_with_capture(
        lambda spark: spark.read.parquet(data_path).filter(condition),
        exist_classes='HybridFileSourceScanExec',
        non_exist_classes='GpuFileSourceScanExec',
        conf=conf)

@ignore_order(local=True)
@pytest.mark.skipif(is_databricks_runtime(), reason="Hybrid feature does not support Databricks currently")
@pytest.mark.skipif(not is_hybrid_backend_loaded(), reason="HybridScan specialized tests")
@hybrid_test
def test_hybrid_parquet_filter_pushdown_aqe(spark_tmp_path):
    stream_data_path = spark_tmp_path + '/PARQUET_DATA/stream_data'
    build_data_path = spark_tmp_path + '/PARQUET_DATA/build_data'
    data_gen_schema = [('key', LongGen(nullable=False, min_val=0, max_val=19)), ('value', long_gen)]
    with_cpu_session(
        lambda spark: gen_df(spark, data_gen_schema, length=4096).write.parquet(stream_data_path),
        conf=rebase_write_corrected_conf)
    with_cpu_session(
        lambda spark: gen_df(spark, data_gen_schema, length=512).write.parquet(build_data_path),
        conf=rebase_write_corrected_conf)
    conf = filter_split_conf.copy()
    conf.update({
        'spark.sql.adaptive.enabled': 'true',
        'spark.rapids.sql.hybrid.parquet.enabled': 'true',
        'spark.rapids.sql.hybrid.parquet.filterPushDown': 'CPU'
    })

    def build_side_df(spark):
        return spark.read.parquet(build_data_path) \
            .filter(f.col('key') > 1) \
            .filter(f.col('value').isNotNull())

    plan = with_gpu_session(
        lambda spark: build_side_df(spark)._jdf.queryExecution().executedPlan(),
        conf=conf)
    check_filter_pushdown(plan, pushed_exprs=['> 1', 'isnotnull'], not_pushed_exprs=[])

    def test_fn(spark):
        probe_df = spark.read.parquet(stream_data_path)
        # Perform a broadcast join, explicitly broadcasting the build-side DataFrame
        build_df = f.broadcast(build_side_df(spark))
        return probe_df.join(build_df, ['key'], 'inner')

    assert_gpu_and_cpu_are_equal_collect(test_fn, conf=conf)

@pytest.mark.skipif(is_databricks_runtime(), reason="Hybrid feature does not support Databricks currently")
@pytest.mark.skipif(not is_hybrid_backend_loaded(), reason="HybridScan specialized tests")
@pytest.mark.parametrize('parquet_gens', parquet_gens_list, ids=idfn)
@hybrid_test
def test_hybrid_parquet_bucket_read(parquet_gens, spark_tmp_path, spark_tmp_table_factory):
    data_path = spark_tmp_path + '/PARQUET_BUCKET_DATA'
    
    gen_list = [('id', long_gen)] + [('_c' + str(i), gen) for i, gen in enumerate(parquet_gens)]
    num_buckets = 8
    table_name = spark_tmp_table_factory.get()
    
    with_cpu_session(lambda spark: 
        gen_df(spark, gen_list, length=10000)
            .write
            .bucketBy(num_buckets, "id")
            .sortBy("id")
            .option("path", data_path)
            .saveAsTable(table_name),
        conf=rebase_write_corrected_conf)
    
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.table(table_name).filter("id > 5000"),
        conf={
            'spark.sql.sources.useV1SourceList': 'parquet',
            'spark.rapids.sql.hybrid.parquet.enabled': 'true',
            'spark.sql.sources.bucketing.enabled': 'true',
            'spark.sql.sources.bucketing.autoBucketedScan.enabled': 'false'
        })

@pytest.mark.skipif(is_databricks_runtime(), reason="Hybrid feature does not support Databricks currently")
@pytest.mark.skipif(not is_hybrid_backend_loaded(), reason="HybridScan specialized tests")
@pytest.mark.parametrize('parquet_enabled', [False, True], ids=idfn)
@hybrid_test
def test_hybrid_parquet_sql_hint_single_table(spark_tmp_path, parquet_enabled):
    data_path = spark_tmp_path + '/PARQUET_DATA'
    gen_list = [('key_col', string_gen), ('int_col', int_gen), ('long_col', long_gen)]
    with_cpu_session(
        lambda spark: gen_df(spark, gen_list, length=2048).write.parquet(data_path),
        conf=rebase_write_corrected_conf)

    assert_cpu_and_gpu_are_equal_collect_with_capture(
        lambda spark: spark.read.parquet(data_path).hint("HYBRID_SCAN").filter("int_col > -1"),
        exist_classes='HybridFileSourceScanExec',
        non_exist_classes='GpuFileSourceScanExec',
        conf={
            'spark.sql.sources.useV1SourceList': 'parquet',
            'spark.rapids.sql.hybrid.parquet.enabled': parquet_enabled,
        })

    assert_cpu_and_gpu_are_equal_collect_with_capture(
        lambda spark: spark.read.parquet(data_path).filter("long_col < 0").select('key_col').hint("hybrid_SCAN"),
        exist_classes='HybridFileSourceScanExec',
        non_exist_classes='GpuFileSourceScanExec',
        conf={
            'spark.sql.sources.useV1SourceList': 'parquet',
            'spark.rapids.sql.hybrid.parquet.enabled': parquet_enabled,
        })

@pytest.mark.skipif(is_databricks_runtime(), reason="Hybrid feature does not support Databricks currently")
@pytest.mark.skipif(not is_hybrid_backend_loaded(), reason="HybridScan specialized tests")
@ignore_order(local=True)
@hybrid_test
def test_hybrid_parquet_sql_hint_multiple_tables(spark_tmp_path):
    data_path = spark_tmp_path + '/PARQUET_DATA'
    gen_list = [('key_col', StringGen(pattern='key_[0-9][0-9]', nullable=False)),
                ('int_col', int_gen),
                ('long_col', long_gen),
                ('float_col', float_gen),
                ('double_col', double_gen)]
    with_cpu_session(
        lambda spark: gen_df(spark, gen_list, length=2048).write.parquet(data_path),
        conf=rebase_write_corrected_conf)

    conf = {
        'spark.sql.sources.useV1SourceList': 'parquet',
        'spark.rapids.sql.hybrid.parquet.enabled': False,
    }
    # case 1: one hybrid scan table and one GPU scan table
    def fn1(spark):
        spark.read.parquet(data_path).createOrReplaceTempView("table_parquet")
        return spark.sql("""
            select t1.key_col, t1.int_col, t1.long_col, t1.float_col, t1.double_col
            from (select /*+hybrid_scan*/ * from table_parquet where int_col > -1) t1
            JOIN (select * from table_parquet where long_col > -1) t2
            ON t1.key_col = t2.key_col
            """)
    assert_cpu_and_gpu_are_equal_collect_with_capture(
        fn1,
        exist_classes='HybridFileSourceScanExec,GpuFileSourceScanExec',
        conf=conf)

    # case 2: two hybrid scan tables
    def fn2(spark):
        spark.read.parquet(data_path).createOrReplaceTempView("table_parquet")
        return spark.sql("""
            select /*+HYBRID_SCAN*/
            t1.key_col, t1.int_col, t1.long_col, t1.float_col, t1.double_col
            from (select * from table_parquet where int_col > -1) t1
            JOIN (select * from table_parquet where long_col > -1) t2
            ON t1.key_col = t2.key_col
            """)
    assert_cpu_and_gpu_are_equal_collect_with_capture(
        fn2,
        exist_classes='HybridFileSourceScanExec',
        non_exist_classes='GpuFileSourceScanExec',
        conf=conf)
