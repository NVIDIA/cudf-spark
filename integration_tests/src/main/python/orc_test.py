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
from marks import *
from pyspark.sql.types import *
from spark_init_internal import spark_version
from spark_session import *
from parquet_test import _nested_pruning_schemas
from conftest import is_databricks_runtime

pytestmark = pytest.mark.nightly_resource_consuming_test

def read_orc_df(data_path):
    return lambda spark : spark.read.orc(data_path)

def read_orc_sql(data_path):
    return lambda spark : spark.sql('select * from orc.`{}`'.format(data_path))

# Using timestamps from 1590 to work around a cudf ORC bug
# https://github.com/NVIDIA/spark-rapids/issues/131.
# https://github.com/NVIDIA/spark-rapids/issues/13272
# Once the bug is fixed we should remove this and use timestamp_gen.
def get_orc_timestamp_gen(nullable=True):
    return TimestampGen(start=datetime(1970, 1, 1, tzinfo=timezone.utc), nullable=nullable)

orc_timestamp_gen = get_orc_timestamp_gen()

# test with original orc file reader, the multi-file parallel reader for cloud
__original_orc_file_reader_conf = {'spark.rapids.sql.format.orc.reader.type': 'PERFILE'}
__multithreaded_orc_file_reader_conf = {'spark.rapids.sql.format.orc.reader.type': 'MULTITHREADED',
                                      'spark.rapids.sql.reader.multithreaded.combine.sizeBytes': '0',
                                      'spark.rapids.sql.reader.multithreaded.read.keepOrder': True}
__multithreaded_orc_file_reader_combine_ordered_conf = {
    'spark.rapids.sql.format.orc.reader.type': 'MULTITHREADED',
    'spark.rapids.sql.reader.multithreaded.combine.sizeBytes': '64m',
    'spark.rapids.sql.reader.multithreaded.read.keepOrder': True}
__multithreaded_orc_file_reader_combine_unordered_conf_no_chunked = {
    'spark.rapids.sql.format.orc.reader.type': 'MULTITHREADED',
    'spark.rapids.sql.reader.multithreaded.combine.sizeBytes': '64m',
    'spark.rapids.sql.reader.multithreaded.read.keepOrder': False,
    'spark.rapids.sql.reader.chunked': False}
__multithreaded_orc_file_reader_combine_unordered_conf_chunked = {
    'spark.rapids.sql.format.orc.reader.type': 'MULTITHREADED',
    'spark.rapids.sql.reader.multithreaded.combine.sizeBytes': '64m',
    'spark.rapids.sql.reader.multithreaded.read.keepOrder': False,
    'spark.rapids.sql.reader.chunked': True,
    'spark.rapids.sql.reader.chunked.limitMemoryUsage': False}
__multithreaded_orc_file_reader_combine_unordered_conf_chunked_limited_memory = {
    'spark.rapids.sql.format.orc.reader.type': 'MULTITHREADED',
    'spark.rapids.sql.reader.multithreaded.combine.sizeBytes': '64m',
    'spark.rapids.sql.reader.multithreaded.read.keepOrder': False,
    'spark.rapids.sql.reader.chunked': True,
    'spark.rapids.sql.reader.chunked.limitMemoryUsage': True}
__coalescing_orc_file_reader_conf = {'spark.rapids.sql.format.orc.reader.type': 'COALESCING'}
__reader_opt_confs_common = [__original_orc_file_reader_conf, __multithreaded_orc_file_reader_conf,
                             __coalescing_orc_file_reader_conf,
                             __multithreaded_orc_file_reader_combine_ordered_conf]
__reader_opt_confs_no_chunked = [{**conf, 'spark.rapids.sql.reader.chunked': False}
                                 for conf in __reader_opt_confs_common] + \
                                [pytest.param(__multithreaded_orc_file_reader_combine_unordered_conf_no_chunked,
                                              marks=pytest.mark.ignore_order(local=True))]
__reader_opt_confs_chunked = [{**conf, 'spark.rapids.sql.reader.chunked': True,
                               'spark.rapids.sql.reader.chunked.limitMemoryUsage': True}
                              for conf in __reader_opt_confs_common] + \
                             [{**conf, 'spark.rapids.sql.reader.chunked': True,
                               'spark.rapids.sql.reader.chunked.limitMemoryUsage': False}
                              for conf in __reader_opt_confs_common] + \
                             [pytest.param(__multithreaded_orc_file_reader_combine_unordered_conf_chunked,
                                           marks=pytest.mark.ignore_order(local=True))] + \
                             [pytest.param(__multithreaded_orc_file_reader_combine_unordered_conf_chunked_limited_memory,
                                           marks=pytest.mark.ignore_order(local=True))]
reader_opt_confs = __reader_opt_confs_no_chunked + __reader_opt_confs_chunked
# The Count result can not be sorted, so local sort can not be used.
reader_opt_confs_for_count = __reader_opt_confs_common + [__multithreaded_orc_file_reader_combine_unordered_conf_no_chunked]

non_utc_allow_orc_file_source_scan=['ColumnarToRowExec', 'FileSourceScanExec', 'BatchScanExec'] if is_not_utc() else []

@pytest.mark.parametrize('name', ['timestamp-date-test.orc'])
@pytest.mark.parametrize('read_func', [read_orc_df, read_orc_sql])
@pytest.mark.parametrize('v1_enabled_list', ["", "orc"])
@pytest.mark.parametrize('orc_impl', ["native", "hive"])
@pytest.mark.parametrize('reader_confs', reader_opt_confs, ids=idfn)
@allow_non_gpu(*non_utc_allow_orc_file_source_scan)
def test_basic_read(std_input_path, name, read_func, v1_enabled_list, orc_impl, reader_confs):
    all_confs = copy_and_update(reader_confs, {
        'spark.sql.sources.useV1SourceList': v1_enabled_list,
        'spark.sql.orc.impl': orc_impl})
    assert_gpu_and_cpu_are_equal_collect(
            read_func(std_input_path + '/' + name),
            conf=all_confs)

# ORC does not support negative scale for decimal. So here is "decimal_gens_no_neg".
# Otherwise it will get the below exception.
# ...
#E                   Caused by: java.lang.IllegalArgumentException: Missing integer at
#   'struct<`_c0`:decimal(7,^-3),`_c1`:decimal(7,3),`_c2`:decimal(7,7),`_c3`:decimal(12,2)>'
#E                   	at org.apache.orc.TypeDescription.parseInt(TypeDescription.java:244)
#E                   	at org.apache.orc.TypeDescription.parseType(TypeDescription.java:362)
# ...
# Use every type except boolean, see https://github.com/NVIDIA/spark-rapids/issues/11762 and
# https://github.com/rapidsai/cudf/issues/6763 .
# Once the first issue is fixed, add back boolean_gen
orc_basic_gens = [byte_gen, short_gen, int_gen, long_gen, float_gen, double_gen,
    string_gen, DateGen(start=date(1590, 1, 1)),
    orc_timestamp_gen] + decimal_gens

orc_basic_struct_gen = StructGen([['child'+str(ind), sub_gen] for ind, sub_gen in enumerate(orc_basic_gens)])

# Some array gens, but not all because of nesting
orc_array_gens_sample = [ArrayGen(sub_gen) for sub_gen in orc_basic_gens] + [
    ArrayGen(ArrayGen(short_gen, max_length=10), max_length=10),
    ArrayGen(ArrayGen(string_gen, max_length=10), max_length=10),
    ArrayGen(ArrayGen(decimal_gen_64bit, max_length=10), max_length=10),
    ArrayGen(StructGen([['child0', byte_gen], ['child1', string_gen], ['child2', float_gen]]))]

# Some struct gens, but not all because of nesting.
# No empty struct gen because it leads to an error as below.
#   '''
#     E               pyspark.sql.utils.AnalysisException:
#     E               Datasource does not support writing empty or nested empty schemas.
#     E               Please make sure the data schema has at least one or more column(s).
#   '''
orc_struct_gens_sample = [orc_basic_struct_gen,
    StructGen([['child0', byte_gen], ['child1', orc_basic_struct_gen]]),
    StructGen([['child0', ArrayGen(short_gen)], ['child1', double_gen]])]

orc_basic_map_gens = [simple_string_to_string_map_gen] + [MapGen(f(nullable=False), f()) for f in [
    BooleanGen, ByteGen, ShortGen, IntegerGen, LongGen, FloatGen, DoubleGen,
    lambda nullable=True: get_orc_timestamp_gen(nullable),
    lambda nullable=True: DateGen(start=date(1590, 1, 1), nullable=nullable),
    lambda nullable=True: DecimalGen(precision=15, scale=1, nullable=nullable),
    lambda nullable=True: DecimalGen(precision=36, scale=5, nullable=nullable)]]

# Some map gens, but not all because of nesting
orc_map_gens_sample = orc_basic_map_gens + [
    MapGen(StringGen(pattern='key_[0-9]', nullable=False), ArrayGen(string_gen), max_length=10),
    MapGen(StringGen(pattern='key_[0-9]', nullable=False), ArrayGen(decimal_gen_128bit), max_length=10),
    MapGen(StringGen(pattern='key_[0-9]', nullable=False),
           ArrayGen(StructGen([["c0", decimal_gen_64bit], ["c1", decimal_gen_128bit]])), max_length=10),
    MapGen(RepeatSeqGen(IntegerGen(nullable=False), 10), long_gen, max_length=10),
    MapGen(StringGen(pattern='key_[0-9]', nullable=False), simple_string_to_string_map_gen),
    MapGen(StructGen([['child0', byte_gen], ['child1', long_gen]], nullable=False),
           StructGen([['child0', byte_gen], ['child1', long_gen]]))]

non_utc_allow_orc_scan=['ColumnarToRowExec', 'FileSourceScanExec', 'BatchScanExec'] if is_not_utc() else []

orc_gens_list = [orc_basic_gens,
    orc_array_gens_sample,
    orc_struct_gens_sample,
    orc_map_gens_sample,
    pytest.param([date_gen], marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/131')),
    pytest.param([timestamp_gen], marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/131'))]

flattened_orc_gens = orc_basic_gens + orc_array_gens_sample + orc_struct_gens_sample

@allow_non_gpu('FileSourceScanExec')
@pytest.mark.parametrize('read_func', [read_orc_df, read_orc_sql])
@pytest.mark.parametrize('disable_conf', ['spark.rapids.sql.format.orc.enabled', 'spark.rapids.sql.format.orc.read.enabled'])
def test_orc_fallback(spark_tmp_path, read_func, disable_conf):
    data_gens =[string_gen,
        byte_gen, short_gen, int_gen, long_gen, boolean_gen]

    gen_list = [('_c' + str(i), gen) for i, gen in enumerate(data_gens)]
    gen = StructGen(gen_list, nullable=False)
    data_path = spark_tmp_path + '/ORC_DATA'
    reader = read_func(data_path)
    with_cpu_session(
            lambda spark : gen_df(spark, gen).write.orc(data_path))
    assert_gpu_fallback_collect(
            # Default Ansi mode is on for Spark 400, here cast c2 to long then add c3 to avoid overflow
            lambda spark : reader(spark).select(f.col('*'), f.col('_c2').cast(long_gen.data_type) + f.col('_c3')),
            'FileSourceScanExec',
            conf={disable_conf: 'false',
                "spark.sql.sources.useV1SourceList": "orc"})

@pytest.mark.order(2)
@pytest.mark.parametrize('orc_gens', orc_gens_list, ids=idfn)
@pytest.mark.parametrize('read_func', [read_orc_df, read_orc_sql])
@pytest.mark.parametrize('reader_confs', reader_opt_confs, ids=idfn)
@pytest.mark.parametrize('v1_enabled_list', ['', 'orc'])
@tz_sensitive_test
@allow_non_gpu(*non_utc_allow_orc_scan)
def test_read_round_trip(spark_tmp_path, orc_gens, read_func, reader_confs, v1_enabled_list):
    gen_list = [('_c' + str(i), gen) for i, gen in enumerate(orc_gens)]
    data_path = spark_tmp_path + '/ORC_DATA'
    with_cpu_session(
            lambda spark : gen_df(spark, gen_list).write.orc(data_path))
    all_confs = copy_and_update(reader_confs, {'spark.sql.sources.useV1SourceList': v1_enabled_list})
    assert_gpu_and_cpu_are_equal_collect(
            read_func(data_path),
            conf=all_confs)

# Use every type except boolean, see https://github.com/NVIDIA/spark-rapids/issues/11762 and
# https://github.com/rapidsai/cudf/issues/6763 .
# Once the first issue is fixed, add back boolean_gen
orc_pred_push_gens = [
        byte_gen, short_gen, int_gen, long_gen, float_gen, double_gen,
        string_gen,
        # Once https://github.com/NVIDIA/spark-rapids/issues/139 is fixed replace this with
        # date_gen
        DateGen(start=date(1590, 1, 1)),
        # Once https://github.com/NVIDIA/spark-rapids/issues/140 is fixed replace this with
        # timestamp_gen
        orc_timestamp_gen]

@pytest.mark.order(2)
@pytest.mark.parametrize('orc_gen', orc_pred_push_gens, ids=idfn)
@pytest.mark.parametrize('read_func', [read_orc_df, read_orc_sql])
@pytest.mark.parametrize('v1_enabled_list', ["", "orc"])
@pytest.mark.parametrize('reader_confs', reader_opt_confs, ids=idfn)
@allow_non_gpu(*non_utc_allow_orc_scan)
def test_pred_push_round_trip(spark_tmp_path, orc_gen, read_func, v1_enabled_list, reader_confs):
    data_path = spark_tmp_path + '/ORC_DATA'
    # Append two struct columns to verify nested predicate pushdown.
    gen_list = [('a', RepeatSeqGen(orc_gen, 100)), ('b', orc_gen),
        ('s1', StructGen([['sa', orc_gen]])),
        ('s2', StructGen([['sa', StructGen([['ssa', orc_gen]])]]))]
    s0 = with_cpu_session(lambda spark: gen_scalar(orc_gen, force_no_nulls=True))
    with_cpu_session(
            lambda spark : gen_df(spark, gen_list).orderBy('a').write.orc(data_path))
    all_confs = copy_and_update(reader_confs, {'spark.sql.sources.useV1SourceList': v1_enabled_list})
    rf = read_func(data_path)
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: rf(spark).select(f.col('a') >= s0, f.col('s1.sa') >= s0, f.col('s2.sa.ssa') >= s0),
            conf=all_confs)

orc_compress_options = ['none', 'uncompressed', 'snappy', 'zlib']
# zstd is available in spark 3.2.0 and later.
if not is_before_spark_320() and not is_spark_cdh():
    orc_compress_options.append('zstd')

# The following need extra jars 'lzo'
# https://github.com/NVIDIA/spark-rapids/issues/143

# Test the different compress combinations
@ignore_order
@pytest.mark.parametrize('v1_enabled_list', ["", "orc"])
@pytest.mark.parametrize('reader_confs', reader_opt_confs, ids=idfn)
def test_mixed_compress_read(spark_tmp_path, v1_enabled_list, reader_confs):
    data_pathes = []
    for compress in orc_compress_options:
        data_path = spark_tmp_path + '/ORC_DATA' + compress
        with_cpu_session(
                lambda spark : binary_op_df(spark, long_gen).write.orc(data_path),
                conf={'spark.sql.orc.compression.codec': compress})
        data_pathes.append(data_path)

    all_confs = copy_and_update(reader_confs, {'spark.sql.sources.useV1SourceList': v1_enabled_list})
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : spark.read.orc(data_pathes),
            conf=all_confs)

@pytest.mark.parametrize('compress', orc_compress_options)
@pytest.mark.parametrize('v1_enabled_list', ["", "orc"])
@pytest.mark.parametrize('reader_confs', reader_opt_confs, ids=idfn)
def test_compress_read_round_trip(spark_tmp_path, compress, v1_enabled_list, reader_confs):
    data_path = spark_tmp_path + '/ORC_DATA'
    with_cpu_session(
            lambda spark : binary_op_df(spark, long_gen).write.orc(data_path),
            conf={'spark.sql.orc.compression.codec': compress})
    all_confs = copy_and_update(reader_confs, {'spark.sql.sources.useV1SourceList': v1_enabled_list})
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : spark.read.orc(data_path),
            conf=all_confs)

@pytest.mark.parametrize('v1_enabled_list', ["", "orc"])
@pytest.mark.parametrize('reader_confs', reader_opt_confs, ids=idfn)
@allow_non_gpu(*non_utc_allow_orc_scan)
def test_simple_partitioned_read(spark_tmp_path, v1_enabled_list, reader_confs):
    # Once https://github.com/NVIDIA/spark-rapids/issues/131 is fixed
    # we should go with a more standard set of generators
    # Use every type except boolean, see https://github.com/NVIDIA/spark-rapids/issues/11762 and
    # https://github.com/rapidsai/cudf/issues/6763 .
    # Once the first issue is fixed, add back boolean_gen
    orc_gens = [byte_gen, short_gen, int_gen, long_gen, float_gen, double_gen,
    string_gen, DateGen(start=date(1590, 1, 1)),
    orc_timestamp_gen]
    gen_list = [('_c' + str(i), gen) for i, gen in enumerate(orc_gens)]
    first_data_path = spark_tmp_path + '/ORC_DATA/key=0/key2=20'
    with_cpu_session(
            lambda spark : gen_df(spark, gen_list).write.orc(first_data_path))
    second_data_path = spark_tmp_path + '/ORC_DATA/key=1/key2=21'
    with_cpu_session(
            lambda spark : gen_df(spark, gen_list).write.orc(second_data_path))
    third_data_path = spark_tmp_path + '/ORC_DATA/key=2/key2=22'
    with_cpu_session(
            lambda spark : gen_df(spark, gen_list).write.orc(third_data_path))
    data_path = spark_tmp_path + '/ORC_DATA'
    all_confs = copy_and_update(reader_confs, {'spark.sql.sources.useV1SourceList': v1_enabled_list})
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: spark.read.orc(data_path),
            conf=all_confs)

# Setup external table by altering column names
def setup_external_table_with_forced_positions(spark, table_name, data_path):
    rename_cols_query = "CREATE EXTERNAL TABLE `{}` (`col10` INT, `_c1` STRING, `col30` DOUBLE) STORED AS orc LOCATION '{}'".format(table_name, data_path)
    spark.sql(rename_cols_query).collect

@pytest.mark.skipif(is_before_spark_320(), reason='ORC forced positional evolution support is added in Spark-3.2')
@pytest.mark.parametrize('reader_confs', reader_opt_confs, ids=idfn)
@pytest.mark.parametrize('forced_position', ["true", "false"])
@pytest.mark.parametrize('orc_impl', ["native", "hive"])
def test_orc_forced_position(spark_tmp_path, spark_tmp_table_factory, reader_confs, forced_position, orc_impl):
    orc_gens = [int_gen, string_gen, double_gen]
    gen_list = [('_c' + str(i), gen) for i, gen in enumerate(orc_gens)]
    data_path = spark_tmp_path + 'ORC_DATA'
    with_cpu_session(lambda spark : gen_df(spark, gen_list).write.orc(data_path))
    table_name = spark_tmp_table_factory.get()
    with_cpu_session(lambda spark : setup_external_table_with_forced_positions(spark, table_name, data_path))

    all_confs = copy_and_update(reader_confs, {
        'orc.force.positional.evolution': forced_position,
        'spark.sql.orc.impl': orc_impl})
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : spark.sql("SELECT * FROM {}".format(table_name)),
        conf=all_confs)

# In this we are reading the data, but only reading the key the data was partitioned by
@pytest.mark.parametrize('v1_enabled_list', ["", "orc"])
@pytest.mark.parametrize('reader_confs', reader_opt_confs, ids=idfn)
def test_partitioned_read_just_partitions(spark_tmp_path, v1_enabled_list, reader_confs):
    orc_gens = [byte_gen]
    gen_list = [('_c' + str(i), gen) for i, gen in enumerate(orc_gens)]
    first_data_path = spark_tmp_path + '/ORC_DATA/key=0'
    with_cpu_session(
            lambda spark : gen_df(spark, gen_list).write.orc(first_data_path))
    second_data_path = spark_tmp_path + '/ORC_DATA/key=1'
    with_cpu_session(
            lambda spark : gen_df(spark, gen_list).write.orc(second_data_path))
    data_path = spark_tmp_path + '/ORC_DATA'
    all_confs = copy_and_update(reader_confs, {'spark.sql.sources.useV1SourceList': v1_enabled_list})
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : spark.read.orc(data_path).select("key"),
            conf=all_confs)

@pytest.mark.parametrize('v1_enabled_list', ["", "orc"])
@pytest.mark.parametrize('reader_confs', reader_opt_confs, ids=idfn)
@allow_non_gpu(*non_utc_allow_orc_scan)
def test_merge_schema_read(spark_tmp_path, v1_enabled_list, reader_confs):
    # Once https://github.com/NVIDIA/spark-rapids/issues/131 is fixed
    # we should go with a more standard set of generators
    # Use every type except boolean, see https://github.com/NVIDIA/spark-rapids/issues/11762 and
    # https://github.com/rapidsai/cudf/issues/6763 .
    # Once the first issue is fixed, add back boolean_gen
    orc_gens = [byte_gen, short_gen, int_gen, long_gen, float_gen, double_gen,
    string_gen, DateGen(start=date(1590, 1, 1)),
    orc_timestamp_gen]
    first_gen_list = [('_c' + str(i), gen) for i, gen in enumerate(orc_gens)]
    first_data_path = spark_tmp_path + '/ORC_DATA/key=0'
    with_cpu_session(
            lambda spark : gen_df(spark, first_gen_list).write.orc(first_data_path))
    second_gen_list = [(('_c' if i % 2 == 0 else '_b') + str(i), gen) for i, gen in enumerate(orc_gens)]
    second_data_path = spark_tmp_path + '/ORC_DATA/key=1'
    with_cpu_session(
            lambda spark : gen_df(spark, second_gen_list).write.orc(second_data_path))
    data_path = spark_tmp_path + '/ORC_DATA'
    all_confs = copy_and_update(reader_confs, {'spark.sql.sources.useV1SourceList': v1_enabled_list})
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : spark.read.option('mergeSchema', 'true').orc(data_path),
            conf=all_confs)

@pytest.mark.parametrize('v1_enabled_list', ["", "orc"])
@pytest.mark.parametrize('reader_confs', reader_opt_confs, ids=idfn)
def test_read_orc_with_empty_clipped_schema(spark_tmp_path, v1_enabled_list, reader_confs):
    data_path = spark_tmp_path + '/ORC_DATA'
    with_cpu_session(
        lambda spark: gen_df(spark, [('a', int_gen)], length=100).write.orc(data_path))
    schema = StructType([StructField('b', IntegerType()), StructField('c', StringType())])
    all_confs = copy_and_update(reader_confs, {'spark.sql.sources.useV1SourceList': v1_enabled_list})
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.schema(schema).orc(data_path), conf=all_confs)

@pytest.mark.parametrize('v1_enabled_list', ["", "orc"])
@pytest.mark.parametrize('reader_confs', reader_opt_confs, ids=idfn)
def test_orc_read_multiple_schema(spark_tmp_path, v1_enabled_list, reader_confs):
    first_gen_list = [('a', int_gen), ('b', int_gen)]
    first_data_path = spark_tmp_path + '/ORC_DATA/key=0'
    with_cpu_session(
        lambda spark: gen_df(spark, first_gen_list, num_slices=10).write.orc(first_data_path))
    second_gen_list = [('c', int_gen), ('b', int_gen), ('a', int_gen)]
    second_data_path = spark_tmp_path + '/ORC_DATA/key=1'
    with_cpu_session(
        lambda spark: gen_df(spark, second_gen_list, num_slices=10).write.orc(second_data_path))
    data_path = spark_tmp_path + '/ORC_DATA'
    read_schema = StructType([StructField("b", IntegerType()),
                              StructField("a", IntegerType()),
                              StructField("c", IntegerType())])
    all_confs = copy_and_update(reader_confs, {'spark.sql.sources.useV1SourceList': v1_enabled_list})
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.schema(read_schema).orc(data_path),
        conf=all_confs)

@pytest.mark.parametrize('v1_enabled_list', ["", "orc"])
def test_orc_read_avoid_coalesce_incompatible_files(spark_tmp_path, v1_enabled_list):
    data_path = spark_tmp_path + '/ORC_DATA'
    def setup_table(spark):
        df1 = spark.createDataFrame([(("a", "b"),)], "x: struct<y: string, z: string>")
        df1.write.orc(data_path + "/data1")
        df2 = spark.createDataFrame([(("a",),)], "x: struct<z: string>")
        df2.write.orc(data_path + "/data2")
    with_cpu_session(setup_table)
    # Configure confs to read as a single task
    all_confs = copy_and_update(__coalescing_orc_file_reader_conf, {
        "spark.sql.sources.useV1SourceList": v1_enabled_list,
        "spark.sql.files.minPartitionNum": "1"})
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read \
            .schema("x STRUCT<y: string, z: string>") \
            .option("recursiveFileLookup", "true").orc(data_path),
        conf=all_confs)

@pytest.mark.parametrize('v1_enabled_list', ["", "orc"])
@pytest.mark.parametrize('reader_confs', reader_opt_confs, ids=idfn)
def test_input_meta(spark_tmp_path, v1_enabled_list, reader_confs):
    first_data_path = spark_tmp_path + '/ORC_DATA/key=0'
    with_cpu_session(
            lambda spark : unary_op_df(spark, long_gen).write.orc(first_data_path))
    second_data_path = spark_tmp_path + '/ORC_DATA/key=1'
    with_cpu_session(
            lambda spark : unary_op_df(spark, long_gen).write.orc(second_data_path))
    data_path = spark_tmp_path + '/ORC_DATA'
    all_confs = copy_and_update(reader_confs, {'spark.sql.sources.useV1SourceList': v1_enabled_list})
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : spark.read.orc(data_path)\
                    .filter(f.col('a') > 0)\
                    .selectExpr('a',
                        'input_file_name()',
                        'input_file_block_start()',
                        'input_file_block_length()'),
            conf=all_confs)

@allow_non_gpu('ProjectExec', 'Alias', 'InputFileName', 'InputFileBlockStart', 'InputFileBlockLength',
               'FilterExec', 'And', 'IsNotNull', 'GreaterThan', 'Literal',
               'FileSourceScanExec', 'ColumnarToRowExec',
               'BatchScanExec', 'OrcScan')
@pytest.mark.parametrize('v1_enabled_list', ["", "orc"])
@pytest.mark.parametrize('disable_conf', ['spark.rapids.sql.format.orc.enabled', 'spark.rapids.sql.format.orc.read.enabled'])
@pytest.mark.parametrize('reader_confs', reader_opt_confs, ids=idfn)
def test_input_meta_fallback(spark_tmp_path, v1_enabled_list, reader_confs, disable_conf):
    first_data_path = spark_tmp_path + '/ORC_DATA/key=0'
    with_cpu_session(
            lambda spark : unary_op_df(spark, long_gen).write.orc(first_data_path))
    second_data_path = spark_tmp_path + '/ORC_DATA/key=1'
    with_cpu_session(
            lambda spark : unary_op_df(spark, long_gen).write.orc(second_data_path))
    data_path = spark_tmp_path + '/ORC_DATA'
    all_confs = copy_and_update(reader_confs, {
        'spark.sql.sources.useV1SourceList': v1_enabled_list,
        disable_conf: 'false'})
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : spark.read.orc(data_path)\
                    .filter(f.col('a') > 0)\
                    .selectExpr('a',
                        'input_file_name()',
                        'input_file_block_start()',
                        'input_file_block_length()'),
            conf=all_confs)

def setup_orc_file_no_column_names(spark, table_name, location=None):
    drop_query = "DROP TABLE IF EXISTS {}".format(table_name)
    create_query = "CREATE TABLE `{}` (`_col1` INT, `_col2` STRING, `_col3` INT) USING orc".format(table_name)
    if location:
        create_query += f" LOCATION '{location}'"
    insert_query = "INSERT INTO {} VALUES(13, '155', 2020)".format(table_name)
    spark.sql(drop_query).collect
    spark.sql(create_query).collect
    spark.sql(insert_query).collect

@pytest.mark.parametrize('reader_confs', reader_opt_confs, ids=idfn)
def test_missing_column_names(spark_tmp_table_factory, reader_confs):
    table_name = spark_tmp_table_factory.get()
    with_cpu_session(lambda spark : setup_orc_file_no_column_names(spark, table_name))
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : spark.sql("SELECT _col3,_col2 FROM {}".format(table_name)),
        reader_confs)

@pytest.mark.parametrize('reader_confs', reader_opt_confs_for_count, ids=idfn)
def test_missing_column_names_count(spark_tmp_table_factory, reader_confs):
    table_name = spark_tmp_table_factory.get()
    with_cpu_session(lambda spark : setup_orc_file_no_column_names(spark, table_name))
    assert_gpu_and_cpu_row_counts_equal(
        lambda spark : spark.sql("SELECT * FROM {}".format(table_name)),
        reader_confs)

# ORC checks if there are no column names by looking at the column names
# so it is possible to have some of the names match the pattern and other not
def setup_orc_file_partial_no_column_names(spark, table_name, location=None):
    drop_query = "DROP TABLE IF EXISTS {}".format(table_name)
    create_query = "CREATE TABLE `{}` (`_col1` INT, `arr` ARRAY<STRING>, `str` STRUCT<a: INT>) USING orc".format(table_name)
    if location:
        create_query += f" LOCATION '{location}'"
    insert_query = "INSERT INTO {} VALUES(13, array('155'), named_struct('a', 2020))".format(table_name)
    spark.sql(drop_query).collect
    spark.sql(create_query).collect
    spark.sql(insert_query).collect


@pytest.mark.parametrize('reader_confs', reader_opt_confs, ids=idfn)
def test_partial_missing_column_names(spark_tmp_table_factory, reader_confs):
    table_name = spark_tmp_table_factory.get()
    with_cpu_session(lambda spark : setup_orc_file_partial_no_column_names(spark, table_name))
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : spark.sql("SELECT str,arr FROM {}".format(table_name)),
        reader_confs)

@pytest.mark.parametrize('reader_confs', reader_opt_confs_for_count, ids=idfn)
def test_partial_missing_column_names_count(spark_tmp_table_factory, reader_confs):
    table_name = spark_tmp_table_factory.get()
    with_cpu_session(lambda spark : setup_orc_file_partial_no_column_names(spark, table_name))
    assert_gpu_and_cpu_row_counts_equal(
        lambda spark : spark.sql("SELECT * FROM {}".format(table_name)),
        reader_confs)



@pytest.mark.parametrize('reader_confs', reader_opt_confs, ids=idfn)
def test_missing_column_names_with_schema(spark_tmp_table_factory, spark_tmp_path, reader_confs):
    table_name = spark_tmp_table_factory.get()
    table_location = spark_tmp_path + "/ORC_DATA"
    with_cpu_session(lambda spark : setup_orc_file_no_column_names(spark, table_name, table_location))
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : spark.read.schema("a int, b string, c int").orc(table_location),
        reader_confs)

@pytest.mark.parametrize('reader_confs', reader_opt_confs_for_count, ids=idfn)
def test_missing_column_names_count_with_schema(spark_tmp_table_factory, spark_tmp_path, reader_confs):
    table_name = spark_tmp_table_factory.get()
    table_location = spark_tmp_path + "/ORC_DATA"
    with_cpu_session(lambda spark : setup_orc_file_no_column_names(spark, table_name, table_location))
    assert_gpu_and_cpu_row_counts_equal(
        lambda spark : spark.read.schema("a int, b string, c int").orc(table_location),
        reader_confs)

def setup_orc_file_with_column_names(spark, table_name):
    drop_query = "DROP TABLE IF EXISTS {}".format(table_name)
    create_query = "CREATE TABLE `{}` (`c_1` INT, `c_2` STRING, `c_3` ARRAY<INT>) USING orc".format(table_name)
    insert_query = "INSERT INTO {} VALUES(13, '155', array(2020))".format(table_name)
    spark.sql(drop_query).collect
    spark.sql(create_query).collect
    spark.sql(insert_query).collect

@pytest.mark.parametrize('reader_confs', reader_opt_confs, ids=idfn)
def test_disorder_read_schema(spark_tmp_table_factory, reader_confs):
    table_name = spark_tmp_table_factory.get()
    with_cpu_session(lambda spark : setup_orc_file_with_column_names(spark, table_name))
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : spark.sql("SELECT c_2,c_1 FROM {}".format(table_name)),
        reader_confs)
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : spark.sql("SELECT c_3,c_1 FROM {}".format(table_name)),
        reader_confs)
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : spark.sql("SELECT c_3,c_2 FROM {}".format(table_name)),
        reader_confs)
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : spark.sql("SELECT c_1,c_3,c_2 FROM {}".format(table_name)),
        reader_confs)
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : spark.sql("SELECT c_1,c_2,c_3 FROM {}".format(table_name)),
        reader_confs)
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : spark.sql("SELECT c_2,c_1,c_3 FROM {}".format(table_name)),
        reader_confs)
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : spark.sql("SELECT c_2,c_3,c_1 FROM {}".format(table_name)),
        reader_confs)
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : spark.sql("SELECT c_3,c_1,c_2 FROM {}".format(table_name)),
        reader_confs)
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : spark.sql("SELECT c_3,c_2,c_1 FROM {}".format(table_name)),
        reader_confs)


@pytest.mark.parametrize('reader_confs', reader_opt_confs, ids=idfn)
def test_missing_column_names_filter(spark_tmp_table_factory, reader_confs):
    table_name = spark_tmp_table_factory.get()
    with_cpu_session(lambda spark : setup_orc_file_no_column_names(spark, table_name))
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : spark.sql("SELECT _col3,_col2 FROM {} WHERE _col2 = '155'".format(table_name)),
        reader_confs)


@pytest.mark.parametrize('data_gen,read_schema', _nested_pruning_schemas, ids=idfn)
@pytest.mark.parametrize('reader_confs', reader_opt_confs, ids=idfn)
@pytest.mark.parametrize('v1_enabled_list', ["", "orc"])
@pytest.mark.parametrize('nested_enabled', ["true", "false"])
def test_read_nested_pruning(spark_tmp_path, data_gen, read_schema, reader_confs, v1_enabled_list, nested_enabled):
    data_path = spark_tmp_path + '/ORC_DATA'
    with_cpu_session(
            lambda spark : gen_df(spark, data_gen).write.orc(data_path))
    all_confs = copy_and_update(reader_confs, {
        'spark.sql.sources.useV1SourceList': v1_enabled_list,
        'spark.sql.optimizer.nestedSchemaPruning.enabled': nested_enabled})
    # This is a hack to get the type in a slightly less verbose way
    rs = StructGen(read_schema, nullable=False).data_type
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : spark.read.schema(rs).orc(data_path),
            conf=all_confs)


@pytest.mark.parametrize('reader_confs', reader_opt_confs, ids=idfn)
@pytest.mark.parametrize('v1_enabled_list', ["", "orc"])
def test_read_orc_with_all_nested_fields_missing(
        spark_tmp_path, reader_confs, v1_enabled_list):
    data_path = spark_tmp_path + '/ORC_DATA'
    with_cpu_session(
        lambda spark: spark.sql("""
            SELECT named_struct('first', 'Janet', 'last', 'Jones') AS name
            UNION ALL SELECT cast(null AS struct<first:string,last:string>)
            """).write.orc(data_path))
    read_schema = StructType([StructField('name', StructType([
        StructField('middle', StringType())]))])
    all_confs = copy_and_update(reader_confs, {
        'spark.sql.sources.useV1SourceList': v1_enabled_list})
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.schema(read_schema).orc(data_path), conf=all_confs)

    array_data_path = spark_tmp_path + '/ORC_ARRAY_DATA'
    with_cpu_session(
        lambda spark: spark.sql("""
            SELECT named_struct('history', array(1, 2)) AS name
            UNION ALL SELECT cast(null AS struct<history:array<int>>)
            """).write.orc(array_data_path))
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.schema(read_schema).orc(array_data_path), conf=all_confs)

    map_data_path = spark_tmp_path + '/ORC_MAP_DATA'
    with_cpu_session(
        lambda spark: spark.sql("""
            SELECT named_struct('attrs', map('a', 1)) AS name
            UNION ALL SELECT cast(null AS struct<attrs:map<string,int>>)
            """).write.orc(map_data_path))
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.schema(read_schema).orc(map_data_path), conf=all_confs)

    array_struct_data_path = spark_tmp_path + '/ORC_ARRAY_STRUCT_DATA'
    with_cpu_session(
        lambda spark: spark.sql("""
            SELECT array(named_struct('first', 'Janet')) AS names
            UNION ALL SELECT cast(null AS array<struct<first:string>>)
            """).write.orc(array_struct_data_path))
    array_struct_read_schema = StructType([StructField('names', ArrayType(StructType([
        StructField('middle', StringType())])))])
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.schema(array_struct_read_schema).orc(array_struct_data_path),
        conf=all_confs)

    map_struct_data_path = spark_tmp_path + '/ORC_MAP_STRUCT_DATA'
    with_cpu_session(
        lambda spark: spark.sql("""
            SELECT map('primary', named_struct('first', 'Janet')) AS names
            UNION ALL SELECT cast(null AS map<string,struct<first:string>>)
            """).write.orc(map_struct_data_path))
    map_struct_read_schema = StructType([StructField('names', MapType(
        StringType(), StructType([StructField('middle', StringType())])))])
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.schema(map_struct_read_schema).orc(map_struct_data_path),
        conf=all_confs)


# This is for the corner case of reading only a struct column that has no nulls.
# Then there will be no streams in a stripe connecting to this column (Its ROW_INDEX
# streams have been pruned by the Plugin.), and CUDF throws an exception for such case.
# Here is the tracking issue: 'https://github.com/rapidsai/cudf/issues/8878'. But it has
# been fixed. Still keep the test here to have this corner case tested.
def test_read_struct_without_stream(spark_tmp_path):
    data_gen = StructGen([['c_byte', ByteGen(nullable=False)]], nullable=False)
    data_path = spark_tmp_path + '/ORC_DATA'
    with_cpu_session(
            lambda spark : unary_op_df(spark, data_gen, 10).write.orc(data_path))
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : spark.read.orc(data_path))


@pytest.mark.parametrize('orc_gen', flattened_orc_gens, ids=idfn)
@pytest.mark.parametrize('reader_confs', reader_opt_confs, ids=idfn)
@pytest.mark.parametrize('v1_enabled_list', ["", "orc"])
@pytest.mark.parametrize('case_sensitive', ["false", "true"])
@allow_non_gpu(*non_utc_allow_orc_scan)
def test_read_with_more_columns(spark_tmp_path, orc_gen, reader_confs, v1_enabled_list, case_sensitive):
    struct_gen = StructGen([('nested_col', orc_gen)])
    # Map is not supported yet.
    gen_list = [("top_pri", orc_gen),
                ("top_st", struct_gen),
                ("top_ar", ArrayGen(struct_gen, max_length=10))]
    data_path = spark_tmp_path + '/ORC_DATA'
    with_cpu_session(
            lambda spark : gen_df(spark, gen_list).write.orc(data_path))
    all_confs = reader_confs.copy()
    all_confs.update({'spark.sql.sources.useV1SourceList': v1_enabled_list,
        'spark.sql.caseSensitive': case_sensitive})
    # This is a hack to get the type in a slightly less verbose way
    extra_struct_gen = StructGen([('nested_col', orc_gen), ("nested_non_existing", orc_gen)])
    extra_gen_list = [("top_pri", orc_gen),
                      ("top_non_existing_mid", orc_gen),
                      ("TOP_AR", ArrayGen(extra_struct_gen, max_length=10)),
                      ("top_ST", extra_struct_gen),
                      ("top_non_existing_end", orc_gen)]
    rs = StructGen(extra_gen_list, nullable=False).data_type
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : spark.read.schema(rs).orc(data_path),
            conf=all_confs)

@allow_non_gpu(any = True)
@pytest.mark.parametrize('metadata_column', ["file_path", "file_name", "file_size", "file_modification_time"])
def test_orc_scan_with_hidden_metadata_fallback(spark_tmp_path, metadata_column):
    data_path = spark_tmp_path + "/hidden_metadata.orc"
    with_cpu_session(lambda spark : spark.range(10) \
                     .selectExpr("id", "id % 3 as p") \
                     .write \
                     .partitionBy("p") \
                     .mode("overwrite") \
                     .orc(data_path))

    def do_orc_scan(spark):
        df = spark.read.orc(data_path).selectExpr("id", "_metadata.{}".format(metadata_column))
        return df

    assert_cpu_and_gpu_are_equal_collect_with_capture(
        do_orc_scan,
        exist_classes= "FileSourceScanExec",
        non_exist_classes= "GpuBatchScanExec")


@ignore_order
@pytest.mark.parametrize('v1_enabled_list', ["", "orc"])
@pytest.mark.parametrize('reader_confs', reader_opt_confs, ids=idfn)
@pytest.mark.skipif(is_databricks_runtime(), reason="Databricks does not support ignoreCorruptFiles")
def test_orc_read_with_corrupt_files(spark_tmp_path, reader_confs, v1_enabled_list):
    first_data_path = spark_tmp_path + '/ORC_DATA/first'
    with_cpu_session(lambda spark : spark.range(1).toDF("a").write.orc(first_data_path))
    second_data_path = spark_tmp_path + '/ORC_DATA/second'
    with_cpu_session(lambda spark : spark.range(1, 2).toDF("a").write.orc(second_data_path))
    third_data_path = spark_tmp_path + '/ORC_DATA/third'
    with_cpu_session(lambda spark : spark.range(2, 3).toDF("a").write.json(third_data_path))

    all_confs = copy_and_update(reader_confs,
                                {'spark.sql.files.ignoreCorruptFiles': "true",
                                 'spark.sql.sources.useV1SourceList': v1_enabled_list})

    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : spark.read.orc([first_data_path, second_data_path, third_data_path]),
            conf=all_confs)

# Spark(330,_) allows aggregate pushdown on ORC by enabling spark.sql.orc.aggregatePushdown.
# Note that Min/Max don't push down partition column. Only Count does.
# The following tests that GPU falls back to CPU when aggregates are pushed down on ORC.
#
# When the spark configuration is enabled we check the following:
# ----------------------------------------------+
# | Aggregate | Partition Column | FallBack CPU |
# +-----------+------------------+--------------+
# |   COUNT   |        Y         |      Y       |
# |    MIN    |        Y         |      N       |
# |    MAX    |        Y         |      N       |
# |   COUNT   |        N         |      Y       |
# |    MIN    |        N         |      Y       |
# |    MAX    |        N         |      Y       |

_aggregate_orc_list_col_partition = ['COUNT']
_aggregate_orc_list_no_col_partition = ['MAX', 'MIN']
_aggregate_orc_list = _aggregate_orc_list_col_partition + _aggregate_orc_list_no_col_partition
_orc_aggregate_pushdown_enabled_conf = {'spark.rapids.sql.format.orc.write.enabled': 'true',
                                        'spark.sql.orc.aggregatePushdown': 'true',
                                        "spark.sql.sources.useV1SourceList": ""}

def _do_orc_scan_with_agg(spark, path, agg):
    spark.range(10).selectExpr("id", "id % 3 as p").write.mode("overwrite").orc(path)
    return spark.read.orc(path).selectExpr('{}(p)'.format(agg))

def _do_orc_scan_with_agg_on_partitioned_column(spark, path, agg):
    spark.range(10).selectExpr("id", "id % 3 as p").write.partitionBy("p").mode("overwrite").orc(path)
    return spark.read.orc(path).selectExpr('{}(p)'.format(agg))

@pytest.mark.parametrize('aggregate', _aggregate_orc_list)
@allow_non_gpu(any = True)
def test_orc_scan_with_aggregate_pushdown(spark_tmp_path, aggregate):
    """
    Spark(330,_) allows aggregate pushdown on ORC by enabling spark.sql.orc.aggregatePushdown.
    When the spark configuration is enabled we check the following:
    ---------------------------+
    | Aggregate | FallBack CPU |
    +-----------+--------------+
    |   COUNT   |      Y       |
    |    MIN    |      Y       |
    |    MAX    |      Y       |
    """
    data_path = spark_tmp_path + '/ORC_DATA/pushdown_00.orc'

    # fallback to CPU
    assert_cpu_and_gpu_are_equal_collect_with_capture(
        lambda spark: _do_orc_scan_with_agg(spark, data_path, aggregate),
        exist_classes="BatchScanExec",
        non_exist_classes="GpuBatchScanExec",
        conf=_orc_aggregate_pushdown_enabled_conf)

@pytest.mark.parametrize('aggregate', _aggregate_orc_list_col_partition)
@allow_non_gpu(any = True)
def test_orc_scan_with_aggregate_pushdown_on_col_partition(spark_tmp_path, aggregate):
    """
    Spark(330,_) allows aggregate pushdown on ORC by enabling spark.sql.orc.aggregatePushdown.
    Note that Min/Max don't push down partition column. Only Count does.
    This test checks that GPU falls back to CPU when aggregates are pushed down on ORC.
    When the spark configuration is enabled we check the following:
    ----------------------------------------------+
    | Aggregate | Partition Column | FallBack CPU |
    +-----------+------------------+--------------+
    |   COUNT   |        Y         |      Y       |
    """
    data_path = spark_tmp_path + '/ORC_DATA/pushdown_01.orc'

    # fallback to CPU only if aggregate is COUNT
    assert_cpu_and_gpu_are_equal_collect_with_capture(
            lambda spark: _do_orc_scan_with_agg_on_partitioned_column(spark, data_path, aggregate),
            exist_classes="BatchScanExec",
            non_exist_classes="GpuBatchScanExec",
            conf=_orc_aggregate_pushdown_enabled_conf)

@pytest.mark.parametrize('aggregate', _aggregate_orc_list_no_col_partition)
def test_orc_scan_with_aggregate_no_pushdown_on_col_partition(spark_tmp_path, aggregate):
    """
    Spark(330,_) allows aggregate pushdown on ORC by enabling spark.sql.orc.aggregatePushdown.
    Note that Min/Max don't push down partition column.
    When the spark configuration is enabled we check the following:
    ----------------------------------------------+
    | Aggregate | Partition Column | FallBack CPU |
    +-----------+------------------+--------------+
    |    MIN    |        Y         |      N       |
    |    MAX    |        Y         |      N       |
    """
    data_path = spark_tmp_path + '/ORC_DATA/pushdown_02.orc'

    # should not fallback to CPU
    assert_gpu_and_cpu_are_equal_collect(
                lambda spark: _do_orc_scan_with_agg_on_partitioned_column(spark, data_path, aggregate),
                conf=_orc_aggregate_pushdown_enabled_conf)


def test_orc_read_count(spark_tmp_path):
    data_path = spark_tmp_path + '/ORC_DATA'
    orc_gens = [int_gen, string_gen, double_gen]
    gen_list = [('_c' + str(i), gen) for i, gen in enumerate(orc_gens)]

    with_cpu_session(lambda spark: gen_df(spark, gen_list).write.orc(data_path))

    assert_gpu_and_cpu_row_counts_equal(lambda spark: spark.read.orc(data_path))

    # assert the spark plan of the equivalent SQL query contains no column in read schema
    assert_cpu_and_gpu_are_equal_sql_with_capture(
        lambda spark: spark.read.orc(data_path), "SELECT COUNT(*) FROM tab", "tab",
        exist_classes=r'GpuFileGpuScan orc .* ReadSchema: struct<>')

# The test_orc_varchar file was created with the Hive CLI like this:
# CREATE TABLE test_orc_varchar(id int, name varchar(20)) STORED AS ORC LOCATION '...';
# INSERT INTO test_orc_varchar values(1, 'abc');
def test_orc_read_varchar_as_string(std_input_path):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.schema("id bigint, name string").orc(std_input_path + "/test_orc_varchar.orc"))


@pytest.mark.parametrize('gens', orc_gens_list, ids=idfn)
@pytest.mark.parametrize('keep_order', [True, pytest.param(False, marks=pytest.mark.ignore_order(local=True))])
@tz_sensitive_test
@allow_non_gpu(*non_utc_allow_orc_scan)
def test_read_round_trip_for_multithreaded_combining(spark_tmp_path, gens, keep_order):
    gen_list = [('_c' + str(i), gen) for i, gen in enumerate(gens)]
    data_path = spark_tmp_path + '/ORC_DATA'
    # 50 partitions to generate enough small files
    with_cpu_session(
        lambda spark: gen_df(spark, gen_list).repartition(50).write.orc(data_path))
    all_confs = {'spark.rapids.sql.format.orc.reader.type': 'MULTITHREADED',
                 'spark.rapids.sql.reader.multithreaded.combine.sizeBytes': '64m',
                 'spark.rapids.sql.reader.multithreaded.read.keepOrder': keep_order}
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.orc(data_path), conf=all_confs)


@pytest.mark.parametrize('keep_order', [True, pytest.param(False, marks=pytest.mark.ignore_order(local=True))])
@allow_non_gpu(*non_utc_allow_orc_scan)
def test_simple_partitioned_read_for_multithreaded_combining(spark_tmp_path, keep_order):
    # Use every type except boolean, see https://github.com/NVIDIA/spark-rapids/issues/11762 and
    # https://github.com/rapidsai/cudf/issues/6763 .
    # Once the first issue is fixed, add back boolean_gen
    orc_gens = [byte_gen, short_gen, int_gen, long_gen, float_gen, double_gen,
                string_gen, DateGen(start=date(1590, 1, 1)),
                orc_timestamp_gen]
    gen_list = [('_c' + str(i), gen) for i, gen in enumerate(orc_gens)]
    first_data_path = spark_tmp_path + '/ORC_DATA/key=0/key2=20'
    with_cpu_session(
        lambda spark: gen_df(spark, gen_list).repartition(50).write.orc(first_data_path))
    second_data_path = spark_tmp_path + '/ORC_DATA/key=1/key2=21'
    with_cpu_session(
        lambda spark: gen_df(spark, gen_list).repartition(50).write.orc(second_data_path))
    third_data_path = spark_tmp_path + '/ORC_DATA/key=2/key2=22'
    with_cpu_session(
        lambda spark: gen_df(spark, gen_list).repartition(50).write.orc(third_data_path))
    data_path = spark_tmp_path + '/ORC_DATA'
    all_confs = {'spark.rapids.sql.format.orc.reader.type': 'MULTITHREADED',
                 'spark.rapids.sql.reader.multithreaded.combine.sizeBytes': '64m',
                 'spark.rapids.sql.reader.multithreaded.read.keepOrder': keep_order}
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.orc(data_path), conf=all_confs)


@pytest.mark.skipif(is_spark_340_or_later() and not is_databricks_runtime(),
                    reason="https://github.com/NVIDIA/spark-rapids/issues/8324")
@pytest.mark.parametrize('data_file', ['fixed-length-char-column-from-hive.orc'])
@pytest.mark.parametrize('reader', [read_orc_df, read_orc_sql])
def test_read_hive_fixed_length_char(std_input_path, data_file, reader):
    """
    Test that a file containing CHAR data is readable as STRING.
    The plugin behaviour matches all Spark versions prior to 3.4.0,
    and Databricks version 13.3 (i.e. 3.4.1) and after.
    """
    assert_gpu_and_cpu_are_equal_collect(
        reader(std_input_path + '/' + data_file),
        conf={})


@allow_non_gpu("ProjectExec")
@pytest.mark.skipif(is_before_spark_340(),
                    reason="https://github.com/NVIDIA/spark-rapids/issues/8324")
@pytest.mark.skipif(is_databricks_version_or_later(13, 3),
                    reason="The SELECT * query does not involve ProjectExec "
                           "on Databricks versions >= 13.3. "
                           "Can't test Project fallback without ProjectExec.")
@pytest.mark.parametrize('data_file', ['fixed-length-char-column-from-hive.orc'])
@pytest.mark.parametrize('reader', [read_orc_df, read_orc_sql])
def test_project_fallback_when_reading_hive_fixed_length_char(std_input_path, data_file, reader):
    """
    Test that reading a file containing fixed-width CHAR data (e.g. CHAR(3)) as a STRING column
    causes the ProjectExec to fall back to CPU.
    Note: This test can be removed when
    https://github.com/NVIDIA/spark-rapids/issues/8324 is resolved.

    This test does not apply to Databricks >= 13.3, because there would be
    no ProjectExec to fall back to CPU.
    """
    assert_gpu_fallback_collect(
        reader(std_input_path + '/' + data_file),
        cpu_fallback_class_name="ProjectExec",
        conf={})


@pytest.mark.parametrize('read_func', [read_orc_df, read_orc_sql])
@pytest.mark.parametrize('v1_enabled_list', ["", "orc"])
@pytest.mark.parametrize('orc_impl', ["native", "hive"])
@pytest.mark.parametrize('reader_confs', reader_opt_confs, ids=idfn)
@pytest.mark.parametrize('col_name', ['K0', 'k0', 'K3', 'k3', 'V0', 'v0'], ids=idfn)
@ignore_order
def test_read_case_col_name(spark_tmp_path, read_func, v1_enabled_list, orc_impl, reader_confs, col_name):
    all_confs = copy_and_update(reader_confs, {
        'spark.sql.sources.useV1SourceList': v1_enabled_list,
        'spark.sql.orc.impl': orc_impl})
    gen_list =[('k0', LongGen(nullable=False, min_val=0, max_val=0)),
            ('k1', LongGen(nullable=False, min_val=1, max_val=1)),
            ('k2', LongGen(nullable=False, min_val=2, max_val=2)),
            ('k3', LongGen(nullable=False, min_val=3, max_val=3)),
            ('v0', LongGen()),
            ('v1', LongGen()),
            ('v2', LongGen()),
            ('v3', LongGen())]

    gen = StructGen(gen_list, nullable=False)
    data_path = spark_tmp_path + '/ORC_DATA'
    reader = read_func(data_path)
    with_cpu_session(
            lambda spark : gen_df(spark, gen).write.partitionBy('k0', 'k1', 'k2', 'k3').orc(data_path))

    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : reader(spark).selectExpr(col_name),
            conf=all_confs)

@pytest.mark.parametrize("reader_confs", reader_opt_confs, ids=idfn)
@ignore_order
def test_orc_column_name_with_dots(spark_tmp_path, reader_confs):
    data_path = spark_tmp_path + "/ORC_DATA"
    reader = read_orc_df(data_path)
    all_confs = reader_confs
    gens = [
        ("a.b", StructGen([
            ("c.d.e", StructGen([
                ("f.g", int_gen),
                ("h", string_gen)])),
            ("i.j", long_gen)])),
        # Use every type except boolean, see https://github.com/NVIDIA/spark-rapids/issues/11762 and
        # https://github.com/rapidsai/cudf/issues/6763 .
        # Once the first issue is fixed, add back boolean_gen for column k
        ("k", int_gen)]
    with_cpu_session(lambda spark: gen_df(spark, gens).write.orc(data_path))
    assert_gpu_and_cpu_are_equal_collect(lambda spark: reader(spark), conf=all_confs)
    assert_gpu_and_cpu_are_equal_collect(lambda spark: reader(spark).selectExpr("`a.b`"), conf=all_confs)
    assert_gpu_and_cpu_are_equal_collect(lambda spark: reader(spark).selectExpr("`a.b`.`c.d.e`.`f.g`"),
                                         conf=all_confs)


# This corresponds to a Spark ORC test in https://github.com/apache/spark/blob/v3.4.0/sql/core/src/test/scala/org/apache/spark/sql/execution/datasources/orc/OrcQuerySuite.scala#L173
@pytest.mark.parametrize("reader_confs", reader_opt_confs, ids=idfn)
@ignore_order
def test_orc_with_null_column(spark_tmp_path, reader_confs):
    data_path = spark_tmp_path + "/ORC_DATA"
    all_confs = reader_confs

    def gen_null_df(spark):
        return spark.createDataFrame(
            [(None, None, None, None, None)],
            # Use every type except boolean, see https://github.com/NVIDIA/spark-rapids/issues/11762 and
            # https://github.com/rapidsai/cudf/issues/6763 .
            # Once the first issue is fixed, add back boolean_gen
            "c1 int, c2 long, c3 float, c4 double, c5 int")

    assert_gpu_and_cpu_writes_are_equal_collect(
        lambda spark, path: gen_null_df(spark).write.orc(path),
        lambda spark, path: spark.read.orc(path),
        data_path,
        conf=all_confs)
    gpu_file_path = data_path + "/GPU"
    reader = read_orc_df(gpu_file_path)
    assert_gpu_and_cpu_are_equal_collect(lambda spark: reader(spark), conf=all_confs)

@ignore_order
@large_data_test
@pytest.mark.parametrize("reader_confs", reader_opt_confs, ids=idfn)
def test_orc_with_null_column_with_1m_rows(spark_tmp_path, reader_confs):
    data_path = spark_tmp_path + "/ORC_DATA"
    all_confs = reader_confs
    data = [(i, None, None, None, None) for i in range(1000000)]
    def gen_null_df(spark):
        return spark.createDataFrame(
            data,
            # Use every type except boolean, see https://github.com/NVIDIA/spark-rapids/issues/11762 and
            # https://github.com/rapidsai/cudf/issues/6763 .
            # Once the first issue is fixed, add back boolean_gen
            "c1 int, c2 long, c3 float, c4 double, c5 int")
    assert_gpu_and_cpu_writes_are_equal_collect(
        lambda spark, path: gen_null_df(spark).write.orc(path),
        lambda spark, path: spark.read.orc(path),
        data_path,
        conf=all_confs)
    gpu_file_path = data_path + "/GPU"
    sqls = ["SELECT * FROM my_large_table",
            "SELECT * FROM my_large_table WHERE c2 = 5",
            "SELECT COUNT(*) FROM my_large_table WHERE c3 IS NOT NULL",
            "SELECT * FROM my_large_table WHERE c4 IS NULL",
            "SELECT * FROM my_large_table WHERE c5 IS NULL",
            ]
    for sql in sqls:
        assert_gpu_and_cpu_are_equal_sql(
            lambda spark: spark.read.orc(gpu_file_path),
            "my_large_table",
            sql)


#
# V_0_11.orc is generated by code:
#   @Test
#   public void testGenerateOrc() throws IOException {
#     TypeDescription schema = TypeDescription.fromString("struct<x:bigint>");
#     Configuration config = new Configuration();
#     Path path = new Path("/tmp/V_0_11.orc");
#
#     long[] inp = new long[]{1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 3, 4, 5, 6,
#         7, 8, 9, 10, 1, 1, 1, 1, 1, 1, 10, 9, 7, 6, 5, 4, 3, 2, 1, 1, 1, 1, 1,
#         2, 5, 1, 3, 7, 1, 9, 2, 6, 3, 7, 1, 9, 2, 6, 3, 7, 1, 9, 2, 6, 3, 7, 1,
#         9, 2, 6, 3, 7, 1, 9, 2, 6, 2000, 2, 1, 1, 1, 1, 1, 3, 7, 1, 9, 2, 6, 1,
#         1, 1, 1, 1};
#     List<Long> input = Lists.newArrayList(Longs.asList(inp));
#     Writer writer = OrcFile.createWriter(path,
#         OrcFile.writerOptions(config)
#             .setSchema(schema)
#             .compress(CompressionKind.SNAPPY)
#             .version(OrcFile.Version.V_0_11) #############: use V_0_11 version ################
#             .bufferSize(10000)
#             .encodingStrategy(OrcFile.EncodingStrategy.SPEED));
#     VectorizedRowBatch batch = schema.createRowBatch();
#     for (Long l : input) {
#       appendLong(batch, l);
#     }
#     writer.addRowBatch(batch);
#     writer.close();
#   }
#
# Change the version can generate V_0_12.orc file
#
@ignore_order
def test_orc_version_V_0_11_and_V_0_12(std_input_path):
    assert_gpu_and_cpu_are_equal_sql(
        lambda spark: spark.read.orc(std_input_path + "/V_0_11.orc"),
        "v11_table",
        "select * from v11_table")

    assert_gpu_and_cpu_are_equal_sql(
        lambda spark: spark.read.orc(std_input_path + "/V_0_12.orc"),
        "v12_table",
        "select * from v12_table")

def test_orc_not_support_timestamp_ltz(std_input_path):
    """
    Test both CPU and GPU does not support ORC type: timestamp with local time zone.
    The meta of `timestamp_ltz.orc` is: Type: struct<ts:timestamp with local time zone>
    Refer to ORC types: https://orc.apache.org/docs/types.html
    The error message is like:
        [PARSE_SYNTAX_ERROR] Syntax error at or near 'with'.(line 1, pos 10)
        == SQL ==
        timestamp with local time zone
        ----------^^^
    """
    data_path = f"{std_input_path}/timestamp_ltz.orc"
    expected_error_message = "timestamp with local time zone" \
        if is_spark_420_or_later() else "ParseException"
    assert_gpu_and_cpu_error(lambda spark: spark.read.orc(data_path).collect(),
                             conf={},
                             error_message=expected_error_message)

@pytest.mark.parametrize("reader_confs", reader_opt_confs, ids=idfn)
# Setting end timestamp as None almost always generate ts >= 2200 year.
# Setting end timestamp < 2200 to test running columnarly on GPU;
@pytest.mark.parametrize('end_timestamp', [None, datetime(2199, 1, 1, tzinfo=timezone.utc)], ids=idfn)
@pytest.mark.parametrize('v1_enabled_list', ["", "orc"])
@pytest.mark.parametrize("timezone_pair", [("UTC", "Asia/Shanghai"), ("Asia/Shanghai", "UTC"), ("Asia/Shanghai", "America/Los_Angeles")], ids=idfn)
@tz_sensitive_test
def test_orc_non_utc_timezone(reader_confs, end_timestamp, spark_tmp_path, v1_enabled_list, timezone_pair):
    d_gen = DateGen(start=date(1590, 1, 1))
    # Update start year to 1590 when https://github.com/NVIDIA/spark-rapids/issues/13272 is fixed.
    ts_gen = TimestampGen(start=datetime(1970, 1, 1, tzinfo=timezone.utc), end=end_timestamp, nullable=True)
    date_timestamp_gens = [('c1', d_gen), ('c2', ts_gen)]

    (write_timezone, read_timezone) = timezone_pair
    write_confs = copy_and_update(reader_confs, {
        'spark.sql.sources.useV1SourceList': v1_enabled_list,
        'spark.rapids.sql.format.orc.enabled': True,
        'spark.rapids.sql.format.orc.write.enabled': True,
        'spark.sql.session.timeZone': write_timezone
    })
    read_confs = copy_and_update(reader_confs, {
        'spark.sql.sources.useV1SourceList': v1_enabled_list,
        'spark.rapids.sql.format.orc.enabled': True,
        'spark.rapids.sql.format.orc.read.enabled': True,
        'spark.sql.session.timeZone': read_timezone,
        # ignore write timezone when reading, this is for test purpose only
        # The `tz_sensitive_test` mark guarantees the write and read are in the same timezone
        'spark.rapids.sql.orc.read.ignore.write.timezone': True
    })

    # write on CPU
    cpu_write_path = spark_tmp_path + "/ORC_DATA_CPU"
    with_cpu_session(lambda spark: gen_df(spark, date_timestamp_gens).write.orc(cpu_write_path), conf=write_confs)
    assert_gpu_and_cpu_are_equal_collect(read_orc_df(cpu_write_path), conf=read_confs)

@pytest.mark.skip(reason='https://github.com/NVIDIA/spark-rapids/issues/13272: CPU can not read ORC file generated by GPU when timestamp is less than 1970 year')
def test_orc_write_but_cpu_read_fail(spark_tmp_path):
    """
    CPU read fails with the following error:
        Caused by: java.lang.IllegalArgumentException: nanos > 999999999 or < 0
	        at java.sql/java.sql.Timestamp.setNanos(Timestamp.java:336)
	        at org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector.asScratchTimestamp
	Reproduce: export DATAGEN_SEED=1754555738; export TZ=UTC; then run this test case
    """
    gpu_write_path = spark_tmp_path + "/ORC_DATA_GPU"
    # If change the start year to 1970, then the test will pass.
    ts_gen = TimestampGen(start=datetime(1590, 1, 1, tzinfo=timezone.utc), nullable=True)
    # Write timestamp on GPU
    with_gpu_session(lambda spark: gen_df(spark, [("c1", ts_gen)]).repartition(1).write.orc(gpu_write_path))
    # Read timestamp on CPU and GPU
    assert_gpu_and_cpu_are_equal_collect(read_orc_df(gpu_write_path))
