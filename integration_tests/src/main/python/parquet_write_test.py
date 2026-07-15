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
from datetime import date, datetime, timezone
from data_gen import *
from enum import Enum
from marks import *
from pyspark.sql.types import *
from spark_session import *

import pyspark.sql.functions as f
import pyspark.sql.utils
import random

pytestmark = pytest.mark.nightly_resource_consuming_test

conf_key_parquet_datetimeRebaseModeInWrite = 'spark.sql.parquet.datetimeRebaseModeInWrite'
conf_key_parquet_int96RebaseModeInWrite = 'spark.sql.parquet.int96RebaseModeInWrite'
conf_key_parquet_datetimeRebaseModeInRead = 'spark.sql.parquet.datetimeRebaseModeInRead'
conf_key_parquet_int96RebaseModeInRead = 'spark.sql.parquet.int96RebaseModeInRead'

# test with original parquet file reader, the multi-file parallel reader for cloud, and coalesce file reader for
# non-cloud
original_parquet_file_reader_conf={'spark.rapids.sql.format.parquet.reader.type': 'PERFILE'}
multithreaded_parquet_file_reader_conf={'spark.rapids.sql.format.parquet.reader.type': 'MULTITHREADED'}
coalesce_parquet_file_reader_conf={'spark.rapids.sql.format.parquet.reader.type': 'COALESCING'}
reader_opt_confs = [original_parquet_file_reader_conf, multithreaded_parquet_file_reader_conf,
        coalesce_parquet_file_reader_conf]
parquet_decimal_struct_gen= StructGen([['child'+str(ind), sub_gen] for ind, sub_gen in enumerate(decimal_gens)])
writer_confs={conf_key_parquet_datetimeRebaseModeInWrite: 'CORRECTED',
              conf_key_parquet_int96RebaseModeInWrite: 'CORRECTED'}

parquet_basic_gen =[byte_gen, short_gen, int_gen, long_gen, float_gen, double_gen,
                    string_gen, boolean_gen, date_gen, TimestampGen(), binary_gen]

parquet_basic_map_gens = [MapGen(f(nullable=False), f()) for f in [
    BooleanGen, ByteGen, ShortGen, IntegerGen, LongGen, FloatGen, DoubleGen, DateGen,
    TimestampGen]] + [simple_string_to_string_map_gen,
                           MapGen(DecimalGen(20, 2, nullable=False), decimal_gen_128bit),
                           # python is not happy with binary values being keys of a map
                           MapGen(StringGen("a{1,5}", nullable=False), binary_gen)]

parquet_struct_gen_no_maps = [
    StructGen([['child' + str(ind), sub_gen] for ind, sub_gen in enumerate(parquet_basic_gen)]),
    StructGen([['child0', StructGen([['child1', byte_gen]])]])
]

parquet_struct_of_map_gen = StructGen([['child0', MapGen(StringGen(nullable=False), StringGen(), max_length=5)], ['child1', IntegerGen()]])

parquet_struct_gen = parquet_struct_gen_no_maps + [parquet_struct_of_map_gen]

parquet_array_gen = [ArrayGen(sub_gen, max_length=10) for sub_gen in parquet_basic_gen + parquet_struct_gen] + [
    ArrayGen(ArrayGen(sub_gen, max_length=10), max_length=10) for sub_gen in parquet_basic_gen + parquet_struct_gen_no_maps] + [
    ArrayGen(ArrayGen(parquet_struct_of_map_gen, max_length=4), max_length=4)]

parquet_map_gens_sample = parquet_basic_map_gens + [MapGen(StringGen(pattern='key_[0-9]', nullable=False),
                                                           ArrayGen(string_gen), max_length=10),
                                                    MapGen(RepeatSeqGen(IntegerGen(nullable=False), 10), long_gen,
                                                           max_length=10),
                                                    MapGen(StringGen(pattern='key_[0-9]', nullable=False),
                                                           simple_string_to_string_map_gen)]

parquet_datetime_gen_simple = [DateGen(start=date(1, 1, 1), end=date(2000, 1, 1))
                               .with_special_case(date(1000, 1, 1), weight=10.0),
                               TimestampGen(start=datetime(1, 1, 1, tzinfo=timezone.utc),
                                            end=datetime(2000, 1, 1, tzinfo=timezone.utc))
                               .with_special_case(datetime(1000, 1, 1, tzinfo=timezone.utc), weight=10.0)]
parquet_datetime_in_struct_gen = [
    StructGen([['child' + str(ind), sub_gen] for ind, sub_gen in enumerate(parquet_datetime_gen_simple)])]
parquet_datetime_in_array_gen = [ArrayGen(sub_gen, max_length=10) for sub_gen in
                                 parquet_datetime_gen_simple + parquet_datetime_in_struct_gen]
parquet_nested_datetime_gen = parquet_datetime_gen_simple + parquet_datetime_in_struct_gen + \
                              parquet_datetime_in_array_gen

parquet_map_gens = parquet_map_gens_sample + [
    MapGen(StructGen([['child0', StringGen()], ['child1', StringGen()]], nullable=False), FloatGen()),
    MapGen(StructGen([['child0', StringGen(nullable=True)]], nullable=False), StringGen())]
parquet_write_gens_list = [[binary_gen], parquet_basic_gen, decimal_gens] +  [ [single_gen] for single_gen in parquet_struct_gen + parquet_array_gen + parquet_map_gens]
parquet_ts_write_options = ['INT96', 'TIMESTAMP_MICROS', 'TIMESTAMP_MILLIS']

@pytest.mark.order(1) # at the head of xdist worker queue if pytest-order is installed
@pytest.mark.parametrize('parquet_gens', parquet_write_gens_list, ids=idfn)
@allow_non_gpu(*non_utc_allow)
def test_write_round_trip(spark_tmp_path, parquet_gens):
    gen_list = [('_c' + str(i), gen) for i, gen in enumerate(parquet_gens)]
    data_path = spark_tmp_path + '/PARQUET_DATA'
    assert_gpu_and_cpu_writes_are_equal_collect(
            lambda spark, path: gen_df(spark, gen_list).coalesce(1).write.parquet(path),
            lambda spark, path: spark.read.parquet(path),
            data_path,
            conf=writer_confs)

@pytest.mark.parametrize('gen', [ByteGen(nullable=False),
    ShortGen(nullable=False),
    IntegerGen(nullable=False),
    LongGen(nullable=False),
    FloatGen(nullable=False),
    DoubleGen(nullable=False),
    BooleanGen(nullable=False),
    StringGen(nullable=False),
    StructGen([('b', LongGen(nullable=False))], nullable=False)], ids=idfn)
@allow_non_gpu(*non_utc_allow)
def test_write_round_trip_nullable_struct(spark_tmp_path, gen):
    gen_for_struct = StructGen([('c', gen)], nullable=True)
    data_path = spark_tmp_path + '/PARQUET_DATA'
    assert_gpu_and_cpu_writes_are_equal_collect(
            lambda spark, path: unary_op_df(spark, gen_for_struct, num_slices=1).write.parquet(path),
            lambda spark, path: spark.read.parquet(path),
            data_path,
            conf=writer_confs)

all_nulls_string_gen = SetValuesGen(StringType(), [None])
empty_or_null_string_gen = SetValuesGen(StringType(), [None, ""])
all_empty_string_gen = SetValuesGen(StringType(), [""])
all_nulls_array_gen = SetValuesGen(ArrayType(StringType()), [None])
all_empty_array_gen = SetValuesGen(ArrayType(StringType()), [[]])
all_array_empty_string_gen = SetValuesGen(ArrayType(StringType()), [["", ""]])
mixed_empty_nulls_array_gen = SetValuesGen(ArrayType(StringType()), [None, [], [None], [""], [None, ""]])
mixed_empty_nulls_map_gen = SetValuesGen(MapType(StringType(), StringType()), [{}, None, {"A": ""}, {"B": None}])
all_nulls_map_gen = SetValuesGen(MapType(StringType(), StringType()), [None])
all_empty_map_gen = SetValuesGen(MapType(StringType(), StringType()), [{}])

par_write_odd_empty_strings_gens_sample = [all_nulls_string_gen,
        empty_or_null_string_gen,
        all_empty_string_gen,
        all_nulls_array_gen,
        all_empty_array_gen,
        all_array_empty_string_gen,
        mixed_empty_nulls_array_gen,
        mixed_empty_nulls_map_gen,
        all_nulls_map_gen,
        all_empty_map_gen]

@pytest.mark.parametrize('par_gen', par_write_odd_empty_strings_gens_sample, ids=idfn)
def test_write_round_trip_corner(spark_tmp_path, par_gen):
    gen_list = [('_c0', par_gen)]
    data_path = spark_tmp_path + '/PAR_DATA'
    assert_gpu_and_cpu_writes_are_equal_collect(
            lambda spark, path: gen_df(spark, gen_list, 128000, num_slices=1).write.parquet(path),
            lambda spark, path: spark.read.parquet(path),
            data_path)

@pytest.mark.parametrize('parquet_gens', [[
    TimestampGen(),
    ArrayGen(TimestampGen(), max_length=10),
    MapGen(TimestampGen(nullable=False), TimestampGen())]], ids=idfn)
@pytest.mark.parametrize('ts_type', parquet_ts_write_options)
@allow_non_gpu(*non_utc_allow)
def test_timestamp_write_round_trip(spark_tmp_path, parquet_gens, ts_type):
    gen_list = [('_c' + str(i), gen) for i, gen in enumerate(parquet_gens)]
    data_path = spark_tmp_path + '/PARQUET_DATA'
    all_confs = copy_and_update(writer_confs, {'spark.sql.parquet.outputTimestampType': ts_type})
    assert_gpu_and_cpu_writes_are_equal_collect(
        lambda spark, path: gen_df(spark, gen_list).coalesce(1).write.parquet(path),
        lambda spark, path: spark.read.parquet(path),
        data_path,
        conf=all_confs)

@pytest.mark.parametrize('ts_type', parquet_ts_write_options)
@pytest.mark.parametrize('ts_rebase', ['CORRECTED'])
@ignore_order
@allow_non_gpu(*non_utc_allow)
def test_write_ts_millis(spark_tmp_path, ts_type, ts_rebase):
    gen = TimestampGen()
    data_path = spark_tmp_path + '/PARQUET_DATA'
    assert_gpu_and_cpu_writes_are_equal_collect(
        lambda spark, path: unary_op_df(spark, gen).write.parquet(path),
        lambda spark, path: spark.read.parquet(path),
        data_path,
        conf={conf_key_parquet_datetimeRebaseModeInWrite: ts_rebase,
              conf_key_parquet_int96RebaseModeInWrite: ts_rebase,
              'spark.sql.parquet.outputTimestampType': ts_type})


parquet_part_write_gens = [
    byte_gen, short_gen, int_gen, long_gen,
    # Some file systems have issues with UTF8 strings so to help the test pass even there
    StringGen('(\\w| ){0,50}'),
    boolean_gen, date_gen,
    TimestampGen()]

# There are race conditions around when individual files are read in for partitioned data
@ignore_order
@pytest.mark.order(1) # at the head of xdist worker queue if pytest-order is installed
@pytest.mark.parametrize('parquet_gen', parquet_part_write_gens, ids=idfn)
@allow_non_gpu(*non_utc_allow)
def test_part_write_round_trip(spark_tmp_path, parquet_gen):
    gen_list = [('a', RepeatSeqGen(parquet_gen, 10)),
                ('b', parquet_gen)]
    data_path = spark_tmp_path + '/PARQUET_DATA'
    assert_gpu_and_cpu_writes_are_equal_collect(
            lambda spark, path: gen_df(spark, gen_list).coalesce(1).write.partitionBy('a').parquet(path),
            lambda spark, path: spark.read.parquet(path),
            data_path,
            conf=writer_confs)


@pytest.mark.skipif(is_spark_340_or_later() or is_databricks122_or_later(), reason="`WriteFilesExec` is only supported in Spark 340+")
@pytest.mark.parametrize('data_gen', [TimestampGen()], ids=idfn)
@pytest.mark.allow_non_gpu("DataWritingCommandExec", *non_utc_allow)
def test_int96_write_conf(spark_tmp_path, data_gen):
    data_path = spark_tmp_path + '/PARQUET_DATA'
    confs = copy_and_update(writer_confs, {
        'spark.sql.parquet.outputTimestampType': 'INT96',
        'spark.rapids.sql.format.parquet.writer.int96.enabled': 'false'})

    assert_gpu_fallback_write(
        lambda spark, path: unary_op_df(spark, data_gen).coalesce(1).write.parquet(path),
        lambda spark, path: spark.read.parquet(path),
        data_path,
        ['DataWritingCommandExec'],
        confs)

@pytest.mark.skipif(is_before_spark_340() and not is_databricks122_or_later(), reason="`WriteFilesExec` is only supported in Spark 340+")
@pytest.mark.parametrize('data_gen', [TimestampGen()], ids=idfn)
# Note: From Spark 340, WriteFilesExec is introduced.
@pytest.mark.allow_non_gpu("DataWritingCommandExec", "WriteFilesExec", *non_utc_allow)
def test_int96_write_conf_with_write_exec(spark_tmp_path, data_gen):
    data_path = spark_tmp_path + '/PARQUET_DATA'
    confs = copy_and_update(writer_confs, {
        'spark.sql.parquet.outputTimestampType': 'INT96',
        'spark.rapids.sql.format.parquet.writer.int96.enabled': 'false'})

    assert_gpu_fallback_write(
        lambda spark, path: unary_op_df(spark, data_gen).coalesce(1).write.parquet(path),
        lambda spark, path: spark.read.parquet(path),
        data_path,
        ['DataWritingCommandExec', 'WriteFilesExec'],
        confs)

@allow_non_gpu(*non_utc_allow)
def test_all_null_int96(spark_tmp_path):
    class AllNullTimestampGen(TimestampGen):
        def start(self, rand):
            self._start(rand, lambda : None)

        def _cache_repr(self):
            return super()._cache_repr() + '(all_nulls)'

    data_path = spark_tmp_path + '/PARQUET_DATA'
    confs = copy_and_update(writer_confs, {'spark.sql.parquet.outputTimestampType': 'INT96'})
    assert_gpu_and_cpu_writes_are_equal_collect(
        lambda spark, path : unary_op_df(spark, AllNullTimestampGen()).coalesce(1).write.parquet(path),
        lambda spark, path : spark.read.parquet(path),
        data_path,
        conf=confs)

parquet_write_compress_options = ['none', 'uncompressed', 'snappy']
# zstd is available in spark 3.2.0 and later.
if not is_before_spark_320():
    parquet_write_compress_options.append('zstd')

@pytest.mark.parametrize('compress', parquet_write_compress_options)
def test_compress_write_round_trip(spark_tmp_path, compress):
    data_path = spark_tmp_path + '/PARQUET_DATA'
    all_confs = {'spark.sql.parquet.compression.codec': compress}
    assert_gpu_and_cpu_writes_are_equal_collect(
            lambda spark, path : binary_op_df(spark, long_gen).coalesce(1).write.parquet(path),
            lambda spark, path : spark.read.parquet(path),
            data_path,
            conf=all_confs)

@pytest.mark.order(2)
@pytest.mark.parametrize('parquet_gens', parquet_write_gens_list, ids=idfn)
@allow_non_gpu(*non_utc_allow)
def test_write_save_table(spark_tmp_path, parquet_gens, spark_tmp_table_factory):
    gen_list = [('_c' + str(i), gen) for i, gen in enumerate(parquet_gens)]
    data_path = spark_tmp_path + '/PARQUET_DATA'
    assert_gpu_and_cpu_writes_are_equal_collect(
            lambda spark, path: gen_df(spark, gen_list).coalesce(1).write.format("parquet").mode('overwrite').option("path", path).saveAsTable(spark_tmp_table_factory.get()),
            lambda spark, path: spark.read.parquet(path),
            data_path,
            conf=writer_confs)

def write_parquet_sql_from(spark, df, data_path, write_to_table):
    tmp_view_name = 'tmp_view_{}'.format(random.randint(0, 1000000))
    df.createOrReplaceTempView(tmp_view_name)
    write_cmd = 'CREATE TABLE `{}` USING PARQUET location \'{}\' AS SELECT * from `{}`'.format(write_to_table, data_path, tmp_view_name)
    spark.sql(write_cmd)

non_utc_hive_save_table_allow = ['ExecutedCommandExec', 'DataWritingCommandExec', 'CreateDataSourceTableAsSelectCommand', 'WriteFilesExec'] if is_not_utc() else []

@pytest.mark.order(2)
@pytest.mark.parametrize('parquet_gens', parquet_write_gens_list, ids=idfn)
@allow_non_gpu(*non_utc_hive_save_table_allow)
def test_write_sql_save_table(spark_tmp_path, parquet_gens, spark_tmp_table_factory):
    gen_list = [('_c' + str(i), gen) for i, gen in enumerate(parquet_gens)]
    data_path = spark_tmp_path + '/PARQUET_DATA'
    assert_gpu_and_cpu_writes_are_equal_collect(
            lambda spark, path: write_parquet_sql_from(spark, gen_df(spark, gen_list).coalesce(1), path, spark_tmp_table_factory.get()),
            lambda spark, path: spark.read.parquet(path),
            data_path,
            conf=writer_confs)

def writeParquetUpgradeCatchException(spark, df, data_path, spark_tmp_table_factory, int96_rebase, datetime_rebase, ts_write):
    spark.conf.set('spark.sql.parquet.outputTimestampType', ts_write)
    spark.conf.set(conf_key_parquet_datetimeRebaseModeInWrite, datetime_rebase)
    spark.conf.set(conf_key_parquet_int96RebaseModeInWrite, int96_rebase) # for spark 310
    with pytest.raises(Exception) as e_info:
        df.coalesce(1).write.format("parquet").mode('overwrite').option("path", data_path).saveAsTable(spark_tmp_table_factory.get())
    assert e_info.match(r".*SparkUpgradeException.*")

@pytest.mark.parametrize('ts_write_data_gen',
                        [('INT96', TimestampGen()),
                         ('TIMESTAMP_MICROS', TimestampGen(start=datetime(1, 1, 1, tzinfo=timezone.utc), end=datetime(1899, 12, 31, tzinfo=timezone.utc))),
                         ('TIMESTAMP_MILLIS', TimestampGen(start=datetime(1, 1, 1, tzinfo=timezone.utc), end=datetime(1899, 12, 31, tzinfo=timezone.utc)))])
@pytest.mark.parametrize('rebase', ["CORRECTED","EXCEPTION"])
@allow_non_gpu(*non_utc_allow)
def test_ts_write_fails_datetime_exception(spark_tmp_path, ts_write_data_gen, spark_tmp_table_factory, rebase):
    ts_write, gen = ts_write_data_gen
    data_path = spark_tmp_path + '/PARQUET_DATA'
    int96_rebase = "EXCEPTION" if (ts_write == "INT96") else rebase
    date_time_rebase = "EXCEPTION" if (ts_write == "TIMESTAMP_MICROS" or ts_write == "TIMESTAMP_MILLIS") else rebase
    with_gpu_session(
        lambda spark : writeParquetUpgradeCatchException(spark,
                                                         unary_op_df(spark, gen), data_path,
                                                         spark_tmp_table_factory,
                                                         int96_rebase, date_time_rebase, ts_write))
    with_cpu_session(
        lambda spark: writeParquetUpgradeCatchException(spark,
                                                        unary_op_df(spark, gen), data_path,
                                                        spark_tmp_table_factory,
                                                        int96_rebase, date_time_rebase, ts_write))

def writeParquetNoOverwriteCatchException(spark, df, data_path, table_name):
    with pytest.raises(Exception) as e_info:
        df.coalesce(1).write.format("parquet").option("path", data_path).saveAsTable(table_name)
    assert e_info.match(r".*already exists.*")

def test_ts_write_twice_fails_exception(spark_tmp_path, spark_tmp_table_factory):
    gen = IntegerGen()
    data_path = spark_tmp_path + '/PARQUET_DATA'
    table_name = spark_tmp_table_factory.get()
    with_gpu_session(
            lambda spark : unary_op_df(spark, gen).coalesce(1).write.format("parquet").mode('overwrite').option("path", data_path).saveAsTable(table_name))
    with_gpu_session(
            lambda spark : writeParquetNoOverwriteCatchException(spark, unary_op_df(spark, gen), data_path, table_name))

@allow_non_gpu('DataWritingCommandExec,ExecutedCommandExec,WriteFilesExec')
@pytest.mark.parametrize('write_options', [{"parquet.encryption.footer.key": "k1"},
                                           {"parquet.encryption.column.keys": "k2:a"},
                                           {"parquet.encryption.footer.key": "k1", "parquet.encryption.column.keys": "k2:a"}])
def test_parquet_write_encryption_option_fallback(spark_tmp_path, spark_tmp_table_factory, write_options):
    def write_func(spark, path):
        writer = unary_op_df(spark, gen).coalesce(1).write
        for key in write_options:
            writer.option(key , write_options[key])
        writer.format("parquet").mode('overwrite').option("path", path).saveAsTable(spark_tmp_table_factory.get())
    gen = IntegerGen()
    data_path = spark_tmp_path + '/PARQUET_DATA'
    assert_gpu_fallback_write(
        write_func,
        lambda spark, path: spark.read.parquet(path),
        data_path,
        'DataWritingCommandExec')

@allow_non_gpu('DataWritingCommandExec,ExecutedCommandExec,WriteFilesExec')
@pytest.mark.parametrize("write_options", [{"parquet.encryption.footer.key": "k1"},
                                           {"parquet.encryption.column.keys": "k2:a"},
                                           {"parquet.encryption.footer.key": "k1", "parquet.encryption.column.keys": "k2:a"}])
def test_parquet_write_encryption_runtimeconfig_fallback(spark_tmp_path, write_options):
    gen = IntegerGen()
    data_path = spark_tmp_path + '/PARQUET_DATA'
    assert_gpu_fallback_write(
        lambda spark, path: unary_op_df(spark, gen).coalesce(1).write.parquet(path),
        lambda spark, path: spark.read.parquet(path),
        data_path,
        "DataWritingCommandExec",
        conf=write_options)

@allow_non_gpu('DataWritingCommandExec,ExecutedCommandExec,WriteFilesExec')
@pytest.mark.parametrize("write_options", [{"parquet.encryption.footer.key": "k1"},
                                           {"parquet.encryption.column.keys": "k2:a"},
                                           {"parquet.encryption.footer.key": "k1", "parquet.encryption.column.keys": "k2:a"}])
def test_parquet_write_encryption_hadoopconfig_fallback(spark_tmp_path, write_options):
    gen = IntegerGen()
    data_path = spark_tmp_path + '/PARQUET_DATA'
    def setup_hadoop_confs(spark):
        for k, v in write_options.items():
            spark.sparkContext._jsc.hadoopConfiguration().set(k, v)
    def reset_hadoop_confs(spark):
        for k in write_options.keys():
            spark.sparkContext._jsc.hadoopConfiguration().unset(k)
    try:
        with_cpu_session(setup_hadoop_confs)
        assert_gpu_fallback_write(
            lambda spark, path: unary_op_df(spark, gen).coalesce(1).write.parquet(path),
            lambda spark, path: spark.read.parquet(path),
            data_path,
            "DataWritingCommandExec")
    finally:
        with_cpu_session(reset_hadoop_confs)

@allow_non_gpu('DataWritingCommandExec,ExecutedCommandExec,WriteFilesExec')
# note that others should fail as well but requires you to load the libraries for them
# 'lzo', 'brotli', 'lz4', 'zstd' should all fallback
@pytest.mark.parametrize('codec', ['gzip'])
def test_parquet_write_compression_fallback(spark_tmp_path, codec, spark_tmp_table_factory):
    gen = IntegerGen()
    data_path = spark_tmp_path + '/PARQUET_DATA'
    all_confs={'spark.sql.parquet.compression.codec': codec}
    assert_gpu_fallback_write(
            lambda spark, path: unary_op_df(spark, gen).coalesce(1).write.format("parquet").mode('overwrite').option("path", path).saveAsTable(spark_tmp_table_factory.get()),
            lambda spark, path: spark.read.parquet(path),
            data_path,
            'DataWritingCommandExec',
            conf=all_confs)

@allow_non_gpu('DataWritingCommandExec,ExecutedCommandExec,WriteFilesExec')
def test_parquet_writeLegacyFormat_fallback(spark_tmp_path, spark_tmp_table_factory):
    gen = IntegerGen()
    data_path = spark_tmp_path + '/PARQUET_DATA'
    all_confs={'spark.sql.parquet.writeLegacyFormat': 'true'}
    assert_gpu_fallback_write(
            lambda spark, path: unary_op_df(spark, gen).coalesce(1).write.format("parquet").mode('overwrite').option("path", path).saveAsTable(spark_tmp_table_factory.get()),
            lambda spark, path: spark.read.parquet(path),
            data_path,
            'DataWritingCommandExec',
            conf=all_confs)

@ignore_order(local=True)
def test_buckets_write_round_trip(spark_tmp_path, spark_tmp_table_factory):
    data_path = spark_tmp_path + '/PARQUET_DATA'
    gen_list = [["id", int_gen], ["data", long_gen]]
    assert_gpu_and_cpu_writes_are_equal_collect(
        lambda spark, path: gen_df(spark, gen_list).selectExpr("id % 100 as b_id", "data").write
            .bucketBy(4, "b_id").format('parquet').mode('overwrite').option("path", path)
            .saveAsTable(spark_tmp_table_factory.get()),
        lambda spark, path: spark.read.parquet(path),
        data_path,
        conf=writer_confs)


def test_buckets_write_correctness(spark_tmp_path, spark_tmp_table_factory):
    cpu_path = spark_tmp_path + '/PARQUET_DATA/CPU'
    gpu_path = spark_tmp_path + '/PARQUET_DATA/GPU'
    gen_list = [["id", int_gen], ["data", long_gen]]
    num_buckets = 4

    def do_bucketing_write(spark, path):
        df = gen_df(spark, gen_list).selectExpr("id % 100 as b_id", "data")
        df.write.bucketBy(num_buckets, "b_id").format('parquet').mode('overwrite') \
            .option("path", path).saveAsTable(spark_tmp_table_factory.get())

    def read_single_bucket(path, bucket_id):
        # Bucket Id string format: f"_$id%05d" + ".c$fileCounter%03d".
        # fileCounter is always 0 in this test. For example '_00002.c000' is for
        # bucket id being 2.
        # We leverage this bucket segment in the file path to filter rows belong
        # to a bucket.
        bucket_segment = '_' + "{}".format(bucket_id).rjust(5, '0') + '.c000'
        return with_cpu_session(
            lambda spark: spark.read.parquet(path)
                .withColumn('file_name', f.input_file_name())
                .filter(f.col('file_name').contains(bucket_segment))
                .selectExpr('b_id', 'data')  # need to drop the file_name column for comparison.
                .collect())

    with_cpu_session(lambda spark: do_bucketing_write(spark, cpu_path), writer_confs)
    with_gpu_session(lambda spark: do_bucketing_write(spark, gpu_path), writer_confs)
    cur_bucket_id = 0
    while cur_bucket_id < num_buckets:
        # Verify the result bucket by bucket
        ret_cpu = read_single_bucket(cpu_path, cur_bucket_id)
        ret_gpu = read_single_bucket(gpu_path, cur_bucket_id)
        assert_equal_with_local_sort(ret_cpu, ret_gpu)
        cur_bucket_id += 1

@ignore_order(local=True)
@allow_non_gpu('DataWritingCommandExec','ExecutedCommandExec','WriteFilesExec', 'SortExec', 
  'Murmur3Hash', 'CollationAwareMurmur3Hash')
def test_buckets_write_fallback_unsupported_types(spark_tmp_path, spark_tmp_table_factory):
    data_path = spark_tmp_path + '/PARQUET_DATA'
    gen_list = [["id", binary_gen], ["data", long_gen]]
    assert_gpu_fallback_write(
        lambda spark, path: gen_df(spark, gen_list).selectExpr("id as b_id", "data").write
            .bucketBy(4, "b_id").format('parquet').mode('overwrite').option("path", path)
            .saveAsTable(spark_tmp_table_factory.get()),
        lambda spark, path: spark.read.parquet(path),
        data_path,
        'DataWritingCommandExec',
        conf=writer_confs)

@ignore_order(local=True)
def test_partitions_and_buckets_write_round_trip(spark_tmp_path, spark_tmp_table_factory):
    data_path = spark_tmp_path + '/PARQUET_DATA'
    gen_list = [["id", int_gen], ["data", long_gen]]
    assert_gpu_and_cpu_writes_are_equal_collect(
        lambda spark, path: gen_df(spark, gen_list)
            .selectExpr("id % 5 as b_id", "id % 10 as p_id", "data").write
            .partitionBy("p_id")
            .bucketBy(4, "b_id").format('parquet').mode('overwrite').option("path", path)
            .saveAsTable(spark_tmp_table_factory.get()),
        lambda spark, path: spark.read.parquet(path),
        data_path,
        conf=writer_confs)

@ignore_order
@allow_non_gpu('DataWritingCommandExec,ExecutedCommandExec,WriteFilesExec')
def test_parquet_write_bloom_filter_with_options_cpu_fallback(spark_tmp_path, spark_tmp_table_factory):
    data_path = spark_tmp_path + '/PARQUET_DATA'
    assert_gpu_fallback_write(
      lambda spark, path: spark.range(10e4).write.mode('overwrite')
                               .option("parquet.bloom.filter.enabled#id", "true")
                               .parquet(path),
      lambda spark, path: spark.read.parquet(path),
      data_path,
      'DataWritingCommandExec')


@ignore_order
@allow_non_gpu('DataWritingCommandExec,ExecutedCommandExec,WriteFilesExec')
def test_parquet_write_bloom_filter_sql_cpu_fallback(spark_tmp_path, spark_tmp_table_factory):
    data_path = spark_tmp_path + '/PARQUET_DATA'
    base_table_name = spark_tmp_table_factory.get()

    def sql_write(spark, path):
        is_gpu = path.endswith('GPU')
        table_name = base_table_name + '_GPU' if is_gpu else base_table_name + '_CPU'
        spark.sql('CREATE TABLE `{}` STORED AS PARQUET location \'{}\' '
                  'TBLPROPERTIES("parquet.bloom.filter.enabled#id"="true") '
                  'AS SELECT id from range(100)'.format(table_name, path))

    assert_gpu_fallback_write(
        sql_write,
        lambda spark, path: spark.read.parquet(path),
        data_path,
        'DataWritingCommandExec')


# This test is testing how the parquet_writer will behave if column has a validity mask without having any nulls.
# There is no straight forward to do it besides creating a vector with nulls and then dropping nulls
# cudf will create a vector with a null_mask even though we have just filtered them
def test_write_map_nullable(spark_tmp_path):
    data_path = spark_tmp_path + '/PARQUET_DATA'

    def generate_map_with_empty_validity(spark, path):
        gen_data = StructGen([['number', IntegerGen()], ['word', LongGen()]], nullable=False)
        gen_df(spark, gen_data)
        df = gen_df(spark, gen_data)
        df_noNulls = df.filter("number is not null")
        df_map = df_noNulls.withColumn("map", f.create_map(["number", "word"])).drop("number").drop("word")
        df_map.coalesce(1).write.parquet(path)

    assert_gpu_and_cpu_writes_are_equal_collect(
            generate_map_with_empty_validity,
            lambda spark, path: spark.read.parquet(path),
            data_path)

@pytest.mark.parametrize('data_gen', parquet_nested_datetime_gen, ids=idfn)
@pytest.mark.parametrize('ts_write', parquet_ts_write_options)
@pytest.mark.parametrize('ts_rebase_write', ['EXCEPTION'])
@allow_non_gpu(*non_utc_allow)
def test_parquet_write_fails_legacy_datetime(spark_tmp_path, data_gen, ts_write, ts_rebase_write):
    data_path = spark_tmp_path + '/PARQUET_DATA'
    all_confs = {'spark.sql.parquet.outputTimestampType': ts_write,
                 conf_key_parquet_datetimeRebaseModeInWrite: ts_rebase_write,
                 conf_key_parquet_int96RebaseModeInWrite: ts_rebase_write}
    def writeParquetCatchException(spark, data_gen, data_path):
        with pytest.raises(Exception) as e_info:
            unary_op_df(spark, data_gen).coalesce(1).write.parquet(data_path)
        assert e_info.match(r".*SparkUpgradeException.*")
    with_gpu_session(
        lambda spark: writeParquetCatchException(spark, data_gen, data_path),
        conf=all_confs)

@pytest.mark.parametrize('data_gen', parquet_nested_datetime_gen, ids=idfn)
@pytest.mark.parametrize('ts_write', parquet_ts_write_options)
@pytest.mark.parametrize('ts_rebase_write', [('CORRECTED', 'LEGACY'), ('LEGACY', 'CORRECTED')])
@pytest.mark.parametrize('ts_rebase_read', [('CORRECTED', 'LEGACY'), ('LEGACY', 'CORRECTED')])
@allow_non_gpu(*non_utc_allow)
def test_parquet_write_roundtrip_datetime_with_legacy_rebase(spark_tmp_path, data_gen, ts_write,
                                                             ts_rebase_write, ts_rebase_read):
    data_path = spark_tmp_path + '/PARQUET_DATA'
    all_confs = {'spark.sql.parquet.outputTimestampType': ts_write,
                 conf_key_parquet_datetimeRebaseModeInWrite: ts_rebase_write[0],
                 conf_key_parquet_int96RebaseModeInWrite: ts_rebase_write[1],
                 # The rebase modes in read configs should be ignored and overridden by the same
                 # modes in write configs, which are retrieved from the written files.
                 conf_key_parquet_datetimeRebaseModeInRead: ts_rebase_read[0],
                 conf_key_parquet_int96RebaseModeInRead: ts_rebase_read[1]}
    assert_gpu_and_cpu_writes_are_equal_collect(
        lambda spark, path: unary_op_df(spark, data_gen).coalesce(1).write.parquet(path),
        lambda spark, path: spark.read.parquet(path),
        data_path,
        conf=all_confs)

test_non_empty_ctas_non_gpu_execs = ["DataWritingCommandExec", "InsertIntoHiveTable", "WriteFilesExec"] if is_spark_340_or_later() or is_databricks122_or_later() else ["DataWritingCommandExec", "HiveTableScanExec"]

@pytest.mark.allow_non_gpu(*test_non_empty_ctas_non_gpu_execs)
@pytest.mark.parametrize('allow_non_empty', [True, False])
def test_non_empty_ctas(spark_tmp_path, spark_tmp_table_factory, allow_non_empty):
    data_path = spark_tmp_path + "/CTAS"
    conf = {
        "spark.sql.hive.convertCTAS": "true",
        "spark.sql.legacy.allowNonEmptyLocationInCTAS": str(allow_non_empty)
    }
    def test_it(spark):
        src_name = spark_tmp_table_factory.get()
        spark.sql("CREATE TABLE {}(id string) LOCATION '{}/src1'".format(src_name, data_path))
        spark.sql("INSERT INTO TABLE {} SELECT 'A'".format(src_name))
        ctas1_name = spark_tmp_table_factory.get()
        spark.sql("CREATE TABLE {}(id string) LOCATION '{}/ctas/ctas1'".format(ctas1_name, data_path))
        spark.sql("INSERT INTO TABLE {} SELECT 'A'".format(ctas1_name))
        try:
            ctas_with_existing_name = spark_tmp_table_factory.get()
            spark.sql("CREATE TABLE {} LOCATION '{}/ctas' AS SELECT * FROM {}".format(
                ctas_with_existing_name, data_path, src_name))
        except pyspark.sql.utils.AnalysisException as e:
            description = e._desc if (is_spark_400_or_later() or is_databricks_version_or_later(14, 3)) else e.desc
            if allow_non_empty or description.find('non-empty directory') == -1:
                raise e
    with_gpu_session(test_it, conf)

@pytest.mark.parametrize('parquet_gens', parquet_write_gens_list, ids=idfn)
@allow_non_gpu(*non_utc_allow)
def test_write_empty_parquet_round_trip(spark_tmp_path, parquet_gens):
    def create_empty_df(spark, path):
        gen_list = [('_c' + str(i), gen) for i, gen in enumerate(parquet_gens)]
        return gen_df(spark, gen_list, length=0).write.parquet(path)
    data_path = spark_tmp_path + '/PARQUET_DATA'
    assert_gpu_and_cpu_writes_are_equal_collect(
        create_empty_df,
        lambda spark, path: spark.read.parquet(path),
        data_path,
        conf=writer_confs)

def get_nested_parquet_meta_data_for_field_id():
    schema = StructType([
        StructField("c1", IntegerType(), metadata={'parquet.field.id': -1}),
        StructField("c2", StructType(
            [StructField("c3", IntegerType(), metadata={'parquet.field.id': -3})]),
                    metadata={'parquet.field.id': -2})
    ])
    data = [(1, (2,)), (11, (22,)), (33, (33,)), ]
    return schema, data


def test_parquet_write_field_id(spark_tmp_path):
    data_path = spark_tmp_path + '/PARQUET_DATA'
    schema, data = get_nested_parquet_meta_data_for_field_id()
    with_gpu_session(
        # default write Parquet IDs
        lambda spark: spark.createDataFrame(data, schema).coalesce(1).write.mode("overwrite")
            .parquet(data_path), conf=enable_parquet_field_id_write)

    # check data, for schema check refer to Scala test case `ParquetFieldIdSuite`
    assert_gpu_and_cpu_writes_are_equal_collect(
        lambda spark, path: spark.createDataFrame(data, schema).coalesce(1).write
            .mode("overwrite").parquet(path),
        lambda spark, path: spark.read.parquet(path),
        data_path,
        conf=enable_parquet_field_id_read)

def test_parquet_write_field_id_disabled(spark_tmp_path):
    data_path = spark_tmp_path + '/PARQUET_DATA'
    schema, data = get_nested_parquet_meta_data_for_field_id()
    with_gpu_session(
        lambda spark: spark.createDataFrame(data, schema).coalesce(1).write.mode("overwrite")
            .parquet(data_path),
        conf=disable_parquet_field_id_write)  # disable write Parquet IDs

    # check data, for schema check refer to Scala test case `ParquetFieldIdSuite`
    assert_gpu_and_cpu_writes_are_equal_collect(
        lambda spark, path: spark.createDataFrame(data, schema).coalesce(1).write
            .mode("overwrite").parquet(path),
        lambda spark, path: spark.read.parquet(path),
        data_path,
        conf=enable_parquet_field_id_read)

@pytest.mark.order(1) # at the head of xdist worker queue if pytest-order is installed
def test_write_daytime_interval(spark_tmp_path):
    gen_list = [('_c1', DayTimeIntervalGen())]
    data_path = spark_tmp_path + '/PARQUET_DATA'
    assert_gpu_and_cpu_writes_are_equal_collect(
            lambda spark, path: gen_df(spark, gen_list).coalesce(1).write.parquet(path),
            lambda spark, path: spark.read.parquet(path),
            data_path,
            conf=writer_confs)


hold_gpu_configs = [True, False]
@pytest.mark.parametrize('hold_gpu', hold_gpu_configs, ids=idfn)
def test_async_writer(spark_tmp_path, hold_gpu):
    data_path = spark_tmp_path + '/PARQUET_DATA'
    num_rows = 2048
    num_cols = 10
    parquet_gen = [int_gen for _ in range(num_cols)]
    gen_list = [('_c' + str(i), gen) for i, gen in enumerate(parquet_gen)]
    assert_gpu_and_cpu_writes_are_equal_collect(
        lambda spark, path: gen_df(spark, gen_list, length=num_rows).write.parquet(path),
        lambda spark, path: spark.read.parquet(path).orderBy([('_c' + str(i)) for i in range(num_cols)]),
        data_path,
        copy_and_update(
            writer_confs,
            {"spark.rapids.sql.asyncWrite.queryOutput.enabled": "true",
             "spark.rapids.sql.batchSizeBytes": 4 * num_cols * 100,  # 100 rows per batch
             "spark.rapids.sql.queryOutput.holdGpuInTask": hold_gpu}
        ))


@ignore_order
@pytest.mark.skipif(is_before_spark_320(), reason="is only supported in Spark 320+")
def test_concurrent_writer(spark_tmp_path):
    data_path = spark_tmp_path + '/PARQUET_DATA'
    assert_gpu_and_cpu_writes_are_equal_collect(
        lambda spark, path: get_25_partitions_df(spark)  # df has 25 partitions for (c1, c2)
            .repartition(2)
            .write.mode("overwrite").partitionBy('c1', 'c2').parquet(path),
        lambda spark, path: spark.read.parquet(path),
        data_path,
        copy_and_update(
            # 26 > 25, will not fall back to single writer
            # Disable AQE temporarily until https://github.com/NVIDIA/spark-rapids/issues/14319 is resolved.
            {"spark.sql.maxConcurrentOutputFileWriters": 26,
             'spark.sql.adaptive.enabled': 'false'}
        ))


@ignore_order
@pytest.mark.skipif(is_before_spark_320(), reason="is only supported in Spark 320+")
@allow_non_gpu(any=True)
@pytest.mark.parametrize('aqe_enabled', [True, False])
def test_fallback_to_single_writer_from_concurrent_writer(spark_tmp_path, aqe_enabled):
    data_path = spark_tmp_path + '/PARQUET_DATA'
    assert_gpu_and_cpu_writes_are_equal_collect(
        lambda spark, path: get_25_partitions_df(spark)  # df has 25 partitions for (c1, c2)
            .repartition(2)
            .write.mode("overwrite").partitionBy('c1', 'c2').parquet(path),
        lambda spark, path: spark.read.parquet(path),
        data_path,
        copy_and_update(
            # 10 < 25, will fall back to single writer
            {"spark.sql.maxConcurrentOutputFileWriters": 10},
            {"spark.rapids.sql.concurrentWriterPartitionFlushSize": 64 * 1024 * 1024},
            {"spark.sql.adaptive.enabled": aqe_enabled},
        ))


@pytest.mark.skipif(True, reason="currently not support write emtpy data: https://github.com/NVIDIA/spark-rapids/issues/6453")
def test_write_empty_data_concurrent_writer(spark_tmp_path):
    schema = StructType(
        [StructField("c1", StringType()), StructField("c2", IntegerType()), StructField("c3", IntegerType())])
    data = []  # empty data
    data_path = spark_tmp_path + '/PARQUET_DATA'
    with_gpu_session(lambda spark: spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
                     .write.mode("overwrite").partitionBy('c1', 'c2').parquet(data_path),
                     # concurrent writer
                     {"spark.sql.maxConcurrentOutputFileWriters": 10})
    with_cpu_session(lambda spark: spark.read.parquet(data_path).collect())


@pytest.mark.skipif(True, reason="currently not support write emtpy data: https://github.com/NVIDIA/spark-rapids/issues/6453")
def test_write_empty_data_single_writer(spark_tmp_path):
    schema = StructType(
        [StructField("c1", StringType()), StructField("c2", IntegerType()), StructField("c3", IntegerType())])
    data = []  # empty data
    data_path = spark_tmp_path + '/PARQUET_DATA'
    with_gpu_session(lambda spark: spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
                     .write.mode("overwrite").partitionBy('c1', 'c2').parquet(data_path))
    with_cpu_session(lambda spark: spark.read.parquet(data_path).collect())


PartitionWriteMode = Enum('PartitionWriteMode', ['Static', 'Dynamic'])


@pytest.mark.skipif(is_databricks_runtime() or is_spark_cdh(),
                    reason="On Databricks and CDH, Hive partitioned SQL writes are routed through InsertIntoHiveTable; "
                           "GpuInsertIntoHiveTable does not support Parquet writes.")
@ignore_order(local=True)
@pytest.mark.parametrize('mode', [PartitionWriteMode.Static, PartitionWriteMode.Dynamic])
def test_partitioned_sql_parquet_write(mode, spark_tmp_table_factory):

    def create_input_table(spark):
        tmp_input = spark_tmp_table_factory.get()
        spark.sql("CREATE TABLE " + tmp_input +
                  " (make STRING, model STRING, year INT, type STRING, comment STRING)" +
                  " STORED AS PARQUET")
        spark.sql("INSERT INTO TABLE " + tmp_input + " VALUES " +
                  "('Ford',   'F-150',       2020, 'ICE',      'Popular' ),"
                  "('GMC',    'Sierra 1500', 1997, 'ICE',      'Older'),"
                  "('Chevy',  'D-Max',       2015, 'ICE',      'Isuzu?' ),"
                  "('Tesla',  'CyberTruck',  2025, 'Electric', 'BladeRunner'),"
                  "('Rivian', 'R1T',         2022, 'Electric', 'Heavy'),"
                  "('Jeep',   'Gladiator',   2024, 'Hybrid',   'Upcoming')")
        return tmp_input

    input_table_name = with_cpu_session(create_input_table)

    def write_partitions(spark, table_name):
        if mode == PartitionWriteMode.Static:
            return [
                "CREATE TABLE {} (make STRING, model STRING, year INT, comment STRING) "
                "PARTITIONED BY (type STRING) STORED AS PARQUET ".format(table_name),

                "INSERT INTO TABLE {} PARTITION (type='ICE') "
                "SELECT make, model, year, comment FROM {} "
                "WHERE type = 'ICE'".format(table_name, input_table_name),

                "INSERT OVERWRITE TABLE {} PARTITION (type='Electric') "
                "SELECT make, model, year, comment FROM {} "
                "WHERE type = 'ICE'".format(table_name, input_table_name),

                "INSERT OVERWRITE TABLE {} PARTITION (type='Hybrid') "
                "SELECT make, model, year, comment FROM {} "
                "WHERE type = 'ICE'".format(table_name, input_table_name)
            ]
        elif mode == PartitionWriteMode.Dynamic:
            return [
                "CREATE TABLE {} (make STRING, model STRING, year INT, comment STRING) "
                "PARTITIONED BY (type STRING) STORED AS PARQUET ".format(table_name),

                "INSERT OVERWRITE TABLE {} "
                "SELECT * FROM {} ".format(table_name, input_table_name)
            ]
        else:
            raise Exception("Unsupported PartitionWriteMode {}".format(mode))

    assert_gpu_and_cpu_sql_writes_are_equal_collect(
        spark_tmp_table_factory, write_partitions,
        conf={"hive.exec.dynamic.partition.mode": "nonstrict"}
    )


@ignore_order(local=True)
def test_dynamic_partitioned_parquet_write(spark_tmp_table_factory, spark_tmp_path):

    def create_input_table(spark):
        tmp_input = spark_tmp_table_factory.get()
        spark.sql("CREATE TABLE " + tmp_input +
                  " (make STRING, model STRING, year INT, type STRING, comment STRING)" +
                  " STORED AS PARQUET")
        spark.sql("INSERT INTO TABLE " + tmp_input + " VALUES " +
                  "('Ford',   'F-150',       2020, 'ICE',      'Popular' ),"
                  "('GMC',    'Sierra 1500', 1997, 'ICE',      'Older'),"
                  "('Chevy',  'D-Max',       2015, 'ICE',      'Isuzu?' ),"
                  "('Tesla',  'CyberTruck',  2025, 'Electric', 'BladeRunner'),"
                  "('Rivian', 'R1T',         2022, 'Electric', 'Heavy'),"
                  "('Jeep',   'Gladiator',   2024, 'Hybrid',   'Upcoming')")
        return tmp_input

    input_table_name = with_cpu_session(create_input_table)
    base_output_path = spark_tmp_path + "/PARQUET_DYN_WRITE"

    def write_partitions(spark, table_path):
        input_df = spark.sql("SELECT * FROM {}".format(input_table_name))
        input_df.write.mode("overwrite").partitionBy("type").parquet(table_path)
        # Second write triggers the actual overwrite.
        input_df.write.mode("overwrite").partitionBy("type").parquet(table_path)

    assert_gpu_and_cpu_writes_are_equal_collect(
        lambda spark, path: write_partitions(spark, path),
        lambda spark, path: spark.read.parquet(path),
        base_output_path,
        conf={}
    )

def hive_timestamp_value(spark_tmp_table_factory, spark_tmp_path, ts_rebase, func):
    conf={conf_key_parquet_datetimeRebaseModeInWrite: ts_rebase,
          conf_key_parquet_int96RebaseModeInWrite: ts_rebase}

    def create_table(spark, path):
        tmp_table = spark_tmp_table_factory.get()
        spark.sql(f"CREATE TABLE {tmp_table} STORED AS PARQUET " +
                  f""" LOCATION '{path}' AS SELECT CAST('2015-01-01 00:00:00' AS TIMESTAMP) as t; """)

    def read_table(spark, path):
        return spark.read.parquet(path)

    data_path = spark_tmp_path + '/PARQUET_DATA'

    func(create_table, read_table, data_path, conf)

non_utc_hive_parquet_write_allow = ['DataWritingCommandExec', 'WriteFilesExec'] if is_not_utc() else []

# Test to avoid regression on a known bug in Spark. For details please visit https://github.com/NVIDIA/spark-rapids/issues/8693
@pytest.mark.parametrize('ts_rebase', [
    pytest.param('LEGACY', marks=pytest.mark.skipif(is_not_utc(), reason="LEGACY datetime rebase mode is only supported for UTC timezone")),
    pytest.param('CORRECTED', marks=pytest.mark.allow_non_gpu(*non_utc_hive_parquet_write_allow))])
def test_hive_timestamp_value(spark_tmp_table_factory, spark_tmp_path, ts_rebase):
    def func_test(create_table, read_table, data_path, conf):
        assert_gpu_and_cpu_writes_are_equal_collect(create_table, read_table, data_path, conf=conf)
        assert_gpu_and_cpu_are_equal_collect(lambda spark: spark.read.parquet(data_path + '/CPU'))
    hive_timestamp_value(spark_tmp_table_factory, spark_tmp_path, ts_rebase, func_test)

@ignore_order
@pytest.mark.skipif(is_before_spark_340(), reason="`spark.sql.optimizer.plannedWrite.enabled` is only supported in Spark 340+")
# empty string will not set the `planned_write_enabled` option
@pytest.mark.parametrize('planned_write_enabled', ["", "true", "false"])
# df to be written has 25 partitions
#   0 will not set the concurrent writers option
#   100 > 25 will always use concurrent writer without fallback
#   20 <25 will fall back to single writer from concurrent writer
@pytest.mark.parametrize('max_concurrent_writers', [0, 100, 20])
def test_write_with_planned_write_enabled(spark_tmp_path, planned_write_enabled, max_concurrent_writers):
    data_path = spark_tmp_path + '/PARQUET_DATA'
    # Disable AQE temporarily until https://github.com/NVIDIA/spark-rapids/issues/14319 is resolved.
    conf = {'spark.sql.adaptive.enabled': 'false'}
    if planned_write_enabled != "":
        conf = copy_and_update(conf, {"spark.sql.optimizer.plannedWrite.enabled": planned_write_enabled})
    if max_concurrent_writers != 0:
        conf = copy_and_update(conf, {"spark.sql.maxConcurrentOutputFileWriters": max_concurrent_writers})

    assert_gpu_and_cpu_writes_are_equal_collect(
        lambda spark, path: get_25_partitions_df(spark)  # df has 25 partitions for (c1, c2)
            .repartition(2)
            .write.mode("overwrite").partitionBy('c1', 'c2').parquet(path),
        lambda spark, path: spark.read.parquet(path),
        data_path,
        conf)

# Issue to test a known bug https://github.com/NVIDIA/spark-rapids/issues/8694 to avoid regression
@ignore_order
@allow_non_gpu("SortExec", "ShuffleExchangeExec")
def test_write_list_struct_single_element(spark_tmp_path):
    data_path = spark_tmp_path + '/PARQUET_DATA'
    data_gen = ArrayGen(StructGen([('element', long_gen)], nullable=False), max_length=10, nullable=False)
    conf = {}
    assert_gpu_and_cpu_writes_are_equal_collect(
        lambda spark, path: gen_df(spark, data_gen).write.parquet(path),
        lambda spark, path: spark.read.parquet(path), data_path, conf)
    cpu_path = data_path + '/CPU'
    assert_gpu_and_cpu_are_equal_collect(lambda spark: spark.read.parquet(cpu_path), conf)

@ignore_order
def test_parquet_write_column_name_with_dots(spark_tmp_path):
    data_path = spark_tmp_path + "/PARQUET_DATA"
    gens = [
        ("a.b", StructGen([
            ("c.d.e", StructGen([
                ("f.g", int_gen),
                ("h", string_gen)])),
            ("i.j", long_gen)])),
        ("k", boolean_gen)]
    assert_gpu_and_cpu_writes_are_equal_collect(
        lambda spark, path:  gen_df(spark, gens).coalesce(1).write.parquet(path),
        lambda spark, path: spark.read.parquet(path),
        data_path)

@ignore_order
def test_parquet_append_with_downcast(spark_tmp_table_factory, spark_tmp_path):
    data_path = spark_tmp_path + "/PARQUET_DATA"
    cpu_table = spark_tmp_table_factory.get()
    gpu_table = spark_tmp_table_factory.get()
    def setup_tables(spark):
        df = unary_op_df(spark, int_gen, length=10)
        df.write.format("parquet").option("path", data_path + "/CPU").saveAsTable(cpu_table)
        df.write.format("parquet").option("path", data_path + "/GPU").saveAsTable(gpu_table)
    with_cpu_session(setup_tables)
    def do_append(spark, path):
        table = cpu_table
        if path.endswith("/GPU"):
            table = gpu_table
        unary_op_df(spark, LongGen(min_val=0, max_val=128, special_cases=[]), length=10)\
            .write.mode("append").saveAsTable(table)
    assert_gpu_and_cpu_writes_are_equal_collect(
        do_append,
        lambda spark, path: spark.read.parquet(path),
        data_path)
