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
import glob
import pyarrow as pa
import pyarrow.orc as orc

from asserts import assert_gpu_and_cpu_writes_are_equal_collect, assert_gpu_fallback_write
from spark_session import is_before_spark_320, is_databricks_version_or_later, \
    is_spark_321cdh, is_spark_400_or_later, is_spark_420_or_later, is_spark_cdh, \
    with_cpu_session, with_gpu_session
from conftest import is_apache_runtime, is_databricks_runtime, is_not_utc
from datetime import date, datetime, timezone
from data_gen import *
from marks import *
from pyspark.sql.functions import col, lit
from pyspark.sql.types import *

pytestmark = pytest.mark.nightly_resource_consuming_test
# Use every type except boolean, see https://github.com/NVIDIA/spark-rapids/issues/11762 and
# https://github.com/rapidsai/cudf/issues/6763 .
# Once the first issue is fixed, add back boolean_gen.
orc_write_basic_gens = [byte_gen, short_gen, int_gen, long_gen, float_gen, double_gen,
        string_gen, DateGen(start=date(1590, 1, 1)),
        TimestampGen(start=datetime(1970, 1, 1, tzinfo=timezone.utc)) ] + \
        decimal_gens

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

orc_write_odd_empty_strings_gens_sample = [all_nulls_string_gen, 
        empty_or_null_string_gen, 
        all_empty_string_gen,
        all_nulls_array_gen,
        all_empty_array_gen,
        all_array_empty_string_gen,
        mixed_empty_nulls_array_gen, 
        mixed_empty_nulls_map_gen,
        all_nulls_map_gen,
        all_empty_map_gen]

orc_write_basic_struct_gen = StructGen(
    [['child'+str(ind), sub_gen] for ind, sub_gen in enumerate(orc_write_basic_gens)])

orc_write_struct_gens_sample = [orc_write_basic_struct_gen,
    StructGen([['child0', byte_gen], ['child1', orc_write_basic_struct_gen]]),
    StructGen([['child0', ArrayGen(short_gen)], ['child1', double_gen]])]

orc_write_array_gens_sample = [ArrayGen(sub_gen) for sub_gen in orc_write_basic_gens] + [
    ArrayGen(ArrayGen(short_gen, max_length=10), max_length=10),
    ArrayGen(ArrayGen(string_gen, max_length=10), max_length=10),
    ArrayGen(StructGen([['child0', byte_gen], ['child1', string_gen], ['child2', float_gen]]))]
# Use every type except boolean, see https://github.com/NVIDIA/spark-rapids/issues/11762 and
# https://github.com/rapidsai/cudf/issues/6763 .
# Once the first issue is fixed, add back boolean_gen.
orc_write_basic_map_gens = [simple_string_to_string_map_gen] + [MapGen(f(nullable=False), f()) for f in [
    ByteGen, ShortGen, IntegerGen, LongGen, FloatGen, DoubleGen,
    # Using timestamps from 1970 to work around a cudf ORC bug
    # https://github.com/NVIDIA/spark-rapids/issues/140.
    lambda nullable=True: TimestampGen(start=datetime(1970, 1, 1, tzinfo=timezone.utc), nullable=nullable),
    lambda nullable=True: DateGen(start=date(1590, 1, 1), nullable=nullable),
    lambda nullable=True: DecimalGen(precision=15, scale=1, nullable=nullable),
    lambda nullable=True: DecimalGen(precision=36, scale=5, nullable=nullable)]] + [MapGen(
    f(nullable=False), f(nullable=False)) for f in [IntegerGen]]

orc_write_gens_list = [orc_write_basic_gens,
        orc_write_struct_gens_sample,
        orc_write_array_gens_sample,
        orc_write_basic_map_gens,
        pytest.param([date_gen], marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/139')),
        pytest.param([timestamp_gen], marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/140'))]

bool_gen = [BooleanGen(nullable=True), BooleanGen(nullable=False)]
@pytest.mark.parametrize('orc_gens', orc_write_gens_list, ids=idfn)
@pytest.mark.parametrize('orc_impl', ["native", "hive"])
@allow_non_gpu(*non_utc_allow)
def test_write_round_trip(spark_tmp_path, orc_gens, orc_impl):
    gen_list = [('_c' + str(i), gen) for i, gen in enumerate(orc_gens)]
    data_path = spark_tmp_path + '/ORC_DATA'
    assert_gpu_and_cpu_writes_are_equal_collect(
            lambda spark, path: gen_df(spark, gen_list).coalesce(1).write.orc(path),
            lambda spark, path: spark.read.orc(path),
            data_path,
            conf={'spark.sql.orc.impl': orc_impl, 'spark.rapids.sql.format.orc.write.enabled': True})

# Only runs on Apache and Databricks, as PyArrow requires files to be stored on the local filesystem.
@pytest.mark.parametrize('orc_gen', [int_gen], ids=idfn)
@pytest.mark.parametrize('orc_impl', ["native", "hive"])
@pytest.mark.skipif(not is_apache_runtime() and not is_databricks_runtime(), reason="Only runs on Apache and Databricks")
def test_write_with_stripe_size_rows(spark_tmp_path, orc_gen, orc_impl):
    gen_list = [('_c0', orc_gen)]
    data_path = spark_tmp_path + '/ORC_DATA'
    # cuDF ORC writer rounds the number of elements in each stripe (except the last) to a multiple
    # of 8 to be able to efficiently encode the validity masks. So use an integer divisible by 8.
    stripe_size_rows = 10000
    with_gpu_session(
        lambda spark: gen_df(spark, gen_list, stripe_size_rows + 1, num_slices=1).write.orc(data_path),
        conf={'spark.sql.orc.impl': orc_impl, 'spark.rapids.sql.format.orc.write.enabled': True,
              'spark.rapids.sql.test.orc.write.stripeSizeRows': stripe_size_rows})
    files = glob.glob(f"{data_path}/*.orc")
    assert len(files) == 1, f"Expecting 1 ORC file, but found {len(files)} files"
    # Verify the number of stripes in the written ORC file
    with pa.OSFile(files[0], 'rb') as f:
        orc_file = orc.ORCFile(f)
        assert orc_file.nstripes == 2, f"Expecting 2 stripes in the ORC file, but found {orc_file.nstripes} stripes"

@pytest.mark.parametrize('orc_gen', [
    BooleanGen(nullable=False),
    pytest.param(BooleanGen(nullable=True), marks=pytest.mark.xfail(reason='https://github.com/rapidsai/cudf/issues/6763'))], ids=idfn)
@pytest.mark.parametrize('orc_impl', ["native", "hive"])
@allow_non_gpu(*non_utc_allow)
def test_write_round_trip_boolean_two_row_groups(spark_tmp_path, orc_gen, orc_impl):
    gen_list = [('_c0', orc_gen)]
    data_path = spark_tmp_path + '/ORC_DATA'
    # Default row index stride (maximum number of rows in each row group) defined in cuDF
    default_row_index_stride = 10000
    assert_gpu_and_cpu_writes_are_equal_collect(
            # Use only one partition to avoid splitting the data
            lambda spark, path: gen_df(spark, gen_list, default_row_index_stride + 1, num_slices=1).write.orc(path),
            lambda spark, path: spark.read.orc(path),
            data_path,
            conf={'spark.sql.orc.impl': orc_impl, 'spark.rapids.sql.format.orc.write.enabled': True,
                  'spark.rapids.sql.format.orc.write.boolType.enabled': True})

@pytest.mark.parametrize('orc_gens', orc_write_gens_list, ids=idfn)
@pytest.mark.parametrize('orc_impl', ["native", "hive"])
@allow_non_gpu(*non_utc_allow)
def test_write_round_trip_two_stripes(spark_tmp_path, orc_gens, orc_impl):
    gen_list = [('_c' + str(i), gen) for i, gen in enumerate(orc_gens)]
    data_path = spark_tmp_path + '/ORC_DATA'
    # The minimum `orc_stripe_size_rows` that can be set is 512.
    # See the documentation for the config `spark.rapids.sql.test.orc.write.stripeSizeRows`.
    stripe_size_rows = 512
    assert_gpu_and_cpu_writes_are_equal_collect(
            # Use only one partition to avoid splitting the data
            lambda spark, path: gen_df(spark, gen_list, stripe_size_rows + 1, num_slices=1).write.orc(path),
            lambda spark, path: spark.read.orc(path),
            data_path,
            conf={'spark.sql.orc.impl': orc_impl, 'spark.rapids.sql.format.orc.write.enabled': True,
                  'spark.rapids.sql.test.orc.write.stripeSizeRows': stripe_size_rows})

@pytest.mark.parametrize('orc_gens', [bool_gen], ids=idfn)
@pytest.mark.parametrize('orc_impl', ["native", "hive"])
@allow_non_gpu('ExecutedCommandExec', 'DataWritingCommandExec', 'WriteFilesExec')
def test_write_round_trip_bools_only_fallback(spark_tmp_path, orc_gens, orc_impl):
    gen_list = [('_c' + str(i), gen) for i, gen in enumerate(orc_gens)]
    data_path = spark_tmp_path + '/ORC_DATA'
    assert_gpu_and_cpu_writes_are_equal_collect(
        lambda spark, path: gen_df(spark, gen_list).coalesce(1).write.orc(path),
        lambda spark, path: spark.read.orc(path),
        data_path,
        conf={'spark.sql.orc.impl': orc_impl, 'spark.rapids.sql.format.orc.write.enabled': True})

@pytest.mark.parametrize('orc_gens', [bool_gen], ids=idfn)
@pytest.mark.parametrize('orc_impl', ["native", "hive"])
def test_write_round_trip_bools_only_no_fallback(spark_tmp_path, orc_gens, orc_impl):
    gen_list = [('_c' + str(i), gen) for i, gen in enumerate(orc_gens)]
    data_path = spark_tmp_path + '/ORC_DATA'
    assert_gpu_and_cpu_writes_are_equal_collect(
        lambda spark, path: gen_df(spark, gen_list).coalesce(1).write.orc(path),
        lambda spark, path: spark.read.orc(path),
        data_path,
        conf={'spark.sql.orc.impl': orc_impl, 'spark.rapids.sql.format.orc.write.enabled': True,
              'spark.rapids.sql.format.orc.write.boolType.enabled': True})

@pytest.mark.parametrize('orc_gen', orc_write_odd_empty_strings_gens_sample, ids=idfn)
@pytest.mark.parametrize('orc_impl', ["native", "hive"])
def test_write_round_trip_corner(spark_tmp_path, orc_gen, orc_impl):
    gen_list = [('_c0', orc_gen)]
    data_path = spark_tmp_path + '/ORC_DATA'
    assert_gpu_and_cpu_writes_are_equal_collect(
            lambda spark, path: gen_df(spark, gen_list, 128000, num_slices=1).write.orc(path),
            lambda spark, path: spark.read.orc(path),
            data_path,
            conf={'spark.sql.orc.impl': orc_impl, 'spark.rapids.sql.format.orc.write.enabled': True})

@pytest.mark.parametrize('gen', [ByteGen(nullable=False),
    ShortGen(nullable=False),
    IntegerGen(nullable=False),
    LongGen(nullable=False),
    FloatGen(nullable=False),
    DoubleGen(nullable=False),
    BooleanGen(nullable=False),
    StringGen(nullable=False),
    StructGen([('b', LongGen(nullable=False))], nullable=False)], ids=idfn)
@pytest.mark.parametrize('orc_impl', ["native", "hive"])
@allow_non_gpu(*non_utc_allow)
def test_write_round_trip_nullable_struct(spark_tmp_path, gen, orc_impl):
    gen_for_struct = StructGen([('c', gen)], nullable=True)
    data_path = spark_tmp_path + '/ORC_DATA'
    assert_gpu_and_cpu_writes_are_equal_collect(
            lambda spark, path: unary_op_df(spark, gen_for_struct, num_slices=1).write.orc(path),
            lambda spark, path: spark.read.orc(path),
            data_path,
            conf={'spark.sql.orc.impl': orc_impl,
                'spark.rapids.sql.format.orc.write.enabled': True,
                # https://github.com/NVIDIA/spark-rapids/issues/11736, so verify that we still do it correctly
                # once this is fixed
                'spark.rapids.sql.format.orc.write.boolType.enabled' : True})

orc_part_write_gens = [
        # Add back boolean_gen when  https://github.com/rapidsai/cudf/issues/6763 is fixed
        byte_gen, short_gen, int_gen, long_gen,
        # Some file systems have issues with UTF8 strings so to help the test pass even there
        StringGen('(\\w| ){0,50}'),
        # Once https://github.com/NVIDIA/spark-rapids/issues/139 is fixed replace this with
        # date_gen
        DateGen(start=date(1590, 1, 1)),
        # Once https://github.com/NVIDIA/spark-rapids/issues/140 is fixed replace this with
        # timestamp_gen 
        TimestampGen(start=datetime(1970, 1, 1, tzinfo=timezone.utc))]

# There are race conditions around when individual files are read in for partitioned data
@ignore_order
@pytest.mark.parametrize('orc_gen', orc_part_write_gens, ids=idfn)
@allow_non_gpu(*non_utc_allow)
def test_part_write_round_trip(spark_tmp_path, orc_gen):
    gen_list = [('a', RepeatSeqGen(orc_gen, 10)),
                ('b', orc_gen)]
    data_path = spark_tmp_path + '/ORC_DATA'
    assert_gpu_and_cpu_writes_are_equal_collect(
            lambda spark, path: gen_df(spark, gen_list).coalesce(1).write.partitionBy('a').orc(path),
            lambda spark, path: spark.read.orc(path),
            data_path,
            conf = {'spark.rapids.sql.format.orc.write.enabled': True})


@ignore_order(local=True)
@pytest.mark.parametrize('orc_gen', [int_gen], ids=idfn)
@pytest.mark.parametrize('orc_impl', ["native", "hive"])
@pytest.mark.skipif(is_spark_321cdh(), reason="3.2.1 CDH not support partitionOverwriteMode=DYNAMIC")
def test_dynamic_partition_write_round_trip(spark_tmp_path, orc_gen, orc_impl):
    gen_list = [('_c0', orc_gen)]
    data_path = spark_tmp_path + '/ORC_DATA'
    def do_writes(spark, path):
        df = gen_df(spark, gen_list).withColumn("my_partition", lit("PART"))
        # first write finds no partitions, it skips the dynamic partition
        # overwrite code
        df.write.mode("overwrite").partitionBy("my_partition").orc(path)
        # second write actually triggers dynamic partition overwrite
        df.write.mode("overwrite").partitionBy("my_partition").orc(path)
    assert_gpu_and_cpu_writes_are_equal_collect(
            lambda spark, path: do_writes(spark, path),
            lambda spark, path: spark.read.orc(path),
            data_path,
            conf={
                'spark.sql.orc.impl': orc_impl,
                'spark.rapids.sql.format.orc.write.enabled': True,
                'spark.sql.sources.partitionOverwriteMode': 'DYNAMIC'
            })


orc_write_compress_options = ['none', 'uncompressed', 'snappy']
# zstd is available in spark 3.2.0 and later.
if not is_before_spark_320() and not is_spark_cdh():
    orc_write_compress_options.append('zstd')

@pytest.mark.parametrize('compress', orc_write_compress_options)
def test_compress_write_round_trip(spark_tmp_path, compress):
    data_path = spark_tmp_path + '/ORC_DATA'
    assert_gpu_and_cpu_writes_are_equal_collect(
            lambda spark, path : binary_op_df(spark, long_gen).coalesce(1).write.orc(path),
            lambda spark, path : spark.read.orc(path),
            data_path,
            conf={'spark.sql.orc.compression.codec': compress, 'spark.rapids.sql.format.orc.write.enabled': True})

@pytest.mark.order(2)
@pytest.mark.parametrize('orc_gens', orc_write_gens_list, ids=idfn)
@pytest.mark.parametrize('orc_impl', ["native", "hive"])
@allow_non_gpu(*non_utc_allow)
def test_write_save_table_orc(spark_tmp_path, orc_gens, orc_impl, spark_tmp_table_factory):
    gen_list = [('_c' + str(i), gen) for i, gen in enumerate(orc_gens)]
    data_path = spark_tmp_path + '/ORC_DATA'
    all_confs={'spark.sql.sources.useV1SourceList': "orc",
               'spark.rapids.sql.format.orc.write.enabled': True,
               "spark.sql.orc.impl": orc_impl}
    assert_gpu_and_cpu_writes_are_equal_collect(
            lambda spark, path: gen_df(spark, gen_list).coalesce(1).write.format("orc").mode('overwrite').option("path", path).saveAsTable(spark_tmp_table_factory.get()),
            lambda spark, path: spark.read.orc(path),
            data_path,
            conf=all_confs)

def write_orc_sql_from(spark, df, data_path, write_to_table):
    tmp_view_name = 'tmp_view_{}'.format(random.randint(0, 1000000))
    df.createOrReplaceTempView(tmp_view_name)
    write_cmd = 'CREATE TABLE `{}` USING ORC location \'{}\' AS SELECT * from `{}`'.format(write_to_table, data_path, tmp_view_name)
    spark.sql(write_cmd)

non_utc_hive_save_table_allow = ['ExecutedCommandExec', 'DataWritingCommandExec', 'CreateDataSourceTableAsSelectCommand', 'WriteFilesExec'] if is_not_utc() else []

@pytest.mark.order(2)
@pytest.mark.parametrize('orc_gens', orc_write_gens_list, ids=idfn)
@pytest.mark.parametrize('ts_type', ["TIMESTAMP_MICROS", "TIMESTAMP_MILLIS"])
@pytest.mark.parametrize('orc_impl', ["native", "hive"])
@allow_non_gpu(*non_utc_hive_save_table_allow)
def test_write_sql_save_table(spark_tmp_path, orc_gens, ts_type, orc_impl, spark_tmp_table_factory):
    gen_list = [('_c' + str(i), gen) for i, gen in enumerate(orc_gens)]
    data_path = spark_tmp_path + '/ORC_DATA'
    assert_gpu_and_cpu_writes_are_equal_collect(
            lambda spark, path: write_orc_sql_from(spark, gen_df(spark, gen_list).coalesce(1), path, spark_tmp_table_factory.get()),
            lambda spark, path: spark.read.orc(path),
            data_path,
            conf={'spark.sql.orc.impl': orc_impl, 'spark.rapids.sql.format.orc.write.enabled': True})

@allow_non_gpu('DataWritingCommandExec,ExecutedCommandExec,WriteFilesExec', *non_utc_allow)
@pytest.mark.parametrize('codec', ['lzo'])
def test_orc_write_compression_fallback(spark_tmp_path, codec, spark_tmp_table_factory):
    gen = TimestampGen()
    data_path = spark_tmp_path + '/ORC_DATA'
    all_confs={'spark.sql.orc.compression.codec': codec, 'spark.rapids.sql.format.orc.write.enabled': True}
    assert_gpu_fallback_write(
            lambda spark, path: unary_op_df(spark, gen).coalesce(1).write.format("orc").mode('overwrite').option("path", path).saveAsTable(spark_tmp_table_factory.get()),
            lambda spark, path: spark.read.orc(path),
            data_path,
            'DataWritingCommandExec',
            conf=all_confs)

@pytest.mark.parametrize('codec', ['zlib'])
def test_orc_write_compression_zlib(spark_tmp_path, codec, spark_tmp_table_factory):
    gen = IntegerGen()
    data_path = spark_tmp_path + '/ORC_DATA'
    all_confs={'spark.sql.orc.compression.codec': codec, 'spark.rapids.sql.format.orc.write.enabled': True}
    assert_gpu_and_cpu_writes_are_equal_collect(
        lambda spark, path: unary_op_df(spark, gen).coalesce(1).write.format("orc").mode('overwrite').option("path", path).saveAsTable(spark_tmp_table_factory.get()),
        lambda spark, path: spark.read.orc(path),
        data_path,
        conf=all_confs)

@ignore_order(local=True)
def test_buckets_write_round_trip(spark_tmp_path, spark_tmp_table_factory):
    data_path = spark_tmp_path + '/ORC_DATA'
    gen_list = [["id", int_gen], ["data", long_gen]]
    assert_gpu_and_cpu_writes_are_equal_collect(
        lambda spark, path: gen_df(spark, gen_list).selectExpr("id % 100 as b_id", "data").write
            .bucketBy(4, "b_id").format('orc').mode('overwrite').option("path", path)
            .saveAsTable(spark_tmp_table_factory.get()),
        lambda spark, path: spark.read.orc(path),
        data_path,
        conf={'spark.rapids.sql.format.orc.write.enabled': True})

@ignore_order(local=True)
@allow_non_gpu('DataWritingCommandExec','ExecutedCommandExec','WriteFilesExec', 'SortExec', 
  'Murmur3Hash', 'CollationAwareMurmur3Hash')
def test_buckets_write_fallback_unsupported_types(spark_tmp_path, spark_tmp_table_factory):
    data_path = spark_tmp_path + '/ORC_DATA'
    gen_list = [["id", binary_gen], ["data", long_gen]]
    assert_gpu_fallback_write(
        lambda spark, path: gen_df(spark, gen_list).selectExpr("id as b_id", "data").write
            .bucketBy(4, "b_id").format('orc').mode('overwrite').option("path", path)
            .saveAsTable(spark_tmp_table_factory.get()),
        lambda spark, path: spark.read.orc(path),
        data_path,
        'DataWritingCommandExec',
        conf={'spark.rapids.sql.format.orc.write.enabled': True})

@ignore_order(local=True)
def test_partitions_and_buckets_write_round_trip(spark_tmp_path, spark_tmp_table_factory):
    data_path = spark_tmp_path + '/ORC_DATA'
    gen_list = [["id", int_gen], ["data", long_gen]]
    assert_gpu_and_cpu_writes_are_equal_collect(
        lambda spark, path: gen_df(spark, gen_list)
            .selectExpr("id % 5 as b_id", "id % 10 as p_id", "data").write
            .partitionBy("p_id")
            .bucketBy(4, "b_id").format('orc').mode('overwrite').option("path", path)
        .saveAsTable(spark_tmp_table_factory.get()),
        lambda spark, path: spark.read.orc(path),
        data_path,
        conf={'spark.rapids.sql.format.orc.write.enabled': True})

@ignore_order
@allow_non_gpu('DataWritingCommandExec,ExecutedCommandExec,WriteFilesExec')
def test_orc_write_bloom_filter_with_options_cpu_fallback(spark_tmp_path, spark_tmp_table_factory):
    data_path = spark_tmp_path + '/ORC_DATA'
    assert_gpu_fallback_write(
      lambda spark, path: spark.range(10e4).write.mode('overwrite').option("orc.bloom.filter.columns", "id").orc(path),
      lambda spark, path: spark.read.orc(path),
      data_path,
      'DataWritingCommandExec',
      conf={'spark.rapids.sql.format.orc.write.enabled': True})


@ignore_order
@allow_non_gpu('DataWritingCommandExec,ExecutedCommandExec,WriteFilesExec')
def test_orc_write_bloom_filter_sql_cpu_fallback(spark_tmp_path, spark_tmp_table_factory):
    data_path = spark_tmp_path + '/ORC_DATA'
    base_table_name = spark_tmp_table_factory.get()

    def sql_write(spark, path):
        is_gpu = path.endswith('GPU')
        table_name = base_table_name + '_GPU' if is_gpu else base_table_name + '_CPU'
        spark.sql('CREATE TABLE `{}` STORED AS ORCFILE location \'{}\' TBLPROPERTIES("orc.bloom.filter.columns"="id") '
                  'AS SELECT id from range(100)'.format(table_name, path))

    assert_gpu_fallback_write(
      sql_write,
      lambda spark, path: spark.read.orc(path),
      data_path,
      'DataWritingCommandExec',
      conf={'spark.rapids.sql.format.orc.write.enabled': True})


@pytest.mark.parametrize('orc_gens', orc_write_gens_list, ids=idfn)
@allow_non_gpu(*non_utc_allow)
def test_write_empty_orc_round_trip(spark_tmp_path, orc_gens):
    def create_empty_df(spark, path):
        gen_list = [('_c' + str(i), gen) for i, gen in enumerate(orc_gens)]
        return gen_df(spark, gen_list, length=0).write.orc(path)
    data_path = spark_tmp_path + '/ORC_DATA'
    assert_gpu_and_cpu_writes_are_equal_collect(
        create_empty_df,
        lambda spark, path: spark.read.orc(path),
        data_path,
        conf={'spark.rapids.sql.format.orc.write.enabled': True})


hold_gpu_configs = [True, False]
@pytest.mark.parametrize('hold_gpu', hold_gpu_configs, ids=idfn)
def test_async_writer(spark_tmp_path, hold_gpu):
    data_path = spark_tmp_path + '/ORC_DATA'
    num_rows = 2048
    num_cols = 10
    orc_gen = [int_gen for _ in range(num_cols)]
    gen_list = [('_c' + str(i), gen) for i, gen in enumerate(orc_gen)]
    assert_gpu_and_cpu_writes_are_equal_collect(
        lambda spark, path: gen_df(spark, gen_list, length=num_rows).write.orc(path),
        lambda spark, path: spark.read.orc(path).orderBy([('_c' + str(i)) for i in range(num_cols)]),
        data_path,
        conf={"spark.rapids.sql.asyncWrite.queryOutput.enabled": "true",
              "spark.rapids.sql.batchSizeBytes": 4 * num_cols * 100,  # 100 rows per batch
              "spark.rapids.sql.queryOutput.holdGpuInTask": hold_gpu})


@ignore_order
@pytest.mark.skipif(is_before_spark_320(), reason="is only supported in Spark 320+")
def test_concurrent_writer(spark_tmp_path):
    data_path = spark_tmp_path + '/PARQUET_DATA'
    assert_gpu_and_cpu_writes_are_equal_collect(
        lambda spark, path: get_25_partitions_df(spark)  # df has 25 partitions for (c1, c2)
            .repartition(2)
            .write.mode("overwrite").partitionBy('c1', 'c2').orc(path),
        lambda spark, path: spark.read.orc(path),
        data_path,
        copy_and_update(
            # 26 > 25, will not fall back to single writer
            # Disable AQE temporarily until https://github.com/NVIDIA/spark-rapids/issues/14319 is resolved.
            {"spark.sql.maxConcurrentOutputFileWriters": 26,
             'spark.sql.adaptive.enabled': 'false'}
        ))


@ignore_order
@pytest.mark.skipif(is_before_spark_320(), reason="is only supported in Spark 320+")
def test_fallback_to_single_writer_from_concurrent_writer(spark_tmp_path):
    data_path = spark_tmp_path + '/PARQUET_DATA'
    assert_gpu_and_cpu_writes_are_equal_collect(
        lambda spark, path: get_25_partitions_df(spark)  # df has 25 partitions for (c1, c2)
            .repartition(2)
            .write.mode("overwrite").partitionBy('c1', 'c2').orc(path),
        lambda spark, path: spark.read.orc(path),
        data_path,
        copy_and_update(
            # 10 < 25, will fall back to single writer
            # Disable AQE temporarily until https://github.com/NVIDIA/spark-rapids/issues/14319 is resolved.
            {"spark.sql.maxConcurrentOutputFileWriters": 10,
             'spark.sql.adaptive.enabled': 'false'},
            {"spark.rapids.sql.concurrentWriterPartitionFlushSize": 64 * 1024 * 1024}
        ))

@ignore_order
def test_orc_write_column_name_with_dots(spark_tmp_path):
    data_path = spark_tmp_path + "/ORC_DATA"
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
    assert_gpu_and_cpu_writes_are_equal_collect(
        lambda spark, path:  gen_df(spark, gens).coalesce(1).write.orc(path),
        lambda spark, path: spark.read.orc(path),
        data_path)


# test case from:
# https://github.com/apache/spark/blob/v3.4.0/sql/core/src/test/scala/org/apache/spark/sql/execution/datasources/orc/OrcQuerySuite.scala#L371
@ignore_order
def test_orc_do_not_lowercase_columns(spark_tmp_path):
    data_path = spark_tmp_path + "/ORC_DATA"

    # The wording of the `is not exists` error message in Spark 4.x is unfortunate, but accurate:
    # https://github.com/apache/spark/blob/4501285a49e4c0429c9cf2c105f044e1c8a93d21/python/pyspark/errors/error-conditions.json#L487
    if is_spark_420_or_later():
        expected_error_message = "Key `acol` does not exist."
    elif is_spark_400_or_later() or is_databricks_version_or_later(14, 3):
        expected_error_message = "Key `acol` is not exists."
    else:
        expected_error_message = "No StructField named acol"
    assert_gpu_and_cpu_writes_are_equal_collect(
        # column is uppercase
        lambda spark, path: spark.range(0, 1000).select(col("id").alias("Acol")).write.orc(path),
        lambda spark, path: spark.read.orc(path),
        data_path)
    try:
        # reading lowercase causes exception
        with_cpu_session(lambda spark: spark.read.orc(data_path + "/CPU").schema["acol"])
        assert False
    except KeyError as e:
        assert expected_error_message in str(e)
    try:
        # reading lowercase causes exception
        with_gpu_session(lambda spark: spark.read.orc(data_path + "/GPU").schema["acol"])
        assert False
    except KeyError as e:
        assert expected_error_message in str(e)
