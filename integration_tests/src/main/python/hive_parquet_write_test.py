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

from asserts import assert_gpu_and_cpu_sql_writes_are_equal_collect, assert_equal_with_local_sort
from conftest import is_databricks_runtime
from data_gen import *
from hive_write_test import _restricted_timestamp
from marks import allow_non_gpu, ignore_order
from spark_session import with_cpu_session, with_gpu_session, is_before_spark_320, is_spark_350_or_later, is_spark_330_or_later, is_databricks122_or_later

# Disable the meta conversion from Hive write to FrameData write in Spark, to test
# "GpuInsertIntoHiveTable" for Parquet write.
_write_to_hive_conf = {"spark.sql.hive.convertMetastoreParquet": False}

_hive_bucket_basic_gens_sans_bools = [
    byte_gen, short_gen, int_gen, long_gen, string_gen, float_gen, double_gen,
    DateGen(start=date(1590, 1, 1)), _restricted_timestamp()]
_hive_bucket_basic_gens = [boolean_gen] + _hive_bucket_basic_gens_sans_bools

_hive_bucket_basic_struct_gen = StructGen(
    [['c'+str(ind), c_gen] for ind, c_gen in enumerate(_hive_bucket_basic_gens_sans_bools)])

_hive_bucket_struct_gens = [
    _hive_bucket_basic_struct_gen,
    StructGen([['child0', byte_gen], ['child1', _hive_bucket_basic_struct_gen]]),
    StructGen([['child0', ArrayGen(short_gen)], ['child1', double_gen]])]

_hive_bucket_array_gens = [ArrayGen(sub_gen) for sub_gen in _hive_bucket_basic_gens_sans_bools] + [
    ArrayGen(ArrayGen(short_gen, max_length=10), max_length=10),
    ArrayGen(ArrayGen(string_gen, max_length=10), max_length=10),
    ArrayGen(StructGen([['child0', byte_gen], ['child1', string_gen], ['child2', float_gen]]))]

_hive_bucket_gens_sans_bools = _hive_bucket_basic_gens_sans_bools + _hive_bucket_struct_gens + _hive_bucket_array_gens

_hive_basic_gens = _hive_bucket_basic_gens + [
    DecimalGen(precision=19, scale=1, nullable=True),
    DecimalGen(precision=23, scale=5, nullable=True),
    DecimalGen(precision=36, scale=3, nullable=True)]

_hive_basic_struct_gen = StructGen(
    [['c'+str(ind), c_gen] for ind, c_gen in enumerate(_hive_basic_gens)])

_hive_struct_gens = [
    _hive_basic_struct_gen,
    StructGen([['child0', byte_gen], ['child1', _hive_basic_struct_gen]]),
    StructGen([['child0', ArrayGen(short_gen)], ['child1', double_gen]])]

_hive_array_gens = [ArrayGen(sub_gen) for sub_gen in _hive_basic_gens] + [
    ArrayGen(ArrayGen(short_gen, max_length=10), max_length=10),
    ArrayGen(ArrayGen(string_gen, max_length=10), max_length=10),
    ArrayGen(StructGen([['child0', byte_gen], ['child1', string_gen], ['child2', float_gen]]))]

_hive_map_gens = [simple_string_to_string_map_gen] + [MapGen(f(nullable=False), f()) for f in [
    BooleanGen, ByteGen, ShortGen, IntegerGen, LongGen, FloatGen, DoubleGen,
    lambda nullable=True: _restricted_timestamp(nullable=nullable),
    lambda nullable=True: DateGen(start=date(1590, 1, 1), nullable=nullable),
    lambda nullable=True: DecimalGen(precision=19, scale=1, nullable=nullable),
    lambda nullable=True: DecimalGen(precision=36, scale=5, nullable=nullable)]]

_hive_write_gens = [_hive_basic_gens, _hive_struct_gens, _hive_array_gens, _hive_map_gens]


def read_single_bucket(table, bucket_id):
    # Bucket Id string format: f"_$id%05d" + ".c$fileCounter%03d".
    # fileCounter is always 0 in this test. For example '_00002.c000' is for
    # bucket id being 2.
    # We leverage this bucket segment in the file path to filter rows belong to a bucket.
    bucket_segment = '_' + "{}".format(bucket_id).rjust(5, '0') + '.c000'
    return with_cpu_session(
        lambda spark: spark.sql("select * from {}".format(table))
        .withColumn('file_name', f.input_file_name())
        .filter(f.col('file_name').contains(bucket_segment))
        .drop('file_name')  # need to drop the file_name column for comparison.
        .collect())


@allow_non_gpu(*(non_utc_allow))
@ignore_order(local=True)
@pytest.mark.parametrize("is_ctas", [True, False], ids=['CTAS', 'CTTW'])
@pytest.mark.parametrize("gens", _hive_write_gens, ids=idfn)
def test_write_parquet_into_hive_table(spark_tmp_table_factory, is_ctas, gens):

    def gen_table(spark):
        gen_list = [('_c' + str(i), gen) for i, gen in enumerate(gens)]
        types_sql_str = ','.join('{} {}'.format(
            name, gen.data_type.simpleString()) for name, gen in gen_list)
        data_table = spark_tmp_table_factory.get()
        gen_df(spark, gen_list).createOrReplaceTempView(data_table)
        return data_table, types_sql_str

    (input_table, input_schema) = with_cpu_session(gen_table)

    def write_to_hive_sql(spark, output_table):
        if is_ctas:
            # Create Table As Select
            return [
                "CREATE TABLE {} STORED AS PARQUET AS SELECT * FROM {}".format(
                    output_table, input_table)
            ]
        else:
            # Create Table Then Write
            return [
                "CREATE TABLE {} ({}) STORED AS PARQUET".format(output_table, input_schema),
                "INSERT OVERWRITE TABLE {} SELECT * FROM {}".format(output_table, input_table)
            ]

    assert_gpu_and_cpu_sql_writes_are_equal_collect(
        spark_tmp_table_factory,
        write_to_hive_sql,
        _write_to_hive_conf)


@allow_non_gpu(*non_utc_allow)
@ignore_order(local=True)
@pytest.mark.parametrize("is_static", [True, False], ids=['Static_Partition', 'Dynamic_Partition'])
def test_write_parquet_into_partitioned_hive_table(spark_tmp_table_factory, is_static):
    # Generate hive table in Parquet format
    def gen_table(spark):
        # gen_list = [('_c' + str(i), gen) for i, gen in enumerate(gens)]
        dates = [date(2024, 2, 28), date(2024, 2, 27), date(2024, 2, 26)]
        gen_list = [('a', int_gen),
                    ('b', long_gen),
                    ('c', short_gen),
                    ('d', string_gen),
                    ('part', SetValuesGen(DateType(), dates))]
        data_table = spark_tmp_table_factory.get()
        gen_df(spark, gen_list).createOrReplaceTempView(data_table)
        return data_table

    input_table = with_cpu_session(gen_table)

    def partitioned_write_to_hive_sql(spark, output_table):
        sql_create_part_table = (
            "CREATE TABLE {} (a INT, b LONG, c SHORT, d STRING) "
            "PARTITIONED BY (part DATE) STORED AS PARQUET"
        ).format(output_table)
        if is_static:
            return [
                # sql_1: Create partitioned hive table
                sql_create_part_table,
                # sql_2: Static partition write only to partition 'par2'
                "INSERT OVERWRITE TABLE {} PARTITION (part='2024-02-25') "
                "SELECT a, b, c, d FROM {}".format(output_table, input_table)
            ]
        else:
            return [
                # sql_1: Create partitioned hive table
                sql_create_part_table,
                # sql_2: Dynamic partition write
                "INSERT OVERWRITE TABLE {} SELECT * FROM {}".format(output_table, input_table)
            ]
    all_confs = copy_and_update(_write_to_hive_conf, {
        "hive.exec.dynamic.partition.mode": "nonstrict"})
    assert_gpu_and_cpu_sql_writes_are_equal_collect(
        spark_tmp_table_factory,
        partitioned_write_to_hive_sql,
        all_confs)


zstd_param = pytest.param('ZSTD',
    marks=pytest.mark.skipif(is_before_spark_320(), reason="zstd is not supported before 320"))

@allow_non_gpu(*(non_utc_allow))
@ignore_order(local=True)
@pytest.mark.parametrize("comp_type", ['UNCOMPRESSED', 'SNAPPY', zstd_param])
def test_write_compressed_parquet_into_hive_table(spark_tmp_table_factory, comp_type):
    # Generate hive table in Parquet format
    def gen_table(spark):
        gens = _hive_basic_gens + _hive_struct_gens + _hive_array_gens + _hive_map_gens
        gen_list = [('_c' + str(i), gen) for i, gen in enumerate(gens)]
        types_sql_str = ','.join('{} {}'.format(
            name, gen.data_type.simpleString()) for name, gen in gen_list)
        data_table = spark_tmp_table_factory.get()
        gen_df(spark, gen_list).createOrReplaceTempView(data_table)
        return data_table, types_sql_str

    input_table, schema_str = with_cpu_session(gen_table)

    def write_to_hive_sql(spark, output_table):
        return [
            # Create table with compression type
            "CREATE TABLE {} ({}) STORED AS PARQUET "
            "TBLPROPERTIES ('parquet.compression'='{}')".format(
                output_table, schema_str, comp_type),
            # Insert into table
            "INSERT OVERWRITE TABLE {} SELECT * FROM {}".format(output_table, input_table)
        ]

    assert_gpu_and_cpu_sql_writes_are_equal_collect(
        spark_tmp_table_factory,
        write_to_hive_sql,
        _write_to_hive_conf)


@allow_non_gpu(*non_utc_allow)
@pytest.mark.skipif(is_databricks_runtime() and not is_databricks122_or_later(),
                    reason="InsertIntoHiveTable supports bucketed write since Spark 330")
def test_insert_hive_bucketed_table(spark_tmp_table_factory):
    num_rows = 2048

    def gen_table(spark):
        gen_list = [('_c' + str(i), gen) for i, gen in enumerate(_hive_bucket_gens_sans_bools)]
        types_sql_str = ','.join('{} {}'.format(
            name, gen.data_type.simpleString()) for name, gen in gen_list)
        col_names_str = ','.join(name for name, gen in gen_list)
        data_table = spark_tmp_table_factory.get()
        gen_df(spark, gen_list, num_rows).createOrReplaceTempView(data_table)
        return data_table, types_sql_str, col_names_str

    (input_data, input_schema, input_cols_str) = with_cpu_session(gen_table)
    num_buckets = 4

    def insert_hive_table(spark, out_table):
        spark.sql(
            "CREATE TABLE {} ({}) STORED AS PARQUET CLUSTERED BY ({}) INTO {} BUCKETS".format(
                out_table, input_schema, input_cols_str, num_buckets))
        spark.sql(
            "INSERT OVERWRITE {} SELECT * FROM {}".format(out_table, input_data))

    cpu_table = spark_tmp_table_factory.get()
    gpu_table = spark_tmp_table_factory.get()
    with_cpu_session(lambda spark: insert_hive_table(spark, cpu_table), _write_to_hive_conf)
    with_gpu_session(lambda spark: insert_hive_table(spark, gpu_table), _write_to_hive_conf)
    cpu_rows, gpu_rows = 0, 0
    for cur_bucket_id in range(num_buckets):
        # Verify the result bucket by bucket
        ret_cpu = read_single_bucket(cpu_table, cur_bucket_id)
        cpu_rows += len(ret_cpu)
        ret_gpu = read_single_bucket(gpu_table, cur_bucket_id)
        gpu_rows += len(ret_gpu)
        assert_equal_with_local_sort(ret_cpu, ret_gpu)

    assert cpu_rows == num_rows
    assert gpu_rows == num_rows


@pytest.mark.skipif(is_spark_330_or_later() or is_databricks122_or_later(),
                    reason="InsertIntoHiveTable supports bucketed write since Spark 330")
@pytest.mark.parametrize("hive_hash", [True, False])
def test_insert_hive_bucketed_table_before_330(spark_tmp_table_factory, hive_hash):
    num_buckets = 4

    def insert_hive_table(spark, out_table):
        data_table = spark_tmp_table_factory.get()
        two_col_df(spark, int_gen, long_gen).createOrReplaceTempView(data_table)
        spark.sql(
            """CREATE TABLE {} (a int, b long) STORED AS PARQUET
            CLUSTERED BY (a) INTO {} BUCKETS""".format(out_table, num_buckets))
        spark.sql(
            "INSERT OVERWRITE {} SELECT * FROM {}".format(out_table, data_table))

    all_confs = copy_and_update(_write_to_hive_conf, {
        "hive.enforce.bucketing": False,  # allow the write with bucket spec
        "hive.enforce.sorting": False,  # allow the write with bucket spec
        "spark.rapids.sql.format.write.forceHiveHashForBucketing": hive_hash
    })
    cpu_table = spark_tmp_table_factory.get()
    gpu_table = spark_tmp_table_factory.get()
    with_cpu_session(lambda spark: insert_hive_table(spark, cpu_table), all_confs)
    with_gpu_session(lambda spark: insert_hive_table(spark, gpu_table), all_confs)

    all_cpu_rows = with_cpu_session(
        lambda spark: spark.sql("select * from {}".format(cpu_table)).collect())
    all_gpu_rows = with_cpu_session(
        lambda spark: spark.sql("select * from {}".format(gpu_table)).collect())
    assert_equal_with_local_sort(all_cpu_rows, all_gpu_rows)

    for cur_bucket_id in range(num_buckets):
        ret_cpu = read_single_bucket(cpu_table, cur_bucket_id)
        ret_gpu = read_single_bucket(gpu_table, cur_bucket_id)
        if hive_hash:
            # GPU will write the right bucketed table, but CPU will not. Because
            # InsertIntoHiveTable supports bucketed write only since Spark 330.
            # GPU behaviors differently than the normal Spark.
            assert len(ret_gpu) > 0 and len(ret_cpu) == 0
        else:
            # Both GPU and CPU write the data but no bucketing, actually.
            assert len(ret_gpu) == 0 and len(ret_cpu) == 0

