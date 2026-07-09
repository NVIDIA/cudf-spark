# Copyright (c) 2022-2026, NVIDIA CORPORATION.
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

from asserts import assert_equal_with_local_sort, assert_gpu_and_cpu_are_equal_collect, assert_gpu_and_cpu_row_counts_equal, assert_gpu_fallback_collect, assert_spark_exception
from conftest import is_iceberg_remote_catalog
from data_gen import *
from iceberg import get_full_table_name, iceberg_unsupported_mark, _build_tblprops, \
    _BASE_TBLPROPS_SQL, create_iceberg_table
from marks import allow_non_gpu, iceberg, ignore_order
from spark_session import is_databricks_runtime, with_cpu_session, \
    with_gpu_session

iceberg_map_gens = [MapGen(f(nullable=False), f()) for f in [
    BooleanGen, ByteGen, ShortGen, IntegerGen, LongGen, FloatGen, DoubleGen, DateGen, TimestampGen ]] + \
                    [simple_string_to_string_map_gen,
                     MapGen(StringGen(pattern='key_[0-9]', nullable=False), ArrayGen(string_gen), max_length=10),
                     MapGen(RepeatSeqGen(IntegerGen(nullable=False), 10), long_gen, max_length=10),
                     MapGen(StringGen(pattern='key_[0-9]', nullable=False), simple_string_to_string_map_gen)]

iceberg_primitive_gens_list = [[byte_gen, short_gen, int_gen, long_gen, float_gen, double_gen,
                               string_gen, boolean_gen, date_gen, timestamp_gen, binary_gen] + decimal_gens]

iceberg_gens_list = [
    [byte_gen, short_gen, int_gen, long_gen, float_gen, double_gen,
     string_gen, boolean_gen, date_gen, timestamp_gen, binary_gen, ArrayGen(binary_gen),
     ArrayGen(byte_gen), ArrayGen(long_gen), ArrayGen(string_gen), ArrayGen(date_gen),
     ArrayGen(timestamp_gen), ArrayGen(decimal_gen_64bit), ArrayGen(ArrayGen(byte_gen)),
     StructGen([['child0', ArrayGen(byte_gen)], ['child1', byte_gen], ['child2', float_gen], ['child3', decimal_gen_64bit]]),
     ArrayGen(StructGen([['child0', string_gen], ['child1', double_gen], ['child2', int_gen]]))
    ] + iceberg_map_gens + decimal_gens ]

rapids_reader_types = ['PERFILE', 'MULTITHREADED', 'COALESCING']
_NO_FANOUT = _BASE_TBLPROPS_SQL

pytestmark = iceberg_unsupported_mark

@allow_non_gpu("BatchScanExec")
@iceberg
@ignore_order(local=True) # Iceberg plans with a thread pool and is not deterministic in file ordering
def test_iceberg_fallback_not_unsafe_row(spark_tmp_table_factory):
    full_table = get_full_table_name(spark_tmp_table_factory)
    def setup_iceberg_table(spark):
        spark.sql(f"CREATE TABLE {full_table} (id BIGINT, data STRING) USING ICEBERG {_NO_FANOUT}")
        spark.sql(f"INSERT INTO {full_table} VALUES (1, 'a'), (2, 'b'), (3, 'c')")
    with_cpu_session(setup_iceberg_table)
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : spark.sql(f"SELECT COUNT(DISTINCT id) from {full_table}"),
        conf={"spark.rapids.sql.format.iceberg.enabled": "false"}
    )

@iceberg
@ignore_order(local=True)
@pytest.mark.skipif(is_databricks_runtime(),
                    reason="AQE+DPP not supported until Spark 3.2.0+ and AQE+DPP not supported on Databricks")
@pytest.mark.parametrize('reader_type', rapids_reader_types)
def test_iceberg_aqe_dpp(spark_tmp_table_factory, reader_type):
    full_table = get_full_table_name(spark_tmp_table_factory)
    tmpview = spark_tmp_table_factory.get()
    def setup_iceberg_table(spark):
        df = two_col_df(spark, int_gen, int_gen)
        df.createOrReplaceTempView(tmpview)
        spark.sql(f"CREATE TABLE {full_table} (a INT, b INT) USING ICEBERG PARTITIONED BY (a) {_NO_FANOUT}")
        spark.sql(f"INSERT INTO {full_table} SELECT * FROM {tmpview}")
    with_cpu_session(setup_iceberg_table)
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : spark.sql(f"SELECT * from {full_table} as X JOIN {full_table} as Y ON X.a = Y.a "
                                 f"WHERE Y.a > 0"),
        conf={"spark.sql.adaptive.enabled": "true",
              "spark.rapids.sql.format.parquet.reader.type": reader_type,
              "spark.sql.optimizer.dynamicPartitionPruning.enabled": "true"})

@iceberg
@ignore_order(local=True) # Iceberg plans with a thread pool and is not deterministic in file ordering
@pytest.mark.parametrize("data_gens", iceberg_gens_list, ids=idfn)
@pytest.mark.parametrize('reader_type', rapids_reader_types)
def test_iceberg_parquet_read_round_trip_select_one(spark_tmp_table_factory, data_gens, reader_type):
    gen_list = [('_c' + str(i), gen) for i, gen in enumerate(data_gens)]
    full_table = get_full_table_name(spark_tmp_table_factory)
    tmpview = spark_tmp_table_factory.get()
    def setup_iceberg_table(spark):
        df = gen_df(spark, gen_list)
        df.createOrReplaceTempView(tmpview)
        spark.sql(f"CREATE TABLE {full_table} USING ICEBERG {_NO_FANOUT} AS SELECT * FROM {tmpview}")
    with_cpu_session(setup_iceberg_table)
    # explicitly only select 1 column to make sure we test that path in the schema parsing code
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : spark.sql(f"SELECT _c0 FROM {full_table}"),
        conf={'spark.rapids.sql.format.parquet.reader.type': reader_type})

@iceberg
@ignore_order(local=True) # Iceberg plans with a thread pool and is not deterministic in file ordering
@pytest.mark.parametrize("data_gens", iceberg_primitive_gens_list, ids=idfn)
@pytest.mark.parametrize('reader_type', rapids_reader_types)
def test_iceberg_parquet_read_round_trip(spark_tmp_table_factory, data_gens, reader_type):
    gen_list = [('_c' + str(i), gen) for i, gen in enumerate(data_gens)]
    full_table = get_full_table_name(spark_tmp_table_factory)
    tmpview = spark_tmp_table_factory.get()
    def setup_iceberg_table(spark):
        df = gen_df(spark, gen_list)
        df.createOrReplaceTempView(tmpview)
        spark.sql(f"CREATE TABLE {full_table} USING ICEBERG {_NO_FANOUT} AS SELECT * FROM {tmpview}")
    with_cpu_session(setup_iceberg_table)
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : spark.sql(f"SELECT * FROM {full_table}"),
        conf={'spark.rapids.sql.format.parquet.reader.type': reader_type})

@iceberg
@ignore_order(local=True) # Iceberg plans with a thread pool and is not deterministic in file ordering
@pytest.mark.parametrize("data_gens", iceberg_gens_list, ids=idfn)
@pytest.mark.parametrize('reader_type', rapids_reader_types)
def test_iceberg_parquet_read_round_trip_all_types(spark_tmp_table_factory, data_gens, reader_type):
    gen_list = [('_c' + str(i), gen) for i, gen in enumerate(data_gens)]
    full_table = get_full_table_name(spark_tmp_table_factory)
    tmpview = spark_tmp_table_factory.get()
    def setup_iceberg_table(spark):
        df = gen_df(spark, gen_list)
        df.createOrReplaceTempView(tmpview)
        spark.sql(f"CREATE TABLE {full_table} USING ICEBERG {_NO_FANOUT} AS SELECT * FROM {tmpview}")
    with_cpu_session(setup_iceberg_table)
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : spark.sql(f"SELECT * FROM {full_table}"),
        conf={'spark.rapids.sql.format.parquet.reader.type': reader_type})

@iceberg
@pytest.mark.parametrize("data_gens", [[long_gen]], ids=idfn)
@pytest.mark.parametrize("iceberg_format", ["orc", "avro"], ids=idfn)
@pytest.mark.parametrize('reader_type', rapids_reader_types)
def test_iceberg_unsupported_formats(spark_tmp_table_factory, data_gens, iceberg_format, reader_type):
    gen_list = [('_c' + str(i), gen) for i, gen in enumerate(data_gens)]
    full_table = get_full_table_name(spark_tmp_table_factory)
    tmpview = spark_tmp_table_factory.get()
    def setup_iceberg_table(spark):
        df = gen_df(spark, gen_list)
        df.createOrReplaceTempView(tmpview)
        props = _build_tblprops({'write.format.default': iceberg_format})
        props_sql = ", ".join(f"'{k}' = '{v}'" for k, v in props.items())
        spark.sql(f"CREATE TABLE {full_table} USING ICEBERG "
                  f"TBLPROPERTIES({props_sql}) "
                  f"AS SELECT * FROM {tmpview}")
    with_cpu_session(setup_iceberg_table)
    assert_spark_exception(
        lambda : with_gpu_session(
            lambda spark : spark.sql(f"SELECT * FROM {full_table}").collect(),
            conf={'spark.rapids.sql.format.parquet.reader.type': reader_type}),
        "UnsupportedOperationException")

@iceberg
@allow_non_gpu("BatchScanExec", "ColumnarToRowExec")
@ignore_order(local=True) # Iceberg plans with a thread pool and is not deterministic in file ordering
@pytest.mark.parametrize("disable_conf", ["spark.rapids.sql.format.iceberg.enabled",
                                          "spark.rapids.sql.format.iceberg.read.enabled"], ids=idfn)
def test_iceberg_read_fallback(spark_tmp_table_factory, disable_conf):
    full_table = get_full_table_name(spark_tmp_table_factory)
    def setup_iceberg_table(spark):
        spark.sql(f"CREATE TABLE {full_table} (id BIGINT, data STRING) USING ICEBERG {_NO_FANOUT}")
        spark.sql(f"INSERT INTO {full_table} VALUES (1, 'a'), (2, 'b'), (3, 'c')")
    with_cpu_session(setup_iceberg_table)
    assert_gpu_fallback_collect(
        lambda spark : spark.sql(f"SELECT * FROM {full_table}"),
        "BatchScanExec",
        conf = {disable_conf : "false"})

@iceberg
@ignore_order(local=True) # Iceberg plans with a thread pool and is not deterministic in file ordering
# Compression codec to test and whether the codec is supported by cudf
# Note that compression codecs brotli and lzo need extra jars
# https://githbub.com/NVIDIA/spark-rapids/issues/143
@pytest.mark.parametrize("codec_info", [
    ("uncompressed", None),
    ("snappy", None),
    ("gzip", None),
    pytest.param(("lz4", "Unsupported Parquet compression type")),
    ("zstd", None)], ids=idfn)
@pytest.mark.parametrize('reader_type', rapids_reader_types)
def test_iceberg_read_parquet_compression_codec(spark_tmp_table_factory, codec_info, reader_type):
    codec, error_msg = codec_info
    full_table = get_full_table_name(spark_tmp_table_factory)
    tmpview = spark_tmp_table_factory.get()
    def setup_iceberg_table(spark):
        df = binary_op_df(spark, long_gen)
        df.createOrReplaceTempView(tmpview)
        props = _build_tblprops({'write.parquet.compression-codec': codec})
        props_sql = ", ".join(f"'{k}' = '{v}'" for k, v in props.items())
        spark.sql(f"CREATE TABLE {full_table} (id BIGINT, data BIGINT) USING ICEBERG "
                  f"TBLPROPERTIES({props_sql})")
        spark.sql(f"INSERT INTO {full_table} SELECT * FROM {tmpview}")
    with_cpu_session(setup_iceberg_table)
    query = f"SELECT * FROM {full_table}"
    read_conf = {'spark.rapids.sql.format.parquet.reader.type': reader_type}
    if error_msg:
        assert_spark_exception(
            lambda : with_gpu_session(lambda spark : spark.sql(query).collect(), conf=read_conf),
            error_msg)
    else:
        assert_gpu_and_cpu_are_equal_collect(lambda spark : spark.sql(query), conf=read_conf)

@iceberg
@ignore_order(local=True) # Iceberg plans with a thread pool and is not deterministic in file ordering
@pytest.mark.parametrize("key_gen", [int_gen, long_gen, string_gen, boolean_gen, date_gen, timestamp_gen, decimal_gen_64bit], ids=idfn)
@pytest.mark.parametrize('reader_type', rapids_reader_types)
def test_iceberg_read_partition_key(spark_tmp_table_factory, key_gen, reader_type):
    full_table = get_full_table_name(spark_tmp_table_factory)
    tmpview = spark_tmp_table_factory.get()
    def setup_iceberg_table(spark):
        df = two_col_df(spark, key_gen, long_gen).orderBy("a")
        df.createOrReplaceTempView(tmpview)
        spark.sql(f"CREATE TABLE {full_table} USING ICEBERG PARTITIONED BY (a) {_NO_FANOUT} " + \
                  f"AS SELECT * FROM {tmpview}")
    with_cpu_session(setup_iceberg_table)
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : spark.sql(f"SELECT a FROM {full_table}"),
        conf={'spark.rapids.sql.format.parquet.reader.type': reader_type})

@iceberg
@ignore_order(local=True) # Iceberg plans with a thread pool and is not deterministic in file ordering
@pytest.mark.parametrize('reader_type', rapids_reader_types)
def test_iceberg_input_meta(spark_tmp_table_factory, reader_type):
    full_table = get_full_table_name(spark_tmp_table_factory)
    tmpview = spark_tmp_table_factory.get()
    def setup_iceberg_table(spark):
        df = binary_op_df(spark, long_gen).orderBy("a")
        df.createOrReplaceTempView(tmpview)
        spark.sql(f"CREATE TABLE {full_table} USING ICEBERG PARTITIONED BY (a) {_NO_FANOUT} " + \
                  f"AS SELECT * FROM {tmpview}")
    with_cpu_session(setup_iceberg_table)
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : spark.sql(
            "SELECT a, input_file_name(), input_file_block_start(), input_file_block_length() " + \
            f"FROM {full_table}"),
        conf={'spark.rapids.sql.format.parquet.reader.type': reader_type})

@iceberg
@ignore_order(local=True) # Iceberg plans with a thread pool and is not deterministic in file ordering
@pytest.mark.parametrize('reader_type', rapids_reader_types)
def test_iceberg_disorder_read_schema(spark_tmp_table_factory, reader_type):
    full_table = get_full_table_name(spark_tmp_table_factory)
    tmpview = spark_tmp_table_factory.get()
    def setup_iceberg_table(spark):
        df = three_col_df(spark, long_gen, string_gen, float_gen)
        df.createOrReplaceTempView(tmpview)
        spark.sql(f"CREATE TABLE {full_table} USING ICEBERG {_NO_FANOUT} " + \
                  f"AS SELECT * FROM {tmpview}")
    with_cpu_session(setup_iceberg_table)
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : spark.sql(f"SELECT b,c,a FROM {full_table}"),
        conf={'spark.rapids.sql.format.parquet.reader.type': reader_type})

@iceberg
@ignore_order(local=True) # Iceberg plans with a thread pool and is not deterministic in file ordering
def test_iceberg_read_appended_table(spark_tmp_table_factory):
    full_table = get_full_table_name(spark_tmp_table_factory)
    tmpview = spark_tmp_table_factory.get()
    def setup_iceberg_table(spark):
        df = binary_op_df(spark, long_gen)
        df.createOrReplaceTempView(tmpview)
        spark.sql(f"CREATE TABLE {full_table} USING ICEBERG {_NO_FANOUT} " + \
                  f"AS SELECT * FROM {tmpview}")
        df = binary_op_df(spark, long_gen, seed=1)
        df.createOrReplaceTempView(tmpview)
        spark.sql(f"INSERT INTO {full_table} " + \
                  f"SELECT * FROM {tmpview}")
    with_cpu_session(setup_iceberg_table)
    assert_gpu_and_cpu_are_equal_collect(lambda spark : spark.sql(f"SELECT * FROM {full_table}"))

@iceberg
# Some metadata files have types that are not supported on the GPU yet (e.g.: BinaryType)
@allow_non_gpu("BatchScanExec", "ProjectExec")
@ignore_order(local=True) # Iceberg plans with a thread pool and is not deterministic in file ordering
def test_iceberg_read_metadata_fallback(spark_tmp_table_factory):
    full_table = get_full_table_name(spark_tmp_table_factory)
    tmpview = spark_tmp_table_factory.get()
    def setup_iceberg_table(spark):
        df = binary_op_df(spark, long_gen)
        df.createOrReplaceTempView(tmpview)
        spark.sql(f"CREATE TABLE {full_table} USING ICEBERG {_NO_FANOUT} " + \
                  f"AS SELECT * FROM {tmpview}")
        df = binary_op_df(spark, long_gen, seed=1)
        df.createOrReplaceTempView(tmpview)
        spark.sql(f"INSERT INTO {full_table} " + \
                  f"SELECT * FROM {tmpview}")
    with_cpu_session(setup_iceberg_table)
    for subtable in ["all_data_files", "all_manifests", "files", "history",
                     "manifests", "partitions", "snapshots"]:
        # SQL does not have syntax to read table metadata
        assert_gpu_fallback_collect(
            lambda spark : spark.read.format("iceberg").load(f"{full_table}.{subtable}"),
            "BatchScanExec")

@iceberg
# Some metadata files have types that are not supported on the GPU yet (e.g.: BinaryType)
@allow_non_gpu("BatchScanExec", "ProjectExec")
def test_iceberg_read_metadata_count(spark_tmp_table_factory):
    full_table = get_full_table_name(spark_tmp_table_factory)
    tmpview = spark_tmp_table_factory.get()
    def setup_iceberg_table(spark):
        df = binary_op_df(spark, long_gen)
        df.createOrReplaceTempView(tmpview)
        spark.sql(f"CREATE TABLE {full_table} USING ICEBERG {_NO_FANOUT} " + \
                  f"AS SELECT * FROM {tmpview}")
        df = binary_op_df(spark, long_gen, seed=1)
        df.createOrReplaceTempView(tmpview)
        spark.sql(f"INSERT INTO {full_table} " + \
                  f"SELECT * FROM {tmpview}")
    with_cpu_session(setup_iceberg_table)
    for subtable in ["all_data_files", "all_manifests", "files", "history",
                     "manifests", "partitions", "snapshots"]:
        # SQL does not have syntax to read table metadata
        assert_gpu_and_cpu_row_counts_equal(
            lambda spark : spark.read.format("iceberg").load(f"{full_table}.{subtable}"))

@iceberg
@ignore_order(local=True) # Iceberg plans with a thread pool and is not deterministic in file ordering
@pytest.mark.parametrize('reader_type', rapids_reader_types)
def test_iceberg_read_timetravel(spark_tmp_table_factory, reader_type):
    full_table = get_full_table_name(spark_tmp_table_factory)
    tmpview = spark_tmp_table_factory.get()
    def setup_snapshots(spark):
        df = binary_op_df(spark, long_gen)
        df.createOrReplaceTempView(tmpview)
        spark.sql(f"CREATE TABLE {full_table} USING ICEBERG {_NO_FANOUT} " + \
                  f"AS SELECT * FROM {tmpview}".format(tmpview))
        df = binary_op_df(spark, long_gen, seed=1)
        df.createOrReplaceTempView(tmpview)
        spark.sql(f"INSERT INTO {full_table} " + \
                  f"SELECT * FROM {tmpview}")
        return spark.sql("SELECT snapshot_id FROM {}.snapshots ".format(full_table) + \
                         "ORDER BY committed_at").head()[0]
    first_snapshot_id = with_cpu_session(setup_snapshots)
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : spark.read.option("versionAsOf", first_snapshot_id) \
            .format("iceberg").load("{}".format(full_table)),
        conf={'spark.rapids.sql.format.parquet.reader.type': reader_type})

@iceberg
@ignore_order(local=True) # Iceberg plans with a thread pool and is not deterministic in file ordering
@pytest.mark.parametrize('reader_type', rapids_reader_types)
def test_iceberg_incremental_read(spark_tmp_table_factory, reader_type):
    full_table = get_full_table_name(spark_tmp_table_factory)
    tmpview = spark_tmp_table_factory.get()
    def setup_snapshots(spark):
        df = binary_op_df(spark, long_gen)
        df.createOrReplaceTempView(tmpview)
        spark.sql("CREATE TABLE {} USING ICEBERG ".format(full_table) + \
                  _NO_FANOUT + " AS SELECT * FROM {}".format(tmpview))
        df = binary_op_df(spark, long_gen, seed=1)
        df.createOrReplaceTempView(tmpview)
        spark.sql("INSERT INTO {} ".format(full_table) + \
                  "SELECT * FROM {}".format(tmpview))
        df = binary_op_df(spark, long_gen, seed=2)
        df.createOrReplaceTempView(tmpview)
        spark.sql("INSERT INTO {} ".format(full_table) + \
                  "SELECT * FROM {}".format(tmpview))
        return spark.sql("SELECT snapshot_id FROM {}.snapshots ".format(full_table) + \
                         "ORDER BY committed_at").collect()
    snapshots = with_cpu_session(setup_snapshots)
    start_snapshot, end_snapshot = [ row[0] for row in snapshots[:2] ]
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : spark.read \
            .option("start-snapshot-id", start_snapshot) \
            .option("end-snapshot-id", end_snapshot) \
            .format("iceberg").load("{}".format(full_table)),
        conf={'spark.rapids.sql.format.parquet.reader.type': reader_type})

@iceberg
@ignore_order(local=True) # Iceberg plans with a thread pool and is not deterministic in file ordering
@pytest.mark.parametrize('reader_type', rapids_reader_types)
def test_iceberg_reorder_columns(spark_tmp_table_factory, reader_type):
    table = get_full_table_name(spark_tmp_table_factory)
    tmpview = spark_tmp_table_factory.get()
    def setup_iceberg_table(spark):
        df = binary_op_df(spark, long_gen)
        df.createOrReplaceTempView(tmpview)
        spark.sql("CREATE TABLE {} USING ICEBERG ".format(table) + \
                  _NO_FANOUT + " AS SELECT * FROM {}".format(tmpview))
        spark.sql("ALTER TABLE {} ALTER COLUMN b FIRST".format(table))
        df = binary_op_df(spark, long_gen, seed=1)
        df.createOrReplaceTempView(tmpview)
        spark.sql("INSERT INTO {} ".format(table) + \
                  "SELECT * FROM {}".format(tmpview))
    with_cpu_session(setup_iceberg_table)
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : spark.sql("SELECT * FROM {}".format(table)),
        conf={'spark.rapids.sql.format.parquet.reader.type': reader_type})

@iceberg
@ignore_order(local=True) # Iceberg plans with a thread pool and is not deterministic in file ordering
@pytest.mark.parametrize('reader_type', rapids_reader_types)
def test_iceberg_rename_column(spark_tmp_table_factory, reader_type):
    table = get_full_table_name(spark_tmp_table_factory)
    tmpview = spark_tmp_table_factory.get()
    def setup_iceberg_table(spark):
        df = binary_op_df(spark, long_gen)
        df.createOrReplaceTempView(tmpview)
        spark.sql("CREATE TABLE {} USING ICEBERG ".format(table) + \
                  _NO_FANOUT + " AS SELECT * FROM {}".format(tmpview))
        spark.sql("ALTER TABLE {} RENAME COLUMN a TO c".format(table))
        df = binary_op_df(spark, long_gen, seed=1)
        df.createOrReplaceTempView(tmpview)
        spark.sql("INSERT INTO {} ".format(table) + \
                  "SELECT * FROM {}".format(tmpview))
    with_cpu_session(setup_iceberg_table)
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : spark.sql("SELECT * FROM {}".format(table)),
        conf={'spark.rapids.sql.format.parquet.reader.type': reader_type})

@iceberg
@ignore_order(local=True) # Iceberg plans with a thread pool and is not deterministic in file ordering
@pytest.mark.parametrize('reader_type', rapids_reader_types)
def test_iceberg_column_names_swapped(spark_tmp_table_factory, reader_type):
    table = get_full_table_name(spark_tmp_table_factory)
    tmpview = spark_tmp_table_factory.get()
    def setup_iceberg_table(spark):
        df = binary_op_df(spark, long_gen)
        df.createOrReplaceTempView(tmpview)
        spark.sql("CREATE TABLE {} USING ICEBERG ".format(table) + \
                  _NO_FANOUT + " AS SELECT * FROM {}".format(tmpview))
        spark.sql("ALTER TABLE {} RENAME COLUMN a TO c".format(table))
        spark.sql("ALTER TABLE {} RENAME COLUMN b TO a".format(table))
        spark.sql("ALTER TABLE {} RENAME COLUMN c TO b".format(table))
        df = binary_op_df(spark, long_gen, seed=1)
        df.createOrReplaceTempView(tmpview)
        spark.sql("INSERT INTO {} ".format(table) + \
                  "SELECT * FROM {}".format(tmpview))
    with_cpu_session(setup_iceberg_table)
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : spark.sql("SELECT * FROM {}".format(table)),
        conf={'spark.rapids.sql.format.parquet.reader.type': reader_type})

@iceberg
@ignore_order(local=True) # Iceberg plans with a thread pool and is not deterministic in file ordering
@pytest.mark.parametrize('reader_type', rapids_reader_types)
def test_iceberg_alter_column_type(spark_tmp_table_factory, reader_type):
    table = get_full_table_name(spark_tmp_table_factory)
    tmpview = spark_tmp_table_factory.get()
    def setup_iceberg_table(spark):
        df = three_col_df(spark, int_gen, float_gen, DecimalGen(precision=7, scale=3))
        df.createOrReplaceTempView(tmpview)
        spark.sql("CREATE TABLE {} USING ICEBERG ".format(table) + \
                  _NO_FANOUT + " AS SELECT * FROM {}".format(tmpview))
        spark.sql("ALTER TABLE {} ALTER COLUMN a TYPE BIGINT".format(table))
        spark.sql("ALTER TABLE {} ALTER COLUMN b TYPE DOUBLE".format(table))
        spark.sql("ALTER TABLE {} ALTER COLUMN c TYPE DECIMAL(17, 3)".format(table))
        df = three_col_df(spark, long_gen, double_gen, DecimalGen(precision=17, scale=3))
        df.createOrReplaceTempView(tmpview)
        spark.sql("INSERT INTO {} ".format(table) + \
                  "SELECT * FROM {}".format(tmpview))
    with_cpu_session(setup_iceberg_table)
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : spark.sql("SELECT * FROM {}".format(table)),
        conf={'spark.rapids.sql.format.parquet.reader.type': reader_type})

@iceberg
@ignore_order(local=True) # Iceberg plans with a thread pool and is not deterministic in file ordering
@pytest.mark.parametrize('reader_type', rapids_reader_types)
def test_iceberg_add_column(spark_tmp_table_factory, reader_type):
    table = get_full_table_name(spark_tmp_table_factory)
    tmpview = spark_tmp_table_factory.get()
    def setup_iceberg_table(spark):
        df = binary_op_df(spark, long_gen)
        df.createOrReplaceTempView(tmpview)
        spark.sql("CREATE TABLE {} USING ICEBERG ".format(table) + \
                  _NO_FANOUT + " AS SELECT * FROM {}".format(tmpview))
        spark.sql("ALTER TABLE {} ADD COLUMNS (c DOUBLE)".format(table))
        df = three_col_df(spark, long_gen, long_gen, double_gen)
        df.createOrReplaceTempView(tmpview)
        spark.sql("INSERT INTO {} ".format(table) + \
                  "SELECT * FROM {}".format(tmpview))
    with_cpu_session(setup_iceberg_table)
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : spark.sql("SELECT * FROM {}".format(table)),
        conf={'spark.rapids.sql.format.parquet.reader.type': reader_type})

@iceberg
@ignore_order(local=True) # Iceberg plans with a thread pool and is not deterministic in file ordering
@pytest.mark.parametrize('reader_type', rapids_reader_types)
def test_iceberg_remove_column(spark_tmp_table_factory, reader_type):
    table = get_full_table_name(spark_tmp_table_factory)
    tmpview = spark_tmp_table_factory.get()
    def setup_iceberg_table(spark):
        df = binary_op_df(spark, long_gen)
        df.createOrReplaceTempView(tmpview)
        spark.sql("CREATE TABLE {} USING ICEBERG ".format(table) + \
                  _NO_FANOUT + " AS SELECT * FROM {}".format(tmpview))
        spark.sql("ALTER TABLE {} DROP COLUMN a".format(table))
        df = unary_op_df(spark, long_gen)
        df.createOrReplaceTempView(tmpview)
        spark.sql("INSERT INTO {} ".format(table) + \
                  "SELECT * FROM {}".format(tmpview))
    with_cpu_session(setup_iceberg_table)
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : spark.sql("SELECT * FROM {}".format(table)),
        conf={'spark.rapids.sql.format.parquet.reader.type': reader_type})

@iceberg
@ignore_order(local=True) # Iceberg plans with a thread pool and is not deterministic in file ordering
@pytest.mark.parametrize('reader_type', rapids_reader_types)
def test_iceberg_add_partition_field(spark_tmp_table_factory, reader_type):
    table = get_full_table_name(spark_tmp_table_factory)
    tmpview = spark_tmp_table_factory.get()
    def setup_iceberg_table(spark):
        df = binary_op_df(spark, int_gen)
        df.createOrReplaceTempView(tmpview)
        spark.sql("CREATE TABLE {} USING ICEBERG ".format(table) + \
                  _NO_FANOUT + " AS SELECT * FROM {}".format(tmpview))
        spark.sql("ALTER TABLE {} ADD PARTITION FIELD b".format(table))
        df = binary_op_df(spark, int_gen)
        df.createOrReplaceTempView(tmpview)
        spark.sql("INSERT INTO {} ".format(table) + \
                  "SELECT * FROM {} ORDER BY b".format(tmpview))
    with_cpu_session(setup_iceberg_table)
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : spark.sql("SELECT * FROM {}".format(table)),
        conf={'spark.rapids.sql.format.parquet.reader.type': reader_type})

@iceberg
@ignore_order(local=True) # Iceberg plans with a thread pool and is not deterministic in file ordering
@pytest.mark.parametrize('reader_type', rapids_reader_types)
def test_iceberg_drop_partition_field(spark_tmp_table_factory, reader_type):
    table = get_full_table_name(spark_tmp_table_factory)
    tmpview = spark_tmp_table_factory.get()
    def setup_iceberg_table(spark):
        df = binary_op_df(spark, int_gen)
        df.createOrReplaceTempView(tmpview)
        spark.sql("CREATE TABLE {} (a INT, b INT) USING ICEBERG PARTITIONED BY (b) ".format(table) + _NO_FANOUT)
        spark.sql("INSERT INTO {} SELECT * FROM {} ORDER BY b".format(table, tmpview))
        spark.sql("ALTER TABLE {} DROP PARTITION FIELD b".format(table))
        df = binary_op_df(spark, int_gen)
        df.createOrReplaceTempView(tmpview)
        spark.sql("INSERT INTO {} ".format(table) + \
                  "SELECT * FROM {}".format(tmpview))
    with_cpu_session(setup_iceberg_table)
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : spark.sql("SELECT * FROM {}".format(table)),
        conf={'spark.rapids.sql.format.parquet.reader.type': reader_type})

@iceberg
@ignore_order(local=True) # Iceberg plans with a thread pool and is not deterministic in file ordering
@pytest.mark.parametrize('reader_type', rapids_reader_types)
def test_iceberg_v1_delete(spark_tmp_table_factory, reader_type):
    table = get_full_table_name(spark_tmp_table_factory)
    tmpview = spark_tmp_table_factory.get()
    def setup_iceberg_table(spark):
        df = binary_op_df(spark, long_gen)
        df.createOrReplaceTempView(tmpview)
        spark.sql("CREATE TABLE {} USING ICEBERG ".format(table) + \
                  _NO_FANOUT + " AS SELECT * FROM {}".format(tmpview))
        spark.sql("DELETE FROM {} WHERE a < 0".format(table))
    with_cpu_session(setup_iceberg_table)
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : spark.sql("SELECT * FROM {}".format(table)),
        conf={'spark.rapids.sql.format.parquet.reader.type': reader_type})

@iceberg
@ignore_order(local=True) # Iceberg plans with a thread pool and is not deterministic in file ordering
@pytest.mark.parametrize('reader_type', rapids_reader_types)
def test_iceberg_parquet_read_with_input_file(spark_tmp_table_factory, reader_type):
    table = get_full_table_name(spark_tmp_table_factory)
    tmpview = spark_tmp_table_factory.get()
    def setup_iceberg_table(spark):
        df = binary_op_df(spark, long_gen)
        df.createOrReplaceTempView(tmpview)
        spark.sql("CREATE TABLE {} USING ICEBERG ".format(table) + _NO_FANOUT + " AS SELECT * FROM {}".format(tmpview))
    with_cpu_session(setup_iceberg_table)
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : spark.sql("SELECT *, input_file_name() FROM {}".format(table)),
        conf={'spark.rapids.sql.format.parquet.reader.type': reader_type})


@iceberg
@ignore_order(local=True) # Iceberg plans with a thread pool and is not deterministic in file ordering
@pytest.mark.parametrize('reader_type', rapids_reader_types)
@pytest.mark.skipif(not is_iceberg_remote_catalog(), reason="Filecache is only meaningful with remote storage, skipping for local Hadoop filesystem")
def test_iceberg_read_with_filecache(spark_tmp_table_factory, reader_type):
    """Create a table on CPU, read it twice on GPU with file cache enabled,
    and verify both reads match the CPU result."""
    filecache_enabled = with_gpu_session(
        lambda spark: spark.conf.get("spark.rapids.filecache.enabled", "false"))
    assert filecache_enabled == "true", \
        "spark.rapids.filecache.enabled must be set to true to run this test"
    table = get_full_table_name(spark_tmp_table_factory)
    tmpview = spark_tmp_table_factory.get()
    def setup_iceberg_table(spark):
        df = binary_op_df(spark, long_gen)
        df.createOrReplaceTempView(tmpview)
        spark.sql(f"CREATE TABLE {table} USING ICEBERG {_NO_FANOUT} AS SELECT * FROM {tmpview}")
    with_cpu_session(setup_iceberg_table)
    query = f"SELECT * FROM {table}"
    cpu_result = with_cpu_session(lambda spark: spark.sql(query).collect())
    # Note: spark.rapids.filecache.enabled is a startup-only config, so it must
    # be set via PYSP_TEST_spark_rapids_filecache_enabled env var, not here.
    filecache_conf = {
        'spark.rapids.sql.format.parquet.reader.type': reader_type,
    }
    gpu_result_1 = with_gpu_session(lambda spark: spark.sql(query).collect(), conf=filecache_conf)
    gpu_result_2 = with_gpu_session(lambda spark: spark.sql(query).collect(), conf=filecache_conf)
    assert_equal_with_local_sort(cpu_result, gpu_result_1)
    assert_equal_with_local_sort(cpu_result, gpu_result_2)

@iceberg
@ignore_order(local=True) # Iceberg plans with a thread pool and is not deterministic in file ordering
@pytest.mark.parametrize('reader_type', rapids_reader_types)
def test_iceberg_parquet_read_from_url_encoded_path(spark_tmp_table_factory, reader_type):
    table = get_full_table_name(spark_tmp_table_factory)
    tmp_view = spark_tmp_table_factory.get()
    partition_gen = StringGen(pattern="(.|\n){1,10}", nullable=False)\
        .with_special_case('%29%3EtkiudF4%3C', 1000)\
        .with_special_case('%2F%23_v9kRtI%27', 1000)\
        .with_special_case('aK%2BAgI%21l8%3E', 1000)\
        .with_special_case('p%2Cmtx%3FCXMd', 1000)
    def setup_iceberg_table(spark):
        df = two_col_df(spark, long_gen, partition_gen).sortWithinPartitions('b')
        df.createOrReplaceTempView(tmp_view)
        spark.sql("CREATE TABLE {} USING ICEBERG PARTITIONED BY (b) ".format(table) + _NO_FANOUT + " AS SELECT * FROM {}".format(tmp_view))
    with_cpu_session(setup_iceberg_table)
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.sql("SELECT * FROM {}".format(table)),
        conf={'spark.rapids.sql.format.parquet.reader.type': reader_type})

@iceberg
@ignore_order(local=True) # Iceberg plans with a thread pool and is not deterministic in file ordering
@pytest.mark.parametrize('reader_type', rapids_reader_types)
def test_iceberg_read_metadata_columns_with_partition_evolution(spark_tmp_table_factory, reader_type):
    """
    Test reading Iceberg metadata columns (_file, _pos, _spec_id, _partition) with partition evolution.
    """
    table = get_full_table_name(spark_tmp_table_factory)
    tmpview = spark_tmp_table_factory.get()
    def setup_iceberg_table(spark):
        # Create table partitioned by a
        df = three_col_df(spark, long_gen, int_gen, string_gen)
        df.createOrReplaceTempView(tmpview)
        spark.sql(f"CREATE TABLE {table} (a BIGINT, b INT, c STRING) USING ICEBERG PARTITIONED BY (a) {_NO_FANOUT}")
        spark.sql(f"INSERT INTO {table} SELECT * FROM {tmpview}")
        
        # Evolve partition: add b as partition field
        spark.sql(f"ALTER TABLE {table} ADD PARTITION FIELD b")
        
        # Insert more data after partition evolution
        df = three_col_df(spark, long_gen, int_gen, string_gen, seed=1)
        df.createOrReplaceTempView(tmpview)
        spark.sql(f"INSERT INTO {table} SELECT * FROM {tmpview}")
        
        # Evolve partition again: drop a, keep b
        spark.sql(f"ALTER TABLE {table} DROP PARTITION FIELD a")
        
        # Insert more data after second partition evolution
        df = three_col_df(spark, long_gen, int_gen, string_gen, seed=2)
        df.createOrReplaceTempView(tmpview)
        spark.sql(f"INSERT INTO {table} SELECT * FROM {tmpview}")
    
    with_cpu_session(setup_iceberg_table)
    
    # Test reading all metadata columns along with data columns
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.sql(f"SELECT a, b, c, _file, _pos, _spec_id, _partition FROM {table}"),
        conf={'spark.rapids.sql.format.parquet.reader.type': reader_type})


@iceberg
@ignore_order(local=True)
@pytest.mark.parametrize('reader_type', rapids_reader_types)
def test_iceberg_read_pos_with_split_file(spark_tmp_table_factory, reader_type):
    # Writes a single Parquet data file containing many row groups, then forces
    # Iceberg's planner to split that file across multiple scan tasks at row-group
    # byte boundaries via a tiny row-group size, a tiny split target, and a zero
    # per-file open cost. The query projects Iceberg's _pos metadata column; the
    # GPU result must match CPU regardless of how the planner splits the file.
    # Note: with the COALESCING reader_type, GpuReaderFactory routes _pos scans
    # away from the coalescing reader (canUseCoalescing excludes
    # hasRowPositionMetadata), so that parametrization actually exercises the
    # per-file/multi-thread path. The split-file _pos correctness assertion still
    # holds in whichever reader is chosen.
    table = get_full_table_name(spark_tmp_table_factory)
    def setup_iceberg_table(spark):
        spark.sql(f"CREATE TABLE {table} (id BIGINT) USING ICEBERG {_NO_FANOUT}")
        spark.sql(
            f"ALTER TABLE {table} SET TBLPROPERTIES ("
            "'write.parquet.row-group-size-bytes' = '4096', "
            "'read.split.target-size'             = '4096', "
            "'read.split.open-file-cost'          = '0')")
        spark.range(0, 1500).coalesce(1).writeTo(table).append()
    with_cpu_session(setup_iceberg_table)
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.sql(f"SELECT id, _pos FROM {table}"),
        conf={'spark.rapids.sql.format.parquet.reader.type': reader_type})


@iceberg
@ignore_order(local=True)
@pytest.mark.parametrize('reader_type', rapids_reader_types)
def test_iceberg_read_mor_with_pos_deletes_split_file(spark_tmp_table_factory, reader_type):
    # Same split-file conditions as test_iceberg_read_pos_with_split_file, but on
    # a v2 Merge-on-Read table with positional delete files. The query does not
    # project _pos; the Iceberg reader still adds it internally to match against
    # the positional delete list. The test deletes a scattered subset by id and
    # asserts CPU and GPU return the same surviving rows.
    # Note: the COALESCING reader_type does not actually exercise the coalescing
    # reader here either — GpuReaderFactory.canUseCoalescing already excludes any
    # scan with delete files (`hasNoDeletes` is false), so the test runs against
    # the per-file/multi-thread reader regardless of the requested reader_type.
    table = get_full_table_name(spark_tmp_table_factory)
    def setup_iceberg_table(spark):
        spark.sql(
            f"CREATE TABLE {table} (id BIGINT) USING ICEBERG TBLPROPERTIES ("
            "'format-version'                     = '2', "
            "'write.delete.mode'                  = 'merge-on-read', "
            "'write.spark.fanout.enabled'         = 'false', "
            "'write.parquet.row-group-size-bytes' = '4096', "
            "'read.split.target-size'             = '4096', "
            "'read.split.open-file-cost'          = '0')")
        spark.range(0, 1500).coalesce(1).writeTo(table).append()
        spark.sql(f"DELETE FROM {table} WHERE id % 7 = 3")
    with_cpu_session(setup_iceberg_table)
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.sql(f"SELECT id FROM {table}"),
        conf={'spark.rapids.sql.format.parquet.reader.type': reader_type})


@iceberg
@ignore_order(local=True)
@pytest.mark.parametrize('reader_type', rapids_reader_types)
def test_iceberg_small_file_combine_with_schema_evolution(spark_tmp_table_factory, reader_type):
    table = get_full_table_name(spark_tmp_table_factory)
    schema_evolution_gens_v1 = [('a', long_gen), ('b', int_gen)]
    schema_evolution_gens_v2 = schema_evolution_gens_v1 + [('c', string_gen)]
    base_seed = get_datagen_seed()
    create_iceberg_table(
        table,
        partition_col_sql='bucket(2, a)',
        df_gen=lambda spark: gen_df(spark, schema_evolution_gens_v1))

    def setup_iceberg_table(spark):
        for seed_offset in range(4):
            gen_df(
                spark,
                schema_evolution_gens_v1,
                length=64,
                seed=base_seed + seed_offset,
                num_slices=1).writeTo(table).append()

        spark.sql(f"ALTER TABLE {table} ADD COLUMN c STRING")
        for seed_offset in range(4):
            gen_df(
                spark,
                schema_evolution_gens_v2,
                length=64,
                seed=base_seed + 100 + seed_offset,
                num_slices=1).writeTo(table).append()

        spark.sql(
            f"ALTER TABLE {table} SET TBLPROPERTIES ("
            "'read.split.target-size' = '268435456', "
            "'read.split.planning-lookback' = '100')")
        spark.sql(f"REFRESH TABLE {table}")

    with_cpu_session(setup_iceberg_table)

    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.sql(f"SELECT a, b, c FROM {table}"),
        conf={'spark.rapids.sql.format.parquet.reader.type': reader_type})


@iceberg
@ignore_order(local=True)
@pytest.mark.parametrize('reader_type', rapids_reader_types)
def test_iceberg_small_file_combine_with_partition_spec_evolution(
        spark_tmp_table_factory, reader_type):
    table = get_full_table_name(spark_tmp_table_factory)
    partition_evolution_gens = [('a', long_gen), ('b', int_gen), ('c', string_gen)]
    base_seed = get_datagen_seed()
    create_iceberg_table(
        table,
        partition_col_sql='bucket(10, a)',
        df_gen=lambda spark: gen_df(spark, partition_evolution_gens))

    def setup_iceberg_table(spark):
        for seed_offset in range(4):
            gen_df(
                spark,
                partition_evolution_gens,
                length=64,
                seed=base_seed + seed_offset,
                num_slices=1).writeTo(table).append()

        spark.sql(f"ALTER TABLE {table} ADD PARTITION FIELD bucket(10, b)")
        for seed_offset in range(4):
            gen_df(
                spark,
                partition_evolution_gens,
                length=64,
                seed=base_seed + 100 + seed_offset,
                num_slices=1).writeTo(table).append()

        spark.sql(f"ALTER TABLE {table} DROP PARTITION FIELD bucket(10, a)")
        for seed_offset in range(4):
            gen_df(
                spark,
                partition_evolution_gens,
                length=64,
                seed=base_seed + 200 + seed_offset,
                num_slices=1).writeTo(table).append()

        spark.sql(
            f"ALTER TABLE {table} SET TBLPROPERTIES ("
            "'read.split.target-size' = '268435456', "
            "'read.split.planning-lookback' = '100')")
        spark.sql(f"REFRESH TABLE {table}")

    with_cpu_session(setup_iceberg_table)

    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.sql(f"SELECT a, b, c, _spec_id, _partition FROM {table}"),
        conf={'spark.rapids.sql.format.parquet.reader.type': reader_type})


@iceberg
@ignore_order(local=True)
@pytest.mark.parametrize('reader_type', rapids_reader_types)
@pytest.mark.skipif(is_iceberg_remote_catalog(), reason = "S3tables catalog is managed")
def test_iceberg_small_file_combine_with_add_files_identity_partition(
        spark_tmp_table_factory, reader_type):
    target_table = get_full_table_name(spark_tmp_table_factory)
    source_table = get_full_table_name(spark_tmp_table_factory)
    create_iceberg_table(
        target_table,
        partition_col_sql='a',
        df_gen=lambda spark: spark.createDataFrame([], 'a long, b string'))

    def setup_imported_table(spark):
        spark.sql(
            f"CREATE TABLE {source_table} (a BIGINT, b STRING) "
            "USING PARQUET "
            "PARTITIONED BY (a)")
        source_columns = spark.table(source_table).columns

        partition_values = [2451350, 2452349, 2452323]
        for batch_id in range(4):
            batch_gens = [
                ('a', RepeatSeqGen(partition_values * 2, data_type=long_gen.data_type)),
                ('b', RepeatSeqGen(
                    [f"batch-{batch_id}-row-{row_idx}" for row_idx in range(6)],
                    data_type=string_gen.data_type))
            ]
            (gen_df(
                spark,
                batch_gens,
                length=6,
                num_slices=1)
                .select(*source_columns)
                .write
                .mode('append')
                .insertInto(source_table))

        spark.sql(
            f"CALL spark_catalog.system.add_files("
            f"table => '{target_table}', "
            f"source_table => '{source_table}')")
        spark.sql(
            f"ALTER TABLE {target_table} SET TBLPROPERTIES ("
            "'read.split.target-size' = '268435456', "
            "'read.split.planning-lookback' = '100')")
        spark.sql(f"REFRESH TABLE {target_table}")

    with_cpu_session(setup_imported_table)

    # Imported partitioned Parquet files materialize `a` from the path, not the file payload.
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.sql(f"SELECT a, b FROM {target_table}"),
        conf={'spark.rapids.sql.format.parquet.reader.type': reader_type})
