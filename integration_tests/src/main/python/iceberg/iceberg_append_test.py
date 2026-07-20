# Copyright (c) 2025-2026, NVIDIA CORPORATION.
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
from typing import Callable, Any

import pytest

from asserts import assert_equal_with_local_sort, assert_gpu_fallback_collect
from conftest import is_iceberg_remote_catalog
from data_gen import gen_df, copy_and_update
from iceberg import create_iceberg_table, \
    iceberg_base_table_cols, iceberg_gens_list, get_full_table_name, \
    iceberg_full_gens_list, \
    iceberg_write_enabled_conf, iceberg_unsupported_mark, _build_tblprops, \
    full_coverage_partition_transforms, assert_iceberg_files_use_codec
from marks import iceberg, ignore_order, allow_non_gpu, datagen_overrides
from spark_session import with_gpu_session, with_cpu_session

pytestmark = iceberg_unsupported_mark


def do_test_insert_into_table_sql(spark_tmp_table_factory,
                                  create_table_func: Callable[[str], Any]):
    base_table_name = get_full_table_name(spark_tmp_table_factory)
    cpu_table_name = f"{base_table_name}_cpu"
    gpu_table_name = f"{base_table_name}_gpu"

    create_table_func(cpu_table_name)
    create_table_func(gpu_table_name)

    def insert_data(spark, table_name: str):
        df = gen_df(spark, list(zip(iceberg_base_table_cols, iceberg_gens_list)))
        view_name = spark_tmp_table_factory.get()
        df.createOrReplaceTempView(view_name)
        spark.sql(f"INSERT INTO {table_name} SELECT * FROM {view_name}")

    with_gpu_session(lambda spark: insert_data(spark, gpu_table_name),
                     conf = iceberg_write_enabled_conf)
    with_cpu_session(lambda spark: insert_data(spark, cpu_table_name),
                     conf = iceberg_write_enabled_conf)

    cpu_data = with_cpu_session(lambda spark: spark.table(cpu_table_name).collect())
    gpu_data = with_cpu_session(lambda spark: spark.table(gpu_table_name).collect())
    assert_equal_with_local_sort(cpu_data, gpu_data)


@iceberg
@ignore_order(local=True)
def test_insert_into_unpartitioned_table(spark_tmp_table_factory):
    table_prop = {"format-version": "2"}

    do_test_insert_into_table_sql(
        spark_tmp_table_factory,
        lambda table_name: create_iceberg_table(table_name, table_prop=table_prop))

@iceberg
@ignore_order(local=True)
@allow_non_gpu('AppendDataExec')
@pytest.mark.skipif(is_iceberg_remote_catalog(), reason="Skip for remote catalog to reduce test time")
@pytest.mark.parametrize("partition_table", [True, False], ids=lambda x: f"partition_table={x}")
def test_insert_into_unpartitioned_table_values(spark_tmp_table_factory,
                                                partition_table):
    base_table_name = get_full_table_name(spark_tmp_table_factory)
    cpu_table_name = f"{base_table_name}_cpu"
    gpu_table_name = f"{base_table_name}_gpu"

    def create_table(spark, table_name: str):
        props = _build_tblprops({"format-version": "2"})
        props_sql = ", ".join(f"'{k}' = '{v}'" for k, v in props.items())
        sql = f"CREATE TABLE {table_name} (id int, name string) USING ICEBERG "
        if partition_table:
            sql += "PARTITIONED BY (bucket(8, id)) "
        sql += f"TBLPROPERTIES ({props_sql})"
        spark.sql(sql)

    with_cpu_session(lambda spark: create_table(spark, cpu_table_name))
    with_cpu_session(lambda spark: create_table(spark, gpu_table_name))

    def insert_data(spark, table_name: str):
        spark.sql(f"INSERT INTO {table_name} VALUES (1, 'a'), (2, 'b'), (3, 'c')")

    # TODO(#14319): re-enable AQE for this test once the AQE+Iceberg write plan mismatch
    # is fixed. Tracking: https://github.com/NVIDIA/spark-rapids/issues/14319
    conf = copy_and_update(iceberg_write_enabled_conf, {'spark.sql.adaptive.enabled': 'false'})

    with_gpu_session(lambda spark: insert_data(spark, gpu_table_name),
                     conf=conf)
    with_cpu_session(lambda spark: insert_data(spark, cpu_table_name),
                     conf=conf)

    cpu_data = with_cpu_session(lambda spark: spark.table(cpu_table_name).collect())
    gpu_data = with_cpu_session(lambda spark: spark.table(gpu_table_name).collect())
    assert_equal_with_local_sort(cpu_data, gpu_data)


@iceberg
@ignore_order(local=True)
@allow_non_gpu('LocalTableScanExec', 'ShuffleExchangeExec')
@pytest.mark.skipif(is_iceberg_remote_catalog(), reason="Skip for remote catalog to reduce test time")
@pytest.mark.parametrize("partition_table", [True, False], ids=lambda x: f"partition_table={x}")
def test_insert_into_table_values_aqe(spark_tmp_table_factory, partition_table):
    """Regression test for GPU V2 writes with AQE and a CPU VALUES input plan."""
    base_table_name = get_full_table_name(spark_tmp_table_factory)
    cpu_table_name = f"{base_table_name}_cpu"
    gpu_table_name = f"{base_table_name}_gpu"

    def create_table(spark, table_name: str):
        props = _build_tblprops({"format-version": "2"})
        props_sql = ", ".join(f"'{k}' = '{v}'" for k, v in props.items())
        sql = f"CREATE TABLE {table_name} (id int, name string) USING ICEBERG "
        if partition_table:
            sql += "PARTITIONED BY (bucket(8, id)) "
        sql += f"TBLPROPERTIES ({props_sql})"
        spark.sql(sql)

    with_cpu_session(lambda spark: create_table(spark, cpu_table_name))
    with_cpu_session(lambda spark: create_table(spark, gpu_table_name))

    def insert_data(spark, table_name: str):
        spark.sql(f"INSERT INTO {table_name} VALUES (1, 'a'), (2, 'b'), (3, 'c')")

    conf = copy_and_update(iceberg_write_enabled_conf, {
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true"
    })

    with_gpu_session(lambda spark: insert_data(spark, gpu_table_name), conf=conf)
    with_cpu_session(lambda spark: insert_data(spark, cpu_table_name), conf=conf)

    cpu_data = with_cpu_session(lambda spark: spark.table(cpu_table_name).collect())
    gpu_data = with_cpu_session(lambda spark: spark.table(gpu_table_name).collect())
    assert_equal_with_local_sort(cpu_data, gpu_data)


@iceberg
@ignore_order(local=True)
@pytest.mark.skipif(is_iceberg_remote_catalog(), reason="Skip for remote catalog to reduce test time")
def test_insert_into_unpartitioned_table_all_cols(spark_tmp_table_factory):
    table_prop = {"format-version": "2"}
    cols = [f"_c{idx}" for idx, _ in enumerate(iceberg_full_gens_list)]
    gen_list = list(zip(cols, iceberg_full_gens_list))

    def this_gen_df(spark):
        return gen_df(spark, gen_list)

    def insert_data(spark, table_name: str):
        df = this_gen_df(spark)
        view_name = spark_tmp_table_factory.get()
        df.createOrReplaceTempView(view_name)
        return spark.sql(f"INSERT INTO {table_name} SELECT * FROM {view_name}")

    base_table_name = get_full_table_name(spark_tmp_table_factory)
    cpu_table_name = f"{base_table_name}_cpu"
    gpu_table_name = f"{base_table_name}_gpu"

    create_iceberg_table(cpu_table_name, table_prop=table_prop, df_gen=this_gen_df)
    create_iceberg_table(gpu_table_name, table_prop=table_prop, df_gen=this_gen_df)

    with_gpu_session(lambda spark: insert_data(spark, gpu_table_name).collect(),
                     conf=iceberg_write_enabled_conf)
    with_cpu_session(lambda spark: insert_data(spark, cpu_table_name).collect(),
                     conf=iceberg_write_enabled_conf)

    cpu_data = with_cpu_session(lambda spark: spark.table(cpu_table_name).collect())
    gpu_data = with_cpu_session(lambda spark: spark.table(gpu_table_name).collect())
    assert_equal_with_local_sort(cpu_data, gpu_data)


def _do_test_insert_into_partitioned_table(spark_tmp_table_factory, partition_col_sql,
                                           table_prop=None):
    """Helper function for partitioned table insert tests."""
    if table_prop is None:
        table_prop = {"format-version": "2"}

    def create_table_and_set_write_order(table_name: str):
        create_iceberg_table(
            table_name,
            partition_col_sql=partition_col_sql,
            table_prop=table_prop)

        sql = f"ALTER TABLE {table_name} WRITE ORDERED BY _c2, _c3, _c4"
        with_cpu_session(lambda spark: spark.sql(sql).collect())

    do_test_insert_into_table_sql(
        spark_tmp_table_factory,
        create_table_and_set_write_order)


@iceberg
@datagen_overrides(seed=0, reason='https://github.com/NVIDIA/spark-rapids-jni/issues/4016')
@ignore_order(local=True)
@pytest.mark.parametrize("partition_col_sql", [
    pytest.param("year(_c9)", id="year(timestamp_col)"),
])
def test_insert_into_partitioned_table(spark_tmp_table_factory, partition_col_sql):
    """Basic partition test - runs for all catalogs including remote."""
    _do_test_insert_into_partitioned_table(spark_tmp_table_factory, partition_col_sql)


@iceberg
@datagen_overrides(seed=0, reason='https://github.com/NVIDIA/spark-rapids-jni/issues/4016')
@ignore_order(local=True)
@pytest.mark.skipif(is_iceberg_remote_catalog(), reason="Skip for remote catalog to reduce test time")
@pytest.mark.parametrize("partition_col_sql", full_coverage_partition_transforms)
def test_insert_into_partitioned_table_full_coverage(spark_tmp_table_factory, partition_col_sql):
    """Partition-transform coverage anchor: this is the single test that exercises
    the partition writer against every transform in full_coverage_partition_transforms.
    Every other DML op's _full_coverage test picks only a few distinct transforms
    from this list (via ctas_/rtas_/overwrite_*_/delete_/update_/merge_-prefixed
    constants in iceberg/__init__.py), so the partition writer's coverage of all
    26 transforms lives here. Skipped for remote catalogs."""
    _do_test_insert_into_partitioned_table(spark_tmp_table_factory, partition_col_sql)

@iceberg
@ignore_order(local=True)
@pytest.mark.skipif(is_iceberg_remote_catalog(), reason="Skip for remote catalog to reduce test time")
def test_insert_into_partitioned_table_all_cols(spark_tmp_table_factory):
    table_prop = {"format-version": "2"}
    cols = [f"_c{idx}" for idx, _ in enumerate(iceberg_full_gens_list)]
    gen_list = list(zip(cols, iceberg_full_gens_list))

    def this_gen_df(spark):
        return gen_df(spark, gen_list)

    def insert_data(spark, table_name: str):
        df = this_gen_df(spark)
        view_name = spark_tmp_table_factory.get()
        df.createOrReplaceTempView(view_name)
        return spark.sql(f"INSERT INTO {table_name} SELECT * FROM {view_name}")

    base_table_name = get_full_table_name(spark_tmp_table_factory)
    cpu_table_name = f"{base_table_name}_cpu"
    gpu_table_name = f"{base_table_name}_gpu"

    create_iceberg_table(cpu_table_name,
                          partition_col_sql="bucket(16, _c2), bucket(16, _c3)",
                          table_prop=table_prop,
                          df_gen=this_gen_df)
    create_iceberg_table(gpu_table_name,
                          partition_col_sql="bucket(16, _c2), bucket(16, _c3)",
                          table_prop=table_prop,
                          df_gen=this_gen_df)

    # TODO(#14319): re-enable AQE for this test once the AQE+Iceberg write plan mismatch
    # is fixed. Tracking: https://github.com/NVIDIA/spark-rapids/issues/14319
    conf = copy_and_update(iceberg_write_enabled_conf, {'spark.sql.adaptive.enabled': 'false'})

    with_gpu_session(lambda spark: insert_data(spark, gpu_table_name).collect(),
                     conf=conf)
    with_cpu_session(lambda spark: insert_data(spark, cpu_table_name).collect(),
                     conf=conf)

    cpu_data = with_cpu_session(lambda spark: spark.table(cpu_table_name).collect())
    gpu_data = with_cpu_session(lambda spark: spark.table(gpu_table_name).collect())
    assert_equal_with_local_sort(cpu_data, gpu_data)


@iceberg
@ignore_order(local=True)
@allow_non_gpu('AppendDataExec', 'ShuffleExchangeExec', 'ProjectExec')
@pytest.mark.skipif(is_iceberg_remote_catalog(), reason="Skip for remote catalog to reduce test time")
@pytest.mark.parametrize("file_format", ["orc", "avro"], ids=lambda x: f"file_format={x}")
def test_insert_into_table_unsupported_file_format_fallback(
        spark_tmp_table_factory, file_format):
    table_prop = {"format-version": "2",
                  "write.format.default": file_format}

    def insert_data(spark, table_name: str):
        df = gen_df(spark, list(zip(iceberg_base_table_cols, iceberg_gens_list)))
        view_name = spark_tmp_table_factory.get()
        df.createOrReplaceTempView(view_name)
        return spark.sql(f"INSERT INTO {table_name} SELECT * FROM {view_name}")

    table_name = get_full_table_name(spark_tmp_table_factory)
    create_iceberg_table(table_name, table_prop=table_prop)

    assert_gpu_fallback_collect(lambda spark: insert_data(spark, table_name),
                                "AppendDataExec",
                                conf = iceberg_write_enabled_conf)

@iceberg
@ignore_order(local=True)
@allow_non_gpu('AppendDataExec', 'ShuffleExchangeExec', 'ProjectExec')
@pytest.mark.skipif(is_iceberg_remote_catalog(), reason="Skip for remote catalog to reduce test time")
@pytest.mark.parametrize("partition_col_sql", [
    pytest.param("bucket(4, contact.email)", id="bucket_nested_struct_field"),
    pytest.param("truncate(3, contact.email)", id="truncate_nested_struct_field"),
    pytest.param("contact.email", id="identity_nested_struct_field"),
], )
def test_insert_into_table_nested_partition_source_fallback(
        spark_tmp_table_factory, partition_col_sql):
    table_name = get_full_table_name(spark_tmp_table_factory)
    table_prop = _build_tblprops({"format-version": "2"})
    props_sql = ", ".join(f"'{k}' = '{v}'" for k, v in table_prop.items())

    def create_table(spark):
        spark.sql(
            f"CREATE TABLE {table_name} "
            f"(id BIGINT, contact STRUCT<email: STRING, phone: STRING>) "
            f"USING ICEBERG "
            f"PARTITIONED BY ({partition_col_sql}) "
            f"TBLPROPERTIES ({props_sql})")

    with_cpu_session(create_table)

    def insert_data(spark):
        return spark.sql(
            f"INSERT INTO {table_name} VALUES "
            f"(1, named_struct('email', 'a@x.test', 'phone', '111')), "
            f"(2, named_struct('email', 'b@y.test', 'phone', '222'))")

    assert_gpu_fallback_collect(lambda spark: insert_data(spark),
                                "AppendDataExec",
                                conf=iceberg_write_enabled_conf)


@iceberg
@ignore_order(local=True)
@allow_non_gpu('AppendDataExec', 'ShuffleExchangeExec', 'ProjectExec')
@pytest.mark.skipif(is_iceberg_remote_catalog(), reason="Skip for remote catalog to reduce test time")
@pytest.mark.parametrize("conf_key", ["spark.rapids.sql.format.iceberg.enabled",
                                      "spark.rapids.sql.format.iceberg.write.enabled"],
                         ids=lambda x: f"{x}=False")
def test_insert_into_iceberg_table_fallback_when_conf_disabled(
        spark_tmp_table_factory, conf_key):
    table_prop = {"format-version": "2"}

    def insert_data(spark, table_name: str):
        df = gen_df(spark, list(zip(iceberg_base_table_cols, iceberg_gens_list)))
        view_name = spark_tmp_table_factory.get()
        df.createOrReplaceTempView(view_name)
        return spark.sql(f"INSERT INTO {table_name} SELECT * FROM {view_name}")

    table_name = get_full_table_name(spark_tmp_table_factory)
    create_iceberg_table(table_name, table_prop=table_prop)

    updated_conf = copy_and_update(iceberg_write_enabled_conf, {conf_key: "false"})
    assert_gpu_fallback_collect(lambda spark: insert_data(spark, table_name),
                                "AppendDataExec",
                                conf = updated_conf)


@iceberg
@ignore_order(local=True)
@pytest.mark.parametrize("partition_col_sql", [
    pytest.param(None, id="unpartitioned"),
    pytest.param("year(_c9)", id="year_partition"),
])
def test_insert_into_aqe(spark_tmp_table_factory, partition_col_sql):
    """
    Test INSERT INTO with AQE enabled.
    """
    table_prop = {"format-version": "2"}

    # Configuration with AQE enabled
    conf = copy_and_update(iceberg_write_enabled_conf, {
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true"
    })

    base_table_name = get_full_table_name(spark_tmp_table_factory)
    cpu_table_name = f"{base_table_name}_cpu"
    gpu_table_name = f"{base_table_name}_gpu"

    # Create tables
    with_cpu_session(lambda spark: create_iceberg_table(
        cpu_table_name, partition_col_sql, table_prop,
        lambda sp: gen_df(sp, list(zip(iceberg_base_table_cols, iceberg_gens_list)))))
    with_cpu_session(lambda spark: create_iceberg_table(
        gpu_table_name, partition_col_sql, table_prop,
        lambda sp: gen_df(sp, list(zip(iceberg_base_table_cols, iceberg_gens_list)))))

    # Insert data
    def insert_data(spark, table_name: str):
        df = gen_df(spark, list(zip(iceberg_base_table_cols, iceberg_gens_list)))
        view_name = spark_tmp_table_factory.get()
        df.createOrReplaceTempView(view_name)
        spark.sql(f"INSERT INTO {table_name} SELECT * FROM {view_name}")

    with_gpu_session(lambda spark: insert_data(spark, gpu_table_name), conf=conf)
    with_cpu_session(lambda spark: insert_data(spark, cpu_table_name), conf=conf)

    cpu_data = with_cpu_session(lambda spark: spark.table(cpu_table_name).collect())
    gpu_data = with_cpu_session(lambda spark: spark.table(gpu_table_name).collect())
    assert_equal_with_local_sort(cpu_data, gpu_data)


@iceberg
@ignore_order(local=True)
@pytest.mark.skipif(is_iceberg_remote_catalog(), reason="Skip for remote catalog to reduce test time")
def test_insert_after_drop_partition_field(spark_tmp_table_factory):
    """Test INSERT on table after dropping a partition field (void transform).
    
    When a partition field is dropped, Iceberg creates a 'void transform' - 
    the field remains in the partition spec but no longer affects partitioning.
    This test verifies INSERT still runs correctly on GPU after partition evolution.
    """
    base_table_name = get_full_table_name(spark_tmp_table_factory)
    cpu_table_name = f"{base_table_name}_cpu"
    gpu_table_name = f"{base_table_name}_gpu"
    
    table_prop = {"format-version": "2"}
    # Use two partition columns so after dropping one, we still have at least one
    partition_col_sql = "bucket(8, _c2), bucket(8, _c3)"
    
    # Create partitioned tables
    create_iceberg_table(cpu_table_name, partition_col_sql=partition_col_sql, table_prop=table_prop)
    create_iceberg_table(gpu_table_name, partition_col_sql=partition_col_sql, table_prop=table_prop)
    
    # Insert initial data before partition evolution using same seed for both tables
    def insert_initial_data(spark, table_name):
        df = gen_df(spark, list(zip(iceberg_base_table_cols, iceberg_gens_list)), seed=42)
        df.writeTo(table_name).append()
    
    with_cpu_session(lambda spark: insert_initial_data(spark, cpu_table_name))
    with_cpu_session(lambda spark: insert_initial_data(spark, gpu_table_name))
    
    # Drop one partition field on both tables (creates void transform)
    def drop_partition_field(spark, table_name):
        spark.sql(f"ALTER TABLE {table_name} DROP PARTITION FIELD bucket(8, _c2)")
    
    with_cpu_session(lambda spark: drop_partition_field(spark, cpu_table_name))
    with_cpu_session(lambda spark: drop_partition_field(spark, gpu_table_name))
    
    # Insert data after partition evolution - this is the operation we're testing on GPU
    # Use same seed for both sessions to ensure identical data
    def insert_data(spark, table_name):
        df = gen_df(spark, list(zip(iceberg_base_table_cols, iceberg_gens_list)), seed=43)
        view_name = spark_tmp_table_factory.get()
        df.createOrReplaceTempView(view_name)
        spark.sql(f"INSERT INTO {table_name} SELECT * FROM {view_name}")
    
    with_gpu_session(lambda spark: insert_data(spark, gpu_table_name),
                     conf=iceberg_write_enabled_conf)
    with_cpu_session(lambda spark: insert_data(spark, cpu_table_name),
                     conf=iceberg_write_enabled_conf)
    
    # Compare results
    cpu_data = with_cpu_session(lambda spark: spark.table(cpu_table_name).collect())
    gpu_data = with_cpu_session(lambda spark: spark.table(gpu_table_name).collect())
    assert_equal_with_local_sort(cpu_data, gpu_data)


@iceberg
@datagen_overrides(seed=0, reason='https://github.com/NVIDIA/spark-rapids-jni/issues/4016')
@ignore_order(local=True)
@pytest.mark.skipif(is_iceberg_remote_catalog(), reason="Skip for remote catalog to reduce test time")
def test_insert_into_partitioned_table_fanout_enabled(spark_tmp_table_factory):
    # Use bucket(2, ...) to keep partition count low and avoid OOM from Iceberg's FanoutDataWriter.
    _do_test_insert_into_partitioned_table(
        spark_tmp_table_factory, "bucket(2, _c9)",
        table_prop={"format-version": "2", "write.spark.fanout.enabled": "true"})


# Regression for https://github.com/NVIDIA/spark-rapids/issues/14905 — the GPU writer
# must honor Iceberg's resolved codec (`write.parquet.compression-codec`, default zstd)
# and ignore `spark.sql.parquet.compression.codec`, matching CPU Iceberg behavior.
# The default branch (table property unset) is the key regression case: pre-fix the
# GPU would silently use Spark's session codec instead of Iceberg's zstd default.
@iceberg
@ignore_order(local=True)
@pytest.mark.skipif(is_iceberg_remote_catalog(), reason="Skip for remote catalog to reduce test time")
# Restricted to codecs whose footer metadata is reliable on small inputs. cuDF skips
# compression for tiny row groups (rapidsai/cudf#14017), so codecs like snappy can leave
# `UNCOMPRESSED` in the footer of small per-task files and make the assertion flaky;
# zstd is the codec the issue is really about (Iceberg's default), so cover that and
# the uncompressed identity case.
@pytest.mark.parametrize("table_codec,expected_codec", [
    (None, "zstd"),     # No override: Iceberg's default (zstd) must win on GPU too.
    ("zstd", "zstd"),
    ("uncompressed", "uncompressed")])
def test_insert_into_table_honors_iceberg_compression_codec(
        spark_tmp_table_factory, table_codec, expected_codec):
    table_name = get_full_table_name(spark_tmp_table_factory)

    extra_props = {"format-version": "2"}
    if table_codec is not None:
        extra_props["write.parquet.compression-codec"] = table_codec

    def create_table(spark):
        props = _build_tblprops(extra_props)
        props_sql = ", ".join(f"'{k}' = '{v}'" for k, v in props.items())
        spark.sql(f"CREATE TABLE {table_name} (id int, name string) USING ICEBERG "
                  f"TBLPROPERTIES ({props_sql})")

    with_cpu_session(create_table)

    # Set the Spark-level codec to something different from what Iceberg should resolve to;
    # the pre-fix bug was that Spark's value leaked through and overrode Iceberg's choice.
    leaky_codec = "snappy" if expected_codec != "snappy" else "uncompressed"
    conf = copy_and_update(iceberg_write_enabled_conf,
                           {"spark.sql.parquet.compression.codec": leaky_codec})

    # cuDF skips compression for tiny row groups (see rapidsai/cudf#14017), so use
    # enough rows that the codec actually appears in the footer column metadata.
    with_gpu_session(
        lambda spark: spark.sql(
            f"INSERT INTO {table_name} SELECT id, CAST(id AS STRING) FROM range(20000)").collect(),
        conf=conf)

    with_cpu_session(
        lambda spark: assert_iceberg_files_use_codec(spark, table_name, expected_codec))


# cuDF's Parquet writer only supports uncompressed/snappy/zstd. A table whose Iceberg-resolved
# codec is something else (e.g. gzip, a valid Iceberg codec) must fall back to the CPU at plan
# time rather than reaching prepareWrite and failing at execution. Regression for #14905.
@iceberg
@ignore_order(local=True)
@allow_non_gpu('AppendDataExec', 'ShuffleExchangeExec', 'ProjectExec')
@pytest.mark.skipif(is_iceberg_remote_catalog(), reason="Skip for remote catalog to reduce test time")
@pytest.mark.parametrize("codec", ["gzip", "lz4"])
def test_insert_into_table_falls_back_on_unsupported_codec(spark_tmp_table_factory, codec):
    table_name = get_full_table_name(spark_tmp_table_factory)
    create_iceberg_table(
        table_name,
        table_prop={"format-version": "2", "write.parquet.compression-codec": codec})

    def insert_data(spark):
        df = gen_df(spark, list(zip(iceberg_base_table_cols, iceberg_gens_list)))
        view_name = spark_tmp_table_factory.get()
        df.createOrReplaceTempView(view_name)
        return spark.sql(f"INSERT INTO {table_name} SELECT * FROM {view_name}")

    assert_gpu_fallback_collect(insert_data, "AppendDataExec", conf=iceberg_write_enabled_conf)
