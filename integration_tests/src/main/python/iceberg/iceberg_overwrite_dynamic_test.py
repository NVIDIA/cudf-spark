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
    iceberg_base_table_cols, iceberg_gens_list, \
    get_full_table_name, iceberg_full_gens_list, iceberg_nested_write_gens_list, \
    iceberg_write_enabled_conf, iceberg_unsupported_mark, \
    overwrite_dynamic_partition_transforms
from marks import iceberg, ignore_order, allow_non_gpu, allow_non_gpu_conditional, datagen_overrides
from spark_session import with_gpu_session, with_cpu_session, is_spark_400_or_later

pytestmark = iceberg_unsupported_mark


# Seed for initial data to ensure different data from overwrite operations
INITIAL_DATA_SEED = 42

# Configuration for dynamic partition overwrite mode
dynamic_overwrite_conf = copy_and_update(iceberg_write_enabled_conf, {
    "spark.sql.sources.partitionOverwriteMode": "dynamic"
})


def do_test_insert_overwrite_dynamic(spark_tmp_table_factory,
                                      create_table_func: Callable[[str], Any]):
    """Test INSERT OVERWRITE with dynamic partition overwrite mode."""
    base_table_name = get_full_table_name(spark_tmp_table_factory)
    cpu_table_name = f"{base_table_name}_cpu"
    gpu_table_name = f"{base_table_name}_gpu"

    create_table_func(cpu_table_name)
    create_table_func(gpu_table_name)

    def initial_insert(spark, table_name: str):
        """Insert initial data to have something to overwrite."""
        # Use a specific seed for initial data to ensure different data from overwrite
        df = gen_df(spark, list(zip(iceberg_base_table_cols, iceberg_gens_list)), seed=INITIAL_DATA_SEED)
        view_name = spark_tmp_table_factory.get()
        df.createOrReplaceTempView(view_name)
        spark.sql(f"INSERT INTO {table_name} SELECT * FROM {view_name}")

    # Insert initial data on both CPU and GPU
    with_gpu_session(lambda spark: initial_insert(spark, gpu_table_name),
                     conf=iceberg_write_enabled_conf)
    with_cpu_session(lambda spark: initial_insert(spark, cpu_table_name),
                     conf=iceberg_write_enabled_conf)

    def overwrite_data(spark, table_name: str):
        """Perform INSERT OVERWRITE with dynamic mode - only overwrites touched partitions."""
        # Use default seed for overwrite data - different from initial
        df = gen_df(spark, list(zip(iceberg_base_table_cols, iceberg_gens_list)))
        view_name = spark_tmp_table_factory.get()
        df.createOrReplaceTempView(view_name)
        spark.sql(f"INSERT OVERWRITE TABLE {table_name} SELECT * FROM {view_name}")

    # Perform overwrite on both tables with dynamic mode
    with_gpu_session(lambda spark: overwrite_data(spark, gpu_table_name),
                     conf=dynamic_overwrite_conf)
    with_cpu_session(lambda spark: overwrite_data(spark, cpu_table_name),
                     conf=dynamic_overwrite_conf)

    cpu_data = with_cpu_session(lambda spark: spark.table(cpu_table_name).collect())
    gpu_data = with_cpu_session(lambda spark: spark.table(gpu_table_name).collect())
    assert_equal_with_local_sort(cpu_data, gpu_data)


@iceberg
@ignore_order(local=True)
def test_insert_overwrite_dynamic_unpartitioned_table(spark_tmp_table_factory):
    """Test dynamic overwrite on unpartitioned tables - should run on GPU."""
    table_prop = {"format-version": "2"}

    do_test_insert_overwrite_dynamic(
        spark_tmp_table_factory,
        lambda table_name: create_iceberg_table(table_name, table_prop=table_prop))


def _do_test_insert_overwrite_dynamic_partitioned(spark_tmp_table_factory, partition_col_sql, table_prop=None):
    """Helper function for partitioned table dynamic overwrite tests."""
    if table_prop is None:
        table_prop = {"format-version": "2"}

    def create_table_with_partition(table_name: str):
        create_iceberg_table(
            table_name,
            partition_col_sql=partition_col_sql,
            table_prop=table_prop)

    do_test_insert_overwrite_dynamic(
        spark_tmp_table_factory,
        create_table_with_partition)


@iceberg
@datagen_overrides(seed=0, reason='https://github.com/NVIDIA/spark-rapids-jni/issues/4016')
@ignore_order(local=True)
@pytest.mark.parametrize("partition_col_sql", [
    pytest.param("year(_c9)", id="year(timestamp_col)"),
])
def test_insert_overwrite_dynamic_bucket_partitioned(spark_tmp_table_factory, partition_col_sql):
    """Basic partition test - runs for all catalogs including remote."""
    _do_test_insert_overwrite_dynamic_partitioned(spark_tmp_table_factory, partition_col_sql)


@iceberg
@datagen_overrides(seed=0, reason='https://github.com/NVIDIA/spark-rapids-jni/issues/4016')
@ignore_order(local=True)
@pytest.mark.skipif(is_iceberg_remote_catalog(), reason="Skip for remote catalog to reduce test time")
@pytest.mark.parametrize("partition_col_sql", overwrite_dynamic_partition_transforms)
@allow_non_gpu_conditional(is_spark_400_or_later(), "EmptyRelationExec")
def test_insert_overwrite_dynamic_bucket_partitioned_full_coverage(spark_tmp_table_factory, partition_col_sql):
    """Sanity-check dynamic INSERT OVERWRITE against a few partition transforms
    distinct from those picked by other DML ops. The 26-transform partition-writer
    coverage anchor lives in
    iceberg_append_test.py::test_insert_into_partitioned_table_full_coverage."""
    _do_test_insert_overwrite_dynamic_partitioned(spark_tmp_table_factory, partition_col_sql)


@iceberg
@ignore_order(local=True)
@pytest.mark.skipif(is_iceberg_remote_catalog(), reason="Skip for remote catalog to reduce test time")
def test_insert_overwrite_dynamic_nested_types(spark_tmp_table_factory):
    """Test INSERT OVERWRITE with dynamic mode on Iceberg-native nested types on GPU."""
    table_prop = {"format-version": "2"}
    cols = [f"_c{idx}" for idx, _ in enumerate(iceberg_nested_write_gens_list)]
    gen_list = list(zip(cols, iceberg_nested_write_gens_list))

    def this_gen_df(spark):
        return gen_df(spark, gen_list)

    base_table_name = get_full_table_name(spark_tmp_table_factory)
    cpu_table_name = f"{base_table_name}_cpu"
    gpu_table_name = f"{base_table_name}_gpu"

    create_iceberg_table(cpu_table_name, table_prop=table_prop, df_gen=this_gen_df)
    create_iceberg_table(gpu_table_name, table_prop=table_prop, df_gen=this_gen_df)

    def initial_insert(spark, table_name):
        df = gen_df(spark, gen_list, seed=INITIAL_DATA_SEED)
        view_name = spark_tmp_table_factory.get()
        df.createOrReplaceTempView(view_name)
        spark.sql(f"INSERT INTO {table_name} SELECT * FROM {view_name}")

    with_gpu_session(lambda spark: initial_insert(spark, gpu_table_name),
                     conf=iceberg_write_enabled_conf)
    with_cpu_session(lambda spark: initial_insert(spark, cpu_table_name),
                     conf=iceberg_write_enabled_conf)

    def overwrite_data(spark, table_name):
        df = this_gen_df(spark)
        view_name = spark_tmp_table_factory.get()
        df.createOrReplaceTempView(view_name)
        return spark.sql(f"INSERT OVERWRITE TABLE {table_name} SELECT * FROM {view_name}")

    with_gpu_session(lambda spark: overwrite_data(spark, gpu_table_name).collect(),
                     conf=dynamic_overwrite_conf)
    with_cpu_session(lambda spark: overwrite_data(spark, cpu_table_name).collect(),
                     conf=dynamic_overwrite_conf)

    cpu_data = with_cpu_session(lambda spark: spark.table(cpu_table_name).collect())
    gpu_data = with_cpu_session(lambda spark: spark.table(gpu_table_name).collect())
    assert_equal_with_local_sort(cpu_data, gpu_data)


@iceberg
@ignore_order(local=True)
@pytest.mark.skipif(is_iceberg_remote_catalog(), reason="Skip for remote catalog to reduce test time")
@allow_non_gpu_conditional(is_spark_400_or_later(), "EmptyRelationExec")
def test_insert_overwrite_dynamic_all_cols(spark_tmp_table_factory):
    """Test INSERT OVERWRITE with dynamic mode on all Iceberg write types on GPU."""
    table_prop = {"format-version": "2"}
    cols = [f"_c{idx}" for idx, _ in enumerate(iceberg_full_gens_list)]
    gen_list = list(zip(cols, iceberg_full_gens_list))

    def this_gen_df(spark):
        return gen_df(spark, gen_list)

    base_table_name = get_full_table_name(spark_tmp_table_factory)
    cpu_table_name = f"{base_table_name}_cpu"
    gpu_table_name = f"{base_table_name}_gpu"

    create_iceberg_table(cpu_table_name, table_prop=table_prop, df_gen=this_gen_df)
    create_iceberg_table(gpu_table_name, table_prop=table_prop, df_gen=this_gen_df)

    def initial_insert(spark, table_name):
        df = gen_df(spark, gen_list, seed=INITIAL_DATA_SEED)
        view_name = spark_tmp_table_factory.get()
        df.createOrReplaceTempView(view_name)
        spark.sql(f"INSERT INTO {table_name} SELECT * FROM {view_name}")

    with_gpu_session(lambda spark: initial_insert(spark, gpu_table_name),
                     conf=iceberg_write_enabled_conf)
    with_cpu_session(lambda spark: initial_insert(spark, cpu_table_name),
                     conf=iceberg_write_enabled_conf)

    def overwrite_data(spark, table_name):
        df = this_gen_df(spark)
        view_name = spark_tmp_table_factory.get()
        df.createOrReplaceTempView(view_name)
        return spark.sql(f"INSERT OVERWRITE TABLE {table_name} SELECT * FROM {view_name}")

    with_gpu_session(lambda spark: overwrite_data(spark, gpu_table_name).collect(),
                     conf=dynamic_overwrite_conf)
    with_cpu_session(lambda spark: overwrite_data(spark, cpu_table_name).collect(),
                     conf=dynamic_overwrite_conf)

    cpu_data = with_cpu_session(lambda spark: spark.table(cpu_table_name).collect())
    gpu_data = with_cpu_session(lambda spark: spark.table(gpu_table_name).collect())
    assert_equal_with_local_sort(cpu_data, gpu_data)


@iceberg
@ignore_order(local=True)
@allow_non_gpu('OverwritePartitionsDynamicExec', 'ShuffleExchangeExec', 'SortExec', 'ProjectExec')
@pytest.mark.skipif(is_iceberg_remote_catalog(), reason="Skip for remote catalog to reduce test time")
@pytest.mark.parametrize("file_format", ["orc", "avro"], ids=lambda x: f"file_format={x}")
@allow_non_gpu_conditional(is_spark_400_or_later(), "EmptyRelationExec")
def test_insert_overwrite_dynamic_unsupported_file_format_fallback(
        spark_tmp_table_factory, file_format):
    """Test that INSERT OVERWRITE falls back to CPU with unsupported file formats."""
    table_prop = {"format-version": "2",
                  "write.format.default": file_format}

    table_name = get_full_table_name(spark_tmp_table_factory)

    # Create table
    create_iceberg_table(table_name, table_prop=table_prop)

    # Initial insert (CPU only to set up the table)
    def initial_insert(spark, table_name: str):
        # Use a specific seed for initial data to ensure different data from overwrite
        df = gen_df(spark, list(zip(iceberg_base_table_cols, iceberg_gens_list)), seed=INITIAL_DATA_SEED)
        view_name = spark_tmp_table_factory.get()
        df.createOrReplaceTempView(view_name)
        spark.sql(f"INSERT INTO {table_name} SELECT * FROM {view_name}")

    with_cpu_session(lambda spark: initial_insert(spark, table_name),
                     conf=iceberg_write_enabled_conf)

    # Overwrite with dynamic mode (assert GPU falls back to CPU)
    def overwrite_data(spark, table_name: str):
        # Use default seed for overwrite data - different from initial
        df = gen_df(spark, list(zip(iceberg_base_table_cols, iceberg_gens_list)))
        view_name = spark_tmp_table_factory.get()
        df.createOrReplaceTempView(view_name)
        return spark.sql(f"INSERT OVERWRITE TABLE {table_name} SELECT * FROM {view_name}")

    assert_gpu_fallback_collect(lambda spark: overwrite_data(spark, table_name),
                                "OverwritePartitionsDynamicExec",
                                conf=dynamic_overwrite_conf)


@iceberg
@ignore_order(local=True)
@allow_non_gpu('OverwritePartitionsDynamicExec', 'ShuffleExchangeExec', 'SortExec', 'ProjectExec')
@pytest.mark.skipif(is_iceberg_remote_catalog(), reason="Skip for remote catalog to reduce test time")
@pytest.mark.parametrize("conf_key", ["spark.rapids.sql.format.iceberg.enabled",
                                      "spark.rapids.sql.format.iceberg.write.enabled"],
                         ids=lambda x: f"{x}=False")
def test_insert_overwrite_dynamic_fallback_when_conf_disabled(
        spark_tmp_table_factory, conf_key):
    """Test that INSERT OVERWRITE falls back to CPU when Iceberg write is disabled."""
    table_prop = {"format-version": "2"}

    table_name = get_full_table_name(spark_tmp_table_factory)

    # Create table
    create_iceberg_table(table_name, table_prop=table_prop)

    # Initial insert (CPU only to set up the table)
    def initial_insert(spark, table_name: str):
        # Use a specific seed for initial data to ensure different data from overwrite
        df = gen_df(spark, list(zip(iceberg_base_table_cols, iceberg_gens_list)), seed=INITIAL_DATA_SEED)
        view_name = spark_tmp_table_factory.get()
        df.createOrReplaceTempView(view_name)
        spark.sql(f"INSERT INTO {table_name} SELECT * FROM {view_name}")

    with_cpu_session(lambda spark: initial_insert(spark, table_name),
                     conf=iceberg_write_enabled_conf)

    # Overwrite with dynamic mode (assert GPU falls back to CPU)
    def overwrite_data(spark, table_name: str):
        # Use default seed for overwrite data - different from initial
        df = gen_df(spark, list(zip(iceberg_base_table_cols, iceberg_gens_list)))
        view_name = spark_tmp_table_factory.get()
        df.createOrReplaceTempView(view_name)
        return spark.sql(f"INSERT OVERWRITE TABLE {table_name} SELECT * FROM {view_name}")

    updated_conf = copy_and_update(dynamic_overwrite_conf, {conf_key: "false"})
    assert_gpu_fallback_collect(lambda spark: overwrite_data(spark, table_name),
                                "OverwritePartitionsDynamicExec",
                                conf=updated_conf)


@iceberg
@ignore_order(local=True)
@pytest.mark.parametrize("partition_col_sql", [
    pytest.param("year(_c9)", id="year_partition"),
])
def test_overwrite_dynamic_aqe(spark_tmp_table_factory, partition_col_sql):
    """
    Test INSERT OVERWRITE (dynamic partitions) with AQE enabled.
    """
    table_prop = {
        'format-version': '2',
    }

    # Configuration with AQE enabled
    conf = copy_and_update(iceberg_write_enabled_conf, {
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        "spark.sql.sources.partitionOverwriteMode": "dynamic"
    })

    base_table_name = get_full_table_name(spark_tmp_table_factory)
    cpu_table = f"{base_table_name}_cpu"
    gpu_table = f"{base_table_name}_gpu"

    def initialize_table(table_name):
        df_gen = lambda spark: gen_df(spark, list(zip(iceberg_base_table_cols, iceberg_gens_list)))
        create_iceberg_table(table_name, partition_col_sql, table_prop, df_gen)

    with_cpu_session(lambda spark: initialize_table(cpu_table))
    with_cpu_session(lambda spark: initialize_table(gpu_table))

    def overwrite_dynamic(spark, table_name):
        df = gen_df(spark, list(zip(iceberg_base_table_cols, iceberg_gens_list)))
        view_name = spark_tmp_table_factory.get()
        df.createOrReplaceTempView(view_name)
        # Dynamic partition overwrite
        spark.sql(f"INSERT OVERWRITE {table_name} SELECT * FROM {view_name}")

    with_gpu_session(lambda spark: overwrite_dynamic(spark, gpu_table), conf=conf)
    with_cpu_session(lambda spark: overwrite_dynamic(spark, cpu_table), conf=conf)

    cpu_data = with_cpu_session(lambda spark: spark.table(cpu_table).collect())
    gpu_data = with_cpu_session(lambda spark: spark.table(gpu_table).collect())
    assert_equal_with_local_sort(cpu_data, gpu_data)


@allow_non_gpu("BatchScanExec", "ColumnarToRowExec")
@iceberg
@ignore_order(local=True)
@pytest.mark.skipif(is_iceberg_remote_catalog(), reason="Skip for remote catalog to reduce test time")
def test_insert_overwrite_dynamic_after_drop_partition_field(spark_tmp_table_factory):
    """Test INSERT OVERWRITE (dynamic mode) on table after dropping a partition field (void transform).
    
    When a partition field is dropped, Iceberg creates a 'void transform' - 
    the field remains in the partition spec but no longer affects partitioning.
    This test verifies INSERT OVERWRITE (dynamic) still runs correctly on GPU after partition evolution.
    """
    base_table_name = get_full_table_name(spark_tmp_table_factory)
    source_table = f"{base_table_name}_source"
    cpu_table_name = f"{base_table_name}_cpu"
    gpu_table_name = f"{base_table_name}_gpu"
    
    table_prop = {"format-version": "2"}
    # Use two partition columns so after dropping one, we still have at least one
    partition_col_sql = "bucket(8, _c2), bucket(8, _c3)"
    
    # Create source table for overwrite data (no partition evolution needed)
    create_iceberg_table(source_table, partition_col_sql=partition_col_sql, table_prop=table_prop)
    
    # Insert overwrite source data
    def insert_source_data(spark):
        df = gen_df(spark, list(zip(iceberg_base_table_cols, iceberg_gens_list)))
        df.writeTo(source_table).append()
    
    with_cpu_session(insert_source_data)
    
    # Create partitioned target tables
    create_iceberg_table(cpu_table_name, partition_col_sql=partition_col_sql, table_prop=table_prop)
    create_iceberg_table(gpu_table_name, partition_col_sql=partition_col_sql, table_prop=table_prop)
    
    # Insert initial data before partition evolution
    def insert_initial_data(spark, table_name):
        df = gen_df(spark, list(zip(iceberg_base_table_cols, iceberg_gens_list)), seed=INITIAL_DATA_SEED)
        df.writeTo(table_name).append()
    
    with_cpu_session(lambda spark: insert_initial_data(spark, cpu_table_name))
    with_cpu_session(lambda spark: insert_initial_data(spark, gpu_table_name))
    
    # Drop one partition field on target tables (creates void transform)
    def drop_partition_field(spark, table_name):
        spark.sql(f"ALTER TABLE {table_name} DROP PARTITION FIELD bucket(8, _c2)")
    
    with_cpu_session(lambda spark: drop_partition_field(spark, cpu_table_name))
    with_cpu_session(lambda spark: drop_partition_field(spark, gpu_table_name))
    
    # INSERT OVERWRITE (dynamic) after partition evolution - read from shared source table
    # This is the operation we're testing on GPU
    def overwrite_data(spark, table_name):
        spark.sql(f"INSERT OVERWRITE TABLE {table_name} SELECT * FROM {source_table}")
    
    with_gpu_session(lambda spark: overwrite_data(spark, gpu_table_name),
                     conf=dynamic_overwrite_conf)
    with_cpu_session(lambda spark: overwrite_data(spark, cpu_table_name),
                     conf=dynamic_overwrite_conf)
    
    # Compare results
    cpu_data = with_cpu_session(lambda spark: spark.table(cpu_table_name).collect())
    gpu_data = with_cpu_session(lambda spark: spark.table(gpu_table_name).collect())
    assert_equal_with_local_sort(cpu_data, gpu_data)


@iceberg
@datagen_overrides(seed=0, reason='https://github.com/NVIDIA/spark-rapids-jni/issues/4016')
@ignore_order(local=True)
@pytest.mark.skipif(is_iceberg_remote_catalog(), reason="Skip for remote catalog to reduce test time")
def test_insert_overwrite_dynamic_partitioned_fanout_enabled(spark_tmp_table_factory):
    # Use bucket(2, ...) to keep partition count low and avoid OOM from Iceberg's FanoutDataWriter.
    _do_test_insert_overwrite_dynamic_partitioned(
        spark_tmp_table_factory,
        "bucket(2, _c9)",
        table_prop={"format-version": "2", "write.spark.fanout.enabled": "true"})
