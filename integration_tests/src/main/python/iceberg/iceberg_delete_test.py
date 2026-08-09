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

import pytest

from asserts import assert_equal_with_local_sort, assert_gpu_fallback_write_sql
from conftest import is_iceberg_remote_catalog
from data_gen import *
from iceberg import (create_iceberg_table, get_full_table_name, iceberg_write_enabled_conf,
                     iceberg_base_table_cols, iceberg_gens_list, iceberg_nested_write_gens_list,
                     iceberg_unsupported_mark, delete_partition_transforms_distributed,
                     _build_tblprops, assert_iceberg_files_use_codec)
from marks import allow_non_gpu, allow_non_gpu_conditional, iceberg, ignore_order, datagen_overrides
from spark_session import is_spark_400_or_later, with_cpu_session, with_gpu_session

pytestmark = iceberg_unsupported_mark

# Configuration for copy-on-write DELETE operations
iceberg_delete_cow_enabled_conf = copy_and_update(iceberg_write_enabled_conf, {})

# Configuration for merge-on-read DELETE operations
iceberg_delete_mor_enabled_conf = copy_and_update(iceberg_write_enabled_conf, {})

# Fixed seed for reproducible test data. Iceberg's delete test plan will be different with different data and filter. For example,
# if deleted data exactly match some data files, we could remove all files using delete metadata only operation, then the physical plan 
# would be DeleteFromTableExec.
DELETE_TEST_SEED = 42
DELETE_TEST_SEED_OVERRIDE_REASON = "Ensure reproducible test data for DELETE operations"

def create_iceberg_table_with_data(table_name: str, 
                                   partition_col_sql=None,
                                   data_gen_func=None,
                                   table_properties=None,
                                   delete_mode='copy-on-write'):
    """Helper function to create and populate an Iceberg table for DELETE tests."""
    # Default to copy-on-write mode, but allow override for merge-on-read tests
    base_props = {
        'format-version': '2',
        'write.delete.mode': delete_mode
    }
    if table_properties:
        base_props.update(table_properties)
    
    # Use standard iceberg table definition if no custom generator provided
    if data_gen_func is None:
        data_gen_func = lambda spark: gen_df(spark, list(zip(iceberg_base_table_cols, iceberg_gens_list)))
    
    create_iceberg_table(table_name, 
                        partition_col_sql=partition_col_sql,
                        table_prop=base_props,
                        df_gen=data_gen_func)
    
    # Insert data
    def insert_data(spark):
        df = data_gen_func(spark)
        df.writeTo(table_name).append()
    
    with_cpu_session(insert_data)

def do_delete_test(spark_tmp_table_factory, delete_sql_func, data_gen_func=None, 
                  partition_col_sql=None, table_properties=None,
                  delete_mode='copy-on-write'):
    """
    Helper function to test DELETE operations by comparing CPU and GPU results.
    
    Args:
        spark_tmp_table_factory: Factory for generating unique table names
        delete_sql_func: Function that takes (spark, table_name) and executes DELETE
        data_gen_func: Function to generate test data
        partition_col_sql: SQL for partitioning clause
        table_properties: Additional table properties
        delete_mode: 'copy-on-write' or 'merge-on-read'
    """
    base_table_name = get_full_table_name(spark_tmp_table_factory)
    cpu_table_name = f"{base_table_name}_cpu"
    gpu_table_name = f"{base_table_name}_gpu"
    
    # Create identical tables for CPU and GPU
    create_iceberg_table_with_data(cpu_table_name, partition_col_sql, 
                                   data_gen_func, table_properties, delete_mode)
    create_iceberg_table_with_data(gpu_table_name, partition_col_sql, 
                                   data_gen_func, table_properties, delete_mode)
    
    # Execute DELETE on GPU
    def do_gpu_delete(spark):
        delete_sql_func(spark, gpu_table_name)
        
    with_gpu_session(do_gpu_delete, conf=iceberg_delete_cow_enabled_conf)
    
    # Execute DELETE on CPU
    def do_cpu_delete(spark):
        delete_sql_func(spark, cpu_table_name)
        
    with_cpu_session(do_cpu_delete)
    
    # Compare results
    cpu_data = with_cpu_session(lambda spark: spark.table(cpu_table_name).collect())
    gpu_data = with_cpu_session(lambda spark: spark.table(gpu_table_name).collect())
    assert_equal_with_local_sort(cpu_data, gpu_data)


# This requires reading of _partition field, which is a struct
@iceberg
@ignore_order(local=True)
@pytest.mark.datagen_overrides(seed=DELETE_TEST_SEED, reason=DELETE_TEST_SEED_OVERRIDE_REASON)
@pytest.mark.parametrize('delete_mode', ['copy-on-write', 'merge-on-read'])
@allow_non_gpu_conditional(is_spark_400_or_later(), "EmptyRelationExec")
def test_iceberg_delete_unpartitioned_table(spark_tmp_table_factory, delete_mode):
    """Test DELETE on unpartitioned table with both copy-on-write and merge-on-read modes"""
    do_delete_test(
        spark_tmp_table_factory,
        lambda spark, table: spark.sql(f"DELETE FROM {table} WHERE _c2 % 3 = 0"),
        delete_mode=delete_mode
    )

def _do_test_iceberg_delete_partitioned_table(spark_tmp_table_factory, partition_col_sql, delete_mode, table_properties=None):
    """Helper function for partitioned table DELETE tests."""
    do_delete_test(
        spark_tmp_table_factory,
        lambda spark, table: spark.sql(f"DELETE FROM {table} WHERE _c2 % 3 = 0"),
        partition_col_sql=partition_col_sql,
        table_properties=table_properties,
        delete_mode=delete_mode
    )


# This requires reading of _partition field, which is a struct
@iceberg
@datagen_overrides(seed=0, reason='https://github.com/NVIDIA/spark-rapids-jni/issues/4016')
@ignore_order(local=True)
@pytest.mark.datagen_overrides(seed=DELETE_TEST_SEED, reason=DELETE_TEST_SEED_OVERRIDE_REASON)
@pytest.mark.parametrize("partition_col_sql", [
    pytest.param("year(_c9)", id="year(timestamp_col)"),
])
@pytest.mark.parametrize('delete_mode', ['copy-on-write', 'merge-on-read'])
@allow_non_gpu_conditional(is_spark_400_or_later(), "EmptyRelationExec")
def test_iceberg_delete_partitioned_table(spark_tmp_table_factory, partition_col_sql, delete_mode):
    """Basic partition test - runs for all catalogs including remote."""
    _do_test_iceberg_delete_partitioned_table(spark_tmp_table_factory, partition_col_sql, delete_mode)


# This requires reading of _partition field, which is a struct
@iceberg
@datagen_overrides(seed=0, reason='https://github.com/NVIDIA/spark-rapids-jni/issues/4016')
@ignore_order(local=True)
@pytest.mark.datagen_overrides(seed=DELETE_TEST_SEED, reason=DELETE_TEST_SEED_OVERRIDE_REASON)
@pytest.mark.skipif(is_iceberg_remote_catalog(), reason="Skip for remote catalog to reduce test time")
@pytest.mark.parametrize("partition_col_sql,delete_mode", delete_partition_transforms_distributed)
@allow_non_gpu_conditional(is_spark_400_or_later(), "EmptyRelationExec")
def test_iceberg_delete_partitioned_table_full_coverage(spark_tmp_table_factory, partition_col_sql, delete_mode):
    """Sanity-check DELETE across the two write modes against partition transforms
    distinct from those picked by other DML ops. The 26-transform partition-writer
    coverage anchor lives in
    iceberg_append_test.py::test_insert_into_partitioned_table_full_coverage."""
    _do_test_iceberg_delete_partitioned_table(spark_tmp_table_factory, partition_col_sql, delete_mode)

@iceberg
@ignore_order(local=True)
@pytest.mark.datagen_overrides(seed=DELETE_TEST_SEED, reason=DELETE_TEST_SEED_OVERRIDE_REASON)
@pytest.mark.skipif(is_iceberg_remote_catalog(), reason="Skip for remote catalog to reduce test time")
@pytest.mark.parametrize('delete_mode', ['copy-on-write', 'merge-on-read'])
@allow_non_gpu_conditional(is_spark_400_or_later(), "EmptyRelationExec")
def test_iceberg_delete_with_complex_predicate(spark_tmp_table_factory, delete_mode):
    """Test DELETE with complex predicate"""
    do_delete_test(
        spark_tmp_table_factory,
        lambda spark, table: spark.sql(
            f"DELETE FROM {table} WHERE _c2 > 100 AND _c3 < 50 OR _c1 IS NULL"
        ),
        delete_mode=delete_mode
    )


@allow_non_gpu("ReplaceDataExec", "WriteDeltaExec", "BatchScanExec", "ShuffleExchangeExec", "SortExec", "ProjectExec", "ColumnarToRowExec")
@iceberg
@ignore_order(local=True)
@pytest.mark.datagen_overrides(seed=DELETE_TEST_SEED, reason=DELETE_TEST_SEED_OVERRIDE_REASON)
@pytest.mark.skipif(is_iceberg_remote_catalog(), reason="Skip for remote catalog to reduce test time")
@pytest.mark.parametrize('delete_mode,fallback_exec', [
    pytest.param('copy-on-write', 'ReplaceDataExec', id='cow'),
    pytest.param('merge-on-read', 'WriteDeltaExec', id='mor')
])
@allow_non_gpu_conditional(is_spark_400_or_later(), "EmptyRelationExec")
def test_iceberg_delete_fallback_write_disabled(spark_tmp_table_factory, delete_mode, fallback_exec):
    """Test DELETE falls back when Iceberg write is disabled (both modes)"""
    base_table_name = get_full_table_name(spark_tmp_table_factory)
    
    # Phase 1: Initialize tables with data (separate for CPU and GPU)
    cpu_table_name = f'{base_table_name}_cpu'
    gpu_table_name = f'{base_table_name}_gpu'
    create_iceberg_table_with_data(cpu_table_name, delete_mode=delete_mode)
    create_iceberg_table_with_data(gpu_table_name, delete_mode=delete_mode)
    
    # Phase 2: DELETE operation (to be tested with fallback)
    def write_func(spark, table_name):
        spark.sql(f"DELETE FROM {table_name} WHERE _c2 % 3 = 0")
    
    # Read function to verify results
    def read_func(spark, table_name):
        return spark.sql(f"SELECT * FROM {table_name}")
    
    assert_gpu_fallback_write_sql(
        write_func,
        read_func,
        base_table_name,
        [fallback_exec],
        conf=copy_and_update(iceberg_delete_cow_enabled_conf, {
            "spark.rapids.sql.format.iceberg.write.enabled": "false"
        })
    )

@allow_non_gpu("ReplaceDataExec", "WriteDeltaExec", "BatchScanExec", "ShuffleExchangeExec", "SortExec", "ProjectExec", "ColumnarToRowExec")
@iceberg
@ignore_order(local=True)
@pytest.mark.datagen_overrides(seed=DELETE_TEST_SEED, reason=DELETE_TEST_SEED_OVERRIDE_REASON)
@pytest.mark.skipif(is_iceberg_remote_catalog(), reason="Skip for remote catalog to reduce test time")
@pytest.mark.parametrize('delete_mode,fallback_exec', [
    pytest.param('copy-on-write', 'ReplaceDataExec', id='cow'),
    pytest.param('merge-on-read', 'WriteDeltaExec', id='mor')
])
@pytest.mark.parametrize("file_format", ["orc", "avro"], ids=lambda x: f"file_format={x}")
@allow_non_gpu_conditional(is_spark_400_or_later(), "EmptyRelationExec")
def test_iceberg_delete_fallback_unsupported_file_format(spark_tmp_table_factory, delete_mode, fallback_exec, file_format):
    """Test DELETE falls back with unsupported file formats (ORC, Avro) for both modes"""
    base_table_name = get_full_table_name(spark_tmp_table_factory)
    
    def data_gen(spark):
        return gen_df(spark, list(zip(iceberg_base_table_cols, iceberg_gens_list)))
    
    # Phase 1: Initialize tables with data (separate for CPU and GPU)
    def init_table(table_name):
        # Step 1: Create table with parquet as default write format
        table_props = {
            'format-version': '2',
            'write.delete.mode': delete_mode,
            'write.format.default': 'parquet'
        }
        
        create_iceberg_table(table_name,
                            table_prop=table_props,
                            df_gen=data_gen)
        
        # Step 2: Insert data into the table (creates parquet files)
        def insert_data(spark):
            df = data_gen(spark)
            df.writeTo(table_name).append()
        
        with_cpu_session(insert_data)
        
        # Step 3: Change default write format to unsupported format
        def change_format(spark):
            spark.sql(f"ALTER TABLE {table_name} SET TBLPROPERTIES ('write.format.default' = '{file_format}')")
        
        with_cpu_session(change_format)
    
    # Initialize both CPU and GPU tables
    cpu_table_name = f'{base_table_name}_cpu'
    gpu_table_name = f'{base_table_name}_gpu'
    init_table(cpu_table_name)
    init_table(gpu_table_name)
    
    # Phase 2: DELETE operation (to be tested with fallback)
    def write_func(spark, table_name):
        spark.sql(f"DELETE FROM {table_name} WHERE _c2 % 3 = 0")
    
    # Read function to verify results
    def read_func(spark, table_name):
        # We should select 1 here to avoid throwing exception for unsupported file format
        return spark.sql(f"SELECT 1")
    
    assert_gpu_fallback_write_sql(
        write_func,
        read_func,
        base_table_name,
        [fallback_exec],
        conf=iceberg_delete_cow_enabled_conf
    )

@iceberg
@ignore_order(local=True)
@pytest.mark.datagen_overrides(seed=DELETE_TEST_SEED, reason=DELETE_TEST_SEED_OVERRIDE_REASON)
@pytest.mark.skipif(is_iceberg_remote_catalog(), reason="Skip for remote catalog to reduce test time")
@pytest.mark.parametrize('delete_mode', ['copy-on-write', 'merge-on-read'])
@allow_non_gpu_conditional(is_spark_400_or_later(), "EmptyRelationExec")
def test_iceberg_delete_nested_types(spark_tmp_table_factory, delete_mode):
    """Test DELETE with supported nested types."""
    cols = [f"_c{idx}" for idx, _ in enumerate(iceberg_nested_write_gens_list)]
    data_gen_func = lambda spark: gen_df(spark, list(zip(cols, iceberg_nested_write_gens_list)))

    do_delete_test(
        spark_tmp_table_factory,
        lambda spark, table: spark.sql(f"DELETE FROM {table} WHERE _c0 % 3 = 0"),
        data_gen_func=data_gen_func,
        delete_mode=delete_mode
    )


@allow_non_gpu("ReplaceDataExec", "WriteDeltaExec", "BatchScanExec", "ShuffleExchangeExec", "SortExec", "ProjectExec", "ColumnarToRowExec")
@iceberg
@ignore_order(local=True)
@pytest.mark.datagen_overrides(seed=DELETE_TEST_SEED, reason=DELETE_TEST_SEED_OVERRIDE_REASON)
@pytest.mark.skipif(is_iceberg_remote_catalog(), reason="Skip for remote catalog to reduce test time")
@pytest.mark.parametrize('delete_mode,fallback_exec', [
    pytest.param('copy-on-write', 'ReplaceDataExec', id='cow'),
    pytest.param('merge-on-read', 'WriteDeltaExec', id='mor')
])
def test_iceberg_delete_fallback_iceberg_disabled(spark_tmp_table_factory, delete_mode, fallback_exec):
    """Test DELETE falls back when Iceberg is completely disabled (both modes)"""
    base_table_name = get_full_table_name(spark_tmp_table_factory)
    
    # Phase 1: Initialize tables with data (separate for CPU and GPU)
    cpu_table_name = f'{base_table_name}_cpu'
    gpu_table_name = f'{base_table_name}_gpu'
    create_iceberg_table_with_data(cpu_table_name, delete_mode=delete_mode)
    create_iceberg_table_with_data(gpu_table_name, delete_mode=delete_mode)
    
    # Phase 2: DELETE operation (to be tested with fallback)
    def write_func(spark, table_name):
        spark.sql(f"DELETE FROM {table_name} WHERE _c2 % 3 = 0")
    
    # Read function to verify results
    def read_func(spark, table_name):
        return spark.sql(f"SELECT * from {table_name}")
    
    assert_gpu_fallback_write_sql(
        write_func,
        read_func,
        base_table_name,
        [fallback_exec],
        conf=copy_and_update(iceberg_delete_cow_enabled_conf, {
            "spark.rapids.sql.format.iceberg.enabled": "false"
        })
    )

@allow_non_gpu("WriteDeltaExec", "BatchScanExec", "ShuffleExchangeExec", "SortExec", "ProjectExec", "ColumnarToRowExec")
@iceberg
@ignore_order(local=True)
@pytest.mark.datagen_overrides(seed=DELETE_TEST_SEED, reason=DELETE_TEST_SEED_OVERRIDE_REASON)
@pytest.mark.skipif(is_iceberg_remote_catalog(), reason="Skip for remote catalog to reduce test time")
def test_iceberg_delete_mor_fallback_writedelta_disabled(spark_tmp_table_factory):
    """Test merge-on-read DELETE falls back when WriteDeltaExec is disabled
    
    This test verifies that when WriteDeltaExec is explicitly disabled (it's disabled by default
    as experimental), merge-on-read DELETE operations correctly fallback to CPU execution.
    """
    base_table_name = get_full_table_name(spark_tmp_table_factory)
    
    # Phase 1: Initialize tables with data (separate for CPU and GPU)
    cpu_table_name = f'{base_table_name}_cpu'
    gpu_table_name = f'{base_table_name}_gpu'
    create_iceberg_table_with_data(cpu_table_name, delete_mode='merge-on-read')
    create_iceberg_table_with_data(gpu_table_name, delete_mode='merge-on-read')
    
    # Phase 2: DELETE operation (to be tested with fallback)
    def write_func(spark, table_name):
        spark.sql(f"DELETE FROM {table_name} WHERE _c2 % 3 = 0")
    
    # Read function to verify results
    def read_func(spark, table_name):
        return spark.sql(f"SELECT * FROM {table_name}")
    
    assert_gpu_fallback_write_sql(
        write_func,
        read_func,
        base_table_name,
        ['WriteDeltaExec'],
        conf=copy_and_update(iceberg_delete_cow_enabled_conf, {
            "spark.rapids.sql.exec.WriteDeltaExec": "false"
        })
    )



@iceberg
@ignore_order(local=True)
@pytest.mark.datagen_overrides(seed=DELETE_TEST_SEED, reason=DELETE_TEST_SEED_OVERRIDE_REASON)
@pytest.mark.parametrize('update_mode', ['copy-on-write', 'merge-on-read'])
@pytest.mark.parametrize("partition_col_sql", [
    pytest.param(None, id="unpartitioned"),
    pytest.param("year(_c9)", id="year_partition"),
])
@allow_non_gpu_conditional(is_spark_400_or_later(), "EmptyRelationExec")
def test_delete_aqe(spark_tmp_table_factory, update_mode, partition_col_sql):
    """
    Test DELETE with AQE enabled.
    """
    table_prop = {
        'format-version': '2',
        'write.delete.mode': update_mode
    }

    # Configuration with AQE enabled
    conf = copy_and_update(iceberg_write_enabled_conf, {
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true"
    })

    base_table_name = get_full_table_name(spark_tmp_table_factory)
    cpu_table = f"{base_table_name}_cpu"
    gpu_table = f"{base_table_name}_gpu"

    def initialize_table(table_name):
        df_gen = lambda spark: gen_df(spark, list(zip(iceberg_base_table_cols, iceberg_gens_list)))
        create_iceberg_table(table_name, partition_col_sql, table_prop, df_gen)

    with_cpu_session(lambda spark: initialize_table(cpu_table))
    with_cpu_session(lambda spark: initialize_table(gpu_table))

    def delete_from_table(spark, table_name):
        spark.sql(f"DELETE FROM {table_name} WHERE _c2 % 3 = 0")

    with_gpu_session(lambda spark: delete_from_table(spark, gpu_table), conf=conf)
    with_cpu_session(lambda spark: delete_from_table(spark, cpu_table), conf=conf)

    cpu_data = with_cpu_session(lambda spark: spark.table(cpu_table).collect())
    gpu_data = with_cpu_session(lambda spark: spark.table(gpu_table).collect())
    assert_equal_with_local_sort(cpu_data, gpu_data)


@iceberg
@ignore_order(local=True)
@pytest.mark.skipif(is_iceberg_remote_catalog(), reason="Skip for remote catalog to reduce test time")
@pytest.mark.datagen_overrides(seed=DELETE_TEST_SEED, reason=DELETE_TEST_SEED_OVERRIDE_REASON)
@pytest.mark.parametrize('delete_mode', ['copy-on-write', 'merge-on-read'])
@allow_non_gpu_conditional(is_spark_400_or_later(), "EmptyRelationExec")
def test_iceberg_delete_after_drop_partition_field(spark_tmp_table_factory, delete_mode):
    """Test DELETE on table after dropping a partition field (void transform).
    
    When a partition field is dropped, Iceberg creates a 'void transform' - 
    the field remains in the partition spec but no longer affects partitioning.
    This test verifies DELETE still runs correctly on GPU after partition evolution.
    """
    base_table_name = get_full_table_name(spark_tmp_table_factory)
    cpu_table_name = f"{base_table_name}_cpu"
    gpu_table_name = f"{base_table_name}_gpu"
    
    # Use two partition columns so after dropping one, we still have at least one
    partition_col_sql = "bucket(8, _c2), bucket(8, _c3)"
    
    # Create partitioned tables with data
    create_iceberg_table_with_data(cpu_table_name, partition_col_sql=partition_col_sql, 
                                   delete_mode=delete_mode)
    create_iceberg_table_with_data(gpu_table_name, partition_col_sql=partition_col_sql, 
                                   delete_mode=delete_mode)
    
    # Drop one partition field on both tables (creates void transform)
    def drop_partition_field(spark, table_name):
        spark.sql(f"ALTER TABLE {table_name} DROP PARTITION FIELD bucket(8, _c2)")
    
    with_cpu_session(lambda spark: drop_partition_field(spark, cpu_table_name))
    with_cpu_session(lambda spark: drop_partition_field(spark, gpu_table_name))
    
    # Execute DELETE after partition evolution - this is the operation we're testing on GPU
    def do_delete(spark, table_name):
        spark.sql(f"DELETE FROM {table_name} WHERE _c2 % 3 = 0")
    
    with_gpu_session(lambda spark: do_delete(spark, gpu_table_name), 
                     conf=iceberg_delete_cow_enabled_conf)
    with_cpu_session(lambda spark: do_delete(spark, cpu_table_name))
    
    # Compare results
    cpu_data = with_cpu_session(lambda spark: spark.table(cpu_table_name).collect())
    gpu_data = with_cpu_session(lambda spark: spark.table(gpu_table_name).collect())
    assert_equal_with_local_sort(cpu_data, gpu_data)


@iceberg
@datagen_overrides(seed=0, reason='https://github.com/NVIDIA/spark-rapids-jni/issues/4016')
@ignore_order(local=True)
@pytest.mark.skipif(is_iceberg_remote_catalog(), reason="Skip for remote catalog to reduce test time")
def test_iceberg_delete_partitioned_table_fanout_enabled(spark_tmp_table_factory):
    # Use bucket(2, ...) to keep partition count low and avoid OOM from Iceberg's FanoutDataWriter.
    _do_test_iceberg_delete_partitioned_table(
        spark_tmp_table_factory,
        partition_col_sql="bucket(2, _c9)",
        delete_mode='copy-on-write',
        table_properties={"write.spark.fanout.enabled": "true"})


# Regression for the GpuSparkPositionDeltaWrite branch of issue #14905.
# `test_insert_into_table_honors_iceberg_compression_codec` covers the append path
# (`GpuSparkWrite.createDataWriterFactory`); this test covers MoR DELETE which
# instead goes through `GpuSparkPositionDeltaWrite.createDeltaWriterFactory` and
# writes position-delete files.
@iceberg
@ignore_order(local=True)
@pytest.mark.skipif(is_iceberg_remote_catalog(), reason="Skip for remote catalog to reduce test time")
@pytest.mark.parametrize("table_codec,expected_codec", [
    (None, "zstd"),
    ("zstd", "zstd"),
    ("uncompressed", "uncompressed")])
def test_mor_delete_honors_iceberg_compression_codec(
        spark_tmp_table_factory, table_codec, expected_codec):
    table_name = get_full_table_name(spark_tmp_table_factory)
    extra_props = {"format-version": "2", "write.delete.mode": "merge-on-read"}
    if table_codec is not None:
        extra_props["write.parquet.compression-codec"] = table_codec

    def create_and_populate(spark):
        props = _build_tblprops(extra_props)
        props_sql = ", ".join(f"'{k}' = '{v}'" for k, v in props.items())
        spark.sql(f"CREATE TABLE {table_name} (id int, name string) USING ICEBERG "
                  f"TBLPROPERTIES ({props_sql})")
        # Enough rows that the position-delete files (one row per deleted record)
        # cross cuDF's compression threshold — see rapidsai/cudf#14017.
        spark.sql(f"INSERT INTO {table_name} SELECT id, CAST(id AS STRING) FROM range(40000)")

    with_cpu_session(create_and_populate)

    leaky_codec = "snappy" if expected_codec != "snappy" else "uncompressed"
    conf = copy_and_update(iceberg_write_enabled_conf,
                           {"spark.sql.parquet.compression.codec": leaky_codec})

    # `id % 3 = 0` cannot be answered from per-file min/max metadata, so Iceberg must
    # actually scan rows and emit position-deletes via GpuSparkPositionDeltaWrite (a
    # range-bound predicate like `id < N` would be served by DeleteFromTableExec).
    with_gpu_session(
        lambda spark: spark.sql(f"DELETE FROM {table_name} WHERE id % 3 = 0").collect(),
        conf=conf)

    with_cpu_session(
        lambda spark: assert_iceberg_files_use_codec(
            spark, table_name, expected_codec, files_table="delete_files"))


# When the resolved delete codec (write.delete.parquet.compression-codec) differs from the
# data codec, the GPU position-delta writer cannot honor both — it builds a single
# ColumnarOutputWriterFactory shared by data and delete files. So it must fall back to CPU
# rather than silently writing delete files with the data codec. Regression for #14905.
@allow_non_gpu("WriteDeltaExec", "BatchScanExec", "ShuffleExchangeExec", "SortExec",
               "ProjectExec", "ColumnarToRowExec")
@iceberg
@ignore_order(local=True)
@pytest.mark.datagen_overrides(seed=DELETE_TEST_SEED, reason=DELETE_TEST_SEED_OVERRIDE_REASON)
@pytest.mark.skipif(is_iceberg_remote_catalog(), reason="Skip for remote catalog to reduce test time")
def test_mor_delete_falls_back_on_divergent_delete_codec(spark_tmp_table_factory):
    base_table_name = get_full_table_name(spark_tmp_table_factory)
    cpu_table_name = f"{base_table_name}_cpu"
    gpu_table_name = f"{base_table_name}_gpu"
    # Data codec zstd, delete codec uncompressed — both GPU-supported individually, but
    # differ, so the single-factory GPU writer cannot reproduce CPU Iceberg's output.
    divergent_codecs = {
        "write.parquet.compression-codec": "zstd",
        "write.delete.parquet.compression-codec": "uncompressed",
    }
    create_iceberg_table_with_data(cpu_table_name, delete_mode="merge-on-read",
                                   table_properties=divergent_codecs)
    create_iceberg_table_with_data(gpu_table_name, delete_mode="merge-on-read",
                                   table_properties=divergent_codecs)

    def write_func(spark, table_name):
        spark.sql(f"DELETE FROM {table_name} WHERE _c2 % 3 = 0")

    def read_func(spark, table_name):
        return spark.sql(f"SELECT * FROM {table_name}")

    # WriteDeltaExec is enabled by iceberg_delete_mor_enabled_conf, so the divergent-codec
    # guard is the only reason it can fall back here.
    assert_gpu_fallback_write_sql(
        write_func, read_func, base_table_name, ["WriteDeltaExec"],
        conf=iceberg_delete_mor_enabled_conf)
