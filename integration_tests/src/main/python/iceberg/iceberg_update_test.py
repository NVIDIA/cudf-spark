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
                     iceberg_unsupported_mark, update_partition_transforms_distributed)
from marks import allow_non_gpu, allow_non_gpu_conditional, disable_ansi_mode, iceberg, ignore_order, datagen_overrides
from spark_session import is_spark_400_or_later, with_cpu_session, with_gpu_session

pytestmark = iceberg_unsupported_mark

# Configuration for copy-on-write UPDATE operations
iceberg_update_cow_enabled_conf = copy_and_update(iceberg_write_enabled_conf, {})

# Fixed seed for reproducible test data. Iceberg's update test plan will be different with different data and filter.
UPDATE_TEST_SEED = 42
UPDATE_TEST_SEED_OVERRIDE_REASON = "Ensure reproducible test data for UPDATE operations"

def create_iceberg_table_with_data(table_name: str, 
                                   partition_col_sql=None,
                                   data_gen_func=None,
                                   table_properties=None,
                                   update_mode='copy-on-write'):
    """Helper function to create and populate an Iceberg table for UPDATE tests.

    Args:
        table_name: Name of the table to create
        partition_col_sql: SQL for partitioning clause
        data_gen_func: Function to generate test data
        table_properties: Additional table properties
        update_mode: Update mode - 'copy-on-write' or 'merge-on-read'
    """
    base_props = {
        'format-version': '2',
        'write.update.mode': update_mode
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

def do_update_test(spark_tmp_table_factory, update_sql_func, data_gen_func=None, 
                  partition_col_sql=None, table_properties=None,
                  update_mode='copy-on-write'):
    """
    Helper function to test UPDATE operations by comparing CPU and GPU results.
    
    Args:
        spark_tmp_table_factory: Factory for generating unique table names
        update_sql_func: Function that takes (spark, table_name) and executes UPDATE
        data_gen_func: Function to generate test data
        partition_col_sql: SQL for partitioning clause
        table_properties: Additional table properties
        update_mode: Update mode - 'copy-on-write' or 'merge-on-read'
    """
    base_table_name = get_full_table_name(spark_tmp_table_factory)
    cpu_table_name = f"{base_table_name}_cpu"
    gpu_table_name = f"{base_table_name}_gpu"
    
    # Create identical tables for CPU and GPU
    create_iceberg_table_with_data(cpu_table_name, partition_col_sql, 
                                   data_gen_func, table_properties, update_mode)
    create_iceberg_table_with_data(gpu_table_name, partition_col_sql, 
                                   data_gen_func, table_properties, update_mode)
    
    # Execute UPDATE on GPU
    def do_gpu_update(spark):
        update_sql_func(spark, gpu_table_name)
        
    with_gpu_session(do_gpu_update, conf=iceberg_update_cow_enabled_conf)
    
    # Execute UPDATE on CPU
    def do_cpu_update(spark):
        update_sql_func(spark, cpu_table_name)
        
    with_cpu_session(do_cpu_update)
    
    # Compare results
    cpu_data = with_cpu_session(lambda spark: spark.table(cpu_table_name).collect())
    gpu_data = with_cpu_session(lambda spark: spark.table(gpu_table_name).collect())
    assert_equal_with_local_sort(cpu_data, gpu_data)


@iceberg
@ignore_order(local=True)
@pytest.mark.datagen_overrides(seed=UPDATE_TEST_SEED, reason=UPDATE_TEST_SEED_OVERRIDE_REASON)
@pytest.mark.parametrize('update_mode', ['copy-on-write', 'merge-on-read'])
@allow_non_gpu_conditional(is_spark_400_or_later(), "EmptyRelationExec")
def test_iceberg_update_unpartitioned_table_single_column(spark_tmp_table_factory, update_mode):
    """Test UPDATE on unpartitioned table with single column update"""
    do_update_test(
        spark_tmp_table_factory,
        lambda spark, table: spark.sql(f"UPDATE {table} SET _c2 = _c2 + 100 WHERE _c2 % 3 = 0"),
        update_mode=update_mode
    )

@iceberg
@ignore_order(local=True)
@pytest.mark.datagen_overrides(seed=UPDATE_TEST_SEED, reason=UPDATE_TEST_SEED_OVERRIDE_REASON)
@pytest.mark.skipif(is_iceberg_remote_catalog(), reason="Skip for remote catalog to reduce test time")
@pytest.mark.parametrize('update_mode', ['copy-on-write', 'merge-on-read'])
@allow_non_gpu_conditional(is_spark_400_or_later(), "EmptyRelationExec")
def test_iceberg_update_unpartitioned_table_multiple_columns(spark_tmp_table_factory, update_mode):
    """Test UPDATE on unpartitioned table with multiple column updates"""
    do_update_test(
        spark_tmp_table_factory,
        lambda spark, table: spark.sql(f"UPDATE {table} SET _c2 = _c2 + 100, _c6 = 'updated' WHERE _c2 % 3 = 0"),
        update_mode=update_mode
    )

def _do_test_iceberg_update_partitioned_table_single_column(spark_tmp_table_factory, update_mode, partition_col_sql, table_properties=None):
    """Helper function for partitioned table UPDATE tests."""
    do_update_test(
        spark_tmp_table_factory,
        lambda spark, table: spark.sql(f"UPDATE {table} SET _c2 = _c2 + 100 WHERE _c2 % 3 = 0"),
        partition_col_sql=partition_col_sql,
        table_properties=table_properties,
        update_mode=update_mode
    )


@iceberg
@datagen_overrides(seed=0, reason='https://github.com/NVIDIA/spark-rapids-jni/issues/4016')
@ignore_order(local=True)
@pytest.mark.datagen_overrides(seed=UPDATE_TEST_SEED, reason=UPDATE_TEST_SEED_OVERRIDE_REASON)
@pytest.mark.parametrize('update_mode', ['copy-on-write', 'merge-on-read'])
@pytest.mark.parametrize("partition_col_sql", [
    pytest.param("year(_c9)", id="year(timestamp_col)"),
])
@allow_non_gpu_conditional(is_spark_400_or_later(), "EmptyRelationExec")
def test_iceberg_update_partitioned_table_single_column(spark_tmp_table_factory, update_mode, partition_col_sql):
    """Basic partition test - runs for all catalogs including remote."""
    _do_test_iceberg_update_partitioned_table_single_column(spark_tmp_table_factory, update_mode, partition_col_sql)


@iceberg
@datagen_overrides(seed=0, reason='https://github.com/NVIDIA/spark-rapids-jni/issues/4016')
@ignore_order(local=True)
@pytest.mark.datagen_overrides(seed=UPDATE_TEST_SEED, reason=UPDATE_TEST_SEED_OVERRIDE_REASON)
@pytest.mark.skipif(is_iceberg_remote_catalog(), reason="Skip for remote catalog to reduce test time")
@pytest.mark.parametrize("partition_col_sql,update_mode", update_partition_transforms_distributed)
@allow_non_gpu_conditional(is_spark_400_or_later(), "EmptyRelationExec")
def test_iceberg_update_partitioned_table_single_column_full_coverage(spark_tmp_table_factory, update_mode, partition_col_sql):
    """Sanity-check UPDATE across the two write modes against partition transforms
    distinct from those picked by other DML ops. The 26-transform partition-writer
    coverage anchor lives in
    iceberg_append_test.py::test_insert_into_partitioned_table_full_coverage."""
    _do_test_iceberg_update_partitioned_table_single_column(spark_tmp_table_factory, update_mode, partition_col_sql)

@iceberg
@ignore_order(local=True)
@pytest.mark.datagen_overrides(seed=UPDATE_TEST_SEED, reason=UPDATE_TEST_SEED_OVERRIDE_REASON)
@pytest.mark.skipif(is_iceberg_remote_catalog(), reason="Skip for remote catalog to reduce test time")
@pytest.mark.parametrize('update_mode', ['copy-on-write', 'merge-on-read'])
@allow_non_gpu_conditional(is_spark_400_or_later(), "EmptyRelationExec")
def test_iceberg_update_partitioned_table_multiple_columns(spark_tmp_table_factory, update_mode):
    """Test UPDATE on bucket-partitioned table with multiple column updates"""
    do_update_test(
        spark_tmp_table_factory,
        lambda spark, table: spark.sql(f"UPDATE {table} SET _c2 = _c2 + 100, _c6 = 'updated' WHERE _c2 % 3 = 0"),
        partition_col_sql="year(_c8)",
        update_mode=update_mode
    )


@iceberg
@ignore_order(local=True)
@disable_ansi_mode
@pytest.mark.skipif(is_iceberg_remote_catalog(), reason="Skip for remote catalog to reduce test time")
@allow_non_gpu_conditional(is_spark_400_or_later(), "EmptyRelationExec")
def test_iceberg_update_mor_then_select_count(spark_tmp_table_factory):
    """Test UPDATE with merge-on-read mode, then select count with the same update filter.

    This test verifies that after a merge-on-read UPDATE operation, subsequent COUNT(*)
    queries with the same filter return correct results on both CPU and GPU.
    Filter is `int_column >= Int.MinValue` to ensure some rows are updated.
    """
    base_table_name = get_full_table_name(spark_tmp_table_factory)

    # Phase 1: Initialize tables with data (separate for CPU and GPU)
    cpu_table_name = f'{base_table_name}_cpu'
    gpu_table_name = f'{base_table_name}_gpu'
    create_iceberg_table_with_data(cpu_table_name, update_mode='merge-on-read', partition_col_sql="hour(_c9)")
    create_iceberg_table_with_data(gpu_table_name, update_mode='merge-on-read', partition_col_sql="hour(_c9)")

    # Phase 2: Execute UPDATE on both CPU and GPU tables
    def _do_update(spark, table_name):
        spark.sql(f"UPDATE {table_name} SET _c2 = _c2 + 1, _c3 = _c3 + 1 WHERE _c2 >= -2147483648")

    # UPDATE on CPU
    with_cpu_session(lambda spark: _do_update(spark, cpu_table_name))

    # UPDATE on GPU
    with_gpu_session(lambda spark: _do_update(spark, gpu_table_name), conf=iceberg_update_cow_enabled_conf)

    # Phase 3: Query COUNT(*) with the same filter and compare results
    def _query_count(spark, table_name):
        return spark.sql(f"SELECT COUNT(*) as cnt FROM {table_name} WHERE _c2 >= -2147483648").collect()[0]['cnt']

    # Query count on CPU
    cpu_count = with_cpu_session(lambda spark: _query_count(spark, cpu_table_name))

    # Query count on GPU
    gpu_count = with_gpu_session(lambda spark: _query_count(spark, gpu_table_name))

    # Phase 4: Compare CPU and GPU counts
    assert cpu_count == gpu_count, f"Count mismatch: CPU={cpu_count}, GPU={gpu_count}"


@allow_non_gpu("ReplaceDataExec", "WriteDeltaExec", "BatchScanExec", "ColumnarToRowExec")
@iceberg
@ignore_order(local=True)
@pytest.mark.datagen_overrides(seed=UPDATE_TEST_SEED, reason=UPDATE_TEST_SEED_OVERRIDE_REASON)
@pytest.mark.skipif(is_iceberg_remote_catalog(), reason="Skip for remote catalog to reduce test time")
@pytest.mark.parametrize('update_mode,fallback_exec', [
    pytest.param('copy-on-write', 'ReplaceDataExec', id='cow'),
    pytest.param('merge-on-read', 'WriteDeltaExec', id='mor')
])
@allow_non_gpu_conditional(is_spark_400_or_later(), "EmptyRelationExec")
def test_iceberg_update_fallback_write_disabled(spark_tmp_table_factory, update_mode, fallback_exec):
    """Test UPDATE falls back when Iceberg write is disabled"""
    base_table_name = get_full_table_name(spark_tmp_table_factory)
    
    # Phase 1: Initialize tables with data (separate for CPU and GPU)
    cpu_table_name = f'{base_table_name}_cpu'
    gpu_table_name = f'{base_table_name}_gpu'
    create_iceberg_table_with_data(cpu_table_name, update_mode=update_mode)
    create_iceberg_table_with_data(gpu_table_name, update_mode=update_mode)
    
    # Phase 2: UPDATE operation (to be tested with fallback)
    def write_func(spark, table_name):
        spark.sql(f"UPDATE {table_name} SET _c2 = _c2 + 100 WHERE _c2 % 3 = 0")
    
    # Read function to verify results
    def read_func(spark, table_name):
        return spark.sql(f"SELECT * FROM {table_name}")
    
    assert_gpu_fallback_write_sql(
        write_func,
        read_func,
        base_table_name,
        [fallback_exec],
        conf=copy_and_update(iceberg_update_cow_enabled_conf, {
            "spark.rapids.sql.format.iceberg.write.enabled": "false"
        })
    )

@allow_non_gpu("ReplaceDataExec", "WriteDeltaExec", "BatchScanExec", "ColumnarToRowExec", "ShuffleExchangeExec", "ProjectExec")
@iceberg
@ignore_order(local=True)
@pytest.mark.datagen_overrides(seed=UPDATE_TEST_SEED, reason=UPDATE_TEST_SEED_OVERRIDE_REASON)
@pytest.mark.skipif(is_iceberg_remote_catalog(), reason="Skip for remote catalog to reduce test time")
@pytest.mark.parametrize('update_mode,fallback_exec', [
    pytest.param('copy-on-write', 'ReplaceDataExec', id='cow'),
    pytest.param('merge-on-read', 'WriteDeltaExec', id='mor')
])
@pytest.mark.parametrize("file_format", ["orc", "avro"], ids=lambda x: f"file_format={x}")
@allow_non_gpu_conditional(is_spark_400_or_later(), "EmptyRelationExec")
def test_iceberg_update_fallback_unsupported_file_format(spark_tmp_table_factory, file_format, update_mode, fallback_exec):
    """Test UPDATE falls back with unsupported file formats (ORC, Avro)
    
    This test creates a table with parquet format, inserts data, then changes the
    default write format to an unsupported format. When UPDATE is executed, it needs
    to write new files in the unsupported format, which should trigger a fallback.
    """
    base_table_name = get_full_table_name(spark_tmp_table_factory)
    
    def data_gen(spark):
        return gen_df(spark, list(zip(iceberg_base_table_cols, iceberg_gens_list)))
    
    # Phase 1: Initialize tables with data (separate for CPU and GPU)
    def init_table(table_name):
        # Step 1: Create table with parquet as default write format
        table_props = {
            'format-version': '2',
            'write.update.mode': update_mode,
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
    
    # Phase 2: UPDATE operation (to be tested with fallback)
    def write_func(spark, table_name):
        spark.sql(f"UPDATE {table_name} SET _c2 = _c2 + 100 WHERE _c2 % 3 = 0")
    
    # Read function to verify results
    def read_func(spark, table_name):
        # We should select 1 here to avoid throwing exception for unsupported file format
        return spark.sql(f"SELECT 1")
    
    assert_gpu_fallback_write_sql(
        write_func,
        read_func,
        base_table_name,
        [fallback_exec],
        conf=iceberg_update_cow_enabled_conf
    )

@iceberg
@ignore_order(local=True)
@pytest.mark.datagen_overrides(seed=UPDATE_TEST_SEED, reason=UPDATE_TEST_SEED_OVERRIDE_REASON)
@pytest.mark.skipif(is_iceberg_remote_catalog(), reason="Skip for remote catalog to reduce test time")
@allow_non_gpu_conditional(is_spark_400_or_later(), "EmptyRelationExec")
@pytest.mark.parametrize('update_mode', ['copy-on-write', 'merge-on-read'])
def test_iceberg_update_nested_types(spark_tmp_table_factory, update_mode):
    """Test UPDATE with supported nested types."""
    cols = [f"_c{idx}" for idx, _ in enumerate(iceberg_nested_write_gens_list)]
    data_gen_func = lambda spark: gen_df(spark, list(zip(cols, iceberg_nested_write_gens_list)))

    do_update_test(
        spark_tmp_table_factory,
        lambda spark, table: spark.sql(f"UPDATE {table} SET _c0 = _c0 + 100 WHERE _c0 % 3 = 0"),
        data_gen_func=data_gen_func,
        update_mode=update_mode
    )

@allow_non_gpu("ReplaceDataExec", "WriteDeltaExec", "BatchScanExec", "ColumnarToRowExec")
@iceberg
@ignore_order(local=True)
@pytest.mark.datagen_overrides(seed=UPDATE_TEST_SEED, reason=UPDATE_TEST_SEED_OVERRIDE_REASON)
@pytest.mark.skipif(is_iceberg_remote_catalog(), reason="Skip for remote catalog to reduce test time")
@pytest.mark.parametrize('update_mode,fallback_exec', [
    pytest.param('copy-on-write', 'ReplaceDataExec', id='cow'),
    pytest.param('merge-on-read', 'WriteDeltaExec', id='mor')
])
@allow_non_gpu_conditional(is_spark_400_or_later(), "EmptyRelationExec")
def test_iceberg_update_fallback_iceberg_disabled(spark_tmp_table_factory, update_mode, fallback_exec):
    """Test UPDATE falls back when Iceberg is completely disabled"""
    base_table_name = get_full_table_name(spark_tmp_table_factory)
    
    # Phase 1: Initialize tables with data (separate for CPU and GPU)
    cpu_table_name = f'{base_table_name}_cpu'
    gpu_table_name = f'{base_table_name}_gpu'
    create_iceberg_table_with_data(cpu_table_name, update_mode=update_mode)
    create_iceberg_table_with_data(gpu_table_name, update_mode=update_mode)
    
    # Phase 2: UPDATE operation (to be tested with fallback)
    def write_func(spark, table_name):
        spark.sql(f"UPDATE {table_name} SET _c2 = _c2 + 100 WHERE _c2 % 3 = 0")
    
    # Read function to verify results
    def read_func(spark, table_name):
        return spark.sql(f"SELECT * from {table_name}")
    
    assert_gpu_fallback_write_sql(
        write_func,
        read_func,
        base_table_name,
        [fallback_exec],
        conf=copy_and_update(iceberg_update_cow_enabled_conf, {
            "spark.rapids.sql.format.iceberg.enabled": "false"
        })
    )

@allow_non_gpu("WriteDeltaExec", "BatchScanExec", "ColumnarToRowExec")
@iceberg
@ignore_order(local=True)
@pytest.mark.datagen_overrides(seed=UPDATE_TEST_SEED, reason=UPDATE_TEST_SEED_OVERRIDE_REASON)
@pytest.mark.skipif(is_iceberg_remote_catalog(), reason="Skip for remote catalog to reduce test time")
@allow_non_gpu_conditional(is_spark_400_or_later(), "EmptyRelationExec")
def test_iceberg_update_mor_fallback_writedelta_disabled(spark_tmp_table_factory):
    """Test merge-on-read UPDATE falls back when WriteDeltaExec is disabled
    
    This test verifies that when WriteDeltaExec is explicitly disabled (it's disabled by default
    as experimental), merge-on-read UPDATE operations correctly fallback to CPU execution.
    """
    base_table_name = get_full_table_name(spark_tmp_table_factory)
    
    # Phase 1: Initialize tables with data (separate for CPU and GPU)
    cpu_table_name = f'{base_table_name}_cpu'
    gpu_table_name = f'{base_table_name}_gpu'
    create_iceberg_table_with_data(cpu_table_name, update_mode='merge-on-read')
    create_iceberg_table_with_data(gpu_table_name, update_mode='merge-on-read')
    
    # Phase 2: UPDATE operation (to be tested with fallback)
    def write_func(spark, table_name):
        spark.sql(f"UPDATE {table_name} SET _c2 = _c2 + 100 WHERE _c2 % 3 = 0")
    
    # Read function to verify results
    def read_func(spark, table_name):
        return spark.sql(f"SELECT * FROM {table_name}")
    
    assert_gpu_fallback_write_sql(
        write_func,
        read_func,
        base_table_name,
        ['WriteDeltaExec'],
        conf=copy_and_update(iceberg_update_cow_enabled_conf, {
            "spark.rapids.sql.exec.WriteDeltaExec": "false"
        })
    )



@iceberg
@ignore_order(local=True)
@pytest.mark.parametrize('update_mode', ['copy-on-write', 'merge-on-read'])
@pytest.mark.parametrize("partition_col_sql", [
    pytest.param(None, id="unpartitioned"),
    pytest.param("year(_c9)", id="year_partition"),
])
@allow_non_gpu_conditional(is_spark_400_or_later(), "EmptyRelationExec")
def test_update_aqe(spark_tmp_table_factory, update_mode, partition_col_sql):
    """
    Test UPDATE with AQE enabled.
    """
    table_prop = {
        'format-version': '2',
        'write.update.mode': update_mode
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

    def update_table(spark, table_name):
        spark.sql(f"UPDATE {table_name} SET _c2 = _c2 + 1 WHERE _c0 > 0")

    with_gpu_session(lambda spark: update_table(spark, gpu_table), conf=conf)
    with_cpu_session(lambda spark: update_table(spark, cpu_table), conf=conf)

    cpu_data = with_cpu_session(lambda spark: spark.table(cpu_table).collect())
    gpu_data = with_cpu_session(lambda spark: spark.table(gpu_table).collect())
    assert_equal_with_local_sort(cpu_data, gpu_data)


@iceberg
@ignore_order(local=True)
@pytest.mark.skipif(is_iceberg_remote_catalog(), reason="Skip for remote catalog to reduce test time")
@pytest.mark.datagen_overrides(seed=UPDATE_TEST_SEED, reason=UPDATE_TEST_SEED_OVERRIDE_REASON)
@pytest.mark.parametrize('update_mode', ['copy-on-write', 'merge-on-read'])
@allow_non_gpu_conditional(is_spark_400_or_later(), "EmptyRelationExec")
def test_iceberg_update_after_drop_partition_field(spark_tmp_table_factory, update_mode):
    """Test UPDATE on table after dropping a partition field (void transform).
    
    When a partition field is dropped, Iceberg creates a 'void transform' - 
    the field remains in the partition spec but no longer affects partitioning.
    This test verifies UPDATE still runs correctly on GPU after partition evolution.
    """
    base_table_name = get_full_table_name(spark_tmp_table_factory)
    cpu_table_name = f"{base_table_name}_cpu"
    gpu_table_name = f"{base_table_name}_gpu"
    
    # Use two partition columns so after dropping one, we still have at least one
    partition_col_sql = "bucket(8, _c2), bucket(8, _c3)"
    
    # Create partitioned tables with data
    create_iceberg_table_with_data(cpu_table_name, partition_col_sql=partition_col_sql, 
                                   update_mode=update_mode)
    create_iceberg_table_with_data(gpu_table_name, partition_col_sql=partition_col_sql, 
                                   update_mode=update_mode)
    
    # Drop one partition field on both tables (creates void transform)
    def drop_partition_field(spark, table_name):
        spark.sql(f"ALTER TABLE {table_name} DROP PARTITION FIELD bucket(8, _c2)")
    
    with_cpu_session(lambda spark: drop_partition_field(spark, cpu_table_name))
    with_cpu_session(lambda spark: drop_partition_field(spark, gpu_table_name))
    
    # Execute UPDATE after partition evolution - this is the operation we're testing on GPU
    def do_update(spark, table_name):
        spark.sql(f"UPDATE {table_name} SET _c2 = _c2 + 100 WHERE _c2 % 3 = 0")
    
    with_gpu_session(lambda spark: do_update(spark, gpu_table_name), 
                     conf=iceberg_update_cow_enabled_conf)
    with_cpu_session(lambda spark: do_update(spark, cpu_table_name))
    
    # Compare results
    cpu_data = with_cpu_session(lambda spark: spark.table(cpu_table_name).collect())
    gpu_data = with_cpu_session(lambda spark: spark.table(gpu_table_name).collect())
    assert_equal_with_local_sort(cpu_data, gpu_data)


@allow_non_gpu("BatchScanExec", "ColumnarToRowExec")
@iceberg
@datagen_overrides(seed=0, reason='https://github.com/NVIDIA/spark-rapids-jni/issues/4016')
@ignore_order(local=True)
@pytest.mark.skipif(is_iceberg_remote_catalog(), reason="Skip for remote catalog to reduce test time")
def test_iceberg_update_partitioned_table_fanout_enabled(spark_tmp_table_factory):
    # Use bucket(2, ...) to keep partition count low and avoid OOM from Iceberg's FanoutDataWriter.
    _do_test_iceberg_update_partitioned_table_single_column(
        spark_tmp_table_factory,
        update_mode='copy-on-write',
        partition_col_sql="bucket(2, _c9)",
        table_properties={"write.spark.fanout.enabled": "true"})
