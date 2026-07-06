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
                     iceberg_unsupported_mark, merge_partition_transforms_distributed)
from marks import allow_non_gpu, allow_non_gpu_conditional, iceberg, ignore_order, datagen_overrides
from spark_session import is_spark_400_or_later, with_gpu_session, with_cpu_session

pytestmark = iceberg_unsupported_mark

# Base configuration for Iceberg MERGE tests
iceberg_merge_enabled_conf = copy_and_update(iceberg_write_enabled_conf, {})

def create_iceberg_table_with_merge_data(
        table_name: str,
        partition_col_sql=None,
        table_properties=None,
        ensure_distinct_key=False,
        seed=None,
        merge_mode='copy-on-write',
        iceberg_base_table_cols=iceberg_base_table_cols,
        iceberg_gens_list=iceberg_gens_list):
    """
    Helper function to create and populate an Iceberg table for MERGE tests.
    
    Args:
        table_name: Name of the table to create
        partition_col_sql: SQL for partitioning clause
        table_properties: Additional table properties
        ensure_distinct_key: If True, ensures _c0 (join key) has distinct values
                           to satisfy MERGE cardinality constraint
        seed: Random seed for data generation (default: None, uses runtime seed)
        merge_mode: Merge mode - 'copy-on-write' or 'merge-on-read'
    """
    base_props = {
        'format-version': '2',
        'write.merge.mode': merge_mode,
        # See https://github.com/NVIDIA/spark-rapids/issues/13698
        'read.parquet.vectorization.enabled': 'false'
    }
    if table_properties:
        base_props.update(table_properties)
    
    def data_gen(spark):
        if seed is not None:
            return gen_df(spark, list(zip(iceberg_base_table_cols, iceberg_gens_list)), seed=seed)
        else:
            return gen_df(spark, list(zip(iceberg_base_table_cols, iceberg_gens_list)))
    
    create_iceberg_table(table_name,
                        partition_col_sql=partition_col_sql,
                        table_prop=base_props,
                        df_gen=data_gen)
    
    # Insert data with optional distinct key constraint
    def insert_data(spark):
        df = data_gen(spark)
        
        # MERGE requires: each target row matches at most one source row
        # Deduplicate before insert to preserve schema and avoid slow RTAS
        if ensure_distinct_key:
            # Create temp view and use SQL to deduplicate while preserving schema
            df.createOrReplaceTempView("temp_merge_data")
            df = spark.sql("""
                SELECT * FROM (
                    SELECT *, ROW_NUMBER() OVER (PARTITION BY _c0 ORDER BY _c0) as rn
                    FROM temp_merge_data
                ) WHERE rn = 1
            """).drop("rn")
        
        df.writeTo(table_name).append()
    
    with_cpu_session(insert_data)


def do_merge_test(
        spark_tmp_table_factory,
        merge_sql_func,
        partition_col_sql=None,
        table_properties=None,
        merge_mode='copy-on-write',
        iceberg_base_table_cols=iceberg_base_table_cols,
        iceberg_gens_list=iceberg_gens_list):
    """
    Helper function to test MERGE operations by comparing CPU and GPU results.
    
    IMPORTANT: MERGE cardinality constraint - each target row must match at most
    one source row. This is enforced by ensuring distinct join keys in source table.
    
    Args:
        spark_tmp_table_factory: Factory for generating unique table names
        merge_sql_func: Function that takes (spark, target_table, source_table) and executes MERGE
        partition_col_sql: SQL for partitioning clause
        table_properties: Additional table properties
        merge_mode: Merge mode - 'copy-on-write' or 'merge-on-read'
    """
    base_table_name = get_full_table_name(spark_tmp_table_factory)
    cpu_target_table = f"{base_table_name}_target_cpu"
    gpu_target_table = f"{base_table_name}_target_gpu"
    source_table = f"{base_table_name}_source"
    
    # Create identical target tables for CPU and GPU (using runtime seed)
    create_iceberg_table_with_merge_data(cpu_target_table, partition_col_sql, table_properties, merge_mode=merge_mode, iceberg_base_table_cols=iceberg_base_table_cols, iceberg_gens_list=iceberg_gens_list)
    create_iceberg_table_with_merge_data(gpu_target_table, partition_col_sql, table_properties, merge_mode=merge_mode, iceberg_base_table_cols=iceberg_base_table_cols, iceberg_gens_list=iceberg_gens_list)
    
    # Create source table with different seed and distinct keys to satisfy MERGE cardinality constraint
    # (each target row matches at most one source row)
    # Using a fixed different seed ensures source data differs from target data
    create_iceberg_table_with_merge_data(source_table, partition_col_sql, table_properties,
                                        ensure_distinct_key=True, seed=42, merge_mode=merge_mode, iceberg_base_table_cols=iceberg_base_table_cols, iceberg_gens_list=iceberg_gens_list)
    
    # Execute MERGE on GPU
    def do_gpu_merge(spark):
        merge_sql_func(spark, gpu_target_table, source_table)
    
    with_gpu_session(do_gpu_merge, conf=iceberg_merge_enabled_conf)

    # Execute MERGE on CPU
    def do_cpu_merge(spark):
        merge_sql_func(spark, cpu_target_table, source_table)

    with_cpu_session(do_cpu_merge)
    
    # Compare results
    cpu_data = with_cpu_session(lambda spark: spark.table(cpu_target_table).collect())
    gpu_data = with_cpu_session(lambda spark: spark.table(gpu_target_table).collect())
    assert_equal_with_local_sort(cpu_data, gpu_data)


def _do_test_iceberg_merge(spark_tmp_table_factory, partition_col_sql, merge_mode, table_properties=None):
    """Helper function for MERGE tests."""
    merge_sql = """
        MERGE INTO {target} t USING {source} s ON t._c0 = s._c0
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
    do_merge_test(
        spark_tmp_table_factory,
        lambda spark, target, source: spark.sql(merge_sql.format(target=target, source=source)),
        partition_col_sql=partition_col_sql,
        table_properties=table_properties,
        merge_mode=merge_mode
    )


@allow_non_gpu("MergeRows$Keep", "MergeRows$Discard", "MergeRows$Split")
@iceberg
@datagen_overrides(seed=0, reason='https://github.com/NVIDIA/spark-rapids-jni/issues/4016')
@ignore_order(local=True)
@pytest.mark.parametrize('merge_mode', ['copy-on-write', 'merge-on-read'])
@pytest.mark.parametrize('partition_col_sql', [
    None,
    pytest.param("year(_c9)", id="year(timestamp_col)"),
])
@allow_non_gpu_conditional(is_spark_400_or_later(), "EmptyRelationExec")
def test_iceberg_merge(spark_tmp_table_factory, partition_col_sql, merge_mode):
    """Basic partition test - runs for all catalogs including remote."""
    _do_test_iceberg_merge(spark_tmp_table_factory, partition_col_sql, merge_mode)


@allow_non_gpu("MergeRows$Keep", "MergeRows$Discard", "MergeRows$Split")
@iceberg
@datagen_overrides(seed=0, reason='https://github.com/NVIDIA/spark-rapids-jni/issues/4016')
@ignore_order(local=True)
@pytest.mark.skipif(is_iceberg_remote_catalog(), reason="Skip for remote catalog to reduce test time")
@pytest.mark.parametrize("partition_col_sql,merge_mode", merge_partition_transforms_distributed)
@allow_non_gpu_conditional(is_spark_400_or_later(), "EmptyRelationExec")
def test_iceberg_merge_full_coverage(spark_tmp_table_factory, partition_col_sql, merge_mode):
    """Sanity-check MERGE across the two write modes against partition transforms
    distinct from those picked by other DML ops. The 26-transform partition-writer
    coverage anchor lives in
    iceberg_append_test.py::test_insert_into_partitioned_table_full_coverage."""
    _do_test_iceberg_merge(spark_tmp_table_factory, partition_col_sql, merge_mode)


@allow_non_gpu("MergeRows$Keep", "MergeRows$Discard", "MergeRows$Split")
@iceberg
@ignore_order(local=True)
@pytest.mark.skipif(is_iceberg_remote_catalog(), reason="Skip for remote catalog to reduce test time")
@pytest.mark.parametrize('merge_mode', ['copy-on-write', 'merge-on-read'])
@pytest.mark.parametrize('partition_col_sql', [
    pytest.param(None, id="unpartitioned"),
    pytest.param("year(_c9)", id="year(timestamp_col)"),
])
@pytest.mark.parametrize('merge_sql', [
    pytest.param(
        """
        MERGE INTO {target} t USING {source} s ON t._c0 = s._c0
        WHEN MATCHED AND t._c2 % 2 = 0 THEN UPDATE SET t._c1 = s._c1
        WHEN NOT MATCHED THEN INSERT *
        """,
        id="conditional_update"),
    pytest.param(
        """
        MERGE INTO {target} t USING {source} s ON t._c0 = s._c0
        WHEN MATCHED THEN DELETE
        """,
        id="delete_when_matched"),
    pytest.param(
        """
        MERGE INTO {target} t USING {source} s ON t._c0 = s._c0
        WHEN MATCHED AND t._c2 < 0 THEN DELETE
        WHEN MATCHED AND t._c2 >= 0 THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """,
        id="multiple_matched_clauses"),
    pytest.param(
        """
        MERGE INTO {target} t USING {source} s ON t._c0 = s._c0
        WHEN NOT MATCHED THEN INSERT *
        """,
        id="insert_only"),
    pytest.param(
        """
        MERGE INTO {target} t USING {source} s ON t._c0 = s._c0
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        WHEN NOT MATCHED BY SOURCE THEN DELETE
        """,
        id="not_matched_by_source_delete"),
    pytest.param(
        """
        MERGE INTO {target} t USING {source} s ON t._c0 = s._c0
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED BY SOURCE AND t._c2 > 0 THEN DELETE
        """,
        id="conditional_not_matched_by_source"),
])
@allow_non_gpu_conditional(is_spark_400_or_later(), "EmptyRelationExec")
def test_iceberg_merge_additional_patterns(spark_tmp_table_factory, partition_col_sql, merge_sql, merge_mode):
    """Test additional MERGE patterns (conditional updates, deletes, not matched by source) on Iceberg tables."""
    do_merge_test(
        spark_tmp_table_factory,
        lambda spark, target, source: spark.sql(merge_sql.format(target=target, source=source)),
        partition_col_sql=partition_col_sql,
        merge_mode=merge_mode
    )

@allow_non_gpu("MergeRows$Keep", "MergeRows$Discard", "MergeRows$Split")
@iceberg
@ignore_order(local=True)
@pytest.mark.skipif(is_iceberg_remote_catalog(), reason="Skip for remote catalog to reduce test time")
@pytest.mark.parametrize('merge_mode', ['copy-on-write'])
@pytest.mark.parametrize('partition_col_sql', [pytest.param("year(_c9)", id="year(timestamp_col)")])
@pytest.mark.parametrize('merge_sql', [
    pytest.param(
        """
        MERGE INTO {target} t USING {source} s ON t._c0 = s._c0
        WHEN MATCHED AND t._c2 < 0 THEN DELETE
        WHEN MATCHED AND t._c2 >= 0 THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """,
        id="multiple_matched_clauses"),
])
@allow_non_gpu_conditional(is_spark_400_or_later(), "EmptyRelationExec")
def test_iceberg_merge_additional_patterns_bug(spark_tmp_table_factory, partition_col_sql, merge_sql, merge_mode):
    """Test additional MERGE patterns (conditional updates, deletes, not matched by source) on Iceberg tables."""
    do_merge_test(
        spark_tmp_table_factory,
        lambda spark, target, source: spark.sql(merge_sql.format(target=target, source=source)),
        partition_col_sql=partition_col_sql,
        merge_mode=merge_mode
    )

@allow_non_gpu("ReplaceDataExec", "WriteDeltaExec", "MergeRowsExec", "BatchScanExec", "ColumnarToRowExec", "ShuffleExchangeExec", "SortExec", "ProjectExec")
@iceberg
@ignore_order(local=True)
@pytest.mark.skipif(is_iceberg_remote_catalog(), reason="Skip for remote catalog to reduce test time")
@pytest.mark.parametrize('merge_mode,fallback_exec', [
    pytest.param('copy-on-write', 'ReplaceDataExec', id='cow'),
    pytest.param('merge-on-read', 'WriteDeltaExec', id='mor')
])
def test_iceberg_merge_fallback_write_disabled(spark_tmp_table_factory, merge_mode, fallback_exec):
    """Test MERGE falls back when Iceberg write is disabled"""
    base_table_name = get_full_table_name(spark_tmp_table_factory)
    
    # Phase 1: Initialize tables with data
    cpu_target_table = f'{base_table_name}_target_cpu'
    gpu_target_table = f'{base_table_name}_target_gpu'
    source_table = f'{base_table_name}_source'
    
    create_iceberg_table_with_merge_data(cpu_target_table, merge_mode=merge_mode)
    create_iceberg_table_with_merge_data(gpu_target_table, merge_mode=merge_mode)
    # Source table needs distinct keys for MERGE cardinality constraint, with different seed
    create_iceberg_table_with_merge_data(source_table, ensure_distinct_key=True, seed=42, merge_mode=merge_mode)
    
    # Phase 2: MERGE operation (to be tested with fallback)
    def write_func(spark, target_table_name):
        spark.sql(f"""
            MERGE INTO {target_table_name} t
            USING {source_table} s
            ON t._c0 = s._c0
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)
    
    # Read function to verify results
    def read_func(spark, table_name):
        return spark.sql(f"SELECT * FROM {table_name}")
    
    assert_gpu_fallback_write_sql(
        write_func,
        read_func,
        base_table_name + "_target",
        [fallback_exec],
        conf=copy_and_update(iceberg_merge_enabled_conf, {
            "spark.rapids.sql.format.iceberg.write.enabled": "false"
        })
    )


@allow_non_gpu("ReplaceDataExec", "WriteDeltaExec", "MergeRowsExec", "BatchScanExec", "ColumnarToRowExec", "ShuffleExchangeExec", "ProjectExec")
@iceberg
@ignore_order(local=True)
@pytest.mark.skipif(is_iceberg_remote_catalog(), reason="Skip for remote catalog to reduce test time")
@pytest.mark.parametrize('merge_mode,fallback_exec', [
    pytest.param('copy-on-write', 'ReplaceDataExec', id='cow'),
    pytest.param('merge-on-read', 'WriteDeltaExec', id='mor')
])
@pytest.mark.parametrize("file_format", ["orc", "avro"], ids=lambda x: f"file_format={x}")
def test_iceberg_merge_fallback_unsupported_file_format(spark_tmp_table_factory, file_format, merge_mode, fallback_exec):
    """Test MERGE falls back with unsupported file formats (ORC, Avro)"""
    base_table_name = get_full_table_name(spark_tmp_table_factory)
    
    def data_gen(spark):
        return gen_df(spark, list(zip(iceberg_base_table_cols, iceberg_gens_list)))
    
    # Phase 1: Initialize tables
    def init_table(table_name, ensure_distinct_key=False):
        # Create table with parquet, insert data, then change format
        table_props = {
            'format-version': '2',
            'write.merge.mode': merge_mode,
            'write.format.default': 'parquet'
        }
        
        create_iceberg_table(table_name, table_prop=table_props, df_gen=data_gen)
        
        def insert_data(spark):
            df = data_gen(spark)
            
            # MERGE requires: each target row matches at most one source row
            # Deduplicate before insert to preserve schema
            if ensure_distinct_key:
                # Create temp view and use SQL to deduplicate while preserving schema
                df.createOrReplaceTempView("temp_merge_data")
                df = spark.sql("""
                    SELECT * FROM (
                        SELECT *, ROW_NUMBER() OVER (PARTITION BY _c0 ORDER BY _c0) as rn
                        FROM temp_merge_data
                    ) WHERE rn = 1
                """).drop("rn")
            
            df.writeTo(table_name).append()
        
        with_cpu_session(insert_data)
        
        # Change format to unsupported
        def change_format(spark):
            spark.sql(f"ALTER TABLE {table_name} SET TBLPROPERTIES ('write.format.default' = '{file_format}')")
        
        with_cpu_session(change_format)
    
    cpu_target_table = f'{base_table_name}_target_cpu'
    gpu_target_table = f'{base_table_name}_target_gpu'
    source_table = f'{base_table_name}_source'
    
    init_table(cpu_target_table)
    init_table(gpu_target_table)
    init_table(source_table, ensure_distinct_key=True)
    
    # Phase 2: MERGE operation
    def write_func(spark, target_table_name):
        spark.sql(f"""
            MERGE INTO {target_table_name} t
            USING {source_table} s
            ON t._c0 = s._c0
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)
    
    def read_func(spark, table_name):
        # Select 1 to avoid unsupported file format read error
        return spark.sql(f"SELECT 1")
    
    assert_gpu_fallback_write_sql(
        write_func,
        read_func,
        base_table_name + "_target",
        [fallback_exec],
        conf=iceberg_merge_enabled_conf
    )


# MergeRows$Keep / $Discard / $Split are the Spark CPU instruction expressions that live
# inside a MergeRowsExec. GpuMergeRowsExec constructs its own GpuKeep/GpuDiscard/GpuSplit
# at planning time and consumes them internally, so these CPU expressions never reach
# execution. However, the plan-tagging step that drives assert-no-CPU checks sees them as
# "not on GPU" because there is no registered ExprRule for them. This allow_non_gpu is
# therefore required even though the actual execution is fully on GPU.
@allow_non_gpu("MergeRows$Keep", "MergeRows$Discard", "MergeRows$Split")
@iceberg
@ignore_order(local=True)
@pytest.mark.skipif(is_iceberg_remote_catalog(), reason="Skip for remote catalog to reduce test time")
@pytest.mark.parametrize('merge_mode', ['copy-on-write', 'merge-on-read'])
@allow_non_gpu_conditional(is_spark_400_or_later(), "EmptyRelationExec")
def test_iceberg_merge_nested_types(spark_tmp_table_factory, merge_mode):
    """Test MERGE on tables containing supported nested types."""
    nested_cols = [f"_c{idx}" for idx, _ in enumerate(iceberg_nested_write_gens_list)]
    merge_sql = """
        MERGE INTO {target} t USING {source} s ON t._c0 = s._c0
        WHEN MATCHED THEN UPDATE SET t._c1 = s._c1
        """

    do_merge_test(
        spark_tmp_table_factory,
        lambda spark, target, source: spark.sql(merge_sql.format(target=target, source=source)),
        merge_mode=merge_mode,
        iceberg_base_table_cols=nested_cols,
        iceberg_gens_list=iceberg_nested_write_gens_list
    )


@allow_non_gpu("ReplaceDataExec", "WriteDeltaExec", "MergeRowsExec", "BatchScanExec", "ColumnarToRowExec")
@iceberg
@ignore_order(local=True)
@pytest.mark.skipif(is_iceberg_remote_catalog(), reason="Skip for remote catalog to reduce test time")
@pytest.mark.parametrize('merge_mode,fallback_exec', [
    pytest.param('copy-on-write', 'ReplaceDataExec', id='cow'),
    pytest.param('merge-on-read', 'WriteDeltaExec', id='mor')
])
def test_iceberg_merge_fallback_iceberg_disabled(spark_tmp_table_factory, merge_mode, fallback_exec):
    """Test MERGE falls back when Iceberg is completely disabled"""
    base_table_name = get_full_table_name(spark_tmp_table_factory)
    
    cpu_target_table = f'{base_table_name}_target_cpu'
    gpu_target_table = f'{base_table_name}_target_gpu'
    source_table = f'{base_table_name}_source'
    
    create_iceberg_table_with_merge_data(cpu_target_table, merge_mode=merge_mode)
    create_iceberg_table_with_merge_data(gpu_target_table, merge_mode=merge_mode)
    # Source table needs distinct keys for MERGE cardinality constraint, with different seed
    create_iceberg_table_with_merge_data(source_table, ensure_distinct_key=True, seed=42, merge_mode=merge_mode)
    
    def write_func(spark, target_table_name):
        spark.sql(f"""
            MERGE INTO {target_table_name} t
            USING {source_table} s
            ON t._c0 = s._c0
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)
    
    def read_func(spark, table_name):
        return spark.sql(f"SELECT * FROM {table_name}")
    
    assert_gpu_fallback_write_sql(
        write_func,
        read_func,
        base_table_name + "_target",
        [fallback_exec],
        conf=copy_and_update(iceberg_merge_enabled_conf, {
            "spark.rapids.sql.format.iceberg.enabled": "false"
        })
    )

@allow_non_gpu("WriteDeltaExec", "MergeRowsExec", "BatchScanExec", "ColumnarToRowExec")
@iceberg
@ignore_order(local=True)
@pytest.mark.skipif(is_iceberg_remote_catalog(), reason="Skip for remote catalog to reduce test time")
def test_iceberg_merge_mor_fallback_writedelta_disabled(spark_tmp_table_factory):
    """Test merge-on-read MERGE falls back when WriteDeltaExec is disabled
    
    This test verifies that when WriteDeltaExec is explicitly disabled (it's disabled by default
    as experimental), merge-on-read MERGE operations correctly fallback to CPU execution.
    """
    base_table_name = get_full_table_name(spark_tmp_table_factory)
    
    cpu_target_table = f'{base_table_name}_target_cpu'
    gpu_target_table = f'{base_table_name}_target_gpu'
    source_table = f'{base_table_name}_source'
    
    create_iceberg_table_with_merge_data(cpu_target_table, merge_mode='merge-on-read')
    create_iceberg_table_with_merge_data(gpu_target_table, merge_mode='merge-on-read')
    # Source table needs distinct keys for MERGE cardinality constraint, with different seed
    create_iceberg_table_with_merge_data(source_table, ensure_distinct_key=True, seed=42, merge_mode='merge-on-read')
    
    def write_func(spark, target_table_name):
        spark.sql(f"""
            MERGE INTO {target_table_name} t
            USING {source_table} s
            ON t._c0 = s._c0
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)
    
    def read_func(spark, table_name):
        return spark.sql(f"SELECT * FROM {table_name}")
    
    assert_gpu_fallback_write_sql(
        write_func,
        read_func,
        base_table_name + "_target",
        ['WriteDeltaExec'],
        conf=copy_and_update(iceberg_merge_enabled_conf, {
            "spark.rapids.sql.exec.WriteDeltaExec": "false"
        })
    )



@iceberg
@ignore_order(local=True)
@pytest.mark.parametrize("partition_col_sql", [
    pytest.param(None, id="unpartitioned"),
    pytest.param("year(_c9)", id="year_partition"),
])
@allow_non_gpu_conditional(is_spark_400_or_later(), "EmptyRelationExec")
def test_merge_aqe(spark_tmp_table_factory, partition_col_sql):
    """
    Test MERGE INTO with AQE enabled.
    """
    table_prop = {
        'format-version': '2',
    }

    # Configuration with AQE enabled
    conf = copy_and_update(iceberg_write_enabled_conf, {
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true"
    })

    base_table_name = get_full_table_name(spark_tmp_table_factory)
    cpu_target = f"{base_table_name}_target_cpu"
    gpu_target = f"{base_table_name}_target_gpu"
    source = f"{base_table_name}_source"

    def initialize_tables():
        df_gen = lambda spark: gen_df(spark, list(zip(iceberg_base_table_cols, iceberg_gens_list)))
        create_iceberg_table(cpu_target, partition_col_sql, table_prop, df_gen)
        create_iceberg_table(gpu_target, partition_col_sql, table_prop, df_gen)
        create_iceberg_table(source, partition_col_sql, table_prop, df_gen)

    with_cpu_session(lambda spark: initialize_tables())

    def merge_table(spark, target_table):
        spark.sql(f"""
            MERGE INTO {target_table} t
            USING {source} s
            ON t._c0 = s._c0
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)

    with_gpu_session(lambda spark: merge_table(spark, gpu_target), conf=conf)
    with_cpu_session(lambda spark: merge_table(spark, cpu_target), conf=conf)

    cpu_data = with_cpu_session(lambda spark: spark.table(cpu_target).collect())
    gpu_data = with_cpu_session(lambda spark: spark.table(gpu_target).collect())
    assert_equal_with_local_sort(cpu_data, gpu_data)


@iceberg
@ignore_order(local=True)
@pytest.mark.skipif(is_iceberg_remote_catalog(), reason="Skip for remote catalog to reduce test time")
@pytest.mark.parametrize('merge_mode', ['copy-on-write', 'merge-on-read'])
@allow_non_gpu_conditional(is_spark_400_or_later(), "EmptyRelationExec")
def test_iceberg_merge_after_drop_partition_field(spark_tmp_table_factory, merge_mode):
    """Test MERGE on table after dropping a partition field (void transform).
    
    When a partition field is dropped, Iceberg creates a 'void transform' - 
    the field remains in the partition spec but no longer affects partitioning.
    This test verifies MERGE still runs correctly on GPU after partition evolution.
    """
    base_table_name = get_full_table_name(spark_tmp_table_factory)
    cpu_target_table = f"{base_table_name}_target_cpu"
    gpu_target_table = f"{base_table_name}_target_gpu"
    source_table = f"{base_table_name}_source"
    
    # Use two partition columns so after dropping one, we still have at least one
    partition_col_sql = "bucket(8, _c2), bucket(8, _c3)"
    
    # Create partitioned target tables with data - use same seed for both to ensure same data
    create_iceberg_table_with_merge_data(cpu_target_table, partition_col_sql=partition_col_sql, 
                                         merge_mode=merge_mode, seed=42)
    create_iceberg_table_with_merge_data(gpu_target_table, partition_col_sql=partition_col_sql, 
                                         merge_mode=merge_mode, seed=42)
    # Source table needs distinct keys for MERGE cardinality constraint, with different seed
    create_iceberg_table_with_merge_data(source_table, partition_col_sql=partition_col_sql,
                                         ensure_distinct_key=True, seed=43, merge_mode=merge_mode)
    
    # Drop one partition field on target tables and source table (creates void transform)
    def drop_partition_field(spark, table_name):
        spark.sql(f"ALTER TABLE {table_name} DROP PARTITION FIELD bucket(8, _c2)")
    
    with_cpu_session(lambda spark: drop_partition_field(spark, cpu_target_table))
    with_cpu_session(lambda spark: drop_partition_field(spark, gpu_target_table))
    with_cpu_session(lambda spark: drop_partition_field(spark, source_table))
    
    # Execute MERGE after partition evolution - this is the operation we're testing on GPU
    def do_merge(spark, target_table):
        spark.sql(f"""
            MERGE INTO {target_table} t
            USING {source_table} s
            ON t._c0 = s._c0
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)
    
    with_gpu_session(lambda spark: do_merge(spark, gpu_target_table), 
                     conf=iceberg_merge_enabled_conf)
    with_cpu_session(lambda spark: do_merge(spark, cpu_target_table))
    
    # Compare results
    cpu_data = with_cpu_session(lambda spark: spark.table(cpu_target_table).collect())
    gpu_data = with_cpu_session(lambda spark: spark.table(gpu_target_table).collect())
    assert_equal_with_local_sort(cpu_data, gpu_data)


@allow_non_gpu("MergeRows$Keep", "MergeRows$Discard", "MergeRows$Split", "BatchScanExec", "ColumnarToRowExec", "ShuffleExchangeExec")
@iceberg
@datagen_overrides(seed=0, reason='https://github.com/NVIDIA/spark-rapids-jni/issues/4016')
@ignore_order(local=True)
@pytest.mark.skipif(is_iceberg_remote_catalog(), reason="Skip for remote catalog to reduce test time")
def test_iceberg_merge_partitioned_fanout_enabled(spark_tmp_table_factory):
    # Use bucket(2, ...) to keep partition count low and avoid OOM from Iceberg's FanoutDataWriter.
    _do_test_iceberg_merge(
        spark_tmp_table_factory,
        "bucket(2, _c9)",
        merge_mode='copy-on-write',
        table_properties={"write.spark.fanout.enabled": "true"})
