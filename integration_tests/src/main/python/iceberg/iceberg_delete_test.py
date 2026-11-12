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
from pyspark.sql import Row, functions as f

from asserts import (assert_equal_with_local_sort, assert_gpu_and_cpu_are_equal_collect,
                     assert_gpu_fallback_collect, assert_gpu_fallback_write_sql)
from conftest import is_iceberg_remote_catalog, spark_jvm
from data_gen import *
from iceberg import (
    _add_eq_deletes_from_df,
    iceberg_cow_format_versions, iceberg_mor_format_versions, with_iceberg_dml_session,
    with_iceberg_format_versions, create_iceberg_table, get_full_table_name,
    iceberg_write_enabled_conf, iceberg_base_table_cols, iceberg_gens_list,
    iceberg_nested_write_gens_list, iceberg_unsupported_mark,
    delete_partition_transforms_distributed, _build_tblprops, assert_iceberg_files_use_codec,
    supports_iceberg_v3, ICEBERG_V3_UNSUPPORTED_REASON, supports_iceberg_row_lineage_inheritance,
    ICEBERG_ROW_LINEAGE_INHERITANCE_UNSUPPORTED_REASON, row_lineage_df, rapids_reader_types)
from marks import allow_non_gpu, allow_non_gpu_conditional, iceberg, ignore_order, datagen_overrides, iceberg_fast_test
from spark_session import is_spark_400_or_later, with_cpu_session, with_gpu_session

pytestmark = iceberg_unsupported_mark

# Configuration for copy-on-write DELETE operations
iceberg_delete_cow_enabled_conf = copy_and_update(iceberg_write_enabled_conf, {})
iceberg_delete_v3_enabled_conf = copy_and_update(
    iceberg_delete_cow_enabled_conf,
    {"spark.rapids.sql.format.iceberg.v3.enabled": "true"})

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
                                   delete_mode='copy-on-write',
                                   write_order=None, format_version="2"):
    """Helper function to create and populate an Iceberg table for DELETE tests."""
    # Default to copy-on-write mode, but allow override for merge-on-read tests
    base_props = {
        'format-version': format_version,
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
        if write_order:
            spark.sql(f"ALTER TABLE {table_name} WRITE ORDERED BY {write_order}").collect()
        df = data_gen_func(spark)
        df.writeTo(table_name).append()
    
    with_cpu_session(insert_data)

def do_delete_test(spark_tmp_table_factory, delete_sql_func, data_gen_func=None, 
                  partition_col_sql=None, table_properties=None,
                  delete_mode='copy-on-write', conf=iceberg_delete_cow_enabled_conf,
                  read_func=None, write_order=None, format_version="2"):
    """
    Helper function to test DELETE operations by comparing CPU and GPU results.
    
    Args:
        spark_tmp_table_factory: Factory for generating unique table names
        delete_sql_func: Function that takes (spark, table_name) and executes DELETE
        data_gen_func: Function to generate test data
        partition_col_sql: SQL for partitioning clause
        table_properties: Additional table properties
        delete_mode: 'copy-on-write' or 'merge-on-read'
        conf: Spark configuration used for DELETE and result reads
        read_func: Optional function that takes (spark, table_name) and returns a DataFrame
        write_order: Optional deterministic Iceberg write order
    """
    base_table_name = get_full_table_name(spark_tmp_table_factory)
    cpu_table_name = f"{base_table_name}_cpu"
    gpu_table_name = f"{base_table_name}_gpu"
    
    # Create identical tables for CPU and GPU
    create_iceberg_table_with_data(cpu_table_name, partition_col_sql, 
                                   data_gen_func, table_properties, delete_mode, write_order, format_version=format_version)
    create_iceberg_table_with_data(gpu_table_name, partition_col_sql, 
                                   data_gen_func, table_properties, delete_mode, write_order, format_version=format_version)
    
    # Execute DELETE on GPU
    def do_gpu_delete(spark):
        delete_sql_func(spark, gpu_table_name)
        
    with_iceberg_dml_session(do_gpu_delete, format_version, delete_mode, conf=conf)
    
    # Execute DELETE on CPU
    def do_cpu_delete(spark):
        delete_sql_func(spark, cpu_table_name)
        
    with_cpu_session(do_cpu_delete, conf=conf)
    
    # Compare results
    if read_func is None:
        read_func = lambda spark, table_name: spark.table(table_name)
    cpu_data = with_cpu_session(
        lambda spark: read_func(spark, cpu_table_name).collect(), conf=conf)
    gpu_data = with_cpu_session(
        lambda spark: read_func(spark, gpu_table_name).collect(), conf=conf)
    assert_equal_with_local_sort(cpu_data, gpu_data)


# This requires reading of _partition field, which is a struct
@iceberg_fast_test
@iceberg
@ignore_order(local=True)
@pytest.mark.datagen_overrides(seed=DELETE_TEST_SEED, reason=DELETE_TEST_SEED_OVERRIDE_REASON)
@pytest.mark.parametrize(
    'format_version,delete_mode',
    with_iceberg_format_versions(['copy-on-write', 'merge-on-read']))
@allow_non_gpu_conditional(is_spark_400_or_later(), "EmptyRelationExec")
def test_iceberg_delete_unpartitioned_table(format_version, spark_tmp_table_factory, delete_mode):
    """Test DELETE on unpartitioned table with both copy-on-write and merge-on-read modes"""
    do_delete_test(
        spark_tmp_table_factory,
        lambda spark, table: spark.sql(f"DELETE FROM {table} WHERE _c2 % 3 = 0"),
        delete_mode=delete_mode,
        format_version=format_version
    )


@iceberg_fast_test
@iceberg
@pytest.mark.skipif(not supports_iceberg_v3, reason=ICEBERG_V3_UNSUPPORTED_REASON)
@ignore_order(local=True)
@allow_non_gpu('ReplaceDataExec', 'WriteDeltaExec', 'BatchScanExec', 'ColumnarToRowExec')
@pytest.mark.parametrize('delete_mode,fallback_exec', [
    pytest.param('copy-on-write', 'ReplaceDataExec', id='cow'),
    pytest.param('merge-on-read', 'WriteDeltaExec', id='mor')
])
def test_iceberg_delete_encryption_fallback(spark_tmp_table_factory, delete_mode, fallback_exec):
    base_table_name = get_full_table_name(spark_tmp_table_factory)
    for suffix in ("_cpu", "_gpu"):
        create_iceberg_table_with_data(
            base_table_name + suffix,
            data_gen_func=lambda spark: row_lineage_df(spark, length=128),
            table_properties={"encryption.key-id": "test-key"},
            delete_mode=delete_mode, format_version="3")

    def delete_data(spark, table):
        spark.sql(f"DELETE FROM {table} WHERE id % 2 = 0")

    assert_gpu_fallback_write_sql(
        delete_data, lambda spark, table: spark.table(table),
        base_table_name, [fallback_exec], conf=iceberg_delete_v3_enabled_conf)


@iceberg_fast_test
@allow_non_gpu(
    "ReplaceDataExec", "WriteDeltaExec", "BatchScanExec", "ShuffleExchangeExec",
    "SortExec", "ProjectExec", "ColumnarToRowExec")
@iceberg
@pytest.mark.skipif(not supports_iceberg_v3, reason=ICEBERG_V3_UNSUPPORTED_REASON)
@ignore_order(local=True)
@pytest.mark.parametrize('delete_mode,fallback_exec', [
    pytest.param('copy-on-write', 'ReplaceDataExec', id='cow'),
    pytest.param('merge-on-read', 'WriteDeltaExec', id='mor')
])
@allow_non_gpu_conditional(is_spark_400_or_later(), "EmptyRelationExec")
def test_iceberg_delete_v3_table_fallback(
        spark_tmp_table_factory, delete_mode, fallback_exec):
    base_table_name = get_full_table_name(spark_tmp_table_factory)
    create_iceberg_table_with_data(
        f"{base_table_name}_cpu",
        table_properties={"format-version": "3"},
        delete_mode=delete_mode)
    create_iceberg_table_with_data(
        f"{base_table_name}_gpu",
        table_properties={"format-version": "3"},
        delete_mode=delete_mode)

    def delete_data(spark, table_name):
        spark.sql(f"DELETE FROM {table_name} WHERE _c2 % 3 = 0")

    assert_gpu_fallback_write_sql(
        delete_data,
        lambda spark, table_name: spark.sql(f"SELECT * FROM {table_name}"),
        base_table_name,
        [fallback_exec],
        conf=copy_and_update(
            iceberg_delete_cow_enabled_conf,
            {"spark.rapids.sql.format.iceberg.v3.enabled": "false"}))


def _assert_only_deletion_vectors(spark, table_name):
    delete_files = spark.sql(f"""
        SELECT file_path, file_format, referenced_data_file
        FROM {table_name}.delete_files
    """).collect()
    assert delete_files, "Expected at least one deletion vector"

    deletion_vectors = {}
    for delete_file in delete_files:
        assert delete_file.file_format == 'PUFFIN', \
            f"Expected only Puffin deletion vectors, found {delete_files}"
        assert delete_file.referenced_data_file is not None, \
            f"Deletion vector must reference a data file: {delete_file}"
        assert delete_file.referenced_data_file not in deletion_vectors, \
            f"Expected at most one deletion vector per data file, found {delete_files}"
        deletion_vectors[delete_file.referenced_data_file] = delete_file.file_path
    return deletion_vectors


@iceberg_fast_test
@iceberg
# Iceberg metadata table scans are CPU-only; the DELETE command is still GPU-validated.
@allow_non_gpu("BatchScanExec")
@ignore_order(local=True)
@pytest.mark.skipif(not supports_iceberg_v3, reason=ICEBERG_V3_UNSUPPORTED_REASON)
@pytest.mark.parametrize('fanout_enabled', [False, True], ids=['clustered', 'fanout'])
def test_iceberg_delete_v3_gpu_writes_and_merges_deletion_vectors(
        spark_tmp_table_factory, fanout_enabled):
    base_table_name = get_full_table_name(spark_tmp_table_factory)
    cpu_table_name = f"{base_table_name}_cpu"
    gpu_table_name = f"{base_table_name}_gpu"
    data_gen_func = lambda spark: gen_df(
        spark, [
            ('id', LongGen(nullable=False, min_val=0, max_val=127)),
            ('value', LongGen()),
            ('data', StringGen())
        ], seed=0)
    table_properties = {
        'format-version': '3',
        'write.spark.fanout.enabled': fanout_enabled
    }
    create_iceberg_table_with_data(
        cpu_table_name, "bucket(2, id)", data_gen_func, table_properties,
        delete_mode='merge-on-read')
    create_iceberg_table_with_data(
        gpu_table_name, "bucket(2, id)", data_gen_func, table_properties,
        delete_mode='merge-on-read')

    def delete_data(spark):
        is_gpu = spark.conf.get('spark.rapids.sql.enabled') == 'true'
        table_name = gpu_table_name if is_gpu else cpu_table_name
        spark.sql(f"DELETE FROM {table_name} WHERE id % 3 = 0")
        first_delete_vectors = _assert_only_deletion_vectors(spark, table_name)
        spark.sql(f"DELETE FROM {table_name} WHERE id % 5 = 0")
        second_delete_vectors = _assert_only_deletion_vectors(spark, table_name)
        rewritten_data_files = {
            data_file for data_file, delete_file in first_delete_vectors.items()
            if data_file in second_delete_vectors and
            second_delete_vectors[data_file] != delete_file
        }
        assert rewritten_data_files, "Expected the second DELETE to rewrite an existing DV"
        return spark.table(table_name)

    write_conf = copy_and_update(iceberg_write_enabled_conf, {
        'spark.rapids.sql.format.iceberg.v3.enabled': 'true'
    })
    assert_gpu_and_cpu_are_equal_collect(delete_data, conf=write_conf)

    # Verify CPU Iceberg can read the GPU-written deletion vectors.
    cpu_data = with_cpu_session(lambda spark: spark.table(cpu_table_name).collect())
    gpu_data = with_cpu_session(lambda spark: spark.table(gpu_table_name).collect())
    assert_equal_with_local_sort(cpu_data, gpu_data)


@iceberg_fast_test
@iceberg
@ignore_order(local=True)
@pytest.mark.skipif(not supports_iceberg_v3, reason=ICEBERG_V3_UNSUPPORTED_REASON)
def test_iceberg_delete_v3_ignores_unprojected_unsupported_default(
        spark_tmp_table_factory):
    base_table_name = get_full_table_name(spark_tmp_table_factory)
    cpu_table_name = f"{base_table_name}_cpu"
    gpu_table_name = f"{base_table_name}_gpu"

    def setup_table(spark, table_name):
        spark.sql(
            f"CREATE TABLE {table_name} (id BIGINT) USING ICEBERG "
            "TBLPROPERTIES ('format-version'='3', 'write.delete.mode'='merge-on-read')")
        spark.createDataFrame([(1,), (2,)], "id BIGINT").coalesce(1).writeTo(table_name).append()

        jvm = spark_jvm()
        table = jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable(
            spark._jsparkSession, table_name)
        uuid_type = jvm.org.apache.iceberg.types.Types.UUIDType.get()
        default_value = jvm.org.apache.iceberg.expressions.Literal.of(
            "123e4567-e89b-12d3-a456-426614174000").to(uuid_type)
        table.updateSchema().addColumn("token", uuid_type, default_value).commit()
        spark.sql(f"REFRESH TABLE {table_name}")

    with_cpu_session(lambda spark: setup_table(spark, cpu_table_name))
    with_cpu_session(lambda spark: setup_table(spark, gpu_table_name))

    def delete_odd_id(spark, table_name):
        spark.sql(f"DELETE FROM {table_name} WHERE id % 2 = 1").collect()

    with_cpu_session(lambda spark: delete_odd_id(spark, cpu_table_name),
                     conf=iceberg_delete_mor_enabled_conf)
    with_iceberg_dml_session(
        lambda spark: delete_odd_id(spark, gpu_table_name),
        "3", "merge-on-read", conf=iceberg_delete_mor_enabled_conf)

    cpu_data = with_cpu_session(lambda spark: spark.table(cpu_table_name).select("id").collect())
    gpu_data = with_cpu_session(lambda spark: spark.table(gpu_table_name).select("id").collect())
    assert cpu_data == [Row(2)]
    assert_equal_with_local_sort(cpu_data, gpu_data)


@iceberg
# Iceberg metadata table scans are CPU-only; the DELETE command is still GPU-validated.
@allow_non_gpu("BatchScanExec")
@ignore_order(local=True)
@pytest.mark.skipif(not supports_iceberg_v3, reason=ICEBERG_V3_UNSUPPORTED_REASON)
def test_iceberg_delete_v3_gpu_upgrades_position_deletes(spark_tmp_table_factory):
    base_table_name = get_full_table_name(spark_tmp_table_factory)
    cpu_table_name = f"{base_table_name}_cpu"
    gpu_table_name = f"{base_table_name}_gpu"
    data_gen_func = lambda spark: gen_df(
        spark, [
            ('id', LongGen(nullable=False, min_val=0, max_val=63)),
            ('value', LongGen()),
            ('data', StringGen())
        ], seed=0)
    table_properties = {'format-version': '2'}
    create_iceberg_table_with_data(
        cpu_table_name, data_gen_func=data_gen_func, table_properties=table_properties,
        delete_mode='merge-on-read')
    create_iceberg_table_with_data(
        gpu_table_name, data_gen_func=data_gen_func, table_properties=table_properties,
        delete_mode='merge-on-read')

    def create_position_deletes(spark, table_name):
        spark.sql(f"DELETE FROM {table_name} WHERE id % 4 = 0")
        delete_files = spark.sql(f"""
            SELECT file_format FROM {table_name}.delete_files
        """).collect()
        assert delete_files, "Expected at least one v2 position-delete file"
        assert all(delete_file.file_format == 'PARQUET' for delete_file in delete_files), \
            f"Expected only Parquet position deletes before the v3 upgrade, found {delete_files}"
        spark.sql(
            f"ALTER TABLE {table_name} SET TBLPROPERTIES ('format-version' = '3')")

    with_cpu_session(lambda spark: create_position_deletes(spark, cpu_table_name))
    with_cpu_session(lambda spark: create_position_deletes(spark, gpu_table_name))
    write_conf = copy_and_update(iceberg_write_enabled_conf, {
        'spark.rapids.sql.format.iceberg.v3.enabled': 'true'
    })

    def delete_data(spark):
        is_gpu = spark.conf.get('spark.rapids.sql.enabled') == 'true'
        table_name = gpu_table_name if is_gpu else cpu_table_name
        spark.sql(f"DELETE FROM {table_name} WHERE id % 5 = 0")
        _assert_only_deletion_vectors(spark, table_name)
        return spark.table(table_name)

    assert_gpu_and_cpu_are_equal_collect(delete_data, conf=write_conf)

    # Verify CPU Iceberg can read the GPU-written deletion vectors.
    cpu_data = with_cpu_session(lambda spark: spark.table(cpu_table_name).collect())
    gpu_data = with_cpu_session(lambda spark: spark.table(gpu_table_name).collect())
    assert_equal_with_local_sort(cpu_data, gpu_data)


@iceberg_fast_test
@iceberg
@ignore_order(local=True)
@pytest.mark.skipif(
    not supports_iceberg_row_lineage_inheritance,
    reason=ICEBERG_ROW_LINEAGE_INHERITANCE_UNSUPPORTED_REASON)
@pytest.mark.parametrize("reader_type", rapids_reader_types)
def test_iceberg_v3_row_lineage_delete_leading_rows(spark_tmp_table_factory, reader_type):
    full_table = get_full_table_name(spark_tmp_table_factory)

    def setup_iceberg_table(spark):
        spark.sql(
            f"CREATE TABLE {full_table} (id BIGINT) USING ICEBERG "
            "TBLPROPERTIES ('format-version' = '3', 'write.delete.mode' = 'copy-on-write')")
        # Consume row ID 0 in sequence 1 and delete it in sequence 2. The next append then
        # assigns row IDs starting at 1 in sequence 3 to one data file.
        row_lineage_df(spark, length=1).writeTo(full_table).append()
        spark.sql(f"DELETE FROM {full_table} WHERE id = 0")
        row_lineage_df(spark, start=1).writeTo(full_table).append()
        spark.sql(f"DELETE FROM {full_table} WHERE id < {DEFAULT_DATA_GEN_LENGTH // 2}")

    with_cpu_session(setup_iceberg_table)
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.sql(
            f"SELECT id, _pos, _row_id, _last_updated_sequence_number FROM {full_table}"),
        conf={
            "spark.rapids.sql.format.iceberg.v3.enabled": "true",
            "spark.rapids.sql.format.parquet.reader.type": reader_type
        })


@iceberg_fast_test
@iceberg
@ignore_order(local=True)
@allow_non_gpu("BatchScanExec")
@pytest.mark.skipif(
    not supports_iceberg_row_lineage_inheritance,
    reason=ICEBERG_ROW_LINEAGE_INHERITANCE_UNSUPPORTED_REASON)
@pytest.mark.parametrize('delete_mode', ['copy-on-write', 'merge-on-read'])
@pytest.mark.parametrize('partitioned', [False, True], ids=['unpartitioned', 'partitioned'])
def test_iceberg_v3_row_lineage_gpu_delete_leading_rows(
        spark_tmp_table_factory, delete_mode, partitioned):
    do_delete_test(
        spark_tmp_table_factory,
        lambda spark, table: spark.sql(f"DELETE FROM {table} WHERE id < 3"),
        # DELETE removes one leading row from each partition and leaves surviving rows in both.
        data_gen_func=lambda spark: (
            row_lineage_df(spark, start=1).withColumn("p", f.col("id") % 2)),
        partition_col_sql="p" if partitioned else None,
        table_properties={"format-version": "3"},
        conf=copy_and_update(
            iceberg_delete_v3_enabled_conf, {"spark.sql.shuffle.partitions": "1"}),
        read_func=lambda spark, table: spark.sql(
            f"SELECT id, p, _pos, _row_id, _last_updated_sequence_number FROM {table}"),
        write_order="id", format_version="3", delete_mode=delete_mode)


def _do_test_iceberg_delete_partitioned_table(
        spark_tmp_table_factory, partition_col_sql, delete_mode, table_properties=None, format_version="2"):
    """Helper function for partitioned table DELETE tests."""
    do_delete_test(
        spark_tmp_table_factory,
        lambda spark, table: spark.sql(f"DELETE FROM {table} WHERE _c2 % 3 = 0"),
        partition_col_sql=partition_col_sql,
        table_properties=table_properties,
        delete_mode=delete_mode,
        format_version=format_version
    )


# This requires reading of _partition field, which is a struct
@iceberg_fast_test
@iceberg
@datagen_overrides(seed=0, reason='https://github.com/NVIDIA/spark-rapids-jni/issues/4016')
@ignore_order(local=True)
@pytest.mark.datagen_overrides(seed=DELETE_TEST_SEED, reason=DELETE_TEST_SEED_OVERRIDE_REASON)
@pytest.mark.parametrize("partition_col_sql", [
    pytest.param("year(_c9)", id="year(timestamp_col)"),
])
@pytest.mark.parametrize(
    'format_version,delete_mode',
    with_iceberg_format_versions(['copy-on-write', 'merge-on-read']))
@allow_non_gpu_conditional(is_spark_400_or_later(), "EmptyRelationExec")
def test_iceberg_delete_partitioned_table(format_version, spark_tmp_table_factory, partition_col_sql, delete_mode):
    """Basic partition test - runs for all catalogs including remote."""
    _do_test_iceberg_delete_partitioned_table(
        spark_tmp_table_factory,
        partition_col_sql,
        delete_mode,
        format_version=format_version)


# This requires reading of _partition field, which is a struct
@iceberg
@datagen_overrides(seed=0, reason='https://github.com/NVIDIA/spark-rapids-jni/issues/4016')
@ignore_order(local=True)
@pytest.mark.datagen_overrides(seed=DELETE_TEST_SEED, reason=DELETE_TEST_SEED_OVERRIDE_REASON)
@pytest.mark.skipif(is_iceberg_remote_catalog(), reason="Skip for remote catalog to reduce test time")
@pytest.mark.parametrize(
    'format_version,partition_col_sql,delete_mode',
    with_iceberg_format_versions(delete_partition_transforms_distributed))
@allow_non_gpu_conditional(is_spark_400_or_later(), "EmptyRelationExec")
def test_iceberg_delete_partitioned_table_full_coverage(
        format_version, spark_tmp_table_factory, partition_col_sql, delete_mode):
    """Sanity-check DELETE across the two write modes against partition transforms
    distinct from those picked by other DML ops. The 26-transform partition-writer
    coverage anchor lives in
    iceberg_append_test.py::test_insert_into_partitioned_table_full_coverage."""
    _do_test_iceberg_delete_partitioned_table(
        spark_tmp_table_factory,
        partition_col_sql,
        delete_mode,
        format_version=format_version)

@iceberg
@ignore_order(local=True)
@pytest.mark.datagen_overrides(seed=DELETE_TEST_SEED, reason=DELETE_TEST_SEED_OVERRIDE_REASON)
@pytest.mark.skipif(is_iceberg_remote_catalog(), reason="Skip for remote catalog to reduce test time")
@pytest.mark.parametrize(
    'format_version,delete_mode',
    with_iceberg_format_versions(['copy-on-write', 'merge-on-read']))
@allow_non_gpu_conditional(is_spark_400_or_later(), "EmptyRelationExec")
def test_iceberg_delete_with_complex_predicate(format_version, spark_tmp_table_factory, delete_mode):
    """Test DELETE with complex predicate"""
    do_delete_test(
        spark_tmp_table_factory,
        lambda spark, table: spark.sql(
            f"DELETE FROM {table} WHERE _c2 > 100 AND _c3 < 50 OR _c1 IS NULL"
        ),
        delete_mode=delete_mode,
        format_version=format_version
    )


@allow_non_gpu("ReplaceDataExec", "WriteDeltaExec", "BatchScanExec", "ShuffleExchangeExec", "SortExec", "ProjectExec", "ColumnarToRowExec")
@iceberg
@ignore_order(local=True)
@pytest.mark.datagen_overrides(seed=DELETE_TEST_SEED, reason=DELETE_TEST_SEED_OVERRIDE_REASON)
@pytest.mark.skipif(is_iceberg_remote_catalog(), reason="Skip for remote catalog to reduce test time")
@pytest.mark.parametrize('format_version,delete_mode,fallback_exec', with_iceberg_format_versions([
    pytest.param('copy-on-write', 'ReplaceDataExec', id='cow'),
    pytest.param('merge-on-read', 'WriteDeltaExec', id='mor')
]))
@allow_non_gpu_conditional(is_spark_400_or_later(), "EmptyRelationExec")
def test_iceberg_delete_fallback_write_disabled(
        format_version, spark_tmp_table_factory, delete_mode, fallback_exec):
    """Test DELETE falls back when Iceberg write is disabled (both modes)"""
    base_table_name = get_full_table_name(spark_tmp_table_factory)
    
    # Phase 1: Initialize tables with data (separate for CPU and GPU)
    cpu_table_name = f'{base_table_name}_cpu'
    gpu_table_name = f'{base_table_name}_gpu'
    create_iceberg_table_with_data(cpu_table_name, delete_mode=delete_mode, format_version=format_version)
    create_iceberg_table_with_data(gpu_table_name, delete_mode=delete_mode, format_version=format_version)
    
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
@pytest.mark.parametrize('format_version,delete_mode,fallback_exec', with_iceberg_format_versions([
    pytest.param('copy-on-write', 'ReplaceDataExec', id='cow'),
    pytest.param('merge-on-read', 'WriteDeltaExec', id='mor')
]))
@pytest.mark.parametrize("file_format", ["orc", "avro"], ids=lambda x: f"file_format={x}")
@allow_non_gpu_conditional(is_spark_400_or_later(), "EmptyRelationExec")
def test_iceberg_delete_fallback_unsupported_file_format(
        format_version, spark_tmp_table_factory, delete_mode, fallback_exec, file_format):
    """Test DELETE falls back with unsupported file formats (ORC, Avro) for both modes"""
    base_table_name = get_full_table_name(spark_tmp_table_factory)
    
    def data_gen(spark):
        return gen_df(spark, list(zip(iceberg_base_table_cols, iceberg_gens_list)))
    
    # Phase 1: Initialize tables with data (separate for CPU and GPU)
    def init_table(table_name):
        # Step 1: Create table with parquet as default write format
        table_props = {
            'format-version': format_version,
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
@pytest.mark.parametrize(
    'format_version,delete_mode',
    with_iceberg_format_versions(['copy-on-write', 'merge-on-read']))
@allow_non_gpu_conditional(is_spark_400_or_later(), "EmptyRelationExec")
def test_iceberg_delete_nested_types(format_version, spark_tmp_table_factory, delete_mode):
    """Test DELETE with supported nested types."""
    cols = [f"_c{idx}" for idx, _ in enumerate(iceberg_nested_write_gens_list)]
    data_gen_func = lambda spark: gen_df(spark, list(zip(cols, iceberg_nested_write_gens_list)))

    do_delete_test(
        spark_tmp_table_factory,
        lambda spark, table: spark.sql(f"DELETE FROM {table} WHERE _c0 % 3 = 0"),
        data_gen_func=data_gen_func,
        delete_mode=delete_mode,
        format_version=format_version
    )


@allow_non_gpu("ReplaceDataExec", "WriteDeltaExec", "BatchScanExec", "ShuffleExchangeExec", "SortExec", "ProjectExec", "ColumnarToRowExec")
@iceberg
@ignore_order(local=True)
@pytest.mark.datagen_overrides(seed=DELETE_TEST_SEED, reason=DELETE_TEST_SEED_OVERRIDE_REASON)
@pytest.mark.skipif(is_iceberg_remote_catalog(), reason="Skip for remote catalog to reduce test time")
@pytest.mark.parametrize('format_version,delete_mode,fallback_exec', with_iceberg_format_versions([
    pytest.param('copy-on-write', 'ReplaceDataExec', id='cow'),
    pytest.param('merge-on-read', 'WriteDeltaExec', id='mor')
]))
def test_iceberg_delete_fallback_iceberg_disabled(
        format_version, spark_tmp_table_factory, delete_mode, fallback_exec):
    """Test DELETE falls back when Iceberg is completely disabled (both modes)"""
    base_table_name = get_full_table_name(spark_tmp_table_factory)
    
    # Phase 1: Initialize tables with data (separate for CPU and GPU)
    cpu_table_name = f'{base_table_name}_cpu'
    gpu_table_name = f'{base_table_name}_gpu'
    create_iceberg_table_with_data(cpu_table_name, delete_mode=delete_mode, format_version=format_version)
    create_iceberg_table_with_data(gpu_table_name, delete_mode=delete_mode, format_version=format_version)
    
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
@pytest.mark.parametrize("format_version", iceberg_mor_format_versions)
def test_iceberg_delete_mor_fallback_writedelta_disabled(format_version, spark_tmp_table_factory):
    """Test merge-on-read DELETE falls back when WriteDeltaExec is disabled
    
    This test verifies that when WriteDeltaExec is explicitly disabled (it's disabled by default
    as experimental), merge-on-read DELETE operations correctly fallback to CPU execution.
    """
    base_table_name = get_full_table_name(spark_tmp_table_factory)
    
    # Phase 1: Initialize tables with data (separate for CPU and GPU)
    cpu_table_name = f'{base_table_name}_cpu'
    gpu_table_name = f'{base_table_name}_gpu'
    create_iceberg_table_with_data(cpu_table_name, delete_mode='merge-on-read', format_version=format_version)
    create_iceberg_table_with_data(gpu_table_name, delete_mode='merge-on-read', format_version=format_version)
    
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



@iceberg_fast_test
@iceberg
@ignore_order(local=True)
@pytest.mark.datagen_overrides(seed=DELETE_TEST_SEED, reason=DELETE_TEST_SEED_OVERRIDE_REASON)
@pytest.mark.parametrize(
    'format_version,update_mode',
    with_iceberg_format_versions(['copy-on-write', 'merge-on-read']))
@pytest.mark.parametrize("partition_col_sql", [
    pytest.param(None, id="unpartitioned"),
    pytest.param("year(_c9)", id="year_partition"),
])
@allow_non_gpu_conditional(is_spark_400_or_later(), "EmptyRelationExec")
def test_delete_aqe(format_version, spark_tmp_table_factory, update_mode, partition_col_sql):
    """
    Test DELETE with AQE enabled.
    """
    table_prop = {
        'format-version': format_version,
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

    with_iceberg_dml_session(
        lambda spark: delete_from_table(spark, gpu_table),
        format_version,
        update_mode,
        conf=conf)
    with_cpu_session(lambda spark: delete_from_table(spark, cpu_table), conf=conf)

    cpu_data = with_cpu_session(lambda spark: spark.table(cpu_table).collect())
    gpu_data = with_cpu_session(lambda spark: spark.table(gpu_table).collect())
    assert_equal_with_local_sort(cpu_data, gpu_data)


@iceberg
@ignore_order(local=True)
@pytest.mark.skipif(is_iceberg_remote_catalog(), reason="Skip for remote catalog to reduce test time")
@pytest.mark.datagen_overrides(seed=DELETE_TEST_SEED, reason=DELETE_TEST_SEED_OVERRIDE_REASON)
@pytest.mark.parametrize(
    'format_version,delete_mode',
    with_iceberg_format_versions(['copy-on-write', 'merge-on-read']))
@allow_non_gpu_conditional(is_spark_400_or_later(), "EmptyRelationExec")
def test_iceberg_delete_after_drop_partition_field(format_version, spark_tmp_table_factory, delete_mode):
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
                                   delete_mode=delete_mode, format_version=format_version)
    create_iceberg_table_with_data(gpu_table_name, partition_col_sql=partition_col_sql, 
                                   delete_mode=delete_mode, format_version=format_version)
    
    # Drop one partition field on both tables (creates void transform)
    def drop_partition_field(spark, table_name):
        spark.sql(f"ALTER TABLE {table_name} DROP PARTITION FIELD bucket(8, _c2)")
    
    with_cpu_session(lambda spark: drop_partition_field(spark, cpu_table_name))
    with_cpu_session(lambda spark: drop_partition_field(spark, gpu_table_name))
    
    # Execute DELETE after partition evolution - this is the operation we're testing on GPU
    def do_delete(spark, table_name):
        spark.sql(f"DELETE FROM {table_name} WHERE _c2 % 3 = 0")
    
    with_iceberg_dml_session(
        lambda spark: do_delete(spark, gpu_table_name),
        format_version, delete_mode, conf=iceberg_delete_cow_enabled_conf)
    with_cpu_session(lambda spark: do_delete(spark, cpu_table_name))
    
    # Compare results
    cpu_data = with_cpu_session(lambda spark: spark.table(cpu_table_name).collect())
    gpu_data = with_cpu_session(lambda spark: spark.table(gpu_table_name).collect())
    assert_equal_with_local_sort(cpu_data, gpu_data)


@iceberg
@datagen_overrides(seed=0, reason='https://github.com/NVIDIA/spark-rapids-jni/issues/4016')
@ignore_order(local=True)
@pytest.mark.skipif(is_iceberg_remote_catalog(), reason="Skip for remote catalog to reduce test time")
@pytest.mark.parametrize("format_version", iceberg_cow_format_versions)
def test_iceberg_delete_partitioned_table_fanout_enabled(format_version, spark_tmp_table_factory):
    # Use bucket(2, ...) to keep partition count low and avoid OOM from Iceberg's FanoutDataWriter.
    _do_test_iceberg_delete_partitioned_table(
        spark_tmp_table_factory,
        partition_col_sql="bucket(2, _c9)",
        delete_mode='copy-on-write',
        table_properties={"write.spark.fanout.enabled": "true"}, format_version=format_version)


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


@iceberg
@pytest.mark.skipif(not supports_iceberg_v3, reason=ICEBERG_V3_UNSUPPORTED_REASON)
@pytest.mark.skipif(is_iceberg_remote_catalog(), reason="Requires local equality-delete UDF")
@allow_non_gpu("BatchScanExec", "ColumnarToRowExec")
@ignore_order(local=True)
def test_iceberg_v3_default_on_implicit_equality_delete_field(
        spark_tmp_table_factory,
        spark_tmp_path,
        register_iceberg_add_eq_deletes_udf):
    table_name = get_full_table_name(spark_tmp_table_factory)
    props = _build_tblprops({"format-version": "3"})
    props_sql = ", ".join(f"'{key}' = '{value}'" for key, value in props.items())
    initial_data_length = 2
    current_id = None

    def setup_table(spark):
        nonlocal current_id
        spark.sql(
            f"CREATE TABLE {table_name} (id BIGINT) USING ICEBERG PARTITIONED BY (id) "
            f"TBLPROPERTIES ({props_sql})")
        initial_data = gen_df(
            spark,
            [("id", UniqueLongGen())],
            length=initial_data_length).select(f.abs(f.col("id")).alias("id"))
        target_id = initial_data.orderBy("id").first().id
        initial_data.coalesce(1).writeTo(table_name).append()

        jvm = spark_jvm()
        table = jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable(
            spark._jsparkSession, table_name)
        timestamp_ntz_type = jvm.org.apache.iceberg.types.Types.TimestampType.withoutZone()
        default_value = jvm.org.apache.iceberg.expressions.Literal.of(
            "2024-01-02T03:04:05").to(timestamp_ntz_type)
        table.updateSchema().addColumn(
            "_c9",
            timestamp_ntz_type,
            default_value).commit()
        spark.sql(f"REFRESH TABLE {table_name}")
        current_data = (gen_df(
            spark,
            [("id", UniqueLongGen()),
             ("_c9", TimestampGen(nullable=False, tzinfo=None))],
            length=1)
            .select(
                (f.abs(f.col("id")) + initial_data_length).alias("id"),
                f.col("_c9")))
        current_id = current_data.first().id
        current_data.coalesce(1).writeTo(table_name).append()

        # _c9 is intentionally omitted from the query below. The equality-delete file makes it an
        # implicit required read field, and the old data file requires its initial default.
        deletes = (spark.table(table_name)
                   .where(f.col("id") == target_id)
                   .select("_c9", "_partition")
                   .coalesce(1))
        _add_eq_deletes_from_df(spark, deletes, table_name, spark_tmp_path)

    with_cpu_session(setup_table)
    remaining_rows = with_cpu_session(
        lambda spark: spark.sql(f"SELECT id FROM {table_name} ORDER BY id").collect())
    assert len(remaining_rows) == initial_data_length
    assert Row(current_id) in remaining_rows
    assert_gpu_fallback_collect(
        lambda spark: spark.sql(f"SELECT id FROM {table_name}"),
        "BatchScanExec",
        conf={"spark.rapids.sql.format.iceberg.v3.enabled": "true"})


@iceberg
@pytest.mark.skipif(not supports_iceberg_v3, reason=ICEBERG_V3_UNSUPPORTED_REASON)
@pytest.mark.skipif(is_iceberg_remote_catalog(), reason="Requires local equality-delete UDF")
@allow_non_gpu("BatchScanExec", "ColumnarToRowExec")
@ignore_order(local=True)
def test_iceberg_v3_nested_equality_delete_field_falls_back(
        spark_tmp_table_factory,
        spark_tmp_path,
        register_iceberg_add_eq_deletes_udf):
    table_name = get_full_table_name(spark_tmp_table_factory)
    props = _build_tblprops({"format-version": "3"})
    props_sql = ", ".join(f"'{key}' = '{value}'" for key, value in props.items())
    initial_data_length = 2
    current_data_length = 1
    current_ids = []

    def setup_table(spark):
        nonlocal current_ids
        spark.sql(
            f"CREATE TABLE {table_name} "
            "(id BIGINT, payload STRUCT<existing: STRING>) USING ICEBERG PARTITIONED BY (id) "
            f"TBLPROPERTIES ({props_sql})")
        initial_data = gen_df(
            spark,
            [("id", UniqueLongGen()),
             ("payload", StructGen(
                 [("existing", StringGen(nullable=False))], nullable=False))],
            length=initial_data_length).select(
                f.abs(f.col("id")).alias("id"), f.col("payload"))
        target_id = initial_data.orderBy("id").first().id
        initial_data.coalesce(1).writeTo(table_name).append()

        jvm = spark_jvm()
        table = jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable(
            spark._jsparkSession, table_name)
        string_type = jvm.org.apache.iceberg.types.Types.StringType.get()
        default_value = jvm.org.apache.iceberg.expressions.Literal.of("legacy")
        table.updateSchema().addColumn(
            "payload", "delete_key", string_type, default_value).commit()
        spark.sql(f"REFRESH TABLE {table_name}")
        current_data = (gen_df(
            spark,
            [("id", UniqueLongGen()),
             ("payload", StructGen([
                 ("existing", StringGen(nullable=False)),
                 ("delete_key", StringGen(pattern="current_[0-9]{4}", nullable=False))
             ], nullable=False))],
            length=current_data_length)
            .select(
                (f.abs(f.col("id")) + initial_data_length).alias("id"),
                f.col("payload")))
        current_ids = [row.id for row in current_data.select("id").collect()]
        current_data.coalesce(1).writeTo(table_name).append()

        # Keep the parent struct in the Parquet delete row so Iceberg can preserve the nested field
        # ID. Only delete_key is included below, making it the equality field rather than payload.
        deletes = (spark.table(table_name)
                   .where(f.col("id") == target_id)
                   .select(
                       f.struct(f.col("payload.delete_key").alias("delete_key")).alias("payload"),
                       f.col("_partition"))
                   .coalesce(1))
        _add_eq_deletes_from_df(spark, deletes, table_name, spark_tmp_path)

    with_cpu_session(setup_table)
    # Keep the nested equality field projected so the CPU baseline can apply the delete. The
    # implicit-field fallback path is covered by GpuPostProcessorSuite.
    read_query = f"SELECT id, payload.delete_key AS delete_key FROM {table_name}"
    remaining_rows = with_cpu_session(
        lambda spark: spark.sql(f"{read_query} ORDER BY id").collect())
    assert len(remaining_rows) == initial_data_length - 1 + current_data_length
    remaining_ids = {row.id for row in remaining_rows}
    assert all(current_id in remaining_ids for current_id in current_ids)
    assert_gpu_fallback_collect(
        lambda spark: spark.sql(read_query),
        "BatchScanExec",
        conf={"spark.rapids.sql.format.iceberg.v3.enabled": "true"})


def _add_supported_delete_defaults(spark, table_name):
    jvm = spark_jvm()
    table = jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable(
        spark._jsparkSession, table_name)
    table.updateSchema().addColumn(
        'delete_key', jvm.org.apache.iceberg.types.Types.IntegerType.get(),
        jvm.org.apache.iceberg.expressions.Literal.of(11)).addColumn(
        'string_key', jvm.org.apache.iceberg.types.Types.StringType.get(),
        jvm.org.apache.iceberg.expressions.Literal.of('legacy')).commit()
    spark.sql(f'REFRESH TABLE {table_name}')


def _assert_delete_rows(expected):
    def check(cpu_rows, gpu_rows):
        for rows in (cpu_rows, gpu_rows):
            assert sorted([tuple(row) for row in rows], key=repr) == sorted(expected, key=repr)
        return cpu_rows, gpu_rows
    return check


@iceberg
@pytest.mark.skipif(not supports_iceberg_v3, reason=ICEBERG_V3_UNSUPPORTED_REASON)
@pytest.mark.skipif(is_iceberg_remote_catalog(), reason='Requires local equality-delete UDF')
@pytest.mark.parametrize('reader_type', ['PERFILE', 'MULTITHREADED'])
@pytest.mark.parametrize('project_keys', [False, True], ids=['implicit', 'projected'])
@ignore_order(local=True)
def test_iceberg_v3_supported_defaults_with_equality_deletes(
        spark_tmp_table_factory, spark_tmp_path, register_iceberg_add_eq_deletes_udf,
        reader_type, project_keys):
    table_name = get_full_table_name(spark_tmp_table_factory)

    def setup(spark):
        spark.sql(f'CREATE TABLE {table_name} (id BIGINT, p INT) USING ICEBERG PARTITIONED BY (p) '
                  "TBLPROPERTIES ('format-version'='3')")
        spark.createDataFrame([(0, 0), (1, 0), (2, 0)], 'id BIGINT, p INT') \
            .coalesce(1).writeTo(table_name).append()
        _add_supported_delete_defaults(spark, table_name)
        spark.createDataFrame([(3, 0, None, None), (4, 0, 11, 'legacy'), (5, 0, 23, 'current')],
            'id BIGINT, p INT, delete_key INT, string_key STRING') \
            .coalesce(1).writeTo(table_name).append()
        # A multi-column delete distinguishes rows sharing the same default. Delete one row
        # whose key is synthesized and one whose matching key is physically present.
        deletes = spark.table(table_name).where('id IN (0, 4)') \
            .select('id', 'delete_key', 'string_key', '_partition').coalesce(1)
        _add_eq_deletes_from_df(spark, deletes, table_name, spark_tmp_path)
        files = spark.sql(f'SELECT content FROM {table_name}.delete_files').collect()
        assert files and all(row.content == 2 for row in files)

    with_cpu_session(setup)
    columns = 'id, delete_key, string_key' if project_keys else 'id'
    expected = ([(1, 11, 'legacy'), (2, 11, 'legacy'), (3, None, None), (5, 23, 'current')]
                if project_keys else [(1,), (2,), (3,), (5,)])
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.sql(f'SELECT {columns} FROM {table_name}'),
        conf={'spark.rapids.sql.format.iceberg.v3.enabled': 'true',
              'spark.rapids.sql.format.parquet.reader.type': reader_type},
        result_canonicalize_func_before_compare=_assert_delete_rows(expected))


@iceberg
@pytest.mark.skipif(not supports_iceberg_v3, reason=ICEBERG_V3_UNSUPPORTED_REASON)
@pytest.mark.skipif(is_iceberg_remote_catalog(), reason='Requires local equality-delete UDF')
@pytest.mark.parametrize('reader_type', ['PERFILE', 'MULTITHREADED'])
@pytest.mark.parametrize('delete_kind', ['position', 'dv', 'dv-equality'])
@ignore_order(local=True)
def test_iceberg_v3_defaults_with_position_and_mixed_deletes(
        spark_tmp_table_factory, spark_tmp_path, register_iceberg_add_eq_deletes_udf,
        reader_type, delete_kind):
    table_name = get_full_table_name(spark_tmp_table_factory)

    def setup(spark):
        version = '2' if delete_kind == 'position' else '3'
        spark.sql(f'CREATE TABLE {table_name} (id BIGINT, p INT) USING ICEBERG PARTITIONED BY (p) '
                  f"TBLPROPERTIES ('format-version'='{version}', "
                  "'write.delete.mode'='merge-on-read')")
        spark.createDataFrame([(i, 0) for i in range(6)], 'id BIGINT, p INT') \
            .coalesce(1).writeTo(table_name).append()
        spark.sql(f'DELETE FROM {table_name} WHERE id = 0')
        if delete_kind == 'position':
            spark.sql(f"ALTER TABLE {table_name} SET TBLPROPERTIES ('format-version'='3')")
        _add_supported_delete_defaults(spark, table_name)
        spark.createDataFrame([(6, 0, None, None), (7, 0, 23, 'current')],
            'id BIGINT, p INT, delete_key INT, string_key STRING') \
            .coalesce(1).writeTo(table_name).append()
        if delete_kind == 'dv-equality':
            deletes = spark.table(table_name).where('id = 1') \
                .select('id', 'delete_key', 'string_key', '_partition').coalesce(1)
            _add_eq_deletes_from_df(spark, deletes, table_name, spark_tmp_path)
        files = {(row.content, row.file_format) for row in
                 spark.sql(f'SELECT content, file_format FROM {table_name}.delete_files').collect()}
        expected = {(1, 'PARQUET')} if delete_kind == 'position' else {(1, 'PUFFIN')}
        if delete_kind == 'dv-equality':
            expected.add((2, 'PARQUET'))
        assert files == expected

    with_cpu_session(setup)
    legacy_ids = list(range(2 if delete_kind == 'dv-equality' else 1, 6))
    expected = [(i, 11, 'legacy') for i in legacy_ids] + [(6, None, None), (7, 23, 'current')]
    conf = {'spark.rapids.sql.format.iceberg.v3.enabled': 'true',
            'spark.rapids.sql.format.parquet.reader.type': reader_type,
            'spark.sql.adaptive.enabled': 'false'}

    def read(spark, projection):
        if spark.conf.get('spark.rapids.sql.enabled') != 'true':
            # Iceberg 1.10.1 caches equality-delete rows in the first projection's field order.
            # Keep the CPU oracle independent of that cache when changing projection order.
            spark.conf.set('spark.sql.iceberg.executor-cache.delete-files.enabled', 'false')
        return spark.sql(f'SELECT {projection} FROM {table_name}')

    # Old files have no projected physical column here. The DV reader must retain a physical
    # helper, apply native filtering, and size generated defaults to the surviving row count.
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: read(spark, 'delete_key, string_key'),
        conf=conf,
        result_canonicalize_func_before_compare=_assert_delete_rows([row[1:] for row in expected]))

    def check_metadata(cpu_rows, gpu_rows):
        for rows in (cpu_rows, gpu_rows):
            assert sorted((row.id, row.delete_key, row.string_key) for row in rows) == expected
        return cpu_rows, gpu_rows

    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: read(
            spark, 'id, delete_key, string_key, _pos, _row_id, _last_updated_sequence_number'),
        conf=conf,
        result_canonicalize_func_before_compare=check_metadata)
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: read(spark, 'count(*)'),
        conf=conf,
        result_canonicalize_func_before_compare=_assert_delete_rows([(len(expected),)]))
