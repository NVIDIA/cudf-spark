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
from pyspark.sql import functions as F

from asserts import assert_equal_with_local_sort, assert_gpu_fallback_collect
from conftest import is_iceberg_remote_catalog
from data_gen import gen_df, copy_and_update, StringGen
from iceberg import create_iceberg_table, \
    iceberg_base_table_cols, iceberg_gens_list, \
    get_full_table_name, iceberg_full_gens_list, iceberg_nested_write_gens_list, \
    iceberg_write_enabled_conf, iceberg_unsupported_mark, _build_tblprops, \
    overwrite_static_partition_transforms
from marks import iceberg, ignore_order, allow_non_gpu, datagen_overrides
from spark_session import with_gpu_session, with_cpu_session

pytestmark = iceberg_unsupported_mark

# Seed for initial insert to ensure different data from overwrite
INITIAL_INSERT_SEED = 7893676382

# Configuration for static overwrite tests
iceberg_static_overwrite_conf = copy_and_update(iceberg_write_enabled_conf, {
    "spark.sql.sources.partitionOverwriteMode": "STATIC"
})


def do_test_insert_overwrite_table_sql(spark_tmp_table_factory,
                                       create_table_func: Callable[[str], Any]):
    """Test INSERT OVERWRITE for Iceberg tables comparing GPU vs CPU results."""
    base_table_name = get_full_table_name(spark_tmp_table_factory)
    cpu_table_name = f"{base_table_name}_cpu"
    gpu_table_name = f"{base_table_name}_gpu"

    create_table_func(cpu_table_name)
    create_table_func(gpu_table_name)

    def insert_initial_data(spark, table_name: str):
        """Insert initial data into the table."""
        df = gen_df(spark, list(zip(iceberg_base_table_cols, iceberg_gens_list)), seed=INITIAL_INSERT_SEED)
        view_name = spark_tmp_table_factory.get()
        df.createOrReplaceTempView(view_name)
        spark.sql(f"INSERT INTO {table_name} SELECT * FROM {view_name}")

    # Insert initial data on both CPU and GPU
    with_cpu_session(lambda spark: insert_initial_data(spark, cpu_table_name),
                     conf = iceberg_static_overwrite_conf)
    with_gpu_session(lambda spark: insert_initial_data(spark, gpu_table_name),
                     conf = iceberg_static_overwrite_conf)

    def overwrite_data(spark, table_name: str):
        """Overwrite the table with new data."""
        df = gen_df(spark, list(zip(iceberg_base_table_cols, iceberg_gens_list)))
        view_name = spark_tmp_table_factory.get()
        df.createOrReplaceTempView(view_name)
        spark.sql(f"INSERT OVERWRITE TABLE {table_name} SELECT * FROM {view_name}")

    # Perform overwrite on GPU and CPU
    with_gpu_session(lambda spark: overwrite_data(spark, gpu_table_name),
                     conf = iceberg_static_overwrite_conf)
    with_cpu_session(lambda spark: overwrite_data(spark, cpu_table_name),
                     conf = iceberg_static_overwrite_conf)

    # Compare results
    cpu_data = with_cpu_session(lambda spark: spark.table(cpu_table_name).collect())
    gpu_data = with_cpu_session(lambda spark: spark.table(gpu_table_name).collect())
    assert_equal_with_local_sort(cpu_data, gpu_data)


@iceberg
@ignore_order(local=True)
def test_insert_overwrite_unpartitioned_table(spark_tmp_table_factory):
    """Test INSERT OVERWRITE on unpartitioned Iceberg tables."""
    table_prop = {"format-version": "2"}

    do_test_insert_overwrite_table_sql(
        spark_tmp_table_factory,
        lambda table_name: create_iceberg_table(table_name, table_prop=table_prop))


@iceberg
@ignore_order(local=True)
@allow_non_gpu('OverwriteByExpressionExec', 'AppendDataExec')
@pytest.mark.skipif(is_iceberg_remote_catalog(), reason="Skip for remote catalog to reduce test time")
def test_insert_overwrite_unpartitioned_table_values(spark_tmp_table_factory):
    """Test INSERT OVERWRITE on unpartitioned tables with VALUES syntax."""
    base_table_name = get_full_table_name(spark_tmp_table_factory)
    cpu_table_name = f"{base_table_name}_cpu"
    gpu_table_name = f"{base_table_name}_gpu"

    def create_table(spark, table_name: str):
        props = _build_tblprops({"format-version": "2"})
        props_sql = ", ".join(f"'{k}' = '{v}'" for k, v in props.items())
        spark.sql(f"CREATE TABLE {table_name} (id int, name string) USING ICEBERG "
                  f"TBLPROPERTIES ({props_sql})")

    with_cpu_session(lambda spark: create_table(spark, cpu_table_name))
    with_cpu_session(lambda spark: create_table(spark, gpu_table_name))

    def insert_initial_data(spark, table_name: str):
        spark.sql(f"INSERT INTO {table_name} VALUES (1, 'a'), (2, 'b'), (3, 'c')")

    def overwrite_data(spark, table_name: str):
        spark.sql(f"INSERT OVERWRITE TABLE {table_name} VALUES (4, 'd'), (5, 'e')")

    # Insert initial data
    with_cpu_session(lambda spark: insert_initial_data(spark, cpu_table_name),
                     conf = iceberg_static_overwrite_conf)
    with_gpu_session(lambda spark: insert_initial_data(spark, gpu_table_name),
                     conf = iceberg_static_overwrite_conf)

    # Overwrite
    with_gpu_session(lambda spark: overwrite_data(spark, gpu_table_name),
                     conf = iceberg_static_overwrite_conf)
    with_cpu_session(lambda spark: overwrite_data(spark, cpu_table_name),
                     conf = iceberg_static_overwrite_conf)

    cpu_data = with_cpu_session(lambda spark: spark.table(cpu_table_name).collect())
    gpu_data = with_cpu_session(lambda spark: spark.table(gpu_table_name).collect())
    assert_equal_with_local_sort(cpu_data, gpu_data)


def _do_test_insert_overwrite_partitioned_table(spark_tmp_table_factory, partition_col_sql, table_prop=None):
    """Helper function for partitioned table INSERT OVERWRITE tests."""
    if table_prop is None:
        table_prop = {"format-version": "2"}

    def create_table_and_set_write_order(table_name: str):
        create_iceberg_table(
            table_name,
            partition_col_sql=partition_col_sql,
            table_prop=table_prop)

        sql = f"ALTER TABLE {table_name} WRITE ORDERED BY _c2, _c3, _c4"
        with_cpu_session(lambda spark: spark.sql(sql).collect())

    do_test_insert_overwrite_table_sql(
        spark_tmp_table_factory,
        create_table_and_set_write_order)


@iceberg
@datagen_overrides(seed=0, reason='https://github.com/NVIDIA/spark-rapids-jni/issues/4016')
@ignore_order(local=True)
@pytest.mark.parametrize("partition_col_sql", [
    pytest.param("year(_c9)", id="year(timestamp_col)"),
])
def test_insert_overwrite_partitioned_table(spark_tmp_table_factory, partition_col_sql):
    """Basic partition test - runs for all catalogs including remote."""
    _do_test_insert_overwrite_partitioned_table(spark_tmp_table_factory, partition_col_sql)


@iceberg
@datagen_overrides(seed=0, reason='https://github.com/NVIDIA/spark-rapids-jni/issues/4016')
@ignore_order(local=True)
@pytest.mark.skipif(is_iceberg_remote_catalog(), reason="Skip for remote catalog to reduce test time")
@pytest.mark.parametrize("partition_col_sql", overwrite_static_partition_transforms)
def test_insert_overwrite_partitioned_table_full_coverage(spark_tmp_table_factory, partition_col_sql):
    """Sanity-check INSERT OVERWRITE against a few partition transforms distinct
    from those picked by other DML ops. The 26-transform partition-writer coverage
    anchor lives in iceberg_append_test.py::test_insert_into_partitioned_table_full_coverage."""
    _do_test_insert_overwrite_partitioned_table(spark_tmp_table_factory, partition_col_sql)


@iceberg
@ignore_order(local=True)
@pytest.mark.skipif(is_iceberg_remote_catalog(), reason="Skip for remote catalog to reduce test time")
def test_insert_overwrite_unpartitioned_table_nested_types(spark_tmp_table_factory):
    """Test INSERT OVERWRITE with Iceberg-native nested types on GPU."""
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

    def insert_initial_data(spark, table_name):
        df = gen_df(spark, gen_list, seed=INITIAL_INSERT_SEED)
        view_name = spark_tmp_table_factory.get()
        df.createOrReplaceTempView(view_name)
        spark.sql(f"INSERT INTO {table_name} SELECT * FROM {view_name}")

    with_cpu_session(lambda spark: insert_initial_data(spark, cpu_table_name),
                     conf=iceberg_static_overwrite_conf)
    with_gpu_session(lambda spark: insert_initial_data(spark, gpu_table_name),
                     conf=iceberg_static_overwrite_conf)

    def overwrite_data(spark, table_name):
        df = this_gen_df(spark)
        view_name = spark_tmp_table_factory.get()
        df.createOrReplaceTempView(view_name)
        return spark.sql(f"INSERT OVERWRITE TABLE {table_name} SELECT * FROM {view_name}")

    with_gpu_session(lambda spark: overwrite_data(spark, gpu_table_name).collect(),
                     conf=iceberg_static_overwrite_conf)
    with_cpu_session(lambda spark: overwrite_data(spark, cpu_table_name).collect(),
                     conf=iceberg_static_overwrite_conf)

    cpu_data = with_cpu_session(lambda spark: spark.table(cpu_table_name).collect())
    gpu_data = with_cpu_session(lambda spark: spark.table(gpu_table_name).collect())
    assert_equal_with_local_sort(cpu_data, gpu_data)


@iceberg
@ignore_order(local=True)
@pytest.mark.skipif(is_iceberg_remote_catalog(), reason="Skip for remote catalog to reduce test time")
def test_insert_overwrite_unpartitioned_table_all_cols(spark_tmp_table_factory):
    """Test INSERT OVERWRITE on unpartitioned table with all Iceberg write types on GPU."""
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

    def insert_initial_data(spark, table_name):
        df = gen_df(spark, gen_list, seed=INITIAL_INSERT_SEED)
        view_name = spark_tmp_table_factory.get()
        df.createOrReplaceTempView(view_name)
        spark.sql(f"INSERT INTO {table_name} SELECT * FROM {view_name}")

    with_cpu_session(lambda spark: insert_initial_data(spark, cpu_table_name),
                     conf=iceberg_static_overwrite_conf)
    with_gpu_session(lambda spark: insert_initial_data(spark, gpu_table_name),
                     conf=iceberg_static_overwrite_conf)

    def overwrite_data(spark, table_name):
        df = this_gen_df(spark)
        view_name = spark_tmp_table_factory.get()
        df.createOrReplaceTempView(view_name)
        return spark.sql(f"INSERT OVERWRITE TABLE {table_name} SELECT * FROM {view_name}")

    with_gpu_session(lambda spark: overwrite_data(spark, gpu_table_name).collect(),
                     conf=iceberg_static_overwrite_conf)
    with_cpu_session(lambda spark: overwrite_data(spark, cpu_table_name).collect(),
                     conf=iceberg_static_overwrite_conf)

    cpu_data = with_cpu_session(lambda spark: spark.table(cpu_table_name).collect())
    gpu_data = with_cpu_session(lambda spark: spark.table(gpu_table_name).collect())
    assert_equal_with_local_sort(cpu_data, gpu_data)


@iceberg
@ignore_order(local=True)
@pytest.mark.skipif(is_iceberg_remote_catalog(), reason="Skip for remote catalog to reduce test time")
def test_insert_overwrite_partitioned_table_nested_types(spark_tmp_table_factory):
    """Test INSERT OVERWRITE on partitioned table with Iceberg-native nested types on GPU."""
    table_prop = {"format-version": "2"}
    cols = [f"_c{idx}" for idx, _ in enumerate(iceberg_nested_write_gens_list)]
    gen_list = list(zip(cols, iceberg_nested_write_gens_list))
    partition_col_sql = "bucket(16, _c0), bucket(16, _c1)"

    def this_gen_df(spark):
        return gen_df(spark, gen_list)

    base_table_name = get_full_table_name(spark_tmp_table_factory)
    cpu_table_name = f"{base_table_name}_cpu"
    gpu_table_name = f"{base_table_name}_gpu"

    create_iceberg_table(cpu_table_name, partition_col_sql=partition_col_sql,
                         table_prop=table_prop, df_gen=this_gen_df)
    create_iceberg_table(gpu_table_name, partition_col_sql=partition_col_sql,
                         table_prop=table_prop, df_gen=this_gen_df)

    def insert_initial_data(spark, table_name):
        df = gen_df(spark, gen_list, seed=INITIAL_INSERT_SEED)
        view_name = spark_tmp_table_factory.get()
        df.createOrReplaceTempView(view_name)
        spark.sql(f"INSERT INTO {table_name} SELECT * FROM {view_name}")

    with_cpu_session(lambda spark: insert_initial_data(spark, cpu_table_name),
                     conf=iceberg_static_overwrite_conf)
    with_gpu_session(lambda spark: insert_initial_data(spark, gpu_table_name),
                     conf=iceberg_static_overwrite_conf)

    def overwrite_data(spark, table_name):
        df = this_gen_df(spark)
        view_name = spark_tmp_table_factory.get()
        df.createOrReplaceTempView(view_name)
        return spark.sql(f"INSERT OVERWRITE TABLE {table_name} SELECT * FROM {view_name}")

    with_gpu_session(lambda spark: overwrite_data(spark, gpu_table_name).collect(),
                     conf=iceberg_static_overwrite_conf)
    with_cpu_session(lambda spark: overwrite_data(spark, cpu_table_name).collect(),
                     conf=iceberg_static_overwrite_conf)

    cpu_data = with_cpu_session(lambda spark: spark.table(cpu_table_name).collect())
    gpu_data = with_cpu_session(lambda spark: spark.table(gpu_table_name).collect())
    assert_equal_with_local_sort(cpu_data, gpu_data)


@iceberg
@ignore_order(local=True)
@pytest.mark.skipif(is_iceberg_remote_catalog(), reason="Skip for remote catalog to reduce test time")
def test_insert_overwrite_partitioned_table_all_cols(spark_tmp_table_factory):
    """Test INSERT OVERWRITE on partitioned table with all Iceberg write types on GPU."""
    table_prop = {"format-version": "2"}
    cols = [f"_c{idx}" for idx, _ in enumerate(iceberg_full_gens_list)]
    gen_list = list(zip(cols, iceberg_full_gens_list))
    partition_col_sql = "bucket(16, _c2), bucket(16, _c3)"

    def this_gen_df(spark):
        return gen_df(spark, gen_list)

    base_table_name = get_full_table_name(spark_tmp_table_factory)
    cpu_table_name = f"{base_table_name}_cpu"
    gpu_table_name = f"{base_table_name}_gpu"

    create_iceberg_table(cpu_table_name, partition_col_sql=partition_col_sql,
                         table_prop=table_prop, df_gen=this_gen_df)
    create_iceberg_table(gpu_table_name, partition_col_sql=partition_col_sql,
                         table_prop=table_prop, df_gen=this_gen_df)

    def insert_initial_data(spark, table_name):
        df = gen_df(spark, gen_list, seed=INITIAL_INSERT_SEED)
        view_name = spark_tmp_table_factory.get()
        df.createOrReplaceTempView(view_name)
        spark.sql(f"INSERT INTO {table_name} SELECT * FROM {view_name}")

    with_cpu_session(lambda spark: insert_initial_data(spark, cpu_table_name),
                     conf=iceberg_static_overwrite_conf)
    with_gpu_session(lambda spark: insert_initial_data(spark, gpu_table_name),
                     conf=iceberg_static_overwrite_conf)

    def overwrite_data(spark, table_name):
        df = this_gen_df(spark)
        view_name = spark_tmp_table_factory.get()
        df.createOrReplaceTempView(view_name)
        return spark.sql(f"INSERT OVERWRITE TABLE {table_name} SELECT * FROM {view_name}")

    with_gpu_session(lambda spark: overwrite_data(spark, gpu_table_name).collect(),
                     conf=iceberg_static_overwrite_conf)
    with_cpu_session(lambda spark: overwrite_data(spark, cpu_table_name).collect(),
                     conf=iceberg_static_overwrite_conf)

    cpu_data = with_cpu_session(lambda spark: spark.table(cpu_table_name).collect())
    gpu_data = with_cpu_session(lambda spark: spark.table(gpu_table_name).collect())
    assert_equal_with_local_sort(cpu_data, gpu_data)


@iceberg
@ignore_order(local=True)
@allow_non_gpu('OverwriteByExpressionExec', 'ShuffleExchangeExec', 'SortExec', 'ProjectExec')
@pytest.mark.skipif(is_iceberg_remote_catalog(), reason="Skip for remote catalog to reduce test time")
@pytest.mark.parametrize("file_format", ["orc", "avro"], ids=lambda x: f"file_format={x}")
def test_insert_overwrite_table_unsupported_file_format_fallback(
        spark_tmp_table_factory, file_format):
    """Test that unsupported file formats fall back to CPU."""
    table_prop = {"format-version": "2",
                  "write.format.default": file_format}

    def insert_initial_data(spark, table_name: str):
        df = gen_df(spark, list(zip(iceberg_base_table_cols, iceberg_gens_list)), seed=INITIAL_INSERT_SEED)
        view_name = spark_tmp_table_factory.get()
        df.createOrReplaceTempView(view_name)
        spark.sql(f"INSERT INTO {table_name} SELECT * FROM {view_name}")
    
    def overwrite_data(spark, table_name: str):
        df = gen_df(spark, list(zip(iceberg_base_table_cols, iceberg_gens_list)))
        view_name = spark_tmp_table_factory.get()
        df.createOrReplaceTempView(view_name)
        return spark.sql(f"INSERT OVERWRITE TABLE {table_name} SELECT * FROM {view_name}")

    table_name = get_full_table_name(spark_tmp_table_factory)
    create_iceberg_table(table_name, table_prop=table_prop)

    # Initial insert (not tested for fallback)
    with_cpu_session(lambda spark: insert_initial_data(spark, table_name),
                     conf = iceberg_static_overwrite_conf)

    # Assert that overwrite falls back to CPU
    assert_gpu_fallback_collect(lambda spark: overwrite_data(spark, table_name),
                                "OverwriteByExpressionExec",
                                conf = iceberg_static_overwrite_conf)


@iceberg
@ignore_order(local=True)
@allow_non_gpu('OverwriteByExpressionExec', 'ShuffleExchangeExec', 'SortExec', 'ProjectExec')
@pytest.mark.skipif(is_iceberg_remote_catalog(), reason="Skip for remote catalog to reduce test time")
@pytest.mark.parametrize("conf_key", ["spark.rapids.sql.format.iceberg.enabled",
                                      "spark.rapids.sql.format.iceberg.write.enabled"],
                         ids=lambda x: f"{x}=False")
def test_insert_overwrite_iceberg_table_fallback_when_conf_disabled(
        spark_tmp_table_factory, conf_key):
    """Test that overwrite falls back to CPU when Iceberg write is disabled."""
    table_prop = {"format-version": "2"}

    def insert_initial_data(spark, table_name: str):
        df = gen_df(spark, list(zip(iceberg_base_table_cols, iceberg_gens_list)), seed=INITIAL_INSERT_SEED)
        view_name = spark_tmp_table_factory.get()
        df.createOrReplaceTempView(view_name)
        spark.sql(f"INSERT INTO {table_name} SELECT * FROM {view_name}")
    
    def overwrite_data(spark, table_name: str):
        df = gen_df(spark, list(zip(iceberg_base_table_cols, iceberg_gens_list)))
        view_name = spark_tmp_table_factory.get()
        df.createOrReplaceTempView(view_name)
        return spark.sql(f"INSERT OVERWRITE TABLE {table_name} SELECT * FROM {view_name}")

    table_name = get_full_table_name(spark_tmp_table_factory)
    create_iceberg_table(table_name, table_prop=table_prop)

    # Initial insert (not tested for fallback)
    with_cpu_session(lambda spark: insert_initial_data(spark, table_name),
                     conf = iceberg_static_overwrite_conf)

    # Assert that overwrite falls back to CPU when config is disabled
    updated_conf = copy_and_update(iceberg_static_overwrite_conf, {conf_key: "false"})
    assert_gpu_fallback_collect(lambda spark: overwrite_data(spark, table_name),
                                "OverwriteByExpressionExec",
                                conf = updated_conf)


@iceberg
@ignore_order(local=True)
@pytest.mark.skipif(is_iceberg_remote_catalog(), reason="Skip for remote catalog to reduce test time")
def test_insert_overwrite_static_after_drop_partition_field(spark_tmp_table_factory):
    """Test INSERT OVERWRITE (static mode) on table after dropping a partition field (void transform).
    
    When a partition field is dropped, Iceberg creates a 'void transform' - 
    the field remains in the partition spec but no longer affects partitioning.
    This test verifies INSERT OVERWRITE still runs correctly on GPU after partition evolution.
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
    
    # Insert initial data before partition evolution
    def insert_initial_data(spark, table_name):
        df = gen_df(spark, list(zip(iceberg_base_table_cols, iceberg_gens_list)), seed=INITIAL_INSERT_SEED)
        df.writeTo(table_name).append()
    
    with_cpu_session(lambda spark: insert_initial_data(spark, cpu_table_name))
    with_cpu_session(lambda spark: insert_initial_data(spark, gpu_table_name))
    
    # Drop one partition field on both tables (creates void transform)
    def drop_partition_field(spark, table_name):
        spark.sql(f"ALTER TABLE {table_name} DROP PARTITION FIELD bucket(8, _c2)")
    
    with_cpu_session(lambda spark: drop_partition_field(spark, cpu_table_name))
    with_cpu_session(lambda spark: drop_partition_field(spark, gpu_table_name))
    
    # INSERT OVERWRITE after partition evolution - this is the operation we're testing on GPU
    def overwrite_data(spark, table_name):
        df = gen_df(spark, list(zip(iceberg_base_table_cols, iceberg_gens_list)), seed=INITIAL_INSERT_SEED + 1)
        view_name = spark_tmp_table_factory.get()
        df.createOrReplaceTempView(view_name)
        spark.sql(f"INSERT OVERWRITE TABLE {table_name} SELECT * FROM {view_name}")
    
    with_gpu_session(lambda spark: overwrite_data(spark, gpu_table_name),
                     conf=iceberg_static_overwrite_conf)
    with_cpu_session(lambda spark: overwrite_data(spark, cpu_table_name),
                     conf=iceberg_static_overwrite_conf)
    
    # Compare results
    cpu_data = with_cpu_session(lambda spark: spark.table(cpu_table_name).collect())
    gpu_data = with_cpu_session(lambda spark: spark.table(gpu_table_name).collect())
    assert_equal_with_local_sort(cpu_data, gpu_data)


@iceberg
@ignore_order(local=True)
@allow_non_gpu('ShuffleExchangeExec')
@pytest.mark.skipif(is_iceberg_remote_catalog(), reason="Skip for remote catalog to reduce test time")
def test_insert_overwrite_static_df_api_truncate_string(spark_tmp_table_factory):
    """Test static overwrite via DataFrame writeTo().overwrite() API with truncate(5, string_col)
    partitioning. Verifies GPU writes produce Parquet files with correct Iceberg field IDs
    so that file-level statistics are available for overwrite validation.
    """
    truncate_width = 5
    str_length = truncate_width - 2
    prefix = "T" * str_length
    partition_col_sql = f"truncate({truncate_width}, _c6)"
    partition_filter = f"_c6 >= '{prefix}10' AND _c6 < '{prefix}20'"

    table_prop = _build_tblprops({"format-version": "2",
                                  "write.format.default": "parquet"})

    conf = copy_and_update(iceberg_static_overwrite_conf, {
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
    })

    # Use standard iceberg schema but override _c6 with a constrained string gen
    # to produce predictable truncate partitions for the range filter
    gen_list = list(zip(iceberg_base_table_cols, iceberg_gens_list))
    gen_list[6] = ('_c6', StringGen(pattern=f'{prefix}[1-9][0-9][A-Z]{{3}}'))

    base_table_name = get_full_table_name(spark_tmp_table_factory)
    cpu_table_name = f"{base_table_name}_cpu"
    gpu_table_name = f"{base_table_name}_gpu"

    def create_table_with_ctas(spark, table_name):
        df = gen_df(spark, gen_list, seed=INITIAL_INSERT_SEED)
        view_name = spark_tmp_table_factory.get()
        df.createOrReplaceTempView(view_name)
        props_sql = ", ".join([f"'{k}' = '{v}'" for k, v in table_prop.items()])
        spark.sql(f"CREATE TABLE {table_name} USING ICEBERG "
                  f"PARTITIONED BY ({partition_col_sql}) "
                  f"TBLPROPERTIES ({props_sql}) "
                  f"AS SELECT * FROM {view_name}")

    with_cpu_session(lambda spark: create_table_with_ctas(spark, cpu_table_name), conf=conf)
    with_gpu_session(lambda spark: create_table_with_ctas(spark, gpu_table_name), conf=conf)

    def overwrite_data(spark, table_name):
        df = gen_df(spark, gen_list, seed=INITIAL_INSERT_SEED + 1)
        filtered_df = df.filter(partition_filter)
        filtered_df.writeTo(table_name).overwrite(F.expr(partition_filter))

    with_cpu_session(lambda spark: overwrite_data(spark, cpu_table_name), conf=conf)
    with_gpu_session(lambda spark: overwrite_data(spark, gpu_table_name), conf=conf)

    cpu_data = with_cpu_session(lambda spark: spark.table(cpu_table_name).collect())
    gpu_data = with_cpu_session(lambda spark: spark.table(gpu_table_name).collect())
    assert_equal_with_local_sort(cpu_data, gpu_data)


@iceberg
@datagen_overrides(seed=0, reason='https://github.com/NVIDIA/spark-rapids-jni/issues/4016')
@ignore_order(local=True)
@pytest.mark.skipif(is_iceberg_remote_catalog(), reason="Skip for remote catalog to reduce test time")
def test_insert_overwrite_partitioned_table_fanout_enabled(spark_tmp_table_factory):
    # Use bucket(2, ...) to keep partition count low and avoid OOM from Iceberg's FanoutDataWriter.
    _do_test_insert_overwrite_partitioned_table(
        spark_tmp_table_factory,
        "bucket(2, _c9)",
        table_prop={"format-version": "2", "write.spark.fanout.enabled": "true"})
