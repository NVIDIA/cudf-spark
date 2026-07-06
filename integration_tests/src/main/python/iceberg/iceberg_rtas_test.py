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

from typing import Callable, Dict, Optional

import pytest

from asserts import assert_equal_with_local_sort, assert_gpu_fallback_collect
from conftest import is_iceberg_remote_catalog
from data_gen import gen_df, copy_and_update
from iceberg import (create_iceberg_table,
                     iceberg_base_table_cols,
                     iceberg_gens_list, iceberg_full_gens_list,
                     iceberg_nested_write_gens_list,
                     get_full_table_name, iceberg_write_enabled_conf,
                     iceberg_unsupported_mark, _build_tblprops,
                     rtas_partition_transforms)
from marks import iceberg, ignore_order, allow_non_gpu, allow_non_gpu_conditional, datagen_overrides
from spark_session import with_gpu_session, with_cpu_session, is_spark_400_or_later

pytestmark = [
    iceberg_unsupported_mark,
]


def _props_to_sql(table_prop: Dict[str, str]) -> str:
    return ", ".join([f"'{k}' = '{v}'" for k, v in table_prop.items()])


def _execute_rtas(spark,
                  target_table: str,
                  spark_tmp_table_factory,
                  df_gen: Callable,
                  table_prop: Dict[str, str],
                  partition_col_sql: Optional[str] = None,
                  create_or_replace: bool = False,
                  ret = True):
    view_name = spark_tmp_table_factory.get()
    df = df_gen(spark)
    df.createOrReplaceTempView(view_name)

    replace_clause = "CREATE OR REPLACE" if create_or_replace else "REPLACE"
    partition_clause = "" if partition_col_sql is None else f"PARTITIONED BY ({partition_col_sql}) "
    table_prop = _build_tblprops(table_prop)
    props_sql = _props_to_sql(table_prop)
    df = spark.sql(
        f"{replace_clause} TABLE {target_table} USING ICEBERG {partition_clause}"
        f"TBLPROPERTIES ({props_sql}) AS SELECT * FROM {view_name}")

    if ret:
        return df
    else:
        return None


def _assert_gpu_equals_cpu_rtas(spark_tmp_table_factory,
                                df_gen: Callable,
                                table_prop: Dict[str, str],
                                partition_col_sql: Optional[str] = None,
                                create_or_replace: bool = False,
                                conf: Optional[Dict[str, str]] = None):
    if conf is None:
        conf = iceberg_write_enabled_conf

    base_name = get_full_table_name(spark_tmp_table_factory)
    gpu_table = f"{base_name}_gpu"
    cpu_table = f"{base_name}_cpu"

    initial_df_gen = lambda spark: gen_df(spark, list(zip(iceberg_base_table_cols, iceberg_gens_list)))
    create_iceberg_table(gpu_table, partition_col_sql, table_prop, initial_df_gen)
    create_iceberg_table(cpu_table, partition_col_sql, table_prop, initial_df_gen)

    def run_gpu_rtas(spark):
        _execute_rtas(spark, gpu_table, spark_tmp_table_factory,
                      df_gen, table_prop, partition_col_sql,
                      create_or_replace, ret=True)

    with_gpu_session(run_gpu_rtas, conf=conf)
    with_cpu_session(lambda spark: _execute_rtas(spark, cpu_table, spark_tmp_table_factory,
                                                 df_gen, table_prop, partition_col_sql,
                                                 create_or_replace, False),
                     conf=conf)

    cpu_data = with_cpu_session(lambda spark: spark.table(cpu_table).collect())
    gpu_data = with_cpu_session(lambda spark: spark.table(gpu_table).collect())
    assert_equal_with_local_sort(cpu_data, gpu_data)


@iceberg
@ignore_order(local=True)
def test_rtas_unpartitioned_table(spark_tmp_table_factory):
    table_prop = {
        "format-version": "2"
    }

    df_gen = lambda spark: gen_df(spark, list(zip(iceberg_base_table_cols, iceberg_gens_list)))

    _assert_gpu_equals_cpu_rtas(spark_tmp_table_factory, df_gen, table_prop)


def _do_test_rtas_partitioned_table(spark_tmp_table_factory, partition_col_sql, table_prop=None):
    """Helper function for partitioned table RTAS tests."""
    if table_prop is None:
        table_prop = {
            "format-version": "2"
        }

    df_gen = lambda spark: gen_df(spark, list(zip(iceberg_base_table_cols, iceberg_gens_list)))

    _assert_gpu_equals_cpu_rtas(spark_tmp_table_factory,
                                df_gen,
                                table_prop,
                                partition_col_sql=partition_col_sql)


@iceberg
@datagen_overrides(seed=0, reason='https://github.com/NVIDIA/spark-rapids-jni/issues/4016')
@ignore_order(local=True)
@pytest.mark.parametrize("partition_col_sql", [
    pytest.param("year(_c9)", id="year(timestamp_col)"),
])
def test_rtas_partitioned_table(spark_tmp_table_factory, partition_col_sql):
    """Basic partition test - runs for all catalogs including remote."""
    _do_test_rtas_partitioned_table(spark_tmp_table_factory, partition_col_sql)


@iceberg
@datagen_overrides(seed=0, reason='https://github.com/NVIDIA/spark-rapids-jni/issues/4016')
@ignore_order(local=True)
@pytest.mark.skipif(is_iceberg_remote_catalog(), reason="Skip for remote catalog to reduce test time")
@pytest.mark.parametrize("partition_col_sql", rtas_partition_transforms)
@allow_non_gpu_conditional(is_spark_400_or_later(), "EmptyRelationExec")
def test_rtas_partitioned_table_full_coverage(spark_tmp_table_factory, partition_col_sql):
    """Sanity-check RTAS against a few partition transforms distinct from those
    picked by other DML ops. The 26-transform partition-writer coverage anchor
    lives in iceberg_append_test.py::test_insert_into_partitioned_table_full_coverage."""
    _do_test_rtas_partitioned_table(spark_tmp_table_factory, partition_col_sql)


@iceberg
@ignore_order(local=True)
@allow_non_gpu_conditional(is_spark_400_or_later(), "EmptyRelationExec")
def test_create_or_replace_table(spark_tmp_table_factory):
    """Test CREATE OR REPLACE TABLE AS SELECT when table doesn't exist"""
    table_prop = {
        "format-version": "2"
    }

    base_name = get_full_table_name(spark_tmp_table_factory)
    gpu_table = f"{base_name}_gpu"
    cpu_table = f"{base_name}_cpu"

    df_gen = lambda spark: gen_df(spark, list(zip(iceberg_base_table_cols, iceberg_gens_list)))

    # Execute CREATE OR REPLACE TABLE
    with_gpu_session(lambda spark: _execute_rtas(spark, gpu_table, spark_tmp_table_factory,
                                                 df_gen, table_prop, None, True, False),
                     conf=iceberg_write_enabled_conf)
    with_cpu_session(lambda spark: _execute_rtas(spark, cpu_table, spark_tmp_table_factory,
                                                 df_gen, table_prop, None, True, False),
                     conf=iceberg_write_enabled_conf)

    cpu_data = with_cpu_session(lambda spark: spark.table(cpu_table).collect())
    gpu_data = with_cpu_session(lambda spark: spark.table(gpu_table).collect())
    assert_equal_with_local_sort(cpu_data, gpu_data)


@iceberg
@ignore_order(local=True)
@allow_non_gpu('AtomicReplaceTableAsSelectExec', 'AppendDataExec')
@pytest.mark.skipif(is_iceberg_remote_catalog(), reason="Skip for remote catalog to reduce test time")
@pytest.mark.parametrize("file_format", ["orc", "avro"], ids=lambda x: f"file_format={x}")
@allow_non_gpu_conditional(is_spark_400_or_later(), "EmptyRelationExec")
def test_rtas_unsupported_file_format_fallback(spark_tmp_table_factory,
                                               file_format):
    table_prop = {
        "format-version": "2",
        "write.format.default": file_format
    }

    def run_rtas(spark):
        target = get_full_table_name(spark_tmp_table_factory)
        # Create initial table
        initial_df_gen = lambda sp: gen_df(sp, list(zip(iceberg_base_table_cols, iceberg_gens_list)))
        create_iceberg_table(target, None, table_prop, initial_df_gen)
        
        # Execute RTAS
        return _execute_rtas(spark,
                      target,
                      spark_tmp_table_factory,
                      lambda sp: gen_df(sp, list(zip(iceberg_base_table_cols, iceberg_gens_list))),
                      table_prop)

    assert_gpu_fallback_collect(run_rtas,
                                'AtomicReplaceTableAsSelectExec',
                                conf=iceberg_write_enabled_conf)


@iceberg
@ignore_order(local=True)
@allow_non_gpu('AtomicReplaceTableAsSelectExec', 'AppendDataExec')
@pytest.mark.skipif(is_iceberg_remote_catalog(), reason="Skip for remote catalog to reduce test time")
@pytest.mark.parametrize("conf_key", ["spark.rapids.sql.format.iceberg.enabled",
                                      "spark.rapids.sql.format.iceberg.write.enabled"],
                         ids=lambda x: f"{x}=False")
@allow_non_gpu_conditional(is_spark_400_or_later(), "EmptyRelationExec")
def test_rtas_fallback_when_conf_disabled(spark_tmp_table_factory,
                                          conf_key):
    table_prop = {
        "format-version": "2"
    }

    def run_rtas(spark):
        target = get_full_table_name(spark_tmp_table_factory)
        # Create initial table
        initial_df_gen = lambda sp: gen_df(sp, list(zip(iceberg_base_table_cols, iceberg_gens_list)))
        create_iceberg_table(target, None, table_prop, initial_df_gen)
        
        # Execute RTAS
        return _execute_rtas(spark,
                      target,
                      spark_tmp_table_factory,
                      lambda sp: gen_df(sp, list(zip(iceberg_base_table_cols, iceberg_gens_list))),
                      table_prop)

    updated_conf = copy_and_update(iceberg_write_enabled_conf, {conf_key: "false"})
    assert_gpu_fallback_collect(run_rtas,
                                'AtomicReplaceTableAsSelectExec',
                                conf=updated_conf)


@iceberg
@ignore_order(local=True)
@pytest.mark.skipif(is_iceberg_remote_catalog(), reason="Skip for remote catalog to reduce test time")
def test_rtas_unpartitioned_table_nested_types(spark_tmp_table_factory):
    table_prop = {
        "format-version": "2"
    }

    cols = [f"_c{idx}" for idx, _ in enumerate(iceberg_nested_write_gens_list)]
    gen_list = list(zip(cols, iceberg_nested_write_gens_list))
    df_gen = lambda spark: gen_df(spark, gen_list)

    _assert_gpu_equals_cpu_rtas(spark_tmp_table_factory, df_gen, table_prop)


@iceberg
@ignore_order(local=True)
@pytest.mark.skipif(is_iceberg_remote_catalog(), reason="Skip for remote catalog to reduce test time")
@allow_non_gpu_conditional(is_spark_400_or_later(), "EmptyRelationExec")
def test_rtas_unpartitioned_table_all_cols(spark_tmp_table_factory):
    """Test RTAS on unpartitioned table with all Iceberg write types on GPU."""
    table_prop = {
        "format-version": "2"
    }

    cols = [f"_c{idx}" for idx, _ in enumerate(iceberg_full_gens_list)]
    gen_list = list(zip(cols, iceberg_full_gens_list))
    df_gen = lambda spark: gen_df(spark, gen_list)

    _assert_gpu_equals_cpu_rtas(spark_tmp_table_factory, df_gen, table_prop)


@iceberg
@ignore_order(local=True)
@pytest.mark.skipif(is_iceberg_remote_catalog(), reason="Skip for remote catalog to reduce test time")
def test_rtas_partitioned_table_nested_types(spark_tmp_table_factory):
    table_prop = {
        "format-version": "2"
    }

    cols = [f"_c{idx}" for idx, _ in enumerate(iceberg_nested_write_gens_list)]
    gen_list = list(zip(cols, iceberg_nested_write_gens_list))
    df_gen = lambda spark: gen_df(spark, gen_list)

    _assert_gpu_equals_cpu_rtas(spark_tmp_table_factory,
                                df_gen,
                                table_prop,
                                partition_col_sql="bucket(16, _c0)")


@iceberg
@ignore_order(local=True)
@pytest.mark.skipif(is_iceberg_remote_catalog(), reason="Skip for remote catalog to reduce test time")
@allow_non_gpu_conditional(is_spark_400_or_later(), "EmptyRelationExec")
def test_rtas_partitioned_table_all_cols(spark_tmp_table_factory):
    """Test RTAS on partitioned table with all Iceberg write types on GPU."""
    table_prop = {
        "format-version": "2"
    }

    cols = [f"_c{idx}" for idx, _ in enumerate(iceberg_full_gens_list)]
    gen_list = list(zip(cols, iceberg_full_gens_list))
    df_gen = lambda spark: gen_df(spark, gen_list)

    _assert_gpu_equals_cpu_rtas(spark_tmp_table_factory,
                                df_gen,
                                table_prop,
                                partition_col_sql="bucket(16, _c2)")


@iceberg
@ignore_order(local=True)
@allow_non_gpu('AppendDataExec')
@pytest.mark.skipif(is_iceberg_remote_catalog(), reason="Skip for remote catalog to reduce test time")
@pytest.mark.parametrize("partition_table", [True, False], ids=lambda x: f"partition_table={x}")
@allow_non_gpu_conditional(is_spark_400_or_later(), "EmptyRelationExec")
def test_rtas_from_values(spark_tmp_table_factory,
                          partition_table):
    table_prop = {
        "format-version": "2"
    }

    base_name = get_full_table_name(spark_tmp_table_factory)
    gpu_table = f"{base_name}_gpu"
    cpu_table = f"{base_name}_cpu"

    def execute_rtas_from_values(spark, target_table: str):
        # Create initial table
        initial_df_gen = lambda sp: gen_df(sp, [("id", iceberg_gens_list[0]), ("name", iceberg_gens_list[1])])
        partition_col_sql = "bucket(8, id)" if partition_table else None
        create_iceberg_table(target_table, partition_col_sql, table_prop, initial_df_gen)
        
        # Execute RTAS
        partition_clause = "" if not partition_table else "PARTITIONED BY (bucket(8, id)) "
        props_sql = _props_to_sql(table_prop)
        
        spark.sql(
            f"REPLACE TABLE {target_table} USING ICEBERG {partition_clause}"
            f"TBLPROPERTIES ({props_sql}) AS "
            f"SELECT * FROM (VALUES (1, 'a'), (2, 'b'), (3, 'c')) AS t(id, name)")

    with_gpu_session(lambda spark: execute_rtas_from_values(spark, gpu_table),
                     conf=iceberg_write_enabled_conf)
    with_cpu_session(lambda spark: execute_rtas_from_values(spark, cpu_table),
                     conf=iceberg_write_enabled_conf)

    cpu_data = with_cpu_session(lambda spark: spark.table(cpu_table).collect())
    gpu_data = with_cpu_session(lambda spark: spark.table(gpu_table).collect())
    assert_equal_with_local_sort(cpu_data, gpu_data)


@iceberg
@ignore_order(local=True)
@pytest.mark.parametrize("partition_col_sql", [
    pytest.param(None, id="unpartitioned"),
    pytest.param("year(_c9)", id="year_partition"),
])
def test_rtas_aqe(spark_tmp_table_factory, partition_col_sql):
    """
    Test REPLACE TABLE AS SELECT with AQE enabled.
    """
    table_prop = {
        "format-version": "2",
    }

    df_gen = lambda spark: gen_df(spark, list(zip(iceberg_base_table_cols, iceberg_gens_list)))

    # Configuration with AQE enabled
    conf = copy_and_update(iceberg_write_enabled_conf, {
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true"
    })

    _assert_gpu_equals_cpu_rtas(spark_tmp_table_factory,
                                df_gen,
                                table_prop,
                                partition_col_sql=partition_col_sql,
                                create_or_replace=False,
                                conf=conf)


@iceberg
@datagen_overrides(seed=0, reason='https://github.com/NVIDIA/spark-rapids-jni/issues/4016')
@ignore_order(local=True)
@pytest.mark.skipif(is_iceberg_remote_catalog(), reason="Skip for remote catalog to reduce test time")
def test_rtas_partitioned_table_fanout_enabled(spark_tmp_table_factory):
    # Use bucket(2, ...) to keep partition count low and avoid OOM from Iceberg's FanoutDataWriter.
    _do_test_rtas_partitioned_table(
        spark_tmp_table_factory,
        "bucket(2, _c9)",
        table_prop={"format-version": "2", "write.spark.fanout.enabled": "true"})
