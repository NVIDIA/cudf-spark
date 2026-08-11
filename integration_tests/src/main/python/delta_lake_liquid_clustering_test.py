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
from typing import Callable, Dict
from pyspark.sql.types import StringType, IntegerType

from asserts import assert_gpu_and_cpu_writes_are_equal_collect
from conftest import is_databricks_runtime
from data_gen import unary_op_df, int_gen, copy_and_update, SetValuesGen, string_gen, long_gen, \
    gen_df, append_unique_int_col_to_df
from delta_lake_delete_test import delta_delete_enabled_conf
from delta_lake_merge_test import delta_merge_enabled_conf
from delta_lake_update_test import delta_update_enabled_conf
from delta_lake_utils import delta_meta_allow, \
    delta_writes_enabled_conf, delta_write_fallback_allow, assert_gpu_and_cpu_delta_logs_equivalent
from marks import allow_non_gpu, delta_lake, ignore_order, disable_ansi_mode, \
    allow_non_gpu_conditional, allow_non_gpu_delta_write_if
from spark_session import is_spark_353_or_later, is_spark_356_or_later, \
    is_before_spark_353, with_cpu_session, is_spark_400_or_later, is_databricks173_or_later


def assert_liquid_clustering_delta_logs_equivalent(data_path):
    # DBR 17.3 liquid clustering can emit runtime-specific domainMetadata and clustering
    # operation parameters that differ between CPU clustered writes and GPU Delta writes.
    if not is_databricks173_or_later():
        with_cpu_session(lambda spark: assert_gpu_and_cpu_delta_logs_equivalent(spark, data_path))


def delta_path_identifier(path):
    return f"delta.`{path}`"


def optimize_liquid_clustered_table(spark, table_identifier):
    if is_databricks173_or_later():
        with_cpu_session(lambda cpu_spark: cpu_spark.sql(
            f"OPTIMIZE {table_identifier}").collect())
    else:
        spark.sql(f"OPTIMIZE {table_identifier}").collect()


@allow_non_gpu(*delta_meta_allow)
@allow_non_gpu_conditional(is_databricks173_or_later(),
                           f"{delta_write_fallback_allow},AtomicCreateTableAsSelectExec,"
                           "AppendDataExecV1")
@allow_non_gpu_delta_write_if(
    is_databricks173_or_later(),
    reason="DBR 17.3 plans Delta liquid CTAS through V2 AtomicCreateTableAsSelectExec")
@delta_lake
@ignore_order
@pytest.mark.skipif(not is_spark_353_or_later(),
                    reason="CTAS with cluster by is only supported on delta 3.3+")
def test_delta_ctas_sql_liquid_clustering(spark_tmp_path, spark_tmp_table_factory):
    view_name = spark_tmp_table_factory.get()
    def write_func(spark, path):
        # Create a temp view to select from for the CTAS operation
        unary_op_df(spark, int_gen).coalesce(1).createOrReplaceTempView(view_name)
        spark.sql(f"""
            CREATE TABLE delta.`{path}`
            USING DELTA
            CLUSTER BY (a)
            AS SELECT * FROM {view_name}
        """)
        optimize_liquid_clustered_table(spark, delta_path_identifier(path))

    data_path = spark_tmp_path + "/DELTA_LIQUID_CLUSTER"

    assert_gpu_and_cpu_writes_are_equal_collect(
        write_func,
        lambda spark, path: spark.read.format("delta").load(path),
        data_path,
        conf=delta_writes_enabled_conf)
    assert_liquid_clustering_delta_logs_equivalent(data_path)



def create_clustered_table_sql(spark, table_name, path, enable_deletion_vectors=None):
    table_properties = ""
    if enable_deletion_vectors is not None:
        table_properties = f"""
            TBLPROPERTIES ('delta.enableDeletionVectors' = '{str(enable_deletion_vectors).lower()}')"""
    spark.sql(f"""
            CREATE TABLE {table_name}
            (a long, 
             b string, 
             c string, 
             d int, 
             e long,
             unique_int int)
            USING DELTA
            LOCATION '{path}'
            CLUSTER BY (a, b, d)
            {table_properties}
        """)


def gen_df_and_replace_view(spark, view_name):
    gen_list = [("a", int_gen),
                ("b", SetValuesGen(StringType(), ["x"])),
                ("c", string_gen),
                ("d", SetValuesGen(IntegerType(), [1, 2, 3])),
                ("e", long_gen)]
    df = gen_df(spark, gen_list)
    df = append_unique_int_col_to_df(spark, df)
    df.coalesce(1).createOrReplaceTempView(view_name)


def setup_clustered_table_sql(spark, path, table_name, view_name,
                              enable_deletion_vectors=None):
    create_clustered_table_sql(spark, table_name, path, enable_deletion_vectors)
    gen_df_and_replace_view(spark, view_name)
    spark.sql(f"""
            INSERT INTO {table_name}
            SELECT * FROM {view_name}
        """)
    optimize_liquid_clustered_table(spark, table_name)


@allow_non_gpu(*delta_meta_allow, "CreateTableExec")
@delta_lake
@ignore_order
@pytest.mark.skipif(not is_spark_353_or_later(),
                    reason="RTAS with cluster by is only supported on delta 3.3+")
@pytest.mark.skipif(is_spark_356_or_later(),
                    reason="https://github.com/delta-io/delta/issues/4671")
def test_delta_rtas_sql_liquid_clustering(spark_tmp_path, spark_tmp_table_factory):
    def write_func(spark, path):
        table_name = spark_tmp_table_factory.get()
        view_name = spark_tmp_table_factory.get()

        create_clustered_table_sql(spark, table_name, path)
        gen_df_and_replace_view(spark, view_name)
        spark.sql(f"""
            REPLACE TABLE {table_name}
            USING DELTA
            LOCATION '{path}'
            CLUSTER BY (a)
            AS SELECT * FROM {view_name}
        """)
        optimize_liquid_clustered_table(spark, table_name)

    data_path = spark_tmp_path + "/DELTA_LIQUID_CLUSTER"

    assert_gpu_and_cpu_writes_are_equal_collect(
        write_func,
        lambda spark, path: spark.read.format("delta").load(path),
        data_path,
        conf=delta_writes_enabled_conf)
    assert_liquid_clustering_delta_logs_equivalent(data_path)



@allow_non_gpu(*delta_meta_allow, "CreateTableExec")
@delta_lake
@ignore_order
@pytest.mark.skipif(not is_spark_353_or_later(),
                    reason="Create table with cluster by is only supported on delta 3.1+")
def test_delta_append_sql_liquid_clustering(spark_tmp_path, spark_tmp_table_factory):
    def write_func(spark, path):
        table_name = spark_tmp_table_factory.get()
        view_name = spark_tmp_table_factory.get()
        create_clustered_table_sql(spark, table_name, path)
        gen_df_and_replace_view(spark, view_name)
        spark.sql(f"""
            INSERT INTO {table_name}
            SELECT * FROM {view_name}
        """)
        optimize_liquid_clustered_table(spark, table_name)

    data_path = spark_tmp_path + "/DELTA_LIQUID_CLUSTER"

    assert_gpu_and_cpu_writes_are_equal_collect(
        write_func,
        lambda spark, path: spark.read.format("delta").load(path),
        data_path,
        conf=delta_writes_enabled_conf
    )
    assert_liquid_clustering_delta_logs_equivalent(data_path)


@allow_non_gpu(*delta_meta_allow, "CreateTableExec")
@delta_lake
@ignore_order
@pytest.mark.skipif(not is_spark_353_or_later(),
                    reason="Create table with cluster by is only supported on delta 3.1+")
def test_delta_insert_overwrite_static_sql_liquid_clustering(spark_tmp_path,
                                                               spark_tmp_table_factory):
    def write_func(spark, path):
        table_name = spark_tmp_table_factory.get()
        view_name = spark_tmp_table_factory.get()
        create_clustered_table_sql(spark, table_name, path)
        gen_df_and_replace_view(spark, view_name)
        spark.sql(f"""
            INSERT OVERWRITE TABLE {table_name}
            SELECT * FROM {view_name}
        """)
        optimize_liquid_clustered_table(spark, table_name)

    data_path = spark_tmp_path + "/DELTA_LIQUID_CLUSTER"

    conf = copy_and_update(delta_writes_enabled_conf,
                           {"spark.sql.sources.partitionOverwriteMode": "STATIC"})

    assert_gpu_and_cpu_writes_are_equal_collect(
        write_func,
        lambda spark, path: spark.read.format("delta").load(path),
        data_path,
        conf=conf)
    assert_liquid_clustering_delta_logs_equivalent(data_path)


@allow_non_gpu(*delta_meta_allow, "CreateTableExec")
@delta_lake
@ignore_order
@pytest.mark.skipif(not is_spark_353_or_later(),
                    reason="Create table with cluster by is only supported on delta 3.1+")
@pytest.mark.skipif(is_databricks173_or_later(),
                    reason="DBR 17.3 disallows dynamic partition overwrite for liquid clustering")
def test_delta_insert_overwrite_dynamic_sql_liquid_clustering(spark_tmp_path,
                                                                      spark_tmp_table_factory):
    def write_func(spark, path):
        table_name = spark_tmp_table_factory.get()
        view_name = spark_tmp_table_factory.get()
        create_clustered_table_sql(spark, table_name, path)
        gen_df_and_replace_view(spark, view_name)
        spark.sql(f"""
            INSERT OVERWRITE TABLE {table_name}
            SELECT * FROM {view_name}
        """)
        optimize_liquid_clustered_table(spark, table_name)

    data_path = spark_tmp_path + "/DELTA_LIQUID_CLUSTER"

    conf = copy_and_update(delta_writes_enabled_conf,
                           {"spark.sql.sources.partitionOverwriteMode": "DYNAMIC"})

    assert_gpu_and_cpu_writes_are_equal_collect(
        write_func,
        lambda spark, path: spark.read.format("delta").load(path),
        data_path,
        conf=conf)
    assert_liquid_clustering_delta_logs_equivalent(data_path)


@allow_non_gpu(*delta_meta_allow, "CreateTableExec")
@allow_non_gpu_conditional(is_databricks173_or_later(),
                           f"{delta_write_fallback_allow},EmptyRelationExec")
@allow_non_gpu_delta_write_if(
    is_databricks173_or_later(),
    reason="DBR 17.3 liquid clustering replaceWhere may use CPU Delta write commands")
@delta_lake
@ignore_order
@pytest.mark.skipif(not is_spark_353_or_later(),
                    reason="Create table with cluster by is only supported on delta 3.1+")
def test_delta_insert_overwrite_replace_where_sql_liquid_clustering(spark_tmp_path,
                                                                             spark_tmp_table_factory):

    def write_func(spark, path):
        table_name = spark_tmp_table_factory.get()
        view_name = spark_tmp_table_factory.get()
        create_clustered_table_sql(spark, table_name, path)
        gen_df_and_replace_view(spark, view_name)
        spark.sql(f"""
            INSERT INTO TABLE {table_name}
            REPLACE WHERE b = 'x'
            SELECT * FROM {view_name}
        """)
        optimize_liquid_clustered_table(spark, table_name)

    data_path = spark_tmp_path + "/DELTA_LIQUID_CLUSTER"

    assert_gpu_and_cpu_writes_are_equal_collect(
        write_func,
        lambda spark, path: spark.read.format("delta").load(path),
        data_path,
        conf=delta_writes_enabled_conf)
    assert_liquid_clustering_delta_logs_equivalent(data_path)


def do_test_delta_dml_sql_liquid_clustering(spark_tmp_path,
                                                     spark_tmp_table_factory,
                                                     conf: Dict[str, str],
                                                     sql_func: Callable[[str], str]):

    base_data_path = spark_tmp_path + "/DELTA_LIQUID_CLUSTER"
    cpu_data_path = f"{base_data_path}/CPU"
    cpu_table_name = spark_tmp_table_factory.get()
    gpu_data_path = f"{base_data_path}/GPU"
    gpu_table_name = spark_tmp_table_factory.get()

    # DBR 17.3 liquid clustered tables otherwise default into persistent DV writes,
    # which are covered by the existing Delta DML DV fallback tests.
    enable_deletion_vectors = False if is_databricks173_or_later() else None
    with_cpu_session(lambda spark: setup_clustered_table_sql(spark, cpu_data_path,
                                                             cpu_table_name,
                                                             spark_tmp_table_factory.get(),
                                                             enable_deletion_vectors))
    with_cpu_session(lambda spark: setup_clustered_table_sql(spark, gpu_data_path,
                                                             gpu_table_name,
                                                             spark_tmp_table_factory.get(),
                                                             enable_deletion_vectors))

    def modify_table(spark, path):
        table_name = cpu_table_name if path == cpu_data_path else gpu_table_name
        spark.sql(sql_func(table_name))
        optimize_liquid_clustered_table(spark, table_name)

    assert_gpu_and_cpu_writes_are_equal_collect(
        modify_table,
        lambda spark, path: spark.read.format("delta").load(path),
        base_data_path,
        conf=conf)

@allow_non_gpu(*delta_meta_allow)
@allow_non_gpu_conditional(is_databricks173_or_later(),
                           "FileSourceScanExec,ColumnarToRowExec")
@delta_lake
@ignore_order
@pytest.mark.skipif(not is_spark_353_or_later(),
                    reason="Create table with cluster by is only supported on delta 3.1+")
def test_delta_delete_sql_liquid_clustering(spark_tmp_path, spark_tmp_table_factory):

    do_test_delta_dml_sql_liquid_clustering(
        spark_tmp_path, spark_tmp_table_factory, delta_delete_enabled_conf,
        lambda table_name: f"DELETE FROM {table_name} WHERE a > 0")

@allow_non_gpu(*delta_meta_allow, "CreateTableExec", "AppendDataExecV1")
@allow_non_gpu_conditional(is_databricks173_or_later(),
                           "FileSourceScanExec,ColumnarToRowExec")
@delta_lake
@ignore_order
@pytest.mark.skipif(not is_spark_353_or_later(),
                    reason="Create table with cluster by is only supported on delta 3.1+")
@disable_ansi_mode
def test_delta_update_sql_liquid_clustering(spark_tmp_path,
                                            spark_tmp_table_factory):

    do_test_delta_dml_sql_liquid_clustering(
        spark_tmp_path, spark_tmp_table_factory, delta_update_enabled_conf,
        lambda table_name: f"UPDATE {table_name} SET e = e+1 WHERE a > 0")


@allow_non_gpu(*delta_meta_allow)
@allow_non_gpu_conditional(is_spark_400_or_later(), "HashAggregateExec")
@allow_non_gpu_conditional(is_databricks173_or_later(),
                           "FileSourceScanExec,ColumnarToRowExec")
@delta_lake
@ignore_order
@pytest.mark.skipif(is_before_spark_353(),
                    reason="Spark-RAPIDS plugin supports liquid clustering for Delta IO 3.3+")
def test_delta_merge_sql_liquid_clustering(spark_tmp_path, spark_tmp_table_factory):

    base_source_path = spark_tmp_path + "/DELTA_MERGE_CLUSTERED_SOURCE"
    cpu_source_path = f"{base_source_path}/CPU"
    cpu_source_name = spark_tmp_table_factory.get()
    gpu_source_path = f"{base_source_path}/GPU"
    gpu_source_name = spark_tmp_table_factory.get()
    base_target_path = spark_tmp_path + "/DELTA_MERGE_CLUSTERED_TARGET"
    cpu_target_path = f"{base_target_path}/CPU"
    cpu_target_name = spark_tmp_table_factory.get()
    gpu_target_path = f"{base_target_path}/GPU"
    gpu_target_name = spark_tmp_table_factory.get()

    enable_deletion_vectors = False if is_databricks173_or_later() else None
    with_cpu_session(lambda spark: setup_clustered_table_sql(spark, cpu_source_path,
                                                             cpu_source_name,
                                                             spark_tmp_table_factory.get(),
                                                             enable_deletion_vectors))
    with_cpu_session(lambda spark: setup_clustered_table_sql(spark, gpu_source_path,
                                                             gpu_source_name,
                                                             spark_tmp_table_factory.get(),
                                                             enable_deletion_vectors))
    with_cpu_session(lambda spark: setup_clustered_table_sql(spark, cpu_target_path,
                                                             cpu_target_name,
                                                             spark_tmp_table_factory.get(),
                                                             enable_deletion_vectors))
    with_cpu_session(lambda spark: setup_clustered_table_sql(spark, gpu_target_path,
                                                             gpu_target_name,
                                                             spark_tmp_table_factory.get(),
                                                             enable_deletion_vectors))

    def merge_table(spark, path):
        type = "cpu" if path == cpu_target_path else "gpu"
        source_name = cpu_source_name if type == "cpu" else gpu_source_name
        target_name = cpu_target_name if type == "cpu" else gpu_target_name
        sql = f"""
            MERGE INTO {target_name} as target
            USING {source_name} as source
            ON target.a == source.unique_int
            WHEN MATCHED THEN
              UPDATE SET *
            WHEN NOT MATCHED THEN
              INSERT *
            """
        spark.sql(sql).collect()
        optimize_liquid_clustered_table(spark, target_name)

    read_table = lambda spark, path: spark.read.format("delta").load(path)
    assert_gpu_and_cpu_writes_are_equal_collect(
        merge_table,
        read_table,
        base_target_path,
        conf=delta_merge_enabled_conf)


def create_clustered_delta_table_df(table_name, table_path):
    from delta.tables import DeltaTable
    (DeltaTable.create()
     .tableName(table_name)
     .addColumn("a", dataType="INT")
     .addColumn("b", dataType="STRING")
     .addColumn("c", dataType="STRING")
     .addColumn("d", dataType="INT")
     .addColumn("e", dataType="LONG")
     .clusterBy("a", "b", "d")
     .location(table_path)
     .execute())

def write_to_delta_table_df(spark, path, mode, opts= None):
    if opts is None:
        opts = {}
    gen_list = [("a", int_gen),
                ("b", SetValuesGen(StringType(), ["x"])),
                ("c", string_gen),
                ("d", SetValuesGen(IntegerType(), [1, 2, 3])),
                ("e", long_gen)]
    df_writer = (gen_df(spark, gen_list)
                 .coalesce(1)
                 .write
                 .format("delta")
                 .mode(mode))

    for k, v in opts.items():
        df_writer = df_writer.option(k, v)

    df_writer.save(path)


@allow_non_gpu(*delta_meta_allow, "CreateTableExec")
@delta_lake
@ignore_order
@pytest.mark.skipif(not is_spark_353_or_later(),
                    reason="Create table with cluster by is only supported on delta 3.1+")
def test_delta_append_df_liquid_clustering(spark_tmp_path, spark_tmp_table_factory):
    def write_func(spark, path):
        table_name = spark_tmp_table_factory.get()
        create_clustered_delta_table_df(table_name, path)
        # Create a temp view to select from for the CTAS operation
        write_to_delta_table_df(spark, path, "append")
        optimize_liquid_clustered_table(spark, table_name)

    data_path = spark_tmp_path + "/DELTA_LIQUID_CLUSTER"

    assert_gpu_and_cpu_writes_are_equal_collect(
        write_func,
        lambda spark, path: spark.read.format("delta").load(path),
        data_path,
        conf=delta_writes_enabled_conf)
    assert_liquid_clustering_delta_logs_equivalent(data_path)


@allow_non_gpu(*delta_meta_allow, "CreateTableExec")
@delta_lake
@ignore_order
@pytest.mark.parametrize("overwrite_mode", ["STATIC", "DYNAMIC"],
                         ids = lambda val: f"overwrite_mode={val}")
@pytest.mark.skipif(not is_spark_353_or_later(),
                    reason="Create table with cluster by is only supported on delta 3.1+")
def test_delta_insert_overwrite_df_liquid_clustering(spark_tmp_path,
                                                              spark_tmp_table_factory,
                                                              overwrite_mode):
    if is_databricks173_or_later() and overwrite_mode == "DYNAMIC":
        pytest.skip("DBR 17.3 disallows dynamic partition overwrite for liquid clustering")

    def write_func(spark, path):
        table_name = spark_tmp_table_factory.get()
        create_clustered_delta_table_df(table_name, path)
        write_to_delta_table_df(spark, path, "overwrite",
                                opts = {'partitionOverwriteMode': overwrite_mode})
        optimize_liquid_clustered_table(spark, table_name)

    data_path = spark_tmp_path + "/DELTA_LIQUID_CLUSTER"

    assert_gpu_and_cpu_writes_are_equal_collect(
        write_func,
        lambda spark, path: spark.read.format("delta").load(path),
        data_path,
        conf=delta_writes_enabled_conf)
    assert_liquid_clustering_delta_logs_equivalent(data_path)


@allow_non_gpu(*delta_meta_allow, "CreateTableExec")
@allow_non_gpu_conditional(is_databricks173_or_later(),
                           f"{delta_write_fallback_allow},EmptyRelationExec")
@allow_non_gpu_delta_write_if(
    is_databricks173_or_later(),
    reason="DBR 17.3 liquid clustering replaceWhere may use CPU Delta write commands")
@delta_lake
@ignore_order
@pytest.mark.skipif(not is_spark_353_or_later(),
                    reason="Create table with cluster by is only supported on delta 3.1+")
def test_delta_insert_overwrite_replace_where_df_liquid_clustering(
        spark_tmp_path, spark_tmp_table_factory):

    def write_func(spark, path):
        table_name = spark_tmp_table_factory.get()
        create_clustered_delta_table_df(table_name, path)
        write_to_delta_table_df(spark, path, "overwrite",
                                opts = {'replaceWhere': " b='x' "})
        optimize_liquid_clustered_table(spark, table_name)

    data_path = spark_tmp_path + "/DELTA_LIQUID_CLUSTER"

    assert_gpu_and_cpu_writes_are_equal_collect(
        write_func,
        lambda spark, path: spark.read.format("delta").load(path),
        data_path,
        conf=delta_writes_enabled_conf)
    assert_liquid_clustering_delta_logs_equivalent(data_path)
