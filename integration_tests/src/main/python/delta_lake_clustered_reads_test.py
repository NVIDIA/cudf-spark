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
from pyspark.sql.types import StringType, IntegerType

from asserts import assert_gpu_and_cpu_are_equal_collect, assert_gpu_fallback_collect
from data_gen import gen_df, int_gen, string_gen, long_gen, SetValuesGen
from delta_lake_utils import delta_meta_allow
from marks import allow_non_gpu, delta_lake, disable_ansi_mode, ignore_order
from spark_session import (
    is_databricks_runtime,
    is_before_spark_353,
    supports_delta_lake_deletion_vectors,
    with_cpu_session,
)


def setup_clustered_table(spark, path, table_name, enable_dv):
    """
    Set up a Delta Lake clustered table with/without deletion vectors (DV).
    Ideally, we should be able to just use `setup_delta_dest_table()` in delta_lake_utils,
    but the `save()` python API does not support clustered tables, which is used in
    `setup_delta_dest_table()`.
    """

    create_tbl_sql = f"""
                CREATE TABLE {table_name}
                (a int,
                 b string,
                 c string,
                 d int,
                 e long)
                USING DELTA
                LOCATION '{path}'
                TBLPROPERTIES ('delta.enableDeletionVectors' = '{str(enable_dv).lower()}')
                CLUSTER BY (a, d)
            """
    spark.sql(create_tbl_sql)

    gen_list = [
        ("a", int_gen),
        ("b", SetValuesGen(StringType(), ["x", "y", "z"])),
        ("c", string_gen),
        ("d", SetValuesGen(IntegerType(), [1, 2, 3])),
        ("e", long_gen),
    ]
    df = gen_df(spark, gen_list)
    df.write.format("delta").mode("append").saveAsTable(table_name)

    # Optimize the table to trigger clustering
    spark.sql(f"optimize {table_name}")

    desc = spark.sql(f"DESCRIBE DETAIL {table_name}").collect()
    # Ensure the table is clustered as expected
    assert desc[0].clusteringColumns == ["a", "d"]


@allow_non_gpu(*delta_meta_allow)
@delta_lake
@ignore_order
@disable_ansi_mode
@pytest.mark.skipif(
    not is_databricks_runtime() and is_before_spark_353(),
    reason="Clustered table DDL is only supported on Delta 3.3+/Spark 3.5.3+",
)
def test_delta_clustered_read_sql(spark_tmp_path, spark_tmp_table_factory):
    """
    Happy-path clustered table read using SQL (no DV): ensure GPU parity.
    """
    data_path = spark_tmp_path + "/DELTA_LIQUID_CLUSTER_READ_SQL"
    table_name = spark_tmp_table_factory.get()
    def setup_tables(spark):
        setup_clustered_table(spark, data_path, table_name, enable_dv=False)
    with_cpu_session(setup_tables)

    def read_query(spark):
        return spark.sql(
            f"""
                SELECT a, b, sum(e) cnt
                FROM {table_name}
                WHERE a = 1 AND d = 3
                GROUP BY a, b
            """
        )

    assert_gpu_and_cpu_are_equal_collect(read_query)


@allow_non_gpu(*delta_meta_allow)
@delta_lake
@ignore_order
@disable_ansi_mode
@pytest.mark.skipif(
    not is_databricks_runtime() and is_before_spark_353(),
    reason="Clustered table DDL is only supported on Delta 3.3+/Spark 3.5.3+",
)
def test_delta_clustered_read_df(spark_tmp_path, spark_tmp_table_factory):
    """
    Happy-path clustered table read using dataframe API (no DV): GPU parity.
    """
    data_path = spark_tmp_path + "/DELTA_LIQUID_CLUSTER_READ_DF"
    table_name = spark_tmp_table_factory.get()
    def setup_tables(spark):
        setup_clustered_table(spark, data_path, table_name, enable_dv=False)
    with_cpu_session(setup_tables)

    assert_gpu_and_cpu_are_equal_collect(
        # same query as in test_delta_clustered_read_sql
        lambda spark: spark.read.table(table_name) \
            .filter("a = 1 AND d = 3") \
            .groupBy("a", "b") \
            .sum("e") \
            .select("a", "b", "sum(e)")
    )
