# Copyright (c) 2022-2026, NVIDIA CORPORATION.
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

from asserts import assert_cpu_and_gpu_are_equal_collect_with_capture, assert_gpu_and_cpu_are_equal_collect
from data_gen import *
from delta_lake_utils import deletion_vector_values_with_xfail_reasons
from marks import allow_non_gpu, ignore_order, delta_lake
from spark_session import is_databricks_runtime, with_cpu_session, with_gpu_session, is_databricks104_or_later, is_databricks113_or_later, supports_delta_lake_deletion_vectors
from dpp_test import _exchange_reuse_conf

# Almost all of this is the metadata query
# the important part is to not have InterleaveBits or HilbertLongIndex and PartitionerExpr
# but there is no good way to check for that so I filed https://github.com/NVIDIA/spark-rapids/issues/6875
# Until then we allow anything to be on the CPU.
@allow_non_gpu(any=True)
@delta_lake
@ignore_order(local=True)
def test_delta_zorder(spark_tmp_table_factory):
    table = spark_tmp_table_factory.get()

    def optimize_table(spark):
        # We need to drop the table and rerun each time because in some
        # versions delta will keep track if it has already been optimized or not
        # and will not re-run if it has been optimized
        df = two_col_df(spark, long_gen, string_gen, length=4096)
        spark.sql("DROP TABLE IF EXISTS {}".format(table)).show()
        spark.sql("CREATE TABLE {} (a BIGINT, b STRING) USING DELTA".format(table)).show()
        df.write.insertInto(table)

        # The optimize returns stats and metadata about the operation, which is different
        # from one run to another, so we cannot just compare them...
        spark.sql("OPTIMIZE {} ZORDER BY a, b".format(table)).show()
        return spark.sql("select * from {} where a = 1".format(table))

    assert_gpu_and_cpu_are_equal_collect(optimize_table,
            conf={"spark.rapids.sql.castFloatToIntegralTypes.enabled": True,
                  "spark.rapids.sql.castFloatToString.enabled": True,
                  "spark.rapids.sql.explain": "ALL"})

_statements = [
    # join on z-ordered column
    '''
    SELECT fact.ex_key, sum(fact.value)
    FROM {0} fact
    JOIN {1} dim
    ON fact.ex_key = dim.ex_key
    WHERE dim.filter = {2}
    GROUP BY fact.ex_key
    ''',
    # join on 2 z-ordered columns
    '''
    SELECT fact.ex_key, fact.ex_skey, sum(fact.value)
    FROM {0} fact
    JOIN {1} dim
    ON fact.ex_key = dim.ex_key AND fact.ex_skey = dim.ex_skey
    WHERE dim.filter = {2}
    GROUP BY fact.ex_key, fact.ex_skey
    ''',
    # join on 1 partitioned and 1 z-ordered column
    '''
    SELECT fact.key, fact.ex_key, sum(fact.value)
    FROM {0} fact
    JOIN {1} dim
    ON fact.key = dim.key AND fact.ex_key = dim.ex_key
    WHERE dim.filter = {2}
    GROUP BY fact.key, fact.ex_key
    ''',
    # join on 2 partitioned and 1 z-ordered columns
    '''
    SELECT fact.key, fact.skey, fact.ex_key, sum(fact.value)
    FROM {0} fact
    JOIN {1} dim
    ON fact.key = dim.key AND fact.skey = dim.skey AND fact.ex_key = dim.ex_key
    WHERE dim.filter = {2}
    GROUP BY fact.key, fact.skey, fact.ex_key
    ''',
    # reused subquery, join on z-ordered column
    '''
    SELECT ex_key, max(value)
    FROM (
        SELECT fact.ex_key as ex_key, fact.value as value
        FROM {0} fact
        JOIN {1} dim
        ON fact.ex_key = dim.ex_key
        WHERE dim.filter = {2}
    UNION ALL
        SELECT fact.ex_key as ex_key, fact.value as value
        FROM {0} fact
        JOIN {1} dim
        ON fact.ex_key = dim.ex_key
        WHERE dim.filter = {2}
    )
    GROUP BY ex_key
    '''
]

# This tests Dynamic File Pruning, a feature in Databricks that is similar to Dynamic Partition Pruning 
# except that it adds the DynamicPruningExpression for columns that are not partition columns but are still
# optimized. In this case the DynamicPruningExpression should be added to the DataFilters in the scan.
# This test is very similar to `test_dpp_reuse_broadcast_exchange` but it tests joining using a Z-ordered
# column
@delta_lake
@allow_non_gpu('CollectLimitExec')
@ignore_order(local=True)
@pytest.mark.skipif(not is_databricks104_or_later(), reason="Dynamic File Pruning is only supported in Databricks 10.4+")
@pytest.mark.parametrize('s_index', list(range(len(_statements))), ids=idfn)
@pytest.mark.parametrize('aqe_enabled', ['false', 'true'])
@pytest.mark.parametrize("enable_deletion_vectors", deletion_vector_values_with_xfail_reasons(
                            enabled_xfail_reason='https://github.com/NVIDIA/spark-rapids/issues/12042'), ids=idfn)
def test_delta_dfp_reuse_broadcast_exchange(spark_tmp_table_factory, s_index, aqe_enabled, enable_deletion_vectors):
    fact_table, dim_table = spark_tmp_table_factory.get(), spark_tmp_table_factory.get()

    def build_and_optimize_tables(spark):
        # Note that ex_key is a high-cardinality column, which makes it a good candidate for 
        # for Z-ordering, which then means it can then be used in Dynamic File Pruning in joins
        df = gen_df(spark, [
            ('key', IntegerGen(nullable=False, min_val=0, max_val=9, special_cases=[])),
            ('skey', IntegerGen(nullable=False, min_val=0, max_val=4, special_cases=[])),
            ('ex_key', IntegerGen(nullable=False, min_val=0, max_val=10000, special_cases=[])),
            ('ex_skey', IntegerGen(nullable=False, min_val=0, max_val=1000, special_cases=[])),
            ('value', int_gen),
        ], 10000)

        writer = df.write.format("delta").mode("overwrite")
        if supports_delta_lake_deletion_vectors():
            writer.option("delta.enableDeletionVectors", str(enable_deletion_vectors).lower())
        writer.partitionBy("key", "skey") \
            .saveAsTable(fact_table)
        spark.sql("OPTIMIZE {} ZORDER BY (ex_key, ex_skey)".format(fact_table)).show()

        df = gen_df(spark, [
            ('key', IntegerGen(nullable=False, min_val=0, max_val=9, special_cases=[])),
            ('skey', IntegerGen(nullable=False, min_val=0, max_val=4, special_cases=[])),
            ('ex_key', IntegerGen(nullable=False, min_val=0, max_val=10000, special_cases=[])),
            ('ex_skey', IntegerGen(nullable=False, min_val=0, max_val=1000, special_cases=[])),
            ('value', int_gen),
            ('filter', RepeatSeqGen(
                IntegerGen(nullable=False, min_val=0, max_val=2000, special_cases=[]),
                length=2000 // 20))
        ], 2000)
        writer = df.write.format("delta") \
            .mode("overwrite")
        if supports_delta_lake_deletion_vectors():
            writer.option("delta.enableDeletionVectors", str(enable_deletion_vectors).lower())
        writer.saveAsTable(dim_table)
        return df.select('filter').first()[0]

    filter_val = with_cpu_session(build_and_optimize_tables)

    statement = _statements[s_index].format(fact_table, dim_table, filter_val)

    if is_databricks113_or_later() and aqe_enabled == 'true':
        # SubqueryBroadcastExec is unoptimized in Databricks 11.3 with EXECUTOR_BROADCAST
        # See https://github.com/NVIDIA/spark-rapids/issues/7425
        exist_classes='DynamicPruningExpression,SubqueryBroadcastExec,ReusedExchangeExec'
    else:
        exist_classes='DynamicPruningExpression,GpuSubqueryBroadcastExec,ReusedExchangeExec'
    assert_cpu_and_gpu_are_equal_collect_with_capture(
        lambda spark: spark.sql(statement),
        # The existence of GpuSubqueryBroadcastExec indicates the reuse works on the GPU
        exist_classes,
        # Ensure Dynamic File Pruning kicks in by setting thresholds to 0
        conf=dict(_exchange_reuse_conf + [
            ('spark.databricks.optimizer.dynamicFilePruning', 'true'),
            ('spark.databricks.optimizer.deltaTableSizeThreshold', '0'),
            ('spark.databricks.optimizer.deltaTableFilesThreshold', '0'),
            ('spark.sql.adaptive.enabled', aqe_enabled)]))
