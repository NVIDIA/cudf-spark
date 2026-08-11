# Copyright (c) 2021-2026, NVIDIA CORPORATION.
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

from pyspark.sql.types import IntegerType

from asserts import assert_cpu_and_gpu_are_equal_collect_with_capture, assert_gpu_and_cpu_are_equal_collect
from conftest import spark_tmp_table_factory
from data_gen import *
from marks import ignore_order, allow_non_gpu, datagen_overrides
from spark_session import is_before_spark_320, with_cpu_session, is_before_spark_312, is_databricks_runtime, is_databricks113_or_later, is_databricks_version_or_later

# non-positive values here can produce a degenerative join, so here we ensure that most values are
# positive to ensure the join will produce rows. See https://github.com/NVIDIA/spark-rapids/issues/10147
value_gen = RepeatSeqGen([None, INT_MIN, -1, 0, 1, INT_MAX], data_type=IntegerType())

def create_dim_table(table_name, table_format, length=500):
    def fn(spark):
        # Pick a random filter value, but make it constant for the whole 
        df = gen_df(spark, [
            ('key', IntegerGen(nullable=False, min_val=0, max_val=9, special_cases=[])),
            ('skey', IntegerGen(nullable=False, min_val=0, max_val=4, special_cases=[])),
            ('ex_key', IntegerGen(nullable=False, min_val=0, max_val=3, special_cases=[])),
            ('value', value_gen),
            # specify nullable=False for `filter` to avoid generating invalid SQL with
            # expression `filter = None` (https://github.com/NVIDIA/spark-rapids/issues/9817)
            ('filter', RepeatSeqGen(IntegerGen(nullable=False), length=1))
        ], length)
        df.cache()
        df.write.format(table_format) \
            .mode("overwrite") \
            .saveAsTable(table_name)
        return df.select('filter').first()[0], df.select('ex_key').first()[0]

    return with_cpu_session(fn)


def create_fact_table(table_name, table_format, length=2000):
    def fn(spark):
        df = gen_df(spark, [
            ('key', IntegerGen(nullable=False, min_val=0, max_val=9, special_cases=[])),
            ('skey', IntegerGen(nullable=False, min_val=0, max_val=4, special_cases=[])),
            # ex_key is not a partition column
            ('ex_key', IntegerGen(nullable=False, min_val=0, max_val=3, special_cases=[])),
            ('value', value_gen)], length)
        df.write.format(table_format) \
            .mode("overwrite") \
            .partitionBy('key', 'skey') \
            .saveAsTable(table_name)

    with_cpu_session(fn)


_dpp_conf = [('spark.sql.optimizer.dynamicPartitionPruning.enabled', 'true')]
_exchange_reuse_conf = _dpp_conf + [
    ('spark.sql.optimizer.dynamicPartitionPruning.reuseBroadcastOnly', 'true'),
    ('spark.sql.exchange.reuse', 'true')
]
_bypass_conf = _dpp_conf + [
    ('spark.sql.optimizer.dynamicPartitionPruning.reuseBroadcastOnly', 'true'),
    ('spark.sql.exchange.reuse', 'false')
]
_no_exchange_reuse_conf = _dpp_conf + [
    ('spark.sql.optimizer.dynamicPartitionPruning.reuseBroadcastOnly', 'false'),
    ('spark.sql.exchange.reuse', 'false')
]
_dpp_fallback_conf = _dpp_conf + [
    ('spark.sql.optimizer.dynamicPartitionPruning.reuseBroadcastOnly', 'false'),
    ('spark.sql.exchange.reuse', 'false'),
    ('spark.sql.optimizer.dynamicPartitionPruning.useStats', 'false'),
    ('spark.sql.optimizer.dynamicPartitionPruning.fallbackFilterRatio', '0'),
]

_statements = [
    '''
    SELECT fact.key, sum(fact.value)
    FROM {0} fact
    JOIN {1} dim
    ON fact.key = dim.key
    WHERE dim.filter = {2} AND fact.value > 0
    GROUP BY fact.key
    ''',
    '''
    SELECT f.key, sum(f.value)
    FROM (SELECT *, struct(key, skey) AS keys FROM {0} fact) f
    JOIN (SELECT *, struct(key, skey) AS keys FROM {1} dim) d
    ON f.keys = d.keys
    WHERE d.filter = {2}
    GROUP BY f.key
    ''',
    '''
    SELECT fact.key, fact.skey, sum(fact.value)
    FROM {0} fact
    JOIN {1} dim
    ON fact.key = dim.key AND fact.skey = dim.skey
    WHERE dim.filter = {2}
    GROUP BY fact.key, fact.skey
    ''',
    '''
    SELECT fact.key, fact.skey, fact.ex_key, sum(fact.value)
    FROM {0} fact
    JOIN {1} dim
    ON fact.key = dim.key AND fact.skey = dim.skey AND fact.ex_key = dim.ex_key
    WHERE dim.filter = {2}
    GROUP BY fact.key, fact.skey, fact.ex_key
    ''',
    # This query checks the pattern of reused broadcast subquery: ReusedSubquery(SubqueryBroadcast(...))
    # https://github.com/NVIDIA/spark-rapids/issues/4625
    """
    SELECT key, max(value)
    FROM (
        SELECT fact.key as key, fact.value as value
        FROM {0} fact
        JOIN {1} dim
        ON fact.key = dim.key
        WHERE dim.filter = {2}
    UNION ALL
        SELECT fact.key as key, fact.value as value
        FROM {0} fact
        JOIN {1} dim
        ON fact.key = dim.key
        WHERE dim.filter = {2}
    )
    GROUP BY key
    """,
    '''
    WITH fact_table AS (
        SELECT fact.key as key, sum(fact.value) as value
        FROM {0} fact
        WHERE fact.value > 0
        GROUP BY fact.key
        ORDER BY fact.key
    ),
    dim_table AS (
        SELECT dim.key as key, dim.value as value, dim.filter as filter
        FROM {1} dim
        WHERE ex_key = {3}
        ORDER BY dim.key
    )
    SELECT key, max(value)
    FROM (
        SELECT f.key as key, f.value as value
        FROM fact_table f
        JOIN dim_table d
        ON f.key = d.key
        WHERE d.filter = {2}
    UNION ALL
        SELECT f.key as key, f.value as value
        FROM fact_table f
        JOIN dim_table d
        ON f.key = d.key
        WHERE d.filter = {2}
    )
    GROUP BY key
    '''
]

# On some Databricks versions (>=14.3), some query plans include a `CollectLimitExec`,
# when filtering partitions.  This exec falls back to CPU.  These tests allow for `CollectLimit` to
# run on the CPU, if everything else in the plan execute as expected.
# Further details are furnished at https://github.com/NVIDIA/spark-rapids/issues/11764.
dpp_fallback_execs=["CollectLimitExec"] if is_databricks_version_or_later(14,3) else []

# When BroadcastExchangeExec is available on filtering side, and it can be reused:
# DynamicPruningExpression(InSubqueryExec(value, GpuSubqueryBroadcastExec)))
@ignore_order
@allow_non_gpu(*dpp_fallback_execs)
@datagen_overrides(seed=0, reason="https://github.com/NVIDIA/spark-rapids/issues/10147")
@pytest.mark.parametrize('store_format', ['parquet', 'orc'], ids=idfn)
@pytest.mark.parametrize('s_index', list(range(len(_statements))), ids=idfn)
@pytest.mark.parametrize('aqe_enabled', [
    'false',
    pytest.param('true', marks=pytest.mark.skipif(is_before_spark_320() and not is_databricks_runtime(),
                                                  reason='Only in Spark 3.2.0+ AQE and DPP can be both enabled'))
], ids=idfn)
def test_dpp_reuse_broadcast_exchange(spark_tmp_table_factory, store_format, s_index, aqe_enabled):
    fact_table, dim_table = spark_tmp_table_factory.get(), spark_tmp_table_factory.get()
    create_fact_table(fact_table, store_format, length=10000)
    filter_val, ex_key_val = create_dim_table(dim_table, store_format, length=2000)
    statement = _statements[s_index].format(fact_table, dim_table, filter_val, ex_key_val)
    
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
        conf=dict(_exchange_reuse_conf + [('spark.sql.adaptive.enabled', aqe_enabled)]))


# The SubqueryBroadcast can work on GPU even if the scan who holds it fallbacks into CPU.
@ignore_order
@pytest.mark.allow_non_gpu('FileSourceScanExec')
def test_dpp_reuse_broadcast_exchange_cpu_scan(spark_tmp_table_factory):
    fact_table, dim_table = spark_tmp_table_factory.get(), spark_tmp_table_factory.get()
    create_fact_table(fact_table, 'parquet', length=10000)
    filter_val, ex_key_val = create_dim_table(dim_table, 'parquet', length=2000)
    statement = _statements[0].format(fact_table, dim_table, filter_val, ex_key_val)
    assert_cpu_and_gpu_are_equal_collect_with_capture(
        lambda spark: spark.sql(statement),
        # The existence of GpuSubqueryBroadcastExec indicates the reuse works on the GPU
        exist_classes='FileSourceScanExec,GpuSubqueryBroadcastExec,ReusedExchangeExec',
        conf=dict(_exchange_reuse_conf + [
            ('spark.sql.adaptive.enabled', 'false'),
            ('spark.rapids.sql.format.parquet.read.enabled', 'false')]))


# When BroadcastExchange is not available and non-broadcast DPPs are forbidden, Spark will bypass it:
# DynamicPruningExpression(Literal.TrueLiteral)
@ignore_order
@pytest.mark.parametrize('store_format', ['parquet', 'orc'], ids=idfn)
@pytest.mark.parametrize('s_index', list(range(len(_statements))), ids=idfn)
@pytest.mark.parametrize('aqe_enabled', [
    'false',
    pytest.param('true', marks=pytest.mark.skipif(is_before_spark_320() and not is_databricks_runtime(),
                                                  reason='Only in Spark 3.2.0+ AQE and DPP can be both enabled'))
], ids=idfn)
def test_dpp_bypass(spark_tmp_table_factory, store_format, s_index, aqe_enabled):
    fact_table, dim_table = spark_tmp_table_factory.get(), spark_tmp_table_factory.get()
    create_fact_table(fact_table, store_format)
    filter_val, ex_key_val = create_dim_table(dim_table, store_format)
    statement = _statements[s_index].format(fact_table, dim_table, filter_val, ex_key_val)
    assert_cpu_and_gpu_are_equal_collect_with_capture(
        lambda spark: spark.sql(statement),
        # Bypass with a true literal, if we can not reuse broadcast exchange.
        exist_classes='DynamicPruningExpression',
        non_exist_classes='SubqueryExec,SubqueryBroadcastExec',
        conf=dict(_bypass_conf + [('spark.sql.adaptive.enabled', aqe_enabled)]))


# When BroadcastExchange is not available, but it is still worthwhile to run DPP,
# then Spark will plan an extra Aggregate to collect filtering values:
# DynamicPruningExpression(InSubqueryExec(value, SubqueryExec(Aggregate(...))))
@ignore_order
@allow_non_gpu(*dpp_fallback_execs)
@pytest.mark.parametrize('store_format', ['parquet', 'orc'], ids=idfn)
@pytest.mark.parametrize('s_index', list(range(len(_statements))), ids=idfn)
@pytest.mark.parametrize('aqe_enabled', [
    'false',
    pytest.param('true', marks=pytest.mark.skipif(is_before_spark_320() and not is_databricks_runtime(),
                                                  reason='Only in Spark 3.2.0+ AQE and DPP can be both enabled'))
], ids=idfn)
def test_dpp_via_aggregate_subquery(spark_tmp_table_factory, store_format, s_index, aqe_enabled):
    fact_table, dim_table = spark_tmp_table_factory.get(), spark_tmp_table_factory.get()
    create_fact_table(fact_table, store_format)
    filter_val, ex_key_val = create_dim_table(dim_table, store_format)
    statement = _statements[s_index].format(fact_table, dim_table, filter_val, ex_key_val)
    assert_cpu_and_gpu_are_equal_collect_with_capture(
        lambda spark: spark.sql(statement),
        # SubqueryExec appears if we plan extra subquery for DPP
        exist_classes='DynamicPruningExpression,SubqueryExec',
        conf=dict(_no_exchange_reuse_conf + [('spark.sql.adaptive.enabled', aqe_enabled)]))


# When BroadcastExchange is not available, Spark will skip DPP if there is no potential benefit
@ignore_order
@pytest.mark.parametrize('store_format', ['parquet', 'orc'], ids=idfn)
@pytest.mark.parametrize('s_index', list(range(len(_statements))), ids=idfn)
@pytest.mark.parametrize('aqe_enabled', [
    'false',
    pytest.param('true', marks=pytest.mark.skipif(is_before_spark_320() and not is_databricks_runtime(),
                                                  reason='Only in Spark 3.2.0+ AQE and DPP can be both enabled'))
], ids=idfn)
def test_dpp_skip(spark_tmp_table_factory, store_format, s_index, aqe_enabled):
    fact_table, dim_table = spark_tmp_table_factory.get(), spark_tmp_table_factory.get()
    create_fact_table(fact_table, store_format)
    filter_val, ex_key_val = create_dim_table(dim_table, store_format)
    statement = _statements[s_index].format(fact_table, dim_table, filter_val, ex_key_val)
    assert_cpu_and_gpu_are_equal_collect_with_capture(
        lambda spark: spark.sql(statement),
        # SubqueryExec appears if we plan extra subquery for DPP
        non_exist_classes='DynamicPruningExpression',
        conf=dict(_dpp_fallback_conf + [('spark.sql.adaptive.enabled', aqe_enabled)]))

dpp_like_any_fallback_execs=['FilterExec', 'CollectLimitExec', 'LikeAny'] if is_databricks_version_or_later(14,3) else ['FilterExec', 'LikeAny']

# GPU verification on https://issues.apache.org/jira/browse/SPARK-34436
@ignore_order
@allow_non_gpu(*dpp_like_any_fallback_execs)
@pytest.mark.parametrize('store_format', ['parquet', 'orc'], ids=idfn)
@pytest.mark.parametrize('aqe_enabled', [
    'false',
    pytest.param('true', marks=pytest.mark.skipif(is_before_spark_320() and not is_databricks_runtime(),
                                                  reason='Only in Spark 3.2.0+ AQE and DPP can be both enabled'))
], ids=idfn)
@pytest.mark.skipif(is_before_spark_312(), reason="DPP over LikeAny/LikeAll filter not enabled until Spark 3.1.2")
def test_dpp_like_any(spark_tmp_table_factory, store_format, aqe_enabled):
    fact_table, dim_table = spark_tmp_table_factory.get(), spark_tmp_table_factory.get()
    create_fact_table(fact_table, store_format)

    def create_dim_table_for_like(spark):
        df = gen_df(spark, [
            ('key', IntegerGen(nullable=False, min_val=0, max_val=9, special_cases=[])),
            ('filter', StringGen(pattern='[0-9]{2,10}')),
        ], 100)
        df.write.format(store_format).mode("overwrite").saveAsTable(dim_table)

    with_cpu_session(create_dim_table_for_like)

    statement = """
    SELECT f.key, f.skey, f.value
    FROM {0} f JOIN {1} s
    ON f.key = s.key
    WHERE s.filter LIKE ANY ('%00%', '%01%', '%10%', '%11%')
    """.format(fact_table, dim_table)

    if is_databricks113_or_later() and aqe_enabled == 'true':
        exist_classes='DynamicPruningExpression,SubqueryBroadcastExec,ReusedExchangeExec'
    else:
        exist_classes='DynamicPruningExpression,GpuSubqueryBroadcastExec,ReusedExchangeExec'
    assert_cpu_and_gpu_are_equal_collect_with_capture(
        lambda spark: spark.sql(statement),
        exist_classes,
        conf=dict(_exchange_reuse_conf + [('spark.sql.adaptive.enabled', aqe_enabled)]))


@allow_non_gpu(*dpp_fallback_execs)
# Test handling DPP expressions from a HashedRelation that rearranges columns
@pytest.mark.parametrize('aqe_enabled', [
    'false',
    pytest.param('true', marks=pytest.mark.skipif(is_before_spark_320() and not is_databricks_runtime(),
                                                  reason='Only in Spark 3.2.0+ AQE and DPP can be both enabled'))
], ids=idfn)
def test_dpp_from_swizzled_hash_keys(spark_tmp_table_factory, aqe_enabled):
    dim_table = spark_tmp_table_factory.get()
    fact_table = spark_tmp_table_factory.get()
    def setup_tables(spark):
        spark.sql("CREATE TABLE {}(id string) PARTITIONED BY (dt date, hr string, mins string) STORED AS PARQUET".format(dim_table))
        spark.sql("INSERT INTO {}(id,dt,hr,mins) values ('somevalue', date('2022-01-01'), '11', '59')".format(dim_table))
        spark.sql("CREATE TABLE {}(id string)".format(fact_table) +
                  " PARTITIONED BY (dt date, hr string, mins string) STORED AS PARQUET")
        spark.sql("INSERT INTO {}(id,dt,hr,mins)".format(fact_table) +
                  " SELECT 'somevalue', to_date('2022-01-01'), '11', '59'")
    with_cpu_session(setup_tables, conf={
        "hive.exec.dynamic.partition" : "true",
        "hive.exec.dynamic.partition.mode" : "nonstrict"
    })
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : spark.sql("SELECT COUNT(*) AS cnt FROM {} f".format(fact_table) +
                                 " LEFT JOIN (SELECT *, " +
                                 " date_format(concat(string(dt),' ',hr,':',mins,':','00'),'yyyy-MM-dd HH:mm:ss.SSS') AS ts" +
                                 " from {}) tmp".format(dim_table) +
                                 " ON f.hr = tmp.hr AND f.dt = tmp.dt WHERE tmp.ts < CURRENT_TIMESTAMP"),
        conf=dict(_dpp_conf + [('spark.sql.adaptive.enabled', aqe_enabled),
                               ("spark.rapids.sql.castStringToTimestamp.enabled", "true"),
                               ("spark.rapids.sql.hasExtendedYearValues", "false")]))


@allow_non_gpu("EmptyRelationExec")  # 'EmptyRelationExec' is introduced from Spark 400
# Test handling DPP subquery that could broadcast EmptyRelation rather than a GPU serialized batch
@pytest.mark.parametrize('aqe_enabled', [
    'false',
    pytest.param('true', marks=pytest.mark.skipif(is_before_spark_320() and not is_databricks_runtime(),
                                                  reason='Only in Spark 3.2.0+ AQE and DPP can be both enabled'))
], ids=idfn)
def test_dpp_empty_relation(spark_tmp_table_factory, aqe_enabled):
    dim_table = spark_tmp_table_factory.get()
    fact_table = spark_tmp_table_factory.get()
    def setup_tables(spark):
        spark.sql("CREATE TABLE {}(id string) PARTITIONED BY (dt date, hr string, mins string) STORED AS PARQUET".format(dim_table))
        spark.sql("INSERT INTO {}(id,dt,hr,mins) values ('somevalue', date('2022-01-01'), '11', '59')".format(dim_table))
        spark.sql("CREATE TABLE {}(id string)".format(fact_table) +
                  " PARTITIONED BY (dt date, hr string, mins string) STORED AS PARQUET")
        spark.sql("INSERT INTO {}(id,dt,hr,mins)".format(fact_table) +
                  " SELECT 'somevalue', to_date('2022-01-01'), '11', '59'")
    with_cpu_session(setup_tables, conf={
        "hive.exec.dynamic.partition" : "true",
        "hive.exec.dynamic.partition.mode" : "nonstrict"
    })
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : spark.sql("SELECT COUNT(*) AS cnt FROM {} f".format(fact_table) +
                                 " LEFT JOIN (SELECT * from {}) tmp".format(dim_table) +
                                 " ON f.hr = tmp.hr AND f.dt = tmp.dt WHERE tmp.mins > 60"),
        conf=dict(_dpp_conf + [('spark.sql.adaptive.enabled', aqe_enabled)]))
