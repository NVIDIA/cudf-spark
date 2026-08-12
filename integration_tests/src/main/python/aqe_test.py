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
from pyspark.sql.functions import when, col, current_date, current_timestamp
from pyspark.sql.types import *
from asserts import assert_gpu_and_cpu_are_equal_collect, assert_cpu_and_gpu_are_equal_collect_with_capture
from conftest import is_databricks_runtime, is_not_utc
from data_gen import *
from spark_session import is_spark_400_or_later
from marks import ignore_order, allow_non_gpu
from spark_session import with_cpu_session, is_databricks113_or_later, is_databricks_version, is_databricks_version_or_later

# allow non gpu when time zone is non-UTC because of https://github.com/NVIDIA/spark-rapids/issues/9653'
not_utc_aqe_allow=['ShuffleExchangeExec', 'HashAggregateExec'] if is_not_utc() else []

_adaptive_conf = { "spark.sql.adaptive.enabled": "true" }

def create_skew_df(spark, length):
    root = spark.range(0, length)
    mid = length / 2
    left = root.select(
        when(col('id') < mid / 2, mid).
            otherwise(col('id')).alias("key1"),
        col('id').alias("value1")
    )
    right = root.select(
        when(col('id') < mid, mid).
            otherwise(col('id')).alias("key2"),
        col('id').alias("value2")
    )
    return left, right


# This replicates the skew join test from scala tests, and is here to test
# the computeStats(...) implementation in GpuRangeExec
@ignore_order(local=True)
def test_aqe_skew_join():
    def do_join(spark):
        left, right = create_skew_df(spark, 500)
        left.createOrReplaceTempView("skewData1")
        right.createOrReplaceTempView("skewData2")
        return spark.sql("SELECT * FROM skewData1 join skewData2 ON key1 = key2")

    assert_gpu_and_cpu_are_equal_collect(do_join, conf=_adaptive_conf)

# Test the computeStats(...) implementation in GpuDataSourceScanExec
@ignore_order(local=True)
@pytest.mark.parametrize("data_gen", integral_gens, ids=idfn)
def test_aqe_join_parquet(spark_tmp_path, data_gen):
    data_path = spark_tmp_path + '/PARQUET_DATA'
    with_cpu_session(
        lambda spark: unary_op_df(spark, data_gen).orderBy('a').write.parquet(data_path)
    )

    def do_it(spark):
        spark.read.parquet(data_path).createOrReplaceTempView('df1')
        spark.read.parquet(data_path).createOrReplaceTempView('df2')
        return spark.sql("select count(*) from df1,df2 where df1.a = df2.a")

    assert_gpu_and_cpu_are_equal_collect(do_it, conf=_adaptive_conf)


# Test the computeStats(...) implementation in GpuBatchScanExec
@ignore_order(local=True)
@pytest.mark.parametrize("data_gen", integral_gens, ids=idfn)
def test_aqe_join_parquet_batch(spark_tmp_path, data_gen):
    # force v2 source for parquet to use BatchScanExec
    conf = copy_and_update(_adaptive_conf, {
        "spark.sql.sources.useV1SourceList": ""
    })

    first_data_path = spark_tmp_path + '/PARQUET_DATA/key=0'
    with_cpu_session(
            lambda spark : unary_op_df(spark, data_gen).write.parquet(first_data_path))
    second_data_path = spark_tmp_path + '/PARQUET_DATA/key=1'
    with_cpu_session(
            lambda spark : unary_op_df(spark, data_gen).write.parquet(second_data_path))
    data_path = spark_tmp_path + '/PARQUET_DATA'

    def do_it(spark):
        spark.read.parquet(data_path).createOrReplaceTempView('df1')
        spark.read.parquet(data_path).createOrReplaceTempView('df2')
        return spark.sql("select count(*) from df1,df2 where df1.a = df2.a")

    assert_gpu_and_cpu_are_equal_collect(do_it, conf=conf)

# Test the map stage submission handling for GpuShuffleExchangeExec
@ignore_order(local=True)
def test_aqe_struct_self_join(spark_tmp_table_factory):
    def do_join(spark):
        data = [
            (("Adam ", "", "Green"), "1", "M", 1000),
            (("Bob ", "Middle", "Green"), "2", "M", 2000),
            (("Cathy ", "", "Green"), "3", "F", 3000)
        ]
        schema = (StructType()
                  .add("name", StructType()
                       .add("firstname", StringType())
                       .add("middlename", StringType())
                       .add("lastname", StringType()))
                  .add("id", StringType())
                  .add("gender", StringType())
                  .add("salary", IntegerType()))
        df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
        df_name = spark_tmp_table_factory.get()
        df.createOrReplaceTempView(df_name)
        resultdf = spark.sql(
            "select struct(name, struct(name.firstname, name.lastname) as newname)" +
            " as col,name from " + df_name + " union" +
            " select struct(name, struct(name.firstname, name.lastname) as newname) as col,name" +
            " from " + df_name)
        resultdf_name = spark_tmp_table_factory.get()
        resultdf.createOrReplaceTempView(resultdf_name)
        return spark.sql("select a.* from {} a, {} b where a.name=b.name".format(
            resultdf_name, resultdf_name))
    assert_gpu_and_cpu_are_equal_collect(do_join, conf=_adaptive_conf)


@allow_non_gpu("ProjectExec")
def test_aqe_broadcast_join_non_columnar_child(spark_tmp_path):
    data_path = spark_tmp_path + '/PARQUET_DATA'
    def prep(spark):
        data = [
            (("Adam ", "", "Green"), "1", "M", 1000, "http://widgets.net"),
            (("Bob ", "Middle", "Green"), "2", "M", 2000, "http://widgets.org"),
            (("Cathy ", "", "Green"), "3", "F", 3000, "http://widgets.net")
        ]
        schema = (StructType()
                  .add("name", StructType()
                       .add("firstname", StringType())
                       .add("middlename", StringType())
                       .add("lastname", StringType()))
                  .add("id", StringType())
                  .add("gender", StringType())
                  .add("salary", IntegerType())
                  .add("website", StringType()))

        df = spark.createDataFrame(spark.sparkContext.parallelize(data),schema)
        df2 = df.withColumn("dt",current_date().alias("dt")).withColumn("ts",current_timestamp().alias("ts"))

        df2.write.format("parquet").mode("overwrite").save(data_path)

    with_cpu_session(prep)

    def do_it(spark):
        newdf2 = spark.read.parquet(data_path)
        newdf2.createOrReplaceTempView("df2")

        return spark.sql(
            """
            select 
                a.name.firstname,
                a.name.lastname,
                b.full_url
            from df2 a join (select id, concat(website,'/path') as full_url from df2) b
                on a.id = b.id
            """
        )

    # Disable the CPU Bridge because the test wants to know what happens when ProjectExec falls back to CPU
    # Not what happens when the bridge falls back an expression.
    conf = copy_and_update(_adaptive_conf, { 'spark.rapids.sql.expression.Concat': 'false',
        'spark.rapids.sql.expression.cpuBridge.enabled': 'false' })

    if is_databricks113_or_later():
        assert_cpu_and_gpu_are_equal_collect_with_capture(do_it, exist_classes="GpuShuffleExchangeExec",conf=conf)
    else:
        assert_cpu_and_gpu_are_equal_collect_with_capture(do_it, exist_classes="GpuBroadcastExchangeExec",conf=conf)


joins = [
    'inner',
    'cross',
    'left semi',
    'left anti',
    'anti'
]


# Databricks-11.3 added new operator EXECUTOR_BROADCAST which does executor side broadcast.
# SparkPlan is different as there is no BroadcastExchange which is replaced by Exchange.
# To handle issue in https://github.com/NVIDIA/spark-rapids/issues/7037, we need to allow a 
# for a CPU ShuffleExchangeExec for a CPU Broadcast join to consume it
db_113_cpu_bnlj_join_allow=["ShuffleExchangeExec"] if is_databricks113_or_later() else []

# see https://github.com/NVIDIA/spark-rapids/issues/7037
# basically this happens when a GPU broadcast exchange is reused from 
# one side of a GPU broadcast join to be used on one side of a CPU 
# broadcast join. The bug currently manifests in Databricks, but could
# theoretically show up in other Spark distributions
@ignore_order(local=True)
@allow_non_gpu('BroadcastNestedLoopJoinExec', 'Cast', 'DateSub', *db_113_cpu_bnlj_join_allow, *not_utc_aqe_allow)
@pytest.mark.skipif(is_spark_400_or_later(), reason="https://github.com/NVIDIA/spark-rapids/issues/11100")
@pytest.mark.parametrize('join', joins, ids=idfn)
def test_aqe_join_reused_exchange_inequality_condition(spark_tmp_path, join):
    data_path = spark_tmp_path + '/PARQUET_DATA'
    def prep(spark):
        data = [
            (("Adam ", "", "Green"), "1", "M", 1000),
            (("Bob ", "Middle", "Green"), "2", "M", 2000),
            (("Cathy ", "", "Green"), "3", "F", 3000)
        ]
        schema = (StructType()
                  .add("name", StructType()
                       .add("firstname", StringType())
                       .add("middlename", StringType())
                       .add("lastname", StringType()))
                  .add("id", StringType())
                  .add("gender", StringType())
                  .add("salary", IntegerType()))

        df = spark.createDataFrame(spark.sparkContext.parallelize(data),schema)
        df2 = df.withColumn("dt",current_date().alias("dt")).withColumn("ts",current_timestamp().alias("ts"))

        df2.write.format("parquet").mode("overwrite").save(data_path)

    with_cpu_session(prep)

    def do_it(spark):
        newdf2 = spark.read.parquet(data_path)
        newdf2.createOrReplaceTempView("df2")

        return spark.sql(
            """
            select *
                from (
                    select distinct a.salary
                    from df2 a {join} join (select max(date(ts)) as state_start from df2) b
                    on date(a.ts) > b.state_start - 2)
                where salary in (
                    select salary from (select a.salary
                    from df2 a inner join (select max(date(ts)) as state_start from df2) b on date(a.ts) > b.state_start - 2
                    order by a.salary limit 1))
            """.format(join=join)
        )

    assert_gpu_and_cpu_are_equal_collect(do_it, conf=_adaptive_conf)


# this is specifically to reproduce the issue found in
# https://github.com/NVIDIA/spark-rapids/issues/10165 where it has an executor broadcast
# but the exchange going into the BroadcastHashJoin is an exchange with multiple partitions
# and goes into AQEShuffleRead that uses CoalescePartitions to go down to a single partition
@ignore_order(local=True)
@pytest.mark.skipif(not (is_databricks_runtime()), \
    reason="Executor side broadcast only supported on Databricks")
@allow_non_gpu('BroadcastHashJoinExec', 'ColumnarToRowExec', *db_113_cpu_bnlj_join_allow)
def test_aqe_join_executor_broadcast_not_single_partition(spark_tmp_path):
    data_path = spark_tmp_path + '/PARQUET_DATA'
    bhj_disable_conf = copy_and_update(_adaptive_conf,
        { "spark.rapids.sql.exec.BroadcastHashJoinExec": "false"}) 

    def prep(spark):
        data = [
            (("Adam ", "", "Green"), "1", "M", 1000),
            (("Bob ", "Middle", "Green"), "2", "M", 2000),
            (("Cathy ", "", "Green"), "3", "F", 3000)
        ]
        schema = (StructType()
                  .add("name", StructType()
                       .add("firstname", StringType())
                       .add("middlename", StringType())
                       .add("lastname", StringType()))
                  .add("id", StringType())
                  .add("gender", StringType())
                  .add("salary", IntegerType()))
        df = spark.createDataFrame(spark.sparkContext.parallelize(data),schema)
        df.write.format("parquet").mode("overwrite").save(data_path)
        data_school= [
            ("1", "school1"),
            ("2", "school1"),
            ("3", "school2")
        ]
        schema_school = (StructType()
                  .add("id", StringType())
                  .add("school", StringType()))
        df_school = spark.createDataFrame(spark.sparkContext.parallelize(data_school),schema_school)
        df_school.createOrReplaceTempView("df_school")

    with_cpu_session(prep)

    def do_it(spark):
        newdf = spark.read.parquet(data_path)
        newdf.createOrReplaceTempView("df")
        return spark.sql(
            """
                select /*+ BROADCAST(df_school) */ * from df a left outer join df_school b on a.id == b.id
            """
        )

    assert_gpu_and_cpu_are_equal_collect(do_it, conf=bhj_disable_conf)


# See https://github.com/NVIDIA/spark-rapids/issues/10645. Sometimes the exchange can provide multiple
# batches, so we to coalesce them into a single batch for the broadcast hash join.
@ignore_order(local=True)
@pytest.mark.skipif(not (is_databricks_runtime()), \
    reason="Executor side broadcast only supported on Databricks")
def test_aqe_join_executor_broadcast_enforce_single_batch():
    # Use a small batch to see if Databricks could send multiple batches
    conf = copy_and_update(_adaptive_conf, { "spark.rapids.sql.batchSizeBytes": "25" })
    def prep(spark):
        id_gen = RepeatSeqGen(IntegerGen(nullable=False), length=250)
        name_gen = RepeatSeqGen(["Adam", "Bob", "Cathy"], data_type=StringType())
        school_gen = RepeatSeqGen(["School1", "School2", "School3"], data_type=StringType())

        df = gen_df(spark, StructGen([('id', id_gen), ('name', name_gen)], nullable=False), length=1000)
        df.createOrReplaceTempView("df")

        df_school = gen_df(spark, StructGen([('id', id_gen), ('school', school_gen)], nullable=False), length=250)
        df.createOrReplaceTempView("df_school")

    with_cpu_session(prep)

    def do_it(spark):
        res = spark.sql(
            """
                select /*+ BROADCAST(df_school) */ * from df, df_school where df.id == df_school.id
            """
        )
        res.explain()
        return res
    # Ensure this is an EXECUTOR_BROADCAST
    assert_cpu_and_gpu_are_equal_collect_with_capture(
        do_it, 
        exist_classes="GpuShuffleExchangeExec,GpuBroadcastHashJoinExec",
        non_exist_classes="GpuBroadcastExchangeExec",
        conf=conf)


@ignore_order(local=True)
@pytest.mark.skipif(not is_databricks_runtime(),
    reason="Executor side broadcast only supported on Databricks")
def test_aqe_bhj_executor_broadcast_shuffle_coalesce_with_residual_condition(spark_tmp_path):
    from datetime import datetime
    from decimal import Decimal

    conf = copy_and_update(_adaptive_conf, {
        "spark.rapids.sql.exec.BroadcastHashJoinExec": "true",
        "spark.sql.shuffle.partitions": "32",
        "spark.sql.autoBroadcastJoinThreshold": str(10 * 1024 * 1024),
        "spark.sql.adaptive.autoBroadcastJoinThreshold": str(10 * 1024 * 1024),
    })
    left_path = spark_tmp_path + '/left'
    right_path = spark_tmp_path + '/right'

    # Keep each composite join key duplicated with different ids. Self-matches
    # are removed by the residual id predicate, while cross-pair matches remain.
    rows = [
        (1, True, 99.99, Decimal("150.50"), datetime(2024, 1, 15, 10, 30, 0)),
        (2, True, 99.99, Decimal("150.50"), datetime(2024, 1, 15, 10, 30, 0)),
        (3, False, 199.99, Decimal("250.75"), datetime(2024, 2, 20, 14, 45, 30)),
        (5, False, 199.99, Decimal("250.75"), datetime(2024, 2, 20, 14, 45, 30)),
        (8, True, 299.99, Decimal("350.25"), datetime(2024, 3, 10, 9, 15, 45)),
        (13, True, 299.99, Decimal("350.25"), datetime(2024, 3, 10, 9, 15, 45)),
    ]
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("is_active", BooleanType(), True),
        StructField("price", DoubleType(), True),
        StructField("amount", DecimalType(10, 2), True),
        StructField("created_at", TimestampType(), True),
    ])

    def prep(spark):
        df = spark.createDataFrame(rows, schema)
        df.write.mode("overwrite").parquet(left_path)
        df.write.mode("overwrite").parquet(right_path)

    with_cpu_session(prep)

    def do_it(spark):
        spark.read.parquet(left_path).createOrReplaceTempView("C_TABLE")
        spark.read.parquet(right_path).createOrReplaceTempView("G_TABLE")
        return spark.sql("""
            SELECT C_TABLE.*, G_TABLE.*
            FROM C_TABLE
            INNER JOIN G_TABLE
              ON C_TABLE.is_active  = G_TABLE.is_active
             AND C_TABLE.price      = G_TABLE.price
             AND C_TABLE.amount     = G_TABLE.amount
             AND C_TABLE.created_at = G_TABLE.created_at
            WHERE ABS(IFNULL(C_TABLE.id, 10) - IFNULL(G_TABLE.id, 10)) > 1e-09
        """)

    exist_classes = "GpuBroadcastHashJoinExec"
    if is_databricks_version(14, 3):
        exist_classes += ",GpuShuffleCoalesceExec"

    assert_cpu_and_gpu_are_equal_collect_with_capture(
        do_it,
        exist_classes=exist_classes,
        non_exist_classes="GpuBroadcastExchangeExec",
        conf=conf,
        require_non_empty=True)

# This test relies on the join not being on the GPU because a join on an array is not
# currently supported. This causes the join to fall back to the CPU, and with AQE the
# broadcast needs to also fall back to the CPU, but it does not always with AQE
# But some versions of Spark will plan this differently, which can result in a shuffle
# falling back to the CPU. This is perfectly acceptible so long as the query produces
# the correct result.
@allow_non_gpu(any = True)
def test_aqe_join_and_agg_single_value():
    test_query = """
    WITH 
    processed AS (
      SELECT 
        SIZE(params) AS result,
        params as input_params
      FROM (SELECT collect_list(1) AS params FROM VALUES (1) AS t(dummy)) input_table
      GROUP BY params
    )
    SELECT processed.result
    FROM (SELECT collect_list(1) AS params FROM VALUES (1) AS t(dummy)) collected
    LEFT OUTER JOIN processed ON collected.params = processed.input_params"""
    assert_gpu_and_cpu_are_equal_collect(lambda spark: spark.sql(test_query), conf=_adaptive_conf)

# this should be fixed by https://github.com/NVIDIA/spark-rapids/issues/11120
aqe_join_with_dpp_fallback=["FilterExec", "InSubqueryExec"] if is_databricks_runtime() else []
if is_databricks_version_or_later(14, 3):
    aqe_join_with_dpp_fallback.append("CollectLimitExec")

# Verify that DPP and AQE can coexist in even some odd cases involving multiple tables
@ignore_order(local=True)
@allow_non_gpu(*aqe_join_with_dpp_fallback)
def test_aqe_join_with_dpp(spark_tmp_path):
    data_path = spark_tmp_path + '/PARQUET_DATA'
    def write_data(spark):
        spark.range(34).selectExpr("concat('test_', id % 9) as test_id",
            "concat('site_', id % 2) as site_id").repartition(200).write.parquet(data_path + "/tests")
        spark.range(1000).selectExpr(
                "CAST(if (id % 2 == 0, '1990-01-01', '1990-01-02') as DATE) as day",
                "concat('test_', (id % 9) + 100) as test_id", 
                "concat('site_', id % 40) as site_id",
                "rand(1) as extra_info").write.partitionBy("day", "site_id", "test_id").parquet(data_path + "/infoA")
        spark.range(1000).selectExpr(
                "CAST(if (id % 2 == 0, '1990-01-01', '1990-01-02') as DATE) as day",
                "concat('exp_', id % 9) as test_spec",
                "concat('LONG_SITE_NAME_', id % 40) as site_id",
                "rand(0) as extra_info").write.partitionBy("day", "site_id").parquet(data_path + "/infoB")

    with_cpu_session(write_data)

    def run_test(spark):
        spark.read.parquet(data_path + "/tests/").createOrReplaceTempView("tests")
        spark.read.parquet(data_path + "/infoA/").createOrReplaceTempView("infoA")
        spark.read.parquet(data_path + "/infoB/").createOrReplaceTempView("infoB")
        return spark.sql("""
        with tmp as 
        (SELECT
          site_id,
          day,
          test_id
         FROM
          infoA
        UNION ALL
         SELECT
           CASE
             WHEN site_id = 'LONG_SITE_NAME_0' then 'site_0'
             WHEN site_id = 'LONG_SITE_NAME_1' then 'site_1'
             ELSE site_id
           END AS site_id,
           day,
           test_spec AS test_id
           FROM infoB
        )
        SELECT *
        FROM tmp a
        JOIN tests b ON a.test_id = b.test_id AND a.site_id = b.site_id
        WHERE day = '1990-01-01'
        AND a.site_id IN ('site_0', 'site_1')
        """)

    assert_gpu_and_cpu_are_equal_collect(run_test, conf=_adaptive_conf)

# Verify that DPP and AQE can coexist in even some odd cases involving 2 tables with multiple columns
@ignore_order(local=True)
@allow_non_gpu(*aqe_join_with_dpp_fallback)
def test_aqe_join_with_dpp_multi_columns(spark_tmp_path):
    conf = copy_and_update(_adaptive_conf, {
        "spark.rapids.sql.explain": "ALL",
        "spark.rapids.sql.debug.logTransformations": "true"})

    data_path = spark_tmp_path + '/PARQUET_DATA'
    def write_data(spark):
        spark.range(100).selectExpr(
            "concat('t_name_', id % 9) as t_name",
            "concat('t_id_', id % 9) as t_id",
            "concat('v_id_', id % 3) as v_id",
            "concat('site_', id % 2) as site_id"
            ).repartition(200).write.parquet(data_path + "/tests")
        spark.range(2000).selectExpr(
            "concat('t_id_', id % 9) as t_spec",
            "concat('v_id_', id % 3) as v_spec",
            "CAST(id as STRING) as extra_1",
            "concat('LONG_SITE_NAME_', id % 2) as site_id",
            "if (id % 2 == 0, '1990-01-01', '1990-01-02') as day"
            ).write.partitionBy("site_id", "day").parquet(data_path + "/infoB")
        spark.range(2000).selectExpr(
            "CAST(id as STRING) as extra_1",
            "concat('v_id_', id % 3) as v_id",
            "CAST(id + 10 as STRING) as extra_3",
            "if (id % 3 == 0, 'site_0', 'site_3') as site_id",
            "if (id % 2 == 0, '1990-01-01', '1990-01-02') as day",
            "concat('t_id_', id % 9) as t_id"
            ).write.partitionBy("site_id", "day", "t_id").parquet(data_path + "/infoA")

    with_cpu_session(write_data)

    def run_test(spark):
        spark.read.parquet(data_path + "/tests/").createOrReplaceTempView("tests")
        spark.read.parquet(data_path + "/infoA/").createOrReplaceTempView("infoA")
        spark.read.parquet(data_path + "/infoB/").createOrReplaceTempView("infoB")
        return spark.sql("""
        WITH tmp AS 
        ( SELECT
            extra_1,
            v_id,
            site_id,
            day,
            t_id
          FROM infoA WHERE
            day >= '1980-01-01' AND
            extra_3 != 50
        UNION ALL
          SELECT
            extra_1,
            v_spec as v_id,
            CASE
              WHEN site_id = 'LONG_SITE_NAME_0' then 'site_0'
              WHEN site_id = 'LONG_SITE_NAME_1' then 'site_1'
              ELSE site_id
            END AS site_id,
            day,
            t_spec as t_id
            FROM infoB
        )
        SELECT
        a.t_id,
        b.t_name,
        a.extra_1,
        day
        FROM tmp a
        JOIN tests b ON a.t_id = b.t_id AND a.v_id = b.v_id AND a.site_id = b.site_id
        and day = '1990-01-01'
        and a.site_id IN ('site_0', 'site_1');
        """)

    assert_gpu_and_cpu_are_equal_collect(run_test, conf=conf)
