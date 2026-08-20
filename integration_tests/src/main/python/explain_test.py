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

from conftest import is_not_utc
from data_gen import *
from marks import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from spark_session import is_spark_420_or_later, with_cpu_session, with_gpu_session

# mark this test as ci_1 for mvn verify sanity check in pre-merge CI
pytestmark = pytest.mark.premerge_ci_1

def create_df(spark, data_gen, left_length, right_length):
    left = binary_op_df(spark, data_gen, length=left_length)
    right = binary_op_df(spark, data_gen, length=right_length).withColumnRenamed("a", "r_a")\
            .withColumnRenamed("b", "r_b")
    return left, right


@pytest.mark.parametrize('data_gen', [StringGen()], ids=idfn)
def test_explain_join(spark_tmp_path, data_gen):
    data_path1 = spark_tmp_path + '/PARQUET_DATA1'
    data_path2 = spark_tmp_path + '/PARQUET_DATA2'

    def do_join_explain(spark):
        left, right = create_df(spark, data_gen, 500, 500)
        left.write.parquet(data_path1)
        right.write.parquet(data_path2)
        df1 = spark.read.parquet(data_path1)
        df2 = spark.read.parquet(data_path2)
        df3 = df1.join(df2, df1.a == df2.r_a, "inner")
        explain_str = spark.sparkContext._jvm.com.nvidia.spark.rapids.ExplainPlan.explainPotentialGpuPlan(df3._jdf, "ALL")
        remove_isnotnull = explain_str.replace("isnotnull", "")
        # everything should be on GPU
        assert "not" not in remove_isnotnull

    with_cpu_session(do_join_explain)

def test_explain_udf():
    slen = udf(lambda s: len(s), IntegerType())

    @udf
    def to_upper(s):
        if s is not None:
            return s.upper()

    @udf(returnType=IntegerType())
    def add_one(x):
        if x is not None:
            return x + 1

    def do_explain(spark):
        df = spark.createDataFrame([(1, "John Doe", 21)], ("id", "name", "age"))
        df2 = df.select(slen("name").alias("slen(name)"), to_upper("name"), add_one("age"))
        explain_str = spark.sparkContext._jvm.com.nvidia.spark.rapids.ExplainPlan.explainPotentialGpuPlan(df2._jdf, "ALL")
        # udf shouldn't be fully on GPU. Spark 4.2 enables Arrow-optimized Python
        # UDFs here, so regular udf(...) is planned as ArrowEvalPythonExec instead
        # of the older BatchEvalPythonExec.
        udf_exec = 'ArrowEvalPythonExec' if is_spark_420_or_later() else 'BatchEvalPythonExec'
        udf_str_not = \
            'cannot run on GPU because GPU does not currently support the operator class ' + \
            f'org.apache.spark.sql.execution.python.{udf_exec}'
        if is_spark_420_or_later():
            assert f'Exec <{udf_exec}> will partially run on GPU' in explain_str
        else:
            assert udf_str_not in explain_str
        not_on_gpu_str = spark.sparkContext._jvm.com.nvidia.spark.rapids.ExplainPlan.explainPotentialGpuPlan(df2._jdf, "NOT")
        if not is_spark_420_or_later():
            assert udf_str_not in not_on_gpu_str
        assert "will run on GPU" not in not_on_gpu_str

    with_cpu_session(do_explain)


@allow_non_gpu(any = True)
def test_explain_bucketd_scan(spark_tmp_table_factory):
    """
    Test the physical plan includes the info of enabling bucketed scan.
    The code is copied from: 
    https://github.com/apache/spark/commit/79515e4b6c#diff-03f119698c3637b87c9ce2634c34c14bb0f7efc043ea37a0891c1ab9fbc3ebadR688
    """
    def do_explain(spark):
        tbl_1 = spark_tmp_table_factory.get()
        tbl_2 = spark_tmp_table_factory.get()
        spark.createDataFrame([(1, 2), (2, 3)], ("i", "j")).write.bucketBy(8, "i").saveAsTable(tbl_1)
        spark.createDataFrame([(2,), (3,)], ("i",)).write.bucketBy(8, "i").saveAsTable(tbl_2)
        df1 = spark.table(tbl_1)
        df2 = spark.table(tbl_2)
        joined_df = df1.join(df2, df1.i == df2.i , "inner")

        assert "Bucketed: true" in joined_df._sc._jvm.PythonSQLUtils.explainString(joined_df._jdf.queryExecution(), "simple")
    
    with_gpu_session(do_explain, {"spark.sql.autoBroadcastJoinThreshold": "0"})


@allow_non_gpu(any = True)
def test_explain_bucket_column_not_read(spark_tmp_table_factory):
    """
    Test the physical plan includes the info of disabling bucketed scan and the reason.
    The code is copied from:
    https://github.com/apache/spark/commit/79515e4b6c#diff-03f119698c3637b87c9ce2634c34c14bb0f7efc043ea37a0891c1ab9fbc3ebadR702
    """
    def do_explain(spark):
        tbl = spark_tmp_table_factory.get()
        spark.createDataFrame([(1, 2), (2, 3)], ("i", "j")).write.bucketBy(8, "i").saveAsTable(tbl)
        df = spark.table(tbl).select(f.col("j"))

        assert "Bucketed: false (bucket column(s) not read)" in df._sc._jvm.PythonSQLUtils.explainString(df._jdf.queryExecution(), "simple")

    with_gpu_session(do_explain)


@allow_non_gpu(any = True)
def test_explain_bucket_disabled_by_conf(spark_tmp_table_factory):
    """
    Test the physical plan includes the info of disabling bucketed scan and the reason.
    The code is copied from:
    https://github.com/apache/spark/commit/79515e4b6c#diff-03f119698c3637b87c9ce2634c34c14bb0f7efc043ea37a0891c1ab9fbc3ebadR694
    """
    def do_explain(spark):
        tbl_1 = spark_tmp_table_factory.get()
        tbl_2 = spark_tmp_table_factory.get()
        spark.createDataFrame([(1, 2), (2, 3)], ("i", "j")).write.bucketBy(8, "i").saveAsTable(tbl_1)
        spark.createDataFrame([(2,), (3,)], ("i",)).write.bucketBy(8, "i").saveAsTable(tbl_2)
        df1 = spark.table(tbl_1)
        df2 = spark.table(tbl_2)
        joined_df = df1.join(df2, df1.i == df2.i , "inner")

        assert "Bucketed: false (disabled by configuration)" in joined_df._sc._jvm.PythonSQLUtils.explainString(joined_df._jdf.queryExecution(), "simple")

    with_gpu_session(do_explain, {"spark.sql.sources.bucketing.enabled": "false"}) 


@allow_non_gpu(any=True)
def test_explain_bucket_disabled_by_query_planner(spark_tmp_table_factory):
    """
    Test the physical plan includes the info of disabling bucketed scan and the reason.
    The code is copied from:
    https://github.com/apache/spark/commit/79515e4b6c#diff-03f119698c3637b87c9ce2634c34c14bb0f7efc043ea37a0891c1ab9fbc3ebadR700

    This test will be skipped if spark version is before 3.1.0. Because the attribute `disableBucketedScan` is not included in `GpuFileSourceScanExec` before 3.1.0.
    """
    def do_explain(spark):
        tbl = spark_tmp_table_factory.get()
        spark.createDataFrame([(1, 2), (2, 3)], ("i", "j")).write.bucketBy(8, "i").saveAsTable(tbl)
        df = spark.table(tbl)

        assert "Bucketed: false (disabled by query planner)" in df._sc._jvm.PythonSQLUtils.explainString(df._jdf.queryExecution(), "simple")

    with_gpu_session(do_explain)
