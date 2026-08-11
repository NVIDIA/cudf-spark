# Copyright (c) 2020-2025, NVIDIA CORPORATION.
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


from pyspark.sql.types import *
from pyspark import SparkConf, SparkContext, SQLContext
import pyspark.sql.functions as f
from conftest import is_not_utc
import datetime
from argparse import ArgumentParser
from decimal import Decimal
from asserts import assert_gpu_and_cpu_are_equal_collect
from qa_nightly_sql import *
import pytest
from spark_session import with_cpu_session, is_jvm_charset_utf8
from marks import approximate_float, ignore_order, incompat, qarun, allow_non_gpu, disable_ansi_mode
from data_gen import copy_and_update, non_utc_allow

def num_stringDf(spark):
    print("### CREATE DATAFRAME 1  ####")
    schema = StructType([StructField("strF", StringType()),
                         StructField("byteF", ByteType()),
                         StructField("shortF", ShortType()),
                         StructField("intF", IntegerType()),
                         StructField("longF", LongType()),
                         StructField("floatF", FloatType()),
                         StructField("doubleF", DoubleType()),
                         StructField("decimalF", DoubleType()),
                         StructField("booleanF", BooleanType()),
                         StructField("timestampF", TimestampType()),
                         StructField("dateF", DateType())])
    dt = datetime.date(1990, 1, 1)
    print(dt)
    tm = datetime.datetime(2020,2,1,12,1,1)

    data = [("FIRST", None, 500, 1200, 10, 10.001, 10.0003, 1.01, True, tm, dt),
            ("sold out", 20, 600, None, 20, 20.12, 2.000013, 2.01, True, tm, dt),
            ("take out", 20, 600, None, 20, 20.12, 2.000013, 2.01, True, tm, dt),
            ("Yuan", 20, 600, 2200, None, 20.12, 2.000013, 2.01, False, tm, dt),
            ("Alex", 30, 700, 3200, 30, None, 3.000013, 2.01, True, None, dt),
            ("Satish", 30, 700, 3200, 30, 30.12, None, 3.01, False, tm, dt),
            ("Gary", 40, 800, 4200, 40, 40.12, 4.000013, None, False, tm, dt),
            ("NVIDIA", 40, 800, 4200, -40, 40.12, 4.00013, 4.01, None, tm, dt),
            ("Mellanox", 40, 800, 4200, -20, -20.12, 4.00013, 4.01, False,None, dt),
            (None, 30, 500, -3200, -20, 2.012, 4.000013, -4.01, False, tm, None),
            ("NVIDIASPARKTEAM", 0, 500, -3200, -20, 2.012, 4.000013, -4.01, False, tm, dt),
            ("NVIDIASPARKTEAM", 20, 0, -3200, -20, 2.012, 4.000013, -4.01, False, tm, dt),
            ("NVIDIASPARKTEAM", 0, 50, 0, -20, 2.012, 4.000013, -4.01, False, tm, dt),
            (None, 0, 500, -3200, 0, 0.0, 0.0, -4.01, False, tm, dt),
            ("phuoc", 30, 500, 3200, -20, 20.12, 4.000013, 4.01, False, tm, dt)]
    df = spark.createDataFrame(data,schema=schema)
    df.createOrReplaceTempView("test_table")


# create dataframe for join & union operation testing
def num_stringDf_two(spark):
    print("### CREATE DATAFRAME TWO  ####")
    schema = StructType([StructField("strF", StringType()),
                         StructField("byteF", ByteType()),
                         StructField("shortF", ShortType()),
                         StructField("intF", IntegerType()),
                         StructField("longF", LongType()),
                         StructField("floatF", FloatType()),
                         StructField("doubleF", DoubleType()),
                         StructField("decimalF", DoubleType()),
                         StructField("booleanF", BooleanType()),
                         StructField("timestampF", TimestampType()),
                         StructField("dateF", DateType())])

    dt = datetime.date(2000, 1, 1)
    print(dt)
    tm = datetime.datetime(2022,12,1,12,1,1)
    data = [("AL", 10, 500, 1200, 10, 10.001, 10.0003, 1.01, True, tm, dt),
            ("Jhon", 20, 600, 2200, 20, 20.12, 2.000013, 2.01, True, tm, dt),
            ("Alex", 30, 700, 3200, 30, 30.12, 3.000013, 3.01, True, tm, dt),
            ("Satish", 30, 700, 3200, 30, 30.12, 3.000013, 3.01, False, tm, dt),
            ("Kary", 40, 800, 4200, 40, 40.12, 4.000013, 4.01, False, tm, dt),
            (None, 40, 800, 4200, -40, 40.12, 4.00013, 4.01, False, tm, dt),
            (None, 40, 800, 4200, -20, -20.12, 4.00013, 4.01, False, tm, dt),
            (None, 30, 500, -3200, -20, 2.012, 4.000013, -4.01, False, tm, dt),
            ("phuoc", 30, 500, 3200, -20, 20.12, 4.000013, 4.01, False, tm, dt)]

    df = spark.createDataFrame(data, schema=schema)
    df.createOrReplaceTempView("test_table1")

def num_stringDf_first_last(spark, field_name):
    print("### CREATE DATAFRAME 1  ####")
    schema = StructType([StructField("strF", StringType()),
                         StructField("byteF", ByteType()),
                         StructField("shortF", ShortType()),
                         StructField("intF", IntegerType()),
                         StructField("longF", LongType()),
                         StructField("floatF", FloatType()),
                         StructField("doubleF", DoubleType()),
                         StructField("decimalF", DoubleType()),
                         StructField("booleanF", BooleanType()),
                         StructField("timestampF", TimestampType()),
                         StructField("dateF", DateType())])
    dt = datetime.date(1990, 1, 1)
    print(dt)
    tm = datetime.datetime(2020,2,1,12,1,1)

    data = [("FIRST", None, 500, 1200, 10, 10.001, 10.0003, 1.01, True, tm, dt),
            ("sold out", 20, 600, None, 20, 20.12, 2.000013, 2.01, True, tm, dt),
            ("take out", 20, 600, None, 20, 20.12, 2.000013, 2.01, True, tm, dt),
            ("Yuan", 20, 600, 2200, None, 20.12, 2.000013, 2.01, False, tm, dt),
            ("Alex", 30, 700, 3200, 30, None, 3.000013, 2.01, True, None, dt),
            ("Satish", 30, 700, 3200, 30, 30.12, None, 3.01, False, tm, dt),
            ("Gary", 40, 800, 4200, 40, 40.12, 4.000013, None, False, tm, dt),
            ("NVIDIA", 40, 800, 4200, -40, 40.12, 4.00013, 4.01, None, tm, dt),
            ("Mellanox", 40, 800, 4200, -20, -20.12, 4.00013, 4.01, False,None, dt),
            (None, 30, 500, -3200, -20, 2.012, 4.000013, -4.01, False, tm, None),
            ("NVIDIASPARKTEAM", 0, 500, -3200, -20, 2.012, 4.000013, -4.01, False, tm, dt),
            ("NVIDIASPARKTEAM", 20, 0, -3200, -20, 2.012, 4.000013, -4.01, False, tm, dt),
            ("NVIDIASPARKTEAM", 0, 50, 0, -20, 2.012, 4.000013, -4.01, False, tm, dt),
            (None, 0, 500, -3200, 0, 0.0, 0.0, -4.01, False, tm, dt),
            ("phuoc", 30, 500, 3200, -20, 20.12, 4.000013, 4.01, False, tm, dt)]
    # First/Last have a lot of odd issues with getting these tests to pass
    # They are non-deterministic unless you have a single partition that is sorted
    # that is why we are coalesce to a single partition and sort within the partition
    # also for sort aggregations (done when variable width types like strings are in the output)
    # spark will re-sort the data based off of the grouping key.  Spark sort appears to
    # have no guarantee about being a stable sort.  In practice I have found that
    # sorting the data desc with nulls last matches with what spark is doing, but
    # there is no real guarantee that it will continue to work, so if the first/last
    # tests fail on strings this might be the cause of it.
    df = spark.createDataFrame(data,schema=schema)\
            .coalesce(1)\
            .sortWithinPartitions(f.col(field_name).desc_nulls_last())
    df.createOrReplaceTempView("test_table")

def idfn(val):
    return val[1]

_qa_conf = {
        'spark.rapids.sql.variableFloatAgg.enabled': 'true',
        'spark.rapids.sql.castStringToFloat.enabled': 'true',
        'spark.rapids.sql.castFloatToIntegralTypes.enabled': 'true',
        'spark.rapids.sql.castFloatToString.enabled': 'true',
        'spark.rapids.sql.regexp.enabled': 'true'
        }

_first_last_qa_conf = copy_and_update(_qa_conf, {
    # some of the first/last tests need a single partition to work reliably when run on a large cluster.
    'spark.sql.shuffle.partitions': '1'})

@disable_ansi_mode
@approximate_float
@incompat
@qarun
@pytest.mark.parametrize('sql_query_line', SELECT_SQL, ids=idfn)
@allow_non_gpu(*non_utc_allow)
def test_select(sql_query_line, pytestconfig):
    sql_query = sql_query_line[0]
    if sql_query:
        print(sql_query)
        with_cpu_session(num_stringDf)
        assert_gpu_and_cpu_are_equal_collect(lambda spark: spark.sql(sql_query), conf=_qa_conf)

@disable_ansi_mode
@ignore_order
@approximate_float
@incompat
@qarun
@pytest.mark.parametrize('sql_query_line', SELECT_NEEDS_SORT_SQL, ids=idfn)
@allow_non_gpu(*non_utc_allow)
def test_needs_sort_select(sql_query_line, pytestconfig):
    sql_query = sql_query_line[0]
    if sql_query:
        print(sql_query)
        with_cpu_session(num_stringDf)
        assert_gpu_and_cpu_are_equal_collect(lambda spark: spark.sql(sql_query), conf=_qa_conf)

@approximate_float
@incompat
@ignore_order(local=True)
@qarun
@pytest.mark.parametrize('sql_query_line', SELECT_JOIN_SQL, ids=idfn)
def test_select_join(sql_query_line, pytestconfig):
    sql_query = sql_query_line[0]
    if sql_query:
        print(sql_query)
        def init_tables(spark):
            num_stringDf(spark)
            if ("UNION" in sql_query) or ("JOIN" in sql_query):
                num_stringDf_two(spark)
        with_cpu_session(init_tables)
        assert_gpu_and_cpu_are_equal_collect(lambda spark: spark.sql(sql_query), conf=_qa_conf)

@approximate_float
@incompat
@ignore_order(local=True)
@qarun
@pytest.mark.parametrize('sql_query_line', SELECT_PRE_ORDER_SQL, ids=idfn)
@disable_ansi_mode
def test_select_first_last(sql_query_line, pytestconfig):
    sql_query = sql_query_line[0]
    if sql_query:
        print(sql_query)
        with_cpu_session(lambda spark: num_stringDf_first_last(spark, sql_query_line[2]))
        assert_gpu_and_cpu_are_equal_collect(lambda spark: spark.sql(sql_query), conf=_first_last_qa_conf)

@approximate_float(abs=1e-6)
@incompat
@ignore_order(local=True)
@qarun
@pytest.mark.parametrize('sql_query_line', SELECT_FLOAT_SQL, ids=idfn)
def test_select_float_order_local(sql_query_line, pytestconfig):
    sql_query = sql_query_line[0]
    if sql_query:
        print(sql_query)
        with_cpu_session(num_stringDf)
        assert_gpu_and_cpu_are_equal_collect(lambda spark: spark.sql(sql_query), conf=_qa_conf)


@approximate_float(abs=1e-6)
@incompat
@ignore_order(local=True)
@qarun
@pytest.mark.parametrize('sql_query_line', SELECT_REGEXP_SQL, ids=idfn)
@pytest.mark.skipif(not is_jvm_charset_utf8(), reason="Regular expressions require UTF-8")
def test_select_regexp(sql_query_line, pytestconfig):
    sql_query = sql_query_line[0]
    if sql_query:
        print(sql_query)
        with_cpu_session(num_stringDf)
        assert_gpu_and_cpu_are_equal_collect(lambda spark: spark.sql(sql_query), conf=_qa_conf)
