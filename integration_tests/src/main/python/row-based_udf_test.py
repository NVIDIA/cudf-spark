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

from asserts import assert_gpu_and_cpu_are_equal_sql
from conftest import skip_unless_precommit_tests
from data_gen import *
from spark_session import is_spark_400_or_later, with_spark_session

def drop_udf(spark, udfname):
    spark.sql("DROP TEMPORARY FUNCTION IF EXISTS {}".format(udfname))

def skip_if_no_hive(spark):
    if spark.conf.get("spark.sql.catalogImplementation") != "hive":
        skip_unless_precommit_tests('The Spark session does not have Hive support')

def load_hive_udf(spark, udfname, udfclass):
    drop_udf(spark, udfname)
    # if UDF failed to load, throws AnalysisException, check if the udf class is in the class path
    spark.sql("CREATE TEMPORARY FUNCTION {} AS '{}'".format(udfname, udfclass))

@pytest.mark.xfail(condition=is_spark_400_or_later(),
                   reason='https://github.com/NVIDIA/cudf-spark/issues/15268')
def test_hive_empty_simple_udf():

    with_spark_session(skip_if_no_hive)
    data_gens = [["i", int_gen], ["s", string_gen]]
    def evalfn(spark):
        load_hive_udf(spark, "emptysimple", "com.nvidia.spark.rapids.tests.udf.hive.EmptyHiveSimpleUDF")
        return gen_df(spark, data_gens)
    assert_gpu_and_cpu_are_equal_sql(
        evalfn,
        "hive_simple_udf_test_table",
        "SELECT i, emptysimple(s, 'const_string') FROM hive_simple_udf_test_table",
        conf={'spark.rapids.sql.rowBasedUDF.enabled': 'true'})

def test_hive_empty_generic_udf():
    with_spark_session(skip_if_no_hive)
    def evalfn(spark):
        load_hive_udf(spark, "emptygeneric", "com.nvidia.spark.rapids.tests.udf.hive.EmptyHiveGenericUDF")
        return gen_df(spark, [["s", string_gen]])
    assert_gpu_and_cpu_are_equal_sql(
        evalfn,
        "hive_generic_udf_test_table",
        "SELECT emptygeneric(s) FROM hive_generic_udf_test_table",
        conf={'spark.rapids.sql.rowBasedUDF.enabled': 'true'})
