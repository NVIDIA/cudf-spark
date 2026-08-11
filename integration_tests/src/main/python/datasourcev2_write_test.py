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

from asserts import assert_gpu_fallback_collect, assert_equal_with_local_sort
from data_gen import gen_df, decimal_gens, non_utc_allow, StructGen, ArrayGen, string_gen
from marks import *
from spark_session import is_hive_available, is_spark_330_or_later, with_cpu_session, with_gpu_session
from hive_parquet_write_test import _hive_bucket_gens_sans_bools
from hive_parquet_write_test import read_single_bucket

_hive_write_conf = {
    "hive.enforce.bucketing": "true",
    "hive.exec.dynamic.partition": "true",
    "hive.exec.dynamic.partition.mode": "nonstrict"}


@pytest.mark.skipif(not (is_hive_available() and is_spark_330_or_later()),
                    reason="Must have Hive on Spark 3.3+")
@pytest.mark.parametrize('file_format', ['parquet', 'orc'])
@allow_non_gpu(*non_utc_allow)
def test_write_hive_bucketed_table(spark_tmp_table_factory, file_format):
    num_rows = 2048
    # Use every type except boolean, see https://github.com/NVIDIA/spark-rapids/issues/11762 and
    # https://github.com/rapidsai/cudf/issues/6763 .
    # Once the first issue is fixed, add back boolean_gen
    def gen_table(spark):
        gen_list = [('_c' + str(i), gen) for i, gen in enumerate(_hive_bucket_gens_sans_bools)]
        types_sql_str = ','.join('{} {}'.format(
            name, gen.data_type.simpleString()) for name, gen in gen_list)
        col_names_str = ','.join(name for name, gen in gen_list)
        data_table = spark_tmp_table_factory.get()
        gen_df(spark, gen_list, num_rows).createOrReplaceTempView(data_table)
        return data_table, types_sql_str, col_names_str

    (input_data, input_schema, input_cols_str) = with_cpu_session(gen_table)
    num_buckets = 4

    def write_hive_table(spark, out_table):
        spark.sql(
            "create table {0} ({1}) stored as {2} clustered by ({3}) into {4} buckets".format(
                out_table, input_schema, file_format, input_cols_str, num_buckets))
        spark.sql(
            "insert into {0} select * from {1}".format(out_table, input_data))

    cpu_table = spark_tmp_table_factory.get()
    gpu_table = spark_tmp_table_factory.get()
    with_cpu_session(lambda spark: write_hive_table(spark, cpu_table), _hive_write_conf)
    with_gpu_session(lambda spark: write_hive_table(spark, gpu_table), _hive_write_conf)
    cpu_rows, gpu_rows = 0, 0
    for cur_bucket_id in range(num_buckets):
        # Verify the result bucket by bucket
        ret_cpu = read_single_bucket(cpu_table, cur_bucket_id)
        cpu_rows += len(ret_cpu)
        ret_gpu = read_single_bucket(gpu_table, cur_bucket_id)
        gpu_rows += len(ret_gpu)
        assert_equal_with_local_sort(ret_cpu, ret_gpu)

    assert cpu_rows == num_rows
    assert gpu_rows == num_rows


@ignore_order
@allow_non_gpu('DataWritingCommandExec','ExecutedCommandExec','SortExec','WriteFilesExec','HiveHash')
@pytest.mark.skipif(not (is_hive_available() and is_spark_330_or_later()),
                    reason="Must have Hive on Spark 3.3+")
@pytest.mark.parametrize('file_format', ['parquet', 'orc'])
@pytest.mark.parametrize('gen', decimal_gens)
def test_write_hive_bucketed_unsupported_types_fallback(spark_tmp_table_factory, file_format, gen):
    out_table = spark_tmp_table_factory.get()

    def create_hive_table(spark):
        spark.sql("create table {0} (a {1}) stored as {2} clustered by (a) into 3 buckets".format(
            out_table, gen.data_type.simpleString(), file_format))
        data_table = spark_tmp_table_factory.get()
        gen_df(spark, [('a', gen)], length=10).createOrReplaceTempView(data_table)
        return data_table

    input_table = with_cpu_session(create_hive_table, _hive_write_conf)
    assert_gpu_fallback_collect(
        lambda spark: spark.sql(
            "insert into {0} select * from {1}".format(out_table, input_table)),
        'DataWritingCommandExec',
        _hive_write_conf)

# The calculation of nesting depth here does not take basic types into account.
nested_gen_depth_9 = [StructGen([('depth_8', StructGen([('depth_7', StructGen([('depth_6',
    StructGen([('depth_5', ArrayGen(StructGen([('depth_3', StructGen([('depth_2',
    ArrayGen(StructGen([('depth_0', string_gen)]), max_length=5))]))]), max_length=5))]))]))]))])]

@ignore_order
@allow_non_gpu('DataWritingCommandExec','ExecutedCommandExec','SortExec','WriteFilesExec','HiveHash')
@pytest.mark.skipif(not (is_hive_available() and is_spark_330_or_later()),
                    reason="Must have Hive on Spark 3.3+")
@pytest.mark.parametrize('file_format', ['parquet', 'orc'])
@pytest.mark.parametrize('gen', nested_gen_depth_9)
def test_write_hive_bucketed_nesting_depth_exceed_limit_fallback(spark_tmp_table_factory, file_format, gen):
    out_table = spark_tmp_table_factory.get()

    def create_hive_table(spark):
        spark.sql("create table {0} (a {1}) stored as {2} clustered by (a) into 3 buckets".format(
            out_table, gen.data_type.simpleString(), file_format))
        data_table = spark_tmp_table_factory.get()
        gen_df(spark, [('a', gen)], length=1).createOrReplaceTempView(data_table)
        return data_table

    input_table = with_cpu_session(create_hive_table, _hive_write_conf)
    assert_gpu_fallback_collect(
        lambda spark: spark.sql(
            "insert into {0} select * from {1}".format(out_table, input_table)),
        'DataWritingCommandExec',
        _hive_write_conf)
