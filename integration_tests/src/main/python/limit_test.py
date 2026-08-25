# Copyright (c) 2020-2026, NVIDIA CORPORATION.
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

from asserts import assert_gpu_and_cpu_are_equal_collect
from data_gen import *
from spark_session import is_before_spark_340
from marks import allow_non_gpu, approximate_float

@pytest.mark.parametrize('data_gen', all_basic_gens + decimal_gens + array_gens_sample + map_gens_sample + struct_gens_sample, ids=idfn)
def test_simple_limit(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
        # We need some processing after the limit to avoid a CollectLimitExec
        lambda spark : unary_op_df(spark, data_gen, num_slices=1).limit(10).repartition(1),
        conf = {'spark.sql.execution.sortBeforeRepartition': 'false'})


def offset_test_wrapper(sql, batch_size):
    # Disable AQE temporarily until https://github.com/NVIDIA/spark-rapids/issues/14319 is resolved.
    conf = {'spark.rapids.sql.exec.CollectLimitExec': 'true',
            'spark.rapids.sql.batchSizeBytes': batch_size,
            'spark.sql.adaptive.enabled': 'false'}

    # Create dataframe to test CollectLimit
    def spark_df(spark):
        unary_op_df(spark, int_gen, length=2048, num_slices=1).createOrReplaceTempView("tmp_table")
        return spark.sql(sql)
    assert_gpu_and_cpu_are_equal_collect(spark_df, conf)

    # Create dataframe to test GlobalLimit
    def spark_df_repartition(spark):
        return spark_df(spark).repartition(1)
    assert_gpu_and_cpu_are_equal_collect(spark_df_repartition, conf)


@pytest.mark.parametrize('offset', [1024, 2048, 4096])
@pytest.mark.parametrize('batch_size', ['1000', '1g'])
@pytest.mark.skipif(is_before_spark_340(), reason='offset is introduced from Spark 3.4.0')
@allow_non_gpu('CollectLimitExec')
def test_non_zero_offset(offset, batch_size):
    # offset is used in the test cases having no limit, that is limit = -1
    # 1024: offset < df.numRows
    # 2048: offset = df.numRows
    # 4096: offset > df.numRows

    sql = "select * from tmp_table offset {}".format(offset)
    offset_test_wrapper(sql, batch_size)


@pytest.mark.parametrize('limit, offset', [(0, 0), (0, 10), (1024, 500), (2048, 456), (3000, 111), (500, 500), (100, 600)])
@pytest.mark.parametrize('batch_size', ['1000', '1g'])
@pytest.mark.skipif(is_before_spark_340(), reason='offset is introduced from Spark 3.4.0')
@allow_non_gpu('ShuffleExchangeExec', 'CollectLimitExec')
def test_non_zero_offset_with_limit(limit, offset, batch_size):
    # In CPU version of spark, (limit, offset) can not be negative number.
    # Test case description:
    # (0, 0): Corner case: both limit and offset are 0
    # (0, 10): Corner case: limit = 0, offset > 0
    # (1024, 500): offset < limit && limit < df.numRows
    # (2048, 456): offset < limit && limit = df.numRows
    # (3000, 111): offset < limit && limit > df.numRows
    # (500, 500): offset = limit
    # (100, 600): offset > limit

    sql = "select * from tmp_table limit {} offset {}".format(limit, offset)
    offset_test_wrapper(sql, batch_size)

@pytest.mark.parametrize('data_gen', all_gen, ids=idfn)
@pytest.mark.parametrize('limit, offset', [(0, 0), (0, 10), (1024, 500), (2048, 456), (3000, 111), (500, 500), (100, 600)])
@pytest.mark.parametrize('batch_size', ['1000', '1g'])
@pytest.mark.skipif(is_before_spark_340(), reason='offset is introduced from Spark 3.4.0')
@allow_non_gpu('ShuffleExchangeExec') # when limit = 0, ShuffleExchangeExec is not replaced.
@approximate_float
def test_order_by_offset_with_limit(limit, offset, data_gen, batch_size):
    # In CPU version of spark, (limit, offset) can not be negative number.
    # Test case description:
    # (0, 0): Corner case: both limit and offset are 0
    # (0, 10): Corner case: limit = 0, offset > 0
    # (1024, 500): offset < limit && limit < df.numRows
    # (2048, 456): offset < limit && limit = df.numRows
    # (3000, 111): offset < limit && limit > df.numRows
    # (500, 500): offset = limit
    # (100, 600): offset > limit

    def spark_df(spark):
        unary_op_df(spark, data_gen).createOrReplaceTempView("tmp_table")
        sql = "select * from tmp_table order by a limit {} offset {}".format(limit, offset)
        return spark.sql(sql)
    assert_gpu_and_cpu_are_equal_collect(spark_df, conf={'spark.rapids.sql.batchSizeBytes': batch_size})
