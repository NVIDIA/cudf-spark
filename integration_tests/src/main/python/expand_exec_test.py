# Copyright (c) 2021-2025, NVIDIA CORPORATION.
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

from asserts import assert_gpu_and_cpu_are_equal_collect, assert_gpu_and_cpu_are_equal_sql
from data_gen import *
from marks import disable_ansi_mode, ignore_order
from pyspark.sql import functions as f

@pytest.mark.parametrize('data_gen', all_gen, ids=idfn)
# Many Spark versions have issues sorting large decimals,
# see https://issues.apache.org/jira/browse/SPARK-40089.
@ignore_order(local=True)
def test_expand_exec(data_gen):
    def op_df(spark, length=2048):
        return gen_df(spark, StructGen([
            ('a', data_gen),
            ('b', IntegerGen())], nullable=False), length=length).rollup(f.col("a"), f.col("b")).agg(f.col("b"))

    assert_gpu_and_cpu_are_equal_collect(op_df)


# "cube" and "rollup" will not run into the pre-projection because of different
# planning by Spark. But it is still good to test them to make sure no regressions.
pre_pro_sqls = [
    "select count(distinct (a+b)), count(distinct if((a+b)>100, c, null)) from pre_pro group by a",
    "select count(b), count(c) from pre_pro group by cube((a+b), if((a+b)>100, c, null))",
    "select count(b), count(c) from pre_pro group by rollup((a+b), if((a+b)>100, c, null))"]

@ignore_order(local=True)
@pytest.mark.parametrize('sql', pre_pro_sqls, ids=["distinct_agg", "cube", "rollup"])
def test_expand_pre_project(sql):
    def get_df(spark):
        # Limit the range of the Integer to avoid overflow issues in ANSI mode
        return three_col_df(spark, short_gen, IntegerGen(min_val=-1000, max_val=1000, special_cases=[]), string_gen)

    assert_gpu_and_cpu_are_equal_sql(get_df, "pre_pro", sql)
