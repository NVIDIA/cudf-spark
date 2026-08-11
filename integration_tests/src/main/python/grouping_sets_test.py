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

import pytest

from spark_session import is_before_spark_320
from asserts import assert_gpu_and_cpu_are_equal_sql
from data_gen import *
from pyspark.sql.types import *
from marks import *

_grouping_set_gen = [
    ('a', StringGen()),
    ('b', StringGen())]

_grouping_set_sqls = [
    'SELECT a, b, count(1) FROM testData GROUP BY a, GROUPING SETS(ROLLUP(a, b))',
    'SELECT a, b, count(1) FROM testData GROUP BY a, GROUPING SETS(GROUPING SETS((a, b), (a), ()))',
    'SELECT a, b, count(1) FROM testData '
        'GROUP BY a, GROUPING SETS((a, b), GROUPING SETS(ROLLUP(a, b)))',
    'SELECT a, b, count(1) FROM testData '
        'GROUP BY a, GROUPING SETS((a, b, a, b), (a, b, a), (a, b))',
    'SELECT a, b, count(1) FROM testData GROUP BY a, '
        'GROUPING SETS(GROUPING SETS((a, b, a, b), (a, b, a), (a, b)))',
    'SELECT a, b, count(1) FROM testData GROUP BY a, GROUPING SETS(ROLLUP(a, b), CUBE(a, b))',
    'SELECT a, b, count(1) FROM testData '
        'GROUP BY a, GROUPING SETS(GROUPING SETS((a, b), (a), ()), '
        'GROUPING SETS((a, b), (a), (b), ()))',
    'SELECT a, b, count(1) FROM testData '
        'GROUP BY a, GROUPING SETS((a, b), (a), (), (a, b), (a), (b), ())',
]


# test nested syntax of grouping set, rollup and cube
@ignore_order
@pytest.mark.parametrize('data_gen', [_grouping_set_gen], ids=idfn)
@pytest.mark.parametrize('sql', _grouping_set_sqls, ids=idfn)
@pytest.mark.skipif(is_before_spark_320(),
                    reason='Nested grouping sets is not supported before spark 3.2.0')
def test_nested_grouping_sets_rollup_cube(data_gen, sql):
    assert_gpu_and_cpu_are_equal_sql(
        lambda spark: gen_df(spark, data_gen, length=2048),
        "testData",
        sql)

