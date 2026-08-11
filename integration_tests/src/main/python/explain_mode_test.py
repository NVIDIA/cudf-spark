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
from pyspark.sql.types import *
from asserts import assert_gpu_fallback_collect
from data_gen import *
from marks import ignore_order

# copied from sort_test and added explainOnly mode
_explain_mode_conf = {'spark.rapids.sql.mode': 'explainOnly',
                      'spark.sql.join.preferSortMergeJoin': 'True',
                      'spark.sql.shuffle.partitions': '2',
                      }

def create_df(spark, data_gen, left_length, right_length):
    left = binary_op_df(spark, data_gen, length=left_length)
    right = binary_op_df(spark, data_gen, length=right_length).withColumnRenamed("a", "r_a")\
            .withColumnRenamed("b", "r_b")
    return left, right


# just run with one join type since not testing join itself
all_join_types = ['Left']

# use a subset of types just to test explain only mode
all_gen = [StringGen(), ByteGen()]

# here we use the assert_gpu_fallback_collect to make sure explain only mode runs on the CPU
@ignore_order(local=True)
@pytest.mark.parametrize('data_gen', all_gen, ids=idfn)
@pytest.mark.parametrize('join_type', all_join_types, ids=idfn)
def test_explain_only_sortmerge_join(data_gen, join_type):
    # Disable AQE as it can change the plan and use a different join algorithm than the sort merge join.
    conf=copy_and_update(_explain_mode_conf, {'spark.sql.adaptive.enabled': 'false'})
    def do_join(spark):
        left, right = create_df(spark, data_gen, 500, 500)
        return left.join(right, left.a == right.r_a, join_type)
    assert_gpu_fallback_collect(do_join, 'SortMergeJoinExec', conf=conf)
