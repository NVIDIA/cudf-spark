# Copyright (c) 2025, NVIDIA CORPORATION.
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

from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType

from asserts import assert_gpu_and_cpu_are_equal_sql, assert_gpu_and_cpu_are_equal_collect
from data_gen import unary_op_df, float_gen, IntegerGen, SetValuesGen
from marks import ignore_order


# This test suite can not run into the empty list sub-path due to the "In" optimization
# in Spark which can not be disabled as below:
#   object OptimizeIn extends Rule[LogicalPlan] {
#     def apply(plan: LogicalPlan): LogicalPlan = plan.transformWithPruning(...) {
#       case q: LogicalPlan => q.transformExpressionsDownWithPruning(_...) {
#         case In(v, list) if list.isEmpty =>
#           if (!SQLConf.get.getConf(SQLConf.LEGACY_NULL_IN_EMPTY_LIST_BEHAVIOR)) {
#             FalseLiteral
#           } else {
#             If(IsNotNull(v), FalseLiteral, Literal(null, BooleanType))
#           }
#       ...
# But we keep it here in case if any change in the future.
@ignore_order(local=True)
@pytest.mark.parametrize('is_null_in_empty', ["true", "false"],
                         ids=["Legacy_Null", "False_for_Null"])
def test_inset_null_value_in_empty_list(is_null_in_empty):
    def test_fn(spark):
        test_df = unary_op_df(spark, SetValuesGen(IntegerType(), [None, 1]))
        return test_df.select(col('a').isin([]))

    assert_gpu_and_cpu_are_equal_collect(
        test_fn,
        conf={"spark.sql.legacy.nullInEmptyListBehavior": is_null_in_empty}
    )


@ignore_order(local=True)
@pytest.mark.parametrize(
    'set_values',
    ["1.0, 0.0",
     "null, 1.0, 0.0",
     "1.0, 0.0, cast('NaN' as float)",
     "1.0, 0.0, cast('NaN' as float), null"],
    ids=["No_Null_NaN", "With_Null", "With_NaN", "With_Null_NaN"])
def test_inset_with_check_nan(set_values):
    assert_gpu_and_cpu_are_equal_sql(
        lambda spark: unary_op_df(spark, float_gen),
        "inset_test_tbl",
        f"SELECT * FROM inset_test_tbl WHERE a IN ({set_values})"
    )


@ignore_order(local=True)
@pytest.mark.parametrize('set_values', ["1,2", "null,1,2"], ids=["No_Null", "With_Null"])
def test_inset_without_check_nan(set_values):
    assert_gpu_and_cpu_are_equal_sql(
        lambda spark: unary_op_df(spark, IntegerGen(min_val=0, max_val=30)),
        "inset_test_tbl",
        f"SELECT * FROM inset_test_tbl WHERE a IN ({set_values})"
    )
