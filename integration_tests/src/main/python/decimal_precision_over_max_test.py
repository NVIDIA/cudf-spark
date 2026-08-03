# Copyright (c) 2025-2026, NVIDIA CORPORATION.
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
from spark_session import is_spark_400_or_later, is_databricks_runtime

# The following tests are designed to verify the Spark change at
# [SPARK-45905](https://github.com/apache/spark/commit/7120e6b88f2)


def test_decimal_precision_over_max():
    dec_gen = DecimalGen(38, 8, full_precision=True)
    dec2_gen = DecimalGen(38, 6, full_precision=True)
    # Given two operands with decimal types DecimalType(38, 6) and DecimalType(38, 8)
    # respectively, in Spark before 400, the result type of one binary operation is
    # DecimalType(38, 8), while it becomes DecimalType(38, 6) from Spark 400 (with the
    # change SPARK-45905) to keep the integral part as much as possible. See the
    # "boundedPreferIntegralDigits()" function as below.
    #
    # The "overflow" here means the intermediate precision returned from ""widerDecimalType"
    # is "40"(=32+8), which is larger than MAX_PRECISION (=38). So this is just the case
    # we want to test.
    #
    #   def widerDecimalType(p1: Int, s1: Int, p2: Int, s2: Int): DecimalType = {
    #     val scale = max(s1, s2)
    #     val range = max(p1 - s1, p2 - s2)
    #     boundedPreferIntegralDigits(scale + range, scale)
    #   }
    #
    #   def boundedPreferIntegralDigits(precision: Int, scale: Int): DecimalType = {
    #     if (precision <= MAX_PRECISION) {
    #       DecimalType(precision, scale)
    #     } else {
    #       val diff = precision - MAX_PRECISION
    #       DecimalType(MAX_PRECISION, math.max(0, scale - diff))
    #     }
    #   }
    result_scale = 6 if is_spark_400_or_later() or is_databricks_runtime() else 8
    expected_dec_type = DecimalType(38, result_scale)

    def test_fn(spark):
        # We can not cover all the operators, but some mentioned in that Spark PR comments.
        df = two_col_df(spark, dec_gen, dec2_gen, length=100).selectExpr(
            "array(a, b, null, 100)",
            "coalesce(a, b, 100)",
            "coalesce(b, a, null, 100)",
            "a > b",
            "a <= b")
        assert df.schema[0].dataType.elementType == expected_dec_type
        assert df.schema[1].dataType == expected_dec_type
        assert df.schema[2].dataType == expected_dec_type
        # the last two are boolean columns
        return df

    assert_gpu_and_cpu_are_equal_collect(test_fn)
