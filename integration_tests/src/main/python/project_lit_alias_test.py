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

from asserts import assert_gpu_and_cpu_are_equal_collect, assert_gpu_fallback_collect
from data_gen import *
from marks import allow_non_gpu
from pyspark.sql.types import *
import pyspark.sql.functions as f
from spark_session import is_before_spark_400

@pytest.mark.parametrize('data_gen', [decimal_gen_128bit], ids=idfn)
def test_project_alias(data_gen):
    dec = Decimal('123123123123123123123123123.456')
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : binary_op_df(spark, data_gen).select(
            f.col('a').alias('col1'),
            f.col('b').alias('col2'),
            f.lit(dec)))


@allow_non_gpu('ProjectExec')
@pytest.mark.skipif(is_before_spark_400(), reason='VariantType is available in Spark 4.0+')
def test_non_null_variant_literal_falls_back():
    # Spark constant-folds parse_json with a literal input into a non-null Variant literal.
    def canonicalize_variant(rows):
        return [row.v.toJson() for row in rows]

    assert_gpu_fallback_collect(
        lambda spark: spark.sql("""
            SELECT parse_json('{"x":1}') AS v
            FROM range(2)
        """),
        'ProjectExec',
        result_canonicalize_func_before_compare=lambda cpu, gpu:
            (canonicalize_variant(cpu), canonicalize_variant(gpu)))
