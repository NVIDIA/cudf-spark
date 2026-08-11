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

from asserts import assert_gpu_and_cpu_are_equal_collect, assert_gpu_and_cpu_are_equal_sql, assert_gpu_and_cpu_error
from data_gen import *
from marks import incompat, approximate_float
from pyspark.sql.types import *
import pyspark.sql.functions as f
from spark_session import is_databricks_version_or_later, is_spark_400_or_later

# post_transform variants exercise SPARK-14393: values from non-deterministic
# functions must stay stable across coalesce/union. See spark-rapids/#14156.
@pytest.mark.parametrize('post_transform', [
    pytest.param(lambda df: df, id='no_transform'),
    pytest.param(lambda df: df.coalesce(2), id='coalesce_2'),
    pytest.param(lambda df: df.coalesce(1), id='coalesce_1'),
])
def test_mono_id(post_transform):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : post_transform(
                unary_op_df(spark, short_gen, num_slices=8).select(
                    f.col('a'),
                    f.monotonically_increasing_id())))

@pytest.mark.parametrize('post_transform', [
    pytest.param(lambda df: df, id='no_transform'),
    pytest.param(lambda df: df.coalesce(2), id='coalesce_2'),
    pytest.param(lambda df: df.coalesce(1), id='coalesce_1'),
])
def test_part_id(post_transform):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : post_transform(
                unary_op_df(spark, short_gen, num_slices=8).select(
                    f.col('a'),
                    f.spark_partition_id())))

# Spark conf key for choosing legacy error semantics.
legacy_semantics_key = "spark.sql.legacy.raiseErrorWithoutErrorClass"

def raise_error_test_impl(test_conf):
    use_new_error_semantics = legacy_semantics_key in test_conf and test_conf[legacy_semantics_key] == False

    data_gen = ShortGen(nullable=False, min_val=0, max_val=20, special_cases=[])

    # Test for "when" selecting the "raise_error()" expression (null-type).
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: unary_op_df(spark, data_gen, num_slices=2).select(
            f.when(f.col('a') > 30, f.raise_error("unexpected"))),
        conf=test_conf)

    # Test for if/else, with raise_error in the else.
    # This should test if the data-type of raise_error() interferes with
    # the result-type of the parent expression (if/else).
    assert_gpu_and_cpu_are_equal_sql(
        lambda spark: unary_op_df(spark, data_gen, num_slices=2),
        'test_table',
        """
        SELECT IF( a < 30, a, raise_error('unexpected') )
        FROM test_table
        """,
        conf=test_conf)

    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.range(0).select(f.raise_error(f.col("id"))),
        conf=test_conf)

    error_fragment = "SparkRuntimeException" if use_new_error_semantics \
      else "java.lang.RuntimeException"
    assert_gpu_and_cpu_error(
        lambda spark: unary_op_df(spark, null_gen, length=2, num_slices=1).select(
                f.raise_error(f.col('a'))).collect(),
        conf=test_conf,
        error_message=error_fragment)

    error_fragment = error_fragment + (": [USER_RAISED_EXCEPTION] unexpected" if use_new_error_semantics
      else ": unexpected")
    assert_gpu_and_cpu_error(
        lambda spark: unary_op_df(spark, short_gen, length=2, num_slices=1).select(
                f.raise_error(f.lit("unexpected"))).collect(),
        conf=test_conf,
        error_message=error_fragment)


def test_raise_error_legacy_semantics():
    """
    Tests the "legacy" semantics of raise_error(), i.e. where the error
    does not include an error class.
    """
    if is_spark_400_or_later() or is_databricks_version_or_later(14, 3):
        # Spark 4+ and Databricks 14.3+ support RaiseError with error-classes included.
        # Must test "legacy" mode, where error-classes are excluded.
        raise_error_test_impl(test_conf={legacy_semantics_key: True})
    else:
        # Spark versions preceding 4.0, or Databricks 14.3 do not support RaiseError with
        # error-classes.  No legacy mode need be selected.
        raise_error_test_impl(test_conf={})


@pytest.mark.skipif(condition=not (is_spark_400_or_later() or is_databricks_version_or_later(14, 3)),
                    reason="RaiseError semantics with error-classes are only supported "
                           "on Spark 4.0+ and Databricks 14.3+.")
def test_raise_error_new_semantics():
    """
    Tests the "new" semantics of raise_error(), i.e. where the error
    includes an error class.  Unsupported in Spark versions predating
    Spark 4.0, Databricks 14.3.
    """
    raise_error_test_impl(test_conf={legacy_semantics_key: False})
