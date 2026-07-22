# Copyright (c) 2026, NVIDIA CORPORATION.
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
from marks import incompat
from spark_session import is_before_spark_400, with_cpu_session

pytestmark = [pytest.mark.premerge_ci_1]

_variant_parquet_conf = {
    'spark.rapids.sql.format.parquet.enabled': 'true',
    'spark.rapids.sql.format.parquet.read.enabled': 'true',
    'spark.sql.sources.useV1SourceList': 'parquet'
}


def _write_variant_parquet(spark, path):
    spark.sql("""
      SELECT parse_json('{"x":7,"y":"hi"}') AS v
      UNION ALL
      SELECT parse_json('{"x":42,"z":"skip"}') AS v
      UNION ALL
      SELECT parse_json('{"y":"zzz"}') AS v
    """).write.mode('overwrite').parquet(path)


@incompat
@pytest.mark.skipif(is_before_spark_400(), reason='VariantType is available in Spark 4.0+')
def test_parquet_variant_try_get_string(spark_tmp_path):
    data_path = spark_tmp_path + '/VARIANT_PARQUET'
    with_cpu_session(lambda spark: _write_variant_parquet(spark, data_path))

    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.parquet(data_path).selectExpr(
            "try_variant_get(v, '$.y', 'string') AS y"),
        conf=_variant_parquet_conf)
