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

import json

import pytest

from asserts import assert_gpu_and_cpu_are_equal_collect
from iceberg import get_full_table_name, iceberg_unsupported_mark, supports_iceberg_v3, \
    ICEBERG_V3_UNSUPPORTED_REASON
from iceberg.iceberg_test import _ICEBERG_VARIANT_READ_CONF, _setup_iceberg_variant_table
from marks import iceberg, ignore_order, incompat
from spark_session import is_spark_411_or_later, with_cpu_session

pytestmark = iceberg_unsupported_mark


@iceberg
@ignore_order(local=True)
@incompat
@pytest.mark.skipif(not supports_iceberg_v3, reason=ICEBERG_V3_UNSUPPORTED_REASON)
@pytest.mark.skipif(not is_spark_411_or_later(),
                   reason="Iceberg Variant requires Spark 4.1.1+")
def test_iceberg_v3_variant_scalar_and_null_read(spark_tmp_table_factory):
    table = get_full_table_name(spark_tmp_table_factory)
    with_cpu_session(
        lambda spark: _setup_iceberg_variant_table(spark, table),
        conf=_ICEBERG_VARIANT_READ_CONF)

    def canonicalize(cpu, gpu):
        def values(rows):
            # Keep JSON null as the string "null", distinct from SQL NULL (Python None).
            return [(row.id, None if row.v is None else json.dumps(
                json.loads(row.v.toJson()), sort_keys=True, separators=(",", ":")))
                for row in rows]

        cpu_values, gpu_values = values(cpu), values(gpu)
        expected = {2: '"scalar"', 3: '3.5', 4: 'true', 5: 'null', 6: None, 7: '{}', 8: '[]'}
        assert {key: value for key, value in cpu_values if key in expected} == expected
        return cpu_values, gpu_values

    # Read the Variant itself: extracting object paths alone returns NULL for every scalar.
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.sql(f"SELECT id, v FROM {table}"),
        conf=_ICEBERG_VARIANT_READ_CONF,
        result_canonicalize_func_before_compare=canonicalize)
