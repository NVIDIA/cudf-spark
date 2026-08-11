# Copyright (c) 2023, NVIDIA CORPORATION.
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

from asserts import assert_gpu_fallback_collect
from spark_session import is_support_default_values_in_schema, with_cpu_session
from marks import allow_non_gpu


# The `DEFAULT` keyword in SQL is not supported before Spark 340, so need to skip it.
@pytest.mark.skipif(not is_support_default_values_in_schema(),
                    reason="Default values in schema is supported from Spark 340")
@allow_non_gpu('ColumnarToRowExec', 'FileSourceScanExec')
@pytest.mark.parametrize('data_source', ['csv', 'json', 'parquet', 'orc'])
def test_scan_fallback_by_default_value(data_source, spark_tmp_table_factory):
    test_table = spark_tmp_table_factory.get()
    def setup_table(spark):
        spark.sql("create table {} (a string, i int default 10) using {}".format(
            test_table, data_source))
        spark.sql("insert into {} values ('s1', DEFAULT)".format(test_table))
        spark.sql("insert into {} values ('s2', DEFAULT)".format(test_table))
        spark.sql("insert into {} values (NULL, DEFAULT)".format(test_table))
        spark.sql("insert into {} values ('s10', null)".format(test_table))
        spark.sql("insert into {} values ('s10', 100)".format(test_table))
    with_cpu_session(setup_table)

    assert_gpu_fallback_collect(
        lambda spark: spark.sql("select * from {}".format(test_table)),
        'FileSourceScanExec',
        conf={'spark.rapids.sql.format.json.enabled': 'true',
              'spark.rapids.sql.format.json.read.enabled': 'true'})
