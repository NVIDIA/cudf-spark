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
import os

from asserts import assert_gpu_and_cpu_are_equal_collect
from conftest import is_apache_runtime, is_databricks_runtime
from data_gen import *
from marks import *
from spark_init_internal import get_spark_i_know_what_i_am_doing
import pyspark.sql.functions as f

@ignore_order(local=True)
@pytest.mark.skipif(not is_apache_runtime() and not is_databricks_runtime(), reason="only test on local file system")
def test_kudo_serializer_debug_dump(spark_tmp_path):
    # When the plugin picks the RAPIDS shuffle manager itself, it sets spark.shuffle.manager too.
    startup = get_spark_i_know_what_i_am_doing().sparkContext.getConf()
    if startup.get('spark.shuffle.manager', '').endswith('RapidsShuffleManager') and \
            startup.get('spark.rapids.shuffle.enabled', 'true').lower() == 'true' and \
            startup.get('spark.rapids.shuffle.mode', 'MULTITHREADED').upper() == 'UCX':
        pytest.skip('The UCX shuffle moves GPU buffers and never serializes with Kudo')
    dump_prefix = spark_tmp_path + "/kudo_dump/"
    conf = {
        "spark.rapids.shuffle.kudo.serializer.enabled": "true",
        # only cpu mode is supported for the debug dump feature
        "spark.rapids.shuffle.kudo.serializer.read.mode": "CPU",
        "spark.rapids.shuffle.kudo.serializer.debug.mode": "ALWAYS",
        "spark.rapids.shuffle.kudo.serializer.debug.dump.path.prefix": dump_prefix
    }

    def shuffle_operation(spark):
        df = gen_df(spark, [('id', int_gen)])
        return df.groupBy(df.id % 10).agg(
            f.count("*").alias("count"),
            f.sum("id").alias("sum")
        )
    
    assert_gpu_and_cpu_are_equal_collect(shuffle_operation, conf=conf)

    assert os.path.exists(dump_prefix)
    assert os.path.isdir(dump_prefix)
    assert len(os.listdir(dump_prefix)) > 0
