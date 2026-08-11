# Copyright (c) 2023-2025, NVIDIA CORPORATION.
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
from spark_session import with_cpu_session
from marks import inject_oom
from pyspark.sql.types import IntegerType, StringType

# Define test cases
gen_list_dict = {
    'single_partition_int_value': [
        ('v0', int_gen),
        ('k0', SetValuesGen(IntegerType(), [INT_MAX]))
    ],
    'single_partition_single_value_without_nulls': [
        ('v0', int_gen),
        ('k0', SetValuesGen(StringType(), ["a" * 100]))
    ],
    'single_partition_empty_value_with_nulls': [
        ('v0', int_gen),
        ('k0', SetValuesGen(StringType(), ["", None]))
    ],
    'single_partition_single_empty_value_with_nulls': [
        ('v0', int_gen),
        ('k0', SetValuesGen(StringType(), ["a" * 100, "", None]))
    ],
    'single_partition_multiple_value': [
        ('v0', int_gen),
        ('k0', SetValuesGen(StringType(), ["a" * 100, "a" * 70, None]))
    ],
    'multiple_partition_single_value': [
        ('v0', int_gen),
        ('k0', SetValuesGen(StringType(), ["a" * 100, None])),
        ('k1', SetValuesGen(StringType(), ["b" * 100, None]))
    ],
    'multiple_partition_int_value': [
        ('v0', int_gen),
        ('k0', SetValuesGen(StringType(), ["a" * 100, None])),
        ('k1', SetValuesGen(IntegerType(), [INT_MAX, INT_MIN]))
    ],
    'multiple_partition_multiple_value_wider_first_col': [
        ('v0', int_gen),
        ('k0', SetValuesGen(StringType(), ["a" * 100, "a" * 70, None])),
        ('k1', SetValuesGen(StringType(), ["b" * 50, "b" * 20, None]))
    ],
    'multiple_partition_multiple_value_narrow_first_col': [
        ('v0', int_gen),
        ('k0', SetValuesGen(StringType(), ["a" * 50, "a" * 20, None])),
        ('k1', SetValuesGen(StringType(), ["b" * 100, "b" * 70, None]))
    ]
}

file_formats = ['parquet', 'orc']
if os.environ.get('INCLUDE_SPARK_AVRO_JAR', 'false') == 'true':
    file_formats = file_formats + ['avro']

conf = {
    'spark.rapids.sql.columnSizeBytes': 1000,
    'spark.sql.orc.impl': 'hive',  # null type column is not supported on native
    'spark.rapids.sql.format.avro.enabled': 'true',
    'spark.rapids.sql.format.avro.read.enabled': 'true'
}


def extract_partition_cols(gen_list):
    partition_cols = [item[0] for item in gen_list if item[0].startswith('k')]
    return partition_cols


@inject_oom
@pytest.mark.parametrize('file_format', file_formats)
@pytest.mark.parametrize('key', gen_list_dict.keys())
@pytest.mark.parametrize('reader_type', ['AUTO', 'COALESCING', 'MULTITHREADED', 'PERFILE'])
def test_col_size_exceeding_cudf_limit(spark_tmp_path, file_format, key, reader_type):
    gen_list = gen_list_dict[key]
    partition_cols = extract_partition_cols(gen_list)
    gen = StructGen(gen_list, nullable=False)
    data_path = spark_tmp_path + '/PART_DATA/' + key
    new_conf=copy_and_update(conf, {'spark.rapids.sql.format.parquet.reader.type': reader_type,
        'spark.rapids.sql.format.orc.reader.type': reader_type,
        'spark.rapids.sql.format.avro.reader.type': reader_type})
    with_cpu_session(
        lambda spark: gen_df(spark, gen, length=5000).coalesce(1).write.partitionBy(partition_cols).format(file_format)
        .save(data_path))
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.format(file_format).load(data_path).coalesce(1), conf)
