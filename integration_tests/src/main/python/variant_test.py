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

from asserts import (assert_gpu_and_cpu_are_equal_collect,
                     assert_gpu_fallback_write,
                     assert_gpu_fallback_collect)
from data_gen import idfn
from marks import allow_non_gpu, incompat
from spark_session import is_before_spark_400, with_cpu_session

pytestmark = [pytest.mark.premerge_ci_1]

_variant_parquet_conf = {
    'spark.rapids.sql.format.parquet.enabled': 'true',
    'spark.rapids.sql.format.parquet.read.enabled': 'true',
    'spark.rapids.sql.format.parquet.write.enabled': 'true',
    'spark.sql.sources.useV1SourceList': 'parquet'
}


def _write_variant_parquet(spark, path):
    spark.sql("""
      SELECT parse_json('{"x":7,"s":300,"i":40000,"l":3000000000,"m":7,"y":"hi","n":{"inner":"deep","num":9},"arr":[10,20],"flag":true}') AS v
      UNION ALL
      SELECT parse_json('{"x":42,"s":32000,"i":50000,"l":4000000000,"m":300,"z":"skip","n":{"inner":"value","num":11},"arr":[30],"flag":false}') AS v
      UNION ALL
      SELECT parse_json('{"y":"zzz","m":40000}') AS v
      UNION ALL
      SELECT parse_json('{"m":3000000000}') AS v
    """).write.mode('overwrite').parquet(path)


@incompat
@allow_non_gpu('DataWritingCommandExec', 'WriteFilesExec',
               'ColumnarToRowExec', 'FileSourceScanExec')
@pytest.mark.skipif(is_before_spark_400(), reason='VariantType is available in Spark 4.0+')
def test_parquet_variant_write_falls_back(spark_tmp_path):
    source_path = spark_tmp_path + '/VARIANT_WRITE_SOURCE'
    with_cpu_session(lambda spark: _write_variant_parquet(spark, source_path))

    def write_data(spark, path):
        spark.read.parquet(source_path).write.mode('overwrite').parquet(path)

    def read_data(spark, path):
        return spark.read.parquet(path).selectExpr(
            "try_variant_get(v, '$.x', 'int') AS x",
            "try_variant_get(v, '$.y', 'string') AS y",
            "try_variant_get(v, '$.n.inner', 'string') AS inner",
            "try_variant_get(v, '$.n.num', 'int') AS num")

    write_conf = dict(_variant_parquet_conf)
    write_conf['spark.rapids.sql.format.parquet.read.enabled'] = 'false'
    assert_gpu_fallback_write(
        write_data, read_data, spark_tmp_path, ['DataWritingCommandExec', 'WriteFilesExec'],
        conf=write_conf)


@incompat
@pytest.mark.skipif(is_before_spark_400(), reason='VariantType is available in Spark 4.0+')
def test_parquet_variant_try_get_string(spark_tmp_path):
    data_path = spark_tmp_path + '/VARIANT_PARQUET'
    with_cpu_session(lambda spark: _write_variant_parquet(spark, data_path))

    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.parquet(data_path).selectExpr(
            "try_variant_get(v, '$.y', 'string') AS y"),
        conf=_variant_parquet_conf)


@pytest.mark.parametrize('field_name,target_type', [
    ('x', 'tinyint'),
    ('x', 'smallint'),
    ('x', 'int'),
    ('x', 'bigint'),
    ('s', 'tinyint'),
    ('s', 'smallint'),
    ('s', 'int'),
    ('s', 'bigint'),
    ('i', 'int'),
    ('i', 'bigint'),
    ('l', 'bigint'),
    ('m', 'tinyint'),
    ('m', 'smallint'),
    ('m', 'int'),
    ('m', 'bigint')
], ids=idfn)
@incompat
@pytest.mark.skipif(is_before_spark_400(), reason='VariantType is available in Spark 4.0+')
def test_parquet_variant_try_get_integral_targets(spark_tmp_path, field_name, target_type):
    data_path = spark_tmp_path + '/VARIANT_PARQUET'
    with_cpu_session(lambda spark: _write_variant_parquet(spark, data_path))

    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.parquet(data_path).selectExpr(
            f"try_variant_get(v, '$.{field_name}', '{target_type}') AS x"),
        conf=_variant_parquet_conf)


@incompat
@pytest.mark.skipif(is_before_spark_400(), reason='VariantType is available in Spark 4.0+')
def test_parquet_variant_try_get_nested_object_path(spark_tmp_path):
    data_path = spark_tmp_path + '/VARIANT_PARQUET'
    with_cpu_session(lambda spark: _write_variant_parquet(spark, data_path))

    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.parquet(data_path).selectExpr(
            "try_variant_get(v, '$.n.inner', 'string') AS inner",
            "try_variant_get(v, '$.n.num', 'int') AS num"),
        conf=_variant_parquet_conf)


@incompat
@pytest.mark.skipif(is_before_spark_400(), reason='VariantType is available in Spark 4.0+')
def test_parquet_variant_try_get_null_variant_rows(spark_tmp_path):
    data_path = spark_tmp_path + '/VARIANT_NULL_PARQUET'

    def write_data(spark):
        spark.sql("""
          SELECT parse_json('{"x":7,"y":"hi"}') AS v
          UNION ALL
          SELECT parse_json(NULL) AS v
          UNION ALL
          SELECT parse_json('{"x":42}') AS v
        """).write.mode('overwrite').parquet(data_path)

    with_cpu_session(write_data)

    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.parquet(data_path).selectExpr(
            "try_variant_get(v, '$.x', 'int') AS x",
            "try_variant_get(v, '$.y', 'string') AS y"),
        conf=_variant_parquet_conf)


@allow_non_gpu('ProjectExec')
@incompat
@pytest.mark.skipif(is_before_spark_400(), reason='VariantType is available in Spark 4.0+')
def test_variant_try_get_array_path_falls_back():
    def do_it(spark):
        return spark.createDataFrame([
            ('{"arr":[10,20]}',),
            ('{"arr":[30]}',),
            ('{"x":7}',)
        ], ['json']).selectExpr(
            "try_variant_get(parse_json(json), '$.arr[0]', 'int') AS first_value")

    assert_gpu_fallback_collect(do_it, 'VariantGet')


@allow_non_gpu('ProjectExec')
@incompat
@pytest.mark.skipif(is_before_spark_400(), reason='VariantType is available in Spark 4.0+')
def test_variant_get_strict_mode_falls_back():
    def do_it(spark):
        return spark.createDataFrame([
            ('{"x":7}',),
            ('{"x":42}',),
            ('{}',)
        ], ['json']).selectExpr(
            "variant_get(parse_json(json), '$.x', 'int') AS x")

    assert_gpu_fallback_collect(do_it, 'VariantGet')


@allow_non_gpu('ProjectExec')
@incompat
@pytest.mark.skipif(is_before_spark_400(), reason='VariantType is available in Spark 4.0+')
def test_variant_try_get_unsupported_target_type_falls_back():
    def do_it(spark):
        return spark.createDataFrame([
            ('{"flag":true}',),
            ('{"flag":false}',),
            ('{"x":7}',)
        ], ['json']).selectExpr(
            "try_variant_get(parse_json(json), '$.flag', 'boolean') AS flag")

    assert_gpu_fallback_collect(do_it, 'VariantGet')
