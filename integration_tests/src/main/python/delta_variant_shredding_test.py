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

from asserts import assert_cpu_and_gpu_are_equal_collect_with_capture
from delta_lake_utils import delta_meta_allow
from marks import allow_non_gpu, incompat, ignore_order
from spark_session import (is_databricks_runtime, is_databricks173_or_later,
                           is_spark_40x, is_spark_41x, with_cpu_session)


pytestmark = [
    pytest.mark.delta_lake,
    pytest.mark.skipif(
        not ((is_databricks_runtime() and is_databricks173_or_later() and is_spark_40x()) or
             (not is_databricks_runtime() and (is_spark_40x() or is_spark_41x()))),
        reason='Requires DBR 17.3 or OSS Spark 4.0/4.1 with Delta 4.x'),
]

_shredding_features = {'variantShredding-preview', 'variantShredding'}
_table_states = ['unshredded', 'feature_only', 'shredded', 'disabled', 'mixed']
_read_conf = {'spark.sql.variant.pushVariantIntoScan': 'false',
              'spark.sql.adaptive.enabled': 'true',
              'spark.sql.shuffle.partitions': '2'}
# OSS Spark 4.0 requires opting in; DBR 17.3 and Spark 4.1 use the native reader default.
if not is_databricks_runtime() and is_spark_40x():
    _read_conf['spark.sql.variant.allowReadingShredded'] = 'true'


def _physical_schemas(spark, path):
    jvm = spark._jvm
    conf = spark.sparkContext._jsc.hadoopConfiguration()
    hpath = jvm.org.apache.hadoop.fs.Path(path)
    fs = hpath.getFileSystem(conf)
    return [str(jvm.org.apache.parquet.hadoop.ParquetFileReader.readFooter(
                conf, status.getPath()).getFileMetaData().getSchema())
            for status in fs.listStatus(hpath)
            if status.getPath().getName().endswith('.parquet')]


def _write_table(spark, path, state):
    write_shredded = state in ('shredded', 'disabled', 'mixed')
    spark.conf.set('spark.sql.variant.writeShredding.enabled', str(write_shredded).lower())
    spark.conf.set('spark.sql.variant.forceShreddingSchemaForTest',
                   'STRUCT<x: BIGINT>' if write_shredded else '')
    df = spark.range(8).selectExpr('id',
        "parse_json(concat('{\"x\":', id, '}')) AS v")
    df.selectExpr('id', 'v', "named_struct('payload', v) AS nested").coalesce(1) \
        .write.format('delta').mode('overwrite') \
        .option('delta.enableVariantShredding', str(state != 'unshredded').lower()) \
        .save(path)
    schemas = _physical_schemas(spark, path)
    assert schemas
    assert all(('typed_value' in schema) == write_shredded for schema in schemas)
    if state in ('disabled', 'mixed'):
        spark.conf.set('spark.sql.variant.writeShredding.enabled', 'false')
        spark.conf.set('spark.sql.variant.forceShreddingSchemaForTest', '')
        spark.sql(f"ALTER TABLE delta.`{path}` SET TBLPROPERTIES "
                  "('delta.enableVariantShredding' = 'false')").collect()
        if state == 'mixed':
            spark.conf.set('spark.sql.variant.writeShredding.enabled', 'false')
            spark.conf.set('spark.sql.variant.forceShreddingSchemaForTest', '')
            df.selectExpr('id + 8 AS id', 'v', "named_struct('payload', v) AS nested") \
                .coalesce(1).write.format('delta').mode('append').save(path)
            schemas = _physical_schemas(spark, path)
            assert any('typed_value' in schema for schema in schemas)
            assert any('typed_value' not in schema for schema in schemas)
    detail = spark.sql(f'DESCRIBE DETAIL delta.`{path}`').first()
    assert bool(set(detail.tableFeatures) & _shredding_features) == (state != 'unshredded')
    if state in ('disabled', 'mixed'):
        assert detail.properties['delta.enableVariantShredding'] == 'false'


def _read_query(spark, path, query, file_format='delta'):
    # Users must not need to disable shredded reads to accelerate safe Delta tables.
    assert spark.conf.get('spark.sql.variant.allowReadingShredded') == 'true'
    df = spark.read.format(file_format).load(path)
    if query == 'ordinary':
        return df.select('id')
    if query == 'raw':
        return df.selectExpr('id', "to_json(named_struct('payload', v)) AS json")
    if query == 'nested':
        return df.selectExpr('id', 'to_json(nested) AS json')
    if query == 'aqe':
        df = df.repartition(2)
    df = df.selectExpr('id', "try_variant_get(v, '$.x', 'bigint') AS x")
    return df.selectExpr('sum(x) AS total') if query == 'aggregate' else df


@ignore_order(local=True)
@incompat
@allow_non_gpu(*delta_meta_allow, 'FileSourceScanExec', 'ProjectExec', 'VariantGet',
               'ColumnarToRowExec', 'ShuffleExchangeExec')
@pytest.mark.parametrize('state', _table_states)
@pytest.mark.parametrize('query', ['raw', 'nested', 'extract', 'aggregate', 'aqe', 'ordinary'])
def test_delta_variant_protocol_fallback(spark_tmp_path, state, query):
    path = spark_tmp_path + '/DELTA_VARIANT_PROTOCOL'
    with_cpu_session(lambda spark: _write_table(spark, path, state))
    use_gpu_scan = state == 'unshredded' or query == 'ordinary'
    exist = ['GpuFileSourceScanExec'] if use_gpu_scan else ['FileSourceScanExec']
    absent = ['FileSourceScanExec'] if use_gpu_scan else ['GpuFileSourceScanExec', 'GpuVariantGet']
    if query == 'aggregate':
        exist.append('GpuHashAggregateExec')
    if state == 'unshredded' and query in ('extract', 'aggregate', 'aqe'):
        exist.append('GpuVariantGet')
    assert_cpu_and_gpu_are_equal_collect_with_capture(
        lambda spark: _read_query(spark, path, query),
        exist_classes=','.join(exist), non_exist_classes=','.join(absent),
        conf=_read_conf)


@pytest.mark.skipif(is_databricks_runtime(), reason='Exercise OSS Variant extraction pushdown')
@ignore_order(local=True)
@incompat
@allow_non_gpu(*delta_meta_allow, 'FileSourceScanExec', 'ProjectExec', 'VariantGet',
               'ColumnarToRowExec')
@pytest.mark.parametrize('state', _table_states)
def test_delta_variant_protocol_with_pushdown(spark_tmp_path, state):
    path = spark_tmp_path + '/DELTA_VARIANT_PUSHDOWN'
    with_cpu_session(lambda spark: _write_table(spark, path, state))
    # Pushed Variant structs remain unsupported even when the protocol rules out shredding.
    assert_cpu_and_gpu_are_equal_collect_with_capture(
        lambda spark: _read_query(spark, path, 'extract'),
        exist_classes='FileSourceScanExec',
        non_exist_classes='GpuFileSourceScanExec,GpuVariantGet',
        conf=dict(_read_conf, **{'spark.sql.variant.pushVariantIntoScan': 'true'}))


@pytest.mark.skipif(is_databricks_runtime() or not is_spark_41x(),
                    reason='Spark 4.1 retains its configuration-based plain Parquet guard')
@ignore_order(local=True)
@incompat
@allow_non_gpu('FileSourceScanExec', 'ProjectExec', 'VariantGet', 'ColumnarToRowExec')
@pytest.mark.parametrize('shredded', [False, True])
@pytest.mark.parametrize('query', ['raw', 'extract', 'ordinary'])
def test_plain_parquet_variant_guard(spark_tmp_path, shredded, query):
    path = spark_tmp_path + '/PARQUET_VARIANT'

    def write(spark):
        spark.conf.set('spark.sql.variant.writeShredding.enabled', str(shredded).lower())
        spark.conf.set('spark.sql.variant.forceShreddingSchemaForTest',
                       'STRUCT<x: BIGINT>' if shredded else '')
        spark.range(8).selectExpr('id',
            "parse_json(concat('{\"x\":', id, '}')) AS v").coalesce(1).write.parquet(path)
        schemas = _physical_schemas(spark, path)
        assert schemas and all(('typed_value' in schema) == shredded for schema in schemas)

    with_cpu_session(write)
    use_gpu_scan = query == 'ordinary'
    assert_cpu_and_gpu_are_equal_collect_with_capture(
        lambda spark: _read_query(spark, path, query, file_format='parquet'),
        exist_classes='GpuFileSourceScanExec' if use_gpu_scan else 'FileSourceScanExec',
        non_exist_classes='FileSourceScanExec' if use_gpu_scan else
            'GpuFileSourceScanExec,GpuVariantGet',
        conf=_read_conf)


@pytest.mark.skipif(is_databricks_runtime() or not is_spark_41x(),
                    reason='Stable shredding requires OSS Delta 4.1 or later')
@ignore_order(local=True)
@incompat
@allow_non_gpu(*delta_meta_allow, 'FileSourceScanExec', 'ProjectExec', 'VariantGet',
               'ColumnarToRowExec')
@pytest.mark.parametrize('state', ['feature_only', 'shredded', 'disabled', 'mixed'])
@pytest.mark.parametrize('query', ['extract', 'ordinary'])
def test_delta_stable_variant_shredding(spark_tmp_path, state, query):
    path = spark_tmp_path + '/DELTA_STABLE_VARIANT'

    def write(spark):
        delta_conf = spark._jvm.org.apache.spark.sql.delta.sources.DeltaSQLConf
        key = delta_conf.FORCE_USE_PREVIEW_SHREDDING_FEATURE().key()
        spark.conf.set(key, 'false')
        _write_table(spark, path, state)
        features = set(spark.sql(f'DESCRIBE DETAIL delta.`{path}`').first().tableFeatures)
        assert 'variantShredding' in features
        assert 'variantShredding-preview' not in features

    with_cpu_session(write)
    use_gpu_scan = query == 'ordinary'
    assert_cpu_and_gpu_are_equal_collect_with_capture(
        lambda spark: _read_query(spark, path, query),
        exist_classes='GpuFileSourceScanExec' if use_gpu_scan else 'FileSourceScanExec',
        non_exist_classes='FileSourceScanExec' if use_gpu_scan else
            'GpuFileSourceScanExec,GpuVariantGet',
        conf=_read_conf)
