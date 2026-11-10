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

from asserts import (assert_cpu_and_gpu_are_equal_collect_with_capture,
                     assert_equal_with_signed_zero,
                     assert_gpu_and_cpu_are_equal_collect,
                     assert_spark_exception,
                     assert_gpu_fallback_write,
                     assert_gpu_fallback_collect,
                     run_with_cpu_and_gpu)
from conftest import is_databricks_runtime, spark_jvm
from data_gen import idfn
from marks import allow_non_gpu, ignore_order, incompat, validate_execs_in_gpu_plan
from spark_session import (is_before_spark_400, is_databricks173_or_later, is_spark_40x,
                           is_spark_410_or_later, is_spark_411_or_later, with_cpu_session,
                           with_gpu_session)

pytestmark = pytest.mark.skipif(
    is_databricks_runtime() and not is_databricks173_or_later(),
    reason='Variant extraction is supported for Databricks 17.3+')

_variant_parquet_conf = {
    'spark.rapids.sql.format.parquet.enabled': 'true',
    'spark.rapids.sql.format.parquet.read.enabled': 'true',
    'spark.rapids.sql.format.parquet.write.enabled': 'true',
    'spark.sql.sources.useV1SourceList': 'parquet'
}

_variant_write_conf = {}

if not is_before_spark_400():
    # Keep extraction above the scan in tests intended to exercise GpuVariantGet.
    _variant_parquet_conf['spark.sql.variant.pushVariantIntoScan'] = 'false'

if is_spark_410_or_later():
    # Spark 4.1+ (including Databricks 18.3, which reports Spark 4.1.0) may write shredded
    # Variant columns. The GPU implementation currently operates on unshredded Variant columns,
    # so require unshredded reads when exercising GpuVariantGet.
    _variant_parquet_conf['spark.sql.variant.allowReadingShredded'] = 'false'
    _variant_write_conf['spark.sql.variant.writeShredding.enabled'] = 'false'


_variant_pushdown_scan_params = [
    pytest.param('parquet', 'FileSourceScanExec', False, id='v1'),
    pytest.param('', 'BatchScanExec', True, id='v2')
]


def _with_cpu_variant_session(func):
    return with_cpu_session(func, conf=_variant_write_conf)


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


def _write_array_variant_parquet(spark, path):
    spark.sql("""
      SELECT id, parse_json(json) AS v
      FROM VALUES
        (0, '{"items":[{"sku":"a","qty":1},{"sku":"b","qty":2}],"matrix":[[10,11],[20]]}'),
        (1, '{"items":[],"matrix":[]}'),
        (2, '{"items":[null],"matrix":[[30]]}'),
        (3, '{"items":"wrong container","matrix":40}'),
        (4, '[{"sku":"root"}]'),
        (5, 'null'),
        (6, NULL)
      AS source(id, json)
    """).write.mode('overwrite').parquet(path)


def _write_common_scalar_variant_parquet(spark, path):
    spark.sql("""
      SELECT id, to_variant_object(named_struct(
        'bool_value', bool_value,
        'float_value', float_value,
        'double_value', double_value)) AS v
      FROM VALUES
        (0, true, CAST(1.25 AS FLOAT), CAST(2.5 AS DOUBLE)),
        (1, false,
          CAST(-1.0 AS FLOAT) * CAST(0.0 AS FLOAT),
          CAST(-1.0 AS DOUBLE) * CAST(0.0 AS DOUBLE)),
        (2, true, CAST('NaN' AS FLOAT), CAST('Infinity' AS DOUBLE))
      AS source(id, bool_value, float_value, double_value)
      UNION ALL
      SELECT 3 AS id, CAST(NULL AS VARIANT) AS v
    """).write.mode('overwrite').parquet(path)


def _write_heterogeneous_variant_parquet(spark, path):
    spark.sql("""
      SELECT id, parse_json(json) AS v
      FROM VALUES
        (0, '{"x":42}'),
        (1, '{"x":"42"}'),
        (2, '{"x":7}'),
        (3, '{"x":true}'),
        (4, '{"x":false}'),
        (5, '{"x":"bad"}'),
        (6, '{"x":null}'),
        (7, '{"y":"missing"}'),
        (8, '{"x":128}'),
        (9, '{"x":32768}'),
        (10, '{"x":2147483648}')
      AS source(id, json)
    """).write.mode('overwrite').parquet(path)


def _write_variant_with_path_parquet(spark, path):
    spark.sql("""
      SELECT parse_json(json) AS v, path
      FROM VALUES
        ('{"x":7}', '$.x'),
        ('{"x":42}', '$.x'),
        ('{}', '$.x')
      AS source(json, path)
    """).write.mode('overwrite').parquet(path)


def _write_variant_if_parquet(spark, path):
    spark.sql("""
      SELECT id, parse_json(left_json) AS left_v, parse_json(right_json) AS right_v
      FROM VALUES
        (0, '{"x":1}', '{"x":10}'),
        (1, '{"x":2}', '{"x":20}'),
        (2, NULL, '{"x":30}'),
        (3, '{"x":4}', NULL)
      AS source(id, left_json, right_json)
    """).write.mode('overwrite').parquet(path)


@incompat
@allow_non_gpu('RDDScanExec')
@ignore_order(local=True)
@pytest.mark.skipif(is_before_spark_400(), reason='VariantType is available in Spark 4.0+')
def test_variant_cpu_rows_convert_to_gpu():
    def make_rows(spark):
        df = spark.sql("""
          SELECT id, parse_json(json) AS v
          FROM VALUES
            (0, '{"x":7,"s":"a"}'),
            (1, '{"x":42,"s":"longer-value"}'),
            (2, NULL)
          AS source(id, json)
        """)
        return df.collect(), df.schema

    rows, schema = with_cpu_session(make_rows)

    def do_it(spark):
        # A DataFrame backed by local VariantVal rows produces a row-based RDDScanExec.
        return spark.createDataFrame(rows, schema).selectExpr(
            'id',
            "try_variant_get(v, '$.x', 'bigint') AS x",
            "try_variant_get(v, '$.s', 'string') AS s")

    assert_cpu_and_gpu_are_equal_collect_with_capture(
        do_it,
        exist_classes='RDDScanExec,GpuRowToColumnarExec,GpuProjectExec,GpuVariantGet',
        non_exist_classes='HostColumnarToGpu',
        conf={'spark.sql.adaptive.enabled': 'false'},
        require_non_empty=True)


@incompat
@allow_non_gpu('FileSourceScanExec', 'ColumnarToRowExec')
@ignore_order(local=True)
@pytest.mark.skipif(is_before_spark_400(), reason='VariantType is available in Spark 4.0+')
def test_cpu_vectorized_parquet_variant_converts_via_rows(spark_tmp_path):
    data_path = spark_tmp_path + '/CPU_VECTORIZED_VARIANT_PARQUET'
    _with_cpu_variant_session(lambda spark: _write_variant_parquet(spark, data_path))

    def do_it(spark):
        return spark.read.parquet(data_path).selectExpr(
            "try_variant_get(v, '$.x', 'int') AS x",
            "try_variant_get(v, '$.y', 'string') AS y")

    conf = dict(_variant_parquet_conf)
    conf.update({
        'spark.sql.adaptive.enabled': 'false',
        'spark.sql.parquet.enableVectorizedReader': 'true',
        # Force a CPU columnar scan while leaving its Variant consumer GPU-eligible.
        'spark.rapids.sql.format.parquet.read.enabled': 'false',
        'spark.sql.variant.pushVariantIntoScan': 'false',
        'spark.sql.variant.allowReadingShredded': 'false',
    })
    assert_cpu_and_gpu_are_equal_collect_with_capture(
        do_it,
        exist_classes=('FileSourceScanExec,ColumnarToRowExec,'
                       'GpuRowToColumnarExec,GpuProjectExec,GpuVariantGet'),
        non_exist_classes='HostColumnarToGpu',
        conf=conf,
        require_non_empty=True)


@incompat
@allow_non_gpu('DataWritingCommandExec', 'WriteFilesExec',
               'ColumnarToRowExec', 'FileSourceScanExec')
@pytest.mark.skipif(is_before_spark_400(), reason='VariantType is available in Spark 4.0+')
def test_parquet_variant_write_falls_back(spark_tmp_path):
    source_path = spark_tmp_path + '/VARIANT_WRITE_SOURCE'
    _with_cpu_variant_session(lambda spark: _write_variant_parquet(spark, source_path))

    def write_data(spark, path):
        spark.read.parquet(source_path).write.mode('overwrite').parquet(path)

    def read_data(spark, path):
        return spark.read.parquet(path).selectExpr(
            "try_variant_get(v, '$.x', 'int') AS x",
            "try_variant_get(v, '$.y', 'string') AS y",
            "try_variant_get(v, '$.n.inner', 'string') AS inner",
            "try_variant_get(v, '$.n.num', 'int') AS num")

    write_conf = dict(_variant_parquet_conf)
    write_conf.update(_variant_write_conf)
    write_conf['spark.rapids.sql.format.parquet.read.enabled'] = 'false'
    assert_gpu_fallback_write(
        write_data, read_data, spark_tmp_path, ['DataWritingCommandExec', 'WriteFilesExec'],
        conf=write_conf)


@allow_non_gpu('FileSourceScanExec', 'BatchScanExec', 'ColumnarToRowExec')
@incompat
@pytest.mark.parametrize(
    'v1_enabled_list,fallback_class,requires_v2_pushdown', _variant_pushdown_scan_params)
@pytest.mark.skipif(not is_spark_40x(),
                    reason='This test covers Variant scan pushdown on Spark 4.0.x')
def test_parquet_variant_scan_pushdown_falls_back(
        spark_tmp_path, v1_enabled_list, fallback_class, requires_v2_pushdown):
    if requires_v2_pushdown:
        supports_v2_pushdown = spark_jvm().com.nvidia.spark.rapids.shims \
            .ParquetVariantShims.supportsV2VariantPushdown()
        if not supports_v2_pushdown:
            pytest.skip('The selected shim does not support V2 Variant scan pushdown')

    data_path = spark_tmp_path + '/VARIANT_SCAN_PUSHDOWN_FALLBACK_PARQUET'
    _with_cpu_variant_session(lambda spark: _write_variant_parquet(spark, data_path))

    def do_it(spark):
        return spark.read.parquet(data_path).selectExpr(
            "try_variant_get(v, '$.x', 'int') AS x",
            "try_variant_get(v, '$.y', 'string') AS y")

    read_conf = dict(_variant_parquet_conf)
    read_conf['spark.sql.sources.useV1SourceList'] = v1_enabled_list
    read_conf['spark.sql.variant.pushVariantIntoScan'] = 'true'
    # TODO(#14251): Replace this fallback assertion when pushed Variant scans run on GPU.
    assert_gpu_fallback_collect(
        do_it, fallback_class, conf=read_conf)


@allow_non_gpu('FileSourceScanExec', 'ColumnarToRowExec')
@incompat
@pytest.mark.skipif(not is_spark_411_or_later(),
                    reason='Variant scan pushdown is available in Spark 4.1.1+')
def test_parquet_variant_shredding_and_scan_pushdown_fall_back(spark_tmp_path):
    data_path = spark_tmp_path + '/VARIANT_SCAN_PUSHDOWN_FALLBACK_PARQUET'
    write_conf = {'spark.sql.variant.writeShredding.enabled': 'true'}
    with_cpu_session(
        lambda spark: _write_variant_parquet(spark, data_path), conf=write_conf)

    def do_it(spark):
        return spark.read.parquet(data_path).selectExpr(
            "try_variant_get(v, '$.x', 'int') AS x",
            "try_variant_get(v, '$.y', 'string') AS y")

    read_conf = dict(_variant_parquet_conf)
    read_conf['spark.sql.variant.pushVariantIntoScan'] = 'true'
    read_conf['spark.sql.variant.allowReadingShredded'] = 'true'
    # TODO(#14251): Replace this fallback assertion when pushed Variant scans run on GPU.
    assert_gpu_fallback_collect(
        do_it, 'FileSourceScanExec', conf=read_conf)


@allow_non_gpu('FileSourceScanExec', 'BatchScanExec', 'ColumnarToRowExec',
               'ProjectExec', 'VariantGet', 'ShuffleExchangeExec')
@incompat
@pytest.mark.parametrize('pushdown_enabled,nested_pass_through,repartition_before_extract', [
    ('true', True, False),
    ('false', False, False),
    ('false', False, True),
], ids=['nested-pass-through', 'pushdown-disabled', 'aqe-query-stage'])
@pytest.mark.parametrize('v1_enabled_list,fallback_class', [
    ('parquet', 'FileSourceScanExec'),
    ('', 'BatchScanExec'),
], ids=['v1', 'v2'])
@pytest.mark.skipif(not is_spark_411_or_later(),
                    reason='Variant shredding is enabled by default in Spark 4.1.1+')
def test_parquet_shredded_raw_variant_scan_falls_back(
        spark_tmp_path, pushdown_enabled, nested_pass_through, repartition_before_extract,
        v1_enabled_list, fallback_class):
    data_path = spark_tmp_path + '/RAW_SHREDDED_VARIANT_SCAN_FALLBACK_PARQUET'

    def write_data(spark):
        spark.sql("""
          SELECT
            parse_json('{"x":7}') AS v,
            named_struct('payload', parse_json('{"x":42}')) AS nested
        """).write.mode('overwrite').parquet(data_path)

    write_conf = {'spark.sql.variant.writeShredding.enabled': 'true'}
    with_cpu_session(write_data, conf=write_conf)

    def do_it(spark):
        df = spark.read.parquet(data_path)
        if nested_pass_through:
            return df.selectExpr('to_json(nested) AS nested')
        if repartition_before_extract:
            df = df.repartition(2)
        return df.selectExpr("try_variant_get(v, '$.x', 'int') AS x")

    read_conf = dict(_variant_parquet_conf)
    read_conf['spark.sql.sources.useV1SourceList'] = v1_enabled_list
    read_conf['spark.sql.adaptive.enabled'] = 'true'
    read_conf['spark.sql.variant.pushVariantIntoScan'] = pushdown_enabled
    read_conf['spark.sql.variant.allowReadingShredded'] = 'true'
    assert_gpu_fallback_collect(
        do_it, fallback_class, conf=read_conf)


@allow_non_gpu('FileSourceScanExec', 'BatchScanExec', 'ColumnarToRowExec',
               'ProjectExec', 'VariantGet')
@incompat
@pytest.mark.parametrize('v1_enabled_list,fallback_class', [
    ('parquet', 'FileSourceScanExec'),
    ('', 'BatchScanExec'),
], ids=['v1', 'v2'])
@pytest.mark.skipif(not is_spark_411_or_later(),
                    reason='Variant shredding is enabled by default in Spark 4.1.1+')
def test_parquet_shredded_variant_aggregate_returns_to_gpu(
        spark_tmp_path, v1_enabled_list, fallback_class):
    data_path = spark_tmp_path + '/SHREDDED_VARIANT_AGGREGATE_PARQUET'
    write_conf = {'spark.sql.variant.writeShredding.enabled': 'true'}
    with_cpu_session(
        lambda spark: _write_variant_parquet(spark, data_path), conf=write_conf)

    def do_it(spark):
        return spark.read.parquet(data_path) \
            .selectExpr("try_variant_get(v, '$.x', 'int') AS x") \
            .selectExpr('sum(x) AS total')

    read_conf = dict(_variant_parquet_conf)
    read_conf['spark.sql.sources.useV1SourceList'] = v1_enabled_list
    read_conf['spark.sql.adaptive.enabled'] = 'true'
    read_conf['spark.sql.variant.pushVariantIntoScan'] = 'false'
    read_conf['spark.sql.variant.allowReadingShredded'] = 'true'
    assert_cpu_and_gpu_are_equal_collect_with_capture(
        do_it,
        exist_classes=f'{fallback_class},ProjectExec,GpuHashAggregateExec',
        non_exist_classes='GpuVariantGet',
        conf=read_conf)


@incompat
@pytest.mark.parametrize('v1_enabled_list', ['parquet', ''], ids=['v1', 'v2'])
@pytest.mark.skipif(is_before_spark_400(), reason='VariantType is available in Spark 4.0+')
def test_parquet_variant_try_get_string(spark_tmp_path, v1_enabled_list):
    data_path = spark_tmp_path + '/VARIANT_PARQUET'
    _with_cpu_variant_session(lambda spark: _write_variant_parquet(spark, data_path))

    read_conf = dict(_variant_parquet_conf)
    read_conf['spark.sql.sources.useV1SourceList'] = v1_enabled_list
    assert_cpu_and_gpu_are_equal_collect_with_capture(
        lambda spark: spark.read.parquet(data_path).selectExpr(
            "try_variant_get(v, '$.y', 'string') AS y"),
        exist_classes='GpuVariantGet',
        conf=read_conf)


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
    _with_cpu_variant_session(lambda spark: _write_variant_parquet(spark, data_path))

    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.parquet(data_path).selectExpr(
            f"try_variant_get(v, '$.{field_name}', '{target_type}') AS x"),
        conf=_variant_parquet_conf)


@incompat
@pytest.mark.skipif(is_before_spark_400(), reason='VariantType is available in Spark 4.0+')
def test_parquet_variant_try_get_nested_object_path(spark_tmp_path):
    data_path = spark_tmp_path + '/VARIANT_PARQUET'
    _with_cpu_variant_session(lambda spark: _write_variant_parquet(spark, data_path))

    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.parquet(data_path).selectExpr(
            "try_variant_get(v, '$.n.inner', 'string') AS inner",
            "try_variant_get(v, '$.n.num', 'int') AS num"),
        conf=_variant_parquet_conf)


@incompat
@pytest.mark.skipif(is_before_spark_400(), reason='VariantType is available in Spark 4.0+')
def test_parquet_nested_struct_variant_try_get(spark_tmp_path):
    data_path = spark_tmp_path + '/NESTED_STRUCT_VARIANT_PARQUET'

    def write_data(spark):
        spark.sql("""
          SELECT id, named_struct('payload', parse_json(json)) AS nested
          FROM VALUES
            (0, '{"x":7}'),
            (1, '{"x":42}'),
            (2, '{}')
          AS source(id, json)
        """).write.mode('overwrite').parquet(data_path)

    _with_cpu_variant_session(write_data)

    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.parquet(data_path).selectExpr(
            "id",
            "try_variant_get(nested.payload, '$.x', 'int') AS x").orderBy("id"),
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

    _with_cpu_variant_session(write_data)

    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.parquet(data_path).selectExpr(
            "try_variant_get(v, '$.x', 'int') AS x",
            "try_variant_get(v, '$.y', 'string') AS y"),
        conf=_variant_parquet_conf)


@incompat
@pytest.mark.skipif(is_before_spark_400(), reason='VariantType is available in Spark 4.0+')
def test_parquet_variant_try_get_null_and_missing_fields(spark_tmp_path):
    data_path = spark_tmp_path + '/VARIANT_NULL_FIELDS_PARQUET'

    def write_data(spark):
        spark.sql("""
          SELECT parse_json('{"x":null,"n":null,"scalar":5}') AS v
          UNION ALL
          SELECT parse_json('{"n":{"inner":"deep"},"scalar":{"inner":9}}') AS v
          UNION ALL
          SELECT parse_json('{"x":7}') AS v
        """).write.mode('overwrite').parquet(data_path)

    _with_cpu_variant_session(write_data)

    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.parquet(data_path).selectExpr(
            "try_variant_get(v, '$.x', 'int') AS x",
            "try_variant_get(v, '$.missing', 'string') AS missing",
            "try_variant_get(v, '$.n.inner', 'string') AS nested",
            "try_variant_get(v, '$.scalar.inner', 'int') AS scalar_nested"),
        conf=_variant_parquet_conf)


@incompat
@pytest.mark.skipif(is_before_spark_400(), reason='VariantType is available in Spark 4.0+')
def test_parquet_variant_try_get_integral_boundaries(spark_tmp_path):
    data_path = spark_tmp_path + '/VARIANT_INTEGRAL_BOUNDARIES_PARQUET'

    def write_data(spark):
        spark.sql("""
          SELECT parse_json(
            '{"bmin":-128,"bmax":127,"smin":-32768,"smax":32767,' ||
            '"imin":-2147483648,"imax":2147483647,' ||
            '"lmin":-9223372036854775808,"lmax":9223372036854775807,' ||
            '"byte_overflow":128,"short_overflow":32768,' ||
            '"int_overflow":2147483648,' ||
            '"byte_underflow":-129,"short_underflow":-32769,' ||
            '"int_underflow":-2147483649}') AS v
        """).write.mode('overwrite').parquet(data_path)

    _with_cpu_variant_session(write_data)

    assert_cpu_and_gpu_are_equal_collect_with_capture(
        lambda spark: spark.read.parquet(data_path).selectExpr(
            "try_variant_get(v, '$.bmin', 'tinyint') AS bmin",
            "try_variant_get(v, '$.bmin', 'int') AS bmin_as_int",
            "try_variant_get(v, '$.bmin', 'bigint') AS bmin_as_bigint",
            "try_variant_get(v, '$.bmax', 'tinyint') AS bmax",
            "try_variant_get(v, '$.smin', 'smallint') AS smin",
            "try_variant_get(v, '$.smin', 'int') AS smin_as_int",
            "try_variant_get(v, '$.smin', 'bigint') AS smin_as_bigint",
            "try_variant_get(v, '$.smax', 'smallint') AS smax",
            "try_variant_get(v, '$.imin', 'int') AS imin",
            "try_variant_get(v, '$.imin', 'bigint') AS imin_as_bigint",
            "try_variant_get(v, '$.imax', 'int') AS imax",
            "try_variant_get(v, '$.lmin', 'bigint') AS lmin",
            "try_variant_get(v, '$.lmax', 'bigint') AS lmax",
            "try_variant_get(v, '$.byte_overflow', 'tinyint') AS byte_overflow",
            "try_variant_get(v, '$.short_overflow', 'smallint') AS short_overflow",
            "try_variant_get(v, '$.int_overflow', 'int') AS int_overflow",
            "try_variant_get(v, '$.byte_underflow', 'tinyint') AS byte_underflow",
            "try_variant_get(v, '$.short_underflow', 'smallint') AS short_underflow",
            "try_variant_get(v, '$.int_underflow', 'int') AS int_underflow"),
        exist_classes='GpuVariantGet',
        conf=_variant_parquet_conf)


@incompat
@allow_non_gpu('Or', 'IsNull', 'GreaterThanOrEqual')
@pytest.mark.skipif(is_before_spark_400(), reason='VariantType is available in Spark 4.0+')
def test_parquet_variant_pass_through_filter_project(spark_tmp_path):
    data_path = spark_tmp_path + '/VARIANT_PASS_THROUGH_PARQUET'
    _with_cpu_variant_session(lambda spark: _write_variant_parquet(spark, data_path))

    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.parquet(data_path)
            .selectExpr("v", "try_variant_get(v, '$.x', 'int') AS x")
            .filter("x IS NULL OR x >= 7")
            .selectExpr(
                "try_variant_get(v, '$.n.inner', 'string') AS inner",
                "try_variant_get(v, '$.m', 'bigint') AS m")
            .orderBy("inner", "m"),
        conf=_variant_parquet_conf)


@incompat
@allow_non_gpu('And', 'IsNotNull', 'GreaterThan')
@pytest.mark.skipif(is_before_spark_400(), reason='VariantType is available in Spark 4.0+')
def test_parquet_variant_try_get_direct_filter(spark_tmp_path):
    data_path = spark_tmp_path + '/VARIANT_FILTER_PARQUET'
    _with_cpu_variant_session(lambda spark: _write_variant_parquet(spark, data_path))

    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.parquet(data_path)
            .filter("try_variant_get(v, '$.x', 'int') > 10")
            .selectExpr("try_variant_get(v, '$.x', 'int') AS x")
            .orderBy("x"),
        conf=_variant_parquet_conf)


@incompat
@allow_non_gpu('HashAggregateExec', 'ShuffleExchangeExec')
@pytest.mark.skipif(is_before_spark_400(), reason='VariantType is available in Spark 4.0+')
def test_parquet_variant_try_get_aggregate(spark_tmp_path):
    data_path = spark_tmp_path + '/VARIANT_AGGREGATE_PARQUET'
    _with_cpu_variant_session(lambda spark: _write_variant_parquet(spark, data_path))

    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.parquet(data_path).selectExpr(
            "sum(try_variant_get(v, '$.x', 'int')) AS total"),
        conf=_variant_parquet_conf)


@incompat
@ignore_order(local=True)
@pytest.mark.skipif(is_before_spark_400(), reason='VariantType is available in Spark 4.0+')
def test_parquet_variant_try_get_heterogeneous_values(spark_tmp_path):
    data_path = spark_tmp_path + '/VARIANT_HETEROGENEOUS_PARQUET'
    _with_cpu_variant_session(lambda spark: _write_heterogeneous_variant_parquet(spark, data_path))

    assert_cpu_and_gpu_are_equal_collect_with_capture(
        lambda spark: spark.read.parquet(data_path).selectExpr(
            "id",
            "try_variant_get(v, '$.x', 'tinyint') AS byte_value",
            "try_variant_get(v, '$.x', 'smallint') AS short_value",
            "try_variant_get(v, '$.x', 'int') AS int_value",
            "try_variant_get(v, '$.x', 'bigint') AS long_value",
            "try_variant_get(v, '$.x', 'boolean') AS boolean_value",
            "try_variant_get(v, '$.x', 'float') AS float_value",
            "try_variant_get(v, '$.x', 'double') AS double_value",
            "try_variant_get(v, '$.x', 'string') AS string_value"),
        exist_classes='GpuVariantGet',
        conf=_variant_parquet_conf)


@incompat
@pytest.mark.skipif(is_before_spark_400(), reason='VariantType is available in Spark 4.0+')
def test_parquet_variant_try_get_before_shuffle(spark_tmp_path):
    data_path = spark_tmp_path + '/VARIANT_SHUFFLE_PARQUET'
    _with_cpu_variant_session(lambda spark: _write_variant_parquet(spark, data_path))

    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.parquet(data_path).selectExpr(
            "try_variant_get(v, '$.x', 'int') AS x",
            "try_variant_get(v, '$.n.inner', 'string') AS inner").repartition(2)
            .orderBy("x", "inner"),
        conf=_variant_parquet_conf)


@incompat
@pytest.mark.skipif(is_before_spark_400(), reason='VariantType is available in Spark 4.0+')
def test_parquet_variant_if(spark_tmp_path):
    data_path = spark_tmp_path + '/VARIANT_IF_PARQUET'
    _with_cpu_variant_session(lambda spark: _write_variant_if_parquet(spark, data_path))

    assert_cpu_and_gpu_are_equal_collect_with_capture(
        lambda spark: spark.read.parquet(data_path)
            .selectExpr("id", "IF(id % 2 = 0, left_v, right_v) AS selected_v")
            .selectExpr(
                "id",
                "try_variant_get(selected_v, '$.x', 'int') AS x")
            .orderBy("id"),
        exist_classes='GpuIf',
        conf=_variant_parquet_conf)


@incompat
@pytest.mark.parametrize('v1_enabled_list', ['parquet', ''], ids=['v1', 'v2'])
@pytest.mark.skipif(is_before_spark_400(), reason='VariantType is available in Spark 4.0+')
def test_parquet_variant_try_get_array_paths(spark_tmp_path, v1_enabled_list):
    data_path = spark_tmp_path + '/VARIANT_ARRAY_PATH_PARQUET'
    read_conf = dict(_variant_parquet_conf)
    read_conf['spark.sql.sources.useV1SourceList'] = v1_enabled_list
    _with_cpu_variant_session(lambda spark: _write_array_variant_parquet(spark, data_path))

    assert_cpu_and_gpu_are_equal_collect_with_capture(
        lambda spark: spark.read.parquet(data_path).selectExpr(
            'id',
            "try_variant_get(v, '$.items[0].sku', 'string') AS first_sku",
            "try_variant_get(v, '$.items[1].qty', 'bigint') AS second_qty",
            "try_variant_get(v, '$.matrix[0][1]', 'int') AS matrix_value",
            "try_variant_get(v, '$[0].sku', 'string') AS root_sku",
            "try_variant_get(v, '$.items[99].sku', 'string') AS out_of_bounds",
            "try_variant_get(v, '$.items[0][0]', 'int') AS wrong_container")
            .orderBy('id'),
        exist_classes='GpuVariantGet',
        conf=read_conf)


@incompat
@pytest.mark.parametrize('v1_enabled_list', ['parquet', ''], ids=['v1', 'v2'])
@pytest.mark.skipif(is_before_spark_400(), reason='VariantType is available in Spark 4.0+')
def test_parquet_variant_try_get_common_scalar_targets(spark_tmp_path, v1_enabled_list):
    data_path = spark_tmp_path + '/VARIANT_COMMON_SCALAR_PARQUET'
    read_conf = dict(_variant_parquet_conf)
    read_conf['spark.sql.sources.useV1SourceList'] = v1_enabled_list
    _with_cpu_variant_session(
        lambda spark: _write_common_scalar_variant_parquet(spark, data_path))

    def do_it(spark):
        return (spark.read.parquet(data_path).selectExpr(
            'id',
            "try_variant_get(v, '$.bool_value', 'boolean') AS bool_value",
            "try_variant_get(v, '$.float_value', 'float') AS float_value",
            "try_variant_get(v, '$.double_value', 'double') AS double_value",
            "try_variant_get(v, '$.float_value', 'double') AS float_as_double",
            "try_variant_get(v, '$.double_value', 'float') AS double_as_float")
            .orderBy('id'))

    assert_cpu_and_gpu_are_equal_collect_with_capture(
        do_it,
        exist_classes='GpuVariantGet',
        conf=read_conf)

    (from_cpu, _), (from_gpu, _) = run_with_cpu_and_gpu(
        do_it, 'COLLECT_WITH_DATAFRAME', conf=read_conf)
    assert_equal_with_signed_zero(from_cpu, from_gpu)


@allow_non_gpu('ProjectExec', 'VariantGet')
@incompat
@pytest.mark.skipif(is_before_spark_400(), reason='VariantType is available in Spark 4.0+')
def test_variant_try_get_non_literal_path_falls_back(spark_tmp_path):
    data_path = spark_tmp_path + '/VARIANT_NON_LITERAL_PATH_FALLBACK_PARQUET'
    _with_cpu_variant_session(lambda spark: _write_variant_with_path_parquet(spark, data_path))

    def do_it(spark):
        return spark.read.parquet(data_path).selectExpr(
            "try_variant_get(v, path, 'int') AS x")

    assert_gpu_fallback_collect(do_it, 'VariantGet', conf=_variant_parquet_conf)


@allow_non_gpu('ProjectExec', 'VariantGet')
@incompat
@pytest.mark.skipif(is_before_spark_400(), reason='VariantType is available in Spark 4.0+')
def test_variant_try_get_quoted_path_falls_back(spark_tmp_path):
    data_path = spark_tmp_path + '/VARIANT_QUOTED_PATH_FALLBACK_PARQUET'
    _with_cpu_variant_session(lambda spark: _write_variant_parquet(spark, data_path))

    def do_it(spark):
        return spark.read.parquet(data_path).selectExpr(
            "try_variant_get(v, '$[\"x\"]', 'int') AS x")

    assert_gpu_fallback_collect(do_it, 'VariantGet', conf=_variant_parquet_conf)


@allow_non_gpu('ProjectExec', 'VariantGet')
@incompat
@pytest.mark.skipif(is_before_spark_400(), reason='VariantType is available in Spark 4.0+')
def test_variant_get_strict_mode_falls_back(spark_tmp_path):
    data_path = spark_tmp_path + '/VARIANT_STRICT_MODE_FALLBACK_PARQUET'
    _with_cpu_variant_session(lambda spark: _write_variant_parquet(spark, data_path))

    def do_it(spark):
        return spark.read.parquet(data_path).selectExpr(
            "variant_get(v, '$.x', 'int') AS x")

    assert_gpu_fallback_collect(do_it, 'VariantGet', conf=_variant_parquet_conf)


@allow_non_gpu('ProjectExec', 'VariantGet')
@incompat
@pytest.mark.skipif(is_before_spark_400(), reason='VariantType is available in Spark 4.0+')
def test_variant_try_get_unsupported_target_type_falls_back(spark_tmp_path):
    data_path = spark_tmp_path + '/VARIANT_TARGET_TYPE_FALLBACK_PARQUET'
    _with_cpu_variant_session(lambda spark: _write_variant_parquet(spark, data_path))

    def do_it(spark):
        return spark.read.parquet(data_path).selectExpr(
            "try_variant_get(v, '$.x', 'date') AS date_value")

    assert_gpu_fallback_collect(do_it, 'VariantGet', conf=_variant_parquet_conf)


@allow_non_gpu('ProjectExec', 'VariantGet')
@incompat
@pytest.mark.skipif(is_before_spark_400(), reason='VariantType is available in Spark 4.0+')
def test_variant_try_get_cpu_bridge_disabled_falls_back(spark_tmp_path):
    data_path = spark_tmp_path + '/VARIANT_CPU_BRIDGE_DISABLED_FALLBACK_PARQUET'
    _with_cpu_variant_session(lambda spark: _write_variant_parquet(spark, data_path))

    def do_it(spark):
        return spark.read.parquet(data_path).selectExpr(
            "try_variant_get(v, '$.x', 'int') AS x")

    conf = dict(_variant_parquet_conf)
    conf['spark.rapids.sql.expression.cpuBridge.enabled'] = 'false'
    assert_gpu_fallback_collect(do_it, 'VariantGet', conf=conf)


# Deliberately cover both physical output-width boundaries and the guard-scale boundary.
# The source decimals include enough significant digits to exercise decimal4/8/16 encodings;
# raw-width and internal no-bridge assertions live in GpuVariantDecimalSuite.
_decimal_target_cases = [
    pytest.param(1, 0, 9, 2, ['0.49', '0.50', '-0.50', '9.50'], id='p1-s0'),
    pytest.param(9, 2, 9, 3, ['123456.789', '9.995', '-9.995', '999999.999'], id='p9-s2'),
    pytest.param(10, 1, 18, 2,
                 ['1234567890123456.78', '9.95', '-9.95', '999999999.95'], id='p10-s1'),
    pytest.param(18, 0, 18, 2,
                 ['1234567890123456.78', '0.50', '-0.50', '9999999999999999.99'], id='p18-s0'),
    pytest.param(19, 18, 38, 20,
                 ['0.12345678901234567895', '-0.12345678901234567895',
                  '9.99999999999999999995'], id='p19-s18'),
    pytest.param(38, 37, 38, 38,
                 ['0.' + '9' * 38, '-0.' + '9' * 38, '0.' + '0' * 37 + '5'], id='p38-s37'),
    pytest.param(38, 38, 38, 38,
                 ['0.' + '9' * 38, '-0.' + '9' * 38, '0.' + '0' * 37 + '1'], id='p38-s38'),
]


@incompat
@pytest.mark.parametrize('v1_enabled_list', ['parquet', ''], ids=['v1', 'v2'])
@pytest.mark.parametrize('precision,scale,source_precision,source_scale,values', _decimal_target_cases)
@pytest.mark.skipif(is_before_spark_400(), reason='VariantType is available in Spark 4.0+')
def test_parquet_variant_try_get_decimal_targets(
        spark_tmp_path, v1_enabled_list, precision, scale, source_precision, source_scale, values):
    from pyspark.sql.types import DecimalType

    data_path = spark_tmp_path + '/VARIANT_DECIMAL_TARGET_PARQUET'
    literals = ','.join(f"({i}, '{value}')" for i, value in enumerate(values))

    def write(spark):
        spark.sql(f"""
          SELECT id, to_variant_object(named_struct(
            'x', CAST(value AS DECIMAL({source_precision},{source_scale})))) AS v
          FROM VALUES {literals} AS source(id, value)
          UNION ALL SELECT {len(values)}, CAST(NULL AS VARIANT)
        """).write.mode('overwrite').parquet(data_path)

    _with_cpu_variant_session(write)

    def do_it(spark):
        df = spark.read.parquet(data_path).selectExpr(
            'id', f"try_variant_get(v, '$.x', 'decimal({precision},{scale})') AS x")
        assert df.schema['x'].dataType == DecimalType(precision, scale)
        return df.orderBy('id')

    conf = dict(_variant_parquet_conf)
    conf['spark.sql.sources.useV1SourceList'] = v1_enabled_list
    assert_cpu_and_gpu_are_equal_collect_with_capture(
        do_it, exist_classes='GpuProjectExec,GpuVariantGet', conf=conf, require_non_empty=True)


@incompat
@pytest.mark.parametrize('v1_enabled_list', ['parquet', ''], ids=['v1', 'v2'])
@pytest.mark.skipif(is_before_spark_400(), reason='VariantType is available in Spark 4.0+')
def test_parquet_variant_try_get_decimal_mixed_nested_coercions(spark_tmp_path, v1_enabled_list):
    from pyspark.sql.types import DecimalType

    data_path = spark_tmp_path + '/VARIANT_DECIMAL_MIXED_PARQUET'

    def write(spark):
        spark.sql("""
          SELECT id, parse_json(json) AS v FROM VALUES
            (0, '{"items":[{"x":9.995}]}'),
            (1, '{"items":[{"x":-9.995}]}'),
            (2, '{"items":[{"x":"12.345"}]}'),
            (3, '{"items":[{"x":12}]}'),
            (4, '{"items":[{"x":true}]}'),
            (5, '{"items":[{"x":"bad"}]}'),
            (6, '{"items":[{"x":null}]}'),
            (7, '{"items":[{}]}'),
            (8, '{"items":[]}'),
            (9, '{"items":[{"x":1e100}]}'),
            (10, 'null'), (11, NULL)
          AS source(id, json)
        """).write.mode('overwrite').parquet(data_path)

    _with_cpu_variant_session(write)

    def do_it(spark):
        df = spark.read.parquet(data_path).selectExpr(
            'id', "try_variant_get(v, '$.items[0].x', 'decimal(3,2)') AS narrow",
            "try_variant_get(v, '$.items[0].x', 'decimal(19,2)') AS wide")
        assert df.schema['narrow'].dataType == DecimalType(3, 2)
        assert df.schema['wide'].dataType == DecimalType(19, 2)
        return df.orderBy('id')

    conf = dict(_variant_parquet_conf)
    conf['spark.sql.sources.useV1SourceList'] = v1_enabled_list
    assert_cpu_and_gpu_are_equal_collect_with_capture(
        do_it, exist_classes='GpuProjectExec,GpuVariantGet', conf=conf, require_non_empty=True)


def _decimal_array_bytes(width, scale, coefficient):
    # Primitive decimal4/8/16 headers, followed by unsigned scale and little-endian coefficient.
    payload = bytes([{4: 32, 8: 36, 16: 40}[width], scale]) + coefficient.to_bytes(
        width, byteorder='little', signed=True)
    return bytes([3, 1, 0, len(payload)]) + payload


@incompat
@allow_non_gpu('RDDScanExec')
@pytest.mark.parametrize('scale', [2, 38])
@pytest.mark.skipif(is_before_spark_400(), reason='VariantType is available in Spark 4.0+')
def test_variant_try_get_decimal_encoded_widths(scale):
    from pyspark.sql.types import DecimalType, VariantVal

    # Explicit bytes guarantee each source width independently of Spark builder minimization.
    values = [(4, 9, 123456789), (8, 18, 123456789012345678),
              (16, 38, 10 ** 38 - 1), (16, 38, -(10 ** 38 - 1)), (16, 37, 10 ** 36)]
    rows = [(i, VariantVal(_decimal_array_bytes(*value), b'\x01\x00\x00'))
            for i, value in enumerate(values)] + [(len(values), None)]

    def do_it(spark):
        df = spark.createDataFrame(rows, 'id int, v variant').selectExpr(
            'id', f"try_variant_get(v, '$[0]', 'decimal(38,{scale})') AS x")
        assert df.schema['x'].dataType == DecimalType(38, scale)
        return df.orderBy('id')

    assert_cpu_and_gpu_are_equal_collect_with_capture(
        do_it, exist_classes='GpuRowToColumnarExec,GpuProjectExec,GpuVariantGet',
        conf={'spark.sql.adaptive.enabled': 'false'}, require_non_empty=True)


@incompat
@allow_non_gpu('RDDScanExec')
@validate_execs_in_gpu_plan('GpuProjectExec')
@pytest.mark.parametrize('value', [
    pytest.param(_decimal_array_bytes(4, 9, 10 ** 9), id='decimal4-invalid-precision'),
    pytest.param(_decimal_array_bytes(4, 9, -(10 ** 9)), id='decimal4-negative-invalid-precision'),
    pytest.param(_decimal_array_bytes(8, 18, 10 ** 18), id='decimal8-invalid-precision'),
    pytest.param(_decimal_array_bytes(16, 38, 10 ** 38), id='decimal16-invalid-precision'),
    pytest.param(_decimal_array_bytes(4, 10, 0), id='decimal4-invalid-scale-zero'),
    pytest.param(_decimal_array_bytes(8, 19, 0), id='decimal8-invalid-scale-zero'),
    pytest.param(_decimal_array_bytes(16, 255, 0), id='unsigned-invalid-scale'),
    pytest.param(bytes([3, 1, 0, 2, 32, 0]), id='truncated-decimal-body'),
])
@pytest.mark.skipif(is_before_spark_400(), reason='VariantType is available in Spark 4.0+')
def test_variant_try_get_malformed_decimal_preserves_error(value):
    from pyspark.sql.types import VariantVal

    def do_it(spark, require_gpu_variant=False):
        # RDDScanExec is required only to inject bytes that a Parquet writer may validate.
        df = spark.createDataFrame(
            [(VariantVal(value, b'\x01\x00\x00'),)], 'v variant').selectExpr(
                "try_variant_get(v, '$[0]', 'decimal(9,0)') AS x")
        if require_gpu_variant:
            # Check expression selection before execution raises the expected malformed error.
            spark_jvm().org.apache.spark.sql.rapids.ExecutionPlanCaptureCallback.assertContains(
                df._jdf, 'GpuVariantGet')
        return df.collect()

    conf = {'spark.sql.adaptive.enabled': 'false',
            'spark.rapids.sql.incompatibleOps.enabled': 'true'}
    assert_spark_exception(lambda: with_cpu_session(do_it, conf=conf), 'MALFORMED_VARIANT')
    assert_spark_exception(
        lambda: with_gpu_session(lambda spark: do_it(spark, True), conf=conf), 'MALFORMED_VARIANT')


@incompat
@pytest.mark.parametrize('v1_enabled_list', ['parquet', ''], ids=['v1', 'v2'])
@pytest.mark.skipif(is_before_spark_400(), reason='VariantType is available in Spark 4.0+')
def test_parquet_variant_decimal_and_legacy_scalar_projection(spark_tmp_path, v1_enabled_list):
    data_path = spark_tmp_path + '/VARIANT_DECIMAL_AND_SCALARS_PARQUET'

    def write(spark):
        spark.sql("""
          SELECT id, to_variant_object(named_struct(
            'd', CAST(value AS DECIMAL(9,3)), 'i', id, 'b', id = 0,
            'f', CAST(id + 0.25 AS FLOAT), 'g', CAST(id - 0.5 AS DOUBLE),
            's', CAST(id AS STRING))) AS v
          FROM VALUES (0, '1.255'), (1, '-9.995') AS source(id, value)
          UNION ALL SELECT 2, parse_json(
            '{"d":"12.345","i":"42","b":true,"f":1.25,"g":-2.5,"s":"mixed"}')
          UNION ALL SELECT 3, parse_json('{"d":null}')
          UNION ALL SELECT 4, parse_json('{}')
          UNION ALL SELECT 5, CAST(NULL AS VARIANT)
        """).write.mode('overwrite').parquet(data_path)

    _with_cpu_variant_session(write)

    def do_it(spark):
        return spark.read.parquet(data_path).selectExpr(
            'id', "try_variant_get(v, '$.d', 'decimal(9,2)') AS d",
            "try_variant_get(v, '$.i', 'tinyint') AS byte_value",
            "try_variant_get(v, '$.i', 'smallint') AS short_value",
            "try_variant_get(v, '$.i', 'int') AS int_value",
            "try_variant_get(v, '$.i', 'bigint') AS long_value",
            "try_variant_get(v, '$.b', 'boolean') AS b",
            "try_variant_get(v, '$.f', 'float') AS f",
            "try_variant_get(v, '$.g', 'double') AS g",
            "try_variant_get(v, '$.s', 'string') AS s").orderBy('id')

    conf = dict(_variant_parquet_conf)
    conf['spark.sql.sources.useV1SourceList'] = v1_enabled_list
    assert_cpu_and_gpu_are_equal_collect_with_capture(
        do_it, exist_classes='GpuProjectExec,GpuVariantGet', conf=conf, require_non_empty=True)
