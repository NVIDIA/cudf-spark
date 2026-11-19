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

from asserts import assert_cpu_and_gpu_are_equal_collect_with_capture, \
    assert_gpu_and_cpu_are_equal_collect, assert_spark_exception
from conftest import is_iceberg_remote_catalog
from data_gen import copy_and_update
from iceberg import get_full_table_name, iceberg_unsupported_mark, rapids_reader_types, \
    supports_iceberg_v3, ICEBERG_V3_UNSUPPORTED_REASON, iceberg_variant_read_conf
from marks import allow_non_gpu, iceberg, ignore_order, incompat
from spark_session import is_spark_411_or_later, with_cpu_session, with_gpu_session

pytestmark = [iceberg_unsupported_mark, iceberg]

requires_variant = pytest.mark.skipif(
    not supports_iceberg_v3 or not is_spark_411_or_later(),
    reason=ICEBERG_V3_UNSUPPORTED_REASON if not supports_iceberg_v3 else
    "Iceberg Variant requires Spark 4.1.1+")

_UNSHREDDED_PROPERTIES = "'format-version'='3', 'write.parquet.shred-variants'='false'"
_VARIANT_CONTAINERS = {
    'top': ('VARIANT', 'parsed', '{column}'),
    'struct': ('STRUCT<leaf: VARIANT>',
               "IF(id = 6, NULL, named_struct('leaf', parsed))", '{column}.leaf'),
    'array': ('ARRAY<VARIANT>',
              'IF(id = 6, NULL, IF(id = 8, array(), array(parsed, parsed)))',
              'get({column}, 0)'),
    'map': ('MAP<STRING, VARIANT>',
            "IF(id = 6, NULL, IF(id = 8, map(), map('key', parsed, 'other', parsed)))",
            "{column}['key']"),
    'mixed': ('STRUCT<items: ARRAY<MAP<STRING, STRUCT<leaf: VARIANT>>>>',
              "named_struct('items', array(map('key', named_struct('leaf', parsed))))",
              "get({column}.items, 0)['key'].leaf"),
}


def _setup_unshredded_variant_table(spark, table, container):
    column_type, expression, _ = _VARIANT_CONTAINERS[container]
    spark.sql(f"CREATE TABLE {table} (id INT, v {column_type}) USING ICEBERG "
              f"TBLPROPERTIES ({_UNSHREDDED_PROPERTIES})")
    spark.sql(f"""
        INSERT INTO {table}
        SELECT /*+ COALESCE(1) */ id, {expression}
        FROM (
          SELECT id, parse_json(json) AS parsed
          FROM VALUES (0, '{{"x":7}}'), (1, '{{"x":9}}'), (2, '"scalar"'),
            (3, '3.5'), (4, 'true'), (5, 'null'), (6, NULL), (7, '{{}}'), (8, '[]')
          AS source(id, json)
        )
        """)


def _import_parquet_file(spark, table, data_path, row_count):
    jvm = spark._jvm
    iceberg_table = jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable(
        spark._jsparkSession, table)
    mapping = jvm.org.apache.iceberg.mapping.MappingUtil.create(iceberg_table.schema())
    mapping_json = jvm.org.apache.iceberg.mapping.NameMappingParser.toJson(mapping)
    iceberg_table.updateProperties().set('schema.name-mapping.default', mapping_json).commit()
    path = jvm.org.apache.hadoop.fs.Path(data_path + '/*.parquet')
    files = path.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration()).globStatus(path)
    assert len(files) == 1, "Expected one imported Parquet file"
    file = files[0]
    data_file = (jvm.org.apache.iceberg.DataFiles.builder(iceberg_table.spec())
                 .withPath(file.getPath().toString()).withFormat('PARQUET')
                 .withFileSizeInBytes(file.getLen()).withRecordCount(row_count).build())
    iceberg_table.newAppend().appendFile(data_file).commit()
    spark.sql(f"REFRESH TABLE {table}")


def _read_variant(spark, table, container, column='v'):
    path = _VARIANT_CONTAINERS[container][2].format(column=column)
    return spark.sql(f"SELECT id, try_variant_get({path}, '$.x', 'int') AS x FROM {table}")


@ignore_order(local=True)
@incompat
@requires_variant
@pytest.mark.parametrize('reader_type', rapids_reader_types)
@pytest.mark.parametrize('container', list(_VARIANT_CONTAINERS))
def test_iceberg_unshredded_variant_read(spark_tmp_table_factory, reader_type, container):
    table = get_full_table_name(spark_tmp_table_factory)
    conf = copy_and_update(iceberg_variant_read_conf, {
        'spark.rapids.sql.format.parquet.reader.type': reader_type,
    })
    with_cpu_session(
        lambda spark: _setup_unshredded_variant_table(spark, table, container), conf=conf)
    # Read the CPU-written data both before and after renaming the Variant-bearing column.
    for column in ['v', 'renamed_v']:
        if column == 'renamed_v':
            def rename_column(spark):
                spark.sql(f"ALTER TABLE {table} RENAME COLUMN v TO renamed_v")

            with_cpu_session(rename_column, conf=conf)
        assert_gpu_and_cpu_are_equal_collect(
            lambda spark: _read_variant(spark, table, container, column),
            conf=conf)


@ignore_order(local=True)
@incompat
@requires_variant
def test_iceberg_variant_scalar_and_null_read(spark_tmp_table_factory):
    table = get_full_table_name(spark_tmp_table_factory)
    with_cpu_session(
        lambda spark: _setup_unshredded_variant_table(spark, table, 'top'),
        conf=iceberg_variant_read_conf)

    def canonicalize(cpu, gpu):
        def values(rows):
            # JSON null must remain distinct from SQL NULL.
            return [(row.id, None if row.v is None else json.dumps(
                json.loads(row.v.toJson()), sort_keys=True, separators=(",", ":")))
                for row in rows]

        cpu_values, gpu_values = values(cpu), values(gpu)
        expected = {2: '"scalar"', 3: '3.5', 4: 'true', 5: 'null', 6: None, 7: '{}', 8: '[]'}
        assert {key: value for key, value in cpu_values if key in expected} == expected
        return cpu_values, gpu_values

    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.sql(f"SELECT id, v FROM {table}"),
        conf=iceberg_variant_read_conf, result_canonicalize_func_before_compare=canonicalize)


@ignore_order(local=True)
@incompat
@allow_non_gpu('BatchScanExec', 'ColumnarToRowExec', 'ProjectExec', 'VariantGet')
@requires_variant
@pytest.mark.parametrize('container', list(_VARIANT_CONTAINERS))
@pytest.mark.parametrize('allow_shredded', [None, 'false', 'true'], ids=['default', 'unshredded', 'shredded'])
def test_iceberg_variant_read_configuration(spark_tmp_table_factory, container, allow_shredded):
    table = get_full_table_name(spark_tmp_table_factory)
    with_cpu_session(
        lambda spark: _setup_unshredded_variant_table(spark, table, container),
        conf=iceberg_variant_read_conf)
    conf = dict(iceberg_variant_read_conf)
    conf.pop('spark.sql.variant.allowReadingShredded')
    if allow_shredded is not None:
        conf['spark.sql.variant.allowReadingShredded'] = allow_shredded
    enabled = allow_shredded == 'false'
    assert_cpu_and_gpu_are_equal_collect_with_capture(
        lambda spark: _read_variant(spark, table, container),
        exist_classes='GpuBatchScanExec,GpuVariantGet' if enabled else 'BatchScanExec',
        non_exist_classes='BatchScanExec' if enabled else 'GpuBatchScanExec,GpuVariantGet',
        conf=conf, require_non_empty=True)


@ignore_order(local=True)
@incompat
@requires_variant
@pytest.mark.parametrize('container', list(_VARIANT_CONTAINERS))
@pytest.mark.parametrize('allow_shredded', [None, 'false', 'true'], ids=['default', 'unshredded', 'shredded'])
def test_iceberg_non_variant_projection_configuration(
        spark_tmp_table_factory, container, allow_shredded):
    table = get_full_table_name(spark_tmp_table_factory)
    with_cpu_session(
        lambda spark: _setup_unshredded_variant_table(spark, table, container),
        conf=iceberg_variant_read_conf)
    conf = dict(iceberg_variant_read_conf)
    conf.pop('spark.sql.variant.allowReadingShredded')
    if allow_shredded is not None:
        conf['spark.sql.variant.allowReadingShredded'] = allow_shredded
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.sql(f"SELECT id FROM {table}"), conf=conf)


@incompat
@requires_variant
@pytest.mark.parametrize('reader_type', rapids_reader_types)
def test_iceberg_shredded_variant_read_and_count(spark_tmp_table_factory, reader_type):
    table = get_full_table_name(spark_tmp_table_factory)
    conf = copy_and_update(iceberg_variant_read_conf, {
        'spark.rapids.sql.format.parquet.reader.type': reader_type,
        'spark.rapids.sql.incompatibleOps.enabled': 'true',
    })

    def setup_table(spark):
        spark.sql(f"CREATE TABLE {table} (v VARIANT, id BIGINT) USING ICEBERG "
                  "TBLPROPERTIES ('format-version'='3', 'write.delete.mode'='merge-on-read', "
                  "'write.parquet.shred-variants'='true', "
                  "'write.parquet.variant-inference-buffer-size'='2')")
        spark.sql(f"""
            INSERT INTO {table}
            SELECT /*+ COALESCE(1) */
              IF(id = 3, NULL, parse_json(concat('{{\"x\":', id, '}}'))), id
            FROM range(6)
            """)
        spark.sql(f"DELETE FROM {table} WHERE id IN (1, 4)")

    with_cpu_session(setup_table, conf=conf)
    assert with_cpu_session(lambda spark: spark.sql(
        f"SELECT id, try_variant_get(v, '$.x', 'bigint') FROM {table} ORDER BY id").collect(),
        conf=conf) == [(0, 0), (2, 2), (3, None), (5, 5)]
    # Counting surviving rows must not require decoding the unrequested shredded Variant.
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.sql(f"SELECT count(*) FROM {table}"), conf=conf)
    for projection in ['v', "try_variant_get(v, '$.x', 'bigint')"]:
        assert_spark_exception(
            lambda: with_gpu_session(
                lambda spark: spark.sql(f"SELECT {projection} FROM {table}").collect(), conf=conf),
            'java.lang.UnsupportedOperationException: GPU Iceberg reader does not support '
            'shredded or incompatible Variant input')


@incompat
@requires_variant
@pytest.mark.parametrize('reader_type', rapids_reader_types)
@pytest.mark.parametrize('layout,column_type,expression,variant_path', [
    ('variant_first', 'VARIANT', 'parsed', 'v'),
    ('variant_only', 'VARIANT', 'parsed', 'v'),
    ('struct', 'STRUCT<child: VARIANT>', "IF(id = 3, NULL, named_struct('child', parsed))", 'v.child'),
    ('array', 'ARRAY<VARIANT>', 'IF(id = 0, array(), IF(id = 3, NULL, array(parsed)))', 'get(v, 0)'),
    ('map', 'MAP<STRING, VARIANT>', "IF(id = 0, map(), IF(id = 3, NULL, map('k', parsed)))", "v['k']"),
])
def test_iceberg_variant_deletion_vector_empty_projection(
        spark_tmp_table_factory, reader_type, layout, column_type, expression, variant_path):
    table = get_full_table_name(spark_tmp_table_factory)

    def setup_table(spark):
        extra_column = ', id BIGINT' if layout == 'variant_first' else ''
        extra_value = ', id' if layout == 'variant_first' else ''
        spark.sql(f"CREATE TABLE {table} (v {column_type}{extra_column}) USING ICEBERG "
                  f"TBLPROPERTIES ({_UNSHREDDED_PROPERTIES}, 'write.delete.mode'='merge-on-read')")
        spark.sql(f"""
            INSERT INTO {table}
            SELECT /*+ COALESCE(1) */ {expression}{extra_value}
            FROM (SELECT id,
              IF(id IN (3, 5), NULL, parse_json(concat('{{"x":', id, '}}'))) AS parsed
              FROM range(6))
            """)
        spark.sql(f"DELETE FROM {table} WHERE try_variant_get({variant_path}, '$.x', 'int') IN (1, 4)")
        assert spark.sql(f"SELECT count(*) FROM {table}").first()[0] == 4
        assert [row[0] for row in spark.sql(f"SELECT _pos FROM {table} ORDER BY _pos").collect()] \
            == [0, 2, 3, 5]

    conf = copy_and_update(iceberg_variant_read_conf, {
        'spark.rapids.sql.format.parquet.reader.type': reader_type,
    })
    with_cpu_session(setup_table, conf=conf)
    for projection in ['count(*)', '_pos, _row_id']:
        order = ' ORDER BY _pos' if projection.startswith('_pos') else ''
        assert_gpu_and_cpu_are_equal_collect(
            lambda spark: spark.sql(f"SELECT {projection} FROM {table}{order}"), conf=conf)


@ignore_order(local=True)
@pytest.mark.parametrize('reader_type,with_deletion_vector',
    [(reader, False) for reader in rapids_reader_types] + [
        pytest.param(reader, True, marks=pytest.mark.skipif(
            not supports_iceberg_v3, reason=ICEBERG_V3_UNSUPPORTED_REASON))
        for reader in rapids_reader_types])
def test_iceberg_renamed_case_sensitive_columns_read(
        spark_tmp_table_factory, reader_type, with_deletion_vector):
    table = get_full_table_name(spark_tmp_table_factory)

    def setup_table(spark):
        version = '3' if with_deletion_vector else '2'
        spark.sql(f"CREATE TABLE {table} (a INT, A INT) USING ICEBERG "
                  f"TBLPROPERTIES ('format-version'='{version}', "
                  "'write.delete.mode'='merge-on-read')")
        spark.sql(f"INSERT INTO {table} SELECT /*+ COALESCE(1) */ * "
                  "FROM VALUES (1, 10), (2, 20) AS source(a, b)")
        spark.sql(f"ALTER TABLE {table} RENAME COLUMN A TO b")
        if with_deletion_vector:
            spark.sql(f"DELETE FROM {table} WHERE a = 1")

    with_cpu_session(setup_table, conf={'spark.sql.caseSensitive': 'true'})
    # Iceberg resolves the old physical a/A names by ID before GPU schema normalization.
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.sql(f"SELECT a, b FROM {table}"),
        conf={
            'spark.sql.caseSensitive': 'false',
            'spark.sql.adaptive.enabled': 'false',
            'spark.rapids.sql.format.parquet.reader.type': reader_type,
            'spark.rapids.sql.format.iceberg.v3.enabled': 'true',
        })


@ignore_order(local=True)
@incompat
@pytest.mark.skipif(is_iceberg_remote_catalog(), reason="Imported files require a local catalog")
@requires_variant
@pytest.mark.parametrize('layout,container,reader_type', [
    pytest.param(layout, 'top', 'PERFILE', id=layout) for layout in [
        'unshredded', 'metadata_first', 'missing_value', 'extra_child', 'nullable_value',
        'nullable_metadata', 'wrong_value_type', 'wrong_metadata_type', 'primitive']
] + [
    (layout, container, reader) for layout in ['extra_child', 'primitive']
    for container in ['struct', 'array', 'map'] for reader in rapids_reader_types
])
def test_iceberg_variant_physical_layout_validation(
        spark_tmp_table_factory, spark_tmp_path, layout, container, reader_type):
    from pyspark.sql.types import ArrayType, BinaryType, IntegerType, MapType, StringType, \
        StructField, StructType

    table = get_full_table_name(spark_tmp_table_factory)
    conf = copy_and_update(iceberg_variant_read_conf, {
        'spark.sql.parquet.fieldId.write.enabled': 'false',
        'spark.rapids.sql.format.parquet.reader.type': reader_type,
        'spark.rapids.sql.incompatibleOps.enabled': 'true',
    })

    def setup_table(spark):
        variant = spark.sql("""SELECT parse_json('{"x":7}') AS v""").first().v
        fields = [StructField('value', BinaryType(), False),
                  StructField('metadata', BinaryType(), False)]
        values = [variant.value, variant.metadata]
        if layout == 'metadata_first':
            fields, values = fields[::-1], values[::-1]
        elif layout == 'missing_value':
            fields, values = fields[1:], values[1:]
        elif layout == 'extra_child':
            fields.append(StructField('extra', IntegerType(), False))
            values.append(1)
        elif layout == 'nullable_value':
            fields[0] = StructField('value', BinaryType(), True)
        elif layout == 'nullable_metadata':
            fields[1] = StructField('metadata', BinaryType(), True)
        elif layout == 'wrong_value_type':
            fields[0], values[0] = StructField('value', IntegerType(), False), 7
        elif layout == 'wrong_metadata_type':
            fields[1], values[1] = StructField('metadata', IntegerType(), False), 1
        physical_type = IntegerType() if layout == 'primitive' else StructType(fields)
        physical_value = 7 if layout == 'primitive' else tuple(values)
        physical_type, physical_value = {
            'top': (physical_type, physical_value),
            'struct': (StructType([StructField('leaf', physical_type)]), (physical_value,)),
            'array': (ArrayType(physical_type), [physical_value]),
            'map': (MapType(StringType(), physical_type), {'key': physical_value}),
        }[container]
        schema = StructType([StructField('id', IntegerType(), False),
                             StructField('v', physical_type, False)])
        data_path = spark_tmp_path + '/physical_variant'
        spark.createDataFrame([(1, physical_value)], schema).coalesce(1).write.parquet(data_path)
        spark.sql(f"CREATE TABLE {table} (id INT, v {_VARIANT_CONTAINERS[container][0]}) USING ICEBERG "
                  "TBLPROPERTIES ('format-version'='3')")
        _import_parquet_file(spark, table, data_path, 1)

    with_cpu_session(setup_table, conf=conf)
    if layout in ['unshredded', 'metadata_first']:
        assert_gpu_and_cpu_are_equal_collect(
            lambda spark: _read_variant(spark, table, container), conf=conf)
    else:
        assert_spark_exception(
            lambda: with_gpu_session(lambda spark: spark.table(table).collect(), conf=conf),
            'java.lang.UnsupportedOperationException: GPU Iceberg reader does not support')


@ignore_order(local=True)
@pytest.mark.parametrize('reader_type', rapids_reader_types)
@pytest.mark.parametrize('use_name_mapping', [False, True])
@pytest.mark.skipif(is_iceberg_remote_catalog(), reason="Imported files require a local catalog")
def test_iceberg_schema_evolution_read(
        spark_tmp_table_factory, reader_type, use_name_mapping):
    table = get_full_table_name(spark_tmp_table_factory)
    source = get_full_table_name(spark_tmp_table_factory) if use_name_mapping else table
    columns = ('id INT, Payload STRUCT<First: INT, Second: STRING>, '
               'Items ARRAY<STRUCT<X: INT, Y: STRING>>, '
               'Attrs MAP<STRING, STRUCT<X: INT, Y: STRING>>')

    def setup_table(spark):
        spark.sql(f"CREATE TABLE {table} (unused INT, {columns}) USING ICEBERG "
                  "TBLPROPERTIES ('format-version'='2')")
        spark.sql(f"ALTER TABLE {table} DROP COLUMN unused")
        if use_name_mapping:
            spark.sql(f"CREATE TABLE {source} ({columns}) USING PARQUET")
        spark.sql(f"""
            INSERT INTO {source} SELECT /*+ COALESCE(1) */ * FROM VALUES
              (1, named_struct('First', 11, 'Second', 'old-a'),
               array(named_struct('X', 101, 'Y', 'a')),
               map('k', named_struct('X', 1001, 'Y', 'ka'))),
              (2, NULL, array(NULL), map('k', NULL))
            """)
        if use_name_mapping:
            spark.sql(f"CALL spark_catalog.system.add_files("
                      f"table => '{table}', source_table => '{source}')")
        spark.sql(f"ALTER TABLE {table} RENAME COLUMN Payload.First TO renamed_first")
        spark.sql(f"ALTER TABLE {table} RENAME COLUMN Items.element.X TO renamed_x")
        spark.sql(f"ALTER TABLE {table} RENAME COLUMN Attrs.value.X TO renamed_x")
        spark.sql(f"ALTER TABLE {table} RENAME COLUMN Payload TO Record")
        spark.sql(f"ALTER TABLE {table} ADD COLUMN Record.extra BIGINT")
        spark.sql(f"ALTER TABLE {table} ALTER COLUMN Record FIRST")
        spark.sql(f"""
            INSERT INTO {table} (id, Record, Items, Attrs)
            SELECT 3, named_struct('renamed_first', 33, 'Second', 'new-c', 'extra', 300L),
              array(named_struct('renamed_x', 303, 'Y', 'c')),
              map('k', named_struct('renamed_x', 3003, 'Y', 'kc'))
            """)
        rows = spark.sql(f"SELECT id, Record.renamed_first, Items[0].renamed_x, "
                         f"Attrs['k'].renamed_x, Record.extra FROM {table}").collect()
        assert sorted(tuple(row) for row in rows) == [
            (1, 11, 101, 1001, None), (2, None, None, None, None), (3, 33, 303, 3003, 300)]

    with_cpu_session(setup_table, conf={
        'spark.sql.caseSensitive': 'true',
        'spark.sql.parquet.fieldId.write.enabled': 'false',
    })
    conf = {
        'spark.sql.caseSensitive': 'false',
        'spark.sql.adaptive.enabled': 'false',
        'spark.rapids.sql.format.parquet.reader.type': reader_type,
    }
    for query in [f"SELECT * FROM {table}",
                  f"SELECT ID, record.renamed_first, items[0].renamed_x, "
                  f"attrs['k'].renamed_x, record.extra FROM {table} WHERE ID <> 2"]:
        assert_gpu_and_cpu_are_equal_collect(lambda spark: spark.sql(query), conf=conf)
