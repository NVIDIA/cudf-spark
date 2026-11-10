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
    assert_gpu_and_cpu_are_equal_collect
from conftest import is_iceberg_remote_catalog
from data_gen import copy_and_update
from iceberg import get_full_table_name, iceberg_unsupported_mark, rapids_reader_types, \
    supports_iceberg_v3, ICEBERG_V3_UNSUPPORTED_REASON
from iceberg.iceberg_test import _ICEBERG_VARIANT_READ_CONF, _setup_iceberg_variant_table
from marks import allow_non_gpu, iceberg, ignore_order, incompat
from spark_session import is_spark_411_or_later, with_cpu_session, with_gpu_session

pytestmark = [iceberg_unsupported_mark, iceberg]


# Variant support needs both Iceberg v3 and Spark's Variant type. General read tests below
# still run on older runtimes.
requires_variant = pytest.mark.skipif(
    not supports_iceberg_v3 or not is_spark_411_or_later(),
    reason=ICEBERG_V3_UNSUPPORTED_REASON if not supports_iceberg_v3 else
    "Iceberg Variant requires Spark 4.1.1+")


def _load_iceberg_table(spark, table):
    return spark._jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable(
        spark._jsparkSession, table)


def _parquet_schema(spark, iceberg_table, path):
    jvm = spark._jvm
    reader = jvm.org.apache.iceberg.shaded.org.apache.parquet.hadoop.ParquetFileReader.open(
        jvm.org.apache.iceberg.parquet.GpuParquetIOAccess.file(
            iceberg_table.io().newInputFile(path)))
    try:
        return reader.getFileMetaData().getSchema()
    finally:
        reader.close()


def _import_parquet_file(spark, table, data_path, row_count, use_name_mapping):
    jvm = spark._jvm
    iceberg_table = _load_iceberg_table(spark, table)
    if use_name_mapping:
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
    return _parquet_schema(spark, iceberg_table, file.getPath().toString())


def _assert_variant_layout(schema, path, shredded, annotated):
    variant = schema
    for name in path:
        variant = variant.asGroupType().getType(name)
    variant = variant.asGroupType()
    assert (variant.getLogicalTypeAnnotation() is not None) == annotated
    assert variant.containsField('typed_value') == shredded


def _assert_deletion_vector(spark, table):
    delete_files = {(row.content, row.file_format) for row in
                    spark.sql(f"SELECT content, file_format FROM {table}.delete_files").collect()}
    assert delete_files == {(1, 'PUFFIN')}, "Fixture must contain a deletion vector"


def _collect_with_gpu_scan_check(spark, query, extract=False):
    df = spark.sql(query)
    if spark.conf.get('spark.rapids.sql.enabled') == 'true':
        callback = spark._jvm.org.apache.spark.sql.rapids.ExecutionPlanCaptureCallback
        callback.assertContains(df._jdf, 'GpuBatchScanExec')
        if extract:
            callback.assertContains(df._jdf, 'GpuVariantGet')
    return df.collect()


def _assert_variant_execution_error(read, conf):
    with pytest.raises(Exception) as error:
        with_gpu_session(read, conf=conf)
    message = str(error.value)
    assert 'java.lang.UnsupportedOperationException: GPU Iceberg reader does not support ' \
        'shredded or incompatible Variant input' in message
    assert 'Expected exactly two REQUIRED BINARY fields named value and metadata' in message
    assert 'spark.rapids.sql.format.iceberg.read.enabled=false' in message


def _assert_shredded_variant_reads(table, column, variant_path, conf):
    raw_query = f"SELECT id, {column} FROM {table}"
    extract_query = f"SELECT id, try_variant_get({variant_path}, '$.x', 'int') AS x FROM {table}"
    # Share setup, but check both raw reads and extraction through each reader independently.
    for query in [raw_query, extract_query]:
        def read(spark):
            return _collect_with_gpu_scan_check(spark, query, extract=query == extract_query)

        with_cpu_session(read, conf=conf)
        _assert_variant_execution_error(read, conf)

    # Check automatic CPU planning and explicit CPU-reader recovery on the same shredded input.
    for overrides, excluded in [
        ({'spark.sql.variant.allowReadingShredded': 'true'}, 'GpuBatchScanExec,GpuVariantGet'),
        ({'spark.rapids.sql.format.iceberg.read.enabled': 'false'}, 'GpuBatchScanExec'),
    ]:
        assert_cpu_and_gpu_are_equal_collect_with_capture(
            lambda spark: spark.sql(extract_query),
            exist_classes='BatchScanExec', non_exist_classes=excluded,
            conf=copy_and_update(conf, overrides), require_non_empty=True)
    # Pruning Variant must still allow GPU reads, even when a file contains shredded values.
    assert_cpu_and_gpu_are_equal_collect_with_capture(
        lambda spark: spark.sql(f"SELECT id FROM {table}"),
        exist_classes='GpuBatchScanExec', non_exist_classes='BatchScanExec',
        conf=conf, require_non_empty=True)


def _assert_variant_empty_projection(table, reader_type):
    conf = copy_and_update(_ICEBERG_VARIANT_READ_CONF, {
        'spark.rapids.sql.format.parquet.reader.type': reader_type,
    })
    # Delete files/row positions route COALESCING to a per-file reader. Cover that setting too.
    # Both queries must preserve native positions and discard the temporary physical carrier.
    for projection in ['count(*)', '_pos, _row_id']:
        order = ' ORDER BY _pos' if projection.startswith('_pos') else ''
        assert_cpu_and_gpu_are_equal_collect_with_capture(
            lambda spark: spark.sql(f"SELECT {projection} FROM {table}{order}"),
            exist_classes='GpuBatchScanExec', non_exist_classes='BatchScanExec', conf=conf,
            require_non_empty=True)


def _delete_variant_rows(spark, table, variant_path):
    spark.sql(f"DELETE FROM {table} WHERE try_variant_get({variant_path}, '$.x', 'int') IN (1, 4)")
    _assert_deletion_vector(spark, table)
    assert spark.sql(f"SELECT count(*) FROM {table}").first()[0] == 4
    assert [row[0] for row in spark.sql(f"SELECT _pos FROM {table} ORDER BY _pos")
            .collect()] == [0, 2, 3, 5]


_DV_VARIANT = "IF(id IN (3, 5), NULL, parse_json(concat('{\"x\":', id, '}')))"
_SHREDDED_VARIANT_FIELDS = ('STRUCT<metadata: BINARY, value: BINARY, '
                            'typed_value: STRUCT<x: STRUCT<value: BINARY, typed_value: BIGINT>>>')
_SHREDDED_WRITE_CONF = {
    'spark.sql.parquet.variant.annotateLogicalType.enabled': 'false',
    'spark.sql.parquet.fieldId.write.enabled': 'false',
    'spark.sql.variant.writeShredding.enabled': 'true',
    'spark.sql.variant.forceShreddingSchemaForTest': 'STRUCT<x: BIGINT>',
}
_DV_TABLE_PROPERTIES = ("'format-version'='3', 'write.delete.mode'='merge-on-read', "
                        "'write.parquet.shred-variants'='true', "
                        "'write.parquet.variant-inference-buffer-size'='2'")


@incompat
@requires_variant
@pytest.mark.parametrize('reader_type', rapids_reader_types)
@pytest.mark.parametrize('layout', ['variant_first', 'variant_only'])
def test_iceberg_shredded_variant_deletion_vector_empty_projection(
        spark_tmp_table_factory, reader_type, layout):
    table = get_full_table_name(spark_tmp_table_factory)

    def setup_table(spark):
        columns, values = ('v VARIANT, id BIGINT', f'{_DV_VARIANT}, id') if \
            layout == 'variant_first' else ('v VARIANT', _DV_VARIANT)
        spark.sql(f"CREATE TABLE {table} ({columns}) USING ICEBERG "
                  f"TBLPROPERTIES ({_DV_TABLE_PROPERTIES})")
        spark.sql(f"INSERT INTO {table} SELECT /*+ COALESCE(1) */ {values} FROM range(6)")
        files = spark.sql(f"SELECT file_path FROM {table}.files").collect()
        assert len(files) == 1
        schema = _parquet_schema(spark, _load_iceberg_table(spark, table), files[0].file_path)
        _assert_variant_layout(schema, ['v'], shredded=True, annotated=True)
        _delete_variant_rows(spark, table, 'v')

    with_cpu_session(setup_table, conf=_ICEBERG_VARIANT_READ_CONF)
    _assert_variant_empty_projection(table, reader_type)


@incompat
@requires_variant
@pytest.mark.skipif(is_iceberg_remote_catalog(), reason="Imported files require a local catalog")
@pytest.mark.parametrize('reader_type', rapids_reader_types)
@pytest.mark.parametrize('column_type,expression,variant_path,physical_path', [
    pytest.param('STRUCT<child: VARIANT>', "IF(id = 3, NULL, named_struct('child', parsed))",
                 'v.child', ['v', 'child'], id='struct'),
    pytest.param('ARRAY<VARIANT>', 'IF(id = 0, array(), IF(id = 3, NULL, array(parsed)))',
                 'get(v, 0)', ['v', 'list', 'element'], id='array'),
    pytest.param('MAP<STRING, VARIANT>', "IF(id = 0, map(), IF(id = 3, NULL, map('k', parsed)))",
                 "v['k']", ['v', 'key_value', 'value'], id='map'),
])
def test_iceberg_nested_shredded_variant_deletion_vector_empty_projection(
        spark_tmp_table_factory, spark_tmp_path, reader_type,
        column_type, expression, variant_path, physical_path):
    table = get_full_table_name(spark_tmp_table_factory)

    def setup_table(spark):
        spark.sql(f"CREATE TABLE {table} (v {column_type}) USING ICEBERG "
                  f"TBLPROPERTIES ({_DV_TABLE_PROPERTIES})")
        # Iceberg does not shred nested leaves. Preserve a real shredded physical struct
        # while nesting it, then import the resulting file with name mapping.
        leaf_path, data_path = spark_tmp_path + '/leaf', spark_tmp_path + '/nested'
        spark.sql(f"SELECT id, {_DV_VARIANT} AS parsed FROM range(6)") \
            .coalesce(1).write.parquet(leaf_path)
        source = spark.read.schema(f'id BIGINT, parsed {_SHREDDED_VARIANT_FIELDS}').parquet(leaf_path)
        source.selectExpr(f'{expression} AS v').coalesce(1).write.parquet(data_path)
        schema = _import_parquet_file(spark, table, data_path, 6, use_name_mapping=True)
        assert not spark._jvm.org.apache.iceberg.parquet.ParquetSchemaUtil.hasIds(schema)
        _assert_variant_layout(schema, physical_path, shredded=True, annotated=False)
        _delete_variant_rows(spark, table, variant_path)

    with_cpu_session(setup_table, conf=copy_and_update(
        _ICEBERG_VARIANT_READ_CONF, _SHREDDED_WRITE_CONF))
    _assert_variant_empty_projection(table, reader_type)


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
            _assert_deletion_vector(spark, table)

    with_cpu_session(setup_table, conf={'spark.sql.caseSensitive': 'true'})
    # Iceberg resolves the old physical a/A names by ID before GPU schema normalization.
    assert_cpu_and_gpu_are_equal_collect_with_capture(
        lambda spark: spark.sql(f"SELECT a, b FROM {table}"),
        exist_classes='GpuBatchScanExec',
        non_exist_classes='BatchScanExec',
        conf={
            'spark.sql.caseSensitive': 'false',
            'spark.sql.adaptive.enabled': 'false',
            'spark.rapids.sql.format.parquet.reader.type': reader_type,
            'spark.rapids.sql.format.iceberg.v3.enabled': 'true',
        },
        require_non_empty=True)


@ignore_order(local=True)
@pytest.mark.parametrize('reader_type', rapids_reader_types)
@pytest.mark.parametrize('use_name_mapping', [False, True])
@pytest.mark.skipif(is_iceberg_remote_catalog(), reason="Imported files require a local catalog")
def test_iceberg_physical_schema_after_evolution(
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
            mapping = json.loads(spark.sql(
                f"SHOW TBLPROPERTIES {table} ('schema.name-mapping.default')").first()['value'])
            assert next(field['field-id'] for field in mapping if 'id' in field['names']) == 2
        jvm = spark._jvm
        iceberg_table = _load_iceberg_table(spark, table)
        assert iceberg_table.schema().findField('id').fieldId() == 2
        files = spark.sql(f"SELECT file_path FROM {table}.files").collect()
        assert len(files) == 1
        schema = _parquet_schema(spark, iceberg_table, files[0].file_path)
        assert jvm.org.apache.iceberg.parquet.ParquetSchemaUtil.hasIds(schema) == (not use_name_mapping)
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
        assert_cpu_and_gpu_are_equal_collect_with_capture(
            lambda spark: spark.sql(query), exist_classes='GpuBatchScanExec',
            non_exist_classes='BatchScanExec', conf=conf, require_non_empty=True)


_IMPORTED_VARIANT_CONTAINERS = {
    'top': ('VARIANT', 'parsed', '', []),
    'struct': ('STRUCT<leaf: VARIANT>', "named_struct('leaf', parsed)", '.leaf', ['leaf']),
    'array': ('ARRAY<VARIANT>', 'array(parsed)', '[0]', ['list', 'element']),
    'map': ('MAP<STRING, VARIANT>', "map('key', parsed)", "['key']", ['key_value', 'value']),
    'mixed': ('STRUCT<items: ARRAY<MAP<STRING, STRUCT<leaf: VARIANT>>>>',
              "named_struct('items', array(map('key', named_struct('leaf', parsed))))",
              ".items[0]['key'].leaf", ['items', 'list', 'element', 'key_value', 'value', 'leaf']),
}


def _setup_imported_variant_table(
        spark, table, data_path, container, variant_column, use_name_mapping, shredded):
    column_type, expression, _, physical_path = _IMPORTED_VARIANT_CONTAINERS[container]
    # Shift table IDs when using name mapping, so positional fallback IDs cannot satisfy the test.
    prefix = 'unused INT, ' if use_name_mapping else ''
    spark.sql(f"CREATE TABLE {table} ({prefix}id INT, v {column_type}) USING ICEBERG "
              "TBLPROPERTIES ('format-version'='3')")
    if use_name_mapping:
        spark.sql(f"ALTER TABLE {table} DROP COLUMN unused")
    source = spark.sql("""
        SELECT id, parse_json(json) AS parsed
        FROM VALUES (0, '{"x":7}'), (1, '{"x":9}'), (2, 'null'), (3, NULL)
        AS source(id, json)
        """)
    if shredded:
        # Preserve a real shredded payload when nesting it, including inside collections.
        # Read its physical struct to avoid depending on collection shredding support.
        shredded_path = data_path + '_shredded_leaf'
        source.coalesce(1).write.parquet(shredded_path)
        source = spark.read.schema(
            f'id INT, parsed {_SHREDDED_VARIANT_FIELDS}').parquet(shredded_path)
    source.selectExpr('id', f'{expression} AS v').coalesce(1).write.parquet(data_path)

    if use_name_mapping:
        assert _load_iceberg_table(spark, table).schema().findField('id').fieldId() == 2
    schema = _import_parquet_file(spark, table, data_path, 4, use_name_mapping)
    assert not spark._jvm.org.apache.iceberg.parquet.ParquetSchemaUtil.hasIds(schema)
    _assert_variant_layout(schema, ['v'] + physical_path, shredded=shredded, annotated=not shredded)
    if variant_column != 'v':
        spark.sql(f"ALTER TABLE {table} RENAME COLUMN v TO {variant_column}")
    rows = _read_imported_variant_table(spark, table, container, variant_column).collect()
    assert sorted(tuple(row) for row in rows) == [(0, 7), (1, 9), (2, None), (3, None)]


def _read_imported_variant_table(spark, table, container, variant_column):
    path = variant_column + _IMPORTED_VARIANT_CONTAINERS[container][2]
    return spark.sql(f"SELECT id, try_variant_get({path}, '$.x', 'int') AS x FROM {table}")


@ignore_order(local=True)
@incompat
@pytest.mark.skipif(is_iceberg_remote_catalog(), reason="Imported files require a local catalog")
@requires_variant
@pytest.mark.parametrize('container', list(_IMPORTED_VARIANT_CONTAINERS))
@pytest.mark.parametrize('variant_column', ['v', 'renamed_v'])
def test_iceberg_v3_imported_variant_read(
        spark_tmp_table_factory, spark_tmp_path, container, variant_column):
    table = get_full_table_name(spark_tmp_table_factory)
    conf = copy_and_update(_ICEBERG_VARIANT_READ_CONF, {
        'spark.sql.parquet.variant.annotateLogicalType.enabled': 'true',
        'spark.sql.parquet.fieldId.write.enabled': 'false',
    })
    with_cpu_session(lambda spark: _setup_imported_variant_table(
        spark, table, spark_tmp_path + '/variant', container, variant_column,
        use_name_mapping=container != 'top', shredded=False), conf=conf)
    assert_cpu_and_gpu_are_equal_collect_with_capture(
        lambda spark: _read_imported_variant_table(spark, table, container, variant_column),
        exist_classes='GpuBatchScanExec,GpuVariantGet',
        non_exist_classes='BatchScanExec,GpuRowToColumnarExec,HostColumnarToGpu',
        conf=conf, require_non_empty=True)


@ignore_order(local=True)
@incompat
@requires_variant
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


@ignore_order(local=True)
@incompat
@allow_non_gpu('BatchScanExec', 'ColumnarToRowExec', 'ProjectExec', 'VariantGet')
@pytest.mark.skipif(is_iceberg_remote_catalog(), reason="Imported files require a local catalog")
@requires_variant
@pytest.mark.parametrize('container', list(_IMPORTED_VARIANT_CONTAINERS))
@pytest.mark.parametrize('allow_shredded', [None, 'false', 'true'], ids=['default', 'unshredded', 'shredded'])
def test_iceberg_variant_shredded_read_configuration(
        spark_tmp_table_factory, spark_tmp_path, container, allow_shredded):
    table = get_full_table_name(spark_tmp_table_factory)
    conf = dict(_ICEBERG_VARIANT_READ_CONF)
    conf.pop('spark.sql.variant.allowReadingShredded')
    if allow_shredded is not None:
        conf['spark.sql.variant.allowReadingShredded'] = allow_shredded
    with_cpu_session(lambda spark: _setup_imported_variant_table(
        spark, table, spark_tmp_path + '/variant_read_config', container, 'v',
        use_name_mapping=True, shredded=False), conf=conf)
    enabled = allow_shredded == 'false'
    assert_cpu_and_gpu_are_equal_collect_with_capture(
        lambda spark: _read_imported_variant_table(spark, table, container, 'v'),
        exist_classes='GpuBatchScanExec,GpuVariantGet' if enabled else 'BatchScanExec',
        non_exist_classes='BatchScanExec' if enabled else 'GpuBatchScanExec,GpuVariantGet',
        conf=conf, require_non_empty=True)
    # The reader setting must not disable scans that prune every Variant column.
    assert_cpu_and_gpu_are_equal_collect_with_capture(
        lambda spark: spark.sql(f"SELECT id FROM {table}"),
        exist_classes='GpuBatchScanExec', non_exist_classes='BatchScanExec',
        conf=conf, require_non_empty=True)


@ignore_order(local=True)
@incompat
@requires_variant
@pytest.mark.skipif(is_iceberg_remote_catalog(), reason="Requires local data-file renames")
def test_iceberg_variant_planning_does_not_open_data_files(spark_tmp_table_factory):
    table = get_full_table_name(spark_tmp_table_factory)

    def setup_table(spark):
        _setup_iceberg_variant_table(spark, table)
        return [row.file_path for row in spark.sql(f"SELECT file_path FROM {table}.files").collect()]

    paths = with_cpu_session(setup_table, conf=_ICEBERG_VARIANT_READ_CONF)
    assert paths, "Expected Variant data files"

    def plan_without_files(spark):
        hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
        moved = []
        try:
            for path in paths:
                source = spark._jvm.org.apache.hadoop.fs.Path(path)
                hidden = spark._jvm.org.apache.hadoop.fs.Path(path + '.hidden_during_planning')
                fs = source.getFileSystem(hadoop_conf)
                assert fs.rename(source, hidden)
                moved.append((fs, source, hidden))
            df = spark.sql(f"SELECT id, try_variant_get(v, '$.x', 'int') AS x FROM {table}")
            # Materialize and cache the physical plan while no data file can be opened.
            # Restore files before the comparison helper executes that same plan.
            df._jdf.queryExecution().executedPlan()
        finally:
            for fs, source, hidden in reversed(moved):
                assert fs.rename(hidden, source)
        return df

    assert_cpu_and_gpu_are_equal_collect_with_capture(
        plan_without_files, exist_classes='GpuBatchScanExec,GpuVariantGet',
        non_exist_classes='BatchScanExec', conf=_ICEBERG_VARIANT_READ_CONF,
        require_non_empty=True)


@ignore_order(local=True)
@incompat
@allow_non_gpu('BatchScanExec', 'ColumnarToRowExec', 'ProjectExec', 'VariantGet')
@pytest.mark.skipif(is_iceberg_remote_catalog(), reason="Imported files require a local catalog")
@requires_variant
@pytest.mark.parametrize('reader_type', rapids_reader_types)
@pytest.mark.parametrize('container', list(_IMPORTED_VARIANT_CONTAINERS))
def test_iceberg_unannotated_shredded_variant_reads(
        spark_tmp_table_factory, spark_tmp_path, reader_type, container):
    table = get_full_table_name(spark_tmp_table_factory)
    conf = copy_and_update(_ICEBERG_VARIANT_READ_CONF, {
        'spark.rapids.sql.format.parquet.reader.type': reader_type,
        # Direct with_gpu_session calls do not apply the @incompat marker themselves.
        'spark.rapids.sql.incompatibleOps.enabled': 'true',
        **_SHREDDED_WRITE_CONF,
    })
    with_cpu_session(lambda spark: _setup_imported_variant_table(
        spark, table, spark_tmp_path + '/shredded_variant', container, 'renamed_v',
        use_name_mapping=True, shredded=True), conf=conf)

    path = 'renamed_v' + _IMPORTED_VARIANT_CONTAINERS[container][2]
    _assert_shredded_variant_reads(table, 'renamed_v', path, conf)


@ignore_order(local=True)
@incompat
@allow_non_gpu('BatchScanExec', 'ColumnarToRowExec', 'ProjectExec', 'VariantGet')
@requires_variant
@pytest.mark.parametrize('reader_type', rapids_reader_types)
def test_iceberg_mixed_shredded_variant_reads(
        spark_tmp_table_factory, reader_type):
    table = get_full_table_name(spark_tmp_table_factory)
    conf = copy_and_update(_ICEBERG_VARIANT_READ_CONF, {
        'spark.rapids.sql.format.parquet.reader.type': reader_type,
        # Direct with_gpu_session calls do not apply the @incompat marker themselves.
        'spark.rapids.sql.incompatibleOps.enabled': 'true',
    })

    def setup_table(spark):
        spark.sql(f"CREATE TABLE {table} (id INT, v VARIANT) USING ICEBERG "
                  "TBLPROPERTIES ('format-version'='3', 'write.parquet.shred-variants'='true', "
                  "'write.parquet.variant-inference-buffer-size'='2')")
        spark.sql(f"INSERT INTO {table} SELECT /*+ COALESCE(1) */ id, parse_json(json) "
                  "FROM VALUES (0, '{\"x\":7}'), (1, '{\"x\":8}'), (2, '{\"x\":9}') "
                  "AS source(id, json)")
        iceberg_table = _load_iceberg_table(spark, table)
        files = spark.sql(f"SELECT file_path FROM {table}.files").collect()
        assert files, "Expected shredded data files"
        for file in files:
            schema = _parquet_schema(spark, iceberg_table, file.file_path)
            _assert_variant_layout(schema, ['v'], shredded=True, annotated=True)
        # Include an old unshredded file to ensure every reader handles mixed input.
        spark.sql(f"ALTER TABLE {table} SET TBLPROPERTIES ('write.parquet.shred-variants'='false')")
        spark.sql(f"INSERT INTO {table} SELECT 3, parse_json('{{\"x\":10}}')")
        schemas = [_parquet_schema(spark, iceberg_table, row.file_path) for row in
                   spark.sql(f"SELECT file_path FROM {table}.files").collect()]
        assert sorted(schema.getType('v').asGroupType().containsField('typed_value')
                      for schema in schemas) == [False, True]

    with_cpu_session(setup_table, conf=conf)

    _assert_shredded_variant_reads(table, 'v', 'v', conf)


@ignore_order(local=True)
@incompat
@pytest.mark.skipif(is_iceberg_remote_catalog(), reason="Imported files require a local catalog")
@requires_variant
@pytest.mark.parametrize('layout', [
    'unshredded', 'metadata_first', 'missing_value', 'extra_child', 'nullable_value', 'nullable_metadata',
    'wrong_value_type', 'wrong_metadata_type', 'primitive',
])
def test_iceberg_variant_physical_layout_validation(spark_tmp_table_factory, spark_tmp_path, layout):
    from pyspark.sql.types import BinaryType, IntegerType, StructField, StructType

    table = get_full_table_name(spark_tmp_table_factory)
    conf = copy_and_update(_ICEBERG_VARIANT_READ_CONF, {
        'spark.sql.parquet.fieldId.write.enabled': 'false',
        'spark.rapids.sql.format.parquet.reader.type': 'PERFILE',
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
        schema = StructType([StructField('id', IntegerType(), False),
                             StructField('v', physical_type, False)])
        data_path = spark_tmp_path + '/physical_variant'
        spark.createDataFrame([(1, physical_value)], schema).coalesce(1).write.parquet(data_path)
        spark.sql(f"CREATE TABLE {table} (id INT, v VARIANT) USING ICEBERG "
                  "TBLPROPERTIES ('format-version'='3')")
        _import_parquet_file(spark, table, data_path, 1, use_name_mapping=True)

    with_cpu_session(setup_table, conf=conf)
    if layout in ['unshredded', 'metadata_first']:
        assert_cpu_and_gpu_are_equal_collect_with_capture(
            lambda spark: spark.sql(f"SELECT id, try_variant_get(v, '$.x', 'int') AS x FROM {table}"),
            exist_classes='GpuBatchScanExec,GpuVariantGet', non_exist_classes='BatchScanExec',
            conf=conf, require_non_empty=True)
    else:
        _assert_variant_execution_error(
            lambda spark: _collect_with_gpu_scan_check(spark, f"SELECT id, v FROM {table}"), conf)
