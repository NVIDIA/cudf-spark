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
from spark_session import is_spark_411_or_later, with_cpu_session

pytestmark = iceberg_unsupported_mark


@iceberg
@ignore_order(local=True)
@pytest.mark.parametrize('reader_type,with_deletion_vector',
    [(reader, False) for reader in rapids_reader_types] + [
        pytest.param(reader, True, marks=pytest.mark.skipif(
            not supports_iceberg_v3, reason=ICEBERG_V3_UNSUPPORTED_REASON))
        for reader in ['PERFILE', 'MULTITHREADED']])
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
            delete_files = {(row.content, row.file_format) for row in
                            spark.sql(f"SELECT content, file_format FROM {table}.delete_files")
                            .collect()}
            assert delete_files == {(1, 'PUFFIN')}, "Fixture must contain a deletion vector"

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


@iceberg
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
        iceberg_table = jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable(
            spark._jsparkSession, table)
        assert iceberg_table.schema().findField('id').fieldId() == 2
        files = spark.sql(f"SELECT file_path FROM {table}.files").collect()
        assert len(files) == 1
        input_file = iceberg_table.io().newInputFile(files[0].file_path)
        reader = jvm.org.apache.iceberg.shaded.org.apache.parquet.hadoop.ParquetFileReader.open(
            jvm.org.apache.iceberg.parquet.GpuParquetIOAccess.file(input_file))
        try:
            has_ids = jvm.org.apache.iceberg.parquet.ParquetSchemaUtil.hasIds(
                reader.getFileMetaData().getSchema())
            assert has_ids == (not use_name_mapping)
        finally:
            reader.close()
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
    spark.sql(f"""
        SELECT id, {expression} AS v FROM (
          SELECT id, parse_json(json) AS parsed
          FROM VALUES (0, '{{"x":7}}'), (1, '{{"x":9}}'), (2, 'null'), (3, NULL)
          AS source(id, json)) AS parsed_source
        """).coalesce(1).write.parquet(data_path)

    jvm = spark._jvm
    iceberg_table = jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable(
        spark._jsparkSession, table)
    if use_name_mapping:
        assert iceberg_table.schema().findField('id').fieldId() == 2
        mapping = jvm.org.apache.iceberg.mapping.MappingUtil.create(iceberg_table.schema())
        mapping_json = jvm.org.apache.iceberg.mapping.NameMappingParser.toJson(mapping)
        iceberg_table.updateProperties().set('schema.name-mapping.default', mapping_json).commit()
    path = jvm.org.apache.hadoop.fs.Path(data_path + '/*.parquet')
    files = path.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration()).globStatus(path)
    assert len(files) == 1, "Expected one imported Parquet file"
    file = files[0]
    input_file = iceberg_table.io().newInputFile(file.getPath().toString())
    reader = jvm.org.apache.iceberg.shaded.org.apache.parquet.hadoop.ParquetFileReader.open(
        jvm.org.apache.iceberg.parquet.GpuParquetIOAccess.file(input_file))
    try:
        file_schema = reader.getFileMetaData().getSchema()
        assert not jvm.org.apache.iceberg.parquet.ParquetSchemaUtil.hasIds(file_schema)
        variant = file_schema.getType('v').asGroupType()
        for name in physical_path:
            variant = variant.getType(name).asGroupType()
        assert (variant.getLogicalTypeAnnotation() is None) == shredded
        assert variant.containsField('typed_value') == shredded
    finally:
        reader.close()
    data_file = (jvm.org.apache.iceberg.DataFiles.builder(iceberg_table.spec())
                 .withPath(file.getPath().toString()).withFormat('PARQUET')
                 .withFileSizeInBytes(file.getLen()).withRecordCount(4).build())
    iceberg_table.newAppend().appendFile(data_file).commit()
    spark.sql(f"REFRESH TABLE {table}")
    if variant_column != 'v':
        spark.sql(f"ALTER TABLE {table} RENAME COLUMN v TO {variant_column}")
    rows = _read_imported_variant_table(spark, table, container, variant_column).collect()
    assert sorted(tuple(row) for row in rows) == [(0, 7), (1, 9), (2, None), (3, None)]


def _read_imported_variant_table(spark, table, container, variant_column):
    path = variant_column + _IMPORTED_VARIANT_CONTAINERS[container][2]
    return spark.sql(f"SELECT id, try_variant_get({path}, '$.x', 'int') AS x FROM {table}")


@iceberg
@ignore_order(local=True)
@incompat
@pytest.mark.skipif(is_iceberg_remote_catalog(), reason="Imported files require a local catalog")
@pytest.mark.skipif(not supports_iceberg_v3, reason=ICEBERG_V3_UNSUPPORTED_REASON)
@pytest.mark.skipif(not is_spark_411_or_later(), reason="Iceberg Variant requires Spark 4.1.1+")
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


@iceberg
@ignore_order(local=True)
@incompat
@allow_non_gpu('BatchScanExec', 'ColumnarToRowExec', 'ProjectExec', 'VariantGet')
@pytest.mark.skipif(is_iceberg_remote_catalog(), reason="Imported files require a local catalog")
@pytest.mark.skipif(not supports_iceberg_v3, reason=ICEBERG_V3_UNSUPPORTED_REASON)
@pytest.mark.skipif(not is_spark_411_or_later(), reason="Iceberg Variant requires Spark 4.1.1+")
@pytest.mark.parametrize('container', list(_IMPORTED_VARIANT_CONTAINERS))
@pytest.mark.parametrize('variant_column', ['v', 'renamed_v'])
def test_iceberg_v3_unannotated_shredded_variant_read_fallback(
        spark_tmp_table_factory, spark_tmp_path, container, variant_column):
    table = get_full_table_name(spark_tmp_table_factory)

    conf = copy_and_update(_ICEBERG_VARIANT_READ_CONF, {
        'spark.sql.parquet.variant.annotateLogicalType.enabled': 'false',
        'spark.sql.parquet.fieldId.write.enabled': 'false',
        'spark.sql.variant.writeShredding.enabled': 'true',
        'spark.sql.variant.forceShreddingSchemaForTest': 'STRUCT<x: BIGINT>',
    })
    with_cpu_session(lambda spark: _setup_imported_variant_table(
        spark, table, spark_tmp_path + '/unannotated_variant', container, variant_column,
        use_name_mapping=container != 'top', shredded=True), conf=conf)
    assert_cpu_and_gpu_are_equal_collect_with_capture(
        lambda spark: _read_imported_variant_table(spark, table, container, variant_column),
        exist_classes='BatchScanExec',
        non_exist_classes='GpuBatchScanExec,GpuVariantGet',
        conf=_ICEBERG_VARIANT_READ_CONF,
        require_non_empty=True)


@iceberg
@ignore_order(local=True)
@incompat
@allow_non_gpu('BatchScanExec', 'ColumnarToRowExec', 'ProjectExec', 'VariantGet')
@pytest.mark.skipif(not supports_iceberg_v3, reason=ICEBERG_V3_UNSUPPORTED_REASON)
@pytest.mark.skipif(not is_spark_411_or_later(), reason="Iceberg Variant requires Spark 4.1.1+")
def test_iceberg_v3_mixed_variant_layout_read_fallback(spark_tmp_table_factory):
    table = get_full_table_name(spark_tmp_table_factory)

    def setup_table(spark):
        spark.sql(f"CREATE TABLE {table} (id INT, v VARIANT) USING ICEBERG "
                  "TBLPROPERTIES ('format-version'='3', 'write.parquet.shred-variants'='false')")
        spark.sql(f"INSERT INTO {table} SELECT /*+ COALESCE(1) */ 0, parse_json('{{\"x\":7}}')")
        spark.sql(f"ALTER TABLE {table} SET TBLPROPERTIES ("
                  "'write.parquet.shred-variants'='true', "
                  "'write.parquet.variant-inference-buffer-size'='2')")
        spark.sql(f"""
            INSERT INTO {table} SELECT /*+ COALESCE(1) */ id, parse_json(json)
            FROM VALUES (1, '{{"x":8}}'), (2, '{{"x":9}}'), (3, '{{"x":10}}')
            AS source(id, json)
            """)
        spark.sql(f"ALTER TABLE {table} SET TBLPROPERTIES ('write.parquet.shred-variants'='false')")
        jvm = spark._jvm
        iceberg_table = jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable(
            spark._jsparkSession, table)
        layouts = {}
        for row in spark.sql(f"SELECT file_path FROM {table}.files").collect():
            input_file = iceberg_table.io().newInputFile(row.file_path)
            reader = jvm.org.apache.iceberg.shaded.org.apache.parquet.hadoop.ParquetFileReader.open(
                jvm.org.apache.iceberg.parquet.GpuParquetIOAccess.file(input_file))
            try:
                variant = reader.getFileMetaData().getSchema().getType('v').asGroupType()
                layouts[row.file_path] = variant.containsField('typed_value')
            finally:
                reader.close()
        assert sorted(layouts.values()) == [False, True], "Fixture must contain both layouts"
        return next(path for path, shredded in layouts.items() if not shredded)

    supported_file = with_cpu_session(setup_table, conf=_ICEBERG_VARIANT_READ_CONF)

    def read_table(spark):
        df = spark.sql(f"SELECT id, try_variant_get(v, '$.x', 'int') AS x FROM {table}")
        scan = df._jdf.queryExecution().optimizedPlan().collectLeaves().apply(0).scan()
        jvm = spark._jvm
        groups = jvm.org.apache.iceberg.spark.source.GpuSparkScanAccess.taskGroups(scan)
        tasks = [task for group in groups.toArray() for task in group.tasks().toArray()]
        assert len({task.file().location() for task in tasks}) == 2
        tasks.sort(key=lambda task: task.file().location() != supported_file)
        assert tasks[0].file().location() == supported_file
        # Fix the actual scan order so checking only the first file cannot satisfy this test.
        # Keep every planned split, using one task per group for this unpartitioned table.
        groups.removeAll(groups)
        for task in tasks:
            groups.add(jvm.org.apache.iceberg.BaseScanTaskGroup(
                jvm.java.util.Collections.singletonList(task)))
        return df

    assert_cpu_and_gpu_are_equal_collect_with_capture(
        read_table, exist_classes='BatchScanExec',
        non_exist_classes='GpuBatchScanExec,GpuVariantGet',
        conf=_ICEBERG_VARIANT_READ_CONF, require_non_empty=True)


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
