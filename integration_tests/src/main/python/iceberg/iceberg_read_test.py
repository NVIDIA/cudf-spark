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

from decimal import Decimal
import json

import pytest

from asserts import assert_cpu_and_gpu_are_equal_collect_with_capture, \
    assert_gpu_and_cpu_are_equal_collect, assert_gpu_fallback_collect, assert_spark_exception
from conftest import is_iceberg_remote_catalog, spark_jvm
from data_gen import copy_and_update, DecimalGen, IntegerGen, LongGen, StringGen, \
    StructGen, TimestampGen, UniqueLongGen, gen_scalar_value, \
    disable_parquet_field_id_write, gen_df
from iceberg import _build_tblprops, check_default_rows, get_full_table_name, iceberg_unsupported_mark, \
    rapids_reader_types, runtime_iceberg_version, supports_iceberg_v3, \
    ICEBERG_V3_UNSUPPORTED_REASON, iceberg_variant_read_conf, setup_iceberg_fixed_default_table
from marks import allow_non_gpu, iceberg, iceberg_fast_test, ignore_order, incompat
from spark_session import is_spark_400_or_later, is_spark_411_or_later, \
    with_cpu_session, with_gpu_session

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


def _setup_iceberg_v3_defaults_table(table_name, unsupported_column=None):
    props = _build_tblprops({"format-version": "3"})
    props_sql = ", ".join(f"'{key}' = '{value}'" for key, value in props.items())

    def setup_table(spark):
        spark.sql(
            f"CREATE TABLE {table_name} "
            "(id BIGINT, s STRUCT<present: BIGINT>) USING ICEBERG "
            f"TBLPROPERTIES ({props_sql})")
        gen_df(
            spark,
            [("id", UniqueLongGen()),
             ("s", StructGen([("present", LongGen())]))],
            length=32).coalesce(1).writeTo(table_name).append()

        jvm = spark_jvm()
        table = jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable(
            spark._jsparkSession, table_name)
        literals = jvm.org.apache.iceberg.expressions.Literal
        types = jvm.org.apache.iceberg.types.Types
        update = table.updateSchema()

        def add_default(name, iceberg_type, value):
            update.addColumn(name, iceberg_type, literals.of(value).to(iceberg_type))

        # Cover every Iceberg 1.9 primitive type that Spark SQL can query. Iceberg TIME is omitted
        # because Iceberg's Spark adapter rejects it with "Spark does not support time fields".
        update.addRequiredColumn(
            "required_added", types.IntegerType.get(), None, literals.of(7))
        update.addColumn("optional_added", types.StringType.get(), literals.of("legacy"))
        update.addColumn("s", "nested_added", types.IntegerType.get(), literals.of(11))
        add_default("boolean_added", types.BooleanType.get(), True)
        add_default("long_added", types.LongType.get(), jvm.java.lang.Long.valueOf(5000000000))
        add_default("float_added", types.FloatType.get(), jvm.java.lang.Float.valueOf("1.25"))
        add_default("double_added", types.DoubleType.get(), 2.5)
        add_default("date_added", types.DateType.get(), "2024-01-02")
        add_default(
            "timestamp_added",
            types.TimestampType.withZone(),
            "2024-01-02T03:04:05Z")
        add_default(
            "binary_added", types.BinaryType.get(),
            jvm.java.nio.ByteBuffer.wrap(bytearray([1, 2, 3])))
        add_default(
            "decimal_added", types.DecimalType.of(9, 2),
            jvm.java.math.BigDecimal("12345.67"))

        # Add one unsupported default at a time so each fallback case is independent.
        if unsupported_column == "timestamp_ntz_added":
            add_default(
                unsupported_column, types.TimestampType.withoutZone(), "2024-01-02T03:04:05")
        elif unsupported_column == "uuid_added":
            add_default(
                unsupported_column, types.UUIDType.get(),
                "123e4567-e89b-12d3-a456-426614174000")
        elif unsupported_column == "fixed_added":
            add_default(
                unsupported_column, types.FixedType.ofLength(3),
                jvm.java.nio.ByteBuffer.wrap(bytearray([4, 5, 6])))
        update.commit()
        spark.sql(f"REFRESH TABLE {table_name}")

    with_cpu_session(setup_table)


@iceberg
@pytest.mark.skipif(not supports_iceberg_v3, reason=ICEBERG_V3_UNSUPPORTED_REASON)
@ignore_order(local=True)
@pytest.mark.parametrize('reader_type', rapids_reader_types)
@iceberg_fast_test
def test_iceberg_v3_initial_defaults_all_types(spark_tmp_table_factory, reader_type):
    table_name = get_full_table_name(spark_tmp_table_factory)
    _setup_iceberg_v3_defaults_table(table_name)
    v3_conf = {"spark.rapids.sql.format.iceberg.v3.enabled": "true",
               "spark.rapids.sql.format.parquet.reader.type": reader_type}
    query = (
        f"SELECT id, s.present, s.nested_added, required_added, optional_added, "
        "boolean_added, long_added, float_added, double_added, date_added, "
        "timestamp_added, binary_added, decimal_added "
        f"FROM {table_name} ORDER BY id")
    assert_cpu_and_gpu_are_equal_collect_with_capture(
        lambda spark: spark.sql(query),
        exist_classes="GpuBatchScanExec",
        conf=v3_conf)


@iceberg
@pytest.mark.skipif(not supports_iceberg_v3, reason=ICEBERG_V3_UNSUPPORTED_REASON)
@pytest.mark.parametrize('reader_type', rapids_reader_types)
@pytest.mark.parametrize('shape', ['list', 'map', 'multiple'])
@ignore_order(local=True)
def test_iceberg_v3_defaulted_collection_paths(spark_tmp_table_factory, reader_type, shape):
    table_name = get_full_table_name(spark_tmp_table_factory)
    present = gen_scalar_value(LongGen(nullable=False))
    current_value = gen_scalar_value(IntegerGen(nullable=False, min_val=100, max_val=10000,
        special_cases=[]))
    current_other = current_value + 1
    old_element, new_element = 'STRUCT<present: BIGINT>', 'STRUCT<present: BIGINT, added: INT>'
    if shape == 'multiple':
        old_fields = f'b ARRAY<{old_element}>, c MAP<STRING, {old_element}>'
        new_fields = f'b ARRAY<{new_element}>, c MAP<STRING, {new_element}>'
        old = [(0, None), (1, (1, None, {'x': (present,)})), (2, (2, [], None)),
               (3, (3, [None], {})),
               (4, (4, [(None,), (present,), None], {'a': (None,), 'b': None}))]
        current_rows = [(5, (5, [(present, current_value), (None, None), None],
                        {'x': (8, current_other), 'y': (None, None), 'z': None}))]
        defaults = [('s.b.element', 11), ('s.c.value', 22)]
        expressions = ['s.b.added', 'map_values(s.c).added']
        map_columns = [2]
        expected = [(0, None, None), (1, None, [22]), (2, [], None), (3, [None], []),
                    (4, [11, 11, None], [22, None]),
                    (5, [current_value, None, None], [current_other, None, None])]
    else:
        is_map = shape == 'map'
        old_fields = f'b MAP<STRING, {old_element}>' if is_map else f'b ARRAY<{old_element}>'
        new_fields = f'b MAP<STRING, {new_element}>' if is_map else f'b ARRAY<{new_element}>'
        values = [None, {}, {'a': None}, {'a': (None,), 'b': (present,), 'c': None}] if is_map else \
            [None, [], [None], [(None,), (present,), None]]
        old = [(0, None)] + [(i, (i, value)) for i, value in enumerate(values, start=1)]
        physical_container = {'a': (present, current_value), 'b': (None, None), 'c': None} if is_map else \
            [(present, current_value), (None, None), None]
        current_rows = [(5, (5, physical_container))]
        defaults = [('s.b.value' if is_map else 's.b.element', 11)]
        expressions = ['map_values(s.b).added' if is_map else 's.b.added']
        map_columns = [1] if is_map else []
        expected = [(0, None), (1, None), (2, []), (3, [None]),
                    (4, [11, 11, None]), (5, [current_value, None, None])]

    def setup(spark):
        spark.sql(f'CREATE TABLE {table_name} '
                  f'(id BIGINT, s STRUCT<unrelated: BIGINT, {old_fields}>) USING ICEBERG '
                  "TBLPROPERTIES ('format-version'='3')")
        spark.createDataFrame(old, f'id BIGINT, s STRUCT<unrelated: BIGINT, {old_fields}>') \
            .coalesce(1).writeTo(table_name).append()
        jvm = spark_jvm()
        table = jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable(
            spark._jsparkSession, table_name)
        update = table.updateSchema()
        for parent, value in defaults:
            update.addColumn(parent, 'added', jvm.org.apache.iceberg.types.Types.IntegerType.get(),
                             jvm.org.apache.iceberg.expressions.Literal.of(value))
        update.commit()
        spark.sql(f'REFRESH TABLE {table_name}')
        spark.createDataFrame(current_rows, f'id BIGINT, s STRUCT<unrelated: BIGINT, {new_fields}>') \
            .coalesce(1).writeTo(table_name).append()

    with_cpu_session(setup)

    def read(spark):
        # Keep an unpruned CPU reference because Iceberg's CPU pruning also loses these paths.
        spark.conf.set('spark.sql.optimizer.nestedSchemaPruning.enabled',
                       spark.conf.get('spark.rapids.sql.enabled') == 'true')
        projections = ', '.join(f'{expr} AS added_{i}' for i, expr in enumerate(expressions))
        return spark.sql(f'SELECT id, {projections} FROM {table_name}')

    def normalize(rows):
        return [tuple(sorted(value, key=repr) if i in map_columns and value is not None else value
                      for i, value in enumerate(row)) for row in rows]

    def check(cpu_rows, gpu_rows):
        normalized_cpu, normalized_gpu = normalize(cpu_rows), normalize(gpu_rows)
        check_default_rows(normalize(expected))(normalized_cpu, normalized_gpu)
        return normalized_cpu, normalized_gpu

    assert_gpu_and_cpu_are_equal_collect(
        read,
        conf={'spark.rapids.sql.format.iceberg.v3.enabled': 'true',
              'spark.rapids.sql.format.parquet.reader.type': reader_type},
        result_canonicalize_func_before_compare=check)


@iceberg
@pytest.mark.skipif(not supports_iceberg_v3, reason=ICEBERG_V3_UNSUPPORTED_REASON)
@allow_non_gpu("BatchScanExec", "ColumnarToRowExec")
@ignore_order(local=True)
@pytest.mark.parametrize(
    "unsupported_column", ["timestamp_ntz_added", "uuid_added", "fixed_added"])
@pytest.mark.parametrize("project_default", [False, True], ids=["unprojected", "projected"])
@pytest.mark.parametrize('reader_type', rapids_reader_types)
@iceberg_fast_test
def test_iceberg_v3_unsupported_initial_defaults_fallback(
        spark_tmp_table_factory, unsupported_column, project_default, reader_type):
    table_name = get_full_table_name(spark_tmp_table_factory)
    _setup_iceberg_v3_defaults_table(table_name, unsupported_column)
    columns = f"id, {unsupported_column}" if project_default else "id"
    assert_gpu_fallback_collect(
        lambda spark: spark.sql(f"SELECT {columns} FROM {table_name}"),
        "BatchScanExec",
        conf={"spark.rapids.sql.format.iceberg.v3.enabled": "true",
              "spark.rapids.sql.format.parquet.reader.type": reader_type})


@pytest.mark.skipif(not supports_iceberg_v3, reason=ICEBERG_V3_UNSUPPORTED_REASON)
@pytest.mark.parametrize("initial_only", [
    pytest.param(True, marks=allow_non_gpu("BatchScanExec", "ColumnarToRowExec"), id="initial-only"),
    pytest.param(False, id="write-only")])
def test_iceberg_v3_scan_checks_only_initial_defaults(spark_tmp_table_factory, initial_only):
    table = get_full_table_name(spark_tmp_table_factory)
    with_cpu_session(lambda spark: setup_iceberg_fixed_default_table(
        spark, table, initial_only=initial_only))
    assert_cpu_and_gpu_are_equal_collect_with_capture(
        lambda spark: spark.sql(f"SELECT id, payload FROM {table}"),
        exist_classes="BatchScanExec" if initial_only else "GpuBatchScanExec",
        non_exist_classes="GpuBatchScanExec" if initial_only else "BatchScanExec",
        conf={"spark.rapids.sql.format.iceberg.v3.enabled": "true"}, require_non_empty=True)


def _assert_default_rows(read, reader_type, expected):
    assert_gpu_and_cpu_are_equal_collect(
        read,
        conf={'spark.rapids.sql.format.iceberg.v3.enabled': 'true',
              'spark.rapids.sql.format.parquet.reader.type': reader_type},
        result_canonicalize_func_before_compare=check_default_rows(expected))


@iceberg
@pytest.mark.skipif(not supports_iceberg_v3, reason=ICEBERG_V3_UNSUPPORTED_REASON)
@pytest.mark.parametrize('reader_type', rapids_reader_types)
@pytest.mark.parametrize('project_default', [False, True], ids=['implicit', 'projected'])
@ignore_order(local=True)
@iceberg_fast_test
def test_iceberg_v3_defaults_with_predicates(
        spark_tmp_table_factory, reader_type, project_default):
    table_name = get_full_table_name(spark_tmp_table_factory)
    initial = gen_scalar_value(IntegerGen(nullable=False, min_val=1, max_val=1000,
        special_cases=[]))
    lower, upper, current, absent = initial - 1, initial + 1, initial + 12, initial + 100

    def setup(spark):
        spark.sql(f"CREATE TABLE {table_name} (id BIGINT) USING ICEBERG "
                  "TBLPROPERTIES ('format-version'='3')")
        spark.createDataFrame([(0,), (1,)], 'id BIGINT').coalesce(1).writeTo(table_name).append()
        jvm = spark_jvm()
        table = jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable(
            spark._jsparkSession, table_name)
        table.updateSchema().addColumn(
            'added', jvm.org.apache.iceberg.types.Types.IntegerType.get(),
            jvm.org.apache.iceberg.expressions.Literal.of(initial)).commit()
        spark.sql(f"REFRESH TABLE {table_name}")
        spark.createDataFrame([(2, None), (3, current)], 'id BIGINT, added INT') \
            .coalesce(1).writeTo(table_name).append()
        frame = spark.table(table_name)
        return frame.collect(), frame.schema

    reference_rows, reference_schema = with_cpu_session(setup)
    assert sorted(tuple(row) for row in reference_rows) == [
        (0, initial), (1, initial), (2, None), (3, current)]
    predicates = [
        (f'added = {initial}', [0, 1]),
        (f'added > {lower}', [0, 1, 3]),
        (f'added < {upper}', [0, 1]),
        (f'added IN ({initial}, {current})', [0, 1, 3]),
        ('added IS NULL', [2]),
        ('added IS NOT NULL', [0, 1, 3]),
        (f'added != {current}', [0, 1]),
        (f'added = {initial} AND id = 1', [1]),
        (f'added = {initial} OR id = 2', [0, 1, 2]),
        (f'NOT (added = {current} OR id = 0)', [1]),
        (f'added = {absent}', [])]
    columns = ['id', 'added'] if project_default else ['id']
    for predicate, expected_ids in predicates:
        def read(spark):
            # Iceberg's CPU row-group pruning also treats an absent defaulted field as null.
            # Materialize its unfiltered result before evaluating the CPU predicate.
            frame = (spark.table(table_name) if
                     spark.conf.get('spark.rapids.sql.enabled') == 'true' else
                     spark.createDataFrame(reference_rows, reference_schema))
            return frame.where(predicate).select(*columns)

        expected = [tuple(row) if project_default else (row.id,)
                    for row in reference_rows if row.id in expected_ids]
        _assert_default_rows(read, reader_type, expected)


@iceberg
@pytest.mark.skipif(not supports_iceberg_v3, reason=ICEBERG_V3_UNSUPPORTED_REASON)
@pytest.mark.parametrize('reader_type', rapids_reader_types)
@pytest.mark.parametrize('shape', ['struct', 'list', 'map'])
@ignore_order(local=True)
@iceberg_fast_test
def test_iceberg_v3_nested_defaults_mixed_files(spark_tmp_table_factory, reader_type, shape):
    table_name = get_full_table_name(spark_tmp_table_factory)
    present = gen_scalar_value(LongGen(nullable=False))
    current_value = gen_scalar_value(IntegerGen(nullable=False, min_val=100, max_val=10000,
        special_cases=[]))
    if shape == 'struct':
        old_type, new_type = 'STRUCT<present: BIGINT>', 'STRUCT<present: BIGINT, added: INT>'
        old = [(0, None), (1, (None,)), (2, (present,))]
        current_rows = [(4, None), (5, (None, None)), (6, (present, current_value))]
        parent, expression = 'c', 'c.added'
        expected = [(0, None), (1, 11), (2, 11), (4, None), (5, None), (6, current_value)]
    elif shape == 'list':
        old_type = 'ARRAY<STRUCT<present: BIGINT>>'
        new_type = 'ARRAY<STRUCT<present: BIGINT, added: INT>>'
        old = [(0, None), (1, []), (2, [None]), (3, [(None,), (present,), None])]
        current_rows = [(4, None), (5, []), (6, [None, (None, None), (present, current_value)])]
        parent, expression = 'c.element', 'c.added'
        expected = [(0, None), (1, []), (2, [None]), (3, [11, 11, None]),
                    (4, None), (5, []), (6, [None, None, current_value])]
    else:
        old_type = 'MAP<STRING, STRUCT<present: BIGINT>>'
        new_type = 'MAP<STRING, STRUCT<present: BIGINT, added: INT>>'
        old = [(0, None), (1, {}), (2, {'a': None}),
               (3, {'a': (None,), 'b': (present,), 'c': None})]
        current_rows = [(4, None), (5, {}), (6, {'a': None, 'b': (None, None), 'c': (present, current_value)})]
        parent, expression = 'c.value', 'map_values(c).added'
        expected = [(0, None), (1, []), (2, [None]), (3, [11, 11, None]),
                    (4, None), (5, []), (6, [None, None, current_value])]

    expected = [(-1, None)] + expected

    def setup(spark):
        spark.sql(f"CREATE TABLE {table_name} (id BIGINT) USING ICEBERG "
                  "TBLPROPERTIES ('format-version'='3', 'read.split.open-file-cost'='0')")
        # This oldest file lacks the entire parent container: descendant defaults must not
        # turn its null parent into a non-null struct, list or map.
        spark.createDataFrame([(-1,)], 'id BIGINT').coalesce(1).writeTo(table_name).append()
        spark.sql(f'ALTER TABLE {table_name} ADD COLUMN c {old_type}')
        for rows in (old[:2], old[2:]):
            spark.createDataFrame(rows, f'id BIGINT, c {old_type}') \
                .coalesce(1).writeTo(table_name).append()
        jvm = spark_jvm()
        table = jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable(
            spark._jsparkSession, table_name)
        table.updateSchema().addColumn(
            parent, 'added', jvm.org.apache.iceberg.types.Types.IntegerType.get(),
            jvm.org.apache.iceberg.expressions.Literal.of(11)).commit()
        spark.sql(f"REFRESH TABLE {table_name}")
        for rows in (current_rows[:2], current_rows[2:]):
            spark.createDataFrame(rows, f'id BIGINT, c {new_type}') \
                .coalesce(1).writeTo(table_name).append()

    with_cpu_session(setup)

    def read(spark):
        gpu = spark.conf.get('spark.rapids.sql.enabled') == 'true'
        spark.conf.set('spark.sql.optimizer.nestedSchemaPruning.enabled', gpu)
        return spark.sql(f'SELECT id, {expression} AS added FROM {table_name}')

    def check(cpu_rows, gpu_rows):
        if shape != 'map':
            return check_default_rows(expected)(cpu_rows, gpu_rows)
        # The order of map_values is unspecified; array-element order remains checked above.
        def normalize(rows):
            return [(row[0], None if row[1] is None else sorted(row[1], key=repr)) for row in rows]
        normalized_expected = normalize(expected)
        normalized_cpu, normalized_gpu = normalize(cpu_rows), normalize(gpu_rows)
        check_default_rows(normalized_expected)(normalized_cpu, normalized_gpu)
        return normalized_cpu, normalized_gpu

    assert_gpu_and_cpu_are_equal_collect(
        read,
        conf={'spark.rapids.sql.format.iceberg.v3.enabled': 'true',
              'spark.rapids.sql.format.parquet.reader.type': reader_type},
        result_canonicalize_func_before_compare=check)

    def collect_reference(spark):
        frame = spark.table(table_name)
        return frame.collect(), frame.schema

    reference_rows, reference_schema = with_cpu_session(collect_reference)
    predicate = 'c.added = 11' if shape == 'struct' else f'array_contains({expression}, 11)'
    expected_ids = [1, 2] if shape == 'struct' else [3]
    filtered_expected = [row for row in expected if row[0] in expected_ids]

    def read_filtered(spark):
        gpu = spark.conf.get('spark.rapids.sql.enabled') == 'true'
        spark.conf.set('spark.sql.optimizer.nestedSchemaPruning.enabled', gpu)
        frame = (spark.table(table_name) if gpu else
                 spark.createDataFrame(reference_rows, reference_schema))
        return frame.where(predicate).selectExpr('id', f'{expression} AS added')

    def check_filtered(cpu_rows, gpu_rows):
        if shape == 'map':
            cpu_rows = [(row[0], sorted(row[1], key=repr)) for row in cpu_rows]
            gpu_rows = [(row[0], sorted(row[1], key=repr)) for row in gpu_rows]
            wanted = [(row[0], sorted(row[1], key=repr)) for row in filtered_expected]
        else:
            wanted = filtered_expected
        return check_default_rows(wanted)(cpu_rows, gpu_rows)

    assert_gpu_and_cpu_are_equal_collect(
        read_filtered,
        conf={'spark.rapids.sql.format.iceberg.v3.enabled': 'true',
              'spark.rapids.sql.format.parquet.reader.type': reader_type},
        result_canonicalize_func_before_compare=check_filtered)


@iceberg
@pytest.mark.skipif(not supports_iceberg_v3, reason=ICEBERG_V3_UNSUPPORTED_REASON)
@pytest.mark.parametrize('reader_type', rapids_reader_types)
@ignore_order(local=True)
def test_iceberg_v3_defaults_combining_and_type_evolution(spark_tmp_table_factory, reader_type):
    table_name = get_full_table_name(spark_tmp_table_factory)
    defaults = tuple(gen_scalar_value(DecimalGen(p, 2, nullable=False)) for p in (9, 18, 38))
    # Scalar generators restart from the test seed. Derive distinct physical values so
    # replacing a physical value with its default always changes the expected result.
    physical = tuple(value.copy_negate() if value else Decimal('1.00') for value in defaults)
    expected = [(11, i, *defaults) for i in range(6)] + [
        (None, 6, None, None, None), (7, 7, *physical), (5000000000, 8, *defaults)]
    file_count = 0

    def setup(spark):
        nonlocal file_count
        spark.sql(f"CREATE TABLE {table_name} (id BIGINT) USING ICEBERG TBLPROPERTIES ("
                  "'format-version'='3', 'read.split.open-file-cost'='0', "
                  "'read.split.target-size'='134217728')")
        for i in range(6):
            spark.createDataFrame([(i,)], 'id BIGINT').coalesce(1).writeTo(table_name).append()
        jvm = spark_jvm()
        types = jvm.org.apache.iceberg.types.Types
        literals = jvm.org.apache.iceberg.expressions.Literal
        table = jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable(
            spark._jsparkSession, table_name)
        update = table.updateSchema().addColumn('added', types.IntegerType.get(), literals.of(11))
        for name, precision, value in zip(('d32', 'd64', 'd128'), (9, 18, 38), defaults):
            dtype = types.DecimalType.of(precision, 2)
            update.addColumn(name, dtype, literals.of(jvm.java.math.BigDecimal(str(value))))
        update.commit()
        spark.sql(f'REFRESH TABLE {table_name}')
        schema = 'id BIGINT, added INT, d32 DECIMAL(9,2), d64 DECIMAL(18,2), d128 DECIMAL(38,2)'
        for row in [(6, None, None, None, None), (7, 7, *physical)]:
            spark.createDataFrame([row], schema).coalesce(1).writeTo(table_name).append()
        table.refresh()
        table.updateSchema().updateColumn('added', types.LongType.get()) \
            .moveFirst('added').renameColumn('added', 'renamed').commit()
        spark.sql(f'REFRESH TABLE {table_name}')
        spark.createDataFrame([(5000000000, 8, *defaults)],
            'renamed BIGINT, id BIGINT, d32 DECIMAL(9,2), d64 DECIMAL(18,2), d128 DECIMAL(38,2)') \
            .coalesce(1).writeTo(table_name).append()
        table.refresh()
        file_count = spark.sql(f'SELECT count(*) FROM {table_name}.data_files').first()[0]
        assert file_count == 9
        groups = table.newScan().planTasks()
        try:
            iterator = groups.iterator()
            assert iterator.hasNext()
            assert iterator.next().files().size() == file_count
            assert not iterator.hasNext()
        finally:
            groups.close()

    with_cpu_session(setup)
    gpu_frame = []

    def read(spark):
        gpu = spark.conf.get('spark.rapids.sql.enabled') == 'true'
        # Iceberg's CPU Arrow reader copies decimal defaults onto its physical int/long
        # fields and fails to cast them. Use its row reader as the independent CPU oracle.
        spark.conf.set('spark.sql.iceberg.vectorization.enabled', gpu)
        frame = spark.sql(f'SELECT renamed, id, d32, d64, d128 FROM {table_name}')
        if gpu:
            gpu_frame.append(frame)
        return frame

    assert_gpu_and_cpu_are_equal_collect(
        read,
        conf={'spark.rapids.sql.format.iceberg.v3.enabled': 'true',
              'spark.rapids.sql.format.parquet.reader.type': reader_type,
              'spark.rapids.sql.reader.multithreaded.combine.sizeBytes': '64m',
              'spark.sql.adaptive.enabled': 'false'},
        result_canonicalize_func_before_compare=check_default_rows(expected))
    if reader_type == 'COALESCING':
        nodes = [gpu_frame[0]._jdf.queryExecution().executedPlan()]
        while nodes:
            node = nodes.pop()
            if node.getClass().getSimpleName() == 'GpuBatchScanExec':
                batches = node.metrics().get('numOutputBatches').get().value()
                assert 0 < batches < file_count, \
                    f'Expected actual combining, found {batches} batches'
                break
            children = node.children()
            nodes.extend(children.apply(i) for i in range(children.size()))
        else:
            raise AssertionError('GpuBatchScanExec was not executed')

    def collect_reference(spark):
        frame = read(spark)
        return frame.collect(), frame.schema

    reference_rows, reference_schema = with_cpu_session(collect_reference)

    def read_filtered(spark):
        gpu = spark.conf.get('spark.rapids.sql.enabled') == 'true'
        spark.conf.set('spark.sql.iceberg.vectorization.enabled', gpu)
        frame = (spark.table(table_name) if gpu else
                 spark.createDataFrame(reference_rows, reference_schema))
        return frame.where('renamed = 11 OR id = 7').select('renamed', 'id', 'd32', 'd64', 'd128')

    assert_gpu_and_cpu_are_equal_collect(
        read_filtered,
        conf={'spark.rapids.sql.format.iceberg.v3.enabled': 'true',
              'spark.rapids.sql.format.parquet.reader.type': reader_type},
        result_canonicalize_func_before_compare=check_default_rows(
            [row for row in expected if row[1] < 6 or row[1] == 7]))


def _assert_pruned_default_rows(query, reader_type, expected):
    def read(spark):
        spark.conf.set('spark.sql.optimizer.nestedSchemaPruning.enabled',
                       spark.conf.get('spark.rapids.sql.enabled') == 'true')
        return spark.sql(query)

    _assert_default_rows(read, reader_type, expected)


@iceberg
@pytest.mark.skipif(not supports_iceberg_v3, reason=ICEBERG_V3_UNSUPPORTED_REASON)
@pytest.mark.skipif(is_iceberg_remote_catalog(), reason='add_files requires a local catalog')
@pytest.mark.parametrize('reader_type', rapids_reader_types)
@ignore_order(local=True)
def test_iceberg_v3_nested_defaults_name_mapping(spark_tmp_table_factory, reader_type):
    table_name = get_full_table_name(spark_tmp_table_factory)
    present = gen_scalar_value(LongGen(nullable=False))
    source_name = get_full_table_name(spark_tmp_table_factory)

    def setup(spark):
        spark.sql(f'CREATE TABLE {table_name} (unused INT, id BIGINT, s STRUCT<present: BIGINT>) '
                  "USING ICEBERG TBLPROPERTIES ('format-version'='3')")
        spark.sql(f'ALTER TABLE {table_name} DROP COLUMN unused')
        spark.sql(f'CREATE TABLE {source_name} '
                  '(id BIGINT, s STRUCT<present: BIGINT>) USING PARQUET')
        spark.createDataFrame([(0, None), (1, (None,)), (2, (present,))],
            'id BIGINT, s STRUCT<present: BIGINT>').coalesce(1).write.insertInto(source_name)
        spark.sql(f"CALL spark_catalog.system.add_files(table => '{table_name}', "
                  f"source_table => '{source_name}')")
        jvm = spark_jvm()
        table = jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable(
            spark._jsparkSession, table_name)
        assert table.schema().findField('id').fieldId() != 1
        assert table.properties().get('schema.name-mapping.default') is not None
        table.updateSchema().addColumn('s', 'added',
            jvm.org.apache.iceberg.types.Types.IntegerType.get(),
            jvm.org.apache.iceberg.expressions.Literal.of(11)) \
            .renameColumn('s.present', 'legacy').renameColumn('s', 'renamed').commit()
        spark.sql(f'REFRESH TABLE {table_name}')

    with_cpu_session(setup, conf=disable_parquet_field_id_write)

    _assert_pruned_default_rows(f'SELECT id, renamed.added FROM {table_name}', reader_type,
                                [(0, None), (1, 11), (2, 11)])


@iceberg
@pytest.mark.skipif(not supports_iceberg_v3, reason=ICEBERG_V3_UNSUPPORTED_REASON)
@pytest.mark.parametrize('reader_type', rapids_reader_types)
@ignore_order(local=True)
def test_iceberg_v3_default_with_unprojected_timestamp_ntz(spark_tmp_table_factory, reader_type):
    table_name = get_full_table_name(spark_tmp_table_factory)

    present = gen_scalar_value(TimestampGen(nullable=False, tzinfo=None))

    def setup(spark):
        spark.sql(f'CREATE TABLE {table_name} (id BIGINT, s STRUCT<legacy: TIMESTAMP_NTZ>) '
                  "USING ICEBERG TBLPROPERTIES ('format-version'='3')")
        spark.createDataFrame(
            [(0, None), (1, (None,)), (2, (present,))],
            'id BIGINT, s STRUCT<legacy: TIMESTAMP_NTZ>') \
            .coalesce(1).writeTo(table_name).append()
        jvm = spark_jvm()
        table = jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable(
            spark._jsparkSession, table_name)
        table.updateSchema().addColumn('s', 'added',
            jvm.org.apache.iceberg.types.Types.IntegerType.get(),
            jvm.org.apache.iceberg.expressions.Literal.of(11)).commit()
        spark.sql(f'REFRESH TABLE {table_name}')

    with_cpu_session(setup)

    _assert_pruned_default_rows(f'SELECT id, s.added FROM {table_name}', reader_type,
                                [(0, None), (1, 11), (2, 11)])


@iceberg
@pytest.mark.parametrize('format_version', ['2', pytest.param('3', marks=pytest.mark.skipif(
    not supports_iceberg_v3, reason=ICEBERG_V3_UNSUPPORTED_REASON))])
@pytest.mark.parametrize('reader_type', rapids_reader_types)
@pytest.mark.parametrize('shape', ['struct', 'list', 'map'])
@ignore_order(local=True)
def test_iceberg_missing_nested_field_with_unprojected_uuid(
        spark_tmp_table_factory, format_version, reader_type, shape):
    table_name = get_full_table_name(spark_tmp_table_factory)
    default = 11 if format_version == '3' else None
    nested_type = {'type': 'struct', 'fields': [
        {'id': 3, 'name': 'legacy', 'required': False, 'type': 'uuid'}]}
    if shape == 'list':
        field_type = {'type': 'list', 'element-id': 4,
                      'element-required': False, 'element': nested_type}
        parent, expression = 'c.element', 'c.added'
        expected = [(0, None), (1, []), (2, [None]), (3, [default, default, None])]
    elif shape == 'map':
        field_type = {'type': 'map', 'key-id': 4, 'key': 'string',
                      'value-id': 5, 'value-required': False, 'value': nested_type}
        parent, expression = 'c.value', 'map_values(c).added'
        expected = [(0, None), (1, []), (2, [None]), (3, [default, default, None])]
    else:
        field_type = nested_type
        parent, expression = 'c', 'c.added'
        expected = [(0, None), (1, default), (2, default)]

    def setup(spark):
        jvm = spark_jvm()
        schema = jvm.org.apache.iceberg.SchemaParser.fromJson(json.dumps({
            'type': 'struct', 'fields': [
                {'id': 1, 'name': 'id', 'required': True, 'type': 'int'},
                {'id': 2, 'name': 'c', 'required': False, 'type': field_type}]}))
        catalog = jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergCatalog(
            spark._jsparkSession, 'spark_catalog')
        props = jvm.java.util.HashMap()
        for key, value in _build_tblprops({'format-version': format_version}).items():
            props.put(key, value)
        table = catalog.createTable(jvm.org.apache.iceberg.catalog.TableIdentifier.parse(table_name),
                                    schema, jvm.org.apache.iceberg.PartitionSpec.unpartitioned(), props)
        schema = table.schema()
        records = jvm.org.apache.iceberg.data.GenericRecord
        old_struct = schema.findType('c')
        if shape == 'list':
            old_struct = old_struct.asListType().elementType()
        elif shape == 'map':
            old_struct = old_struct.asMapType().valueType()
        null_leaf = records.create(old_struct.asStructType())
        uuid_leaf = records.create(old_struct.asStructType())
        uuid_leaf.setField('legacy', jvm.java.util.UUID.fromString(
            gen_scalar_value(StringGen(
                pattern='[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-a[0-9a-f]{3}-[0-9a-f]{12}',
                nullable=False))))
        if shape == 'list':
            empty, null_element, elements = [jvm.java.util.ArrayList() for _ in range(3)]
            null_element.add(None)
            for value in (null_leaf, uuid_leaf, None):
                elements.add(value)
            values = [None, empty, null_element, elements]
        elif shape == 'map':
            empty, null_element, elements = [jvm.java.util.LinkedHashMap() for _ in range(3)]
            null_element.put('a', None)
            for key, value in zip(('a', 'b', 'c'), (null_leaf, uuid_leaf, None)):
                elements.put(key, value)
            values = [None, empty, null_element, elements]
        else:
            values = [None, null_leaf, uuid_leaf]

        # Use Iceberg's writer because Spark SQL represents UUID as STRING and cannot create
        # the original UUID-annotated physical column used as the validity helper.
        path = table.location() + '/data/uuid-helper.parquet'
        appender = jvm.org.apache.iceberg.data.GenericAppenderFactory(schema).newAppender(
            table.io().newOutputFile(path), jvm.org.apache.iceberg.FileFormat.PARQUET)
        try:
            for index, value in enumerate(values):
                row = records.create(schema)
                row.setField('id', index)
                row.setField('c', value)
                appender.add(row)
        finally:
            appender.close()
        data_file = jvm.org.apache.iceberg.DataFiles.builder(table.spec()).withPath(path) \
            .withFormat(jvm.org.apache.iceberg.FileFormat.PARQUET) \
            .withFileSizeInBytes(table.io().newInputFile(path).getLength()) \
            .withMetrics(appender.metrics()).build()
        table.newAppend().appendFile(data_file).commit()
        update = table.updateSchema()
        int_type = jvm.org.apache.iceberg.types.Types.IntegerType.get()
        if format_version == '3':
            update.addColumn(parent, 'added', int_type,
                             jvm.org.apache.iceberg.expressions.Literal.of(default)).commit()
        else:
            update.addColumn(parent, 'added', int_type).commit()
        spark.sql(f'REFRESH TABLE {table_name}')

    with_cpu_session(setup)

    def read(spark):
        # An unpruned CPU reference preserves parent validity for default-only projections.
        spark.conf.set('spark.sql.optimizer.nestedSchemaPruning.enabled',
                       spark.conf.get('spark.rapids.sql.enabled') == 'true')
        return spark.sql(f'SELECT id, {expression} AS added FROM {table_name}')

    def check(cpu_rows, gpu_rows):
        if shape == 'map':
            def normalize(rows):
                return [(row[0], None if row[1] is None else sorted(row[1], key=repr))
                        for row in rows]
            cpu_rows, gpu_rows = normalize(cpu_rows), normalize(gpu_rows)
        return check_default_rows(expected)(cpu_rows, gpu_rows)

    assert_gpu_and_cpu_are_equal_collect(
        read,
        conf={'spark.rapids.sql.format.iceberg.v3.enabled': 'true',
              'spark.rapids.sql.format.parquet.reader.type': reader_type,
              'spark.sql.iceberg.vectorization.enabled': 'false'},
        result_canonicalize_func_before_compare=check)


def _create_iceberg_table_from_schema(spark, table_name, fields, format_version):
    jvm = spark_jvm()
    schema = jvm.org.apache.iceberg.SchemaParser.fromJson(json.dumps({
        'type': 'struct', 'fields': fields}))
    catalog = jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergCatalog(
        spark._jsparkSession, 'spark_catalog')
    props = jvm.java.util.HashMap()
    for key, value in _build_tblprops({'format-version': format_version}).items():
        props.put(key, value)
    return catalog.createTable(jvm.org.apache.iceberg.catalog.TableIdentifier.parse(table_name),
                               schema, jvm.org.apache.iceberg.PartitionSpec.unpartitioned(), props)


def _append_external_parquet(spark, table, path, row_count):
    jvm = spark_jvm()
    hpath = jvm.org.apache.hadoop.fs.Path(path)
    size = hpath.getFileSystem(spark._jsc.hadoopConfiguration()).getFileStatus(hpath).getLen()
    data_file = jvm.org.apache.iceberg.DataFiles.builder(table.spec()).withPath(path) \
        .withFormat(jvm.org.apache.iceberg.FileFormat.PARQUET) \
        .withFileSizeInBytes(size).withRecordCount(row_count).build()
    mapping = jvm.org.apache.iceberg.mapping.MappingUtil.create(table.schema())
    table.updateProperties().set('schema.name-mapping.default',
        jvm.org.apache.iceberg.mapping.NameMappingParser.toJson(mapping)).commit()
    table.newAppend().appendFile(data_file).commit()


@iceberg
@pytest.mark.parametrize('format_version', ['2', pytest.param('3', marks=pytest.mark.skipif(
    not supports_iceberg_v3, reason=ICEBERG_V3_UNSUPPORTED_REASON))])
@pytest.mark.parametrize('reader_type', rapids_reader_types)
@ignore_order(local=True)
@iceberg_fast_test
def test_iceberg_unmapped_validity_helper_id(
        spark_tmp_table_factory, spark_tmp_path, format_version, reader_type):
    table_name = get_full_table_name(spark_tmp_table_factory)
    present = gen_scalar_value(LongGen(nullable=False))
    # Keep helper values distinct from the default 11 so accidental binding is observable.
    untracked = gen_scalar_value(LongGen(nullable=False, min_val=100, max_val=10000,
                                         special_cases=[]))
    untracked_other = untracked + 1

    def setup(spark):
        fields = [
            {'id': 1, 'name': 'id', 'required': False, 'type': 'int'},
            {'id': 2, 'name': 's', 'required': False, 'type': {'type': 'struct', 'fields': [
                {'id': 3, 'name': 'present', 'required': False, 'type': 'long'}]}}]
        # Evolve through real fields so the new column receives ID 1000, the first ID that
        # Iceberg's Parquet converter would otherwise invent for an unmapped helper leaf.
        fields += [{'id': i, 'name': f'unused_{i}', 'required': False, 'type': 'int'}
                   for i in range(4, 1000)]
        table = _create_iceberg_table_from_schema(spark, table_name, fields, format_version)
        update = table.updateSchema()
        for i in range(4, 1000):
            update.deleteColumn(f'unused_{i}')
        update.commit()
        source = spark_tmp_path + '/unmapped'
        spark.createDataFrame([(0, None), (1, (untracked, present)), (2, (untracked_other, None))],
            'id INT, s STRUCT<untracked: BIGINT, present: BIGINT>') \
            .coalesce(1).write.parquet(source)
        jvm = spark_jvm()
        path = jvm.org.apache.hadoop.fs.Path(source)
        files = path.getFileSystem(spark._jsc.hadoopConfiguration()).listStatus(path)
        parquet = [f.getPath().toString() for f in files
                   if f.getPath().getName().endswith('.parquet')]
        assert len(parquet) == 1
        _append_external_parquet(spark, table, parquet[0], 3)
        update = table.updateSchema()
        int_type = jvm.org.apache.iceberg.types.Types.IntegerType.get()
        if format_version == '3':
            update.addColumn('s', 'added', int_type,
                             jvm.org.apache.iceberg.expressions.Literal.of(11)).commit()
        else:
            update.addColumn('s', 'added', int_type).commit()
        assert table.schema().findField('s.added').fieldId() == 1000
        spark.sql(f'REFRESH TABLE {table_name}')

    with_cpu_session(setup, conf=disable_parquet_field_id_write)

    def read(spark):
        spark.conf.set('spark.sql.optimizer.nestedSchemaPruning.enabled',
                       spark.conf.get('spark.rapids.sql.enabled') == 'true')
        return spark.sql(f'SELECT id, s.added FROM {table_name}')

    default = 11 if format_version == '3' else None
    assert_gpu_and_cpu_are_equal_collect(
        read,
        conf={'spark.rapids.sql.format.iceberg.v3.enabled': str(format_version == '3').lower(),
              'spark.rapids.sql.format.parquet.reader.type': reader_type,
              'spark.sql.iceberg.vectorization.enabled': 'false'},
        result_canonicalize_func_before_compare=check_default_rows([
            (0, None), (1, default), (2, default)]))


@iceberg
@pytest.mark.parametrize('format_version', ['2', pytest.param('3', marks=pytest.mark.skipif(
    not supports_iceberg_v3, reason=ICEBERG_V3_UNSUPPORTED_REASON))])
@pytest.mark.parametrize('reader_type', rapids_reader_types)
@ignore_order(local=True)
def test_iceberg_legacy_list_initial_defaults(
        spark_tmp_table_factory, spark_tmp_path, format_version, reader_type):
    table_name = get_full_table_name(spark_tmp_table_factory)
    # Stay outside INT32 so Py4J selects the INT64 append overload.
    present_a = gen_scalar_value(LongGen(nullable=False, min_val=5000000000,
                                         max_val=6000000000, special_cases=[]))
    present_b = present_a + 1
    path = spark_tmp_path + '/legacy-list.parquet'

    def setup(spark):
        fields = [
            {'id': 1, 'name': 'id', 'required': False, 'type': 'int'},
            {'id': 2, 'name': 'items', 'required': False, 'type': {
                'type': 'list', 'element-id': 3, 'element-required': True,
                'element': {'type': 'struct', 'fields': [
                    {'id': 4, 'name': 'a', 'required': False, 'type': 'long'},
                    {'id': 5, 'name': 'b', 'required': False, 'type': 'long'}]}}}]
        table = _create_iceberg_table_from_schema(spark, table_name, fields, format_version)
        jvm = spark_jvm()
        schema = jvm.org.apache.parquet.schema.MessageTypeParser.parseMessageType(
            'message test { optional int32 id; optional group items (LIST) { '
            'repeated group element { optional int64 a; optional int64 b; } } }')
        factory = jvm.org.apache.parquet.example.data.simple.SimpleGroupFactory(schema)
        writer = jvm.org.apache.parquet.hadoop.example.ExampleParquetWriter \
            .builder(jvm.org.apache.hadoop.fs.Path(path)) \
            .withConf(spark._jsc.hadoopConfiguration()).withType(schema).build()
        try:
            for i in range(3):
                row = factory.newGroup().append('id', i)
                if i > 0:
                    items = row.addGroup('items')
                    if i == 2:
                        items.addGroup('element')
                        # Values outside INT32 force Py4J to use the INT64 append overload.
                        items.addGroup('element').append('a', present_a).append('b', present_b)
                writer.write(row)
        finally:
            writer.close()
        _append_external_parquet(spark, table, path, 3)
        update = table.updateSchema()
        int_type = jvm.org.apache.iceberg.types.Types.IntegerType.get()
        if format_version == '3':
            update.addColumn('items.element', 'added', int_type,
                             jvm.org.apache.iceberg.expressions.Literal.of(11)).commit()
        else:
            update.addColumn('items.element', 'added', int_type).commit()
        spark.sql(f'REFRESH TABLE {table_name}')

    with_cpu_session(setup)
    default = 11 if format_version == '3' else None
    for project_sibling in (False, True):
        def read(spark):
            if spark.conf.get('spark.rapids.sql.enabled') == 'true':
                spark.conf.set('spark.sql.optimizer.nestedSchemaPruning.enabled', 'true')
                columns = 'id, items.b AS present, items.added' if project_sibling else \
                    'id, items.added'
                return spark.sql(f'SELECT {columns} FROM {table_name}')
            # Iceberg's CPU row reader fails on this legacy layout. Spark's Parquet reader
            # provides an independent reference, including null and empty list cardinality.
            spark.conf.set('spark.sql.optimizer.nestedSchemaPruning.enabled', 'false')
            literal = '11' if format_version == '3' else 'NULL'
            columns = ['id'] + (['items.b AS present'] if project_sibling else [])
            columns += [f'transform(items, x -> CAST({literal} AS INT)) AS added']
            return spark.read.parquet(path).selectExpr(*columns)

        expected = ([(0, None, None), (1, [], []), (2, [None, present_b], [default, default])]
                    if project_sibling else [(0, None), (1, []), (2, [default, default])])
        assert_gpu_and_cpu_are_equal_collect(
            read,
            conf={'spark.rapids.sql.format.iceberg.v3.enabled': str(format_version == '3').lower(),
                  'spark.rapids.sql.format.parquet.reader.type': reader_type},
            result_canonicalize_func_before_compare=check_default_rows(expected))


@iceberg
@pytest.mark.skipif(
    not is_spark_400_or_later() or not supports_iceberg_v3 or
    tuple(int(part) for part in runtime_iceberg_version.split('.')[:2]) < (1, 11),
    reason='Iceberg VARIANT writes require Spark 4.x and Iceberg 1.11 or later')
@pytest.mark.parametrize('reader_type', rapids_reader_types)
@pytest.mark.parametrize('project_variant', [
    False,
    pytest.param(True, marks=pytest.mark.skipif(
        not is_spark_411_or_later(), reason='GPU Iceberg Variant reads require Spark 4.1.1+'))])
@ignore_order(local=True)
@incompat
def test_iceberg_v3_default_with_variant(spark_tmp_table_factory, reader_type, project_variant):
    table_name = get_full_table_name(spark_tmp_table_factory)

    value = gen_scalar_value(IntegerGen(nullable=False))

    def setup(spark):
        props = _build_tblprops({'format-version': '3'})
        props_sql = ', '.join(f"'{key}' = '{value}'" for key, value in props.items())
        spark.sql(f'CREATE TABLE {table_name} (id INT, s STRUCT<present: VARIANT>) '
                  f'USING ICEBERG TBLPROPERTIES ({props_sql})')
        # Distinguish a null parent, SQL-null Variant, JSON null, and a non-null object.
        spark.sql(
            'SELECT id, CASE WHEN id = 0 THEN NULL ELSE '
            "named_struct('present', parse_json(text)) END AS s "
            "FROM VALUES (0, 'null'), (1, CAST(NULL AS STRING)), "
            f"(2, 'null'), (3, '{{\"a\":{value}}}') AS v(id, text)") \
            .coalesce(1).writeTo(table_name).append()
        jvm = spark_jvm()
        table = jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable(
            spark._jsparkSession, table_name)
        table.updateSchema().addColumn(
            's', 'added', jvm.org.apache.iceberg.types.Types.IntegerType.get(),
            jvm.org.apache.iceberg.expressions.Literal.of(11)).commit()
        spark.sql(f'REFRESH TABLE {table_name}')

    with_cpu_session(setup)

    def read(spark):
        spark.conf.set('spark.sql.optimizer.nestedSchemaPruning.enabled',
                       spark.conf.get('spark.rapids.sql.enabled') == 'true')
        projection = "id, s.added"
        if project_variant:
            projection += ", try_variant_get(s.present, '$.a', 'int') AS a"
        return spark.sql(f'SELECT {projection} FROM {table_name}')

    conf = {'spark.rapids.sql.format.iceberg.v3.enabled': 'true',
            'spark.rapids.sql.format.parquet.reader.type': reader_type,
            'spark.sql.iceberg.vectorization.enabled': 'false'}
    if project_variant:
        conf = copy_and_update(conf, iceberg_variant_read_conf)
    expected = ([(0, None, None), (1, 11, None), (2, 11, None), (3, 11, value)]
                if project_variant else [(0, None), (1, 11), (2, 11), (3, 11)])
    assert_gpu_and_cpu_are_equal_collect(
        read, conf=conf, result_canonicalize_func_before_compare=check_default_rows(expected))
