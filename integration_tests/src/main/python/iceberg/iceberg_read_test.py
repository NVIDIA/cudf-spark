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
    assert_gpu_and_cpu_are_equal_collect, assert_gpu_fallback_collect
from conftest import is_iceberg_remote_catalog, spark_jvm
from data_gen import LongGen, StructGen, UniqueLongGen, disable_parquet_field_id_write, gen_df
from iceberg import _build_tblprops, get_full_table_name, iceberg_unsupported_mark, \
    rapids_reader_types, runtime_iceberg_version, supports_iceberg_v3, ICEBERG_V3_UNSUPPORTED_REASON
from marks import allow_non_gpu, iceberg, ignore_order
from spark_session import is_spark_400_or_later, with_cpu_session

pytestmark = iceberg_unsupported_mark


def _setup_iceberg_v3_defaults_table(table_name):
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

        # These types are representable by Iceberg/Spark but are not supported by the GPU
        # default-materialization path and must cause scan fallback when projected.
        add_default(
            "timestamp_ntz_added",
            types.TimestampType.withoutZone(),
            "2024-01-02T03:04:05")
        add_default(
            "uuid_added", types.UUIDType.get(),
            "123e4567-e89b-12d3-a456-426614174000")
        add_default(
            "fixed_added", types.FixedType.ofLength(3),
            jvm.java.nio.ByteBuffer.wrap(bytearray([4, 5, 6])))
        update.commit()
        spark.sql(f"REFRESH TABLE {table_name}")

    with_cpu_session(setup_table)


@iceberg
@pytest.mark.skipif(not supports_iceberg_v3, reason=ICEBERG_V3_UNSUPPORTED_REASON)
@ignore_order(local=True)
def test_iceberg_v3_initial_defaults_all_types(spark_tmp_table_factory):
    table_name = get_full_table_name(spark_tmp_table_factory)
    _setup_iceberg_v3_defaults_table(table_name)
    v3_conf = {"spark.rapids.sql.format.iceberg.v3.enabled": "true"}
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
@ignore_order(local=True)
def test_iceberg_v3_project_only_defaulted_struct_child(spark_tmp_table_factory, reader_type):
    table_name = get_full_table_name(spark_tmp_table_factory)
    props_sql = ", ".join(
        f"'{key}' = '{value}'" for key, value in _build_tblprops({"format-version": "3"}).items())

    def setup_table(spark):
        spark.sql(
            f"CREATE TABLE {table_name} "
            "(id BIGINT, s STRUCT<present: BIGINT>) USING ICEBERG "
            f"TBLPROPERTIES ({props_sql})")
        # A null physical leaf must still distinguish a non-null struct from a null parent.
        spark.createDataFrame(
            [(0, None), (1, (None,)), (2, (7,))],
            "id BIGINT, s STRUCT<present: BIGINT>"
        ).coalesce(1).writeTo(table_name).append()

        jvm = spark_jvm()
        table = jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable(
            spark._jsparkSession, table_name)
        table.updateSchema().addColumn(
            "s", "nested_added", jvm.org.apache.iceberg.types.Types.IntegerType.get(),
            jvm.org.apache.iceberg.expressions.Literal.of(11)).commit()
        spark.sql(f"REFRESH TABLE {table_name}")

    with_cpu_session(setup_table)

    def check_defaults(cpu_rows, gpu_rows):
        for rows in (cpu_rows, gpu_rows):
            assert sorted((row.id, row.nested_added) for row in rows) == [
                (0, None), (1, 11), (2, 11)]
        return cpu_rows, gpu_rows

    def read_defaults(spark):
        # Iceberg's CPU nested pruning loses parent validity for this projection. Use an
        # unpruned CPU reference while keeping GPU pruning enabled to exercise the regression.
        gpu_enabled = spark.conf.get("spark.rapids.sql.enabled") == "true"
        spark.conf.set("spark.sql.optimizer.nestedSchemaPruning.enabled", gpu_enabled)
        return spark.sql(f"SELECT id, s.nested_added FROM {table_name}")

    assert_gpu_and_cpu_are_equal_collect(
        read_defaults,
        conf={
            "spark.rapids.sql.format.iceberg.v3.enabled": "true",
            "spark.rapids.sql.format.parquet.reader.type": reader_type},
        result_canonicalize_func_before_compare=check_defaults)


@iceberg
@pytest.mark.skipif(not supports_iceberg_v3, reason=ICEBERG_V3_UNSUPPORTED_REASON)
@pytest.mark.parametrize('reader_type', rapids_reader_types)
@pytest.mark.parametrize('shape', ['list', 'map', 'multiple'])
@ignore_order(local=True)
def test_iceberg_v3_defaulted_collection_paths(spark_tmp_table_factory, reader_type, shape):
    table_name = get_full_table_name(spark_tmp_table_factory)
    old_element, new_element = 'STRUCT<present: BIGINT>', 'STRUCT<present: BIGINT, added: INT>'
    if shape == 'multiple':
        old_fields = f'b ARRAY<{old_element}>, c MAP<STRING, {old_element}>'
        new_fields = f'b ARRAY<{new_element}>, c MAP<STRING, {new_element}>'
        old = [(0, None), (1, (1, None, {'x': (7,)})), (2, (2, [], None)),
               (3, (3, [None], {})),
               (4, (4, [(None,), (7,), None], {'a': (None,), 'b': None}))]
        current = [(5, (5, [(9, 99), (None, None), None],
                        {'x': (8, 77), 'y': (None, None), 'z': None}))]
        defaults = [('s.b.element', 11), ('s.c.value', 22)]
        expressions = ['s.b.added', 'map_values(s.c).added']
        map_columns = [2]
        expected = [(0, None, None), (1, None, [22]), (2, [], None), (3, [None], []),
                    (4, [11, 11, None], [22, None]),
                    (5, [99, None, None], [77, None, None])]
    else:
        is_map = shape == 'map'
        old_fields = f'b MAP<STRING, {old_element}>' if is_map else f'b ARRAY<{old_element}>'
        new_fields = f'b MAP<STRING, {new_element}>' if is_map else f'b ARRAY<{new_element}>'
        values = [None, {}, {'a': None}, {'a': (None,), 'b': (7,), 'c': None}] if is_map else \
            [None, [], [None], [(None,), (7,), None]]
        old = [(0, None)] + [(i, (i, value)) for i, value in enumerate(values, start=1)]
        current_value = {'a': (9, 99), 'b': (None, None), 'c': None} if is_map else \
            [(9, 99), (None, None), None]
        current = [(5, (5, current_value))]
        defaults = [('s.b.value' if is_map else 's.b.element', 11)]
        expressions = ['map_values(s.b).added' if is_map else 's.b.added']
        map_columns = [1] if is_map else []
        expected = [(0, None), (1, None), (2, []), (3, [None]),
                    (4, [11, 11, None]), (5, [99, None, None])]

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
        spark.createDataFrame(current, f'id BIGINT, s STRUCT<unrelated: BIGINT, {new_fields}>') \
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
        _check_default_rows(normalize(expected))(normalized_cpu, normalized_gpu)
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
def test_iceberg_v3_unsupported_initial_defaults_fallback(spark_tmp_table_factory):
    table_name = get_full_table_name(spark_tmp_table_factory)
    _setup_iceberg_v3_defaults_table(table_name)
    v3_conf = {"spark.rapids.sql.format.iceberg.v3.enabled": "true"}

    for unsupported_column in ["timestamp_ntz_added", "uuid_added", "fixed_added"]:
        assert_gpu_fallback_collect(
            lambda spark, column=unsupported_column: spark.sql(
                f"SELECT id, {column} FROM {table_name}"),
            "BatchScanExec",
            conf=v3_conf)


def _check_default_rows(expected):
    def check(cpu_rows, gpu_rows):
        for rows in (cpu_rows, gpu_rows):
            assert sorted([tuple(row) for row in rows], key=repr) == sorted(expected, key=repr)
        return cpu_rows, gpu_rows
    return check


@iceberg
@pytest.mark.skipif(not supports_iceberg_v3, reason=ICEBERG_V3_UNSUPPORTED_REASON)
@pytest.mark.parametrize('reader_type', rapids_reader_types)
@pytest.mark.parametrize('project_default', [False, True], ids=['implicit', 'projected'])
@ignore_order(local=True)
def test_iceberg_v3_defaults_with_predicates(
        spark_tmp_table_factory, reader_type, project_default):
    table_name = get_full_table_name(spark_tmp_table_factory)

    def setup(spark):
        spark.sql(f"CREATE TABLE {table_name} (id BIGINT) USING ICEBERG "
                  "TBLPROPERTIES ('format-version'='3')")
        spark.createDataFrame([(0,), (1,)], 'id BIGINT').coalesce(1).writeTo(table_name).append()
        jvm = spark_jvm()
        table = jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable(
            spark._jsparkSession, table_name)
        table.updateSchema().addColumn(
            'added', jvm.org.apache.iceberg.types.Types.IntegerType.get(),
            jvm.org.apache.iceberg.expressions.Literal.of(11)).commit()
        spark.sql(f"REFRESH TABLE {table_name}")
        spark.createDataFrame([(2, None), (3, 23)], 'id BIGINT, added INT') \
            .coalesce(1).writeTo(table_name).append()
        frame = spark.table(table_name)
        return frame.collect(), frame.schema

    reference_rows, reference_schema = with_cpu_session(setup)
    assert sorted(tuple(row) for row in reference_rows) == [
        (0, 11), (1, 11), (2, None), (3, 23)]
    predicates = [
        ('added = 11', [0, 1]),
        ('added > 10', [0, 1, 3]),
        ('added < 12', [0, 1]),
        ('added IN (11, 23)', [0, 1, 3]),
        ('added IS NULL', [2]),
        ('added IS NOT NULL', [0, 1, 3]),
        ('added != 23', [0, 1]),
        ('added = 11 AND id = 1', [1]),
        ('added = 11 OR id = 2', [0, 1, 2]),
        ('NOT (added = 23 OR id = 0)', [1]),
        ('added = 99', [])]
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
        assert_gpu_and_cpu_are_equal_collect(
            read,
            conf={'spark.rapids.sql.format.iceberg.v3.enabled': 'true',
                  'spark.rapids.sql.format.parquet.reader.type': reader_type},
            result_canonicalize_func_before_compare=_check_default_rows(expected))


@iceberg
@pytest.mark.skipif(not supports_iceberg_v3, reason=ICEBERG_V3_UNSUPPORTED_REASON)
@pytest.mark.parametrize('reader_type', rapids_reader_types)
@pytest.mark.parametrize('shape', ['struct', 'list', 'map'])
@ignore_order(local=True)
def test_iceberg_v3_nested_defaults_mixed_files(spark_tmp_table_factory, reader_type, shape):
    table_name = get_full_table_name(spark_tmp_table_factory)
    if shape == 'struct':
        old_type, new_type = 'STRUCT<present: BIGINT>', 'STRUCT<present: BIGINT, added: INT>'
        old = [(0, None), (1, (None,)), (2, (7,))]
        current = [(4, None), (5, (None, None)), (6, (9, 99))]
        parent, expression = 'c', 'c.added'
        expected = [(0, None), (1, 11), (2, 11), (4, None), (5, None), (6, 99)]
    elif shape == 'list':
        old_type = 'ARRAY<STRUCT<present: BIGINT>>'
        new_type = 'ARRAY<STRUCT<present: BIGINT, added: INT>>'
        old = [(0, None), (1, []), (2, [None]), (3, [(None,), (7,), None])]
        current = [(4, None), (5, []), (6, [None, (None, None), (9, 99)])]
        parent, expression = 'c.element', 'c.added'
        expected = [(0, None), (1, []), (2, [None]), (3, [11, 11, None]),
                    (4, None), (5, []), (6, [None, None, 99])]
    else:
        old_type = 'MAP<STRING, STRUCT<present: BIGINT>>'
        new_type = 'MAP<STRING, STRUCT<present: BIGINT, added: INT>>'
        old = [(0, None), (1, {}), (2, {'a': None}),
               (3, {'a': (None,), 'b': (7,), 'c': None})]
        current = [(4, None), (5, {}), (6, {'a': None, 'b': (None, None), 'c': (9, 99)})]
        parent, expression = 'c.value', 'map_values(c).added'
        expected = [(0, None), (1, []), (2, [None]), (3, [11, 11, None]),
                    (4, None), (5, []), (6, [None, None, 99])]

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
        for rows in (current[:2], current[2:]):
            spark.createDataFrame(rows, f'id BIGINT, c {new_type}') \
                .coalesce(1).writeTo(table_name).append()

    with_cpu_session(setup)

    def read(spark):
        gpu = spark.conf.get('spark.rapids.sql.enabled') == 'true'
        spark.conf.set('spark.sql.optimizer.nestedSchemaPruning.enabled', gpu)
        return spark.sql(f'SELECT id, {expression} AS added FROM {table_name}')

    def check(cpu_rows, gpu_rows):
        if shape != 'map':
            return _check_default_rows(expected)(cpu_rows, gpu_rows)
        # The order of map_values is unspecified; array-element order remains checked above.
        def normalize(rows):
            return [(row[0], None if row[1] is None else sorted(row[1], key=repr)) for row in rows]
        normalized_expected = normalize(expected)
        normalized_cpu, normalized_gpu = normalize(cpu_rows), normalize(gpu_rows)
        _check_default_rows(normalized_expected)(normalized_cpu, normalized_gpu)
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
        return _check_default_rows(wanted)(cpu_rows, gpu_rows)

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
    defaults = (Decimal('1.25'), Decimal('1234567890.25'), Decimal('12345678901234567890.25'))
    physical = (Decimal('2.50'), Decimal('2345678901.25'), Decimal('-22345678901234567890.25'))
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
        result_canonicalize_func_before_compare=_check_default_rows(expected))
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
        result_canonicalize_func_before_compare=_check_default_rows(
            [row for row in expected if row[1] < 6 or row[1] == 7]))


@iceberg
@pytest.mark.skipif(not supports_iceberg_v3, reason=ICEBERG_V3_UNSUPPORTED_REASON)
@pytest.mark.skipif(is_iceberg_remote_catalog(), reason='add_files requires a local catalog')
@pytest.mark.parametrize('reader_type', rapids_reader_types)
@ignore_order(local=True)
def test_iceberg_v3_nested_defaults_name_mapping(spark_tmp_table_factory, reader_type):
    table_name = get_full_table_name(spark_tmp_table_factory)
    source_name = get_full_table_name(spark_tmp_table_factory)

    def setup(spark):
        spark.sql(f'CREATE TABLE {table_name} (unused INT, id BIGINT, s STRUCT<present: BIGINT>) '
                  "USING ICEBERG TBLPROPERTIES ('format-version'='3')")
        spark.sql(f'ALTER TABLE {table_name} DROP COLUMN unused')
        spark.sql(f'CREATE TABLE {source_name} '
                  '(id BIGINT, s STRUCT<present: BIGINT>) USING PARQUET')
        spark.createDataFrame([(0, None), (1, (None,)), (2, (7,))],
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

    def read(spark):
        spark.conf.set('spark.sql.optimizer.nestedSchemaPruning.enabled',
                       spark.conf.get('spark.rapids.sql.enabled') == 'true')
        return spark.sql(f'SELECT id, renamed.added FROM {table_name}')

    assert_gpu_and_cpu_are_equal_collect(
        read,
        conf={'spark.rapids.sql.format.iceberg.v3.enabled': 'true',
              'spark.rapids.sql.format.parquet.reader.type': reader_type},
        result_canonicalize_func_before_compare=_check_default_rows([(0, None), (1, 11), (2, 11)]))


@iceberg
@pytest.mark.skipif(not supports_iceberg_v3, reason=ICEBERG_V3_UNSUPPORTED_REASON)
@pytest.mark.parametrize('reader_type', rapids_reader_types)
@ignore_order(local=True)
def test_iceberg_v3_default_with_unprojected_timestamp_ntz(spark_tmp_table_factory, reader_type):
    table_name = get_full_table_name(spark_tmp_table_factory)

    def setup(spark):
        spark.sql(f'CREATE TABLE {table_name} (id BIGINT, s STRUCT<legacy: TIMESTAMP_NTZ>) '
                  "USING ICEBERG TBLPROPERTIES ('format-version'='3')")
        spark.sql(f"INSERT INTO {table_name} VALUES (0, NULL), (1, named_struct('legacy', "
                  "CAST(NULL AS TIMESTAMP_NTZ))), (2, named_struct('legacy', "
                  "CAST('2024-01-02 03:04:05' AS TIMESTAMP_NTZ)))")
        jvm = spark_jvm()
        table = jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable(
            spark._jsparkSession, table_name)
        table.updateSchema().addColumn('s', 'added',
            jvm.org.apache.iceberg.types.Types.IntegerType.get(),
            jvm.org.apache.iceberg.expressions.Literal.of(11)).commit()
        spark.sql(f'REFRESH TABLE {table_name}')

    with_cpu_session(setup)

    def read(spark):
        spark.conf.set('spark.sql.optimizer.nestedSchemaPruning.enabled',
                       spark.conf.get('spark.rapids.sql.enabled') == 'true')
        return spark.sql(f'SELECT id, s.added FROM {table_name}')

    assert_gpu_and_cpu_are_equal_collect(
        read,
        conf={'spark.rapids.sql.format.iceberg.v3.enabled': 'true',
              'spark.rapids.sql.format.parquet.reader.type': reader_type},
        result_canonicalize_func_before_compare=_check_default_rows([(0, None), (1, 11), (2, 11)]))


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
            '123e4567-e89b-12d3-a456-426614174000'))
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
        return _check_default_rows(expected)(cpu_rows, gpu_rows)

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
def test_iceberg_unmapped_validity_helper_id(
        spark_tmp_table_factory, spark_tmp_path, format_version, reader_type):
    table_name = get_full_table_name(spark_tmp_table_factory)

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
        spark.createDataFrame([(0, None), (1, (99, 7)), (2, (100, None))],
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
        result_canonicalize_func_before_compare=_check_default_rows([
            (0, None), (1, default), (2, default)]))


@iceberg
@pytest.mark.parametrize('format_version', ['2', pytest.param('3', marks=pytest.mark.skipif(
    not supports_iceberg_v3, reason=ICEBERG_V3_UNSUPPORTED_REASON))])
@pytest.mark.parametrize('reader_type', rapids_reader_types)
@ignore_order(local=True)
def test_iceberg_legacy_list_initial_defaults(
        spark_tmp_table_factory, spark_tmp_path, format_version, reader_type):
    table_name = get_full_table_name(spark_tmp_table_factory)
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
                        items.addGroup('element').append('a', 5000000000).append('b', 6000000000)
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

        expected = ([(0, None, None), (1, [], []), (2, [None, 6000000000], [default, default])]
                    if project_sibling else [(0, None), (1, []), (2, [default, default])])
        assert_gpu_and_cpu_are_equal_collect(
            read,
            conf={'spark.rapids.sql.format.iceberg.v3.enabled': str(format_version == '3').lower(),
                  'spark.rapids.sql.format.parquet.reader.type': reader_type},
            result_canonicalize_func_before_compare=_check_default_rows(expected))


@iceberg
@pytest.mark.skipif(
    not is_spark_400_or_later() or not supports_iceberg_v3 or
    tuple(int(part) for part in runtime_iceberg_version.split('.')[:2]) < (1, 11),
    reason='Iceberg VARIANT writes require Spark 4.x and Iceberg 1.11 or later')
@pytest.mark.parametrize('reader_type', rapids_reader_types)
@ignore_order(local=True)
def test_iceberg_v3_default_with_unprojected_variant(spark_tmp_table_factory, reader_type):
    table_name = get_full_table_name(spark_tmp_table_factory)

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
            "(2, 'null'), (3, '{\"a\":1}') AS v(id, text)") \
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
        return spark.sql(f'SELECT id, s.added FROM {table_name}')

    assert_gpu_and_cpu_are_equal_collect(
        read,
        conf={'spark.rapids.sql.format.iceberg.v3.enabled': 'true',
              'spark.rapids.sql.format.parquet.reader.type': reader_type,
              'spark.sql.iceberg.vectorization.enabled': 'false'},
        result_canonicalize_func_before_compare=_check_default_rows([
            (0, None), (1, 11), (2, 11), (3, 11)]))
