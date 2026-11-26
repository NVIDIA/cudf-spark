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

import hashlib
import json
from contextlib import closing
from decimal import Decimal

from conftest import spark_jvm


def _iceberg_data_files(spark, name):
    table = spark_jvm().org.apache.iceberg.spark.Spark3Util.loadIcebergTable(
        spark._jsparkSession, name)
    with closing(table.newScan().planFiles()) as tasks, closing(tasks.iterator()) as iterator:
        while iterator.hasNext():
            path = str(iterator.next().file().location())
            with closing(table.io().newInputFile(path).newStream()) as stream:
                yield path, stream.readAllBytes()


def iceberg_file_hashes(spark, name):
    """Hash data files without decoding Parquet rows or evaluating footer defaults."""
    files = {path: hashlib.sha256(data).hexdigest()
             for path, data in _iceberg_data_files(spark, name)}
    assert files, name
    return files


def assert_iceberg_gpu_writer(spark, write, expected_writer):
    """Capture the DML itself, since the subsequent read cannot identify its writer."""
    if spark.conf.get("spark.rapids.sql.enabled") != "true":
        return write()
    callback = spark_jvm().org.apache.spark.sql.rapids.ExecutionPlanCaptureCallback
    callback.startCapture()
    try:
        result = write()
        plans = callback.getResultsWithTimeout(10000)
        assert any(callback.contains(plan, expected_writer) for plan in plans), \
            f"Expected {expected_writer}:\n" + "\n".join(str(plan) for plan in plans)
        return result
    finally:
        callback.endCapture()


def iceberg_default_file_snapshot(spark, name):
    """Inspect current data files without applying Iceberg defaults or delete filtering."""
    from pyarrow import BufferReader
    from pyarrow.parquet import ParquetFile

    def iceberg_value(iceberg_type, value):
        if value is None:
            return None
        type_id = iceberg_type.typeId().name()
        if type_id in ("BINARY", "FIXED"):
            return bytes(spark_jvm().org.apache.iceberg.util.ByteBuffers.toByteArray(value))
        if type_id == "DECIMAL":
            return Decimal(str(value))
        if type_id == "STRUCT":
            object_class = spark_jvm().java.lang.Class.forName("java.lang.Object")
            return tuple(iceberg_value(field.type(), value.get(index, object_class))
                         for index, field in enumerate(iceberg_type.asStructType().fields()))
        # Iceberg DATE and TIMESTAMP defaults are already epoch days and microseconds.
        return value

    def spark_value(literal, data_type):
        if isinstance(data_type, dict) and data_type["type"] == "struct":
            if spark.sql(f"SELECT {literal}").first()[0] is None:
                return None
            return tuple(spark_value(
                f"({literal}).`{field['name'].replace('`', '``')}`", field["type"])
                for field in data_type["fields"])
        if data_type == "date":
            literal = f"unix_date({literal})"
        elif data_type == "timestamp":
            # Avoid Python/session-local datetime conversion, including DST overlaps.
            literal = f"unix_micros({literal})"
        value = spark.sql(f"SELECT {literal}").first()[0]
        return bytes(value) if isinstance(value, (bytes, bytearray, memoryview)) else value

    def spark_fields(data_type):
        if not isinstance(data_type, dict):
            return
        if data_type["type"] == "struct":
            for field in data_type["fields"]:
                yield field
                yield from spark_fields(field["type"])
        elif data_type["type"] == "array":
            yield from spark_fields(data_type["elementType"])
        elif data_type["type"] == "map":
            yield from spark_fields(data_type["keyType"])
            yield from spark_fields(data_type["valueType"])

    def defaults(metadata):
        result = []
        if b"iceberg.schema" in metadata:
            # Let Iceberg decode its typed JSON defaults (notably binary, decimal and time).
            schema = spark_jvm().org.apache.iceberg.SchemaParser.fromJson(
                metadata[b"iceberg.schema"].decode())
            for field_id in schema.idToName().keySet():
                field = schema.findField(field_id)
                for kind, literal, value in (
                        ("initial-default", field.initialDefaultLiteral(), field.initialDefault()),
                        ("write-default", field.writeDefaultLiteral(), field.writeDefault())):
                    if literal is not None:
                        result.append((field_id, kind, iceberg_value(field.type(), value)))
        else:
            schema = json.loads(metadata[b"org.apache.spark.sql.parquet.row.metadata"])
            for field in spark_fields(schema):
                attrs = field["metadata"]
                for key, kind in (("EXISTS_DEFAULT", "initial-default"),
                                  ("CURRENT_DEFAULT", "write-default")):
                    if key in attrs:
                        result.append((attrs["parquet.field.id"], kind,
                                       spark_value(attrs[key], field["type"])))
        return tuple(sorted(result))

    table = spark_jvm().org.apache.iceberg.spark.Spark3Util.loadIcebergTable(
        spark._jsparkSession, name)
    columns = [field.name() for field in table.schema().columns()]
    records, file_defaults, files = [], set(), {}
    for path, data in _iceberg_data_files(spark, name):
        files[path] = hashlib.sha256(data).hexdigest()
        parquet = ParquetFile(BufferReader(data))
        present = tuple(name for name in columns if name in parquet.schema_arrow.names)
        attributes = defaults(parquet.metadata.metadata or {})
        file_defaults.add((present, attributes))
        for row in parquet.read(columns=list(present)).to_pylist():
            # Keep absent fields distinct from physical NULLs. Include defaults per row
            # so different file grouping cannot hide a wrong schema generation.
            records.append((tuple(row.items()), attributes))
    assert files, name
    return {"records": sorted(records, key=repr),
            "defaults": sorted(file_defaults, key=repr), "files": files}


def assert_iceberg_default_file_parity(spark, cpu_table, gpu_table):
    cpu = iceberg_default_file_snapshot(spark, cpu_table)
    gpu = iceberg_default_file_snapshot(spark, gpu_table)
    assert cpu["records"] == gpu["records"], (cpu["records"], gpu["records"])
    assert cpu["defaults"] == gpu["defaults"], (cpu["defaults"], gpu["defaults"])
