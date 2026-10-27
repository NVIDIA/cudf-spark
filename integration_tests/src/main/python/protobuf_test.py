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

import inspect
import os
import struct

import pytest

from asserts import (
    assert_gpu_and_cpu_are_equal_collect,
    assert_gpu_and_cpu_error,
    assert_gpu_fallback_collect,
)
from data_gen import (
    ArrayGen, BinaryGen, BooleanGen, DoubleGen, FloatGen, IntegerGen, LongGen,
    SetValuesGen, StringGen, StructGen, idfn
)
from marks import allow_non_gpu, ignore_order
from protobuf_data_gen import call_protobuf_function, materialize_protobuf_data
from spark_session import (
    is_before_spark_340,
    is_databricks_runtime,
    is_spark_protobuf_available,
    spark_version,
    with_cpu_session,
)
import pyspark.sql.functions as f
from pyspark.sql.types import StringType

try:
    from google.protobuf import descriptor_pb2
except ModuleNotFoundError:
    descriptor_pb2 = None

pytestmark = [
    pytest.mark.premerge_ci_1,
    pytest.mark.skipif(
        not is_spark_protobuf_available(),
        reason="from_protobuf is unavailable"),
    pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+"),
]

def _lacks_null_result_row(spark):
    expr_class = spark.sparkContext._jvm.org.apache.spark.util.Utils.classForName(
        "org.apache.spark.sql.protobuf.ProtobufDataToCatalyst", True, False)
    return all(field.getName() != "nullResultRow" for field in expr_class.getDeclaredFields())


def _honors_enums_as_ints(spark):
    """Spark 3.4 has no enums.as.ints and always decodes enums as strings."""
    options_class = spark.sparkContext._jvm.org.apache.spark.util.Utils.classForName(
        "org.apache.spark.sql.protobuf.utils.ProtobufOptions", True, False)
    return any(field.getName() == "enumsAsInts" for field in options_class.getDeclaredFields())


def _has_permissive_null_row_fix():
    """Whether Spark has SPARK-46275 (3.4.3, 3.5.1): malformed PERMISSIVE rows become null."""
    if not is_spark_protobuf_available():
        return True
    if is_databricks_runtime():
        # Databricks may backport the fix, so look for the member it removed.
        return with_cpu_session(_lacks_null_result_row)
    version = spark_version()
    return version >= "3.5.1" or "3.4.3" <= version < "3.5.0"


# Without SPARK-46275, Spark returns a struct of null fields for malformed PERMISSIVE rows and
# rejects DROPMALFORMED during analysis.
_permissive_null_row_fix = _has_permissive_null_row_fix()
_modes = ["PERMISSIVE", "FAILFAST"]


# ---------------------------------------------------------------------------
# Shared fixtures and helpers
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def from_protobuf_fn():
    """Return the runtime-provided PySpark API."""
    from pyspark.sql.protobuf.functions import from_protobuf
    return from_protobuf


def _setup_protobuf_desc(local_tmp_path, desc_name, build_fn):
    """Build descriptor bytes and write them to the driver's local filesystem."""
    if descriptor_pb2 is None:
        pytest.skip("Python protobuf package is required to generate descriptors")
    desc_path = os.path.join(local_tmp_path, desc_name)
    desc_bytes = with_cpu_session(build_fn)
    with open(desc_path, "wb") as fp:
        fp.write(desc_bytes)
    return desc_path, desc_bytes


def _call_from_protobuf(from_protobuf_fn, col, message_name,
                        desc_path, desc_bytes, options=None):
    """Call from_protobuf using the right API variant."""
    return call_protobuf_function(
        from_protobuf_fn, col, message_name, desc_path, desc_bytes, options)


def _decode(from_protobuf_fn, desc, message, options=None):
    """from_protobuf of the `bin` column as test.<message>."""
    desc_path, desc_bytes = desc
    return _call_from_protobuf(
        from_protobuf_fn, f.col("bin"), f"test.{message}", desc_path, desc_bytes, options)


def _get_field_by_path(expr, path):
    current = expr
    for name in path:
        current = current.getField(name)
    return current


def _select_paths(decoded, paths):
    """The decoded struct itself for None, otherwise the fields at the dotted paths."""
    if paths is None:
        return [decoded.alias("decoded")]
    return [_get_field_by_path(decoded, path.split(".")).alias(path.replace(".", "_"))
            for path in paths]


def _encode_varint(value):
    """Encode a non-negative integer as a protobuf varint (for hand-crafting test bytes)."""
    if value < 0:
        raise ValueError("varint value must be non-negative")
    out = bytearray()
    v = int(value)
    while True:
        b = v & 0x7F
        v >>= 7
        if v:
            out.append(b | 0x80)
        else:
            out.append(b)
            break
    return bytes(out)


def _encode_tag(field_number, wire_type):
    return _encode_varint((field_number << 3) | wire_type)


def _encode_varint_field(field_number, value):
    return _encode_tag(field_number, 0) + _encode_varint(value)


def _encode_length_delimited_field(field_number, value):
    data = value.encode("utf-8") if isinstance(value, str) else bytes(value)
    return _encode_tag(field_number, 2) + _encode_varint(len(data)) + data


# Eleven 0x80 bytes: a varint that never terminates. CPU `CodedInputStream.readRawVarint64`
# raises "CodedInputStream encountered a malformed varint." after 10 continuation bytes.
_MALFORMED_VARINT = b"\x80" * 11


def _new_proto2_file(_spark, name):
    """Create a descriptor without depending on Spark's private protobuf package."""
    return descriptor_pb2.FileDescriptorProto(
        name=name, package="test", syntax="proto2")


def _field(name, number, ftype, label="optional", default=None,
           type_name=None, packed=False, oneof_index=None):
    """Declarative field spec for `_build_proto2_descriptor`."""
    return {
        "name": name,
        "number": number,
        "type": ftype,
        "label": label,
        "default": default,
        "type_name": type_name,
        "packed": packed,
        "oneof_index": oneof_index,
    }


def _msg(name, fields, enums=None, oneofs=None):
    """Declarative message spec for `_build_proto2_descriptor`."""
    return {
        "name": name,
        "fields": fields,
        "enums": enums or [],
        "oneofs": oneofs or [],
    }


def _enum(name, values, allow_alias=False):
    """Declarative enum spec. Values are `(name, number)` tuples."""
    return {"name": name, "values": values, "allow_alias": allow_alias}


def _build_proto2_descriptor(
        spark, filename, messages, file_enums=None, java_string_check_utf8=False):
    """Build FileDescriptorSet bytes from declarative message and enum specs."""
    fd = _new_proto2_file(spark, filename)
    if java_string_check_utf8:
        fd.options.java_string_check_utf8 = True
    type_map = {
        name: getattr(descriptor_pb2.FieldDescriptorProto, f"TYPE_{name}")
        for name in [
            "BOOL", "INT32", "INT64", "UINT32", "UINT64", "SINT32", "SINT64",
            "FIXED32", "FIXED64", "SFIXED32", "SFIXED64", "FLOAT", "DOUBLE",
            "STRING", "BYTES", "MESSAGE", "GROUP", "ENUM",
        ]
    }
    label_map = {
        name: getattr(descriptor_pb2.FieldDescriptorProto, f"LABEL_{name.upper()}")
        for name in ["optional", "repeated", "required"]
    }

    def _default_literal(value):
        if isinstance(value, bool):
            return "true" if value else "false"
        return str(value)

    def _build_enum(enum_spec):
        enum_proto = descriptor_pb2.EnumDescriptorProto(name=enum_spec["name"])
        if enum_spec.get("allow_alias", False):
            enum_proto.options.allow_alias = True
        for value_name, value_number in enum_spec["values"]:
            enum_proto.value.add(name=value_name, number=value_number)
        return enum_proto

    for enum_spec in file_enums or []:
        fd.enum_type.add().CopyFrom(_build_enum(enum_spec))

    for message_spec in messages:
        message_proto = fd.message_type.add(name=message_spec["name"])
        for oneof_name in message_spec.get("oneofs", []):
            message_proto.oneof_decl.add(name=oneof_name)
        for enum_spec in message_spec["enums"]:
            message_proto.enum_type.add().CopyFrom(_build_enum(enum_spec))
        for field_spec in message_spec["fields"]:
            field_proto = message_proto.field.add(
                name=field_spec["name"],
                number=field_spec["number"],
                label=label_map[field_spec["label"]],
                type=type_map[field_spec["type"]])
            if field_spec["type_name"] is not None:
                field_proto.type_name = field_spec["type_name"]
            if field_spec["default"] is not None:
                field_proto.default_value = _default_literal(field_spec["default"])
            if field_spec["packed"]:
                field_proto.options.packed = True
            if field_spec.get("oneof_index") is not None:
                field_proto.oneof_index = field_spec["oneof_index"]

    fds = descriptor_pb2.FileDescriptorSet()
    fds.file.add().CopyFrom(fd)
    return fds.SerializeToString()


# ---------------------------------------------------------------------------
# Descriptors shared by several sections
# ---------------------------------------------------------------------------

def _build_simple_descriptor_set_bytes(spark):
    return _build_proto2_descriptor(spark, "simple.proto", [
        _msg("Simple", [
            _field("b", 1, "BOOL"),
            _field("i32", 2, "INT32"),
            _field("i64", 3, "INT64"),
            _field("f32", 4, "FLOAT"),
            _field("f64", 5, "DOUBLE"),
            _field("s", 6, "STRING"),
        ]),
    ])


_STATUS_NAMES = ["TEN", "ONE", "FIVE"]


def _build_enum_descriptor_set_bytes(spark):
    """Enum fields at the root, in a nested message and in repeated messages."""
    enum_fields = [
        _field("status", 1, "ENUM", type_name=".test.Status"),
        _field("statuses", 2, "ENUM", label="repeated", type_name=".test.Status"),
    ]
    return _build_proto2_descriptor(spark, "enum.proto", [
        _msg("EnumDetail", enum_fields + [_field("count", 3, "INT32")]),
        _msg("EnumRoot", enum_fields + [
            _field("detail", 3, "MESSAGE", type_name=".test.EnumDetail"),
            _field("items", 4, "MESSAGE", label="repeated", type_name=".test.EnumDetail"),
            _field("id", 5, "INT32"),
        ]),
    # Values are declared out of numeric order and without zero.
    ], file_enums=[_enum("Status", [("TEN", 10), ("ONE", 1), ("FIVE", 5)])])


def _build_nested_depth_descriptor_set_bytes(spark, depth):
    messages = []
    for i in range(depth, 0, -1):
        fields = [_field(f"val{i}", 1, "INT32")]
        if i < depth:
            fields.append(
                _field(
                    f"level{i + 1}", 2, "MESSAGE",
                    type_name=f".test.Level{i + 1}"))
        messages.append(_msg(f"Level{i}", fields))
    return _build_proto2_descriptor(spark, f"nested_depth_{depth}.proto", messages)


def _build_nested_depth_gen(level, depth, nullable):
    fields = [(f"val{level}", IntegerGen())]
    if level < depth:
        fields.append((
            f"level{level + 1}",
            _build_nested_depth_gen(level + 1, depth, nullable=True)))
    return StructGen(fields, nullable=nullable)


def _encode_nested_depth_message(level, depth):
    body = _encode_varint_field(1, level)
    if level < depth:
        body += _encode_length_delimited_field(
            2, _encode_nested_depth_message(level + 1, depth))
    return body


# ===========================================================================
# Valid input: the GPU decodes it and matches the CPU
# ===========================================================================

_ALL_TYPES_SCALARS = [
    ("b", "BOOL", BooleanGen()),
    ("i32", "INT32", IntegerGen()),
    ("i64", "INT64", LongGen()),
    ("u32", "UINT32", IntegerGen()),
    ("u64", "UINT64", LongGen()),
    ("si32", "SINT32", IntegerGen()),
    ("si64", "SINT64", LongGen()),
    ("fx32", "FIXED32", IntegerGen()),
    ("fx64", "FIXED64", LongGen()),
    ("sfx32", "SFIXED32", IntegerGen()),
    ("sfx64", "SFIXED64", LongGen()),
    ("f32", "FLOAT", FloatGen()),
    ("f64", "DOUBLE", DoubleGen()),
    ("s", "STRING", StringGen()),
    ("payload", "BYTES", BinaryGen()),
]

# (name, type, packed, element generator)
_ALL_TYPES_REPEATED = [
    ("r_i32", "INT32", False, IntegerGen(nullable=False)),
    ("r_i64", "INT64", False, LongGen(nullable=False)),
    ("r_f32", "FLOAT", False, FloatGen(nullable=False)),
    ("r_f64", "DOUBLE", False, DoubleGen(nullable=False)),
    ("r_b", "BOOL", False, BooleanGen(nullable=False)),
    ("r_s", "STRING", False, StringGen(nullable=False)),
    ("r_bytes", "BYTES", False, BinaryGen(nullable=False)),
    ("p_i32", "INT32", True, IntegerGen(nullable=False)),
    ("p_si64", "SINT64", True, LongGen(nullable=False)),
    ("p_fx32", "FIXED32", True, IntegerGen(nullable=False)),
    ("p_f64", "DOUBLE", True, DoubleGen(nullable=False)),
    ("p_b", "BOOL", True, BooleanGen(nullable=False)),
]


def _all_types_fields():
    fields = [_field(name, number, ftype)
              for number, (name, ftype, _) in enumerate(_ALL_TYPES_SCALARS, start=1)]
    fields += [_field(name, number, ftype, label="repeated", packed=packed)
               for number, (name, ftype, packed, _) in
               enumerate(_ALL_TYPES_REPEATED, start=len(fields) + 1)]
    return fields


def _build_all_types_descriptor_set_bytes(spark):
    child_fields = _all_types_fields()
    child_fields.append(
        _field("leaf", len(child_fields) + 1, "MESSAGE", type_name=".test.Leaf"))
    fields = _all_types_fields()
    fields += [
        _field("child", len(fields) + 1, "MESSAGE", type_name=".test.Child"),
        _field("items", len(fields) + 2, "MESSAGE", label="repeated", type_name=".test.Child"),
    ]
    return _build_proto2_descriptor(spark, "all_types.proto", [
        _msg("Leaf", [_field("value", 1, "INT32")]),
        _msg("Child", child_fields),
        _msg("AllTypes", fields),
    ])


def _all_types_field_gens():
    return ([(name, gen) for name, _, gen in _ALL_TYPES_SCALARS] +
            [(name, ArrayGen(gen, max_length=5, nullable=False))
             for name, _, _, gen in _ALL_TYPES_REPEATED])


def _all_types_child_gen(nullable):
    return StructGen(
        _all_types_field_gens() + [("leaf", StructGen([("value", IntegerGen())]))],
        nullable=nullable)


_all_types_gen = StructGen(
    _all_types_field_gens() +
    [("child", _all_types_child_gen(nullable=True)),
     ("items", ArrayGen(_all_types_child_gen(nullable=False), max_length=3, nullable=False))],
    nullable=False)


@ignore_order(local=True)
def test_from_protobuf_all_field_types(local_tmp_path, from_protobuf_fn):
    """Every scalar type, packed and unpacked repeated fields, and nested and repeated messages
    decode to the values Spark's to_protobuf encoded."""
    desc = _setup_protobuf_desc(
        local_tmp_path, "all_types.desc", _build_all_types_descriptor_set_bytes)
    fixture_rows, fixture_schema = materialize_protobuf_data(
        _all_types_gen, "test.AllTypes", *desc)

    def run_on_spark(spark):
        df = spark.createDataFrame(fixture_rows, fixture_schema)
        expected = f.struct(*(f.col(field.name) for field in _all_types_gen.data_type.fields))
        return df.select(expected.alias("expected"),
                         _decode(from_protobuf_fn, desc, "AllTypes").alias("decoded"))

    # Spark compares NaN as equal to NaN, unlike Python.
    mismatches = with_cpu_session(
        lambda spark: run_on_spark(spark).filter("NOT (expected <=> decoded)").count())
    assert mismatches == 0
    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


# proto2 test.UnsignedRoot with required, optional, repeated, and defaulted unsigned fields,
# test.ImportedRoot whose child and grandchild come from imported files, and
# test.RepeatedRoot with three levels of repeated messages.
# Kept as bytes so these cross-file contracts do not depend on the protobuf runtime.
_protobuf_semantics_desc_bytes = bytes.fromhex(
    "0a3f0a0a6c6561662e70726f746f12047465737422230a12496d706f72746564"
    "4772616e646368696c64120d0a0576616c7565180120012805620670726f746f"
    "320ad5010a0b6368696c642e70726f746f1204746573741a0a6c6561662e7072"
    "6f746f223d0a0d496d706f727465644368696c64122c0a0a6772616e64636869"
    "6c6418012001280b32182e746573742e496d706f727465644772616e64636869"
    "6c6422380a0c52657065617465644c65616612280a0676616c75657318012003"
    "280b32182e746573742e496d706f727465644772616e646368696c6422330a0d"
    "52657065617465644368696c6412220a066c656176657318012003280b32122e"
    "746573742e52657065617465644c656166620670726f746f320ad3020a0a726f"
    "6f742e70726f746f1204746573741a0b6368696c642e70726f746f22320a0c49"
    "6d706f72746564526f6f7412220a056368696c6418012001280b32132e746573"
    "742e496d706f727465644368696c64229c010a0c556e7369676e6564526f6f74"
    "121c0a0673746174757318012002280e320c2e746573742e5374617475731214"
    "0a0c72657175697265645f75363418022002280412140a0c6f7074696f6e616c"
    "5f75333218032001280d12140a0c72657065617465645f75333218042003280d"
    "12140a0c72657065617465645f75363418052003280412160a0b64656661756c"
    "745f7536341806200128043a013022350a0c5265706561746564526f6f741225"
    "0a086368696c6472656e18012003280b32132e746573742e5265706561746564"
    "4368696c642a200a06537461747573120b0a07554e4b4e4f574e100012090a05"
    "52454144591002620670726f746f32")


@pytest.fixture
def protobuf_semantics_desc(local_tmp_path):
    desc_path = os.path.join(local_tmp_path, "protobuf_semantics.desc")
    with open(desc_path, "wb") as fp:
        fp.write(_protobuf_semantics_desc_bytes)
    return desc_path, _protobuf_semantics_desc_bytes


def test_from_protobuf_unsigned_boundaries(protobuf_semantics_desc, from_protobuf_fn):
    row = (
        _encode_tag(1, 0) + _encode_varint(2) +
        _encode_tag(2, 0) + _encode_varint((1 << 64) - 1) +
        _encode_tag(3, 0) + _encode_varint(1 << 31) +
        _encode_tag(4, 0) + _encode_varint(0) +
        _encode_tag(4, 0) + _encode_varint((1 << 32) - 1) +
        _encode_tag(5, 0) + _encode_varint(1 << 63) +
        _encode_tag(5, 0) + _encode_varint((1 << 64) - 1))

    def run_on_spark(spark):
        return spark.createDataFrame([(row,)], ["bin"]).select(
            _decode(from_protobuf_fn, protobuf_semantics_desc, "UnsignedRoot").alias("decoded"))

    cpu_rows = with_cpu_session(lambda spark: run_on_spark(spark).collect())
    assert cpu_rows[0]["decoded"].asDict() == {
        "status": "READY",
        "required_u64": -1,
        "optional_u32": -(1 << 31),
        "repeated_u32": [0, -1],
        "repeated_u64": [-(1 << 63), -1],
        "default_u64": 0,
    }
    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


def test_from_protobuf_proto2_transitive_import(protobuf_semantics_desc, from_protobuf_fn):
    def run_on_spark(spark):
        decoded = _decode(from_protobuf_fn, protobuf_semantics_desc, "ImportedRoot")
        return spark.createDataFrame([(b"\x0a\x04\x0a\x02\x08\x07",)], ["bin"]).select(
            decoded.getField("child").getField("grandchild").getField("value").alias("value"))

    cpu_rows = with_cpu_session(lambda spark: run_on_spark(spark).collect())
    assert [row["value"] for row in cpu_rows] == [7]
    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


def test_from_protobuf_multilevel_repeated_messages(protobuf_semantics_desc, from_protobuf_fn):
    logical_gen = StructGen([
        ("children", ArrayGen(
            StructGen([
                ("leaves", ArrayGen(
                    StructGen([
                        ("values", ArrayGen(
                            StructGen([
                                ("value", IntegerGen()),
                            ], nullable=False),
                            min_length=0, max_length=5, nullable=False)),
                    ], nullable=False),
                    min_length=0, max_length=5, nullable=False)),
            ], nullable=False),
            min_length=0, max_length=5, nullable=False)),
    ], nullable=False)
    values = [
        {"children": []},
        {"children": [{"leaves": []}]},
        {"children": [
            {"leaves": [{"values": [{"value": 1}, {"value": 2}]}]},
            {"leaves": [{"values": []}]},
        ]},
    ]
    fixture_rows, _ = materialize_protobuf_data(
        logical_gen, "test.RepeatedRoot", *protobuf_semantics_desc, logical_rows=values)
    rows = [(index, row[-1]) for index, row in enumerate(fixture_rows)]

    def run_on_spark(spark):
        decoded = _decode(from_protobuf_fn, protobuf_semantics_desc, "RepeatedRoot")
        return spark.createDataFrame(rows, ["id", "bin"]).select(
            "id", decoded.getField("children").alias("children")).orderBy("id")

    cpu_rows = with_cpu_session(lambda spark: run_on_spark(spark).collect())
    assert [row.asDict(recursive=True)["children"] for row in cpu_rows] == [
        value["children"] for value in values]
    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


def _enum_detail_gen(nullable):
    return StructGen([
        ("status", SetValuesGen(StringType(), _STATUS_NAMES + [None])),
        ("statuses", ArrayGen(SetValuesGen(StringType(), _STATUS_NAMES), nullable=False)),
        ("count", IntegerGen()),
    ], nullable=nullable)


_enum_root_gen = StructGen([
    ("status", SetValuesGen(StringType(), _STATUS_NAMES + [None])),
    ("statuses", ArrayGen(SetValuesGen(StringType(), _STATUS_NAMES), nullable=False)),
    ("detail", _enum_detail_gen(nullable=True)),
    ("items", ArrayGen(_enum_detail_gen(nullable=False), max_length=3, nullable=False)),
    ("id", IntegerGen()),
], nullable=False)


@pytest.mark.parametrize("enums_as_ints", [False, True], ids=["string", "int"])
@ignore_order(local=True)
def test_from_protobuf_enums(local_tmp_path, from_protobuf_fn, enums_as_ints):
    """Enum values at every nesting level decode by name or by number."""
    desc = _setup_protobuf_desc(local_tmp_path, "enum.desc", _build_enum_descriptor_set_bytes)
    fixture_rows, fixture_schema = materialize_protobuf_data(
        _enum_root_gen, "test.EnumRoot", *desc)
    options = {"enums.as.ints": "true"} if enums_as_ints else None

    def run_on_spark(spark):
        df = spark.createDataFrame(fixture_rows, fixture_schema)
        expected = f.struct(*(f.col(field.name) for field in _enum_root_gen.data_type.fields))
        return df.select(expected.alias("expected"),
                         _decode(from_protobuf_fn, desc, "EnumRoot", options).alias("decoded"))

    if not enums_as_ints:
        mismatches = with_cpu_session(
            lambda spark: run_on_spark(spark).filter("NOT (expected <=> decoded)").count())
        assert mismatches == 0
    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


@pytest.mark.parametrize("location", ["nested", "nested_repeated", "repeated_message"], ids=idfn)
@pytest.mark.parametrize("mode", _modes, ids=idfn)
@pytest.mark.parametrize("enums_as_ints", [False, True], ids=["string", "int"])
@ignore_order(local=True)
def test_from_protobuf_unknown_nested_enum(
        local_tmp_path, from_protobuf_fn, location, mode, enums_as_ints):
    """Spark checks unknown enum values only at the root, so nested ones keep the row valid."""
    desc = _setup_protobuf_desc(local_tmp_path, "enum.desc", _build_enum_descriptor_set_bytes)
    known_detail = _encode_varint_field(1, 1) + _encode_varint_field(3, 10)
    if location == "nested_repeated":
        unknown_detail = (_encode_varint_field(2, 1) + _encode_varint_field(2, 999) +
                          _encode_varint_field(3, 20))
    else:
        unknown_detail = _encode_varint_field(1, 999) + _encode_varint_field(3, 20)
    detail_field = 4 if location == "repeated_message" else 3

    def row(detail, row_id):
        return (_encode_varint_field(1, 5) +
                _encode_length_delimited_field(detail_field, detail) +
                _encode_varint_field(5, row_id))

    rows = [(0, row(known_detail, 0)), (1, row(unknown_detail, 1))]
    options = {"mode": mode}
    if enums_as_ints:
        options["enums.as.ints"] = "true"

    def run_on_spark(spark):
        df = spark.createDataFrame(rows, schema="idx int, bin binary")
        return df.select("idx", _decode(from_protobuf_fn, desc, "EnumRoot", options)
                         .alias("decoded"))

    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


def _build_defaults_descriptor_set_bytes(spark):
    """Explicit proto2 defaults at the root and inside a nested message."""
    return _build_proto2_descriptor(
        spark,
        "defaults.proto",
        [
            _msg("DefaultsInner", [
                _field("count", 1, "INT32", default=42),
                _field("label", 2, "STRING", default="hello"),
                _field("flag", 3, "BOOL", default=True),
                _field("language", 4, "ENUM", type_name=".test.Language", default="EN"),
                _field("note", 5, "STRING"),
            ]),
            _msg("Defaults", [
                _field("count", 1, "INT32", default=42),
                _field("name", 2, "STRING", default="unknown"),
                _field("flag", 3, "BOOL", default=True),
                _field("zero_count", 4, "INT32", default=0),
                _field("empty_name", 5, "STRING", default=""),
                _field("language", 6, "ENUM", type_name=".test.Language", default="ZH"),
                _field("note", 7, "STRING"),
                _field("ratio", 8, "FLOAT", default=1.5),
                _field("score", 9, "DOUBLE", default=-2.25),
                _field("inner", 10, "MESSAGE", type_name=".test.DefaultsInner"),
            ]),
        ],
        file_enums=[_enum("Language", [("UNKNOWN_LANGUAGE", 0), ("EN", 1), ("ZH", 2)])])


@pytest.mark.parametrize("enums_as_ints", [False, True], ids=["string", "int"])
@ignore_order(local=True)
def test_from_protobuf_default_values(local_tmp_path, from_protobuf_fn, enums_as_ints):
    """Absent fields take their declared defaults, including inside a present nested message,
    while an absent nested message stays null."""
    desc = _setup_protobuf_desc(
        local_tmp_path, "defaults.desc", _build_defaults_descriptor_set_bytes)
    full_inner = (_encode_varint_field(1, 7) + _encode_length_delimited_field(2, "l") +
                  _encode_varint_field(3, 0) + _encode_varint_field(4, 2) +
                  _encode_length_delimited_field(5, "n"))
    rows = [
        (0, _encode_varint_field(1, 100) + _encode_length_delimited_field(2, "test") +
            _encode_varint_field(3, 0) + _encode_varint_field(4, 5) +
            _encode_length_delimited_field(5, "x") + _encode_varint_field(6, 1) +
            _encode_length_delimited_field(7, "n") +
            _encode_tag(8, 5) + struct.pack("<f", 0.25) +
            _encode_tag(9, 1) + struct.pack("<d", 3.5) +
            _encode_length_delimited_field(10, full_inner)),
        (1, b""),
        (2, _encode_varint_field(1, 100)),
        (3, _encode_length_delimited_field(10, b"")),
        (4, _encode_length_delimited_field(10, _encode_varint_field(1, 7))),
        (5, None),
    ]

    def run_on_spark(spark):
        df = spark.createDataFrame(rows, schema="idx int, bin binary")
        return df.select("idx", _decode(from_protobuf_fn, desc, "Defaults", options)
                         .alias("decoded"))

    options = {"enums.as.ints": "true"} if enums_as_ints else None
    cpu_rows = {row["idx"]: row["decoded"] for row in
                with_cpu_session(lambda spark: run_on_spark(spark).collect())}
    assert cpu_rows[1]["zero_count"] == 0 and cpu_rows[1]["empty_name"] == ""
    assert cpu_rows[1]["count"] == 42 and cpu_rows[1]["note"] is None
    assert (cpu_rows[1]["ratio"], cpu_rows[1]["score"]) == (1.5, -2.25)
    as_ints = enums_as_ints and with_cpu_session(_honors_enums_as_ints)
    assert cpu_rows[1]["language"] == (2 if as_ints else "ZH")
    assert cpu_rows[1]["inner"] is None
    assert cpu_rows[3]["inner"]["count"] == 42 and cpu_rows[3]["inner"]["note"] is None
    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


def _build_wire_variants_descriptor_set_bytes(spark):
    return _build_proto2_descriptor(spark, "wire_variants.proto", [
        _msg("WireChild", [
            _field("a", 1, "INT32"),
            _field("b", 2, "STRING"),
        ]),
        _msg("WireParent", [
            _field("child", 1, "MESSAGE", type_name=".test.WireChild"),
            _field("ordinal", 2, "INT32"),
        ]),
        _msg("Wire", [
            _field("b", 1, "BOOL"),
            _field("i32", 2, "INT32"),
            _field("s", 3, "STRING"),
            _field("flags", 4, "BOOL", label="repeated"),
            _field("packed_ints", 5, "INT32", label="repeated", packed=True),
            _field("unpacked_doubles", 6, "DOUBLE", label="repeated"),
            _field("packed_fixed32", 7, "FIXED32", label="repeated", packed=True),
            _field("child", 8, "MESSAGE", type_name=".test.WireChild"),
            _field("parents", 9, "MESSAGE", label="repeated", type_name=".test.WireParent"),
        ]),
    ])


def _wire_variant_rows():
    doubles = struct.pack("<dd", 1.5, -2.0)
    fixed = struct.pack("<II", 7, 0xFFFFFFFF)
    unknown_group = _encode_varint_field(1, 7) + _encode_length_delimited_field(2, "in group")
    unknown_fields = (
        _encode_varint_field(50, 300) +
        _encode_tag(51, 1) + bytes(range(8)) +
        _encode_length_delimited_field(52, b"\x08\x01unknown") +
        _encode_tag(53, 3) + unknown_group + _encode_tag(53, 4) +
        _encode_tag(54, 5) + bytes(range(4)))
    child_a = _encode_varint_field(1, 1)
    child_b = _encode_length_delimited_field(2, "x")
    invalid_utf8 = b"\xed\xa0\x80"
    return {
        "empty_message": [b""],
        # protobuf-java reads any non-zero varint as true, including 256 whose low byte is 0.
        "noncanonical_bool": [
            _encode_varint_field(1, 256) + _encode_varint_field(2, 99),
            _encode_varint_field(1, 1) + _encode_varint_field(2, 100),
            _encode_varint_field(1, 512) + _encode_varint_field(2, 101),
            _encode_varint_field(4, 256) + _encode_varint_field(4, 1) +
            _encode_varint_field(4, 512),
        ],
        "noncanonical_tag_and_length": [
            bytes([0x90] + [0x80] * 8 + [0]) + _encode_varint(42) +
            _encode_tag(3, 2) + bytes([0x80] * 9 + [0]),
        ],
        "last_scalar_wins": [
            _encode_varint_field(2, 1) + _encode_varint_field(2, 42),
            _encode_varint_field(2, 3) + _encode_varint_field(2, 4) + _encode_varint_field(2, 5),
        ],
        "message_fields_merge": [
            _encode_length_delimited_field(8, child_a) + _encode_length_delimited_field(8, child_b) +
            _encode_length_delimited_field(
                9, _encode_length_delimited_field(1, child_a) +
                _encode_length_delimited_field(1, child_b) + _encode_varint_field(2, 7)),
        ],
        "unknown_fields_of_every_wire_type": [
            _encode_varint_field(2, 1) + unknown_fields + _encode_length_delimited_field(3, "s"),
            unknown_fields + _encode_varint_field(1, 1),
            unknown_fields,
        ],
        # Parsers accept packed and unpacked encodings regardless of the declared packing.
        "either_packing": [
            (_encode_varint_field(5, 1) + _encode_varint_field(5, 2) +
             _encode_tag(7, 5) + fixed[:4] + _encode_tag(7, 5) + fixed[4:]),
            _encode_length_delimited_field(6, doubles),
            # Both encodings for the same field append in wire order.
            (_encode_varint_field(5, 3) +
             _encode_length_delimited_field(5, _encode_varint(4) + _encode_varint(5)) +
             _encode_tag(6, 1) + doubles[:8] + _encode_length_delimited_field(6, doubles[8:]) +
             _encode_length_delimited_field(7, fixed[:4]) + _encode_tag(7, 5) + fixed[4:]),
        ],
        "invalid_utf8_surrogate": [_encode_length_delimited_field(3, invalid_utf8)],
    }


@pytest.mark.parametrize("variant", list(_wire_variant_rows().keys()), ids=idfn)
@ignore_order(local=True)
def test_from_protobuf_wire_encoding_variants(local_tmp_path, from_protobuf_fn, variant):
    """Valid but unusual encodings that protobuf-java accepts."""
    desc = _setup_protobuf_desc(
        local_tmp_path, "wire_variants.desc", _build_wire_variants_descriptor_set_bytes)
    rows = [(i, row) for i, row in enumerate(_wire_variant_rows()[variant])]
    rows.append((len(rows), None))

    def run_on_spark(spark):
        df = spark.createDataFrame(rows, schema="idx int, bin binary")
        return df.select("idx", _decode(from_protobuf_fn, desc, "Wire").alias("decoded"))

    if variant == "message_fields_merge":
        decoded = with_cpu_session(lambda spark: run_on_spark(spark).collect())[0]["decoded"]
        assert decoded["child"].asDict() == {"a": 1, "b": "x"}
        assert decoded["parents"][0]["child"].asDict() == {"a": 1, "b": "x"}
        assert decoded["parents"][0]["ordinal"] == 7
    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


def _build_many_repeated_fields_descriptor_set_bytes(spark, field_count):
    return _build_proto2_descriptor(spark, "many_repeated.proto", [
        _msg("ManyRepeated", [
            _field(f"values_{number}", number, "INT32", label="repeated")
            for number in range(1, field_count + 1)
        ]),
    ])


@pytest.mark.parametrize("field_count", [32, 33, 65], ids=idfn)
@pytest.mark.parametrize("select_whole_message", [True, False], ids=["whole", "field"])
@ignore_order(local=True)
def test_from_protobuf_many_repeated_fields(
        local_tmp_path, from_protobuf_fn, field_count, select_whole_message):
    """The decoder scans repeated fields in batches, so their count per message is unlimited."""
    desc = _setup_protobuf_desc(
        local_tmp_path,
        f"many_repeated_{field_count}.desc",
        lambda spark: _build_many_repeated_fields_descriptor_set_bytes(spark, field_count))
    row = (_encode_tag(1, 0) + _encode_varint(7) +
           _encode_tag(field_count, 0) + _encode_varint(9))

    def run_on_spark(spark):
        df = spark.createDataFrame([(0, row), (1, None)], schema="idx int, bin binary")
        decoded = _decode(from_protobuf_fn, desc, "ManyRepeated")
        if select_whole_message:
            return df.select("idx", decoded.alias("decoded"))
        return df.select("idx", decoded.getField(f"values_{field_count}").alias("values"))

    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


def _build_repeated_int_descriptor_set_bytes(spark):
    return _build_proto2_descriptor(spark, "repeated_int.proto", [
        _msg("WithRepeatedInt", [
            _field("id", 1, "INT32"),
            _field("values", 2, "INT32", label="repeated"),
        ]),
    ])


@ignore_order(local=True)
def test_from_protobuf_large_repeated_array(local_tmp_path, from_protobuf_fn):
    """Arrays of 500-1000 elements per row."""
    logical_gen = StructGen([
        ("id", IntegerGen()),
        ("values", ArrayGen(
            IntegerGen(nullable=False), min_length=500, max_length=1000,
            nullable=False)),
    ], nullable=False)
    desc = _setup_protobuf_desc(
        local_tmp_path, "large_repeated_int.desc", _build_repeated_int_descriptor_set_bytes)
    fixture_rows, fixture_schema = materialize_protobuf_data(
        logical_gen, "test.WithRepeatedInt", *desc, length=128)

    def run_on_spark(spark):
        df = spark.createDataFrame(fixture_rows, fixture_schema)
        decoded = _decode(from_protobuf_fn, desc, "WithRepeatedInt")
        return df.select(decoded.getField("id").alias("id"),
                         decoded.getField("values").alias("values"))

    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


@ignore_order(local=True)
def test_from_protobuf_max_nesting_depth(local_tmp_path, from_protobuf_fn):
    """Ten levels is the deepest schema the GPU decoder supports."""
    depth = 10
    desc = _setup_protobuf_desc(
        local_tmp_path, f"nested_depth_{depth}.desc",
        lambda spark: _build_nested_depth_descriptor_set_bytes(spark, depth))
    fixture_rows, fixture_schema = materialize_protobuf_data(
        _build_nested_depth_gen(1, depth, nullable=False), "test.Level1", *desc)

    def run_on_spark(spark):
        df = spark.createDataFrame(fixture_rows, fixture_schema)
        decoded = _decode(from_protobuf_fn, desc, "Level1")
        deepest = _get_field_by_path(decoded, [f"level{i}" for i in range(2, depth + 1)])
        return df.select(decoded.alias("decoded"),
                         deepest.getField(f"val{depth}").alias(f"val{depth}"))

    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


@ignore_order(local=True)
def test_from_protobuf_all_null_input(local_tmp_path, from_protobuf_fn):
    desc = _setup_protobuf_desc(
        local_tmp_path, "simple.desc", _build_simple_descriptor_set_bytes)

    def run_on_spark(spark):
        df = spark.createDataFrame([(None,), (None,), (None,)], schema="bin binary")
        return df.select(_decode(from_protobuf_fn, desc, "Simple").alias("decoded"))

    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


@ignore_order(local=True)
def test_from_protobuf_failfast_skips_rows_conditional_does_not_select(
        local_tmp_path, from_protobuf_fn):
    """FAILFAST decoding under a conditional only sees the rows that branch selects."""
    desc = _setup_protobuf_desc(
        local_tmp_path, "simple.desc", _build_simple_descriptor_set_bytes)
    valid_row = _encode_tag(2, 0) + _encode_varint(42)
    malformed_row = bytes([0x08, 0x80])

    def run_on_spark(spark):
        df = spark.createDataFrame(
            [(0, valid_row), (1, malformed_row)], schema="idx int, bin binary")
        decoded = _decode(from_protobuf_fn, desc, "Simple", {"mode": "FAILFAST"})
        return df.select(
            "idx", f.when(f.col("idx") == 0, decoded.getField("i32")).alias("i32"))

    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


# Kept as bytes so baseline API coverage does not depend on the Python protobuf package.
# proto2 test.Simple { optional int32 i32 = 1; optional string s = 2; }
_simple_desc_bytes = bytes.fromhex(
    "0a360a0c73696d706c652e70726f746f12047465737422200a0653696d706c65"
    "120b0a0369333218012001280512090a0173180220012809")

# proto2 test.Child { optional int32 a = 1; optional string b = 2; }
# test.Root { optional int32 top = 1; optional Child child = 2; repeated Child items = 3; }
_nested_smoke_desc_bytes = bytes.fromhex(
    "0a8e010a126e65737465645f736d6f6b652e70726f746f120474657374221d0a"
    "054368696c6412090a016118012001280512090a0162180220012809224b0a04"
    "526f6f74120b0a03746f70180120012805121a0a056368696c6418022001280b"
    "320b2e746573742e4368696c64121a0a056974656d7318032003280b320b2e74"
    "6573742e4368696c64620670726f746f32")

_nested_smoke_gen = StructGen([
    ("top", IntegerGen()),
    ("child", StructGen([
        ("a", IntegerGen()),
        ("b", StringGen()),
    ])),
    ("items", ArrayGen(
        StructGen([
            ("a", IntegerGen()),
            ("b", StringGen()),
        ], nullable=False),
        min_length=0, max_length=5, nullable=False)),
], nullable=False)


@pytest.fixture
def simple_desc(local_tmp_path):
    desc_path = os.path.join(local_tmp_path, "simple_smoke.desc")
    with open(desc_path, "wb") as fp:
        fp.write(_simple_desc_bytes)
    return desc_path, _simple_desc_bytes


@pytest.fixture
def nested_smoke_desc(local_tmp_path):
    desc_path = os.path.join(local_tmp_path, "nested_smoke.desc")
    with open(desc_path, "wb") as fp:
        fp.write(_nested_smoke_desc_bytes)
    return desc_path, _nested_smoke_desc_bytes


def _encode_simple_message(i32_value, s_value):
    return (_encode_varint_field(1, i32_value & 0xFFFFFFFFFFFFFFFF) +
            _encode_length_delimited_field(2, s_value))


@pytest.mark.parametrize("api", ["descriptor_file", "binary_descriptor_set"], ids=idfn)
def test_from_protobuf_smoke_api(simple_desc, from_protobuf_fn, api):
    desc_path, desc_bytes = simple_desc
    if api == "descriptor_file":
        decode = lambda: from_protobuf_fn(f.col("bin"), "test.Simple", desc_path)
    elif "binaryDescriptorSet" not in inspect.signature(from_protobuf_fn).parameters:
        pytest.skip("binaryDescriptorSet kwarg is Spark 3.5+ only")
    else:
        decode = lambda: from_protobuf_fn(
            f.col("bin"), "test.Simple", binaryDescriptorSet=bytearray(desc_bytes))
    values = [(1, "a"), (-2, "bb"), (0, ""), (12345, "hello")]

    def run_on_spark(spark):
        df = spark.createDataFrame([(_encode_simple_message(i, s),) for i, s in values], ["bin"])
        return df.select(decode().alias("decoded"))

    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


def test_from_protobuf_smoke_nested(nested_smoke_desc, from_protobuf_fn):
    values = [
        {"top": 1, "child": {"a": 2, "b": "child"},
         "items": [{"a": 3, "b": "first"}, {"a": 4, "b": "second"}]},
        {"top": 5, "child": None, "items": []},
    ]
    fixture_rows, _ = materialize_protobuf_data(
        _nested_smoke_gen, "test.Root", *nested_smoke_desc, logical_rows=values)
    encoded = [(row[-1],) for row in fixture_rows]

    def run_on_spark(spark):
        decoded = _decode(from_protobuf_fn, nested_smoke_desc, "Root")
        return spark.createDataFrame(encoded, ["bin"]).select(
            decoded.getField("top").alias("top"),
            decoded.getField("child").getField("a").alias("child_a"),
            f.get(decoded.getField("items"), 0).getField("b").alias("first_item_b"))

    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


# ===========================================================================
# Malformed input: PERMISSIVE nulls the row and FAILFAST fails, as on the CPU
#
# The CPU parses the whole payload, so cases that select fields also check that the GPU
# validates the fields the query does not read.
# ===========================================================================

def _build_required_field_descriptor_set_bytes(spark):
    return _build_proto2_descriptor(spark, "required.proto", [
        _msg("WithRequired", [
            _field("id", 1, "INT64", label="required"),
            _field("name", 2, "STRING"),
            _field("count", 3, "INT32"),
        ]),
    ])


def _build_nested_required_field_descriptor_set_bytes(spark):
    return _build_proto2_descriptor(spark, "nested_required.proto", [
        _msg("InnerRequired", [
            _field("child_id", 1, "INT32", label="required"),
            _field("note", 2, "STRING"),
        ]),
        _msg("WithNestedRequired", [
            _field("id", 1, "INT64"),
            _field("inner", 2, "MESSAGE", type_name=".test.InnerRequired"),
            _field("name", 3, "STRING"),
        ]),
    ])


def _build_packed_repeated_descriptor_set_bytes(spark):
    return _build_proto2_descriptor(spark, "packed_repeated.proto", [
        _msg("WithPackedRepeated", [
            _field("id", 1, "INT32"),
            _field("int_values", 2, "INT32", label="repeated", packed=True),
            _field("double_values", 3, "DOUBLE", label="repeated", packed=True),
        ]),
    ])


def _build_nested_packed_descriptor_set_bytes(spark):
    return _build_proto2_descriptor(spark, "nested_packed.proto", [
        _msg("InnerPacked", [
            _field("keep", 1, "INT32"),
            _field("bad_values", 2, "INT32", label="repeated", packed=True),
        ]),
        _msg("OuterPacked", [
            _field("inner", 1, "MESSAGE", type_name=".test.InnerPacked"),
        ]),
    ])


def _build_nested_descriptor_set_bytes(spark):
    return _build_proto2_descriptor(spark, "nested.proto", [
        _msg("Nested", [_field("x", 1, "INT32")]),
        _msg("WithNested", [
            _field("simple_int", 1, "INT32"),
            _field("simple_str", 2, "STRING"),
            _field("nested_msg", 3, "MESSAGE", type_name=".test.Nested"),
            _field("simple_long", 4, "INT64"),
        ]),
    ])


_simple_valid = _encode_varint_field(1, 1) + _encode_varint_field(2, 42)
_required_valid = (_encode_varint_field(1, 1) + _encode_length_delimited_field(2, "ok") +
                   _encode_varint_field(3, 10))
_nested_required_valid = (
    _encode_varint_field(1, 100) +
    _encode_length_delimited_field(
        2, _encode_varint_field(1, 7) + _encode_length_delimited_field(2, "ok")) +
    _encode_length_delimited_field(3, "valid"))
_nested_required_missing = (
    _encode_varint_field(1, 200) +
    _encode_length_delimited_field(2, _encode_length_delimited_field(2, "oops")) +
    _encode_length_delimited_field(3, "missing"))
_with_nested_scalars = (_encode_varint_field(1, 42) + _encode_length_delimited_field(2, "hello") +
                        _encode_varint_field(4, 99))


def _malformed_case(case_id, build_fn, message, valid, malformed, paths=None, options=None):
    """`valid` is one or more valid rows. `paths` None selects the whole struct, which tells a
    null row from a struct of nulls; field paths leave the rest of the message unselected."""
    valid_rows = valid if isinstance(valid, list) else [valid]
    return pytest.param(build_fn, message, valid_rows, malformed, paths, options or {},
                        id=case_id)


_malformed_cases = [
    _malformed_case("truncated_varint", _build_simple_descriptor_set_bytes, "Simple",
                    _simple_valid, bytes([0x08, 0x80])),
    _malformed_case("wrong_wire_type", _build_simple_descriptor_set_bytes, "Simple",
                    _encode_varint_field(2, 42),
                    _encode_varint_field(2, 7) + _encode_tag(2, 5) + bytes([77, 0, 0, 0]) +
                    _encode_varint_field(2, 42) + _encode_length_delimited_field(6, "ok")),
    _malformed_case("unterminated_varint_in_unselected_field",
                    _build_simple_descriptor_set_bytes, "Simple", _simple_valid,
                    _simple_valid + _encode_tag(3, 0) + _MALFORMED_VARINT, ["b", "i32"]),
    _malformed_case("truncated_length_in_unselected_field",
                    _build_simple_descriptor_set_bytes, "Simple", _simple_valid,
                    _simple_valid + _encode_tag(6, 2) + _encode_varint(100) + b"short",
                    ["b", "i32"]),
    _malformed_case("wrong_wire_type_in_unselected_field",
                    _build_simple_descriptor_set_bytes, "Simple", _simple_valid,
                    _simple_valid + _encode_length_delimited_field(3, b"x"), ["b", "i32"]),
    _malformed_case("missing_required", _build_required_field_descriptor_set_bytes,
                    "WithRequired", _required_valid, _encode_length_delimited_field(2, "bad")),
    _malformed_case("missing_required_unselected", _build_required_field_descriptor_set_bytes,
                    "WithRequired", _required_valid, _encode_length_delimited_field(2, "bad"),
                    ["name", "count"]),
    _malformed_case("missing_nested_required",
                    _build_nested_required_field_descriptor_set_bytes, "WithNestedRequired",
                    # An absent optional message has no required fields to check.
                    [_nested_required_valid, _encode_varint_field(1, 300)],
                    _nested_required_missing),
    _malformed_case("missing_nested_required_unselected",
                    _build_nested_required_field_descriptor_set_bytes, "WithNestedRequired",
                    _nested_required_valid, _nested_required_missing, ["name", "inner.note"]),
    # Spark checks the root message for unknown enum values.
    _malformed_case("unknown_enum", _build_enum_descriptor_set_bytes, "EnumRoot",
                    _encode_varint_field(1, 1), _encode_varint_field(1, 999)),
    _malformed_case("unknown_enum_as_int", _build_enum_descriptor_set_bytes, "EnumRoot",
                    _encode_varint_field(1, 1), _encode_varint_field(1, 999),
                    options={"enums.as.ints": "true"}),
    _malformed_case("unknown_enum_in_unselected_field",
                    _build_enum_descriptor_set_bytes, "EnumRoot",
                    _encode_varint_field(1, 1) + _encode_varint_field(5, 1),
                    _encode_varint_field(1, 999) + _encode_varint_field(5, 2), ["id"]),
    _malformed_case("unknown_repeated_enum", _build_enum_descriptor_set_bytes, "EnumRoot",
                    _encode_varint_field(2, 10) + _encode_varint_field(2, 5),
                    _encode_varint_field(2, 1) + _encode_varint_field(2, 999)),
    _malformed_case("unterminated_packed_varint_in_unselected_field",
                    _build_packed_repeated_descriptor_set_bytes, "WithPackedRepeated",
                    _encode_varint_field(1, 7),
                    _encode_varint_field(1, 7) + _encode_length_delimited_field(2, b"\x80"),
                    ["id"]),
    _malformed_case("misaligned_packed_fixed64_in_unselected_field",
                    _build_packed_repeated_descriptor_set_bytes, "WithPackedRepeated",
                    _encode_varint_field(1, 7),
                    _encode_varint_field(1, 7) + _encode_length_delimited_field(3, b"\x00"),
                    ["id"]),
    _malformed_case("malformed_packed_in_unselected_nested_field",
                    _build_nested_packed_descriptor_set_bytes, "OuterPacked",
                    _encode_length_delimited_field(1, _encode_varint_field(1, 7)),
                    _encode_length_delimited_field(
                        1, _encode_varint_field(1, 7) +
                        _encode_length_delimited_field(2, b"\x80")),
                    ["inner.keep"]),
    _malformed_case("unterminated_varint_in_unselected_message",
                    _build_nested_descriptor_set_bytes, "WithNested", _with_nested_scalars,
                    _with_nested_scalars + _encode_length_delimited_field(
                        3, _encode_tag(99, 0) + _MALFORMED_VARINT),
                    ["simple_int", "simple_str"]),
    _malformed_case("truncated_length_in_unselected_message",
                    _build_nested_descriptor_set_bytes, "WithNested", _with_nested_scalars,
                    _with_nested_scalars + _encode_length_delimited_field(
                        3, _encode_tag(50, 2) + _encode_varint(100) + b"x"),
                    ["simple_int", "simple_str"]),
]


@pytest.mark.parametrize("build_fn,message,valid,malformed,paths,options", _malformed_cases)
@pytest.mark.parametrize("mode", _modes, ids=idfn)
@ignore_order(local=True)
def test_from_protobuf_malformed_input(
        local_tmp_path, from_protobuf_fn, build_fn, message, valid, malformed, paths, options,
        mode):
    desc = _setup_protobuf_desc(local_tmp_path, f"{message}.desc", build_fn)
    rows = [(i, row) for i, row in enumerate(valid + [malformed, None])]

    def run_on_spark(spark):
        df = spark.createDataFrame(rows, schema="idx int, bin binary")
        decoded = _decode(from_protobuf_fn, desc, message, dict(options, mode=mode))
        return df.select("idx", *_select_paths(decoded, paths))

    if mode == "FAILFAST":
        assert_gpu_and_cpu_error(lambda spark: run_on_spark(spark).collect(), {},
                                 "MALFORMED_PROTOBUF_MESSAGE")
    else:
        assert_gpu_and_cpu_are_equal_collect(run_on_spark)


@pytest.mark.parametrize("null_input", [False, True], ids=idfn)
@ignore_order(local=True)
def test_from_protobuf_permissive_malformed_rows_in_one_batch(
        local_tmp_path, from_protobuf_fn, null_input):
    """Without SPARK-46275, the GPU rebuilds struct validity only for batches with malformed
    rows, so keep valid, malformed and null rows in the same batch."""
    desc = _setup_protobuf_desc(
        local_tmp_path, "simple.desc", _build_simple_descriptor_set_bytes)
    rows = [(0, _simple_valid), (1, bytes([0x08, 0x80]))] + ([(2, None)] if null_input else [])

    def run_on_spark(spark):
        df = spark.createDataFrame(rows, schema="idx int, bin binary").coalesce(1)
        return df.select("idx", _decode(from_protobuf_fn, desc, "Simple", {"mode": "PERMISSIVE"})
                         .alias("decoded"))

    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


@pytest.mark.parametrize("malformed", [False, True], ids=["valid", "malformed"])
@allow_non_gpu("ProtobufDataToCatalyst")
def test_from_protobuf_dropmalformed_mode_falls_back(
        local_tmp_path, from_protobuf_fn, malformed):
    """With SPARK-46275, Spark rejects DROPMALFORMED only when a malformed row reaches its error
    handler."""
    desc = _setup_protobuf_desc(
        local_tmp_path, "simple.desc", _build_simple_descriptor_set_bytes)
    row = bytes([0x08, 0x80]) if malformed else _encode_varint_field(2, 42)

    def run_on_spark(spark):
        df = spark.createDataFrame([(row,)], schema="bin binary")
        return df.select(_decode(from_protobuf_fn, desc, "Simple", {"mode": "DROPMALFORMED"})
                         .alias("decoded"))

    # Without SPARK-46275, Spark rejects DROPMALFORMED during analysis, even for valid rows.
    if malformed or not _permissive_null_row_fix:
        assert_gpu_and_cpu_error(
            lambda spark: run_on_spark(spark).collect(), {}, "DROPMALFORMED")
    else:
        assert_gpu_fallback_collect(run_on_spark, "ProtobufDataToCatalyst")


# ===========================================================================
# Unsupported schemas, options and configs fall back to the CPU
# ===========================================================================

def _build_proto2_root_with_proto3_import(spark, reference_kind):
    root_file = _new_proto2_file(spark, f"proto3_{reference_kind}_root.proto")
    child_file = descriptor_pb2.FileDescriptorProto(
        name=f"proto3_{reference_kind}_child.proto", package="test", syntax="proto3")
    root_file.dependency.append(child_file.name)

    if reference_kind == "message":
        child_file.message_type.add(name="ImportedChild").field.add(
            name="value",
            number=1,
            label=descriptor_pb2.FieldDescriptorProto.LABEL_OPTIONAL,
            type=descriptor_pb2.FieldDescriptorProto.TYPE_INT32)
        type_name = ".test.ImportedChild"
        field_type = descriptor_pb2.FieldDescriptorProto.TYPE_MESSAGE
    else:
        child_enum = child_file.enum_type.add(name="ImportedStatus")
        child_enum.value.add(name="UNKNOWN", number=0)
        child_enum.value.add(name="OK", number=1)
        type_name = ".test.ImportedStatus"
        field_type = descriptor_pb2.FieldDescriptorProto.TYPE_ENUM

    root_file.message_type.add(name="Proto2Root").field.add(
        name="value",
        number=1,
        label=descriptor_pb2.FieldDescriptorProto.LABEL_OPTIONAL,
        type=field_type,
        type_name=type_name)
    fds = descriptor_pb2.FileDescriptorSet()
    fds.file.add().CopyFrom(child_file)
    fds.file.add().CopyFrom(root_file)
    return fds.SerializeToString()


def _build_aliased_enum_descriptor_set_bytes(spark):
    return _build_proto2_descriptor(spark, "aliased_enum.proto", [
        _msg("AliasedEnum", [
            _field("status", 1, "ENUM", type_name=".test.AliasedEnum.Status"),
        ], enums=[
            _enum("Status", [("UNKNOWN", 0), ("FIRST", 1), ("ALIAS", 1)],
                  allow_alias=True),
        ]),
    ])


def _build_strict_utf8_descriptor_set_bytes(spark):
    return _build_proto2_descriptor(
        spark,
        "strict_utf8.proto",
        [_msg("StrictUtf8", [_field("value", 1, "STRING")])],
        java_string_check_utf8=True)


def _build_bytes_default_descriptor_set_bytes(spark):
    return _build_proto2_descriptor(spark, "bytes_default.proto", [
        _msg("BytesDefault", [
            _field("payload", 1, "BYTES", default="abc"),
        ]),
    ])


def _build_oneof_descriptor_set_bytes(spark):
    return _build_proto2_descriptor(spark, "oneof.proto", [
        _msg("NestedChoice", [
            _field("int_value", 1, "INT32", oneof_index=0),
            _field("string_value", 2, "STRING", oneof_index=0),
        ], oneofs=["value"]),
        _msg("Choice", [
            _field("int_value", 1, "INT32", oneof_index=0),
            _field("string_value", 2, "STRING", oneof_index=0),
            _field("sibling", 3, "INT32"),
        ], oneofs=["value"]),
        _msg("OuterChoice", [
            _field("child", 1, "MESSAGE", type_name=".test.NestedChoice"),
            _field("sibling", 2, "INT32"),
        ]),
    ])


def _build_group_descriptor_set_bytes(spark):
    return _build_proto2_descriptor(spark, "group.proto", [
        _msg("Item", [_field("x", 1, "INT32")]),
        _msg("WithGroup", [
            _field("id", 1, "INT32"),
            _field("item", 2, "GROUP", label="repeated", type_name=".test.Item"),
        ]),
    ])


def _build_empty_message_field_descriptor_set_bytes(spark, label):
    return _build_proto2_descriptor(spark, f"empty_field_{label}.proto", [
        _msg("Empty", []),
        _msg("Root", [
            _field("id", 1, "INT32"),
            _field("empty", 2, "MESSAGE", label=label, type_name=".test.Empty"),
        ]),
    ])


def _fallback_case(case_id, build_fn, message, row, paths=None, options=None, conf=None):
    return pytest.param(build_fn, message, row, paths, options, conf or {}, id=case_id)


_oneof_child = _encode_varint_field(1, 7) + _encode_length_delimited_field(2, b"x")

_fallback_cases = [
    _fallback_case("proto3_imported_message",
                   lambda spark: _build_proto2_root_with_proto3_import(spark, "message"),
                   "Proto2Root", _encode_length_delimited_field(1, _encode_varint_field(1, 7))),
    _fallback_case("proto3_imported_enum",
                   lambda spark: _build_proto2_root_with_proto3_import(spark, "enum"),
                   "Proto2Root", _encode_varint_field(1, 1)),
    _fallback_case("aliased_enum_as_string", _build_aliased_enum_descriptor_set_bytes,
                   "AliasedEnum", _encode_varint_field(1, 1)),
    _fallback_case("aliased_enum_as_int", _build_aliased_enum_descriptor_set_bytes,
                   "AliasedEnum", _encode_varint_field(1, 1),
                   options={"enums.as.ints": "true"}),
    _fallback_case("strict_utf8", _build_strict_utf8_descriptor_set_bytes, "StrictUtf8",
                   b"\x0a\x02ok"),
    _fallback_case("bytes_default", _build_bytes_default_descriptor_set_bytes, "BytesDefault",
                   b""),
    _fallback_case("oneof_member", _build_oneof_descriptor_set_bytes, "Choice",
                   _oneof_child + _encode_varint_field(3, 9), ["int_value", "sibling"]),
    _fallback_case("nested_oneof_member", _build_oneof_descriptor_set_bytes, "OuterChoice",
                   _encode_length_delimited_field(1, _oneof_child) + _encode_varint_field(2, 9),
                   ["child.int_value", "sibling"]),
    # Spark drops empty-message fields from its schema but still validates them.
    _fallback_case("empty_message_field_optional",
                   lambda spark: _build_empty_message_field_descriptor_set_bytes(
                       spark, "optional"),
                   "Root", _encode_varint_field(1, 7) + _encode_length_delimited_field(2, b"")),
    _fallback_case("empty_message_field_required",
                   lambda spark: _build_empty_message_field_descriptor_set_bytes(
                       spark, "required"),
                   "Root", _encode_varint_field(1, 7) + _encode_length_delimited_field(2, b"")),
    # Groups decode to structs like messages, but use their own wire types.
    _fallback_case("group_field", _build_group_descriptor_set_bytes, "WithGroup",
                   _encode_varint_field(1, 7) + _encode_tag(2, 3) + _encode_varint_field(1, 9) +
                   _encode_tag(2, 4)),
    _fallback_case("nesting_too_deep",
                   lambda spark: _build_nested_depth_descriptor_set_bytes(spark, 12),
                   "Level1", _encode_nested_depth_message(1, 12)),
    _fallback_case("unsupported_option", _build_simple_descriptor_set_bytes, "Simple",
                   _simple_valid, options={"emit.default.values": "true"}),
    _fallback_case("disabled_by_expression_config", _build_simple_descriptor_set_bytes, "Simple",
                   _simple_valid,
                   conf={"spark.rapids.sql.expression.ProtobufDataToCatalyst": "false"}),
]


@pytest.mark.parametrize("build_fn,message,row,paths,options,conf", _fallback_cases)
@allow_non_gpu("ProtobufDataToCatalyst")
@ignore_order(local=True)
def test_from_protobuf_falls_back(
        request, local_tmp_path, from_protobuf_fn, build_fn, message, row, paths, options,
        conf):
    desc = _setup_protobuf_desc(
        local_tmp_path, f"{request.node.callspec.id}.desc", build_fn)

    def run_on_spark(spark):
        df = spark.createDataFrame([(0, row), (1, None)], schema="idx int, bin binary")
        decoded = _decode(from_protobuf_fn, desc, message, options)
        return df.select("idx", *_select_paths(decoded, paths))

    assert_gpu_fallback_collect(run_on_spark, "ProtobufDataToCatalyst", conf=conf)


# ===========================================================================
# Descriptor metadata read during planning
# ===========================================================================

@pytest.mark.parametrize("check_utf8", [False, True])
def test_protobuf_descriptor_compat(local_tmp_path, from_protobuf_fn, check_utf8):
    # proto2 test.Compat: optional int32 id = 1 [default = 7]; Status FIRST = ALIAS = 0;
    # optional Status first = 2 [default = FIRST]; optional Status alias = 3 [default = ALIAS].
    # repeated int32 items = 4; required string required_name = 5;
    # oneof choice { int32 selected = 6; }
    # message Nested { optional string value = 1; } optional Nested nested = 7;
    # message GroupContainer { optional group Legacy = 1 { optional string value = 1; } }
    # The final byte sets the file's java_string_check_utf8 option.
    desc_bytes = bytes.fromhex(
        "0ad6030a0c636f6d7061742e70726f746f12047465737422d3020a06436f6d70617412110a0269641801200128"
        "053a01375202696412300a05666972737418022001280e32132e746573742e436f6d7061742e5374617475733a"
        "0546495253545205666972737412300a05616c69617318032001280e32132e746573742e436f6d7061742e5374"
        "617475733a05414c4941535205616c69617312140a056974656d7318042003280552056974656d7312230a0d72"
        "657175697265645f6e616d65180520022809520c72657175697265644e616d65121c0a0873656c656374656418"
        "06200128054800520873656c6563746564122b0a066e657374656418072001280b32132e746573742e436f6d70"
        "61742e4e657374656452066e65737465641a1e0a064e657374656412140a0576616c7565180120012809520576"
        "616c756522220a0653746174757312090a054649525354100012090a05414c49415310001a02100142080a0663"
        "686f69636522650a0e47726f7570436f6e7461696e657212330a066c656761637918012001280a321b2e746573"
        "742e47726f7570436f6e7461696e65722e4c656761637952066c65676163791a1e0a064c656761637912140a05"
        "76616c7565180120012809520576616c75654203d801" + ("01" if check_utf8 else "00"))
    # Add a proto3 file: message Presence { optional int32 optional_value = 1;
    # oneof choice { int32 selected = 2; } int32 plain = 3; }
    desc_bytes += bytes.fromhex(
        "0a87010a0e70726573656e63652e70726f746f12047465737422670a0850726573656e6365121b0a0e6f7074"
        "696f6e616c5f76616c7565180120012805480188010112120a0873656c65637465641802200128054800120d"
        "0a05706c61696e18032001280542080a0663686f69636542110a0f5f6f7074696f6e616c5f76616c75656206"
        "70726f746f33")
    desc_path = local_tmp_path + "/compat.desc"
    with open(desc_path, "wb") as fp:
        fp.write(desc_bytes)

    def check(spark):
        decoded = call_protobuf_function(
            from_protobuf_fn, f.col("bin"), "test.Compat", desc_path, desc_bytes,
            options={"mode": "FAILFAST"})
        # Match the plugin's planning phase: replace RuntimeReplaceable, but avoid constant folding.
        source = spark.createDataFrame([(bytearray(),)], "bin binary")
        expr = source.select(decoded.alias("d"))._jdf.queryExecution() \
            .optimizedPlan().projectList().apply(0).child()
        assert expr.getClass().getName() == "org.apache.spark.sql.protobuf.ProtobufDataToCatalyst"
        cls = spark._jvm.com.nvidia.spark.rapids.ShimReflectionUtils.loadClass(
            "com.nvidia.spark.rapids.shims.SparkProtobufCompat$")
        compat = cls.getField("MODULE$").get(None)
        extracted = compat.extractExprInfo(expr)
        assert extracted.isRight(), str(extracted)
        info = extracted.toOption().get()
        assert info.messageName() == "test.Compat"
        assert info.options().apply("mode") == "FAILFAST"
        resolved = compat.resolveMessageDescriptor(info)
        assert resolved.isRight(), str(resolved)
        descriptor = resolved.toOption().get()
        assert descriptor.syntax() == "PROTO2"
        assert descriptor.javaStringCheckUtf8() == check_utf8
        assert descriptor.findField("missing").isEmpty()
        for name, number, proto_type, default in [
                ("id", 1, "INT32", 7), ("first", 2, "ENUM", "FIRST"),
                ("alias", 3, "ENUM", "ALIAS")]:
            field = descriptor.findField(name).get()
            assert (field.name(), field.fieldNumber(), field.protoTypeName()) == \
                (name, number, proto_type)
            assert not field.isRepeated() and not field.isRequired() and not field.isInOneof()
            assert field.messageDescriptor().isEmpty()
            result = field.explicitDefaultValue()
            assert result.isRight(), str(result)
            value = result.toOption().get().get()
            if proto_type == "ENUM":
                assert field.referencedTypeSyntax().get() == "PROTO2"
                assert (value.number(), value.name()) == (0, default)
                values = field.enumMetadata().get().values()
                assert [(values.apply(i).number(), values.apply(i).name())
                        for i in range(values.size())] == [(0, "FIRST"), (0, "ALIAS")]
            else:
                assert field.enumMetadata().isEmpty()
                assert field.referencedTypeSyntax().isEmpty()
                assert value.value() == default
        for name, number, proto_type, labels in [
                ("items", 4, "INT32", (True, False, False)),
                ("required_name", 5, "STRING", (False, True, False)),
                ("selected", 6, "INT32", (False, False, True)),
                ("nested", 7, "MESSAGE", (False, False, False))]:
            field = descriptor.findField(name).get()
            assert (field.fieldNumber(), field.protoTypeName()) == (number, proto_type)
            assert (field.isRepeated(), field.isRequired(), field.isInOneof()) == labels
            default = field.explicitDefaultValue()
            assert default.isRight() and default.toOption().get().isEmpty()
            assert field.enumMetadata().isEmpty()
            if proto_type == "MESSAGE":
                assert field.referencedTypeSyntax().get() == "PROTO2"
                nested = field.messageDescriptor().get()
                assert nested.syntax() == "PROTO2"
                assert nested.javaStringCheckUtf8() == check_utf8
                nested_field = nested.findField("value").get()
                assert (nested_field.fieldNumber(), nested_field.protoTypeName()) == (1, "STRING")
                assert nested.findField("missing").isEmpty()
            else:
                assert field.messageDescriptor().isEmpty()
                assert field.referencedTypeSyntax().isEmpty()
        # Inspect GROUP metadata without requiring Spark SQL to support group decoding.
        group_info = info.copy("test.GroupContainer", info.descriptorSource(), info.options())
        group_result = compat.resolveMessageDescriptor(group_info)
        assert group_result.isRight(), str(group_result)
        group = group_result.toOption().get().findField("legacy").get()
        assert (group.fieldNumber(), group.protoTypeName()) == (1, "GROUP")
        assert group.referencedTypeSyntax().get() == "PROTO2"
        nested = group.messageDescriptor().get()
        assert nested.syntax() == "PROTO2"
        assert nested.javaStringCheckUtf8() == check_utf8
        value = nested.findField("value").get()
        assert (value.fieldNumber(), value.protoTypeName()) == (1, "STRING")
        assert nested.findField("missing").isEmpty()
        missing = info.copy("test.Missing", info.descriptorSource(), info.options())
        assert compat.resolveMessageDescriptor(missing).isLeft()
        presence_info = info.copy("test.Presence", info.descriptorSource(), info.options())
        presence_result = compat.resolveMessageDescriptor(presence_info)
        assert presence_result.isRight(), str(presence_result)
        presence = presence_result.toOption().get()
        assert presence.syntax() == "PROTO3"
        for name, in_oneof in [("optional_value", False), ("selected", True), ("plain", False)]:
            assert presence.findField(name).get().isInOneof() == in_oneof, name

    # This exercises the CPU-side metadata API, not execution of a GPU expression.
    with_cpu_session(check)
