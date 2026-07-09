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
    BooleanGen, IntegerGen, LongGen, FloatGen, DoubleGen, StringGen, BinaryGen,
    gen_df, idfn
)
from marks import allow_non_gpu, ignore_order, validate_execs_in_gpu_plan
from protobuf_data_gen import pb, encode_pb_message
from spark_session import (
    is_before_spark_340,
    is_spark_protobuf_available,
    is_spark_protobuf_descriptor_runtime_available,
    with_cpu_session,
)
import pyspark.sql.functions as f
from pyspark.sql.window import Window
from pyspark.sql.types import (
    BinaryType,
    BooleanType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    StringType,
)

pytestmark = [
    pytest.mark.premerge_ci_1,
    pytest.mark.skipif(
        not is_spark_protobuf_available(),
        reason="from_protobuf is unavailable"),
]


def _scalar(name, field_number, gen, encoding='default', default=None):
    spark_type = gen.data_type
    if isinstance(spark_type, BooleanType):
        return pb.bool(name, field_number, gen=gen, default=default)
    if isinstance(spark_type, IntegerType):
        if encoding == 'fixed':
            return pb.fixed32(name, field_number, gen=gen, default=default)
        if encoding == 'zigzag':
            return pb.sint32(name, field_number, gen=gen, default=default)
        return pb.int32(name, field_number, gen=gen, default=default)
    if isinstance(spark_type, LongType):
        if encoding == 'fixed':
            return pb.fixed64(name, field_number, gen=gen, default=default)
        if encoding == 'zigzag':
            return pb.sint64(name, field_number, gen=gen, default=default)
        return pb.int64(name, field_number, gen=gen, default=default)
    if isinstance(spark_type, FloatType):
        return pb.float(name, field_number, gen=gen, default=default)
    if isinstance(spark_type, DoubleType):
        return pb.double(name, field_number, gen=gen, default=default)
    if isinstance(spark_type, StringType):
        return pb.string(name, field_number, gen=gen, default=default)
    if isinstance(spark_type, BinaryType):
        return pb.bytes(name, field_number, gen=gen, default=default)
    raise ValueError(f"Unsupported DataGen for protobuf scalar: {spark_type}")


def _nested(name, field_number, children):
    return pb.nested(name, field_number, children)


def _repeated(name, field_number, element_gen, packed=False,
              encoding='default', min_len=0, max_len=5):
    return pb.repeated(
        _scalar(name, field_number, element_gen, encoding=encoding),
        min_len=min_len,
        max_len=max_len,
        packed=packed)


def _repeated_message(name, field_number, children, min_len=0, max_len=5):
    return pb.repeated_message(
        name, field_number, children, min_len=min_len, max_len=max_len)


def _schema(name, fields):
    return pb.message(name, fields)


def _as_datagen(fields, binary_col_name="bin", schema_name="Generated"):
    return _schema(schema_name, fields).as_datagen(binary_col_name=binary_col_name)


# Random data generation configurations for simple scalars
_random_scalar_test_configs = [
    # (test_id, data_gen_config)
    ("all_types", [
        _scalar("b", 1, BooleanGen()),
        _scalar("i32", 2, IntegerGen()),
        _scalar("i64", 3, LongGen()),
        _scalar("f32", 4, FloatGen()),
        _scalar("f64", 5, DoubleGen()),
        _scalar("s", 6, StringGen()),
    ]),
    ("integers_edge_cases", [
        _scalar("b", 1, BooleanGen()),
        _scalar("i32", 2, IntegerGen(
            min_val=-2147483648, max_val=2147483647,
            special_cases=[-2147483648, -1, 0, 1, 2147483647])),
        _scalar("i64", 3, LongGen(
            min_val=-9223372036854775808, max_val=9223372036854775807,
            special_cases=[-9223372036854775808, -1, 0, 1, 9223372036854775807])),
        _scalar("f32", 4, FloatGen()),
        _scalar("f64", 5, DoubleGen()),
        _scalar("s", 6, StringGen()),
    ]),
]


# ---------------------------------------------------------------------------
# Shared fixture and helpers to reduce per-test boilerplate
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def from_protobuf_fn():
    """Return the runtime-provided PySpark API."""
    from pyspark.sql.protobuf.functions import from_protobuf
    return from_protobuf


def _setup_protobuf_desc(local_tmp_path, desc_name, build_fn):
    """Build descriptor bytes and write them to the driver's local filesystem."""
    if not is_spark_protobuf_descriptor_runtime_available():
        pytest.skip("Spark protobuf descriptor runtime is unavailable")
    desc_path = os.path.join(local_tmp_path, desc_name)
    desc_bytes = with_cpu_session(build_fn)
    with open(desc_path, "wb") as fp:
        fp.write(desc_bytes)
    return desc_path, desc_bytes


def _call_from_protobuf(from_protobuf_fn, col, message_name,
                         desc_path, desc_bytes, options=None):
    """Call from_protobuf using the right API variant."""
    sig = inspect.signature(from_protobuf_fn)
    if "binaryDescriptorSet" in sig.parameters:
        kw = dict(binaryDescriptorSet=bytearray(desc_bytes))
        if options is not None:
            kw["options"] = options
        return from_protobuf_fn(col, message_name, **kw)
    if options is not None:
        return from_protobuf_fn(col, message_name, desc_path, options)
    return from_protobuf_fn(col, message_name, desc_path)


def test_call_from_protobuf_preserves_options_for_legacy_signature():
    calls = []

    def fake_from_protobuf(col, message_name, desc_path, *args):
        calls.append((col, message_name, desc_path, args))
        return "ok"

    options = {"enums.as.ints": "true"}
    result = _call_from_protobuf(
        fake_from_protobuf, "col", "msg", "/tmp/test.desc", b"desc", options=options)

    assert result == "ok"
    assert calls == [("col", "msg", "/tmp/test.desc", (options,))]


def test_encode_protobuf_packed_repeated_fixed_uses_unsigned_twos_complement():
    i32_schema = pb.message("PackedFixed32", [
        pb.repeated(pb.fixed32("values", 1), packed=True),
    ])
    i64_schema = pb.message("PackedFixed64", [
        pb.repeated(pb.fixed64("values", 1), packed=True),
    ])

    assert i32_schema.encode({"values": [0xFFFFFFFF]}) == \
        b"\x0a\x04" + struct.pack("<I", 0xFFFFFFFF)
    assert i64_schema.encode({"values": [0xFFFFFFFFFFFFFFFF]}) == \
        b"\x0a\x08" + struct.pack("<Q", 0xFFFFFFFFFFFFFFFF)


def _build_simple_descriptor_set_bytes(spark):
    """Build a simple scalar proto2 descriptor."""
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


def _new_proto2_file(spark, name):
    """Create a descriptor builder without requiring an unshaded protobuf runtime."""
    D = spark.sparkContext._jvm.org.sparkproject.spark_protobuf.protobuf.DescriptorProtos
    fd = D.FileDescriptorProto.newBuilder() \
        .setName(name) \
        .setPackage("test")
    try:
        fd = fd.setSyntax("proto2")
    except Exception:
        pass
    return D, fd


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


def _build_proto2_descriptor(spark, filename, messages, file_enums=None):
    """Build FileDescriptorSet bytes from declarative message and enum specs."""
    D, fd = _new_proto2_file(spark, filename)
    type_map = {
        "BOOL": D.FieldDescriptorProto.Type.TYPE_BOOL,
        "INT32": D.FieldDescriptorProto.Type.TYPE_INT32,
        "INT64": D.FieldDescriptorProto.Type.TYPE_INT64,
        "UINT32": D.FieldDescriptorProto.Type.TYPE_UINT32,
        "UINT64": D.FieldDescriptorProto.Type.TYPE_UINT64,
        "SINT32": D.FieldDescriptorProto.Type.TYPE_SINT32,
        "SINT64": D.FieldDescriptorProto.Type.TYPE_SINT64,
        "FIXED32": D.FieldDescriptorProto.Type.TYPE_FIXED32,
        "FIXED64": D.FieldDescriptorProto.Type.TYPE_FIXED64,
        "SFIXED32": D.FieldDescriptorProto.Type.TYPE_SFIXED32,
        "SFIXED64": D.FieldDescriptorProto.Type.TYPE_SFIXED64,
        "FLOAT": D.FieldDescriptorProto.Type.TYPE_FLOAT,
        "DOUBLE": D.FieldDescriptorProto.Type.TYPE_DOUBLE,
        "STRING": D.FieldDescriptorProto.Type.TYPE_STRING,
        "BYTES": D.FieldDescriptorProto.Type.TYPE_BYTES,
        "MESSAGE": D.FieldDescriptorProto.Type.TYPE_MESSAGE,
        "ENUM": D.FieldDescriptorProto.Type.TYPE_ENUM,
    }
    label_map = {
        "optional": D.FieldDescriptorProto.Label.LABEL_OPTIONAL,
        "repeated": D.FieldDescriptorProto.Label.LABEL_REPEATED,
        "required": D.FieldDescriptorProto.Label.LABEL_REQUIRED,
    }

    def _default_literal(value):
        if isinstance(value, bool):
            return "true" if value else "false"
        return str(value)

    def _build_enum(enum_spec):
        enum_builder = D.EnumDescriptorProto.newBuilder().setName(enum_spec["name"])
        if enum_spec.get("allow_alias", False):
            enum_builder.setOptions(
                D.EnumOptions.newBuilder().setAllowAlias(True).build())
        for value_name, value_number in enum_spec["values"]:
            enum_builder.addValue(
                D.EnumValueDescriptorProto.newBuilder()
                    .setName(value_name)
                    .setNumber(value_number)
                    .build()
            )
        return enum_builder.build()

    packed_options = D.FieldOptions.newBuilder().setPacked(True).build()

    for enum_spec in file_enums or []:
        fd.addEnumType(_build_enum(enum_spec))

    for message_spec in messages:
        message_builder = D.DescriptorProto.newBuilder().setName(message_spec["name"])
        for oneof_name in message_spec.get("oneofs", []):
            message_builder.addOneofDecl(
                D.OneofDescriptorProto.newBuilder().setName(oneof_name).build())
        for enum_spec in message_spec["enums"]:
            message_builder.addEnumType(_build_enum(enum_spec))
        for field_spec in message_spec["fields"]:
            field_builder = (
                D.FieldDescriptorProto.newBuilder()
                    .setName(field_spec["name"])
                    .setNumber(field_spec["number"])
                    .setLabel(label_map[field_spec["label"]])
                    .setType(type_map[field_spec["type"]])
            )
            if field_spec["type_name"] is not None:
                field_builder.setTypeName(field_spec["type_name"])
            if field_spec["default"] is not None:
                field_builder.setDefaultValue(_default_literal(field_spec["default"]))
            if field_spec["packed"]:
                field_builder.setOptions(packed_options)
            if field_spec.get("oneof_index") is not None:
                field_builder.setOneofIndex(field_spec["oneof_index"])
            message_builder.addField(field_builder.build())
        fd.addMessageType(message_builder.build())

    fds = D.FileDescriptorSet.newBuilder().addFile(fd.build()).build()
    return bytes(fds.toByteArray())


def _build_proto2_root_with_proto3_import(spark, reference_kind):
    D, root_file = _new_proto2_file(spark, f"proto3_{reference_kind}_root.proto")
    child_file = (D.FileDescriptorProto.newBuilder()
                  .setName(f"proto3_{reference_kind}_child.proto")
                  .setPackage("test")
                  .setSyntax("proto3"))
    root_file.addDependency(f"proto3_{reference_kind}_child.proto")

    if reference_kind == "message":
        child_file.addMessageType(
            D.DescriptorProto.newBuilder()
                .setName("ImportedChild")
                .addField(D.FieldDescriptorProto.newBuilder()
                          .setName("value")
                          .setNumber(1)
                          .setLabel(D.FieldDescriptorProto.Label.LABEL_OPTIONAL)
                          .setType(D.FieldDescriptorProto.Type.TYPE_INT32)
                          .build())
                .build())
        type_name = ".test.ImportedChild"
        field_type = D.FieldDescriptorProto.Type.TYPE_MESSAGE
    else:
        child_file.addEnumType(
            D.EnumDescriptorProto.newBuilder()
                .setName("ImportedStatus")
                .addValue(D.EnumValueDescriptorProto.newBuilder()
                          .setName("UNKNOWN").setNumber(0).build())
                .addValue(D.EnumValueDescriptorProto.newBuilder()
                          .setName("OK").setNumber(1).build())
                .build())
        type_name = ".test.ImportedStatus"
        field_type = D.FieldDescriptorProto.Type.TYPE_ENUM

    root_file.addMessageType(
        D.DescriptorProto.newBuilder()
            .setName("Proto2Root")
            .addField(D.FieldDescriptorProto.newBuilder()
                      .setName("value")
                      .setNumber(1)
                      .setLabel(D.FieldDescriptorProto.Label.LABEL_OPTIONAL)
                      .setType(field_type)
                      .setTypeName(type_name)
                      .build())
            .build())
    fds = (D.FileDescriptorSet.newBuilder()
           .addFile(child_file.build())
           .addFile(root_file.build())
           .build())
    return bytes(fds.toByteArray())


@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@pytest.mark.parametrize("reference_kind", ["message", "enum"], ids=idfn)
@allow_non_gpu("ProtobufDataToCatalyst")
def test_from_protobuf_proto3_import_falls_back(
        local_tmp_path, from_protobuf_fn, reference_kind):
    desc_path, desc_bytes = _setup_protobuf_desc(
        local_tmp_path,
        f"proto3_{reference_kind}_import.desc",
        lambda spark: _build_proto2_root_with_proto3_import(spark, reference_kind))
    if reference_kind == "message":
        child = _encode_tag(1, 0) + _encode_varint(7)
        row = _encode_tag(1, 2) + _encode_varint(len(child)) + child
    else:
        row = _encode_tag(1, 0) + _encode_varint(1)

    def run_on_spark(spark):
        df = spark.createDataFrame([(row,)], schema="bin binary")
        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), "test.Proto2Root", desc_path, desc_bytes)
        return df.select(decoded.alias("decoded"))

    assert_gpu_fallback_collect(run_on_spark, "ProtobufDataToCatalyst")


def _build_oneof_descriptor_set_bytes(spark):
    """Build a proto2 descriptor containing a oneof plus an independent sibling."""
    return _build_proto2_descriptor(spark, "oneof.proto", [
        _msg("Choice", [
            _field("int_value", 1, "INT32", oneof_index=0),
            _field("string_value", 2, "STRING", oneof_index=0),
            _field("sibling", 3, "INT32"),
        ], oneofs=["value"]),
    ])


def _build_nested_oneof_descriptor_set_bytes(spark):
    return _build_proto2_descriptor(spark, "nested_oneof.proto", [
        _msg("NestedChoice", [
            _field("int_value", 1, "INT32", oneof_index=0),
            _field("string_value", 2, "STRING", oneof_index=0),
        ], oneofs=["value"]),
        _msg("OuterChoice", [
            _field("child", 1, "MESSAGE", type_name=".test.NestedChoice"),
            _field("sibling", 2, "INT32"),
        ]),
    ])


def _build_oneof_message_descriptor_set_bytes(spark):
    return _build_proto2_descriptor(spark, "oneof_message.proto", [
        _msg("OneofChild", [
            _field("value", 1, "INT32"),
        ]),
        _msg("ChoiceWithMessage", [
            _field("child", 1, "MESSAGE", type_name=".test.OneofChild", oneof_index=0),
            _field("scalar", 2, "INT32", oneof_index=0),
            _field("sibling", 3, "INT32"),
        ], oneofs=["choice"]),
    ])


@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@ignore_order(local=True)
def test_from_protobuf_simple_parquet_binary_round_trip(
        spark_tmp_path, local_tmp_path, from_protobuf_fn):
    data_path = spark_tmp_path + "/PROTOBUF_SIMPLE_PARQUET/"
    message_name = "test.Simple"
    desc_path, desc_bytes = _setup_protobuf_desc(
        local_tmp_path, "simple.desc", _build_simple_descriptor_set_bytes)

    # Build a DF with scalar columns + binary protobuf column and write to parquet
    row_gen = _as_datagen([
        _scalar("b", 1, BooleanGen(nullable=True)),
        _scalar("i32", 2, IntegerGen(nullable=True, min_val=0, max_val=1 << 20)),
        _scalar("i64", 3, LongGen(nullable=True, min_val=0, max_val=1 << 40, special_cases=[])),
        _scalar("f32", 4, FloatGen(nullable=True, no_nans=True)),
        _scalar("f64", 5, DoubleGen(nullable=True, no_nans=True)),
        _scalar("s", 6, StringGen(nullable=True)),
    ], binary_col_name="bin")

    def write_parquet(spark):
        df = gen_df(spark, row_gen)
        df.write.mode("overwrite").parquet(data_path)

    with_cpu_session(write_parquet)

    # Sanity check correctness on CPU (decoded struct matches the original scalar columns)
    def cpu_correctness_check(spark):
        df = spark.read.parquet(data_path)
        expected = f.struct(
            f.col("b").alias("b"),
            f.col("i32").alias("i32"),
            f.col("i64").alias("i64"),
            f.col("f32").alias("f32"),
            f.col("f64").alias("f64"),
            f.col("s").alias("s"),
        ).alias("expected")
        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), message_name,
            desc_path, desc_bytes).alias("decoded")
        rows = df.select(expected, decoded).collect()
        for r in rows:
            assert r["expected"] == r["decoded"]

    with_cpu_session(cpu_correctness_check)

    # Main assertion: CPU and GPU results match for from_protobuf on a binary column read from parquet
    def run_on_spark(spark):
        df = spark.read.parquet(data_path)
        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), message_name, desc_path, desc_bytes)
        return df.select(decoded.alias("decoded"))

    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


def _build_nested_descriptor_set_bytes(spark):
    """Build a descriptor for a message with a nested child struct."""
    return _build_proto2_descriptor(spark, "nested.proto", [
        _msg("Nested", [_field("x", 1, "INT32")]),
        _msg("WithNested", [
            _field("simple_int", 1, "INT32"),
            _field("simple_str", 2, "STRING"),
            _field("nested_msg", 3, "MESSAGE", type_name=".test.Nested"),
            _field("simple_long", 4, "INT64"),
        ]),
    ])


@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@ignore_order(local=True)
def test_from_protobuf_schema_projection_simple_fields_only(local_tmp_path, from_protobuf_fn):
    """
    Test schema projection: when only simple fields are selected from a protobuf message
    that also contains unsupported types (nested message), GPU should be able to decode
    just the simple fields without falling back to CPU.
    """
    desc_path, desc_bytes = _setup_protobuf_desc(
        local_tmp_path, "nested.desc", _build_nested_descriptor_set_bytes)
    message_name = "test.WithNested"

    data_gen = _as_datagen([
        _scalar("simple_int", 1, IntegerGen()),
        _scalar("simple_str", 2, StringGen()),
        _scalar("simple_long", 4, LongGen()),
    ])

    def run_on_spark(spark):
        df = gen_df(spark, data_gen)
        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), message_name, desc_path, desc_bytes)
        # Only select simple fields, not the nested_msg field
        return df.select(
            decoded.getField("simple_int").alias("simple_int"),
            decoded.getField("simple_str").alias("simple_str"),
            decoded.getField("simple_long").alias("simple_long")
        )

    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


def _build_enum_descriptor_set_bytes(spark):
    """Build a descriptor with a message-local enum field."""
    return _build_proto2_descriptor(spark, "enum.proto", [
        _msg("WithEnum", [
            _field("color", 1, "ENUM", type_name=".test.WithEnum.Color"),
            _field("count", 2, "INT32"),
            _field("name", 3, "STRING"),
        ], enums=[
            _enum("Color", [("RED", 0), ("GREEN", 1), ("BLUE", 2)]),
        ]),
    ])


def _build_nested_enum_descriptor_set_bytes(spark):
    """Build a descriptor with a nested message that owns an enum field."""
    return _build_proto2_descriptor(spark, "nested_enum.proto", [
        _msg("Detail", [
            _field("status", 1, "ENUM", type_name=".test.Detail.Status"),
            _field("count", 2, "INT32"),
        ], enums=[
            _enum("Status", [("UNKNOWN", 0), ("OK", 1), ("BAD", 2)]),
        ]),
        _msg("WithNestedEnum", [
            _field("id", 1, "INT32"),
            _field("detail", 2, "MESSAGE", type_name=".test.Detail"),
            _field("name", 3, "STRING"),
        ]),
    ])


@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@pytest.mark.parametrize("enum_case", [
    "as_int",
    "unknown_as_int",
    "as_string",
    "unknown_as_string",
], ids=lambda x: x)
@ignore_order(local=True)
def test_from_protobuf_enum_cases(local_tmp_path, from_protobuf_fn, enum_case):
    """Parametrized enum decoding tests for int/string modes and unknown values."""
    desc_path, desc_bytes = _setup_protobuf_desc(
        local_tmp_path, "enum.desc", _build_enum_descriptor_set_bytes)
    message_name = "test.WithEnum"
    color_enum = pb.enum_type("Color", [("RED", 0), ("GREEN", 1), ("BLUE", 2)])
    full_schema = _schema("WithEnumManual", [
        pb.enum_field("color", 1, color_enum),
        _scalar("count", 2, IntegerGen()),
        _scalar("name", 3, StringGen()),
    ])
    partial_schema = _schema("WithEnumManualPartial", [
        pb.enum_field("color", 1, color_enum),
        _scalar("count", 2, IntegerGen()),
    ])

    if enum_case == "as_int":
        rows = [
            (encode_pb_message(full_schema, {"color": 1, "count": 42, "name": "test"}),),
            (encode_pb_message(full_schema, {"color": 0, "count": 100}),),
            (encode_pb_message(full_schema, {"count": 200, "name": "hello"}),),
            (None,),
        ]
        options = {"enums.as.ints": "true"}
        select_mode = "fields3"
    elif enum_case == "unknown_as_int":
        rows = [(encode_pb_message(partial_schema, {"color": 999, "count": 42}),)]
        options = {"enums.as.ints": "true", "mode": "PERMISSIVE"}
        select_mode = "fields2"
    elif enum_case == "as_string":
        rows = [
            (encode_pb_message(full_schema, {"color": 1, "count": 42, "name": "test"}),),
            (encode_pb_message(full_schema, {"color": 0, "count": 100}),),
            (encode_pb_message(full_schema, {"color": 2, "count": 200, "name": "hello"}),),
            (encode_pb_message(full_schema, {"count": 300, "name": "world"}),),
            (None,),
        ]
        options = None
        select_mode = "fields3"
    else:
        rows = [
            (encode_pb_message(partial_schema, {"color": 1, "count": 10}),),
            (encode_pb_message(partial_schema, {"color": 999, "count": 20}),),
            (encode_pb_message(partial_schema, {"color": 2, "count": 30}),),
        ]
        options = {"mode": "PERMISSIVE"}
        select_mode = "decoded"

    def run_on_spark(spark):
        df = spark.createDataFrame(rows, schema="bin binary")
        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), message_name, desc_path, desc_bytes,
            options=options)
        if select_mode == "fields3":
            return df.select(
                decoded.getField("color").alias("color"),
                decoded.getField("count").alias("count"),
                decoded.getField("name").alias("name"))
        if select_mode == "fields2":
            return df.select(
                decoded.getField("color").alias("color"),
                decoded.getField("count").alias("count"))
        return df.select(decoded.alias("decoded"))

    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


def _build_out_of_order_enum_descriptor_set_bytes(spark):
    return _build_proto2_descriptor(spark, "out_of_order_enum.proto", [
        _msg("OutOfOrderEnum", [
            _field("status", 1, "ENUM", type_name=".test.OutOfOrderEnum.Status"),
        ], enums=[
            _enum("Status", [("TEN", 10), ("ONE", 1), ("FIVE", 5)]),
        ]),
    ])


@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@pytest.mark.parametrize("options", [None, {"enums.as.ints": "true"}],
                         ids=["string", "int"])
def test_from_protobuf_out_of_order_enum_values(
        local_tmp_path, from_protobuf_fn, options):
    desc_path, desc_bytes = _setup_protobuf_desc(
        local_tmp_path, "out_of_order_enum.desc",
        _build_out_of_order_enum_descriptor_set_bytes)
    rows = [
        (_encode_tag(1, 0) + _encode_varint(value),)
        for value in (10, 1, 5)
    ]

    def run_on_spark(spark):
        df = spark.createDataFrame(rows, schema="bin binary")
        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), "test.OutOfOrderEnum",
            desc_path, desc_bytes, options=options)
        return df.select(decoded.getField("status").alias("status"))

    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


def _build_aliased_enum_descriptor_set_bytes(spark):
    return _build_proto2_descriptor(spark, "aliased_enum.proto", [
        _msg("AliasedEnum", [
            _field("status", 1, "ENUM", type_name=".test.AliasedEnum.Status"),
        ], enums=[
            _enum("Status", [("UNKNOWN", 0), ("FIRST", 1), ("ALIAS", 1)],
                  allow_alias=True),
        ]),
    ])


@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@pytest.mark.parametrize("options", [None, {"enums.as.ints": "true"}],
                         ids=["string", "int"])
@allow_non_gpu("ProtobufDataToCatalyst")
def test_from_protobuf_aliased_enum_falls_back(
        local_tmp_path, from_protobuf_fn, options):
    desc_path, desc_bytes = _setup_protobuf_desc(
        local_tmp_path, "aliased_enum.desc", _build_aliased_enum_descriptor_set_bytes)
    row = _encode_tag(1, 0) + _encode_varint(1)

    def run_on_spark(spark):
        df = spark.createDataFrame([(row,)], schema="bin binary")
        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), "test.AliasedEnum",
            desc_path, desc_bytes, options=options)
        return df.select(decoded.alias("decoded"))

    assert_gpu_fallback_collect(run_on_spark, "ProtobufDataToCatalyst")


@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@ignore_order(local=True)
def test_from_protobuf_nested_enum_valid_and_missing_values(local_tmp_path, from_protobuf_fn):
    """
    Nested enum decode parity test:
      - valid nested enum values decode to names in string mode
      - missing nested enum stays null
    This protects nested enum metadata propagation in GPU schema flattening.
    """
    desc_path, desc_bytes = _setup_protobuf_desc(
        local_tmp_path, "nested_enum.desc", _build_nested_enum_descriptor_set_bytes)
    message_name = "test.WithNestedEnum"
    status_enum = pb.enum_type("Status", [("UNKNOWN", 0), ("OK", 1), ("BAD", 2)])
    schema = _schema("WithNestedEnumManual", [
        _scalar("id", 1, IntegerGen()),
        _nested("detail", 2, [
            pb.enum_field("status", 1, status_enum),
            _scalar("count", 2, IntegerGen()),
        ]),
        _scalar("name", 3, StringGen(nullable=True)),
    ])
    rows = [
        (0, encode_pb_message(schema, {"id": 1, "detail": {"status": 1, "count": 10}, "name": "ok"})),
        (1, encode_pb_message(schema, {"id": 2, "detail": {"status": 2, "count": 20}, "name": "bad"})),
        (2, encode_pb_message(schema, {"id": 3, "detail": {"count": 30}, "name": "none"})),
        (3, None),
    ]

    def run_on_spark(spark):
        df = spark.createDataFrame(rows, schema="idx int, bin binary")
        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), message_name, desc_path, desc_bytes,
            options={"mode": "PERMISSIVE"})
        return df.select(
            f.col("idx"),
            decoded.isNull().alias("decoded_is_null"),
            decoded.getField("id").alias("id"),
            decoded.getField("detail").getField("status").alias("status"),
            decoded.getField("name").alias("name")).orderBy("idx")

    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@pytest.mark.parametrize("options", [
    {"mode": "PERMISSIVE"},
    {"mode": "PERMISSIVE", "enums.as.ints": "true"},
    {"mode": "FAILFAST"},
    {"mode": "FAILFAST", "enums.as.ints": "true"},
], ids=["permissive-string", "permissive-int", "failfast-string", "failfast-int"])
@ignore_order(local=True)
def test_from_protobuf_nested_unknown_enum_preserves_sibling_fields(
        local_tmp_path, from_protobuf_fn, options):
    desc_path, desc_bytes = _setup_protobuf_desc(
        local_tmp_path, "nested_enum.desc", _build_nested_enum_descriptor_set_bytes)
    message_name = "test.WithNestedEnum"

    detail_valid = (_encode_tag(1, 0) + _encode_varint(1) +
                    _encode_tag(2, 0) + _encode_varint(10))
    detail_unknown = (_encode_tag(1, 0) + _encode_varint(999) +
                      _encode_tag(2, 0) + _encode_varint(20))
    row_valid = (_encode_tag(1, 0) + _encode_varint(1) +
                 _encode_tag(2, 2) + _encode_varint(len(detail_valid)) + detail_valid +
                 _encode_tag(3, 2) + _encode_varint(2) + b"ok")
    row_unknown = (_encode_tag(1, 0) + _encode_varint(2) +
                   _encode_tag(2, 2) + _encode_varint(len(detail_unknown)) + detail_unknown +
                   _encode_tag(3, 2) + _encode_varint(3) + b"bad")

    def run_on_spark(spark):
        df = spark.createDataFrame([(0, row_valid), (1, row_unknown)], schema="idx int, bin binary")
        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), message_name, desc_path, desc_bytes,
            options=options)
        return df.select(
            f.col("idx"),
            decoded.isNull().alias("decoded_is_null"),
            decoded.getField("id").alias("id"),
            decoded.getField("detail").getField("status").alias("status"),
            decoded.getField("detail").getField("count").alias("count"),
            decoded.getField("name").alias("name")).orderBy("idx")

    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


def _build_nested_enum_default_struct_descriptor_set_bytes(spark):
    """Build a descriptor with file-level enum defaults inside a nested struct."""
    return _build_proto2_descriptor(
        spark,
        "nested_enum_defaults.proto",
        [
            _msg("CommonWithEnumDefaults", [
                _field("logid", 1, "STRING"),
                _field("language", 2, "ENUM", type_name=".test.Language", default="EN"),
                _field("code_type", 3, "ENUM", type_name=".test.CodeType", default="UTF8"),
            ]),
            _msg("OuterWithCommonEnumDefaults", [
                _field("id", 1, "INT32"),
                _field("common", 2, "MESSAGE", type_name=".test.CommonWithEnumDefaults"),
            ]),
        ],
        file_enums=[
            _enum("Language", [
                ("UNKNOWN_LANGUAGE", 0),
                ("EN", 1),
                ("ZH", 2),
            ]),
            _enum("CodeType", [
                ("UNKNOWN_CODE", 0),
                ("UTF8", 1),
                ("GBK", 2),
            ]),
        ],
    )


@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@ignore_order(local=True)
def test_from_protobuf_nested_enum_defaults_string_mode(local_tmp_path, from_protobuf_fn):
    """Regression test: nested enum defaults in string mode must not crash GPU planning.

    Selecting the whole nested struct forces GPU planning to visit all of its children,
    including enum fields whose proto2 defaults are represented as EnumValueDescriptor
    objects by spark-protobuf.
    """
    desc_path, desc_bytes = _setup_protobuf_desc(
        local_tmp_path, "nested_enum_defaults.desc",
        _build_nested_enum_default_struct_descriptor_set_bytes)
    message_name = "test.OuterWithCommonEnumDefaults"

    common_with_logid = (_encode_tag(1, 2) + _encode_varint(5) + b"log-1")
    row_logid_only = (_encode_tag(1, 0) + _encode_varint(1) +
                      _encode_tag(2, 2) + _encode_varint(len(common_with_logid)) +
                      common_with_logid)
    row_empty_common = (_encode_tag(1, 0) + _encode_varint(2) +
                        _encode_tag(2, 2) + _encode_varint(0))
    row_no_common = _encode_tag(1, 0) + _encode_varint(3)

    def run_on_spark(spark):
        df = spark.createDataFrame(
            [(row_logid_only,), (row_empty_common,), (row_no_common,)],
            schema="bin binary")
        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), message_name, desc_path, desc_bytes)
        return df.select(
            decoded.getField("id").alias("id"),
            decoded.getField("common").alias("common"))

    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


def _build_repeated_enum_descriptor_set_bytes(spark):
    """Build a descriptor with a repeated enum field."""
    return _build_proto2_descriptor(spark, "repeated_enum.proto", [
        _msg("WithRepeatedEnum", [
            _field("id", 1, "INT32"),
            _field("priority", 2, "ENUM", label="repeated",
                   type_name=".test.WithRepeatedEnum.Priority"),
        ], enums=[
            _enum("Priority", [("UNKNOWN", 0), ("FOO", 1), ("BAR", 2)]),
        ]),
    ])


@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@ignore_order(local=True)
def test_from_protobuf_repeated_enum_invalid_permissive(local_tmp_path, from_protobuf_fn):
    """Repeated enum with invalid values in PERMISSIVE mode.

    Row with any invalid enum value should become null (entire struct row),
    while rows with all-valid enum values decode normally.
    """
    desc_path, desc_bytes = _setup_protobuf_desc(
        local_tmp_path, "repeated_enum.desc", _build_repeated_enum_descriptor_set_bytes)
    message_name = "test.WithRepeatedEnum"

    row1 = (_encode_tag(1, 0) + _encode_varint(1) +
            _encode_tag(2, 0) + _encode_varint(0) +
            _encode_tag(2, 0) + _encode_varint(1) +
            _encode_tag(2, 0) + _encode_varint(2))
    row2 = (_encode_tag(1, 0) + _encode_varint(2) +
            _encode_tag(2, 0) + _encode_varint(1) +
            _encode_tag(2, 0) + _encode_varint(99))
    row3 = (_encode_tag(1, 0) + _encode_varint(3) +
            _encode_tag(2, 0) + _encode_varint(0))

    def run_on_spark(spark):
        df = spark.createDataFrame(
            [(row1,), (row2,), (row3,)], schema="bin binary")
        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), message_name, desc_path, desc_bytes,
            options={"mode": "PERMISSIVE"})
        return df.select(decoded.alias("decoded"))

    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@ignore_order(local=True)
def test_from_protobuf_repeated_enum_string_invalid_permissive_nulls_sibling_fields(
        local_tmp_path, from_protobuf_fn):
    """Repeated enum string mode must null sibling fields when any enum value is invalid."""
    desc_path, desc_bytes = _setup_protobuf_desc(
        local_tmp_path, "repeated_enum.desc", _build_repeated_enum_descriptor_set_bytes)
    message_name = "test.WithRepeatedEnum"

    row_valid = (_encode_tag(1, 0) + _encode_varint(1) +
                 _encode_tag(2, 0) + _encode_varint(0) +
                 _encode_tag(2, 0) + _encode_varint(2))
    row_invalid = (_encode_tag(1, 0) + _encode_varint(2) +
                   _encode_tag(2, 0) + _encode_varint(1) +
                   _encode_tag(2, 0) + _encode_varint(99))

    def run_on_spark(spark):
        df = spark.createDataFrame([(0, row_valid), (1, row_invalid)], schema="idx int, bin binary")
        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), message_name, desc_path, desc_bytes,
            options={"mode": "PERMISSIVE"})
        return df.select(
            f.col("idx"),
            decoded.isNull().alias("decoded_is_null"),
            decoded.getField("id").alias("id"),
            decoded.getField("priority").alias("priority")).orderBy("idx")

    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


def _build_repeated_message_enum_descriptor_set_bytes(spark):
    """Build a descriptor for repeated messages whose child struct contains an enum."""
    return _build_proto2_descriptor(spark, "repeated_message_enum.proto", [
        _msg("ItemWithPriority", [
            _field("priority", 1, "ENUM", type_name=".test.ItemWithPriority.Priority"),
            _field("count", 2, "INT32"),
        ], enums=[
            _enum("Priority", [("UNKNOWN", 0), ("FOO", 1), ("BAR", 2)]),
        ]),
        _msg("ContainerWithPriorityItems", [
            _field("id", 1, "INT32"),
            _field("items", 2, "MESSAGE", label="repeated", type_name=".test.ItemWithPriority"),
            _field("title", 3, "STRING"),
        ]),
    ])


@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@ignore_order(local=True)
def test_from_protobuf_repeated_message_child_enum_string(
        local_tmp_path, from_protobuf_fn):
    desc_path, desc_bytes = _setup_protobuf_desc(
        local_tmp_path, "repeated_message_enum.desc",
        _build_repeated_message_enum_descriptor_set_bytes)
    message_name = "test.ContainerWithPriorityItems"

    item_foo = (_encode_tag(1, 0) + _encode_varint(1) +
                _encode_tag(2, 0) + _encode_varint(10))
    item_bar = (_encode_tag(1, 0) + _encode_varint(2) +
                _encode_tag(2, 0) + _encode_varint(20))
    row_with_items = (_encode_tag(1, 0) + _encode_varint(1) +
                      _encode_tag(2, 2) + _encode_varint(len(item_foo)) + item_foo +
                      _encode_tag(2, 2) + _encode_varint(len(item_bar)) + item_bar +
                      _encode_tag(3, 2) + _encode_varint(5) + b"hello")
    row_no_items = (_encode_tag(1, 0) + _encode_varint(2) +
                    _encode_tag(3, 2) + _encode_varint(5) + b"empty")

    def run_on_spark(spark):
        df = spark.createDataFrame(
            [(row_with_items,), (row_no_items,), (None,)], schema="bin binary")
        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), message_name, desc_path, desc_bytes)
        return df.select(
            decoded.getField("id").alias("id"),
            decoded.getField("title").alias("title"),
            decoded.getField("items").getField("priority").alias("priorities"))

    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@ignore_order(local=True)
def test_from_protobuf_repeated_message_child_enum_string_invalid_permissive(
        local_tmp_path, from_protobuf_fn):
    desc_path, desc_bytes = _setup_protobuf_desc(
        local_tmp_path, "repeated_message_enum.desc",
        _build_repeated_message_enum_descriptor_set_bytes)
    message_name = "test.ContainerWithPriorityItems"

    item_valid = (_encode_tag(1, 0) + _encode_varint(1) +
                  _encode_tag(2, 0) + _encode_varint(10))
    item_invalid = (_encode_tag(1, 0) + _encode_varint(99) +
                    _encode_tag(2, 0) + _encode_varint(20))
    row_valid = (_encode_tag(1, 0) + _encode_varint(1) +
                 _encode_tag(2, 2) + _encode_varint(len(item_valid)) + item_valid +
                 _encode_tag(3, 2) + _encode_varint(2) + b"ok")
    row_invalid = (_encode_tag(1, 0) + _encode_varint(2) +
                   _encode_tag(2, 2) + _encode_varint(len(item_invalid)) + item_invalid +
                   _encode_tag(3, 2) + _encode_varint(3) + b"bad")

    def run_on_spark(spark):
        df = spark.createDataFrame(
            [(0, row_valid), (1, row_invalid)], schema="idx int, bin binary")
        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), message_name, desc_path, desc_bytes,
            options={"mode": "PERMISSIVE"})
        return df.select(
            f.col("idx"),
            decoded.isNull().alias("decoded_is_null"),
            decoded.getField("id").alias("id"),
            decoded.getField("title").alias("title"),
            decoded.getField("items").getField("priority").getItem(0).alias("priority0")
        )

    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


def _build_nested_repeated_enum_descriptor_set_bytes(spark):
    """Build a descriptor for a nested struct containing a repeated enum field."""
    return _build_proto2_descriptor(spark, "nested_repeated_enum.proto", [
        _msg("InnerWithRepeatedPriority", [
            _field("priority", 1, "ENUM", label="repeated",
                   type_name=".test.InnerWithRepeatedPriority.Priority"),
            _field("count", 2, "INT32"),
        ], enums=[
            _enum("Priority", [("UNKNOWN", 0), ("FOO", 1), ("BAR", 2)]),
        ]),
        _msg("OuterWithNestedRepeatedEnum", [
            _field("id", 1, "INT32"),
            _field("inner", 2, "MESSAGE", type_name=".test.InnerWithRepeatedPriority"),
            _field("name", 3, "STRING"),
        ]),
    ])


@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@ignore_order(local=True)
def test_from_protobuf_nested_repeated_enum_string(
        local_tmp_path, from_protobuf_fn):
    desc_path, desc_bytes = _setup_protobuf_desc(
        local_tmp_path, "nested_repeated_enum.desc",
        _build_nested_repeated_enum_descriptor_set_bytes)
    message_name = "test.OuterWithNestedRepeatedEnum"

    inner = (_encode_tag(1, 0) + _encode_varint(0) +
             _encode_tag(1, 0) + _encode_varint(2) +
             _encode_tag(1, 0) + _encode_varint(1) +
             _encode_tag(2, 0) + _encode_varint(7))
    row_with_inner = (_encode_tag(1, 0) + _encode_varint(1) +
                      _encode_tag(2, 2) + _encode_varint(len(inner)) + inner +
                      _encode_tag(3, 2) + _encode_varint(5) + b"hello")
    row_no_inner = (_encode_tag(1, 0) + _encode_varint(2) +
                    _encode_tag(3, 2) + _encode_varint(4) + b"none")

    def run_on_spark(spark):
        df = spark.createDataFrame(
            [(row_with_inner,), (row_no_inner,), (None,)], schema="bin binary")
        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), message_name, desc_path, desc_bytes)
        return df.select(
            decoded.getField("id").alias("id"),
            decoded.getField("name").alias("name"),
            decoded.getField("inner").getField("priority").alias("priorities"))

    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@ignore_order(local=True)
def test_from_protobuf_nested_repeated_enum_string_invalid_permissive(
        local_tmp_path, from_protobuf_fn):
    desc_path, desc_bytes = _setup_protobuf_desc(
        local_tmp_path, "nested_repeated_enum.desc",
        _build_nested_repeated_enum_descriptor_set_bytes)
    message_name = "test.OuterWithNestedRepeatedEnum"

    inner_valid = (_encode_tag(1, 0) + _encode_varint(1) +
                   _encode_tag(1, 0) + _encode_varint(2) +
                   _encode_tag(2, 0) + _encode_varint(7))
    inner_invalid = (_encode_tag(1, 0) + _encode_varint(1) +
                     _encode_tag(1, 0) + _encode_varint(99) +
                     _encode_tag(2, 0) + _encode_varint(9))
    row_valid = (_encode_tag(1, 0) + _encode_varint(1) +
                 _encode_tag(2, 2) + _encode_varint(len(inner_valid)) + inner_valid +
                 _encode_tag(3, 2) + _encode_varint(2) + b"ok")
    row_invalid = (_encode_tag(1, 0) + _encode_varint(2) +
                   _encode_tag(2, 2) + _encode_varint(len(inner_invalid)) + inner_invalid +
                   _encode_tag(3, 2) + _encode_varint(3) + b"bad")

    def run_on_spark(spark):
        df = spark.createDataFrame(
            [(0, row_valid), (1, row_invalid)], schema="idx int, bin binary")
        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), message_name, desc_path, desc_bytes,
            options={"mode": "PERMISSIVE"})
        return df.select(
            f.col("idx"),
            decoded.isNull().alias("decoded_is_null"),
            decoded.getField("id").alias("id"),
            decoded.getField("name").alias("name"),
            decoded.getField("inner").getField("priority").getItem(0).alias("priority0"),
            decoded.getField("inner").getField("priority").getItem(1).alias("priority1")
        )

    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


def _build_required_field_descriptor_set_bytes(spark):
    """Build a descriptor for top-level proto2 required-field validation."""
    return _build_proto2_descriptor(spark, "required.proto", [
        _msg("WithRequired", [
            _field("id", 1, "INT64", label="required"),
            _field("name", 2, "STRING"),
            _field("count", 3, "INT32"),
        ]),
    ])


def _build_nested_required_field_descriptor_set_bytes(spark):
    """Build a descriptor with a nested proto2 required field."""
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


@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@ignore_order(local=True)
def test_from_protobuf_required_field_present(local_tmp_path, from_protobuf_fn):
    """
    Test that required fields decode correctly when present.
    GPU should produce same results as CPU.
    """
    desc_path, desc_bytes = _setup_protobuf_desc(
        local_tmp_path, "required.desc", _build_required_field_descriptor_set_bytes)
    message_name = "test.WithRequired"

    # Create test data with required field present
    # Row 0: id=100, name="test", count=42
    # Row 1: id=200, name missing, count missing
    required_schema = _schema("WithRequiredManual", [
        _scalar("id", 1, LongGen()),
        _scalar("name", 2, StringGen()),
        _scalar("count", 3, IntegerGen()),
    ])
    test_data_row0 = encode_pb_message(required_schema, {"id": 100, "name": "test", "count": 42})
    test_data_row1 = encode_pb_message(required_schema, {"id": 200})

    def run_on_spark(spark):
        df = spark.createDataFrame(
            [(test_data_row0,), (test_data_row1,)],
            schema="bin binary",
        )
        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), message_name, desc_path, desc_bytes)
        return df.select(
            decoded.getField("id").alias("id"),
            decoded.getField("name").alias("name"),
            decoded.getField("count").alias("count")
        )

    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@ignore_order(local=True)
def test_from_protobuf_required_field_missing_failfast(local_tmp_path, from_protobuf_fn):
    """Required-field violations should fail consistently on CPU and GPU in FAILFAST mode."""
    desc_path, desc_bytes = _setup_protobuf_desc(
        local_tmp_path, "required.desc", _build_required_field_descriptor_set_bytes)
    message_name = "test.WithRequired"

    missing_required_row = _encode_tag(2, 2) + _encode_varint(4) + b"oops"

    def run_on_spark(spark):
        df = spark.createDataFrame([(missing_required_row,)], schema="bin binary")
        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), message_name, desc_path, desc_bytes)
        return df.select(decoded.alias("decoded")).collect()

    assert_gpu_and_cpu_error(run_on_spark, conf={}, error_message="Malformed")


@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@ignore_order(local=True)
def test_from_protobuf_required_field_missing_permissive(local_tmp_path, from_protobuf_fn):
    """Required-field violations should null the whole row in PERMISSIVE mode."""
    desc_path, desc_bytes = _setup_protobuf_desc(
        local_tmp_path, "required.desc", _build_required_field_descriptor_set_bytes)
    message_name = "test.WithRequired"

    missing_required_row = _encode_tag(2, 2) + _encode_varint(5) + b"hello"

    def run_on_spark(spark):
        df = spark.createDataFrame([(missing_required_row,)], schema="bin binary")
        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), message_name, desc_path, desc_bytes,
            options={"mode": "PERMISSIVE"})
        return df.select(
            decoded.isNull().alias("decoded_is_null"),
            decoded.getField("id").alias("id"),
            decoded.getField("name").alias("name"),
            decoded.getField("count").alias("count")
        )

    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@ignore_order(local=True)
def test_from_protobuf_nested_required_field_missing_permissive(
        local_tmp_path, from_protobuf_fn):
    """Observe CPU/GPU parity when a nested proto2 required field is missing in PERMISSIVE mode."""
    desc_path, desc_bytes = _setup_protobuf_desc(
        local_tmp_path, "nested_required.desc", _build_nested_required_field_descriptor_set_bytes)
    message_name = "test.WithNestedRequired"

    inner_valid = (_encode_tag(1, 0) + _encode_varint(7) +
                   _encode_tag(2, 2) + _encode_varint(2) + b"ok")
    inner_missing_required = _encode_tag(2, 2) + _encode_varint(4) + b"oops"

    row_valid = (_encode_tag(1, 0) + _encode_varint(100) +
                 _encode_tag(2, 2) + _encode_varint(len(inner_valid)) + inner_valid +
                 _encode_tag(3, 2) + _encode_varint(5) + b"valid")
    row_missing_required = (_encode_tag(1, 0) + _encode_varint(200) +
                            _encode_tag(2, 2) + _encode_varint(len(inner_missing_required)) +
                            inner_missing_required +
                            _encode_tag(3, 2) + _encode_varint(7) + b"missing")
    row_missing_inner = (_encode_tag(1, 0) + _encode_varint(300) +
                         _encode_tag(3, 2) + _encode_varint(8) + b"no_inner")

    def run_on_spark(spark):
        df = spark.createDataFrame(
            [(0, row_valid), (1, row_missing_required), (2, row_missing_inner)],
            schema="idx int, bin binary")
        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), message_name, desc_path, desc_bytes,
            options={"mode": "PERMISSIVE"})
        inner = decoded.getField("inner")
        return df.select(
            f.col("idx"),
            decoded.isNull().alias("decoded_is_null"),
            decoded.getField("id").alias("id"),
            decoded.getField("name").alias("name"),
            inner.isNull().alias("inner_is_null"),
            inner.getField("child_id").alias("child_id"),
            inner.getField("note").alias("note")
        ).orderBy("idx")

    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@ignore_order(local=True)
def test_from_protobuf_required_field_pruned_permissive(local_tmp_path, from_protobuf_fn):
    """When schema projection prunes a required field, missing-required must still
    null the struct row in PERMISSIVE mode — matching CPU behavior."""
    desc_path, desc_bytes = _setup_protobuf_desc(
        local_tmp_path, "required.desc", _build_required_field_descriptor_set_bytes)
    message_name = "test.WithRequired"

    # Row that has required `id` present: id=1, name="ok", count=10
    row_valid = (_encode_tag(1, 0) + _encode_varint(1) +
                 _encode_tag(2, 2) + _encode_varint(2) + b"ok" +
                 _encode_tag(3, 0) + _encode_varint(10))
    # Row missing required `id`: only name="bad"
    row_missing_required = _encode_tag(2, 2) + _encode_varint(3) + b"bad"

    def run_on_spark(spark):
        df = spark.createDataFrame(
            [(0, row_valid), (1, row_missing_required)],
            schema="idx int, bin binary")
        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), message_name, desc_path, desc_bytes,
            options={"mode": "PERMISSIVE"})
        # Only access non-required fields so that schema projection prunes `id`.
        return df.select(
            f.col("idx"),
            decoded.getField("name").alias("name"),
            decoded.getField("count").alias("count")
        ).orderBy("idx")

    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@ignore_order(local=True)
def test_from_protobuf_nested_required_field_pruned_permissive(
        local_tmp_path, from_protobuf_fn):
    """When nested pruning prunes a required child, missing-required must still
    null the struct row in PERMISSIVE mode — matching CPU behavior."""
    desc_path, desc_bytes = _setup_protobuf_desc(
        local_tmp_path, "nested_required.desc",
        _build_nested_required_field_descriptor_set_bytes)
    message_name = "test.WithNestedRequired"

    inner_valid = (_encode_tag(1, 0) + _encode_varint(7) +
                   _encode_tag(2, 2) + _encode_varint(2) + b"ok")
    inner_missing_required = _encode_tag(2, 2) + _encode_varint(4) + b"oops"

    row_valid = (_encode_tag(1, 0) + _encode_varint(100) +
                 _encode_tag(2, 2) + _encode_varint(len(inner_valid)) + inner_valid +
                 _encode_tag(3, 2) + _encode_varint(5) + b"valid")
    row_missing_required = (_encode_tag(1, 0) + _encode_varint(200) +
                            _encode_tag(2, 2) + _encode_varint(len(inner_missing_required)) +
                            inner_missing_required +
                            _encode_tag(3, 2) + _encode_varint(7) + b"missing")

    def run_on_spark(spark):
        df = spark.createDataFrame(
            [(0, row_valid), (1, row_missing_required)],
            schema="idx int, bin binary")
        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), message_name, desc_path, desc_bytes,
            options={"mode": "PERMISSIVE"})
        # Only access non-required inner child (note) — required child_id should
        # still be decoded so the GPU can detect missing-required.
        return df.select(
            f.col("idx"),
            decoded.getField("name").alias("name"),
            decoded.getField("inner").getField("note").alias("note")
        ).orderBy("idx")

    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


# ---------------------------------------------------------------------------
# Schema-projection validation parity / gap
#
# CPU `ProtobufDataToCatalyst` calls `DynamicMessage.parseFrom(bytes)` which
# validates the *entire* wire payload (varints, lengths, wire types) before
# Catalyst extracts any field. With GPU schema projection, fields not
# referenced downstream are scanned past rather than fully decoded, so we
# need parity tests to make sure validation is preserved.
#
# Empirically:
#   - top-level scalar fields: the GPU kernel reads varints and length
#     prefixes during its outer scan, so malformed bytes in a pruned
#     scalar field are still rejected. The first three tests pin that
#     parity so a future kernel refactor that drops outer validation
#     would be caught here.
#   - pruned nested messages: the outer scan only reads the message's
#     length varint. GPU still needs to recurse into validation-only
#     message fields so malformed bytes nested *inside* a pruned message
#     match CPU's recursive parseFrom behavior.
# ---------------------------------------------------------------------------

# Eleven 0x80 bytes — a varint that never terminates. CPU
# `CodedInputStream.readRawVarint64` raises "CodedInputStream encountered
# a malformed varint." after 10 continuation bytes.
_MALFORMED_VARINT = b"\x80" * 11


@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@ignore_order(local=True)
def test_from_protobuf_pruned_scalar_malformed_varint_permissive(
        local_tmp_path, from_protobuf_fn):
    """PERMISSIVE: malformed varint inside a pruned non-required scalar field.

    GPU's outer scan reads the varint to advance past field 3 and rejects
    the unterminated value, matching CPU's null-row PERMISSIVE behavior.
    Pins that parity.
    """
    desc_path, desc_bytes = _setup_protobuf_desc(
        local_tmp_path, "simple.desc", _build_simple_descriptor_set_bytes)
    message_name = "test.Simple"

    # Valid b=true, valid i32=42, then a malformed varint as the value of
    # field 3 (i64, wire type 0). All other fields are absent.
    row = (_encode_tag(1, 0) + _encode_varint(1) +
           _encode_tag(2, 0) + _encode_varint(42) +
           _encode_tag(3, 0) + _MALFORMED_VARINT)

    def run_on_spark(spark):
        df = spark.createDataFrame([(row,)], schema="bin binary")
        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), message_name, desc_path, desc_bytes,
            options={"mode": "PERMISSIVE"})
        # Only reference b and i32 so projection prunes i64/f32/f64/s.
        return df.select(
            decoded.getField("b").alias("b"),
            decoded.getField("i32").alias("i32"),
        )

    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@ignore_order(local=True)
def test_from_protobuf_pruned_scalar_malformed_varint_failfast(
        local_tmp_path, from_protobuf_fn):
    """FAILFAST: malformed varint inside a pruned non-required scalar field.

    Both CPU and GPU throw — pins that GPU outer-scan validation surfaces
    the same error class as CPU.
    """
    desc_path, desc_bytes = _setup_protobuf_desc(
        local_tmp_path, "simple.desc", _build_simple_descriptor_set_bytes)
    message_name = "test.Simple"

    row = (_encode_tag(1, 0) + _encode_varint(1) +
           _encode_tag(2, 0) + _encode_varint(42) +
           _encode_tag(3, 0) + _MALFORMED_VARINT)

    def run_on_spark(spark):
        df = spark.createDataFrame([(row,)], schema="bin binary")
        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), message_name, desc_path, desc_bytes)
        return df.select(
            decoded.getField("b").alias("b"),
            decoded.getField("i32").alias("i32"),
        ).collect()

    assert_gpu_and_cpu_error(run_on_spark, conf={}, error_message="Malformed")


@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@ignore_order(local=True)
def test_from_protobuf_pruned_string_truncated_length_permissive(
        local_tmp_path, from_protobuf_fn):
    """PERMISSIVE: length-delimited field declares more bytes than available.

    Field 6 (`s`) declares length=100 but only 5 payload bytes follow. The
    GPU outer scan tries to advance 100 bytes from position 5 and detects
    the buffer underrun, matching CPU's null-row PERMISSIVE behavior.
    """
    desc_path, desc_bytes = _setup_protobuf_desc(
        local_tmp_path, "simple.desc", _build_simple_descriptor_set_bytes)
    message_name = "test.Simple"

    row = (_encode_tag(1, 0) + _encode_varint(1) +
           _encode_tag(2, 0) + _encode_varint(42) +
           _encode_tag(6, 2) + _encode_varint(100) + b"short")

    def run_on_spark(spark):
        df = spark.createDataFrame([(row,)], schema="bin binary")
        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), message_name, desc_path, desc_bytes,
            options={"mode": "PERMISSIVE"})
        return df.select(
            decoded.getField("b").alias("b"),
            decoded.getField("i32").alias("i32"),
        )

    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@ignore_order(local=True)
def test_from_protobuf_pruned_known_scalar_wrong_wire_type(
        local_tmp_path, from_protobuf_fn):
    desc_path, desc_bytes = _setup_protobuf_desc(
        local_tmp_path, "simple.desc", _build_simple_descriptor_set_bytes)
    row = (_encode_tag(1, 0) + _encode_varint(1) +
           _encode_tag(2, 0) + _encode_varint(42) +
           _encode_tag(3, 2) + _encode_varint(1) + b"x")

    def run_on_spark(spark):
        df = spark.createDataFrame([(row,)], schema="bin binary")
        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), "test.Simple", desc_path, desc_bytes,
            options={"mode": "PERMISSIVE"})
        return df.select(
            decoded.getField("b").alias("b"),
            decoded.getField("i32").alias("i32"))

    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@ignore_order(local=True)
def test_from_protobuf_pruned_unknown_enum(local_tmp_path, from_protobuf_fn):
    desc_path, desc_bytes = _setup_protobuf_desc(
        local_tmp_path, "enum.desc", _build_enum_descriptor_set_bytes)
    row = (_encode_tag(1, 0) + _encode_varint(999) +
           _encode_tag(2, 0) + _encode_varint(42))

    def run_on_spark(spark):
        df = spark.createDataFrame([(row,)], schema="bin binary")
        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), "test.WithEnum", desc_path, desc_bytes,
            options={"mode": "PERMISSIVE"})
        return df.select(decoded.getField("count").alias("count"))

    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@pytest.mark.parametrize("field_number,payload", [
    (2, b"\x80"),
    (3, b"\x00"),
], ids=["unterminated-packed-varint", "misaligned-packed-fixed64"])
@ignore_order(local=True)
def test_from_protobuf_pruned_packed_repeated_validation(
        local_tmp_path, from_protobuf_fn, field_number, payload):
    desc_path, desc_bytes = _setup_protobuf_desc(
        local_tmp_path, "packed.desc", _build_packed_repeated_descriptor_set_bytes)
    row = (_encode_tag(1, 0) + _encode_varint(7) +
           _encode_tag(field_number, 2) + _encode_varint(len(payload)) + payload)

    def run_on_spark(spark):
        df = spark.createDataFrame([(row,)], schema="bin binary")
        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), "test.WithPackedRepeated",
            desc_path, desc_bytes, options={"mode": "PERMISSIVE"})
        return df.select(decoded.getField("id").alias("id"))

    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


def _build_nested_packed_validation_descriptor_set_bytes(spark):
    return _build_proto2_descriptor(spark, "nested_packed_validation.proto", [
        _msg("InnerPacked", [
            _field("keep", 1, "INT32"),
            _field("bad_values", 2, "INT32", label="repeated", packed=True),
        ]),
        _msg("OuterPacked", [
            _field("inner", 1, "MESSAGE", type_name=".test.InnerPacked"),
        ]),
    ])


@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@ignore_order(local=True)
def test_from_protobuf_partially_pruned_nested_packed_validation(
        local_tmp_path, from_protobuf_fn):
    desc_path, desc_bytes = _setup_protobuf_desc(
        local_tmp_path, "nested_packed_validation.desc",
        _build_nested_packed_validation_descriptor_set_bytes)
    inner = (_encode_tag(1, 0) + _encode_varint(7) +
             _encode_tag(2, 2) + _encode_varint(1) + b"\x80")
    row = _encode_tag(1, 2) + _encode_varint(len(inner)) + inner

    def run_on_spark(spark):
        df = spark.createDataFrame([(row,)], schema="bin binary")
        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), "test.OuterPacked",
            desc_path, desc_bytes, options={"mode": "PERMISSIVE"})
        return df.select(decoded.getField("inner").getField("keep").alias("keep"))

    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@ignore_order(local=True)
def test_from_protobuf_pruned_nested_malformed_inner_permissive(
        local_tmp_path, from_protobuf_fn):
    """A pruned nested message must still be parsed for malformed payloads."""
    desc_path, desc_bytes = _setup_protobuf_desc(
        local_tmp_path, "nested.desc", _build_nested_descriptor_set_bytes)
    message_name = "test.WithNested"

    # Inner payload: an unknown-field tag followed by a malformed varint.
    # CPU's recursive parseFrom fails when it tries to skip the unknown
    # field and finds an unterminated varint.
    inner_malformed = _encode_tag(99, 0) + _MALFORMED_VARINT

    row = (_encode_tag(1, 0) + _encode_varint(42) +              # simple_int = 42
           _encode_tag(2, 2) + _encode_varint(5) + b"hello" +    # simple_str = "hello"
           _encode_tag(4, 0) + _encode_varint(99) +              # simple_long = 99
           _encode_tag(3, 2) +                                   # nested_msg
               _encode_varint(len(inner_malformed)) + inner_malformed)

    def run_on_spark(spark):
        df = spark.createDataFrame([(row,)], schema="bin binary")
        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), message_name, desc_path, desc_bytes,
            options={"mode": "PERMISSIVE"})
        # Only reference top-level scalars so nested_msg gets pruned.
        return df.select(
            decoded.getField("simple_int").alias("simple_int"),
            decoded.getField("simple_str").alias("simple_str"),
        )

    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@ignore_order(local=True)
def test_from_protobuf_pruned_nested_truncated_inner_permissive(
        local_tmp_path, from_protobuf_fn):
    """A pruned nested message must still validate inner length delimiters."""
    desc_path, desc_bytes = _setup_protobuf_desc(
        local_tmp_path, "nested.desc", _build_nested_descriptor_set_bytes)
    message_name = "test.WithNested"

    # Inner: an unknown length-delimited field declaring 100 bytes with
    # only 1 byte of payload. CPU recursively parses inner and throws on
    # the buffer underrun.
    inner_truncated = _encode_tag(50, 2) + _encode_varint(100) + b"x"

    row = (_encode_tag(1, 0) + _encode_varint(42) +
           _encode_tag(2, 2) + _encode_varint(5) + b"hello" +
           _encode_tag(4, 0) + _encode_varint(99) +
           _encode_tag(3, 2) +
               _encode_varint(len(inner_truncated)) + inner_truncated)

    def run_on_spark(spark):
        df = spark.createDataFrame([(row,)], schema="bin binary")
        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), message_name, desc_path, desc_bytes,
            options={"mode": "PERMISSIVE"})
        return df.select(
            decoded.getField("simple_int").alias("simple_int"),
            decoded.getField("simple_str").alias("simple_str"),
        )

    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


def _build_default_value_descriptor_set_bytes(spark):
    """Build a descriptor for proto2 scalar default-value behavior."""
    return _build_proto2_descriptor(spark, "defaults.proto", [
        _msg("WithDefaults", [
            _field("count", 1, "INT32", default=42),
            _field("name", 2, "STRING", default="unknown"),
            _field("flag", 3, "BOOL", default=True),
            _field("zero_count", 4, "INT32", default=0),
            _field("empty_name", 5, "STRING", default=""),
        ]),
    ])


@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@pytest.mark.parametrize("default_case", [
    "field_present",
    "missing_fields",
    "string_default_only",
], ids=lambda x: x)
@ignore_order(local=True)
def test_from_protobuf_default_values_cases(local_tmp_path, from_protobuf_fn, default_case):
    """Parametrized tests for proto2 default-value behavior."""
    desc_path, desc_bytes = _setup_protobuf_desc(
        local_tmp_path, "defaults.desc", _build_default_value_descriptor_set_bytes)
    message_name = "test.WithDefaults"

    defaults_schema = _schema("WithDefaultsManual", [
        _scalar("count", 1, IntegerGen(), default=42),
        _scalar("name", 2, StringGen(), default="unknown"),
        _scalar("flag", 3, BooleanGen(), default=True),
        _scalar("zero_count", 4, IntegerGen(), default=0),
        _scalar("empty_name", 5, StringGen(), default=""),
    ])

    if default_case == "field_present":
        rows = [(encode_pb_message(defaults_schema, {"count": 100, "name": "test", "flag": False}),)]
        select_mode = "all"
    elif default_case == "missing_fields":
        rows = [
            (encode_pb_message(defaults_schema, {}),),
            (encode_pb_message(defaults_schema, {"count": 100}),),
        ]
        select_mode = "all"
    else:
        rows = [(encode_pb_message(defaults_schema, {"count": 42, "flag": True}),)]
        select_mode = "name_only"

    def run_on_spark(spark):
        df = spark.createDataFrame(rows, schema="bin binary")
        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), message_name, desc_path, desc_bytes)
        if select_mode == "all":
            return df.select(
                decoded.getField("count").alias("count"),
                decoded.getField("name").alias("name"),
                decoded.getField("flag").alias("flag"),
                decoded.getField("zero_count").alias("zero_count"),
                decoded.getField("empty_name").alias("empty_name"))
        return df.select(decoded.getField("name").alias("name"))

    if default_case == "missing_fields":
        cpu_rows = with_cpu_session(lambda spark: run_on_spark(spark).collect())
        assert all(row["zero_count"] == 0 and row["empty_name"] == ""
                   for row in cpu_rows)
    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


def _build_bytes_default_descriptor_set_bytes(spark):
    return _build_proto2_descriptor(spark, "bytes_default.proto", [
        _msg("BytesDefault", [
            _field("payload", 1, "BYTES", default="abc"),
        ]),
    ])


@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@allow_non_gpu("ProtobufDataToCatalyst")
def test_from_protobuf_bytes_default_falls_back(local_tmp_path, from_protobuf_fn):
    desc_path, desc_bytes = _setup_protobuf_desc(
        local_tmp_path, "bytes_default.desc", _build_bytes_default_descriptor_set_bytes)

    def run_on_spark(spark):
        df = spark.createDataFrame([(b"",)], schema="bin binary")
        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), "test.BytesDefault", desc_path, desc_bytes)
        return df.select(decoded.alias("decoded"))

    assert_gpu_fallback_collect(run_on_spark, "ProtobufDataToCatalyst")


def _build_all_scalars_descriptor_set_bytes(spark):
    """Build a descriptor covering all scalar wire encodings used in tests."""
    return _build_proto2_descriptor(spark, "all_scalars.proto", [
        _msg("AllScalars", [
            _field("b", 1, "BOOL"),
            _field("i32", 2, "INT32"),
            _field("i64", 3, "INT64"),
            _field("f32", 4, "FLOAT"),
            _field("f64", 5, "DOUBLE"),
            _field("s", 6, "STRING"),
            _field("si32", 7, "SINT32"),
            _field("si64", 8, "SINT64"),
            _field("fx32", 9, "FIXED32"),
            _field("fx64", 10, "FIXED64"),
        ]),
    ])


def _build_scalar_bytes_descriptor_set_bytes(spark):
    """Build a descriptor for a single optional bytes field."""
    return _build_proto2_descriptor(spark, "scalar_bytes.proto", [
        _msg("ScalarBytes", [_field("payload", 1, "BYTES")]),
    ])


def _scalar_test_id(config):
    """Generate stable test ID using only the first element (test name)."""
    return config[0] if isinstance(config, tuple) else str(config)


@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@pytest.mark.parametrize("test_config", _random_scalar_test_configs, ids=_scalar_test_id)
@ignore_order(local=True)
def test_from_protobuf_random_scalars(local_tmp_path, from_protobuf_fn, test_config):
    """
    Parametrized test for from_protobuf with randomly generated scalar data.
    Covers: all types, integer edge cases, float edge cases, large datasets.
    """
    test_id, field_configs = test_config

    desc_path, desc_bytes = _setup_protobuf_desc(
        local_tmp_path, "simple.desc", _build_simple_descriptor_set_bytes)
    message_name = "test.Simple"

    data_gen = _as_datagen(field_configs)

    def run_on_spark(spark):
        df = gen_df(spark, data_gen)

        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), message_name, desc_path, desc_bytes)

        # Select all decoded fields
        return df.select(
            decoded.getField("b").alias("b"),
            decoded.getField("i32").alias("i32"),
            decoded.getField("i64").alias("i64"),
            decoded.getField("f32").alias("f32"),
            decoded.getField("f64").alias("f64"),
            decoded.getField("s").alias("s"),
        )

    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@ignore_order(local=True)
def test_from_protobuf_all_scalar_types(local_tmp_path, from_protobuf_fn):
    """Decode all scalar wire encodings together in one message."""
    desc_path, desc_bytes = _setup_protobuf_desc(
        local_tmp_path, "all_scalars.desc", _build_all_scalars_descriptor_set_bytes)
    message_name = "test.AllScalars"

    data_gen = _as_datagen([
        _scalar("b", 1, BooleanGen()),
        _scalar("i32", 2, IntegerGen()),
        _scalar("i64", 3, LongGen()),
        _scalar("f32", 4, FloatGen()),
        _scalar("f64", 5, DoubleGen()),
        _scalar("s", 6, StringGen()),
        _scalar("si32", 7, IntegerGen(
            special_cases=[-1, 0, 1, -2147483648, 2147483647]), encoding='zigzag'),
        _scalar("si64", 8, LongGen(
            special_cases=[-1, 0, 1, -9223372036854775808, 9223372036854775807]),
            encoding='zigzag'),
        _scalar("fx32", 9, IntegerGen(
            special_cases=[0, 1, -1, 2147483647, -2147483648]), encoding='fixed'),
        _scalar("fx64", 10, LongGen(
            special_cases=[0, 1, -1]), encoding='fixed'),
    ])

    def run_on_spark(spark):
        df = gen_df(spark, data_gen)
        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), message_name, desc_path, desc_bytes)
        return df.select(
            decoded.getField("b").alias("b"),
            decoded.getField("i32").alias("i32"),
            decoded.getField("i64").alias("i64"),
            decoded.getField("f32").alias("f32"),
            decoded.getField("f64").alias("f64"),
            decoded.getField("s").alias("s"),
            decoded.getField("si32").alias("si32"),
            decoded.getField("si64").alias("si64"),
            decoded.getField("fx32").alias("fx32"),
            decoded.getField("fx64").alias("fx64"),
        )

    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@ignore_order(local=True)
def test_from_protobuf_scalar_bytes(local_tmp_path, from_protobuf_fn):
    """Decode optional bytes scalar field, including empty bytes."""
    desc_path, desc_bytes = _setup_protobuf_desc(
        local_tmp_path, "scalar_bytes.desc", _build_scalar_bytes_descriptor_set_bytes)
    message_name = "test.ScalarBytes"

    data_gen = _as_datagen([
        _scalar("payload", 1, BinaryGen(min_length=0, max_length=16)),
    ])

    def run_on_spark(spark):
        df = gen_df(spark, data_gen)
        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), message_name, desc_path, desc_bytes)
        return df.select(decoded.getField("payload").alias("payload"))

    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@ignore_order(local=True)
def test_from_protobuf_duplicate_fields(local_tmp_path, from_protobuf_fn):
    """Duplicate non-repeated field should follow protobuf last-one-wins semantics."""
    desc_path, desc_bytes = _setup_protobuf_desc(
        local_tmp_path, "simple.desc", _build_simple_descriptor_set_bytes)
    message_name = "test.Simple"

    # i32 (field 2) appears twice in row0 and three times in row1.
    test_data_row0 = bytes([0x10, 0x01, 0x10, 0x2A])
    test_data_row1 = bytes([0x10, 0x03, 0x10, 0x04, 0x10, 0x05])

    def run_on_spark(spark):
        df = spark.createDataFrame(
            [(test_data_row0,), (test_data_row1,), (None,)],
            schema="bin binary")
        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), message_name, desc_path, desc_bytes)
        return df.select(decoded.getField("i32").alias("i32"))

    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


def _build_singular_message_merge_descriptor_set_bytes(spark):
    return _build_proto2_descriptor(spark, "singular_message_merge.proto", [
        _msg("MergeChild", [
            _field("a", 1, "INT32"),
            _field("b", 2, "STRING"),
        ]),
        _msg("MergeParent", [
            _field("child", 1, "MESSAGE", type_name=".test.MergeChild"),
            _field("ordinal", 2, "INT32"),
        ]),
        _msg("MergeContainer", [
            _field("parents", 1, "MESSAGE", label="repeated",
                   type_name=".test.MergeParent"),
        ]),
    ])


@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@ignore_order(local=True)
def test_from_protobuf_duplicate_singular_messages_merge(local_tmp_path, from_protobuf_fn):
    desc_path, desc_bytes = _setup_protobuf_desc(
        local_tmp_path,
        "singular_message_merge.desc",
        _build_singular_message_merge_descriptor_set_bytes)
    message_name = "test.MergeContainer"
    child_fields = [
        _scalar("a", 1, IntegerGen()),
        _scalar("b", 2, StringGen("[a-z]{0,8}")),
    ]
    data_gen = _as_datagen([
        _repeated_message("parents", 1, [
            _repeated_message("child", 1, child_fields, min_len=2, max_len=2),
            _scalar("ordinal", 2, IntegerGen(nullable=False)),
        ], min_len=0, max_len=4),
    ])
    first_child = _encode_tag(1, 0) + _encode_varint(1)
    second_child = _encode_tag(2, 2) + _encode_varint(1) + b"x"
    parent = (_encode_tag(1, 2) + _encode_varint(len(first_child)) + first_child +
              _encode_tag(1, 2) + _encode_varint(len(second_child)) + second_child +
              _encode_tag(2, 0) + _encode_varint(7))
    sentinel_bin = _encode_tag(1, 2) + _encode_varint(len(parent)) + parent

    def run_on_spark(spark):
        generated = gen_df(spark, data_gen)
        sentinel = spark.createDataFrame(
            [([([(1, None), (None, "x")], 7)], sentinel_bin)],
            schema=generated.schema)
        df = generated.unionByName(sentinel)
        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), message_name, desc_path, desc_bytes)
        wire_parents = f.col("parents")
        decoded_parents = decoded.getField("parents")
        return df.select(
            f.transform(
                wire_parents,
                lambda parent: f.coalesce(
                    parent["child"][1]["a"], parent["child"][0]["a"]))
                .alias("expected_a"),
            f.transform(decoded_parents, lambda parent: parent["child"]["a"])
                .alias("actual_a"),
            f.transform(
                wire_parents,
                lambda parent: f.coalesce(
                    parent["child"][1]["b"], parent["child"][0]["b"]))
                .alias("expected_b"),
            f.transform(decoded_parents, lambda parent: parent["child"]["b"])
                .alias("actual_b"),
            f.transform(wire_parents, lambda parent: parent["ordinal"])
                .alias("expected_ordinal"),
            f.transform(decoded_parents, lambda parent: parent["ordinal"])
                .alias("actual_ordinal"))

    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


def _build_repeated_int_descriptor_set_bytes(spark):
    """Build a descriptor for an optional id plus repeated int32 values."""
    return _build_proto2_descriptor(spark, "repeated_int.proto", [
        _msg("WithRepeatedInt", [
            _field("id", 1, "INT32"),
            _field("values", 2, "INT32", label="repeated"),
        ]),
    ])


@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@ignore_order(local=True)
def test_from_protobuf_repeated_int32(local_tmp_path, from_protobuf_fn):
    """
    Test decoding repeated int32 field (ArrayType of integers).
    """
    desc_path, desc_bytes = _setup_protobuf_desc(
        local_tmp_path, "repeated_int.desc", _build_repeated_int_descriptor_set_bytes)
    message_name = "test.WithRepeatedInt"

    data_gen = _as_datagen([
        _scalar("id", 1, IntegerGen()),
        _repeated("values", 2, IntegerGen(), min_len=0, max_len=10),
    ])

    def run_on_spark(spark):
        df = gen_df(spark, data_gen)
        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), message_name, desc_path, desc_bytes)
        return df.select(
            decoded.getField("id").alias("id"),
            decoded.getField("values").alias("values"),
        )

    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


def _build_repeated_string_descriptor_set_bytes(spark):
    """Build a descriptor for an optional string plus repeated strings."""
    return _build_proto2_descriptor(spark, "repeated_string.proto", [
        _msg("WithRepeatedString", [
            _field("name", 1, "STRING"),
            _field("tags", 2, "STRING", label="repeated"),
        ]),
    ])


@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@ignore_order(local=True)
def test_from_protobuf_repeated_string(local_tmp_path, from_protobuf_fn):
    """
    Test decoding repeated string field (ArrayType of strings).
    """
    desc_path, desc_bytes = _setup_protobuf_desc(
        local_tmp_path, "repeated_string.desc", _build_repeated_string_descriptor_set_bytes)
    message_name = "test.WithRepeatedString"

    data_gen = _as_datagen([
        _scalar("name", 1, StringGen()),
        _repeated("tags", 2, StringGen(), min_len=0, max_len=5),
    ])

    def run_on_spark(spark):
        df = gen_df(spark, data_gen)
        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), message_name, desc_path, desc_bytes)
        return df.select(
            decoded.getField("name").alias("name"),
            decoded.getField("tags").alias("tags"),
        )

    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@ignore_order(local=True)
def test_from_protobuf_nested_message(local_tmp_path, from_protobuf_fn):
    """
    Test decoding nested message field (StructType).
    """
    desc_path, desc_bytes = _setup_protobuf_desc(
        local_tmp_path, "nested.desc", _build_nested_descriptor_set_bytes)
    message_name = "test.WithNested"

    data_gen = _as_datagen([
        _scalar("simple_int", 1, IntegerGen()),
        _scalar("simple_str", 2, StringGen(nullable=True)),
        _nested("nested_msg", 3, [_scalar("x", 1, IntegerGen())]),
        _scalar("simple_long", 4, LongGen()),
    ])

    def run_on_spark(spark):
        df = gen_df(spark, data_gen)
        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), message_name, desc_path, desc_bytes)
        # Select all fields including nested
        return df.select(
            decoded.getField("simple_int").alias("simple_int"),
            decoded.getField("simple_str").alias("simple_str"),
            decoded.getField("nested_msg").alias("nested_msg"),
            decoded.getField("simple_long").alias("simple_long"),
        )

    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


def _build_deep_nested_descriptor_set_bytes(spark):
    """Build a descriptor for three levels of nested messages."""
    return _build_proto2_descriptor(spark, "deep_nested.proto", [
        _msg("Inner", [_field("value", 1, "INT32")]),
        _msg("Middle", [
            _field("name", 1, "STRING"),
            _field("inner", 2, "MESSAGE", type_name=".test.Inner"),
        ]),
        _msg("Outer", [
            _field("id", 1, "INT32"),
            _field("middle", 2, "MESSAGE", type_name=".test.Middle"),
        ]),
    ])


@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@ignore_order(local=True)
def test_from_protobuf_deep_nested(local_tmp_path, from_protobuf_fn):
    """
    Test decoding deeply nested messages (3 levels).
    """
    desc_path, desc_bytes = _setup_protobuf_desc(
        local_tmp_path, "deep_nested.desc", _build_deep_nested_descriptor_set_bytes)
    message_name = "test.Outer"

    data_gen = _as_datagen([
        _scalar("id", 1, IntegerGen()),
        _nested("middle", 2, [
            _scalar("name", 1, StringGen()),
            _nested("inner", 2, [_scalar("value", 1, IntegerGen())]),
        ]),
    ])

    def run_on_spark(spark):
        df = gen_df(spark, data_gen)
        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), message_name, desc_path, desc_bytes)
        return df.select(
            decoded.getField("id").alias("id"),
            decoded.getField("middle").alias("middle"),
        )

    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


def _build_repeated_message_descriptor_set_bytes(spark):
    """Build a descriptor for an array-of-struct repeated message field."""
    return _build_proto2_descriptor(spark, "repeated_message.proto", [
        _msg("Item", [
            _field("id", 1, "INT32"),
            _field("name", 2, "STRING"),
        ]),
        _msg("Container", [
            _field("title", 1, "STRING"),
            _field("items", 2, "MESSAGE", label="repeated", type_name=".test.Item"),
        ]),
    ])


@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@ignore_order(local=True)
def test_from_protobuf_repeated_message(local_tmp_path, from_protobuf_fn):
    """
    Test decoding repeated message field (ArrayType of StructType).
    """
    desc_path, desc_bytes = _setup_protobuf_desc(
        local_tmp_path, "repeated_message.desc", _build_repeated_message_descriptor_set_bytes)
    message_name = "test.Container"

    data_gen = _as_datagen([
        _scalar("title", 1, StringGen()),
        _repeated_message("items", 2, [
            _scalar("id", 1, IntegerGen()),
            _scalar("name", 2, StringGen()),
        ], min_len=0, max_len=5),
    ])

    def run_on_spark(spark):
        df = gen_df(spark, data_gen)
        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), message_name, desc_path, desc_bytes)
        return df.select(
            decoded.getField("title").alias("title"),
            decoded.getField("items").alias("items"),
        )

    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


def _build_nested_with_repeated_descriptor_set_bytes(spark):
    """Build a descriptor for a nested message that contains a repeated field."""
    return _build_proto2_descriptor(spark, "nested_with_repeated.proto", [
        _msg("NestedWithRepeated", [
            _field("name", 1, "STRING"),
            _field("values", 2, "INT32", label="repeated"),
            _field("count", 3, "INT32"),
        ]),
        _msg("OuterWithNestedRepeated", [
            _field("id", 1, "INT32"),
            _field("nested", 2, "MESSAGE", type_name=".test.NestedWithRepeated"),
        ]),
    ])


@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@ignore_order(local=True)
def test_from_protobuf_nested_with_repeated(local_tmp_path, from_protobuf_fn):
    """
    Test decoding nested message that contains repeated fields.
    Schema: OuterWithNestedRepeated { id, nested: NestedWithRepeated { name, values[], count } }
    This tests StructType containing StructType containing ArrayType.
    """
    desc_path, desc_bytes = _setup_protobuf_desc(
        local_tmp_path, "nested_with_repeated.desc",
        _build_nested_with_repeated_descriptor_set_bytes)
    message_name = "test.OuterWithNestedRepeated"

    data_gen = _as_datagen([
        _scalar("id", 1, IntegerGen()),
        _nested("nested", 2, [
            _scalar("name", 1, StringGen()),
            _repeated("values", 2, IntegerGen(), min_len=0, max_len=5),
            _scalar("count", 3, IntegerGen()),
        ]),
    ])

    def run_on_spark(spark):
        df = gen_df(spark, data_gen)
        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), message_name, desc_path, desc_bytes)
        return df.select(
            decoded.getField("id").alias("id"),
            decoded.getField("nested").alias("nested"),
        )

    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


def _build_repeated_with_nested_descriptor_set_bytes(spark):
    """Build a descriptor for repeated messages that each contain a nested struct."""
    return _build_proto2_descriptor(spark, "repeated_with_nested.proto", [
        _msg("Inner", [_field("value", 1, "INT32")]),
        _msg("ItemWithNested", [
            _field("id", 1, "INT32"),
            _field("inner", 2, "MESSAGE", type_name=".test.Inner"),
            _field("name", 3, "STRING"),
        ]),
        _msg("ContainerWithNestedItems", [
            _field("title", 1, "STRING"),
            _field("items", 2, "MESSAGE", label="repeated", type_name=".test.ItemWithNested"),
        ]),
    ])


@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@ignore_order(local=True)
def test_from_protobuf_repeated_with_nested(local_tmp_path, from_protobuf_fn):
    """
    Test decoding repeated message that contains nested message.
    Schema: ContainerWithNestedItems { title, items[]: ItemWithNested { id, inner: Inner { value }, name } }
    This tests ArrayType(StructType(StructType)) - nested struct inside repeated message.
    """
    desc_path, desc_bytes = _setup_protobuf_desc(
        local_tmp_path, "repeated_with_nested.desc",
        _build_repeated_with_nested_descriptor_set_bytes)
    message_name = "test.ContainerWithNestedItems"

    data_gen = _as_datagen([
        _scalar("title", 1, StringGen()),
        _repeated_message("items", 2, [
            _scalar("id", 1, IntegerGen()),
            _nested("inner", 2, [_scalar("value", 1, IntegerGen())]),
            _scalar("name", 3, StringGen()),
        ], min_len=0, max_len=3),
    ])

    def run_on_spark(spark):
        df = gen_df(spark, data_gen)
        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), message_name, desc_path, desc_bytes)
        return df.select(
            decoded.getField("title").alias("title"),
            decoded.getField("items").alias("items"),
        )

    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


def _build_packed_repeated_descriptor_set_bytes(spark):
    """Build a descriptor for packed repeated numeric and bool fields."""
    return _build_proto2_descriptor(spark, "packed_repeated.proto", [
        _msg("WithPackedRepeated", [
            _field("id", 1, "INT32"),
            _field("int_values", 2, "INT32", label="repeated", packed=True),
            _field("double_values", 3, "DOUBLE", label="repeated", packed=True),
            _field("bool_values", 4, "BOOL", label="repeated", packed=True),
        ]),
    ])


@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@ignore_order(local=True)
def test_from_protobuf_packed_repeated(local_tmp_path, from_protobuf_fn):
    """
    Test packed repeated fields (int, double, bool) using DataGen.
    """
    desc_path, desc_bytes = _setup_protobuf_desc(
        local_tmp_path, "packed.desc", _build_packed_repeated_descriptor_set_bytes)
    message_name = "test.WithPackedRepeated"

    data_gen = _as_datagen([
        _scalar("id", 1, IntegerGen()),
        _repeated("int_values", 2, IntegerGen(), packed=True, min_len=0, max_len=10),
        _repeated("double_values", 3, DoubleGen(), packed=True, min_len=0, max_len=5),
        _repeated("bool_values", 4, BooleanGen(), packed=True, min_len=0, max_len=5),
    ])

    def run_on_spark(spark):
        df = gen_df(spark, data_gen)
        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), message_name, desc_path, desc_bytes)
        return df.select(
            decoded.getField("id").alias("id"),
            decoded.getField("int_values").alias("int_values"),
            decoded.getField("double_values").alias("double_values"),
            decoded.getField("bool_values").alias("bool_values"),
        )

    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


def _build_repeated_all_types_descriptor_set_bytes(spark):
    """Build a descriptor with repeated fields of several scalar types."""
    return _build_proto2_descriptor(spark, "repeated_all.proto", [
        _msg("WithRepeatedAllTypes", [
            _field("id", 1, "INT32"),
            _field("long_values", 2, "INT64", label="repeated"),
            _field("float_values", 3, "FLOAT", label="repeated"),
            _field("double_values", 4, "DOUBLE", label="repeated"),
            _field("bool_values", 5, "BOOL", label="repeated"),
            _field("bytes_values", 6, "BYTES", label="repeated"),
        ]),
    ])


@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@ignore_order(local=True)
def test_from_protobuf_repeated_all_types(local_tmp_path, from_protobuf_fn):
    """Test repeated fields of various types (int64, float, double, bool, bytes) using DataGen."""
    desc_path, desc_bytes = _setup_protobuf_desc(
        local_tmp_path, "repeated_all.desc", _build_repeated_all_types_descriptor_set_bytes)
    message_name = "test.WithRepeatedAllTypes"

    data_gen = _as_datagen([
        _scalar("id", 1, IntegerGen()),
        _repeated("long_values", 2, LongGen()),
        _repeated("float_values", 3, FloatGen()),
        _repeated("double_values", 4, DoubleGen()),
        _repeated("bool_values", 5, BooleanGen()),
        _repeated("bytes_values", 6, BinaryGen()),
    ])

    def run_on_spark(spark):
        df = gen_df(spark, data_gen)
        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), message_name, desc_path, desc_bytes)
        return df.select(
            decoded.getField("id").alias("id"),
            decoded.getField("long_values").alias("long_values"),
            decoded.getField("float_values").alias("float_values"),
            decoded.getField("double_values").alias("double_values"),
            decoded.getField("bool_values").alias("bool_values"),
            decoded.getField("bytes_values").alias("bytes_values"),
        )

    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@ignore_order(local=True)
def test_from_protobuf_large_repeated_array(local_tmp_path, from_protobuf_fn):
    """
    Test decoding large repeated field (500-1000 elements).
    Stress test for array handling.
    """
    desc_path, desc_bytes = _setup_protobuf_desc(
        local_tmp_path, "repeated_int.desc", _build_repeated_int_descriptor_set_bytes)
    message_name = "test.WithRepeatedInt"

    data_gen = _as_datagen([
        _scalar("id", 1, IntegerGen()),
        _repeated("values", 2, IntegerGen(), min_len=500, max_len=1000),
    ])

    def run_on_spark(spark):
        df = gen_df(spark, data_gen)
        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), message_name, desc_path, desc_bytes)
        return df.select(
            decoded.getField("id").alias("id"),
            f.size(decoded.getField("values")).alias("array_size"),
        )

    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


def _build_signed_int_descriptor_set_bytes(spark):
    """Build a descriptor for zigzag and signed fixed-width integer fields."""
    return _build_proto2_descriptor(spark, "signed_int.proto", [
        _msg("WithSignedInts", [
            _field("si32", 1, "SINT32"),
            _field("si64", 2, "SINT64"),
            _field("sf32", 3, "SFIXED32"),
            _field("sf64", 4, "SFIXED64"),
        ]),
    ])


@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@ignore_order(local=True)
def test_from_protobuf_signed_integers(local_tmp_path, from_protobuf_fn):
    """
    Test decoding signed integer types with zigzag encoding.
    Zigzag: -1 -> 1, 1 -> 2, -2 -> 3, 2 -> 4, etc.
    """
    desc_path, desc_bytes = _setup_protobuf_desc(
        local_tmp_path, "signed.desc", _build_signed_int_descriptor_set_bytes)
    message_name = "test.WithSignedInts"

    data_gen = _as_datagen([
        _scalar("si32", 1, IntegerGen(
            special_cases=[-1, 0, 1, -2147483648, 2147483647]), encoding='zigzag'),
        _scalar("si64", 2, LongGen(
            special_cases=[-1, 0, 1, -9223372036854775808, 9223372036854775807]),
            encoding='zigzag'),
        _scalar("sf32", 3, IntegerGen(
            special_cases=[0, 1, -1, 2147483647, -2147483648]), encoding='fixed'),
        _scalar("sf64", 4, LongGen(
            special_cases=[0, 1, -1]), encoding='fixed'),
    ])

    def run_on_spark(spark):
        df = gen_df(spark, data_gen)
        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), message_name, desc_path, desc_bytes)
        
        return df.select(
            decoded.getField("si32").alias("si32"),
            decoded.getField("si64").alias("si64"),
            decoded.getField("sf32").alias("sf32"),
            decoded.getField("sf64").alias("sf64"),
        )

    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


def _build_fixed_int_descriptor_set_bytes(spark):
    """Build a descriptor for fixed-width integer fields."""
    return _build_proto2_descriptor(spark, "fixed_int.proto", [
        _msg("WithFixedInts", [
            _field("fx32", 1, "FIXED32"),
            _field("fx64", 2, "FIXED64"),
        ]),
    ])


@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@ignore_order(local=True)
def test_from_protobuf_fixed_integers(local_tmp_path, from_protobuf_fn):
    """
    Test decoding fixed-width unsigned integer types.
    """
    desc_path, desc_bytes = _setup_protobuf_desc(
        local_tmp_path, "fixed.desc", _build_fixed_int_descriptor_set_bytes)
    message_name = "test.WithFixedInts"

    data_gen = _as_datagen([
        _scalar("fx32", 1, IntegerGen(
            special_cases=[0, 1, -1, 2147483647, -2147483648]), encoding='fixed'),
        _scalar("fx64", 2, LongGen(
            special_cases=[0, 1, -1]), encoding='fixed'),
    ])

    def run_on_spark(spark):
        df = gen_df(spark, data_gen)
        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), message_name, desc_path, desc_bytes)
        return df.select(
            decoded.getField("fx32").alias("fx32"),
            decoded.getField("fx64").alias("fx64"),
        )

    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


def _build_schema_projection_descriptor_set_bytes(spark):
    """Build a descriptor with nested and repeated struct fields for pruning tests."""
    schema_fields = [
        _field("id", 1, "INT32"),
        _field("name", 2, "STRING"),
        _field("detail", 3, "MESSAGE", type_name=".test.Detail"),
        _field("items", 4, "MESSAGE", label="repeated", type_name=".test.Detail"),
    ]
    return _build_proto2_descriptor(spark, "schema_proj.proto", [
        _msg("Detail", [
            _field("a", 1, "INT32"),
            _field("b", 2, "INT32"),
            _field("c", 3, "STRING"),
        ]),
        _msg("SchemaProj", schema_fields),
    ])


# Field descriptors for SchemaProj: {id, name, detail: {a, b, c}, items[]: {a, b, c}}
_detail_children = [
    _scalar("a", 1, IntegerGen()),
    _scalar("b", 2, IntegerGen()),
    _scalar("c", 3, StringGen()),
]
_schema_proj_schema = _schema("SchemaProjManual", [
    _scalar("id", 1, IntegerGen()),
    _scalar("name", 2, StringGen()),
    _nested("detail", 3, _detail_children),
    _repeated_message("items", 4, _detail_children),
])

_schema_proj_test_data = [
    encode_pb_message(_schema_proj_schema, {
        "id": 1,
        "name": "alice",
        "detail": {"a": 10, "b": 20, "c": "d1"},
        "items": [
            {"a": 100, "b": 200, "c": "i1"},
            {"a": 101, "b": 201, "c": "i2"},
        ],
    }),
    encode_pb_message(_schema_proj_schema, {
        "id": 2,
        "name": "bob",
        "detail": {"a": 30, "b": 40, "c": "d2"},
        "items": [
            {"a": 300, "b": 400, "c": "i3"},
        ],
    }),
    encode_pb_message(_schema_proj_schema, {
        "id": 3,
        "name": "carol",
        "detail": {"a": 50, "b": 60, "c": "d3"},
        "items": [],
    }),
]


_schema_proj_cases = [
    ("nested_single_field", [("id", ("id",)), ("detail_a", ("detail", "a"))]),
    ("nested_two_fields", [("detail_a", ("detail", "a")), ("detail_c", ("detail", "c"))]),
    ("whole_struct_no_pruning", [("id", ("id",)), ("detail", ("detail",))]),
    ("whole_and_subfield", [("detail", ("detail",)), ("detail_a", ("detail", "a"))]),
    ("scalar_plus_nested", [("id", ("id",)), ("name", ("name",)), ("detail_a", ("detail", "a"))]),
    ("repeated_msg_single_subfield", [("id", ("id",)), ("items_a", ("items", "a"))]),
    ("repeated_msg_two_subfields", [("items_a", ("items", "a")), ("items_c", ("items", "c"))]),
    ("repeated_whole_no_pruning", [("id", ("id",)), ("items", ("items",))]),
    ("mix_struct_and_repeated", [("id", ("id",)), ("detail_a", ("detail", "a")), ("items_c", ("items", "c"))]),
]


def _get_field_by_path(expr, path):
    current = expr
    for name in path:
        current = current.getField(name)
    return current


def _schema_projection_data_gen():
    def detail_fields():
        return [
            _scalar("a", 1, IntegerGen()),
            _scalar("b", 2, IntegerGen(
                nullable=False, min_val=-100, max_val=100, special_cases=[])),
            _scalar("c", 3, StringGen("[a-z]{0,12}")),
        ]

    return _as_datagen([
        _scalar("id", 1, IntegerGen(
            nullable=False, min_val=0, max_val=7, special_cases=[])),
        _scalar("name", 2, StringGen("[a-z]{0,16}", nullable=True)),
        _nested("detail", 3, detail_fields()),
        _repeated_message("items", 4, detail_fields(), min_len=0, max_len=4),
    ])


@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@validate_execs_in_gpu_plan("GpuGlobalLimitExec")
@ignore_order(local=True)
def test_from_protobuf_nonzero_offset_binary_view(local_tmp_path, from_protobuf_fn):
    desc_path, desc_bytes = _setup_protobuf_desc(
        local_tmp_path,
        "schema_proj_offset.desc",
        _build_schema_projection_descriptor_set_bytes)
    message_name = "test.SchemaProj"
    data_gen = _schema_projection_data_gen()

    def run_on_spark(spark):
        generated = gen_df(spark, data_gen, length=257, num_slices=1)
        sliced = generated.offset(17)
        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), message_name, desc_path, desc_bytes)
        return sliced.select(
            "id",
            "name",
            "detail",
            "items",
            decoded.getField("id").alias("decoded_id"),
            decoded.getField("name").alias("decoded_name"),
            decoded.getField("detail").alias("decoded_detail"),
            decoded.getField("items").alias("decoded_items")).repartition(1)

    assert_gpu_and_cpu_are_equal_collect(run_on_spark, conf={
        "spark.sql.adaptive.enabled": "false",
        "spark.rapids.sql.batchSizeBytes": "1g",
    })


@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@validate_execs_in_gpu_plan("GpuHashAggregateExec")
@ignore_order(local=True)
def test_from_protobuf_projection_through_aggregate(local_tmp_path, from_protobuf_fn):
    desc_path, desc_bytes = _setup_protobuf_desc(
        local_tmp_path,
        "schema_proj_aggregate.desc",
        _build_schema_projection_descriptor_set_bytes)
    message_name = "test.SchemaProj"
    data_gen = _schema_projection_data_gen()

    def run_on_spark(spark):
        generated = gen_df(spark, data_gen, length=257, num_slices=4)
        partitioned = generated.repartition(4, "id")
        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), message_name, desc_path, desc_bytes)
        decoded_df = partitioned.select("id", "detail", decoded.alias("decoded"))
        return decoded_df.groupBy("id").agg(
            f.max(f.col("detail.a")).alias("expected_max_a"),
            f.max(f.col("decoded.detail.a")).alias("decoded_max_a"))

    assert_gpu_and_cpu_are_equal_collect(run_on_spark, conf={
        "spark.sql.adaptive.enabled": "false",
    })


@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@validate_execs_in_gpu_plan("GpuSortExec")
@ignore_order(local=True)
def test_from_protobuf_projection_through_sort(local_tmp_path, from_protobuf_fn):
    desc_path, desc_bytes = _setup_protobuf_desc(
        local_tmp_path,
        "schema_proj_sort.desc",
        _build_schema_projection_descriptor_set_bytes)
    message_name = "test.SchemaProj"
    data_gen = _schema_projection_data_gen()

    def run_on_spark(spark):
        generated = gen_df(spark, data_gen, length=257, num_slices=1)
        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), message_name, desc_path, desc_bytes)
        decoded_df = generated.select("id", "name", "detail", decoded.alias("decoded"))
        return decoded_df.sortWithinPartitions(
            f.col("decoded.detail.b"),
            f.col("decoded.id"),
            f.col("decoded.name")).select(
                "id",
                "name",
                f.col("detail.b").alias("expected_detail_b"),
                f.col("decoded.id").alias("decoded_id"),
                f.col("decoded.name").alias("decoded_name"),
                f.col("decoded.detail.b").alias("decoded_detail_b"))

    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@validate_execs_in_gpu_plan("GpuRunningWindowExec")
@ignore_order(local=True)
def test_from_protobuf_projection_through_window(local_tmp_path, from_protobuf_fn):
    desc_path, desc_bytes = _setup_protobuf_desc(
        local_tmp_path,
        "schema_proj_window.desc",
        _build_schema_projection_descriptor_set_bytes)
    message_name = "test.SchemaProj"
    data_gen = _schema_projection_data_gen()

    def run_on_spark(spark):
        generated = gen_df(spark, data_gen, length=257, num_slices=4)
        partitioned = generated.repartition(4, "id")
        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), message_name, desc_path, desc_bytes)
        decoded_df = partitioned.select("id", "detail", decoded.alias("decoded"))
        window = Window.partitionBy("id").orderBy(f.col("decoded.detail.b"))
        return decoded_df.select(
            "id",
            f.col("detail.b").alias("expected_detail_b"),
            f.col("decoded.detail.b").alias("decoded_detail_b"),
            f.dense_rank().over(window).alias("decoded_rank"))

    assert_gpu_and_cpu_are_equal_collect(run_on_spark, conf={
        "spark.sql.adaptive.enabled": "false",
    })


@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@pytest.mark.parametrize("boundary", ["alias", "withcolumn"], ids=idfn)
@ignore_order(local=True)
def test_from_protobuf_projection_across_plan_boundary(
        local_tmp_path, from_protobuf_fn, boundary):
    """Schema projection across alias (select→select) and withColumn plan boundaries."""
    desc_path, desc_bytes = _setup_protobuf_desc(
        local_tmp_path, "schema_proj_boundary.desc",
        _build_schema_projection_descriptor_set_bytes)
    message_name = "test.SchemaProj"

    def run_on_spark(spark):
        df = spark.createDataFrame([(row,) for row in _schema_proj_test_data], schema="bin binary")
        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), message_name, desc_path, desc_bytes)
        if boundary == "alias":
            aliased = df.select(decoded.alias("decoded"))
            return aliased.select(
                f.col("decoded").getField("detail").getField("a").alias("detail_a"),
                f.col("decoded").getField("id").alias("id"))
        else:
            with_decoded = df.withColumn("decoded", decoded)
            return with_decoded.select(
                f.col("decoded").getField("items").getField("a").alias("items_a"),
                f.col("decoded").getField("id").alias("id"))

    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@pytest.mark.parametrize("terminal", ["project", "sort"], ids=idfn)
@ignore_order(local=True)
def test_from_protobuf_terminal_whole_struct_and_field(
        local_tmp_path, from_protobuf_fn, terminal):
    desc_path, desc_bytes = _setup_protobuf_desc(
        local_tmp_path, "schema_proj_whole.desc",
        _build_schema_projection_descriptor_set_bytes)

    def run_on_spark(spark):
        df = spark.createDataFrame(
            [(row,) for row in _schema_proj_test_data], schema="bin binary")
        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), "test.SchemaProj", desc_path, desc_bytes)
        result = df.select(
            decoded.alias("whole"),
            decoded.getField("id").alias("id"))
        return result if terminal == "project" else result.sortWithinPartitions("id")

    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


def _build_two_int_projection_descriptor_set_bytes(spark):
    return _build_proto2_descriptor(spark, "two_int_projection.proto", [
        _msg("TwoInts", [
            _field("first", 1, "INT32"),
            _field("last", 2, "INT32"),
        ]),
    ])


@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@allow_non_gpu("ProjectExec")
@validate_execs_in_gpu_plan("GpuProjectExec")
@ignore_order(local=True)
def test_from_protobuf_cpu_project_does_not_publish_pruned_schema(
        local_tmp_path, from_protobuf_fn):
    desc_path, desc_bytes = _setup_protobuf_desc(
        local_tmp_path, "two_int_projection.desc",
        _build_two_int_projection_descriptor_set_bytes)
    rows = [
        (_encode_tag(1, 0) + _encode_varint(11) +
         _encode_tag(2, 0) + _encode_varint(33),),
    ]

    def run_on_spark(spark):
        df = spark.createDataFrame(rows, schema="bin binary")
        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), "test.TwoInts", desc_path, desc_bytes)
        with_decoded = df.select(
            decoded.alias("decoded"),
            f.rand(42).alias("force_cpu"))
        return with_decoded.select(
            f.col("decoded").getField("last").alias("last"),
            f.col("force_cpu"))

    assert_gpu_and_cpu_are_equal_collect(run_on_spark, conf={
        "spark.rapids.sql.expression.cpuBridge.enabled": "true",
        "spark.rapids.sql.expression.Rand": "false",
    })


def _build_dual_message_projection_descriptor_set_bytes(spark):
    """Build descriptors for two logical views over the same binary payload column."""
    return _build_proto2_descriptor(spark, "dual_projection.proto", [
        _msg("NestedPayload", [_field("count", 1, "INT32")]),
        _msg("BytesView", [
            _field("status", 1, "INT32"),
            _field("payload", 2, "BYTES"),
        ]),
        _msg("NestedView", [
            _field("status", 1, "INT32"),
            _field("payload", 2, "MESSAGE", type_name=".test.NestedPayload"),
        ]),
    ])


@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@ignore_order(local=True)
def test_from_protobuf_different_messages_same_binary_column_do_not_interfere(
        local_tmp_path, from_protobuf_fn):
    desc_path, desc_bytes = _setup_protobuf_desc(
        local_tmp_path, "dual_projection.desc",
        _build_dual_message_projection_descriptor_set_bytes)

    payload_keep = _encode_tag(1, 0) + _encode_varint(7)
    payload_drop = _encode_tag(1, 0) + _encode_varint(9)
    row_keep = (_encode_tag(1, 0) + _encode_varint(1) +
                _encode_tag(2, 2) + _encode_varint(len(payload_keep)) + payload_keep)
    row_drop = (_encode_tag(1, 0) + _encode_varint(0) +
                _encode_tag(2, 2) + _encode_varint(len(payload_drop)) + payload_drop)

    def run_on_spark(spark):
        df = spark.createDataFrame([(row_keep,), (row_drop,)], schema="bin binary")
        bytes_view = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), "test.BytesView", desc_path, desc_bytes)
        nested_view = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), "test.NestedView", desc_path, desc_bytes)
        return df.filter(bytes_view.getField("status") == 1).select(
            nested_view.getField("payload").getField("count").alias("payload_count"))

    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


def _build_deep_nested_5_level_descriptor_set_bytes(spark):
    """Build a descriptor for a five-level nested message chain."""
    return _build_proto2_descriptor(spark, "deep_nested_5_level.proto", [
        _msg("Level5", [_field("val5", 1, "INT32")]),
        _msg("Level4", [
            _field("val4", 1, "INT32"),
            _field("level5", 2, "MESSAGE", type_name=".test.Level5"),
        ]),
        _msg("Level3", [
            _field("val3", 1, "INT32"),
            _field("level4", 2, "MESSAGE", type_name=".test.Level4"),
        ]),
        _msg("Level2", [
            _field("val2", 1, "INT32"),
            _field("level3", 2, "MESSAGE", type_name=".test.Level3"),
        ]),
        _msg("Level1", [
            _field("val1", 1, "INT32"),
            _field("level2", 2, "MESSAGE", type_name=".test.Level2"),
        ]),
    ])


@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@ignore_order(local=True)
def test_from_protobuf_deep_nesting_5_levels(local_tmp_path, from_protobuf_fn):
    desc_path, desc_bytes = _setup_protobuf_desc(
        local_tmp_path, "deep_nested_5_level.desc",
        _build_deep_nested_5_level_descriptor_set_bytes)
    message_name = "test.Level1"
    data_gen = _as_datagen([
        _scalar("val1", 1, IntegerGen()),
        _nested("level2", 2, [
            _scalar("val2", 1, IntegerGen()),
            _nested("level3", 2, [
                _scalar("val3", 1, IntegerGen()),
                _nested("level4", 2, [
                    _scalar("val4", 1, IntegerGen()),
                    _nested("level5", 2, [
                        _scalar("val5", 1, IntegerGen()),
                    ]),
                ]),
            ]),
        ]),
    ])
    def run_on_spark(spark):
        df = gen_df(spark, data_gen)
        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), message_name, desc_path, desc_bytes)
        return df.select(
            decoded.getField("val1").alias("val1"),
            decoded.getField("level2").alias("level2"),
        )
    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@pytest.mark.parametrize("case_id,select_specs", _schema_proj_cases, ids=lambda c: c[0] if isinstance(c, tuple) else str(c))
@ignore_order(local=True)
def test_from_protobuf_schema_projection_cases(
        local_tmp_path, from_protobuf_fn, case_id, select_specs):
    """Parametrized nested-schema projection tests."""
    desc_path, desc_bytes = _setup_protobuf_desc(
        local_tmp_path, "schema_proj.desc", _build_schema_projection_descriptor_set_bytes)
    message_name = "test.SchemaProj"

    def run_on_spark(spark):
        df = spark.createDataFrame(
            [(d,) for d in _schema_proj_test_data], schema="bin binary")
        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), message_name, desc_path, desc_bytes)
        selected = [_get_field_by_path(decoded, path).alias(alias)
                    for alias, path in select_specs]
        return df.select(*selected)

    assert_gpu_and_cpu_are_equal_collect(run_on_spark)

def _build_name_collision_descriptor_set_bytes(spark):
    """Build a regression schema with same-named fields in unrelated nested messages."""
    return _build_proto2_descriptor(spark, "name_collision.proto", [
        _msg("User", [
            _field("age", 1, "INT32"),
            _field("id", 2, "INT32"),
        ]),
        _msg("Ad", [_field("id", 1, "INT32")]),
        _msg("Event", [
            _field("user_info", 1, "MESSAGE", type_name=".test.User"),
            _field("ad_info", 2, "MESSAGE", type_name=".test.Ad"),
        ]),
    ])

@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@ignore_order(local=True)
def test_from_protobuf_bug1_name_collision(local_tmp_path, from_protobuf_fn):
    desc_path, desc_bytes = _setup_protobuf_desc(
        local_tmp_path, "name_collision.desc",
        _build_name_collision_descriptor_set_bytes)
    message_name = "test.Event"

    data_gen = _as_datagen([
        _nested("user_info", 1, [
            _scalar("age", 1, IntegerGen()),
            _scalar("id", 2, IntegerGen()),
        ]),
        _nested("ad_info", 2, [
            _scalar("id", 1, IntegerGen()),
        ]),
    ])

    def run_on_spark(spark):
        df = gen_df(spark, data_gen)
        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), message_name, desc_path, desc_bytes)
        
        return df.select(
            decoded.getField("user_info").getField("age").alias("age"),
            decoded.getField("user_info").getField("id").alias("user_id"),
            decoded.getField("ad_info").getField("id").alias("ad_id")
        )

    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


def _build_filter_jump_descriptor_set_bytes(spark):
    """Build a minimal descriptor used by the filter-jump regression test."""
    return _build_proto2_descriptor(spark, "filter_jump.proto", [
        _msg("Event", [
            _field("status", 1, "INT32"),
            _field("ad_info", 2, "STRING"),
        ]),
    ])

@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@ignore_order(local=True)
def test_from_protobuf_bug2_filter_jump(local_tmp_path, from_protobuf_fn):
    desc_path, desc_bytes = _setup_protobuf_desc(
        local_tmp_path, "filter_jump.desc",
        _build_filter_jump_descriptor_set_bytes)
    message_name = "test.Event"

    data_gen = _as_datagen([
        _scalar("status", 1, IntegerGen(min_val=1, max_val=1)),
        _scalar("ad_info", 2, StringGen()),
    ])

    def run_on_spark(spark):
        df = gen_df(spark, data_gen)
        pb_expr1 = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), message_name, desc_path, desc_bytes)
        pb_expr2 = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), message_name, desc_path, desc_bytes)
        
        return df.filter(pb_expr1.getField("status") == 1).select(pb_expr2.getField("ad_info").alias("ad_info"))

    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


def _build_unrelated_struct_name_collision_descriptor_set_bytes(spark):
    """Build a regression schema where an unrelated nested struct shares a field name."""
    return _build_proto2_descriptor(spark, "unrelated_struct.proto", [
        _msg("Nested", [
            _field("dummy", 1, "INT32"),
            _field("winfoid", 2, "INT32"),
        ]),
        _msg("Event", [
            _field("ad_info", 1, "MESSAGE", type_name=".test.Nested"),
        ]),
    ])

@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@ignore_order(local=True)
def test_from_protobuf_bug3_unrelated_struct_name_collision(
        spark_tmp_path, local_tmp_path, from_protobuf_fn):
    desc_path, desc_bytes = _setup_protobuf_desc(
        local_tmp_path, "unrelated_struct.desc",
        _build_unrelated_struct_name_collision_descriptor_set_bytes)
    message_name = "test.Event"

    data_gen = _as_datagen([
        _nested("ad_info", 1, [
            _scalar("dummy", 1, IntegerGen()),
            _scalar("winfoid", 2, IntegerGen()),
        ]),
    ])

    def run_on_spark(spark):
        df = gen_df(spark, data_gen)
        # Write to parquet to prevent Catalyst from optimizing away the GetStructField,
        # and to ensure it runs on the GPU.
        df_with_other = df.withColumn("other_struct",
                                      f.struct(f.lit("hello").alias("dummy_str"), f.lit(42).alias("winfoid")))

        path = spark_tmp_path + "/bug3_data.parquet"
        df_with_other.write.mode("overwrite").parquet(path)
        read_df = spark.read.parquet(path)
        
        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), message_name, desc_path, desc_bytes)
        
        # We only select decoded.ad_info.winfoid, so dummy is pruned.
        # winfoid gets ordinal 0 in the pruned schema.
        # But for other_struct, winfoid is ordinal 1.
        # Regression coverage: protobuf nested pruning must not affect unrelated structs
        # that happen to use the same field name.
        return read_df.select(
            decoded.getField("ad_info").getField("winfoid").alias("pb_winfoid"),
            f.col("other_struct").getField("winfoid").alias("other_winfoid")
        )

    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


def _build_nested_depth_descriptor_set_bytes(spark, depth):
    messages = []
    for i in range(depth, 0, -1):
        fields = [_field(f"val{i}", 1, "INT32")]
        if i < depth:
            fields.append(
                _field(f"level{i + 1}", 2, "MESSAGE", type_name=f".test.Level{i + 1}")
            )
        messages.append(_msg(f"Level{i}", fields))
    return _build_proto2_descriptor(spark, f"nested_depth_{depth}.proto", messages)


def _build_nested_depth_fields(level, depth):
    fields = [_scalar(f"val{level}", 1, IntegerGen())]
    if level < depth:
        fields.append(_nested(
            f"level{level + 1}", 2, _build_nested_depth_fields(level + 1, depth)))
    return fields


def _build_nested_depth_value(level, depth):
    value = {f"val{level}": level}
    if level < depth:
        value[f"level{level + 1}"] = _build_nested_depth_value(level + 1, depth)
    return value


def _build_many_repeated_fields_descriptor_set_bytes(spark, field_count):
    return _build_proto2_descriptor(spark, "many_repeated.proto", [
        _msg("ManyRepeated", [
            _field(f"values_{number}", number, "INT32", label="repeated")
            for number in range(1, field_count + 1)
        ]),
    ])


@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@pytest.mark.parametrize("select_whole_message", [True, False], ids=["whole", "pruned"])
@allow_non_gpu("ProtobufDataToCatalyst")
@ignore_order(local=True)
def test_from_protobuf_repeated_field_capacity_falls_back(
        local_tmp_path, from_protobuf_fn, select_whole_message):
    field_count = 33
    desc_path, desc_bytes = _setup_protobuf_desc(
        local_tmp_path,
        "many_repeated_33.desc",
        lambda spark: _build_many_repeated_fields_descriptor_set_bytes(spark, field_count))
    row = (_encode_tag(1, 0) + _encode_varint(7) +
           _encode_tag(field_count, 0) + _encode_varint(9))

    def run_on_spark(spark):
        df = spark.createDataFrame([(0, row), (1, None)], schema="idx int, bin binary")
        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), "test.ManyRepeated", desc_path, desc_bytes)
        if select_whole_message:
            return df.select("idx", decoded.alias("decoded"))
        return df.select("idx", decoded.getField("values_1").alias("values_1"))

    assert_gpu_fallback_collect(run_on_spark, "ProtobufDataToCatalyst")


@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@ignore_order(local=True)
def test_from_protobuf_repeated_field_capacity_boundary(
        local_tmp_path, from_protobuf_fn):
    field_count = 32
    desc_path, desc_bytes = _setup_protobuf_desc(
        local_tmp_path,
        f"many_repeated_{field_count}.desc",
        lambda spark: _build_many_repeated_fields_descriptor_set_bytes(spark, field_count))
    row = (_encode_tag(1, 0) + _encode_varint(7) +
           _encode_tag(field_count, 0) + _encode_varint(9))

    def run_on_spark(spark):
        df = spark.createDataFrame([(0, row), (1, None)], schema="idx int, bin binary")
        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), "test.ManyRepeated", desc_path, desc_bytes)
        return df.select("idx", decoded.alias("decoded"))

    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@ignore_order(local=True)
def test_from_protobuf_max_depth_boundary(local_tmp_path, from_protobuf_fn):
    depth = 10
    desc_path, desc_bytes = _setup_protobuf_desc(
        local_tmp_path,
        f"nested_depth_{depth}.desc",
        lambda spark: _build_nested_depth_descriptor_set_bytes(spark, depth))
    schema = _schema("Level1", _build_nested_depth_fields(1, depth))
    row = schema.encode(_build_nested_depth_value(1, depth))

    def run_on_spark(spark):
        decoded = _call_from_protobuf(
            from_protobuf_fn,
            f.col("bin"),
            "test.Level1",
            desc_path,
            desc_bytes)
        field = decoded
        for i in range(2, depth + 1):
            field = field.getField(f"level{i}")
        return spark.createDataFrame([(row,)], ["bin"]).select(
            field.getField(f"val{depth}").alias(f"val{depth}"))

    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@allow_non_gpu("ProtobufDataToCatalyst")
@ignore_order(local=True)
def test_from_protobuf_max_depth_falls_back(local_tmp_path, from_protobuf_fn):
    depth = 12
    desc_path, desc_bytes = _setup_protobuf_desc(
        local_tmp_path,
        f"nested_depth_{depth}.desc",
        lambda spark: _build_nested_depth_descriptor_set_bytes(spark, depth))
    message_name = "test.Level1"
    schema = _schema("Level1", _build_nested_depth_fields(1, depth))
    row = schema.encode(_build_nested_depth_value(1, depth))

    def run_on_spark(spark):
        df = spark.createDataFrame([(row,)], ["bin"])
        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), message_name, desc_path, desc_bytes)
        field = decoded
        for i in range(2, depth + 1):
            field = field.getField(f"level{i}")
        return df.select(field.getField(f"val{depth}").alias(f"val{depth}"))

    assert_gpu_fallback_collect(run_on_spark, "ProtobufDataToCatalyst")


def _encode_varint(value):
    """Encode a non-negative integer as a protobuf varint (for hand-crafting test bytes)."""
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


def _encode_simple_message(i32_value, s_value):
    buf = bytearray()
    buf += _encode_tag(1, 0)
    buf += _encode_varint(i32_value & 0xFFFFFFFFFFFFFFFF)
    s_bytes = s_value.encode("utf-8")
    buf += _encode_tag(2, 2)
    buf += _encode_varint(len(s_bytes))
    buf += s_bytes
    return bytes(buf)


# This keeps the baseline API coverage independent of protobuf runtime versions.
_simple_desc_bytes = bytes.fromhex(
    "0a360a0c73696d706c652e70726f746f12047465737422200a0653696d706c65"
    "120b0a0369333218012001280512090a0173180220012809")

_nested_smoke_desc_bytes = bytes.fromhex(
    "0a8e010a126e65737465645f736d6f6b652e70726f746f120474657374221d0a"
    "054368696c6412090a016118012001280512090a0162180220012809224b0a04"
    "526f6f74120b0a03746f70180120012805121a0a056368696c6418022001280b"
    "320b2e746573742e4368696c64121a0a056974656d7318032003280b320b2e74"
    "6573742e4368696c64620670726f746f32")

# Keep these cross-file contracts independent of Spark's shaded descriptor runtime.
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

_oneof_smoke_desc_bytes = bytes.fromhex(
    "0a5e0a116f6e656f665f736d6f6b652e70726f746f120474657374223b0a0643"
    "686f696365120b0a01691801200128054800120b0a0173180220012809480012"
    "0d0a056f7468657218032001280542080a0663686f696365620670726f746f32")

_nested_smoke_message = pb.message("Root", [
    pb.int32("top", 1),
    pb.nested("child", 2, [
        pb.int32("a", 1),
        pb.string("b", 2),
    ]),
    pb.repeated_message("items", 3, [
        pb.int32("a", 1),
        pb.string("b", 2),
    ]),
])


@pytest.fixture
def simple_desc(local_tmp_path):
    desc_path = os.path.join(local_tmp_path, "simple.desc")
    with open(desc_path, "wb") as fp:
        fp.write(_simple_desc_bytes)
    return desc_path, _simple_desc_bytes


@pytest.fixture
def nested_smoke_desc(local_tmp_path):
    desc_path = os.path.join(local_tmp_path, "nested_smoke.desc")
    with open(desc_path, "wb") as fp:
        fp.write(_nested_smoke_desc_bytes)
    return desc_path, _nested_smoke_desc_bytes


@pytest.fixture
def protobuf_semantics_desc(local_tmp_path):
    desc_path = os.path.join(local_tmp_path, "protobuf_semantics.desc")
    with open(desc_path, "wb") as fp:
        fp.write(_protobuf_semantics_desc_bytes)
    return desc_path, _protobuf_semantics_desc_bytes


@pytest.fixture
def oneof_smoke_desc(local_tmp_path):
    desc_path = os.path.join(local_tmp_path, "oneof_smoke.desc")
    with open(desc_path, "wb") as fp:
        fp.write(_oneof_smoke_desc_bytes)
    return desc_path, _oneof_smoke_desc_bytes


def _make_smoke_df(spark):
    values = [(1, "a"), (-2, "bb"), (0, ""), (12345, "hello")]
    encoded = [(_encode_simple_message(i, s),) for i, s in values]
    return spark.createDataFrame(encoded, ["bin"])


@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
def test_from_protobuf_smoke_path_api(simple_desc, from_protobuf_fn):
    desc_path, _ = simple_desc

    def run_on_spark(spark):
        return _make_smoke_df(spark).select(
            from_protobuf_fn(f.col("bin"), "test.Simple", desc_path).alias("decoded"))

    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
def test_from_protobuf_smoke_nested_projection(nested_smoke_desc, from_protobuf_fn):
    desc_path, _ = nested_smoke_desc
    values = [
        {"top": 1, "child": {"a": 2, "b": "child"},
         "items": [{"a": 3, "b": "first"}, {"a": 4, "b": "second"}]},
        {"top": 5, "child": None, "items": []},
    ]

    def run_on_spark(spark):
        encoded = [(_nested_smoke_message.encode(value),) for value in values]
        decoded = _call_from_protobuf(
            from_protobuf_fn,
            f.col("bin"),
            "test.Root",
            desc_path,
            _nested_smoke_desc_bytes)
        return spark.createDataFrame(encoded, ["bin"]).select(
            decoded.getField("top").alias("top"),
            decoded.getField("child").getField("a").alias("child_a"),
            decoded.getField("items")[0].getField("b").alias("first_item_b"))

    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
def test_from_protobuf_optional_nested_message_presence(
        nested_smoke_desc, from_protobuf_fn):
    desc_path, desc_bytes = nested_smoke_desc
    rows = [
        (0, b""),
        (1, b"\x12\x00"),
        (2, b"\x12\x02\x08\x07"),
    ]

    def run_on_spark(spark):
        decoded = _call_from_protobuf(
            from_protobuf_fn,
            f.col("bin"),
            "test.Root",
            desc_path,
            desc_bytes)
        return spark.createDataFrame(rows, ["id", "bin"]).select(
            "id", decoded.getField("child").alias("child")).orderBy("id")

    cpu_rows = with_cpu_session(
        lambda spark: run_on_spark(spark).orderBy("id").collect())
    assert cpu_rows[0]["child"] is None
    assert cpu_rows[1]["child"].asDict() == {"a": None, "b": None}
    assert cpu_rows[2]["child"].asDict() == {"a": 7, "b": None}
    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
def test_from_protobuf_proto2_transitive_import(
        protobuf_semantics_desc, from_protobuf_fn):
    desc_path, desc_bytes = protobuf_semantics_desc

    def run_on_spark(spark):
        decoded = _call_from_protobuf(
            from_protobuf_fn,
            f.col("bin"),
            "test.ImportedRoot",
            desc_path,
            desc_bytes)
        return spark.createDataFrame(
            [(b"\x0a\x04\x0a\x02\x08\x07",)], ["bin"]).select(
                decoded.getField("child").getField("grandchild")
                    .getField("value").alias("value"))

    cpu_rows = with_cpu_session(lambda spark: run_on_spark(spark).collect())
    assert [row["value"] for row in cpu_rows] == [7]
    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
def test_from_protobuf_unsigned_boundaries(
        protobuf_semantics_desc, from_protobuf_fn):
    desc_path, desc_bytes = protobuf_semantics_desc
    row = (
        _encode_tag(1, 0) + _encode_varint(2) +
        _encode_tag(2, 0) + _encode_varint((1 << 64) - 1) +
        _encode_tag(3, 0) + _encode_varint(1 << 31) +
        _encode_tag(4, 0) + _encode_varint(0) +
        _encode_tag(4, 0) + _encode_varint((1 << 32) - 1) +
        _encode_tag(5, 0) + _encode_varint(1 << 63) +
        _encode_tag(5, 0) + _encode_varint((1 << 64) - 1))

    def run_on_spark(spark):
        decoded = _call_from_protobuf(
            from_protobuf_fn,
            f.col("bin"),
            "test.UnsignedRoot",
            desc_path,
            desc_bytes)
        return spark.createDataFrame([(row,)], ["bin"]).select(decoded.alias("decoded"))

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


@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
def test_from_protobuf_multilevel_repeated_messages(
        protobuf_semantics_desc, from_protobuf_fn):
    desc_path, desc_bytes = protobuf_semantics_desc
    schema = pb.message("RepeatedRoot", [
        pb.repeated_message("children", 1, [
            pb.repeated_message("leaves", 1, [
                pb.repeated_message("values", 1, [
                    pb.int32("value", 1),
                ]),
            ]),
        ]),
    ])
    values = [
        {"children": []},
        {"children": [{"leaves": []}]},
        {"children": [
            {"leaves": [{"values": [{"value": 1}, {"value": 2}]}]},
            {"leaves": [{"values": []}]},
        ]},
    ]

    def run_on_spark(spark):
        decoded = _call_from_protobuf(
            from_protobuf_fn,
            f.col("bin"),
            "test.RepeatedRoot",
            desc_path,
            desc_bytes)
        rows = [(index, schema.encode(value)) for index, value in enumerate(values)]
        return spark.createDataFrame(rows, ["id", "bin"]).select(
            "id", decoded.getField("children").alias("children")).orderBy("id")

    cpu_rows = with_cpu_session(lambda spark: run_on_spark(spark).collect())
    assert [row.asDict(recursive=True)["children"] for row in cpu_rows] == [
        value["children"] for value in values]
    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@allow_non_gpu("ProtobufDataToCatalyst")
def test_from_protobuf_smoke_oneof_falls_back(oneof_smoke_desc, from_protobuf_fn):
    desc_path, desc_bytes = oneof_smoke_desc
    rows = [
        (_encode_tag(1, 0) + _encode_varint(7) +
         _encode_tag(3, 0) + _encode_varint(9)),
        (_encode_tag(2, 2) + _encode_varint(1) + b"x" +
         _encode_tag(3, 0) + _encode_varint(10)),
    ]

    def run_on_spark(spark):
        decoded = _call_from_protobuf(
            from_protobuf_fn,
            f.col("bin"),
            "test.Choice",
            desc_path,
            desc_bytes)
        return spark.createDataFrame([(row,) for row in rows], ["bin"]).select(
            decoded.alias("decoded"))

    assert_gpu_fallback_collect(run_on_spark, "ProtobufDataToCatalyst")


@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
def test_from_protobuf_smoke_binary_descriptor_api(simple_desc, from_protobuf_fn):
    if "binaryDescriptorSet" not in inspect.signature(from_protobuf_fn).parameters:
        pytest.skip("binaryDescriptorSet kwarg is Spark 3.5+ only")
    _, desc_bytes = simple_desc

    def run_on_spark(spark):
        return _make_smoke_df(spark).select(
            from_protobuf_fn(
                f.col("bin"),
                "test.Simple",
                binaryDescriptorSet=bytearray(desc_bytes)).alias("decoded"))

    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


# ===========================================================================
# Regression tests for known bugs found by code review
# ===========================================================================

@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@allow_non_gpu("ProtobufDataToCatalyst")
def test_from_protobuf_oneof_member_falls_back(local_tmp_path, from_protobuf_fn):
    desc_path, desc_bytes = _setup_protobuf_desc(
        local_tmp_path, "oneof.desc", _build_oneof_descriptor_set_bytes)
    message_name = "test.Choice"
    row = (_encode_tag(1, 0) + _encode_varint(7) +
           _encode_tag(2, 2) + _encode_varint(1) + b"x" +
           _encode_tag(3, 0) + _encode_varint(9))

    def run_on_spark(spark):
        df = spark.createDataFrame([(0, row), (1, None)], schema="idx int, bin binary")
        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), message_name, desc_path, desc_bytes)
        return df.select(
            "idx",
            decoded.getField("int_value").alias("int_value"),
            decoded.getField("sibling").alias("sibling"))

    assert_gpu_fallback_collect(run_on_spark, "ProtobufDataToCatalyst")


@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@allow_non_gpu("ProtobufDataToCatalyst")
def test_from_protobuf_nested_oneof_member_falls_back(local_tmp_path, from_protobuf_fn):
    desc_path, desc_bytes = _setup_protobuf_desc(
        local_tmp_path, "nested_oneof.desc", _build_nested_oneof_descriptor_set_bytes)
    child = (_encode_tag(1, 0) + _encode_varint(7) +
             _encode_tag(2, 2) + _encode_varint(1) + b"x")
    row = (_encode_tag(1, 2) + _encode_varint(len(child)) + child +
           _encode_tag(2, 0) + _encode_varint(9))

    def run_on_spark(spark):
        df = spark.createDataFrame([(0, row), (1, None)], schema="idx int, bin binary")
        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), "test.OuterChoice", desc_path, desc_bytes)
        return df.select(
            "idx",
            decoded.getField("child").getField("int_value").alias("int_value"),
            decoded.getField("sibling").alias("sibling"))

    assert_gpu_fallback_collect(run_on_spark, "ProtobufDataToCatalyst")


@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@allow_non_gpu("ProtobufDataToCatalyst")
def test_from_protobuf_unselected_oneof_message_with_malformed_payload_falls_back(
        local_tmp_path, from_protobuf_fn):
    desc_path, desc_bytes = _setup_protobuf_desc(
        local_tmp_path, "oneof_message.desc", _build_oneof_message_descriptor_set_bytes)
    malformed_child = _encode_tag(1, 0)
    row = (_encode_tag(1, 2) + _encode_varint(len(malformed_child)) + malformed_child +
           _encode_tag(3, 0) + _encode_varint(9))

    def run_on_spark(spark):
        df = spark.createDataFrame([(0, row), (1, None)], schema="idx int, bin binary")
        decoded = _call_from_protobuf(
            from_protobuf_fn,
            f.col("bin"),
            "test.ChoiceWithMessage",
            desc_path,
            desc_bytes,
            options={"mode": "PERMISSIVE"})
        return df.select(
            "idx",
            decoded.getField("sibling").alias("sibling"))

    assert_gpu_fallback_collect(run_on_spark, "ProtobufDataToCatalyst")


# ---------------------------------------------------------------------------
# Bug 1: BOOL8 truncation — non-canonical bool varint values >= 256
#
# Protobuf spec: bool is a varint; any non-zero value means true.
# CPU decoder (protobuf-java): CodedInputStream.readBool() = readRawVarint64() != 0  →  true
# GPU decoder: extract_varint_kernel writes static_cast<uint8_t>(v).
#   For v = 256, static_cast<uint8_t>(256) == 0  →  false.  BUG.
# ---------------------------------------------------------------------------

@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@ignore_order(local=True)
def test_from_protobuf_bool_noncanonical_varint_scalar(local_tmp_path, from_protobuf_fn):
    """Regression test: scalar bool encoded as non-canonical varint (e.g. 256) must decode as true.

    Protobuf allows any non-zero varint for bool true. The GPU decoder previously
    truncated to uint8_t, causing values >= 256 to wrap to 0 (false).
    """
    desc_path, desc_bytes = _setup_protobuf_desc(
        local_tmp_path, "simple_bool_bug.desc", _build_simple_descriptor_set_bytes)
    message_name = "test.Simple"

    # varint(256) = 0x80 0x02, varint(512) = 0x80 0x04 — valid non-canonical bool true
    row_bool_256 = _encode_tag(1, 0) + _encode_varint(256) + \
                   _encode_tag(2, 0) + _encode_varint(99)

    # Control row: canonical bool true (varint 1) — should work on both
    row_bool_1 = _encode_tag(1, 0) + _encode_varint(1) + \
                 _encode_tag(2, 0) + _encode_varint(100)

    # Another non-canonical value: varint(512)
    row_bool_512 = _encode_tag(1, 0) + _encode_varint(512) + \
                   _encode_tag(2, 0) + _encode_varint(101)

    def run_on_spark(spark):
        df = spark.createDataFrame(
            [(row_bool_256,), (row_bool_1,), (row_bool_512,)],
            schema="bin binary",
        )
        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), message_name, desc_path, desc_bytes)
        return df.select(
            decoded.getField("b").alias("b"),
            decoded.getField("i32").alias("i32"),
        )

    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


def _build_repeated_bool_descriptor_set_bytes(spark):
    """Build a descriptor for an optional id plus repeated bool flags."""
    return _build_proto2_descriptor(spark, "repeated_bool.proto", [
        _msg("WithRepeatedBool", [
            _field("id", 1, "INT32"),
            _field("flags", 2, "BOOL", label="repeated"),
        ]),
    ])


@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@ignore_order(local=True)
def test_from_protobuf_bool_noncanonical_varint_repeated(local_tmp_path, from_protobuf_fn):
    """Regression test: repeated bool with non-canonical varint values must all decode as true.

    Same uint8_t truncation issue as the scalar case, exercised with repeated fields.
    """
    desc_path, desc_bytes = _setup_protobuf_desc(
        local_tmp_path, "repeated_bool_bug.desc", _build_repeated_bool_descriptor_set_bytes)
    message_name = "test.WithRepeatedBool"

    # Repeated bool field 2 (wire type 0 = varint), unpacked.
    # Three elements: varint(256), varint(1), varint(512) — all should decode as true.
    row = (_encode_tag(1, 0) + _encode_varint(42) +
           _encode_tag(2, 0) + _encode_varint(256) +
           _encode_tag(2, 0) + _encode_varint(1) +
           _encode_tag(2, 0) + _encode_varint(512))

    def run_on_spark(spark):
        df = spark.createDataFrame([(row,)], schema="bin binary")
        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), message_name, desc_path, desc_bytes)
        return df.select(
            decoded.getField("id").alias("id"),
            decoded.getField("flags").alias("flags"),
        )

    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


# ---------------------------------------------------------------------------
# Regression guard: nested message child field default values
# ---------------------------------------------------------------------------

def _build_nested_with_defaults_descriptor_set_bytes(spark):
    """Build a descriptor with proto2 defaults inside a nested child struct."""
    return _build_proto2_descriptor(spark, "nested_defaults.proto", [
        _msg("Inner", [
            _field("count", 1, "INT32", default=42),
            _field("label", 2, "STRING", default="hello"),
            _field("flag", 3, "BOOL", default=True),
        ]),
        _msg("OuterWithNestedDefaults", [
            _field("id", 1, "INT32"),
            _field("inner", 2, "MESSAGE", type_name=".test.Inner"),
        ]),
    ])


@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@ignore_order(local=True)
def test_from_protobuf_nested_child_default_values(local_tmp_path, from_protobuf_fn):
    """Regression test: proto2 default values for fields inside nested messages must be honored.

    When `inner` is present but its child fields are absent, the decoder must
    return the proto2 defaults (count=42, label="hello", flag=true), not null.
    """
    desc_path, desc_bytes = _setup_protobuf_desc(
        local_tmp_path, "nested_defaults.desc",
        _build_nested_with_defaults_descriptor_set_bytes)
    message_name = "test.OuterWithNestedDefaults"

    # Row 1: outer.id = 10, inner is present but EMPTY (0-length nested message).
    #   Wire: field 1 varint(10), field 2 length-delimited with length 0.
    #   CPU should fill inner.count=42, inner.label="hello", inner.flag=true.
    row_empty_inner = (_encode_tag(1, 0) + _encode_varint(10) +
                       _encode_tag(2, 2) + _encode_varint(0))

    # Row 2: outer.id = 20, inner has only count=7 (label and flag should get defaults).
    inner_partial = _encode_tag(1, 0) + _encode_varint(7)
    row_partial_inner = (_encode_tag(1, 0) + _encode_varint(20) +
                         _encode_tag(2, 2) + _encode_varint(len(inner_partial)) +
                         inner_partial)

    # Row 3: outer.id = 30, inner is fully absent → inner itself is null.
    row_no_inner = _encode_tag(1, 0) + _encode_varint(30)

    def run_on_spark(spark):
        df = spark.createDataFrame(
            [(row_empty_inner,), (row_partial_inner,), (row_no_inner,)],
            schema="bin binary",
        )
        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), message_name, desc_path, desc_bytes)
        return df.select(
            decoded.getField("id").alias("id"),
            decoded.getField("inner").getField("count").alias("inner_count"),
            decoded.getField("inner").getField("label").alias("inner_label"),
            decoded.getField("inner").getField("flag").alias("inner_flag"),
        )

    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


# ===========================================================================
# Deep nested schema pruning tests
#
# These verify that the GPU path correctly prunes nested fields at depth > 2.
# Previously, collectStructFieldReferences only recognized 2-level
# GetStructField chains, so accessing decoded.level2.level3.val3 would
# decode ALL of level3's children instead of only val3.
# ===========================================================================

def _deep_5_level_data_gen():
    return _as_datagen([
        _scalar("val1", 1, IntegerGen()),
        _nested("level2", 2, [
            _scalar("val2", 1, IntegerGen()),
            _nested("level3", 2, [
                _scalar("val3", 1, IntegerGen()),
                _nested("level4", 2, [
                    _scalar("val4", 1, IntegerGen()),
                    _nested("level5", 2, [
                        _scalar("val5", 1, IntegerGen()),
                    ]),
                ]),
            ]),
        ]),
    ])


@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@ignore_order(local=True)
def test_deep_pruning_3_level_leaf(local_tmp_path, from_protobuf_fn):
    """Access decoded.level2.level3.val3 -- triggers 3-level deep pruning."""
    desc_path, desc_bytes = _setup_protobuf_desc(
        local_tmp_path, "dp3.desc", _build_deep_nested_5_level_descriptor_set_bytes)
    message_name = "test.Level1"
    data_gen = _deep_5_level_data_gen()

    def run_on_spark(spark):
        df = gen_df(spark, data_gen)
        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), message_name, desc_path, desc_bytes)
        return df.select(
            decoded.getField("val1").alias("val1"),
            decoded.getField("level2").getField("level3").getField("val3").alias("deep_val3"),
        )

    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@ignore_order(local=True)
def test_deep_pruning_5_level_leaf(local_tmp_path, from_protobuf_fn):
    """Access decoded.level2.level3.level4.level5.val5 -- deepest leaf."""
    desc_path, desc_bytes = _setup_protobuf_desc(
        local_tmp_path, "dp5.desc", _build_deep_nested_5_level_descriptor_set_bytes)
    message_name = "test.Level1"
    data_gen = _deep_5_level_data_gen()

    def run_on_spark(spark):
        df = gen_df(spark, data_gen)
        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), message_name, desc_path, desc_bytes)
        return df.select(
            _get_field_by_path(decoded, ["level2", "level3", "level4", "level5", "val5"])
                .alias("val5"),
        )

    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@ignore_order(local=True)
def test_deep_pruning_mixed_depths(local_tmp_path, from_protobuf_fn):
    """Access leaves at different depths in the same query.

    Select val1 (depth 1), val2 (depth 2), val3 (depth 3), and val5 (depth 5)
    to exercise pruning at every intermediate level simultaneously.
    """
    desc_path, desc_bytes = _setup_protobuf_desc(
        local_tmp_path, "dp_mix.desc", _build_deep_nested_5_level_descriptor_set_bytes)
    message_name = "test.Level1"
    data_gen = _deep_5_level_data_gen()

    def run_on_spark(spark):
        df = gen_df(spark, data_gen)
        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), message_name, desc_path, desc_bytes)
        return df.select(
            decoded.getField("val1").alias("val1"),
            decoded.getField("level2").getField("val2").alias("val2"),
            _get_field_by_path(decoded, ["level2", "level3", "val3"]).alias("val3"),
            _get_field_by_path(decoded, ["level2", "level3", "level4", "level5", "val5"])
                .alias("val5"),
        )

    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@ignore_order(local=True)
def test_deep_pruning_whole_struct_at_depth_3(local_tmp_path, from_protobuf_fn):
    """Select the whole level3 struct -- no deep pruning inside level3."""
    desc_path, desc_bytes = _setup_protobuf_desc(
        local_tmp_path, "dp_whole3.desc", _build_deep_nested_5_level_descriptor_set_bytes)
    message_name = "test.Level1"
    data_gen = _deep_5_level_data_gen()

    def run_on_spark(spark):
        df = gen_df(spark, data_gen)
        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), message_name, desc_path, desc_bytes)
        return df.select(
            decoded.getField("level2").getField("level3").alias("level3"),
        )

    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@ignore_order(local=True)
def test_deep_pruning_whole_struct_and_leaf_at_depth_3(
        local_tmp_path, from_protobuf_fn):
    desc_path, desc_bytes = _setup_protobuf_desc(
        local_tmp_path, "dp_whole_and_leaf3.desc",
        _build_deep_nested_5_level_descriptor_set_bytes)
    data_gen = _deep_5_level_data_gen()

    def run_on_spark(spark):
        df = gen_df(spark, data_gen)
        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), "test.Level1", desc_path, desc_bytes)
        level3 = decoded.getField("level2").getField("level3")
        return df.select(
            level3.alias("whole_level3"),
            level3.getField("val3").alias("val3"))

    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@ignore_order(local=True)
def test_deep_pruning_whole_ancestor_and_deep_leaf(
        local_tmp_path, from_protobuf_fn):
    desc_path, desc_bytes = _setup_protobuf_desc(
        local_tmp_path, "dp_whole_ancestor.desc",
        _build_deep_nested_5_level_descriptor_set_bytes)
    data_gen = _deep_5_level_data_gen()

    def run_on_spark(spark):
        df = gen_df(spark, data_gen)
        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), "test.Level1", desc_path, desc_bytes)
        level2 = decoded.getField("level2")
        return df.select(
            level2.alias("whole_level2"),
            level2.getField("level3").getField("val3").alias("val3"))

    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


# ===========================================================================
# FAILFAST mode tests
# ===========================================================================

@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@pytest.mark.parametrize("options", [
    {"mode": "DROPMALFORMED"},
    {"mode": "DROPMALFORMED", "emit.default.values": "true"},
], ids=["dropmalformed", "dropmalformed-with-unsupported-option"])
@allow_non_gpu("ProtobufDataToCatalyst")
def test_from_protobuf_dropmalformed_mode_is_rejected(
        local_tmp_path, from_protobuf_fn, options):
    desc_path, desc_bytes = _setup_protobuf_desc(
        local_tmp_path, "dropmalformed.desc", _build_simple_descriptor_set_bytes)
    message_name = "test.Simple"
    row = _encode_tag(2, 0) + _encode_varint(42)

    def run_on_spark(spark):
        df = spark.createDataFrame([(row,)], schema="bin binary")
        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), message_name, desc_path, desc_bytes,
            options=options)
        return df.select(decoded.alias("decoded")).collect()

    assert_gpu_and_cpu_error(run_on_spark, {}, "DROPMALFORMED")


@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
def test_from_protobuf_failfast_malformed_data(local_tmp_path, from_protobuf_fn):
    """FAILFAST mode should throw on malformed protobuf data (both CPU and GPU)."""
    desc_path, desc_bytes = _setup_protobuf_desc(
        local_tmp_path, "failfast.desc", _build_simple_descriptor_set_bytes)
    message_name = "test.Simple"

    # Craft a valid row and a malformed row (truncated varint with continuation bit)
    valid_row = _encode_tag(1, 0) + _encode_varint(1) + \
                _encode_tag(2, 0) + _encode_varint(42)
    malformed_row = bytes([0x08, 0x80])  # field 1, varint, but only continuation byte -- no end

    def run_on_spark(spark):
        df = spark.createDataFrame(
            [(valid_row,), (malformed_row,)],
            schema="bin binary",
        )
        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), message_name, desc_path, desc_bytes,
            options={"mode": "FAILFAST"})
        # Must call .collect() so the exception surfaces inside with_*_session
        return df.select(decoded.getField("b").alias("b")).collect()

    assert_gpu_and_cpu_error(run_on_spark, {}, "Malformed")


@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@ignore_order(local=True)
def test_from_protobuf_permissive_malformed_returns_null(local_tmp_path, from_protobuf_fn):
    """PERMISSIVE mode should return null for malformed rows, not throw.

    Note: Spark's from_protobuf defaults to FAILFAST (unlike JSON/CSV which
    default to PERMISSIVE), so mode must be set explicitly.
    """
    desc_path, desc_bytes = _setup_protobuf_desc(
        local_tmp_path, "permissive.desc", _build_simple_descriptor_set_bytes)
    message_name = "test.Simple"

    valid_row = _encode_tag(2, 0) + _encode_varint(99)
    malformed_row = bytes([0x08, 0x80])  # truncated varint

    def run_on_spark(spark):
        df = spark.createDataFrame(
            [(valid_row,), (malformed_row,)],
            schema="bin binary",
        )
        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), message_name, desc_path, desc_bytes,
            options={"mode": "PERMISSIVE"})
        return df.select(
            decoded.getField("i32").alias("i32"),
        )

    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@ignore_order(local=True)
def test_from_protobuf_all_null_input(local_tmp_path, from_protobuf_fn):
    """All rows in the input binary column are null (not empty bytes, actual nulls).
    GPU should produce all-null struct rows matching CPU behavior."""
    desc_path, desc_bytes = _setup_protobuf_desc(
        local_tmp_path, "allnull.desc", _build_simple_descriptor_set_bytes)
    message_name = "test.Simple"

    def run_on_spark(spark):
        df = spark.createDataFrame(
            [(None,), (None,), (None,)],
            schema="bin binary",
        )
        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), message_name, desc_path, desc_bytes)
        return df.select(
            decoded.getField("i32").alias("i32"),
            decoded.getField("s").alias("s"),
        )

    assert_gpu_and_cpu_are_equal_collect(run_on_spark)
