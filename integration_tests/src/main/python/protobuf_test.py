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

import pytest

from asserts import assert_gpu_fallback_collect
from marks import allow_non_gpu
from protobuf_data_gen import call_protobuf_function
from spark_session import is_before_spark_340, is_spark_protobuf_available, with_cpu_session
import pyspark.sql.functions as f

pytestmark = pytest.mark.skipif(
    not is_spark_protobuf_available(), reason="from_protobuf is unavailable")


@pytest.fixture(scope="module")
def from_protobuf_fn():
    from pyspark.sql.protobuf.functions import from_protobuf
    return from_protobuf


def _encode_varint(value):
    out = bytearray()
    value &= 0xFFFFFFFFFFFFFFFF
    while True:
        bits = value & 0x7F
        value >>= 7
        if value:
            out.append(bits | 0x80)
        else:
            out.append(bits)
            return bytes(out)


def _encode_simple_message(i32_value, s_value):
    buf = bytearray()
    buf += _encode_varint((1 << 3) | 0)  # field 1, VARINT
    buf += _encode_varint(i32_value)
    s_bytes = s_value.encode("utf-8")
    buf += _encode_varint((2 << 3) | 2)  # field 2, LENGTH-DELIMITED
    buf += _encode_varint(len(s_bytes))
    buf += s_bytes
    return bytes(buf)


# Avoid depending on whichever unshaded protobuf runtime the Spark driver provides.
_simple_desc_bytes = bytes.fromhex(
    "0a360a0c73696d706c652e70726f746f12047465737422200a0653696d706c65"
    "120b0a0369333218012001280512090a0173180220012809")


@pytest.fixture
def simple_desc(local_tmp_path):
    # from_protobuf reads descFilePath via java.io.File on the driver.
    desc_path = local_tmp_path + "/simple.desc"
    with open(desc_path, "wb") as fp:
        fp.write(_simple_desc_bytes)
    return desc_path, _simple_desc_bytes


@pytest.mark.skipif(is_before_spark_340(), reason="descriptor compatibility shims start at Spark 3.4")
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


_smoke_rows = [(1, "a"), (-2, "bb"), (0, ""), (12345, "hello")]


def _make_smoke_df(spark):
    encoded = [(_encode_simple_message(i, s),) for (i, s) in _smoke_rows]
    return spark.createDataFrame(encoded, ["bin"])


@allow_non_gpu("ProjectExec", "ProtobufDataToCatalyst")
def test_from_protobuf_smoke_path_api(simple_desc, from_protobuf_fn):
    desc_path, _ = simple_desc

    def run(spark):
        return _make_smoke_df(spark).select(
            from_protobuf_fn(f.col("bin"), "test.Simple", desc_path).alias("d"))

    assert_gpu_fallback_collect(run, "ProtobufDataToCatalyst")


@allow_non_gpu("ProjectExec", "ProtobufDataToCatalyst")
def test_from_protobuf_smoke_binary_descriptor_api(simple_desc, from_protobuf_fn):
    if "binaryDescriptorSet" not in inspect.signature(from_protobuf_fn).parameters:
        pytest.skip("binaryDescriptorSet kwarg is Spark 3.5+ only")
    _, desc_bytes = simple_desc

    def run(spark):
        return _make_smoke_df(spark).select(
            from_protobuf_fn(f.col("bin"), "test.Simple",
                             binaryDescriptorSet=bytearray(desc_bytes)).alias("d"))

    assert_gpu_fallback_collect(run, "ProtobufDataToCatalyst")
