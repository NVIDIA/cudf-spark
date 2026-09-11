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

from data_gen import IntegerGen, StringGen, StructGen
from protobuf_data_gen import call_protobuf_function, materialize_protobuf_data
from spark_session import is_spark_protobuf_available


def test_call_protobuf_function_legacy_signature():
    calls = []

    def legacy_fn(col, message_name, desc_path, *args):
        calls.append((col, message_name, desc_path, args))
        return "result"

    options = {"enums.as.ints": "true"}
    result = call_protobuf_function(
        legacy_fn, "col", "test.Message", "/tmp/test.desc", b"descriptor",
        options=options)

    assert result == "result"
    assert calls == [
        ("col", "test.Message", "/tmp/test.desc", (options,))
    ]


def test_call_protobuf_function_binary_signature():
    calls = []

    def binary_fn(col, message_name, binaryDescriptorSet=None, options=None):
        calls.append((col, message_name, binaryDescriptorSet, options))
        return "result"

    options = {"mode": "PERMISSIVE"}
    result = call_protobuf_function(
        binary_fn, "col", "test.Message", "/tmp/test.desc", b"descriptor",
        options=options)

    assert result == "result"
    assert calls == [
        ("col", "test.Message", bytearray(b"descriptor"), options)
    ]


def test_materialize_protobuf_data_requires_struct_gen():
    with pytest.raises(TypeError, match="logical_gen must be a StructGen"):
        materialize_protobuf_data(
            IntegerGen(), "test.Message", "/tmp/test.desc", b"descriptor")


def test_materialize_protobuf_data_requires_non_nullable_root():
    logical_gen = StructGen([("i32", IntegerGen())], nullable=True)

    with pytest.raises(ValueError, match="must be non-nullable"):
        materialize_protobuf_data(
            logical_gen, "test.Message", "/tmp/test.desc", b"descriptor")


def test_materialize_protobuf_data_reserves_binary_column_name():
    logical_gen = StructGen([("BIN", IntegerGen())], nullable=False)

    with pytest.raises(ValueError, match="conflicts with binary column"):
        materialize_protobuf_data(
            logical_gen, "test.Message", "/tmp/test.desc", b"descriptor")


def test_materialize_protobuf_data_rejects_two_row_sources():
    logical_gen = StructGen([("i32", IntegerGen())], nullable=False)

    with pytest.raises(ValueError, match="length cannot be used"):
        materialize_protobuf_data(
            logical_gen, "test.Message", "/tmp/test.desc", b"descriptor",
            length=1, logical_rows=[(1,)])


# Avoid depending on whichever unshaded protobuf runtime the Spark driver provides.
_simple_desc_bytes = bytes.fromhex(
    "0a360a0c73696d706c652e70726f746f12047465737422200a0653696d706c65"
    "120b0a0369333218012001280512090a0173180220012809")


@pytest.mark.skipif(
    not is_spark_protobuf_available(), reason="from_protobuf is unavailable")
def test_materialize_protobuf_data_with_explicit_rows(local_tmp_path):
    desc_path = local_tmp_path + "/simple.desc"
    with open(desc_path, "wb") as fp:
        fp.write(_simple_desc_bytes)

    logical_gen = StructGen([
        ("i32", IntegerGen(nullable=False)),
        ("s", StringGen(nullable=False)),
    ], nullable=False)
    logical_rows = [(1, "a"), (12345, "hello")]

    rows, schema = materialize_protobuf_data(
        logical_gen, "test.Simple", desc_path, _simple_desc_bytes,
        logical_rows=logical_rows)

    assert [tuple(row[:2]) for row in rows] == logical_rows
    assert all(isinstance(row[2], bytes) and row[2] for row in rows)
    assert schema.fieldNames() == ["i32", "s", "bin"]
