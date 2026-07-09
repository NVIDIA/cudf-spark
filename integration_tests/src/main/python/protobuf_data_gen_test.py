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

import random
import unittest

from data_gen import IntegerGen, SetValuesGen, StringGen
from protobuf_data_gen import pb
from pyspark.sql.types import BooleanType, IntegerType, LongType, StringType
from spark_session import is_spark_protobuf_descriptor_runtime_available, with_cpu_session


_requires_spark_protobuf = unittest.skipIf(
    not is_spark_protobuf_descriptor_runtime_available(),
    "descriptor adapter requires Spark's protobuf descriptor runtime")


def _build_single_int_descriptor(spark):
    descriptor_protos = (
        spark.sparkContext._jvm.org.sparkproject.spark_protobuf.protobuf.DescriptorProtos)
    field = (descriptor_protos.FieldDescriptorProto.newBuilder()
             .setName("value")
             .setNumber(1)
             .setLabel(descriptor_protos.FieldDescriptorProto.Label.LABEL_OPTIONAL)
             .setType(descriptor_protos.FieldDescriptorProto.Type.TYPE_INT32)
             .build())
    message = (descriptor_protos.DescriptorProto.newBuilder()
               .setName("Simple")
               .addField(field)
               .build())
    file_descriptor = (descriptor_protos.FileDescriptorProto.newBuilder()
                       .setName("simple.proto")
                       .setPackage("test")
                       .setSyntax("proto2")
                       .addMessageType(message)
                       .build())
    descriptor_set = (descriptor_protos.FileDescriptorSet.newBuilder()
                      .addFile(file_descriptor)
                      .build())
    return bytes(descriptor_set.toByteArray())


def _build_nested_descriptor(spark):
    descriptor_protos = (
        spark.sparkContext._jvm.org.sparkproject.spark_protobuf.protobuf.DescriptorProtos)
    child = (descriptor_protos.DescriptorProto.newBuilder()
             .setName("Child")
             .addField(descriptor_protos.FieldDescriptorProto.newBuilder()
                       .setName("value")
                       .setNumber(1)
                       .setLabel(descriptor_protos.FieldDescriptorProto.Label.LABEL_OPTIONAL)
                       .setType(descriptor_protos.FieldDescriptorProto.Type.TYPE_INT32)
                       .build())
             .build())
    outer = (descriptor_protos.DescriptorProto.newBuilder()
             .setName("Outer")
             .addField(descriptor_protos.FieldDescriptorProto.newBuilder()
                       .setName("child")
                       .setNumber(1)
                       .setLabel(descriptor_protos.FieldDescriptorProto.Label.LABEL_OPTIONAL)
                       .setType(descriptor_protos.FieldDescriptorProto.Type.TYPE_MESSAGE)
                       .setTypeName(".test.Child")
                       .build())
             .build())
    file_descriptor = (descriptor_protos.FileDescriptorProto.newBuilder()
                       .setName("nested.proto")
                       .setPackage("test")
                       .setSyntax("proto2")
                       .addMessageType(child)
                       .addMessageType(outer)
                       .build())
    descriptor_set = (descriptor_protos.FileDescriptorSet.newBuilder()
                      .addFile(file_descriptor)
                      .build())
    return bytes(descriptor_set.toByteArray())


def _build_repeated_int_descriptor(spark, packed=False):
    descriptor_protos = (
        spark.sparkContext._jvm.org.sparkproject.spark_protobuf.protobuf.DescriptorProtos)
    field_builder = (descriptor_protos.FieldDescriptorProto.newBuilder()
                     .setName("values")
                     .setNumber(1)
                     .setLabel(descriptor_protos.FieldDescriptorProto.Label.LABEL_REPEATED)
                     .setType(descriptor_protos.FieldDescriptorProto.Type.TYPE_INT32))
    if packed:
        field_builder.setOptions(
            descriptor_protos.FieldOptions.newBuilder().setPacked(True).build())
    field = field_builder.build()
    message = (descriptor_protos.DescriptorProto.newBuilder()
               .setName("Repeated")
               .addField(field)
               .build())
    file_descriptor = (descriptor_protos.FileDescriptorProto.newBuilder()
                       .setName("repeated.proto")
                       .setPackage("test")
                       .setSyntax("proto2")
                       .addMessageType(message)
                       .build())
    descriptor_set = (descriptor_protos.FileDescriptorSet.newBuilder()
                      .addFile(file_descriptor)
                      .build())
    return bytes(descriptor_set.toByteArray())


def _build_enum_default_descriptor(spark):
    descriptor_protos = (
        spark.sparkContext._jvm.org.sparkproject.spark_protobuf.protobuf.DescriptorProtos)
    status = (descriptor_protos.EnumDescriptorProto.newBuilder()
              .setName("Status")
              .addValue(descriptor_protos.EnumValueDescriptorProto.newBuilder()
                        .setName("UNKNOWN")
                        .setNumber(0)
                        .build())
              .addValue(descriptor_protos.EnumValueDescriptorProto.newBuilder()
                        .setName("READY")
                        .setNumber(2)
                        .build())
              .build())
    message = (descriptor_protos.DescriptorProto.newBuilder()
               .setName("Record")
               .addField(descriptor_protos.FieldDescriptorProto.newBuilder()
                         .setName("status")
                         .setNumber(1)
                         .setLabel(descriptor_protos.FieldDescriptorProto.Label.LABEL_REQUIRED)
                         .setType(descriptor_protos.FieldDescriptorProto.Type.TYPE_ENUM)
                         .setTypeName(".test.Status")
                         .build())
               .addField(descriptor_protos.FieldDescriptorProto.newBuilder()
                         .setName("count")
                         .setNumber(2)
                         .setLabel(descriptor_protos.FieldDescriptorProto.Label.LABEL_REQUIRED)
                         .setType(descriptor_protos.FieldDescriptorProto.Type.TYPE_UINT64)
                         .build())
               .addField(descriptor_protos.FieldDescriptorProto.newBuilder()
                         .setName("label")
                         .setNumber(3)
                         .setLabel(descriptor_protos.FieldDescriptorProto.Label.LABEL_OPTIONAL)
                         .setType(descriptor_protos.FieldDescriptorProto.Type.TYPE_STRING)
                         .setDefaultValue("fallback")
                         .build())
               .build())
    file_descriptor = (descriptor_protos.FileDescriptorProto.newBuilder()
                       .setName("enum_default.proto")
                       .setPackage("test")
                       .setSyntax("proto2")
                       .addEnumType(status)
                       .addMessageType(message)
                       .build())
    descriptor_set = (descriptor_protos.FileDescriptorSet.newBuilder()
                      .addFile(file_descriptor)
                      .build())
    return bytes(descriptor_set.toByteArray())


def _build_oneof_descriptor(spark):
    descriptor_protos = (
        spark.sparkContext._jvm.org.sparkproject.spark_protobuf.protobuf.DescriptorProtos)
    message = (descriptor_protos.DescriptorProto.newBuilder()
               .setName("Choice")
               .addOneofDecl(descriptor_protos.OneofDescriptorProto.newBuilder()
                             .setName("value")
                             .build())
               .addField(descriptor_protos.FieldDescriptorProto.newBuilder()
                         .setName("int_value")
                         .setNumber(1)
                         .setLabel(descriptor_protos.FieldDescriptorProto.Label.LABEL_OPTIONAL)
                         .setType(descriptor_protos.FieldDescriptorProto.Type.TYPE_INT32)
                         .setOneofIndex(0)
                         .build())
               .addField(descriptor_protos.FieldDescriptorProto.newBuilder()
                         .setName("string_value")
                         .setNumber(2)
                         .setLabel(descriptor_protos.FieldDescriptorProto.Label.LABEL_OPTIONAL)
                         .setType(descriptor_protos.FieldDescriptorProto.Type.TYPE_STRING)
                         .setOneofIndex(0)
                         .build())
               .build())
    file_descriptor = (descriptor_protos.FileDescriptorProto.newBuilder()
                       .setName("oneof.proto")
                       .setPackage("test")
                       .setSyntax("proto2")
                       .addMessageType(message)
                       .build())
    descriptor_set = (descriptor_protos.FileDescriptorSet.newBuilder()
                      .addFile(file_descriptor)
                      .build())
    return bytes(descriptor_set.toByteArray())


def _build_map_descriptor(spark):
    descriptor_protos = (
        spark.sparkContext._jvm.org.sparkproject.spark_protobuf.protobuf.DescriptorProtos)
    entry = (descriptor_protos.DescriptorProto.newBuilder()
             .setName("ItemsEntry")
             .setOptions(descriptor_protos.MessageOptions.newBuilder()
                         .setMapEntry(True)
                         .build())
             .addField(descriptor_protos.FieldDescriptorProto.newBuilder()
                       .setName("key")
                       .setNumber(1)
                       .setLabel(descriptor_protos.FieldDescriptorProto.Label.LABEL_OPTIONAL)
                       .setType(descriptor_protos.FieldDescriptorProto.Type.TYPE_STRING)
                       .build())
             .addField(descriptor_protos.FieldDescriptorProto.newBuilder()
                       .setName("value")
                       .setNumber(2)
                       .setLabel(descriptor_protos.FieldDescriptorProto.Label.LABEL_OPTIONAL)
                       .setType(descriptor_protos.FieldDescriptorProto.Type.TYPE_INT32)
                       .build())
             .build())
    message = (descriptor_protos.DescriptorProto.newBuilder()
               .setName("WithMap")
               .addNestedType(entry)
               .addField(descriptor_protos.FieldDescriptorProto.newBuilder()
                         .setName("items")
                         .setNumber(1)
                         .setLabel(descriptor_protos.FieldDescriptorProto.Label.LABEL_REPEATED)
                         .setType(descriptor_protos.FieldDescriptorProto.Type.TYPE_MESSAGE)
                         .setTypeName(".test.WithMap.ItemsEntry")
                         .build())
               .build())
    file_descriptor = (descriptor_protos.FileDescriptorProto.newBuilder()
                       .setName("map.proto")
                       .setPackage("test")
                       .setSyntax("proto2")
                       .addMessageType(message)
                       .build())
    descriptor_set = (descriptor_protos.FileDescriptorSet.newBuilder()
                      .addFile(file_descriptor)
                      .build())
    return bytes(descriptor_set.toByteArray())


def _build_unsigned_default_descriptor(spark):
    descriptor_protos = (
        spark.sparkContext._jvm.org.sparkproject.spark_protobuf.protobuf.DescriptorProtos)
    message = (descriptor_protos.DescriptorProto.newBuilder()
               .setName("UnsignedDefaults")
               .addField(descriptor_protos.FieldDescriptorProto.newBuilder()
                         .setName("u32")
                         .setNumber(1)
                         .setLabel(descriptor_protos.FieldDescriptorProto.Label.LABEL_OPTIONAL)
                         .setType(descriptor_protos.FieldDescriptorProto.Type.TYPE_UINT32)
                         .setDefaultValue("4294967295")
                         .build())
               .addField(descriptor_protos.FieldDescriptorProto.newBuilder()
                         .setName("u64")
                         .setNumber(2)
                         .setLabel(descriptor_protos.FieldDescriptorProto.Label.LABEL_OPTIONAL)
                         .setType(descriptor_protos.FieldDescriptorProto.Type.TYPE_UINT64)
                         .setDefaultValue("18446744073709551615")
                         .build())
               .build())
    file_descriptor = (descriptor_protos.FileDescriptorProto.newBuilder()
                       .setName("unsigned_defaults.proto")
                       .setPackage("test")
                       .setSyntax("proto2")
                       .addMessageType(message)
                       .build())
    descriptor_set = (descriptor_protos.FileDescriptorSet.newBuilder()
                      .addFile(file_descriptor)
                      .build())
    return bytes(descriptor_set.toByteArray())


def _build_mixed_syntax_descriptor(spark, referenced):
    descriptor_protos = (
        spark.sparkContext._jvm.org.sparkproject.spark_protobuf.protobuf.DescriptorProtos)
    child = (descriptor_protos.DescriptorProto.newBuilder()
             .setName("Child")
             .addField(descriptor_protos.FieldDescriptorProto.newBuilder()
                       .setName("value")
                       .setNumber(1)
                       .setLabel(descriptor_protos.FieldDescriptorProto.Label.LABEL_OPTIONAL)
                       .setType(descriptor_protos.FieldDescriptorProto.Type.TYPE_INT32)
                       .build())
             .build())
    child_file = (descriptor_protos.FileDescriptorProto.newBuilder()
                  .setName("child.proto")
                  .setPackage("other")
                  .setSyntax("proto3")
                  .addMessageType(child)
                  .build())
    root_field = descriptor_protos.FieldDescriptorProto.newBuilder()
    if referenced:
        root_field = (root_field
                      .setName("child")
                      .setType(descriptor_protos.FieldDescriptorProto.Type.TYPE_MESSAGE)
                      .setTypeName(".other.Child"))
    else:
        root_field = (root_field
                      .setName("value")
                      .setType(descriptor_protos.FieldDescriptorProto.Type.TYPE_INT32))
    root = (descriptor_protos.DescriptorProto.newBuilder()
            .setName("Root")
            .addField(root_field
                      .setNumber(1)
                      .setLabel(descriptor_protos.FieldDescriptorProto.Label.LABEL_OPTIONAL)
                      .build())
            .build())
    root_file_builder = (descriptor_protos.FileDescriptorProto.newBuilder()
                         .setName("root.proto")
                         .setPackage("test")
                         .setSyntax("proto2")
                         .addMessageType(root))
    if referenced:
        root_file_builder.addDependency("child.proto")
    descriptor_set = (descriptor_protos.FileDescriptorSet.newBuilder()
                      .addFile(child_file)
                      .addFile(root_file_builder.build())
                      .build())
    return bytes(descriptor_set.toByteArray())


class ProtobufDataGenTest(unittest.TestCase):
    def test_message_encodes_scalar_value(self):
        schema = pb.message("Simple", [pb.int32("value", 1)])

        self.assertEqual(b"\x08\x96\x01", schema.encode({"value": 150}))

    def test_row_gen_emits_logical_value_and_matching_wire(self):
        schema = pb.message("Simple", [
            pb.required(pb.int32(
                "value", 1,
                gen=IntegerGen(
                    min_val=7, max_val=7, nullable=False, special_cases=[]))),
        ])
        row_gen = schema.as_datagen()
        row_gen.start(random.Random(0))

        self.assertEqual((7, b"\x08\x07"), row_gen.gen())

    def test_row_gen_materializes_default_without_encoding_field(self):
        schema = pb.message("WithDefault", [
            pb.int32(
                "count", 1,
                gen=SetValuesGen(IntegerType(), [None]),
                default=42),
        ])
        row_gen = schema.as_datagen()
        row_gen.start(random.Random(0))

        self.assertEqual((42, b""), row_gen.gen())

    def test_repeated_int_encoding_honors_packed_option(self):
        unpacked = pb.message("Unpacked", [
            pb.repeated(pb.int32("values", 1), min_len=0, max_len=2),
        ])
        packed = pb.message("Packed", [
            pb.repeated(
                pb.int32("values", 1), min_len=0, max_len=2, packed=True),
        ])

        self.assertEqual(
            b"\x08\x01\x08\x96\x01", unpacked.encode({"values": [1, 150]}))
        self.assertEqual(
            b"\x0a\x03\x01\x96\x01", packed.encode({"values": [1, 150]}))

    def test_equivalent_row_gens_have_equal_cache_identity(self):
        def make_row_gen():
            return pb.message("Simple", [
                pb.int32(
                    "value", 1,
                    gen=IntegerGen(
                        min_val=1, max_val=9, nullable=False, special_cases=[])),
            ]).as_datagen()

        left = make_row_gen()
        right = make_row_gen()

        self.assertEqual(left, right)
        self.assertEqual(hash(left), hash(right))

    def test_binary_column_name_must_not_collide_with_root_field(self):
        schema = pb.message("Collision", [pb.int32("BIN", 1)])

        with self.assertRaisesRegex(ValueError, "binary column"):
            schema.as_datagen()

        self.assertEqual(
            ["BIN", "wire"],
            schema.as_datagen(binary_col_name="wire").data_type.fieldNames())

    @_requires_spark_protobuf
    def test_generation_policy_is_part_of_row_gen_cache_identity(self):
        nested_descriptor = with_cpu_session(_build_nested_descriptor)
        repeated_descriptor = with_cpu_session(_build_repeated_int_descriptor)

        present = with_cpu_session(
            lambda spark: pb.from_descriptor(
                spark,
                nested_descriptor,
                "test.Outer",
                generation=pb.generation(message_presence={"child": True}))
            .as_datagen())
        absent = with_cpu_session(
            lambda spark: pb.from_descriptor(
                spark,
                nested_descriptor,
                "test.Outer",
                generation=pb.generation(message_presence={"child": False}))
            .as_datagen())
        one_element = with_cpu_session(
            lambda spark: pb.from_descriptor(
                spark,
                repeated_descriptor,
                "test.Repeated",
                generation=pb.generation(repeated_lengths={"values": 1}))
            .as_datagen())
        two_elements = with_cpu_session(
            lambda spark: pb.from_descriptor(
                spark,
                repeated_descriptor,
                "test.Repeated",
                generation=pb.generation(repeated_lengths={"values": 2}))
            .as_datagen())

        self.assertNotEqual(present, absent)
        self.assertNotEqual(one_element, two_elements)

    def test_legacy_nested_generation_keeps_child_derived_presence(self):
        schema = pb.message("Outer", [
            pb.nested("child", 1, [
                pb.int32(
                    "value", 1,
                    gen=SetValuesGen(IntegerType(), [None])),
            ]),
        ])
        row_gen = schema.as_datagen()
        row_gen.start(random.Random(0))

        self.assertEqual((None, b""), row_gen.gen())

    def test_nested_message_rejects_duplicate_field_numbers(self):
        with self.assertRaisesRegex(ValueError, "duplicate field numbers"):
            pb.message("Outer", [
                pb.nested("child", 1, [
                    pb.int32("a", 1),
                    pb.string("b", 1),
                ]),
            ])

    def test_field_number_must_fit_protobuf_tag(self):
        with self.assertRaisesRegex(ValueError, "field number"):
            pb.int32("value", 1 << 29)

    def test_reserved_field_numbers_are_rejected(self):
        for number in (19000, 19999):
            with self.subTest(number=number):
                with self.assertRaisesRegex(ValueError, "field number"):
                    pb.int32("value", number)

    def test_default_repeated_length_is_validated_when_policy_is_created(self):
        with self.assertRaisesRegex(ValueError, "default repeated length"):
            pb.generation(default_repeated_length=(-1, 3))

    @_requires_spark_protobuf
    def test_descriptor_is_the_source_for_valid_encoding(self):
        descriptor_bytes = with_cpu_session(_build_single_int_descriptor)
        schema = with_cpu_session(
            lambda spark: pb.from_descriptor(
                spark, descriptor_bytes, "test.Simple"))

        self.assertEqual(b"\x08\x96\x01", schema.encode({"value": 150}))

    @_requires_spark_protobuf
    def test_descriptor_resolves_nested_message_types(self):
        descriptor_bytes = with_cpu_session(_build_nested_descriptor)
        schema = with_cpu_session(
            lambda spark: pb.from_descriptor(
                spark, descriptor_bytes, "test.Outer"))

        self.assertEqual(
            b"\x0a\x02\x08\x07", schema.encode({"child": {"value": 7}}))

    @_requires_spark_protobuf
    def test_descriptor_generation_rejects_oneof_until_it_has_a_policy(self):
        descriptor_bytes = with_cpu_session(_build_oneof_descriptor)

        with self.assertRaisesRegex(ValueError, "oneof"):
            with_cpu_session(
                lambda spark: pb.from_descriptor(
                    spark, descriptor_bytes, "test.Choice"))

    @_requires_spark_protobuf
    def test_descriptor_generation_rejects_map_until_it_has_a_policy(self):
        descriptor_bytes = with_cpu_session(_build_map_descriptor)

        with self.assertRaisesRegex(ValueError, "map"):
            with_cpu_session(
                lambda spark: pb.from_descriptor(
                    spark, descriptor_bytes, "test.WithMap"))

    @_requires_spark_protobuf
    def test_descriptor_generation_overlay_supplies_values(self):
        descriptor_bytes = with_cpu_session(_build_single_int_descriptor)
        generation = pb.generation(value_gens={
            "value": IntegerGen(
                min_val=7, max_val=7, nullable=False, special_cases=[]),
        })
        schema = with_cpu_session(
            lambda spark: pb.from_descriptor(
                spark, descriptor_bytes, "test.Simple", generation=generation))
        row_gen = schema.as_datagen()
        row_gen.start(random.Random(0))

        self.assertEqual((7, b"\x08\x07"), row_gen.gen())

    @_requires_spark_protobuf
    def test_descriptor_rejects_value_generator_with_wrong_type(self):
        descriptor_bytes = with_cpu_session(_build_single_int_descriptor)
        generation = pb.generation(value_gens={
            "value": StringGen(nullable=False),
        })

        with self.assertRaisesRegex(TypeError, "value generator.*value.*IntegerType"):
            with_cpu_session(
                lambda spark: pb.from_descriptor(
                    spark, descriptor_bytes, "test.Simple", generation=generation))

    @_requires_spark_protobuf
    def test_descriptor_rejects_nullable_repeated_value_generator(self):
        descriptor_bytes = with_cpu_session(_build_repeated_int_descriptor)
        generation = pb.generation(value_gens={
            "values": SetValuesGen(IntegerType(), [None]),
        })

        with self.assertRaisesRegex(ValueError, "values.*non-nullable"):
            with_cpu_session(
                lambda spark: pb.from_descriptor(
                    spark, descriptor_bytes, "test.Repeated", generation=generation))

    @_requires_spark_protobuf
    def test_descriptor_generation_controls_optional_message_presence(self):
        descriptor_bytes = with_cpu_session(_build_nested_descriptor)
        generation = pb.generation(
            value_gens={
                "child.value": SetValuesGen(IntegerType(), [None]),
            },
            message_presence={
                "child": SetValuesGen(BooleanType(), [True]),
            })
        schema = with_cpu_session(
            lambda spark: pb.from_descriptor(
                spark, descriptor_bytes, "test.Outer", generation=generation))
        row_gen = schema.as_datagen()
        row_gen.start(random.Random(0))

        self.assertEqual(((None,), b"\x0a\x00"), row_gen.gen())

    @_requires_spark_protobuf
    def test_descriptor_presence_can_suppress_populated_child(self):
        descriptor_bytes = with_cpu_session(_build_nested_descriptor)
        generation = pb.generation(
            value_gens={
                "child.value": SetValuesGen(IntegerType(), [7]),
            },
            message_presence={
                "child": False,
            })
        schema = with_cpu_session(
            lambda spark: pb.from_descriptor(
                spark, descriptor_bytes, "test.Outer", generation=generation))
        row_gen = schema.as_datagen()
        row_gen.start(random.Random(0))

        self.assertEqual((None, b""), row_gen.gen())

    @_requires_spark_protobuf
    def test_descriptor_generation_controls_repeated_length(self):
        descriptor_bytes = with_cpu_session(_build_repeated_int_descriptor)
        generation = pb.generation(
            value_gens={
                "values": SetValuesGen(IntegerType(), [7]),
            },
            repeated_lengths={
                "values": (2, 2),
            })
        schema = with_cpu_session(
            lambda spark: pb.from_descriptor(
                spark, descriptor_bytes, "test.Repeated", generation=generation))
        row_gen = schema.as_datagen()
        row_gen.start(random.Random(0))

        self.assertEqual(([7, 7], b"\x08\x07\x08\x07"), row_gen.gen())

    @_requires_spark_protobuf
    def test_descriptor_preserves_packed_repeated_option(self):
        descriptor_bytes = with_cpu_session(
            lambda spark: _build_repeated_int_descriptor(spark, packed=True))
        schema = with_cpu_session(
            lambda spark: pb.from_descriptor(
                spark, descriptor_bytes, "test.Repeated"))

        self.assertEqual(
            b"\x0a\x03\x01\x96\x01",
            schema.encode({"values": [1, 150]}))

    @_requires_spark_protobuf
    def test_descriptor_preserves_enum_required_and_default_semantics(self):
        descriptor_bytes = with_cpu_session(_build_enum_default_descriptor)
        generation = pb.generation(value_gens={
            "status": SetValuesGen(IntegerType(), [2]),
            "count": SetValuesGen(LongType(), [5]),
            "label": SetValuesGen(StringType(), [None]),
        }, enums_as_ints=True)
        schema = with_cpu_session(
            lambda spark: pb.from_descriptor(
                spark, descriptor_bytes, "test.Record", generation=generation))
        row_gen = schema.as_datagen()
        row_gen.start(random.Random(0))

        self.assertEqual(
            (2, 5, "fallback", b"\x08\x02\x10\x05"),
            row_gen.gen())

    @_requires_spark_protobuf
    def test_descriptor_supplies_standard_generation_for_unconfigured_fields(self):
        descriptor_bytes = with_cpu_session(_build_enum_default_descriptor)
        schema = with_cpu_session(
            lambda spark: pb.from_descriptor(
                spark, descriptor_bytes, "test.Record"))
        row_gen = schema.as_datagen()
        row_gen.start(random.Random(0))

        status, count, label, encoded = row_gen.gen()
        self.assertIn(status, ("UNKNOWN", "READY"))
        self.assertIsInstance(count, int)
        self.assertIsInstance(label, str)
        self.assertTrue(encoded.startswith(b"\x08"))

    @_requires_spark_protobuf
    def test_descriptor_row_schema_matches_spark_nullability(self):
        enum_descriptor = with_cpu_session(_build_enum_default_descriptor)
        repeated_descriptor = with_cpu_session(_build_repeated_int_descriptor)
        enum_schema = with_cpu_session(
            lambda spark: pb.from_descriptor(
                spark, enum_descriptor, "test.Record").as_datagen().data_type)
        repeated_schema = with_cpu_session(
            lambda spark: pb.from_descriptor(
                spark, repeated_descriptor, "test.Repeated").as_datagen().data_type)

        self.assertFalse(enum_schema["status"].nullable)
        self.assertEqual(StringType(), enum_schema["status"].dataType)
        self.assertFalse(enum_schema["count"].nullable)
        self.assertTrue(enum_schema["label"].nullable)
        self.assertTrue(repeated_schema["values"].nullable)
        self.assertFalse(repeated_schema["values"].dataType.containsNull)

    @_requires_spark_protobuf
    def test_descriptor_normalizes_unsigned_high_bits_for_spark_rows(self):
        descriptor_bytes = with_cpu_session(_build_unsigned_default_descriptor)
        generation = pb.generation(value_gens={
            "u32": SetValuesGen(IntegerType(), [None]),
            "u64": SetValuesGen(LongType(), [None]),
        })
        schema = with_cpu_session(
            lambda spark: pb.from_descriptor(
                spark, descriptor_bytes, "test.UnsignedDefaults",
                generation=generation))
        row_gen = schema.as_datagen()
        row_gen.start(random.Random(0))

        expected_wire = (
            b"\x08\xff\xff\xff\xff\x0f\x10" + b"\xff" * 9 + b"\x01")
        self.assertEqual(expected_wire, schema.encode({"u32": -1, "u64": -1}))
        self.assertEqual((-1, -1, b""), row_gen.gen())

    @_requires_spark_protobuf
    def test_descriptor_validates_syntax_only_for_reachable_types(self):
        unrelated = with_cpu_session(
            lambda spark: _build_mixed_syntax_descriptor(spark, referenced=False))
        referenced = with_cpu_session(
            lambda spark: _build_mixed_syntax_descriptor(spark, referenced=True))

        schema = with_cpu_session(
            lambda spark: pb.from_descriptor(
                spark, unrelated, "test.Root"))
        self.assertEqual(b"\x08\x07", schema.encode({"value": 7}))
        with self.assertRaisesRegex(ValueError, "proto3"):
            with_cpu_session(
                lambda spark: pb.from_descriptor(
                    spark, referenced, "test.Root"))
