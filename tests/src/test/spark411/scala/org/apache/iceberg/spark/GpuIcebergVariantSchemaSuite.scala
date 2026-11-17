/*
 * Copyright (c) 2026, NVIDIA CORPORATION.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*** spark-rapids-shim-json-lines
{"spark":["411","412","413"],"carryForward":"none"}
spark-rapids-shim-json-lines ***/
package org.apache.iceberg.spark

import com.nvidia.spark.rapids.SchemaUtils._
import com.nvidia.spark.rapids.iceberg.ShimUtils
import com.nvidia.spark.rapids.iceberg.parquet.converter.ParquetUnshader
import org.apache.iceberg.Schema
import org.apache.iceberg.parquet.ParquetSchemaUtil
import org.apache.iceberg.shaded.org.apache.parquet.schema.{LogicalTypeAnnotation => ShadedLogicalTypeAnnotation, Types => ShadedTypes}
import org.apache.iceberg.shaded.org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY
import org.apache.iceberg.types.{Type, Types}
import org.apache.parquet.schema.{LogicalTypeAnnotation, Types => ParquetTypes}
import org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit.MICROS
import org.apache.parquet.schema.PrimitiveType.{PrimitiveTypeName => ParquetPrimitiveTypeName}
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.sql.execution.datasources.parquet.ParquetToSparkSchemaConverter
import org.apache.spark.sql.execution.datasources.parquet.ParquetUtils.FIELD_ID_METADATA_KEY
import org.apache.spark.sql.types.{ArrayType, IntegerType, MapType, Metadata, MetadataBuilder,
    StringType, StructField, StructType, VariantType}

class GpuIcebergVariantSchemaSuite extends AnyFunSuite {

  private def variantSchema: Schema = {
    val payload = Types.StructType.of(
      Types.NestedField.optional(3, "items",
        Types.ListType.ofOptional(4, Types.VariantType.get())),
      Types.NestedField.optional(5, "attributes",
        Types.MapType.ofOptional(6, 7, Types.StringType.get(), Types.VariantType.get())))
    new Schema(
      Types.NestedField.optional(1, "variant_value", Types.VariantType.get()),
      Types.NestedField.optional(2, "payload", payload))
  }

  test("toSparkType preserves field IDs around top-level and nested Variant fields") {
    val sparkSchema = GpuTypeToSparkType.toSparkType(variantSchema)
    val variantField = sparkSchema("variant_value")
    assert(variantField.dataType == VariantType)
    assert(variantField.metadata.getLong(FIELD_ID_METADATA_KEY) == 1L)

    val payloadField = sparkSchema("payload")
    assert(payloadField.metadata.getLong(FIELD_ID_METADATA_KEY) == 2L)
    val payloadType = payloadField.dataType.asInstanceOf[StructType]

    val items = payloadType("items")
    assert(items.metadata.getLong(FIELD_ID_METADATA_KEY) == 3L)
    assert(items.metadata.getLong(LIST_ELEMENT_FIELD_ID_METADATA_KEY) == 4L)
    assert(items.dataType == ArrayType(VariantType, containsNull = true))

    val attributes = payloadType("attributes")
    assert(attributes.metadata.getLong(FIELD_ID_METADATA_KEY) == 5L)
    assert(attributes.metadata.getLong(MAP_KEY_FIELD_ID_METADATA_KEY) == 6L)
    assert(attributes.metadata.getLong(MAP_VALUE_FIELD_ID_METADATA_KEY) == 7L)
    assert(attributes.dataType ==
      MapType(StringType, VariantType, valueContainsNull = true))
  }

  // Pairwise coverage of field, collection-child, and Variant nullability.
  for ((fieldOptional, childOptional, variantOptional) <- Seq(
      (false, false, false),
      (false, true, true),
      (true, false, true),
      (true, true, false))) {
    test("nested Variant metadata and nullability: " +
        s"field=$fieldOptional child=$childOptional variant=$variantOptional") {
      def listType(id: Int, elementType: Type, optional: Boolean): Types.ListType = {
        if (optional) {
          Types.ListType.ofOptional(id, elementType)
        } else {
          Types.ListType.ofRequired(id, elementType)
        }
      }

      def field(id: Int, optional: Boolean, name: String, fieldType: Type): Types.NestedField = {
        if (optional) {
          Types.NestedField.optional(id, name, fieldType)
        } else {
          Types.NestedField.required(id, name, fieldType)
        }
      }

      val nestedLists = listType(11,
        listType(12, Types.VariantType.get(), variantOptional), childOptional)
      val mapValue = listType(23, Types.VariantType.get(), variantOptional)
      val mappedLists = if (childOptional) {
        Types.MapType.ofOptional(21, 22, Types.StringType.get(), mapValue)
      } else {
        Types.MapType.ofRequired(21, 22, Types.StringType.get(), mapValue)
      }
      val record = Types.StructType.of(
        field(32, variantOptional, "v", Types.VariantType.get()),
        field(33, !variantOptional, "count", Types.IntegerType.get()))
      val mapKey = listType(42, Types.StringType.get(), childOptional)
      val keyedVariants = if (variantOptional) {
        Types.MapType.ofOptional(41, 43, mapKey, Types.VariantType.get())
      } else {
        Types.MapType.ofRequired(41, 43, mapKey, Types.VariantType.get())
      }
      val schema = new Schema(
        Types.NestedField.required(1, "id", Types.IntegerType.get()),
        field(10, fieldOptional, "nested_lists", nestedLists),
        field(2, !fieldOptional, "marker", Types.VariantType.get()),
        field(20, !fieldOptional, "mapped_lists", mappedLists),
        field(30, fieldOptional, "records", listType(31, record, childOptional)),
        field(40, !fieldOptional, "keyed_variants", keyedVariants),
        Types.NestedField.optional(3, "tail", Types.StringType.get()))

      def fieldMetadata(id: Long): Metadata =
        new MetadataBuilder().putLong(FIELD_ID_METADATA_KEY, id).build()

      val nestedListsMetadata = new MetadataBuilder()
        .putLong(FIELD_ID_METADATA_KEY, 10L)
        .putLong(LIST_ELEMENT_FIELD_ID_METADATA_KEY, 11L)
        .putString(LIST_ELEMENT_NESTED_IDS_METADATA_KEY,
          new MetadataBuilder().putLong(LIST_ELEMENT_FIELD_ID_METADATA_KEY, 12L).build().json)
        .build()
      val mappedListsMetadata = new MetadataBuilder()
        .putLong(FIELD_ID_METADATA_KEY, 20L)
        .putLong(MAP_KEY_FIELD_ID_METADATA_KEY, 21L)
        .putLong(MAP_VALUE_FIELD_ID_METADATA_KEY, 22L)
        .putString(MAP_VALUE_NESTED_IDS_METADATA_KEY,
          new MetadataBuilder().putLong(LIST_ELEMENT_FIELD_ID_METADATA_KEY, 23L).build().json)
        .build()
      val recordsMetadata = new MetadataBuilder()
        .putLong(FIELD_ID_METADATA_KEY, 30L)
        .putLong(LIST_ELEMENT_FIELD_ID_METADATA_KEY, 31L)
        .build()
      val keyedVariantsMetadata = new MetadataBuilder()
        .putLong(FIELD_ID_METADATA_KEY, 40L)
        .putLong(MAP_KEY_FIELD_ID_METADATA_KEY, 41L)
        .putLong(MAP_VALUE_FIELD_ID_METADATA_KEY, 43L)
        .putString(MAP_KEY_NESTED_IDS_METADATA_KEY,
          new MetadataBuilder().putLong(LIST_ELEMENT_FIELD_ID_METADATA_KEY, 42L).build().json)
        .build()
      val expectedRecord = StructType(Seq(
        StructField("v", VariantType, variantOptional, fieldMetadata(32L)),
        StructField("count", IntegerType, !variantOptional, fieldMetadata(33L))))
      val expected = StructType(Seq(
        StructField("id", IntegerType, nullable = false, metadata = fieldMetadata(1L)),
        StructField("nested_lists",
          ArrayType(ArrayType(VariantType, variantOptional), childOptional),
          fieldOptional, nestedListsMetadata),
        StructField("marker", VariantType, !fieldOptional, fieldMetadata(2L)),
        StructField("mapped_lists",
          MapType(StringType, ArrayType(VariantType, variantOptional), childOptional),
          !fieldOptional, mappedListsMetadata),
        StructField("records", ArrayType(expectedRecord, childOptional),
          fieldOptional, recordsMetadata),
        StructField("keyed_variants",
          MapType(ArrayType(StringType, childOptional), VariantType, variantOptional),
          !fieldOptional, keyedVariantsMetadata),
        StructField("tail", StringType, nullable = true, metadata = fieldMetadata(3L))))

      assert(GpuTypeToSparkType.toSparkType(schema) == expected)
    }
  }

  // Check annotation parameter forwarding, not support for decoding another Variant format.
  for (specVersion <- Seq(1.toByte, 2.toByte)) {
    test(s"unshade preserves the Variant logical type spec byte $specVersion") {
      val shaded = ShadedLogicalTypeAnnotation.variantType(specVersion)
      val unshaded = ShimUtils.getParquetUnshader().unshade(shaded)
        .asInstanceOf[LogicalTypeAnnotation.VariantLogicalTypeAnnotation]

      assert(unshaded.getSpecVersion == specVersion)
    }
  }

  test("the selected converter handles Variant annotations throughout a Parquet schema") {
    val shadedSchema = ParquetSchemaUtil.convert(variantSchema, "variant_schema")
    val unshadedSchema = ShimUtils.getParquetUnshader().unshade(shadedSchema)
    val expected = ParquetTypes.buildMessage()
      .optionalGroup().as(LogicalTypeAnnotation.variantType(1.toByte)).id(1)
      .required(ParquetPrimitiveTypeName.BINARY).named("metadata")
      .required(ParquetPrimitiveTypeName.BINARY).named("value")
      .named("variant_value")
      .optionalGroup().id(2)
      .optionalGroup().as(LogicalTypeAnnotation.listType()).id(3)
      .repeatedGroup()
      .optionalGroup().as(LogicalTypeAnnotation.variantType(1.toByte)).id(4)
      .required(ParquetPrimitiveTypeName.BINARY).named("metadata")
      .required(ParquetPrimitiveTypeName.BINARY).named("value")
      .named("element")
      .named("list")
      .named("items")
      .optionalGroup().as(LogicalTypeAnnotation.mapType()).id(5)
      .repeatedGroup()
      .required(ParquetPrimitiveTypeName.BINARY)
      .as(LogicalTypeAnnotation.stringType()).id(6).named("key")
      .optionalGroup().as(LogicalTypeAnnotation.variantType(1.toByte)).id(7)
      .required(ParquetPrimitiveTypeName.BINARY).named("metadata")
      .required(ParquetPrimitiveTypeName.BINARY).named("value")
      .named("value")
      .named("key_value")
      .named("attributes")
      .named("payload")
      .named("variant_schema")

    assert(unshadedSchema == expected)
    val sparkSchema = new ParquetToSparkSchemaConverter(
      false, true, true, true, false).convert(unshadedSchema)

    assert(sparkSchema("variant_value").dataType == VariantType)
    assert(unshadedSchema.asGroupType().getType("variant_value").getId.intValue() == 1)
    val payload = sparkSchema("payload").dataType.asInstanceOf[StructType]
    assert(payload("items").dataType == ArrayType(VariantType, containsNull = true))
    assert(payload("attributes").dataType ==
      MapType(StringType, VariantType, valueContainsNull = true))
  }

  test("recursive conversion preserves nested Variant annotations, IDs, and repetition") {
    val shaded = ShadedTypes.buildMessage()
      .requiredGroup().id(1)
      .optionalGroup().as(ShadedLogicalTypeAnnotation.variantType(1.toByte)).id(2)
      .required(BINARY).id(3).named("metadata")
      .required(BINARY).id(4).named("value")
      .named("variant_value")
      .named("payload")
      .named("variant_schema")
    val expected = org.apache.parquet.schema.Types.buildMessage()
      .requiredGroup().id(1)
      .optionalGroup().as(LogicalTypeAnnotation.variantType(1.toByte)).id(2)
      .required(org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY)
      .id(3).named("metadata")
      .required(org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY)
      .id(4).named("value")
      .named("variant_value")
      .named("payload")
      .named("variant_schema")

    assert(ShimUtils.getParquetUnshader().unshade(shaded) == expected)
    assert(ShimUtils.unshadeParquetSchema(shaded) == expected)
  }

  test("common and selected converters preserve ordinary nested Parquet schemas") {
    val schema = new Schema(
      Types.NestedField.required(1, "id", Types.IntegerType.get()),
      Types.NestedField.optional(2, "payload", Types.StructType.of(
        Types.NestedField.optional(3, "amounts",
          Types.ListType.ofOptional(4, Types.DecimalType.of(9, 2))),
        Types.NestedField.optional(5, "timestamps",
          Types.MapType.ofOptional(6, 7, Types.StringType.get(), Types.TimestampType.withZone())),
        Types.NestedField.required(8, "uuid", Types.UUIDType.get()))))
    val shadedSchema = ParquetSchemaUtil.convert(schema, "ordinary_schema")
    val expected = ParquetTypes.buildMessage()
      .required(ParquetPrimitiveTypeName.INT32).id(1).named("id")
      .optionalGroup().id(2)
      .optionalGroup().as(LogicalTypeAnnotation.listType()).id(3)
      .repeatedGroup()
      .optional(ParquetPrimitiveTypeName.INT32)
      .as(LogicalTypeAnnotation.decimalType(2, 9)).id(4).named("element")
      .named("list")
      .named("amounts")
      .optionalGroup().as(LogicalTypeAnnotation.mapType()).id(5)
      .repeatedGroup()
      .required(ParquetPrimitiveTypeName.BINARY)
      .as(LogicalTypeAnnotation.stringType()).id(6).named("key")
      .optional(ParquetPrimitiveTypeName.INT64)
      .as(LogicalTypeAnnotation.timestampType(true, MICROS)).id(7).named("value")
      .named("key_value")
      .named("timestamps")
      .required(ParquetPrimitiveTypeName.FIXED_LEN_BYTE_ARRAY).length(16)
      .as(LogicalTypeAnnotation.uuidType()).id(8).named("uuid")
      .named("payload")
      .named("ordinary_schema")

    assert(new ParquetUnshader().unshade(shadedSchema) == expected)
    assert(ShimUtils.getParquetUnshader().unshade(shadedSchema) == expected)
    assert(ShimUtils.unshadeParquetSchema(shadedSchema) == expected)
  }
}
