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
{"spark": "411"}
{"spark": "412"}
{"spark": "413"}
spark-rapids-shim-json-lines ***/
package org.apache.iceberg.spark

import com.nvidia.spark.rapids.SchemaUtils._
import com.nvidia.spark.rapids.iceberg.ShimUtils
import com.nvidia.spark.rapids.iceberg.parquet.converter.ParquetUnshader
import org.apache.iceberg.Schema
import org.apache.iceberg.parquet.ParquetSchemaUtil
import org.apache.iceberg.shaded.org.apache.parquet.schema.{LogicalTypeAnnotation => ShadedLogicalTypeAnnotation, Types => ShadedTypes}
import org.apache.iceberg.shaded.org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY
import org.apache.iceberg.types.Types
import org.apache.parquet.schema.{LogicalTypeAnnotation, Types => ParquetTypes}
import org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit.MICROS
import org.apache.parquet.schema.PrimitiveType.{PrimitiveTypeName => ParquetPrimitiveTypeName}
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.sql.execution.datasources.parquet.ParquetToSparkSchemaConverter
import org.apache.spark.sql.execution.datasources.parquet.ParquetUtils.FIELD_ID_METADATA_KEY
import org.apache.spark.sql.types.{ArrayType, MapType, StringType, StructType, VariantType}

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

  test("unshade preserves the Variant logical type spec version") {
    val shaded = ShadedTypes.buildMessage()
      .optionalGroup().as(ShadedLogicalTypeAnnotation.variantType(1.toByte))
      .required(BINARY).named("metadata")
      .required(BINARY).named("value")
      .named("variant_value")
      .named("variant_schema")
    val unshaded = ShimUtils.getParquetUnshader().unshade(shaded)
      .asGroupType().getType("variant_value").getLogicalTypeAnnotation
      .asInstanceOf[LogicalTypeAnnotation.VariantLogicalTypeAnnotation]

    assert(unshaded.getSpecVersion == 1.toByte)
  }

  test("the selected converter handles Variant annotations throughout a Parquet schema") {
    val shadedSchema = ParquetSchemaUtil.convert(variantSchema, "variant_schema")
    val unshadedSchema = ShimUtils.getParquetUnshader().unshade(shadedSchema)
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
  }
}
