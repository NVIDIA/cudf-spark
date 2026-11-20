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
{"spark":["350"],"carryForward":"sameMinor"}
{"spark":["400","411"],"carryForward":"sameMinor"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.iceberg

import java.util.{HashMap => JHashMap}

import scala.collection.JavaConverters._

import com.nvidia.spark.rapids.iceberg.parquet.{GpuIcebergParquetReader, GpuParquetReaderPostProcessor, ParquetSchemaProjection}
import org.apache.iceberg.Schema
import org.apache.iceberg.parquet.ParquetSchemaUtil
import org.apache.iceberg.shaded.org.apache.parquet.schema.{
  MessageType => ShadedMessageType, Type => ShadedType, Types => ShadedTypes
}
import org.apache.iceberg.shaded.org.apache.parquet.schema.LogicalTypeAnnotation
import org.apache.iceberg.shaded.org.apache.parquet.schema.PrimitiveType.{
  PrimitiveTypeName => ShadedPrimitiveTypeName
}
import org.apache.iceberg.types.Types
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.sql.execution.datasources.parquet.ParquetToSparkSchemaConverter
import org.apache.spark.sql.types.{BinaryType, DateType, LongType, StructType}

/** Focused schema tests for GpuIcebergParquetReader. */
class GpuIcebergParquetReaderSuite extends AnyFunSuite {
  import IcebergDefaultTestUtils.{createParquetInfo, fieldWithDefaults}

  test("Validity helpers decode physical types and preserve projected logical types") {
    val dateField = ShadedTypes.optional(ShadedPrimitiveTypeName.INT32)
      .as(LogicalTypeAnnotation.dateType()).id(4).named("date")
    val expectedSchema = new Schema(
      Types.NestedField.optional(1, "s", Types.StructType.of(
        Types.NestedField.optional(3, "added", Types.IntegerType.get()))),
      Types.NestedField.optional(4, "date", Types.DateType.get()))
    val helpers = Seq(
      ShadedTypes.optional(ShadedPrimitiveTypeName.FIXED_LEN_BYTE_ARRAY).length(16)
        .as(LogicalTypeAnnotation.uuidType()).id(2).named("legacy") -> BinaryType,
      ShadedTypes.optional(ShadedPrimitiveTypeName.INT64)
        .as(LogicalTypeAnnotation.timeType(false, LogicalTypeAnnotation.TimeUnit.MICROS))
        .id(2).named("legacy") -> LongType,
      ShadedTypes.optional(ShadedPrimitiveTypeName.INT64)
        .as(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.NANOS))
        .id(2).named("legacy") -> LongType,
      ShadedTypes.optional(ShadedPrimitiveTypeName.INT64)
        .as(LogicalTypeAnnotation.timestampType(false, LogicalTypeAnnotation.TimeUnit.MICROS))
        .id(2).named("legacy") -> LongType)

    helpers.foreach { case (helper, sparkType) =>
      val fileSchema = new ShadedMessageType("test", Seq[ShadedType](
        ShadedTypes.optionalGroup().addField(helper).id(1).named("s"), dateField).asJava)
      val projected = ParquetSchemaUtil.pruneColumns(fileSchema, expectedSchema)
      val retained = ParquetSchemaProjection.preserveStructValidity(
        fileSchema, projected, expectedSchema)
      val physical = GpuIcebergParquetReader.physicalHelperSchema(retained, expectedSchema)
      val converted = new ParquetToSparkSchemaConverter(false, true, true, true, false)
        .convert(ShimUtils.getParquetUnshader().unshade(physical))
      assert(converted("s").dataType.asInstanceOf[StructType]("legacy").dataType == sparkType)
      assert(converted("date").dataType == DateType)
      assert(physical.getType("s", "legacy").getId.intValue() == 2)
      assert(physical.asGroupType().getType("date") == dateField)
    }
  }

  test("Opaque logical groups used as validity helpers decode physically") {
    val variant = try {
      classOf[LogicalTypeAnnotation].getMethod("variantType", java.lang.Byte.TYPE)
        .invoke(null, Byte.box(1.toByte)).asInstanceOf[LogicalTypeAnnotation]
    } catch {
      case _: NoSuchMethodException => cancel("Parquet runtime does not expose VARIANT")
    }
    val helper = ShadedTypes.optionalGroup()
      .addField(ShadedTypes.required(ShadedPrimitiveTypeName.BINARY).named("metadata"))
      .addField(ShadedTypes.required(ShadedPrimitiveTypeName.BINARY).named("value"))
      .as(variant).id(2).named("legacy")
    val fileSchema = new ShadedMessageType("test", Seq[ShadedType](
      ShadedTypes.optionalGroup().addField(helper).id(1).named("s")).asJava)
    val defaultField = fieldWithDefaults(
      3, "added", Types.IntegerType.get(), required = false,
      initialDefault = Some(Int.box(11))).getOrElse {
      cancel("Iceberg runtime does not expose v3 field defaults")
    }
    val expectedSchema = new Schema(
      Types.NestedField.optional(1, "s", Types.StructType.of(defaultField)))
    val retained = ParquetSchemaProjection.preserveStructValidity(
      fileSchema, ParquetSchemaUtil.pruneColumns(fileSchema, expectedSchema), expectedSchema)
    val physical = GpuIcebergParquetReader.physicalHelperSchema(retained, expectedSchema)
    val physicalHelper = physical.getType("s", "legacy").asGroupType()
    assert(physicalHelper.getLogicalTypeAnnotation == null)
    assert(physicalHelper.getId.intValue() == 2)
    assert(physicalHelper.getFieldCount == 1)
    assert(helper.getLogicalTypeAnnotation == variant)
    val converted = new ParquetToSparkSchemaConverter(false, true, true, true, false)
      .convert(ShimUtils.getParquetUnshader().unshade(physical))
    assert(converted("s").dataType.asInstanceOf[StructType]("legacy")
      .dataType.asInstanceOf[StructType]("metadata").dataType == BinaryType)
    val (info, shaded) = createParquetInfo(physical)
    val processor = new GpuParquetReaderPostProcessor(
      info, new JHashMap[Integer, Any](), expectedSchema, shaded, Map.empty)
    assert(processor.displayActionPlan() ==
      """ProcessStruct
        |  s (input[0]):
        |    ProcessStruct
        |      field[0] (generated):
        |        FillDefault(fieldId=3, int)""".stripMargin)

    val projectedVariant = org.apache.iceberg.SchemaParser.fromJson(
      """{"type":"struct","fields":[{"id":1,"name":"s","required":false,
        |"type":{"type":"struct","fields":[{"id":2,"name":"legacy",
        |"required":false,"type":"variant"}]}}]}""".stripMargin)
    val projected = GpuIcebergParquetReader.physicalHelperSchema(fileSchema, projectedVariant)
    assert(projected.getType("s", "legacy").getLogicalTypeAnnotation == variant)
  }

  test("Unmapped validity helpers cannot bind to requested field IDs") {
    val defaultField = fieldWithDefaults(
      1000, "added", Types.IntegerType.get(), required = false,
      initialDefault = Some(Int.box(11))).getOrElse {
      cancel("Iceberg runtime does not expose v3 field defaults")
    }
    val expectedSchema = new Schema(
      Types.NestedField.optional(1, "id", Types.IntegerType.get()),
      Types.NestedField.optional(2, "s", Types.StructType.of(defaultField)),
      Types.NestedField.optional(3, "other", Types.LongType.get()))
    val helpers = Seq(
      ShadedTypes.optional(ShadedPrimitiveTypeName.INT64).named("untracked"),
      ShadedTypes.optionalGroup().addField(
        ShadedTypes.optional(ShadedPrimitiveTypeName.INT64).named("leaf")).named("untracked"),
      ShadedTypes.optionalList().element(ShadedTypes.optionalGroup().addField(
        ShadedTypes.optional(ShadedPrimitiveTypeName.INT64).named("leaf")).named("element"))
        .named("untracked"),
      ShadedTypes.optionalMap().key(
        ShadedTypes.required(ShadedPrimitiveTypeName.BINARY)
          .as(LogicalTypeAnnotation.stringType()).named("key"))
        .value(ShadedTypes.optionalGroup().addField(
          ShadedTypes.optional(ShadedPrimitiveTypeName.INT64).named("leaf")).named("value"))
        .named("untracked"))
    helpers.foreach { helper =>
      val fileSchema = new ShadedMessageType("test", Seq[ShadedType](
        ShadedTypes.optional(ShadedPrimitiveTypeName.INT32).id(1).named("id"),
        ShadedTypes.optionalGroup().addField(helper).id(2).named("s")).asJava)
      val retained = ShimUtils.pruneColumns(fileSchema, expectedSchema, false)
      val physical = GpuIcebergParquetReader.physicalHelperSchema(retained, expectedSchema)
      val partner = ParquetSchemaUtil.convert(physical)
      assert(partner.findField(1000) == null)
      assert(partner.findField(3) == null)
      assert(partner.findField("s.untracked").fieldId() != 1000)
      if (helper.getLogicalTypeAnnotation != null) {
        assert(physical.getType("s", "untracked").asGroupType().getType(0).getId == null)
      }
      val (info, shaded) = createParquetInfo(physical)
      val processor = new GpuParquetReaderPostProcessor(
        info, new JHashMap[Integer, Any](), expectedSchema, shaded, Map.empty)
      assert(processor.displayActionPlan() ==
        """ProcessStruct
          |  id (input[0]):
          |    PassThrough
          |  s (input[1]):
          |    ProcessStruct
          |      field[0] (generated):
          |        FillDefault(fieldId=1000, int)
          |  other (generated):
          |    FillNull(bigint)""".stripMargin)
    }
  }
}
