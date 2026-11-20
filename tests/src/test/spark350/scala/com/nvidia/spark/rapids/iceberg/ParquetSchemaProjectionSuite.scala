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
  MessageType => ShadedMessageType,
  MessageTypeParser => ShadedMessageTypeParser,
  Type => ShadedType,
  Types => ShadedTypes
}
import org.apache.iceberg.shaded.org.apache.parquet.schema.PrimitiveType.{
  PrimitiveTypeName => ShadedPrimitiveTypeName
}
import org.apache.iceberg.shaded.org.apache.parquet.schema.Type.{Repetition => ShadedRepetition}
import org.apache.iceberg.types.Types
import org.scalatest.funsuite.AnyFunSuite

/** Focused schema tests for ParquetSchemaProjection. */
class ParquetSchemaProjectionSuite extends AnyFunSuite {
  import IcebergDefaultTestUtils.{createParquetInfo, fieldWithDefaults}

  test("Legacy two-level LIST projections preserve struct element interpretation") {
    val defaultField = fieldWithDefaults(
      5, "added", Types.IntegerType.get(), required = false,
      initialDefault = Some(Int.box(11))).getOrElse {
      cancel("Iceberg runtime does not expose v3 field defaults")
    }
    for (elementName <- Seq("element", "array"); projectSibling <- Seq(false, true)) {
      val fileSchema = ShadedMessageTypeParser.parseMessageType(
        s"""message test {
           |  optional group items (LIST) = 1 {
           |    repeated group $elementName = 2 {
           |      optional int64 a = 3;
           |      optional int64 b = 4;
           |    }
           |  }
           |}""".stripMargin)
      val fields = if (projectSibling) {
        Seq(Types.NestedField.optional(3, "a", Types.LongType.get()), defaultField)
      } else {
        Seq(defaultField)
      }
      val expectedSchema = new Schema(Types.NestedField.optional(1, "items",
        Types.ListType.ofRequired(2, Types.StructType.of(fields.asJava))))
      val retained = ParquetSchemaProjection.preserveStructValidity(
        fileSchema, ParquetSchemaUtil.pruneColumns(fileSchema, expectedSchema), expectedSchema)
      val physical = GpuIcebergParquetReader.physicalHelperSchema(retained, expectedSchema)
      val element = ParquetSchemaUtil.convert(physical).findType("items")
        .asListType().elementType()
      assert(element.isStructType)
      assert(element.asStructType().fields().size() == (if (elementName == "element") 2 else 1))
      val (info, shaded) = createParquetInfo(physical)
      val processor = new GpuParquetReaderPostProcessor(
        info, new JHashMap[Integer, Any](), expectedSchema, shaded, Map.empty)
      val expected = if (projectSibling) {
        """ProcessStruct
          |  items (input[0]):
          |    ProcessList
          |      element:
          |        ProcessStruct
          |          field[0] (input[0]):
          |            PassThrough
          |          field[1] (generated):
          |            FillDefault(fieldId=5, int)""".stripMargin
      } else {
        """ProcessStruct
          |  items (input[0]):
          |    ProcessList
          |      element:
          |        ProcessStruct
          |          field[0] (generated):
          |            FillDefault(fieldId=5, int)""".stripMargin
      }
      assert(processor.displayActionPlan() == expected)
    }
  }

  test("Default-only struct projections retain physical validity through nested containers") {
    val defaultField = fieldWithDefaults(
      3,
      "added",
      Types.IntegerType.get(),
      required = false,
      initialDefault = Some(Int.box(11))).getOrElse {
      cancel("Iceberg runtime does not expose v3 field defaults")
    }
    val expectedStruct = Types.StructType.of(defaultField)
    val presentField =
      ShadedTypes.primitive(ShadedPrimitiveTypeName.INT64, ShadedRepetition.OPTIONAL)
        .id(2).named("present")
    val ignoredField =
      ShadedTypes.primitive(ShadedPrimitiveTypeName.INT64, ShadedRepetition.OPTIONAL)
        .id(6).named("ignored")
    val keyField =
      ShadedTypes.primitive(ShadedPrimitiveTypeName.INT64, ShadedRepetition.REQUIRED)
        .id(5).named("key")
    val unrelatedField =
      ShadedTypes.primitive(ShadedPrimitiveTypeName.INT64, ShadedRepetition.OPTIONAL)
        .id(9).named("unrelated")

    def physicalStruct(name: String, includeIgnored: Boolean) = {
      val builder = ShadedTypes.optionalGroup().addField(presentField)
      if (includeIgnored) {
        builder.addField(ignoredField)
      }
      builder.id(1).named(name)
    }

    def physicalFields(includeIgnored: Boolean): Seq[ShadedType] = Seq(
      physicalStruct("s", includeIgnored),
      ShadedTypes.optionalGroup().addField(physicalStruct("s", includeIgnored))
        .id(4).named("outer"),
      ShadedTypes.optionalList().element(physicalStruct("element", includeIgnored))
        .id(4).named("items"),
      ShadedTypes.optionalMap().key(keyField).value(physicalStruct("value", includeIgnored))
        .id(4).named("entries"),
      ShadedTypes.optionalGroup().addField(
        ShadedTypes.optionalMap().key(keyField).value(physicalStruct("value", includeIgnored))
          .id(7).named("entries"))
        .id(4).named("outer"),
      ShadedTypes.optionalGroup().addField(
        ShadedTypes.optionalList().element(physicalStruct("element", includeIgnored))
          .id(7).named("items"))
        .id(4).named("outer"))

    val expectedFields = Seq(
      Types.NestedField.optional(1, "s", expectedStruct),
      Types.NestedField.optional(4, "outer", Types.StructType.of(
        Types.NestedField.optional(1, "s", expectedStruct))),
      Types.NestedField.optional(4, "items", Types.ListType.ofOptional(1, expectedStruct)),
      Types.NestedField.optional(4, "entries",
        Types.MapType.ofOptional(5, 1, Types.LongType.get(), expectedStruct)),
      Types.NestedField.optional(4, "outer", Types.StructType.of(
        Types.NestedField.optional(7, "entries",
          Types.MapType.ofOptional(5, 1, Types.LongType.get(), expectedStruct)))),
      Types.NestedField.optional(4, "outer", Types.StructType.of(
        Types.NestedField.optional(7, "items", Types.ListType.ofOptional(1, expectedStruct)))))

    val expectedPlans = Seq(
      """ProcessStruct
        |  s (input[0]):
        |    ProcessStruct
        |      field[0] (generated):
        |        FillDefault(fieldId=3, int)""".stripMargin,
      """ProcessStruct
        |  outer (input[0]):
        |    ProcessStruct
        |      field[0] (input[0]):
        |        ProcessStruct
        |          field[0] (generated):
        |            FillDefault(fieldId=3, int)""".stripMargin,
      """ProcessStruct
        |  items (input[0]):
        |    ProcessList
        |      element:
        |        ProcessStruct
        |          field[0] (generated):
        |            FillDefault(fieldId=3, int)""".stripMargin,
      """ProcessStruct
        |  entries (input[0]):
        |    ProcessMap
        |      key:
        |        PassThrough
        |      value:
        |        ProcessStruct
        |          field[0] (generated):
        |            FillDefault(fieldId=3, int)""".stripMargin,
      """ProcessStruct
        |  outer (input[0]):
        |    ProcessStruct
        |      field[0] (input[0]):
        |        ProcessMap
        |          key:
        |            PassThrough
        |          value:
        |            ProcessStruct
        |              field[0] (generated):
        |                FillDefault(fieldId=3, int)""".stripMargin,
      """ProcessStruct
        |  outer (input[0]):
        |    ProcessStruct
        |      field[0] (input[0]):
        |        ProcessList
        |          element:
        |            ProcessStruct
        |              field[0] (generated):
        |                FillDefault(fieldId=3, int)""".stripMargin)

    physicalFields(includeIgnored = true).zip(physicalFields(includeIgnored = false))
      .zip(expectedFields).zip(expectedPlans).foreach {
        case (((fileField, retainedField), expectedField), expectedPlan) =>
          withClue(s"${expectedField.name()}: ") {
            val fileSchema = new ShadedMessageType("test", Seq(fileField, unrelatedField).asJava)
            val expectedSchema = new Schema(expectedField)
            val projected = ParquetSchemaUtil.pruneColumns(fileSchema, expectedSchema)
            val restored = ParquetSchemaProjection.preserveStructValidity(
              fileSchema, projected, expectedSchema)
            val projectedPaths = projected.getPaths.asScala.map(_.toSeq).toSet
            val helperSchema = new ShadedMessageType("test", Seq(retainedField).asJava)
            val helperPaths = helperSchema.getPaths.asScala.map(_.toSeq).toSet
            // Iceberg can already retain extra physical siblings for nested projections.
            // Preserve those fields and add only the helper paths needed by empty structs.
            assert(restored.getPaths.asScala.map(_.toSeq).toSet == projectedPaths ++ helperPaths)
            val partnerSchema = ParquetSchemaUtil.convert(restored)
            assert(partnerSchema.findField(1) != null)
            assert(partnerSchema.findField(2) != null)
            assert((partnerSchema.findField(6) != null) ==
              projectedPaths.exists(_.last == "ignored"))
            assert(partnerSchema.findField(9) == null)
            assert(ParquetSchemaProjection.preserveStructValidity(
              fileSchema, fileSchema, expectedSchema) == fileSchema)
            val emptySchema = new ShadedMessageType("test", Seq.empty[ShadedType].asJava)
            assert(ParquetSchemaProjection.preserveStructValidity(
              fileSchema, emptySchema, new Schema()) == emptySchema)

            val (parquetInfo, shadedSchema) = createParquetInfo(restored)
            val processor = new GpuParquetReaderPostProcessor(
              parquetInfo,
              new JHashMap[Integer, Any](),
              expectedSchema,
              shadedSchema,
              Map.empty)
            assert(processor.displayActionPlan() == expectedPlan)
          }
      }
  }

  test("Default-only projections restore all requested sibling collection paths") {
    def addedField(id: Int) = fieldWithDefaults(
      id, "added", Types.IntegerType.get(), required = false,
      initialDefault = Some(Int.box(11))).getOrElse {
      cancel("Iceberg runtime does not expose v3 field defaults")
    }
    val unrelated = ShadedTypes.optional(ShadedPrimitiveTypeName.INT64).id(2).named("unrelated")
    val listElement = ShadedTypes.optionalGroup().addField(
      ShadedTypes.optional(ShadedPrimitiveTypeName.INT64).id(5).named("present"))
      .id(4).named("element")
    val items = ShadedTypes.optionalList().element(listElement).id(3).named("b")
    val mapKey = ShadedTypes.required(ShadedPrimitiveTypeName.INT64).id(8).named("key")
    val mapValue = ShadedTypes.optionalGroup().addField(
      ShadedTypes.optional(ShadedPrimitiveTypeName.INT64).id(10).named("present"))
      .id(9).named("value")
    val entries = ShadedTypes.optionalMap().key(mapKey).value(mapValue).id(7).named("c")
    val fileStruct = ShadedTypes.optionalGroup().addFields(unrelated, items, entries)
      .id(1).named("s")
    val requestedCollections = Seq(
      Types.NestedField.optional(3, "b",
        Types.ListType.ofOptional(4, Types.StructType.of(addedField(6)))),
      Types.NestedField.optional(7, "c", Types.MapType.ofOptional(
        8, 9, Types.LongType.get(), Types.StructType.of(addedField(11)))))

    for (nested <- Seq(false, true); projectSibling <- Seq(false, true)) {
      withClue(s"nested=$nested, projectSibling=$projectSibling: ") {
        val fields = if (projectSibling) {
          Types.NestedField.optional(2, "unrelated", Types.LongType.get()) +:
            requestedCollections
        } else {
          requestedCollections
        }
        val requiredStruct = Types.NestedField.optional(1, "s", Types.StructType.of(fields.asJava))
        val fileField = if (nested) {
          ShadedTypes.optionalGroup().addField(
            ShadedTypes.optional(ShadedPrimitiveTypeName.INT64).id(13).named("first"))
            .addField(fileStruct).id(12).named("outer")
        } else {
          fileStruct
        }
        val requiredField = if (nested) {
          Types.NestedField.optional(12, "outer", Types.StructType.of(requiredStruct))
        } else {
          requiredStruct
        }
        val fileSchema = new ShadedMessageType("test", Seq[ShadedType](fileField).asJava)
        val requiredSchema = new Schema(requiredField)
        val projected = ParquetSchemaUtil.pruneColumns(fileSchema, requiredSchema)
        val restored = ParquetSchemaProjection.preserveStructValidity(
          fileSchema, projected, requiredSchema)
        val partner = ParquetSchemaUtil.convert(restored)
        val projectedPaths = projected.getPaths.asScala.map(_.toSeq).toSet
        Seq(1, 3, 4, 5, 7, 8, 9, 10).foreach(id => assert(partner.findField(id) != null))
        // Keep extra siblings already retained by Iceberg, but do not add unrelated helpers.
        assert((partner.findField(2) != null) == projectedPaths.exists(_.last == "unrelated"))
        assert((partner.findField(13) != null) == projectedPaths.exists(_.last == "first"))
        assert(ParquetSchemaProjection.preserveStructValidity(
          fileSchema, restored, requiredSchema) == restored)

        val (parquetInfo, shadedSchema) = createParquetInfo(restored)
        val processor = new GpuParquetReaderPostProcessor(
          parquetInfo, new JHashMap[Integer, Any](), requiredSchema, shadedSchema, Map.empty)
        // Iceberg retains the inner sibling for nested projections, even if it is not
        // requested. Output field order and physical input column order can therefore differ.
        val listIndex = if (projectSibling) 1 else 0
        val mapIndex = listIndex + 1
        val listInputIndex = if (nested || projectSibling) 1 else 0
        val mapInputIndex = listInputIndex + 1
        val siblingAction = if (projectSibling) {
          "\n  field[0] (input[0]):\n    PassThrough"
        } else {
          ""
        }
        val structActions =
          s"""ProcessStruct$siblingAction
             |  field[$listIndex] (input[$listInputIndex]):
             |    ProcessList
             |      element:
             |        ProcessStruct
             |          field[0] (generated):
             |            FillDefault(fieldId=6, int)
             |  field[$mapIndex] (input[$mapInputIndex]):
             |    ProcessMap
             |      key:
             |        PassThrough
             |      value:
             |        ProcessStruct
             |          field[0] (generated):
             |            FillDefault(fieldId=11, int)""".stripMargin
        val action = if (nested) {
          "ProcessStruct\n  field[0] (input[0]):\n" +
            structActions.linesIterator.map("    " + _).mkString("\n")
        } else {
          structActions
        }
        val name = if (nested) "outer" else "s"
        val expected = s"ProcessStruct\n  $name (input[0]):\n" +
          action.linesIterator.map("    " + _).mkString("\n")
        assert(processor.displayActionPlan() == expected)
      }
    }
  }
}
