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

package com.nvidia.spark.rapids.parquet

import scala.collection.JavaConverters._

import com.nvidia.spark.rapids.shims.parquet.ParquetUnknownTypeAnnotationShims
import org.apache.parquet.schema.{LogicalTypeAnnotation, MessageTypeParser, Types}
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.parquet.schema.Type.Repetition
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.sql.types.{StringType, StructType}

class ParquetSchemaUtilsSuite extends AnyFunSuite {
  private val requestedSchema = new StructType()
      .add("name", new StructType().add("middle", StringType))
      .add("address", StringType)

  private val fileSchema = MessageTypeParser.parseMessageType(
    """message root {
      |  optional group name {
      |    optional binary first (STRING);
      |    optional binary last (STRING);
      |  }
      |  optional binary address (STRING);
      |}
      |""".stripMargin)

  private def paths(schema: org.apache.parquet.schema.MessageType): Seq[String] =
    schema.getPaths.asScala.map(_.mkString(".")).toSeq

  test("legacy missing struct drops a leafless group but keeps physical siblings") {
    val clipped = ParquetSchemaUtils.clipParquetSchema(
      fileSchema,
      requestedSchema,
      caseSensitive = true,
      useFieldId = false,
      returnNullStructIfAllFieldsMissing = true)

    assertResult(Seq("address"))(paths(clipped))
    assert(!clipped.containsField("name"))
  }

  test("legacy missing list element drops a leafless parent but keeps physical siblings") {
    val nestedList = MessageTypeParser.parseMessageType(
      """message root {
        |  optional group name {
        |    optional group arr (LIST) {
        |      repeated group list {
        |        optional group element {
        |          optional binary first (STRING);
        |        }
        |      }
        |    }
        |  }
        |  optional binary address (STRING);
        |}
        |""".stripMargin)
    val requested = new StructType()
        .add("name", new StructType()
          .add("arr", org.apache.spark.sql.types.ArrayType(
            new StructType().add("middle", StringType))))
        .add("address", StringType)
    val clipped = ParquetSchemaUtils.clipParquetSchema(
      nestedList, requested, caseSensitive = true, useFieldId = false,
      returnNullStructIfAllFieldsMissing = true)

    assertResult(Seq("address"))(paths(clipped))
    assert(!clipped.containsField("name"))
  }

  test("missing struct retains a carrier leaf when parent validity is required") {
    val clipped = ParquetSchemaUtils.clipParquetSchema(
      fileSchema,
      requestedSchema,
      caseSensitive = true,
      useFieldId = false,
      returnNullStructIfAllFieldsMissing = false)

    assertResult(Seq("name.first", "address"))(paths(clipped))
  }

  test("missing struct carrier minimizes repetition level before primitive cost") {
    val repeatedFirst = MessageTypeParser.parseMessageType(
      """message root {
        |  optional group name {
        |    repeated boolean repeated_flag;
        |    optional binary scalar_value (STRING);
        |  }
        |}
        |""".stripMargin)
    val clipped = ParquetSchemaUtils.clipParquetSchema(
      repeatedFirst,
      new StructType().add("name", new StructType().add("middle", StringType)),
      caseSensitive = true,
      useFieldId = false,
      returnNullStructIfAllFieldsMissing = false)

    assertResult(Seq("name.scalar_value"))(paths(clipped))
  }

  test("missing struct carrier minimizes primitive width at equal repetition level") {
    val wideFirst = MessageTypeParser.parseMessageType(
      """message root {
        |  optional group name {
        |    optional binary wide_value (STRING);
        |    optional boolean cheap_value;
        |  }
        |}
        |""".stripMargin)
    val clipped = ParquetSchemaUtils.clipParquetSchema(
      wideFirst,
      new StructType().add("name", new StructType().add("middle", StringType)),
      caseSensitive = true,
      useFieldId = false,
      returnNullStructIfAllFieldsMissing = false)

    assertResult(Seq("name.cheap_value"))(paths(clipped))
  }

  test("nested missing structs leave no physical group on legacy reads") {
    val nestedFileSchema = MessageTypeParser.parseMessageType(
      """message root {
        |  optional group name {
        |    optional group nested {
        |      optional binary first (STRING);
        |    }
        |  }
        |  optional binary address (STRING);
        |}
        |""".stripMargin)
    val requested = new StructType()
        .add("name", new StructType()
          .add("nested", new StructType().add("middle", StringType)))
        .add("address", StringType)
    val clipped = ParquetSchemaUtils.clipParquetSchema(
      nestedFileSchema, requested, caseSensitive = true, useFieldId = false,
      returnNullStructIfAllFieldsMissing = true)

    assertResult(Seq("address"))(paths(clipped))
  }

  test("missing struct carrier normalizes UNKNOWN annotation and selects its zero cost") {
    val unknownTypeFactory = classOf[LogicalTypeAnnotation].getMethods.find { method =>
      method.getName == "unknownType" && method.getParameterCount == 0
    }
    assume(unknownTypeFactory.nonEmpty, "Parquet UNKNOWN annotation requires Parquet 1.16+")
    val unknownAnnotation = unknownTypeFactory.get.invoke(null)
        .asInstanceOf[LogicalTypeAnnotation]
    val unknownFileSchema = Types.buildMessage()
        .addField(Types.buildGroup(Repetition.OPTIONAL)
          .addField(Types.primitive(PrimitiveTypeName.BOOLEAN, Repetition.OPTIONAL)
            .named("known_value"))
          .addField(Types.primitive(PrimitiveTypeName.BINARY, Repetition.OPTIONAL)
            .as(unknownAnnotation)
            .named("unknown_value"))
          .named("name"))
        .named("root")
    val clipped = ParquetSchemaUtils.clipParquetSchema(
      unknownFileSchema,
      new StructType().add("name", new StructType().add("middle", StringType)),
      caseSensitive = true,
      useFieldId = false,
      returnNullStructIfAllFieldsMissing = false)

    assertResult(Seq("name.unknown_value"))(paths(clipped))
    val originalAnnotation = unknownFileSchema.asGroupType().getType("name").asGroupType()
        .getType("unknown_value").asPrimitiveType().getLogicalTypeAnnotation
    val carrierAnnotation = clipped.asGroupType().getType("name").asGroupType()
        .getType("unknown_value").asPrimitiveType().getLogicalTypeAnnotation
    assertResult(ParquetUnknownTypeAnnotationShims.effectiveLogicalTypeAnnotation(
      originalAnnotation))(carrierAnnotation)
  }

  test("missing struct carrier preserves map key and value paths") {
    val mapFileSchema = MessageTypeParser.parseMessageType(
      """message root {
        |  optional group name {
        |    optional group attrs (MAP) {
        |      repeated group key_value {
        |        required binary key (STRING);
        |        optional binary value (STRING);
        |      }
        |    }
        |  }
        |}
        |""".stripMargin)
    val clipped = ParquetSchemaUtils.clipParquetSchema(
      mapFileSchema,
      new StructType().add("name", new StructType().add("middle", StringType)),
      caseSensitive = true,
      useFieldId = false,
      returnNullStructIfAllFieldsMissing = false)

    assertResult(Seq("name.attrs.key_value.key", "name.attrs.key_value.value"))(paths(clipped))
  }

  test("missing struct carrier preserves both Variant physical children") {
    val variantFileSchema = MessageTypeParser.parseMessageType(
      """message root {
        |  optional group name {
        |    optional group v {
        |      required binary value;
        |      required binary metadata;
        |    }
        |  }
        |}
        |""".stripMargin)
    val clipped = ParquetSchemaUtils.clipParquetSchema(
      variantFileSchema,
      new StructType().add("name", new StructType().add("middle", StringType)),
      caseSensitive = true,
      useFieldId = false,
      returnNullStructIfAllFieldsMissing = false)

    assertResult(Seq("name.v.value", "name.v.metadata"))(paths(clipped))
  }
}
