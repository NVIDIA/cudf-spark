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

package com.nvidia.spark.rapids

import ai.rapids.cudf.{ColumnFilterOptions, ORCOptions}
import org.apache.hadoop.conf.Configuration
import org.apache.orc.TypeDescription
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.sql.types.{ArrayType, DecimalType, MapType, StringType, StructField, StructType}

class OrcReaderOptionsSuite extends AnyFunSuite {
  private val reader = new FilePartitionReaderBase(new Configuration(), Map.empty)
      with OrcCommonFunctions {
    override val conf: Configuration = new Configuration()
    override val debugDumpPrefix: Option[String] = None
    override val debugDumpAlways: Boolean = false
    override val readDataSchema: StructType = StructType(Nil)
    override def next(): Boolean = false
  }

  // cuDF exposes these options only through package-private JNI accessors.
  private def included(options: ORCOptions): Seq[String] = {
    val getter = classOf[ColumnFilterOptions].getDeclaredMethod("getIncludeColumnNames")
    getter.setAccessible(true)
    getter.invoke(options).asInstanceOf[Array[String]].toSeq
  }

  private def decimals(options: ORCOptions): Seq[String] = {
    val getter = classOf[ORCOptions].getDeclaredMethod("getDecimal128Columns")
    getter.setAccessible(true)
    getter.invoke(options).asInstanceOf[Array[String]].toSeq
  }

  test("ordinary names retain column selection and requested decimal128 overrides") {
    val fileSchema = TypeDescription.fromString("struct<small:decimal(15,2),large:decimal(25,2)>")
    val readSchema = StructType(Seq(
      StructField("small", DecimalType(25, 2)), StructField("large", DecimalType(25, 2))))
    val (options, prepared) = reader.getORCOptionsAndSchema(fileSchema, None, readSchema)
    assert(prepared == fileSchema)
    assert(included(options) == Seq("small", "large"))
    assert(decimals(options) == Seq("small", "large"))
    assert(prepared.getChildren.get(0).getPrecision == 15)
  }

  test("literal dotted root disables ambiguous selectors and decimal overrides") {
    val fileSchema = TypeDescription.createStruct()
      .addField("a", TypeDescription.createStruct()
        .addField("b", TypeDescription.createDecimal().withPrecision(15).withScale(2)))
      .addField("a.b", TypeDescription.createDecimal().withPrecision(25).withScale(2))
    val readSchema = StructType(Seq(
      StructField("a", StructType(Seq(StructField("b", DecimalType(15, 2))))),
      StructField("a.b", DecimalType(25, 2))))
    val (options, prepared) = reader.getORCOptionsAndSchema(fileSchema, None, readSchema)
    assert(prepared == fileSchema)
    assert(included(options).isEmpty)
    assert(decimals(options).isEmpty)
  }

  for (container <- Seq("struct", "array", "map")) {
    test(s"nested dotted $container retains root selection but disables decimal overrides") {
      val dotted = TypeDescription.createStruct()
        .addField("a.b", TypeDescription.createDecimal().withPrecision(25).withScale(2))
      val child = container match {
        case "array" => TypeDescription.createList(dotted)
        case "map" => TypeDescription.createMap(TypeDescription.createString(), dotted)
        case _ => dotted
      }
      val fileSchema = TypeDescription.createStruct().addField("s", child)
        .addField("wide", TypeDescription.createDecimal().withPrecision(25).withScale(2))
      val dottedReadType = StructType(Seq(StructField("a.b", DecimalType(25, 2))))
      val childReadType = container match {
        case "array" => ArrayType(dottedReadType)
        case "map" => MapType(StringType, dottedReadType)
        case _ => dottedReadType
      }
      val readSchema = StructType(Seq(
        StructField("s", childReadType), StructField("wide", DecimalType(25, 2))))
      val (options, prepared) = reader.getORCOptionsAndSchema(fileSchema, None, readSchema)
      assert(prepared == fileSchema)
      assert(included(options) == Seq("s", "wide"))
      assert(decimals(options).isEmpty)
    }
  }

  test("requested mapping preserves order and excludes missing and unused fields") {
    val fileSchema = TypeDescription.fromString(
      "struct<unused:int,small:decimal(15,2),large:decimal(25,2)>")
    val readSchema = StructType(Seq(
      StructField("large", DecimalType(25, 2)), StructField("missing", StringType),
      StructField("small", DecimalType(25, 2))))
    val (options, prepared) = reader.getORCOptionsAndSchema(
      fileSchema, Some(Array(2, -1, 1)), readSchema)
    assert(prepared.getFieldNames.toArray.toSeq == Seq("large", "small"))
    assert(included(options) == Seq("large", "small"))
    assert(decimals(options) == Seq("large", "small"))
    assert(fileSchema.getFieldNames.size() == 3)
  }
}
