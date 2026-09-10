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

import org.apache.orc.TypeDescription
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.sql.types.{ArrayType, MapType, StringType, StructField, StructType}

class SchemaUtilsSuite extends AnyFunSuite {

  test("convert ORC schemas with numeric-only field names to Catalyst") {
    val orcSchema = TypeDescription.createStruct()
      .addField("1", TypeDescription.createString())
      .addField("50", TypeDescription.createStruct()
        .addField("20", TypeDescription.createString())
        .addField("30", TypeDescription.createStruct()
          .addField("40", TypeDescription.createString())))
      .addField("789", TypeDescription.createList(
        TypeDescription.createStruct()
          .addField("123", TypeDescription.createString())))
      .addField("012", TypeDescription.createMap(
        TypeDescription.createStruct()
          .addField("321", TypeDescription.createString()),
        TypeDescription.createStruct()
          .addField("456", TypeDescription.createString())))

    val expected = StructType(Seq(
      StructField("1", StringType),
      StructField("50", StructType(Seq(
        StructField("20", StringType),
        StructField("30", StructType(Seq(
          StructField("40", StringType))))))),
      StructField("789", ArrayType(StructType(Seq(
        StructField("123", StringType))))),
      StructField("012", MapType(
        StructType(Seq(StructField("321", StringType))),
        StructType(Seq(StructField("456", StringType)))))))

    assert(SchemaUtils.toCatalystSchema(orcSchema) === expected)
  }
}
