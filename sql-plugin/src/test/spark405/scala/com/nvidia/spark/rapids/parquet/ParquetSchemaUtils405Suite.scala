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
{"spark":["405"],"carryForward":"sameMinor"}
spark-rapids-shim-json-lines ***/

package com.nvidia.spark.rapids.parquet

import com.nvidia.spark.rapids.FQSuiteName
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32
import org.apache.parquet.schema.Type.Repetition.OPTIONAL
import org.apache.parquet.schema.Types
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.SparkThrowable
import org.apache.spark.sql.types.{IntegerType, MetadataBuilder, StructField, StructType}

class ParquetSchemaUtils405Suite extends AnyFunSuite with FQSuiteName {
  test("reject two requested fields that select the same Parquet field") {
    val fileSchema = Types.buildMessage()
      .addField(Types.primitive(INT32, OPTIONAL).id(1).named("A"))
      .named("root")
    val idMetadata = new MetadataBuilder()
      .putLong("parquet.field.id", 1L)
      .build()
    val requestedSchema = StructType(Seq(
      StructField("x", IntegerType, nullable = true, idMetadata),
      StructField("a", IntegerType, nullable = true)))

    val error = intercept[SparkThrowable] {
      ParquetSchemaUtils.clipParquetSchema(
        fileSchema, requestedSchema, caseSensitive = false, useFieldId = true)
    }
    assert(error.getCondition === "PARQUET_FIELD_REQUESTED_MORE_THAN_ONCE")
  }
}
