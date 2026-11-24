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

import scala.collection.JavaConverters._

import com.nvidia.spark.rapids.iceberg.parquet.ParquetReadUtils
import org.apache.iceberg.Schema
import org.apache.iceberg.expressions.Expressions
import org.apache.iceberg.shaded.org.apache.parquet.schema.{
  MessageType => ShadedMessageType, Type => ShadedType, Types => ShadedTypes
}
import org.apache.iceberg.shaded.org.apache.parquet.schema.PrimitiveType.{
  PrimitiveTypeName => ShadedPrimitiveTypeName
}
import org.apache.iceberg.types.Types
import org.scalatest.funsuite.AnyFunSuite

/** Focused schema tests for ParquetReadUtils. */
class ParquetReadUtilsSuite extends AnyFunSuite {
  import IcebergDefaultTestUtils.fieldWithDefaults

  test("Row-group pruning preserves missing defaults and recognizes physical field IDs") {
    val defaultField = fieldWithDefaults(
      2, "added", Types.IntegerType.get(), required = false,
      initialDefault = Some(Int.box(11))).getOrElse {
      cancel("Iceberg runtime does not expose v3 field defaults")
    }
    val nestedDefault = fieldWithDefaults(
      4, "added", Types.IntegerType.get(), required = false,
      initialDefault = Some(Int.box(11))).get
    val requiredSchema = new Schema(
      Types.NestedField.optional(1, "id", Types.LongType.get()),
      defaultField,
      Types.NestedField.optional(3, "s", Types.StructType.of(nestedDefault)))
    val physicalId = ShadedTypes.optional(ShadedPrimitiveTypeName.INT64).id(1).named("id")
    val physicalSibling =
      ShadedTypes.optional(ShadedPrimitiveTypeName.INT64).id(5).named("present")
    val physicalStruct = ShadedTypes.optionalGroup().addField(physicalSibling).id(3).named("s")
    val oldSchema = new ShadedMessageType(
      "test", Seq[ShadedType](physicalId, physicalStruct).asJava)

    Seq(
      Expressions.equal("added", 11),
      Expressions.greaterThan("added", 10),
      Expressions.notNull("added"),
      Expressions.not(Expressions.equal("added", 23)),
      Expressions.and(Expressions.equal("id", 1L), Expressions.equal("added", 11)),
      Expressions.or(Expressions.equal("id", 1L), Expressions.equal("added", 11)),
      Expressions.equal("s.added", 11)).foreach { filter =>
      assert(ParquetReadUtils.hasMissingDefaultFilter(
        oldSchema, requiredSchema, filter, caseSensitive = true), filter.toString)
    }
    assert(!ParquetReadUtils.hasMissingDefaultFilter(
      oldSchema, requiredSchema, Expressions.equal("id", 1L), caseSensitive = true))
    assert(ParquetReadUtils.hasMissingDefaultFilter(
      oldSchema, requiredSchema, Expressions.equal("ADDED", 11), caseSensitive = false))

    val renamedPhysical =
      ShadedTypes.optional(ShadedPrimitiveTypeName.INT32).id(2).named("old_name")
    val currentSchema = new ShadedMessageType(
      "test", Seq[ShadedType](physicalId, renamedPhysical, physicalStruct).asJava)
    assert(!ParquetReadUtils.hasMissingDefaultFilter(
      currentSchema, requiredSchema, Expressions.notNull("added"), caseSensitive = true))
    val nullDefaultSchema = new Schema(
      Types.NestedField.optional(2, "added", Types.IntegerType.get()))
    assert(!ParquetReadUtils.hasMissingDefaultFilter(
      oldSchema, nullDefaultSchema, Expressions.notNull("added"), caseSensitive = true))
  }
}
