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

import org.apache.iceberg.Schema
import org.apache.iceberg.types.Types
import org.scalatest.funsuite.AnyFunSuite

class GpuWriteDefaultsSuite extends AnyFunSuite {

  private def fieldWithWriteDefault(
      id: Int,
      name: String,
      icebergType: org.apache.iceberg.types.Type,
      writeDefault: AnyRef): Option[Types.NestedField] = {
    try {
      val builder = classOf[Types.NestedField].getMethod("optional", classOf[String])
        .invoke(null, name)
      val builderClass = builder.getClass
      builderClass.getMethod("withId", java.lang.Integer.TYPE).invoke(builder, Int.box(id))
      builderClass.getMethod("ofType", classOf[org.apache.iceberg.types.Type])
        .invoke(builder, icebergType)
      builderClass.getMethod("withWriteDefault", classOf[Object])
        .invoke(builder, writeDefault)
      Some(builderClass.getMethod("build").invoke(builder).asInstanceOf[Types.NestedField])
    } catch {
      case _: NoSuchMethodException => None
    }
  }

  test("write default validation accepts supported primitives") {
    val field = fieldWithWriteDefault(
      1,
      "value",
      Types.IntegerType.get(),
      Int.box(9)).getOrElse {
      cancel("Iceberg runtime does not expose v3 field defaults")
    }

    assert(IcebergFormatVersionSupport.unsupportedWriteDefault(new Schema(field)).isEmpty)
  }

  test("write default validation reports unsupported nested fields") {
    val field = fieldWithWriteDefault(
      2,
      "created_at",
      Types.TimestampType.withoutZone(),
      Long.box(0L)).getOrElse {
      cancel("Iceberg runtime does not expose v3 field defaults")
    }
    val schema = new Schema(Types.NestedField.optional(
      1, "payload", Types.StructType.of(field)))

    assert(IcebergFormatVersionSupport.unsupportedWriteDefault(schema)
      .contains("payload.created_at" -> "timestamp"))
  }
}
