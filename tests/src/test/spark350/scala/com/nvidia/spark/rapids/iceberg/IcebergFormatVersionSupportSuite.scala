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

import java.nio.ByteBuffer
import java.util.UUID

import org.apache.iceberg.Schema
import org.apache.iceberg.types.Types
import org.scalatest.funsuite.AnyFunSuite

/** Focused schema tests for IcebergFormatVersionSupport. */
class IcebergFormatVersionSupportSuite extends AnyFunSuite {
  import IcebergDefaultTestUtils.fieldWithDefaults

  test("Unsupported Iceberg defaults are identified by subtype and nested path") {
    val timestampNtzField = fieldWithDefaults(
      3,
      "created_at",
      Types.TimestampType.withoutZone(),
      required = false,
      initialDefault = Some(Long.box(0L))).getOrElse {
      cancel("Iceberg runtime does not expose v3 field defaults")
    }
    val timestampField = fieldWithDefaults(
      4,
      "updated_at",
      Types.TimestampType.withZone(),
      required = false,
      initialDefault = Some(Long.box(0L))).getOrElse {
      cancel("Iceberg runtime does not expose v3 field defaults")
    }
    val uuidField = fieldWithDefaults(
      5,
      "uuid_value",
      Types.UUIDType.get(),
      required = false,
      initialDefault = Some(UUID.fromString("123e4567-e89b-12d3-a456-426614174000"))).getOrElse {
      cancel("Iceberg runtime does not expose v3 field defaults")
    }
    val fixedType = Types.FixedType.ofLength(2)
    val fixedField = fieldWithDefaults(
      6,
      "fixed_value",
      fixedType,
      required = false,
      initialDefault = Some(ByteBuffer.wrap(Array[Byte](1, 2)))).getOrElse {
      cancel("Iceberg runtime does not expose v3 field defaults")
    }

    val unsupportedSchema = new Schema(Types.NestedField.optional(
      1, "payload", Types.StructType.of(timestampNtzField)))
    val unsupportedListSchema = new Schema(Types.NestedField.optional(
      1, "events", Types.ListType.ofOptional(
        2, Types.StructType.of(timestampNtzField))))
    val unsupportedMapKeySchema = new Schema(Types.NestedField.optional(
      1, "lookup", Types.MapType.ofOptional(
        2, 4, Types.StructType.of(timestampNtzField), Types.StringType.get())))
    val unsupportedMapValueSchema = new Schema(Types.NestedField.optional(
      1, "lookup", Types.MapType.ofOptional(
        2, 4, Types.StringType.get(), Types.StructType.of(timestampNtzField))))
    val supportedSchema = new Schema(timestampField)

    assert(IcebergFormatVersionSupport.unsupportedDefault(unsupportedSchema)
      .contains("payload.created_at" -> "timestamp"))
    assert(IcebergFormatVersionSupport.unsupportedDefault(unsupportedListSchema)
      .contains("events.element.created_at" -> "timestamp"))
    assert(IcebergFormatVersionSupport.unsupportedDefault(unsupportedMapKeySchema)
      .contains("lookup.key.created_at" -> "timestamp"))
    assert(IcebergFormatVersionSupport.unsupportedDefault(unsupportedMapValueSchema)
      .contains("lookup.value.created_at" -> "timestamp"))
    assert(IcebergFormatVersionSupport.unsupportedDefault(supportedSchema).isEmpty)
    assert(IcebergFormatVersionSupport.unsupportedDefault(new Schema(uuidField))
      .contains("uuid_value" -> Types.UUIDType.get().toString))
    assert(IcebergFormatVersionSupport.unsupportedDefault(new Schema(fixedField))
      .contains("fixed_value" -> fixedType.toString))
  }
}
