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
{"spark": "350"}
{"spark": "351"}
{"spark": "352"}
{"spark": "353"}
{"spark": "354"}
{"spark": "355"}
{"spark": "356"}
{"spark": "357"}
{"spark": "358"}
spark-rapids-shim-json-lines ***/
package org.apache.iceberg.spark

import com.nvidia.spark.rapids.SchemaUtils._
import org.apache.iceberg.Schema
import org.apache.iceberg.types.Types
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.sql.execution.datasources.parquet.ParquetUtils.FIELD_ID_METADATA_KEY
import org.apache.spark.sql.types.{ArrayType, IntegerType, MapType, Metadata, StructType}

class GpuTypeToSparkTypeSuite extends AnyFunSuite {

  private def fieldOf(field: Types.NestedField): Metadata = {
    val schema = new Schema(field)
    GpuTypeToSparkType.toSparkType(schema)(field.name()).metadata
  }

  test("toSparkType records no nested ids for a primitive top-level field") {
    val md = fieldOf(Types.NestedField.required(1, "id", Types.IntegerType.get()))
    assert(md.getLong(FIELD_ID_METADATA_KEY) == 1L)
    assert(!md.contains(LIST_ELEMENT_FIELD_ID_METADATA_KEY))
    assert(!md.contains(LIST_ELEMENT_NESTED_IDS_METADATA_KEY))
  }

  test("toSparkType: flat list records only the element id") {
    val md = fieldOf(Types.NestedField.optional(10, "tags",
      Types.ListType.ofOptional(11, Types.IntegerType.get())))
    assert(md.getLong(FIELD_ID_METADATA_KEY) == 10L)
    assert(md.getLong(LIST_ELEMENT_FIELD_ID_METADATA_KEY) == 11L)
    assert(!md.contains(LIST_ELEMENT_NESTED_IDS_METADATA_KEY))
  }

  test("toSparkType: flat map records only the key/value ids") {
    val md = fieldOf(Types.NestedField.optional(20, "props",
      Types.MapType.ofOptional(21, 22,
        Types.StringType.get(), Types.IntegerType.get())))
    assert(md.getLong(FIELD_ID_METADATA_KEY) == 20L)
    assert(md.getLong(MAP_KEY_FIELD_ID_METADATA_KEY) == 21L)
    assert(md.getLong(MAP_VALUE_FIELD_ID_METADATA_KEY) == 22L)
    assert(!md.contains(MAP_KEY_NESTED_IDS_METADATA_KEY))
    assert(!md.contains(MAP_VALUE_NESTED_IDS_METADATA_KEY))
  }

  test("toSparkType: list-of-list nests the inner element id under NESTED_IDS") {
    val inner = Types.ListType.ofOptional(31, Types.IntegerType.get())
    val md = fieldOf(Types.NestedField.optional(30, "nested_lists",
      Types.ListType.ofOptional(32, inner)))

    assert(md.getLong(FIELD_ID_METADATA_KEY) == 30L)
    assert(md.getLong(LIST_ELEMENT_FIELD_ID_METADATA_KEY) == 32L)
    val innerMd = Metadata.fromJson(md.getString(LIST_ELEMENT_NESTED_IDS_METADATA_KEY))
    assert(innerMd.getLong(LIST_ELEMENT_FIELD_ID_METADATA_KEY) == 31L)
    assert(!innerMd.contains(LIST_ELEMENT_NESTED_IDS_METADATA_KEY))
  }

  test("toSparkType: map-of-(list, list) nests both key and value element ids") {
    val keyList = Types.ListType.ofOptional(41, Types.StringType.get())
    val valueList = Types.ListType.ofOptional(42, Types.IntegerType.get())
    val md = fieldOf(Types.NestedField.optional(40, "m",
      Types.MapType.ofOptional(43, 44, keyList, valueList)))

    assert(md.getLong(MAP_KEY_FIELD_ID_METADATA_KEY) == 43L)
    assert(md.getLong(MAP_VALUE_FIELD_ID_METADATA_KEY) == 44L)

    val keyMd = Metadata.fromJson(md.getString(MAP_KEY_NESTED_IDS_METADATA_KEY))
    assert(keyMd.getLong(LIST_ELEMENT_FIELD_ID_METADATA_KEY) == 41L)

    val valueMd = Metadata.fromJson(md.getString(MAP_VALUE_NESTED_IDS_METADATA_KEY))
    assert(valueMd.getLong(LIST_ELEMENT_FIELD_ID_METADATA_KEY) == 42L)
  }

  test("toSparkType: list-of-list-of-struct keeps the struct's child ids on inner StructFields") {
    // Ray's `List<List<Struct<a, b>>>` case: the outer field's nested-ids JSON only carries
    // list element ids; the struct's child ids (`aId`, `bId`) live on the inner StructFields
    // built by the visitor, where SchemaUtils reads them from the StructType directly.
    val innerStruct = Types.StructType.of(
      Types.NestedField.required(53, "a", Types.IntegerType.get()),
      Types.NestedField.optional(54, "b", Types.StringType.get()))
    val innerList = Types.ListType.ofOptional(52, innerStruct)
    val outerField = Types.NestedField.optional(50, "lol",
      Types.ListType.ofOptional(51, innerList))

    val outerMd = fieldOf(outerField)
    assert(outerMd.getLong(FIELD_ID_METADATA_KEY) == 50L)
    assert(outerMd.getLong(LIST_ELEMENT_FIELD_ID_METADATA_KEY) == 51L)
    val innerListMd =
      Metadata.fromJson(outerMd.getString(LIST_ELEMENT_NESTED_IDS_METADATA_KEY))
    assert(innerListMd.getLong(LIST_ELEMENT_FIELD_ID_METADATA_KEY) == 52L)
    // Struct's child ids are *not* serialized into the parent's nested-ids JSON.
    assert(!innerListMd.contains(LIST_ELEMENT_NESTED_IDS_METADATA_KEY))

    // Instead, they are attached to the inner StructFields:
    val sparkSchema = GpuTypeToSparkType.toSparkType(new Schema(outerField))
    val outerArray = sparkSchema("lol").dataType.asInstanceOf[ArrayType]
    val innerArray = outerArray.elementType.asInstanceOf[ArrayType]
    val struct = innerArray.elementType.asInstanceOf[StructType]
    assert(struct("a").metadata.getLong(FIELD_ID_METADATA_KEY) == 53L)
    assert(struct("b").metadata.getLong(FIELD_ID_METADATA_KEY) == 54L)
  }

  test("toSparkType: list-element-struct with a nested list keeps ids on the inner StructField") {
    // `List<Struct<x: int, ys: List<long>>>`: the struct's `ys` field needs LIST_ELEMENT
    // metadata on its own StructField for SchemaUtils to wire its element id.
    val innerStruct = Types.StructType.of(
      Types.NestedField.required(63, "x", Types.IntegerType.get()),
      Types.NestedField.optional(64, "ys",
        Types.ListType.ofOptional(65, Types.LongType.get())))
    val outerField = Types.NestedField.optional(60, "los",
      Types.ListType.ofOptional(62, innerStruct))

    val sparkSchema = GpuTypeToSparkType.toSparkType(new Schema(outerField))
    val outerArray = sparkSchema("los").dataType.asInstanceOf[ArrayType]
    val struct = outerArray.elementType.asInstanceOf[StructType]
    val ys = struct("ys")
    assert(ys.metadata.getLong(FIELD_ID_METADATA_KEY) == 64L)
    assert(ys.metadata.getLong(LIST_ELEMENT_FIELD_ID_METADATA_KEY) == 65L)
  }

  test("toSparkType attaches nested field-id metadata to the parent StructField") {
    val schema = new Schema(
      Types.NestedField.required(1, "id", Types.IntegerType.get()),
      Types.NestedField.optional(2, "tags",
        Types.ListType.ofOptional(3, Types.StringType.get())),
      Types.NestedField.optional(4, "props",
        Types.MapType.ofOptional(5, 6, Types.StringType.get(), Types.IntegerType.get())))

    val sparkSchema = GpuTypeToSparkType.toSparkType(schema)

    val idField = sparkSchema("id")
    assert(idField.dataType == IntegerType)
    assert(idField.metadata.getLong(FIELD_ID_METADATA_KEY) == 1L)

    val tagsField = sparkSchema("tags")
    assert(tagsField.dataType.isInstanceOf[ArrayType])
    assert(tagsField.metadata.getLong(FIELD_ID_METADATA_KEY) == 2L)
    assert(tagsField.metadata.getLong(LIST_ELEMENT_FIELD_ID_METADATA_KEY) == 3L)
    assert(!tagsField.metadata.contains(LIST_ELEMENT_NESTED_IDS_METADATA_KEY))

    val propsField = sparkSchema("props")
    assert(propsField.dataType.isInstanceOf[MapType])
    assert(propsField.metadata.getLong(FIELD_ID_METADATA_KEY) == 4L)
    assert(propsField.metadata.getLong(MAP_KEY_FIELD_ID_METADATA_KEY) == 5L)
    assert(propsField.metadata.getLong(MAP_VALUE_FIELD_ID_METADATA_KEY) == 6L)
  }

  test("toSparkType handles a struct of struct preserving each child's id") {
    val innerStruct = Types.StructType.of(
      Types.NestedField.required(101, "inner_a", Types.IntegerType.get()),
      Types.NestedField.optional(102, "inner_b", Types.StringType.get()))
    val schema = new Schema(
      Types.NestedField.optional(100, "outer", innerStruct))

    val sparkSchema = GpuTypeToSparkType.toSparkType(schema)
    val outer = sparkSchema("outer")
    assert(outer.metadata.getLong(FIELD_ID_METADATA_KEY) == 100L)

    val outerStruct = outer.dataType.asInstanceOf[StructType]
    assert(outerStruct("inner_a").metadata.getLong(FIELD_ID_METADATA_KEY) == 101L)
    assert(outerStruct("inner_b").metadata.getLong(FIELD_ID_METADATA_KEY) == 102L)
  }

}
