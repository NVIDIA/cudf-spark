/*
 * Copyright (c) 2025-2026, NVIDIA CORPORATION.
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

package org.apache.iceberg.spark

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.iceberg.{MetadataColumns, Schema}
import org.apache.iceberg.types.{Type, Types, TypeUtil}

import org.apache.spark.sql.catalyst.util.METADATA_COL_ATTR_KEY
import org.apache.spark.sql.execution.datasources.parquet.ParquetUtils.FIELD_ID_METADATA_KEY
import org.apache.spark.sql.types._

object GpuTypeToSparkType {
  private[iceberg] val LIST_ELEMENT_FIELD_ID_METADATA_KEY =
    "rapids.parquet.list.element.field.id"
  private[iceberg] val LIST_ELEMENT_NESTED_IDS_METADATA_KEY =
    "rapids.parquet.list.element.nested.ids"
  private[iceberg] val MAP_KEY_FIELD_ID_METADATA_KEY =
    "rapids.parquet.map.key.field.id"
  private[iceberg] val MAP_KEY_NESTED_IDS_METADATA_KEY =
    "rapids.parquet.map.key.nested.ids"
  private[iceberg] val MAP_VALUE_FIELD_ID_METADATA_KEY =
    "rapids.parquet.map.value.field.id"
  private[iceberg] val MAP_VALUE_NESTED_IDS_METADATA_KEY =
    "rapids.parquet.map.value.nested.ids"

  def toSparkType(schema: Schema): StructType = {
    TypeUtil.visit(schema, new GpuTypeToSparkType).asInstanceOf[StructType]
  }

  def toSparkType(icebergStruct: Types.StructType): StructType = {
    TypeUtil.visit(icebergStruct, new GpuTypeToSparkType).asInstanceOf[StructType]
  }

  private[iceberg] def fieldMetadataOf(fieldId: Int): Metadata = {
    val builder = new MetadataBuilder()
    .putLong(FIELD_ID_METADATA_KEY, fieldId)

    if (MetadataColumns.metadataFieldIds().contains(fieldId)) {
      builder.putBoolean(METADATA_COL_ATTR_KEY, true)
    }

    builder.build()
  }
}

/**
 * Iceberg-to-Spark schema visitor. Builds the Spark `StructType` and, using the visitor's
 * natural post-order traversal, threads each top-level `StructField`'s nested-id metadata
 * via a per-subtree stack:
 *
 *  - `primitive` and `struct` push `None` — primitives have no children, and a struct
 *    attaches each child's id to the inner `StructField`, so neither contributes nested ids
 *    to an enclosing list/map.
 *  - `list` / `map` consume the child entries on the stack, build a JSON-serialized
 *    `Metadata` containing the element / key / value ids (plus any popped nested-ids JSON
 *    under `_NESTED_IDS_METADATA_KEY`), and push that JSON.
 *  - `struct` pops one entry per field and merges the popped JSON into each
 *    `StructField`'s metadata alongside the field id.
 *
 * Keeping the nested-ids JSON only on top-level `StructField`s matches what
 * `SchemaUtils.writerOptionsFromField` reads back when wiring cuDF `ColumnWriterOptions`,
 * and naturally handles list/map elements that are themselves structs without a
 * special-case recursion through `Types.StructType`.
 */
class GpuTypeToSparkType extends TypeToSparkType {
  private val nestedIdsStack = mutable.ArrayBuffer.empty[Option[String]]

  private def pushNested(nested: Option[String]): Unit = nestedIdsStack += nested

  private def popNested(): Option[String] =
    nestedIdsStack.remove(nestedIdsStack.length - 1)

  private def jsonOrNone(builder: MetadataBuilder): Option[String] = {
    val json = builder.build().json
    if (json == "{}") None else Some(json)
  }

  override def primitive(primitive: Type.PrimitiveType): DataType = {
    pushNested(None)
    super.primitive(primitive)
  }

  override def list(list: Types.ListType, elementResult: DataType): DataType = {
    val elementNested = popNested()
    val builder = new MetadataBuilder()
      .putLong(GpuTypeToSparkType.LIST_ELEMENT_FIELD_ID_METADATA_KEY, list.elementId().toLong)
    elementNested.foreach(
      builder.putString(GpuTypeToSparkType.LIST_ELEMENT_NESTED_IDS_METADATA_KEY, _))
    pushNested(jsonOrNone(builder))
    super.list(list, elementResult)
  }

  override def map(map: Types.MapType,
                   keyResult: DataType,
                   valueResult: DataType): DataType = {
    // Visitor order is key first then value; pop matches that in reverse.
    val valueNested = popNested()
    val keyNested = popNested()
    val builder = new MetadataBuilder()
      .putLong(GpuTypeToSparkType.MAP_KEY_FIELD_ID_METADATA_KEY, map.keyId().toLong)
    keyNested.foreach(builder.putString(GpuTypeToSparkType.MAP_KEY_NESTED_IDS_METADATA_KEY, _))
    builder.putLong(GpuTypeToSparkType.MAP_VALUE_FIELD_ID_METADATA_KEY, map.valueId().toLong)
    valueNested.foreach(builder.putString(GpuTypeToSparkType.MAP_VALUE_NESTED_IDS_METADATA_KEY, _))
    pushNested(jsonOrNone(builder))
    super.map(map, keyResult, valueResult)
  }

  override def struct(struct: Types.StructType,
                      fieldResults: java.util.List[DataType]): DataType = {
    // Pop one entry per field; the stack is LIFO so reverse to align with field order.
    val fieldNested = Array.fill(fieldResults.size())(popNested()).reverse

    val sparkFields = struct.fields().asScala
      .zip(fieldResults.asScala)
      .zip(fieldNested)
      .map {
        case ((field, fieldResult), nestedJson) =>
          val metadataBuilder = new MetadataBuilder()
            .withMetadata(GpuTypeToSparkType.fieldMetadataOf(field.fieldId()))
          nestedJson.foreach(json =>
            metadataBuilder.withMetadata(Metadata.fromJson(json)))
          var sparkField =
            StructField(field.name(), fieldResult, field.isOptional, metadataBuilder.build())
          if (field.doc() != null) {
            sparkField = sparkField.withComment(field.doc())
          }
          sparkField
      }

    // A struct attaches each child's id to its inner StructField, so it contributes nothing
    // to an enclosing list/map's nested-ids metadata.
    pushNested(None)

    StructType(sparkFields.toSeq)
  }
}
