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

package com.nvidia.spark.rapids.iceberg.parquet

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.iceberg.shaded.org.apache.parquet.schema.{GroupType, PrimitiveType, Type}

/**
 * Visits every physical field, including LIST/MAP wrappers and children of opaque logical groups.
 *
 * Iceberg's ParquetTypeVisitor and TypeWithSchemaVisitor visit logical LIST elements and MAP
 * keys/values rather than producing a result for each physical wrapper. ParquetTypeVisitor in
 * Iceberg 1.10/1.11 also dispatches VARIANT to a callback without visiting its physical children;
 * that callback is absent in 1.6/1.9. TypeWithSchemaVisitor has a separate ParquetVariantVisitor
 * path starting in 1.9, selected by the companion Iceberg type in 1.9 and also by the Parquet
 * annotation in 1.10/1.11. Adapting these visitors would require reconstructing wrapper results
 * and handling the version-specific VARIANT traversal.
 *
 * Helper rewriting and field-ID collection instead require the same physical walk on Iceberg
 * 1.6, 1.9, 1.10 and 1.11: descend into every group, visit fields after their children, and retain
 * each field's physical parent. This lets helper rewriting preserve container layout while
 * removing unsupported helper annotations, without linking to version-specific VARIANT APIs.
 */
private[parquet] abstract class PhysicalParquetSchemaVisitor[T] {
  def primitive(primitive: PrimitiveType): T

  def group(group: GroupType, children: Seq[T]): T

  /** Completes a field after its children, retaining its physical parent for wrapper handling. */
  def field(field: Type, parent: GroupType, result: T): T = result
}

private[parquet] object PhysicalParquetSchemaVisitor {
  def visitFields[T](group: GroupType, visitor: PhysicalParquetSchemaVisitor[T]): Seq[T] = {
    group.getFields.asScala.map { field =>
      val result = if (field.isPrimitive) {
        visitor.primitive(field.asPrimitiveType())
      } else {
        val childGroup = field.asGroupType()
        visitor.group(childGroup, visitFields(childGroup, visitor))
      }
      visitor.field(field, group, result)
    }.toSeq
  }

  def fieldIds(schema: GroupType): Set[Int] = {
    val ids = mutable.HashSet.empty[Int]
    visitFields(schema, new PhysicalParquetSchemaVisitor[Unit] {
      override def primitive(primitive: PrimitiveType): Unit = ()

      override def group(group: GroupType, children: Seq[Unit]): Unit = ()

      override def field(field: Type, parent: GroupType, result: Unit): Unit = {
        Option(field.getId).foreach(id => ids += id.intValue())
      }
    })
    ids.toSet
  }
}
