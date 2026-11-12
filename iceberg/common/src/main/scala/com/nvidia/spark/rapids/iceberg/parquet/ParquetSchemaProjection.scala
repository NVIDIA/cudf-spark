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

import java.util.Collections

import scala.collection.JavaConverters._

import org.apache.iceberg.Schema
import org.apache.iceberg.parquet.ParquetSchemaUtil
import org.apache.iceberg.shaded.org.apache.parquet.schema.{GroupType, MessageType, Type}
import org.apache.iceberg.shaded.org.apache.parquet.schema.LogicalTypeAnnotation.{ListLogicalTypeAnnotation, MapLogicalTypeAnnotation}

/** Restores physical container paths and validity leaves dropped by Iceberg projection. */
private[iceberg] object ParquetSchemaProjection {
  def preserveStructValidity(
      fileSchema: MessageType,
      projectedSchema: MessageType,
      requiredSchema: Schema): MessageType = {
    val projection = new Projection(requiredSchema)
    new MessageType(projectedSchema.getName,
      projection.projectChildren(fileSchema, projectedSchema).asJava)
  }

  /** Matches the projection to physical fields before choosing any validity helpers. */
  private class Projection(requiredSchema: Schema) {
    def projectChildren(fileGroup: GroupType, projectedGroup: GroupType): Seq[Type] = {
      val projectedByName = projectedGroup.getFields.asScala.map(f => f.getName -> f).toMap
      fileGroup.getFields.asScala.flatMap { field =>
        projectedByName.get(field.getName) match {
          case Some(projectedField) => Some(projectField(field, projectedField))
          case None if isRequested(field) =>
            // Restore every requested path before choosing helper leaves. One leaf under an
            // ancestor cannot preserve a sibling collection's validity or element counts.
            val emptyProjection = if (field.isPrimitive) {
              field
            } else {
              field.asGroupType().withNewFields(Collections.emptyList[Type]())
            }
            Some(projectField(field, emptyProjection))
          case _ => None
        }
      }.toSeq
    }

    private def projectField(fileField: Type, projectedField: Type): Type = {
      if (projectedField.isPrimitive) {
        projectedField
      } else {
        val fileGroup = fileField.asGroupType()
        val projectedGroup = projectedField.asGroupType()
        val children = projectChildren(fileGroup, projectedGroup)
        if (children.isEmpty) {
          // ParquetSchemaUtil.convert drops empty structs. A retained physical leaf carries
          // definition levels for the parent even when the leaf itself is null. The
          // post-processor removes this helper field while applying the projected defaults.
          ContainerProjection.firstLeaf(fileField)
        } else {
          ContainerProjection.withChildren(fileGroup, projectedGroup, children)
        }
      }
    }

    private def isRequested(field: Type): Boolean = {
      if (field.getId != null) {
        requiredSchema.findField(field.getId.intValue()) != null
      } else {
        // Parquet's repeated list and map wrapper groups do not have Iceberg field IDs.
        !field.isPrimitive && field.asGroupType().getFields.asScala.exists(isRequested)
      }
    }
  }

  /** Retains helper leaves without changing the physical LIST/MAP container interpretation. */
  private object ContainerProjection {
    def firstLeaf(field: Type, keepAllChildren: Boolean = false): Type = {
      if (field.isPrimitive) {
        field
      } else {
        val group = field.asGroupType()
        val children = group.getFields.asScala
        // A map's repeated key/value group needs both children to remain a valid map schema.
        val isMap = group.getLogicalTypeAnnotation.isInstanceOf[MapLogicalTypeAnnotation]
        val retained = if (keepAllChildren) children else children.take(1)
        withChildren(group, group, retained.map(child => firstLeaf(child, isMap)).toSeq)
      }
    }

    def withChildren(
        fileGroup: GroupType,
        projectedGroup: GroupType,
        children: Seq[Type]): GroupType = {
      val result = projectedGroup.withNewFields(children.asJava)
      if (fileGroup.getLogicalTypeAnnotation.isInstanceOf[ListLogicalTypeAnnotation]) {
        preserveListElement(fileGroup, result)
      } else {
        result
      }
    }

    private def preserveListElement(fileList: GroupType, projectedList: GroupType): GroupType = {
      if (!fileList.getType(0).isPrimitive &&
          (ParquetSchemaUtil.determineListElementType(fileList) eq fileList.getType(0)) &&
          !(ParquetSchemaUtil.determineListElementType(projectedList) eq
            projectedList.getType(0))) {
        // A legacy two-level LIST can identify its repeated struct by its multiple fields.
        // Keeping only one field would reinterpret that struct as a three-level list wrapper.
        val fileElement = fileList.getType(0).asGroupType()
        val element = projectedList.getType(0).asGroupType()
        val retained = element.getFields.asScala.map(f => f.getName -> f).toMap
        val helper = fileElement.getFields.asScala.find(f => !retained.contains(f.getName))
          .map(firstLeaf(_))
        val fields = fileElement.getFields.asScala.flatMap { field =>
          retained.get(field.getName).orElse(helper.filter(_.getName == field.getName))
        }
        projectedList.withNewFields(Collections.singletonList[Type](
          element.withNewFields(fields.asJava)))
      } else {
        projectedList
      }
    }
  }
}
