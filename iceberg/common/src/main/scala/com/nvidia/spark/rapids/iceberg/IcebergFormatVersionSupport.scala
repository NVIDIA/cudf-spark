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

package com.nvidia.spark.rapids.iceberg

import java.util.EnumSet

import scala.collection.JavaConverters._

import com.nvidia.spark.rapids.{RapidsConf, RapidsMeta}
import org.apache.iceberg.{Schema, Table, TableProperties}
import org.apache.iceberg.types.{Type, Types}

import org.apache.spark.sql.execution.SparkPlan

/** Common planning gate for Iceberg table format versions. */
object IcebergFormatVersionSupport {
  /** Maximum table format version supported without enabling v3 support. */
  val MaxSupportedFormatVersion = 2
  private val SupportedDefaultTypeIds = EnumSet.of(
    Type.TypeID.BOOLEAN, Type.TypeID.INTEGER, Type.TypeID.LONG, Type.TypeID.FLOAT,
    Type.TypeID.DOUBLE, Type.TypeID.DATE, Type.TypeID.TIMESTAMP, Type.TypeID.STRING,
    Type.TypeID.BINARY, Type.TypeID.DECIMAL)

  /** Check the table's initial defaults before reading files written under an older schema. */
  def tagForScan(table: Table, meta: RapidsMeta[_, _, _]): Unit = {
    tagForDefaults(table, meta, "default")(unsupportedDefault(table.schema()))
  }

  /** Check write defaults for the data or position-delete schema used by the writer. */
  def tagForWriteSchema(
      table: Table,
      writeSchema: Schema,
      meta: RapidsMeta[_, _, _]): Unit = {
    tagForDefaults(table, meta, "write default")(unsupportedWriteDefault(writeSchema))
  }

  private def tagForDefaults(
      table: Table,
      meta: RapidsMeta[_, _, _],
      description: String)(
      unsupported: => Option[(String, String)]): Unit = {
    val formatVersion = ShimUtils.formatVersion(table)
    tagForFormatVersion(formatVersion, meta)
    if (formatVersion > MaxSupportedFormatVersion && meta.conf.isIcebergV3Enabled) {
      unsupported.foreach { case (path, typeName) =>
        meta.willNotWorkOnGpu(
          s"Iceberg $description for field '$path' with type $typeName is not supported on GPU")
      }
    }
  }

  /** Tag unsupported format versions from table properties, defaulting to v2 when absent. */
  def tagForFormatVersion(
      properties: Map[String, String],
      meta: RapidsMeta[_, _, _]): Unit = {
    val formatVersion = properties.get(TableProperties.FORMAT_VERSION).map(_.toInt).getOrElse(2)
    tagForFormatVersion(formatVersion, meta)
  }

  private def tagForFormatVersion(formatVersion: Int, meta: RapidsMeta[_, _, _]): Unit = {
    if (formatVersion > MaxSupportedFormatVersion && !meta.conf.isIcebergV3Enabled) {
      val reason = s"Iceberg table format version $formatVersion is not supported. " +
        s"To enable set ${RapidsConf.ENABLE_ICEBERG_V3} to true"
      meta.willNotWorkOnGpu(reason)

      // A scan is tagged before its enclosing SparkPlan nodes. Keep MergeRowsExec on CPU with a
      // v3 target because v3 row-lineage metadata changes the row schemas it processes. This is
      // especially important with AQE, where the CPU V2 write and its query can be planned in
      // separate passes.
      tagMergeRowsAncestor(meta.parent, reason)
    }
  }

  private[iceberg] def unsupportedDefault(schema: Schema): Option[(String, String)] = {
    unsupportedDefault(schema, field => ShimUtils.hasInitialDefault(field))
  }

  private[iceberg] def unsupportedWriteDefault(schema: Schema): Option[(String, String)] = {
    unsupportedDefault(schema, field => ShimUtils.hasWriteDefault(field))
  }

  private def unsupportedDefault(
      schema: Schema,
      hasDefault: Types.NestedField => Boolean): Option[(String, String)] = {
    schema.idToName().keySet().asScala.iterator
      .map(id => schema.findField(id.intValue()))
      .find { field =>
        val fieldType = field.`type`()
        hasDefault(field) && {
          if (fieldType.isPrimitiveType) {
            !SupportedDefaultTypeIds.contains(fieldType.typeId()) ||
              fieldType == Types.TimestampType.withoutZone()
          } else {
            !fieldType.isStructType
          }
        }
      }.map { field =>
        schema.findColumnName(field.fieldId()) -> field.`type`().toString
      }
  }

  private def tagMergeRowsAncestor(
      meta: Option[RapidsMeta[_, _, _]],
      reason: String): Unit = meta match {
    case Some(current) =>
      current.wrapped match {
        case plan: SparkPlan if plan.getClass.getSimpleName == "MergeRowsExec" =>
          plan.setTagValue(RapidsMeta.gpuSupportedTag,
            plan.getTagValue(RapidsMeta.gpuSupportedTag).getOrElse(Set.empty) + reason)
        case _ => tagMergeRowsAncestor(current.parent, reason)
      }
    case None =>
  }
}
