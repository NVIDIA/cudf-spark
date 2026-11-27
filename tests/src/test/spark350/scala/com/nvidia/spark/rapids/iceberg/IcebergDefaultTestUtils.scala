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

import com.nvidia.spark.rapids.parquet.ParquetFileInfoWithBlockMeta
import org.apache.hadoop.fs.Path
import org.apache.iceberg.shaded.org.apache.parquet.schema.{MessageType => ShadedMessageType}
import org.apache.iceberg.types.Types
import org.apache.parquet.hadoop.metadata.BlockMetaData

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType

/** Schema fixtures shared by the default-value unit suites. */
object IcebergDefaultTestUtils {
  def fieldWithDefaults(
      id: Int,
      name: String,
      icebergType: org.apache.iceberg.types.Type,
      required: Boolean,
      initialDefault: Option[AnyRef],
      writeDefault: Option[AnyRef] = None): Option[Types.NestedField] = {
    try {
      val factory = classOf[Types.NestedField].getMethod(
        if (required) "required" else "optional", classOf[String])
      val builder = factory.invoke(null, name)
      val builderClass = builder.getClass
      builderClass.getMethod("withId", java.lang.Integer.TYPE)
        .invoke(builder, Int.box(id))
      builderClass.getMethod("ofType", classOf[org.apache.iceberg.types.Type])
        .invoke(builder, icebergType)
      initialDefault.foreach { value =>
        builderClass.getMethod("withInitialDefault", classOf[Object]).invoke(builder, value)
      }
      writeDefault.foreach { value =>
        builderClass.getMethod("withWriteDefault", classOf[Object]).invoke(builder, value)
      }
      Some(builderClass.getMethod("build").invoke(builder).asInstanceOf[Types.NestedField])
    } catch {
      case _: NoSuchMethodException => None
    }
  }

  def fieldWithWriteDefault(
      id: Int,
      name: String,
      icebergType: org.apache.iceberg.types.Type,
      writeDefault: AnyRef,
      initialDefault: Option[AnyRef] = None): Types.NestedField = {
    fieldWithDefaults(id, name, icebergType, required = false, initialDefault = initialDefault,
      writeDefault = Some(writeDefault)).getOrElse {
      org.scalatest.Assertions.cancel("Iceberg runtime does not expose v3 field defaults")
    }
  }

  private def createBlockMetaData(rowCount: Long): BlockMetaData = {
    val block = new BlockMetaData()
    block.setRowCount(rowCount)
    block
  }

  def createParquetInfo(
      shadedSchema: ShadedMessageType,
      rowCount: Long = 10): (ParquetFileInfoWithBlockMeta, ShadedMessageType) = {
    val block = createBlockMetaData(rowCount)
    val info = ParquetFileInfoWithBlockMeta(
      filePath = new Path("/test/file.parquet"),
      blocks = Seq(block),
      partValues = InternalRow.empty,
      schema = ShimUtils.getParquetUnshader().unshade(shadedSchema),
      readSchema = StructType(Seq.empty),
      dateRebaseMode = null,
      timestampRebaseMode = null,
      hasInt96Timestamps = false,
      blocksFirstRowIndices = Seq(0L)
    )
    (info, shadedSchema)
  }
}
