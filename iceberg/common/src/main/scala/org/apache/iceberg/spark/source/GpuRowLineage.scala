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

package org.apache.iceberg.spark.source

import ai.rapids.cudf.{ColumnVector => CudfColumnVector, DType, Scalar}
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.GpuColumnVector
import org.apache.iceberg.Schema

import org.apache.spark.sql.types.{LongType, StructType}
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}

/**
 * GPU helper mirroring Iceberg's ExtractRowLineage / decorateWithRowLineage.
 *
 * When the write schema requires row lineage columns, appends `_row_id` and
 * `_last_updated_sequence_number` from the metadata batch (or nulls when metadata
 * is absent) onto the data batch.
 *
 * Field names are hard-coded (not [[org.apache.iceberg.MetadataColumns]]) so this
 * compiles against Iceberg 1.6.x, which does not yet expose those constants.
 */
object GpuRowLineage {
  // Keep in sync with Iceberg MetadataColumns.ROW_ID / LAST_UPDATED_SEQUENCE_NUMBER.
  private val rowIdName: String = "_row_id"
  private val lastUpdatedName: String = "_last_updated_sequence_number"

  def isRequired(writeSchema: Schema): Boolean = {
    writeSchema.findField(rowIdName) != null
  }

  /**
   * Decorate `data` with lineage columns when required by `writeSchema`.
   *
   * Takes ownership of `data`. Does not take ownership of `meta`.
   *
   * @return a batch owned by the caller
   */
  def decorate(
      writeSchema: Schema,
      meta: ColumnarBatch,
      metaSchema: StructType,
      data: ColumnarBatch): ColumnarBatch = {
    if (!isRequired(writeSchema)) {
      data
    } else {
      closeOnExcept(data) { _ =>
        val numRows = data.numRows()
        val numDataCols = data.numCols()
        closeOnExcept(new Array[ColumnVector](numDataCols + 2)) { cols =>
          var i = 0
          while (i < numDataCols) {
            cols(i) = data.column(i).asInstanceOf[GpuColumnVector].incRefCount()
            i += 1
          }
          cols(numDataCols) = lineageColumn(meta, metaSchema, rowIdName, numRows)
          cols(numDataCols + 1) = lineageColumn(meta, metaSchema, lastUpdatedName, numRows)
          val result = new ColumnarBatch(cols, numRows)
          data.close()
          result
        }
      }
    }
  }

  private def lineageColumn(
      meta: ColumnarBatch,
      metaSchema: StructType,
      fieldName: String,
      numRows: Int): ColumnVector = {
    if (meta == null) {
      nullLongColumn(numRows)
    } else {
      require(metaSchema != null && metaSchema.fieldNames.contains(fieldName),
        s"Metadata schema must contain $fieldName for row-lineage writes, got: $metaSchema")
      val ordinal = metaSchema.fieldIndex(fieldName)
      meta.column(ordinal).asInstanceOf[GpuColumnVector].incRefCount()
    }
  }

  private def nullLongColumn(numRows: Int): ColumnVector = {
    withResource(Scalar.fromNull(DType.INT64)) { nullScalar =>
      GpuColumnVector.from(CudfColumnVector.fromScalar(nullScalar, numRows), LongType)
    }
  }
}
