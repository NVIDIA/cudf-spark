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
{"spark": "400"}
{"spark": "401"}
{"spark": "402"}
{"spark": "403"}
{"spark": "404"}
{"spark": "411"}
{"spark": "412"}
{"spark": "413"}
{"spark": "420"}
spark-rapids-shim-json-lines ***/

package com.nvidia.spark.rapids.shims

import ai.rapids.cudf.{ColumnVector => CudfColumnVector, Scalar => CudfScalar}
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.GpuColumnVector

import org.apache.spark.sql.catalyst.GpuProjectingColumnarBatch
import org.apache.spark.sql.catalyst.util.RowDeltaUtils.{INSERT_OPERATION, REINSERT_OPERATION}
import org.apache.spark.sql.connector.write.DeltaWriter
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.vectorized.ColumnarBatch

object DeltaInsertFilter {
  def filterInsertRows(batch: ColumnarBatch): CudfColumnVector = {
    withResource(CudfScalar.fromInt(INSERT_OPERATION)) { s =>
      batch.column(0).asInstanceOf[GpuColumnVector].getBase.equalTo(s)
    }
  }

  def filterReinsertRows(batch: ColumnarBatch): CudfColumnVector = {
    withResource(CudfScalar.fromInt(REINSERT_OPERATION)) { s =>
      batch.column(0).asInstanceOf[GpuColumnVector].getBase.equalTo(s)
    }
  }

  /**
   * Write INSERT_OPERATION rows via [[DeltaWriter.insert]].
   * REINSERT_OPERATION rows are handled separately by [[writeReinserts]].
   */
  def writeInserts(
      writer: DeltaWriter[ColumnarBatch],
      batch: ColumnarBatch,
      rowProjection: GpuProjectingColumnarBatch,
      rowDataTypes: Array[DataType]): Unit = {
    val insertFilter = filterInsertRows(batch)
    withResource(insertFilter) { _ =>
      withResource(rowProjection.project(batch)) { rows =>
        val filteredRows = GpuColumnVector.filter(rows, rowDataTypes, insertFilter)
        if (filteredRows.numRows() > 0) {
          writer.insert(filteredRows)
        } else {
          filteredRows.close()
        }
      }
    }
  }

  /**
   * Write REINSERT_OPERATION rows via [[DeltaWriter.reinsert]], matching Spark's
   * DeltaWritingSparkTask / DeltaWithMetadataWritingSparkTask (SPARK-50820).
   *
   * @param metadataProjection metadata projection, or null when metadata is unavailable
   * @param metadataDataTypes metadata column types when metadataProjection is non-null
   */
  def writeReinserts(
      writer: DeltaWriter[ColumnarBatch],
      batch: ColumnarBatch,
      rowProjection: GpuProjectingColumnarBatch,
      rowDataTypes: Array[DataType],
      metadataProjection: GpuProjectingColumnarBatch,
      metadataDataTypes: Array[DataType]): Unit = {
    val reinsertFilter = filterReinsertRows(batch)
    withResource(reinsertFilter) { _ =>
      withResource(rowProjection.project(batch)) { rows =>
        val filteredRows = GpuColumnVector.filter(rows, rowDataTypes, reinsertFilter)
        closeOnExcept(filteredRows) { _ =>
          if (filteredRows.numRows() > 0) {
            if (metadataProjection != null) {
              val metadataBatch = withResource(metadataProjection.project(batch)) { metadata =>
                GpuColumnVector.filter(metadata, metadataDataTypes, reinsertFilter)
              }
              writer.reinsert(metadataBatch, filteredRows)
            } else {
              writer.reinsert(null, filteredRows)
            }
          } else {
            filteredRows.close()
          }
        }
      }
    }
  }
}
