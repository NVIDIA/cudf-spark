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
{"spark": "359"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.shims

import ai.rapids.cudf.{ColumnVector => CudfColumnVector, Scalar => CudfScalar}
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.GpuColumnVector

import org.apache.spark.sql.catalyst.GpuProjectingColumnarBatch
import org.apache.spark.sql.catalyst.util.RowDeltaUtils.INSERT_OPERATION
import org.apache.spark.sql.connector.write.DeltaWriter
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.vectorized.ColumnarBatch

object DeltaInsertFilter {
  def filterInsertRows(batch: ColumnarBatch): CudfColumnVector = {
    withResource(CudfScalar.fromInt(INSERT_OPERATION)) { s =>
      batch.column(0).asInstanceOf[GpuColumnVector].getBase.equalTo(s)
    }
  }

  /**
   * Write INSERT_OPERATION rows via [[DeltaWriter.insert]].
   * Spark 3.5 has no REINSERT_OPERATION path.
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
   * Spark 3.5 does not distinguish REINSERT from INSERT.
   */
  def writeReinserts(
      writer: DeltaWriter[ColumnarBatch],
      batch: ColumnarBatch,
      rowProjection: GpuProjectingColumnarBatch,
      rowDataTypes: Array[DataType],
      metadataProjection: GpuProjectingColumnarBatch,
      metadataDataTypes: Array[DataType]): Unit = {
    // no-op
  }
}
