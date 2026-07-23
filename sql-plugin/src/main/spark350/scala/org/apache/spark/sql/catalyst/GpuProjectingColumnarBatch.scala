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

package org.apache.spark.sql.catalyst

import com.nvidia.spark.rapids.Arm.closeOnExcept
import com.nvidia.spark.rapids.GpuColumnVector

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}


case class GpuProjectingColumnarBatch(schema: StructType, colOrdinals: Seq[Int]) {

  /**
   * Project a subset of columns from a `ColumnarBatch` onto a new batch
   * based on the specified column ordinals and output schema. 
   *
   * @param batch The input batch to project. It's caller's responsibility to close batch.
   * @return The projected batch.
   */
  def project(batch: ColumnarBatch): ColumnarBatch = {
    closeOnExcept(new Array[ColumnVector](colOrdinals.length)) { arr =>
      for ((ordinal, idx) <- colOrdinals.zipWithIndex) {
        arr(idx) = batch.column(ordinal).asInstanceOf[GpuColumnVector].incRefCount()
      }

      new ColumnarBatch(arr, batch.numRows())
    }
  }
}

object GpuProjectingColumnarBatch {
  def apply(cpu: ProjectingInternalRow): GpuProjectingColumnarBatch = {
    GpuProjectingColumnarBatch(cpu.schema, cpu.colOrdinals)
  }
}
