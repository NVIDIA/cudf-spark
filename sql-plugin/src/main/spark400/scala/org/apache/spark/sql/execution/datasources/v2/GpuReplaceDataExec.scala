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

package org.apache.spark.sql.execution.datasources.v2

import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.GpuWrite

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.GpuProjectingColumnarBatch
import org.apache.spark.sql.catalyst.util.ReplaceDataProjections
import org.apache.spark.sql.connector.write.DataWriter
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.vectorized.ColumnarBatch

case class GpuReplaceDataExec(
    inner: SparkPlan,
    refreshCache: () => Unit,
    projections: ReplaceDataProjections,
    write: GpuWrite) extends GpuV2ExistingTableWriteExec {

  override def supportsColumnar: Boolean = false

  override def query: SparkPlan = inner

  override lazy val writingTask: GpuWritingSparkTask[_] =
    GpuReplaceDataWritingSparkTask(projections)

  override protected def internalDoExecuteColumnar(): RDD[ColumnarBatch] = {
    throw new IllegalStateException(
      "GpuReplaceDataExec does not support columnar execution")
  }

  override protected def withNewChildInternal(newChild: SparkPlan): GpuReplaceDataExec = {
    copy(inner = newChild)
  }
}

case class GpuReplaceDataWritingSparkTask(
    projs: ReplaceDataProjections)
  extends GpuWritingSparkTask[DataWriter[ColumnarBatch]] {

  private lazy val rowProjection = GpuProjectingColumnarBatch(projs.rowProjection)
  override protected def write(
      writer: DataWriter[ColumnarBatch],
      batch: ColumnarBatch): Unit = {
    withResource(rowProjection.project(batch)) { projected =>
      writer.write(projected)
    }
  }
}
