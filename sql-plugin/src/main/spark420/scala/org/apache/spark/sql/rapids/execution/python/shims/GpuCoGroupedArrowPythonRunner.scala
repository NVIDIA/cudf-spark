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
{"spark": "420"}
spark-rapids-shim-json-lines ***/
package org.apache.spark.sql.rapids.execution.python.shims

import java.io.DataOutputStream

import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.GpuSemaphore

import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.api.python.{ChainedPythonFunctions, PythonWorker}
import org.apache.spark.sql.rapids.execution.python.{GpuArrowWriter, GpuPythonRunnerCommon}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * Spark 4.2 writes runnerConf/evalConf before writeCommand, so writeCommand
 * only writes the UDF section.
 */
class GpuCoGroupedArrowPythonRunner(
    funcs: Seq[(ChainedPythonFunctions, Long)],
    evalType: Int,
    argOffsets: Array[Array[Int]],
    leftSchema: StructType,
    rightSchema: StructType,
    timeZoneId: String,
    conf: Map[String, String],
    batchSize: Int,
    override val pythonOutSchema: StructType,
    jobArtifactUUID: Option[String] = None)
  extends GpuBasePythonRunner[(ColumnarBatch, ColumnarBatch)](funcs.map(_._1), evalType,
    argOffsets, jobArtifactUUID) with GpuArrowPythonOutput with GpuPythonRunnerCommon {

  override def runnerConf: Map[String, String] = super.runnerConf ++ conf

  protected override def newWriter(
      env: SparkEnv,
      worker: PythonWorker,
      inputIterator: Iterator[(ColumnarBatch, ColumnarBatch)],
      partitionIndex: Int,
      context: TaskContext): Writer = {
    new Writer(env, worker, inputIterator, partitionIndex, context) {

      protected override def writeCommand(dataOut: DataOutputStream): Unit = {
        WritePythonUDFUtils.writeUDFs(dataOut, funcs, argOffsets)
      }

      override def writeNextInputToStream(dataOut: DataOutputStream): Boolean = {
        if (inputIterator.hasNext) {
          dataOut.writeInt(2)
          val (leftGroupBatch, rightGroupBatch) = inputIterator.next()
          withResource(Seq(leftGroupBatch, rightGroupBatch)) { _ =>
            writeGroupBatch(leftGroupBatch, leftSchema, dataOut)
            writeGroupBatch(rightGroupBatch, rightSchema, dataOut)
          }
          true
        } else {
          GpuSemaphore.releaseIfNecessary(TaskContext.get())
          dataOut.writeInt(0)
          false
        }
      }

      private def writeGroupBatch(groupBatch: ColumnarBatch, batchSchema: StructType,
          dataOut: DataOutputStream): Unit = {
        val gpuArrowWriter = GpuArrowWriter(batchSchema, batchSize)
        try {
          gpuArrowWriter.start(dataOut)
          gpuArrowWriter.write(groupBatch)
        } catch {
          case t: Throwable =>
            GpuSemaphore.releaseIfNecessary(TaskContext.get())
            throw t
        } finally {
          gpuArrowWriter.reset()
          dataOut.flush()
        }
      }
    }
  }
}
