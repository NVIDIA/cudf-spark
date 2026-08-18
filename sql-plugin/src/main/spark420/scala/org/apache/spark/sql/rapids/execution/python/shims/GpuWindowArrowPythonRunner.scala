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

import com.nvidia.spark.rapids.GpuSemaphore

import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.api.python._
import org.apache.spark.sql.rapids.execution.python.{GpuArrowPythonWriter, GpuPythonRunnerCommon}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * Grouped/window Python runner for Spark 4.2. The grouped input protocol still
 * uses 1/0 markers, but runner conf is written by BasePythonRunner before this
 * writer command.
 */
class GpuWindowArrowPythonRunner(
    funcs: Seq[(ChainedPythonFunctions, Long)],
    evalType: Int,
    argOffsets: Array[Array[Int]],
    pythonInSchema: StructType,
    timeZoneId: String,
    conf: Map[String, String],
    batchSize: Long,
    override val pythonOutSchema: StructType,
    argNames: Option[Array[Array[Option[String]]]] = None,
    jobArtifactUUID: Option[String] = None)
  extends GpuBasePythonRunner[ColumnarBatch](funcs.map(_._1), evalType, argOffsets,
    jobArtifactUUID) with GpuArrowPythonOutput with GpuPythonRunnerCommon {

  override def runnerConf: Map[String, String] = super.runnerConf ++ conf

  protected override def newWriter(
      env: SparkEnv,
      worker: PythonWorker,
      inputIterator: Iterator[ColumnarBatch],
      partitionIndex: Int,
      context: TaskContext): Writer = {
    new Writer(env, worker, inputIterator, partitionIndex, context) {

      val arrowWriter = new GpuArrowPythonWriter(pythonInSchema, batchSize) {
        override protected def writeUDFs(dataOut: DataOutputStream): Unit = {
          WritePythonUDFUtils.writeUDFs(dataOut, funcs, argOffsets, argNames)
        }
      }

      protected override def writeCommand(dataOut: DataOutputStream): Unit = {
        WritePythonUDFUtils.writeUDFs(dataOut, funcs, argOffsets, argNames)
      }

      override def writeNextInputToStream(dataOut: DataOutputStream): Boolean = {
        try {
          if (inputIterator.hasNext) {
            dataOut.writeInt(1)
            arrowWriter.start(dataOut)
            arrowWriter.writeAndClose(inputIterator.next())
            arrowWriter.reset()
            dataOut.flush()
            true
          } else {
            GpuSemaphore.releaseIfNecessary(TaskContext.get())
            dataOut.writeInt(0)
            dataOut.flush()
            false
          }
        } catch {
          case t: Throwable =>
            arrowWriter.close()
            GpuSemaphore.releaseIfNecessary(TaskContext.get())
            throw t
        }
      }
    }
  }
}
