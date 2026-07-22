/*
 * Copyright (c) 2023-2026, NVIDIA CORPORATION.
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
{"spark": "341db"}
{"spark": "350db143"}
{"spark": "400"}
{"spark": "400db173"}
{"spark": "401"}
{"spark": "402"}
{"spark": "403"}
{"spark": "404"}
{"spark": "411"}
{"spark": "412"}
{"spark": "413"}
spark-rapids-shim-json-lines ***/

package org.apache.spark.sql.rapids.execution.python.shims

import java.io.DataOutputStream

import com.nvidia.spark.rapids.GpuSemaphore

import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.api.python._
import org.apache.spark.sql.rapids.execution.python.{GpuArrowPythonWriter, GpuPythonRunnerCommon}
import org.apache.spark.sql.rapids.shims.ArrowUtilsShim
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * Similar to `PythonUDFRunner`, but exchange data with Python worker via Arrow stream.
 */
class GpuArrowPythonRunner(
    funcs: Seq[(ChainedPythonFunctions, Long)],
    evalType: Int,
    argOffsets: Array[Array[Int]],
    pythonInSchema: StructType,
    timeZoneId: String,
    conf: Map[String, String],
    maxBatchSize: Long,
    override val pythonOutSchema: StructType,
    argNames: Option[Array[Array[Option[String]]]] = None,
    jobArtifactUUID: Option[String] = None)
  extends GpuBasePythonRunner[ColumnarBatch](funcs.map(_._1), evalType, argOffsets,
    jobArtifactUUID) with GpuArrowPythonOutput with GpuPythonRunnerCommon {

  protected override def newWriter(
      env: SparkEnv,
      worker: PythonWorker,
      inputIterator: Iterator[ColumnarBatch],
      partitionIndex: Int,
      context: TaskContext): Writer = {
    new Writer(env, worker, inputIterator, partitionIndex, context) {

      val arrowWriter = new GpuArrowPythonWriter(pythonInSchema, maxBatchSize) {
        override protected def writeUDFs(dataOut: DataOutputStream): Unit = {
          WritePythonUDFUtils.writeUDFs(dataOut, funcs, argOffsets, argNames)
        }
      }
      val isInputNonEmpty = inputIterator.nonEmpty
      lazy val arrowSchema = ArrowUtilsShim.toArrowSchema(pythonInSchema, timeZoneId)

      protected override def writeCommand(dataOut: DataOutputStream): Unit = {
        arrowWriter.writeCommand(dataOut, conf)
      }

      override def writeNextInputToStream(dataOut: DataOutputStream): Boolean = {
        if (isInputNonEmpty) {
          arrowWriter.start(dataOut)
          try {
            if (inputIterator.hasNext) {
              arrowWriter.writeAndClose(inputIterator.next())
              dataOut.flush()
              true
            } else {
              arrowWriter.close() // all batches are written, close the writer
              false
            }
          } catch {
            case t: Throwable =>
              arrowWriter.close()
              GpuSemaphore.releaseIfNecessary(TaskContext.get())
              throw t
          }
        } else {
          // The iterator can grab the semaphore even on an empty batch
          GpuSemaphore.releaseIfNecessary(TaskContext.get())
          arrowWriter.writeEmptyIteratorOnCpu(dataOut, arrowSchema)
          false
        }
      }
    }
  }
}
