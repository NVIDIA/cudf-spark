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
import org.apache.spark.sql.rapids.shims.ArrowUtilsShim
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * Spark 4.2 writes runnerConf/evalConf in BasePythonRunner before writeCommand.
 * Keep writeCommand limited to the UDF section so the Python worker reads fields
 * in the same order as Spark's ArrowPythonRunner.
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

  // Spark 4.2 serializes runnerConf before writeCommand, unlike older shims where
  // GpuArrowPythonWriter.writeCommand wrote these values itself.
  override def runnerConf: Map[String, String] = super.runnerConf ++ conf

  override def evalConf: Map[String, String] = {
    if (evalType == PythonEvalType.SQL_ARROW_BATCHED_UDF) {
      // Spark 4.2 Python worker expects Arrow batched UDF input_type in evalConf.
      super.evalConf ++ Map("input_type" -> pythonInSchema.json)
    } else {
      super.evalConf
    }
  }

  protected override def newWriter(
      env: SparkEnv,
      worker: PythonWorker,
      inputIterator: Iterator[ColumnarBatch],
      partitionIndex: Int,
      context: TaskContext): Writer = {
    new Writer(env, worker, inputIterator, partitionIndex, context) {

      val arrowWriter = new GpuArrowPythonWriter(pythonInSchema, maxBatchSize) {
        // Required by GpuArrowPythonWriter helpers. The 4.2 command path below writes
        // the same UDF section directly after BasePythonRunner has written configs.
        override protected def writeUDFs(dataOut: DataOutputStream): Unit = {
          WritePythonUDFUtils.writeUDFs(dataOut, funcs, argOffsets, argNames)
        }
      }
      private var wroteAnyInput = false
      lazy val arrowSchema = ArrowUtilsShim.toArrowSchema(pythonInSchema, timeZoneId)

      protected override def writeCommand(dataOut: DataOutputStream): Unit = {
        WritePythonUDFUtils.writeUDFs(dataOut, funcs, argOffsets, argNames)
      }

      override def writeNextInputToStream(dataOut: DataOutputStream): Boolean = {
        // Keep hasNext lazy. Probing nonEmpty before the writer loop can pull from a
        // GPU-backed iterator too early and leave the Python worker waiting for input.
        if (inputIterator.hasNext) {
          arrowWriter.start(dataOut)
          try {
            arrowWriter.writeAndClose(inputIterator.next())
            wroteAnyInput = true
            dataOut.flush()
            true
          } catch {
            case t: Throwable =>
              arrowWriter.close()
              GpuSemaphore.releaseIfNecessary(TaskContext.get())
              throw t
          }
        } else {
          GpuSemaphore.releaseIfNecessary(TaskContext.get())
          if (wroteAnyInput) {
            arrowWriter.close()
            dataOut.flush()
          } else {
            // CPU Arrow writers still emit a schema for empty partitions; mirror that
            // so the Spark 4.2 Python worker does not block waiting for an Arrow stream.
            arrowWriter.writeEmptyIteratorOnCpu(dataOut, arrowSchema)
            dataOut.flush()
          }
          false
        }
      }
    }
  }
}
