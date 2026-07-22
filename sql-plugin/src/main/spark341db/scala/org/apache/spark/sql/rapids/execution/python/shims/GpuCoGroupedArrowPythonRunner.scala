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

import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.GpuSemaphore

import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.api.python.{ChainedPythonFunctions, PythonRDD, PythonWorker}
import org.apache.spark.sql.rapids.execution.python.{GpuArrowWriter, GpuPythonRunnerCommon}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * Python UDF Runner for cogrouped UDFs, designed for `GpuFlatMapCoGroupsInPandasExec` only.
 *
 * It sends Arrow batches from two different DataFrames, groups them in Python,
 * and receive it back in JVM as batches of single DataFrame.
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

  protected override def newWriter(
      env: SparkEnv,
      worker: PythonWorker,  // Changed from "Socket" to this "PythonWorker" from db341
      inputIterator: Iterator[(ColumnarBatch, ColumnarBatch)],
      partitionIndex: Int,
      context: TaskContext): Writer = {
    new Writer(env, worker, inputIterator, partitionIndex, context) {

      protected override def writeCommand(dataOut: DataOutputStream): Unit = {
        // Write config for the worker as a number of key -> value pairs of strings
        dataOut.writeInt(conf.size)
        for ((k, v) <- conf) {
          PythonRDD.writeUTF(k, dataOut)
          PythonRDD.writeUTF(v, dataOut)
        }
        WritePythonUDFUtils.writeUDFs(dataOut, funcs, argOffsets)
      }

      override def writeNextInputToStream(dataOut: DataOutputStream): Boolean = {
        if (inputIterator.hasNext) {
          // For each we first send the number of dataframes in each group then send
          // first df, then send second df.
          dataOut.writeInt(2)
          val (leftGroupBatch, rightGroupBatch) = inputIterator.next()
          withResource(Seq(leftGroupBatch, rightGroupBatch)) { _ =>
            writeGroupBatch(leftGroupBatch, leftSchema, dataOut)
            writeGroupBatch(rightGroupBatch, rightSchema, dataOut)
          }
          true
        } else {
          // The iterator can grab the semaphore even on an empty batch
          GpuSemaphore.releaseIfNecessary(TaskContext.get())
          //  End of data is marked by sending 0.
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
            // release the semaphore in case of exception in the middle of writing a batch
            GpuSemaphore.releaseIfNecessary(TaskContext.get())
            throw t
        } finally {
          gpuArrowWriter.reset()
          dataOut.flush()
        }
      } // end of writeGroupBatch
    }
  }
}
