/*
 * Copyright (c) 2022-2026, NVIDIA CORPORATION.
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
{"spark": "330"}
{"spark": "330db"}
{"spark": "331"}
{"spark": "332"}
{"spark": "332db"}
{"spark": "333"}
{"spark": "334"}
{"spark": "340"}
{"spark": "341"}
{"spark": "342"}
{"spark": "343"}
{"spark": "344"}
{"spark": "350"}
{"spark": "351"}
{"spark": "352"}
{"spark": "353"}
{"spark": "354"}
{"spark": "355"}
{"spark": "356"}
{"spark": "357"}
{"spark": "358"}
spark-rapids-shim-json-lines ***/
package org.apache.spark.sql.rapids.execution.python.shims

import java.io.DataOutputStream
import java.net.Socket

import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.GpuSemaphore

import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.api.python.{ChainedPythonFunctions, PythonRDD}
import org.apache.spark.sql.rapids.execution.python.{GpuArrowWriter, GpuPythonRunnerCommon}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.Utils

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

  protected override def newWriterThread(
      env: SparkEnv,
      worker: Socket,
      inputIterator: Iterator[(ColumnarBatch, ColumnarBatch)],
      partitionIndex: Int,
      context: TaskContext): WriterThread = {
    new WriterThread(env, worker, inputIterator, partitionIndex, context) {

      protected override def writeCommand(dataOut: DataOutputStream): Unit = {
        dataOut.writeInt(conf.size)
        for ((k, v) <- conf) {
          PythonRDD.writeUTF(k, dataOut)
          PythonRDD.writeUTF(v, dataOut)
        }
        WritePythonUDFUtils.writeUDFs(dataOut, funcs, argOffsets)
      }

      protected override def writeIteratorToStream(dataOut: DataOutputStream): Unit = {
        // For each we first send the number of dataframes in each group then send
        // first df, then send second df.  End of data is marked by sending 0.
        while (inputIterator.hasNext) {
          dataOut.writeInt(2)
          val (leftGroupBatch, rightGroupBatch) = inputIterator.next()
          withResource(Seq(leftGroupBatch, rightGroupBatch)) { _ =>
            writeGroupBatch(leftGroupBatch, leftSchema, dataOut)
            writeGroupBatch(rightGroupBatch, rightSchema, dataOut)
          }
        }
        // The iterator can grab the semaphore even on an empty batch
        GpuSemaphore.releaseIfNecessary(TaskContext.get())
        dataOut.writeInt(0)
        dataOut.flush()
      }

      private def writeGroupBatch(groupBatch: ColumnarBatch, batchSchema: StructType,
          dataOut: DataOutputStream): Unit = {
        val gpuArrowWriter = GpuArrowWriter(batchSchema, batchSize)
        Utils.tryWithSafeFinally {
          gpuArrowWriter.start(dataOut)
          gpuArrowWriter.write(groupBatch)
        } {
          gpuArrowWriter.reset()
          dataOut.flush()
        }
      } // end of writeGroup
    }
  } // end of newWriterThread
}
