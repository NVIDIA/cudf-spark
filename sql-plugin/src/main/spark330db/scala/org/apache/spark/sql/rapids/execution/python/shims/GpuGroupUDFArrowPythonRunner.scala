/*
 * Copyright (c) 2021-2025, NVIDIA CORPORATION.
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*** spark-rapids-shim-json-lines
{"spark": "330db"}
{"spark": "332db"}
spark-rapids-shim-json-lines ***/
package org.apache.spark.sql.rapids.execution.python.shims

import java.io.DataOutputStream
import java.net.Socket

import com.nvidia.spark.rapids.GpuSemaphore

import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.api.python._
import org.apache.spark.sql.rapids.execution.python.{GpuArrowPythonWriter, GpuPythonRunnerCommon}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.Utils

/**
 * Group Map UDF specific serializer for Databricks because they have a special GroupUDFSerializer.
 * The main difference here from the GpuArrowPythonRunner is that it creates a new Arrow
 * Stream for each grouped data.
 * The overall flow is:
 *   - send a 1 to indicate more data is coming
 *   - create a new Arrow Stream for each grouped data
 *   - send the schema
 *   - send that group of data
 *   - close that Arrow stream
 *   - Repeat starting at sending 1 if more data, otherwise send a 0 to indicate no
 *     more data being sent.
 */
class GpuGroupUDFArrowPythonRunner(
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

  protected override def newWriterThread(
      env: SparkEnv,
      worker: Socket,
      inputIterator: Iterator[ColumnarBatch],
      partitionIndex: Int,
      context: TaskContext): WriterThread = {
    new WriterThread(env, worker, inputIterator, partitionIndex, context) {

      val arrowWriter = new GpuArrowPythonWriter(pythonInSchema, maxBatchSize) {
        override protected def writeUDFs(dataOut: DataOutputStream): Unit = {
          WritePythonUDFUtils.writeUDFs(dataOut, funcs, argOffsets, argNames)
        }
      }

      protected override def writeCommand(dataOut: DataOutputStream): Unit = {
        arrowWriter.writeCommand(dataOut, conf)
      }

      protected override def writeIteratorToStream(dataOut: DataOutputStream): Unit = {
        Utils.tryWithSafeFinally {
          while(inputIterator.hasNext) {
            // write 1 out to indicate there is more to read
            dataOut.writeInt(1)
            arrowWriter.start(dataOut)
            arrowWriter.writeAndClose(inputIterator.next())
            arrowWriter.reset()
            dataOut.flush()
          }
        } {
          arrowWriter.close()
          // tell serializer we are done
          dataOut.writeInt(0)
          dataOut.flush()
          GpuSemaphore.releaseIfNecessary(TaskContext.get())
        }
      }
    }
  }
}
