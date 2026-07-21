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
{"spark": "411"}
{"spark": "412"}
{"spark": "420"}
spark-rapids-shim-json-lines ***/
package org.apache.spark.sql.rapids.execution.python.shims

import java.io.DataInputStream
import java.util.concurrent.atomic.AtomicBoolean

import com.nvidia.spark.rapids.GpuSemaphore

import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.api.python._
import org.apache.spark.sql.rapids.execution.python.GpuArrowOutput
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * A trait that can be mixed-in with `GpuBasePythonRunner`. It implements the logic from
 * Python (Arrow) to GPU/JVM (ColumnarBatch).
 */
trait GpuArrowPythonOutput extends GpuArrowOutput { _: GpuBasePythonRunner[_] =>

  protected def newReaderIterator(
      stream: DataInputStream,
      writer: Writer,
      startTime: Long,
      env: SparkEnv,
      worker: PythonWorker,
      pid: Option[Int],
      releasedOrClosed: AtomicBoolean,
      context: TaskContext): Iterator[ColumnarBatch] = {

    new ReaderIterator(stream, writer, startTime, env, worker, pid, releasedOrClosed, context) {
      val gpuArrowReader = newGpuArrowReader

      protected override def read(): ColumnarBatch = {
        if (writer.exception.isDefined) {
          throw writer.exception.get
        }
        try {
          // Because of batching and other things we have to be sure that we release the semaphore
          // before any operation that could block. This is because we are using multiple threads
          // for a single task and the GpuSemaphore might not wake up both threads associated with
          // the task, so a reader can be blocked waiting for data, while a writer is waiting on
          // the semaphore
          GpuSemaphore.releaseIfNecessary(TaskContext.get())
          if (gpuArrowReader.isStarted && gpuArrowReader.mayHasNext) {
            val batch = gpuArrowReader.readNext()
            if (batch != null) {
              batch
            } else {
              gpuArrowReader.close() // reach the end, close the reader
              read() // read the end signal
            }
          } else {
            stream.readInt() match {
              case SpecialLengths.START_ARROW_STREAM =>
                gpuArrowReader.start(stream)
                read()
              case SpecialLengths.TIMING_DATA =>
                handleTimingData()
                read()
              case SpecialLengths.PYTHON_EXCEPTION_THROWN =>
                throw handlePythonException()
              case SpecialLengths.END_OF_DATA_SECTION =>
                handleEndOfDataSection()
                null
            }
          }
        } catch handleException
      }
    }
  }
}
