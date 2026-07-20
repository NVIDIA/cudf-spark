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

import java.io.DataInputStream
import java.net.Socket
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
      writerThread: WriterThread,
      startTime: Long,
      env: SparkEnv,
      worker: Socket,
      pid: Option[Int],  // new paramter from Spark 320
      releasedOrClosed: AtomicBoolean,
      context: TaskContext): Iterator[ColumnarBatch] = {

    new ReaderIterator(stream, writerThread, startTime, env, worker, pid, releasedOrClosed,
      context) {
      val gpuArrowReader = newGpuArrowReader

      protected override def read(): ColumnarBatch = {
        if (writerThread.exception.isDefined) {
          throw writerThread.exception.get
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
