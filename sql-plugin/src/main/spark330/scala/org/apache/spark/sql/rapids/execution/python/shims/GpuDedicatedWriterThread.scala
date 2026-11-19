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
{"spark":["330","340","350"],"carryForward":"sameMinor"}
spark-rapids-shim-json-lines ***/
package org.apache.spark.sql.rapids.execution.python.shims

import java.io.DataOutputStream
import java.net.Socket

import com.nvidia.spark.rapids.TaskRegistryTracker

import org.apache.spark.{SparkEnv, TaskContext}

/**
 * Mixed into a runner whose Python writer is a thread of its own, as in Spark 3.x. That thread
 * pulls the task's GPU input, so it registers with RmmSpark as dedicated to the task while it
 * writes.
 */
trait GpuDedicatedWriterThread[IN] { self: GpuBasePythonRunner[IN] =>

  /** A writer thread that is registered as dedicated to its task while it writes its input. */
  abstract class DedicatedWriterThread(
      env: SparkEnv,
      worker: Socket,
      inputIterator: Iterator[IN],
      partitionIndex: Int,
      context: TaskContext)
    extends WriterThread(env, worker, inputIterator, partitionIndex, context) {

    /** Writes the input to the Python worker, as `writeIteratorToStream` does in Spark. */
    protected def writeInput(dataOut: DataOutputStream): Unit

    protected final override def writeIteratorToStream(dataOut: DataOutputStream): Unit =
      TaskRegistryTracker.withDedicatedThreadForRetry(writeInput(dataOut))
  }
}
