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
package org.apache.spark.sql.rapids.execution

import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.RapidsPluginImplicits._

import org.apache.spark.TaskContext
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch

private[execution] object GpuShuffleBroadcastHelper {
  def assemble(
      input: Iterator[ColumnarBatch],
      schema: StructType,
      output: Seq[Attribute],
      metrics: Map[String, GpuMetric],
      maxBytes: Long): ColumnarBatch = {
    var inputBytes = BigInt(0)
    val boundedInput = input.map { batch =>
      closeOnExcept(batch) { _ =>
        inputBytes += GpuColumnVector.getTotalDeviceMemoryUsed(batch)
        BroadcastBuildAdmission.checkActualBytes(inputBytes, maxBytes, "received build")
        batch
      }
    }
    val coalesced = new GpuCoalesceIterator(boundedInput, GpuColumnVector.extractTypes(schema),
      RequireSingleBatch, NoopMetric, NoopMetric, NoopMetric, NoopMetric, NoopMetric,
      metrics(GpuMetric.CONCAT_TIME), metrics(GpuMetric.OP_TIME_LEGACY),
      "executor broadcast build") with AutoCloseable {
      override def cleanupConcatIsDone(): Unit = {
        // A rejected input can leave earlier accepted batches waiting for concatenation.
        try batches.safeClose() finally batches.clear()
      }

      override def close(): Unit = {
        try cleanupConcatIsDone() finally clearOnDeck()
      }
    }
    withResource(coalesced) { batches =>
      if (!batches.hasNext) {
        // An empty shuffle does not acquire the semaphore through its reader.
        GpuSemaphore.acquireIfNecessary(TaskContext.get())
      }
      closeOnExcept(ConcatAndConsumeAll.getSingleBatchWithVerification(batches, output)) { batch =>
        // Concatenation can add null masks absent from some input batches.
        BroadcastBuildAdmission.checkActualBytes(
          BigInt(GpuColumnVector.getTotalDeviceMemoryUsed(batch)), maxBytes, "assembled build")
        batch
      }
    }
  }
}
