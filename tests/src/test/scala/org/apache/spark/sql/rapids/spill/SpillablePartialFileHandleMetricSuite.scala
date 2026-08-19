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

package org.apache.spark.sql.rapids.spill

import java.io.File

import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.RapidsConf
import com.nvidia.spark.rapids.spill.{SpillablePartialFileHandle, SpillFramework}
import org.mockito.Mockito.{mock, when}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.TaskContext
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.sql.rapids.execution.TrampolineUtil

/**
 * Regression tests for issue #14704: `SpillablePartialFileHandle` must not bump
 * the disk-bytes-spilled task metric when the spill is triggered as part of a
 * shuffle commit (those bytes are already accounted for in shuffle-write
 * metrics). The `spill()` path — invoked by the spill framework under memory
 * pressure — must still bump the metric.
 *
 * Lives in `org.apache.spark.*` so we have access to package-private members
 * such as `TaskMetrics#diskBytesSpilled` and the `TaskContext` constructor.
 */
class SpillablePartialFileHandleMetricSuite extends AnyFunSuite with BeforeAndAfterEach {

  private val testMaxBufferSize = 1L * 1024 * 1024 * 1024

  override def beforeEach(): Unit = {
    super.beforeEach()
    val conf = new RapidsConf(Map(
      "spark.rapids.memory.host.partialFileBufferMaxSize" -> testMaxBufferSize.toString
    ))
    SpillFramework.initialize(conf)
  }

  override def afterEach(): Unit = {
    SpillFramework.shutdown()
    super.afterEach()
  }

  // Install a TaskContext returning a real TaskMetrics so that
  // TrampolineUtil.incTaskMetricsDiskBytesSpilled actually records into it.
  // (TaskMetrics is `private[spark]`-constructed; we have access here because
  // this test lives in the org.apache.spark.* package tree.)
  private def withTaskContext[T](f: TaskMetrics => T): T = {
    val taskMetrics = new TaskMetrics
    val taskContext = mock(classOf[TaskContext])
    when(taskContext.taskMetrics()).thenReturn(taskMetrics)
    TrampolineUtil.setTaskContext(taskContext)
    try f(taskMetrics)
    finally TrampolineUtil.unsetTaskContext()
  }

  test("spillForCommit must NOT increment diskBytesSpilled (issue #14704)") {
    // Bytes written during a shuffle commit are reported by the shuffle-write
    // metrics; charging them again as memory-pressure spill produces the
    // double-count described in the bug.
    val tempFile = File.createTempFile("test-spill-for-commit-metric-", ".tmp")
    try {
      withTaskContext { taskMetrics =>
        withResource(SpillablePartialFileHandle.createMemoryWithSpill(
          initialCapacity = 1024,
          maxBufferSize = testMaxBufferSize,
          memoryThreshold = 0.5,
          spillFile = tempFile)) { handle =>

          val testData = "Bytes flushed as part of shuffle commit".getBytes("UTF-8")
          handle.write(testData, 0, testData.length)
          handle.finishWrite()

          assert(taskMetrics.diskBytesSpilled == 0L)

          val flushed = handle.spillForCommit()
          assert(flushed == testData.length)
          assert(handle.isSpilled)

          assert(taskMetrics.diskBytesSpilled == 0L,
            s"spillForCommit should not increment diskBytesSpilled, " +
              s"but got ${taskMetrics.diskBytesSpilled}")
        }
      }
    } finally {
      tempFile.delete()
    }
  }

  test("spill (memory pressure) increments diskBytesSpilled (issue #14704)") {
    // Counterpart of the fix: spill() — used for memory-pressure eviction —
    // must still account bytes against the disk-bytes-spilled task metric.
    val tempFile = File.createTempFile("test-spill-pressure-metric-", ".tmp")
    try {
      withTaskContext { taskMetrics =>
        withResource(SpillablePartialFileHandle.createMemoryWithSpill(
          initialCapacity = 1024,
          maxBufferSize = testMaxBufferSize,
          memoryThreshold = 0.5,
          spillFile = tempFile)) { handle =>

          val testData = "Bytes spilled due to memory pressure".getBytes("UTF-8")
          handle.write(testData, 0, testData.length)
          handle.finishWrite()

          assert(taskMetrics.diskBytesSpilled == 0L)

          val spilled = handle.spill()
          assert(spilled == testData.length)
          assert(handle.isSpilled)

          assert(taskMetrics.diskBytesSpilled == testData.length.toLong,
            s"spill should increment diskBytesSpilled to ${testData.length}, " +
              s"but got ${taskMetrics.diskBytesSpilled}")
        }
      }
    } finally {
      tempFile.delete()
    }
  }
}
