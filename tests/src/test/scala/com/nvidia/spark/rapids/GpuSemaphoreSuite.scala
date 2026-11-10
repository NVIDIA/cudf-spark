/*
 * Copyright (c) 2019-2026, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids

import scala.collection.mutable.ArrayBuffer

import com.nvidia.spark.rapids.Arm.withResource
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito._
import org.scalatest.BeforeAndAfterEach
import org.scalatest.concurrent.{TimeLimitedTests, TimeLimits}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.time.{Seconds, Span}
import org.scalatestplus.mockito.MockitoSugar

import org.apache.spark.TaskContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector
import org.apache.spark.sql.rapids.metrics.source.MockTaskContext
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.vectorized.ColumnarBatch

class GpuSemaphoreSuite extends AnyFunSuite
    with BeforeAndAfterEach with MockitoSugar with TimeLimits  with TimeLimitedTests {
  // SparkSession startup can exceed 10 seconds when multiple isolated test JVMs start together.
  val timeLimit: Span = Span(30, Seconds)

  override def beforeEach(): Unit = {
    ScalableTaskCompletion.reset()
    GpuSemaphore.shutdown()
    // semaphore tests depend on a SparkEnv being available
    val activeSession = SparkSession.getActiveSession
    if (activeSession.isDefined) {
      SparkSession.getActiveSession.foreach(_.stop())
      SparkSession.clearActiveSession()
    }
    SparkSession.builder
        .appName("semaphoreTests")
        .master("local[1]")
        // only 1 task at a time so we can verify what blocks and what does not block
        .config("spark.rapids.sql.concurrentGpuTasks", "1")
        .getOrCreate()
  }

  override def afterEach(): Unit = {
    ScalableTaskCompletion.reset()
    GpuSemaphore.shutdown()
    SparkSession.getActiveSession.foreach(_.stop())
    SparkSession.clearActiveSession()
  }

  def mockContext(taskAttemptId: Long): TaskContext = {
    val context = mock[TaskContext]
    when(context.taskAttemptId()).thenReturn(taskAttemptId)
    context
  }

  test("Releasing before acquiring is not an error") {
    val context = mockContext(1)
    GpuSemaphore.releaseIfNecessary(context)
  }

  test("Double release is not an error") {
    GpuDeviceManager.setRmmTaskInitEnabled(false)
    val context = mockContext(1)
    GpuSemaphore.acquireIfNecessary(context)
    GpuSemaphore.acquireIfNecessary(context)
    GpuSemaphore.releaseIfNecessary(context)
    GpuSemaphore.releaseIfNecessary(context)
  }

  test("Completion listener registered on first acquire") {
    val context = mockContext(1)
    GpuSemaphore.acquireIfNecessary(context)
    verify(context, times(1)).addTaskCompletionListener[Unit](any())
    GpuSemaphore.acquireIfNecessary(context)
    GpuSemaphore.acquireIfNecessary(context)
    verify(context, times(1)).addTaskCompletionListener[Unit](any())
  }

  def assertAcquired(result: TryAcquireResult): Unit = result match {
    case SemaphoreAcquired => // NOOP
    case AcquireFailed(_) =>
      fail("The Semaphore was not acquired")
  }

  def assertNotAcquired(numExpectedWaiting: Int, result: TryAcquireResult): Unit = result match {
    case SemaphoreAcquired =>
      fail("The Semaphore was acquired when we didn't expect it")
    case AcquireFailed(numWaiting) =>
      assert(numWaiting == numExpectedWaiting, "The number of waiting tasks didn't match")
  }

  test("multi tryAcquire") {
    GpuDeviceManager.setRmmTaskInitEnabled(false)
    val context = mockContext(1)
    try {
      assertAcquired(GpuSemaphore.tryAcquire(context))
      assertAcquired(GpuSemaphore.tryAcquire(context))
    } finally {
      GpuSemaphore.releaseIfNecessary(context)
    }
  }

  test("tryAcquire non-blocking") {
    GpuDeviceManager.setRmmTaskInitEnabled(false)
    val context1 = mockContext(1)
    val context2 = mockContext(2)
    try {
      GpuSemaphore.acquireIfNecessary(context1)
      assertNotAcquired(1, GpuSemaphore.tryAcquire(context2))
      assertNotAcquired(1, GpuSemaphore.tryAcquire(context2))
    } finally {
      GpuSemaphore.releaseIfNecessary(context1)
      GpuSemaphore.releaseIfNecessary(context2)
    }
  }

  Seq(true, false).foreach { enabled =>
    val behavior = if (enabled) {
      "releases the semaphore only while next() pulls host batches"
    } else {
      "keeps the semaphore while it pulls host batches when the release is disabled"
    }
    test(s"HostToGpuCoalesceIterator $behavior") {
      hostToGpuCoalesceIteratorPulls(enabled)
    }
  }

  private def hostToGpuCoalesceIteratorPulls(releaseSemaphore: Boolean): Unit = {
    GpuDeviceManager.setRmmTaskInitEnabled(false)
    val context1 = new MockTaskContext(taskAttemptId = 1, partitionId = 0)
    val context2 = mockContext(2)
    // With one concurrent GPU task, task 2 gets the semaphore only if task 1 released it.
    def released: Boolean = GpuSemaphore.tryAcquire(context2) match {
      case SemaphoreAcquired =>
        GpuSemaphore.releaseIfNecessary(context2)
        true
      case AcquireFailed(_) => false
    }
    var inNext = false
    // Whether each pull of a host batch happened in next() and with the semaphore released
    val pulls = ArrayBuffer[(Boolean, Boolean)]()
    val schema = StructType(Seq(StructField("i", IntegerType)))
    val numRows = 100
    // The producer closes its batches, not HostToGpuCoalesceIterator.
    val produced = ArrayBuffer[ColumnarBatch]()
    val hostBatches = new Iterator[ColumnarBatch] {
      private var remaining = 3
      override def hasNext: Boolean = {
        pulls += ((inNext, released))
        remaining > 0
      }
      override def next(): ColumnarBatch = {
        pulls += ((inNext, released))
        remaining -= 1
        val ints = new OnHeapColumnVector(numRows, IntegerType)
        (0 until numRows).foreach(i => ints.putInt(i, i))
        produced += new ColumnarBatch(Array(ints), numRows)
        produced.last
      }
    }
    TestUtils.withTaskContext(context1) {
      try {
        // A target size below one host batch, so that each host batch is a GPU batch
        val iter = new HostToGpuCoalesceIterator(hostBatches, TargetSize(1), schema,
          NoopMetric, NoopMetric, NoopMetric, NoopMetric, NoopMetric, NoopMetric, NoopMetric,
          NoopMetric, "test", useArrowCopyOpt = false,
          releaseSemaphoreWhilePulling = releaseSemaphore)
        // As an operator above that still uses the GPU when it asks for more
        GpuSemaphore.acquireIfNecessary(context1)
        var rows = 0
        var gpuBatches = 0
        while (iter.hasNext) {
          inNext = true
          withResource(iter.next()) { batch =>
            inNext = false
            assert(!released, "the semaphore is not held for the GPU batch")
            rows += batch.numRows()
            gpuBatches += 1
          }
        }
        assertResult(3 * numRows)(rows)
        // Released and acquired again for each GPU batch, with the next host batch on deck
        assert(gpuBatches >= 2, s"$gpuBatches GPU batches")
        assert(pulls.exists(_._1) && pulls.exists(!_._1), pulls)
        assert(pulls.forall { case (pulledInNext, wasReleased) =>
          wasReleased == (releaseSemaphore && pulledInNext)
        }, pulls)
      } finally {
        GpuSemaphore.releaseIfNecessary(context1)
        GpuSemaphore.releaseIfNecessary(context2)
        TaskRegistryTracker.clearRegistry()
        produced.foreach(_.close())
      }
    }
  }
}
