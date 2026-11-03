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

import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito._
import org.scalatest.BeforeAndAfterEach
import org.scalatest.concurrent.{TimeLimitedTests, TimeLimits}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.time.{Seconds, Span}
import org.scalatestplus.mockito.MockitoSugar

import org.apache.spark.TaskContext
import org.apache.spark.sql.SparkSession

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
}
