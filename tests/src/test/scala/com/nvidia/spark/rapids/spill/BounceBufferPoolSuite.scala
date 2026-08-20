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

package com.nvidia.spark.rapids.spill

import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}

import ai.rapids.cudf.{DeviceMemoryBuffer, Rmm, RmmAllocationMode, RmmCudaMemoryResource,
  RmmDeviceMemoryResource, RmmEventHandler, RmmLimitingResourceAdaptor, RmmTrackingResourceAdaptor}
import com.nvidia.spark.rapids.{Arm, RmmSparkRetrySuiteBase, ScalableTaskCompletion,
  TaskRegistryTracker}
import com.nvidia.spark.rapids.jni.{GpuRetryOOM, RmmSpark, TaskPriority}
import org.scalatest.concurrent.{Signaler, TimeLimits}
import org.scalatest.time.{Seconds, Span}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.rapids.execution.TrampolineUtil
import org.apache.spark.sql.rapids.metrics.source.MockTaskContext

class BounceBufferPoolSuite extends RmmSparkRetrySuiteBase with TimeLimits {
  private val rmmAllocationAlignment = 256L

  object ThreadDumpSignaler extends Signaler {
    override def apply(testThread: Thread): Unit = {
      System.err.println("\n\n\t\tTEST THREAD APPEARS TO BE STUCK")
      Thread.getAllStackTraces.forEach {
        case (thread, trace) =>
          val name = if (thread.getId == testThread.getId) {
            s"TEST THREAD ${thread.getName}"
          } else {
            thread.getName
          }
          System.err.println(name + "\n\t" + trace.mkString("\n\t"))
      }
    }
  }

  implicit val signaler: Signaler = ThreadDumpSignaler

  override def beforeEach(): Unit = {
    super.beforeEach()
    ScalableTaskCompletion.reset()
    TaskRegistryTracker.clearRegistry()
  }

  override def afterEach(): Unit = {
    try {
      ScalableTaskCompletion.reset()
      TaskRegistryTracker.clearRegistry()
    } finally {
      super.afterEach()
    }
  }

  private def registerTaskThread(taskAttemptId: Long): Unit = {
    TrampolineUtil.setTaskContext(new MockTaskContext(taskAttemptId, partitionId = 0))
    TaskPriority.getTaskPriority(taskAttemptId)
    TaskRegistryTracker.registerDedicatedThreadForRetry()
  }

  private def useLimitedRmmPool(limit: Long): Unit = {
    SpillFramework.shutdown()
    if (Rmm.isInitialized) {
      Rmm.shutdown()
    }
    var resource: RmmDeviceMemoryResource = null
    var succeeded = false
    try {
      resource = new RmmCudaMemoryResource()
      resource = new RmmLimitingResourceAdaptor[RmmDeviceMemoryResource](
        resource, limit, rmmAllocationAlignment)
      resource = new RmmTrackingResourceAdaptor[RmmDeviceMemoryResource](
        resource, rmmAllocationAlignment)
      Rmm.setCurrentDeviceResource(resource, null, false)
      succeeded = true
    } finally {
      if (!succeeded && resource != null) {
        resource.close()
      }
    }
    RmmSpark.setEventHandler(new TestRmmEventHandler, "stderr")
  }

  private def restoreDefaultRmm(): Unit = {
    RmmSpark.clearEventHandler()
    if (Rmm.isInitialized) {
      Rmm.shutdown()
    }
    Rmm.initialize(RmmAllocationMode.CUDA_DEFAULT, null, 512L * 1024 * 1024)
    RmmSpark.setEventHandler(new TestRmmEventHandler)
  }

  private class TestRmmEventHandler extends RmmEventHandler {
    override def getAllocThresholds: Array[Long] = null
    override def getDeallocThresholds: Array[Long] = null
    override def onAllocThreshold(totalAllocSize: Long): Unit = {}
    override def onDeallocThreshold(totalAllocSize: Long): Unit = {}
    override def onAllocFailure(sizeRequested: Long, retryCount: Int): Boolean = false
  }

  test("TaskRegistryTracker identifies dedicated task threads") {
    val spark = SparkSession.builder()
        .master("local[1]")
        .appName("BounceBufferPoolSuite")
        .getOrCreate()

    try {
      spark.sparkContext.parallelize(Seq(1), 1).foreachPartition { _ =>
        TaskRegistryTracker.clearRegistry()
        assert(!TaskRegistryTracker.isCurrentThreadRegisteredAsDedicatedTaskThread)

        TaskRegistryTracker.registerThreadForRetry()
        assert(!TaskRegistryTracker.isCurrentThreadRegisteredAsDedicatedTaskThread)

        TaskRegistryTracker.registerDedicatedThreadForRetry()
        assert(TaskRegistryTracker.isCurrentThreadRegisteredAsDedicatedTaskThread)

        TaskRegistryTracker.clearRegistry()
        assert(!TaskRegistryTracker.isCurrentThreadRegisteredAsDedicatedTaskThread)
      }
    } finally {
      spark.stop()
      SparkSession.clearActiveSession()
    }
  }

  test("nextBuffer does not mark unregistered threads as waiting on the RMM pool") {
    val pool = new BounceBufferPool[TestBuffer](1L, 1, _ => new TestBuffer)
    val firstBuffer = pool.nextBuffer()
    var firstBufferReturned = false
    val started = new CountDownLatch(1)
    val acquired = new AtomicReference[BounceBuffer[TestBuffer]]()
    val failure = new AtomicReference[Throwable]()
    val waiter = new Thread("bounce-buffer-pool-test-waiter") {
      override def run(): Unit = {
        started.countDown()
        try {
          acquired.set(pool.nextBuffer())
        } catch {
          case t: Throwable => failure.set(t)
        }
      }
    }

    try {
      waiter.start()
      assert(started.await(5, TimeUnit.SECONDS))
      waitUntilBlockedOrDone(waiter, failure)
      assertNoFailure(failure)

      firstBuffer.close()
      firstBufferReturned = true
      waiter.join(TimeUnit.SECONDS.toMillis(5))

      assert(!waiter.isAlive)
      assertNoFailure(failure)
      assert(acquired.get() != null)
    } finally {
      if (!firstBufferReturned) {
        firstBuffer.close()
      }
      val acquiredBuffer = acquired.get()
      if (acquiredBuffer != null) {
        acquiredBuffer.close()
      }
      pool.close()
      if (waiter.isAlive) {
        waiter.join(TimeUnit.SECONDS.toMillis(5))
      }
    }
  }

  test("RMM retry can proceed while a task waits for a bounce buffer") {
    useLimitedRmmPool(10L * 1024 * 1024)

    val firstAllocationSize = 5L * 1024 * 1024
    val secondAllocationSize = 6L * 1024 * 1024
    val pool = new BounceBufferPool[TestBuffer](1L, 1, _ => new TestBuffer)
    val firstBounceBuffer = new AtomicReference[BounceBuffer[TestBuffer]]()
    val task1AboutToWait = new CountDownLatch(1)
    val task1Done = new CountDownLatch(1)
    val task2Done = new CountDownLatch(1)
    val task2SawRetry = new AtomicBoolean(false)
    val task1Failure = new AtomicReference[Throwable]()
    val task2Failure = new AtomicReference[Throwable]()

    val task1 = new Thread("bounce-buffer-rmm-task-1") {
      override def run(): Unit = {
        var deviceBuffer: DeviceMemoryBuffer = null
        var secondBounceBuffer: BounceBuffer[TestBuffer] = null
        try {
          registerTaskThread(taskAttemptId = 101)
          deviceBuffer = DeviceMemoryBuffer.allocate(firstAllocationSize)
          firstBounceBuffer.set(pool.nextBuffer())
          task1AboutToWait.countDown()
          secondBounceBuffer = pool.nextBuffer()
        } catch {
          case t: Throwable => task1Failure.set(t)
        } finally {
          if (secondBounceBuffer != null) {
            secondBounceBuffer.close()
          }
          val first = firstBounceBuffer.getAndSet(null)
          if (first != null) {
            first.close()
          }
          if (deviceBuffer != null) {
            deviceBuffer.close()
          }
          task1Done.countDown()
        }
      }
    }

    val task2 = new Thread("bounce-buffer-rmm-task-2") {
      override def run(): Unit = {
        try {
          registerTaskThread(taskAttemptId = 202)
          try {
            Arm.withResource(DeviceMemoryBuffer.allocate(secondAllocationSize)) { _ =>
              throw new AssertionError("second allocation unexpectedly succeeded")
            }
          } catch {
            case _: GpuRetryOOM =>
              task2SawRetry.set(true)
          }
        } catch {
          case t: Throwable => task2Failure.set(t)
        } finally {
          val first = firstBounceBuffer.getAndSet(null)
          if (first != null) {
            first.close()
          }
          task2Done.countDown()
        }
      }
    }

    task1.setDaemon(true)
    task2.setDaemon(true)

    try {
      failAfter(Span(20, Seconds)) {
        task1.start()
        assert(task1AboutToWait.await(5, TimeUnit.SECONDS))
        waitUntilBlockedOrDone(task1, task1Failure)
        assertNoFailure(task1Failure)

        task2.start()

        assert(task2Done.await(20, TimeUnit.SECONDS))
        assert(task1Done.await(20, TimeUnit.SECONDS))
        assertNoFailure(task1Failure)
        assertNoFailure(task2Failure)
        assert(task2SawRetry.get())
      }
    } finally {
      val first = firstBounceBuffer.getAndSet(null)
      if (first != null) {
        first.close()
      }
      task1.join(TimeUnit.SECONDS.toMillis(5))
      task2.join(TimeUnit.SECONDS.toMillis(5))
      pool.close()
      TaskRegistryTracker.clearRegistry()
      restoreDefaultRmm()
    }
  }

  private def waitUntilBlockedOrDone(
      thread: Thread,
      failure: AtomicReference[Throwable]): Unit = {
    val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5)
    while (thread.isAlive &&
        failure.get() == null &&
        thread.getState != Thread.State.WAITING &&
        System.nanoTime() < deadline) {
      Thread.sleep(10)
    }
    if (thread.isAlive && failure.get() == null && thread.getState != Thread.State.WAITING) {
      throw new AssertionError(s"${thread.getName} did not block waiting for a bounce buffer")
    }
  }

  private def assertNoFailure(failure: AtomicReference[Throwable]): Unit = {
    val error = failure.get()
    if (error != null) {
      throw error
    }
  }

  private class TestBuffer extends AutoCloseable {
    override def close(): Unit = {}
  }
}
