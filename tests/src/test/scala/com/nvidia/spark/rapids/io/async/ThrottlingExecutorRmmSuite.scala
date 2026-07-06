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

package com.nvidia.spark.rapids.io.async

import java.lang.reflect.InvocationTargetException
import java.util.concurrent.{Callable, CountDownLatch, Executors, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}

import ai.rapids.cudf.{DeviceMemoryBuffer, Rmm, RmmAllocationMode, RmmCudaMemoryResource,
  RmmDeviceMemoryResource, RmmEventHandler, RmmLimitingResourceAdaptor, RmmTrackingResourceAdaptor}
import com.nvidia.spark.rapids.{Arm, RmmSparkRetrySuiteBase, ScalableTaskCompletion,
  TaskRegistryTracker}
import com.nvidia.spark.rapids.jni.{GpuRetryOOM, RmmSpark, TaskPriority}
import com.nvidia.spark.rapids.spill.SpillFramework
import org.scalatest.concurrent.{Signaler, TimeLimits}
import org.scalatest.time.{Seconds, Span}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.rapids.execution.TrampolineUtil
import org.apache.spark.sql.rapids.metrics.source.MockTaskContext

class ThrottlingExecutorRmmSuite extends RmmSparkRetrySuiteBase with TimeLimits {
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

  private def withRmmPoolWaitIfAvailable(body: => Unit): Unit = {
    val waitBody: Function0[Unit] = () => body
    try {
      TaskRegistryTracker.getClass
          .getMethod("withRmmPoolWait", classOf[Function0[_]])
          .invoke(TaskRegistryTracker, waitBody)
    } catch {
      case _: NoSuchMethodException =>
        body
      case e: InvocationTargetException =>
        throw e.getCause
    }
  }

  private def assertNoFailure(failure: AtomicReference[Throwable]): Unit = {
    val t = failure.get()
    if (t != null) {
      throw t
    }
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

  test("throttled submit can wait from a dedicated task thread") {
    val spark = SparkSession.builder()
        .master("local[1]")
        .appName("ThrottlingExecutorRmmSuite")
        .getOrCreate()

    try {
      spark.sparkContext.parallelize(Seq(1), 1).foreachPartition { _ =>
        TaskRegistryTracker.clearRegistry()
        TaskRegistryTracker.registerDedicatedThreadForRetry()

        val throttle = new HostMemoryThrottle(100)
        val trafficController = new TrafficController(throttle)
        val executor = new ThrottlingExecutor(
          Executors.newSingleThreadExecutor(),
          trafficController,
          _ => ())

        val firstStarted = new CountDownLatch(1)
        val releaseFirst = new CountDownLatch(1)
        val releaserFailure = new AtomicReference[Throwable]()
        val releaserDone = new CountDownLatch(1)

        val releaser = new Thread("throttling-executor-rmm-test-releaser") {
          override def run(): Unit = {
            try {
              Thread.sleep(100)
              releaseFirst.countDown()
            } catch {
              case t: Throwable => releaserFailure.set(t)
            } finally {
              releaserDone.countDown()
            }
          }
        }

        try {
          val first = executor.submit(new Callable[Unit] {
            override def call(): Unit = {
              firstStarted.countDown()
              assert(releaseFirst.await(5, TimeUnit.SECONDS))
            }
          }, 100)
          assert(firstStarted.await(5, TimeUnit.SECONDS))

          releaser.start()
          val second = executor.submit(new Callable[Unit] {
            override def call(): Unit = {}
          }, 100)

          second.get(5, TimeUnit.SECONDS)
          first.get(5, TimeUnit.SECONDS)
          assert(releaserDone.await(5, TimeUnit.SECONDS))
          val failure = releaserFailure.get()
          if (failure != null) {
            throw failure
          }
        } finally {
          executor.shutdownNow(5, TimeUnit.SECONDS)
          TaskRegistryTracker.clearRegistry()
        }
      }
    } finally {
      spark.stop()
      SparkSession.clearActiveSession()
    }
  }

  test("RMM retry can proceed while another task waits on a non-RMM pool") {
    useLimitedRmmPool(10L * 1024 * 1024)

    val firstAllocationSize = 5L * 1024 * 1024
    val secondAllocationSize = 6L * 1024 * 1024
    val releasePoolWait = new AtomicBoolean(false)
    val task1InPoolWait = new CountDownLatch(1)
    val task1Done = new CountDownLatch(1)
    val task2Done = new CountDownLatch(1)
    val task2SawRetry = new AtomicBoolean(false)
    val task1Failure = new AtomicReference[Throwable]()
    val task2Failure = new AtomicReference[Throwable]()

    val task1 = new Thread("traffic-controller-rmm-task-1") {
      override def run(): Unit = {
        var task1Buffer: DeviceMemoryBuffer = null
        try {
          registerTaskThread(taskAttemptId = 101)
          task1Buffer = DeviceMemoryBuffer.allocate(firstAllocationSize)

          withRmmPoolWaitIfAvailable {
            task1InPoolWait.countDown()
            while (!releasePoolWait.get()) {
              Thread.`yield`()
            }
          }
        } catch {
          case t: Throwable => task1Failure.set(t)
        } finally {
          if (task1Buffer != null) {
            task1Buffer.close()
          }
          task1Done.countDown()
        }
      }
    }

    val task2 = new Thread("traffic-controller-rmm-task-2") {
      override def run(): Unit = {
        try {
          registerTaskThread(taskAttemptId = 202)
          assert(task1InPoolWait.await(5, TimeUnit.SECONDS))

          try {
            Arm.withResource(DeviceMemoryBuffer.allocate(secondAllocationSize)) { _ =>
              throw new AssertionError("second allocation unexpectedly succeeded")
            }
          } catch {
            case _: GpuRetryOOM =>
              task2SawRetry.set(true)
              releasePoolWait.set(true)
          }
        } catch {
          case t: Throwable => task2Failure.set(t)
        } finally {
          releasePoolWait.set(true)
          task2Done.countDown()
        }
      }
    }

    task1.setDaemon(true)
    task2.setDaemon(true)

    try {
      failAfter(Span(20, Seconds)) {
        task1.start()
        task2.start()

        assert(task2Done.await(20, TimeUnit.SECONDS))
        assert(task1Done.await(20, TimeUnit.SECONDS))
        assertNoFailure(task1Failure)
        assertNoFailure(task2Failure)
        assert(task2SawRetry.get())
      }
    } finally {
      releasePoolWait.set(true)
      try {
        task1.join(TimeUnit.SECONDS.toMillis(5))
        task2.join(TimeUnit.SECONDS.toMillis(5))
      } finally {
        TaskRegistryTracker.clearRegistry()
        restoreDefaultRmm()
      }
    }
  }
}
