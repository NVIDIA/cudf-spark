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

import java.util.concurrent.{Callable, CountDownLatch, Executors, TimeoutException, TimeUnit}
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}

import com.nvidia.spark.rapids.{GpuBoundReference, GpuBuildSide, GpuDeviceManager, GpuSemaphore,
  PrioritySemaphore}
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.spill.{SharedRecomputableHandle, SpillUnitTestBase}
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Seconds, Span}

import org.apache.spark.TaskContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.ExprId
import org.apache.spark.sql.rapids.metrics.source.MockTaskContext
import org.apache.spark.sql.types.IntegerType

/**
 * A task holding the GPU semaphore must not wait forever for a shared hash build that a task
 * without the semaphore claimed first.
 */
class HashBuildSemaphoreSuite extends SpillUnitTestBase with Eventually {
  private class TestResource(val id: Int) extends AutoCloseable {
    override def close(): Unit = {}
  }

  private class TestArtifact extends HashArtifact {
    override val stats: JoinBuildSideStats =
      JoinBuildSideStats(streamMagnificationFactor = 1.0, isDistinct = true)
    override def isReady: Boolean = true
    override def backend(
        buildSide: GpuBuildSide,
        metrics: HashBuildMetrics,
        onRebuild: () => Unit): HashProbeBackend =
      throw new UnsupportedOperationException("not needed by this test")
    override def close(): Unit = {}
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    // The semaphore initializes the GPU for a task from the SparkEnv.
    SparkSession.builder.master("local[1]").appName(getClass.getSimpleName).getOrCreate()
    GpuDeviceManager.setRmmTaskInitEnabled(false)
    GpuSemaphore.shutdown()
    GpuSemaphore.initialize(maxConcurrentGpuTasksLimit = 1)
  }

  override def afterEach(): Unit = {
    GpuSemaphore.shutdown()
    SparkSession.getActiveSession.foreach(_.stop())
    SparkSession.clearActiveSession()
    super.afterEach()
  }

  /** Like the real builds, which need the GPU to read the spillable build batch. */
  private def onGpu[T](build: => T): T = {
    GpuSemaphore.acquireIfNecessary(TaskContext.get())
    build
  }

  private def asTask[T](context: TaskContext)(body: => T): T = {
    TrampolineUtil.setTaskContext(context)
    try body finally TrampolineUtil.unsetTaskContext()
  }

  /**
   * Calls `claim` from two tasks while only one task can hold the GPU semaphore. The task
   * without the semaphore calls first and waits on the semaphore, then the task holding the
   * semaphore calls. The holder must not end up waiting for a build that the other task claimed
   * but cannot run.
   *
   * @return the results of the holder and of the other task
   */
  private def claimFromTwoTasks[T](claim: () => T): (T, T) = {
    val holderContext = new MockTaskContext(taskAttemptId = 1, partitionId = 0)
    val otherContext = new MockTaskContext(taskAttemptId = 2, partitionId = 1)
    val holderOnGpu = new CountDownLatch(1)
    val holderMayClaim = new CountDownLatch(1)
    val otherThread = new AtomicReference[Thread]()
    val pool = Executors.newFixedThreadPool(2)
    try {
      val holder = pool.submit(new Callable[T] {
        override def call(): T = asTask(holderContext) {
          GpuSemaphore.acquireIfNecessary(holderContext)
          holderOnGpu.countDown()
          assert(holderMayClaim.await(30, TimeUnit.SECONDS))
          try claim() finally GpuSemaphore.releaseIfNecessary(holderContext)
        }
      })
      assert(holderOnGpu.await(30, TimeUnit.SECONDS))

      val other = pool.submit(new Callable[T] {
        override def call(): T = asTask(otherContext) {
          otherThread.set(Thread.currentThread())
          try claim() finally GpuSemaphore.releaseIfNecessary(otherContext)
        }
      })
      eventually(timeout(Span(30, Seconds))) {
        val thread = otherThread.get()
        assert(thread != null && thread.getState == Thread.State.WAITING)
        assert(thread.getStackTrace.exists(
          _.getClassName == classOf[PrioritySemaphore[_]].getName))
      }

      holderMayClaim.countDown()
      val holderResult = try {
        holder.get(30, TimeUnit.SECONDS)
      } catch {
        case _: TimeoutException =>
          fail("deadlock: the task holding the GPU semaphore is stuck behind the task without it")
      }
      (holderResult, other.get(30, TimeUnit.SECONDS))
    } finally {
      holderMayClaim.countDown()
      // Completing the holder releases its semaphore, which also ends a deadlock.
      holderContext.markTaskComplete()
      pool.shutdown()
      assert(pool.awaitTermination(30, TimeUnit.SECONDS))
      otherContext.markTaskComplete()
    }
  }

  test("a rebuild after eviction does not deadlock with a task holding the GPU semaphore") {
    val builds = new AtomicInteger()
    def build(): TestResource = new TestResource(builds.incrementAndGet())

    withResource(SharedRecomputableHandle(1024L, build())(onGpu(build()))) { handle =>
      assertResult(handle.approxSizeInBytes)(handle.spill())
      handle.releaseSpilled()

      val (holder, other) = claimFromTwoTasks { () =>
        withResource(handle.acquire())(_.resource.id)
      }
      assertResult((2, 2))((holder, other))
      // The initial build and exactly one rebuild.
      assertResult(2)(builds.get())
    }
  }

  test("an initial build does not deadlock with a task holding the GPU semaphore") {
    val cache = new HashBuildCache
    val key = HashBuildKey(
      sourceProjection = Seq.empty,
      projectedKeys = Seq(GpuBoundReference(0, IntegerType, nullable = false)(ExprId(0), "key")),
      compareNullsEqual = false,
      filterOutNulls = false)
    val artifact = new TestArtifact
    val builds = new AtomicInteger()
    try {
      val (holder, other) = claimFromTwoTasks { () =>
        cache.getOrBuild(key, HashBuildMetrics()) {
          onGpu {
            builds.incrementAndGet()
            artifact
          }
        }._1
      }
      assert(holder eq artifact)
      assert(other eq artifact)
      assertResult(1)(builds.get())
    } finally {
      cache.close()
    }
  }
}
