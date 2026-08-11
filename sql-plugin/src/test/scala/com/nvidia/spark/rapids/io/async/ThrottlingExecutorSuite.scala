/*
 * Copyright (c) 2024-2025, NVIDIA CORPORATION.
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

import java.util.concurrent.{Callable, CountDownLatch, ExecutionException, Executors, Future, RejectedExecutionException, TimeUnit}
import java.util.concurrent.atomic.AtomicLong

import com.nvidia.spark.rapids.{GpuMetric, RapidsConf}
import org.apache.hadoop.conf.Configuration
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.rapids.{GpuWriteJobStatsTracker, GpuWriteTaskStatsTracker}

class ThrottlingExecutorSuite extends AnyFunSuite with BeforeAndAfterEach {

  // Some tests might take longer than usual in the limited CI environment.
  // Use a long timeout to avoid flakiness.
  val longTimeoutSec = 5

  var throttle: HostMemoryThrottle = _
  var trafficController: TrafficController = _
  var executor: ThrottlingExecutor = _

  // Initialize SparkSession to initialize the GpuMetrics.
  SparkSession.builder
       .master("local")
       .appName("ThrottlingExecutorSuite")
       .config(RapidsConf.METRICS_LEVEL.key, "DEBUG")
       .getOrCreate()

  val taskMetrics: Map[String, GpuMetric] = GpuWriteJobStatsTracker.taskMetrics

  class TestTask extends Callable[Unit] {
    val latch = new CountDownLatch(1)
    override def call(): Unit = {
      latch.await()
    }
  }

  override def beforeEach(): Unit = {
    throttle = new HostMemoryThrottle(100)
    trafficController = new TrafficController(throttle)
    executor = new ThrottlingExecutor(
      Executors.newSingleThreadExecutor(),
      trafficController,
      new StatsUpdaterForWriteFunc(
        Seq(new GpuWriteTaskStatsTracker(new Configuration(), taskMetrics))).func
    )
  }

  override def afterEach(): Unit = {
    executor.shutdownNow(longTimeoutSec, TimeUnit.SECONDS)
  }

  test("tasks submitted should update the state") {
    val task1 = new TestTask
    val future1 = executor.submit(task1, 10)
    assertResult(1)(trafficController.numScheduledTasks)
    assertResult(10)(throttle.getTotalHostMemoryBytes)

    val task2 = new TestTask
    val future2 = executor.submit(task2, 20)
    assertResult(2)(trafficController.numScheduledTasks)
    assertResult(30)(throttle.getTotalHostMemoryBytes)

    task1.latch.countDown()
    future1.get(longTimeoutSec, TimeUnit.SECONDS)
    assertResult(1)(trafficController.numScheduledTasks)
    assertResult(20)(throttle.getTotalHostMemoryBytes)

    task2.latch.countDown()
    future2.get(longTimeoutSec, TimeUnit.SECONDS)
    assertResult(0)(trafficController.numScheduledTasks)
    assertResult(0)(throttle.getTotalHostMemoryBytes)
  }

  test("tasks submission fails if totalHostMemoryBytes exceeds maxHostMemoryBytes") {
    val task1 = new TestTask
    val future1 = executor.submit(task1, 10)
    assertResult(1)(trafficController.numScheduledTasks)
    assertResult(10)(throttle.getTotalHostMemoryBytes)

    val task2 = new TestTask
    val task2HostMemory = 100
    val exec = Executors.newSingleThreadExecutor()
    val future2 = exec.submit(new Runnable {
      override def run(): Unit = executor.submit(task2, task2HostMemory)
    })
    Thread.sleep(100)
    assert(!future2.isDone)
    assertResult(1)(trafficController.numScheduledTasks)
    assertResult(10)(throttle.getTotalHostMemoryBytes)

    task1.latch.countDown()
    future1.get(longTimeoutSec, TimeUnit.SECONDS)
    future2.get(longTimeoutSec, TimeUnit.SECONDS)
    assertResult(1)(trafficController.numScheduledTasks)
    assertResult(task2HostMemory)(throttle.getTotalHostMemoryBytes)
  }

  test("submit one task heavier than maxHostMemoryBytes") {
    val future = executor.submit(() => Thread.sleep(10), throttle.maxInFlightHostMemoryBytes + 1)
    future.get(longTimeoutSec, TimeUnit.SECONDS)
    assert(future.isDone)
    assertResult(0)(trafficController.numScheduledTasks)
    assertResult(0)(throttle.getTotalHostMemoryBytes)
  }

  test("submit multiple tasks such that totalHostMemoryBytes does not exceed maxHostMemoryBytes") {
    val numTasks = 10
    val taskRunTime = 10
    var future: Future[Unit] = null
    for (_ <- 0 to numTasks) {
      future = executor.submit(() => Thread.sleep(taskRunTime), 1)
    }
    // Give enough time for all tasks to complete
    future.get(numTasks * taskRunTime * 5, TimeUnit.MILLISECONDS)
    assertResult(0)(trafficController.numScheduledTasks)
    assertResult(0)(throttle.getTotalHostMemoryBytes)
  }

  test("shutdown while a task is blocked") {
    val task1 = new TestTask
    val future1 = executor.submit(task1, 10)
    assertResult(1)(trafficController.numScheduledTasks)
    assertResult(10)(throttle.getTotalHostMemoryBytes)

    val task2 = new TestTask
    val task2HostMemory = 100
    val exec = Executors.newSingleThreadExecutor()
    val future2 = exec.submit(new Runnable {
      override def run(): Unit = executor.submit(task2, task2HostMemory)
    })
    executor.shutdownNow(longTimeoutSec, TimeUnit.SECONDS)

    def assertCause(t: Throwable, cause: Class[_]): Unit = {
      assert(t.getCause != null)
      assert(cause.isInstance(t.getCause))
    }

    val e1 = intercept[ExecutionException](future1.get())
    assertCause(e1, classOf[InterruptedException])
    val e2 = intercept[ExecutionException](future2.get())
    assertCause(e2, classOf[RejectedExecutionException])
  }

  test("task metrics") {
    val exec = Executors.newSingleThreadExecutor()
    // Run a task. Note that the first task never waits in ThrottlingExecutor.
    var runningTask = new TestTask
    exec.submit(new Runnable {
      override def run(): Unit = executor.submit(runningTask, 100)
    })
    var taskCount = 1
    var actualMaxThrottleTimeNs = 0L

    for (i <- 0 to 9) {
      val sleepTimeMs = (i + 1) * 10L
      val waitingTask = new TestTask
      // Latch indicating that the Runnable has been submitted.
      val runnableSubmitted = new CountDownLatch(1)
      // Latch indicating that waitingTask has been submitted to ThrottlingExecutor.
      val waitingTaskSubmitted = new CountDownLatch(1)
      val actualWaitTimeNs = new AtomicLong(0)
      exec.submit(new Runnable {
        override def run(): Unit = {
          runnableSubmitted.countDown()
          val before = System.nanoTime()
          executor.submit(waitingTask, 100)
          actualWaitTimeNs.set(System.nanoTime() - before)
          waitingTaskSubmitted.countDown()
        }
      })
      taskCount += 1
      // Wait until the Runnable is submitted, and then sleep.
      // This is to ensure that the waitingTask will wait for at least sleepTimeMs.
      runnableSubmitted.await(longTimeoutSec, TimeUnit.SECONDS)
      // Let the waitingTask wait for sleepTimeMs.
      Thread.sleep(sleepTimeMs)
      // Finish the running task.
      runningTask.latch.countDown()
      // Wait until the waitingTask is submitted.
      waitingTaskSubmitted.await(longTimeoutSec, TimeUnit.SECONDS)
      actualMaxThrottleTimeNs = math.max(actualMaxThrottleTimeNs, actualWaitTimeNs.get())

      // Skip the check on the min throttle time as the first task never waits.

      // This test incrementally increases the task wait time in a loop, which will update
      // the max throttle time metric in the ThrottlingExecutor. However, it seems possible
      // that the wait time in a previous iteration can be larger than the wait time in the
      // current iteration in some cases, especially when the task has to wait for longer
      // than the given wait time because of some other factors such as limited resources.
      // As such, we compute the actual max wait time (actualMaxThrottleTimeNs) and compare
      // it with the max throttle time metric instead of the current wait time
      // (actualWaitTimeNs.get()).
      assert(actualMaxThrottleTimeNs >=
        taskMetrics(GpuWriteJobStatsTracker.ASYNC_WRITE_MAX_THROTTLE_TIME_KEY).value
      )

      runningTask = waitingTask
    }

    // Finish the last task.
    runningTask.latch.countDown()

    // Verify the average throttle time.
    assert(Seq.range(0, 10).sum * TimeUnit.MILLISECONDS.toNanos(10).toDouble / taskCount <=
      taskMetrics(GpuWriteJobStatsTracker.ASYNC_WRITE_AVG_THROTTLE_TIME_KEY).value)
  }
}
