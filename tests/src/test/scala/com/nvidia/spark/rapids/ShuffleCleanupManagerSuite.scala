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

package com.nvidia.spark.rapids

import java.util.concurrent.{ConcurrentLinkedQueue, CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.AtomicLong

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import com.nvidia.spark.rapids.ShuffleReclaimOutcome.{ABSENT, FAILED, REMOVED}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.mockito.MockitoSugar.mock

import org.apache.spark.SparkContext

class ShuffleCleanupManagerSuite extends AnyFunSuite with BeforeAndAfterEach {
  private val awaitSeconds = 10L
  private val fiveMinutesMs = TimeUnit.MINUTES.toMillis(5)
  // the scheduled tick never runs within a case, which calls the tick's body itself
  private val oneHourMs = TimeUnit.HOURS.toMillis(1)
  // ids that no count, time or watermark in a log line can match
  private val shuffle1 = 4101
  private val shuffle2 = 4102
  private val shuffle3 = 4103
  private val execution1 = 51L
  private val execution2 = 52L
  private val w1 = 900L
  private val w2 = 950L
  private val managerLogger = Seq(classOf[ShuffleCleanupManager].getName)
  private val managers = new ArrayBuffer[ShuffleCleanupManager]()

  override def afterEach(): Unit = {
    try {
      managers.foreach(_.shutdown())
    } finally {
      managers.clear()
      super.afterEach()
    }
  }

  /**
   * One manager built with its primary constructor, with a fake clock and a driver step that
   * records the shuffle and the thread of each call, then answers from `step`.
   */
  private class Harness(step: Int => Boolean) {
    // a shuffle id whose driver step only shows that the manager's thread reached it
    private val drainId = -1
    @volatile private var drained = new CountDownLatch(1)
    val now = new AtomicLong(0L)
    val steps = new ConcurrentLinkedQueue[(Int, String)]()
    val manager = new ShuffleCleanupManager(
      clock = () => now.get,
      postEvent = _ => (),
      driverStep = shuffleId => {
        if (shuffleId == drainId) {
          drained.countDown()
          false
        } else {
          steps.add((shuffleId, Thread.currentThread().getName))
          step(shuffleId)
        }
      },
      cleanupIntervalMs = oneHourMs)
    managers += manager

    /** Hands an ended execution's shuffles to the manager, and waits for their driver steps. */
    def reclaim(executionId: Long, watermark: Long, shuffleIds: Int*): Unit = {
      manager.reclaimAfterExecution(executionId, shuffleIds, watermark)
      awaitManagerThread()
    }

    /** Waits until the manager's thread has run every driver step handed to it so far. */
    def awaitManagerThread(): Unit = {
      drained = new CountDownLatch(1)
      // the manager's one thread runs what it is handed in order
      manager.reclaimAfterExecution(-1L, Seq(drainId), 0L)
      assert(drained.await(awaitSeconds, TimeUnit.SECONDS), "the manager's thread never ran")
    }

    /** The requests one poll of `executorId` gets, in shuffle id order. */
    def poll(executorId: String): Seq[ShuffleReclaimRequest] =
      manager.handleReclaimPoll(executorId).requests.toSeq.sortBy(_.shuffleId)

    def report(executorId: String, results: ShuffleReclaimResult*): Unit =
      manager.handleReclaimResult(executorId, results.toArray)
  }

  private def result(
      shuffleId: Int,
      watermark: Long,
      outcome: ShuffleReclaimOutcome): ShuffleReclaimResult =
    ShuffleReclaimResult(shuffleId, watermark, outcome, 1, 10L, 1L,
      if (outcome == FAILED) Some("java.lang.IllegalStateException: disk gone") else None)

  private def capture(body: => Unit): Seq[String] =
    LogCaptureUtils.captureLogsFrom(managerLogger)(body).toSeq

  test("the hand-over returns at once, and the driver step runs on the manager's thread") {
    val entered = new CountDownLatch(1)
    val release = new CountDownLatch(1)
    val h = new Harness(_ => {
      entered.countDown()
      release.await(awaitSeconds, TimeUnit.SECONDS)
      true
    })
    try {
      val start = System.nanoTime()
      h.manager.reclaimAfterExecution(execution1, Seq(shuffle1), w1)
      // the listener's thread hands the shuffles over, and must not wait for the driver step
      assert(entered.await(awaitSeconds, TimeUnit.SECONDS), "the driver step never ran")
      assert(System.nanoTime() - start < TimeUnit.SECONDS.toNanos(awaitSeconds))
      assertResult(Seq.empty, "requests open while the driver step runs")(h.poll("e1"))
    } finally {
      release.countDown()
    }
    h.awaitManagerThread()
    assertResult(Seq((shuffle1, "shuffle-cleanup-manager")))(h.steps.asScala.toSeq)
    assertResult(Seq(ShuffleReclaimRequest(shuffle1, w1)))(h.poll("e1"))
  }

  test("a shuffle whose status the driver held goes to every executor at each poll, with its " +
      "execution's watermark, until the executor reports a result for it") {
    val h = new Harness(_ => true)
    h.reclaim(execution1, w1, shuffle1, shuffle2)
    val both = Seq(ShuffleReclaimRequest(shuffle1, w1), ShuffleReclaimRequest(shuffle2, w1))
    assertResult(both)(h.poll("e1"))
    assertResult(both)(h.poll("e1"))
    h.report("e1", result(shuffle1, w1, REMOVED))
    // a result for another watermark settles nothing
    h.report("e1", result(shuffle2, w1 - 1, REMOVED))
    assertResult(Seq(ShuffleReclaimRequest(shuffle2, w1)))(h.poll("e1"))
    assertResult(both)(h.poll("e2"))
    h.report("e2", result(shuffle1, w1, ABSENT), result(shuffle2, w1, ABSENT))
    assertResult(Seq.empty)(h.poll("e2"))
    assertResult(Some((w1, Set("e1", "e2"))))(h.manager.pendingReclaimForTesting(shuffle1))
  }

  test("a failed reclaim counts as the executor's result, and logs one WARN") {
    val h = new Harness(_ => true)
    h.reclaim(execution1, w1, shuffle1)
    val lines = capture(h.report("e1", result(shuffle1, w1, FAILED)))
    assertResult(Seq.empty)(h.poll("e1"))
    val warned = lines.filter(_.startsWith("UCX shuffle reclaim failed on executor e1"))
    assertResult(1, lines.mkString("\n"))(warned.size)
    assert(warned.head.contains(s"$shuffle1") && warned.head.contains("disk gone"), warned.head)
  }

  test("a driver step that finds no status counts the shuffle as already removed, and one that " +
      "throws leaves its shuffle to GC with one WARN, while the other shuffles go on") {
    val h = new Harness(shuffleId =>
      if (shuffleId == shuffle1) {
        false
      } else if (shuffleId == shuffle2) {
        throw new IllegalStateException("injected driver step failure")
      } else {
        true
      })
    val lines = capture(h.reclaim(execution1, w1, shuffle1, shuffle2, shuffle3))
    assertResult(Seq(shuffle1, shuffle2, shuffle3))(h.steps.asScala.map(_._1).toSeq)
    assertResult(Seq(ShuffleReclaimRequest(shuffle3, w1)))(h.poll("e1"))
    val warned = lines.filter(_.startsWith("UCX shuffle reclaim could not clear shuffle"))
    assertResult(1, lines.mkString("\n"))(warned.size)
    assert(warned.head.contains(s"$shuffle2"), warned.head)
  }

  test("a fatal error in a driver step logs one ERROR, and ends that execution's driver steps") {
    val h = new Harness(shuffleId =>
      if (shuffleId == shuffle1) throw new LinkageError("injected fatal error") else true)
    val lines = capture(h.reclaim(execution1, w1, shuffle1, shuffle2))
    assertResult(Seq(shuffle1))(h.steps.asScala.map(_._1).toSeq)
    assertResult(Seq.empty)(h.poll("e1"))
    val logged = lines.filter(_.startsWith(s"UCX shuffle reclaim of SQL execution $execution1"))
    assertResult(1, lines.mkString("\n"))(logged.size)
    assert(logged.head.contains(s"$shuffle1"), logged.head)
  }

  test("a later execution's reclaim of the same shuffle replaces its watermark and asks every " +
      "executor again") {
    val h = new Harness(_ => true)
    h.reclaim(execution1, w1, shuffle1)
    h.report("e1", result(shuffle1, w1, REMOVED))
    assertResult(Seq.empty)(h.poll("e1"))
    h.reclaim(execution2, w2, shuffle1)
    assertResult(Seq(ShuffleReclaimRequest(shuffle1, w2)))(h.poll("e1"))
    // a late result for the replaced watermark leaves the new request open
    h.report("e2", result(shuffle1, w1, REMOVED))
    assertResult(Seq(ShuffleReclaimRequest(shuffle1, w2)))(h.poll("e2"))
  }

  test("a request expires 5 minutes after its driver step, answered or not") {
    val h = new Harness(_ => true)
    h.reclaim(execution1, w1, shuffle1, shuffle2)
    h.report("e1", result(shuffle1, w1, REMOVED))
    h.now.set(fiveMinutesMs)
    h.manager.runTick()
    assert(h.manager.pendingReclaimForTesting(shuffle1).isDefined)
    assert(h.manager.pendingReclaimForTesting(shuffle2).isDefined)
    h.now.set(fiveMinutesMs + 1)
    val lines = capture(h.manager.runTick())
    assertResult((None, None))(
      (h.manager.pendingReclaimForTesting(shuffle1), h.manager.pendingReclaimForTesting(shuffle2)))
    assertResult(Seq.empty)(h.poll("e2"))
    // only the request that no executor answered warns
    val warned = lines.filter(_.contains("with NO executor result reported"))
    assertResult(1, lines.mkString("\n"))(warned.size)
    assert(warned.head.contains(s"$shuffle2"), warned.head)
  }

  test("a held poll hands out no request until the release") {
    val h = new Harness(_ => true)
    h.reclaim(execution1, w1, shuffle1)
    h.manager.holdReclaimPollsForTesting()
    try {
      assertResult(Seq.empty)(h.poll("e1"))
    } finally {
      h.manager.releaseReclaimPollsForTesting()
    }
    assertResult(Seq(ShuffleReclaimRequest(shuffle1, w1)))(h.poll("e1"))
  }

  test("a stopped manager drops a hand-over, and answers a poll with no request") {
    val h = new Harness(_ => true)
    h.reclaim(execution1, w1, shuffle1)
    h.manager.shutdown()
    h.manager.reclaimAfterExecution(execution2, Seq(shuffle2), w2)
    assertResult(Seq.empty)(h.poll("e1"))
  }

  test("the driver plugin answers a UCX reclaim poll with a reclaim response, and a reclaim " +
      "result with null") {
    // another suite in this JVM may leave a manager installed, so only the reply's type counts
    val plugin = new RapidsDriverPlugin()
    val reported = RapidsShuffleReclaimResultMsg("1",
      Array(ShuffleReclaimResult.absent(shuffle1, w1)))
    assert(plugin.receive(reported) == null)
    val reply = plugin.receive(RapidsShuffleReclaimPollMsg("1"))
    assert(reply.isInstanceOf[RapidsShuffleReclaimResponseMsg], reply)
  }

  test("init replaces a manager that another SparkContext built, as when that context's plugin " +
      "init failed before Spark could shut the plugin down") {
    val first = mock[SparkContext]
    val second = mock[SparkContext]
    try {
      ShuffleCleanupManager.init(first)
      val firstManager = ShuffleCleanupManager.get
      ShuffleCleanupManager.init(first)
      assert(ShuffleCleanupManager.get eq firstManager, "init replaced its own context's manager")
      ShuffleCleanupManager.init(second)
      assert(ShuffleCleanupManager.get ne firstManager,
        "the next SparkContext got the manager of a context that never shut it down")
    } finally {
      ShuffleCleanupManager.shutdown()
    }
  }
}
