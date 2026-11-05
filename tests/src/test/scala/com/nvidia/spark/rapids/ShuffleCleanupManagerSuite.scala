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

import java.util.concurrent.{ConcurrentLinkedQueue, CountDownLatch, LinkedBlockingQueue,
  TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong, AtomicReference}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.util.Try

import com.nvidia.spark.rapids.ShuffleCleanupManager.{DRIVER_STEP_WARN_PREFIX,
  FAILED_WARN_PREFIX, FENCE_TIMEOUT_WARN_PREFIX, IGNORED_RESULT_INFO_PREFIX,
  TIMEOUT_WARN_PREFIX}
import com.nvidia.spark.rapids.ShuffleReclaimOutcome.{DEFERRED, FAILED, REMOVED}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.scheduler.SparkListenerEvent
import org.apache.spark.sql.rapids.{RapidsShuffleReclaimFence, ShuffleCleanupListener,
  ShuffleReclaimDecision, ShuffleReclaimHandoff, ShuffleReclaimRule}

class ShuffleCleanupManagerSuite extends AnyFunSuite with BeforeAndAfterEach {
  private val awaitSeconds = 10L
  private val minuteMs = TimeUnit.MINUTES.toMillis(1)
  private val tenMinutesMs = 10 * minuteMs
  // the scheduled tick never runs within a case, which calls the tick's body itself
  private val oneHourMs = TimeUnit.HOURS.toMillis(1)
  // a held poll, and the calls beside it, must not wait on anything
  private val heldPollBoundMs = 1000L
  // the calls made while a driver step waits must not wait for it
  private val stepBoundMs = 5000L
  // ids that no count, time or watermark in a log line can match
  private val shuffle1 = 4101
  private val shuffle2 = 4102
  private val shuffle3 = 4103
  private val shuffle4 = 4104
  private val shuffle5 = 4105
  private val shuffle6 = 4106
  private val multithreadedShuffle = 4199
  private val execution1 = 51L
  private val execution2 = 52L
  private val execution3 = 53L
  private val w1 = 900L
  private val w2 = 950L
  private val elapsedNanos = 1000L
  private val failure = "java.lang.IllegalStateException: disk gone"
  private val managerLogger = Seq(classOf[ShuffleCleanupManager].getName)
  private val warnPrefixes = Seq(FAILED_WARN_PREFIX, DRIVER_STEP_WARN_PREFIX,
    FENCE_TIMEOUT_WARN_PREFIX, TIMEOUT_WARN_PREFIX)
  private val managers = new ArrayBuffer[ShuffleCleanupManager]()

  override def afterEach(): Unit = {
    try {
      managers.foreach(_.shutdown())
    } finally {
      managers.clear()
      super.afterEach()
    }
  }

  /** What one minute of a timeline logged and posted. */
  private class Minute(
      val minute: Int,
      val lines: Seq[String],
      val posted: Seq[SparkRapidsShuffleReclaimEvent])

  /**
   * One manager built with its primary constructor, with a fake clock, an event sink that sorts
   * what the manager posts into queues, a driver step that records each shuffle it is called
   * for, and a drop counter that stays 0 unless a case raises it.
   *
   * @param step the driver step's answer for each shuffle id
   * @param eventLogEnabled the event key
   */
  private class Harness(step: Int => Boolean, eventLogEnabled: Boolean) {
    val now = new AtomicLong(0L)
    val disabled = new AtomicBoolean(false)
    val dropped = new AtomicLong(0L)
    // set to make each fence's marker post throw
    val failMarkers = new AtomicBoolean(false)
    val steps = new ConcurrentLinkedQueue[Int]()
    val fences = new LinkedBlockingQueue[RapidsShuffleReclaimFence]()
    val events = new LinkedBlockingQueue[SparkRapidsShuffleReclaimEvent]()
    val others = new LinkedBlockingQueue[SparkListenerEvent]()
    // each poll's executor and its requests, in shuffle id order
    val replies = new ArrayBuffer[(String, Seq[ShuffleReclaimRequest])]()
    val manager = new ShuffleCleanupManager(
      clock = () => now.get,
      postEvent = {
        case fence: RapidsShuffleReclaimFence =>
          if (failMarkers.get) {
            throw new IllegalStateException("injected failure to post a marker")
          }
          fences.put(fence)
        case event: SparkRapidsShuffleReclaimEvent => events.put(event)
        case other => others.put(other)
      },
      driverStep = shuffleId => {
        steps.add(shuffleId)
        step(shuffleId)
      },
      eventLogEnabled = eventLogEnabled,
      disabled = disabled,
      droppedEvents = () => dropped.get,
      cleanupIntervalMs = oneHourMs)
    managers += manager

    def dispatch(fenceId: Long, decisions: ShuffleReclaimDecision*): Unit =
      manager.dispatch(fenceId, decisions, now.get)

    /** Waits for the fence's marker, which the manager posts after the fence's driver steps. */
    def awaitFence(fenceId: Long): Unit = {
      val fence = fences.poll(awaitSeconds, TimeUnit.SECONDS)
      assert(fence != null, s"fence $fenceId posted no marker within $awaitSeconds s")
      assertResult(fenceId)(fence.fenceId)
    }

    /** Dispatches one fence, waits for its marker, and releases each of its shuffles. */
    def dispatchAndRelease(fenceId: Long, decisions: ShuffleReclaimDecision*): Unit = {
      dispatch(fenceId, decisions: _*)
      awaitFence(fenceId)
      decisions.foreach(d => manager.applyRelease(fenceId, d.shuffleId))
    }

    /** Polls through the method that `RapidsDriverPlugin.receive` calls. */
    def poll(executorId: String): Seq[ShuffleReclaimRequest] = {
      val requests = manager.handleReclaimPoll(executorId).requests.toList.sortBy(_.shuffleId)
      replies += (executorId -> requests)
      requests
    }

    /** Reports through the method that `RapidsDriverPlugin.receive` calls. */
    def report(executorId: String, results: ShuffleReclaimResult*): Unit =
      manager.handleReclaimResult(executorId, results.toArray)

    /** The event that a call on the test thread has just posted. */
    def takeEvent(): SparkRapidsShuffleReclaimEvent = {
      val event = events.poll()
      assert(event != null, "no reclaim event was posted")
      event
    }

    /** The next event, which the manager's thread may post. */
    def awaitEvent(): SparkRapidsShuffleReclaimEvent = {
      val event = events.poll(awaitSeconds, TimeUnit.SECONDS)
      assert(event != null, s"no reclaim event was posted within $awaitSeconds s")
      event
    }

    def drainEvents(): Seq[SparkRapidsShuffleReclaimEvent] =
      Iterator.continually(events.poll()).takeWhile(_ != null).toList

    /**
     * Sets the clock to each minute in turn, runs that minute's work and then the timeout check,
     * and records what the minute logged and posted.
     */
    def everyMinute(minutes: Range)(work: Int => Unit): Seq[Minute] = minutes.map { minute =>
      now.set(minute * minuteMs)
      val lines = capture {
        work(minute)
        manager.checkReclaimTimeouts()
      }
      new Minute(minute, lines, drainEvents())
    }
  }

  private def decision(
      shuffleId: Int,
      owner: Long,
      watermark: Long,
      targets: String*): ShuffleReclaimDecision =
    ShuffleReclaimDecision(shuffleId, owner, watermark, targets.toSet)

  private def removed(
      shuffleId: Int,
      watermark: Long,
      buffers: Int,
      bytes: Long): ShuffleReclaimResult =
    ShuffleReclaimResult(shuffleId, watermark, REMOVED, buffers, bytes, elapsedNanos, None)

  /** What `body` logs. `LogCaptureUtils` returns messages at DEBUG and above, without levels. */
  private def capture(body: => Unit): Seq[String] =
    LogCaptureUtils.captureLogsFrom(managerLogger)(body).toSeq

  /** Whether `line` holds each token as a whole word, as when it names a shuffle or executor. */
  private def names(line: String, tokens: Any*): Boolean = {
    val words = line.split("\\W+").toSet
    tokens.forall(token => words.contains(token.toString))
  }

  private def isWarnLine(line: String): Boolean =
    warnPrefixes.exists(prefix => line.startsWith(prefix))

  /** Whether `line` is one of the manager's WARN lines, or its INFO line for a result. */
  private def isReclaimLine(line: String): Boolean =
    isWarnLine(line) || line.startsWith(IGNORED_RESULT_INFO_PREFIX)

  /** Each timeout WARN of a timeline, with the minute that logged it. */
  private def timeoutWarnings(timeline: Seq[Minute]): Seq[(Int, String)] =
    for (m <- timeline; line <- m.lines if line.startsWith(TIMEOUT_WARN_PREFIX))
      yield (m.minute, line)

  private def postingMinutes(timeline: Seq[Minute]): Seq[Int] =
    timeline.filter(_.posted.nonEmpty).map(_.minute)

  /** Runs `body` on a new thread, and fails unless it returns within `millis`. */
  private def onAnotherThread[T](millis: Long)(body: => T): T = {
    val result = new AtomicReference[Try[T]]()
    val thread = new Thread(() => result.set(Try(body)), "shuffle-cleanup-manager-suite")
    thread.setDaemon(true)
    thread.start()
    thread.join(millis)
    assert(!thread.isAlive, s"the call did not return within $millis ms")
    result.get.get
  }

  test("a released request goes only to its targets, with its watermark, and each poll gets " +
      "only the polling executor's requests") {
    val h = new Harness(_ => true, eventLogEnabled = true)
    h.dispatchAndRelease(1L, decision(shuffle1, execution1, w1, "1", "2"))
    h.dispatchAndRelease(2L, decision(shuffle2, execution1, w2, "2", "3"))
    assertResult(Seq(ShuffleReclaimRequest(shuffle1, w1)))(h.poll("1"))
    assertResult(Seq(ShuffleReclaimRequest(shuffle1, w1), ShuffleReclaimRequest(shuffle2, w2)))(
      h.poll("2"))
    assertResult(Seq(ShuffleReclaimRequest(shuffle2, w2)))(h.poll("3"))
    assert(h.poll("4").isEmpty, "an executor that is no target got a request")
  }

  test("a repeat request leaves the older watermark deliverable until its own fence releases, " +
      "and then asks every target of both requests with the newer one") {
    val h = new Harness(_ => true, eventLogEnabled = true)
    h.dispatchAndRelease(1L, decision(shuffle1, execution1, w1, "1", "2"))
    h.dispatch(2L, decision(shuffle1, execution2, w2, "2", "3"))
    h.awaitFence(2L)
    val older = Seq(ShuffleReclaimRequest(shuffle1, w1))
    assertResult(older)(h.poll("1"))
    assertResult(older)(h.poll("2"))
    assert(h.poll("3").isEmpty, "a target of the fenced repeat request got the shuffle")
    h.manager.applyRelease(2L, shuffle1)
    Seq("1", "2", "3").foreach { executorId =>
      assertResult(Seq(ShuffleReclaimRequest(shuffle1, w2)), executorId)(h.poll(executorId))
    }
  }

  test("MULTITHREADED's cleanup polls, stats and events are unchanged beside UCX requests") {
    val h = new Harness(_ => true, eventLogEnabled = true)
    h.dispatchAndRelease(1L, decision(shuffle1, execution1, w1, "1"))
    h.manager.registerForCleanup(multithreadedShuffle)
    assertResult(Seq(multithreadedShuffle))(h.manager.handlePoll("1").shuffleIds.toSeq)
    h.manager.handleStats("1", Array(ShuffleCleanupStats(multithreadedShuffle, 10L, 0L)))
    assert(h.manager.handlePoll("1").shuffleIds.isEmpty, "executor 1 has reported its stats")
    assertResult(Seq(multithreadedShuffle))(h.manager.handlePoll("2").shuffleIds.toSeq)
    assertResult(SparkRapidsShuffleDiskSavingsEvent(multithreadedShuffle, 10L, 0L))(
      h.others.poll())
    assertResult(Seq(ShuffleReclaimRequest(shuffle1, w1)))(h.poll("1"))
  }

  /**
   * Execution 1 owns shuffle 1, with targets e1 to e4, and shuffle 2, with target e1. e1 answers
   * shuffle 2, then the targets of shuffle 1 answer REMOVED, ABSENT, DEFERRED and FAILED, in that
   * order, and no event is posted before the last answer. Returns what the answers logged.
   */
  private def answerEachOutcome(h: Harness): Seq[String] = {
    h.manager.executionClosed(execution1, 0L, Seq(shuffle1, shuffle2))
    h.dispatchAndRelease(1L, decision(shuffle1, execution1, w1, "e1", "e2", "e3", "e4"),
      decision(shuffle2, execution1, w1, "e1"))
    assertResult(Seq(shuffle1, shuffle2))(h.poll("e1").map(_.shuffleId))
    Seq("e2", "e3", "e4").foreach { executorId =>
      assertResult(Seq(ShuffleReclaimRequest(shuffle1, w1)), executorId)(h.poll(executorId))
    }
    val answers = Seq(
      "e1" -> removed(shuffle2, w1, 1, 50L),
      "e1" -> removed(shuffle1, w1, 3, 300L),
      "e2" -> ShuffleReclaimResult.absent(shuffle1, w1),
      "e3" -> ShuffleReclaimResult(shuffle1, w1, DEFERRED, 0, 0L, elapsedNanos, None),
      "e4" -> ShuffleReclaimResult(shuffle1, w1, FAILED, 0, 0L, elapsedNanos, Some(failure)))
    val lines = answers.flatMap { case (executorId, result) =>
      assert(h.events.isEmpty, s"an event was posted before $executorId answered")
      capture(h.report(executorId, result))
    }
    Seq("e1", "e2", "e3", "e4").foreach { executorId =>
      assert(h.poll(executorId).isEmpty, s"$executorId got a shuffle it answered")
    }
    lines
  }

  test("each target's outcome counts once in its execution's event, which posts after the last " +
      "outcome, and a FAILED result logs one WARN naming the shuffle, the executor and the " +
      "error") {
    val h = new Harness(_ => true, eventLogEnabled = true)
    val lines = answerEachOutcome(h)
    val event = h.takeEvent()
    assert(h.events.isEmpty, "the execution's event was posted twice")
    assertResult((execution1, 2, 2, 0))(
      (event.executionId, event.shuffles, event.reclaimed, event.alreadyRemoved))
    // failed, timed out, never polled and deferred
    assertResult((1, 0, 0, 1))((event.executorsFailed, event.executorsTimedOut,
      event.executorsNeverPolled, event.executorsDeferred))
    assertResult((4L, 350L))((event.buffersFreed, event.bytesFreed))
    val warned = lines.filter(_.startsWith(FAILED_WARN_PREFIX))
    assertResult(1, lines.mkString("\n"))(warned.length)
    assert(names(warned.head, shuffle1, "e4") && warned.head.contains(failure), warned.head)
  }

  test("with the event key off no event is posted, and the poll replies and WARN lines match " +
      "those with the key on") {
    val on = new Harness(_ => true, eventLogEnabled = true)
    val off = new Harness(_ => true, eventLogEnabled = false)
    val onLines = answerEachOutcome(on).filter(isReclaimLine)
    val offLines = answerEachOutcome(off).filter(isReclaimLine)
    assertResult(1)(on.events.size)
    assert(off.events.isEmpty, "an event was posted with the event key off")
    assertResult(on.replies)(off.replies)
    assert(onLines.nonEmpty, "the FAILED result logged no WARN")
    assertResult(onLines)(offLines)
  }

  test("a result is applied on the thread that delivers it while the manager's thread is busy, " +
      "and the same result again changes nothing") {
    val entered = new CountDownLatch(1)
    val release = new CountDownLatch(1)
    val h = new Harness(shuffleId => {
      if (shuffleId == shuffle2) {
        entered.countDown()
        release.await(awaitSeconds, TimeUnit.SECONDS)
      }
      true
    }, eventLogEnabled = true)
    try {
      h.manager.executionClosed(execution1, 0L, Seq(shuffle1))
      h.dispatchAndRelease(1L, decision(shuffle1, execution1, w1, "1", "2"))
      val handed = Seq(ShuffleReclaimRequest(shuffle1, w1))
      assertResult(handed)(h.poll("1"))
      h.dispatch(2L, decision(shuffle2, execution2, w1, "1"))
      assert(entered.await(awaitSeconds, TimeUnit.SECONDS), "the driver step never started")
      val result = removed(shuffle1, w1, 1, 10L)
      h.report("1", result)
      assert(h.poll("1").isEmpty, "the result waited for the manager's thread")
      assertResult(handed)(h.poll("2"))
      h.report("1", result)
      assert(h.poll("1").isEmpty, "the same result again reopened its target")
      assert(h.events.isEmpty, "target 2 has not answered")
      h.report("2", removed(shuffle1, w1, 2, 20L))
      val event = h.takeEvent()
      assertResult((3L, 30L))((event.buffersFreed, event.bytesFreed))
    } finally {
      release.countDown()
    }
  }

  test("the dispatch records its request on the calling thread, so a removal and a poll before " +
      "its driver step runs neither wait for the manager's thread nor get lost") {
    val entered = new CountDownLatch(1)
    val release = new CountDownLatch(1)
    val stepReturned = new AtomicBoolean(false)
    // shuffle 2's driver step keeps the manager's thread busy, so shuffle 1's step waits behind it
    val h = new Harness(shuffleId => {
      if (shuffleId == shuffle2) {
        entered.countDown()
        release.await(awaitSeconds, TimeUnit.SECONDS)
        stepReturned.set(true)
      }
      true
    }, eventLogEnabled = true)
    try {
      h.manager.executionClosed(execution1, 0L, Seq(shuffle1))
      h.dispatch(1L, decision(shuffle2, execution2, w1, "1"))
      assert(entered.await(awaitSeconds, TimeUnit.SECONDS), "the driver step never started")
      val reply = onAnotherThread(stepBoundMs) {
        h.dispatch(2L, decision(shuffle1, execution1, w1, "1", "2"))
        h.manager.executorRemoved("2")
        h.manager.handleReclaimPoll("1")
      }
      assert(!stepReturned.get, "the driver step returned before its latch opened")
      assert(reply.requests.isEmpty, "a poll got a shuffle whose driver step had not run")
    } finally {
      release.countDown()
    }
    h.awaitFence(1L)
    h.awaitFence(2L)
    h.manager.applyRelease(2L, shuffle1)
    assertResult(Seq(ShuffleReclaimRequest(shuffle1, w1)))(h.poll("1"))
    h.report("1", removed(shuffle1, w1, 1, 10L))
    val event = h.takeEvent()
    assert(h.events.isEmpty, "the execution's event was posted twice")
    assertResult((execution1, 0, 0))(
      (event.executionId, event.executorsTimedOut, event.executorsNeverPolled))
    assertResult(0L)(h.now.get)
  }

  /**
   * Execution 1 owns six shuffles: shuffle 1, cleared with targets e1 and e2, shuffle 2, which its
   * driver step finds removed, and one skip under each of R2, R4, R5 and R7. e1 answers shuffle 1,
   * then `prepare` runs, and `last`, which settles e2's target, runs twice. Returns the events
   * that the first `last` posted.
   */
  private def settleLastTarget(
      h: Harness,
      prepare: Harness => Unit,
      last: Harness => Unit): Seq[SparkRapidsShuffleReclaimEvent] = {
    h.manager.skip(shuffle3, execution1, ShuffleReclaimRule.R2)
    h.manager.skip(shuffle4, execution1, ShuffleReclaimRule.R4)
    h.manager.skip(shuffle5, execution1, ShuffleReclaimRule.R5)
    h.manager.skip(shuffle6, execution1, ShuffleReclaimRule.R7)
    h.dispatchAndRelease(1L, decision(shuffle1, execution1, w1, "e1", "e2"),
      decision(shuffle2, execution1, w1, "e1"))
    h.manager.executionClosed(execution1, 0L,
      Seq(shuffle1, shuffle2, shuffle3, shuffle4, shuffle5, shuffle6))
    assertResult(Seq(ShuffleReclaimRequest(shuffle1, w1)))(h.poll("e1"))
    h.report("e1", removed(shuffle1, w1, 2, 200L))
    prepare(h)
    assert(h.events.isEmpty, "the event was posted before e2's target settled")
    last(h)
    val posted = h.drainEvents()
    last(h)
    assert(h.events.isEmpty, "repeating the call that settled the last target posted again")
    Seq("e1", "e2").foreach { executorId =>
      assert(h.poll(executorId).isEmpty, s"$executorId got a settled shuffle")
    }
    posted
  }

  /**
   * Runs `settleLastTarget` with the event key on, where it must post one event that counts its
   * inputs, and with the key off, where it must post none and get the same poll replies.
   */
  private def assertLastTargetPostsOnce(timedOut: Int)(prepare: Harness => Unit)(
      last: Harness => Unit): Unit = {
    val on = new Harness(_ != shuffle2, eventLogEnabled = true)
    val posted = settleLastTarget(on, prepare, last)
    assertResult(1, posted)(posted.size)
    val event = posted.head
    assertResult((execution1, 6, 1, 1))(
      (event.executionId, event.shuffles, event.reclaimed, event.alreadyRemoved))
    // the shared, unconsumed, cached and indeterminate lists
    assertResult((Seq(shuffle3), Seq(shuffle4), Seq(shuffle5), Seq(shuffle6)))(
      (event.skippedShared, event.skippedUnconsumed, event.skippedCached,
        event.skippedIndeterminate))
    assertResult(timedOut)(event.executorsTimedOut)
    val off = new Harness(_ != shuffle2, eventLogEnabled = false)
    assert(settleLastTarget(off, prepare, last).isEmpty, "an event was posted with the key off")
    assertResult(on.replies)(off.replies)
  }

  test("a result that settles an execution's last target posts its event once, and with the " +
      "event key off nothing") {
    assertLastTargetPostsOnce(timedOut = 0)(
      h => assertResult(Seq(ShuffleReclaimRequest(shuffle1, w1)))(h.poll("e2")))(
      h => h.report("e2", ShuffleReclaimResult.absent(shuffle1, w1)))
  }

  test("an executor removal that settles an execution's last target posts its event once, and " +
      "with the event key off nothing") {
    assertLastTargetPostsOnce(timedOut = 0)(_ => ())(_.manager.executorRemoved("e2"))
  }

  test("a timeout in the tick that settles an execution's last target posts its event once, " +
      "and with the event key off nothing") {
    assertLastTargetPostsOnce(timedOut = 1)(_.now.set(11 * minuteMs))(_.manager.runTick())
  }

  test("an execution's event lists a repeated R7 skip once, and each rule's skips in that " +
      "rule's list") {
    val h = new Harness(_ => true, eventLogEnabled = true)
    // a retry attempt of the map stage under the same owner repeats the skip
    h.manager.skip(shuffle1, execution1, ShuffleReclaimRule.R7)
    h.manager.skip(shuffle1, execution1, ShuffleReclaimRule.R7)
    h.dispatchAndRelease(1L, decision(shuffle2, execution1, w1, "e1", "e2"))
    Seq("e1", "e2").foreach { executorId =>
      assertResult(Seq(ShuffleReclaimRequest(shuffle2, w1)), executorId)(h.poll(executorId))
      h.report(executorId, removed(shuffle2, w1, 1, 10L))
    }
    assert(h.events.isEmpty, "an event was posted before its execution closed")
    h.manager.executionClosed(execution1, 0L, Seq(shuffle1, shuffle2))
    val event = h.takeEvent()
    assertResult((execution1, 2, 1))((event.executionId, event.shuffles, event.reclaimed))
    assertResult((Seq.empty[Int], Seq.empty[Int], Seq.empty[Int], Seq(shuffle1)))(
      (event.skippedShared, event.skippedUnconsumed, event.skippedCached,
        event.skippedIndeterminate))
    h.manager.skip(shuffle3, execution2, ShuffleReclaimRule.R2)
    h.manager.skip(shuffle4, execution2, ShuffleReclaimRule.R4)
    h.manager.skip(shuffle5, execution2, ShuffleReclaimRule.R5)
    h.manager.executionClosed(execution2, 0L, Seq(shuffle3, shuffle4, shuffle5))
    val byRule = h.takeEvent()
    assertResult((execution2, 3, 0))((byRule.executionId, byRule.shuffles, byRule.reclaimed))
    assertResult((Seq(shuffle3), Seq(shuffle4), Seq(shuffle5), Seq.empty[Int]))(
      (byRule.skippedShared, byRule.skippedUnconsumed, byRule.skippedCached,
        byRule.skippedIndeterminate))
  }

  test("a repeat request credits the earlier owner with what its targets reported, and a later " +
      "result only the new owner") {
    val h = new Harness(_ => true, eventLogEnabled = true)
    h.manager.executionClosed(execution1, 0L, Seq(shuffle1))
    h.dispatchAndRelease(1L, decision(shuffle1, execution1, w1, "1", "2"))
    assertResult(Seq(ShuffleReclaimRequest(shuffle1, w1)))(h.poll("1"))
    h.report("1", removed(shuffle1, w1, 3, 300L))
    assert(h.events.isEmpty, "target 2 of the first request has not answered")
    h.manager.executionClosed(execution2, 0L, Seq(shuffle1))
    h.dispatchAndRelease(2L, decision(shuffle1, execution2, w2, "2", "3"))
    val earlier = h.takeEvent()
    assertResult((execution1, 1, 3L, 300L, 0))((earlier.executionId, earlier.reclaimed,
      earlier.buffersFreed, earlier.bytesFreed, earlier.executorsTimedOut))
    Seq("1", "2", "3").foreach { executorId =>
      assertResult(Seq(ShuffleReclaimRequest(shuffle1, w2)), executorId)(h.poll(executorId))
    }
    h.report("2", removed(shuffle1, w2, 20, 2000L))
    h.report("1", ShuffleReclaimResult.absent(shuffle1, w2))
    h.report("3", ShuffleReclaimResult.absent(shuffle1, w2))
    val later = h.takeEvent()
    assert(h.events.isEmpty, "an execution's event was posted twice")
    assertResult((execution2, 1, 20L, 2000L))(
      (later.executionId, later.reclaimed, later.buffersFreed, later.bytesFreed))
  }

  test("a stopped manager ignores every call, answers a poll with no request and posts nothing") {
    val h = new Harness(_ => true, eventLogEnabled = true)
    h.manager.shutdown()
    h.manager.skip(shuffle2, execution1, ShuffleReclaimRule.R4)
    h.manager.executionClosed(execution1, 0L, Seq(shuffle1, shuffle2))
    h.dispatch(1L, decision(shuffle1, execution1, w1, "1"))
    h.manager.executorRemoved("1")
    assert(h.poll("1").isEmpty, "a stopped manager handed out a request")
    h.report("1", removed(shuffle1, w1, 1, 10L))
    assert(h.steps.isEmpty, "a stopped manager ran a driver step")
    assert(h.fences.isEmpty && h.events.isEmpty && h.others.isEmpty, "a stopped manager posted")
  }

  test("the driver plugin answers a UCX reclaim poll with a reclaim response, and a reclaim " +
      "result with null") {
    // another suite in this JVM may leave a manager installed, so only the reply's type counts
    val plugin = new RapidsDriverPlugin()
    val result = RapidsShuffleReclaimResultMsg("1",
      Array(ShuffleReclaimResult.absent(shuffle1, w1)))
    assert(plugin.receive(result) == null)
    val reply = plugin.receive(RapidsShuffleReclaimPollMsg("1"))
    assert(reply.isInstanceOf[RapidsShuffleReclaimResponseMsg], reply)
  }

  test("a held poll returns at once with no request while results and MULTITHREADED polls go " +
      "on, and the first poll after the release gets the request") {
    val h = new Harness(_ => true, eventLogEnabled = true)
    h.manager.registerForCleanup(multithreadedShuffle)
    h.manager.executionClosed(execution2, 0L, Seq(shuffle2))
    h.dispatchAndRelease(1L, decision(shuffle1, execution1, w1, "1"),
      decision(shuffle2, execution2, w1, "2"))
    assert(h.manager.isReclaimReleasedForTesting(shuffle1))
    assertResult(Seq(ShuffleReclaimRequest(shuffle2, w1)))(h.poll("2"))
    h.manager.holdReclaimPollsForTesting()
    try {
      val held = onAnotherThread(heldPollBoundMs)(h.manager.handleReclaimPoll("1"))
      assert(held.requests.isEmpty, "a held poll handed out a request")
      val cleanup = onAnotherThread(heldPollBoundMs) {
        h.report("2", removed(shuffle2, w1, 1, 10L))
        h.manager.handlePoll("1")
      }
      assertResult(Seq(multithreadedShuffle))(cleanup.shuffleIds.toSeq)
      // the result settled the only shuffle that execution 2 owns
      assertResult(execution2)(h.takeEvent().executionId)
    } finally {
      h.manager.releaseReclaimPollsForTesting()
    }
    assertResult(Seq(ShuffleReclaimRequest(shuffle1, w1)))(h.poll("1"))
  }

  test("the driver step runs on the manager's thread at dispatch, before its fence's marker, " +
      "with the production tick") {
    val order = new ConcurrentLinkedQueue[String]()
    val stepThread = new AtomicReference[String]()
    val stepped = new CountDownLatch(1)
    val fences = new LinkedBlockingQueue[RapidsShuffleReclaimFence]()
    val manager = new ShuffleCleanupManager(
      clock = () => 0L,
      postEvent = {
        case fence: RapidsShuffleReclaimFence =>
          order.add(s"fence ${fence.fenceId}")
          fences.put(fence)
        case _ =>
      },
      driverStep = shuffleId => {
        stepThread.set(Thread.currentThread().getName)
        order.add(s"step $shuffleId")
        stepped.countDown()
        true
      },
      eventLogEnabled = true,
      disabled = new AtomicBoolean(false),
      droppedEvents = () => 0L)
    managers += manager
    manager.dispatch(1L, Seq(decision(shuffle1, execution1, w1, "1")), 0L)
    assert(stepped.await(awaitSeconds, TimeUnit.SECONDS),
      s"the driver step did not run within $awaitSeconds s of the dispatch")
    assertResult("shuffle-cleanup-manager")(stepThread.get)
    val fence = fences.poll(awaitSeconds, TimeUnit.SECONDS)
    assert(fence != null, "the fence posted no marker")
    assertResult(1L)(fence.fenceId)
    assertResult(List(s"step $shuffle1", "fence 1"))(order.asScala.toList)
  }

  test("a driver step that throws leaves its shuffle to GC with one WARN, and the dispatch's " +
      "next step and its marker still run") {
    val h = new Harness(shuffleId =>
      if (shuffleId == shuffle1) {
        throw new IllegalStateException("injected driver step failure")
      } else {
        true
      }, eventLogEnabled = true)
    h.manager.executionClosed(execution1, 0L, Seq(shuffle1))
    val lines = capture {
      h.dispatch(1L, decision(shuffle1, execution1, w1, "e1"),
        decision(shuffle2, execution2, w1, "e1"))
      h.awaitFence(1L)
    }
    val warned = lines.filter(_.startsWith(DRIVER_STEP_WARN_PREFIX))
    assertResult(1, lines.mkString("\n"))(warned.length)
    assert(names(warned.head, shuffle1), warned.head)
    assertResult(List(shuffle1, shuffle2))(h.steps.asScala.toList)
    val event = h.awaitEvent()
    assertResult((execution1, Seq(shuffle1), 0))(
      (event.executionId, event.skippedShared, event.reclaimed))
    // the policy never learns of the fault, so it releases every shuffle of the fence
    h.manager.applyRelease(1L, shuffle1)
    h.manager.applyRelease(1L, shuffle2)
    assertResult(Seq(ShuffleReclaimRequest(shuffle2, w1)))(h.poll("e1"))
  }

  test("a fenced request stays out of every poll until its release, and a withdrawn one never " +
      "enters them") {
    val h = new Harness(_ => true, eventLogEnabled = true)
    h.dispatch(1L, decision(shuffle1, execution1, w1, "e1", "e2"))
    h.awaitFence(1L)
    assert(h.poll("e1").isEmpty, "e1 got a fenced shuffle")
    assert(h.poll("e2").isEmpty, "e2 got a fenced shuffle")
    h.manager.applyRelease(1L, shuffle1)
    assertResult(Seq(ShuffleReclaimRequest(shuffle1, w1)))(h.poll("e1"))
    h.report("e1", removed(shuffle1, w1, 1, 10L))
    h.manager.executionClosed(execution2, 0L, Seq(shuffle2))
    h.dispatch(2L, decision(shuffle2, execution2, w2, "e1", "e2"))
    h.awaitFence(2L)
    h.manager.applyWithdrawal(2L, shuffle2)
    Seq("e1", "e2").foreach { executorId =>
      assert(!h.poll(executorId).exists(_.shuffleId == shuffle2), s"$executorId got a withdrawal")
    }
    val event = h.takeEvent()
    assertResult((execution2, Seq(shuffle2), 0))(
      (event.executionId, event.skippedShared, event.reclaimed))
  }

  test("a fence with no verdict 10 minutes after its clear is withdrawn with one WARN, and a " +
      "release after that is ignored") {
    val h = new Harness(_ => true, eventLogEnabled = true)
    val cleared = 5 * minuteMs
    h.now.set(cleared)
    h.manager.executionClosed(execution1, cleared, Seq(shuffle1))
    h.dispatch(1L, decision(shuffle1, execution1, w1, "e1"))
    h.awaitFence(1L)
    h.now.set(cleared + tenMinutesMs + 1L)
    val lines = capture(h.manager.runTick())
    val warned = lines.filter(_.startsWith(FENCE_TIMEOUT_WARN_PREFIX))
    assertResult(1, lines.mkString("\n"))(warned.length)
    assert(names(warned.head, shuffle1), warned.head)
    val event = h.takeEvent()
    assertResult((Seq(shuffle1), 0))((event.skippedShared, event.reclaimed))
    h.manager.applyRelease(1L, shuffle1)
    assert(h.poll("e1").isEmpty, "a release after the fence timed out reached the executor")
    assert(h.events.isEmpty, "a release after the fence timed out posted a second event")
  }

  test("a disable through the hand-off withdraws every pending fence, and a driver step after " +
      "it leaves its shuffle to GC without a clear") {
    val h = new Harness(_ => true, eventLogEnabled = true)
    h.manager.executionClosed(execution1, 0L, Seq(shuffle1))
    h.manager.executionClosed(execution2, 0L, Seq(shuffle2))
    h.dispatch(1L, decision(shuffle1, execution1, w1, "e1"))
    h.dispatch(2L, decision(shuffle2, execution2, w1, "e1"))
    h.awaitFence(1L)
    h.awaitFence(2L)
    val handoff: ShuffleReclaimHandoff = h.manager
    handoff.disable("the test disabled the reclaim")
    assert(h.disabled.get, "disable(reason) left the constructor's flag clear")
    assert(handoff.isDisabled)
    // the withdraw-all runs on the manager's thread
    val withdrawn = Seq(h.awaitEvent(), h.awaitEvent()).sortBy(_.executionId)
    assertResult(Seq((execution1, Seq(shuffle1)), (execution2, Seq(shuffle2))))(
      withdrawn.map(e => (e.executionId, e.skippedShared)))
    h.manager.applyRelease(1L, shuffle1)
    assert(h.poll("e1").isEmpty, "a release after the disable reached the executor")
    // stands for a dispatch queued before the disable, whose driver step runs after it
    h.manager.executionClosed(execution3, 0L, Seq(shuffle3))
    h.dispatch(3L, decision(shuffle3, execution3, w1, "e1"))
    val settled = h.awaitEvent()
    assertResult((execution3, Seq(shuffle3)))((settled.executionId, settled.skippedShared))
    assertResult(List(shuffle1, shuffle2))(h.steps.asScala.toList)
  }

  test("a close, skip or dispatch that names an execution the disable dropped keeps no " +
      "account for it until shutdown") {
    val h = new Harness(_ => true, eventLogEnabled = true)
    // execution 1's account is not closed, so the withdraw-all drops it
    h.manager.skip(shuffle1, execution1, ShuffleReclaimRule.R4)
    h.manager.executionClosed(execution2, 0L, Seq(shuffle2))
    h.dispatch(2L, decision(shuffle2, execution2, w1, "e1"))
    h.awaitFence(2L)
    val handoff: ShuffleReclaimHandoff = h.manager
    handoff.disable("the test disabled the reclaim")
    // the withdraw-all posts execution 2's event, so it has run by then
    assertResult(execution2)(h.awaitEvent().executionId)
    val late = capture {
      // stands for the tracker's handler that was in flight when the disable landed
      h.manager.executionClosed(execution1, 0L, Seq(shuffle1, shuffle3, shuffle4))
      h.manager.skip(shuffle3, execution1, ShuffleReclaimRule.R4)
      h.dispatch(4L, decision(shuffle4, execution1, w1, "e1"))
      // the manager's thread runs fence 4's step before fence 5's, whose event follows it
      h.manager.executionClosed(execution3, 0L, Seq(shuffle5))
      h.dispatch(5L, decision(shuffle5, execution3, w1, "e1"))
      assertResult(execution3)(h.awaitEvent().executionId)
    }
    // each late shuffle still logs its outcome, with no account to count it
    Seq(shuffle3 -> "skipped, unconsumed (R4)", shuffle4 -> "the reclaim is disabled").foreach {
      case (shuffleId, outcome) => assertResult(1, late.mkString("\n"))(
        late.count(line => names(line, shuffleId, execution1) && line.contains(outcome)))
    }
    val lines = capture(h.manager.shutdown())
    assert(!lines.exists(_.contains("SQL executions at shutdown")), lines.mkString("\n"))
  }

  test("a driver step that finds an event dropped from the policy's queue leaves its shuffle to " +
      "GC with no clear, and disables the reclaim with one WARN") {
    val h = new Harness(_ => true, eventLogEnabled = true)
    h.manager.executionClosed(execution1, 0L, Seq(shuffle1))
    h.dropped.set(1L)
    val lines = capture {
      h.dispatch(1L, decision(shuffle1, execution1, w1, "e1"))
      h.awaitFence(1L)
    }
    assert(h.steps.isEmpty, "the driver step ran after the queue dropped an event")
    assert(h.disabled.get, "the dropped event left the reclaim enabled")
    // the disable's WARN names the key that raises the queue's capacity
    val queue = ShuffleCleanupListener.QUEUE_NAME
    val capacityKey = s"spark.scheduler.listenerbus.eventqueue.$queue.capacity"
    assertResult(1, lines.mkString("\n"))(lines.count(_.contains(capacityKey)))
    val event = h.awaitEvent()
    assertResult((execution1, Seq(shuffle1), 0))(
      (event.executionId, event.skippedShared, event.reclaimed))
    h.manager.applyRelease(1L, shuffle1)
    assert(h.poll("e1").isEmpty, "a release reached the executor after the dropped event")
  }

  test("a fence whose marker cannot be posted is withdrawn with one WARN, and a release after " +
      "that is ignored") {
    val h = new Harness(_ => true, eventLogEnabled = true)
    h.failMarkers.set(true)
    h.manager.executionClosed(execution1, 0L, Seq(shuffle1))
    val lines = capture {
      h.dispatch(1L, decision(shuffle1, execution1, w1, "e1"))
      val event = h.awaitEvent()
      assertResult((execution1, Seq(shuffle1), 0))(
        (event.executionId, event.skippedShared, event.reclaimed))
    }
    val warned = lines.filter(_.contains("could not post the marker of fence 1"))
    assertResult(1, lines.mkString("\n"))(warned.length)
    assert(names(warned.head, shuffle1), warned.head)
    assertResult(List(shuffle1))(h.steps.asScala.toList)
    h.manager.applyRelease(1L, shuffle1)
    assert(h.poll("e1").isEmpty, "a release after the failed marker reached the executor")
  }

  test("a busy executor's targets never time out while it answers one every 6 minutes") {
    val h = new Harness(_ => true, eventLogEnabled = true)
    val shuffles = Seq(shuffle1, shuffle2, shuffle3)
    def answer(shuffleId: Int, buffers: Int): Unit = {
      h.report("e1", removed(shuffleId, w1, buffers, buffers * 100L))
      assert(!h.manager.isReclaimReleasedForTesting(shuffleId),
        s"the answer left shuffle $shuffleId open")
    }
    val timeline = h.everyMinute(0 to 18) {
      case 0 =>
        h.manager.executionClosed(execution1, 0L, shuffles)
        h.dispatchAndRelease(1L, shuffles.map(decision(_, execution1, w1, "e1")): _*)
        assertResult(shuffles)(h.poll("e1").map(_.shuffleId))
      case 6 => answer(shuffle1, 1)
      case 12 => answer(shuffle2, 2)
      case 18 => answer(shuffle3, 3)
      case _ =>
    }
    val warned = timeoutWarnings(timeline)
    assert(warned.isEmpty, warned)
    assertResult(Seq(18))(postingMinutes(timeline))
    val event = timeline.flatMap(_.posted).head
    assertResult((0, 6L, 600L))((event.executorsTimedOut, event.buffersFreed, event.bytesFreed))
  }

  test("a target whose executor never polls times out as never polled 10 minutes after the " +
      "release, and gets nothing after") {
    val h = new Harness(_ => true, eventLogEnabled = true)
    val timeline = h.everyMinute(0 to 12) {
      case 0 =>
        h.manager.executionClosed(execution1, 0L, Seq(shuffle1))
        h.dispatch(1L, decision(shuffle1, execution1, w1, "e1", "e2"))
        h.awaitFence(1L)
      case 1 =>
        h.manager.applyRelease(1L, shuffle1)
        assertResult(Seq(ShuffleReclaimRequest(shuffle1, w1)))(h.poll("e1"))
      case 2 => h.report("e1", removed(shuffle1, w1, 1, 100L))
      case 12 => assert(h.poll("e2").isEmpty, "e2 got the shuffle after its target timed out")
      case _ =>
    }
    val warned = timeoutWarnings(timeline)
    assertResult(Seq(11), warned)(warned.map(_._1))
    val line = warned.head._2
    assert(names(line, "e2", shuffle1) && line.contains("never polled"), line)
    assertResult(Seq(11))(postingMinutes(timeline))
    val event = timeline.flatMap(_.posted).head
    assertResult((1, 1))((event.executorsTimedOut, event.executorsNeverPolled))
  }

  test("a handed target times out as polled, no result after 10 silent minutes, and its late " +
      "result logs one INFO line and changes no count") {
    val h = new Harness(_ => true, eventLogEnabled = true)
    val timeline = h.everyMinute(0 to 13) {
      case 0 =>
        h.manager.executionClosed(execution1, 0L, Seq(shuffle1, shuffle2))
        h.dispatchAndRelease(1L, decision(shuffle1, execution1, w1, "e1"))
        assertResult(Seq(ShuffleReclaimRequest(shuffle1, w1)))(h.poll("e1"))
      case 11 =>
        // dispatched late, since a fence with no verdict for 10 minutes is withdrawn
        h.dispatchAndRelease(2L, decision(shuffle2, execution1, w2, "e3"))
        assertResult(Seq(ShuffleReclaimRequest(shuffle2, w2)))(h.poll("e3"))
      case 12 => h.report("e1", removed(shuffle1, w1, 5, 500L))
      case 13 => h.report("e3", removed(shuffle2, w2, 7, 700L))
      case _ =>
    }
    val warned = timeoutWarnings(timeline)
    assertResult(Seq(10), warned)(warned.map(_._1))
    val line = warned.head._2
    assert(names(line, "e1", shuffle1) && line.contains("polled, no result"), line)
    val lateLines = timeline.filter(_.minute == 12).flatMap(_.lines)
    val ignored = lateLines.filter(_.startsWith(IGNORED_RESULT_INFO_PREFIX))
    assertResult(1, lateLines.mkString("\n"))(ignored.length)
    assert(names(ignored.head, "e1", shuffle1, "REMOVED", 5, 500), ignored.head)
    assertResult(Seq(13))(postingMinutes(timeline))
    val event = timeline.flatMap(_.posted).head
    assertResult((1, 0, 7L, 700L))((event.executorsTimedOut, event.executorsNeverPolled,
      event.buffersFreed, event.bytesFreed))
  }

  test("a target released while its executor is busy reaches it at its next poll without " +
      "timing out") {
    val h = new Harness(_ => true, eventLogEnabled = true)
    val busy = Seq(shuffle2, shuffle3, shuffle4)
    val timeline = h.everyMinute(0 to 19) {
      case 0 =>
        h.manager.executionClosed(execution1, 0L, busy :+ shuffle1)
        h.dispatchAndRelease(1L, busy.map(decision(_, execution1, w1, "e1")): _*)
        assertResult(busy)(h.poll("e1").map(_.shuffleId))
      case 1 => h.dispatchAndRelease(2L, decision(shuffle1, execution1, w2, "e1"))
      case 6 => h.report("e1", removed(shuffle2, w1, 1, 10L))
      case 12 => h.report("e1", removed(shuffle3, w1, 1, 10L))
      case 18 =>
        h.report("e1", removed(shuffle4, w1, 1, 10L))
        assertResult(Seq(ShuffleReclaimRequest(shuffle1, w2)))(h.poll("e1"))
      case 19 => h.report("e1", removed(shuffle1, w2, 1, 10L))
      case _ =>
    }
    val warned = timeoutWarnings(timeline)
    assert(warned.isEmpty, warned)
    assertResult(Seq(19))(postingMinutes(timeline))
    assertResult(0)(timeline.flatMap(_.posted).head.executorsTimedOut)
  }

  test("a lost result gets one re-hand-out, and the third poll times the target out as " +
      "polled, no result") {
    val h = new Harness(_ => true, eventLogEnabled = true)
    h.manager.executionClosed(execution1, 0L, Seq(shuffle1))
    h.dispatchAndRelease(1L, decision(shuffle1, execution1, w1, "e1"))
    val handed = Seq(ShuffleReclaimRequest(shuffle1, w1))
    assertResult(handed)(h.poll("e1"))
    h.now.set(1000L)
    assertResult(handed)(h.poll("e1"))
    h.now.set(2000L)
    val lines = capture(assert(h.poll("e1").isEmpty, "the third poll got the shuffle again"))
    val warned = lines.filter(_.startsWith(TIMEOUT_WARN_PREFIX))
    assertResult(1, lines.mkString("\n"))(warned.length)
    assert(names(warned.head, "e1", shuffle1) && warned.head.contains("polled, no result"),
      warned.head)
    assertResult(1)(h.takeEvent().executorsTimedOut)
  }

  test("a result after the re-hand-out settles the target, and the third poll gets nothing") {
    val h = new Harness(_ => true, eventLogEnabled = true)
    h.manager.executionClosed(execution1, 0L, Seq(shuffle1))
    h.dispatchAndRelease(1L, decision(shuffle1, execution1, w1, "e1"))
    val handed = Seq(ShuffleReclaimRequest(shuffle1, w1))
    assertResult(handed)(h.poll("e1"))
    h.now.set(1000L)
    assertResult(handed)(h.poll("e1"))
    h.report("e1", ShuffleReclaimResult.absent(shuffle1, w1))
    assertResult(0)(h.takeEvent().executorsTimedOut)
    h.now.set(2000L)
    val lines = capture(assert(h.poll("e1").isEmpty, "the third poll got a settled shuffle"))
    assert(!lines.exists(_.startsWith(TIMEOUT_WARN_PREFIX)), lines.mkString("\n"))
  }

  test("a repeat request restarts the hand-out count, and a result for the replaced watermark " +
      "logs one INFO line and changes neither execution's event") {
    Seq(false, true).foreach { answersNewer =>
      val h = new Harness(_ => true, eventLogEnabled = true)
      h.manager.executionClosed(execution1, 0L, Seq(shuffle1))
      h.dispatchAndRelease(1L, decision(shuffle1, execution1, w1, "e1"))
      assertResult(Seq(ShuffleReclaimRequest(shuffle1, w1)))(h.poll("e1"))
      h.manager.executionClosed(execution2, 0L, Seq(shuffle1))
      h.dispatchAndRelease(2L, decision(shuffle1, execution2, w2, "e1"))
      // the release replaced the first request, which settles execution 1 with no result
      val first = h.takeEvent()
      assertResult((execution1, 0L))((first.executionId, first.bytesFreed))
      val lines = capture(h.report("e1", removed(shuffle1, w1, 5, 500L)))
      val ignored = lines.filter(_.startsWith(IGNORED_RESULT_INFO_PREFIX))
      assertResult(1, lines.mkString("\n"))(ignored.length)
      assert(names(ignored.head, "e1", shuffle1, "REMOVED", 5, 500), ignored.head)
      assert(h.events.isEmpty, "the result for the replaced watermark posted an event")
      val newer = Seq(ShuffleReclaimRequest(shuffle1, w2))
      assertResult(newer)(h.poll("e1"))
      if (answersNewer) {
        h.report("e1", removed(shuffle1, w2, 3, 300L))
      } else {
        assertResult(newer)(h.poll("e1"))
        assert(h.poll("e1").isEmpty, "the third poll after the repeat request got the shuffle")
      }
      val second = h.takeEvent()
      val expected = if (answersNewer) (execution2, 0, 300L) else (execution2, 1, 0L)
      assertResult(expected)((second.executionId, second.executorsTimedOut, second.bytesFreed))
    }
  }

  test("an executor's removal settles its target, which never times out, and the request " +
      "completes with the other target's answer") {
    val h = new Harness(_ => true, eventLogEnabled = true)
    val timeline = h.everyMinute(0 to 11) {
      case 0 =>
        h.manager.executionClosed(execution1, 0L, Seq(shuffle1))
        h.dispatchAndRelease(1L, decision(shuffle1, execution1, w1, "e1", "e2"))
        assertResult(Seq(ShuffleReclaimRequest(shuffle1, w1)))(h.poll("e1"))
      case 5 => h.manager.executorRemoved("e2")
      case 6 => h.report("e1", removed(shuffle1, w1, 1, 10L))
      case _ =>
    }
    val warnings = timeline.flatMap(_.lines).filter(isWarnLine)
    assert(!warnings.exists(line => names(line, "e2")), warnings)
    assertResult(Seq(6))(postingMinutes(timeline))
    val event = timeline.flatMap(_.posted).head
    assertResult((0, 0))((event.executorsTimedOut, event.executorsNeverPolled))
  }
}
