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

import java.util.concurrent.{ConcurrentLinkedQueue, CountDownLatch, ScheduledThreadPoolExecutor,
  TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong, AtomicReference}

import scala.collection.JavaConverters._

import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{doAnswer, times, verify, when}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.mockito.MockitoSugar.mock

import org.apache.spark.api.plugin.PluginContext

class ShuffleCleanupEndpointSuite extends AnyFunSuite {
  private val awaitSeconds = 30L
  private val watermark = 100L
  // under the 5 s that close() waits for the poll thread, so a close() this fast did not time out
  private val closeBoundSeconds = 4L

  /**
   * A plugin context for executor 1, whose first UCX poll returns `firstReply` and whose later
   * UCX polls return no requests. Any other message it is asked gets null. It adds each message
   * it sends to `sent`.
   */
  private def mockContext(
      firstReply: RapidsShuffleReclaimResponseMsg,
      sent: ConcurrentLinkedQueue[AnyRef]): PluginContext = {
    val ctx = mock[PluginContext]
    when(ctx.executorID()).thenReturn("1")
    when(ctx.ask(RapidsShuffleReclaimPollMsg("1")))
      .thenReturn(firstReply, RapidsShuffleReclaimResponseMsg(Array.empty))
    doAnswer(invocation => {
      sent.add(invocation.getArgument[AnyRef](0))
      null
    }).when(ctx).send(any[AnyRef]())
    ctx
  }

  /** Requests to reclaim shuffles 7 and 8, the first reply each case gets. */
  private val twoRequests = RapidsShuffleReclaimResponseMsg(
    Array(ShuffleReclaimRequest(7, watermark), ShuffleReclaimRequest(8, watermark)))

  /**
   * One daemon poll thread, whose shutdown counts down `shutdownCalled`, and which runs
   * `afterPoll` on itself after each poll.
   */
  private def newScheduler(
      shutdownCalled: CountDownLatch,
      afterPoll: () => Unit = () => ()): ScheduledThreadPoolExecutor = {
    val threads = new ThreadFactoryBuilder()
      .setNameFormat("shuffle-cleanup-endpoint-test")
      .setDaemon(true)
      .build()
    new ScheduledThreadPoolExecutor(1, threads) {
      override def shutdown(): Unit = {
        super.shutdown()
        shutdownCalled.countDown()
      }

      override def afterExecute(r: Runnable, t: Throwable): Unit = {
        super.afterExecute(r, t)
        afterPoll()
      }
    }
  }

  /** Builds an endpoint's UCX reclaimer whose reclaim runs `onReclaim` and frees nothing. */
  private def ucxReclaimer(
      onReclaim: Int => Unit): (PluginContext, () => Boolean) => CatalogReclaimer =
    (context, isClosed) => new UcxCatalogReclaimer(isClosed,
      (shuffleId, w) => {
        onReclaim(shuffleId)
        ShuffleReclaimResult.absent(shuffleId, w)
      },
      UcxCatalogReclaimer.sendTo(context))

  /** Waits for `latch` as the catalog's frees do, keeping an interrupt for when it is done. */
  private def awaitIgnoringInterrupts(latch: CountDownLatch): Unit = {
    var interrupted = false
    var waiting = true
    while (waiting) {
      try {
        latch.await(awaitSeconds, TimeUnit.SECONDS)
        waiting = false
      } catch {
        case _: InterruptedException => interrupted = true
      }
    }
    if (interrupted) {
      Thread.currentThread().interrupt()
    }
  }

  test("a UCX endpoint adds no JVM shutdown hook") {
    val scheduler = newScheduler(new CountDownLatch(1))
    val ctx = mockContext(RapidsShuffleReclaimResponseMsg(Array.empty),
      new ConcurrentLinkedQueue[AnyRef]())
    val endpoint = new ShuffleCleanupEndpoint(ctx, pollIntervalMs = 10,
      newReclaimer = ucxReclaimer(_ => ()), executorService = scheduler)
    try {
      endpoint.start()
      assert(!endpoint.isShutdownHookAdded)
    } finally {
      endpoint.close()
      scheduler.shutdownNow()
    }
  }

  test("a closed UCX endpoint starts no further reclaim") {
    val started = new CountDownLatch(1)
    val release = new CountDownLatch(1)
    val shutdownCalled = new CountDownLatch(1)
    val reclaimed = new ConcurrentLinkedQueue[Int]()
    val sent = new ConcurrentLinkedQueue[AnyRef]()
    val ctx = mockContext(twoRequests, sent)
    val scheduler = newScheduler(shutdownCalled)
    val endpoint = new ShuffleCleanupEndpoint(ctx, pollIntervalMs = 10,
      newReclaimer = ucxReclaimer { shuffleId =>
        reclaimed.add(shuffleId)
        if (shuffleId == 7) {
          started.countDown()
          release.await(awaitSeconds, TimeUnit.SECONDS)
        }
      },
      executorService = scheduler)
    val closeNanos = new AtomicLong(-1)
    val closer = new Thread(() => {
      val start = System.nanoTime()
      endpoint.close()
      closeNanos.set(System.nanoTime() - start)
    }, "shuffle-cleanup-endpoint-close")
    closer.setDaemon(true)
    try {
      endpoint.start()
      assert(started.await(awaitSeconds, TimeUnit.SECONDS),
        "the reclaim of shuffle 7 never started")
      closer.start()
      assert(shutdownCalled.await(awaitSeconds, TimeUnit.SECONDS),
        "close() never stopped the polls")
      release.countDown()
      closer.join(TimeUnit.SECONDS.toMillis(awaitSeconds))
      assert(!closer.isAlive, "close() never returned")
      assertResult(List(7))(reclaimed.asScala.toList)
      val results = sent.asScala.toList.map {
        case RapidsShuffleReclaimResultMsg(executorId, rs) =>
          (executorId, rs.map(_.shuffleId).toList)
        case other => fail(s"sent $other")
      }
      assertResult(List(("1", List(7))))(results)
      verify(ctx, times(1)).ask(any[AnyRef]())
      verify(ctx, times(1)).ask(RapidsShuffleReclaimPollMsg("1"))
      assert(closeNanos.get < TimeUnit.SECONDS.toNanos(closeBoundSeconds),
        s"close() took ${closeNanos.get} ns")
      assert(scheduler.isTerminated)
    } finally {
      release.countDown()
      endpoint.close()
      scheduler.shutdownNow()
    }
  }

  test("an interrupted close restores the interrupt status of the thread that called it") {
    val started = new CountDownLatch(1)
    val release = new CountDownLatch(1)
    val shutdownCalled = new CountDownLatch(1)
    val ctx = mockContext(twoRequests, new ConcurrentLinkedQueue[AnyRef]())
    val scheduler = newScheduler(shutdownCalled)
    val endpoint = new ShuffleCleanupEndpoint(ctx, pollIntervalMs = 10,
      newReclaimer = ucxReclaimer { shuffleId =>
        if (shuffleId == 7) {
          started.countDown()
          awaitIgnoringInterrupts(release)
        }
      },
      executorService = scheduler)
    val closeNanos = new AtomicLong(-1)
    val interruptedAfterClose = new AtomicBoolean(false)
    val closer = new Thread(() => {
      val start = System.nanoTime()
      endpoint.close()
      closeNanos.set(System.nanoTime() - start)
      interruptedAfterClose.set(Thread.currentThread().isInterrupted)
    }, "shuffle-cleanup-endpoint-close")
    closer.setDaemon(true)
    try {
      endpoint.start()
      assert(started.await(awaitSeconds, TimeUnit.SECONDS),
        "the reclaim of shuffle 7 never started")
      closer.start()
      assert(shutdownCalled.await(awaitSeconds, TimeUnit.SECONDS),
        "close() never stopped the polls")
      closer.interrupt()
      closer.join(TimeUnit.SECONDS.toMillis(awaitSeconds))
      assert(!closer.isAlive, "close() never returned")
      assert(interruptedAfterClose.get, "close() cleared the interrupt")
      assert(closeNanos.get < TimeUnit.SECONDS.toNanos(closeBoundSeconds),
        s"close() took ${closeNanos.get} ns")
    } finally {
      release.countDown()
      endpoint.close()
      scheduler.awaitTermination(awaitSeconds, TimeUnit.SECONDS)
      scheduler.shutdownNow()
    }
  }

  test("a poll that close() interrupts while it waits for the driver keeps the poll thread's " +
      "interrupt status, and logs no warning") {
    val asked = new CountDownLatch(1)
    val reply = new CountDownLatch(1)
    val shutdownCalled = new CountDownLatch(1)
    val interruptedAfterPoll = new AtomicReference[Option[Boolean]](None)
    val ctx = mock[PluginContext]
    when(ctx.executorID()).thenReturn("1")
    // the poll waits for the driver's reply, as an ask does, and an interrupt ends the wait
    doAnswer(_ => {
      asked.countDown()
      reply.await(awaitSeconds, TimeUnit.SECONDS)
      RapidsShuffleReclaimResponseMsg(Array.empty)
    }).when(ctx).ask(any[AnyRef]())
    val scheduler = newScheduler(shutdownCalled, () =>
      interruptedAfterPoll.compareAndSet(None, Some(Thread.currentThread().isInterrupted)))
    val endpoint = new ShuffleCleanupEndpoint(ctx, pollIntervalMs = 10,
      newReclaimer = ucxReclaimer(_ => ()), executorService = scheduler)
    val closer = new Thread(() => endpoint.close(), "shuffle-cleanup-endpoint-close")
    closer.setDaemon(true)
    val lines = LogCaptureUtils.captureLogsFrom(Seq(classOf[ShuffleCleanupEndpoint].getName)) {
      try {
        endpoint.start()
        assert(asked.await(awaitSeconds, TimeUnit.SECONDS), "the poll never asked the driver")
        closer.start()
        assert(shutdownCalled.await(awaitSeconds, TimeUnit.SECONDS),
          "close() never stopped the polls")
        // close() then waits for the poll, and an interrupt makes it interrupt the poll thread
        closer.interrupt()
        closer.join(TimeUnit.SECONDS.toMillis(awaitSeconds))
        assert(!closer.isAlive, "close() never returned")
        assert(scheduler.awaitTermination(awaitSeconds, TimeUnit.SECONDS),
          "the poll thread never ended")
      } finally {
        reply.countDown()
        endpoint.close()
        scheduler.shutdownNow()
      }
    }
    assertResult(Some(true), "the poll thread's interrupt status after its poll")(
      interruptedAfterPoll.get)
    assert(!lines.exists(_.contains("Error during shuffle cleanup poll")), lines.mkString("\n"))
  }

  test("a poll that throws a fatal error logs it once") {
    val polled = new CountDownLatch(1)
    val ctx = mockContext(RapidsShuffleReclaimResponseMsg(Array.empty),
      new ConcurrentLinkedQueue[AnyRef]())
    val failing: (PluginContext, () => Boolean) => CatalogReclaimer = (_, _) =>
      new CatalogReclaimer {
        override def poll(executorId: String, ask: AnyRef => AnyRef): Unit =
          throw new LinkageError("injected fatal poll failure")

        override def finalStep: Option[() => Unit] = None
      }
    val scheduler = newScheduler(new CountDownLatch(1), () => polled.countDown())
    val endpoint = new ShuffleCleanupEndpoint(ctx, pollIntervalMs = 10, newReclaimer = failing,
      executorService = scheduler)
    val lines = LogCaptureUtils.captureLogsFrom(Seq(classOf[ShuffleCleanupEndpoint].getName)) {
      try {
        endpoint.start()
        assert(polled.await(awaitSeconds, TimeUnit.SECONDS), "the endpoint never polled")
      } finally {
        endpoint.close()
        scheduler.shutdownNow()
      }
    }
    assertResult(1, lines.mkString("\n"))(
      lines.count(_.contains("stops after a fatal error")))
  }
}
