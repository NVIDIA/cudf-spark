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
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicLong, AtomicReference}

import scala.collection.JavaConverters._

import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{doAnswer, times, verify, when}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.mockito.MockitoSugar.mock

import org.apache.spark.api.plugin.PluginContext

/**
 * Tests of `ShuffleCleanupEndpoint` in both its modes: a UCX endpoint's reclaim polls and their
 * stop at close, the wait in `close()` for the poll thread in each mode, each mode's JVM shutdown
 * hook, and the poll's error handling. MULTITHREADED's poll and final cleanup are not tested here.
 */
class ShuffleCleanupEndpointSuite extends AnyFunSuite {
  private val awaitSeconds = 30L
  private val watermark = 100L
  // under the time close() waits for the poll thread, so a close() this fast did not time out,
  // and one this slow did not stop waiting early
  private val closeBoundSeconds = ShuffleCleanupEndpoint.CLOSE_WAIT_SECONDS - 1

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
      shutdownCalled: CountDownLatch = new CountDownLatch(1),
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

  /**
   * Starts an endpoint that polls every 10 ms on `scheduler`, runs `body` on it, and then runs
   * `release`, closes the endpoint and stops the scheduler, whatever `body` did.
   */
  private def withEndpoint(
      ctx: PluginContext,
      reclaimer: Option[UcxCatalogReclaimer],
      scheduler: ScheduledThreadPoolExecutor,
      release: () => Unit = () => ())(body: ShuffleCleanupEndpoint => Unit): Unit = {
    val endpoint = new ShuffleCleanupEndpoint(ctx, pollIntervalMs = 10,
      ucxReclaimer = reclaimer, executorService = scheduler)
    try {
      endpoint.start()
      body(endpoint)
    } finally {
      release()
      endpoint.close()
      scheduler.shutdownNow()
    }
  }

  /** A UCX reclaimer that sends through `ctx`, whose reclaim runs `onReclaim` and frees nothing. */
  private def reclaimerOf(ctx: PluginContext)(
      onReclaim: Int => Unit): Option[UcxCatalogReclaimer] =
    Some(new UcxCatalogReclaimer(
      (shuffleId, w, _) => {
        onReclaim(shuffleId)
        ShuffleReclaimResult.absent(shuffleId, w)
      },
      UcxCatalogReclaimer.sendTo(ctx)))

  /**
   * A UCX reclaimer whose reclaim of `blockedShuffleId` counts down `started` and then waits
   * for `release`, as a slow free would. It adds each shuffle it reclaims to `reclaimed`.
   */
  private def blockingOnShuffleId(
      ctx: PluginContext,
      blockedShuffleId: Int,
      started: CountDownLatch,
      release: CountDownLatch,
      reclaimed: ConcurrentLinkedQueue[Int] = new ConcurrentLinkedQueue[Int]())
      : Option[UcxCatalogReclaimer] =
    reclaimerOf(ctx) { shuffleId =>
      reclaimed.add(shuffleId)
      if (shuffleId == blockedShuffleId) {
        started.countDown()
        awaitIgnoringInterrupts(release)
      }
    }

  /**
   * Waits up to `awaitSeconds` for `latch` as the catalog's frees do, keeping an interrupt for
   * when it is done.
   */
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

  /**
   * Calls `endpoint.close()` on a thread of its own, and records how long it took, when it
   * returned, and whether the thread was interrupted after it.
   */
  private final class Closer(endpoint: ShuffleCleanupEndpoint) {
    val nanos = new AtomicLong(-1)
    val returnedAt = new AtomicLong(-1)
    val interruptedAfter = new AtomicBoolean(false)
    val thread = new Thread(() => {
      val start = System.nanoTime()
      endpoint.close()
      returnedAt.set(System.nanoTime())
      nanos.set(returnedAt.get - start)
      interruptedAfter.set(Thread.currentThread().isInterrupted)
    }, "shuffle-cleanup-endpoint-close")
    thread.setDaemon(true)

    /** Starts the close, and returns once it has stopped the polls. */
    def startAndAwaitShutdown(shutdownCalled: CountDownLatch): Unit = {
      thread.start()
      assert(shutdownCalled.await(awaitSeconds, TimeUnit.SECONDS),
        "close() never stopped the polls")
    }

    def join(): Unit = {
      thread.join(TimeUnit.SECONDS.toMillis(awaitSeconds))
      assert(!thread.isAlive, "close() never returned")
    }
  }

  test("a UCX endpoint adds no JVM shutdown hook") {
    val ctx = mockContext(RapidsShuffleReclaimResponseMsg(Array.empty),
      new ConcurrentLinkedQueue[AnyRef]())
    withEndpoint(ctx, reclaimerOf(ctx)(_ => ()), newScheduler()) { endpoint =>
      assert(!endpoint.isShutdownHookAdded)
    }
  }

  test("a MULTITHREADED endpoint adds its JVM shutdown hook") {
    val ctx = mock[PluginContext]
    when(ctx.executorID()).thenReturn("1")
    doAnswer(_ => RapidsShuffleCleanupResponseMsg(Array.empty)).when(ctx).ask(any[AnyRef]())
    withEndpoint(ctx, None, newScheduler()) { endpoint =>
      // the hook stays registered in this JVM, and finds the endpoint closed at exit
      assert(endpoint.isShutdownHookAdded)
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
    withEndpoint(ctx, blockingOnShuffleId(ctx, 7, started, release, reclaimed), scheduler,
        () => release.countDown()) { endpoint =>
      assert(started.await(awaitSeconds, TimeUnit.SECONDS),
        "the reclaim of shuffle 7 never started")
      val closer = new Closer(endpoint)
      closer.startAndAwaitShutdown(shutdownCalled)
      release.countDown()
      closer.join()
      assertResult(List(7))(reclaimed.asScala.toList)
      val results = sent.asScala.toList.map {
        case RapidsShuffleReclaimResultMsg(executorId, rs) =>
          (executorId, rs.map(_.shuffleId).toList)
        case other => fail(s"sent $other")
      }
      assertResult(List(("1", List(7))))(results)
      verify(ctx, times(1)).ask(any[AnyRef]())
      verify(ctx, times(1)).ask(RapidsShuffleReclaimPollMsg("1"))
      assert(closer.nanos.get < TimeUnit.SECONDS.toNanos(closeBoundSeconds),
        s"close() took ${closer.nanos.get} ns")
      assert(scheduler.isTerminated)
    }
  }

  test("a closed UCX endpoint stops the reclaim in flight at its next free, and close() waits " +
      "for that free") {
    val freeing = new CountDownLatch(1)
    val releaseFree = new CountDownLatch(1)
    val shutdownCalled = new CountDownLatch(1)
    val frees = new AtomicInteger(0)
    val lastFreeEndedAt = new AtomicLong(-1)
    val ctx = mockContext(
      RapidsShuffleReclaimResponseMsg(Array(ShuffleReclaimRequest(7, watermark))),
      new ConcurrentLinkedQueue[AnyRef]())
    // up to three frees, each waiting for the test as a slow free would, and ignoring an
    // interrupt as the catalog's frees do
    val threeFrees = Some(new UcxCatalogReclaimer(
        (shuffleId, w, isStopped) => {
          while (frees.get < 3 && !isStopped()) {
            freeing.countDown()
            awaitIgnoringInterrupts(releaseFree)
            frees.incrementAndGet()
            lastFreeEndedAt.set(System.nanoTime())
          }
          ShuffleReclaimResult.absent(shuffleId, w)
        },
        UcxCatalogReclaimer.sendTo(ctx)))
    withEndpoint(ctx, threeFrees, newScheduler(shutdownCalled),
        () => releaseFree.countDown()) { endpoint =>
      assert(freeing.await(awaitSeconds, TimeUnit.SECONDS), "the reclaim never started freeing")
      val closer = new Closer(endpoint)
      closer.startAndAwaitShutdown(shutdownCalled)
      releaseFree.countDown()
      closer.join()
      assertResult(1, "frees, which the stop should end after the one in flight")(frees.get)
      assert(closer.returnedAt.get >= lastFreeEndedAt.get,
        "close() returned before the free in flight ended")
      assert(closer.nanos.get < TimeUnit.SECONDS.toNanos(closeBoundSeconds),
        s"close() took ${closer.nanos.get} ns")
    }
  }

  test("an interrupted close still waits up to its bound for the reclaim in flight, and " +
      "restores the interrupt status of the thread that called it") {
    val started = new CountDownLatch(1)
    val release = new CountDownLatch(1)
    val shutdownCalled = new CountDownLatch(1)
    val ctx = mockContext(twoRequests, new ConcurrentLinkedQueue[AnyRef]())
    val scheduler = newScheduler(shutdownCalled)
    // the reclaim of shuffle 7 is one free that outlasts close()'s wait
    withEndpoint(ctx, blockingOnShuffleId(ctx, 7, started, release), scheduler,
        () => release.countDown()) { endpoint =>
      assert(started.await(awaitSeconds, TimeUnit.SECONDS),
        "the reclaim of shuffle 7 never started")
      val closer = new Closer(endpoint)
      closer.startAndAwaitShutdown(shutdownCalled)
      closer.thread.interrupt()
      closer.join()
      assert(closer.nanos.get >= TimeUnit.SECONDS.toNanos(closeBoundSeconds),
        s"close() stopped waiting for the reclaim in flight after ${closer.nanos.get} ns")
      assert(closer.interruptedAfter.get, "close() cleared the interrupt")
      release.countDown()
      scheduler.awaitTermination(awaitSeconds, TimeUnit.SECONDS)
    }
  }

  test("an interrupted close of a MULTITHREADED endpoint stops waiting for its poll") {
    val asked = new CountDownLatch(1)
    val reply = new CountDownLatch(1)
    val shutdownCalled = new CountDownLatch(1)
    val ctx = mock[PluginContext]
    when(ctx.executorID()).thenReturn("1")
    // the poll waits for the driver's reply past close()'s wait, whatever interrupts it
    doAnswer(_ => {
      asked.countDown()
      awaitIgnoringInterrupts(reply)
      RapidsShuffleCleanupResponseMsg(Array.empty)
    }).when(ctx).ask(any[AnyRef]())
    withEndpoint(ctx, None, newScheduler(shutdownCalled), () => reply.countDown()) { endpoint =>
      assert(asked.await(awaitSeconds, TimeUnit.SECONDS), "the poll never asked the driver")
      val closer = new Closer(endpoint)
      closer.startAndAwaitShutdown(shutdownCalled)
      closer.thread.interrupt()
      closer.join()
      // this endpoint closes after device shutdown, so its wait guards no free
      assert(closer.nanos.get < TimeUnit.SECONDS.toNanos(closeBoundSeconds),
        s"close() went on waiting for ${closer.nanos.get} ns after its interrupt")
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
    val lines = LogCaptureUtils.captureLogsFrom(Seq(classOf[ShuffleCleanupEndpoint].getName)) {
      withEndpoint(ctx, reclaimerOf(ctx)(_ => ()), scheduler,
          () => reply.countDown()) { endpoint =>
        assert(asked.await(awaitSeconds, TimeUnit.SECONDS), "the poll never asked the driver")
        val closer = new Closer(endpoint)
        closer.startAndAwaitShutdown(shutdownCalled)
        // close() waits up to its bound for the poll, interrupted or not, and then interrupts
        // the poll thread
        closer.thread.interrupt()
        closer.join()
        assert(scheduler.awaitTermination(awaitSeconds, TimeUnit.SECONDS),
          "the poll thread never ended")
      }
    }
    assertResult(Some(true), "the poll thread's interrupt status after its poll")(
      interruptedAfterPoll.get)
    assert(!lines.exists(_.contains("Error during shuffle cleanup poll")), lines.mkString("\n"))
  }

  test("a poll that throws a fatal error logs it once") {
    val polled = new CountDownLatch(1)
    val ctx = mockContext(twoRequests, new ConcurrentLinkedQueue[AnyRef]())
    // a reclaim catches NonFatal only, so this error leaves the poll
    val failing = reclaimerOf(ctx)(_ => throw new LinkageError("injected fatal poll failure"))
    val lines = LogCaptureUtils.captureLogsFrom(Seq(classOf[ShuffleCleanupEndpoint].getName)) {
      withEndpoint(ctx, failing, newScheduler(afterPoll = () => polled.countDown())) { _ =>
        assert(polled.await(awaitSeconds, TimeUnit.SECONDS), "the endpoint never polled")
      }
    }
    assertResult(1, lines.mkString("\n"))(
      lines.count(_.contains("stops after a fatal error")))
  }
}
