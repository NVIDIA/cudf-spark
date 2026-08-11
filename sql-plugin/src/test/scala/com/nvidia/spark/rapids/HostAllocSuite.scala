/*
 * Copyright (c) 2023-2025, NVIDIA CORPORATION.
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

import java.util.concurrent.{ExecutionException, Future, LinkedBlockingQueue, TimeoutException, TimeUnit}

import ai.rapids.cudf.{HostMemoryBuffer, PinnedMemoryPool, Rmm, RmmAllocationMode}
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.jni.{RmmSpark, RmmSparkThreadState, TaskPriority, ThreadStateRegistry}
import com.nvidia.spark.rapids.spill._
import org.mockito.Mockito.when
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.concurrent.{Signaler, TimeLimits}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.time._
import org.scalatestplus.mockito.MockitoSugar.mock

import org.apache.spark.{SparkConf, TaskContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.execution.TrampolineUtil

class HostAllocSuite extends AnyFunSuite with BeforeAndAfterEach with
    BeforeAndAfterAll with TimeLimits {
  private val sqlConf = new SQLConf()
  Rmm.shutdown()
  private val timeoutMs = 10000

  def setMockContext(taskAttemptId: Long): Unit = {
    val context = mock[TaskContext]
    when(context.taskAttemptId()).thenReturn(taskAttemptId)
    TrampolineUtil.setTaskContext(context)
  }

  trait TaskThreadOp[T] {
    def doIt(): T
  }

  object TaskThread {
    private class TaskThreadDoneOp(private var wrapped: TaskThread) extends TaskThreadOp[Void]
        with Future[Void]{
      override def toString = "TASK DONE"

      override def doIt(): Void = {
        TaskPriority.taskDone(wrapped.taskId)
        null
      }

      override def cancel(b: Boolean) = false

      override def isCancelled = false

      override def isDone: Boolean = !wrapped.isAlive

      override def get = throw new RuntimeException("FUTURE NEEDS A TIMEOUT. THIS IS A TEST!")

      override def get(l: Long, timeUnit: TimeUnit): Void = {
        wrapped.join(timeUnit.toMillis(l))
        null
      }
    }

    private class TaskThreadTrackingOp[T](private val wrapped: TaskThreadOp[T])
        extends TaskThreadOp[T] with Future[T] {
      private var done = false
      private var t: Option[Throwable] = None
      private var ret: T = null.asInstanceOf[T]

      override def toString: String = wrapped.toString

      override def doIt(): T = try {
        val tmp = wrapped.doIt()
        this.synchronized {
          ret = tmp
          return ret
        }
      } catch {
        case t: Throwable =>
          synchronized {
            this.t = Some(t)
          }
          null.asInstanceOf[T]
      } finally synchronized {
        done = true
        this.notifyAll()
      }

      override def cancel(b: Boolean) = false

      override def isCancelled = false

      override def isDone: Boolean = synchronized {
        done
      }

      override def get =
        throw new RuntimeException("This is a test you should always have timeouts...")

      override def get(l: Long, timeUnit: TimeUnit): T = synchronized {
        if (!done) {
          wait(timeUnit.toMillis(l))
          if (!done) throw new TimeoutException()
        }
        t.map( e => throw new ExecutionException(e))
        ret
      }
    }
  }

  class TaskThread(private val name: String, private val taskId: Long) extends Thread(name) {
    private val queue = new LinkedBlockingQueue[TaskThreadOp[_]]()
    private var nativeThreadId = -1L

    def initialize(): Unit = {
      setDaemon(true)
      start()
      val waitForStart = doIt(new TaskThreadOp[Void]() {
        override def doIt(): Void = {
          TaskPriority.getTaskPriority(taskId)
          null
        }

        override def toString: String = s"INIT TASK $name TASK $taskId"
      })
      waitForStart.get(timeoutMs, TimeUnit.MILLISECONDS)
    }

    def done: Future[Void] = {
      val op = new TaskThread.TaskThreadDoneOp(this)
      queue.offer(op)
      op
    }

    def waitForBlockedOnAlloc(): Unit = {
      val start = System.nanoTime()
      var state = RmmSpark.getStateOf(nativeThreadId)
      while (!isBlockedState(state)) {
        val end = System.nanoTime()
        if (TimeUnit.SECONDS.toNanos(1) <= (end - start)) {
          throw new TimeoutException(s"$name in $state after ${end - start} ns")
        }
        Thread.sleep(10)
        synchronized {
          state = RmmSpark.getStateOf(nativeThreadId)
        }
      }
    }

    private def isBlockedState(state: RmmSparkThreadState): Boolean = state match {
      case RmmSparkThreadState.THREAD_BUFN | RmmSparkThreadState.THREAD_BLOCKED => true
      case _ => false
    }

    def isBlocked: Boolean = synchronized {
      isBlockedState(RmmSpark.getStateOf(nativeThreadId))
    }

    def doIt[T](op: TaskThreadOp[T]): Future[T] = {
      if (!isAlive) throw new IllegalStateException("Thread is already done...")
      val tracking = new TaskThread.TaskThreadTrackingOp[T](op)
      queue.offer(tracking)
      tracking
    }

    override def run(): Unit = {
      try {
        this.nativeThreadId = RmmSpark.getCurrentThreadId
        setMockContext(taskId)
        RmmSpark.currentThreadIsDedicatedToTask(taskId)
        try {
          // Without this the retry does not work properly
          SQLConf.withExistingConf(sqlConf) {
            var isDone = false
            while (!isDone) {
              val op = queue.poll()
              if (op.isInstanceOf[TaskThread.TaskThreadDoneOp]) {
                isDone = true
              } else if (op != null) {
                op.doIt()
              } else {
                Thread.`yield`()
              }
            }
          }
        } finally {
          RmmSpark.removeCurrentDedicatedThreadAssociation(taskId)
        }
      } catch {
        case t: Throwable =>
          System.err.println("THROWABLE CAUGHT IN " + name)
          t.printStackTrace(System.err)
      }
    }
  }

  class AllocOnAnotherThread(val thread: TaskThread,
      val size: Long,
      val preferPinned: Boolean = true) extends AutoCloseable {

    var b: Option[HostMemoryBuffer] = None
    val fb: Future[Void] = thread.doIt(new TaskThreadOp[Void] {
      override def doIt(): Void = {
        doAlloc()
        null
      }

      override def toString: String = "ALLOC(" + size + ")"
    })
    var sb: Option[SpillableHostBuffer] = None
    var fsb: Option[Future[Void]] = None
    var fc: Option[Future[Void]] = None

    def waitForAlloc(): Unit = {
      fb.get(timeoutMs, TimeUnit.MILLISECONDS)
    }

    def assertAllocSize(expectedSize: Long): Unit = synchronized {
      assert(b.isDefined)
      assertResult(expectedSize)(b.get.getLength)
    }

    def makeSpillableOnThread(): Unit = {
      if (fsb.isDefined) throw new IllegalStateException("Can only make the buffer spillable once")
      fsb = Option(thread.doIt(new TaskThreadOp[Void] {
        override def doIt(): Void = {
          doMakeSpillable(SpillPriorities.ACTIVE_ON_DECK_PRIORITY)
          null
        }

        override def toString: String = "MAKE_SPILLABLE(" + size + ")"
      }))
    }

    def waitForSpillable(): Unit = {
      if (fsb.isEmpty) makeSpillableOnThread()
      fsb.get.get(timeoutMs, TimeUnit.MILLISECONDS)
    }

    def freeOnThread(): Unit = {
      if (fc.isDefined) throw new IllegalStateException("free called multiple times")
      fc = Option(thread.doIt(new TaskThreadOp[Void]() {
        override def doIt(): Void = {
          close()
          null
        }

        override def toString: String = "FREE(" + size + ")"
      }))
    }

    def waitForFree(): Unit = {
      if (fc.isEmpty) freeOnThread()
      fc.get.get(timeoutMs, TimeUnit.MILLISECONDS)
    }

    def freeAndWait(): Unit = {
      waitForFree()
    }

    def doAlloc(): Void = {
      RmmRapidsRetryIterator.withRetryNoSplit {
        val tmp = HostAlloc.alloc(size, preferPinned)
        synchronized {
          closeOnExcept(tmp) { _ =>
            assert(b.isEmpty)
            b = Option(tmp)
          }
        }
      }
      null
    }

    private def doMakeSpillable(priority: Long): Void = synchronized {
      val tmp = b.getOrElse {
        throw new IllegalStateException("Buffer made spillable without a buffer")
      }
      closeOnExcept(tmp) { _ =>
        b = None
        assert(sb.isEmpty)
      }
      sb = Some(SpillableHostBuffer(tmp, tmp.getLength, priority))
      null
    }

    override def close(): Unit = synchronized {
      b.foreach(_.close())
      b = None
      sb.foreach(_.close())
      sb = None
    }
  }

  object MyThreadSignaler extends Signaler {
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

  implicit val signaler: Signaler = MyThreadSignaler
  private var rmmWasInitialized = false

  override def beforeEach(): Unit = {
    val sc = new SparkConf
    SparkSession.getActiveSession.foreach(_.stop())
    SparkSession.clearActiveSession()
    SpillFramework.shutdown()
    if (Rmm.isInitialized) {
      rmmWasInitialized = true
      Rmm.shutdown()
    }
    Rmm.initialize(RmmAllocationMode.CUDA_DEFAULT, null, 512 * 1024 * 1024)
    // this doesn't allocate memory for bounce buffers, as HostAllocSuite
    // is playing games with the pools.
    SpillFramework.storesInternal = new SpillableStores {
      override var deviceStore: SpillableDeviceStore = new SpillableDeviceStore
      override var hostStore: SpillableHostStore = new SpillableHostStore(None)
      override var diskStore: DiskHandleStore = new DiskHandleStore(sc)
    }
    // some tests need an event handler
    RmmSpark.setEventHandler(
      new DeviceMemoryEventHandler(SpillFramework.stores.deviceStore, None, 0), "STDERR")
    PinnedMemoryPool.shutdown()
    HostAlloc.initialize(-1)
  }

  override def afterAll(): Unit = {
    try {
      SpillFramework.shutdown()
      PinnedMemoryPool.shutdown()
      Rmm.shutdown()
      RmmSpark.clearEventHandler()
      if (rmmWasInitialized) {
        // put RMM back for other tests to use
        Rmm.initialize(RmmAllocationMode.CUDA_DEFAULT, null, 512 * 1024 * 1024)
      }
      // less than 1 GiB, see more background at:
      // https://github.com/NVIDIA/spark-rapids/issues/12194#issuecomment-2703186601
      PinnedMemoryPool.initialize(500 * 1024 * 1024)
      HostAlloc.initialize(-1)
    } catch {
      case t: Throwable =>
        // if the exception does not have a message set, then scalatest fails in
        // ways that make it so you do not see the exception at all.
        System.err.println(t)
        t.printStackTrace(System.err)
        throw t
    }
  }

  test("simple pinned tryAlloc") {
    PinnedMemoryPool.initialize(4 * 1024)
    HostAlloc.initialize(0)

    failAfter(Span(10, Seconds)) {
      withResource(HostAlloc.tryAlloc(1024, preferPinned = false)) { got =>
        assert(got.isDefined)
        val buffer = got.get
        assertResult(buffer.getLength)(1024)
      }

      withResource(HostAlloc.tryAlloc(4 * 1024)) { got =>
        assert(got.isDefined)
        val buffer = got.get
        assertResult(buffer.getLength)(4 * 1024)
        withResource(HostAlloc.tryAlloc(1)) { got2 =>
          // We ran out of memory
          assert(got2.isEmpty)
        }
      }
      withResource(HostAlloc.tryAlloc(4 * 1024 + 1)) { buffer =>
        assert(buffer.isEmpty)
      }
    }
  }

  test("simple non-pinned tryAlloc") {
    PinnedMemoryPool.initialize(0)
    HostAlloc.initialize(4 * 1024)

    failAfter(Span(10, Seconds)) {
      withResource(HostAlloc.tryAlloc(1024, preferPinned = false)) { got =>
        assert(got.isDefined)
        val buffer = got.get
        assertResult(buffer.getLength)(1024)
      }

      withResource(HostAlloc.tryAlloc(4 * 1024)) { got =>
        assert(got.isDefined)
        val buffer = got.get
        assertResult(buffer.getLength)(4 * 1024)
        withResource(HostAlloc.tryAlloc(1)) { got2 =>
          // We ran out of memory
          assert(got2.isEmpty)
        }
      }

      withResource(HostAlloc.tryAlloc(4 * 1024 + 1)) { buffer =>
        assert(buffer.isEmpty)
      }
    }
  }

  test("simple mixed tryAlloc") {
    PinnedMemoryPool.initialize(4 * 1024)
    HostAlloc.initialize(4 * 1024)

    failAfter(Span(10, Seconds)) {
      withResource(HostAlloc.tryAlloc(1024, preferPinned = false)) { got =>
        assert(got.isDefined)
        val buffer = got.get
        assertResult(buffer.getLength)(1024)
      }

      withResource(HostAlloc.tryAlloc(4 * 1024)) { got =>
        assert(got.isDefined)
        val buffer = got.get
        assertResult(buffer.getLength)(4 * 1024)
        withResource(HostAlloc.tryAlloc(4 * 1024)) { got2 =>
          assert(got2.isDefined)
          val buffer2 = got2.get
          assertResult(buffer2.getLength)(4 * 1024)
          withResource(HostAlloc.tryAlloc(1)) { got3 =>
            // We ran out of memory
            assert(got3.isEmpty)
          }
        }
      }

      withResource(HostAlloc.tryAlloc(4 * 1024 + 1)) { buffer =>
        assert(buffer.isEmpty)
      }
    }
  }

  test("simple pinned blocking alloc") {
    PinnedMemoryPool.initialize(4 * 1024)
    HostAlloc.initialize(0)

    failAfter(Span(10, Seconds)) {
      val thread1 = new TaskThread("thread1", 1)
      thread1.initialize()
      val thread2 = new TaskThread("thread2", 2)
      thread2.initialize()

      try {
        withResource(new AllocOnAnotherThread(thread1, 1024, preferPinned = false)) { a =>
          a.waitForAlloc()
          a.assertAllocSize(1024)
          a.freeAndWait()
        }

        withResource(new AllocOnAnotherThread(thread1,4 * 1024)) { a =>
          a.waitForAlloc()
          a.assertAllocSize(4 * 1024)

          withResource(new AllocOnAnotherThread(thread2, 1)) { a2 =>
            // We ran out of memory, and this should have blocked
            thread2.waitForBlockedOnAlloc()

            a.freeOnThread()
            a2.waitForAlloc()
            a2.assertAllocSize(1)
            a2.freeAndWait()
          }
        }
      } finally {
        thread1.done.get(1, TimeUnit.SECONDS)
        thread2.done.get(1, TimeUnit.SECONDS)
      }
    }
  }

  test("simple non-pinned blocking alloc") {
    PinnedMemoryPool.initialize(0)
    HostAlloc.initialize(4 * 1024)

    failAfter(Span(10, Seconds)) {
      val thread1 = new TaskThread("thread1", 1)
      thread1.initialize()
      val thread2 = new TaskThread("thread2", 2)
      thread2.initialize()

      try {
        withResource(new AllocOnAnotherThread(thread1, 1024, preferPinned = false)) { a =>
          a.waitForAlloc()
          a.assertAllocSize(1024)
          a.freeAndWait()
        }

        withResource(new AllocOnAnotherThread(thread1, 4 * 1024)) { a =>
          a.waitForAlloc()
          a.assertAllocSize(4 * 1024)

          withResource(new AllocOnAnotherThread(thread2, 1)) { a2 =>
            // We ran out of memory, and this should have blocked
            thread2.waitForBlockedOnAlloc()

            a.freeOnThread()
            a2.waitForAlloc()
            a2.assertAllocSize(1)
            a2.freeAndWait()
          }
        }
      } finally {
        thread1.done.get(1, TimeUnit.SECONDS)
        thread2.done.get(1, TimeUnit.SECONDS)
      }
    }
  }

  test("simple mixed blocking alloc") {
    PinnedMemoryPool.initialize(4 * 1024)
    HostAlloc.initialize(4 * 1024)

    failAfter(Span(10, Seconds)) {
      val thread1 = new TaskThread("thread1", 1)
      thread1.initialize()
      val thread2 = new TaskThread("thread2", 2)
      thread2.initialize()
      val thread3 = new TaskThread("thread3", 3)
      thread3.initialize()

      try {
        withResource(new AllocOnAnotherThread(thread1, 4 * 1024)) { a =>
          a.waitForAlloc()
          a.assertAllocSize(4 * 1024)

          withResource(new AllocOnAnotherThread(thread2, 4 * 1024, false)) { a2 =>
            a2.waitForAlloc()
            a2.assertAllocSize(4 * 1024)

            withResource(new AllocOnAnotherThread(thread3, 1, false)) { a3 =>
              // We ran out of memory, and this should have blocked
              thread3.waitForBlockedOnAlloc()
              a.freeAndWait()
              // We should still be unblocked because pinned memory was freed, and even if we
              // don't really want pinned, it is memory, so lets go
              a3.waitForAlloc()
              a3.assertAllocSize(1)
              a3.waitForFree()

              a2.waitForFree()
            }
          }
        }
      } finally {
        thread1.done.get(1, TimeUnit.SECONDS)
        thread2.done.get(1, TimeUnit.SECONDS)
        thread3.done.get(1, TimeUnit.SECONDS)
      }
    }
  }

  test("pinned blocking alloc with spill") {
    PinnedMemoryPool.initialize(4 * 1024)
    HostAlloc.initialize(0)

    failAfter(Span(10, Seconds)) {
      val thread1 = new TaskThread("thread1", 1)
      thread1.initialize()
      val thread2 = new TaskThread("thread2", 2)
      thread2.initialize()

      try {
        withResource(new AllocOnAnotherThread(thread1, 4 * 1024)) { a =>
          a.waitForAlloc()
          a.assertAllocSize(4 * 1024)
          // We don't have a way to wake up a thread when something is marked as spillable yet
          // https://github.com/NVIDIA/spark-rapids/issues/9216
          a.makeSpillableOnThread()
          a.waitForSpillable()

          withResource(new AllocOnAnotherThread(thread2, 1)) { a2 =>
            // We ran out of memory, but instead of blocking this should spill...
            a2.waitForAlloc()
            a2.assertAllocSize(1)

            a.freeOnThread()
            a2.freeAndWait()
          }
        }
      } finally {
        thread1.done.get(1, TimeUnit.SECONDS)
        thread2.done.get(1, TimeUnit.SECONDS)
      }
    }
  }

  test("non-pinned blocking alloc with spill") {
    PinnedMemoryPool.initialize(0)
    HostAlloc.initialize(4 * 1024)

    failAfter(Span(10, Seconds)) {
      val thread1 = new TaskThread("thread1", 1)
      thread1.initialize()
      val thread2 = new TaskThread("thread2", 2)
      thread2.initialize()

      try {
        withResource(new AllocOnAnotherThread(thread1, 4 * 1024)) { a =>
          a.waitForAlloc()
          a.assertAllocSize(4 * 1024)
          // We don't have a way to wake up a thread when something is marked as spillable yet
          // https://github.com/NVIDIA/spark-rapids/issues/9216
          a.makeSpillableOnThread()
          a.waitForSpillable()

          withResource(new AllocOnAnotherThread(thread2, 1)) { a2 =>
            // We ran out of memory, but instead of blocking this should spill...
            a2.waitForAlloc()
            a2.assertAllocSize(1)

            a.freeOnThread()
            a2.freeAndWait()
          }
        }
      } finally {
        thread1.done.get(1, TimeUnit.SECONDS)
        thread2.done.get(1, TimeUnit.SECONDS)
      }
    }
  }

  test("mixed blocking alloc with spill") {
    PinnedMemoryPool.initialize(4 * 1024)
    HostAlloc.initialize(4 * 1024)

    failAfter(Span(10, Seconds)) {
      val thread1 = new TaskThread("thread1", 1)
      thread1.initialize()
      val thread2 = new TaskThread("thread2", 2)
      thread2.initialize()
      val thread3 = new TaskThread("thread3", 3)
      thread3.initialize()

      try {
        withResource(new AllocOnAnotherThread(thread1, 4 * 1024)) { a =>
          a.waitForAlloc()
          a.assertAllocSize(4 * 1024)
          // We don't have a way to wake up a thread when something is marked as spillable yet
          // https://github.com/NVIDIA/spark-rapids/issues/9216
          a.makeSpillableOnThread()
          a.waitForSpillable()


          withResource(new AllocOnAnotherThread(thread2, 4 * 1024, false)) { a2 =>
            a2.waitForAlloc()
            a2.assertAllocSize(4 * 1024)

            withResource(new AllocOnAnotherThread(thread3, 1, false)) { a3 =>
              // We should still not be blocked because pinned memory was made spillable
              a3.waitForAlloc()
              a3.assertAllocSize(1)
              a3.waitForFree()

              a2.waitForFree()
            }
          }
        }
      } finally {
        thread1.done.get(1, TimeUnit.SECONDS)
        thread2.done.get(1, TimeUnit.SECONDS)
        thread3.done.get(1, TimeUnit.SECONDS)
      }
    }
  }

  test("split should not happen immediately after fallback on memory contention") {

    ThreadStateRegistry.enablePrintStackTraceCausingThreadBlocked()

    // It allocates a small piece of memory (1024) as a warmup, sleep 1s,
    // then allocates a large piece of memory
    class AllocOnAnotherThreadWithWarmup(override val thread: TaskThread,
        override val size: Long) extends AllocOnAnotherThread(thread, size) {

      override def doAlloc(): Void = {
        RmmRapidsRetryIterator.withRetryNoSplit {

          // read shuffle data from remote and put it into Spillalble Framework
          val w = SpillableHostBuffer.apply(HostAlloc.alloc(1024, preferPinned),
            1024, SpillPriorities.ACTIVE_BATCHING_PRIORITY)

          // let's say we will do sth with the shuffle data read, e.g. concat them
          // for simplicity, we just have one SpillableHostBuffer as input, in real case there
          // should be multiple SpillableHostBuffer as input
          withResource(w.getHostBuffer()) { _ =>

            // Why can't we just use Thread.sleep here? Because TIMED_WAITING is a special state
            // treated as BUFN in spark_resource_adaptor::is_thread_bufn_or_above, this will cause
            // issues like https://github.com/NVIDIA/spark-rapids/issues/12883
            val startTime = System.currentTimeMillis
            while (System.currentTimeMillis < startTime + 100) {
              Thread.`yield`() // when yielding, the thread is in RUNNABLE state
            }

            // create another buffer as destination for concat
            val tmp = HostAlloc.alloc(size, preferPinned)
            synchronized {
              closeOnExcept(tmp) { _ =>
                assert(b.isEmpty)
                b = Option(tmp)
              }
            }
          }

          // at some point close w
          w.close()
        }
        null
      }
    }

    PinnedMemoryPool.initialize(0)
    HostAlloc.initialize(10 * 1024) // memory only big enough for the one concurrent thread to pass

    failAfter(Span(10, Seconds)) {
      val thread1 = new TaskThread("thread1", 1)
      thread1.initialize()
      val thread2 = new TaskThread("thread2", 2)
      thread2.initialize()

      try {
        // Actually it will require 10 * 1024 memory
        withResource(new AllocOnAnotherThreadWithWarmup(thread2, 9 * 1024)) { a =>

          // Actually it will require 10 * 1024 memory
          withResource(new AllocOnAnotherThreadWithWarmup(thread1, 9 * 1024)) { b =>

            // At this point, both threads are blocked because of memory contention,
            // we expect both thread1 and thread2 to fall back to BUFN state, and then thread1
            // should attempt another try before doing splitting

            b.waitForAlloc()
            b.assertAllocSize(9 * 1024)
            b.freeAndWait()
          }
        }
      } finally {
        thread1.done.get(1, TimeUnit.SECONDS)
        thread2.done.get(1, TimeUnit.SECONDS)

        ThreadStateRegistry.disablePrintStackTraceCausingThreadBlocked()
      }
    }
  }
}