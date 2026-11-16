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

/*** spark-rapids-shim-json-lines
{"spark":["330","340","350"],"carryForward":"sameMinor"}
spark-rapids-shim-json-lines ***/
package org.apache.spark.sql.rapids.execution.python.shims

import java.io.{ByteArrayOutputStream, OutputStream}
import java.net.Socket
import java.util.concurrent.{CountDownLatch, LinkedBlockingQueue, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong, AtomicReference}

import scala.collection.mutable.ArrayBuffer
import scala.util.Try

import ai.rapids.cudf.{ColumnVector, DeviceMemoryBuffer, Rmm, RmmAllocationMode,
  RmmCudaMemoryResource, RmmDeviceMemoryResource, RmmEventHandler, RmmLimitingResourceAdaptor,
  RmmTrackingResourceAdaptor}
import com.nvidia.spark.rapids.{GpuColumnVector, RapidsConf, RapidsShuffleHandle,
  RmmSparkRetrySuiteBase, ScalableTaskCompletion, ShuffleReceivedBufferCatalog,
  TaskRegistryTracker}
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.jni.{GpuRetryOOM, GpuSplitAndRetryOOM, RmmSpark,
  RmmSparkThreadState}
import com.nvidia.spark.rapids.shuffle.{RapidsShuffleClient, RapidsShuffleFetchHandler,
  RapidsShuffleIterator, RapidsShuffleTestHelper, RapidsShuffleTransport,
  TestShuffleMetricsUpdater}
import com.nvidia.spark.rapids.spill.SpillFramework
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{doAnswer, mockStatic, when}
import org.mockito.invocation.InvocationOnMock
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Seconds, Span}
import org.scalatestplus.mockito.MockitoSugar

import org.apache.spark.{SparkConf, SparkEnv, TaskContext}
import org.apache.spark.api.python.{ChainedPythonFunctions, PythonBroadcast, PythonEvalType,
  PythonFunction}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.serializer.SerializerManager
import org.apache.spark.shuffle.rapids.RapidsShuffleFetchFailedException
import org.apache.spark.sql.rapids.execution.TrampolineUtil
import org.apache.spark.sql.rapids.metrics.source.MockTaskContext
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.storage.{BlockId, ShuffleBlockBatchId}

/**
 * Spark 3.x writes a pandas UDF's input to Python from a thread of its own, which pulls the
 * task's input, a remote UCX shuffle among them. These tests run the writer of each Arrow runner
 * the plugin owns, and check how RmmSpark and `TaskRegistryTracker` see that thread while it
 * writes and after it ends.
 */
class GpuPythonWriterRegistrationSuite extends RmmSparkRetrySuiteBase
    with MockitoSugar with Eventually {

  /** A writer thread of either runner, with the members of Spark's writer that a test uses. */
  private class Writer(val thread: Thread, failure: () => Option[Throwable], stop: () => Unit) {
    def exception: Option[Throwable] = failure()
    def shutdownOnTaskCompletion(): Unit = stop()
  }

  /** How RmmSpark and the tracker saw a thread. */
  private case class Seen(threadId: Long, native: RmmSparkThreadState, dedicated: Boolean)

  // Every wait that should end does so long before this.
  private val boundMs = 20000L
  private val bound = Span(20, Seconds)

  private val schema = StructType(Seq(StructField("a", IntegerType)))
  private val mib = 1024L * 1024
  private var lastTaskId = 1000L

  override def beforeEach(): Unit = {
    super.beforeEach()
    // The base dedicates this thread to task 1, which RmmSpark's deadlock checks would count
    // although no test here runs it.
    RmmSpark.removeAllCurrentThreadAssociation()
    ScalableTaskCompletion.reset()
    TaskRegistryTracker.clearRegistry()
    val env = mock[SparkEnv]
    when(env.conf).thenReturn(new SparkConf(false))
    when(env.serializerManager).thenReturn(mock[SerializerManager])
    when(env.driverTmpDir).thenReturn(Some(System.getProperty("java.io.tmpdir")))
    SparkEnv.set(env)
  }

  override def afterEach(): Unit = {
    try {
      TrampolineUtil.unsetTaskContext()
      ScalableTaskCompletion.reset()
      TaskRegistryTracker.clearRegistry()
      SparkEnv.set(null)
    } finally {
      super.afterEach()
    }
  }

  private def newTaskId(): Long = {
    lastTaskId += 1
    lastTaskId
  }

  private def pythonFunctions: Seq[(ChainedPythonFunctions, Long)] = {
    val f = mock[PythonFunction]
    when(f.command).thenReturn(Seq.empty[Byte])
    when(f.envVars).thenReturn(new java.util.HashMap[String, String]())
    when(f.pythonIncludes).thenReturn(java.util.Collections.emptyList[String]())
    when(f.broadcastVars).thenReturn(java.util.Collections.emptyList[Broadcast[PythonBroadcast]]())
    when(f.pythonExec).thenReturn("python3")
    when(f.pythonVer).thenReturn("3.11")
    Seq((ChainedPythonFunctions(Seq(f)), 0L))
  }

  /** A socket whose output stays in memory, so that a writer runs without a Python worker. */
  private class MemorySocket extends Socket {
    private val out = new ByteArrayOutputStream()
    override def getOutputStream: OutputStream = out
    override def shutdownOutput(): Unit = {}
  }

  private class OrdinaryRunner extends GpuArrowPythonRunner(pythonFunctions,
      PythonEvalType.SQL_SCALAR_PANDAS_UDF, Array(Array(0)), schema, "UTC", Map.empty, 1024L,
      schema) {
    def writer(input: Iterator[ColumnarBatch], ctx: TaskContext): WriterThread =
      newWriterThread(SparkEnv.get, new MemorySocket, input, 0, ctx)
  }

  private class CogroupedRunner extends GpuCoGroupedArrowPythonRunner(pythonFunctions,
      PythonEvalType.SQL_COGROUPED_MAP_PANDAS_UDF, Array(Array(0)), schema, schema, "UTC",
      Map.empty, 1024, schema) {
    def writer(input: Iterator[(ColumnarBatch, ColumnarBatch)], ctx: TaskContext): WriterThread =
      newWriterThread(SparkEnv.get, new MemorySocket, input, 0, ctx)
  }

  /** One of the two Arrow runners whose writer the plugin owns, fed one stream of batches. */
  private sealed abstract class Runner(name: String) {
    def writer(input: Iterator[ColumnarBatch], ctx: TaskContext): Writer
    override def toString: String = name
  }

  private object Ordinary extends Runner("ordinary") {
    override def writer(input: Iterator[ColumnarBatch], ctx: TaskContext): Writer = {
      val w = new OrdinaryRunner().writer(input, ctx)
      new Writer(w, () => w.exception, () => w.shutdownOnTaskCompletion())
    }
  }

  private object Cogrouped extends Runner("cogrouped") {
    override def writer(input: Iterator[ColumnarBatch], ctx: TaskContext): Writer = {
      val w = new CogroupedRunner().writer(input.map(left => (left, intBatch(4, 5))), ctx)
      new Writer(w, () => w.exception, () => w.shutdownOnTaskCompletion())
    }
  }

  private def intBatch(values: Int*): ColumnarBatch =
    closeOnExcept(ColumnVector.fromInts(values: _*)) { cv =>
      new ColumnarBatch(Array(GpuColumnVector.from(cv, IntegerType)), values.length)
    }

  private def seen(): Seen = {
    val id = RmmSpark.getCurrentThreadId
    Seen(id, RmmSpark.getStateOf(id),
      TaskRegistryTracker.isCurrentThreadRegisteredAsDedicatedTaskThread)
  }

  /** Passes `input` through, and records how the writer was seen when it asked for a batch. */
  private class Recording(input: Iterator[ColumnarBatch]) extends Iterator[ColumnarBatch] {
    private val first = new AtomicReference[Seen]()
    override def hasNext: Boolean = input.hasNext
    override def next(): ColumnarBatch = {
      first.compareAndSet(null, seen())
      input.next()
    }
    def writerSeen: Seen = Option(first.get).getOrElse(fail("the writer asked for no batch"))
  }

  /** Registers this thread as the task's own thread, as `Plugin.onTaskStart` does. */
  private def registerTaskThread(ctx: TaskContext): Long = {
    TrampolineUtil.setTaskContext(ctx)
    TaskRegistryTracker.registerDedicatedThreadForRetry()
    RmmSpark.getCurrentThreadId
  }

  /** A task whose own thread is this one. */
  private class Task[C <: TaskContext](val ctx: C) {
    val id: Long = ctx.taskAttemptId()
    val thread: Long = registerTaskThread(ctx)
  }

  private def newTask(): Task[MockTaskContext] = new Task(new MockTaskContext(newTaskId(), 0))

  /**
   * Whether the tracker holds a thread in its registered set, in its dedicated set and in the
   * task's list. The tracker offers no query for another thread, so this reads its fields.
   */
  private def tracked(threadId: Long, taskId: Long): (Boolean, Boolean, Boolean) =
    TaskRegistryTracker.synchronized {
      def field[T](name: String): T = {
        val f = TaskRegistryTracker.getClass.getDeclaredFields.find(_.getName.endsWith(name))
          .getOrElse(fail(s"TaskRegistryTracker has no field $name"))
        f.setAccessible(true)
        f.get(TaskRegistryTracker).asInstanceOf[T]
      }
      val threads = field[java.util.Map[Long, ArrayBuffer[Long]]]("taskToThread").get(taskId)
      (field[java.util.Set[Long]]("registeredThreads").contains(threadId),
        field[java.util.Set[Long]]("dedicatedTaskThreads").contains(threadId),
        threads != null && threads.contains(threadId))
    }

  /** A thread's native state, and whether the tracker holds it, as `tracked` answers. */
  private type Held = (RmmSparkThreadState, (Boolean, Boolean, Boolean))

  private def held(threadId: Long, taskId: Long): Held =
    (RmmSpark.getStateOf(threadId), tracked(threadId, taskId))

  private val notHeld: Held = (RmmSparkThreadState.UNKNOWN, (false, false, false))

  private def assertNoRegistration(threadId: Long, taskId: Long): Unit =
    assertResult(notHeld)(held(threadId, taskId))

  private def assertTaskThreadRegistered(threadId: Long, taskId: Long): Unit = {
    assertResult(RmmSparkThreadState.THREAD_RUNNING)(RmmSpark.getStateOf(threadId))
    assertResult((true, true, true))(tracked(threadId, taskId))
  }

  private def thread(name: String)(body: => Unit): Thread = {
    val t = new Thread(new Runnable { override def run(): Unit = body }, name)
    t.setDaemon(true)
    t
  }

  private def assertEnds(t: Thread): Unit = {
    t.join(boundMs)
    assert(!t.isAlive, s"${t.getName} did not end")
  }

  private def startAndEnd(writer: Writer): Unit = {
    writer.thread.start()
    assertEnds(writer.thread)
  }

  /**
   * Runs `runner`'s writer over `input` for a new task, with `run`, which also checks how the
   * writer ended. Then checks that the writer was dedicated to the task while it wrote, that it
   * left no registration behind, and that the task's own thread kept its registration.
   */
  private def runAndCheckRegistration(runner: Runner, input: Long => Iterator[ColumnarBatch])(
      run: Writer => Unit): Unit = {
    val task = newTask()
    val recording = new Recording(input(task.id))
    run(runner.writer(recording, task.ctx))
    assert(recording.writerSeen.dedicated, "the tracker did not hold the writer as dedicated")
    assertResult(RmmSparkThreadState.THREAD_RUNNING)(recording.writerSeen.native)
    assertNoRegistration(recording.writerSeen.threadId, task.id)
    assertTaskThreadRegistered(task.thread, task.id)
  }

  /**
   * A shuffle iterator over one remote block, as a UCX reader makes it. Its client hands the
   * fetch handler to `onFetch` on the thread that fetches, which is the first one to ask for a
   * batch. A batch handed to the handler comes back as three rows. Build it with the task's
   * context set, as a reader does.
   */
  private def remoteShuffle(taskId: Long, metrics: TestShuffleMetricsUpdater)(
      onFetch: RapidsShuffleFetchHandler => Unit): RapidsShuffleIterator = {
    val client = mock[RapidsShuffleClient]
    doAnswer { invocation =>
      onFetch(invocation.getArgument[RapidsShuffleFetchHandler](1))
      null
    }.when(client).doFetch(any(), any())
    val transport = mock[RapidsShuffleTransport]
    when(transport.makeClient(any())).thenReturn(client)
    val catalog = mock[ShuffleReceivedBufferCatalog]
    doAnswer(_ => (intBatch(1, 2, 3), 12L)).when(catalog)
      .getColumnarBatchAndRemove(any[RapidsShuffleHandle](), any())
    val block: (BlockId, Long, Int) = (ShuffleBlockBatchId(1, 1, 1, 1), 123L, 1)
    new RapidsShuffleIterator(RapidsShuffleTestHelper.makeMockBlockManager("1", "local"),
      mock[RapidsConf], transport,
      Array((RapidsShuffleTestHelper.makeMockBlockManager("2", "remote"), Seq(block))),
      metrics, Array(IntegerType), taskId, catalog, timeoutSeconds = 30)
  }

  /** Runs `writer` on a thread of its own, on which RmmSpark's `method` throws `failure`. */
  private def runWithFailingRmmSpark(method: String, failure: Throwable)(writer: Writer): Long = {
    val threadId = new AtomicLong()
    // Mockito intercepts a static only on the thread that mocked it.
    val t = thread("writer-with-failing-rmm-spark") {
      threadId.set(RmmSpark.getCurrentThreadId)
      val rmmSpark = mockStatic(classOf[RmmSpark], (invocation: InvocationOnMock) =>
        if (invocation.getMethod.getName == method) throw failure else invocation.callRealMethod())
      try {
        writer.thread.run()
      } finally {
        rmmSpark.close()
      }
    }
    t.start()
    assertEnds(t)
    threadId.get
  }

  private class TestRmmEventHandler extends RmmEventHandler {
    override def getAllocThresholds: Array[Long] = null
    override def getDeallocThresholds: Array[Long] = null
    override def onAllocThreshold(totalAllocSize: Long): Unit = {}
    override def onDeallocThreshold(totalAllocSize: Long): Unit = {}
    override def onAllocFailure(sizeRequested: Long, retryCount: Int): Boolean = false
  }

  /** Runs `body` with RmmSpark over a GPU resource of `limit` bytes, then restores the default. */
  private def withLimitedRmm(limit: Long)(body: => Unit): Unit = {
    SpillFramework.shutdown()
    RmmSpark.clearEventHandler()
    Rmm.shutdown()
    // RmmSpark's event handler needs a tracking resource under it.
    val resource = closeOnExcept(new RmmCudaMemoryResource()) { cuda =>
      new RmmTrackingResourceAdaptor[RmmDeviceMemoryResource](
        new RmmLimitingResourceAdaptor[RmmDeviceMemoryResource](cuda, limit, 256), 256)
    }
    closeOnExcept(resource)(Rmm.setCurrentDeviceResource(_, null, false))
    try {
      RmmSpark.setEventHandler(new TestRmmEventHandler)
      body
    } finally {
      RmmSpark.clearEventHandler()
      Rmm.shutdown()
      Rmm.initialize(RmmAllocationMode.CUDA_DEFAULT, null, 512 * mib)
      RmmSpark.setEventHandler(new TestRmmEventHandler)
    }
  }

  /**
   * A task's thread that keeps running while it holds `bytes` of GPU memory, until `release`,
   * as a task thread reading Python's output does.
   */
  private class Holder(ctx: TaskContext, bytes: Long) {
    val holding = new CountDownLatch(1)
    val release = new AtomicBoolean(false)
    val failure = new AtomicReference[Throwable]()
    val t: Thread = thread(s"holder-${ctx.taskAttemptId()}") {
      try {
        registerTaskThread(ctx)
        withResource(DeviceMemoryBuffer.allocate(bytes)) { _ =>
          holding.countDown()
          while (!release.get()) {
            Thread.`yield`()
          }
        }
      } catch {
        case e: Throwable => failure.set(e)
      }
    }
    t.start()
    assert(holding.await(boundMs, TimeUnit.MILLISECONDS), "the holder could not allocate")
  }

  /**
   * A writer's input whose batch allocates `bytes` of GPU memory, which it frees again before it
   * returns the batch, or throws if its thread was interrupted meanwhile.
   */
  private class Allocating(bytes: Long) extends Iterator[ColumnarBatch] {
    val threadId = new AtomicLong(-1)
    private var done = false
    override def hasNext: Boolean = !done
    override def next(): ColumnarBatch = {
      done = true
      threadId.set(RmmSpark.getCurrentThreadId)
      withResource(DeviceMemoryBuffer.allocate(bytes))(_ => ())
      if (Thread.interrupted()) {
        throw new InterruptedException("the task was killed while the writer allocated")
      }
      intBatch(1)
    }
  }

  /**
   * A task context that a test can kill and complete. It runs its completion listeners last in,
   * first out, as Spark's own does, so a Python runner's listener, added after the tracker's,
   * stops the writer before the tracker removes the task.
   */
  private class KillableTaskContext(taskId: Long) extends MockTaskContext(taskId, 0) {
    @volatile private var killed = false
    @volatile private var completed = false
    override def isInterrupted(): Boolean = killed
    override def isCompleted(): Boolean = completed
    def kill(): Unit = killed = true
    def complete(): Unit = {
      completed = true
      listeners.reverse.foreach(_.onTaskCompletion(this))
    }
  }

  /**
   * Starts `writer` as a Python runner does, with the listener that stops it at task end. Returns
   * how RmmSpark and the tracker held the writer's thread just after that listener stopped it,
   * which is before the tracker's own listener cleans up the task.
   */
  private def startPythonWriter(writer: Writer, ctx: KillableTaskContext,
      writerThread: AtomicLong): AtomicReference[Held] = {
    val afterStop = new AtomicReference[Held]()
    ctx.addTaskCompletionListener[Unit] { _ =>
      writer.shutdownOnTaskCompletion()
      afterStop.set(held(writerThread.get, ctx.taskAttemptId()))
    }
    writer.thread.start()
    afterStop
  }

  /** Kills the task, and completes it on a thread of its own, as an executor ends a task. */
  private def killTask(ctx: KillableTaskContext): Thread = {
    ctx.kill()
    val completer = thread(s"task-${ctx.taskAttemptId()}-completion")(ctx.complete())
    completer.start()
    completer
  }

  /**
   * Starts a Python writer for `task` whose input allocates `bytes`, and kills the task once the
   * writer waits inside that allocation. Checks that the writer still waits once the kill has
   * interrupted it, then runs `release` and checks that the writer ends, and that it removed its
   * own registration before the task's cleanup ran.
   */
  private def checkKilledWhileAllocating(task: Task[KillableTaskContext], bytes: Long)(
      release: => Unit): Unit = {
    val input = new Allocating(bytes)
    var writer: Writer = null
    var completer: Thread = null
    var stopped: AtomicReference[Held] = null
    try {
      writer = Ordinary.writer(input, task.ctx)
      stopped = startPythonWriter(writer, task.ctx, input.threadId)
      eventually(timeout(bound)) {
        assertResult(RmmSparkThreadState.THREAD_BLOCKED)(RmmSpark.getStateOf(input.threadId.get))
      }
      completer = killTask(task.ctx)
      // Spark's listener interrupts the writer, then joins it. An interrupt does not end a wait
      // inside RmmSpark, so the writer still waits while the join does.
      eventually(timeout(bound)) {
        assert(writer.thread.isInterrupted, "the kill did not interrupt the writer")
        assertResult(Thread.State.WAITING)(completer.getState)
      }
      assert(writer.thread.isAlive)
      assertResult(RmmSparkThreadState.THREAD_BLOCKED)(RmmSpark.getStateOf(input.threadId.get))
    } finally {
      release
      Option(writer).foreach(w => assertEnds(w.thread))
      Option(completer).foreach(assertEnds)
    }
    assertResult(notHeld, "when the Python listener stopped the writer")(stopped.get)
    assertNoRegistration(task.thread, task.id)
  }

  Seq(Ordinary, Cogrouped).foreach { runner =>
    test(s"the $runner runner's writer reads a remote shuffle batch as a dedicated thread") {
      val handle = mock[RapidsShuffleHandle]
      val metrics = new TestShuffleMetricsUpdater
      runAndCheckRegistration(runner, taskId => remoteShuffle(taskId, metrics) { handler =>
        handler.start(1)
        handler.batchReceived(handle)
      }) { writer =>
        startAndEnd(writer)
        writer.exception.foreach(e => fail("the writer failed", e))
        assertResult(3)(metrics.totalRowsFetched)
      }
    }
  }

  test("a writer whose input fails leaves no registration") {
    val failure = new IllegalStateException("the input failed")
    runAndCheckRegistration(Ordinary, _ => Iterator.fill[ColumnarBatch](1)(throw failure)) {
      writer =>
        startAndEnd(writer)
        assert(writer.exception.contains(failure))
    }
  }

  test("an interrupted writer leaves no registration") {
    val waiting = new CountDownLatch(1)
    runAndCheckRegistration(Ordinary, _ => Iterator.fill(1) {
      waiting.countDown()
      new LinkedBlockingQueue[ColumnarBatch]().take()
    }) { writer =>
      writer.thread.start()
      try {
        assert(waiting.await(boundMs, TimeUnit.MILLISECONDS), "the writer did not ask for input")
      } finally {
        writer.thread.interrupt()
        assertEnds(writer.thread)
      }
      assert(writer.exception.exists(_.isInstanceOf[InterruptedException]), writer.exception)
    }
  }

  test("a failure to remove the writer's registration is added to the writer's own failure") {
    val task = newTask()
    val primary = new IllegalStateException("the input failed")
    val cleanup = new IllegalStateException("the removal failed")
    val writer = Ordinary.writer(Iterator.fill[ColumnarBatch](1)(throw primary), task.ctx)
    runWithFailingRmmSpark("removeCurrentDedicatedThreadAssociation", cleanup)(writer)
    assert(writer.exception.contains(primary))
    assert(primary.getSuppressed.toSeq == Seq(cleanup))
  }

  test("a failure to remove the writer's registration fails a writer that succeeded") {
    val task = newTask()
    val cleanup = new IllegalStateException("the removal failed")
    val writer = Ordinary.writer(Iterator.fill(1)(intBatch(1, 2)), task.ctx)
    runWithFailingRmmSpark("removeCurrentDedicatedThreadAssociation", cleanup)(writer)
    assert(writer.exception.contains(cleanup))
  }

  test("a writer whose registration fails partway leaves no registration") {
    val task = newTask()
    val failure = new IllegalStateException("the registration failed")
    val unread = new IllegalStateException("the writer read its input")
    val writer = Ordinary.writer(Iterator.fill[ColumnarBatch](1)(throw unread), task.ctx)
    val writerThread =
      runWithFailingRmmSpark("currentThreadIsDedicatedToTask", failure)(writer)
    assert(writer.exception.contains(failure))
    assertNoRegistration(writerThread, task.id)
    assertTaskThreadRegistered(task.thread, task.id)
  }

  test("a writer on an ended writer's native thread ID stays registered when that task ends") {
    val taskA = newTask()
    val ctxB = new MockTaskContext(newTaskId(), 1)
    val inputA = new Recording(Iterator.fill(1)(intBatch(1)))
    val writing = new CountDownLatch(1)
    val taskAEnded = new CountDownLatch(1)
    val whileA = new AtomicReference[Seen]()
    val afterA = new AtomicReference[Seen]()
    val waitAfterA = new AtomicReference[Try[Unit]]()
    val inputB = Iterator.fill(1) {
      whileA.set(seen())
      writing.countDown()
      taskAEnded.await(boundMs, TimeUnit.MILLISECONDS)
      afterA.set(seen())
      waitAfterA.set(Try {
        RmmSpark.waitingOnPool()
        RmmSpark.doneWaitingOnPool()
      })
      intBatch(1)
    }
    val writerA = Ordinary.writer(inputA, taskA.ctx)
    val writerB = Ordinary.writer(inputB, ctxB)
    // Both writers run on one thread, so the second has the first's native ID.
    val writers = thread("writer-a-then-writer-b") {
      writerA.thread.run()
      writerB.thread.run()
    }
    writers.start()
    try {
      assert(writing.await(boundMs, TimeUnit.MILLISECONDS), "the second writer did not start")
      taskA.ctx.markTaskComplete()
    } finally {
      taskAEnded.countDown()
      assertEnds(writers)
    }
    writerA.exception.foreach(e => fail("the first writer failed", e))
    writerB.exception.foreach(e => fail("the second writer failed", e))
    assertResult(inputA.writerSeen.threadId)(whileA.get.threadId)
    assertResult(Seen(whileA.get.threadId, RmmSparkThreadState.THREAD_RUNNING, dedicated = true))(
      afterA.get)
    waitAfterA.get.get
  }

  test("a registered writer's wait for a remote batch lets RmmSpark recover a blocked receive") {
    withLimitedRmm(10 * mib) {
      val taskId = newTaskId()
      val ctx = new MockTaskContext(taskId, 0)
      val taskThread = new Holder(ctx, 6 * mib)
      val receiveId = new AtomicLong(-1)
      val retryOom = new AtomicReference[Throwable]()
      val retryOomAfterFetch = new AtomicBoolean(false)
      val handler = new AtomicReference[RapidsShuffleFetchHandler]()
      val fetching = new CountDownLatch(1)
      val receiveFailure = new AtomicReference[Throwable]()
      // A UCX receive for the task, which cannot get its memory while the task thread holds it.
      val receive = thread("receive") {
        try {
          receiveId.set(RmmSpark.getCurrentThreadId)
          RmmSpark.shuffleThreadWorkingOnTasks(Array(taskId))
          try {
            withResource(DeviceMemoryBuffer.allocate(6 * mib))(_ => ())
          } catch {
            case oom @ (_: GpuRetryOOM | _: GpuSplitAndRetryOOM) =>
              // The writer's fetch counts `fetching` down before the writer waits for the batch.
              retryOomAfterFetch.set(fetching.getCount == 0)
              retryOom.set(oom)
              if (fetching.await(boundMs, TimeUnit.MILLISECONDS)) {
                handler.get.transferError("the receive could not allocate", oom)
              }
          } finally {
            RmmSpark.shuffleThreadFinishedForTasks(Array(taskId))
          }
        } catch {
          case t: Throwable => receiveFailure.set(t)
        }
      }
      var writer: Writer = null
      try {
        receive.start()
        eventually(timeout(bound)) {
          assertResult(RmmSparkThreadState.THREAD_BLOCKED)(RmmSpark.getStateOf(receiveId.get))
        }
        TrampolineUtil.setTaskContext(ctx)
        writer = Ordinary.writer(remoteShuffle(taskId, new TestShuffleMetricsUpdater) { h =>
          handler.set(h)
          fetching.countDown()
        }, ctx)
        writer.thread.start()
        eventually(timeout(bound)) {
          assert(retryOom.get != null, "the blocked receive got no retry OOM")
        }
        assert(retryOomAfterFetch.get, "the receive got its retry OOM before the writer fetched")
        assertEnds(writer.thread)
        writer.exception match {
          case Some(e: RapidsShuffleFetchFailedException) => assert(e.getCause eq retryOom.get)
          case other => fail(s"the writer ended with $other")
        }
      } finally {
        taskThread.release.set(true)
        assertEnds(receive)
        assertEnds(taskThread.t)
        Option(writer).foreach(w => assertEnds(w.thread))
      }
      Option(receiveFailure.get).foreach(e => fail("the receive failed", e))
      Option(taskThread.failure.get).foreach(e => fail("the task thread failed", e))
    }
  }

  test("a writer killed while it waits for memory ends once its task's other work ends") {
    withLimitedRmm(10 * mib) {
      val task = new Task(new KillableTaskContext(newTaskId()))
      val endPoolWork = new AtomicBoolean(false)
      val poolFailure = new AtomicReference[Throwable]()
      // Shuffle work for the task, which keeps RmmSpark from finding the task stuck.
      val pool = thread("pool-work") {
        try {
          RmmSpark.shuffleThreadWorkingOnTasks(Array(task.id))
          while (!endPoolWork.get()) {
            Thread.`yield`()
          }
          RmmSpark.shuffleThreadFinishedForTasks(Array(task.id))
        } catch {
          case t: Throwable => poolFailure.set(t)
        }
      }
      pool.start()
      // More than the resource holds, so the allocation can only wait or fail.
      checkKilledWhileAllocating(task, 12 * mib) {
        endPoolWork.set(true)
        assertEnds(pool)
      }
      Option(poolFailure.get).foreach(e => fail("the pool work failed", e))
    }
  }

  test("a writer killed while another task holds the memory ends once that memory is freed") {
    withLimitedRmm(10 * mib) {
      val task = new Task(new KillableTaskContext(newTaskId()))
      val other = new Holder(new MockTaskContext(newTaskId(), 1), 6 * mib)
      checkKilledWhileAllocating(task, 6 * mib) {
        other.release.set(true)
        assertEnds(other.t)
      }
      Option(other.failure.get).foreach(e => fail("the other task failed", e))
    }
  }
}
