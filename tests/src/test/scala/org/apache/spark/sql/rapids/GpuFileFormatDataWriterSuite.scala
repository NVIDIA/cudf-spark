/*
 * Copyright (c) 2023-2026, NVIDIA CORPORATION.
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
package org.apache.spark.sql.rapids

import java.io.{IOException, OutputStream}
import java.net.URI
import java.util.concurrent.{ExecutorService, TimeUnit}

import ai.rapids.cudf.{CudaFatalException, HostMemoryBuffer, Rmm, RmmAllocationMode, Table,
  TableWriter}
import com.nvidia.spark.rapids.{ColumnarOutputWriter, ColumnarOutputWriterFactory, GpuColumnVector, GpuLiteral, NvtxId, NvtxRegistry, RapidsConf, ScalableTaskCompletion, SpillableColumnarBatch}
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.SpillPriorities.ACTIVE_ON_DECK_PRIORITY
import com.nvidia.spark.rapids.io.async.{AsyncOutputStream, HostMemoryThrottle, ThrottlingExecutor, TrafficController}
import com.nvidia.spark.rapids.jni.{CpuRetryOOM, GpuRetryOOM, GpuSplitAndRetryOOM}
import com.nvidia.spark.rapids.spill.SpillFramework
import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, FSDataInputStream, FSDataOutputStream, Path}
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.mapred.TaskAttemptContext
import org.apache.hadoop.util.Progressable
import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito._
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.mockito.MockitoSugar.mock

import org.apache.spark.SparkConf
import org.apache.spark.internal.io.FileCommitProtocol
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.expressions.{Ascending, AttributeReference, ExprId, SortOrder}
import org.apache.spark.sql.execution.datasources.WriteTaskStats
import org.apache.spark.sql.rapids.GpuFileFormatWriter.GpuConcurrentOutputWriterSpec
import org.apache.spark.sql.rapids.execution.TrampolineUtil
import org.apache.spark.sql.rapids.metrics.source.MockTaskContext
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}

class GpuFileFormatDataWriterSuite extends AnyFunSuite with BeforeAndAfterEach {
  private var mockJobDescription: GpuWriteJobDescription = _
  private var mockTaskAttemptContext: TaskAttemptContext = _
  private var mockCommitter: FileCommitProtocol = _
  private var mockOutputWriterFactory: ColumnarOutputWriterFactory = _
  private var mockOutputWriter: NoTransformColumnarOutputWriter = _
  private var allCols: Seq[AttributeReference] = _
  private var partSpec: Seq[AttributeReference] = _
  private var dataSpec: Seq[AttributeReference] = _
  private var bucketSpec: Option[GpuWriterBucketSpec] = None
  private var includeRetry: Boolean = false

  class NoTransformColumnarOutputWriter(
      context: TaskAttemptContext,
      dataSchema: StructType,
      nvtxId: NvtxId,
      includeRetry: Boolean)
        extends ColumnarOutputWriter(
          context,
          dataSchema,
          nvtxId,
          includeRetry,
          mockJobDescription.statsTrackers.map(_.newTaskInstance()),
          None,
          false,
          false,
          mockJobDescription.fileIO) {

    // this writer (for tests) doesn't do anything and passes through the
    // batch passed to it when asked to transform, which is done to
    // check for leaks
    override def transformAndClose(cb: ColumnarBatch): ColumnarBatch = cb
    override val tableWriter: TableWriter = mock[TableWriter]
    var nativeGpuOomRetryEnabled: Boolean = true
    override protected def canRetryGpuOomFromNativeWrite: Boolean = nativeGpuOomRetryEnabled
    override def getOutputStream: FSDataOutputStream = mock[FSDataOutputStream]
    def outputStreamMock: FSDataOutputStream = outputStream.asInstanceOf[FSDataOutputStream]
    override def path(): String = null
    private var throwOnce: Option[Throwable] = None
    override def bufferBatchAndClose(batch: ColumnarBatch): Long = {
      //closeOnExcept to maintain the contract of `bufferBatchAndClose`
      // we have to close the batch.
      closeOnExcept(batch) { _ =>
        throwOnce.foreach { t =>
          throwOnce = None
          throw t
        }
      }
      super.bufferBatchAndClose(batch)
    }

    def throwOnNextBufferBatchAndClose(exception: Throwable): Unit = {
      throwOnce = Some(exception)
    }

  }

  /** Records what reaches it and the thread that closed it; can fail its writes or its close. */
  class RecordingOutputStream(
      writeFailure: IOException = null,
      closeFailure: Throwable = null) extends OutputStream {
    @volatile var bytesWritten: Int = 0
    @volatile var closeCount: Int = 0
    @volatile var closeThread: Thread = _
    @volatile var interruptedAtClose: Boolean = false

    override def write(b: Int): Unit = write(Array(b.toByte), 0, 1)

    override def write(b: Array[Byte], off: Int, len: Int): Unit = {
      if (writeFailure != null) {
        throw writeFailure
      }
      bytesWritten += len
    }

    override def close(): Unit = {
      closeCount += 1
      closeThread = Thread.currentThread()
      interruptedAtClose = Thread.currentThread().isInterrupted
      if (closeFailure != null) {
        throw closeFailure
      }
    }
  }

  // ColumnarOutputWriter opens its stream in its constructor, before a subclass's own fields
  // are set, so StreamOutputWriter takes the stream from here.
  private var nextWriterStream: OutputStream = _

  /** A writer over a given stream that encodes nothing, so closing it needs no GPU work. */
  class StreamOutputWriter(debugPath: Option[String] = None) extends ColumnarOutputWriter(
      mockTaskAttemptContext,
      StructType(Seq.empty),
      NvtxRegistry.FILE_FORMAT_WRITE,
      false,
      Seq.empty,
      debugPath,
      rapidsFileIO = mockJobDescription.fileIO) {
    override val tableWriter: TableWriter = mock[TableWriter]
    override def getOutputStream: OutputStream = nextWriterStream
    override def path(): String = null
    override def bufferBatchAndClose(batch: ColumnarBatch): Long = {
      batch.close()
      0L
    }

    /** Buffers `len` bytes, which close() writes to the stream before closing it. */
    def bufferBytes(len: Int): Unit = handleBuffer(HostMemoryBuffer.allocate(len), len)

    /** Closes the way a writer that returns its footer does. */
    def closeReturning(footer: AutoCloseable): AutoCloseable = closeAndReturn(footer)

    /** Encodes `batch` as a real writer does, which also writes it to the debug stream. */
    def encode(batch: ColumnarBatch): Long = super.bufferBatchAndClose(batch)
  }

  /** A writer over `stream`, with `debugStream` as its debug dump when that is set. */
  def streamOutputWriter(
      stream: OutputStream,
      debugStream: OutputStream = null): StreamOutputWriter = {
    resetMocks()
    nextWriterStream = stream
    if (debugStream == null) {
      new StreamOutputWriter
    } else {
      // The writer opens its debug dump through the task's Hadoop configuration.
      val conf = new Configuration(false)
      conf.setClass("fs.probe.impl", classOf[DebugStreamFileSystem], classOf[FileSystem])
      conf.setBoolean("fs.probe.impl.disable.cache", true)
      when(mockTaskAttemptContext.getConfiguration).thenReturn(conf)
      DebugStreamFileSystem.nextStream = debugStream
      new StreamOutputWriter(Some("probe:/debug"))
    }
  }

  private val asyncWriterThreadName = "GpuFileFormatDataWriterSuite async output"

  /** The pools asyncOutputStream made in this test, stopped afterwards in case no close did. */
  private var asyncPools: List[ExecutorService] = Nil

  /** An async stream over `delegate`, with the pool that runs its writes and its close. */
  def asyncOutputStream(delegate: OutputStream): (AsyncOutputStream, ExecutorService) = {
    val pool = TrampolineUtil.newDaemonSingleThreadExecutor(asyncWriterThreadName)
    asyncPools ::= pool
    val throttle = new TrafficController(new HostMemoryThrottle(Long.MaxValue)) {}
    (new AsyncOutputStream(() => delegate, new ThrottlingExecutor(pool, throttle, _ => ())), pool)
  }

  def assertClosedOnWriterThread(delegate: RecordingOutputStream, pool: ExecutorService): Unit = {
    assert(delegate.closeCount == 1)
    assert(delegate.closeThread.getName == asyncWriterThreadName)
    assert(pool.isTerminated)
  }

  /**
   * Runs `body` on a new thread, interrupted first if `interruptFirst`, so the test runner's own
   * interrupt status is never touched. Returns what `body` threw, and whether the thread was
   * interrupted when `body` finished.
   */
  def runOnNewThread(interruptFirst: Boolean)(body: => Unit): (Option[Throwable], Boolean) = {
    var thrown: Option[Throwable] = None
    var interruptedAfter = false
    val thread = new Thread(() => {
      if (interruptFirst) {
        Thread.currentThread().interrupt()
      }
      try {
        body
      } catch {
        case t: Throwable => thrown = Some(t)
      }
      interruptedAfter = Thread.currentThread().isInterrupted
    })
    thread.setDaemon(true)
    thread.start()
    try {
      thread.join(TimeUnit.MINUTES.toMillis(1))
      assert(!thread.isAlive, "the closing thread did not finish")
    } finally {
      if (thread.isAlive) {
        // Do not leave the thread blocked behind a failed test.
        thread.interrupt()
        thread.join(TimeUnit.SECONDS.toMillis(1))
      }
    }
    (thrown, interruptedAfter)
  }

  def mockOutputWriter(types: StructType, includeRetry: Boolean): Unit = {
    mockOutputWriter = spy(new NoTransformColumnarOutputWriter(
      mockTaskAttemptContext,
      types,
      NvtxRegistry.FILE_FORMAT_WRITE,
      includeRetry))
    when(mockOutputWriterFactory.newInstance(any(), any(), any(), any(), any(), any()))
        .thenAnswer(_ => mockOutputWriter)
  }

  def resetMocks(): Unit = {
    ScalableTaskCompletion.reset()
    allCols = null
    partSpec = null
    dataSpec = null
    bucketSpec = None
    mockJobDescription = mock[GpuWriteJobDescription]
    when(mockJobDescription.statsTrackers).thenReturn(Seq.empty)
    mockTaskAttemptContext = mock[TaskAttemptContext]
    mockCommitter = mock[FileCommitProtocol]
    mockOutputWriterFactory = mock[ColumnarOutputWriterFactory]
    when(mockJobDescription.outputWriterFactory)
        .thenAnswer(_ => mockOutputWriterFactory)
  }

  def mockEmptyOutputWriter(): Unit = {
    resetMocks()
    mockOutputWriter(StructType(Seq.empty[StructField]), includeRetry = false)
  }

  def resetMocksWithAndWithoutRetry[V](body: => V): Unit = {
    Seq(false, true).foreach { retry =>
      resetMocks()
      includeRetry = retry
      body
    }
  }

  /**
   * This function takes a seq of GPU-backed `ColumnarBatch` instances and a function body.
   * It is used to setup certain mocks before `body` is executed. After execution, the
   * columns in the batches are checked for `refCount==0` (e.g. that they were closed).
   * @note it is assumed that the schema of each batch is identical.
   *    numBuckets > 0: Bucketing only
   *    numBuckets == 0: Partition only
   *    numBuckets < 0: Both partition and bucketing
   */
  def withColumnarBatchesVerifyClosed[V](
      cbs: Seq[ColumnarBatch], numBuckets: Int = 0)(body: => V): Unit = {
    val allTypes = cbs.map(GpuColumnVector.extractTypes)
    allCols = Seq.empty
    dataSpec = Seq.empty
    partSpec = Seq.empty
    if (allTypes.nonEmpty) {
      allCols = allTypes.head.zipWithIndex.map { case (dataType, colIx) =>
        AttributeReference(s"col_$colIx", dataType, nullable = false)(ExprId(colIx))
      }
      if (numBuckets <= 0) {
        partSpec = Seq(allCols.head)
        dataSpec = allCols.tail
      } else {
        dataSpec = allCols
      }
      if (numBuckets != 0) {
        bucketSpec = Some(GpuWriterBucketSpec(
          GpuPmod(GpuMurmur3Hash(Seq(allCols.last), 42), GpuLiteral(Math.abs(numBuckets))),
          _ => ""))
      }
    }
    val fields = new Array[StructField](allCols.size)
    allCols.zipWithIndex.foreach { case (col, ix) =>
      fields(ix) = StructField(col.name, col.dataType, nullable = col.nullable)
    }
    mockOutputWriter(StructType(fields), includeRetry)
    if (dataSpec.isEmpty) {
      dataSpec = allCols // special case for single column batches
    }
    when(mockJobDescription.dataColumns).thenReturn(dataSpec)
    when(mockJobDescription.partitionColumns).thenReturn(partSpec)
    when(mockJobDescription.bucketSpec).thenReturn(bucketSpec)
    when(mockJobDescription.allColumns).thenReturn(allCols)
    try {
      body
    } finally {
      verifyClosed(cbs)
    }
  }

  override def beforeEach(): Unit = {
    Rmm.initialize(RmmAllocationMode.CUDA_DEFAULT, null, 512 * 1024 * 1024)
    SpillFramework.initialize(new RapidsConf(new SparkConf))
  }

  override def afterEach(): Unit = {
    try {
      // A test that failed before its writer closed would otherwise leave its pool running.
      asyncPools.foreach(_.shutdownNow())
      asyncPools.foreach(_.awaitTermination(10, TimeUnit.SECONDS))
    } finally {
      asyncPools = Nil
      SpillFramework.shutdown()
      Rmm.shutdown()
    }
  }

  def buildEmptyBatch: ColumnarBatch =
    new ColumnarBatch(Array.empty[ColumnVector], 0)

  def buildBatchWithPartitionedCol(ints: Int*): ColumnarBatch = {
    val rowCount = ints.size
    val cols: Array[ColumnVector] = new Array[ColumnVector](2)
    val partCol = ai.rapids.cudf.ColumnVector.fromInts(ints:_*)
    val dataCol = ai.rapids.cudf.ColumnVector.fromStrings(ints.map(_.toString):_*)
    cols(0) = GpuColumnVector.from(partCol, IntegerType)
    cols(1) = GpuColumnVector.from(dataCol, StringType)
    new ColumnarBatch(cols, rowCount)
  }

  def buildBatchWithPartitionedAndBucketCols(
      partInts: Seq[Int], bucketInts: Seq[Int]): ColumnarBatch = {
    assert(partInts.length == bucketInts.length)
    val rowCount = partInts.size
    val cols: Array[ColumnVector] = new Array[ColumnVector](3)
    val partCol = ai.rapids.cudf.ColumnVector.fromInts(partInts: _*)
    val dataCol = ai.rapids.cudf.ColumnVector.fromStrings(partInts.map(_.toString): _*)
    val bucketCol = ai.rapids.cudf.ColumnVector.fromInts(bucketInts: _*)
    cols(0) = GpuColumnVector.from(partCol, IntegerType)
    cols(1) = GpuColumnVector.from(dataCol, StringType)
    cols(2) = GpuColumnVector.from(bucketCol, IntegerType)
    new ColumnarBatch(cols, rowCount)
  }

  def verifyClosed(cbs: Seq[ColumnarBatch]): Unit = {
    cbs.foreach { cb =>
      val cols = GpuColumnVector.extractBases(cb)
      cols.foreach { col =>
        assertResult(0)(col.getRefCount)
      }
    }
  }

  def prepareDynamicPartitionSingleWriter():
  GpuDynamicPartitionDataSingleWriter = {
    when(mockJobDescription.customPartitionLocations)
        .thenReturn(Map.empty[TablePartitionSpec, String])

    spy(new GpuDynamicPartitionDataSingleWriter(
      mockJobDescription,
      mockTaskAttemptContext,
      mockCommitter,
      None))
  }

  def prepareDynamicPartitionConcurrentWriter(maxWriters: Int, batchSize: Long):
  GpuDynamicPartitionDataConcurrentWriter = {
    val mockConfig = new Configuration()
    when(mockTaskAttemptContext.getConfiguration).thenReturn(mockConfig)
    when(mockJobDescription.customPartitionLocations)
        .thenReturn(Map.empty[TablePartitionSpec, String])
    val sortSpec = (partSpec ++ bucketSpec.map(_.bucketIdExpression))
      .map(SortOrder(_, Ascending))
    val concurrentSpec = GpuConcurrentOutputWriterSpec(
      maxWriters, allCols, batchSize, sortSpec)

    spy(new GpuDynamicPartitionDataConcurrentWriter(
      mockJobDescription,
      mockTaskAttemptContext,
      mockCommitter,
      concurrentSpec,
      None))
  }

  test("empty directory data writer") {
    mockEmptyOutputWriter()
    val emptyWriter = spy(new GpuEmptyDirectoryDataWriter(
      mockJobDescription, mockTaskAttemptContext, mockCommitter))
    emptyWriter.writeWithIterator(Iterator.empty)
    emptyWriter.commit()
    verify(emptyWriter, times(0))
        .write(any[ColumnarBatch])
    verify(emptyWriter, times(1))
        .releaseResources()
  }

  test("empty directory data writer with non-empty iterator") {
    mockEmptyOutputWriter()
    // this should never be the case, as the empty directory writer
    // is only instantiated when the iterator is empty. Adding it
    // because the expected behavior is to fully consume the iterator
    // and close all the empty batches.
    val emptyWriter = spy(new GpuEmptyDirectoryDataWriter(
      mockJobDescription, mockTaskAttemptContext, mockCommitter))
    val cbs = Seq(
      spy(buildEmptyBatch),
      spy(buildEmptyBatch))
    emptyWriter.writeWithIterator(cbs.iterator)
    emptyWriter.commit()
    verify(emptyWriter, times(2))
        .write(any[ColumnarBatch])
    verify(emptyWriter, times(1))
        .releaseResources()
    cbs.foreach { cb => verify(cb, times(1)).close()}
  }

  test("single directory data writer with empty iterator") {
    resetMocksWithAndWithoutRetry {
      // build a batch just so that the test code can infer the schema
      val cbs = Seq(buildBatchWithPartitionedCol(1))
      withColumnarBatchesVerifyClosed(cbs) {
        withResource(cbs) { _ =>
          val singleWriter = spy(new GpuSingleDirectoryDataWriter(
            mockJobDescription, mockTaskAttemptContext, mockCommitter, None))
          singleWriter.writeWithIterator(Iterator.empty)
          singleWriter.commit()
        }
      }
    }
  }

  test("single directory data writer") {
    resetMocksWithAndWithoutRetry {
      val cb = buildBatchWithPartitionedCol(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
      val cb2 = buildBatchWithPartitionedCol(1, 2, 3, 4, 5)
      val cbs = Seq(spy(cb), spy(cb2))
      withColumnarBatchesVerifyClosed(cbs) {
        val singleWriter = spy(new GpuSingleDirectoryDataWriter(
          mockJobDescription, mockTaskAttemptContext, mockCommitter, None))
        singleWriter.writeWithIterator(cbs.iterator)
        singleWriter.commit()
        // we write 2 batches
        verify(mockOutputWriter, times(2))
            .writeSpillableAndClose(any())
        verify(mockOutputWriter, times(1)).close()
      }
    }
  }

  test("CPU OOM during a native file write must fail the task") {
    resetMocks()
    includeRetry = true
    val cb = buildBatchWithPartitionedCol(1, 2, 3)
    withColumnarBatchesVerifyClosed(Seq(cb)) {
      val nativeWriter = mockOutputWriter.tableWriter
      doThrow(new CpuRetryOOM("native write failed"))
        .when(nativeWriter).write(any[Table]())
      val spillable = SpillableColumnarBatch(cb, ACTIVE_ON_DECK_PRIORITY)
      val failure = intercept[IllegalStateException] {
        mockOutputWriter.writeSpillableAndClose(spillable)
      }
      assert(failure.getSuppressed.exists(_.isInstanceOf[CpuRetryOOM]))
      verify(nativeWriter, times(1)).write(any[Table]())
    }
  }

  test("a native write error that withRetry would not retry is rethrown unchanged") {
    resetMocks()
    includeRetry = true
    val cb = buildBatchWithPartitionedCol(1, 2, 3)
    withColumnarBatchesVerifyClosed(Seq(cb)) {
      val nativeWriter = mockOutputWriter.tableWriter
      val writeError = new RuntimeException("native write failed")
      doThrow(writeError).when(nativeWriter).write(any[Table]())
      val spillable = SpillableColumnarBatch(cb, ACTIVE_ON_DECK_PRIORITY)
      val failure = intercept[RuntimeException] {
        mockOutputWriter.writeSpillableAndClose(spillable)
      }
      assert(failure eq writeError)
      verify(nativeWriter, times(1)).write(any[Table]())
    }
  }

  test("fatal CUDA errors in native write causes remain visible") {
    resetMocks()
    includeRetry = true
    val cb = buildBatchWithPartitionedCol(1, 2, 3)
    withColumnarBatchesVerifyClosed(Seq(cb)) {
      val nativeWriter = mockOutputWriter.tableWriter
      val fatalError = mock[CudaFatalException]
      val writeError = new RuntimeException("native write failed", fatalError)
      doThrow(writeError).when(nativeWriter).write(any[Table]())
      val spillable = SpillableColumnarBatch(cb, ACTIVE_ON_DECK_PRIORITY)
      val failure = intercept[RuntimeException] {
        mockOutputWriter.writeSpillableAndClose(spillable)
      }
      assert(failure eq writeError)
      assert(ExceptionUtils.getThrowableList(failure).contains(fatalError))
      verify(nativeWriter, times(1)).write(any[Table]())
    }
  }

  test("a wrapped native CPU OOM must fail the task") {
    resetMocks()
    includeRetry = true
    val cb = buildBatchWithPartitionedCol(1, 2, 3)
    withColumnarBatchesVerifyClosed(Seq(cb)) {
      val nativeWriter = mockOutputWriter.tableWriter
      val writeError = new RuntimeException("native write failed",
        new CpuRetryOOM("host allocation failed"))
      doThrow(writeError).when(nativeWriter).write(any[Table]())
      val spillable = SpillableColumnarBatch(cb, ACTIVE_ON_DECK_PRIORITY)
      val failure = intercept[IllegalStateException] {
        mockOutputWriter.writeSpillableAndClose(spillable)
      }
      assert(failure.getSuppressed.exists(_ eq writeError))
      verify(nativeWriter, times(1)).write(any[Table]())
    }
  }

  test("Parquet native writer retries a GPU OOM") {
    resetMocks()
    includeRetry = true
    val cb = buildBatchWithPartitionedCol(1, 2, 3)
    withColumnarBatchesVerifyClosed(Seq(cb)) {
      val nativeWriter = mockOutputWriter.tableWriter
      doThrow(new GpuRetryOOM("native encoding failed"))
        .doNothing()
        .when(nativeWriter).write(any[Table]())
      val spillable = SpillableColumnarBatch(cb, ACTIVE_ON_DECK_PRIORITY)
      mockOutputWriter.writeSpillableAndClose(spillable)
      verify(nativeWriter, times(2)).write(any[Table]())
    }
  }

  test("native GPU OOM without retry opt-in must fail the task") {
    resetMocks()
    includeRetry = true
    val cb = buildBatchWithPartitionedCol(1, 2, 3)
    withColumnarBatchesVerifyClosed(Seq(cb)) {
      mockOutputWriter.nativeGpuOomRetryEnabled = false
      val nativeWriter = mockOutputWriter.tableWriter
      doThrow(new GpuRetryOOM("native encoding failed"))
        .when(nativeWriter).write(any[Table]())
      val spillable = SpillableColumnarBatch(cb, ACTIVE_ON_DECK_PRIORITY)
      val failure = intercept[IllegalStateException] {
        mockOutputWriter.writeSpillableAndClose(spillable)
      }
      assert(failure.getSuppressed.exists(_.isInstanceOf[GpuRetryOOM]))
      verify(nativeWriter, times(1)).write(any[Table]())
    }
  }

  test("GPU OOM after native output must fail the task") {
    Seq[Throwable](new GpuRetryOOM("retry after output"),
      new GpuSplitAndRetryOOM("split after output")).foreach { oom =>
      resetMocks()
      includeRetry = true
      val cb = buildBatchWithPartitionedCol(1, 2, 3)
      withColumnarBatchesVerifyClosed(Seq(cb)) {
        val nativeWriter = mockOutputWriter.tableWriter
        closeOnExcept(HostMemoryBuffer.allocate(4)) { buffer =>
          doAnswer { _ =>
            mockOutputWriter.handleBuffer(buffer, 4)
            throw oom
          }.when(nativeWriter).write(any[Table]())

          val spillable = SpillableColumnarBatch(cb, ACTIVE_ON_DECK_PRIORITY)
          val failure = intercept[IllegalStateException] {
            mockOutputWriter.writeSpillableAndClose(spillable)
          }
          assert(failure.getSuppressed.exists(_ eq oom))
          verify(nativeWriter, times(1)).write(any[Table]())
        }
      }
    }
  }

  test("GPU OOM on a later split preserves earlier split output") {
    resetMocks()
    includeRetry = true
    val cb = buildBatchWithPartitionedCol(1, 2, 3, 4)
    withColumnarBatchesVerifyClosed(Seq(cb)) {
      val nativeWriter = mockOutputWriter.tableWriter
      val payload = Array[Byte](1, 2, 3, 4)
      closeOnExcept(HostMemoryBuffer.allocate(payload.length)) { buffer =>
        buffer.setBytes(0, payload, 0, payload.length)
        var attempts = 0
        doAnswer { _ =>
          attempts += 1
          attempts match {
            case 1 => throw new GpuSplitAndRetryOOM("split the input")
            case 2 => mockOutputWriter.handleBuffer(buffer, payload.length)
            case 3 => throw new GpuRetryOOM("retry the second split")
            case _ => ()
          }
          null
        }.when(nativeWriter).write(any[Table]())

        val spillable = SpillableColumnarBatch(cb, ACTIVE_ON_DECK_PRIORITY)
        mockOutputWriter.writeSpillableAndClose(spillable)

        verify(nativeWriter, times(4)).write(any[Table]())
        val written = ArgumentCaptor.forClass(classOf[Array[Byte]])
        verify(mockOutputWriter.outputStreamMock).write(
          written.capture(), org.mockito.ArgumentMatchers.eq(0),
          org.mockito.ArgumentMatchers.eq(payload.length))
        assert(written.getValue.take(payload.length).sameElements(payload))
        assert(mockOutputWriter.getFileLength == payload.length)
      }
    }
  }

  test("single directory data writer with splits") {
    resetMocksWithAndWithoutRetry {
      val cb = buildBatchWithPartitionedCol(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
      val cb2 = buildBatchWithPartitionedCol(1, 2, 3, 4, 5)
      val cbs = Seq(spy(cb), spy(cb2))
      withColumnarBatchesVerifyClosed(cbs) {
        // setting this to 5 makes the single writer have to split at the 5 row boundary
        when(mockJobDescription.maxRecordsPerFile).thenReturn(5)
        val singleWriter = spy(new GpuSingleDirectoryDataWriter(
          mockJobDescription, mockTaskAttemptContext, mockCommitter, None))

        singleWriter.writeWithIterator(cbs.iterator)
        singleWriter.commit()
        // twice for the first batch given the split, and once for the second batch
        verify(mockOutputWriter, times(3))
          .writeSpillableAndClose(any())
        // three because we wrote 3 files (15 rows, limit was 5 rows per file)
        verify(mockOutputWriter, times(3)).close()
      }
    }
  }

  test("dynamic partition data writer without splits") {
    resetMocksWithAndWithoutRetry {
      // 4 partitions
      val cb = buildBatchWithPartitionedCol(1, 1, 2, 2, 3, 3, 4, 4)
      // 5 partitions
      val cb2 = buildBatchWithPartitionedCol(1, 2, 3, 4, 5)
      val cbs = Seq(spy(cb), spy(cb2))
      withColumnarBatchesVerifyClosed(cbs) {
        // setting this to 3 => the writer won't split as no partition has more than 3 rows
        when(mockJobDescription.maxRecordsPerFile).thenReturn(3)
        val dynamicSingleWriter = prepareDynamicPartitionSingleWriter()
        dynamicSingleWriter.writeWithIterator(cbs.iterator)
        dynamicSingleWriter.commit()
        // we write 9 batches (4 partitions in the first bach, and 5 partitions in the second)
        verify(mockOutputWriter, times(9))
          .writeSpillableAndClose(any())
        verify(dynamicSingleWriter, times(9)).newWriter(any(), any(), any())
        // it uses 9 writers because the single writer mode only keeps one writer open at a time
        // and once a new partition is seen, the old writer is closed and a new one is opened.
        verify(mockOutputWriter, times(9)).close()
      }
    }
  }

  test("dynamic partition data writer bucketing write without splits") {
    Seq(5, -5).foreach { numBuckets =>
      val (numWrites, numNewWriters) = if (numBuckets > 0) { // Bucket only
        (6, 6) // 3 buckets + 3 buckets
      } else { // partition and bucket
        (10, 10) // 5 pairs + 5 pairs
      }
      resetMocksWithAndWithoutRetry {
        val cb = buildBatchWithPartitionedAndBucketCols(
          IndexedSeq(1, 1, 2, 2, 3, 3, 4, 4),
          IndexedSeq(1, 1, 1, 1, 2, 2, 2, 3))
        val cb2 = buildBatchWithPartitionedAndBucketCols(
          IndexedSeq(1, 2, 3, 4, 5),
          IndexedSeq(1, 1, 2, 2, 3))
        val cbs = Seq(spy(cb), spy(cb2))
        withColumnarBatchesVerifyClosed(cbs, numBuckets) {
          // setting to 9 then the writer won't split as no group has more than 9 rows
          when(mockJobDescription.maxRecordsPerFile).thenReturn(9)
          val dynamicSingleWriter = prepareDynamicPartitionSingleWriter()
          dynamicSingleWriter.writeWithIterator(cbs.iterator)
          dynamicSingleWriter.commit()
          verify(mockOutputWriter, times(numWrites)).writeSpillableAndClose(any())
          verify(dynamicSingleWriter, times(numNewWriters)).newWriter(any(), any(), any())
          verify(mockOutputWriter, times(numNewWriters)).close()
        }
      }
    }
  }

  test("dynamic partition data writer with splits") {
    resetMocksWithAndWithoutRetry {
      val cb = buildBatchWithPartitionedCol(1, 1, 2, 2, 3, 3, 4, 4)
      val cb2 = buildBatchWithPartitionedCol(1, 2, 3, 4, 5)
      val cbs = Seq(spy(cb), spy(cb2))
      withColumnarBatchesVerifyClosed(cbs) {
        // force 1 row batches to be written
        when(mockJobDescription.maxRecordsPerFile).thenReturn(1)
        val dynamicSingleWriter = prepareDynamicPartitionSingleWriter()
        dynamicSingleWriter.writeWithIterator(cbs.iterator)
        dynamicSingleWriter.commit()
        // we get 13 calls because we write 13 individual batches after splitting
        verify(mockOutputWriter, times(13))
            .writeSpillableAndClose(any())
        verify(dynamicSingleWriter, times(13)).newWriter(any(), any(), any())
        // since we have a limit of 1 record per file, we write 13 files
        verify(mockOutputWriter, times(13))
            .close()
      }
    }
  }

  test("dynamic partition concurrent data writer with splits") {
    resetMocksWithAndWithoutRetry {
      // 4 partitions
      val cb = buildBatchWithPartitionedCol(1, 1, 2, 2, 3, 3, 4, 4)
      // 5 partitions
      val cb2 = buildBatchWithPartitionedCol(1, 2, 3, 4, 5)
      val cbs = Seq(spy(cb), spy(cb2))
      withColumnarBatchesVerifyClosed(cbs) {
        when(mockJobDescription.maxRecordsPerFile).thenReturn(3)
        val dynamicConcurrentWriter =
          prepareDynamicPartitionConcurrentWriter(maxWriters = 9, batchSize = 1)
        dynamicConcurrentWriter.writeWithIterator(cbs.iterator)
        dynamicConcurrentWriter.commit()
        // we get 9 calls because we have 9 partitions total
        verify(mockOutputWriter, times(9))
            .writeSpillableAndClose(any())
        // we write 5 files because we write 1 file per partition, since this concurrent
        // writer was able to keep the writers alive
        verify(dynamicConcurrentWriter, times(5)).newWriter(any(), any(), any())
        verify(mockOutputWriter, times(5)).close()
      }
    }
  }

  test("dynamic partition concurrent data writer bucketing write without splits") {
    Seq(5, -5).foreach { numBuckets =>
      val (numWrites, numNewWriters) = if (numBuckets > 0) { // Bucket only
        (3, 3) // 3 distinct buckets in total
      } else { // partition and bucket
        (6, 6) // 6 distinct pairs in total
      }
      resetMocksWithAndWithoutRetry {
        val cb = buildBatchWithPartitionedAndBucketCols(
          IndexedSeq(1, 1, 2, 2, 3, 3, 4, 4),
          IndexedSeq(1, 1, 1, 1, 2, 2, 2, 3))
        val cb2 = buildBatchWithPartitionedAndBucketCols(
          IndexedSeq(1, 2, 3, 4, 5),
          IndexedSeq(1, 1, 2, 2, 3))
        val cbs = Seq(spy(cb), spy(cb2))
        withColumnarBatchesVerifyClosed(cbs, numBuckets) {
          // setting to 9 then the writer won't split as no group has more than 9 rows
          when(mockJobDescription.maxRecordsPerFile).thenReturn(9)
          // I would like to not flush on the first iteration of the `write` method
          when(mockJobDescription.concurrentWriterPartitionFlushSize).thenReturn(1000)
          val dynamicConcurrentWriter =
            prepareDynamicPartitionConcurrentWriter(maxWriters = 20, batchSize = 100)
          dynamicConcurrentWriter.writeWithIterator(cbs.iterator)
          dynamicConcurrentWriter.commit()
          verify(mockOutputWriter, times(numWrites)).writeSpillableAndClose(any())
          verify(dynamicConcurrentWriter, times(numNewWriters)).newWriter(any(), any(), any())
          verify(mockOutputWriter, times(numNewWriters)).close()
        }
      }
    }
  }

  test("dynamic partition concurrent data writer with splits and flush") {
    resetMocksWithAndWithoutRetry {
      val cb = buildBatchWithPartitionedCol(1, 1, 2, 2, 3, 3, 4, 4)
      val cb2 = buildBatchWithPartitionedCol(1, 2, 3, 4, 5)
      val cbs = Seq(spy(cb), spy(cb2))
      withColumnarBatchesVerifyClosed(cbs) {
        // I would like to not flush on the first iteration of the `write` method
        when(mockJobDescription.concurrentWriterPartitionFlushSize).thenReturn(1000)
        when(mockJobDescription.maxRecordsPerFile).thenReturn(1)
        val dynamicConcurrentWriter =
          prepareDynamicPartitionConcurrentWriter(maxWriters = 9, batchSize = 1)
        dynamicConcurrentWriter.writeWithIterator(cbs.iterator)
        dynamicConcurrentWriter.commit()

        // we get 13 calls here because we write 1 row files
        verify(mockOutputWriter, times(13))
            .writeSpillableAndClose(any())
        verify(dynamicConcurrentWriter, times(13)).newWriter(any(), any(), any())

        // we have to open 13 writers (1 per row) given the record limit of 1
        verify(mockOutputWriter, times(13)).close()
      }
    }
  }

  test("dynamic partition concurrent data writer fallback without splits") {
    resetMocksWithAndWithoutRetry {
      val cb = buildBatchWithPartitionedCol(1, 1, 2, 2, 3, 3, 4, 4)
      val cb2 = buildBatchWithPartitionedCol(1, 2, 3, 4, 5)
      val cbs = Seq(spy(cb), spy(cb2))
      withColumnarBatchesVerifyClosed(cbs) {
        // no splitting will occur because the partitions have 3 or less rows.
        when(mockJobDescription.maxRecordsPerFile).thenReturn(3)
        // because `maxWriters==1` we will fallback right away and
        // behave like the dynamic single writer
        val dynamicConcurrentWriter =
          prepareDynamicPartitionConcurrentWriter(maxWriters = 1, batchSize = 1)
        dynamicConcurrentWriter.writeWithIterator(cbs.iterator)
        dynamicConcurrentWriter.commit()
        // 6 batches written, one per partition (no splitting) plus one written by
        // the concurrent writer.
        verify(mockOutputWriter, times(6))
            .writeSpillableAndClose(any())
        verify(dynamicConcurrentWriter, times(5)).newWriter(any(), any(), any())
        // 5 files written because this is the single writer mode
        verify(mockOutputWriter, times(5)).close()
      }
    }
  }

  test("dynamic partition concurrent data writer fallback after cache flush") {
    resetMocksWithAndWithoutRetry {
      val cb = buildBatchWithPartitionedCol(1, 2)
      val cb2 = buildBatchWithPartitionedCol(1, 2, 3)
      val cbs = Seq(spy(cb), spy(cb2))
      withColumnarBatchesVerifyClosed(cbs) {
        // Force the first batch to flush the two open writer caches. The second batch then hits
        // the writer limit and falls back, so previously flushed empty caches must be skipped.
        when(mockJobDescription.concurrentWriterPartitionFlushSize).thenReturn(1)
        when(mockJobDescription.maxRecordsPerFile).thenReturn(0)
        val dynamicConcurrentWriter =
          prepareDynamicPartitionConcurrentWriter(maxWriters = 2, batchSize = 1)
        dynamicConcurrentWriter.writeWithIterator(cbs.iterator)
        dynamicConcurrentWriter.commit()

        verify(mockOutputWriter, times(5))
            .writeSpillableAndClose(any())
      }
    }
  }

  test("dynamic partition concurrent data writer fallback with splits") {
    resetMocksWithAndWithoutRetry {
      val cb = buildBatchWithPartitionedCol(1, 1, 1, 2, 2, 3, 3, 4, 4)
      val cb2 = buildBatchWithPartitionedCol(1, 2, 3, 4)
      val cb3 = buildBatchWithPartitionedCol(1, 2, 3, 4, 5) // fallback here (5 writers)
      val cbs = Seq(spy(cb), spy(cb2), spy(cb3))
      withColumnarBatchesVerifyClosed(cbs) {
        // I would like to not flush on the first iteration of the `write` method
        when(mockJobDescription.concurrentWriterPartitionFlushSize).thenReturn(1000)
        when(mockJobDescription.maxRecordsPerFile).thenReturn(1)
        val dynamicConcurrentWriter =
          prepareDynamicPartitionConcurrentWriter(maxWriters = 5, batchSize = 1)
        dynamicConcurrentWriter.writeWithIterator(cbs.iterator)
        dynamicConcurrentWriter.commit()
        // 18 batches are written, once per row above given maxRecorsPerFile
        verify(mockOutputWriter, times(18))
            .writeSpillableAndClose(any())
        verify(dynamicConcurrentWriter, times(18)).newWriter(any(), any(), any())
        // dynamic partitioning code calls close several times on the same ColumnarOutputWriter
        // that doesn't seem to be an issue right now, but verifying that the writer was closed
        // is not as clean here, especially during a fallback like in this test.
        // A follow on issue is filed to handle this better:
        // https://github.com/NVIDIA/spark-rapids/issues/8736
        // verify(mockOutputWriter, times(18)).close()
      }
    }
  }

  test("call newBatch only once when there is a failure writing") {
    // this test is to exercise the contract that the ColumnarWriteTaskStatsTracker.newBatch
    // has. When there is a retry within writeSpillableAndClose, we will guarantee that
    // newBatch will be called only once. If there are exceptions within newBatch they are fatal,
    // and are not retried.
    resetMocksWithAndWithoutRetry {
      val cb = buildBatchWithPartitionedCol(1, 1, 1, 1, 1, 1, 1, 1, 1)
      val cbs = Seq(spy(cb))

      // Mock jobDescription before it is passed to the mockOutputWriter in
      // withColumnarBatchesVerifyClosed().

      // I would like to not flush on the first iteration of the `write` method
      when(mockJobDescription.concurrentWriterPartitionFlushSize).thenReturn(1000)
      when(mockJobDescription.maxRecordsPerFile).thenReturn(9)

      val statsTracker = mock[ColumnarWriteTaskStatsTracker]
      val jobTracker = new ColumnarWriteJobStatsTracker {
        override def newTaskInstance(): ColumnarWriteTaskStatsTracker = {
          statsTracker
        }
        override def processStats(stats: Seq[WriteTaskStats], jobCommitTime: Long): Unit = {}
      }
      when(mockJobDescription.statsTrackers)
        .thenReturn(Seq(jobTracker))

      withColumnarBatchesVerifyClosed(cbs) {
                // throw once from bufferBatchAndClose to simulate an exception after we call the
        // stats tracker
        mockOutputWriter.throwOnNextBufferBatchAndClose(
          new GpuSplitAndRetryOOM("mocking a split and retry"))
        val dynamicConcurrentWriter =
          prepareDynamicPartitionConcurrentWriter(maxWriters = 5, batchSize = 1)

        if (includeRetry) {
          dynamicConcurrentWriter.writeWithIterator(cbs.iterator)
          dynamicConcurrentWriter.commit()
        } else {
          assertThrows[GpuSplitAndRetryOOM] {
            dynamicConcurrentWriter.writeWithIterator(cbs.iterator)
            dynamicConcurrentWriter.commit()
          }
        }

        // 1 batch is written, all rows fit
        verify(mockOutputWriter, times(1))
            .writeSpillableAndClose(any())
        // we call newBatch once
        verify(statsTracker, times(1)).newBatch(any(), any())
        if (includeRetry) {
          // we call it 3 times, once for the first whole batch that fails with OOM
          // and twice for the two halves after we handle the OOM
          verify(mockOutputWriter, times(3)).bufferBatchAndClose(any())
        } else {
          // once and we fail, so we don't retry
          verify(mockOutputWriter, times(1)).bufferBatchAndClose(any())
        }
      }
    }
  }

  test("newBatch throwing is fatal") {
    // this test is to exercise the contract that the ColumnarWriteTaskStatsTracker.newBatch
    // has. When there is a retry within writeSpillableAndClose, we will guarantee that
    // newBatch will be called only once. If there are exceptions within newBatch they are fatal,
    // and are not retried.
    resetMocksWithAndWithoutRetry {
      val cb = buildBatchWithPartitionedCol(1, 1, 1, 1, 1, 1, 1, 1, 1)
      val cbs = Seq(spy(cb))

      // Mock jobDescription before mockOutputWriter is initialized in
      // withColumnarBatchesVerifyClosed().

      // I would like to not flush on the first iteration of the `write` method
      when(mockJobDescription.concurrentWriterPartitionFlushSize).thenReturn(1000)
      when(mockJobDescription.maxRecordsPerFile).thenReturn(9)

      val statsTracker = mock[ColumnarWriteTaskStatsTracker]
      val jobTracker = new ColumnarWriteJobStatsTracker {
        override def newTaskInstance(): ColumnarWriteTaskStatsTracker = {
          statsTracker
        }

        override def processStats(stats: Seq[WriteTaskStats], jobCommitTime: Long): Unit = {}
      }
      when(mockJobDescription.statsTrackers)
        .thenReturn(Seq(jobTracker))
      when(statsTracker.newBatch(any(), any()))
        .thenThrow(new GpuRetryOOM("mocking a retry"))

      withColumnarBatchesVerifyClosed(cbs) {
        val dynamicConcurrentWriter =
          prepareDynamicPartitionConcurrentWriter(maxWriters = 5, batchSize = 1)

        assertThrows[GpuRetryOOM] {
          dynamicConcurrentWriter.writeWithIterator(cbs.iterator)
          dynamicConcurrentWriter.commit()
        }

        // we never reach the buffer stage
        verify(mockOutputWriter, times(0)).bufferBatchAndClose(any())
        // we attempt to write one batch
        verify(mockOutputWriter, times(1))
            .writeSpillableAndClose(any())
        // we call newBatch once
        verify(statsTracker, times(1)).newBatch(any(), any())
      }
    }
  }

  test("close closes the output stream when writing the buffered data fails") {
    val writeFailure = new IOException("write failed")
    val stream = new RecordingOutputStream(writeFailure = writeFailure)
    val writer = streamOutputWriter(stream)
    writer.bufferBytes(16)
    val thrown = intercept[IOException](writer.close())
    assert(thrown eq writeFailure)
    assert(stream.closeCount == 1)
  }

  test("close keeps the write failure when closing the output stream also fails") {
    val writeFailure = new IOException("write failed")
    val closeFailure = new IOException("close failed")
    val stream = new RecordingOutputStream(writeFailure, closeFailure)
    val writer = streamOutputWriter(stream)
    writer.bufferBytes(16)
    val thrown = intercept[IOException](writer.close())
    assert(thrown eq writeFailure)
    assert(thrown.getSuppressed.toSeq == Seq(closeFailure))
    assert(stream.closeCount == 1)
  }

  test("an interrupt set before close stops the buffered write, not the async close") {
    val delegate = new RecordingOutputStream()
    val (stream, pool) = asyncOutputStream(delegate)
    val writer = streamOutputWriter(stream)
    writer.bufferBytes(16)
    val (thrown, interrupted) = runOnNewThread(interruptFirst = true)(writer.close())
    assert(thrown.exists(_.isInstanceOf[InterruptedException]))
    // The wait that threw consumed the interrupt, and the exception carries it to the caller.
    assert(!interrupted)
    assert(delegate.bytesWritten == 0)
    assertClosedOnWriterThread(delegate, pool)
  }

  test("an interrupt set before close does not stop the async close with nothing to write") {
    val delegate = new RecordingOutputStream()
    val (stream, pool) = asyncOutputStream(delegate)
    val writer = streamOutputWriter(stream)
    val (thrown, interrupted) = runOnNewThread(interruptFirst = true)(writer.close())
    assert(thrown.isEmpty)
    assert(interrupted)
    assertClosedOnWriterThread(delegate, pool)
  }

  test("close throws the output stream's close failure after a clean write") {
    // The same type self-suppression throws, which the writer must not hide either.
    val closeFailure = new IllegalArgumentException("close failed")
    val stream = new RecordingOutputStream(closeFailure = closeFailure)
    val writer = streamOutputWriter(stream)
    writer.bufferBytes(16)
    val thrown = intercept[IllegalArgumentException](writer.close())
    assert(thrown eq closeFailure)
    assert(stream.bytesWritten == 16)
    assert(stream.closeCount == 1)
  }

  test("close suppresses an interrupted close behind a write failure and restores it") {
    val writeFailure = new IOException("write failed")
    val interruptedClose = new InterruptedException("interrupted close")
    val stream = new RecordingOutputStream(writeFailure, interruptedClose)
    val writer = streamOutputWriter(stream)
    writer.bufferBytes(16)
    val (thrown, interrupted) = runOnNewThread(interruptFirst = false)(writer.close())
    assert(thrown.exists(_ eq writeFailure))
    assert(writeFailure.getSuppressed.toSeq == Seq(interruptedClose))
    assert(interrupted)
    assert(stream.closeCount == 1)
  }

  test("an interrupt from the output stream's close waits until the debug stream closes") {
    val writeFailure = new IOException("write failed")
    val interruptedClose = new InterruptedException("interrupted close")
    val stream = new RecordingOutputStream(writeFailure, interruptedClose)
    val debugStream = new RecordingOutputStream()
    val writer = streamOutputWriter(stream, debugStream)
    // The writer opens its debug stream at the first batch it encodes, which needs a task.
    TrampolineUtil.setTaskContext(new MockTaskContext(taskAttemptId = 1, partitionId = 0))
    try {
      writer.encode(buildBatchWithPartitionedCol(1))
    } finally {
      TrampolineUtil.unsetTaskContext()
    }
    assert(debugStream.bytesWritten > 0)
    writer.bufferBytes(16)
    val (thrown, interrupted) = runOnNewThread(interruptFirst = false)(writer.close())
    assert(thrown.exists(_ eq writeFailure))
    assert(writeFailure.getSuppressed.toSeq == Seq(interruptedClose))
    assert(interrupted)
    assert(stream.closeCount == 1)
    assert(debugStream.closeCount == 1)
    assert(!debugStream.interruptedAtClose)
  }

  test("close keeps the write failure when the output stream rethrows it from close") {
    val writeFailure = new IOException("write failed")
    val stream = new RecordingOutputStream(writeFailure, writeFailure)
    val writer = streamOutputWriter(stream)
    writer.bufferBytes(16)
    val (thrown, interrupted) = runOnNewThread(interruptFirst = false)(writer.close())
    assert(thrown.exists(_ eq writeFailure))
    assert(writeFailure.getSuppressed.isEmpty)
    assert(!interrupted)
    assert(stream.closeCount == 1)
  }

  Seq(false, true).foreach { closeFails =>
    test(s"close keeps a cached async write failure, closeFails=$closeFails") {
      val writeFailure = new IOException("write failed")
      val closeFailure = new IOException("close failed")
      val delegate =
        new RecordingOutputStream(writeFailure, if (closeFails) closeFailure else null)
      val (stream, pool) = asyncOutputStream(delegate)
      val writer = streamOutputWriter(stream)
      // The async stream caches a failed write, then rethrows that object from later writes,
      // from flush and from close.
      stream.write(1)
      assert(intercept[IOException](stream.flush()) eq writeFailure)
      writer.bufferBytes(16)
      val (thrown, interrupted) = runOnNewThread(interruptFirst = false)(writer.close())
      assert(thrown.exists(_ eq writeFailure))
      assert(writeFailure.getSuppressed.toSeq ==
        (if (closeFails) Seq(closeFailure) else Seq.empty))
      assert(!interrupted)
      // The cached failure refused the writer's own buffered write too.
      assert(delegate.bytesWritten == 0)
      assertClosedOnWriterThread(delegate, pool)
    }
  }

  test("closeAndReturn keeps a cached async write failure and closes the footer once") {
    val writeFailure = new IOException("write failed")
    val delegate = new RecordingOutputStream(writeFailure)
    val (stream, pool) = asyncOutputStream(delegate)
    val writer = streamOutputWriter(stream)
    stream.write(1)
    assert(intercept[IOException](stream.flush()) eq writeFailure)
    writer.bufferBytes(16)
    var footerCloses = 0
    val footer: AutoCloseable = () => footerCloses += 1
    val (thrown, interrupted) = runOnNewThread(interruptFirst = false) {
      writer.closeReturning(footer)
      ()
    }
    assert(thrown.exists(_ eq writeFailure))
    assert(writeFailure.getSuppressed.isEmpty)
    assert(!interrupted)
    assert(footerCloses == 1)
    assertClosedOnWriterThread(delegate, pool)
  }

  test("close keeps a cached async write failure that the delegate rethrows from close") {
    val writeFailure = new IOException("write failed")
    val delegate = new RecordingOutputStream(writeFailure, writeFailure)
    val (stream, pool) = asyncOutputStream(delegate)
    val writer = streamOutputWriter(stream)
    stream.write(1)
    assert(intercept[IOException](stream.flush()) eq writeFailure)
    writer.bufferBytes(16)
    val (thrown, interrupted) = runOnNewThread(interruptFirst = false)(writer.close())
    assert(thrown.exists(_ eq writeFailure))
    assert(writeFailure.getSuppressed.isEmpty)
    assert(!interrupted)
    assertClosedOnWriterThread(delegate, pool)
  }

  test("a writer closed after a nested safeClose restored an interrupt closes its output") {
    val delegate = new RecordingOutputStream()
    val (stream, pool) = asyncOutputStream(delegate)
    val writer = streamOutputWriter(stream)
    writer.bufferBytes(16)
    val earlierFailure = new IOException("earlier close failed")
    val resources = Seq[AutoCloseable](
      // Its own safeClose suppresses the interrupt and so restores it before the writer closes.
      () => Seq[AutoCloseable](
        () => throw earlierFailure,
        () => throw new InterruptedException("interrupted close")).safeClose(),
      () => writer.close())
    val (thrown, interrupted) = runOnNewThread(interruptFirst = false)(resources.safeClose())
    assert(thrown.contains(earlierFailure))
    // One from the nested safeClose, one from the writer's buffered write that saw it restored.
    assert(earlierFailure.getSuppressed.count(_.isInstanceOf[InterruptedException]) == 2)
    assert(interrupted)
    assert(delegate.bytesWritten == 0)
    assertClosedOnWriterThread(delegate, pool)
  }
}

/** A file system whose files are all the stream a test set, to see how a writer closes it. */
class DebugStreamFileSystem extends FileSystem {
  private def unsupported: Nothing = throw new UnsupportedOperationException()

  override def getUri: URI = URI.create("probe:///")

  override def create(
      f: Path,
      permission: FsPermission,
      overwrite: Boolean,
      bufferSize: Int,
      replication: Short,
      blockSize: Long,
      progress: Progressable): FSDataOutputStream =
    new FSDataOutputStream(DebugStreamFileSystem.nextStream, null)

  override def open(f: Path, bufferSize: Int): FSDataInputStream = unsupported

  override def append(f: Path, bufferSize: Int, progress: Progressable): FSDataOutputStream =
    unsupported

  override def rename(src: Path, dst: Path): Boolean = unsupported

  override def delete(f: Path, recursive: Boolean): Boolean = unsupported

  override def listStatus(f: Path): Array[FileStatus] = unsupported

  override def setWorkingDirectory(dir: Path): Unit = unsupported

  override def getWorkingDirectory: Path = new Path("probe:///")

  override def mkdirs(f: Path, permission: FsPermission): Boolean = unsupported

  override def getFileStatus(f: Path): FileStatus = unsupported
}

object DebugStreamFileSystem {
  /** The stream that every file this file system creates writes to. */
  @volatile var nextStream: OutputStream = _
}
