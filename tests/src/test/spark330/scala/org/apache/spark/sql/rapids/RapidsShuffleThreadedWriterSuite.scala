/*
 * Copyright (c) 2022-2026, NVIDIA CORPORATION.
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
{"spark": "330"}
{"spark": "331"}
{"spark": "332"}
{"spark": "333"}
{"spark": "334"}
{"spark": "340"}
{"spark": "341"}
{"spark": "342"}
{"spark": "343"}
{"spark": "344"}
{"spark": "350"}
{"spark": "350db143"}
{"spark": "351"}
{"spark": "352"}
{"spark": "353"}
{"spark": "354"}
{"spark": "355"}
{"spark": "356"}
{"spark": "357"}
{"spark": "358"}
{"spark": "400"}
{"spark": "401"}
{"spark": "402"}
{"spark": "403"}
{"spark": "411"}
spark-rapids-shim-json-lines ***/
package org.apache.spark.sql.rapids

import java.io._
import java.nio.ByteBuffer
import java.util.UUID

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag
import scala.util.control.NonFatal

import ai.rapids.cudf.HostMemoryBuffer
import com.nvidia.spark.rapids.SlicedSerializedColumnVector
import org.mockito.{Mock, MockitoAnnotations}
import org.mockito.Answers.RETURNS_SMART_NULLS
import org.mockito.ArgumentMatchers.{any, anyInt, anyLong}
import org.mockito.Mockito._
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.mockito.MockitoSugar

import org.apache.spark.{HashPartitioner, SparkConf, TaskContext}
import org.apache.spark.executor.{ShuffleWriteMetrics, TaskMetrics}
import org.apache.spark.internal.Logging
import org.apache.spark.serializer._
import org.apache.spark.shuffle.IndexShuffleBlockResolver
import org.apache.spark.shuffle.api.ShuffleExecutorComponents
import org.apache.spark.shuffle.sort.io.RapidsLocalDiskShuffleExecutorComponents
import org.apache.spark.sql.rapids.shims.RapidsShuffleThreadedWriter
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.storage._
import org.apache.spark.util.Utils


/**
 * A simple serializer for testing ColumnarBatch with SlicedSerializedColumnVector.
 */
class TestColumnarBatchSerializer extends Serializer with Serializable {
  override def newInstance(): SerializerInstance = new TestColumnarBatchSerializerInstance()
  override def supportsRelocationOfSerializedObjects: Boolean = true
}

class TestColumnarBatchSerializerInstance extends SerializerInstance {
  override def serialize[T: ClassTag](t: T): ByteBuffer = {
    val bos = new ByteArrayOutputStream()
    val stream = serializeStream(bos)
    stream.writeObject(t)
    stream.close()
    ByteBuffer.wrap(bos.toByteArray)
  }

  override def deserialize[T: ClassTag](bytes: ByteBuffer): T =
    throw new UnsupportedOperationException("Not implemented for test")

  override def deserialize[T: ClassTag](bytes: ByteBuffer, loader: ClassLoader): T =
    throw new UnsupportedOperationException("Not implemented for test")

  override def serializeStream(s: OutputStream): SerializationStream =
    new TestColumnarBatchSerializationStream(s)

  override def deserializeStream(s: InputStream): DeserializationStream =
    throw new UnsupportedOperationException("Not implemented for test")
}

class TestColumnarBatchSerializationStream(out: OutputStream) extends SerializationStream {
  private val dataOut = new DataOutputStream(out)

  override def writeObject[T: ClassTag](t: T): SerializationStream = {
    t match {
      case batch: ColumnarBatch =>
        dataOut.writeInt(batch.numCols())
        for (i <- 0 until batch.numCols()) {
          batch.column(i) match {
            case col: SlicedSerializedColumnVector =>
              val hmb = col.getWrap
              val size = hmb.getLength.toInt
              dataOut.writeInt(size)
              val bytes = new Array[Byte](size)
              hmb.getBytes(bytes, 0, 0, size)
              dataOut.write(bytes)
            case _ =>
              dataOut.writeInt(0)
          }
        }
      case key: Int =>
        dataOut.writeInt(key)
      case _ =>
        dataOut.writeInt(-1)
    }
    this
  }

  override def writeKey[T: ClassTag](key: T): SerializationStream = writeObject(key)
  override def writeValue[T: ClassTag](value: T): SerializationStream = writeObject(value)
  override def flush(): Unit = dataOut.flush()
  override def close(): Unit = dataOut.close()
}


// Shim for Spark 3.3.0+ createTempFile method
trait ShimIndexShuffleBlockResolver330 {
  def createTempFile(file: File): File
}

class TestIndexShuffleBlockResolver(conf: SparkConf, bm: BlockManager)
    extends IndexShuffleBlockResolver(conf, bm) with ShimIndexShuffleBlockResolver330 {
  override def createTempFile(file: File): File = null
}


class RapidsShuffleThreadedWriterSuite extends AnyFunSuite
    with BeforeAndAfterEach
    with BeforeAndAfterAll
    with MockitoSugar
    with Logging {

  @scala.annotation.nowarn("msg=consider using immutable val")
  @Mock(answer = RETURNS_SMART_NULLS) private var blockManager: BlockManager = _

  @scala.annotation.nowarn("msg=consider using immutable val")
  @Mock(answer = RETURNS_SMART_NULLS) private var diskBlockManager: DiskBlockManager = _

  @scala.annotation.nowarn("msg=consider using immutable val")
  @Mock(answer = RETURNS_SMART_NULLS) private var taskContext: TaskContext = _

  @scala.annotation.nowarn("msg=consider using immutable val")
  @Mock(answer = RETURNS_SMART_NULLS) private var blockResolver: TestIndexShuffleBlockResolver = _

  @scala.annotation.nowarn("msg=consider using immutable val")
  @Mock(answer = RETURNS_SMART_NULLS)
  private var dependency: GpuShuffleDependency[Int, ColumnarBatch, ColumnarBatch] = _

  private var taskMetrics: TaskMetrics = _
  private var tempDir: File = _
  private var outputFile: File = _
  private var shuffleExecutorComponents: ShuffleExecutorComponents = _
  private val conf: SparkConf = new SparkConf(loadDefaults = false)
    .set("spark.app.id", "sampleApp")
  private val temporaryFilesCreated: mutable.Buffer[File] = new ArrayBuffer[File]()
  private val blockIdToFileMap: mutable.Map[BlockId, File] = new mutable.HashMap[BlockId, File]
  private var shuffleHandle: ShuffleHandleWithMetrics[Int, ColumnarBatch, ColumnarBatch] = _

  // Track the sliced buffers (wrap) for cleanup, since incRefCountAndGetSize increases refCount
  private val slicedBuffersToClean: mutable.Buffer[HostMemoryBuffer] =
    new ArrayBuffer[HostMemoryBuffer]()

  private val numWriterThreads = 2

  private def createTestBatch(value: Int): ColumnarBatch = {
    val bufferSize = 64 + (value % 64)
    val hmb = HostMemoryBuffer.allocate(bufferSize)
    for (i <- 0 until bufferSize) {
      hmb.setByte(i, (value + i).toByte)
    }
    val cv = new SlicedSerializedColumnVector(hmb, 0, bufferSize)
    // Save the sliced buffer (wrap) for cleanup, NOT the original hmb
    // incRefCountAndGetSize will increase wrap's refCount, we need to close it once more
    slicedBuffersToClean += cv.getWrap
    // Close original hmb since SlicedSerializedColumnVector.slice() increased its refCount
    hmb.close()
    new ColumnarBatch(Array(cv), 1)
  }

  private def createTestRecords(keys: Iterator[Int]): Iterator[(Int, ColumnarBatch)] =
    keys.map(key => (key, createTestBatch(key)))

  private def createWriter(): RapidsShuffleThreadedWriter[Int, ColumnarBatch] = {
    new RapidsShuffleThreadedWriter[Int, ColumnarBatch](
      blockManager, shuffleHandle, 0L, conf,
      new ThreadSafeShuffleWriteMetricsReporter(taskContext.taskMetrics().shuffleWriteMetrics),
      1024 * 1024, shuffleExecutorComponents, numWriterThreads)
  }

  /**
   * Verify write results including partition data presence.
   * @param partitionsWithData Set of partition IDs that should have data
   * @param minWritesPerPartition Optional map specifying minimum write count per partition.
   *                              Used to verify multiple batches wrote to same partition.
   */
  private def verifyWrite(
      writer: RapidsShuffleThreadedWriter[Int, ColumnarBatch],
      expectedRecords: Int,
      partitionsWithData: Set[Int],
      minWritesPerPartition: Map[Int, Int] = Map.empty): Unit = {
    val partitionLengths = writer.getPartitionLengths
    val numPartitions = partitionLengths.length

    // Basic checks
    assert(partitionLengths.sum === outputFile.length(),
      s"Partition lengths sum ${partitionLengths.sum} != file length ${outputFile.length()}")
    assert(writer.getBytesInFlight == 0, "bytesInFlight should be 0 after completion")
    assert(taskContext.taskMetrics().shuffleWriteMetrics.recordsWritten === expectedRecords,
      s"Expected $expectedRecords records, got " +
        s"${taskContext.taskMetrics().shuffleWriteMetrics.recordsWritten}")

    // Verify each partition that should have data actually has data
    for (partitionId <- partitionsWithData) {
      assert(partitionLengths(partitionId) > 0,
        s"Partition $partitionId should have data but length is ${partitionLengths(partitionId)}")
    }

    // Verify partitions NOT in the set are empty
    for (partitionId <- 0 until numPartitions if !partitionsWithData.contains(partitionId)) {
      assert(partitionLengths(partitionId) == 0,
        s"Partition $partitionId should be empty but length is ${partitionLengths(partitionId)}")
    }

    // Verify multiple writes to same partition by checking data length
    // Each write to partition P adds at least minBytesPerWrite bytes
    // (key int + column count int + buffer size int + buffer data)
    val minBytesPerWrite = 4 + 4 + 4 + 64 // at least 76 bytes per record
    for ((partitionId, minWrites) <- minWritesPerPartition) {
      val expectedMinLength = minWrites * minBytesPerWrite
      assert(partitionLengths(partitionId) >= expectedMinLength,
        s"Partition $partitionId: expected at least $minWrites writes " +
          s"(>= $expectedMinLength bytes), but got ${partitionLengths(partitionId)} bytes. " +
          s"This suggests fewer records were written than expected.")
    }
  }

  override def beforeAll(): Unit = {
    RapidsShuffleInternalManagerBase.startThreadPoolIfNeeded(numWriterThreads, 0)
  }

  override def afterAll(): Unit = {
    RapidsShuffleInternalManagerBase.stopThreadPool()
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    RapidsShuffleInternalManagerBase.startThreadPoolIfNeeded(numWriterThreads, 0)
    TaskContext.setTaskContext(taskContext)
    MockitoAnnotations.openMocks(this).close()
    tempDir = Utils.createTempDir()
    outputFile = File.createTempFile("shuffle", null, tempDir)
    taskMetrics = spy(new TaskMetrics)
    val shuffleWriteMetrics = new ShuffleWriteMetrics
    shuffleHandle = new ShuffleHandleWithMetrics[Int, ColumnarBatch, ColumnarBatch](
      0, Map.empty, dependency)
    when(dependency.partitioner).thenReturn(new HashPartitioner(7))
    when(dependency.serializer).thenReturn(new TestColumnarBatchSerializer())
    when(taskMetrics.shuffleWriteMetrics).thenReturn(shuffleWriteMetrics)
    when(taskContext.taskMetrics()).thenReturn(taskMetrics)
    when(blockResolver.getDataFile(0, 0)).thenReturn(outputFile)
    when(blockManager.diskBlockManager).thenReturn(diskBlockManager)
    when(blockManager.serializerManager)
      .thenReturn(new SerializerManager(new TestColumnarBatchSerializer(), conf))

    when(blockResolver.writeMetadataFileAndCommit(
      anyInt, anyLong, any(classOf[Array[Long]]), any(classOf[Array[Long]]), any(classOf[File])))
      .thenAnswer { invocationOnMock =>
        val tmp = invocationOnMock.getArguments()(4).asInstanceOf[File]
        if (tmp != null) {
          outputFile.delete
          tmp.renameTo(outputFile)
        }
        null
      }

    when(blockManager.getDiskWriter(
      any[BlockId], any[File], any[SerializerInstance], anyInt(), any[ShuffleWriteMetrics]))
      .thenAnswer { invocation =>
        val args = invocation.getArguments
        val manager = new SerializerManager(new TestColumnarBatchSerializer(), conf)
        new DiskBlockObjectWriter(
          args(1).asInstanceOf[File], manager, args(2).asInstanceOf[SerializerInstance],
          args(3).asInstanceOf[Int], syncWrites = false,
          args(4).asInstanceOf[ShuffleWriteMetrics], blockId = args(0).asInstanceOf[BlockId])
      }

    when(diskBlockManager.createTempShuffleBlock())
      .thenAnswer { _ =>
        val blockId = new TempShuffleBlockId(UUID.randomUUID)
        val file = new File(tempDir, blockId.name)
        blockIdToFileMap.put(blockId, file)
        temporaryFilesCreated += file
        (blockId, file)
      }

    shuffleExecutorComponents =
      new RapidsLocalDiskShuffleExecutorComponents(conf, blockManager, blockResolver)
  }

  override def afterEach(): Unit = {
    TaskContext.unset()
    blockIdToFileMap.clear()
    temporaryFilesCreated.clear()
    // Close sliced buffers to release the refCount added by incRefCountAndGetSize
    slicedBuffersToClean.foreach { buf =>
      try { buf.close() } catch { case NonFatal(_) => }
    }
    slicedBuffersToClean.clear()
    RapidsShuffleInternalManagerBase.stopThreadPool()
    try { Utils.deleteRecursively(tempDir) } catch { case NonFatal(_) => }
  }

  // ==================== Basic Tests ====================

  test("write empty iterator") {
    val writer = createWriter()
    writer.write(Iterator.empty)
    writer.stop(true)
    assert(writer.getPartitionLengths.sum === 0)
    assert(outputFile.length() === 0)
  }

  test("single batch: sequential partitions") {
    // Single batch with strictly increasing partition IDs
    // Input: 0,1,2,3,4,5,6 -> all in one batch
    val writer = createWriter()
    writer.write(createTestRecords(Iterator(0, 1, 2, 3, 4, 5, 6)))
    writer.stop(true)
    verifyWrite(writer, expectedRecords = 7, partitionsWithData = Set(0, 1, 2, 3, 4, 5, 6))
  }

  // ==================== Multi-batch: Basic Scenarios ====================

  test("multi-batch: batch2 fills batch1 gaps") {
    // Batch1: 1,3,5 (odd partitions)
    // Batch2: 0,2,4 (even partitions, triggered by 0 < 5)
    // Result: partition 6 empty
    val writer = createWriter()
    writer.write(createTestRecords(Iterator(1, 3, 5, 0, 2, 4)))
    writer.stop(true)
    // Both batch1 (1,3,5) and batch2 (0,2,4) data must be present
    verifyWrite(writer, expectedRecords = 6, partitionsWithData = Set(0, 1, 2, 3, 4, 5))
  }

  test("multi-batch: extreme jump max to min") {
    // Batch1: 6 (max partition only)
    // Batch2: 0 (min partition only, triggered by 0 < 6)
    // Result: partitions 1-5 empty
    val writer = createWriter()
    writer.write(createTestRecords(Iterator(6, 0)))
    writer.stop(true)
    // batch1 has partition 6, batch2 has partition 0
    verifyWrite(writer, expectedRecords = 2, partitionsWithData = Set(0, 6))
  }

  // ==================== Multi-batch: Overlap Scenarios ====================

  test("multi-batch: partitions overlap between batches") {
    // Batch1: 1,3,5
    // Batch2: 3,4,5 (triggered by 3 < 5, partitions 3,5 written again)
    // Partitions 3,5 have data from BOTH batches
    val writer = createWriter()
    writer.write(createTestRecords(Iterator(1, 3, 5, 3, 4, 5)))
    writer.stop(true)
    // batch1 contributes 1,3,5; batch2 contributes 3,4,5 -> union is 1,3,4,5
    // Partitions 3 and 5 should have 2 writes each
    verifyWrite(writer, expectedRecords = 6, partitionsWithData = Set(1, 3, 4, 5),
      minWritesPerPartition = Map(3 -> 2, 5 -> 2))
  }

  test("multi-batch: batch2 fully within batch1 range") {
    // Batch1: 0,1,2,3,4,5,6 (all partitions)
    // Batch2: 2,3,4 (triggered by 2 < 6, subset of batch1)
    val writer = createWriter()
    writer.write(createTestRecords(Iterator(0, 1, 2, 3, 4, 5, 6, 2, 3, 4)))
    writer.stop(true)
    // All 7 partitions have data; partitions 2,3,4 have 2 writes each
    verifyWrite(writer, expectedRecords = 10, partitionsWithData = Set(0, 1, 2, 3, 4, 5, 6),
      minWritesPerPartition = Map(2 -> 2, 3 -> 2, 4 -> 2))
  }

  // ==================== Multi-batch: Repeated Partitions ====================

  test("single batch: same partition repeated") {
    // Consecutive identical partition IDs can occur in two scenarios:
    // 1. Reslicing: a large partition is split into multiple smaller batches
    // 2. Data skew: multiple GPU batches each containing only the same partition's data
    // In both cases, they should be merged into a single shuffle batch (more efficient,
    // fewer partial files). This does NOT affect correctness since shuffle write only
    // cares about final data completeness per partition.
    // Input: 0,0,0,0,0 -> all in one batch
    val writer = createWriter()
    writer.write(createTestRecords(Iterator(0, 0, 0, 0, 0)))
    writer.stop(true)
    // Only partition 0 has data, all 5 records in a single batch
    // Verify partition 0 was written 5 times
    verifyWrite(writer, expectedRecords = 5, partitionsWithData = Set(0),
      minWritesPerPartition = Map(0 -> 5))
  }

  test("multi-batch: strictly decreasing creates one batch per record") {
    // Input: 5,4,3,2,1,0
    // Each partition ID < previous max, so 6 batches total
    // Batch1:5, Batch2:4, Batch3:3, Batch4:2, Batch5:1, Batch6:0
    val writer = createWriter()
    writer.write(createTestRecords(Iterator(5, 4, 3, 2, 1, 0)))
    writer.stop(true)
    // All batches contribute: partitions 0,1,2,3,4,5 have data
    verifyWrite(writer, expectedRecords = 6, partitionsWithData = Set(0, 1, 2, 3, 4, 5))
  }

  test("multi-batch: oscillating between two partitions") {
    // Input: 2,5,2,5,2,5
    // Batch1: 2,5; Batch2: 2,5; Batch3: 2,5
    val writer = createWriter()
    writer.write(createTestRecords(Iterator(2, 5, 2, 5, 2, 5)))
    writer.stop(true)
    // Only partitions 2 and 5 have data (from all 3 batches)
    // Each partition should have 3 writes
    verifyWrite(writer, expectedRecords = 6, partitionsWithData = Set(2, 5),
      minWritesPerPartition = Map(2 -> 3, 5 -> 3))
  }

  // ==================== Multi-batch: Size Variations ====================

  test("multi-batch: batch1 sparse, batch2 full") {
    // Batch1: 0,6 (only first and last)
    // Batch2: 0,1,2,3,4,5,6 (all partitions, triggered by 0 < 6)
    val writer = createWriter()
    writer.write(createTestRecords(Iterator(0, 6, 0, 1, 2, 3, 4, 5, 6)))
    writer.stop(true)
    // batch1 contributes 0,6; batch2 contributes all -> all partitions have data
    // Partitions 0 and 6 should have 2 writes each
    verifyWrite(writer, expectedRecords = 9, partitionsWithData = Set(0, 1, 2, 3, 4, 5, 6),
      minWritesPerPartition = Map(0 -> 2, 6 -> 2))
  }

  test("multi-batch: batch2 extends beyond batch1 range") {
    // Batch1: 2,3 (middle partitions)
    // Batch2: 0,1,4,5,6 (triggered by 0 < 3, covers both sides)
    val writer = createWriter()
    writer.write(createTestRecords(Iterator(2, 3, 0, 1, 4, 5, 6)))
    writer.stop(true)
    // batch1: 2,3; batch2: 0,1,4,5,6 -> all partitions
    verifyWrite(writer, expectedRecords = 7, partitionsWithData = Set(0, 1, 2, 3, 4, 5, 6))
  }

  // ==================== Multi-batch: Three+ Batches ====================

  test("multi-batch: three batches interleaved") {
    // Batch1: 2,4,6
    // Batch2: 1,3,5 (triggered by 1 < 6)
    // Batch3: 0 (triggered by 0 < 5)
    val writer = createWriter()
    writer.write(createTestRecords(Iterator(2, 4, 6, 1, 3, 5, 0)))
    writer.stop(true)
    // batch1: 2,4,6; batch2: 1,3,5; batch3: 0 -> all partitions
    verifyWrite(writer, expectedRecords = 7, partitionsWithData = Set(0, 1, 2, 3, 4, 5, 6))
  }

  // ==================== Cancellation Tests ====================

  test("merger thread responds to cancellation during write") {
    // Test that merger thread properly exits when task is cancelled.
    // This validates the fix for zombie merger thread issue where
    // merger could get stuck in wait() when task is killed.
    val writer = createWriter()

    // Start writing in a separate thread to simulate async operation
    val writeThread = new Thread(() => {
      try {
        // Write some data that will keep merger busy
        writer.write(createTestRecords(Iterator(0, 1, 2, 3, 4, 5, 6)))
      } catch {
        case _: Exception => // Expected if cancelled
      }
    })
    writeThread.start()

    // Give writer time to start processing
    Thread.sleep(100)

    // Simulate task cancellation by calling stop(false)
    // This should trigger cleanup and cancel merger futures
    val stopStartTime = System.currentTimeMillis()
    writer.stop(false)
    val stopDuration = System.currentTimeMillis() - stopStartTime

    // Wait for write thread to finish
    writeThread.join(5000)

    // Verify stop() completed in reasonable time (not stuck waiting for merger)
    // If merger thread was stuck in wait(), stop() would hang
    assert(stopDuration < 3000,
      s"stop() took ${stopDuration}ms, suggesting merger thread may be stuck")
    assert(!writeThread.isAlive, "Write thread should have finished")
  }

  test("merger thread handles interrupt flag correctly") {
    // Verify that merger thread checks interrupt flag and exits gracefully
    // when interrupted, rather than getting stuck in wait()
    val writer = createWriter()

    // Create a slow iterator that allows time for interruption
    val slowIterator = new Iterator[(Int, ColumnarBatch)] {
      private var count = 0
      override def hasNext: Boolean = count < 100
      override def next(): (Int, ColumnarBatch) = {
        count += 1
        Thread.sleep(10) // Slow down to allow interruption
        (count % 7, createTestBatch(count))
      }
    }

    // Start writing in background
    @volatile var writeException: Throwable = null
    val writeThread = new Thread(() => {
      try {
        writer.write(slowIterator)
      } catch {
        case e: Throwable => writeException = e
      }
    })
    writeThread.start()

    // Let it process a few records
    Thread.sleep(200)

    // Cancel the write operation
    writer.stop(false)

    // Write thread should finish quickly after stop()
    writeThread.join(3000)
    assert(!writeThread.isAlive,
      "Write thread should exit after stop(), merger thread may be stuck")
  }
}
