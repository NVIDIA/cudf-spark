/*
 * Copyright (c) 2020-2026, NVIDIA CORPORATION.
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

import java.lang.management.ManagementFactory
import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.AtomicReference

import scala.util.Try

import ai.rapids.cudf.{Cuda, DeviceMemoryBuffer, Table}
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.format.{CodecType, TableMeta}
import com.nvidia.spark.rapids.shuffle.RapidsShuffleTestHelper
import com.nvidia.spark.rapids.spill.{SpillableDeviceStore, SpillFramework}
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{doAnswer, doThrow, spy}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.concurrent.Eventually.eventually
import org.scalatest.concurrent.PatienceConfiguration.Timeout
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.time.{Seconds, Span}
import org.scalatestplus.mockito.MockitoSugar

import org.apache.spark.SparkConf
import org.apache.spark.shuffle.rapids.ShuffleOutputGoneException
import org.apache.spark.sql.types.{DataType, IntegerType}
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.storage.ShuffleBlockId

class ShuffleBufferCatalogSuite
  extends AnyFunSuite with MockitoSugar with BeforeAndAfterEach {

  override def beforeEach(): Unit = {
    super.beforeEach()
    SpillFramework.initialize(new RapidsConf(new SparkConf))
  }

  override def afterEach(): Unit = {
    super.afterEach()
    SpillFramework.shutdown()
  }

  test("registered shuffles should be active") {
    val shuffleCatalog = new ShuffleBufferCatalog()
    assertResult(false)(shuffleCatalog.hasActiveShuffle(123))
    shuffleCatalog.registerShuffle(123)
    assertResult(true)(shuffleCatalog.hasActiveShuffle(123))
    shuffleCatalog.unregisterShuffle(123)
    assertResult(false)(shuffleCatalog.hasActiveShuffle(123))
  }

  test("adding a degenerate batch") {
    val shuffleCatalog = new ShuffleBufferCatalog()
    val tableMeta = mock[TableMeta]
    // need to register the shuffle id first
    assertThrows[IllegalStateException] {
      shuffleCatalog.addDegenerateRapidsBuffer(ShuffleBlockId(1, 1L, 1), tableMeta)
    }
    shuffleCatalog.registerShuffle(1)
    shuffleCatalog.addDegenerateRapidsBuffer(ShuffleBlockId(1,1L,1), tableMeta)
    val storedMetas = shuffleCatalog.blockIdToMetas(ShuffleBlockId(1, 1L, 1))
    assertResult(1)(storedMetas.size)
    assertResult(tableMeta)(storedMetas.head)
  }

  test("adding a contiguous batch adds it to the spill store") {
    val shuffleCatalog = new ShuffleBufferCatalog()
    val ct = RapidsShuffleTestHelper.buildContiguousTable(1000)
    shuffleCatalog.registerShuffle(1)
    assertResult(0)(SpillFramework.stores.deviceStore.numHandles)
    withResource(ct)(shuffleCatalog.addContiguousTable(ShuffleBlockId(1, 1L, 1), _, -1))
    assertResult(1)(SpillFramework.stores.deviceStore.numHandles)
    val storedMetas = shuffleCatalog.blockIdToMetas(ShuffleBlockId(1, 1L, 1))
    assertResult(1)(storedMetas.size)
    shuffleCatalog.unregisterShuffle(1)
  }

  test("an add that fails after allocating its buffer id leaves nothing registered") {
    val shuffleCatalog = new ShuffleBufferCatalog()
    shuffleCatalog.registerShuffle(1)
    val block = ShuffleBlockId(1, 1L, 1)
    // a packed batch is not compressed, so the add throws once the id is in the block's list
    val packed = GpuPackedTableColumn.from(RapidsShuffleTestHelper.buildContiguousTable(10))
    val buffer = packed.column(0).asInstanceOf[GpuPackedTableColumn].getTableBuffer
    val refsBefore = buffer.getRefCount
    withResource(packed) { _ =>
      assertThrows[ClassCastException](shuffleCatalog.addCompressedBatch(block, packed, -1))
      assertResult(refsBefore)(buffer.getRefCount)
    }
    assertResult((0, 0, 0))(shuffleCatalog.bookkeepingSizes)
    assertResult(0)(SpillFramework.stores.deviceStore.numHandles)
    assertThrows[NoSuchElementException](shuffleCatalog.blockIdToMetas(block))
    shuffleCatalog.unregisterShuffle(1)
  }

  /** A batch of `numRows` rows compressed with `codecId`, as the shuffle exchange makes one. */
  private def compressedBatch(numRows: Int, codecId: Byte = CodecType.COPY): ColumnarBatch = {
    val codec = TableCompressionCodec.getCodec(codecId,
      TableCompressionCodec.makeCodecConfig(new RapidsConf(new SparkConf)))
    withResource(codec.createBatchCompressor(0, Cuda.DEFAULT_STREAM)) { c =>
      c.addTableToCompress(RapidsShuffleTestHelper.buildContiguousTable(numRows))
      withResource(c.finish())(tables => GpuCompressedColumnVector.from(tables.head))
    }
  }

  /** Reads the one batch of `block` back through the local read path and checks its rows. */
  private def assertReadsBack(
      catalog: ShuffleBufferCatalog,
      block: ShuffleBlockId,
      numRows: Int): Unit = {
    def check(table: Table): Unit =
      withResource(RapidsShuffleTestHelper.buildContiguousTable(numRows)) { expected =>
        TestUtils.compareColumns(expected.getTable.getColumn(0), table.getColumn(0))
      }
    withResource(catalog.getColumnarBatchIterator(block, Array[DataType](IntegerType)).toArray) {
      batches =>
        assertResult(1)(batches.length)
        batches.head.column(0) match {
          case column: GpuCompressedColumnVector =>
            val meta = column.getTableMeta
            val codec = TableCompressionCodec.getCodec(meta.bufferMeta.codecBufferDescrs(0).codec,
              TableCompressionCodec.makeCodecConfig(new RapidsConf(new SparkConf)))
            withResource(codec.createBatchDecompressor(0, Cuda.DEFAULT_STREAM)) { decompressor =>
              // the decompressor closes the buffer it is given
              column.getTableBuffer.incRefCount()
              decompressor.addBufferToDecompress(column.getTableBuffer, meta.bufferMeta)
              withResource(decompressor.finishAsync()) { decoded =>
                withResource(MetaUtils.getTableFromMeta(decoded.head, meta))(check)
              }
            }
          case _ => withResource(GpuColumnVector.from(batches.head))(check)
        }
    }
  }

  test("each add borrows its input") {
    val numRows = 1000
    val catalog = new ShuffleBufferCatalog()
    catalog.registerShuffle(1)
    val tableBlock = ShuffleBlockId(1, 1L, 1)
    val batchBlock = ShuffleBlockId(1, 1L, 2)
    val table = RapidsShuffleTestHelper.buildContiguousTable(numRows)
    val tableBuffer = table.getBuffer
    val batch = compressedBatch(numRows)
    val batchBuffer = batch.column(0).asInstanceOf[GpuCompressedColumnVector].getTableBuffer
    def refs: (Int, Int) = (tableBuffer.getRefCount, batchBuffer.getRefCount)
    // the input keeps the producer's reference, and the catalog takes one of its own
    val refsAfterAdds = withResource(table) { _ =>
      withResource(batch) { _ =>
        catalog.addContiguousTable(tableBlock, table, -1)
        catalog.addCompressedBatch(batchBlock, batch, -1)
        refs
      }
    }
    val refsAfterCloses = refs
    // a freed buffer is not read, since that touches freed device memory
    def read(block: ShuffleBlockId, buffer: DeviceMemoryBuffer): String =
      if (buffer.getRefCount < 1) {
        "skipped"
      } else {
        Try(assertReadsBack(catalog, block, numRows)).fold(e => s"failed: $e", _ => "ok")
      }
    val tableRead = read(tableBlock, tableBuffer)
    val batchRead = read(batchBlock, batchBuffer)
    val unregister = Try(catalog.unregisterShuffle(1)).fold(e => s"threw: $e", _ => "ok")
    val refsAfterUnregister = refs
    val handles = SpillFramework.stores.deviceStore.numHandles
    assertResult(((2, 2), (1, 1), "ok", "ok", "ok", (0, 0), 0), "(references after the adds, " +
      "after the producer's closes, table read, batch read, unregister, references after it, " +
      "device handles)")(
      (refsAfterAdds, refsAfterCloses, tableRead, batchRead, unregister, refsAfterUnregister,
        handles))
  }

  test("an add to an unregistered shuffle leaves its input with the producer") {
    val catalog = new ShuffleBufferCatalog()
    catalog.registerShuffle(1)
    catalog.unregisterShuffle(1)
    val block = ShuffleBlockId(1, 1L, 1)
    val sizesBefore = catalog.bookkeepingSizes
    val table = RapidsShuffleTestHelper.buildContiguousTable(10)
    val batch = compressedBatch(10)
    val tableBuffer = table.getBuffer
    val batchBuffer = batch.column(0).asInstanceOf[GpuCompressedColumnVector].getTableBuffer
    // the class of what an add threw
    def thrown(add: => ShuffleBufferId): String =
      Try(add).failed.map(_.getClass.getSimpleName).getOrElse("nothing")
    val adds = (thrown(catalog.addContiguousTable(block, table, -1)),
      thrown(catalog.addCompressedBatch(block, batch, -1)))
    val refs = (tableBuffer.getRefCount, batchBuffer.getRefCount)
    val sizes = catalog.bookkeepingSizes
    val closes = (Try(table.close()).fold(e => s"threw: $e", _ => "ok"),
      Try(batch.close()).fold(e => s"threw: $e", _ => "ok"))
    assertResult((("IllegalStateException", "IllegalStateException"), (1, 1), ("ok", "ok"),
      sizesBefore), "(what the adds threw, references after them, the producer's closes, " +
      "bookkeeping sizes)")((adds, refs, closes, sizes))
  }

  test("a handle factory that throws releases the catalog's reference") {
    val catalog = new ShuffleBufferCatalog()
    catalog.registerShuffle(1)
    val block = ShuffleBlockId(1, 1L, 1)
    val table = RapidsShuffleTestHelper.buildContiguousTable(10)
    val batch = compressedBatch(10)
    // a device store that refuses to track a handle makes SpillableDeviceBufferHandle throw
    SpillFramework.storesInternal.deviceStore.close()
    val store = spy(new SpillableDeviceStore)
    SpillFramework.storesInternal.deviceStore = store
    val adds = Seq[(AutoCloseable, DeviceMemoryBuffer, () => ShuffleBufferId)](
      (table, table.getBuffer, () => catalog.addContiguousTable(block, table, -1)),
      (batch, batch.column(0).asInstanceOf[GpuCompressedColumnVector].getTableBuffer,
        () => catalog.addCompressedBatch(block, batch, -1)))
    val outcomes = adds.map { case (input, buffer, add) =>
      doThrow(new IllegalStateException("injected")).doCallRealMethod().when(store).track(any())
      val thrown = Try(add()).failed.map(_.getMessage).getOrElse("nothing")
      val refsAfterAdd = buffer.getRefCount
      val handles = SpillFramework.stores.deviceStore.numHandles
      val producerClose = Try(input.close()).fold(e => s"threw: $e", _ => "ok")
      (thrown, refsAfterAdd, handles, producerClose, buffer.getRefCount)
    }
    catalog.unregisterShuffle(1)
    assertResult(Seq.fill(2)(("injected", 1, 0, "ok", 0)), "packed, then COPY: (what the add " +
      "threw, references after the add, device handles, the producer's close, references " +
      "after it)")(outcomes)
  }

  /** A block's list of buffer ids, which the catalog keeps private and locks for every change. */
  private def blockBufferIds(catalog: ShuffleBufferCatalog, block: ShuffleBlockId): AnyRef = {
    val field = classOf[ShuffleBufferCatalog].getDeclaredField("activeShuffles")
    field.setAccessible(true)
    val shuffles = field.get(catalog).asInstanceOf[
      java.util.Map[Any, ShuffleBufferCatalog.ShuffleRegistration]]
    shuffles.get(block.shuffleId).blocks.get(block)
  }

  test("a removal that empties a block waits for an append to that block in flight") {
    val awaitSeconds = 30
    val shuffleCatalog = new ShuffleBufferCatalog()
    shuffleCatalog.registerShuffle(1)
    val block = ShuffleBlockId(1, 1L, 1)
    val failed = shuffleCatalog.addDegenerateRapidsBuffer(block, mock[TableMeta])
    val appendedMeta = mock[TableMeta]
    val appender = new Thread(() => shuffleCatalog.addDegenerateRapidsBuffer(block, appendedMeta),
      "block-appender")
    val remover = new Thread(() => shuffleCatalog.removeCachedHandles(Seq(failed)),
      "block-remover")
    appender.setDaemon(true)
    remover.setDaemon(true)
    val threads = ManagementFactory.getThreadMXBean
    val list = blockBufferIds(shuffleCatalog, block)
    list.synchronized {
      appender.start()
      eventually(Timeout(Span(awaitSeconds, Seconds))) {
        val info = threads.getThreadInfo(appender.getId)
        assert(info.getThreadState == Thread.State.BLOCKED &&
          info.getLockInfo.getIdentityHashCode == System.identityHashCode(list),
          "the append never reached the block's list")
      }
      remover.start()
      // The append holds the block's map entry while it waits for the list, so the removal must
      // wait for the append; waiting for the list instead would let it drop the list under it.
      eventually(Timeout(Span(awaitSeconds, Seconds))) {
        val info = threads.getThreadInfo(remover.getId)
        assert(info.getThreadState == Thread.State.BLOCKED &&
          info.getLockOwnerId == appender.getId, "the removal did not wait for the append")
      }
    }
    appender.join(TimeUnit.SECONDS.toMillis(awaitSeconds))
    remover.join(TimeUnit.SECONDS.toMillis(awaitSeconds))
    assert(!appender.isAlive && !remover.isAlive)
    assertResult(Seq(appendedMeta))(shuffleCatalog.blockIdToMetas(block))
    assertResult((1, 1, 1))(shuffleCatalog.bookkeepingSizes)
    shuffleCatalog.unregisterShuffle(1)
  }

  test("unregisterShuffle copies a block's list under the list's lock") {
    val awaitSeconds = 30
    val shuffleCatalog = new ShuffleBufferCatalog()
    shuffleCatalog.registerShuffle(1)
    val block = ShuffleBlockId(1, 1L, 1)
    shuffleCatalog.addDegenerateRapidsBuffer(block, mock[TableMeta])
    shuffleCatalog.addDegenerateRapidsBuffer(block, mock[TableMeta])
    val unregister = new Thread(() => shuffleCatalog.unregisterShuffle(1), "shuffle-unregister")
    unregister.setDaemon(true)
    val threads = ManagementFactory.getThreadMXBean
    val list = blockBufferIds(shuffleCatalog, block)
    list.synchronized {
      unregister.start()
      // A failed writer's cleanup can be shifting the list, so the copy must wait for its lock.
      eventually(Timeout(Span(awaitSeconds, Seconds))) {
        val info = threads.getThreadInfo(unregister.getId)
        assert(info != null && info.getThreadState == Thread.State.BLOCKED &&
          info.getLockInfo.getIdentityHashCode == System.identityHashCode(list),
          "unregisterShuffle did not wait for the block's list")
      }
    }
    unregister.join(TimeUnit.SECONDS.toMillis(awaitSeconds))
    assert(!unregister.isAlive)
    assert(!shuffleCatalog.hasActiveShuffle(1))
    assertResult((0, 0, 0))(shuffleCatalog.bookkeepingSizes)
  }

  test("an add whose shuffle is unregistered while the add is in flight fails and leaves " +
      "nothing behind") {
    val awaitSeconds = 30
    val shuffleCatalog = new ShuffleBufferCatalog()
    shuffleCatalog.registerShuffle(1)
    val inTrack = new CountDownLatch(1)
    val resumeTrack = new CountDownLatch(1)
    // the store tracks a new handle after the catalog lists its buffer and before the catalog
    // maps the buffer to it
    SpillFramework.storesInternal.deviceStore.close()
    SpillFramework.storesInternal.deviceStore = spy(new SpillableDeviceStore)
    doAnswer(invocation => {
      inTrack.countDown()
      resumeTrack.await(awaitSeconds, TimeUnit.SECONDS)
      invocation.callRealMethod()
    }).when(SpillFramework.stores.deviceStore).track(any())
    val handlesBefore = SpillFramework.stores.deviceStore.numHandles
    val table = RapidsShuffleTestHelper.buildContiguousTable(1000)
    val added = new AtomicReference[Try[ShuffleBufferId]]()
    // the catalog closes the table it is given
    val adder = new Thread(() => added.set(Try(shuffleCatalog.addContiguousTable(
      ShuffleBlockId(1, 7L, 0), table, -1))), "shuffle-add")
    adder.setDaemon(true)
    try {
      adder.start()
      assert(inTrack.await(awaitSeconds, TimeUnit.SECONDS), "the add never tracked its buffer")
      shuffleCatalog.unregisterShuffle(1)
    } finally {
      resumeTrack.countDown()
      adder.join(TimeUnit.SECONDS.toMillis(awaitSeconds))
    }
    assert(!adder.isAlive)
    val outcome = added.get
    val handlesAfter = SpillFramework.stores.deviceStore.numHandles
    val bookkeepingAfter = shuffleCatalog.bookkeepingSizes
    // an add that returns hands its buffer to the caller
    outcome.foreach(id => shuffleCatalog.removeCachedHandles(Seq(id)))
    assert(outcome.failed.toOption.exists(_.isInstanceOf[IllegalStateException]),
      s"the add returned $outcome, leaving $bookkeepingAfter " +
        "(bufferIdToHandle, tableMap, block lists)")
    assertResult(handlesBefore)(handlesAfter)
    assertResult((0, 0, 0))(bookkeepingAfter)
    assert(!shuffleCatalog.hasActiveShuffle(1))
  }

  test("a degenerate add whose shuffle is unregistered and registered again while the add is " +
      "in flight fails and leaves nothing behind") {
    val awaitSeconds = 30
    val shuffleCatalog = new ShuffleBufferCatalog()
    shuffleCatalog.registerShuffle(1)
    val block = ShuffleBlockId(1, 7L, 0)
    shuffleCatalog.addDegenerateRapidsBuffer(block, mock[TableMeta])
    val meta = mock[TableMeta]
    val added = new AtomicReference[Try[ShuffleBufferId]]()
    val adder = new Thread(() => added.set(Try(shuffleCatalog.addDegenerateRapidsBuffer(block,
      meta))), "shuffle-add")
    adder.setDaemon(true)
    val threads = ManagementFactory.getThreadMXBean
    val list = blockBufferIds(shuffleCatalog, block)
    try {
      list.synchronized {
        adder.start()
        eventually(Timeout(Span(awaitSeconds, Seconds))) {
          val info = threads.getThreadInfo(adder.getId)
          assert(info != null && info.getThreadState == Thread.State.BLOCKED &&
            info.getLockInfo.getIdentityHashCode == System.identityHashCode(list),
            "the add never reached the block's list")
        }
        // This thread holds the list's lock, so the removal copies the list without the add's
        // id. A later writer of the shuffle then registers it again before the add checks.
        shuffleCatalog.unregisterShuffle(1)
        shuffleCatalog.registerShuffle(1)
      }
    } finally {
      adder.join(TimeUnit.SECONDS.toMillis(awaitSeconds))
    }
    assert(!adder.isAlive)
    val outcome = added.get
    val bookkeepingAfter = shuffleCatalog.bookkeepingSizes
    // an add that returns hands its buffer to the caller
    outcome.foreach(id => shuffleCatalog.removeCachedHandles(Seq(id)))
    assert(outcome.failed.toOption.exists(_.isInstanceOf[IllegalStateException]),
      s"the add returned $outcome, leaving $bookkeepingAfter " +
        "(bufferIdToHandle, tableMap, block lists)")
    assertResult((0, 0, 0))(bookkeepingAfter)
    assert(shuffleCatalog.hasActiveShuffle(1))
    shuffleCatalog.unregisterShuffle(1)
  }

  test("get a columnar batch iterator from catalog") {
    val shuffleCatalog = new ShuffleBufferCatalog()
    shuffleCatalog.registerShuffle(1)
    // add metadata only table
    val tableMeta = RapidsShuffleTestHelper.mockTableMeta(0)
    shuffleCatalog.addDegenerateRapidsBuffer(ShuffleBlockId(1, 1L, 1), tableMeta)
    val ct = RapidsShuffleTestHelper.buildContiguousTable(1000)
    withResource(ct)(shuffleCatalog.addContiguousTable(ShuffleBlockId(1, 1L, 1), _, -1))
    val iter =
      shuffleCatalog.getColumnarBatchIterator(
        ShuffleBlockId(1, 1L, 1), Array[DataType](IntegerType))
    withResource(iter.toArray) { cbs =>
      assertResult(2)(cbs.length)
      assertResult(0)(cbs.head.numRows())
      assertResult(1)(cbs.head.numCols())
      assertResult(1000)(cbs.last.numRows())
      assertResult(1)(cbs.last.numCols())
      shuffleCatalog.unregisterShuffle(1)
    }
  }

  test("a compressed batch reads back compressed through the local read path") {
    val numRows = 1000
    val codec = TableCompressionCodec.getCodec(CodecType.NVCOMP_LZ4,
      TableCompressionCodec.makeCodecConfig(new RapidsConf(new SparkConf)))
    val shuffleCatalog = new ShuffleBufferCatalog()
    shuffleCatalog.registerShuffle(1)
    val block = ShuffleBlockId(1, 1L, 1)
    // the read comes before the producer's close, which frees the buffer if an add also closed it
    withResource(compressedBatch(numRows, CodecType.NVCOMP_LZ4)) { compressed =>
      shuffleCatalog.addCompressedBatch(block, compressed, -1)
      withResource(shuffleCatalog.getColumnarBatchIterator(
          block, Array[DataType](IntegerType)).toArray) { batches =>
        assertResult(1)(batches.length)
        assert(GpuCompressedColumnVector.isBatchCompressed(batches.head))
        val column = batches.head.column(0).asInstanceOf[GpuCompressedColumnVector]
        val meta = column.getTableMeta
        withResource(codec.createBatchDecompressor(0, Cuda.DEFAULT_STREAM)) {
          decompressor =>
            // the decompressor closes the buffer it is given
            column.getTableBuffer.incRefCount()
            decompressor.addBufferToDecompress(column.getTableBuffer, meta.bufferMeta)
            withResource(decompressor.finishAsync()) { decoded =>
              withResource(MetaUtils.getTableFromMeta(decoded.head, meta)) { table =>
                withResource(RapidsShuffleTestHelper.buildContiguousTable(numRows)) { expected =>
                  TestUtils.compareColumns(expected.getTable.getColumn(0), table.getColumn(0))
                }
              }
            }
        }
      }
    }
    shuffleCatalog.unregisterShuffle(1)
    assertResult(0)(SpillFramework.stores.deviceStore.numHandles)
  }

  test("when map ids repeat, only the first output committed for a map is read") {
    // With spark.shuffle.useOldFetchProtocol=true every attempt of a map writes the same blocks.
    val catalog = new ShuffleBufferCatalog(mapIdsCanRepeat = true)
    catalog.registerShuffle(1)
    val blocks = (0 until 3).map(ShuffleBlockId(1, 7L, _))
    def rowCounts(block: ShuffleBlockId): Seq[Long] = {
      val metaRows = catalog.blockIdToMetas(block).map(_.rowCount())
      withResource(catalog.getColumnarBatchIterator(block, Array[DataType](IntegerType)).toArray) {
        batches => assertResult(metaRows)(batches.map(_.numRows().toLong).toSeq)
      }
      metaRows
    }
    val first = Seq(
      withResource(RapidsShuffleTestHelper.buildContiguousTable(10))(
        catalog.addContiguousTable(blocks(0), _, -1)),
      catalog.addDegenerateRapidsBuffer(blocks(0),
        MetaUtils.buildDegenerateTableMeta(new ColumnarBatch(Array.empty, 11))),
      withResource(RapidsShuffleTestHelper.buildContiguousTable(5))(
        catalog.addContiguousTable(blocks(1), _, -1)))
    // an attempt is not read before it commits
    blocks.foreach(block => assertThrows[NoSuchElementException](catalog.blockIdToMetas(block)))
    val firstSizes = Array(200L, 100L, 0L)
    catalog.commitMapOutput(1, 7L, first, firstSizes)
    assertResult(Seq(200L, 100L, 0L))(firstSizes.toSeq)

    val second = Seq(
      withResource(RapidsShuffleTestHelper.buildContiguousTable(20))(
        catalog.addContiguousTable(blocks(0), _, -1)),
      withResource(RapidsShuffleTestHelper.buildContiguousTable(30))(
        catalog.addContiguousTable(blocks(2), _, -1)))
    assertResult(Seq(10L, 11L))(rowCounts(blocks(0)))
    val secondSizes = Array(400L, 0L, 300L)
    catalog.commitMapOutput(1, 7L, second, secondSizes)
    // the later attempt reports the committed output's sizes, as Spark's sort shuffle does
    assertResult(Seq(200L, 100L, 0L))(secondSizes.toSeq)
    assertResult(Seq(10L, 11L))(rowCounts(blocks(0)))
    assertResult(Seq(5L))(rowCounts(blocks(1)))
    assertThrows[NoSuchElementException](catalog.blockIdToMetas(blocks(2)))
    second.foreach { id =>
      assertThrows[NoSuchElementException](catalog.getShuffleBufferHandle(id.tableId))
    }
    assertResult(2)(SpillFramework.stores.deviceStore.numHandles)
    assertResult((3, 3, 3))(catalog.bookkeepingSizes)

    catalog.unregisterShuffle(1)
    assertResult(0)(SpillFramework.stores.deviceStore.numHandles)
    assertResult((0, 0, 0))(catalog.bookkeepingSizes)
  }

  /** Reads every batch of `block` through the catalog's local read path, closing each. */
  private def readLocally(catalog: ShuffleBufferCatalog, block: ShuffleBlockId): Unit =
    catalog.getColumnarBatchIterator(block, Array[DataType](IntegerType)).foreach(_.close())

  test("a lookup of output the catalog does not hold throws ShuffleOutputGoneException") {
    Seq(false, true).foreach { mapIdsCanRepeat =>
      val catalog = new ShuffleBufferCatalog(mapIdsCanRepeat)
      val block = ShuffleBlockId(1, 7L, 0)
      withClue(s"unknown shuffle, mapIdsCanRepeat=$mapIdsCanRepeat") {
        val gone = intercept[ShuffleOutputGoneException](catalog.blockIdToMetas(block))
        assertResult("unknown shuffle 1 for block shuffle_1_7_0")(gone.getMessage)
        assertThrows[ShuffleOutputGoneException](readLocally(catalog, block))
      }
      catalog.registerShuffle(1)
      withClue(s"unknown block, mapIdsCanRepeat=$mapIdsCanRepeat") {
        assertThrows[ShuffleOutputGoneException](catalog.blockIdToMetas(block))
        assertThrows[ShuffleOutputGoneException](readLocally(catalog, block))
      }
      catalog.unregisterShuffle(1)
    }
  }

  test("when map ids repeat, a read of committed buffers that were removed throws " +
      "ShuffleOutputGoneException") {
    val catalog = new ShuffleBufferCatalog(mapIdsCanRepeat = true)
    catalog.registerShuffle(1)
    val block = ShuffleBlockId(1, 7L, 0)
    val ids = Seq(withResource(RapidsShuffleTestHelper.buildContiguousTable(10))(
      catalog.addContiguousTable(block, _, -1)))
    catalog.commitMapOutput(1, 7L, ids, Array(100L))
    // the committed output keeps its ids, so both reads find ids with no entries
    catalog.removeCachedHandles(ids)
    assertThrows[ShuffleOutputGoneException](catalog.blockIdToMetas(block))
    assertThrows[ShuffleOutputGoneException](readLocally(catalog, block))
    catalog.unregisterShuffle(1)
  }

  test("when map ids repeat, a commit after unregisterShuffle fails") {
    val catalog = new ShuffleBufferCatalog(mapIdsCanRepeat = true)
    catalog.registerShuffle(1)
    val block = ShuffleBlockId(1, 7L, 0)
    val ids = Seq(withResource(RapidsShuffleTestHelper.buildContiguousTable(10))(
      catalog.addContiguousTable(block, _, -1)))
    catalog.unregisterShuffle(1)
    // the map's MapStatus would name output the executor no longer has
    assertThrows[IllegalStateException](catalog.commitMapOutput(1, 7L, ids, Array(100L)))
    assertResult(0)(SpillFramework.stores.deviceStore.numHandles)
    assertResult((0, 0, 0))(catalog.bookkeepingSizes)
  }

  test("when map ids repeat, a commit fails if the shuffle's registration does not list its " +
      "buffers") {
    val awaitSeconds = 30
    val catalog = new ShuffleBufferCatalog(mapIdsCanRepeat = true)
    catalog.registerShuffle(1)
    val block = ShuffleBlockId(1, 7L, 0)
    val late = Seq(catalog.addDegenerateRapidsBuffer(block, mock[TableMeta]))
    val unregister = new Thread(() => catalog.unregisterShuffle(1), "shuffle-unregister")
    unregister.setDaemon(true)
    val threads = ManagementFactory.getThreadMXBean
    val list = blockBufferIds(catalog, block)
    try {
      list.synchronized {
        unregister.start()
        eventually(Timeout(Span(awaitSeconds, Seconds))) {
          val info = threads.getThreadInfo(unregister.getId)
          assert(info != null && info.getThreadState == Thread.State.BLOCKED &&
            info.getLockInfo.getIdentityHashCode == System.identityHashCode(list),
            "unregisterShuffle did not wait for the block's list")
        }
        // The removal has taken the registration but not yet freed its buffers, and a later
        // writer of the shuffle registers it again before the late writer commits.
        assert(!catalog.hasActiveShuffle(1))
        catalog.registerShuffle(1)
        assertThrows[IllegalStateException](catalog.commitMapOutput(1, 7L, late, Array(100L)))
      }
    } finally {
      unregister.join(TimeUnit.SECONDS.toMillis(awaitSeconds))
    }
    assert(!unregister.isAlive)
    // as the late writer's stop(false) would
    catalog.removeCachedHandles(late)
    val freshMeta = mock[TableMeta]
    val fresh = Seq(catalog.addDegenerateRapidsBuffer(block, freshMeta))
    val freshSizes = Array(200L)
    catalog.commitMapOutput(1, 7L, fresh, freshSizes)
    assertResult(Seq(200L))(freshSizes.toSeq)
    assertResult(Seq(freshMeta))(catalog.blockIdToMetas(block))
    assertResult((1, 1, 1))(catalog.bookkeepingSizes)
    catalog.unregisterShuffle(1)
    assertResult((0, 0, 0))(catalog.bookkeepingSizes)
  }

  test("when map ids repeat, a commit fails if the shuffle's registration lists its block but " +
      "not its buffers") {
    val catalog = new ShuffleBufferCatalog(mapIdsCanRepeat = true)
    catalog.registerShuffle(1)
    val block = ShuffleBlockId(1, 7L, 0)
    val late = Seq(catalog.addDegenerateRapidsBuffer(block, mock[TableMeta]))
    catalog.unregisterShuffle(1)
    // a later attempt of the map registers the shuffle again and writes the same block before
    // the late attempt commits
    catalog.registerShuffle(1)
    val freshMeta = mock[TableMeta]
    val fresh = Seq(catalog.addDegenerateRapidsBuffer(block, freshMeta))
    assertThrows[IllegalStateException](catalog.commitMapOutput(1, 7L, late, Array(100L)))
    // as the late writer's stop(false) would
    catalog.removeCachedHandles(late)
    val freshSizes = Array(200L)
    catalog.commitMapOutput(1, 7L, fresh, freshSizes)
    assertResult(Seq(200L))(freshSizes.toSeq)
    assertResult(Seq(freshMeta))(catalog.blockIdToMetas(block))
    assertResult((1, 1, 1))(catalog.bookkeepingSizes)
    catalog.unregisterShuffle(1)
    assertResult((0, 0, 0))(catalog.bookkeepingSizes)
  }
}
