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

package org.apache.spark.sql.rapids

import java.util.concurrent.atomic.AtomicBoolean

import scala.collection.mutable.ArrayBuffer
import scala.util.{Failure, Success, Try}

import ai.rapids.cudf.{Cuda, MemoryBuffer, Rmm}
import com.nvidia.spark.rapids.{GpuCompressedColumnVector, GpuPackedTableColumn, RapidsConf,
  RmmSparkRetrySuiteBase, ShuffleBufferCatalog, ShuffleBufferCatalogTestUtils,
  TableCompressionCodec, WithTableBuffer}
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.format.CodecType
import com.nvidia.spark.rapids.shuffle.RapidsShuffleTestHelper
import com.nvidia.spark.rapids.spill.SpillFramework
import org.mockito.Mockito.when
import org.scalatestplus.mockito.MockitoSugar

import org.apache.spark.{HashPartitioner, SparkConf}
import org.apache.spark.executor.ShuffleWriteMetrics
import org.apache.spark.shuffle.BaseShuffleHandle
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.rapids.execution.GpuShuffleExchangeExecBase.METRIC_DATA_SIZE
import org.apache.spark.sql.types.{DataType, IntegerType}
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.storage.{BlockManager, ShuffleBlockId}

/**
 * Drives RapidsCachingWriter the way Spark's ShuffleWriteProcessor does, over a real
 * ShuffleBufferCatalog: write(), then stop(true), or stop(false) once write() has thrown.
 */
class RapidsCachingWriterSuite extends RmmSparkRetrySuiteBase with MockitoSugar {
  private val numPartitions = 4
  private val sparkTypes: Array[DataType] = Array(IntegerType)

  private sealed trait Kind
  private case object Packed extends Kind
  private case object Compressed extends Kind
  private case object NoColumns extends Kind

  /** One batch a map task hands to its writer. */
  private case class Input(partId: Int, rows: Int, kind: Kind) {
    /** Whether the catalog holds a device buffer for it, rather than only its metadata. */
    def deviceBacked: Boolean = kind != NoColumns && rows > 0

    def build(): ColumnarBatch = kind match {
      case Packed => GpuPackedTableColumn.from(RapidsShuffleTestHelper.buildContiguousTable(rows))
      case Compressed => compressedBatch(rows)
      case NoColumns => new ColumnarBatch(Array.empty, rows)
    }
  }

  private def compressedBatch(rows: Int): ColumnarBatch = {
    val codec = TableCompressionCodec.getCodec(CodecType.COPY,
      TableCompressionCodec.makeCodecConfig(new RapidsConf(new SparkConf)))
    withResource(codec.createBatchCompressor(0, Cuda.DEFAULT_STREAM)) { compressor =>
      compressor.addTableToCompress(RapidsShuffleTestHelper.buildContiguousTable(rows))
      withResource(compressor.finish()) { tables =>
        GpuCompressedColumnVector.from(tables.head)
      }
    }
  }

  private def newWriter(
      catalog: ShuffleBufferCatalog,
      shuffleId: Int,
      mapId: Long): RapidsCachingWriter[Int, ColumnarBatch] = {
    val dependency = mock[GpuShuffleDependency[Int, ColumnarBatch, ColumnarBatch]]
    when(dependency.partitioner).thenReturn(new HashPartitioner(numPartitions))
    val metrics = Map(METRIC_DATA_SIZE -> new SQLMetric("size"))
    val handle = new GpuShuffleHandle[Int, ColumnarBatch](
      new BaseShuffleHandle(shuffleId, dependency), dependency)
    // the shuffle manager registers the shuffle before it builds a writer for it
    catalog.registerShuffle(shuffleId)
    new RapidsCachingWriter(mock[BlockManager], handle, mapId, new ShuffleWriteMetrics,
      catalog, None, metrics)
  }

  /**
   * Feeds `inputs` to the writer, building each batch when the writer asks for it, as an
   * upstream operator does, then fails the map task's input if `thenFail`. `afterFirstBatch` runs
   * once the first batch is cached. Afterwards the batches are closed as the shuffle exchange
   * closes them, except compressed ones, which the catalog has already closed.
   */
  private def write(
      writer: RapidsCachingWriter[Int, ColumnarBatch],
      inputs: Seq[Input],
      thenFail: Boolean = false,
      afterFirstBatch: () => Unit = () => ()): Unit = {
    val built = new ArrayBuffer[ColumnarBatch]()
    val records = inputs.iterator.zipWithIndex.map { case (input, i) =>
      if (i == 1) {
        afterFirstBatch()
      }
      val batch = input.build()
      built += batch
      (input.partId, batch)
    }
    val failure = new Iterator[(Int, ColumnarBatch)] {
      override def hasNext: Boolean = thenFail
      override def next(): (Int, ColumnarBatch) =
        throw new IllegalStateException("injected map task failure")
    }
    try {
      writer.write(records ++ failure)
    } finally {
      built.filterNot(GpuCompressedColumnVector.isBatchCompressed).safeClose()
    }
  }

  private def succeed(writer: RapidsCachingWriter[Int, ColumnarBatch], inputs: Seq[Input]): Unit = {
    write(writer, inputs)
    assert(writer.stop(true).isDefined)
  }

  /** The table ids of a map's device-backed entries, read back through the catalog. */
  private def deviceTableIds(
      catalog: ShuffleBufferCatalog,
      shuffleId: Int,
      mapId: Long,
      inputs: Seq[Input]): Seq[Int] = {
    inputs.groupBy(_.partId).toSeq.flatMap { case (partId, expected) =>
      val metas = catalog.blockIdToMetas(ShuffleBlockId(shuffleId, mapId, partId))
      metas.zip(expected).collect { case (meta, input) if input.deviceBacked =>
        meta.bufferMeta().id()
      }
    }
  }

  /** Asserts that a map's output reads back whole: metadata, batches and device buffers. */
  private def assertReadable(
      catalog: ShuffleBufferCatalog,
      shuffleId: Int,
      mapId: Long,
      inputs: Seq[Input]): Unit = {
    inputs.groupBy(_.partId).foreach { case (partId, expected) =>
      val block = ShuffleBlockId(shuffleId, mapId, partId)
      assertResult(expected.map(_.rows.toLong))(catalog.blockIdToMetas(block).map(_.rowCount()))
      withResource(catalog.getColumnarBatchIterator(block, sparkTypes).toArray) { batches =>
        assertResult(expected.map(_.rows))(batches.map(_.numRows()).toSeq)
      }
    }
    deviceTableIds(catalog, shuffleId, mapId, inputs).foreach { tableId =>
      val handle = catalog.getShuffleBufferHandle(tableId)
      withResource(handle.spillable.materialize()) { buffer =>
        assert(buffer.getLength > 0)
      }
    }
  }

  private def assertGone(
      catalog: ShuffleBufferCatalog,
      shuffleId: Int,
      mapId: Long,
      inputs: Seq[Input]): Unit = {
    inputs.map(_.partId).distinct.foreach { partId =>
      assertThrows[NoSuchElementException] {
        catalog.blockIdToMetas(ShuffleBlockId(shuffleId, mapId, partId))
      }
    }
  }

  /**
   * The table ids the catalog still maps, as (device-backed, metadata-only). A fresh catalog
   * hands out table ids from 0, so `numAdded` bounds every id it has used.
   */
  private def mappedTableIds(catalog: ShuffleBufferCatalog, numAdded: Int): (Set[Int], Set[Int]) = {
    val lookups = (0 until numAdded).map(id => id -> Try(catalog.getShuffleBufferHandle(id)))
    lookups.foreach {
      case (_, Success(_)) | (_, Failure(_: IllegalStateException | _: NoSuchElementException)) =>
      case (id, Failure(e)) => fail(s"table id $id is inconsistent", e)
    }
    (lookups.collect { case (id, Success(_)) => id }.toSet,
      lookups.collect { case (id, Failure(_: IllegalStateException)) => id }.toSet)
  }

  private def numDeviceHandles: Int = SpillFramework.stores.deviceStore.numHandles

  /**
   * Asserts that bufferIdToHandle, tableMap and the per-block lists each hold exactly `buffers`
   * entries, so none of them keeps an entry the others have dropped.
   */
  private def assertConsistent(catalog: ShuffleBufferCatalog, buffers: Int): Unit = {
    assertResult((buffers, buffers, buffers), "(bufferIdToHandle, tableMap, block lists)")(
      ShuffleBufferCatalogTestUtils.bookkeepingSizes(catalog))
  }

  // survivors: another map of the same shuffle, and a map of another shuffle that reuses the
  // failed writer's map id
  private val sameShuffleMap = Seq(Input(0, 10, Packed), Input(0, 11, Packed),
    Input(1, 12, Compressed), Input(2, 0, Packed), Input(3, 13, NoColumns))
  private val otherShuffleMap = Seq(Input(0, 20, Packed), Input(1, 21, Compressed),
    Input(3, 23, NoColumns))
  // what the failing writer caches before its input fails, several batches to one block
  private val failedMap = Seq(Input(0, 30, Packed), Input(0, 32, Compressed),
    Input(0, 34, NoColumns), Input(1, 31, Compressed), Input(2, 0, Packed),
    Input(3, 33, NoColumns))
  private val numAdded = sameShuffleMap.size + otherShuffleMap.size + failedMap.size

  /**
   * Shuffle 1 map 10 and shuffle 2 map 11 succeed, then shuffle 1 map 11 fails after caching
   * part of its output. Returns the failed writer and its device table ids.
   */
  private def failOneWriter(catalog: ShuffleBufferCatalog)
      : (RapidsCachingWriter[Int, ColumnarBatch], Seq[Int]) = {
    succeed(newWriter(catalog, 1, 10L), sameShuffleMap)
    succeed(newWriter(catalog, 2, 11L), otherShuffleMap)
    val failed = newWriter(catalog, 1, 11L)
    assertThrows[IllegalStateException](write(failed, failedMap, thenFail = true))
    val failedTableIds = deviceTableIds(catalog, 1, 11L, failedMap)
    assert(failed.stop(false).isEmpty)
    (failed, failedTableIds)
  }

  private def assertOnlySurvivorsRemain(
      catalog: ShuffleBufferCatalog,
      failedTableIds: Seq[Int]): Unit = {
    val survivors = sameShuffleMap ++ otherShuffleMap
    assertResult(survivors.count(_.deviceBacked), "device buffers left after the cleanup")(
      numDeviceHandles)
    assertReadable(catalog, 1, 10L, sameShuffleMap)
    assertReadable(catalog, 2, 11L, otherShuffleMap)
    assertGone(catalog, 1, 11L, failedMap)
    failedTableIds.foreach { tableId =>
      assertThrows[NoSuchElementException](catalog.getShuffleBufferHandle(tableId))
    }
    val (device, metadataOnly) = mappedTableIds(catalog, numAdded)
    assertResult((deviceTableIds(catalog, 1, 10L, sameShuffleMap) ++
      deviceTableIds(catalog, 2, 11L, otherShuffleMap)).toSet)(device)
    assertResult(survivors.count(!_.deviceBacked))(metadataOnly.size)
    assertConsistent(catalog, survivors.size)
  }

  test("a failed writer removes only the output it wrote") {
    val catalog = new ShuffleBufferCatalog()
    val (_, failedTableIds) = failOneWriter(catalog)
    assertOnlySurvivorsRemain(catalog, failedTableIds)
    catalog.unregisterShuffle(1)
    catalog.unregisterShuffle(2)
  }

  test("unregisterShuffle after a failed writer releases every buffer") {
    val deviceBytesBefore = Rmm.getTotalBytesAllocated
    val catalog = new ShuffleBufferCatalog()
    failOneWriter(catalog)
    catalog.unregisterShuffle(1)
    assertReadable(catalog, 2, 11L, otherShuffleMap)
    assertResult(otherShuffleMap.count(_.deviceBacked))(numDeviceHandles)
    assertConsistent(catalog, otherShuffleMap.size)
    catalog.unregisterShuffle(2)
    assert(!catalog.hasActiveShuffle(1) && !catalog.hasActiveShuffle(2))
    assertResult(0)(numDeviceHandles)
    assertResult((Set.empty[Int], Set.empty[Int]))(mappedTableIds(catalog, numAdded))
    assertConsistent(catalog, 0)
    assertResult(deviceBytesBefore)(Rmm.getTotalBytesAllocated)
  }

  test("a failed writer's cleanup after its shuffle was unregistered is harmless") {
    // unregisterShuffle can run while a failed or killed map task of the shuffle is unwinding
    val catalog = new ShuffleBufferCatalog()
    succeed(newWriter(catalog, 2, 11L), otherShuffleMap)
    val failed = newWriter(catalog, 1, 11L)
    assertThrows[IllegalStateException](write(failed, failedMap, thenFail = true))
    catalog.unregisterShuffle(1)
    assert(failed.stop(false).isEmpty)
    assertReadable(catalog, 2, 11L, otherShuffleMap)
    assertResult(otherShuffleMap.count(_.deviceBacked))(numDeviceHandles)
    assertConsistent(catalog, otherShuffleMap.size)
    catalog.unregisterShuffle(2)
    assertResult(0)(numDeviceHandles)
    assertConsistent(catalog, 0)
  }

  test("repeated cleanup of a failed writer is a no-op") {
    val catalog = new ShuffleBufferCatalog()
    val (failed, failedTableIds) = failOneWriter(catalog)
    assert(failed.stop(false).isEmpty)
    assertOnlySurvivorsRemain(catalog, failedTableIds)
    // output cached after the failure, here by a later map of the same shuffle, survives too
    val laterMap = Seq(Input(0, 40, Packed), Input(3, 43, NoColumns))
    succeed(newWriter(catalog, 1, 12L), laterMap)
    assert(failed.stop(false).isEmpty)
    assertReadable(catalog, 1, 12L, laterMap)
    assertReadable(catalog, 1, 10L, sameShuffleMap)
    catalog.unregisterShuffle(1)
    catalog.unregisterShuffle(2)
    assertResult(0)(numDeviceHandles)
  }

  test("attempts that share a map id do not remove each other's output") {
    // With spark.shuffle.useOldFetchProtocol=true Spark uses the partition id as the map id, so
    // every attempt of a partition writes to the same shuffle blocks.
    val catalog = new ShuffleBufferCatalog()
    val committed = Seq(Input(0, 50, Packed), Input(1, 51, Compressed), Input(2, 52, NoColumns))
    succeed(newWriter(catalog, 1, 7L), committed)
    val laterAttempt = newWriter(catalog, 1, 7L)
    assertThrows[IllegalStateException](write(laterAttempt,
      Seq(Input(0, 60, Packed), Input(1, 61, Packed), Input(2, 62, NoColumns)), thenFail = true))
    assert(laterAttempt.stop(false).isEmpty)
    assertReadable(catalog, 1, 7L, committed)

    // a failing attempt that overlaps a running one, as a zombie task of an earlier stage
    // attempt can, leaves the running attempt's output alone. The running attempt writes
    // between the zombie's batches, so the zombie's buffers sit on both sides of its own.
    val running = newWriter(catalog, 2, 7L)
    val runningOutput = Seq(Input(0, 70, Packed), Input(0, 72, Compressed),
      Input(1, 71, NoColumns))
    val zombie = newWriter(catalog, 2, 7L)
    assertThrows[IllegalStateException](write(zombie,
      Seq(Input(0, 80, Packed), Input(0, 82, Packed), Input(1, 81, NoColumns)),
      thenFail = true, afterFirstBatch = () => write(running, runningOutput)))
    assert(zombie.stop(false).isEmpty)
    assert(running.stop(true).isDefined)
    assertReadable(catalog, 2, 7L, runningOutput)
    assertResult((committed ++ runningOutput).count(_.deviceBacked))(numDeviceHandles)
    assertConsistent(catalog, committed.size + runningOutput.size)

    catalog.unregisterShuffle(1)
    catalog.unregisterShuffle(2)
    assertResult(0)(numDeviceHandles)
    assertConsistent(catalog, 0)
  }

  Seq(Packed, Compressed).foreach { kind =>
    test(s"a failed writer leaves nothing behind when closing its $kind input fails") {
      val catalog = new ShuffleBufferCatalog()
      val survivors = Seq(Input(0, 10, Packed), Input(1, 11, Compressed))
      succeed(newWriter(catalog, 1, 10L), survivors)
      // an earlier attempt that shares the map id, as with spark.shuffle.useOldFetchProtocol=true,
      // also owns the block whose add fails
      val committed = Seq(Input(0, 12, Packed))
      succeed(newWriter(catalog, 1, 11L), committed)
      val deviceHandlesBefore = numDeviceHandles
      val deviceBytesBefore = Rmm.getTotalBytesAllocated
      val failed = newWriter(catalog, 1, 11L)
      val cachedInput = Input(1, 31, Compressed)
      val closeFailsInput = Input(0, 30, kind)
      val cached = cachedInput.build()
      val closeFails = closeFailsInput.build()
      val buffer = closeFails.column(0).asInstanceOf[WithTableBuffer].getTableBuffer
      val armed = new AtomicBoolean(true)
      buffer.setEventHandler(new MemoryBuffer.EventHandler {
        override def onClosed(refCount: Int): Unit = {
          if (armed.getAndSet(false)) {
            throw new IllegalStateException("injected input close failure")
          }
        }
      })
      try {
        val e = intercept[IllegalStateException] {
          failed.write(Iterator(1 -> cached, 0 -> closeFails))
        }
        assertResult("injected input close failure")(e.getMessage)
        assert(failed.stop(false).isEmpty)
        assertResult(deviceHandlesBefore, "device buffers left after the cleanup")(
          numDeviceHandles)
        assertResult(0, "references left on the input whose close failed")(buffer.getRefCount)
        assertResult(deviceBytesBefore)(Rmm.getTotalBytesAllocated)
        assertConsistent(catalog, survivors.size + committed.size)
        assertGone(catalog, 1, 11L, Seq(cachedInput))
        assertReadable(catalog, 1, 11L, committed)
        assertReadable(catalog, 1, 10L, survivors)
      } finally {
        // The adds closed both inputs, and the failing close dropped its reference before it
        // threw, so neither input is closed again here.
        buffer.setEventHandler(null)
        catalog.unregisterShuffle(1)
      }
      assertResult(0)(numDeviceHandles)
      assertConsistent(catalog, 0)
    }
  }
}
