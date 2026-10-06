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
import java.util.concurrent.TimeUnit

import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.format.TableMeta
import com.nvidia.spark.rapids.shuffle.RapidsShuffleTestHelper
import com.nvidia.spark.rapids.spill.SpillFramework
import org.scalatest.BeforeAndAfterEach
import org.scalatest.concurrent.Eventually.eventually
import org.scalatest.concurrent.PatienceConfiguration.Timeout
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.time.{Seconds, Span}
import org.scalatestplus.mockito.MockitoSugar

import org.apache.spark.SparkConf
import org.apache.spark.sql.types.{DataType, IntegerType}
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
    shuffleCatalog.addContiguousTable(ShuffleBlockId(1, 1L, 1), ct, -1)
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
    assertThrows[ClassCastException](shuffleCatalog.addCompressedBatch(block, packed, -1))
    assertResult((0, 0, 0))(shuffleCatalog.bookkeepingSizes)
    assertResult(0)(SpillFramework.stores.deviceStore.numHandles)
    assertThrows[NoSuchElementException](shuffleCatalog.blockIdToMetas(block))
    shuffleCatalog.unregisterShuffle(1)
  }

  /** A block's list of buffer ids, which the catalog keeps private and locks for every change. */
  private def blockBufferIds(catalog: ShuffleBufferCatalog, block: ShuffleBlockId): AnyRef = {
    val field = classOf[ShuffleBufferCatalog].getDeclaredField("activeShuffles")
    field.setAccessible(true)
    val shuffles = field.get(catalog).asInstanceOf[
      java.util.Map[Any, java.util.Map[ShuffleBlockId, AnyRef]]]
    shuffles.get(block.shuffleId).get(block)
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

  test("get a columnar batch iterator from catalog") {
    val shuffleCatalog = new ShuffleBufferCatalog()
    shuffleCatalog.registerShuffle(1)
    // add metadata only table
    val tableMeta = RapidsShuffleTestHelper.mockTableMeta(0)
    shuffleCatalog.addDegenerateRapidsBuffer(ShuffleBlockId(1, 1L, 1), tableMeta)
    val ct = RapidsShuffleTestHelper.buildContiguousTable(1000)
    shuffleCatalog.addContiguousTable(ShuffleBlockId(1, 1L, 1), ct, -1)
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
}
