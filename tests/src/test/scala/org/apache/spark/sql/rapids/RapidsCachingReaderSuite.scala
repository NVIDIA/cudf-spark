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

import java.lang.management.ManagementFactory
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference

import scala.collection
import scala.util.{Failure, Try}

import com.nvidia.spark.rapids.{RapidsConf, RmmSparkRetrySuiteBase, ShuffleBufferCatalog}
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.shuffle.RapidsShuffleTestHelper
import com.nvidia.spark.rapids.spill.SpillableDeviceBufferHandle
import org.scalatest.concurrent.Eventually.eventually
import org.scalatest.concurrent.PatienceConfiguration.Timeout
import org.scalatest.time.{Seconds, Span}
import org.scalatestplus.mockito.MockitoSugar

import org.apache.spark.{FetchFailed, SparkConf, TaskContext}
import org.apache.spark.shuffle.{FetchFailedException, ShuffleReadMetricsReporter}
import org.apache.spark.shuffle.rapids.ShuffleOutputGoneException
import org.apache.spark.sql.rapids.execution.TrampolineUtil
import org.apache.spark.sql.types.{DataType, IntegerType}
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.storage.{BlockId, BlockManagerId, ShuffleBlockBatchId, ShuffleBlockId}

/** RapidsCachingReader reading blocks cached on its own executor, from a real catalog. */
class RapidsCachingReaderSuite extends RmmSparkRetrySuiteBase with MockitoSugar {
  private val sparkTypes: Array[DataType] = Array(IntegerType)
  private val localId = BlockManagerId("1", "host-1", 1234)
  // where the MapStatus of the map says its output is: this executor, as its shuffle server
  private val mapLocation = BlockManagerId("1", "host-1", 1234, Some("rapids=5678"))
  private val block = ShuffleBlockId(1, 7L, 2)
  private val mapIndex = 3

  /** Runs `body` with a catalog that has registered the block's shuffle, unregistered after. */
  private def withCatalog(body: ShuffleBufferCatalog => Unit): Unit = {
    val catalog = new ShuffleBufferCatalog()
    catalog.registerShuffle(block.shuffleId)
    try {
      body(catalog)
    } finally {
      catalog.unregisterShuffle(block.shuffleId)
    }
  }

  private def read(
      catalog: ShuffleBufferCatalog,
      context: TaskContext = TaskContext.empty(),
      blockId: BlockId = block): Iterator[Product2[Int, ColumnarBatch]] = {
    val blocks: collection.Seq[(BlockId, Long, Int)] = Seq((blockId, 100L, mapIndex))
    new RapidsCachingReader[Int, ColumnarBatch](new RapidsConf(new SparkConf), localId,
      Iterator((mapLocation, blocks)), context, mock[ShuffleReadMetricsReporter],
      None, catalog, sparkTypes).read()
  }

  /** Consumes `it`, closing each batch, and returns the fetch failure it must end in. */
  private def fetchFailure(it: Iterator[Product2[Int, ColumnarBatch]]): FetchFailedException =
    intercept[FetchFailedException] {
      it.foreach(record => record._2.close())
    }

  /** Asserts that `e` names `gone` and comes from the catalog finding its output gone. */
  private def assertOutputGone(e: FetchFailedException, gone: ShuffleBlockId = block): Unit = {
    // a reader that converted every NoSuchElementException would pass without this
    assert(e.getCause.isInstanceOf[ShuffleOutputGoneException], e.getCause)
    val failure = e.toTaskFailedReason.asInstanceOf[FetchFailed]
    assertResult(mapLocation)(failure.bmAddress)
    assertResult((gone.shuffleId, gone.mapId, mapIndex, gone.reduceId))(
      (failure.shuffleId, failure.mapId, failure.mapIndex, failure.reduceId))
  }

  test("a local read of a block whose cached output is gone ends in a fetch failure") {
    withCatalog { catalog =>
      val ids = Seq(
        catalog.addContiguousTable(block, RapidsShuffleTestHelper.buildContiguousTable(10), -1))
      // as a writer's cleanup removes what it added
      catalog.removeCachedHandles(ids)
      assertOutputGone(fetchFailure(read(catalog)))
    }
  }

  test("a local fetch failure is recorded in the task's context") {
    withCatalog { catalog =>
      val ids = Seq(
        catalog.addContiguousTable(block, RapidsShuffleTestHelper.buildContiguousTable(10), -1))
      catalog.removeCachedHandles(ids)
      val context = TaskContext.empty()
      // Spark reports a fetch failure only when the context of the thread that built it has it
      TrampolineUtil.setTaskContext(context)
      try {
        val e = fetchFailure(read(catalog, context))
        assert(context.fetchFailed.exists(_ eq e), context.fetchFailed)
      } finally {
        TrampolineUtil.unsetTaskContext()
      }
    }
  }

  test("a local read of a shuffle this executor unregistered ends in a fetch failure") {
    withCatalog { catalog =>
      catalog.addContiguousTable(block, RapidsShuffleTestHelper.buildContiguousTable(10), -1)
      catalog.unregisterShuffle(block.shuffleId)
      assertOutputGone(fetchFailure(read(catalog)))
    }
  }

  test("a local read whose next buffer is removed after the block was looked up ends in a " +
      "fetch failure") {
    withCatalog { catalog =>
      catalog.addContiguousTable(block, RapidsShuffleTestHelper.buildContiguousTable(10), -1)
      val second =
        catalog.addContiguousTable(block, RapidsShuffleTestHelper.buildContiguousTable(20), -1)
      val it = read(catalog)
      // the first batch looks the block up, and with it both of its buffers
      withResource(it.next()._2) { first =>
        assertResult(10)(first.numRows())
      }
      catalog.removeCachedHandles(Seq(second))
      assertOutputGone(fetchFailure(it))
    }
  }

  test("a local read of a batch block ends in a fetch failure naming the reduce id whose " +
      "output is gone") {
    withCatalog { catalog =>
      catalog.addContiguousTable(block, RapidsShuffleTestHelper.buildContiguousTable(10), -1)
      val gone = ShuffleBlockId(block.shuffleId, block.mapId, block.reduceId + 1)
      catalog.removeCachedHandles(Seq(
        catalog.addContiguousTable(gone, RapidsShuffleTestHelper.buildContiguousTable(20), -1)))
      // Spark's end is exclusive, so this batch is the two reduce ids above
      val batch =
        ShuffleBlockBatchId(block.shuffleId, block.mapId, block.reduceId, gone.reduceId + 1)
      val it = read(catalog, blockId = batch)
      withResource(it.next()._2) { first =>
        assertResult(10)(first.numRows())
      }
      assertOutputGone(fetchFailure(it), gone)
    }
  }

  test("a local read whose buffer is removed between its lookup and its materialize ends in " +
      "a fetch failure") {
    val awaitSeconds = 30
    withCatalog { catalog =>
      val id =
        catalog.addContiguousTable(block, RapidsShuffleTestHelper.buildContiguousTable(10), -1)
      // the catalog still owns the buffer, so this lookup is not closed
      val handle = catalog.getShuffleBufferHandle(id.tableId).spillable
      val outcome = new AtomicReference[Try[Unit]]()
      val reader = new Thread(
        () => outcome.set(Try(read(catalog).foreach(record => record._2.close()))),
        "cached-block-reader")
      reader.setDaemon(true)
      val threads = ManagementFactory.getThreadMXBean
      // materialize checks for a close under the handle's monitor, so holding it stops the
      // read after it has looked the buffer up
      handle.synchronized {
        reader.start()
        eventually(Timeout(Span(awaitSeconds, Seconds))) {
          val info = threads.getThreadInfo(reader.getId)
          assert(info != null && info.getThreadState == Thread.State.BLOCKED &&
            info.getLockInfo.getIdentityHashCode == System.identityHashCode(handle),
            "the read never blocked on the buffer's monitor")
        }
        catalog.unregisterShuffle(block.shuffleId)
      }
      reader.join(TimeUnit.SECONDS.toMillis(awaitSeconds))
      assert(!reader.isAlive)
      outcome.get match {
        case Failure(e: FetchFailedException) =>
          assertOutputGone(e)
          // only the catch around materialize gives the gone output a cause, so a read that
          // blocked on the monitor anywhere before materialize fails here
          assert(e.getCause.getCause.isInstanceOf[IllegalStateException], e.getCause.getCause)
        case other => fail(s"the read ended in $other")
      }
    }
  }

  test("a local read of a buffer closed while the catalog still holds it fails with the " +
      "original error") {
    withCatalog { catalog =>
      val id =
        catalog.addContiguousTable(block, RapidsShuffleTestHelper.buildContiguousTable(10), -1)
      // as executor shutdown closes the handles and leaves the catalog's entries in place
      catalog.getShuffleBufferHandle(id.tableId).spillable.close()
      val e = intercept[IllegalStateException] {
        read(catalog).foreach(record => record._2.close())
      }
      val top = e.getStackTrace.head
      assertResult((classOf[SpillableDeviceBufferHandle].getName, "materialize"))(
        (top.getClassName, top.getMethodName))
    }
  }

  test("a NoSuchElementException from the catalog's iterator surfaces unchanged") {
    val error = new NoSuchElementException("not a lookup of removed output")
    // the call that opens the block, then the iterator it returns
    Seq("getColumnarBatchIterator", "hasNext", "next").foreach { thrower =>
      val catalog = new ShuffleBufferCatalog() {
        override def getColumnarBatchIterator(
            blockId: ShuffleBlockId,
            sparkTypes: Array[DataType]): Iterator[ColumnarBatch] = {
          if (thrower == "getColumnarBatchIterator") {
            throw error
          }
          new Iterator[ColumnarBatch] {
            override def hasNext: Boolean = if (thrower == "hasNext") throw error else true
            override def next(): ColumnarBatch = throw error
          }
        }
      }
      val e = intercept[NoSuchElementException] {
        read(catalog).foreach(record => record._2.close())
      }
      assert(e eq error, s"$thrower threw, and the read ended in $e")
    }
  }
}
