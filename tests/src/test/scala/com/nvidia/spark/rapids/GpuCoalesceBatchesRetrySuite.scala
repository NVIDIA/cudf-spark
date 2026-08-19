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

package com.nvidia.spark.rapids

import scala.collection.mutable.ArrayBuffer

import ai.rapids.cudf.Table
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.jni.{GpuSplitAndRetryOOM, RmmSpark}
import org.mockito.Mockito._
import org.scalatestplus.mockito.MockitoSugar

import org.apache.spark.sql.types.{DataType, LongType, StructField, StructType}
import org.apache.spark.sql.vectorized.ColumnarBatch

class GpuCoalesceBatchesRetrySuite
  extends RmmSparkRetrySuiteBase
    with MockitoSugar {

  private def buildBatchesToCoalesce(): Seq[ColumnarBatch] = {
    (0 until 10).map { _ =>
      withResource(new Table.TestBuilder()
          .column(1L.asInstanceOf[java.lang.Long])
          .build()) { tbl =>
        spy(GpuColumnVector.from(tbl, Seq(LongType).toArray[DataType]))
      }
    }
  }

  private def buildHostBatchesToCoalesce(): Seq[ColumnarBatch] = {
    buildBatchesToCoalesce().map { dcb =>
      withResource(dcb) { _ =>
        val hostColumns = (0 until dcb.numCols()).map(
          i => dcb.column(i).asInstanceOf[GpuColumnVector].copyToHost())
        spy(new ColumnarBatch(hostColumns.toArray, dcb.numRows()))
      }
    }
  }

  def getIters(
      goal: CoalesceSizeGoal = TargetSize(1024),
      injectRetry: Int = 0,
      injectSplitAndRetry: Int = 0,
      mockInjectSplitAndRetry: Boolean = false): Seq[Iterator[ColumnarBatch]] = {
    val ab = new ArrayBuffer[Iterator[ColumnarBatch]]()
    ab.append(new InjectableCoalesceIterator(
      buildBatchesToCoalesce(),
      goal,
      injectRetry,
      injectSplitAndRetry,
      mockInjectSplitAndRetry))
    ab.append(new InjectableCompressionAwareCoalesceIterator(
      buildBatchesToCoalesce(),
      goal,
      injectRetry,
      injectSplitAndRetry,
      mockInjectSplitAndRetry))
    ab.toSeq
  }

  def getHostIter(
      injectRetry: Int = 0,
      goal: CoalesceSizeGoal = TargetSize(1024)): Iterator[ColumnarBatch] = {
    new InjectableHostToGpuCoalesceIterator(
      buildHostBatchesToCoalesce(),
      goal = goal,
      injectRetry)
  }

  test("coalesce gpu batches without failures") {
    val iters = getIters()
    iters.foreach { iter =>
      withResource(iter.next()) { coalesced =>
        assertResult(10)(coalesced.numRows())
        assertResult(true)(GpuColumnVector.isTaggedAsFinalBatch(coalesced))
      }
    }
  }

  test("coalesce host batches without failures") {
    val iter = getHostIter()
    withResource(iter.next()) { coalesced =>
      assertResult(10)(coalesced.numRows())
      assertResult(true)(GpuColumnVector.isTaggedAsFinalBatch(coalesced))
    }

    // ensure that this iterator _did not close_ the incoming batches
    // as that is the semantics of the HostToGpuCoalesceIterator
    val allBatches = iter.asInstanceOf[CoalesceIteratorMocks].getBatches()
    assertResult(10)(allBatches.length)
    allBatches.foreach { x =>
      verify(x, times(0)).close()
    }
    allBatches.foreach(_.close())
  }

  test("coalesce gpu batches with retry") {
    val iters = getIters(injectRetry = 1)
    iters.foreach { iter =>
      withResource(iter.next()) { coalesced =>
        assertResult(10)(coalesced.numRows())
        assertResult(true)(GpuColumnVector.isTaggedAsFinalBatch(coalesced))
      }
    }
  }

  test("coalesce gpu batches with retry host iter") {
    val iter = getHostIter(injectRetry = 1)
    withResource(iter.next()) { coalesced =>
      assertResult(10)(coalesced.numRows())
    }
    // ensure that this iterator _did not close_ the incoming batches
    // as that is the semantics of the HostToGpuCoalesceIterator
    val allBatches = iter.asInstanceOf[CoalesceIteratorMocks].getBatches()
    assertResult(10)(allBatches.length)
    allBatches.foreach { x =>
      verify(x, times(0)).close()
    }
    allBatches.foreach(_.close())
  }

  test("coalesce gpu batches splits in half with GpuSplitAndRetryOOM") {
    val iters = getIters(injectSplitAndRetry = 1)
    iters.foreach { iter =>
      withResource(iter.next()) { coalesced =>
        assertResult(5)(coalesced.numRows())
        assertResult(false)(GpuColumnVector.isTaggedAsFinalBatch(coalesced))
      }
      withResource(iter.next()) { coalesced =>
        assertResult(5)(coalesced.numRows())
        assertResult(true)(GpuColumnVector.isTaggedAsFinalBatch(coalesced))
      }
      assertResult(false)(iter.hasNext)
    }
  }

  test("coalesce gpu batches splits in quarters with GpuSplitAndRetryOOM") {
    val iters = getIters(injectSplitAndRetry = 2)
    iters.foreach { iter =>
      withResource(iter.next()) { coalesced =>
        assertResult(2)(coalesced.numRows())
        assertResult(false)(GpuColumnVector.isTaggedAsFinalBatch(coalesced))
      }
      withResource(iter.next()) { coalesced =>
        assertResult(3)(coalesced.numRows())
        assertResult(false)(GpuColumnVector.isTaggedAsFinalBatch(coalesced))
      }
      withResource(iter.next()) { coalesced =>
        assertResult(5)(coalesced.numRows())
        assertResult(true)(GpuColumnVector.isTaggedAsFinalBatch(coalesced))
      }
      assertResult(false)(iter.hasNext)
    }
  }

  test("coalesce gpu batches fails with OOM if it cannot split enough") {
    val iters = getIters(mockInjectSplitAndRetry = true)
    iters.foreach { iter =>
      assertThrows[GpuSplitAndRetryOOM] {
        iter.next() // throws
      }
      val batches = iter.asInstanceOf[CoalesceIteratorMocks].getBatches()
      assertResult(10)(batches.length)
      batches.foreach(b =>
        verify(b, times(1)).close()
      )
    }
  }

  test("coalesce gpu batches with retry with non-splittable goal") {
    val iters = getIters(injectRetry = 1, goal = RequireSingleBatch)
    iters.foreach { iter =>
      withResource(iter.next()) { coalesced =>
        assertResult(10)(coalesced.numRows())
      }
    }
  }

  test("coalesce gpu batches throws if GpuSplitAndRetryOOM with non-splittable goal") {
    val iters = getIters(injectSplitAndRetry = 1, goal = RequireSingleBatch)
    iters.foreach { iter =>
      assertThrows[GpuSplitAndRetryOOM] {
        iter.next()
      }
      val batches = iter.asInstanceOf[CoalesceIteratorMocks].getBatches()
      assertResult(10)(batches.length)
      batches.foreach(b =>
        GpuColumnVector.extractBases(b).forall(_.getRefCount == 0)
      )
    }
  }

  class SpillableColumnarBatchThatThrows(batch: ColumnarBatch)
      extends SpillableColumnarBatch {
    var refCount = 1
    override def numRows(): Int = 0
    override def setSpillPriority(priority: Long): Unit = {}
    override def getColumnarBatch(): ColumnarBatch = {
      throw new GpuSplitAndRetryOOM()
    }
    override def sizeInBytes: Long = 0
    override def dataTypes: Array[DataType] = Array.empty
    override def close(): Unit = {
      if (refCount <= 0) {
        throw new IllegalStateException("double free")
      }
      refCount -= 1
      if (refCount == 0) {
        batch.close()
      }
    }

    override def incRefCount(): SpillableColumnarBatch = {
      if (refCount <= 0) {
        throw new IllegalStateException("Use after free")
      }
      refCount += 1
      this
    }
  }

  trait CoalesceIteratorMocks {
    def getBatches(): Seq[ColumnarBatch]

    def injectError(injectRetry: Int, injectSplitAndRetry: Int): Unit = {
      if (injectRetry > 0) {
        RmmSpark.forceRetryOOM(RmmSpark.getCurrentThreadId, injectRetry,
          RmmSpark.OomInjectionType.GPU.ordinal, 0)
      }
      if (injectSplitAndRetry > 0) {
        RmmSpark.forceSplitAndRetryOOM(RmmSpark.getCurrentThreadId, injectSplitAndRetry,
          RmmSpark.OomInjectionType.GPU.ordinal, 0)
      }
    }

    def getBatchToConcat(
        mockInjectSplitAndRetry: Boolean,
        batch: ColumnarBatch): SpillableColumnarBatch = {
      val spillableSpy = if (mockInjectSplitAndRetry) {
        spy(
          new SpillableColumnarBatchThatThrows(batch))
      } else {
        spy(SpillableColumnarBatch(
          batch,
          SpillPriorities.ACTIVE_BATCHING_PRIORITY))
      }
      spillableSpy
    }
  }

  class InjectableHostToGpuCoalesceIterator(
      batchesToConcat: Seq[ColumnarBatch],
      goal: CoalesceSizeGoal,
      injectRetry: Int = 0)
    extends HostToGpuCoalesceIterator(
      batchesToConcat.iterator,
      goal,
      StructType(Seq(StructField("col0", LongType, nullable = true))),
      NoopMetric,
      NoopMetric,
      NoopMetric,
      NoopMetric,
      NoopMetric,
      NoopMetric,
      NoopMetric,
      NoopMetric,
      "test",
      false)
    with CoalesceIteratorMocks {

    override def populateCandidateBatches(): Boolean = {
      val lastBatchTag = super.populateCandidateBatches()
      injectError(injectRetry, injectSplitAndRetry = 0)
      lastBatchTag
    }

    override def getBatches(): Seq[ColumnarBatch] = batchesToConcat
  }

  class InjectableCompressionAwareCoalesceIterator(
      batchesToConcat: Seq[ColumnarBatch],
      goal: CoalesceSizeGoal,
      injectRetry: Int = 0,
      injectSplitAndRetry: Int = 0,
      mockInjectSplitAndRetry: Boolean = false)
      extends GpuCompressionAwareCoalesceIterator(
        batchesToConcat.iterator,
        Seq(LongType).toArray,
        goal,
        maxDecompressBatchMemory=10240,
        NoopMetric,
        NoopMetric,
        NoopMetric,
        NoopMetric,
        NoopMetric,
        NoopMetric,
        NoopMetric,
        "test",
        TableCompressionCodecConfig(1024, 1024)) with CoalesceIteratorMocks {
    override def populateCandidateBatches(): Boolean = {
      val lastBatchTag = super.populateCandidateBatches()
      injectError(injectRetry, injectSplitAndRetry)
      lastBatchTag
    }

    override def addBatchToConcat(batch: ColumnarBatch): Unit = {
      batches.append(getBatchToConcat(mockInjectSplitAndRetry, batch))
    }

    override def getBatches(): Seq[ColumnarBatch] = batchesToConcat
  }

  class InjectableCoalesceIterator(
      batchesToConcat: Seq[ColumnarBatch],
      goal: CoalesceSizeGoal,
      injectRetry: Int = 0,
      injectSplitAndRetry: Int = 0,
      mockInjectSplitAndRetry: Boolean = false)
      extends GpuCoalesceIterator (
        batchesToConcat.iterator,
        Seq(LongType).toArray,
        goal,
        NoopMetric,
        NoopMetric,
        NoopMetric,
        NoopMetric,
        NoopMetric,
        NoopMetric,
        NoopMetric,
        "test") with CoalesceIteratorMocks {
    override def populateCandidateBatches(): Boolean = {
      val lastBatchTag = super.populateCandidateBatches()
      injectError(injectRetry, injectSplitAndRetry)
      lastBatchTag
    }

    override def addBatchToConcat(batch: ColumnarBatch): Unit = {
      batches.append(getBatchToConcat(mockInjectSplitAndRetry, batch))
    }

    override def getBatches(): Seq[ColumnarBatch] = batchesToConcat
  }
}
