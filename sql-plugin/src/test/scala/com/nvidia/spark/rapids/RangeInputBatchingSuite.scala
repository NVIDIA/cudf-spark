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

import scala.collection.mutable.ArrayBuffer

import ai.rapids.cudf.Table
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.vectorized.ColumnarBatch

class RangeInputBatchingSuite extends AnyFunSuite {
  private class TestCoalesceIterator(input: Iterator[ColumnarBatch])
      extends AbstractGpuCoalesceIterator(
        input,
        TargetSize(Long.MaxValue),
        NoopMetric,
        NoopMetric,
        NoopMetric,
        NoopMetric,
        NoopMetric,
        NoopMetric,
        NoopMetric,
        "test") {
    private val candidates = new ArrayBuffer[ColumnarBatch]
    private var onDeck: ColumnarBatch = _

    def collectCandidates(): Boolean = populateCandidateBatches()
    def candidateCount: Int = candidates.size
    def closeCandidates(): Unit = candidates.foreach(_.close())

    override protected def hasOnDeck: Boolean = onDeck != null
    override protected def saveOnDeck(batch: ColumnarBatch): Unit = onDeck = batch
    override protected def clearOnDeck(): Unit = {
      if (onDeck != null) onDeck.close()
      onDeck = null
    }
    override protected def popOnDeck(): ColumnarBatch = {
      val result = onDeck
      onDeck = null
      result
    }
    override def initNewBatch(batch: ColumnarBatch): Unit = candidates.clear()
    override def addBatchToConcat(batch: ColumnarBatch): Unit = candidates += batch
    override def hasAnyToConcat: Boolean = candidates.nonEmpty
    override def concatAllAndPutOnGPU(): ColumnarBatch = candidates.remove(0)
    override protected val supportsRetryIterator: Boolean = false
    override def getCoalesceRetryIterator: Iterator[ColumnarBatch] = Iterator.empty
    override def cleanupConcatIsDone(): Unit = candidates.clear()
  }

  test("range input does not read ahead beyond the first candidate batch") {
    var reads = 0
    val input = Seq(
      new ColumnarBatch(Array.empty, 1),
      new ColumnarBatch(Array.empty, 1),
      new ColumnarBatch(Array.empty, 1)).iterator.map { batch =>
      reads += 1
      batch
    }
    val coalesce = new TestCoalesceIterator(input)

    try {
      val isLast = RangeInputBatching.withRangeInput {
        assert(coalesce.hasNext)
        coalesce.collectCandidates()
      }
      assert(!isLast)
      assert(reads == 1)
      assert(coalesce.candidateCount == 1)
    } finally {
      coalesce.closeCandidates()
      input.foreach(_.close())
    }
  }

  test("normal input retains size-based coalescing behavior") {
    var reads = 0
    val input = Seq(
      new ColumnarBatch(Array.empty, 1),
      new ColumnarBatch(Array.empty, 1),
      new ColumnarBatch(Array.empty, 1)).iterator.map { batch =>
      reads += 1
      batch
    }
    val coalesce = new TestCoalesceIterator(input)

    try {
      assert(coalesce.hasNext)
      assert(coalesce.collectCandidates())
      assert(reads == 3)
      assert(coalesce.candidateCount == 3)
    } finally {
      coalesce.closeCandidates()
      input.foreach(_.close())
    }
  }

  test("range input marker restores its scope after nesting and exceptions") {
    assert(!RangeInputBatching.isActive)
    intercept[RuntimeException] {
      RangeInputBatching.withRangeInput {
        assert(RangeInputBatching.isActive)
        RangeInputBatching.withRangeInput(assert(RangeInputBatching.isActive))
        assert(RangeInputBatching.isActive)
        throw new RuntimeException("test")
      }
    }
    assert(!RangeInputBatching.isActive)
  }

  test("range producer is closed when it is exhausted") {
    var closed = false
    val producer = new EmptyGpuDataProducer[Table] {
      override def canReleaseSemaphoreBetweenBatches: Boolean = true
      override def close(): Unit = closed = true
    }
    val iter = RangeInputBatching.withRangeInput {
      CachedGpuBatchIterator(producer, Array.empty[DataType])
    }

    assert(!closed)
    assert(!iter.hasNext)
    assert(closed)
  }

  test("range input eagerly caches a producer that cannot release the semaphore") {
    var closed = false
    val producer = new EmptyGpuDataProducer[Table] {
      override def close(): Unit = closed = true
    }
    val iter = RangeInputBatching.withRangeInput {
      CachedGpuBatchIterator(producer, Array.empty[DataType])
    }

    assert(closed)
    assert(!iter.hasNext)
  }
}
