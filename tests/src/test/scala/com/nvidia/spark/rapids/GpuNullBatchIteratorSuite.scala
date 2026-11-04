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

import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.rapids.execution.TrampolineUtil
import org.apache.spark.sql.rapids.metrics.source.MockTaskContext
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructType}

class GpuNullBatchIteratorSuite extends AnyFunSuite with BeforeAndAfterEach {
  private val longs = new StructType().add("a", LongType).add("b", LongType)

  override def afterEach(): Unit = {
    try {
      TrampolineUtil.unsetTaskContext()
      GpuSemaphore.shutdown()
      SparkSession.getActiveSession.foreach(_.stop())
      SparkSession.clearActiveSession()
    } finally {
      super.afterEach()
    }
  }

  test("the row cap is the lesser of the row limit and the rows fitting the byte target") {
    val bytesPerRow = GpuBatchUtils.estimateGpuMemory(longs, 1 << 20) / (1 << 20).toDouble
    val rowsInTarget = (1000000 / bytesPerRow).toLong
    assert(GpuNullBatchIterator.maxRows(longs, Int.MaxValue, 1000000) == rowsInTarget)
    assert(GpuNullBatchIterator.maxRows(longs, 100, 1000000) == 100)
    assert(GpuNullBatchIterator.maxRows(longs, 100, Long.MaxValue / 4) == 100)
  }

  test("the row cap is at least one row") {
    assert(GpuNullBatchIterator.maxRows(longs, Int.MaxValue, 1) == 1)
    assert(GpuNullBatchIterator.maxRows(longs, Int.MaxValue, 0) == 1)
  }

  test("a schema with no bytes is only limited by the row limit") {
    assert(GpuNullBatchIterator.maxRows(new StructType(), 77, 1) == 77)
    assert(GpuNullBatchIterator.maxRows(new StructType(), Int.MaxValue, 1) == Int.MaxValue)
  }

  test("wider and partition columns lower the row cap") {
    val withPartition = longs.add("p", IntegerType)
    val withString = longs.add("s", StringType)
    val base = GpuNullBatchIterator.maxRows(longs, Int.MaxValue, 1 << 20)
    assert(GpuNullBatchIterator.maxRows(withPartition, Int.MaxValue, 1 << 20) < base)
    assert(GpuNullBatchIterator.maxRows(withString, Int.MaxValue, 1 << 20) < base)
  }

  private def sizes(total: Long, cap: Int): List[Int] =
    GpuNullBatchIterator(new StructType(), total, cap).map(_.numRows()).toList

  test("batches cover every row and none exceeds the cap") {
    assert(sizes(0, 10).isEmpty)
    assert(sizes(5, 10) == List(5))
    assert(sizes(10, 10) == List(10))
    assert(sizes(25, 10) == List(10, 10, 5))
    assert(sizes(1, 1) == List(1))
  }

  test("a row count over Int.MaxValue is streamed lazily") {
    val total = Int.MaxValue.toLong + 5
    val iter = GpuNullBatchIterator(new StructType(), total, Int.MaxValue)
    assert(iter.next().numRows() == Int.MaxValue)
    assert(iter.next().numRows() == 5)
    assert(!iter.hasNext)
    val big = GpuNullBatchIterator(new StructType(), 3L * Int.MaxValue, 1 << 20)
    // Taking one batch of a huge split neither allocates the rest nor walks the whole range.
    assert(big.next().numRows() == (1 << 20))
    assert(big.hasNext)
  }

  test("next on an exhausted iterator fails") {
    val iter = GpuNullBatchIterator(new StructType(), 1, 10)
    iter.next()
    assertThrows[NoSuchElementException](iter.next())
  }

  test("every batch holds the GPU semaphore, even with no columns to build") {
    // The semaphore needs a SparkEnv to initialize, and one task at a time can hold it. Another
    // suite in this JVM may have initialized it with other limits.
    GpuSemaphore.shutdown()
    SparkSession.getActiveSession.foreach(_.stop())
    SparkSession.clearActiveSession()
    SparkSession.builder.appName("nullBatchIteratorTests").master("local[1]").getOrCreate()
    GpuSemaphore.initialize(1)
    GpuDeviceManager.setRmmTaskInitEnabled(false)
    val other = new MockTaskContext(2, 0)
    TrampolineUtil.setTaskContext(new MockTaskContext(1, 0))
    // A partitioned count(*) reads no columns, but partition columns are added on the GPU
    val iter = GpuNullBatchIterator(new StructType(), 10, 10)
    assert(GpuSemaphore.tryAcquire(other) == SemaphoreAcquired)
    GpuSemaphore.releaseIfNecessary(other)
    iter.next().close()
    assert(GpuSemaphore.tryAcquire(other).isInstanceOf[AcquireFailed])
  }
}
