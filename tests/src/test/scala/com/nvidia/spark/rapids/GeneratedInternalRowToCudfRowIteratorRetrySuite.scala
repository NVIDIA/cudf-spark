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

import ai.rapids.cudf.{HostMemoryBuffer, Table}
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.jni.{GpuSplitAndRetryOOM, RmmSpark}
import com.nvidia.spark.rapids.spill.{SpillableColumnarBatchHandle, SpillableDeviceStore, SpillFramework}
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{doAnswer, spy}
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatestplus.mockito.MockitoSugar

import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.types.{DataType, LongType}
import org.apache.spark.sql.vectorized.ColumnarBatch

class GeneratedInternalRowToCudfRowIteratorRetrySuite
    extends RmmSparkRetrySuiteBase
        with MockitoSugar {
  private def buildBatch(): ColumnarBatch = {
    val reductionTable = new Table.TestBuilder()
        .column(5L, null.asInstanceOf[java.lang.Long], 3L, 1L)
        .build()
    withResource(reductionTable) { tbl =>
      GpuColumnVector.from(tbl, Seq(LongType).toArray[DataType])
    }
  }

  override def beforeEach(): Unit = {
    // some tests in this suite will want to perform `verify` calls on the device store
    // so we close it and create a spy around one.
    super.beforeEach()
    SpillFramework.storesInternal.deviceStore.close()
    SpillFramework.storesInternal.deviceStore = spy(new SpillableDeviceStore)
  }

  private def getAndResetNumRetryThrowCurrentTask: Int = {
    // taskId 1 was associated with the current thread in RmmSparkRetrySuiteBase
    RmmSpark.getAndResetNumRetryThrow(/*taskId*/ 1)
  }

  test("a retry when copying to device is handled") {
    val batch = buildBatch()
    val batchIter = Seq(batch).iterator
    withResource(new ColumnarToRowIterator(batchIter, NoopMetric, NoopMetric, NoopMetric,
      NoopMetric)) { ctriter =>
      val schema = Array(AttributeReference("longcol", LongType)().toAttribute)
      val myIter = GeneratedInternalRowToCudfRowIterator(
        ctriter, schema, TargetSize(Int.MaxValue),
        NoopMetric, NoopMetric, NoopMetric, NoopMetric, NoopMetric)
      // this forces a retry on the copy of the host column to a device column
      RmmSpark.forceRetryOOM(RmmSpark.getCurrentThreadId, 1,
        RmmSpark.OomInjectionType.GPU.ordinal, 0)
      withResource(myIter.next()) { devBatch =>
        withResource(buildBatch()) { expected =>
          TestUtils.compareBatches(expected, devBatch)
        }
      }
      assert(!GpuColumnVector.extractBases(batch).exists(_.getRefCount > 0))
      assert(!myIter.hasNext)
      assertResult(0)(SpillFramework.stores.deviceStore.spill(1))
    }
  }

  test("a retry when converting to a table is handled") {
    val batch = buildBatch()
    val batchIter = Seq(batch).iterator
    doAnswer(new Answer[Boolean]() {
      override def answer(invocation: InvocationOnMock): Boolean = {
        invocation.callRealMethod()
        // we mock things this way due to code generation issues with mockito.
        // when we add a table we have
        RmmSpark.forceRetryOOM(RmmSpark.getCurrentThreadId, 3,
          RmmSpark.OomInjectionType.GPU.ordinal, 0)
        true
      }
    }).when(SpillFramework.stores.deviceStore)
      .track(any())

    withResource(new ColumnarToRowIterator(batchIter, NoopMetric, NoopMetric, NoopMetric,
      NoopMetric)) { ctriter =>
      val schema = Array(AttributeReference("longcol", LongType)().toAttribute)
      val myIter = spy(GeneratedInternalRowToCudfRowIterator(
        ctriter, schema, TargetSize(Int.MaxValue),
        NoopMetric, NoopMetric, NoopMetric, NoopMetric, NoopMetric))
      RmmSpark.forceRetryOOM(RmmSpark.getCurrentThreadId, 2,
        RmmSpark.OomInjectionType.GPU.ordinal, 0)
      assertResult(0)(getAndResetNumRetryThrowCurrentTask)
      withResource(myIter.next()) { devBatch =>
        withResource(buildBatch()) { expected =>
          TestUtils.compareBatches(expected, devBatch)
        }
      }
      assertResult(6)(getAndResetNumRetryThrowCurrentTask)
      assert(!myIter.hasNext)
      assertResult(0)(SpillFramework.stores.deviceStore.spill(1))
    }
  }

  test("spilling the device column of rows works") {
    val batch = buildBatch()
    val batchIter = Seq(batch).iterator
    doAnswer(new Answer[Boolean]() {
      override def answer(invocation: InvocationOnMock): Boolean = {
        val handle = invocation.getArgument(0).asInstanceOf[SpillableColumnarBatchHandle]
        // we mock things this way due to code generation issues with mockito.
        // when we add a table we have
        RmmSpark.forceRetryOOM(RmmSpark.getCurrentThreadId, 3,
          RmmSpark.OomInjectionType.GPU.ordinal, 0)
        // at this point we have created a buffer in the Spill Framework
        // lets spill it
        SpillFramework.stores.deviceStore.spill(handle.approxSizeInBytes)
        true
      }
    }).when(SpillFramework.stores.deviceStore)
        .track(any())

    withResource(new ColumnarToRowIterator(batchIter, NoopMetric, NoopMetric, NoopMetric,
      NoopMetric)) { ctriter =>
      val schema = Array(AttributeReference("longcol", LongType)().toAttribute)
      val myIter = spy(GeneratedInternalRowToCudfRowIterator(
        ctriter, schema, TargetSize(Int.MaxValue),
        NoopMetric, NoopMetric, NoopMetric, NoopMetric, NoopMetric))
      RmmSpark.forceRetryOOM(RmmSpark.getCurrentThreadId, 2,
        RmmSpark.OomInjectionType.GPU.ordinal, 0)
      assertResult(0)(getAndResetNumRetryThrowCurrentTask)
      withResource(myIter.next()) { devBatch =>
        withResource(buildBatch()) { expected =>
          TestUtils.compareBatches(expected, devBatch)
        }
      }
      assertResult(6)(getAndResetNumRetryThrowCurrentTask)
      assert(!myIter.hasNext)
      assertResult(0)(SpillFramework.stores.deviceStore.spill(1))
    }
  }

  test("a split and retry when copying to device is not handled, and we throw") {
    val batch = buildBatch()
    val batchIter = Seq(batch).iterator

    withResource(new ColumnarToRowIterator(batchIter, NoopMetric, NoopMetric, NoopMetric,
      NoopMetric)) { ctriter =>
      val schema = Array(AttributeReference("longcol", LongType)().toAttribute)
      val myIter = GeneratedInternalRowToCudfRowIterator(
        ctriter, schema, TargetSize(1),
        NoopMetric, NoopMetric, NoopMetric, NoopMetric, NoopMetric)
      RmmSpark.forceSplitAndRetryOOM(RmmSpark.getCurrentThreadId, 1,
        RmmSpark.OomInjectionType.GPU.ordinal, 0)
      assertThrows[GpuSplitAndRetryOOM] {
        myIter.next()
      }
      assertResult(0)(SpillFramework.stores.deviceStore.spill(1))
    }
  }

  test("a retry when allocating host buffer for data and offsets is handled") {
    val batch = buildBatch()
    val batchIter = Seq(batch).iterator
    withResource(new ColumnarToRowIterator(batchIter, NoopMetric, NoopMetric, NoopMetric,
      NoopMetric)) { ctriter =>
      val schema = Array(AttributeReference("longcol", LongType)().toAttribute)
      val myIter = GeneratedInternalRowToCudfRowIterator(
        ctriter, schema, TargetSize(Int.MaxValue),
        NoopMetric, NoopMetric, NoopMetric, NoopMetric, NoopMetric)
      // Do this so we can avoid forcing failures in any host allocations
      // in ColumnarToRowIterator.hasNext()
      assert(ctriter.hasNext)
      // this forces a retry on the allocation of the combined offsets/data buffer
      RmmSpark.forceRetryOOM(RmmSpark.getCurrentThreadId, 1,
        RmmSpark.OomInjectionType.CPU.ordinal, 0)
      withResource(myIter.next()) { devBatch =>
        withResource(buildBatch()) { expected =>
          TestUtils.compareBatches(expected, devBatch)
        }
      }
      assert(!GpuColumnVector.extractBases(batch).exists(_.getRefCount > 0))
      assert(!myIter.hasNext)
      assertResult(0)(SpillFramework.stores.deviceStore.spill(1))
    }
  }

  test("a split and retry when allocating host buffer for data and offsets is handled") {
    val batch = buildBatch()
    val batchIter = Seq(batch).iterator
    withResource(new ColumnarToRowIterator(batchIter, NoopMetric, NoopMetric, NoopMetric,
      NoopMetric)) { ctriter =>
      val schema = Array(AttributeReference("longcol", LongType)().toAttribute)
      val myIter = GeneratedInternalRowToCudfRowIterator(
        ctriter, schema, TargetSize(Int.MaxValue),
        NoopMetric, NoopMetric, NoopMetric, NoopMetric, NoopMetric)
      // Do this so we can avoid forcing failures in any host allocations
      // in ColumnarToRowIterator.hasNext()
      assert(ctriter.hasNext)
      // this forces a split retry on the allocation of the combined offsets/data buffer
      RmmSpark.forceSplitAndRetryOOM(RmmSpark.getCurrentThreadId, 1,
        RmmSpark.OomInjectionType.CPU.ordinal, 0)
      withResource(myIter.next()) { devBatch =>
        withResource(buildBatch()) { expected =>
          TestUtils.compareBatches(expected, devBatch)
        }
      }
      assert(!GpuColumnVector.extractBases(batch).exists(_.getRefCount > 0))
      assert(!myIter.hasNext)
      assertResult(0)(SpillFramework.stores.deviceStore.spill(1))
    }
  }

  test("fillBatch writes the terminal offset of the packed rows list column") {
    val batch = buildBatch()
    val batchIter = Seq(batch).iterator
    withResource(new ColumnarToRowIterator(batchIter, NoopMetric, NoopMetric, NoopMetric,
      NoopMetric)) { ctriter =>
      val schema = Array(AttributeReference("longcol", LongType)().toAttribute)
      val myIter = GeneratedInternalRowToCudfRowIterator(
        ctriter, schema, TargetSize(Int.MaxValue),
        NoopMetric, NoopMetric, NoopMetric, NoopMetric, NoopMetric)
      val numRows = 4
      withResource(HostMemoryBuffer.allocate(1024)) { dataBuffer =>
        withResource(HostMemoryBuffer.allocate((numRows + 1) * 4L)) { offsetsBuffer =>
          // Poison the offsets buffer to mimic recycled host memory: fillBatch must
          // overwrite every slot it is responsible for, including the terminal one.
          (0 to numRows).foreach(i => offsetsBuffer.setInt(i * 4L, 0xDEADBEEF))
          val used = myIter.fillBatch(dataBuffer, offsetsBuffer, 1024L, numRows)
          val dataOffset = used(0)
          val rowsCopied = used(1)
          assertResult(numRows)(rowsCopied)
          assertResult(0)(offsetsBuffer.getInt(0))
          // A cudf list requires the terminal offset to equal the child size in bytes,
          // otherwise offsets-reading consumers like chunked_pack (spill) compute a
          // corrupt child size.
          assertResult(dataOffset)(offsetsBuffer.getInt(rowsCopied * 4L))
        }
      }
    }
  }

  test("converting from rows works after the device batch actually spills") {
    val batch = buildBatch()
    val batchIter = Seq(batch).iterator
    var bytesSpilled = 0L
    doAnswer(new Answer[Boolean]() {
      override def answer(invocation: InvocationOnMock): Boolean = {
        val handle = invocation.getArgument(0).asInstanceOf[SpillableColumnarBatchHandle]
        invocation.callRealMethod()
        // Really spill the device column of rows: materializing it afterwards rebuilds
        // the batch from the chunked-pack metadata, which derives the list child size
        // from the terminal offset written by fillBatch.
        bytesSpilled += SpillFramework.stores.deviceStore.spill(handle.approxSizeInBytes)
        true
      }
    }).when(SpillFramework.stores.deviceStore)
      .track(any())

    withResource(new ColumnarToRowIterator(batchIter, NoopMetric, NoopMetric, NoopMetric,
      NoopMetric)) { ctriter =>
      val schema = Array(AttributeReference("longcol", LongType)().toAttribute)
      val myIter = GeneratedInternalRowToCudfRowIterator(
        ctriter, schema, TargetSize(Int.MaxValue),
        NoopMetric, NoopMetric, NoopMetric, NoopMetric, NoopMetric)
      withResource(myIter.next()) { devBatch =>
        withResource(buildBatch()) { expected =>
          TestUtils.compareBatches(expected, devBatch)
        }
      }
      assert(bytesSpilled > 0)
      assert(!myIter.hasNext)
      assertResult(0)(SpillFramework.stores.deviceStore.spill(1))
    }
  }
}