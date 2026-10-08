/*
 * Copyright (c) 2021-2026, NVIDIA CORPORATION.
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

import ai.rapids.cudf.{ColumnView, DefaultHostMemoryAllocator, DType, HostColumnVector, HostColumnVectorCore, HostMemoryAllocator, HostMemoryBuffer}
import com.nvidia.spark.rapids.Arm.withResource
import org.junit.jupiter.api.Assertions.{assertArrayEquals, assertEquals}

/**
 * Convenience methods for testing cuDF calls directly. This code is largely copied
 * from the cuDF Java test suite.
 */
object CudfTestHelper {

  /**
   * Checks and asserts that passed in columns match
   *
   * @param expect The expected result column
   * @param cv     The input column
   */
  def assertColumnsAreEqual(expect: ColumnView, cv: ColumnView): Unit = {
    assertColumnsAreEqual(expect, cv, colName = "unnamed")
  }

  /**
   * Checks and asserts that passed in columns match
   *
   * @param expected The expected result column
   * @param cv       The input column
   * @param colName  The name of the column
   */
  def assertColumnsAreEqual(expected: ColumnView, cv: ColumnView, colName: String): Unit = {
    assertPartialColumnsAreEqual(expected, 0, expected.getRowCount, cv, colName,
      enableNullCheck = true)
  }

  /**
   * Checks and asserts that passed in host columns match
   *
   * @param expected The expected result host column
   * @param cv       The input host column
   * @param colName  The name of the host column
   */
  def assertColumnsAreEqual(expected: HostColumnVector,
                            cv: HostColumnVector, colName: String): Unit = {
    assertPartialColumnsAreEqual(expected, 0,
      expected.getRowCount, cv, colName, enableNullCheck = true)
  }

  def assertPartialColumnsAreEqual(
      expected: ColumnView,
      rowOffset: Long,
      length: Long,
      cv: ColumnView,
      colName: String,
      enableNullCheck: Boolean): Unit = {
    withResource(expected.copyToHost) { hostExpected =>
      withResource(cv.copyToHost) { hostcv =>
        assertPartialColumnsAreEqual(hostExpected, rowOffset, length,
          hostcv, colName, enableNullCheck)
      }
    }
  }

  def assertPartialColumnsAreEqual(
      expected: HostColumnVectorCore,
      rowOffset: Long, length: Long,
      cv: HostColumnVectorCore,
      colName: String, enableNullCheck: Boolean): Unit = {
    assertEquals(expected.getType, cv.getType, "Type For Column " + colName)
    assertEquals(length, cv.getRowCount, "Row Count For Column " + colName)
    assertEquals(expected.getNumChildren, cv.getNumChildren, "Child Count for Column " + colName)
    if (enableNullCheck) assertEquals(expected.getNullCount,
      cv.getNullCount, "Null Count For Column " + colName)
    else {
      // TODO add in a proper check when null counts are
      //  supported by serializing a partitioned column
    }

    import ai.rapids.cudf.DType.DTypeEnum._

    val `type`: DType = expected.getType
    for (expectedRow <- rowOffset until (rowOffset + length)) {
      val tableRow: Long = expectedRow - rowOffset
      assertEquals(expected.isNull(expectedRow), cv.isNull(tableRow),
        "NULL for Column " + colName + " Row " + tableRow)
      if (!expected.isNull(expectedRow)) `type`.getTypeId match {
        case BOOL8 | INT8 | UINT8 =>
          assertEquals(expected.getByte(expectedRow), cv.getByte(tableRow),
            "Column " + colName + " Row " + tableRow)

        case INT16 | UINT16 =>
          assertEquals(expected.getShort(expectedRow), cv.getShort(tableRow),
            "Column " + colName + " Row " + tableRow)

        case INT32 | UINT32 | TIMESTAMP_DAYS | DURATION_DAYS | DECIMAL32 =>
          assertEquals(expected.getInt(expectedRow), cv.getInt(tableRow),
            "Column " + colName + " Row " + tableRow)

        case INT64 | UINT64 |
             DURATION_MICROSECONDS | DURATION_MILLISECONDS | DURATION_NANOSECONDS |
             DURATION_SECONDS | TIMESTAMP_MICROSECONDS | TIMESTAMP_MILLISECONDS |
             TIMESTAMP_NANOSECONDS | TIMESTAMP_SECONDS | DECIMAL64 =>
          assertEquals(expected.getLong(expectedRow), cv.getLong(tableRow),
            "Column " + colName + " Row " + tableRow)

        case STRING =>
          assertArrayEquals(expected.getUTF8(expectedRow), cv.getUTF8(tableRow),
            "Column " + colName + " Row " + tableRow)

        case _ =>
          throw new IllegalArgumentException(`type` + " is not supported yet")
      }
    }
  }

  /** Delegates host allocations, recording the largest request and every buffer returned. */
  class RecordingHostAllocator(delegate: HostMemoryAllocator) extends HostMemoryAllocator {
    private val buffers = ArrayBuffer[HostMemoryBuffer]()
    private var largestRequest = 0L

    override def allocate(amount: Long, preferPinned: Boolean): HostMemoryBuffer =
      record(amount)(delegate.allocate(amount, preferPinned))

    override def allocate(amount: Long): HostMemoryBuffer =
      record(amount)(delegate.allocate(amount))

    private def record(amount: Long)(doAllocate: => HostMemoryBuffer): HostMemoryBuffer = {
      synchronized {
        largestRequest = math.max(largestRequest, amount)
      }
      val buffer = doAllocate
      synchronized {
        buffers += buffer
      }
      buffer
    }

    def largest: Long = synchronized(largestRequest)

    def allClosed: Boolean = synchronized(buffers.forall(_.getRefCount == 0))
  }

  /** Runs `body` with cuDF's default host allocator wrapped in a recorder. */
  def withRecordedHostAllocations[T](body: RecordingHostAllocator => T): T = {
    val previous = DefaultHostMemoryAllocator.get()
    val recorder = new RecordingHostAllocator(previous)
    DefaultHostMemoryAllocator.set(recorder)
    try {
      body(recorder)
    } finally {
      DefaultHostMemoryAllocator.set(previous)
    }
  }
}
