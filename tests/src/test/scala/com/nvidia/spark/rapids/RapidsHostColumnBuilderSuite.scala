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

import java.io.ByteArrayOutputStream
import java.nio.{ByteBuffer, ByteOrder}
import java.nio.charset.StandardCharsets
import java.util.function.Supplier

import scala.util.{Failure, Try}

import ai.rapids.cudf.{DType, HostColumnVector, HostColumnVectorCore}
import ai.rapids.cudf.HostColumnVector.{BasicType, DataType, ListType, StructType}
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.RapidsHostColumnBuilder.Limits
import com.nvidia.spark.rapids.RapidsHostColumnBuilderSuite._
import org.scalatest.Assertions
import org.scalatest.funsuite.AnyFunSuite

class RapidsHostColumnBuilderSuite extends AnyFunSuite {
  // Small enough that a test reaches a limit with a few KiB of host memory.
  private val smallLimit = 4096

  // An estimate of 1 makes every buffer grow by doubling; an estimate above the limit makes
  // the first allocation exceed it unless that allocation is bounded too.
  private val estimates = Seq(1L, 2L * smallLimit)

  private val intType = new BasicType(true, DType.INT32)
  private val stringType = new BasicType(true, DType.STRING)
  private val binaryType = new ListType(true, new BasicType(false, DType.UINT8))

  private def bytesMessage(attempted: Long, limit: Long): String =
    limitMessage("The string data size in bytes", attempted, limit, DType.STRING)

  private def elementsMessage(attempted: Long, limit: Long, dtype: DType): String =
    limitMessage("The number of elements", attempted, limit, dtype)

  private def rowsMessage(attempted: Long, limit: Long, dtype: DType): String =
    limitMessage("The number of rows", attempted, limit, dtype)

  private def assertRejects(expectedMessage: String)(append: => Any): Unit = {
    val e = intercept[ColumnLimitExceededException](append)
    assertResult(expectedMessage)(e.getMessage)
  }

  private def isNullRow(i: Int): Boolean = i % 7 == 0

  private def letters(size: Int): Array[Byte] =
    Array.tabulate[Byte](size)(i => ('a' + i % 26).toByte)

  /** Appends one string of the given size to a new builder; false if a limit rejects it. */
  private def acceptsString(size: Int): Boolean =
    withResource(new RapidsHostColumnBuilder(stringType, 1)) { b =>
      try {
        b.appendUTF8String(new Array[Byte](size))
        true
      } catch {
        case _: ColumnLimitExceededException => false
      }
    }

  private def validityBytes(rows: Long): Long = ((rows + 7) / 8 + 63) / 64 * 64

  /** Checks that no buffer of the column or of its children is longer than the limits allow. */
  private def assertBuffersWithin(col: HostColumnVectorCore, l: Limits): Unit = {
    val dtype = col.getType
    val rowLimit = dtype match {
      case DType.STRING | DType.LIST => l.maxOffsetRows
      case DType.STRUCT => l.maxStructRows
      case _ => l.maxFixedWidthElements
    }
    val dataLimit = dtype match {
      case DType.STRING => l.maxStringBytes
      case _ => l.maxFixedWidthElements * dtype.getSizeInBytes
    }
    Option(col.getData).foreach { data =>
      assert(data.getLength <= dataLimit, s"$dtype data")
    }
    Option(col.getOffsets).foreach { offsets =>
      assert(offsets.getLength <= (rowLimit + 1) * DType.INT32.getSizeInBytes, s"$dtype offsets")
    }
    Option(col.getValidity).foreach { valid =>
      assert(valid.getLength <= validityBytes(rowLimit), s"$dtype validity")
    }
    (0 until col.getNumChildren).foreach(i => assertBuffersWithin(col.getChildColumnView(i), l))
  }

  private def buildAndCheck(b: RapidsHostColumnBuilder, l: Limits)(
      check: HostColumnVector => Unit): Unit =
    withResource(b.build()) { v =>
      check(v)
      assertNullCountsMatchMasks(v)
      assertBuffersWithin(v, l)
    }

  test("growing buffer preserves correctness") {
    val b1 = new RapidsHostColumnBuilder(new BasicType(false, DType.INT32), 0) // grows
    val b2 = new RapidsHostColumnBuilder(new BasicType(false, DType.INT32), 8) // does not grow
    for (i <- 0 to 7) {
      b1.append(i)
      b2.append(i)
    }
    val v1 = b1.build()
    val v2 = b2.build()
    for (i <- 0 to 7) {
      assertResult(v1.getInt(i))(v2.getInt(i))
    }
    v1.close()
    v2.close()
    b1.close()
    b2.close()
  }

  test("appendLists walks appendChildOrNull for typed and null list elements") {
    // appendLists -> append(List) -> appendChildOrNull: one arm per element type, plus the null arm
    def buildList(childType: DType, elems: AnyRef*): Unit = {
      val lt = new ListType(true, new BasicType(true, childType))
      val b = new RapidsHostColumnBuilder(lt, 1)
      try {
        b.appendLists(java.util.Arrays.asList(elems: _*))
        val v = b.build()
        try {
          assertResult(1L)(v.getRowCount)
        } finally {
          v.close()
        }
      } finally {
        b.close()
      }
    }
    buildList(DType.INT32, Integer.valueOf(1), null, Integer.valueOf(3))
    buildList(DType.INT64, java.lang.Long.valueOf(1L), null)
    buildList(DType.FLOAT64, java.lang.Double.valueOf(1.0d), null)
    buildList(DType.FLOAT32, java.lang.Float.valueOf(1.0f), null)
    buildList(DType.BOOL8, java.lang.Boolean.TRUE, null)
    buildList(DType.STRING, "a", null)
  }

  test("captureState then restoreState rolls back appended struct rows including children") {
    val st = new StructType(true,
      new BasicType(true, DType.INT32),
      new BasicType(true, DType.INT32))
    val b = new RapidsHostColumnBuilder(st, 4)
    try {
      b.getChild(0).append(1)
      b.getChild(1).append(10)
      b.endStruct()
      val snapshot = b.captureState()
      b.getChild(0).append(2)
      b.getChild(1).append(20)
      b.endStruct()
      b.restoreState(snapshot)
      val v = b.build()
      try {
        assertResult(1L)(v.getRowCount)
      } finally {
        v.close()
      }
    } finally {
      b.close()
    }
  }

  test("restoreState handles non-null rows beyond the allocated validity bitmap") {
    withResource(new RapidsHostColumnBuilder(new BasicType(true, DType.INT32), 4)) { b =>
      b.appendNull()
      (1 to 4096).foreach(i => b.append(i))
      val snapshot = b.captureState()
      b.append(4097)
      b.restoreState(snapshot)
      b.append(4098)
      withResource(b.build()) { column =>
        assertResult(4098L)(column.getRowCount)
        assertResult(1L)(column.getNullCount)
        assert(column.isNull(0))
        (1 to 4096).foreach(i => assertResult(i)(column.getInt(i)))
        assertResult(4098)(column.getInt(4097))
      }
    }
  }

  test("restoreState preserves earlier nulls and is idempotent across validity bytes") {
    withResource(new RapidsHostColumnBuilder(new BasicType(true, DType.INT32), 16)) { b =>
      b.appendNull()
      (1 to 6).foreach(i => b.append(i))
      val snapshot = b.captureState()
      b.appendNull()
      b.append(8)
      b.appendNull()
      b.restoreState(snapshot)
      b.restoreState(snapshot)
      (7 to 9).foreach(i => b.append(i))
      withResource(b.build()) { column =>
        assertResult(10L)(column.getRowCount)
        assertResult(1L)(column.getNullCount)
        assert(column.isNull(0))
        (1 to 9).foreach { i =>
          assert(!column.isNull(i))
          assertResult(i)(column.getInt(i))
        }
      }
    }
  }

  test("restoreState rolls back a partial struct child before non-null replay") {
    val byteList = new ListType(true, new BasicType(false, DType.UINT8))
    withResource(new RapidsHostColumnBuilder(new StructType(true, byteList, byteList), 4)) {
      b =>
        val snapshot = b.captureState()
        b.getChild(0).appendNull()
        b.restoreState(snapshot)
        b.getChild(0).appendByteList(Array[Byte](1, 2))
        b.getChild(1).appendByteList(Array[Byte](3))
        b.endStruct()
        withResource(b.build()) { column =>
          assertResult(1L)(column.getRowCount)
          assertResult(0L)(column.getNullCount)
          (0 until column.getNumChildren).foreach { index =>
            withResource(column.getChildColumnView(index)) { child =>
              assertResult(0L)(child.getNullCount)
              assert(!child.isNull(0))
              withResource(child.getChildColumnView(0)) { values =>
                val expected = if (index == 0) Array[Byte](1, 2) else Array[Byte](3)
                assertResult(expected.length.toLong)(values.getRowCount)
                expected.indices.foreach(i => assertResult(expected(i))(values.getByte(i)))
              }
            }
          }
        }
    }
  }

  test("restoreState rolls back null counts and validity recursively") {
    val byteList = new ListType(true, new BasicType(false, DType.UINT8))
    val st = new StructType(true, byteList, byteList)
    val b = new RapidsHostColumnBuilder(st, 4)
    try {
      b.getChild(0).appendByteList(Array[Byte](1, 2))
      b.getChild(1).appendByteList(Array[Byte](3))
      b.endStruct()

      val snapshot = b.captureState()
      b.appendNull()
      b.restoreState(snapshot)

      val partial = b.build()
      try {
        assertResult(1L)(partial.getRowCount)
        assertResult(0L)(partial.getNullCount)
        (0 until partial.getNumChildren).foreach { index =>
          withResource(partial.getChildColumnView(index)) { child =>
            assertResult(0L)(child.getNullCount)
          }
        }
      } finally {
        partial.close()
      }

      b.appendNull()
      val replayed = b.build()
      try {
        assertResult(2L)(replayed.getRowCount)
        assertResult(1L)(replayed.getNullCount)
        (0 until replayed.getNumChildren).foreach { index =>
          withResource(replayed.getChildColumnView(index)) { child =>
            assertResult(1L)(child.getNullCount)
          }
        }
      } finally {
        replayed.close()
      }
    } finally {
      b.close()
    }
  }

  test("a refilled snapshot restores the state of its last refill, string bytes included") {
    withResource(new RapidsHostColumnBuilder(new ListType(true, stringType), 1)) { b =>
      val strings = b.getChild(0)
      def appendRow(value: String): Unit = {
        strings.append(value)
        b.endList()
      }
      appendRow("a")
      val state = b.captureState()
      appendRow("bb")
      b.captureState(state)
      appendRow("ccc")
      b.restoreState(state)
      appendRow("dd")
      withResource(b.build()) { v =>
        assertResult(3L)(v.getRowCount)
        val child = v.getChildColumnView(0)
        assertResult(Seq("a", "bb", "dd"))((0 until 3).map(i => child.getJavaString(i)))
        assertNullCountsMatchMasks(v)
      }
    }
  }

  test("appendUTF8String asserts on a subrange past the array before changing the builder") {
    // The subrange check is a Java assertion, so it only runs under -ea.
    assume(classOf[RapidsHostColumnBuilder].desiredAssertionStatus(), "needs -ea")
    val bytes = "abcd".getBytes(StandardCharsets.UTF_8)
    withResource(new RapidsHostColumnBuilder(stringType, 2)) { b =>
      b.appendUTF8String(bytes, 0, 2)
      // srcOffset + length overflows Int, so only a check that subtracts rejects it.
      intercept[AssertionError](b.appendUTF8String(bytes, 1, Int.MaxValue))
      b.appendUTF8String(bytes, 2, 2)
      withResource(b.build()) { v =>
        assertResult(2L)(v.getRowCount)
        assertResult(Seq("ab", "cd"))((0 until 2).map(i => v.getJavaString(i)))
      }
    }
  }

  test("a string column rejects the value that would cross its byte limit and keeps its rows") {
    // One below a power of two, like the production limit of 2^31 - 1
    val limit = smallLimit - 1
    val testLimits = limits(stringBytes = limit)
    // A power-of-two value size, and one that makes the buffer double unevenly
    Seq(64, 61).foreach { valueSize =>
      val value = letters(valueSize)
      val fullRows = limit / valueSize
      val rest = limit - fullRows * valueSize
      withLimits(testLimits) {
        withResource(new RapidsHostColumnBuilder(stringType, 1)) { b =>
          (0 until fullRows).foreach(_ => b.appendUTF8String(value))
          assertRejects(bytesMessage((fullRows + 1L) * valueSize, limit)) {
            b.appendUTF8String(value)
          }
          b.appendUTF8String(value, 0, rest)
          assertRejects(bytesMessage(limit + 1L, limit))(b.appendUTF8String(value, 0, 1))
          buildAndCheck(b, testLimits) { v =>
            assertResult(fullRows + 1L)(v.getRowCount)
            (0 until fullRows).foreach(i => assert(v.getUTF8(i).sameElements(value)))
            assert(v.getUTF8(fullRows).sameElements(value.take(rest)))
            assertResult(limit.toLong)(v.getEndListOffset(fullRows))
          }
        }
      }
    }
  }

  test("a binary column rejects the value that would cross its element limit and keeps its rows") {
    // Two below a power of two, like the production limit of 2^31 - 2
    val limit = smallLimit - 2
    val testLimits = limits(fixedWidthElements = limit)
    val value = Array.tabulate[Byte](64)(_.toByte)
    val fullRows = limit / value.length
    val rest = limit - fullRows * value.length
    estimates.foreach { estimate =>
      withLimits(testLimits) {
        withResource(new RapidsHostColumnBuilder(binaryType, estimate)) { b =>
          (0 until fullRows).foreach(_ => b.appendByteList(value))
          assertRejects(elementsMessage((fullRows + 1L) * value.length, limit, DType.UINT8)) {
            b.appendByteList(value)
          }
          b.appendByteList(value, 0, rest)
          assertRejects(elementsMessage(limit + 1L, limit, DType.UINT8)) {
            b.appendByteList(value, 0, 1)
          }
          buildAndCheck(b, testLimits) { v =>
            assertResult(fullRows + 1L)(v.getRowCount)
            assertResult(limit.toLong)(v.getChildColumnView(0).getRowCount)
            (0 until fullRows).foreach(i => assert(v.getBytesFromList(i).sameElements(value)))
            assert(v.getBytesFromList(fullRows).sameElements(value.take(rest)))
          }
        }
      }
    }
  }

  test("a LIST<STRING> column rejects in its STRING child and rolls back to a valid prefix") {
    val limit = smallLimit - 1
    val testLimits = limits(stringBytes = limit)
    val value = letters(64)
    val valuesPerRow = 2
    val fullRows = limit / (valuesPerRow * value.length)
    withLimits(testLimits) {
      withResource(new RapidsHostColumnBuilder(new ListType(true, stringType), 1)) { b =>
        val strings = b.getChild(0)
        def appendRow(): Unit = {
          (0 until valuesPerRow).foreach(_ => strings.appendUTF8String(value))
          b.endList()
        }
        (0 until fullRows).foreach(_ => appendRow())
        val state = b.captureState()
        assertRejects(bytesMessage((fullRows + 1L) * valuesPerRow * value.length, limit)) {
          appendRow()
        }
        // The row's first string went in before its second one was rejected.
        assertResult(fullRows * valuesPerRow + 1)(strings.getCurrentIndex)
        b.restoreState(state)
        buildAndCheck(b, testLimits) { v =>
          assertResult(fullRows.toLong)(v.getRowCount)
          val child = v.getChildColumnView(0)
          assertResult(fullRows.toLong * valuesPerRow)(child.getRowCount)
          (0 until fullRows * valuesPerRow).foreach { i =>
            assert(child.getUTF8(i).sameElements(value))
          }
        }
      }
    }
  }

  test("a LIST<LIST<INT8>> column limits its INT8 elements") {
    val listOfByteLists = new ListType(true, new ListType(true, new BasicType(true, DType.INT8)))
    val testLimits = limits(fixedWidthElements = smallLimit)
    val innerListsPerRow = 4
    val elementsPerInnerList = 16
    val fullRows = smallLimit / (innerListsPerRow * elementsPerInnerList)
    estimates.foreach { estimate =>
      withLimits(testLimits) {
        withResource(new RapidsHostColumnBuilder(listOfByteLists, estimate)) { b =>
          val innerLists = b.getChild(0)
          val elements = innerLists.getChild(0)
          (0 until fullRows).foreach { _ =>
            (0 until innerListsPerRow).foreach { _ =>
              (0 until elementsPerInnerList).foreach(k => elements.append(k.toByte))
              innerLists.endList()
            }
            b.endList()
          }
          assertRejects(elementsMessage(smallLimit + 1L, smallLimit, DType.INT8)) {
            elements.append(0.toByte)
          }
          b.endList()
          buildAndCheck(b, testLimits) { v =>
            assertResult(fullRows + 1L)(v.getRowCount)
            val innerView = v.getChildColumnView(0)
            assertResult(fullRows.toLong * innerListsPerRow)(innerView.getRowCount)
            val byteView = innerView.getChildColumnView(0)
            assertResult(smallLimit.toLong)(byteView.getRowCount)
            (0 until smallLimit).foreach { k =>
              assertResult((k % elementsPerInnerList).toByte)(byteView.getByte(k))
            }
          }
        }
      }
    }
  }

  /** A column with a row or element limit: how to append a row to it and to read one back. */
  private case class RowLimitCase(
      name: String,
      dtype: DataType,
      testLimits: Limits,
      expected: String,
      appendRow: (RapidsHostColumnBuilder, Int) => Unit,
      appendEmptyRow: RapidsHostColumnBuilder => Unit,
      checkRow: (HostColumnVector, Int) => Unit)

  private val rowLimitCases = Seq(
    RowLimitCase("a fixed-width column", new BasicType(true, DType.INT64),
      limits(fixedWidthElements = smallLimit),
      elementsMessage(smallLimit + 1L, smallLimit, DType.INT64),
      (b, i) => b.append(i.toLong), _.append(-1L),
      (v, i) => assertResult(i.toLong)(v.getLong(i))),
    RowLimitCase("a string column", stringType, limits(offsetRows = smallLimit),
      rowsMessage(smallLimit + 1L, smallLimit, DType.STRING),
      (b, i) => b.append(i.toString), _.append(""),
      (v, i) => assertResult(i.toString)(v.getJavaString(i))),
    RowLimitCase("a list column", new ListType(true, intType), limits(offsetRows = smallLimit),
      rowsMessage(smallLimit + 1L, smallLimit, DType.LIST),
      (b, i) => {
        b.getChild(0).append(i)
        b.endList()
      },
      _.endList(),
      (v, i) => assertResult(Seq(i)) {
        (v.getStartListOffset(i) until v.getEndListOffset(i))
          .map(j => v.getChildColumnView(0).getInt(j))
      }),
    RowLimitCase("a struct column", new StructType(true, intType), limits(structRows = smallLimit),
      rowsMessage(smallLimit + 1L, smallLimit, DType.STRUCT),
      (b, i) => {
        b.getChild(0).append(i)
        b.endStruct()
      },
      _.endStruct(),
      (v, i) => assertResult(i)(v.getChildColumnView(0).getInt(i))))

  rowLimitCases.foreach { c =>
    test(s"${c.name} accepts rows up to its limit and rejects one more") {
      estimates.foreach { estimate =>
        withLimits(c.testLimits) {
          withResource(new RapidsHostColumnBuilder(c.dtype, estimate)) { b =>
            (0 until smallLimit).foreach { i =>
              if (isNullRow(i)) b.appendNull() else c.appendRow(b, i)
            }
            def childRows: Seq[Int] =
              (0 until c.dtype.getNumChildren).map(k => b.getChild(k).getCurrentIndex)
            val childRowsBefore = childRows
            assertRejects(c.expected)(c.appendEmptyRow(b))
            assertRejects(c.expected)(b.appendNull())
            // A struct null would also append a null to each child.
            assertResult(childRowsBefore)(childRows)
            buildAndCheck(b, c.testLimits) { v =>
              assertResult(smallLimit.toLong)(v.getRowCount)
              (0 until smallLimit).foreach { i =>
                if (isNullRow(i)) assert(v.isNull(i)) else c.checkRow(v, i)
              }
            }
          }
        }
      }
    }
  }

  test("the production limits are the most a cuDF column can hold") {
    // String bytes and offset rows are bounded by Int offsets, which need one entry more than
    // there are rows; fixed-width elements and struct rows by the original builder's caps.
    assertResult(2147483647L)(productionLimits.maxStringBytes)
    assertResult(2147483646L)(productionLimits.maxFixedWidthElements)
    assertResult(2147483645L)(productionLimits.maxOffsetRows)
    assertResult(2147483646L)(productionLimits.maxStructRows)
  }

  test("test limits must be valid and are restored after a return, a throw or a rejection") {
    val outer = limits(stringBytes = smallLimit)
    val inner = limits(stringBytes = smallLimit / 2)
    def outerInEffect: Boolean =
      acceptsString(smallLimit / 2 + 1) && !acceptsString(smallLimit + 1)
    val invalid = Seq(
      limits(stringBytes = 0),
      limits(stringBytes = -1),
      limits(stringBytes = productionLimits.maxStringBytes + 1),
      limits(fixedWidthElements = 0),
      limits(fixedWidthElements = productionLimits.maxFixedWidthElements + 1),
      limits(offsetRows = 0),
      limits(offsetRows = productionLimits.maxOffsetRows + 1),
      limits(structRows = 0),
      limits(structRows = productionLimits.maxStructRows + 1))
    val result = withLimits(outer) {
      assert(outerInEffect)
      invalid.foreach { testLimits =>
        var ran = false
        val e = intercept[IllegalArgumentException](withLimits(testLimits) { ran = true })
        assert(!e.isInstanceOf[ColumnLimitExceededException])
        assert(!ran)
        assert(outerInEffect)
      }
      val innerResult = withLimits(inner) {
        assert(!acceptsString(smallLimit / 2 + 1))
        "inner"
      }
      assertResult("inner")(innerResult)
      assert(outerInEffect)
      val failure = new IllegalStateException("the body failed")
      val thrown = intercept[IllegalStateException](withLimits[Unit](inner)(throw failure))
      assert(thrown eq failure)
      assert(outerInEffect)
      "outer"
    }
    assertResult("outer")(result)
    assert(acceptsString(smallLimit + 1))
    intercept[IllegalStateException] {
      withLimits[Unit](outer)(throw new IllegalStateException("the body failed"))
    }
    assert(acceptsString(smallLimit + 1))
    assert(withLimits(productionLimits)(acceptsString(smallLimit + 1)))
    assert(withLimits(limits(stringBytes = 1))(acceptsString(1)))
  }

  test("test limits do not reach builders created on another thread") {
    withLimits(limits(stringBytes = smallLimit)) {
      var accepted: Try[Boolean] = Failure(new IllegalStateException("the thread did not run"))
      val thread = new Thread(new Runnable {
        override def run(): Unit = {
          accepted = Try(acceptsString(smallLimit + 1))
        }
      })
      thread.start()
      thread.join()
      assert(accepted.get)
      assert(!acceptsString(smallLimit + 1))
    }
  }

  test("a data size estimate sizes the string bytes or list children up front") {
    withResource(new RapidsHostColumnBuilder(stringType, 4, 64)) { b =>
      b.appendUTF8String(letters(10))
      b.appendUTF8String(letters(30))
      withResource(b.build()) { v =>
        assertResult(64L)(v.getData.getLength)
        assertResult(letters(30).toSeq)(v.getJavaString(1).getBytes(StandardCharsets.UTF_8).toSeq)
      }
    }
    withResource(new RapidsHostColumnBuilder(binaryType, 4, 64)) { b =>
      b.appendByteList(letters(10))
      b.appendByteList(letters(30))
      withResource(b.build()) { v =>
        assertResult(64L)(v.getChildColumnView(0).getData.getLength)
        assert(v.getBytesFromList(1).sameElements(letters(30)))
      }
    }
  }

  // Chunks of 8, 5, 6 and 11 rows start at aligned and unaligned rows of the validity mask, and
  // the last one spans two of its bytes. The third one has no nulls.
  private def chunksOf[T](value: (Int, Int) => T): Seq[Seq[Option[T]]] =
    Seq(8, 5, 6, 11).zipWithIndex.map { case (size, c) =>
      (0 until size).map(i => if (c != 2 && (i + c) % 3 == 0) None else Some(value(c, i)))
    }

  /** An Arrow validity bitmap of the chunk, or null when it has no nulls. */
  private def validityOf(chunk: Seq[Option[Any]]): ByteBuffer =
    if (chunk.forall(_.isDefined)) {
      null
    } else {
      val bits = new Array[Byte]((chunk.length + 7) / 8)
      chunk.zipWithIndex.foreach { case (v, i) =>
        if (v.isDefined) {
          bits(i / 8) = (bits(i / 8) | 1 << (i % 8)).toByte
        }
      }
      ByteBuffer.wrap(bits)
    }

  /** Arrow offsets and data of the chunk, the data starting a few bytes in for odd sizes. */
  private def variableWidth(chunk: Seq[Option[Array[Byte]]]): (ByteBuffer, ByteBuffer) = {
    val skip = chunk.length % 2 * 3
    val offsets = ByteBuffer.allocate((chunk.length + 1) * 4).order(ByteOrder.LITTLE_ENDIAN)
    val data = new ByteArrayOutputStream()
    data.write(new Array[Byte](skip), 0, skip)
    offsets.putInt(0, skip)
    chunk.zipWithIndex.foreach { case (v, i) =>
      v.foreach(bytes => data.write(bytes, 0, bytes.length))
      offsets.putInt((i + 1) * 4, data.size())
    }
    (offsets, ByteBuffer.wrap(data.toByteArray))
  }

  /**
   * Appends the chunks to one builder in bulk and to another row by row, and checks that both
   * build the same column. The bulk builder also gets every other chunk row by row, as a column
   * that switched to row-by-row copies does.
   */
  private def assertBulkAppendsMatch[T](dataType: DataType, chunks: Seq[Seq[Option[T]]])(
      appendBulk: (RapidsHostColumnBuilder, Seq[Option[T]], ByteBuffer, Long) => Long)(
      appendRow: (RapidsHostColumnBuilder, T) => Any)(
      read: (HostColumnVector, Int) => Any): Unit = {
    def appendByRow(b: RapidsHostColumnBuilder, chunk: Seq[Option[T]]): Unit = chunk.foreach {
      case Some(v) => appendRow(b, v)
      case None => b.appendNull()
    }
    for (estimate <- estimates; interleave <- Seq(false, true)) {
      withResource(new RapidsHostColumnBuilder(dataType, estimate)) { bulk =>
        withResource(new RapidsHostColumnBuilder(dataType, estimate)) { byRow =>
          chunks.zipWithIndex.foreach { case (chunk, c) =>
            if (interleave && c % 2 == 0) {
              appendByRow(bulk, chunk)
            } else {
              appendBulk(bulk, chunk, validityOf(chunk), chunk.count(_.isEmpty))
            }
            appendByRow(byRow, chunk)
          }
          withResource(byRow.build()) { expected =>
            withResource(bulk.build()) { actual =>
              assertResult(expected.getRowCount)(actual.getRowCount)
              assertResult(expected.getNullCount)(actual.getNullCount)
              assertNullCountsMatchMasks(actual)
              (0 until expected.getRowCount.toInt).foreach { i =>
                assertResult(expected.isNull(i), s"row $i")(actual.isNull(i))
                if (!expected.isNull(i)) {
                  assertResult(read(expected, i), s"row $i")(read(actual, i))
                }
              }
            }
          }
        }
      }
    }
  }

  test("appendFixedWidth builds what append builds") {
    assertBulkAppendsMatch(intType, chunksOf((c, i) => c * 100 - i)) {
      (b, chunk, validity, nulls) =>
        val values = ByteBuffer.allocate(chunk.length * 4).order(ByteOrder.LITTLE_ENDIAN)
        chunk.zipWithIndex.foreach { case (v, i) => values.putInt(i * 4, v.getOrElse(0)) }
        b.appendFixedWidth(values, validity, chunk.length, nulls)
    } { (b, v) => b.append(v) } { (v, i) => v.getInt(i) }
  }

  test("appendBooleanBits builds what append builds") {
    val boolType = new BasicType(true, DType.BOOL8)
    assertBulkAppendsMatch(boolType, chunksOf((c, i) => (i + c) % 2 == 0)) {
      (b, chunk, validity, nulls) =>
        val bits = new Array[Byte]((chunk.length + 7) / 8)
        chunk.zipWithIndex.foreach { case (v, i) =>
          if (v.contains(true)) {
            bits(i / 8) = (bits(i / 8) | 1 << (i % 8)).toByte
          }
        }
        b.appendBooleanBits(ByteBuffer.wrap(bits), validity, chunk.length, nulls)
    } { (b, v) => b.append(v) } { (v, i) => v.getBoolean(i) }
  }

  test("appendStrings builds what appendUTF8String builds") {
    assertBulkAppendsMatch(stringType, chunksOf((c, i) => "x" * ((i + c) % 3) + "é" * (i % 2))) {
      (b, chunk, validity, nulls) =>
        val (offsets, chars) =
          variableWidth(chunk.map(_.map(_.getBytes(StandardCharsets.UTF_8))))
        b.appendStrings(offsets, chars, validity, chunk.length, nulls)
    } { (b, v) => b.appendUTF8String(v.getBytes(StandardCharsets.UTF_8)) } {
      (v, i) => v.getJavaString(i)
    }
  }

  test("appendByteLists builds what appendByteList builds") {
    val bytes = chunksOf((c, i) => Array.tabulate[Byte]((i + c) % 3)(j => (i * j - c).toByte))
    assertBulkAppendsMatch(binaryType, bytes) { (b, chunk, validity, nulls) =>
      val (offsets, data) = variableWidth(chunk)
      b.appendByteLists(offsets, data, validity, chunk.length, nulls)
    } { (b, v) => b.appendByteList(v) } { (v, i) => v.getBytesFromList(i).toSeq }
  }

  test("the bulk appends check the column type and the null count without assertions") {
    val values = ByteBuffer.allocate(16)
    // Row 1 of 3 is null.
    val validity = ByteBuffer.wrap(Array[Byte](5))
    withResource(new RapidsHostColumnBuilder(intType, 4)) { b =>
      intercept[IllegalStateException](b.appendBooleanBits(values, null, 3, 0))
      intercept[IllegalStateException](b.appendStrings(values, values, null, 3, 0))
      intercept[IllegalStateException](b.appendByteLists(values, values, null, 3, 0))
      intercept[IllegalArgumentException](b.appendFixedWidth(values, validity, 3, 0))
      intercept[IllegalArgumentException](b.appendFixedWidth(values, validity, 3, 2))
      intercept[IllegalArgumentException](b.appendFixedWidth(values, null, 3, 1))
      // The rejected appends left the builder as it was.
      b.appendFixedWidth(values, validity, 3, 1)
      withResource(b.build()) { v =>
        assertResult(3L)(v.getRowCount)
        assertResult(1L)(v.getNullCount)
        assertNullCountsMatchMasks(v)
      }
    }
    withResource(new RapidsHostColumnBuilder(stringType, 4)) { b =>
      intercept[IllegalStateException](b.appendFixedWidth(values, null, 3, 0))
    }
  }

  test("a bulk append that would cross a limit is rejected before it changes the builder") {
    val limit = 6
    withLimits(limits(fixedWidthElements = limit)) {
      withResource(new RapidsHostColumnBuilder(intType, 1)) { b =>
        assertRejects(elementsMessage(limit + 1L, limit, DType.INT32)) {
          b.appendFixedWidth(ByteBuffer.allocate((limit + 1) * 4), null, limit + 1, 0)
        }
        assertResult(0)(b.getCurrentIndex)
      }
    }
    withLimits(limits(offsetRows = 2)) {
      withResource(new RapidsHostColumnBuilder(binaryType, 1)) { b =>
        val (offsets, bytes) = variableWidth(Seq(Some(Array[Byte](1, 2))))
        b.appendByteLists(offsets, bytes, null, 1, 0)
        // The parent rejects the rows before its child takes their bytes.
        val (more, moreBytes) = variableWidth(Seq(Some(Array[Byte](3)), Some(Array[Byte](4))))
        assertRejects(rowsMessage(3, 2, DType.LIST)) {
          b.appendByteLists(more, moreBytes, null, 2, 0)
        }
        withResource(b.build()) { v =>
          assertResult(1L)(v.getRowCount)
          assertResult(2L)(v.getChildColumnView(0).getRowCount)
          assert(v.getBytesFromList(0).sameElements(Array[Byte](1, 2)))
        }
      }
    }
  }
}

/** Helpers for the suites that lower the builder's limits. */
object RapidsHostColumnBuilderSuite {
  private[rapids] val productionLimits: Limits = RapidsHostColumnBuilder.PRODUCTION_LIMITS

  private[rapids] def limits(
      stringBytes: Long = productionLimits.maxStringBytes,
      fixedWidthElements: Long = productionLimits.maxFixedWidthElements,
      offsetRows: Long = productionLimits.maxOffsetRows,
      structRows: Long = productionLimits.maxStructRows): Limits =
    new Limits(stringBytes, fixedWidthElements, offsetRows, structRows)

  private[rapids] def withLimits[T](testLimits: Limits)(body: => T): T =
    RapidsHostColumnBuilder.withTestLimits(testLimits, new Supplier[T] {
      override def get(): T = body
    })

  private[rapids] def limitMessage(
      what: String, attempted: Long, limit: Long, dtype: DType): String =
    s"$what would be $attempted, exceeding the limit of $limit for a column of cuDF type " +
      s"$dtype; split the input into smaller batches or partitions, or reduce the size of " +
      "individual values"

  /** Checks that the null count of the column and of each child matches its validity mask. */
  private[rapids] def assertNullCountsMatchMasks(col: HostColumnVectorCore): Unit = {
    val maskNulls = (0L until col.getRowCount).count(i => col.isNull(i)).toLong
    Assertions.assertResult(maskNulls, s"${col.getType} null count")(col.getNullCount)
    (0 until col.getNumChildren).foreach { i =>
      assertNullCountsMatchMasks(col.getChildColumnView(i))
    }
  }
}
