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

import java.util.Arrays
import java.util.zip.CRC32

import scala.collection.mutable.ArrayBuffer

import ai.rapids.cudf.{DType, HostColumnVectorCore, MemoryBuffer}
import ai.rapids.cudf.HostColumnVector.{BasicType, ListType}
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.CudfTestHelper.{withRecordedHostAllocations, RecordingHostAllocator}
import com.nvidia.spark.rapids.RapidsHostColumnBuilderSuite.limitMessage
import com.nvidia.spark.rapids.RapidsPluginImplicits.AutoCloseableProducingArray
import com.nvidia.spark.rapids.parquet.ParquetCachedBatchSerializer
import org.scalactic.source.Position

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession.setActiveSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, GenericInternalRow, UnsafeProjection}
import org.apache.spark.sql.columnar.CachedBatch
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.execution.TrampolineUtil
import org.apache.spark.sql.types.{BinaryType, DataType, StringType}
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.unsafe.array.ByteArrayMethods
import org.apache.spark.unsafe.types.UTF8String

/**
 * Tests that need several GiB of host memory: RapidsHostColumnBuilder at its real string byte
 * and element limits, and ParquetCachedBatchSerializer caching, through its row entry, one
 * partition whose string or binary column passes 2 GiB.
 *
 * They are opt-in: each test is canceled unless spark.rapids.test.largeHostMemory.enabled=true
 * is passed through SPARK_CONF, and fails unless assertions are enabled for
 * RapidsHostColumnBuilder and for cuDF's MemoryBuffer, so that a column built past its limit
 * fails an assertion instead of corrupting memory. See "Large Host Memory Tests" in
 * tests/README.md for how to run them.
 */
class LargeHostMemorySuite extends SparkQueryCompareTestSuite {
  import LargeHostMemorySuite._

  largeTest("a string column accepts 1 MiB values up to its byte limit and rejects the next " +
      "one without changing") {
    checkStringByteLimit(ONE_MIB, ROWS_UNDER_LIMIT, END_OFFSET)
  }

  largeTest("a binary column accepts bytes up to its element limit and rejects the next value " +
      "without changing") {
    val value = newValue(ONE_MIB)
    withRecordedHostAllocations { allocations =>
      withResource(new RapidsHostColumnBuilder(BINARY, 1)) { builder =>
        (0 until ROWS_UNDER_LIMIT).foreach { row =>
          writeRowPrefix(value, row)
          builder.appendByteList(value)
        }
        writeRowPrefix(value, ROWS_UNDER_LIMIT)
        assertRejected(builder, ELEMENT_COUNT, END_OFFSET + ONE_MIB, MAX_ELEMENTS,
          DType.UINT8)(builder.appendByteList(value))
        builder.appendByteList(newValue((MAX_ELEMENTS - END_OFFSET).toInt))
        assertRejected(builder, ELEMENT_COUNT, MAX_ELEMENTS + 1, MAX_ELEMENTS,
          DType.UINT8)(builder.appendByteList(newValue(1)))
        withResource(builder.build()) { column =>
          assertResult(ROWS_UNDER_LIMIT + 1L)(column.getRowCount)
          assertResult(END_OFFSET)(column.getStartListOffset(ROWS_UNDER_LIMIT))
          assertResult(MAX_ELEMENTS)(column.getEndListOffset(ROWS_UNDER_LIMIT))
        }
      }
      assertAllClosed(allocations)
      assertLargestAtMost(allocations, MAX_ELEMENTS)
    }
  }

  largeTest("a cached string partition past 2 GiB splits at the column limit and reads back " +
      "whole on every path") {
    checkLimitCrossingCache(StringType)
  }

  largeTest("a cached binary partition past 2 GiB splits at the column limit and reads back " +
      "whole on every path") {
    checkLimitCrossingCache(BinaryType)
  }

  largeTest("two 1.2 GiB string columns cache as one host batch, sliced by the compression " +
      "budget") {
    checkTwoStringColumnCache(ROWS_1_2_GIB, Seq(ROWS_1_2_GIB), new SparkConf())
  }

  largeTest("two 2.2 GiB string columns cache split where the second reaches its limit, with " +
      "the crossing row whole in the second batch") {
    checkTwoStringColumnCache(ROWS_2_2_GIB,
      Seq(ROWS_UNDER_LIMIT, ROWS_2_2_GIB - ROWS_UNDER_LIMIT), new SparkConf())
  }

  // At 3g the setting's own budget, 0.995 of it, is above the GPU ceiling, so the ceiling is
  // what cuts the slices: without it they would be larger than this test expects.
  largeTest("two 2.2 GiB string columns cached at batchSizeBytes=3g slice by the GPU ceiling " +
      "and keep every payload under the array limit") {
    checkTwoStringColumnCache(ROWS_2_2_GIB,
      Seq(ROWS_UNDER_LIMIT, ROWS_2_2_GIB - ROWS_UNDER_LIMIT),
      new SparkConf().set(RapidsConf.GPU_BATCH_SIZE_BYTES.key, "3g"))
  }

  /** Registers a test that runs only when opted in, and only with assertions enabled. */
  private def largeTest(name: String)(body: => Unit)(implicit pos: Position): Unit =
    test(name) {
      assume(largeHostMemoryEnabled, SKIP_REASON)
      assert(classOf[RapidsHostColumnBuilder].desiredAssertionStatus(),
        "RapidsHostColumnBuilder must run with assertions enabled (-ea)")
      assert(classOf[MemoryBuffer].desiredAssertionStatus(),
        "cuDF's MemoryBuffer must run with assertions enabled (-ea)")
      body
    }

  private def largeHostMemoryEnabled: Boolean =
    SparkSessionHolder.sparkSession.sparkContext.getConf.getBoolean(ENABLED_KEY, false)

  private def checkStringByteLimit(valueBytes: Int, acceptedRows: Int, endOffset: Long): Unit = {
    assertResult(endOffset)(acceptedRows.toLong * valueBytes)
    val value = newValue(valueBytes)
    withRecordedHostAllocations { allocations =>
      withResource(new RapidsHostColumnBuilder(STRING, 1)) { builder =>
        (0 until acceptedRows).foreach { row =>
          writeRowPrefix(value, row)
          builder.appendUTF8String(value)
        }
        writeRowPrefix(value, acceptedRows)
        assertRejected(builder, STRING_BYTES, endOffset + valueBytes, MAX_STRING_BYTES,
          DType.STRING)(builder.appendUTF8String(value))
        builder.appendUTF8String(newValue((MAX_STRING_BYTES - endOffset).toInt))
        assertRejected(builder, STRING_BYTES, MAX_STRING_BYTES + 1, MAX_STRING_BYTES,
          DType.STRING)(builder.appendUTF8String(newValue(1)))
        withResource(builder.build()) { column =>
          assertResult(acceptedRows + 1L)(column.getRowCount)
          assertResult(endOffset)(stringOffset(column, acceptedRows))
          assertResult(MAX_STRING_BYTES)(stringOffset(column, acceptedRows + 1L))
          assert(hasRowPrefix(column.getUTF8(acceptedRows - 1L), acceptedRows - 1L))
        }
      }
      assertAllClosed(allocations)
      assertLargestAtMost(allocations, MAX_STRING_BYTES)
    }
  }

  private def checkLimitCrossingCache(dataType: DataType): Unit = {
    val columns = Seq(ValueColumn("c", dataType, ONE_MIB))
    val expected = expectedDigest(columns, LIMIT_CROSSING_ROWS)
    // The host build ends its first batch at the column limit, after 2047 rows, and the default
    // slice budget of 1,068,373,114 bytes cuts it every 1018 rows of 1,048,580 GPU bytes.
    val cachedRows = Seq(1018, 1018, 11, 253)
    withGpuSparkSession { spark =>
      setActiveSession(spark)
      withResource(new RowEntryCache(spark.sparkContext, TrampolineUtil.getSparkConf(spark),
          columns, LIMIT_CROSSING_ROWS)) { cache =>
        assertResult(cachedRows)(cache.batchRows)
        assertPayloadsFit(cache)
        val gpuScan = cache.gpuScan()
        assertResult(expected)(gpuScan.digest)
        assertResult(cachedRows)(gpuScan.batchRows)
        assertResult(expected)(cache.read(columns, cache.conf).digest)
        assertResult(expected)(cache.readRowsPluginOff(columns))
        assertResult(cachedRows)(cache.countRead())
      }
    }
  }

  private def checkTwoStringColumnCache(rows: Int, hostBatches: Seq[Int],
      conf: SparkConf): Unit = {
    val columns = Seq(ValueColumn("c1", StringType, ONE_MIB - 1),
      ValueColumn("c2", StringType, ONE_MIB))
    val expected = expectedDigest(columns, rows)
    withGpuSparkSession({ spark =>
      setActiveSession(spark)
      withResource(new RowEntryCache(spark.sparkContext, TrampolineUtil.getSparkConf(spark),
          columns, rows)) { cache =>
        val budget = math.min(cache.serializer.getBytesAllowedPerBatch(cache.conf),
          GPU_SLICE_BUDGET_CEILING)
        assertResult(hostBatches.flatMap(sliceRows(columns, _, budget)))(cache.batchRows)
        assertPayloadsFit(cache)
        val read = cache.read(columns, cache.conf)
        assertResult(expected)(read.digest)
        assertResult(cache.batchRows)(read.batchRows)
      }
    }, conf)
  }

  /** Runs `append`, which must be rejected at a limit before it changes `builder`. */
  private def assertRejected(
      builder: RapidsHostColumnBuilder,
      what: String,
      attempted: Long,
      limit: Long,
      columnType: DType)(append: => Any): Unit = {
    val before = builder.toString
    val e = intercept[ColumnLimitExceededException](append)
    assertResult(limitMessage(what, attempted, limit, columnType))(e.getMessage)
    assertResult(before)(builder.toString)
  }

  private def assertAllClosed(allocations: RecordingHostAllocator): Unit =
    assert(allocations.allClosed, "a host buffer allocated by the test is still open")

  private def assertLargestAtMost(allocations: RecordingHostAllocator, bytes: Long): Unit =
    assert(allocations.largest <= bytes,
      s"a host buffer of ${allocations.largest} bytes was requested, more than $bytes")

  private def assertPayloadsFit(cache: RowEntryCache): Unit =
    cache.payloadBytes.foreach { bytes =>
      assert(bytes > 0 && bytes <= ByteArrayMethods.MAX_ROUNDED_ARRAY_LENGTH,
        s"a cached batch of $bytes bytes does not fit in a Java array")
    }
}

object LargeHostMemorySuite {
  private val ENABLED_KEY = "spark.rapids.test.largeHostMemory.enabled"
  private val SKIP_REASON = s"set SPARK_CONF=$ENABLED_KEY=true to run; see tests/README.md"

  private val ONE_MIB = 1024 * 1024
  private val OFFSET_BYTES = DType.INT32.getSizeInBytes

  // The production limits, written out so that a change to them fails here.
  private val MAX_STRING_BYTES = 2147483647L
  private val MAX_ELEMENTS = 2147483646L

  // 2047 values of 1 MiB end at END_OFFSET, within both the string byte limit and the element
  // limit; the 2048th crosses both.
  private val ROWS_UNDER_LIMIT = 2047
  private val END_OFFSET = 2146435072L
  private val LIMIT_CROSSING_ROWS = 2300
  // Rows of about 1 MiB per column: 1.2 GiB and 2.2 GiB of each column.
  private val ROWS_1_2_GIB = 1229
  private val ROWS_2_2_GIB = 2253
  // The GPU writer's slice budget stays 10 MiB below 2 GiB.
  private val GPU_SLICE_BUDGET_CEILING = 2L * 1024 * 1024 * 1024 - 10L * 1024 * 1024

  private val STRING_BYTES = "The string data size in bytes"
  private val ELEMENT_COUNT = "The number of elements"

  private val STRING = new BasicType(false, DType.STRING)
  private val BINARY = new ListType(false, new BasicType(false, DType.UINT8))

  // Every value starts with its zero-padded row number, so a value in the wrong row is found.
  private val PREFIX_BYTES = 10
  private val FILLER = 'x'.toByte

  private def newValue(bytes: Int): Array[Byte] = {
    val value = new Array[Byte](bytes)
    Arrays.fill(value, FILLER)
    value
  }

  private def writeRowPrefix(value: Array[Byte], row: Long): Unit = {
    var rest = row
    var i = PREFIX_BYTES - 1
    while (i >= 0) {
      value(i) = ('0' + rest % 10).toByte
      rest /= 10
      i -= 1
    }
  }

  private def hasRowPrefix(value: Array[Byte], row: Long): Boolean = {
    var matches = value.length >= PREFIX_BYTES
    var rest = row
    var i = PREFIX_BYTES - 1
    while (matches && i >= 0) {
      matches = value(i) == ('0' + rest % 10).toByte
      rest /= 10
      i -= 1
    }
    matches
  }

  private def stringOffset(column: HostColumnVectorCore, row: Long): Long =
    column.getOffsets.getInt(row * OFFSET_BYTES).toLong

  /** A non-null column of `valueBytes`-byte values. */
  private case class ValueColumn(name: String, dataType: DataType, valueBytes: Int)

  /** Rows, value bytes, an order-sensitive CRC32 of the values and the misplaced rows. */
  private case class Digest(rows: Long, bytes: Long, crc: Long, misplacedRows: Long)

  /** The digest of what a read returned, and the row count of each batch it returned. */
  private case class ReadResult(digest: Digest, batchRows: Seq[Int])

  private final class DigestBuilder {
    private val crc = new CRC32
    private var rows = 0L
    private var bytes = 0L
    private var misplacedRows = 0L

    /** Adds the next row, which is misplaced if a value does not start with its row number. */
    def addRow(numColumns: Int)(value: Int => Array[Byte]): Unit = {
      var misplaced = false
      (0 until numColumns).foreach { c =>
        val v = value(c)
        crc.update(v, 0, v.length)
        bytes += v.length
        if (!hasRowPrefix(v, rows)) {
          misplaced = true
        }
      }
      if (misplaced) {
        misplacedRows += 1
      }
      rows += 1
    }

    def result: Digest = Digest(rows, bytes, crc.getValue, misplacedRows)
  }

  private def expectedDigest(columns: Seq[ValueColumn], rows: Int): Digest = {
    val values = columns.map(c => newValue(c.valueBytes)).toArray
    val digest = new DigestBuilder
    (0 until rows).foreach { row =>
      values.foreach(writeRowPrefix(_, row))
      digest.addRow(values.length)(values(_))
    }
    digest.result
  }

  /** The rows of `columns` as Spark's scans produce them: UnsafeRows sharing one buffer. */
  private def valueRows(columns: Seq[ValueColumn], rows: Int): Iterator[InternalRow] = {
    val values = columns.map(c => newValue(c.valueBytes)).toArray
    val row = new GenericInternalRow(values.length)
    values.indices.foreach { c =>
      val value = if (columns(c).dataType == StringType) {
        UTF8String.fromBytes(values(c))
      } else {
        values(c)
      }
      row.update(c, value)
    }
    val toUnsafe = UnsafeProjection.create(columns.map(_.dataType).toArray)
    Iterator.range(0, rows).map { r =>
      values.foreach(writeRowPrefix(_, r))
      toUnsafe(row)
    }
  }

  private def valueRowRdd(sc: SparkContext, columns: Seq[ValueColumn],
      rows: Int): RDD[InternalRow] =
    sc.parallelize(Seq(0), 1).mapPartitions(_ => valueRows(columns, rows))

  private def valueOf(column: ColumnVector, dataType: DataType, row: Int): Array[Byte] =
    dataType match {
      case StringType => column.getUTF8String(row).getBytes
      case BinaryType => column.getBinary(row)
      case other => throw new IllegalArgumentException(s"unexpected value type $other")
    }

  private def addBatch(digest: DigestBuilder, batch: ColumnarBatch,
      columns: Seq[ValueColumn]): Unit =
    (0 until batch.numRows()).foreach { row =>
      digest.addRow(columns.length)(c => valueOf(batch.column(c), columns(c).dataType, row))
    }

  private def hostCopy(gpuBatch: ColumnarBatch): ColumnarBatch = {
    val hostColumns = GpuColumnVector.extractColumns(gpuBatch).safeMap(_.copyToHost())
    new ColumnarBatch(hostColumns.toArray[ColumnVector], gpuBatch.numRows())
  }

  private def digestBatches(batches: Iterator[ColumnarBatch], columns: Seq[ValueColumn],
      onGpu: Boolean): ReadResult = {
    val digest = new DigestBuilder
    val batchRows = ArrayBuffer[Int]()
    batches.foreach { batch =>
      batchRows += batch.numRows()
      if (onGpu) {
        withResource(batch) { _ =>
          withResource(hostCopy(batch))(addBatch(digest, _, columns))
        }
      } else {
        // The producer closes its host batches itself.
        addBatch(digest, batch, columns)
      }
    }
    ReadResult(digest.result, batchRows.toList)
  }

  private def readHostBatches(batches: RDD[ColumnarBatch],
      columns: Seq[ValueColumn]): ReadResult =
    batches.mapPartitions(it => Iterator(digestBatches(it, columns, onGpu = false)))
      .collect().head

  private def readGpuBatches(batches: RDD[ColumnarBatch],
      columns: Seq[ValueColumn]): ReadResult =
    batches.mapPartitions(it => Iterator(digestBatches(it, columns, onGpu = true)))
      .collect().head

  private def batchLayout(cached: RDD[CachedBatch]): Seq[(Int, Long)] =
    cached.map(b => (b.numRows, b.sizeInBytes)).collect().toSeq

  private def batchRowCounts(batches: RDD[ColumnarBatch]): Seq[Int] =
    batches.map(_.numRows()).collect().toSeq

  private def pluginOff(conf: SQLConf): SQLConf = {
    val off = conf.clone()
    off.setConfString(RapidsConf.SQL_ENABLED.key, "false")
    off
  }

  private def rowValueOf(row: InternalRow, ordinal: Int, dataType: DataType): Array[Byte] =
    dataType match {
      case StringType => row.getUTF8String(ordinal).getBytes
      case BinaryType => row.getBinary(ordinal)
      case other => throw new IllegalArgumentException(s"unexpected value type $other")
    }

  /**
   * The row counts compressColumnarBatchWithParquet slices a host batch of `rows` rows into:
   * the budget over the batch's GPU bytes per row, where a non-null string column holds its
   * values and rows + 1 Int offsets.
   */
  private def sliceRows(columns: Seq[ValueColumn], rows: Int, budget: Long): Seq[Int] = {
    val rowBytes = columns.map { c =>
      (rows.toLong * c.valueBytes + (rows + 1L) * OFFSET_BYTES) / rows
    }.sum
    val perSlice = math.max(1, (budget / rowBytes).toInt)
    Seq.fill(rows / perSlice)(perSlice) ++ Seq(rows % perSlice).filter(_ > 0)
  }

  /**
   * One partition of value rows cached through the serializer's row entry and persisted, so
   * that every read decodes the same cached batches. Closing it drops them.
   */
  private final class RowEntryCache(
      sc: SparkContext,
      val conf: SQLConf,
      columns: Seq[ValueColumn],
      rows: Int) extends AutoCloseable {
    val serializer = new ParquetCachedBatchSerializer
    private val attributes: Seq[Attribute] =
      columns.map(c => AttributeReference(c.name, c.dataType, nullable = false)())
    private val cached: RDD[CachedBatch] = serializer.convertInternalRowToCachedBatch(
      valueRowRdd(sc, columns, rows), attributes, StorageLevel.MEMORY_ONLY, conf)
      .persist(StorageLevel.MEMORY_ONLY)
    private lazy val layout = batchLayout(cached)

    def batchRows: Seq[Int] = layout.map(_._1)

    def payloadBytes: Seq[Long] = layout.map(_._2)

    /** Decodes on the GPU, as the GPU in-memory table scan does. */
    def gpuScan(): ReadResult = readGpuBatches(
      serializer.gpuConvertCachedBatchToColumnarBatch(cached, attributes, attributes, conf),
      columns)

    /** Decodes the selected columns to host batches, with the plugin on or off per `readConf`. */
    def read(selected: Seq[ValueColumn], readConf: SQLConf): ReadResult = {
      val selectedAttributes = attributes.filter(a => selected.exists(_.name == a.name))
      readHostBatches(serializer.convertCachedBatchToColumnarBatch(cached, attributes,
        selectedAttributes, readConf), selected)
    }

    /** Decodes the selected columns to rows with the plugin off, as Spark's CPU scan of it does. */
    def readRowsPluginOff(selected: Seq[ValueColumn]): Digest = {
      val selectedAttributes = attributes.filter(a => selected.exists(_.name == a.name))
      serializer.convertCachedBatchToInternalRow(cached, attributes, selectedAttributes,
        pluginOff(conf)).mapPartitions { rows =>
        val digest = new DigestBuilder
        rows.foreach { row =>
          digest.addRow(selected.length)(c => rowValueOf(row, c, selected(c).dataType))
        }
        Iterator(digest.result)
      }.collect().head
    }

    /** The row count of each batch that an empty projection, as count() uses, reads. */
    def countRead(): Seq[Int] = batchRowCounts(
      serializer.convertCachedBatchToColumnarBatch(cached, attributes, Seq.empty, conf))

    override def close(): Unit = cached.unpersist(blocking = true)
  }
}
