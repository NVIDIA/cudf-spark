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

/*** spark-rapids-shim-json-lines
{"spark": "350"}
{"spark": "351"}
{"spark": "352"}
{"spark": "353"}
{"spark": "354"}
{"spark": "355"}
{"spark": "356"}
{"spark": "357"}
{"spark": "358"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.iceberg.parquet.staged

import java.nio.file.Files
import java.util.{Collections, Iterator => JIterator, List => JList}
import java.util.concurrent.{
  ConcurrentLinkedQueue,
  CountDownLatch,
  Executors,
  Semaphore,
  TimeUnit
}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicReference}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import ai.rapids.cudf.HostMemoryBuffer
import com.nvidia.spark.rapids.{
  CombineConf,
  DateTimeRebaseCorrected,
  HostAlloc,
  ThreadPoolConfBuilder
}
import com.nvidia.spark.rapids.iceberg.parquet.{
  GpuIcebergParquetReaderConf,
  GpuStagedIcebergParquetReader,
  IcebergPartitionedFile,
  MultiThread
}
import com.nvidia.spark.rapids.jni.fileio.{RapidsInputFile, SeekableInputStream}
import com.nvidia.spark.rapids.shims.PartitionedFileUtilsShim
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.iceberg.Schema
import org.apache.iceberg.types.{Types => IcebergTypes}
import org.apache.parquet.column.{Encoding, EncodingStats}
import org.apache.parquet.column.statistics.Statistics
import org.apache.parquet.hadoop.metadata.{
  BlockMetaData,
  ColumnChunkMetaData,
  ColumnPath,
  CompressionCodecName
}
import org.apache.parquet.schema.{MessageType, Types}
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.vectorized.ColumnarBatch

class StagedParquetPlannerSuite extends AnyFunSuite with BeforeAndAfterEach {

  override protected def afterEach(): Unit = {
    try {
      StagedScanThreadPools.resetForTesting()
    } finally {
      super.afterEach()
    }
  }

  private case class ColumnSpec(
      name: String,
      firstDataPageOffset: Long,
      dictionaryPageOffset: Long,
      compressedSize: Long,
      uncompressedSize: Long)

  private val oneColumnParquetSchema = Types.buildMessage()
    .addField(Types.required(INT32).named("id"))
    .named("staged_test")

  private val oneColumnReadSchema = StructType(Seq(
    StructField("id", IntegerType, nullable = false)))

  private val twoColumnParquetSchema = Types.buildMessage()
    .addField(Types.required(INT32).named("id"))
    .addField(Types.required(INT32).named("value"))
    .named("staged_test")

  private val twoColumnReadSchema = StructType(Seq(
    StructField("id", IntegerType, nullable = false),
    StructField("value", IntegerType, nullable = false)))

  private def column(
      schema: MessageType,
      rows: Long,
      spec: ColumnSpec): ColumnChunkMetaData = {
    val primitive = schema.getFields.asScala
      .find(_.getName == spec.name)
      .getOrElse(throw new IllegalArgumentException(s"unknown column ${spec.name}"))
      .asPrimitiveType()
    val encodingStats = new EncodingStats.Builder()
      .addDataEncoding(Encoding.PLAIN)
      .build()
    ColumnChunkMetaData.get(
      ColumnPath.get(spec.name),
      primitive,
      CompressionCodecName.UNCOMPRESSED,
      encodingStats,
      Collections.singleton(Encoding.PLAIN),
      Statistics.createStats(primitive),
      spec.firstDataPageOffset,
      spec.dictionaryPageOffset,
      rows,
      spec.compressedSize,
      spec.uncompressedSize)
  }

  private def block(
      schema: MessageType,
      rows: Long,
      specs: ColumnSpec*): BlockMetaData = {
    val result = new BlockMetaData()
    result.setRowCount(rows)
    specs.foreach(spec => result.addColumn(column(schema, rows, spec)))
    result.setTotalByteSize(specs.map(_.uncompressedSize).sum)
    result
  }

  private def oneColumnBlock(rows: Long, offset: Long, size: Long): BlockMetaData = {
    block(oneColumnParquetSchema, rows,
      ColumnSpec("id", offset, 0L, size, size * 2))
  }

  private def footer(
      ordinal: Int,
      schema: MessageType,
      readSchema: StructType,
      blocks: Seq[BlockMetaData]): FooterResult[String] = {
    footerWithOpener(
      ordinal,
      schema,
      readSchema,
      blocks,
      new InputFileOpener {
        override def open(): RapidsInputFile =
          throw new UnsupportedOperationException("planner tests do not perform I/O")
      })
  }

  private def footerWithOpener(
      ordinal: Int,
      schema: MessageType,
      readSchema: StructType,
      blocks: Seq[BlockMetaData],
      opener: InputFileOpener): FooterResult[String] = {
    val path = new Path(s"file:///tmp/staged-planner-$ordinal.parquet")
    val partitionedFile = PartitionedFileUtilsShim.newPartitionedFile(
      InternalRow.empty, path.toString, 0L, Long.MaxValue)
    val source = new StagedFileSource(
      ordinal,
      path,
      partitionedFile,
      opener)
    val firstRowIndices = blocks
      .scanLeft(0L)((index, current) => index + current.getRowCount)
      .dropRight(1)
      .map(Long.box)
      .asJava
    new FooterResult[String](
      source,
      blocks.asJava,
      schema,
      readSchema,
      firstRowIndices,
      DateTimeRebaseCorrected,
      DateTimeRebaseCorrected,
      false,
      s"context-$ordinal")
  }

  private def compatible(value: Boolean): FooterCompatibility[String] =
    new FooterCompatibility[String] {
      override def canCombine(
          existing: FooterResult[String],
          candidate: FooterResult[String]): Boolean = value
    }

  private def planShape(plan: PartitionReadPlan[String]): Seq[(Long, Long, Seq[Int])] = {
    plan.getSubtasks.asScala.map { subtask =>
      val sources = subtask.getSegments.asScala.map(_.getFooter.getSource.getOrdinal)
      (subtask.getSubtaskId, subtask.getRowCount, sources.toSeq)
    }.toSeq
  }

  /**
   * Records the exact vectored requests made by an output and fills their destinations from a
   * byte array. open() deliberately fails so these tests also catch a serial-stream fallback.
   */
  private final class RecordingVectoredInput(sourceBytes: Array[Byte]) extends RapidsInputFile {
    val vectoredCalls = ArrayBuffer.empty[Seq[(Long, Long, Long)]]
    var openCalls = 0

    override def getLength(): Long = sourceBytes.length

    override def readVectored(
        output: HostMemoryBuffer,
        copyRanges: JList[RapidsInputFile.CopyRange]): Unit = {
      vectoredCalls += copyRanges.asScala.map { range =>
        (range.getInputOffset, range.getLength, range.getOutputOffset)
      }.toSeq
      copyRanges.asScala.foreach { range =>
        output.setBytes(
          range.getOutputOffset,
          sourceBytes,
          range.getInputOffset,
          range.getLength)
      }
    }

    override def open(): SeekableInputStream = {
      openCalls += 1
      throw new AssertionError("staged output must use readVectored, not open")
    }
  }

  private final class SourceReadTracker(expectedSources: Int) {
    val activeCalls = new AtomicInteger()
    val maximumActiveCalls = new AtomicInteger()
    val startedCalls = new AtomicInteger()
    val calls = new ConcurrentLinkedQueue[(Int, Seq[(Long, Long, Long)])]()
    val firstTwoStarted = new CountDownLatch(2)
    val thirdStarted = new CountDownLatch(1)
    val allStarted = new CountDownLatch(expectedSources)
    val firstFinished = new CountDownLatch(1)
    val allFinished = new CountDownLatch(expectedSources)
    val writeFailures = new ConcurrentLinkedQueue[Throwable]()
    private val releases = new Semaphore(0)

    def start(sourceOrdinal: Int, ranges: JList[RapidsInputFile.CopyRange]): Unit = {
      calls.add(sourceOrdinal -> ranges.asScala.map { range =>
        (range.getInputOffset, range.getLength, range.getOutputOffset)
      }.toSeq)
      val active = activeCalls.incrementAndGet()
      updateMaximum(active)
      val started = startedCalls.incrementAndGet()
      if (started <= 2) {
        firstTwoStarted.countDown()
      }
      if (started == 3) {
        thirdStarted.countDown()
      }
      allStarted.countDown()
    }

    def block(): Unit = releases.acquire()

    def finish(): Unit = {
      activeCalls.decrementAndGet()
      firstFinished.countDown()
      allFinished.countDown()
    }

    def release(count: Int): Unit = releases.release(count)

    private def updateMaximum(candidate: Int): Unit = {
      var previous = maximumActiveCalls.get()
      while (candidate > previous &&
          !maximumActiveCalls.compareAndSet(previous, candidate)) {
        previous = maximumActiveCalls.get()
      }
    }
  }

  private final class BlockingVectoredInput(
      sourceOrdinal: Int,
      sourceBytes: Array[Byte],
      tracker: SourceReadTracker) extends RapidsInputFile {
    override def getLength(): Long = sourceBytes.length

    override def readVectored(
        output: HostMemoryBuffer,
        copyRanges: JList[RapidsInputFile.CopyRange]): Unit = {
      tracker.start(sourceOrdinal, copyRanges)
      try {
        tracker.block()
        copyRanges.asScala.foreach { range =>
          output.setBytes(
            range.getOutputOffset,
            sourceBytes,
            range.getInputOffset,
            range.getLength)
        }
      } catch {
        case error: Throwable =>
          tracker.writeFailures.add(error)
          throw error
      } finally {
        tracker.finish()
      }
    }

    override def open(): SeekableInputStream =
      throw new AssertionError("staged output must use readVectored, not open")
  }

  private def recordingObserver(
      observed: ArrayBuffer[(PlannedReadRange, Seq[Byte])]) =
    new StagedParquetOutput.RangeCopyObserver {
      override def rangeCopied(range: PlannedReadRange, data: HostMemoryBuffer): Unit = {
        try {
          val bytes = new Array[Byte](Math.toIntExact(data.getLength))
          data.getBytes(bytes, 0L, 0L, bytes.length)
          observed += range -> bytes.toIndexedSeq
        } finally {
          data.close()
        }
      }
    }

  test("stable planning preserves input order and honors the row limit") {
    val file0 = footer(0, oneColumnParquetSchema, oneColumnReadSchema, Seq(
      oneColumnBlock(rows = 2L, offset = 100L, size = 10L),
      oneColumnBlock(rows = 3L, offset = 200L, size = 11L)))
    val file1 = footer(1, oneColumnParquetSchema, oneColumnReadSchema, Seq(
      oneColumnBlock(rows = 2L, offset = 300L, size = 12L)))
    val planner = new StableGreedyReadPlanner[String](
      5, Long.MaxValue, Long.MaxValue, compatible(value = true))

    val firstPlan = planner.plan(Seq(file0, file1).asJava)
    val secondPlan = planner.plan(Seq(file0, file1).asJava)
    val expected = Seq((0L, 5L, Seq(0)), (1L, 2L, Seq(1)))

    assert(planShape(firstPlan) === expected)
    assert(planShape(secondPlan) === expected)
    assert(firstPlan.getSubtasks.get(0).getSegments.get(0).getBlocks.size() === 2)
    assert(firstPlan.getTotalRows === 7L)
  }

  test("planner honors GPU limits and retains an oversized row group") {
    val gpuLimitedFooter = footer(0, oneColumnParquetSchema, oneColumnReadSchema, Seq(
      oneColumnBlock(rows = 3L, offset = 100L, size = 4L),
      oneColumnBlock(rows = 3L, offset = 200L, size = 4L)))

    // One required INT column is estimated at four bytes per row. Each three-row block fits
    // under 20 bytes, but their 24-byte combined estimate does not.
    val gpuLimited = new StableGreedyReadPlanner[String](
      Int.MaxValue, 20L, Long.MaxValue, compatible(value = true))
      .plan(Seq(gpuLimitedFooter).asJava)
    assert(gpuLimited.getSubtasks.asScala.map(_.getRowCount) === Seq(3L, 3L))

    val finalOutputEstimator = new FooterGpuSizeEstimator[String] {
      override def estimate(footer: FooterResult[String], rowCount: Long): Long = rowCount * 10L
    }
    val evolvedOutputLimited = new StableGreedyReadPlanner[String](
      Int.MaxValue, 50L, Long.MaxValue, compatible(value = true), finalOutputEstimator)
      .plan(Seq(gpuLimitedFooter).asJava)
    assert(evolvedOutputLimited.getSubtasks.asScala.map(_.getRowCount) === Seq(3L, 3L))

    val oversizedFooter = footer(0, oneColumnParquetSchema, oneColumnReadSchema, Seq(
      oneColumnBlock(rows = 6L, offset = 100L, size = 10L),
      oneColumnBlock(rows = 1L, offset = 200L, size = 3L)))
    val oversized = new StableGreedyReadPlanner[String](
      5, Long.MaxValue, Long.MaxValue, compatible(value = true))
      .plan(Seq(oversizedFooter).asJava)
    assert(oversized.getSubtasks.asScala.map(_.getRowCount) === Seq(6L, 1L))
  }

  test("copied-data target is enforced only when crossing a source boundary") {
    val mib = 1024L * 1024L
    val target = 64L * mib
    val file0 = footer(0, oneColumnParquetSchema, oneColumnReadSchema, Seq(
      oneColumnBlock(rows = 1L, offset = 100L, size = 40L * mib)))
    val file1 = footer(1, oneColumnParquetSchema, oneColumnReadSchema, Seq(
      oneColumnBlock(rows = 1L, offset = 100L, size = 40L * mib),
      oneColumnBlock(rows = 1L, offset = 100L + 40L * mib, size = 40L * mib)))
    val file2 = footer(2, oneColumnParquetSchema, oneColumnReadSchema, Seq(
      oneColumnBlock(rows = 1L, offset = 100L, size = 1L * mib)))

    val plan = new StableGreedyReadPlanner[String](
      Int.MaxValue, Long.MaxValue, target, compatible(value = true))
      .plan(Seq(file0, file1, file2).asJava)

    // file0 is below the target, so file1 is admitted. Its second row group must stay with its
    // first even though the combined subtask has crossed 64 MiB. The target is consulted again
    // only at the boundary before file2.
    assert(planShape(plan) === Seq((0L, 3L, Seq(0, 1)), (1L, 1L, Seq(2))))
    assert(plan.getSubtasks.get(0).getSegments.get(1).getBlocks.size() === 2)
    assert(plan.getSubtasks.get(0).getLayout.getDataSizeBytes === 120L * mib)
    assert(plan.getSubtasks.get(1).getLayout.getDataSizeBytes === 1L * mib)
  }

  test("cross-file combination requires compatibility and a positive target") {
    val file0 = footer(0, oneColumnParquetSchema, oneColumnReadSchema, Seq(
      oneColumnBlock(rows = 2L, offset = 100L, size = 4L)))
    val file1 = footer(1, oneColumnParquetSchema, oneColumnReadSchema, Seq(
      oneColumnBlock(rows = 3L, offset = 200L, size = 5L)))

    val combined = new StableGreedyReadPlanner[String](
      Int.MaxValue, Long.MaxValue, 1024L, compatible(value = true))
      .plan(Seq(file0, file1).asJava)
    assert(planShape(combined) === Seq((0L, 5L, Seq(0, 1))))

    val incompatible = new StableGreedyReadPlanner[String](
      Int.MaxValue, Long.MaxValue, 1024L, compatible(value = false))
      .plan(Seq(file0, file1).asJava)
    assert(planShape(incompatible) === Seq((0L, 2L, Seq(0)), (1L, 3L, Seq(1))))

    val crossFileDisabled = new StableGreedyReadPlanner[String](
      Int.MaxValue, Long.MaxValue, 0L, compatible(value = true))
      .plan(Seq(file0, file1).asJava)
    assert(planShape(crossFileDisabled) ===
      Seq((0L, 2L, Seq(0)), (1L, 3L, Seq(1))))

    val sameFile = footer(2, oneColumnParquetSchema, oneColumnReadSchema, Seq(
      oneColumnBlock(rows = 2L, offset = 300L, size = 4L),
      oneColumnBlock(rows = 3L, offset = 400L, size = 5L)))
    val sameFilePlan = new StableGreedyReadPlanner[String](
      Int.MaxValue, Long.MaxValue, 0L, compatible(value = false))
      .plan(Seq(sameFile).asJava)
    assert(planShape(sameFilePlan) === Seq((0L, 5L, Seq(2))))
  }

  test("synthetic layout has exact contiguous ranges and relocated page offsets") {
    val file0 = footer(0, twoColumnParquetSchema, twoColumnReadSchema, Seq(
      block(twoColumnParquetSchema, rows = 2L,
        ColumnSpec("id", 104L, 100L, 12L, 18L),
        ColumnSpec("value", 300L, 0L, 20L, 24L))))
    val file1 = footer(1, twoColumnParquetSchema, twoColumnReadSchema, Seq(
      block(twoColumnParquetSchema, rows = 3L,
        ColumnSpec("id", 504L, 500L, 7L, 11L),
        ColumnSpec("value", 600L, 0L, 9L, 13L))))
    val plan = new StableGreedyReadPlanner[String](
      Int.MaxValue, Long.MaxValue, Long.MaxValue, compatible(value = true))
      .plan(Seq(file0, file1).asJava)
    val layout = plan.getSubtasks.get(0).getLayout
    val ranges = layout.getRanges.asScala

    assert(ranges.map(_.getSource.getOrdinal) === Seq(0, 0, 1, 1))
    assert(ranges.map(_.getInputOffset) === Seq(100L, 300L, 500L, 600L))
    assert(ranges.map(_.getLength) === Seq(12L, 20L, 7L, 9L))
    assert(ranges.map(_.getOutputOffset) === Seq(4L, 16L, 36L, 43L))
    assert(layout.getDataSizeBytes === 48L)
    assert(layout.getFooterOffset === 52L)
    assert(layout.getTotalSizeBytes === layout.getFooterOffset +
      layout.getFooterAndTrailerBytes.length)
    assert(layout.getHeaderBytes.toSeq === "PAR1".getBytes("UTF-8").toSeq)

    val adjustedColumns = layout.getAdjustedBlocks.asScala
      .flatMap(_.getColumns.asScala)
    assert(adjustedColumns.map(_.getStartingPos) === Seq(4L, 16L, 36L, 43L))
    assert(adjustedColumns.map(_.getFirstDataPageOffset) === Seq(8L, 16L, 40L, 43L))
    assert(adjustedColumns.map(_.getDictionaryPageOffset) === Seq(4L, 0L, 36L, 0L))

    val footerAndTrailer = layout.getFooterAndTrailerBytes
    assert(footerAndTrailer.takeRight(4).toSeq === "PAR1".getBytes("UTF-8").toSeq)
    val footerLengthOffset = footerAndTrailer.length - 8
    val encodedFooterLength =
      (footerAndTrailer(footerLengthOffset) & 0xff) |
        ((footerAndTrailer(footerLengthOffset + 1) & 0xff) << 8) |
        ((footerAndTrailer(footerLengthOffset + 2) & 0xff) << 16) |
        ((footerAndTrailer(footerLengthOffset + 3) & 0xff) << 24)
    assert(encodedFooterLength === footerLengthOffset)
  }

  test("memory output preserves one vectored CopyRange per column chunk") {
    val sourceBytes = (0 until 32).map(_.toByte).toArray
    val input = new RecordingVectoredInput(sourceBytes)
    val source = footer(10, oneColumnParquetSchema, oneColumnReadSchema, Seq.empty)
      .getSource
    // The first two ranges are adjacent in both input and output. They must still be sent as
    // distinct requests because each range represents one Parquet column chunk.
    val ranges = Seq(
      new PlannedReadRange(source, 1L, 3L, 2L),
      new PlannedReadRange(source, 4L, 2L, 5L),
      new PlannedReadRange(source, 9L, 4L, 10L))
    val observed = ArrayBuffer.empty[(PlannedReadRange, Seq[Byte])]
    val output = new MemoryStagedParquetOutput(HostMemoryBuffer.allocate(16L), 16L)

    try {
      output.copyRanges(input, ranges.asJava, 1, recordingObserver(observed))

      assert(input.vectoredCalls === Seq(Seq(
        (1L, 3L, 2L),
        (4L, 2L, 5L),
        (9L, 4L, 10L))))
      assert(input.openCalls === 0)
      assert(observed.map(_._1) === ranges)
      assert(observed.map(_._2) === ranges.map { range =>
        sourceBytes.slice(
          Math.toIntExact(range.getInputOffset),
          Math.toIntExact(range.getInputOffset + range.getLength)).toIndexedSeq
      })
    } finally {
      output.close()
    }
  }

  test("file output preserves one vectored request per source and materializes exact bytes") {
    val sourceBytes = (0 until 32).map(index => (index + 20).toByte).toArray
    val input = new RecordingVectoredInput(sourceBytes)
    val source = footer(11, oneColumnParquetSchema, oneColumnReadSchema, Seq.empty)
      .getSource
    val ranges = Seq(
      new PlannedReadRange(source, 1L, 3L, 2L),
      new PlannedReadRange(source, 4L, 2L, 5L),
      new PlannedReadRange(source, 9L, 4L, 10L))
    val observed = ArrayBuffer.empty[(PlannedReadRange, Seq[Byte])]
    val path = Files.createTempFile("staged-parquet-output-test-", ".parquet")
    val output = new FileStagedParquetOutput(path, 16L)

    try {
      // The writable mmap lets every column chunk for this source share one vectored request.
      // scratchBytes is intentionally smaller than their total to catch accidental batching.
      output.copyRanges(input, ranges.asJava, 5, recordingObserver(observed))
      output.writeBytes(0L, Array[Byte](1, 2), 0, 2)
      output.writeBytes(7L, Array[Byte](7, 8, 9), 0, 3)
      output.writeBytes(14L, Array[Byte](14, 15), 0, 2)
      output.seal(16L)

      assert(input.vectoredCalls === Seq(Seq(
        (1L, 3L, 2L),
        (4L, 2L, 5L),
        (9L, 4L, 10L))))
      assert(input.openCalls === 0)
      assert(observed.map(_._1) === ranges)

      val expected = Array.fill[Byte](16)(0)
      expected(0) = 1
      expected(1) = 2
      expected(7) = 7
      expected(8) = 8
      expected(9) = 9
      expected(14) = 14
      expected(15) = 15
      ranges.foreach { range =>
        Array.copy(
          sourceBytes,
          Math.toIntExact(range.getInputOffset),
          expected,
          Math.toIntExact(range.getOutputOffset),
          Math.toIntExact(range.getLength))
      }

      val materialized = output.materialize()
      try {
        // The task thread releases/unlinks the staged output before GPU decode. The owning slice
        // must keep the mmap valid independently on EMR/Linux.
        output.close()
        assert(!Files.exists(path))
        val actual = new Array[Byte](16)
        materialized.getBytes(actual, 0L, 0L, actual.length)
        assert(actual.toIndexedSeq === expected.toIndexedSeq)
      } finally {
        materialized.close()
      }
    } finally {
      output.close()
    }
    assert(!Files.exists(path))
  }

  test("memory output accepts concurrent source writes into disjoint ranges") {
    val sourceBytes = Array.tabulate[Byte](64)(index => (index + 10).toByte)
    val tracker = new SourceReadTracker(2)
    val inputs = Seq(
      new BlockingVectoredInput(0, sourceBytes, tracker),
      new BlockingVectoredInput(1, sourceBytes, tracker))
    val sources = Seq(
      footer(20, oneColumnParquetSchema, oneColumnReadSchema, Seq.empty).getSource,
      footer(21, oneColumnParquetSchema, oneColumnReadSchema, Seq.empty).getSource)
    val ranges = Seq(
      Seq(
        new PlannedReadRange(sources(0), 1L, 4L, 2L),
        new PlannedReadRange(sources(0), 8L, 3L, 6L)),
      Seq(
        new PlannedReadRange(sources(1), 20L, 5L, 12L),
        new PlannedReadRange(sources(1), 30L, 2L, 20L)))
    val writableBuffer = HostMemoryBuffer.allocate(32L)
    val output = new MemoryStagedParquetOutput(writableBuffer, 32L)
    val observer = new StagedParquetOutput.RangeCopyObserver {
      override def rangeCopied(range: PlannedReadRange, data: HostMemoryBuffer): Unit = data.close()
    }
    val workers = Executors.newFixedThreadPool(2)

    try {
      val writes = inputs.indices.map { index =>
        workers.submit(new Runnable {
          override def run(): Unit =
            output.copyRanges(inputs(index), ranges(index).asJava, 1, observer)
        })
      }

      assert(tracker.firstTwoStarted.await(10, TimeUnit.SECONDS))
      assert(tracker.activeCalls.get() === 2)
      assert(tracker.maximumActiveCalls.get() === 2)
      tracker.release(2)
      writes.foreach(_.get(10, TimeUnit.SECONDS))

      ranges.flatten.foreach { range =>
        val actual = new Array[Byte](Math.toIntExact(range.getLength))
        writableBuffer.getBytes(actual, 0L, range.getOutputOffset, actual.length)
        val expected = sourceBytes.slice(
          Math.toIntExact(range.getInputOffset),
          Math.toIntExact(range.getInputOffset + range.getLength))
        assert(actual.toIndexedSeq === expected.toIndexedSeq)
      }
    } finally {
      tracker.release(4)
      workers.shutdownNow()
      output.close()
    }
  }

  test("I/O pool bounds concurrent source reads without merging column-chunk ranges") {
    val sourceCount = 4
    val tracker = new SourceReadTracker(sourceCount)
    val sourceBytes = Array.tabulate[Byte](2048)(index => (index & 0xff).toByte)
    val footers = (0 until sourceCount).map { ordinal =>
      val input = new BlockingVectoredInput(ordinal, sourceBytes, tracker)
      val baseOffset = 100L + ordinal * 300L
      footerWithOpener(
        ordinal,
        twoColumnParquetSchema,
        twoColumnReadSchema,
        Seq(block(twoColumnParquetSchema, rows = 1L,
          ColumnSpec("id", baseOffset + 4L, baseOffset, 4L, 4L),
          ColumnSpec("value", baseOffset + 100L, 0L, 5L, 5L))),
        new InputFileOpener {
          override def open(): RapidsInputFile = input
        })
    }
    val scanFiles = footers.map { footerResult =>
      new StagedScanFile[Int](
        footerResult.getSource,
        footerResult.getSource.getOrdinal)
    }
    val adapter = new StagedScanAdapter[Int, String] {
      override def readAndFilterFooter(file: StagedScanFile[Int]): FooterResult[String] =
        footers(file.getFormatFile)

      override def canCombine(
          left: FooterResult[String],
          right: FooterResult[String]): Boolean = false

      override def decodeAndPostProcess(
          subtask: ReadSubtask[String],
          parquetData: HostMemoryBuffer): JIterator[ColumnarBatch] = {
        parquetData.close()
        Collections.emptyList[ColumnarBatch]().iterator()
      }

      override def closeContext(context: String): Unit = {}

      override def close(): Unit = {}
    }

    // Force local-file outputs so the test does not need to initialize the spill framework.
    HostAlloc.initialize(0L)
    val reader = new StagedParquetPartitionReader[Int, String](
      scanFiles.asJava,
      adapter,
      Int.MaxValue,
      Long.MaxValue,
      0L,
      sourceCount,
      2,
      5,
      null)
    val caller = Executors.newSingleThreadExecutor()
    try {
      val hasNext = caller.submit(new java.util.concurrent.Callable[Boolean] {
        override def call(): Boolean = reader.hasNext()
      })

      assert(tracker.firstTwoStarted.await(10, TimeUnit.SECONDS))
      assert(tracker.activeCalls.get() === 2)
      assert(tracker.startedCalls.get() === 2)
      // Both I/O workers are blocked, so the executor cannot start another source job.
      assert(!tracker.thirdStarted.await(500, TimeUnit.MILLISECONDS))
      assert(tracker.maximumActiveCalls.get() === 2)

      tracker.release(1)
      assert(tracker.thirdStarted.await(10, TimeUnit.SECONDS))
      assert(tracker.activeCalls.get() === 2)
      assert(tracker.startedCalls.get() === 3)
      assert(tracker.maximumActiveCalls.get() === 2)

      tracker.release(sourceCount)
      assert(tracker.allStarted.await(10, TimeUnit.SECONDS))
      assert(!hasNext.get(10, TimeUnit.SECONDS))
      assert(tracker.activeCalls.get() === 0)
      assert(tracker.maximumActiveCalls.get() === 2)

      val calls = tracker.calls.asScala.toSeq.sortBy(_._1)
      assert(calls.map(_._1) === (0 until sourceCount))
      calls.foreach { case (ordinal, ranges) =>
        val baseOffset = 100L + ordinal * 300L
        // One source-level call retains both distinct column chunks at their final output offsets.
        assert(ranges === Seq(
          (baseOffset, 4L, 4L),
          (baseOffset + 100L, 5L, 8L)))
      }
    } finally {
      tracker.release(sourceCount * 2)
      reader.close()
      caller.shutdownNow()
      HostAlloc.initialize(-1L)
    }
  }

  test("one subtask reads source files concurrently and preserves column chunks") {
    val sourceCount = 2
    val tracker = new SourceReadTracker(sourceCount)
    val sourceBytes = Array.tabulate[Byte](1024)(index => (index & 0xff).toByte)
    val footers = (0 until sourceCount).map { ordinal =>
      val input = new BlockingVectoredInput(ordinal, sourceBytes, tracker)
      val baseOffset = 100L + ordinal * 300L
      footerWithOpener(
        ordinal,
        twoColumnParquetSchema,
        twoColumnReadSchema,
        Seq(block(twoColumnParquetSchema, rows = 1L,
          ColumnSpec("id", baseOffset + 4L, baseOffset, 4L, 4L),
          ColumnSpec("value", baseOffset + 100L, 0L, 5L, 5L))),
        new InputFileOpener {
          override def open(): RapidsInputFile = input
        })
    }
    val scanFiles = footers.map { footerResult =>
      new StagedScanFile[Int](footerResult.getSource, footerResult.getSource.getOrdinal)
    }
    val combinedBytesVerified = new AtomicBoolean()
    val adapter = new StagedScanAdapter[Int, String] {
      override def readAndFilterFooter(file: StagedScanFile[Int]): FooterResult[String] =
        footers(file.getFormatFile)

      override def canCombine(
          left: FooterResult[String],
          right: FooterResult[String]): Boolean = true

      override def decodeAndPostProcess(
          subtask: ReadSubtask[String],
          parquetData: HostMemoryBuffer): JIterator[ColumnarBatch] = {
        try {
          val layout = subtask.getLayout
          val header = new Array[Byte](layout.getHeaderBytes.length)
          parquetData.getBytes(header, 0L, 0L, header.length)
          assert(header.toIndexedSeq === layout.getHeaderBytes.toIndexedSeq)
          layout.getRanges.asScala.foreach { range =>
            val actual = new Array[Byte](Math.toIntExact(range.getLength))
            parquetData.getBytes(actual, 0L, range.getOutputOffset, actual.length)
            val expected = sourceBytes.slice(
              Math.toIntExact(range.getInputOffset),
              Math.toIntExact(range.getInputOffset + range.getLength))
            assert(actual.toIndexedSeq === expected.toIndexedSeq)
          }
          val footer = new Array[Byte](layout.getFooterAndTrailerBytes.length)
          parquetData.getBytes(footer, 0L, layout.getFooterOffset, footer.length)
          assert(footer.toIndexedSeq === layout.getFooterAndTrailerBytes.toIndexedSeq)
          combinedBytesVerified.set(true)
          Collections.emptyList[ColumnarBatch]().iterator()
        } finally {
          parquetData.close()
        }
      }

      override def closeContext(context: String): Unit = {}

      override def close(): Unit = {}
    }

    HostAlloc.initialize(0L)
    val reader = new StagedParquetPartitionReader[Int, String](
      scanFiles.asJava,
      adapter,
      Int.MaxValue,
      Long.MaxValue,
      Long.MaxValue,
      2,
      2,
      5,
      null)
    val caller = Executors.newSingleThreadExecutor()
    try {
      val hasNext = caller.submit(new java.util.concurrent.Callable[Boolean] {
        override def call(): Boolean = reader.hasNext()
      })

      assert(tracker.firstTwoStarted.await(10, TimeUnit.SECONDS))
      assert(tracker.startedCalls.get() === 2)
      assert(tracker.activeCalls.get() === 2)
      assert(tracker.maximumActiveCalls.get() === 2)

      tracker.release(1)
      assert(tracker.firstFinished.await(10, TimeUnit.SECONDS))
      assert(tracker.activeCalls.get() === 1)
      assert(!hasNext.isDone)
      assert(!combinedBytesVerified.get())

      tracker.release(1)
      assert(!hasNext.get(10, TimeUnit.SECONDS))
      assert(tracker.activeCalls.get() === 0)
      assert(tracker.writeFailures.isEmpty)
      assert(combinedBytesVerified.get())

      val calls = tracker.calls.asScala.toSeq.sortBy(_._1)
      assert(calls.map(_._1) === Seq(0, 1))
      assert(calls.head._2 === Seq(
        (100L, 4L, 4L),
        (200L, 5L, 8L)))
      assert(calls.last._2 === Seq(
        (400L, 4L, 13L),
        (500L, 5L, 17L)))
    } finally {
      tracker.release(sourceCount * 2)
      reader.close()
      caller.shutdownNow()
      HostAlloc.initialize(-1L)
    }
  }

  test("close leaves a shared output alive until every active source writer exits") {
    val sourceCount = 2
    val tracker = new SourceReadTracker(sourceCount)
    val sourceBytes = Array.tabulate[Byte](1024)(index => (index & 0xff).toByte)
    val footers = (0 until sourceCount).map { ordinal =>
      val input = new BlockingVectoredInput(ordinal, sourceBytes, tracker)
      val baseOffset = 100L + ordinal * 300L
      footerWithOpener(
        ordinal,
        twoColumnParquetSchema,
        twoColumnReadSchema,
        Seq(block(twoColumnParquetSchema, rows = 1L,
          ColumnSpec("id", baseOffset + 4L, baseOffset, 4L, 4L),
          ColumnSpec("value", baseOffset + 100L, 0L, 5L, 5L))),
        new InputFileOpener {
          override def open(): RapidsInputFile = input
        })
    }
    val scanFiles = footers.map { footerResult =>
      new StagedScanFile[Int](footerResult.getSource, footerResult.getSource.getOrdinal)
    }
    val decodeCalled = new AtomicBoolean()
    val adapter = new StagedScanAdapter[Int, String] {
      override def readAndFilterFooter(file: StagedScanFile[Int]): FooterResult[String] =
        footers(file.getFormatFile)

      override def canCombine(
          left: FooterResult[String],
          right: FooterResult[String]): Boolean = true

      override def decodeAndPostProcess(
          subtask: ReadSubtask[String],
          parquetData: HostMemoryBuffer): JIterator[ColumnarBatch] = {
        decodeCalled.set(true)
        parquetData.close()
        Collections.emptyList[ColumnarBatch]().iterator()
      }

      override def closeContext(context: String): Unit = {}

      override def close(): Unit = {}
    }

    HostAlloc.initialize(0L)
    val reader = new StagedParquetPartitionReader[Int, String](
      scanFiles.asJava,
      adapter,
      Int.MaxValue,
      Long.MaxValue,
      Long.MaxValue,
      2,
      2,
      5,
      null)
    val caller = Executors.newSingleThreadExecutor()
    try {
      val hasNext = caller.submit(new java.util.concurrent.Callable[Boolean] {
        override def call(): Boolean = reader.hasNext()
      })

      assert(tracker.firstTwoStarted.await(10, TimeUnit.SECONDS))
      assert(tracker.activeCalls.get() === 2)
      reader.close()
      assert(!hasNext.get(10, TimeUnit.SECONDS))
      assert(tracker.activeCalls.get() === 2)
      assert(!decodeCalled.get())

      // The fake inputs write only after they are released. Successful writes after close prove
      // that reclamation remains behind the all-source terminal barrier.
      tracker.release(2)
      assert(tracker.allFinished.await(10, TimeUnit.SECONDS))
      assert(tracker.writeFailures.isEmpty)
      assert(!decodeCalled.get())
    } finally {
      tracker.release(sourceCount * 2)
      reader.close()
      caller.shutdownNow()
      HostAlloc.initialize(-1L)
    }
  }

  test("staged decode support has a Hadoop configuration during trait initialization") {
    val hadoopConf = new Configuration(false)
    hadoopConf.setInt("parquet.read.allocation.size", 12345)
    val icebergSchema = new Schema(
      IcebergTypes.NestedField.required(1, "id", IcebergTypes.IntegerType.get()))
    val threadConf = MultiThread(
      new ThreadPoolConfBuilder(1, false, 0L, 0L, false),
      maxNumFilesProcessed = 1,
      CombineConf(1024L, 0),
      disableCombining = false,
      hasFilePathMetadata = false,
      hasRowPositionMetadata = false,
      queryUsesInputFile = false)
    val readerConf = GpuIcebergParquetReaderConf(
      caseSensitive = true,
      conf = hadoopConf,
      maxBatchSizeRows = 1024,
      maxBatchSizeBytes = 1024L * 1024L,
      targetBatchSizeBytes = 1024L * 1024L,
      maxGpuColumnSizeBytes = 1024L * 1024L,
      useChunkedReader = false,
      maxChunkedReaderMemoryUsageSizeBytes = 1024L * 1024L,
      parquetDebugDumpPrefix = None,
      parquetDebugDumpAlways = false,
      metrics = Map.empty,
      threadConf = threadConf,
      expectedSchema = icebergSchema,
      nameMapping = None)
    val reader = new GpuStagedIcebergParquetReader(
      rapidsFileIO = null,
      files = Seq.empty,
      constantsProvider = (_: IcebergPartitionedFile) =>
        Collections.emptyMap[Integer, Object](),
      conf = readerConf,
      cpuThreads = 1,
      ioThreads = 1)

    try {
      // Before the fix, this first access initialized DecodeSupport and failed because the
      // ParquetPartitionReaderBase trait constructor observed an uninitialized override val.
      assert(reader.stagedParquetOptions(oneColumnReadSchema, oneColumnParquetSchema) != null)
    } finally {
      reader.close()
    }
  }

  test("close unblocks a footer wait and defers adapter cleanup until the worker exits") {
    val input = footer(0, oneColumnParquetSchema, oneColumnReadSchema, Seq.empty)
    val scanFile = new StagedScanFile[String](input.getSource, "format-file")
    val footerStarted = new CountDownLatch(1)
    val releaseFooter = new CountDownLatch(1)
    val adapterClosed = new CountDownLatch(1)
    val footerThreadName = new AtomicReference[String]()
    val contextCloseCount = new AtomicInteger()
    val adapterWasClosed = new AtomicBoolean()
    val adapter = new StagedScanAdapter[String, String] {
      override def readAndFilterFooter(file: StagedScanFile[String]): FooterResult[String] = {
        footerThreadName.set(Thread.currentThread().getName)
        footerStarted.countDown()
        releaseFooter.await()
        input
      }

      override def canCombine(
          left: FooterResult[String],
          right: FooterResult[String]): Boolean = true

      override def decodeAndPostProcess(
          subtask: ReadSubtask[String],
          parquetData: HostMemoryBuffer): JIterator[ColumnarBatch] = {
        parquetData.close()
        throw new AssertionError("empty footer must not produce a decode subtask")
      }

      override def closeContext(context: String): Unit = contextCloseCount.incrementAndGet()

      override def close(): Unit = {
        adapterWasClosed.set(true)
        adapterClosed.countDown()
      }
    }
    val reader = new StagedParquetPartitionReader[String, String](
      Seq(scanFile).asJava,
      adapter,
      Int.MaxValue,
      Long.MaxValue,
      1L,
      1,
      1,
      1024,
      null)
    val caller = Executors.newSingleThreadExecutor()
    try {
      val hasNext = caller.submit(new java.util.concurrent.Callable[Boolean] {
        override def call(): Boolean = reader.hasNext()
      })
      assert(footerStarted.await(10, TimeUnit.SECONDS))
      assert(footerThreadName.get().startsWith("iceberg-staged-cpu-"))

      reader.close()
      assert(!hasNext.get(10, TimeUnit.SECONDS))
      assert(!adapterWasClosed.get())

      releaseFooter.countDown()
      assert(adapterClosed.await(10, TimeUnit.SECONDS))
      assert(contextCloseCount.get() === 1)
    } finally {
      releaseFooter.countDown()
      reader.close()
      caller.shutdownNow()
    }
  }
}
