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

import java.io.ByteArrayInputStream
import java.util.{Collections, IdentityHashMap, Iterator => JIterator, List => JList}
import java.util.concurrent.{
  ConcurrentLinkedQueue,
  CountDownLatch,
  Executors,
  TimeUnit
}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicReference}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import ai.rapids.cudf.HostMemoryBuffer
import com.nvidia.spark.rapids.{
  CombineConf,
  DateTimeRebaseCorrected,
  GpuDeviceManager,
  GpuSemaphore,
  HostAlloc,
  ScalableTaskCompletion,
  ThreadPoolConfBuilder
}
import com.nvidia.spark.rapids.fileio.iceberg.IcebergInputFile
import com.nvidia.spark.rapids.iceberg.parquet.{
  GpuIcebergParquetReaderConf,
  GpuParquetReaderPostProcessor,
  GpuStagedIcebergParquetReader,
  IcebergPartitionedFile,
  MultiThread
}
import com.nvidia.spark.rapids.jni.fileio.{RapidsInputFile, SeekableInputStream}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.iceberg.Schema
import org.apache.iceberg.hadoop.HadoopInputFile
import org.apache.iceberg.types.{Types => IcebergTypes}
import org.apache.parquet.column.{Encoding, EncodingStats}
import org.apache.parquet.column.statistics.Statistics
import org.apache.parquet.format.Util
import org.apache.parquet.hadoop.metadata.{
  BlockMetaData,
  ColumnChunkMetaData,
  ColumnPath,
  CompressionCodecName
}
import org.apache.parquet.schema.{MessageType, Types}
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{mock, when}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.{SparkConf, SparkEnv, TaskContext}
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.vectorized.ColumnarBatch

class StagedParquetPlannerSuite extends AnyFunSuite with BeforeAndAfterEach {

  private val sourceInputs = new IdentityHashMap[IcebergPartitionedFile, RapidsInputFile]()

  override protected def afterEach(): Unit = {
    try {
      StagedScanThreadPools.resetForTesting()
    } finally {
      sourceInputs.clear()
      super.afterEach()
    }
  }

  private abstract class TestAdapter extends StagedScanAdapter {
    override def openInputFile(file: IcebergPartitionedFile): RapidsInputFile = {
      val input = sourceInputs.get(file)
      require(input != null, s"missing test input for ${file.path}")
      input
    }
  }

  /** Materialize and concatenate the zero-copy segments for byte-layout assertions. */
  private def materializeParquetBytes(input: StagedParquetInput): Array[Byte] = {
    val buffers = input.materialize()
    try {
      concatenateParquetBuffers(buffers)
    } finally {
      buffers.foreach(_.close())
    }
  }

  private def concatenateParquetBuffers(buffers: Array[HostMemoryBuffer]): Array[Byte] = {
    val totalBytes = buffers.map(_.getLength).sum
    val bytes = new Array[Byte](Math.toIntExact(totalBytes))
    var outputOffset = 0L
    buffers.foreach { buffer =>
      val length = Math.toIntExact(buffer.getLength)
      buffer.getBytes(bytes, outputOffset, 0L, length)
      outputOffset += length
    }
    bytes
  }

  /** Derive the logical synthetic offsets without retaining runtime per-column range objects. */
  private case class LogicalRange(
      footer: FooterResult,
      inputOffset: Long,
      length: Long,
      outputOffset: Long)

  private def logicalRanges(subtask: ReadSubtask): Seq[LogicalRange] = {
    var outputOffset = subtask.getHeaderBytes.length.toLong
    subtask.getFileSlices.asScala.flatMap { slice =>
      slice.getBlocks.asScala.flatMap { block =>
        block.getColumns.asScala.map { column =>
          val range = LogicalRange(
            slice.getFooter,
            column.getStartingPos,
            column.getTotalSize,
            outputOffset)
          outputOffset += column.getTotalSize
          range
        }
      }
    }.toSeq
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
    val encodingStatsBuilder = new EncodingStats.Builder()
      .addDataEncoding(Encoding.PLAIN)
    // ParquetMetadataConverter serializes dictionary_page_offset only when EncodingStats says
    // that the chunk actually contains a dictionary page. Keep the synthetic source metadata
    // internally consistent so the footer round-trip below exercises dictionary relocation rather
    // than the converter's intentional omission of an impossible offset.
    if (spec.dictionaryPageOffset > 0) {
      encodingStatsBuilder.addDictEncoding(Encoding.PLAIN)
    }
    val encodingStats = encodingStatsBuilder.build()
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
      blocks: Seq[BlockMetaData]): FooterResult = {
    footerWithInput(
      ordinal,
      schema,
      readSchema,
      blocks,
      new RapidsInputFile {
        override def getLength(): Long = 0L

        override def open(): SeekableInputStream =
          throw new UnsupportedOperationException("planner tests do not perform I/O")
      })
  }

  private def footerWithInput(
      ordinal: Int,
      schema: MessageType,
      readSchema: StructType,
      blocks: Seq[BlockMetaData],
      input: RapidsInputFile): FooterResult = {
    val path = new Path(s"file:///tmp/staged-planner-$ordinal.parquet")
    val icebergInput = new IcebergInputFile(HadoopInputFile.fromPath(path, new Configuration()))
    val file = IcebergPartitionedFile(icebergInput)
    sourceInputs.put(file, input)
    val postProcessor = mock(classOf[GpuParquetReaderPostProcessor])
    when(postProcessor.compatibleForCombining(any[GpuParquetReaderPostProcessor]()))
      .thenReturn(true)
    new FooterResult(
      file,
      blocks.asJava,
      schema,
      readSchema,
      DateTimeRebaseCorrected,
      DateTimeRebaseCorrected,
      false,
      postProcessor)
  }

  private def sourceOrdinal(file: IcebergPartitionedFile): Int = {
    file.path.getName
      .stripPrefix("staged-planner-")
      .stripSuffix(".parquet")
      .toInt
  }

  private def planShape(plan: JList[ReadSubtask]): Seq[(Long, Long, Seq[Int])] = {
    plan.asScala.map { subtask =>
      val sources = subtask.getFileSlices.asScala.map(fileSlice =>
        sourceOrdinal(fileSlice.getFooter.getFile))
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
    val registeredCalls = new AtomicInteger()
    val calls = new ConcurrentLinkedQueue[(Int, Seq[(Long, Long, Long)])]()
    val threadNames = new ConcurrentLinkedQueue[String]()
    val firstStarted = new CountDownLatch(1)
    val firstTwoStarted = new CountDownLatch(2)
    val allStarted = new CountDownLatch(expectedSources)
    val firstFinished = new CountDownLatch(1)
    val allFinished = new CountDownLatch(expectedSources)
    val writeFailures = new ConcurrentLinkedQueue[Throwable]()
    private val pending = ArrayBuffer.empty[(Int, () => Unit)]

    def start(sourceOrdinal: Int, ranges: JList[RapidsInputFile.CopyRange]): Unit = {
      threadNames.add(Thread.currentThread().getName)
      calls.add(sourceOrdinal -> ranges.asScala.map { range =>
        (range.getInputOffset, range.getLength, range.getOutputOffset)
      }.toSeq)
      val active = activeCalls.incrementAndGet()
      updateMaximum(active)
      startedCalls.incrementAndGet()
    }

    def addPending(sourceOrdinal: Int, completion: () => Unit): Unit = {
      synchronized {
        pending += sourceOrdinal -> completion
      }
      // Signal only after release() can observe the completion. Signalling from start() leaves a
      // race where the test thread releases the currently registered requests before this source
      // has entered the pending queue.
      val registered = registeredCalls.incrementAndGet()
      firstStarted.countDown()
      if (registered <= 2) {
        firstTwoStarted.countDown()
      }
      allStarted.countDown()
    }

    def finish(): Unit = {
      activeCalls.decrementAndGet()
      firstFinished.countDown()
      allFinished.countDown()
    }

    def release(count: Int): Unit = {
      val completions = synchronized {
        val amount = Math.min(count, pending.size)
        val selected = pending.take(amount).map(_._2).toSeq
        pending.remove(0, amount)
        selected
      }
      completions.foreach(_())
    }

    def releaseOrdinal(sourceOrdinal: Int): Unit = {
      val completion = synchronized {
        val index = pending.indexWhere(_._1 == sourceOrdinal)
        if (index < 0) {
          throw new AssertionError(s"source $sourceOrdinal is not pending")
        }
        pending.remove(index)._2
      }
      completion()
    }

    private def updateMaximum(candidate: Int): Unit = {
      var previous = maximumActiveCalls.get()
      while (candidate > previous &&
          !maximumActiveCalls.compareAndSet(previous, candidate)) {
        previous = maximumActiveCalls.get()
      }
    }
  }

  /**
   * Blocks the calling reader thread inside readVectored — the synchronous shape the staged
   * reader now shares with the base reader — until the test releases this source.
   */
  private final class BlockingVectoredInput(
      sourceOrdinal: Int,
      sourceBytes: Array[Byte],
      tracker: SourceReadTracker) extends RapidsInputFile {
    override def getLength(): Long = sourceBytes.length

    override def readVectored(
        output: HostMemoryBuffer,
        copyRanges: JList[RapidsInputFile.CopyRange]): Unit = {
      tracker.start(sourceOrdinal, copyRanges)
      val released = new CountDownLatch(1)
      val failure = new AtomicReference[Throwable]()
      tracker.addPending(sourceOrdinal, () => {
        try {
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
            failure.set(error)
        } finally {
          tracker.finish()
          released.countDown()
        }
      })
      released.await()
      Option(failure.get()).foreach { error =>
        throw new RuntimeException("blocked vectored write failed", error)
      }
    }

    override def open(): SeekableInputStream =
      throw new AssertionError("staged output must use readVectored, not open")
  }

  private def recordCopiedRanges(
      output: StagedParquetOutput,
      ranges: Seq[PlannedReadRange],
      observed: ArrayBuffer[(PlannedReadRange, Seq[Byte])]): Unit = {
    ranges.foreach { range =>
      val data = output.sliceForCache(range.getOutputOffset, range.getLength)
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
    val planner = new StableGreedyReadPlanner(
      5, Long.MaxValue, Long.MaxValue)

    val firstPlan = planner.plan(Seq(file0, file1).asJava)
    val secondPlan = planner.plan(Seq(file0, file1).asJava)
    val expected = Seq((0L, 5L, Seq(0)), (1L, 2L, Seq(1)))

    assert(planShape(firstPlan) === expected)
    assert(planShape(secondPlan) === expected)
    assert(firstPlan.get(0).getFileSlices.get(0).getBlocks.size() === 2)
  }

  test("planner honors GPU limits and retains an oversized row group") {
    val gpuLimitedFooter = footer(0, oneColumnParquetSchema, oneColumnReadSchema, Seq(
      oneColumnBlock(rows = 3L, offset = 100L, size = 4L),
      oneColumnBlock(rows = 3L, offset = 200L, size = 4L)))

    // One required INT column is estimated at four bytes per row. Each three-row block fits
    // under 20 bytes, but their 24-byte combined estimate does not.
    val gpuLimited = new StableGreedyReadPlanner(
      Int.MaxValue, 20L, Long.MaxValue)
      .plan(Seq(gpuLimitedFooter).asJava)
    assert(gpuLimited.asScala.map(_.getRowCount) === Seq(3L, 3L))

    // GPU estimates use the physical footer read schema (one INT), not wider Iceberg output
    // schemas containing synthesized/missing columns. At 50 bytes both row groups fit together.
    val readSchemaSized = new StableGreedyReadPlanner(
      Int.MaxValue, 50L, Long.MaxValue)
      .plan(Seq(gpuLimitedFooter).asJava)
    assert(readSchemaSized.asScala.map(_.getRowCount) === Seq(6L))

    // At a complete-file boundary, the same reader-byte limit uses encoded host bytes. The head
    // file's one-row GPU estimate fits under 10 bytes, but its 15 encoded bytes close the group
    // before the compatible tail file can combine with it.
    val encodedHead = footer(1, oneColumnParquetSchema, oneColumnReadSchema, Seq(
      oneColumnBlock(rows = 1L, offset = 100L, size = 15L)))
    val encodedTail = footer(2, oneColumnParquetSchema, oneColumnReadSchema, Seq(
      oneColumnBlock(rows = 1L, offset = 200L, size = 1L)))
    val encodedLimited = new StableGreedyReadPlanner(
      Int.MaxValue, 10L, Long.MaxValue)
      .plan(Seq(encodedHead, encodedTail).asJava)
    assert(planShape(encodedLimited) === Seq((0L, 1L, Seq(1)), (1L, 1L, Seq(2))))

    val oversizedFooter = footer(0, oneColumnParquetSchema, oneColumnReadSchema, Seq(
      oneColumnBlock(rows = 6L, offset = 100L, size = 10L),
      oneColumnBlock(rows = 1L, offset = 200L, size = 3L)))
    val oversized = new StableGreedyReadPlanner(
      5, Long.MaxValue, Long.MaxValue)
      .plan(Seq(oversizedFooter).asJava)
    assert(oversized.asScala.map(_.getRowCount) === Seq(6L, 1L))
  }

  test("copied-data target combines complete files without splitting a file") {
    val mib = 1024L * 1024L
    val target = 64L * mib
    val file0 = footer(0, oneColumnParquetSchema, oneColumnReadSchema, Seq(
      oneColumnBlock(rows = 1L, offset = 100L, size = 40L * mib)))
    val file1 = footer(1, oneColumnParquetSchema, oneColumnReadSchema, Seq(
      oneColumnBlock(rows = 1L, offset = 100L, size = 40L * mib),
      oneColumnBlock(rows = 1L, offset = 100L + 40L * mib, size = 40L * mib)))
    val file2 = footer(2, oneColumnParquetSchema, oneColumnReadSchema, Seq(
      oneColumnBlock(rows = 1L, offset = 100L, size = 1L * mib)))

    val plan = new StableGreedyReadPlanner(
      Int.MaxValue, Long.MaxValue, target)
      .plan(Seq(file0, file1, file2).asJava)

    // The base reader combines complete file results. file0 is below the target, so all of file1
    // is admitted before the target is checked again; file1 is never split at a row-group
    // boundary merely because the combined bytes crossed 64 MiB.
    assert(planShape(plan) === Seq((0L, 3L, Seq(0, 1)), (1L, 1L, Seq(2))))
    assert(plan.get(0).getDataSizeBytes === 120L * mib)
    assert(plan.get(1).getDataSizeBytes === 1L * mib)
  }

  test("copied-data target does not split one large file") {
    val mib = 1024L * 1024L
    val target = 64L * mib
    val largeFile = footer(0, oneColumnParquetSchema, oneColumnReadSchema, Seq(
      oneColumnBlock(rows = 1L, offset = 100L, size = 40L * mib),
      oneColumnBlock(rows = 1L, offset = 100L + 40L * mib, size = 40L * mib),
      oneColumnBlock(rows = 1L, offset = 100L + 80L * mib, size = 40L * mib)))

    val plan = new StableGreedyReadPlanner(
      Int.MaxValue, Long.MaxValue, target)
      .plan(Seq(largeFile).asJava)
    assert(planShape(plan) === Seq((0L, 3L, Seq(0))))
    assert(plan.get(0).getDataSizeBytes === 120L * mib)

    // A row group larger than the target remains grouped with the rest of its file; only the
    // independent row/GPU limits are allowed to split one file.
    val oversized = footer(1, oneColumnParquetSchema, oneColumnReadSchema, Seq(
      oneColumnBlock(rows = 1L, offset = 100L, size = 100L * mib),
      oneColumnBlock(rows = 1L, offset = 100L + 100L * mib, size = 1L * mib)))
    val oversizedPlan = new StableGreedyReadPlanner(
      Int.MaxValue, Long.MaxValue, target)
      .plan(Seq(oversized).asJava)
    assert(planShape(oversizedPlan) === Seq((0L, 2L, Seq(1))))
    assert(oversizedPlan.get(0).getDataSizeBytes === 101L * mib)
  }

  test("cross-file combine admits the next complete file with a soft limit overshoot") {
    val head = footer(0, oneColumnParquetSchema, oneColumnReadSchema, Seq(
      oneColumnBlock(rows = 2L, offset = 100L, size = 4L)))
    val next = footer(1, oneColumnParquetSchema, oneColumnReadSchema, Seq(
      oneColumnBlock(rows = 2L, offset = 200L, size = 4L),
      oneColumnBlock(rows = 2L, offset = 300L, size = 4L)))

    // In isolation, the four-row file is split by the three-row reader limit.
    val isolated = new StableGreedyReadPlanner(
      3, Long.MaxValue, Long.MaxValue)
      .plan(Seq(next).asJava)
    assert(planShape(isolated) === Seq((0L, 2L, Seq(1)), (1L, 2L, Seq(1))))

    // With a two-row file already selected below every limit, the base reader admits the next
    // complete file result before checking again. The combined result therefore soft-overshoots
    // the row limit atomically instead of splitting the newly admitted file midway.
    val combined = new StableGreedyReadPlanner(
      3, Long.MaxValue, Long.MaxValue)
      .plan(Seq(head, next).asJava)
    assert(planShape(combined) === Seq((0L, 6L, Seq(0, 1))))
  }

  test("incremental planning emits subtasks before later footers are fed") {
    val mib = 1024L * 1024L
    val target = 64L * mib
    val largeFile = footer(0, oneColumnParquetSchema, oneColumnReadSchema, Seq(
      oneColumnBlock(rows = 1L, offset = 100L, size = 40L * mib),
      oneColumnBlock(rows = 1L, offset = 100L + 40L * mib, size = 40L * mib),
      oneColumnBlock(rows = 1L, offset = 100L + 80L * mib, size = 40L * mib)))
    val tail = footer(1, oneColumnParquetSchema, oneColumnReadSchema, Seq(
      oneColumnBlock(rows = 1L, offset = 100L, size = 1L * mib)))
    val planner = new StableGreedyReadPlanner(
      Int.MaxValue, Long.MaxValue, target)

    val session = planner.newSession()
    val fromLarge = session.add(largeFile)
    // The target is checked after the complete file, so the whole large file is published while
    // only its footer has been fed.
    assert(planShape(fromLarge) === Seq((0L, 3L, Seq(0))))
    val fromTail = session.add(tail)
    assert(fromTail.isEmpty)
    val fin = session.finish()
    assert(planShape(fin) === Seq((1L, 1L, Seq(1))))

    // Streamed planning is identical to full-barrier planning.
    val batch = planner.plan(Seq(largeFile, tail).asJava)
    assert(planShape(batch) ===
      planShape((fromLarge.asScala ++ fromTail.asScala ++ fin.asScala).asJava))
  }

  test("cross-file combination requires compatibility and a positive target") {
    val file0 = footer(0, oneColumnParquetSchema, oneColumnReadSchema, Seq(
      oneColumnBlock(rows = 2L, offset = 100L, size = 4L)))
    val file1 = footer(1, oneColumnParquetSchema, oneColumnReadSchema, Seq(
      oneColumnBlock(rows = 3L, offset = 200L, size = 5L)))

    val combined = new StableGreedyReadPlanner(
      Int.MaxValue, Long.MaxValue, 1024L)
      .plan(Seq(file0, file1).asJava)
    assert(planShape(combined) === Seq((0L, 5L, Seq(0, 1))))

    val incompatibleFile = footer(1, twoColumnParquetSchema, twoColumnReadSchema, Seq(
      block(twoColumnParquetSchema, rows = 3L,
        ColumnSpec("id", 200L, 0L, 5L, 5L),
        ColumnSpec("value", 300L, 0L, 5L, 5L))))
    val incompatible = new StableGreedyReadPlanner(
      Int.MaxValue, Long.MaxValue, 1024L)
      .plan(Seq(file0, incompatibleFile).asJava)
    assert(planShape(incompatible) === Seq((0L, 2L, Seq(0)), (1L, 3L, Seq(1))))

    val crossFileDisabled = new StableGreedyReadPlanner(
      Int.MaxValue, Long.MaxValue, 0L)
      .plan(Seq(file0, file1).asJava)
    assert(planShape(crossFileDisabled) ===
      Seq((0L, 2L, Seq(0)), (1L, 3L, Seq(1))))

    val sameFile = footer(2, oneColumnParquetSchema, oneColumnReadSchema, Seq(
      oneColumnBlock(rows = 2L, offset = 300L, size = 4L),
      oneColumnBlock(rows = 3L, offset = 400L, size = 5L)))
    val sameFilePlan = new StableGreedyReadPlanner(
      Int.MaxValue, Long.MaxValue, 0L)
      .plan(Seq(sameFile).asJava)
    assert(planShape(sameFilePlan) === Seq((0L, 5L, Seq(2))))
  }

  test("distinct footer occurrences remain distinct when they share one Iceberg file") {
    val first = footer(0, oneColumnParquetSchema, oneColumnReadSchema, Seq(
      oneColumnBlock(rows = 2L, offset = 100L, size = 4L)))
    val second = new FooterResult(
      first.getFile,
      Seq(oneColumnBlock(rows = 3L, offset = 200L, size = 5L)).asJava,
      first.getClippedSchema,
      first.getReadSchema,
      first.getDateRebaseMode,
      first.getTimestampRebaseMode,
      first.hasInt96Timestamps(),
      first.getPostProcessor)

    assert(first.getFile eq second.getFile)
    val plan = new StableGreedyReadPlanner(
      Int.MaxValue, Long.MaxValue, 0L)
      .plan(Seq(first, second).asJava)
    assert(planShape(plan) === Seq((0L, 2L, Seq(0)), (1L, 3L, Seq(0))))
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
    val plan = new StableGreedyReadPlanner(
      Int.MaxValue, Long.MaxValue, Long.MaxValue)
      .plan(Seq(file0, file1).asJava)
    val layout = plan.get(0)
    val ranges = logicalRanges(layout)

    assert(ranges.map(range => sourceOrdinal(range.footer.getFile)) === Seq(0, 0, 1, 1))
    assert(ranges.map(_.inputOffset) === Seq(100L, 300L, 500L, 600L))
    assert(ranges.map(_.length) === Seq(12L, 20L, 7L, 9L))
    assert(ranges.map(_.outputOffset) === Seq(4L, 16L, 36L, 43L))
    assert(layout.getDataSizeBytes === 48L)
    assert(layout.getFooterOffset === 52L)
    assert(layout.getTotalSizeBytes === layout.getFooterOffset +
      layout.getFooterAndTrailerBytes.length)
    assert(layout.getHeaderBytes.toSeq === "PAR1".getBytes("UTF-8").toSeq)

    val footerAndTrailer = layout.getFooterAndTrailerBytes
    assert(footerAndTrailer.takeRight(4).toSeq === "PAR1".getBytes("UTF-8").toSeq)
    val footerLengthOffset = footerAndTrailer.length - 8
    val encodedFooterLength =
      (footerAndTrailer(footerLengthOffset) & 0xff) |
        ((footerAndTrailer(footerLengthOffset + 1) & 0xff) << 8) |
        ((footerAndTrailer(footerLengthOffset + 2) & 0xff) << 16) |
        ((footerAndTrailer(footerLengthOffset + 3) & 0xff) << 24)
    assert(encodedFooterLength === footerLengthOffset)

    val thriftFooter = Util.readFileMetaData(
      new ByteArrayInputStream(footerAndTrailer, 0, encodedFooterLength))
    val thriftColumns = thriftFooter.getRow_groups.asScala
      .flatMap(_.getColumns.asScala)
      .map(_.getMeta_data)
    assert(thriftColumns.map(_.getData_page_offset) === Seq(8L, 16L, 40L, 43L))
    assert(thriftColumns.map(column =>
      if (column.isSetDictionary_page_offset) column.getDictionary_page_offset else 0L) ===
      Seq(4L, 0L, 36L, 0L))
  }

  test("memory output preserves one vectored CopyRange per column chunk") {
    val sourceBytes = (0 until 32).map(_.toByte).toArray
    val input = new RecordingVectoredInput(sourceBytes)
    val source = footer(10, oneColumnParquetSchema, oneColumnReadSchema, Seq.empty)
    // The first two ranges are adjacent in both input and output. They must still be sent as
    // distinct requests because each range represents one Parquet column chunk.
    val ranges = Seq(
      new PlannedReadRange(source, 1L, 3L, 2L),
      new PlannedReadRange(source, 4L, 2L, 5L),
      new PlannedReadRange(source, 9L, 4L, 10L))
    val observed = ArrayBuffer.empty[(PlannedReadRange, Seq[Byte])]
    val output = new MemoryStagedParquetOutput(HostMemoryBuffer.allocate(16L), 16L)

    try {
      output.copyRanges(input, ranges.asJava)
      recordCopiedRanges(output, ranges, observed)

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

  test("memory output accepts concurrent source writes into disjoint ranges") {
    val sourceBytes = Array.tabulate[Byte](64)(index => (index + 10).toByte)
    val tracker = new SourceReadTracker(2)
    val inputs = Seq(
      new BlockingVectoredInput(0, sourceBytes, tracker),
      new BlockingVectoredInput(1, sourceBytes, tracker))
    val sources = Seq(
      footer(20, oneColumnParquetSchema, oneColumnReadSchema, Seq.empty),
      footer(21, oneColumnParquetSchema, oneColumnReadSchema, Seq.empty))
    val ranges = Seq(
      Seq(
        new PlannedReadRange(sources(0), 1L, 4L, 2L),
        new PlannedReadRange(sources(0), 8L, 3L, 6L)),
      Seq(
        new PlannedReadRange(sources(1), 20L, 5L, 12L),
        new PlannedReadRange(sources(1), 30L, 2L, 20L)))
    val writableBuffer = HostMemoryBuffer.allocate(32L)
    val output = new MemoryStagedParquetOutput(writableBuffer, 32L)
    val workers = Executors.newFixedThreadPool(2)

    try {
      val writes = inputs.indices.map { index =>
        workers.submit(new Runnable {
          override def run(): Unit =
            output.copyRanges(inputs(index), ranges(index).asJava)
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

  test("every file coalesces its cache misses into merged vectored reads") {
    val sourceCount = 4
    val tracker = new SourceReadTracker(sourceCount)
    val sourceBytes = Array.tabulate[Byte](2048)(index => (index & 0xff).toByte)
    val footers = (0 until sourceCount).map { ordinal =>
      val input = new BlockingVectoredInput(ordinal, sourceBytes, tracker)
      val baseOffset = 100L + ordinal * 300L
      // Files 0-2 leave a 96-byte source gap, so their chunks remain separate reads;
      // file 3's source-adjacent chunks coalesce directly into the packed fragment.
      val valueSpec = if (ordinal == 3) {
        ColumnSpec("value", baseOffset + 4L, 0L, 5L, 5L)
      } else {
        ColumnSpec("value", baseOffset + 100L, 0L, 5L, 5L)
      }
      footerWithInput(
        ordinal,
        twoColumnParquetSchema,
        twoColumnReadSchema,
        Seq(block(twoColumnParquetSchema, rows = 1L,
          ColumnSpec("id", baseOffset + 4L, baseOffset, 4L, 4L),
          valueSpec)),
        input)
    }
    val scanFiles = footers.map(_.getFile)
    val decodedSubtasks = new AtomicInteger()
    val adapter = new TestAdapter {
      override def readAndFilterFooter(file: IcebergPartitionedFile): FooterResult =
        footers.find(_.getFile eq file).get

      override def decodeAndPostProcess(
          subtask: ReadSubtask,
          parquetInput: StagedParquetInput): JIterator[ColumnarBatch] = {
        // Header + one owning fragment slice per source + footer: encoded data is represented
        // without a second task-sized assembly allocation.
        assert(parquetInput.getSegmentCount === sourceCount + 2)
        // Retry materialization must return independent owning references. Closing one attempt
        // cannot invalidate another attempt over the same borrowed fragment owners.
        val discardedAttempt = parquetInput.materialize()
        val liveAttempt = parquetInput.materialize()
        val parquetData = try {
          discardedAttempt.foreach(_.close())
          concatenateParquetBuffers(liveAttempt)
        } finally {
          liveAttempt.foreach(_.close())
        }
        // Byte-verify the packed fragment layout end to end.
        logicalRanges(subtask).foreach { range =>
          val actual = parquetData.slice(
            Math.toIntExact(range.outputOffset),
            Math.toIntExact(range.outputOffset + range.length))
          val expected = sourceBytes.slice(
            Math.toIntExact(range.inputOffset),
            Math.toIntExact(range.inputOffset + range.length))
          assert(actual.toIndexedSeq === expected.toIndexedSeq)
        }
        decodedSubtasks.incrementAndGet()
        Collections.emptyList[ColumnarBatch]().iterator()
      }
    }

    // Bound the pinned-preferred host allocations that back staged outputs in these tests.
    HostAlloc.initialize(1L << 26)
    // Enough workers for every footer to chain straight into a concurrently running download.
    val reader = new StagedParquetPartitionReader(
      scanFiles.asJava,
      adapter,
      Int.MaxValue,
      Long.MaxValue,
      Long.MaxValue,
      30000L,
      sourceCount,
      null)
    val caller = Executors.newSingleThreadExecutor()
    try {
      val hasNext = caller.submit(new java.util.concurrent.Callable[Boolean] {
        override def call(): Boolean = reader.hasNext()
      })

      assert(tracker.allStarted.await(10, TimeUnit.SECONDS))
      assert(tracker.activeCalls.get() === sourceCount)
      assert(tracker.startedCalls.get() === sourceCount)
      assert(tracker.maximumActiveCalls.get() === sourceCount)
      assert(tracker.threadNames.asScala.forall(_.startsWith("iceberg-staged-worker-")))

      tracker.release(sourceCount)
      assert(!hasNext.get(10, TimeUnit.SECONDS))
      assert(tracker.activeCalls.get() === 0)

      val calls = tracker.calls.asScala.toSeq.sortBy(_._1)
      assert(calls.map(_._1) === (0 until sourceCount))
      assert(decodedSubtasks.get() === 1)
      calls.foreach { case (ordinal, ranges) =>
        val baseOffset = 100L + ordinal * 300L
        if (ordinal == 3) {
          // Source-adjacent chunks merge into one direct read landing on the packed fragment.
          assert(ranges === Seq((baseOffset, 9L, 0L)))
        } else {
          // A 96-byte source gap splits the merge — contiguous-only coalescing, matching the
          // base reader: gapped chunks stay separate parallel requests and no gap byte is read.
          assert(ranges === Seq(
            (baseOffset, 4L, 0L),
            (baseOffset + 100L, 5L, 4L)))
        }
      }
    } finally {
      tracker.release(sourceCount * 2)
      reader.close()
      caller.shutdownNow()
      HostAlloc.initialize(-1L)
    }
  }

  test("decode keeps input order even when a later download finishes first") {
    val sourceCount = 2
    val tracker = new SourceReadTracker(sourceCount)
    val sourceBytes = Array.tabulate[Byte](1024)(index => (index & 0xff).toByte)
    val footers = (0 until sourceCount).map { ordinal =>
      val input = new BlockingVectoredInput(ordinal, sourceBytes, tracker)
      val baseOffset = 100L + ordinal * 300L
      footerWithInput(
        ordinal,
        oneColumnParquetSchema,
        oneColumnReadSchema,
        Seq(block(oneColumnParquetSchema, rows = 1L,
          ColumnSpec("id", baseOffset + 4L, baseOffset, 4L, 4L))),
        input)
    }
    val scanFiles = footers.map(_.getFile)
    val decoded = new ConcurrentLinkedQueue[Int]()
    val adapter = new TestAdapter {
      override def readAndFilterFooter(file: IcebergPartitionedFile): FooterResult =
        footers.find(_.getFile eq file).get

      override def decodeAndPostProcess(
          subtask: ReadSubtask,
          parquetInput: StagedParquetInput): JIterator[ColumnarBatch] = {
        decoded.add(sourceOrdinal(subtask.getFileSlices.get(0).getFooter.getFile))
        Collections.singletonList(new ColumnarBatch(
          Array.empty[org.apache.spark.sql.vectorized.ColumnVector], 1)).iterator()
      }
    }

    HostAlloc.initialize(1L << 26)
    val reader = new StagedParquetPartitionReader(
      scanFiles.asJava,
      adapter,
      Int.MaxValue,
      Long.MaxValue,
      0L,
      0L,
      2,
      null)
    val caller = Executors.newSingleThreadExecutor()
    try {
      val hasNext = caller.submit(new java.util.concurrent.Callable[Boolean] {
        override def call(): Boolean = reader.hasNext()
      })

      // With two workers available, both file downloads run their I/O concurrently.
      assert(tracker.firstTwoStarted.await(10, TimeUnit.SECONDS))
      assert(tracker.startedCalls.get() === 2)
      assert(tracker.calls.asScala.map(_._1).toSet === Set(0, 1))

      // Finish the second file first. Decode keeps input order, so the task thread must keep
      // waiting for file 0 rather than consuming the already-finished file 1.
      tracker.releaseOrdinal(1)
      val holdUntil = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(300)
      while (System.nanoTime() < holdUntil) {
        assert(!hasNext.isDone, "decode must not run ahead of input order")
        Thread.sleep(20)
      }

      tracker.releaseOrdinal(0)
      assert(hasNext.get(10, TimeUnit.SECONDS))
      reader.next().close()
      assert(decoded.asScala.toSeq === Seq(0))
      assert(reader.hasNext())
      reader.next().close()
      assert(!reader.hasNext)
      assert(decoded.asScala.toSeq === Seq(0, 1))
    } finally {
      tracker.release(sourceCount * 2)
      reader.close()
      caller.shutdownNow()
      HostAlloc.initialize(-1L)
    }
  }

  test("staged scan retains the GPU semaphore until Spark task completion") {
    val sourceBytes = Array.tabulate[Byte](1024)(index => (index & 0xff).toByte)
    val secondReadTracker = new SourceReadTracker(expectedSources = 1)
    val first = footerWithInput(
      0,
      oneColumnParquetSchema,
      oneColumnReadSchema,
      Seq(oneColumnBlock(rows = 1L, offset = 100L, size = 4L)),
      new RecordingVectoredInput(sourceBytes))
    val second = footerWithInput(
      1,
      oneColumnParquetSchema,
      oneColumnReadSchema,
      Seq(oneColumnBlock(rows = 1L, offset = 400L, size = 4L)),
      new BlockingVectoredInput(1, sourceBytes, secondReadTracker))
    val footers = Seq(first, second)

    val ownerContext = mock(classOf[TaskContext])
    when(ownerContext.taskAttemptId()).thenReturn(1001L)
    when(ownerContext.stageId()).thenReturn(7)

    val firstIteratorClosed = new CountDownLatch(1)
    val adapter = new TestAdapter {
      override def readAndFilterFooter(file: IcebergPartitionedFile): FooterResult =
        footers.find(_.getFile eq file).get

      override def decodeAndPostProcess(
          subtask: ReadSubtask,
          parquetInput: StagedParquetInput): JIterator[ColumnarBatch] = {
        // The production adapter acquires immediately before cuDF decode. Use the captured mock
        // context here because this test deliberately does not initialize a CUDA device.
        GpuSemaphore.acquireIfNecessary(ownerContext)
        val ordinal = sourceOrdinal(subtask.getFileSlices.get(0).getFooter.getFile)
        val batchCount = if (ordinal == 0) 2 else 1
        val batches = (0 until batchCount).map { _ =>
          new ColumnarBatch(
            Array.empty[org.apache.spark.sql.vectorized.ColumnVector], 1)
        }.iterator
        new JIterator[ColumnarBatch] with AutoCloseable {
          private val closed = new AtomicBoolean()

          override def hasNext: Boolean = batches.hasNext

          override def next(): ColumnarBatch = batches.next()

          override def close(): Unit = {
            if (closed.compareAndSet(false, true) && ordinal == 0) {
              firstIteratorClosed.countDown()
            }
          }
        }
      }
    }

    val previousRmmTaskInit = GpuDeviceManager.rmmTaskInitEnabled
    val previousSparkEnv = SparkEnv.get
    val testSparkEnv = mock(classOf[SparkEnv])
    when(testSparkEnv.conf).thenReturn(new SparkConf(false)
      .set("spark.rapids.sql.concurrentGpuTasks", "1"))
    val setSparkEnv = SparkEnv.getClass.getMethod("set", classOf[SparkEnv])
    var reader: StagedParquetPartitionReader = null
    var caller: java.util.concurrent.ExecutorService = null
    val continueAfterFirstBatch = new CountDownLatch(1)
    val firstBatchObserved = new CountDownLatch(1)
    HostAlloc.initialize(1L << 26)
    ScalableTaskCompletion.reset()
    GpuSemaphore.shutdown()
    GpuSemaphore.initialize(1)
    GpuDeviceManager.setRmmTaskInitEnabled(false)
    setSparkEnv.invoke(SparkEnv, testSparkEnv)
    try {
      reader = new StagedParquetPartitionReader(
        footers.map(_.getFile).asJava,
        adapter,
        Int.MaxValue,
        Long.MaxValue,
        0L,
        0L,
        2,
        ownerContext)
      caller = Executors.newSingleThreadExecutor()

      val scan = caller.submit(new java.util.concurrent.Callable[Boolean] {
        override def call(): Boolean = {
          assert(reader.hasNext())
          reader.next().close()
          // Advancing within one eagerly decoded iterator must not yield the task-wide permit.
          assert(reader.hasNext())
          firstBatchObserved.countDown()
          assert(continueAfterFirstBatch.await(10, TimeUnit.SECONDS))
          reader.next().close()

          // This call closes the first decoded iterator, then blocks for file 1's fragment.
          assert(reader.hasNext())
          reader.next().close()
          !reader.hasNext
        }
      })

      assert(firstBatchObserved.await(10, TimeUnit.SECONDS))
      val (firstAcquire, firstRelease) =
        GpuSemaphore.getLastSemAcqAndRelTime(ownerContext)
      assert(firstAcquire > firstRelease,
        "the scan yielded its GPU permit while decoded batches were still pending")

      assert(secondReadTracker.firstStarted.await(10, TimeUnit.SECONDS))
      continueAfterFirstBatch.countDown()
      assert(firstIteratorClosed.await(10, TimeUnit.SECONDS))
      assert(!scan.isDone, "the scan should be blocked waiting for the second file")
      val (waitAcquire, waitRelease) =
        GpuSemaphore.getLastSemAcqAndRelTime(ownerContext)
      assert(waitAcquire > waitRelease,
        "the scan yielded its GPU permit between decoded subtasks")

      secondReadTracker.releaseOrdinal(1)
      assert(scan.get(10, TimeUnit.SECONDS))
      val (terminalAcquire, terminalRelease) =
        GpuSemaphore.getLastSemAcqAndRelTime(ownerContext)
      assert(terminalAcquire > terminalRelease,
        "the scan released its GPU permit before Spark task completion")

      // GpuSemaphore registers this callback on the first acquire. Simulate Spark completing the
      // task and verify that task completion, rather than the reader, releases the permit.
      ScalableTaskCompletion.reset()
      assert(GpuSemaphore.getLastSemAcqAndRelTime(ownerContext) === ((0L, 0L)))
    } finally {
      continueAfterFirstBatch.countDown()
      secondReadTracker.release(1)
      if (reader != null) {
        reader.close()
      }
      if (caller != null) {
        caller.shutdownNow()
      }
      secondReadTracker.allFinished.await(10, TimeUnit.SECONDS)
      ScalableTaskCompletion.reset()
      GpuSemaphore.shutdown()
      setSparkEnv.invoke(SparkEnv, previousSparkEnv)
      GpuDeviceManager.setRmmTaskInitEnabled(previousRmmTaskInit)
      HostAlloc.initialize(-1L)
    }
  }

  test("completion-order combine gives every admitted result a fresh wait") {
    val sourceCount = 3
    val tracker = new SourceReadTracker(sourceCount)
    val sourceBytes = Array.tabulate[Byte](1024)(index => (index & 0xff).toByte)
    val footers = (0 until sourceCount).map { ordinal =>
      val input = new BlockingVectoredInput(ordinal, sourceBytes, tracker)
      val baseOffset = 100L + ordinal * 300L
      footerWithInput(
        ordinal,
        oneColumnParquetSchema,
        oneColumnReadSchema,
        Seq(block(oneColumnParquetSchema, rows = 1L,
          ColumnSpec("id", baseOffset + 4L, baseOffset, 4L, 4L))),
        input)
    }
    val decoded = new ConcurrentLinkedQueue[Seq[Int]]()
    val firstAdmitted = new CountDownLatch(1)
    val secondAdmitted = new CountDownLatch(1)
    val resultWaits = new AtomicInteger()
    val adapter = new TestAdapter {
      override def readAndFilterFooter(file: IcebergPartitionedFile): FooterResult =
        footers.find(_.getFile eq file).get

      override def onResultWait(waitNanos: Long): Unit = {
        resultWaits.incrementAndGet() match {
          case 1 => firstAdmitted.countDown()
          case 2 => secondAdmitted.countDown()
          case _ =>
        }
      }

      override def decodeAndPostProcess(
          subtask: ReadSubtask,
          parquetInput: StagedParquetInput): JIterator[ColumnarBatch] = {
        decoded.add(subtask.getFileSlices.asScala
          .map(slice => sourceOrdinal(slice.getFooter.getFile)).toSeq)
        Collections.singletonList(new ColumnarBatch(
          Array.empty[org.apache.spark.sql.vectorized.ColumnVector], 1)).iterator()
      }
    }

    HostAlloc.initialize(1L << 26)
    val reader = new StagedParquetPartitionReader(
      footers.map(_.getFile).asJava,
      adapter,
      Int.MaxValue,
      Long.MaxValue,
      Long.MaxValue,
      1000L,
      sourceCount,
      null)
    val caller = Executors.newSingleThreadExecutor()
    try {
      val hasNext = caller.submit(new java.util.concurrent.Callable[Boolean] {
        override def call(): Boolean = reader.hasNext()
      })
      assert(tracker.allStarted.await(10, TimeUnit.SECONDS))

      // Finish file 2 first. Combine mode must begin with that completion, not file-list head 0.
      tracker.releaseOrdinal(2)
      assert(firstAdmitted.await(10, TimeUnit.SECONDS))

      // Each gap is shorter than one second, but their sum exceeds one second. A cumulative
      // budget would flush before file 1; a fresh wait after file 0 admits all three.
      Thread.sleep(600)
      tracker.releaseOrdinal(0)
      assert(secondAdmitted.await(10, TimeUnit.SECONDS))
      Thread.sleep(600)
      assert(!hasNext.isDone, "the second admission must reset the combine wait")
      tracker.releaseOrdinal(1)

      assert(hasNext.get(10, TimeUnit.SECONDS))
      reader.next().close()
      assert(!reader.hasNext)
      assert(decoded.asScala.toSeq === Seq(Seq(2, 0, 1)))
    } finally {
      tracker.release(sourceCount * 2)
      reader.close()
      caller.shutdownNow()
      HostAlloc.initialize(-1L)
    }
  }

  test("combine timeout flushes the completed prefix") {
    val sourceCount = 2
    val tracker = new SourceReadTracker(sourceCount)
    val sourceBytes = Array.tabulate[Byte](1024)(index => (index & 0xff).toByte)
    val footers = (0 until sourceCount).map { ordinal =>
      val input = new BlockingVectoredInput(ordinal, sourceBytes, tracker)
      val baseOffset = 100L + ordinal * 300L
      footerWithInput(
        ordinal,
        oneColumnParquetSchema,
        oneColumnReadSchema,
        Seq(block(oneColumnParquetSchema, rows = 1L,
          ColumnSpec("id", baseOffset + 4L, baseOffset, 4L, 4L))),
        input)
    }
    val scanFiles = footers.map(_.getFile)
    val decoded = new ConcurrentLinkedQueue[Seq[Int]]()
    val adapter = new TestAdapter {
      override def readAndFilterFooter(file: IcebergPartitionedFile): FooterResult =
        footers.find(_.getFile eq file).get

      override def decodeAndPostProcess(
          subtask: ReadSubtask,
          parquetInput: StagedParquetInput): JIterator[ColumnarBatch] = {
        decoded.add(subtask.getFileSlices.asScala
          .map(slice => sourceOrdinal(slice.getFooter.getFile)).toSeq)
        Collections.singletonList(new ColumnarBatch(
          Array.empty[org.apache.spark.sql.vectorized.ColumnVector], 1)).iterator()
      }
    }

    HostAlloc.initialize(1L << 26)
    // File 0 completes first and file 1 stays blocked beyond the fresh 100 ms wait.
    val reader = new StagedParquetPartitionReader(
      scanFiles.asJava,
      adapter,
      Int.MaxValue,
      Long.MaxValue,
      Long.MaxValue,
      100L,
      2,
      null)
    val caller = Executors.newSingleThreadExecutor()
    try {
      val hasNext = caller.submit(new java.util.concurrent.Callable[Boolean] {
        override def call(): Boolean = reader.hasNext()
      })
      assert(tracker.firstTwoStarted.await(10, TimeUnit.SECONDS))
      tracker.releaseOrdinal(0)

      assert(hasNext.get(10, TimeUnit.SECONDS))
      reader.next().close()
      assert(decoded.asScala.toSeq === Seq(Seq(0)))

      tracker.releaseOrdinal(1)
      assert(reader.hasNext())
      reader.next().close()
      assert(!reader.hasNext)
      assert(decoded.asScala.toSeq === Seq(Seq(0), Seq(1)))
    } finally {
      tracker.release(sourceCount * 2)
      reader.close()
      caller.shutdownNow()
      HostAlloc.initialize(-1L)
    }
  }

  test("a downloaded fragment frees its worker before the task thread consumes it") {
    val sourceCount = 3
    val tracker = new SourceReadTracker(sourceCount)
    val sourceBytes = Array.tabulate[Byte](1024)(index => (index & 0xff).toByte)
    val footers = (0 until sourceCount).map { ordinal =>
      val input = new BlockingVectoredInput(ordinal, sourceBytes, tracker)
      val baseOffset = 100L + ordinal * 300L
      footerWithInput(
        ordinal,
        oneColumnParquetSchema,
        oneColumnReadSchema,
        Seq(block(oneColumnParquetSchema, rows = 1L,
          ColumnSpec("id", baseOffset + 4L, baseOffset, 4L, 4L))),
        input)
    }
    val scanFiles = footers.map(_.getFile)
    val firstDecodeGate = new CountDownLatch(1)
    val decodeCalls = new AtomicInteger()
    val adapter = new TestAdapter {
      override def readAndFilterFooter(file: IcebergPartitionedFile): FooterResult =
        footers.find(_.getFile eq file).get

      override def decodeAndPostProcess(
          subtask: ReadSubtask,
          parquetInput: StagedParquetInput): JIterator[ColumnarBatch] = {
        if (decodeCalls.incrementAndGet() == 1) {
          // Keep the task thread busy inside its first decode, exactly like a long GPU stage,
          // so later completions stay unclaimed while workers publish them.
          assert(firstDecodeGate.await(30, TimeUnit.SECONDS))
        }
        Collections.singletonList(new ColumnarBatch(
          Array.empty[org.apache.spark.sql.vectorized.ColumnVector], 1)).iterator()
      }
    }

    // Bound the pinned-preferred host allocations that back staged outputs in these tests.
    HostAlloc.initialize(1L << 26)
    val reader = new StagedParquetPartitionReader(
      scanFiles.asJava,
      adapter,
      Int.MaxValue,
      Long.MaxValue,
      0L,
      0L,
      1,
      null)
    val caller = Executors.newSingleThreadExecutor()
    try {
      val hasNext = caller.submit(new java.util.concurrent.Callable[Boolean] {
        override def call(): Boolean = reader.hasNext()
      })

      // A single worker bounds the concurrently downloading files to one.
      assert(tracker.firstStarted.await(10, TimeUnit.SECONDS))
      assert(tracker.startedCalls.get() === 1)

      // Completing file 0 lets the task thread assemble it and enter the gated decode; the
      // freed worker starts file 1's download.
      tracker.release(1)
      assert(tracker.firstTwoStarted.await(10, TimeUnit.SECONDS))

      // File 1's fragment publishes and immediately frees the only pool slot, so file 2's
      // download starts even though the task thread is still inside decode and nobody has
      // consumed fragment 1.
      tracker.release(1)
      assert(tracker.allStarted.await(10, TimeUnit.SECONDS))
      assert(tracker.startedCalls.get() === 3)
      tracker.release(1)

      // Unblock the first decode and drain the remaining completions.
      firstDecodeGate.countDown()
      assert(hasNext.get(10, TimeUnit.SECONDS))
      reader.next().close()
      while (reader.hasNext()) {
        reader.next().close()
      }
      assert(tracker.writeFailures.isEmpty)
    } finally {
      firstDecodeGate.countDown()
      tracker.release(sourceCount * 2)
      reader.close()
      caller.shutdownNow()
      HostAlloc.initialize(-1L)
    }
  }

  test("independent Spark tasks submit without executor-wide admission") {
    val tracker = new SourceReadTracker(2)
    val sourceBytes = Array.tabulate[Byte](1024)(index => (index & 0xff).toByte)
    val footers = (0 until 2).map { ordinal =>
      val baseOffset = 100L + ordinal * 300L
      footerWithInput(
        ordinal,
        oneColumnParquetSchema,
        oneColumnReadSchema,
        Seq(block(oneColumnParquetSchema, rows = 1L,
          ColumnSpec("id", baseOffset + 4L, baseOffset, 4L, 4L))),
        new BlockingVectoredInput(ordinal, sourceBytes, tracker))
    }
    def adapterFor(footer: FooterResult): StagedScanAdapter =
      new TestAdapter {
        override def readAndFilterFooter(file: IcebergPartitionedFile): FooterResult = footer

        override def decodeAndPostProcess(
          subtask: ReadSubtask,
          parquetInput: StagedParquetInput): JIterator[ColumnarBatch] = {
          Collections.singletonList(new ColumnarBatch(
            Array.empty[org.apache.spark.sql.vectorized.ColumnVector], 1)).iterator()
        }
      }

    HostAlloc.initialize(1L << 26)
    val firstReader = new StagedParquetPartitionReader(
      Seq(footers.head.getFile).asJava,
      adapterFor(footers.head),
      Int.MaxValue,
      Long.MaxValue,
      0L,
      0L,
      2,
      null)
    val secondReader = new StagedParquetPartitionReader(
      Seq(footers.last.getFile).asJava,
      adapterFor(footers.last),
      Int.MaxValue,
      Long.MaxValue,
      0L,
      0L,
      2,
      null)
    val callers = Executors.newFixedThreadPool(2)
    try {
      val firstHasNext = callers.submit(new java.util.concurrent.Callable[Boolean] {
        override def call(): Boolean = firstReader.hasNext()
      })
      assert(tracker.firstStarted.await(10, TimeUnit.SECONDS))

      val secondHasNext = callers.submit(new java.util.concurrent.Callable[Boolean] {
        override def call(): Boolean = secondReader.hasNext()
      })
      // There is no global admission slot: the second task submits while the first request is
      // still active.
      assert(tracker.firstTwoStarted.await(10, TimeUnit.SECONDS))
      assert(tracker.startedCalls.get() === 2)

      tracker.release(2)
      assert(firstHasNext.get(10, TimeUnit.SECONDS))
      assert(secondHasNext.get(10, TimeUnit.SECONDS))
      firstReader.next().close()
      secondReader.next().close()
    } finally {
      tracker.release(4)
      firstReader.close()
      secondReader.close()
      callers.shutdownNow()
      HostAlloc.initialize(-1L)
    }
  }

  test("equal file occurrences download independently and combine into one subtask") {
    val sourceCount = 2
    val tracker = new SourceReadTracker(sourceCount)
    val sourceBytes = Array.tabulate[Byte](1024)(index => (index & 0xff).toByte)
    val first = footerWithInput(
      0,
      twoColumnParquetSchema,
      twoColumnReadSchema,
      Seq(block(twoColumnParquetSchema, rows = 1L,
        ColumnSpec("id", 104L, 100L, 4L, 4L),
        ColumnSpec("value", 200L, 0L, 5L, 5L))),
      new BlockingVectoredInput(0, sourceBytes, tracker))
    // These are distinct scan occurrences but compare equal as case-class values. They must keep
    // separate footer/post-processing identity and therefore become two independent source jobs.
    val secondFile = IcebergPartitionedFile(first.getFile.file)
    sourceInputs.put(secondFile, new BlockingVectoredInput(1, sourceBytes, tracker))
    val second = new FooterResult(
      secondFile,
      Seq(block(twoColumnParquetSchema, rows = 1L,
        ColumnSpec("id", 404L, 400L, 4L, 4L),
        ColumnSpec("value", 500L, 0L, 5L, 5L))).asJava,
      first.getClippedSchema,
      first.getReadSchema,
      first.getDateRebaseMode,
      first.getTimestampRebaseMode,
      first.hasInt96Timestamps(),
      first.getPostProcessor)
    assert(first.getFile == second.getFile)
    assert(!(first.getFile eq second.getFile))
    val footers = Seq(first, second)
    val scanFiles = footers.map(_.getFile)
    val combinedBytesVerified = new AtomicBoolean()
    val adapter = new TestAdapter {
      override def readAndFilterFooter(file: IcebergPartitionedFile): FooterResult =
        footers.find(_.getFile eq file).get

      override def decodeAndPostProcess(
          subtask: ReadSubtask,
          parquetInput: StagedParquetInput): JIterator[ColumnarBatch] = {
        val layout = subtask
        val parquetData = materializeParquetBytes(parquetInput)
        val header = parquetData.take(layout.getHeaderBytes.length)
        assert(header.toIndexedSeq === layout.getHeaderBytes.toIndexedSeq)
        logicalRanges(layout).foreach { range =>
          val actual = parquetData.slice(
            Math.toIntExact(range.outputOffset),
            Math.toIntExact(range.outputOffset + range.length))
          val expected = sourceBytes.slice(
            Math.toIntExact(range.inputOffset),
            Math.toIntExact(range.inputOffset + range.length))
          assert(actual.toIndexedSeq === expected.toIndexedSeq)
        }
        val footer = parquetData.drop(Math.toIntExact(layout.getFooterOffset))
        assert(footer.toIndexedSeq === layout.getFooterAndTrailerBytes.toIndexedSeq)
        combinedBytesVerified.set(true)
        Collections.emptyList[ColumnarBatch]().iterator()
      }

    }

    HostAlloc.initialize(1L << 26)
    val reader = new StagedParquetPartitionReader(
      scanFiles.asJava,
      adapter,
      Int.MaxValue,
      Long.MaxValue,
      Long.MaxValue,
      30000L,
      2,
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

      // Gapped chunks stay separate requests under contiguous-only coalescing; the assembled
      // synthetic offsets (4/8 and 13/17) were verified inside decode.
      val calls = tracker.calls.asScala.toSeq.sortBy(_._1)
      assert(calls.map(_._1) === Seq(0, 1))
      assert(calls.head._2 === Seq(
        (100L, 4L, 0L),
        (200L, 5L, 4L)))
      assert(calls.last._2 === Seq(
        (400L, 4L, 0L),
        (500L, 5L, 4L)))
    } finally {
      tracker.release(sourceCount * 2)
      reader.close()
      caller.shutdownNow()
      HostAlloc.initialize(-1L)
    }
  }

  test("close leaves fragment outputs alive until every active writer exits") {
    val sourceCount = 2
    val tracker = new SourceReadTracker(sourceCount)
    val sourceBytes = Array.tabulate[Byte](1024)(index => (index & 0xff).toByte)
    val footers = (0 until sourceCount).map { ordinal =>
      val input = new BlockingVectoredInput(ordinal, sourceBytes, tracker)
      val baseOffset = 100L + ordinal * 300L
      footerWithInput(
        ordinal,
        twoColumnParquetSchema,
        twoColumnReadSchema,
        Seq(block(twoColumnParquetSchema, rows = 1L,
          ColumnSpec("id", baseOffset + 4L, baseOffset, 4L, 4L),
          ColumnSpec("value", baseOffset + 100L, 0L, 5L, 5L))),
        input)
    }
    val scanFiles = footers.map(_.getFile)
    val decodeCalled = new AtomicBoolean()
    val adapter = new TestAdapter {
      override def readAndFilterFooter(file: IcebergPartitionedFile): FooterResult =
        footers.find(_.getFile eq file).get

      override def decodeAndPostProcess(
          subtask: ReadSubtask,
          parquetInput: StagedParquetInput): JIterator[ColumnarBatch] = {
        decodeCalled.set(true)
        Collections.emptyList[ColumnarBatch]().iterator()
      }

    }

    HostAlloc.initialize(1L << 26)
    val reader = new StagedParquetPartitionReader(
      scanFiles.asJava,
      adapter,
      Int.MaxValue,
      Long.MaxValue,
      Long.MaxValue,
      30000L,
      2,
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
      workerThreads = 1)

    try {
      // Before the fix, this first access initialized DecodeSupport and failed because the
      // ParquetPartitionReaderBase trait constructor observed an uninitialized override val.
      assert(reader.stagedParquetOptions(oneColumnReadSchema, oneColumnParquetSchema) != null)
    } finally {
      reader.close()
    }
  }

  test("file downloads start before the last footer resolves") {
    val tracker = new SourceReadTracker(2)
    val sourceBytes = Array.tabulate[Byte](1024)(index => (index & 0xff).toByte)
    val input0 = new BlockingVectoredInput(0, sourceBytes, tracker)
    val input1 = new BlockingVectoredInput(1, sourceBytes, tracker)
    val file0 = footerWithInput(0, oneColumnParquetSchema, oneColumnReadSchema, Seq(
      oneColumnBlock(rows = 1L, offset = 100L, size = 4L),
      oneColumnBlock(rows = 1L, offset = 200L, size = 4L)), input0)
    val file1 = footerWithInput(1, oneColumnParquetSchema, oneColumnReadSchema, Seq(
      oneColumnBlock(rows = 1L, offset = 300L, size = 4L)), input1)
    val scanFiles = Seq(file0, file1).map(_.getFile)
    val lastFooterGate = new CountDownLatch(1)
    val decodedRangesVerified = new AtomicInteger()
    val adapter = new TestAdapter {
      override def readAndFilterFooter(file: IcebergPartitionedFile): FooterResult = {
        val result = Seq(file0, file1).find(_.getFile eq file).get
        if (result eq file1) {
          // Hold the last footer hostage so the test can observe earlier file I/O running.
          assert(lastFooterGate.await(30, TimeUnit.SECONDS))
        }
        result
      }

      override def decodeAndPostProcess(
          subtask: ReadSubtask,
          parquetInput: StagedParquetInput): JIterator[ColumnarBatch] = {
        val parquetData = materializeParquetBytes(parquetInput)
        logicalRanges(subtask).foreach { range =>
          val actual = parquetData.slice(
            Math.toIntExact(range.outputOffset),
            Math.toIntExact(range.outputOffset + range.length))
          val expected = sourceBytes.slice(
            Math.toIntExact(range.inputOffset),
            Math.toIntExact(range.inputOffset + range.length))
          assert(actual.toIndexedSeq === expected.toIndexedSeq)
        }
        decodedRangesVerified.incrementAndGet()
        Collections.emptyList[ColumnarBatch]().iterator()
      }
    }

    // Bound the pinned-preferred host allocations that back staged outputs in these tests.
    HostAlloc.initialize(1L << 26)
    // maxRows = 1 closes a subtask per row group, so one fragment serves several subtasks.
    val reader = new StagedParquetPartitionReader(
      scanFiles.asJava,
      adapter,
      1,
      Long.MaxValue,
      Long.MaxValue,
      30000L,
      2,
      null)
    val caller = Executors.newSingleThreadExecutor()
    try {
      val hasNext = caller.submit(new java.util.concurrent.Callable[Boolean] {
        override def call(): Boolean = reader.hasNext()
      })

      // file0's download runs inside its own fused job, so its fragment read (per-chunk
      // parallel ranges under contiguous-only coalescing) begins while the last footer has not
      // resolved.
      assert(tracker.firstStarted.await(10, TimeUnit.SECONDS))
      assert(tracker.startedCalls.get() === 1)
      assert(tracker.calls.asScala.head._2 === Seq(
        (100L, 4L, 0L),
        (200L, 4L, 4L)))

      lastFooterGate.countDown()
      tracker.release(1)
      assert(tracker.firstTwoStarted.await(10, TimeUnit.SECONDS))
      tracker.release(1)
      assert(!hasNext.get(10, TimeUnit.SECONDS))
      assert(tracker.writeFailures.isEmpty)
      // Three subtasks decoded: fragment 0 was sliced into two split subtasks, so byte
      // verification also covers assembling several subtasks from one fragment.
      assert(decodedRangesVerified.get() === 3)
    } finally {
      lastFooterGate.countDown()
      tracker.release(6)
      reader.close()
      caller.shutdownNow()
      HostAlloc.initialize(-1L)
    }
  }

  test("a slow first footer does not block later files' downloads") {
    val tracker = new SourceReadTracker(2)
    val sourceBytes = Array.tabulate[Byte](1024)(index => (index & 0xff).toByte)
    val input0 = new BlockingVectoredInput(0, sourceBytes, tracker)
    val input1 = new BlockingVectoredInput(1, sourceBytes, tracker)
    val file0 = footerWithInput(0, oneColumnParquetSchema, oneColumnReadSchema, Seq(
      oneColumnBlock(rows = 1L, offset = 100L, size = 4L)), input0)
    val file1 = footerWithInput(1, oneColumnParquetSchema, oneColumnReadSchema, Seq(
      oneColumnBlock(rows = 1L, offset = 300L, size = 4L),
      oneColumnBlock(rows = 1L, offset = 400L, size = 4L)), input1)
    val scanFiles = Seq(file0, file1).map(_.getFile)
    val firstFooterGate = new CountDownLatch(1)
    val adapter = new TestAdapter {
      override def readAndFilterFooter(file: IcebergPartitionedFile): FooterResult = {
        val result = Seq(file0, file1).find(_.getFile eq file).get
        if (result eq file0) {
          // The FIRST file's footer is the slow one. Downloads chain on each footer's own
          // completion, so file1's data I/O must run ahead of it even though planning and
          // decode keep input order.
          assert(firstFooterGate.await(30, TimeUnit.SECONDS))
        }
        result
      }

      override def decodeAndPostProcess(
          subtask: ReadSubtask,
          parquetInput: StagedParquetInput): JIterator[ColumnarBatch] = {
        Collections.emptyList[ColumnarBatch]().iterator()
      }
    }

    // Bound the pinned-preferred host allocations that back staged outputs in these tests.
    HostAlloc.initialize(1L << 26)
    val reader = new StagedParquetPartitionReader(
      scanFiles.asJava,
      adapter,
      1,
      Long.MaxValue,
      Long.MaxValue,
      30000L,
      2,
      null)
    val caller = Executors.newSingleThreadExecutor()
    try {
      val hasNext = caller.submit(new java.util.concurrent.Callable[Boolean] {
        override def call(): Boolean = reader.hasNext()
      })

      // file1's whole-file download starts while file0's footer is still pending, but nothing
      // decodes: in-order planning is still waiting on the first footer.
      assert(tracker.firstStarted.await(10, TimeUnit.SECONDS))
      assert(tracker.startedCalls.get() === 1)
      assert(tracker.calls.asScala.head._1 === 1)
      tracker.release(1)
      val holdUntil = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(300)
      while (System.nanoTime() < holdUntil) {
        assert(!hasNext.isDone, "decode must wait for the first footer to keep input order")
        Thread.sleep(20)
      }

      firstFooterGate.countDown()
      assert(tracker.firstTwoStarted.await(10, TimeUnit.SECONDS))
      tracker.release(1)
      assert(!hasNext.get(10, TimeUnit.SECONDS))
      assert(tracker.writeFailures.isEmpty)
    } finally {
      firstFooterGate.countDown()
      tracker.release(6)
      reader.close()
      caller.shutdownNow()
      HostAlloc.initialize(-1L)
    }
  }

  test("close unblocks a footer wait and ignores a footer that finishes later") {
    val input = footer(0, oneColumnParquetSchema, oneColumnReadSchema, Seq.empty)
    val scanFile = input.getFile
    val footerStarted = new CountDownLatch(1)
    val releaseFooter = new CountDownLatch(1)
    val footerFinished = new CountDownLatch(1)
    val footerThreadName = new AtomicReference[String]()
    val decodeCalled = new AtomicBoolean()
    val adapter = new TestAdapter {
      override def readAndFilterFooter(file: IcebergPartitionedFile): FooterResult = {
        footerThreadName.set(Thread.currentThread().getName)
        footerStarted.countDown()
        releaseFooter.await()
        footerFinished.countDown()
        input
      }

      override def decodeAndPostProcess(
          subtask: ReadSubtask,
          parquetInput: StagedParquetInput): JIterator[ColumnarBatch] = {
        decodeCalled.set(true)
        throw new AssertionError("empty footer must not produce a decode subtask")
      }
    }
    val reader = new StagedParquetPartitionReader(
      Seq(scanFile).asJava,
      adapter,
      Int.MaxValue,
      Long.MaxValue,
      1L,
      0L,
      1,
      null)
    val caller = Executors.newSingleThreadExecutor()
    try {
      val hasNext = caller.submit(new java.util.concurrent.Callable[Boolean] {
        override def call(): Boolean = reader.hasNext()
      })
      assert(footerStarted.await(10, TimeUnit.SECONDS))
      assert(footerThreadName.get().startsWith("iceberg-staged-worker-"))

      reader.close()
      assert(!hasNext.get(10, TimeUnit.SECONDS))

      releaseFooter.countDown()
      assert(footerFinished.await(10, TimeUnit.SECONDS))
      assert(!decodeCalled.get())
    } finally {
      releaseFooter.countDown()
      reader.close()
      caller.shutdownNow()
    }
  }
}
