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
import java.util.{Collections, List => JList}

import scala.collection.JavaConverters._

import com.nvidia.spark.rapids.{
  CombineConf,
  DateTimeRebaseCorrected,
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

import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

class StagedParquetPlannerSuite extends AnyFunSuite with BeforeAndAfterEach {

  override protected def afterEach(): Unit = {
    try {
      StagedScanThreadPools.resetForTesting()
    } finally {
      super.afterEach()
    }
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
    val path = new Path(s"file:///tmp/staged-planner-$ordinal.parquet")
    val icebergInput = new IcebergInputFile(HadoopInputFile.fromPath(path, new Configuration()))
    val file = IcebergPartitionedFile(icebergInput)
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
      workerThreads = 1,
      assemblyBufferCount = 2)

    try {
      // Before the fix, this first access initialized DecodeSupport and failed because the
      // ParquetPartitionReaderBase trait constructor observed an uninitialized override val.
      assert(reader.stagedParquetOptions(oneColumnReadSchema, oneColumnParquetSchema) != null)
    } finally {
      reader.close()
    }
  }

  test("assembly buffer pool bounds concurrent leases") {
    val pool = new AssemblyBufferPool(2)
    var first: AssemblyBufferPool.Lease = null
    var second: AssemblyBufferPool.Lease = null
    var third: AssemblyBufferPool.Lease = null
    try {
      first = pool.acquire().getNow(null)
      second = pool.acquire().getNow(null)
      val pending = pool.acquire()

      assert(first != null)
      assert(second != null)
      assert(!pending.isDone)

      first.close()
      first = null
      assert(pending.isDone)
      third = pending.getNow(null)
      assert(third != null)
    } finally {
      if (first != null) {
        first.close()
      }
      if (second != null) {
        second.close()
      }
      if (third != null) {
        third.close()
      }
      pool.close()
    }
  }

  test("assembly buffer pool hands released slots to waiters in FIFO order") {
    val pool = new AssemblyBufferPool(1)
    var owner: AssemblyBufferPool.Lease = null
    var firstWaiterLease: AssemblyBufferPool.Lease = null
    var secondWaiterLease: AssemblyBufferPool.Lease = null
    try {
      owner = pool.acquire().getNow(null)
      val firstWaiter = pool.acquire()
      val secondWaiter = pool.acquire()

      assert(!firstWaiter.isDone)
      assert(!secondWaiter.isDone)

      owner.close()
      owner = null
      assert(firstWaiter.isDone)
      assert(!secondWaiter.isDone)

      firstWaiterLease = firstWaiter.getNow(null)
      firstWaiterLease.close()
      firstWaiterLease = null
      assert(secondWaiter.isDone)

      secondWaiterLease = secondWaiter.getNow(null)
      assert(secondWaiterLease != null)
    } finally {
      if (owner != null) {
        owner.close()
      }
      if (firstWaiterLease != null) {
        firstWaiterLease.close()
      }
      if (secondWaiterLease != null) {
        secondWaiterLease.close()
      }
      pool.close()
    }
  }

  test("assembly buffer pool tracks retained current and peak capacity") {
    val pool = new AssemblyBufferPool(2)
    var first: AssemblyBufferPool.Lease = null
    var second: AssemblyBufferPool.Lease = null
    var handedOff: AssemblyBufferPool.Lease = null

    def assertCapacity(current: Long, peak: Long): Unit = {
      val snapshot = pool.capacitySnapshot()
      assert(snapshot.getCurrentCapacityBytes === current)
      assert(snapshot.getPeakCapacityBytes === peak)
    }

    try {
      // Slots are pre-created, but their host buffers are allocated lazily.
      assertCapacity(current = 0L, peak = 0L)
      first = pool.acquire().getNow(null)
      second = pool.acquire().getNow(null)
      assertCapacity(current = 0L, peak = 0L)

      first.ensureCapacity(64L)
      assertCapacity(current = 64L, peak = 64L)

      // Reusing a larger allocation neither shrinks the slot nor increases the peak.
      first.ensureCapacity(32L)
      assertCapacity(current = 64L, peak = 64L)

      second.ensureCapacity(96L)
      assertCapacity(current = 160L, peak = 160L)

      // Growing a slot replaces its old allocation and accounts only the retained capacities.
      first.ensureCapacity(128L)
      assertCapacity(current = 224L, peak = 224L)

      first.close()
      first = null
      assertCapacity(current = 224L, peak = 224L)

      // Returning and handing off a lease retains the slot's high-water allocation for reuse.
      handedOff = pool.acquire().getNow(null)
      handedOff.ensureCapacity(16L)
      assertCapacity(current = 224L, peak = 224L)

      handedOff.close()
      handedOff = null
      second.close()
      second = null
      pool.close()
      assertCapacity(current = 0L, peak = 224L)
    } finally {
      if (first != null) {
        first.close()
      }
      if (second != null) {
        second.close()
      }
      if (handedOff != null) {
        handedOff.close()
      }
      pool.close()
    }
  }
}
