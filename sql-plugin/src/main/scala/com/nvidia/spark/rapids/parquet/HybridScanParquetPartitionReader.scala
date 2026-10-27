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

package com.nvidia.spark.rapids.parquet

import ai.rapids.cudf.{ByteRange, DeviceMemoryBuffer, DType, HostMemoryBuffer, HybridScanReader, ParquetOptions, Table}
import ai.rapids.cudf.ast.{AstExpression, CompiledExpression}
import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.GpuMetric._
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.RmmRapidsRetryIterator.withRetryNoSplit

import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.util.RebaseDateTime
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * Reads one Parquet file with the libcudf hybrid scan reader, bypassing parquet-mr.
 *
 * The footer is the only part of the file parsed on the CPU. The reader then prunes row groups
 * with the pushed filter if there is one (statistics, then dictionary pages), asks libcudf which
 * column chunk byte ranges the read still needs, fetches only those, and materializes them.
 *
 * Only the conjuncts in `HybridScanFilters.pushed` are evaluated here. The output is a
 * superset of the rows the full filter accepts, which is the same contract the parquet-mr path
 * has: it prunes row groups and returns every surviving row. Spark re-applies its data filters
 * above the scan either way.
 *
 * A `PERFILE` scan builds one per split, which owns the row groups whose first byte falls in
 * `[splitStart, splitStart + splitLength)`.
 *
 * It is gated by `isEligible` to the cases it handles: one batch per read (no chunking), a flat
 * schema of primitive types, and no field IDs.
 * Datetime values are checked for a needed rebase after materialize, since the footer metadata
 * that records how the file was written is not parsed here.
 */
private[parquet] class HybridScanParquetPartitionReader(
    source: HybridScanByteSource,
    splitStart: Long,
    splitLength: Long,
    footer: HostMemoryBuffer,
    parquetOptions: ParquetOptions,
    readDataSchema: StructType,
    scanFilters: HybridScanFilters,
    filterKeepThreshold: Double,
    execMetrics: Map[String, GpuMetric])
  extends PartitionReader[ColumnarBatch] with Logging {

  private var batchIter: GpuColumnarBatchIterator = _
  private var footerClosed = false

  private def totalBytes(ranges: Array[ByteRange]): Long = ranges.map(_.size()).sum

  override def next(): Boolean = {
    if (batchIter == null) {
      batchIter = readBatch()
    }
    batchIter.hasNext
  }

  override def get(): ColumnarBatch = batchIter.next()

  override def close(): Unit = {
    if (batchIter != null) {
      batchIter.close()
      batchIter = null
    }
    closeFooter()
  }

  private def closeFooter(): Unit = {
    if (!footerClosed) {
      footerClosed = true
      footer.close()
    }
  }

  private def readBatch(): GpuColumnarBatchIterator = readTable() match {
    case Some(table) =>
      withResource(table) { t =>
        new SingleGpuColumnarBatchIterator(
          GpuColumnVector.from(t, readDataSchema.fields.map(_.dataType)))
      }
    case None => EmptyGpuColumnarBatchIterator
  }

  /**
   * The reader decodes columns straight from the file by name with no schema handling of its own,
   * so a column whose stored form does not match the read schema comes back as the wrong type (an
   * unsigned or widened integer, a decimal of another scale) and a column missing from the file or
   * spelled in a different case does not come back at all. Eligibility cannot catch these without
   * the file's own schema, so fail loudly here rather than return wrong results; the full gate
   * routes such files to legacy instead. GpuColumnVector.from checks the same thing only with an
   * assert, which does not run in production.
   */
  private def checkDecodedSchema(table: Table): Unit = {
    val expected = readDataSchema.fields
    if (table.getNumberOfColumns != expected.length) {
      throw new IllegalStateException(
        s"Hybrid scan decoded ${table.getNumberOfColumns} columns but the read schema has " +
          s"${expected.length}: a column is missing from the file or named differently")
    }
    expected.zipWithIndex.foreach { case (field, i) =>
      val actual = table.getColumn(i).getType
      val wanted = GpuColumnVector.getNonNestedRapidsType(field.dataType)
      if (!wanted.equals(actual)) {
        throw new IllegalStateException(
          s"Hybrid scan decoded column ${field.name} as $actual but the read schema wants " +
            s"${field.dataType.simpleString} ($wanted)")
      }
    }
  }

  /**
   * Run the read: parse the footer, prune the row groups the filter rules out, fetch what is left
   * of the projection and decode it. None when nothing survives, either because this read owns no
   * row groups or because the filter ruled them all out.
   */
  private def readTable(): Option[Table] = {
    NvtxRegistry.PARQUET_READ_BATCH {
      withResource(scanFilters.ast.map(_.compile())) { ast =>
        withResource(outputFilter(ast.isDefined)) { outputAst =>
          val parsed = NvtxRegistry.HYBRID_SCAN_PARSE_FOOTER {
            new HybridScanReader(footer, parquetOptions)
          }
          withResource(parsed) { reader =>
            ast.foreach(reader.setFilter)
            // The footer bytes are only read while the reader is being constructed.
            closeFooter()
            val rowGroups = selectRowGroups(reader, ast.isDefined, scanFilters.pushed)
            if (rowGroups.isEmpty) {
              None
            } else {
              // Pruning is what the reader's own copy of the filter was for. Applying it to the
              // rows is done here instead when there is a keep threshold to weigh it against.
              if (outputAst.isDefined) {
                reader.setFilter(null)
              }
              materialize(reader, rowGroups, outputAst)
            }
          }
        }
      }
    }
  }

  /**
   * The same conjuncts the reader was given, but referencing columns by projection index rather
   * than by name, for applying the filter to the table the reader returns. Only built when there
   * is a keep threshold to decide against, since that is the only case where the filter is
   * applied outside the reader.
   *
   * Both forms push the same conjuncts: which ones convert does not depend on how a column is
   * referenced.
   */
  private def outputFilter(hasFilter: Boolean): Option[CompiledExpression] = {
    if (hasFilter && filterKeepThreshold < 1.0) {
      scanFilters.outputAst.map(_.compile())
    } else {
      None
    }
  }

  /** Prune the row groups this read owns down to those the pushed filter cannot rule out. */
  private def selectRowGroups(
      reader: HybridScanReader,
      hasFilter: Boolean,
      pushed: Array[Filter]): Array[Int] = {
    val inSplit = rowGroupsInScope(reader, reader.allRowGroups())
    if (inSplit.isEmpty || !hasFilter) {
      inSplit
    } else {
      val afterStats = NvtxRegistry.HYBRID_SCAN_FILTER_STATS {
        reader.filterRowGroupsWithStats(inSplit)
      }
      logDebug(s"HybridScan statistics pruning: ${inSplit.length} -> ${afterStats.length} " +
        "row groups")
      if (afterStats.isEmpty) {
        afterStats
      } else if (!pushed.exists(FilterToAstConverter.canPruneWithDictionary)) {
        // Reading the dictionary pages costs an IO, a copy to the device and a semaphore hold,
        // so it is only worth doing for a filter the reader can prune with.
        afterStats
      } else {
        NvtxRegistry.HYBRID_SCAN_FILTER_DICT {
          filterRowGroupsWithDictionaries(reader, afterStats)
        }
      }
    }
  }

  /**
   * Narrow the footer's row groups to the ones this split owns: those whose first byte falls in
   * its range. Splits tile a file, so every row group is claimed by exactly one split.
   *
   * The row group's first byte is the lowest column chunk offset, which costs one call per row
   * group to look up.
   */
  private def rowGroupsInScope(reader: HybridScanReader, rowGroups: Array[Int]): Array[Int] = {
    val splitEnd = splitStart + splitLength
    rowGroups.filter { rowGroup =>
      val ranges = reader.allColumnChunksByteRanges(Array(rowGroup))
      if (ranges.isEmpty) {
        // A row group with no column chunks has no offset to place, so give it to the first
        // split rather than dropping it.
        splitStart == 0
      } else {
        val firstByte = ranges.map(_.offset()).min
        firstByte >= splitStart && firstByte < splitEnd
      }
    }
  }

  private def filterRowGroupsWithDictionaries(
      reader: HybridScanReader,
      rowGroups: Array[Int]): Array[Int] = {
    val dictRanges = reader.dictionaryPagesByteRanges(rowGroups)
    // The reader reports one range per filter column per row group and an empty range for a chunk
    // it will not prune with, so a list of nothing but empty ranges means there is nothing to do.
    // A chunk is skipped both when its pages are not all dictionary encoded and when the footer
    // does not say how they are encoded, which a writer is allowed to leave out, so this says
    // nothing about whether the file really has dictionary pages. The empty ranges are kept and
    // passed along, because the reader matches the buffers it is given to those ranges by position.
    if (totalBytes(dictRanges) == 0) {
      rowGroups
    } else {
      val afterDict = withResource(source.fetch(dictRanges)) { hostPages =>
        compareDictionaryPages(reader, rowGroups, hostPages)
      }
      logDebug(s"HybridScan dictionary pruning: ${rowGroups.length} -> ${afterDict.length} " +
        "row groups")
      afterDict
    }
  }

  /** Compare the dictionary pages against the filter and return the row groups that survive. */
  private def compareDictionaryPages(
      reader: HybridScanReader,
      rowGroups: Array[Int],
      hostPages: Array[HostMemoryBuffer]): Array[Int] = {
    // Dictionary pages are compared on the GPU, so the semaphore is held from the copy over until
    // they are freed. They are small: a writer caps a dictionary page at 1MB and gives up on
    // dictionary encoding the chunk rather than exceed it, and only the filter columns have pages
    // here.
    GpuSemaphore.acquireIfNecessary(TaskContext.get())
    // The host pages outlive each attempt, so a retry copies them again.
    val kept = withRetryNoSplit[Array[Int]] {
      withResource(copyToDevice(hostPages)) { dictPages =>
        reader.filterRowGroupsWithDictionaryPages(dictPages, rowGroups)
      }
    }
    // Leaving the GPU for a while: what survives the pruning is a host-side list of row group
    // indexes, and reading the column chunks it selects is the longest IO of the read.
    GpuSemaphore.releaseIfNecessary(TaskContext.get())
    kept
  }

  private def materialize(
      reader: HybridScanReader,
      rowGroups: Array[Int],
      outputAst: Option[CompiledExpression]): Option[Table] = {
    if (reader.totalRowsInRowGroups(rowGroups) == 0) {
      None
    } else {
      val table = materializeSingleStage(reader, rowGroups, outputAst)
      val materialized = closeOnExcept(table) { t =>
        // The file's own rebase mode is unknown without its key-value metadata, so fall back on
        // the value based check the parquet-mr path uses in EXCEPTION mode: shifted dates are
        // reported rather than returned. isEligible only allows datetime types in that mode.
        GpuParquetScan.throwIfRebaseNeededInExceptionMode(t, DateTimeRebaseException,
          DateTimeRebaseException)
        t
      }
      if (materialized.getRowCount == 0) {
        // The filter accepted no row of any surviving row group. Reported as no table rather than
        // an empty one, so that every way of ending up with no rows looks the same to the caller.
        withResource(materialized) { _ =>
          None
        }
      } else {
        execMetrics(NUM_OUTPUT_BATCHES) += 1
        Some(materialized)
      }
    }
  }

  /**
   * Choose the column-chunk byte ranges the projection needs and fetch them into host memory, off
   * the GPU semaphore so the read does not hold the GPU from another task. The caller owns the
   * returned buffers.
   */
  private def planAndReadColumnChunks(
      reader: HybridScanReader,
      rowGroups: Array[Int]): Array[HostMemoryBuffer] = {
    val ranges = NvtxRegistry.HYBRID_SCAN_GET_BYTE_RANGES {
      reader.allColumnChunksByteRanges(rowGroups)
    }
    NvtxRegistry.HYBRID_SCAN_READ_DATA {
      source.fetch(ranges)
    }
  }

  /**
   * Copy the already-fetched host buffers to the device under the GPU semaphore and decode them
   * into a table. The semaphore is taken only now, after the host read, which is what keeps the
   * fetch off the GPU. A direct-to-device (GDS) source would instead acquire before the fetch and
   * hand `materialize*` device buffers directly.
   *
   * The host buffers stay the caller's to close; this does not take them.
   */
  private def materializeFromHostBuffers(
      reader: HybridScanReader,
      rowGroups: Array[Int],
      hostChunks: Array[HostMemoryBuffer]): Table = {
    GpuSemaphore.acquireIfNecessary(TaskContext.get())
    NvtxRegistry.HYBRID_SCAN_MATERIALIZE {
      // The host chunks outlive each attempt, so a retry copies them again.
      withRetryNoSplit[Table] {
        withResource(copyToDevice(hostChunks)) { deviceChunks =>
          reader.materializeAllColumns(rowGroups, deviceChunks)
        }
      }
    }
  }

  /** Read every column, then apply the filter to what was decoded. */
  private def materializeSingleStage(
      reader: HybridScanReader,
      rowGroups: Array[Int],
      outputAst: Option[CompiledExpression]): Table = {
    closeOnExcept(withResource(planAndReadColumnChunks(reader, rowGroups)) { hostChunks =>
      materializeFromHostBuffers(reader, rowGroups, hostChunks)
    }) { table =>
      // Before the output filter, which references columns by index.
      checkDecodedSchema(table)
      outputAst match {
        case Some(ast) => NvtxRegistry.HYBRID_SCAN_OUTPUT_FILTER {
          applyOutputFilter(table, ast)
        }
        case None => table
      }
    }
  }

  /**
   * Apply `ast` to a table the reader decoded in full, unless it keeps more than
   * `filterKeepThreshold` of the rows.
   *
   * Every projected column of every surviving row group has already been decoded, so the filter
   * saves no decoding here; it shrinks the batch handed to the rest of the query, and the rows
   * that survive have to be copied out to do it. Once nearly all of them survive, that copy of
   * the whole projection costs more than the smaller batch saves, so the rows are returned as
   * they are. Spark's filter above the scan removes the rest either way, which is what makes
   * both choices correct.
   *
   * On return the argument table is either the returned table or has been closed.
   */
  private def applyOutputFilter(table: Table, ast: CompiledExpression): Table = {
    withResource(ast.computeColumn(table)) { mask =>
      // A null verdict rejects the row, matching what the reader would have done with this same
      // filter, so counting the true entries counts exactly the rows a gather would keep.
      val kept = withResource(mask.sum(DType.INT32)) { sum =>
        if (sum.isValid) sum.getInt else 0
      }
      val rows = table.getRowCount
      val keepFraction = if (rows == 0) 0.0 else kept.toDouble / rows
      if (keepFraction > filterKeepThreshold) {
        table
      } else {
        withResource(table) { t =>
          t.filter(mask)
        }
      }
    }
  }

  /**
   * Copy each host buffer to the device, one buffer for one. An empty chunk stays in place as an
   * empty device buffer rather than being dropped, so the result stays aligned with the request.
   */
  private def copyToDevice(hostChunks: Array[HostMemoryBuffer]): Array[DeviceMemoryBuffer] = {
    hostChunks.safeMap { hostChunk =>
      val length = hostChunk.getLength
      closeOnExcept(DeviceMemoryBuffer.allocate(length)) { deviceChunk =>
        if (length > 0) {
          deviceChunk.copyFromHostBuffer(0, hostChunk, 0, length)
        }
        deviceChunk
      }
    }
  }
}

object HybridScanParquetPartitionReader extends Logging {

  /**
   * The reader's column selection. Only the projected columns are decoded, as microsecond
   * timestamps, which is what the parquet-mr path asks libcudf for as well.
   */
  private[parquet] def parquetOptions(columnNames: Array[String]): ParquetOptions = {
    ParquetOptions.builder()
        .withTimeUnit(DType.TIMESTAMP_MICROSECONDS)
        .includeColumn(columnNames: _*)
        .build()
  }

  /**
   * Where a set of byte ranges lands in the one host buffer that holds them all.
   *
   * @param fileOrder the range indexes ordered by where they start in the file
   * @param offsets   where each range starts in the buffer, indexed as the ranges are
   * @param totalSize how large the buffer has to be
   */
  private[parquet] case class HostBufferLayout(
      fileOrder: Array[Int],
      offsets: Array[Long],
      totalSize: Long)

  /**
   * Lay the ranges out in file order rather than in the order they were asked for, so that two
   * ranges adjacent in the file are adjacent in the buffer and their reads can be coalesced into
   * one. The layout is packed: a gap between two ranges in the file is not a gap in the buffer.
   */
  private[parquet] def hostBufferLayout(ranges: Array[ByteRange]): HostBufferLayout = {
    val fileOrder = ranges.indices.sortBy(i => ranges(i).offset()).toArray
    val offsets = new Array[Long](ranges.length)
    var totalSize = 0L
    fileOrder.foreach { i =>
      offsets(i) = totalSize
      totalSize += ranges(i).size()
    }
    HostBufferLayout(fileOrder, offsets, totalSize)
  }

  /**
   * Whether this scan's reads can go through the hybrid scan reader, reporting why not when they
   * cannot. Reported rather than left silent: the hybrid path is off by default and needs several
   * settings to line up, so a scan that quietly stays on parquet-mr is otherwise hard to tell
   * from one that ran. See [[ineligibleReason]] for the other arguments.
   *
   * @param failOnFallback throw rather than fall back, so a test meant to run on the hybrid path
   *                       cannot pass on parquet-mr
   */
  def isEligible(
      hybridScanEnabled: Boolean,
      failOnFallback: Boolean,
      readDataSchema: StructType,
      scanFilters: HybridScanFilters,
      useChunkedReader: Boolean,
      decompressAnyCpu: Boolean,
      useFieldId: Boolean,
      ignoreCorruptFiles: Boolean,
      decryptionConfigured: Boolean,
      dateRebaseMode: DateTimeRebaseMode,
      int96RebaseMode: DateTimeRebaseMode): Boolean = {
    val reason = ineligibleReason(hybridScanEnabled, readDataSchema, scanFilters,
      useChunkedReader, decompressAnyCpu, useFieldId, ignoreCorruptFiles, decryptionConfigured,
      dateRebaseMode, int96RebaseMode)
    if (hybridScanEnabled) {
      reason.foreach { r =>
        if (failOnFallback) {
          throw new IllegalStateException(
            s"Hybrid scan read fell back to the parquet-mr path: $r")
        }
        logWarning(s"Hybrid scan is enabled but this read stays on the parquet-mr path: $r")
      }
    }
    reason.isEmpty
  }

  /**
   * Why a read cannot go through the hybrid scan reader, or None when it can. A read with a
   * reason goes to the parquet-mr path instead. The reason is worth reporting: the hybrid path
   * is off by default and needs several settings to line up, so a read that quietly stays on
   * parquet-mr is otherwise hard to tell from one that ran.
   *
   * @param hybridScanEnabled whether the hybrid scan reader is enabled
   * @param readDataSchema    the columns the scan must produce
   * @param scanFilters       the data filters Spark pushed to the scan
   * @param useChunkedReader  whether the read may be split into several batches
   * @param decompressAnyCpu  whether any codec is set to decompress on the CPU
   * @param useFieldId        whether columns are matched by Parquet field ID
   * @param ignoreCorruptFiles whether a file that fails to read is skipped rather than failing
   *                          the query
   * @param decryptionConfigured whether the Hadoop configuration sets up Parquet decryption
   * @param dateRebaseMode    the configured date rebase-on-read mode
   * @param int96RebaseMode   the configured INT96 timestamp rebase-on-read mode
   */
  def ineligibleReason(
      hybridScanEnabled: Boolean,
      readDataSchema: StructType,
      scanFilters: HybridScanFilters,
      useChunkedReader: Boolean,
      decompressAnyCpu: Boolean,
      useFieldId: Boolean,
      ignoreCorruptFiles: Boolean,
      decryptionConfigured: Boolean,
      dateRebaseMode: DateTimeRebaseMode,
      int96RebaseMode: DateTimeRebaseMode): Option[String] = {
    // The hybrid path produces the whole split as one batch, reads the
    // column chunks as the file stores them (no CPU decompression pass), and matches columns by
    // name.
    if (!hybridScanEnabled) {
      Some(s"the hybrid scan reader is disabled (${RapidsConf.PARQUET_HYBRID_SCAN_ENABLED.key})")
    } else if (useChunkedReader) {
      Some(s"the chunked reader is enabled (${RapidsConf.CHUNKED_READER.key})")
    } else if (decompressAnyCpu) {
      Some("a codec is set to decompress on the CPU")
    } else if (useFieldId) {
      Some("columns are matched by Parquet field ID")
    } else if (ignoreCorruptFiles) {
      // The reader fails a file whose schema it cannot decode, which would skip a readable file.
      Some("corrupt files are ignored (spark.sql.files.ignoreCorruptFiles)")
    } else if (decryptionConfigured) {
      // libcudf skips a column's encryption metadata, so an encrypted column would fail in decode
      // with no mention of encryption. The parquet-mr path reports it as unsupported.
      Some("Parquet decryption is configured")
    } else if (readDataSchema.isEmpty) {
      Some("the read projects no columns")
    } else {
      schemaReason(readDataSchema, dateRebaseMode, int96RebaseMode)
          .orElse(scanFilters.ineligibleReason)
    }
  }

  private def schemaReason(
      readDataSchema: StructType,
      dateRebaseMode: DateTimeRebaseMode,
      int96RebaseMode: DateTimeRebaseMode): Option[String] = {
    readDataSchema.fields.find(field => !isSupportedType(field.dataType)) match {
      case Some(field) =>
        Some(s"column ${field.name} has type ${field.dataType.simpleString}")
      case None if !rebaseCheckable(readDataSchema, dateRebaseMode, int96RebaseMode) =>
        Some(s"a datetime column is read with rebase modes ${dateRebaseMode.value}/" +
          s"${int96RebaseMode.value} rather than ${DateTimeRebaseException.value}")
      case None => None
    }
  }

  /**
   * Types the hybrid path reads with no schema handling of its own. Nested types are excluded
   * because the projection is a flat list of top-level column names, TimestampNTZType because it
   * needs the file schema to be read correctly, and BinaryType because libcudf decodes a Parquet
   * byte array as a string, so the column would come back as the wrong type.
   */
  private def isSupportedType(dataType: DataType): Boolean = dataType match {
    case BooleanType | ByteType | ShortType | IntegerType | LongType => true
    case FloatType | DoubleType => true
    case StringType => true
    case DateType | TimestampType => true
    case _: DecimalType => true
    case _ => false
  }

  /**
   * Whether a needed datetime rebase would be caught. The reader checks materialized values the
   * way the parquet-mr path does in EXCEPTION mode, so any other mode has to read the file's own
   * metadata to be correct, which this path does not parse.
   */
  private def rebaseCheckable(
      readDataSchema: StructType,
      dateRebaseMode: DateTimeRebaseMode,
      int96RebaseMode: DateTimeRebaseMode): Boolean = {
    val hasDateTime = readDataSchema.exists { field =>
      field.dataType == DateType || field.dataType == TimestampType
    }
    !hasDateTime || (dateRebaseMode == DateTimeRebaseException &&
      int96RebaseMode == DateTimeRebaseException)
  }
}

/**
 * The settings that shape a hybrid read, which the read itself does not decide. Everything else
 * about it — which row groups survive, which byte ranges are needed — comes from the reader.
 */
case class HybridScanConfig(
    filters: HybridScanFilters,
    filterKeepThreshold: Double)

object HybridScanConfig {
  def apply(rapidsConf: RapidsConf, filters: HybridScanFilters): HybridScanConfig = {
    HybridScanConfig(filters, rapidsConf.hybridScanSingleStageFilterKeepThreshold)
  }
}

/**
 * A scan's data filters as its hybrid reads use them, worked out once for the whole scan. Which
 * conjuncts to push is decided on the driver; their ASTs are built lazily on the executor, since
 * cuDF's AST does not serialize.
 *
 * @param readDataSchema  the columns the scan must produce
 * @param isCaseSensitive whether column name matching is case-sensitive
 * @param filters         the data filters Spark pushed to the scan
 */
class HybridScanFilters(
    readDataSchema: StructType,
    isCaseSensitive: Boolean,
    val filters: Array[Filter]) extends Serializable {

  private val byName = FilterToAstConverter(readDataSchema, isCaseSensitive,
    useColumnNames = true)

  private val conjuncts = FilterToAstConverter.splitConjuncts(filters)

  /**
   * Conjuncts on a timestamp column are withheld: it may be stored as INT96, whose statistics
   * cannot be compared with a literal, and only the file's schema says which. So are conjuncts
   * with a date before 1582-10-15: a file written with LEGACY rebasing stores such dates in
   * another calendar, which only its footer metadata says, so pruning could drop every row group
   * before the decode-time rebase check sees one. Spark applies the withheld ones above the scan.
   */
  private def isPushable(conjunct: Filter): Boolean =
    !FilterToAstConverter.getReferencedColumns(conjunct)
      .exists(byName.columnType(_).contains(TimestampType)) &&
      !FilterToAstConverter.getLiterals(conjunct).flatMap(FilterToAstConverter.dateLiteralDays)
        .exists(_ < RebaseDateTime.lastSwitchJulianDay)

  /** The conjuncts the reader may be given. */
  val pushable: Array[Filter] = conjuncts.filter(isPushable)

  /**
   * The conjuncts the reader applies, and those Spark applies above the scan. Eligibility decides
   * them on the driver, and they are serialized with the factory so executors do not redecide.
   */
  private lazy val split: (Array[Filter], Array[Filter]) = {
    val plan = byName.planPushdown(pushable)
    (plan.pushed, conjuncts.filterNot(isPushable) ++ plan.residual)
  }

  def pushed: Array[Filter] = split._1

  def residual: Array[Filter] = split._2

  /** `pushed`'s AST, referencing columns by name, for the reader to prune and filter with. */
  @transient lazy val ast: Option[AstExpression] = byName.pushedAst(pushed)

  /** `pushed`'s AST referencing columns by projection index, to filter the reader's output. */
  @transient lazy val outputAst: Option[AstExpression] =
    FilterToAstConverter(readDataSchema, isCaseSensitive, useColumnNames = false)
      .pushedAst(pushed)

  /**
   * Why the filter is not safe to push, or None. Every conjunct left over has to be one the
   * scan's own output can be filtered on afterwards.
   */
  def ineligibleReason: Option[String] = {
    if (!byName.resolvesAllColumns(residual)) {
      Some("a conjunct that cannot be pushed reads a column the scan does not output")
    } else {
      None
    }
  }
}
