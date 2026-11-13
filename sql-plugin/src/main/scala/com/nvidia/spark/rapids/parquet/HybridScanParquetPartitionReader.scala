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
import com.nvidia.spark.rapids.shims.SparkShimImpl
import org.apache.hadoop.conf.Configuration

import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.util.RebaseDateTime
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * What the plan phase hands the materialize phase: the row groups the read kept and the
 * column-chunk bytes fetched for them, one host buffer per planned range in request order.
 * `backing` is the one buffer the chunks slice, when they came straight from a fetch. The host
 * buffers are this object's to close, and closing it again does nothing.
 */
private[parquet] class FetchedColumnChunks(
    val rowGroups: Array[Int],
    val hostChunks: Array[HostMemoryBuffer],
    val backing: Option[HostMemoryBuffer] = None) extends AutoCloseable {
  private var closed = false

  def isClosed: Boolean = closed

  override def close(): Unit = {
    if (!closed) {
      closed = true
      (hostChunks ++ backing).safeClose()
    }
  }
}

/** What a hybrid read takes from its scan, the same for every file the scan reads. */
private[parquet] case class HybridScanContext(
    readDataSchema: StructType,
    scanFilters: HybridScanFilters,
    filterKeepThreshold: Double,
    execMetrics: Map[String, GpuMetric])

/** The part of one file a hybrid read owns: its row groups whose first byte is in the range. */
private[parquet] case class HybridScanSplit(
    source: HybridScanByteSource,
    start: Long,
    length: Long)

/**
 * The hybrid Parquet read pipeline, shared across the modes that drive it: prune the row groups
 * the pushed filter rules out, fetch the column chunks that survive, and materialize them into a
 * table. Built once per scan; each call is given the split it reads. It is split into a phase that
 * selects and fetches ([[planAndFetch]]) and one that decodes ([[materializeFetched]]). Dictionary
 * pruning and the decode take the GPU semaphore, so only the task thread may run them. It holds no
 * per-read state, so pool threads may share it.
 */
private[parquet] class HybridScanPipeline(context: HybridScanContext) extends Logging {
  import context.{execMetrics, filterKeepThreshold, readDataSchema, scanFilters}

  private def totalBytes(ranges: Array[ByteRange]): Long = ranges.map(_.size()).sum

  /**
   * The plan phase: prune the split's row groups with the pushed filter, and if anything survives
   * fetch its column chunks into host memory, off the GPU semaphore so the read does not hold the
   * GPU from another task. None when the split owns no surviving rows. The result is the caller's.
   *
   * Dictionary pruning copies pages to the device under the semaphore, which only a task thread
   * may do, so a `MULTITHREADED` pool thread passes `allowDictionaryPruning = false` and the task
   * prunes the fetched chunks instead ([[pruneFetchedWithDictionaries]]). A direct-to-device
   * (GDS) source would instead acquire the semaphore and fetch straight to the device.
   */
  def planAndFetch(
      split: HybridScanSplit,
      reader: HybridScanReader,
      ast: Option[CompiledExpression],
      allowDictionaryPruning: Boolean): Option[FetchedColumnChunks] = {
    val rowGroups = selectRowGroups(split, reader, ast, allowDictionaryPruning)
    if (rowGroups.isEmpty || reader.totalRowsInRowGroups(rowGroups) == 0) {
      None
    } else {
      setDecodeFilter(reader, ast)
      val ranges = NvtxRegistry.HYBRID_SCAN_GET_BYTE_RANGES {
        reader.allColumnChunksByteRanges(rowGroups)
      }
      val buffer = NvtxRegistry.HYBRID_SCAN_READ_DATA {
        split.source.fetchToBuffer(ranges)
      }
      closeOnExcept(buffer) { _ =>
        val hostChunks = HybridScanPipeline.sliceRanges(buffer, ranges)
        Some(new FetchedColumnChunks(rowGroups, hostChunks, Some(buffer)))
      }
    }
  }

  /**
   * The materialize phase: set the decode-time filter, materialize the fetched chunks, and apply
   * the output filter. Takes `fetched`, freeing its host chunks once they are decoded. The reader
   * stays the caller's to close.
   */
  def materializeFetched(
      reader: HybridScanReader,
      ast: Option[CompiledExpression],
      fetched: FetchedColumnChunks): Option[Table] = {
    require(!fetched.isClosed, "hybrid scan column chunks were already materialized or closed")
    val outputAst = closeOnExcept(fetched)(_ => outputFilter(ast.isDefined))
    withResource(outputAst) { o =>
      materialize(reader, fetched, ast, o)
    }
  }

  /**
   * Prune the row groups this split owns down to those the pushed filter cannot rule out. The
   * reader's filter is set to `ast` here, which is what its statistics and dictionary pruning
   * compare against.
   */
  private def selectRowGroups(
      split: HybridScanSplit,
      reader: HybridScanReader,
      ast: Option[CompiledExpression],
      allowDictionaryPruning: Boolean): Array[Int] = {
    ast.foreach(reader.setFilter)
    val inSplit = rowGroupsInScope(split, reader, reader.allRowGroups())
    if (inSplit.isEmpty || ast.isEmpty) {
      inSplit
    } else {
      // Runs on the GPU without the semaphore, as it exists to bound large allocations. This
      // only allocates a few bytes per row group and frees them before returning. 
      // The retry covers an OOM when other tasks have filled the GPU.
      val afterStats = NvtxRegistry.HYBRID_SCAN_FILTER_STATS {
        withRetryNoSplit[Array[Int]] {
          reader.filterRowGroupsWithStats(inSplit)
        }
      }
      logDebug(s"HybridScan statistics pruning: ${inSplit.length} -> ${afterStats.length} " +
        "row groups")
      if (afterStats.isEmpty) {
        afterStats
      } else if (!allowDictionaryPruning ||
          !scanFilters.pushed.exists(FilterToAstConverter.canPruneWithDictionary)) {
        // Reading the dictionary pages costs an IO, a copy to the device and a semaphore hold,
        // so it is only worth doing for a filter the reader can prune with.
        afterStats
      } else {
        NvtxRegistry.HYBRID_SCAN_FILTER_DICT {
          filterRowGroupsWithDictionaries(reader, afterStats, split.source.fetch(_))
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
  private def rowGroupsInScope(
      split: HybridScanSplit,
      reader: HybridScanReader,
      rowGroups: Array[Int]): Array[Int] = {
    val splitEnd = split.start + split.length
    rowGroups.filter { rowGroup =>
      val ranges = reader.allColumnChunksByteRanges(Array(rowGroup))
      if (ranges.isEmpty) {
        // A row group with no column chunks has no offset to place, so give it to the first
        // split rather than dropping it.
        split.start == 0
      } else {
        val firstByte = ranges.map(_.offset()).min
        firstByte >= split.start && firstByte < splitEnd
      }
    }
  }

  /**
   * `getDictPages` supplies the dictionary page buffers for the ranges the reader asks for: read
   * from the file by a single-threaded read, sliced from the fetched chunks by a `MULTITHREADED`
   * task. The buffers it returns are closed here.
   */
  private def filterRowGroupsWithDictionaries(
      reader: HybridScanReader,
      rowGroups: Array[Int],
      getDictPages: Array[ByteRange] => Array[HostMemoryBuffer]): Array[Int] = {
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
      val afterDict = withResource(getDictPages(dictRanges)) { hostPages =>
        compareDictionaryPages(reader, rowGroups, hostPages)
      }
      logDebug(s"HybridScan dictionary pruning: ${rowGroups.length} -> ${afterDict.length} " +
        "row groups")
      afterDict
    }
  }

  /**
   * Prune already-fetched chunks to the row groups the filter's dictionaries cannot rule out,
   * slicing the dictionary pages out of the chunks rather than reading them again. The pruned row
   * groups' IO is already spent, but their device copy and decode are saved. Takes `fetched` and
   * returns what to decode: `fetched` itself when nothing is pruned, the surviving chunks when
   * some are, or None when no row group survives.
   */
  def pruneFetchedWithDictionaries(
      reader: HybridScanReader,
      ast: Option[CompiledExpression],
      fetched: FetchedColumnChunks): Option[FetchedColumnChunks] = {
    if (ast.isEmpty ||
        !scanFilters.pushed.exists(FilterToAstConverter.canPruneWithDictionary)) {
      Some(fetched)
    } else {
      val survivors = closeOnExcept(fetched) { f =>
        ast.foreach(reader.setFilter)
        NvtxRegistry.HYBRID_SCAN_FILTER_DICT {
          filterRowGroupsWithDictionaries(reader, f.rowGroups,
            sliceDictPagesFromChunks(reader, f.rowGroups, f.hostChunks, _))
        }
      }
      if (survivors.length == fetched.rowGroups.length) {
        Some(fetched)
      } else {
        withResource(fetched) { f =>
          if (survivors.isEmpty) None else Some(selectSurvivingChunks(f, survivors))
        }
      }
    }
  }

  /**
   * The dictionary pages for `dictRanges`, sliced from the fetched chunks. A dictionary page is
   * the first page of its column chunk, so a non-empty range starts where exactly one chunk does
   * and is that chunk's leading bytes. An empty range (a chunk the reader will not prune with)
   * keeps its slot as an empty buffer, since the reader matches buffers to ranges by position.
   * The slices are the caller's to close.
   */
  private def sliceDictPagesFromChunks(
      reader: HybridScanReader,
      rowGroups: Array[Int],
      chunks: Array[HostMemoryBuffer],
      dictRanges: Array[ByteRange]): Array[HostMemoryBuffer] = {
    val chunkRanges = reader.allColumnChunksByteRanges(rowGroups)
    require(chunkRanges.length == chunks.length,
      "hybrid scan column chunk count does not match the fetched buffers")
    val chunkByOffset = chunkRanges.indices.map(i => chunkRanges(i).offset() -> i).toMap
    dictRanges.safeMap { r =>
      if (r.size() == 0) {
        chunks.head.slice(0, 0)
      } else {
        chunks(chunkByOffset(r.offset())).slice(0, r.size())
      }
    }
  }

  /**
   * The chunks of the surviving row groups, in row group order, as new slices that own their own
   * references, so `fetched` can be closed independently. Every selected column has one chunk in
   * every row group, so each row group's run of chunks is the same length.
   */
  private def selectSurvivingChunks(
      fetched: FetchedColumnChunks,
      survivors: Array[Int]): FetchedColumnChunks = {
    val numRowGroups = fetched.rowGroups.length
    val chunksPerRowGroup = fetched.hostChunks.length / numRowGroups
    require(chunksPerRowGroup * numRowGroups == fetched.hostChunks.length,
      "hybrid scan column chunks do not divide evenly into row groups")
    val survivorSet = survivors.toSet
    val kept = fetched.rowGroups.indices.filter(i => survivorSet.contains(fetched.rowGroups(i)))
    val keptChunks = kept.flatMap { i =>
      fetched.hostChunks.slice(i * chunksPerRowGroup, (i + 1) * chunksPerRowGroup)
    }.toArray
    new FetchedColumnChunks(kept.map(fetched.rowGroups(_)).toArray,
      keptChunks.safeMap(c => c.slice(0, c.getLength)))
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

  /**
   * Under the GPU semaphore, copy the already-fetched host buffers to the device and decode them,
   * then check the decoded schema, apply the output filter and the rebase check. The semaphore is
   * taken only now, after the host read, which is what keeps the fetch off the GPU. None when no
   * row survives the filter.
   *
   * The reader's decode-time filter is set here too, since a reader other than the one that
   * planned may decode. Takes `fetched` and closes it as soon as it is decoded, before the output
   * filter runs.
   */
  private def materialize(
      reader: HybridScanReader,
      fetched: FetchedColumnChunks,
      ast: Option[CompiledExpression],
      outputAst: Option[CompiledExpression]): Option[Table] = {
    val table = withResource(fetched) { f =>
      setDecodeFilter(reader, ast)
      materializeFromHostBuffers(reader, f)
    }
    val filtered = closeOnExcept(table) { t =>
      // Before the output filter, which references columns by index.
      checkDecodedSchema(t)
      outputAst.flatMap { compiled =>
        NvtxRegistry.HYBRID_SCAN_OUTPUT_FILTER {
          // The decoded table outlives each attempt, so a retry filters it again.
          withRetryNoSplit[Option[Table]] {
            applyOutputFilter(t, compiled)
          }
        }
      }
    }
    val decoded = filtered match {
      case Some(f) => withResource(table)(_ => f)
      case None => table
    }
    val materialized = closeOnExcept(decoded) { t =>
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

  /** Copy the fetched host buffers to the device under the semaphore and decode them. */
  private def materializeFromHostBuffers(
      reader: HybridScanReader,
      fetched: FetchedColumnChunks): Table = {
    GpuSemaphore.acquireIfNecessary(TaskContext.get())
    NvtxRegistry.HYBRID_SCAN_MATERIALIZE {
      // The host chunks outlive each attempt, so a retry copies them again.
      withRetryNoSplit[Table] {
        withResource(copyToDevice(fetched.hostChunks)) { deviceChunks =>
          reader.materializeAllColumns(fetched.rowGroups, deviceChunks)
        }
      }
    }
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
   * None when the rows are returned as they are. Never closes `table`.
   */
  private def applyOutputFilter(
      table: Table,
      ast: CompiledExpression): Option[Table] = {
    withResource(ast.computeColumn(table)) { mask =>
      // A null verdict rejects the row, matching what the reader would have done with this same
      // filter, so counting the true entries counts exactly the rows a gather would keep.
      val kept = withResource(mask.sum(DType.INT32)) { sum =>
        if (sum.isValid) sum.getInt else 0
      }
      val rows = table.getRowCount
      val keepFraction = if (rows == 0) 0.0 else kept.toDouble / rows
      if (keepFraction > filterKeepThreshold) None else Some(table.filter(mask))
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
    if (filtersOnOutput(hasFilter)) {
      scanFilters.outputAst.map(HybridScanPipeline.compileFilter)
    } else {
      None
    }
  }

  /** Whether the output filter applies the pushed filter, rather than the reader during decode. */
  private def filtersOnOutput(hasFilter: Boolean): Boolean =
    hasFilter && filterKeepThreshold < 1.0 && scanFilters.outputAst.isDefined

  /**
   * Set the filter the reader takes byte ranges and decodes with: none when the output filter
   * applies it instead, else `ast`. libcudf selects columns from it, so the fetch and the decode
   * must both use it.
   */
  private def setDecodeFilter(reader: HybridScanReader, ast: Option[CompiledExpression]): Unit =
    if (filtersOnOutput(ast.isDefined)) reader.setFilter(null) else ast.foreach(reader.setFilter)

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

/**
 * The parts of the hybrid scan that take no per-scan state, so they are called without
 * constructing a `HybridScanPipeline`: parsing the footer, choosing the columns the reader
 * decodes, laying out byte ranges, and deciding on the driver whether a scan may take the hybrid
 * path at all.
 */
object HybridScanPipeline extends Logging {

  /** Parse the footer into a reader. The caller owns and closes both the reader and the footer. */
  private[parquet] def openReader(
      footer: HostMemoryBuffer,
      parquetOptions: ParquetOptions): HybridScanReader = {
    NvtxRegistry.HYBRID_SCAN_PARSE_FOOTER {
      new HybridScanReader(footer, parquetOptions)
    }
  }

  /** Compile a filter. Each literal allocates a GPU scalar, so an OOM here is retried. */
  private[parquet] def compileFilter(expr: AstExpression): CompiledExpression =
    withRetryNoSplit[CompiledExpression](expr.compile())

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
   * One slice per range of a buffer laid out by [[hostBufferLayout]], in the order requested.
   * Each slice holds a reference to the buffer, which stays the caller's to close.
   */
  private[parquet] def sliceRanges(
      buffer: HostMemoryBuffer,
      ranges: Array[ByteRange]): Array[HostMemoryBuffer] = {
    val layout = hostBufferLayout(ranges)
    ranges.indices.toArray.safeMap(i => buffer.slice(layout.offsets(i), ranges(i).size()))
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
      reason.foreach(reportFallback(failOnFallback, _))
    }
    reason.isEmpty
  }

  /**
   * The hybrid scan config for a scan, or None to keep it on parquet-mr. Both Parquet factories
   * call this so they decide the same way. See [[isEligible]] for what makes a scan eligible.
   *
   * @param hybridScanAllowed whether the caller supports the hybrid scan for this scan
   */
  def configFor(
      rapidsConf: RapidsConf,
      sqlConf: SQLConf,
      hadoopConf: Configuration,
      hybridScanAllowed: Boolean,
      readDataSchema: StructType,
      filters: Array[Filter],
      useChunkedReader: Boolean,
      compressCfg: CpuCompressionConfig,
      useFieldId: Boolean): Option[HybridScanConfig] = {
    val failOnFallback = rapidsConf.testParquetHybridScanFailOnFallback
    if (!rapidsConf.isParquetHybridScanEnabled) {
      None
    } else if (!hybridScanAllowed) {
      reportFallback(failOnFallback, "the hybrid scan is not supported for this read in this " +
        s"configuration, see ${RapidsConf.PARQUET_HYBRID_SCAN_ENABLED.key}")
      None
    } else {
      val pushedFilters = if (sqlConf.parquetFilterPushDown) filters else Array.empty[Filter]
      val config = HybridScanConfig(rapidsConf,
        new HybridScanFilters(readDataSchema, sqlConf.caseSensitiveAnalysis, pushedFilters))
      val eligible = isEligible(hybridScanEnabled = true, failOnFallback, readDataSchema,
        config.filters, useChunkedReader,
        compressCfg.decompressSnappyCpu || compressCfg.decompressZstdCpu, useFieldId,
        sqlConf.ignoreCorruptFiles, GpuParquetFileFilterHandler.isDecryptionConfigured(hadoopConf),
        DateTimeRebaseMode.fromName(SparkShimImpl.parquetRebaseRead(sqlConf)),
        DateTimeRebaseMode.fromName(SparkShimImpl.int96ParquetRebaseRead(sqlConf)))
      if (eligible) Some(config) else None
    }
  }

  /** Whether the multi-file reader type can run the hybrid scan, which COALESCING cannot. */
  def multiFileReaderTypeAllowsHybrid(rapidsConf: RapidsConf): Boolean =
    !rapidsConf.isParquetCoalesceFileReadEnabled || rapidsConf.isParquetAutoReaderEnabled

  /** Report that a read stays on the parquet-mr path although the hybrid scan is enabled. */
  def reportFallback(failOnFallback: Boolean, reason: String): Unit = {
    if (failOnFallback) {
      throw new IllegalStateException(
        s"Hybrid scan read fell back to the parquet-mr path: $reason")
    }
    logWarning(s"Hybrid scan is enabled but this read stays on the parquet-mr path: $reason")
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
 * A `PERFILE` scan builds one per split, which owns the row groups whose first byte falls in the
 * split's byte range.
 *
 * It is gated by `isEligible` to the cases it handles: one batch per read (no chunking), a flat
 * schema of primitive types, and no field IDs.
 * Datetime values are checked for a needed rebase after materialize, since the footer metadata
 * that records how the file was written is not parsed here.
 */
private[parquet] class HybridScanPerFileParquetPartitionReader(
    split: HybridScanSplit,
    footer: HostMemoryBuffer,
    parquetOptions: ParquetOptions,
    context: HybridScanContext)
  extends PartitionReader[ColumnarBatch] {

  import HybridScanPipeline.openReader

  private val pipeline = new HybridScanPipeline(context)
  private var batchIter: GpuColumnarBatchIterator = _
  private var footerClosed = false

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
          GpuColumnVector.from(t, context.readDataSchema.fields.map(_.dataType)))
      }
    case None => EmptyGpuColumnarBatchIterator
  }

  /**
   * The read, start to finish on one thread: parse the footer, prune and fetch what the filter
   * leaves, then decode it. None when nothing survives, either because this read owns no row
   * groups or because the filter ruled them all out.
   */
  private def readTable(): Option[Table] = {
    NvtxRegistry.PARQUET_READ_BATCH {
      withResource(context.scanFilters.ast.map(HybridScanPipeline.compileFilter)) { ast =>
        withResource(openReader(footer, parquetOptions)) { reader =>
          // The footer bytes are only read while the reader is being constructed.
          closeFooter()
          pipeline.planAndFetch(split, reader, ast, allowDictionaryPruning = true)
            .flatMap(pipeline.materializeFetched(reader, ast, _))
        }
      }
    }
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
