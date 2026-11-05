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

import java.io.EOFException
import java.nio.ByteBuffer
import java.util
import java.util.Locale
import java.util.concurrent.atomic.AtomicLong

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal

import com.nvidia.spark.rapids.Arm.withResource
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.HadoopReadOptions
import org.apache.parquet.column.Encoding
import org.apache.parquet.filter2.predicate.{FilterPredicate, LogicalInverseRewriter}
import org.apache.parquet.filter2.statisticslevel.StatisticsFilter
import org.apache.parquet.hadoop.{ParquetFileReader, ParquetInputFormat}
import org.apache.parquet.hadoop.ParquetFileWriter.MAGIC
import org.apache.parquet.hadoop.metadata.{BlockMetaData, ColumnChunkMetaData}
import org.apache.parquet.io.{InputFile, SeekableInputStream => ParquetSeekableInputStream}

import org.apache.spark.internal.Logging

/**
 * Row group filtering (statistics, dictionary and bloom filter levels) with dictionary pages
 * prefetched through the plugin file IO.
 *
 * A path based ParquetFileReader opens the file through Hadoop itself and reads each dictionary
 * page with a serial seek + read on one stream. Here parquet-mr runs the same filtering on a
 * virtual InputFile instead: the dictionary pages that dictionary filtering may need are fetched
 * up front with one vectored read, the footer is served from the bytes already read, and any
 * other read (e.g. a bloom filter) becomes a positional read through the plugin file IO.
 */
object ParquetDictFilterPrefetch extends Logging {
  // do not prefetch when one file needs more dictionary bytes than this
  private val MAX_PREFETCH_BYTES = 64L << 20
  // reads outside the prefetched ranges fetch this much at once
  private val MISS_WINDOW = 64 << 10

  /** Reads (offset, length) ranges of the file, returning the bytes of each range. */
  type RangeReader = Seq[(Long, Int)] => Seq[Array[Byte]]

  private val failures = new AtomicLong

  /**
   * Row groups of `blocks` kept by parquet-mr filtering with `predicate` and the filter levels
   * enabled in `conf`, as the same BlockMetaData objects. Falls back to `legacy` on any failure.
   *
   * @param footerFile the footer as a minimal parquet file: MAGIC + footer + length + MAGIC
   * @param range split range to apply when `footerFile` holds all row groups of the file,
   *              None when it holds exactly `blocks`
   * @param filterColumns lower case top level names of the columns the filters reference; their
   *                      dictionary pages are prefetched, other reads are served on demand
   */
  def rowGroups(
      conf: Configuration,
      filePath: Path,
      footerFile: Array[Byte],
      range: Option[(Long, Long)],
      blocks: util.List[BlockMetaData],
      predicate: FilterPredicate,
      filterColumns: Set[String],
      read: RangeReader,
      legacy: () => util.List[BlockMetaData]): util.List[BlockMetaData] = {
    try {
      filter(conf, footerFile, range, blocks, predicate, filterColumns, read)
    } catch {
      case NonFatal(e) =>
        if (failures.incrementAndGet() <= 10) {
          logWarning(s"Dictionary filter prefetch failed for $filePath, using parquet-mr", e)
        }
        legacy()
    }
  }

  private[parquet] def filter(
      conf: Configuration,
      footerFile: Array[Byte],
      range: Option[(Long, Long)],
      blocks: util.List[BlockMetaData],
      predicate: FilterPredicate,
      filterColumns: Set[String],
      read: RangeReader): util.List[BlockMetaData] = {
    if (blocks.isEmpty) {
      blocks
    } else {
      val ranges = dictionaryRanges(conf, blocks, predicate, filterColumns)
      val data = if (ranges.isEmpty) Seq.empty else read(ranges)
      require(data.length == ranges.length, s"read ${data.length} of ${ranges.length} ranges")
      // The footer goes right after the last column chunk of `blocks`; every offset parquet-mr
      // reads for these row groups lies below that, so the virtual file needs no real length.
      val dataEnd = blocks.asScala.flatMap(_.getColumns.asScala)
        .map(c => c.getStartingPos + c.getTotalSize).max
      val inputFile = new PrefetchedInputFile(dataEnd, footerFile, ranges, data, read, MISS_WINDOW)
      val builder = HadoopReadOptions.builder(conf)
      range.foreach { case (start, end) => builder.withRange(start, end) }
      val kept = withResource(new ParquetFileReader(inputFile, builder.build())) { reader =>
        reader.getRowGroups
      }
      // parquet-mr parsed its own copy of the footer; map back to the caller's block objects,
      // which may carry extra state such as row index offsets
      val byPos = blocks.asScala.map(b => b.getStartingPos -> b).toMap
      require(byPos.size == blocks.size, "row groups with the same starting position")
      val out = new util.ArrayList[BlockMetaData](kept.size)
      kept.asScala.foreach { b =>
        out.add(byPos.getOrElse(b.getStartingPos,
          throw new IllegalStateException(s"unknown row group at ${b.getStartingPos}")))
      }
      out
    }
  }

  /** Byte ranges of the dictionary pages that dictionary filtering may read, sorted. */
  private def dictionaryRanges(
      conf: Configuration,
      blocks: util.List[BlockMetaData],
      predicate: FilterPredicate,
      filterColumns: Set[String]): Seq[(Long, Int)] = {
    if (!conf.getBoolean(ParquetInputFormat.DICTIONARY_FILTERING_ENABLED, true)) {
      Seq.empty
    } else {
      val useStats = conf.getBoolean(ParquetInputFormat.STATS_FILTERING_ENABLED, true)
      val pred = LogicalInverseRewriter.rewrite(predicate)
      val ranges = new ArrayBuffer[(Long, Int)]
      blocks.asScala.foreach { block =>
        // row groups dropped by statistics never reach the dictionary level
        if (!(useStats && canDropByStats(pred, block))) {
          block.getColumns.asScala.foreach { c =>
            val name = c.getPath.toArray.headOption.map(_.toLowerCase(Locale.ROOT))
            if (name.exists(filterColumns.contains) && c.hasDictionaryPage &&
                !hasNonDictionaryPages(c)) {
              // readDictionary reads the page header and the page from the chunk start
              val len = c.getFirstDataPageOffset - c.getStartingPos
              if (len > 0 && len <= Int.MaxValue) {
                ranges += ((c.getStartingPos, len.toInt))
              }
            }
          }
        }
      }
      if (ranges.map(_._2.toLong).sum > MAX_PREFETCH_BYTES) {
        Seq.empty
      } else {
        ranges.sortBy(_._1).toList
      }
    }
  }

  private def canDropByStats(pred: FilterPredicate, block: BlockMetaData): Boolean = {
    try {
      StatisticsFilter.canDrop(pred, block.getColumns)
    } catch {
      case NonFatal(_) => false
    }
  }

  // same rule as parquet-mr DictionaryFilter.hasNonDictionaryPages
  @scala.annotation.nowarn("msg=in Java enum Encoding is deprecated")
  private def hasNonDictionaryPages(meta: ColumnChunkMetaData): Boolean = {
    val stats = meta.getEncodingStats
    if (stats != null) {
      stats.hasNonDictionaryEncodedPages
    } else {
      val encodings = new util.HashSet[Encoding](meta.getEncodings)
      if (encodings.remove(Encoding.PLAIN_DICTIONARY)) {
        encodings.remove(Encoding.RLE)
        encodings.remove(Encoding.BIT_PACKED)
        !encodings.isEmpty
      } else {
        true
      }
    }
  }
}

/**
 * Virtual parquet file for ParquetDictFilterPrefetch: offsets below `dataEnd` come from the
 * prefetched ranges, or are read on demand in windows when not prefetched, and the footer
 * (`footerFile` without its leading MAGIC) sits at `dataEnd`.
 */
private class PrefetchedInputFile(
    dataEnd: Long,
    footerFile: Array[Byte],
    ranges: Seq[(Long, Int)],
    data: Seq[Array[Byte]],
    read: ParquetDictFilterPrefetch.RangeReader,
    missWindow: Int) extends InputFile {
  require(footerFile.length >= 2 * MAGIC.length + Integer.BYTES &&
    util.Arrays.equals(footerFile.take(MAGIC.length), MAGIC), "not a parquet footer file")
  private val tailLength = footerFile.length - MAGIC.length
  private val starts = ranges.map(_._1).toArray
  private val ends = ranges.map(r => r._1 + r._2).toArray
  private val bytes = data.toArray
  private var window: Array[Byte] = Array.emptyByteArray
  private var windowStart = 0L

  override def getLength: Long = dataEnd + tailLength

  override def newStream(): ParquetSeekableInputStream = new PrefetchedSeekableInputStream(this)

  /** Copies up to `len` bytes at `pos` into `b`, returning the count or -1 at end of file. */
  def readAt(pos: Long, b: Array[Byte], off: Int, len: Int): Int = {
    if (len == 0) {
      0
    } else if (pos < 0 || pos >= getLength) {
      -1
    } else if (pos >= dataEnd) {
      copy(footerFile, MAGIC.length + (pos - dataEnd), getLength - pos, b, off, len)
    } else {
      // last range starting at or before pos
      var lo = 0
      var hi = starts.length - 1
      while (lo <= hi) {
        val mid = (lo + hi) >>> 1
        if (starts(mid) <= pos) lo = mid + 1 else hi = mid - 1
      }
      if (hi >= 0 && pos < ends(hi)) {
        copy(bytes(hi), pos - starts(hi), ends(hi) - pos, b, off, len)
      } else {
        if (pos < windowStart || pos >= windowStart + window.length) {
          val n = math.min(missWindow.toLong, dataEnd - pos).toInt
          window = read(Seq((pos, n))).head
          windowStart = pos
        }
        copy(window, pos - windowStart, window.length - (pos - windowStart), b, off, len)
      }
    }
  }

  private def copy(src: Array[Byte], srcPos: Long, available: Long, b: Array[Byte], off: Int,
      len: Int): Int = {
    val n = math.min(len.toLong, available).toInt
    System.arraycopy(src, srcPos.toInt, b, off, n)
    n
  }
}

private class PrefetchedSeekableInputStream(file: PrefetchedInputFile)
    extends ParquetSeekableInputStream {
  private var pos = 0L
  private val oneByte = new Array[Byte](1)
  private val temp = new Array[Byte](8192)

  override def getPos: Long = pos

  override def seek(newPos: Long): Unit = {
    pos = newPos
  }

  override def read(): Int = {
    if (read(oneByte, 0, 1) < 0) -1 else oneByte(0) & 0xff
  }

  override def read(b: Array[Byte], off: Int, len: Int): Int = {
    val n = file.readAt(pos, b, off, len)
    if (n > 0) {
      pos += n
    }
    n
  }

  override def readFully(b: Array[Byte]): Unit = readFully(b, 0, b.length)

  override def readFully(b: Array[Byte], off: Int, len: Int): Unit = {
    var done = 0
    while (done < len) {
      val n = read(b, off + done, len - done)
      if (n < 0) {
        throw new EOFException(s"Reached the end of stream with ${len - done} bytes left to read")
      }
      done += n
    }
  }

  override def read(buf: ByteBuffer): Int = {
    if (!buf.hasRemaining) {
      0
    } else if (buf.hasArray) {
      val n = read(buf.array, buf.arrayOffset + buf.position(), buf.remaining)
      if (n > 0) {
        buf.position(buf.position() + n)
      }
      n
    } else {
      val n = read(temp, 0, math.min(buf.remaining, temp.length))
      if (n > 0) {
        buf.put(temp, 0, n)
      }
      n
    }
  }

  override def readFully(buf: ByteBuffer): Unit = {
    while (buf.hasRemaining) {
      if (read(buf) < 0) {
        throw new EOFException(s"Reached the end of stream with ${buf.remaining} bytes left")
      }
    }
  }
}
