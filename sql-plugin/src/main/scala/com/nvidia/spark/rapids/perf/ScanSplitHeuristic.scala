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

package com.nvidia.spark.rapids.perf

import java.net.URI
import java.nio.charset.StandardCharsets.UTF_8
import java.security.MessageDigest
import java.time.Duration

import scala.collection.JavaConverters._
import scala.util.Try

import com.nvidia.spark.history.{DimValue, MetricStore, MetricStores, Retention}
import com.nvidia.spark.rapids.{GpuMetric, NoopMetric, RapidsConf, ScanWithMetrics}
import org.apache.hadoop.fs.Path

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.metric.CustomTaskMetric
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader,
  PartitionReaderFactory}
import org.apache.spark.sql.execution.datasources.{FilePartition, PartitionedFile}
import org.apache.spark.sql.execution.datasources.v2.FileScan
import org.apache.spark.sql.execution.rapids.shims.FilePartitionShims
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.CollectionAccumulator

/**
 * Decode expansion: GPU output batch bytes / on-disk bytes of the scan's planned files. One
 * observation per completed scan, stamped at SQL execution end.
 *
 * Dimension `table`: the catalog identifier, or "path:" + root paths
 * (see `ScanSplitHeuristic.scanKey`), capped at the API's key size. Compared as exact bytes, so
 * decide and observe must derive it identically.
 */
object ScanExpansionRatio extends HistoryMetric {

  /** `scan.decode_expansion_ratio` in the production catalog. */
  override val id: Int = 1

  override val version: Int = 1

  override val dimension: String = "table"

  override val retention: Retention = new Retention(Duration.ofDays(7), Duration.ofDays(14))
}

/**
 * Inputs for sizing one scan.
 *
 * @param table history key
 * @param listedBytes on-disk bytes the scan lists; bounds the split
 * @param drainSplits returns and clears the (split, decoded, listed) reads
 * @param isCurrent false once the scan was re-planned; only the current planning is recorded
 * @param splitFloorBytes lower bound of a learned split
 *                        (`spark.rapids.sql.history.scanSplit.minBytes`)
 * @param splitCapBytes upper bound of a learned split
 *                      (`spark.rapids.sql.history.scanSplit.maxBytes`)
 * @param maxAge how old a reading may be (`spark.rapids.sql.history.scanSplit.maxAgeDays`); None
 *               keeps the metric's declared planning age
 */
final case class ScanContext(
    table: String,
    listedBytes: Long,
    batchSizeBytes: Long,
    minPartitionNum: Long,
    maxSplitBytes: Long,
    plannedSplits: Int = 0,
    drainSplits: () => Seq[(Int, Long, Long)] = () => Seq.empty,
    isCurrent: () => Boolean = () => true,
    splitFloorBytes: Long = ScanSplitSizer.MIN_SPLIT_BYTES,
    splitCapBytes: Long = ScanSplitSizer.MAX_SPLIT_BYTES,
    maxAge: Option[Duration] = None)

/** Sizes a scan's split from its table's last decode-expansion ratio. */
class ScanSplitHeuristic(store: () => MetricStore) extends HistoryHeuristic(store) {

  type Ctx = ScanContext
  type Decision = Long

  def name: String = "scan.split"

  def enabled(conf: RapidsConf): Boolean = conf.historyScanSplitEnabled

  def metrics: Seq[HistoryMetric] = Seq(ScanExpansionRatio)

  /** Keyed by table. */
  protected def keyFor(metric: HistoryMetric, ctx: ScanContext): String =
    ScanSplitHeuristic.capped(ctx.table)

  override protected def planningMaxAge(ctx: ScanContext): Option[Duration] = ctx.maxAge

  protected def staticDecision(ctx: ScanContext): Long = ctx.maxSplitBytes

  protected def decideFrom(observed: Map[HistoryMetric, Double], ctx: ScanContext): Long =
    observed.get(ScanExpansionRatio)
      .map { ratio =>
        val raw = ScanSplitSizer.rawSplit(ratio, ctx.batchSizeBytes)
        logInfo(s"scan.split: table=${ctx.table} ratio=$ratio " +
          s"targetBatch=${ctx.batchSizeBytes} rawSplit=$raw")
        raw
      }
      .getOrElse(0L)

  override protected def constrain(raw: Long, ctx: ScanContext): Long =
    ScanSplitSizer.bound(raw, ctx.listedBytes, ctx.minPartitionNum, ctx.maxSplitBytes,
      ctx.splitFloorBytes, ctx.splitCapBytes)

  /**
   * File-source split; `sparkMaxSplitBytes` for scans without a key or while history is off.
   *
   * @param context only called when history is used
   */
  def fileSourceSplit(
      table: Option[String],
      sparkMaxSplitBytes: Long,
      context: String => ScanContext,
      nowMs: Long): Long = table match {
    case Some(name) if isEnabled =>
      val ctx = context(name)
      val decided = decide(ctx, nowMs)
      if (decided == sparkMaxSplitBytes) {
        logInfo(s"scan.split: table=$name listed=${ctx.listedBytes} -> no history, spark default")
      } else {
        logInfo(s"scan.split: table=$name listed=${ctx.listedBytes} -> split=$decided bytes")
      }
      decided
    case _ => sparkMaxSplitBytes
  }

  /** The current planning's ratio, or logs why not. */
  protected def observe(ctx: ScanContext): Map[HistoryMetric, Double] = {
    if (!ctx.isCurrent()) {
      logDebug(s"scan.split: table=${ctx.table} not recorded: superseded planning")
      Map.empty
    } else {
      val entries = ctx.drainSplits()
      val result = if (entries.isEmpty) {
        Left("planned but not executed in this query")
      } else {
        ScanSplitHeuristic.ratioFromSplits(ctx.plannedSplits, entries)
      }
      result match {
        case Right(ratio) => Map(ScanExpansionRatio -> ratio)
        case Left(reason) =>
          logInfo(s"scan.split: table=${ctx.table} not recorded: $reason")
          Map.empty
      }
    }
  }

  override protected def shouldObserve(ctx: ScanContext): Boolean =
    ctx.table != null && ctx.table.nonEmpty && ctx.plannedSplits > 0
}

object ScanSplitHeuristic extends ScanSplitHeuristic(() => MetricStores.current()) {

  /** Largest key the history API takes, in UTF-8 bytes. */
  val MAX_KEY_BYTES: Int = DimValue.MAX_CANONICAL_BYTES - 3

  /**
   * Created when a split's reader opens; on exhaustion adds (split, growth of `decoded`, listed).
   * A task reads splits sequentially, so the growth is this split's. Metrics may start at -1.
   */
  def recordSplitOnExhaustion(
      split: Int,
      listed: Long,
      decoded: GpuMetric,
      acc: CollectionAccumulator[(Int, Long, Long)]): Boolean => Boolean = {
    val start = decoded.value.max(0L)
    hasMore => {
      if (!hasMore) acc.add((split, decoded.value.max(0L) - start, listed))
      hasMore
    }
  }

  /** Wraps `reader` to record its split's decoded bytes when it is exhausted. */
  def observeReader(
      reader: PartitionReader[ColumnarBatch],
      split: Int,
      listed: Long,
      decoded: GpuMetric,
      acc: CollectionAccumulator[(Int, Long, Long)]): PartitionReader[ColumnarBatch] = {
    val record = recordSplitOnExhaustion(split, listed, decoded, acc)
    new PartitionReader[ColumnarBatch] {
      override def next(): Boolean = record(reader.next())
      override def get(): ColumnarBatch = reader.get()
      override def close(): Unit = reader.close()
      override def currentMetricsValues(): Array[CustomTaskMetric] = reader.currentMetricsValues()
    }
  }

  /** Returns and clears `acc` atomically. */
  def drainSplits(acc: CollectionAccumulator[(Int, Long, Long)]): Seq[(Int, Long, Long)] =
    acc.synchronized {
      val entries = acc.value.asScala.toList
      acc.reset()
      entries
    }

  /** sum(decoded) / sum(listed) over each split's first read if exactly 0..planned-1 were read. */
  def ratioFromSplits(
      planned: Int,
      entries: Seq[(Int, Long, Long)]): Either[String, Double] = {
    val firstReads = entries.groupBy(_._1).map { case (split, reads) => split -> reads.head }
    val decoded = firstReads.values.map(_._2).sum
    val listed = firstReads.values.map(_._3).sum
    val inRange = firstReads.keySet.count(split => split >= 0 && split < planned)
    if (planned <= 0) {
      Left("nothing planned")
    } else if (inRange < firstReads.size) {
      Left(s"read split indices outside 0..${planned - 1}")
    } else if (inRange < planned) {
      Left(s"read $inRange of $planned planned splits to the end")
    } else if (listed <= 0L) {
      Left("no listed bytes in the splits read")
    } else if (decoded <= 0L) {
      Left("no decoded bytes (decoded-bytes metric off or zero)")
    } else {
      Right(decoded.toDouble / listed.toDouble)
    }
  }

  /** `key` if it fits, else a readable prefix + "#" + 16 hex of its SHA-256, within the cap. */
  def capped(key: String): String = {
    val bytes = key.getBytes(UTF_8)
    if (bytes.length <= MAX_KEY_BYTES) {
      key
    } else {
      val hash = "#" + MessageDigest.getInstance("SHA-256").digest(bytes).take(8)
        .map(b => f"${b & 0xff}%02x").mkString
      def utf8Len(cp: Int): Int =
        if (cp < 0x80) 1 else if (cp < 0x800) 2 else if (cp < 0x10000) 3 else 4
      var end = 0
      var used = hash.length
      while (end < key.length && used + utf8Len(key.codePointAt(end)) <= MAX_KEY_BYTES) {
        used += utf8Len(key.codePointAt(end))
        end += Character.charCount(key.codePointAt(end))
      }
      key.substring(0, end) + hash
    }
  }

  /** URI without user info or a trailing "/". */
  private def normalized(path: Path): String = {
    val u = path.toUri
    val clean = if (u.getRawUserInfo == null) {
      u
    } else {
      new URI(u.getScheme, null, u.getHost, u.getPort, u.getPath, u.getQuery, u.getFragment)
    }
    clean.toString.stripSuffix("/")
  }

  /**
   * History key: the catalog name, else "path:" + the sorted distinct roots (directories or
   * files); capped. Left when there are no roots.
   */
  def scanKey(table: Option[String], roots: => Seq[Path]): Either[String, String] = table match {
    case Some(name) => Right(capped(name))
    case None =>
      val rootKeys = roots.map(normalized).distinct.sorted
      if (rootKeys.isEmpty) {
        Left("no root paths")
      } else {
        Right(capped("path:" + rootKeys.mkString(",")))
      }
  }

  /** `scanKey` while history is on; logs why a scan has no usable key. */
  def historyKey(table: Option[String], roots: => Seq[Path]): Option[String] = {
    lazy val rootPaths = roots
    if (!isEnabled) {
      None
    } else {
      scanKey(table, rootPaths) match {
        case Right(key) => Some(key)
        case Left(reason) =>
          val desc = rootPaths.headOption.map(r => "path:" + normalized(r)).getOrElse("<none>")
          logInfo(s"scan.split: table=$desc not recorded: $reason")
          None
      }
    }
  }

  /** Learned split for `key` in `spark`, else `sparkMaxSplitBytes`. */
  def learnedFileSplit(
      key: Option[String],
      spark: SparkSession,
      listedBytes: => Long,
      sparkMaxSplitBytes: Long): Long =
    fileSourceSplit(key, sparkMaxSplitBytes, name => {
      val rapidsConf = new RapidsConf(spark.sessionState.conf)
      ScanContext(
        table = name,
        listedBytes = listedBytes,
        batchSizeBytes = rapidsConf.gpuTargetBatchSizeBytes,
        minPartitionNum = minPartitionNum(spark),
        maxSplitBytes = sparkMaxSplitBytes,
        splitFloorBytes = rapidsConf.historyScanSplitMinBytes,
        splitCapBytes = rapidsConf.historyScanSplitMaxBytes,
        maxAge = Some(rapidsConf.historyScanSplitMaxAge))
    }, System.currentTimeMillis())

  /** Queues a file scan for recording at query end; its split i lists `listed(i)` bytes. */
  def registerFileScan(
      key: Option[String],
      listed: Array[Long],
      acc: Option[CollectionAccumulator[(Int, Long, Long)]],
      sc: SparkContext): Unit =
    for (name <- key; splits <- acc) {
      register(HistoryObservations.currentExecutionId(sc), ScanContext(
        table = name,
        listedBytes = 0L,
        batchSizeBytes = 0L,
        minPartitionNum = 0L,
        maxSplitBytes = 0L,
        plannedSplits = listed.length,
        drainSplits = () => drainSplits(splits).map {
          case (i, decoded, _) => (i, decoded, listed.lift(i).getOrElse(0L))
        }))
    }

  /** Re-packs `planned` file partitions at `split` bytes; unchanged unless `split` > 0. */
  def resizeSplits(
      spark: SparkSession,
      planned: Array[InputPartition],
      split: Long,
      isSplitable: Path => Boolean): Array[InputPartition] = {
    if (split <= 0L || !planned.forall(_.isInstanceOf[FilePartition])) {
      planned
    } else {
      val files = planned.toSeq.flatMap { p =>
        FilePartitionShims.getFiles(p.asInstanceOf[FilePartition]).toSeq
      }
      FilePartition.getFilePartitions(spark, recut(files, split, isSplitable), split).toArray
    }
  }

  /** Re-joins each file's contiguous chunks, then cuts splitable ones at `split`; largest first. */
  def recut(
      files: Seq[PartitionedFile],
      split: Long,
      isSplitable: Path => Boolean): Seq[PartitionedFile] = {
    val byPath = files.groupBy(_.filePath.toString)
    val joined = files.map(_.filePath.toString).distinct.flatMap { path =>
      byPath(path).sortBy(_.start).foldLeft(List.empty[PartitionedFile]) {
        case (prev :: done, f) if f.start == prev.start + prev.length &&
            f.partitionValues == prev.partitionValues =>
          prev.copy(length = prev.length + f.length) :: done
        case (done, f) => f :: done
      }.reverse
    }
    joined.flatMap { f =>
      val end = f.start + f.length
      if (f.length > split && isSplitable(new Path(new URI(f.filePath.toString)))) {
        (f.start until end by split).map { at =>
          f.copy(start = at, length = math.min(split, end - at))
        }
      } else {
        Seq(f)
      }
    }.sortBy(_.length)(Ordering[Long].reverse)
  }

  def minPartitionNum(spark: SparkSession): Long =
    minPartitionNum(spark.sessionState.conf, spark.sparkContext.defaultParallelism)

  /** `registeredCores` is read only when no parallelism or executor slots are configured. */
  def minPartitionNum(sqlConf: SQLConf, registeredCores: => Int): Long = {
    def intConf(key: String): Option[Int] =
      Option(sqlConf.getConfString(key, null)).flatMap(v => Try(v.trim.toInt).toOption)
    ScanSplitSizer.minPartitionNum(
      filesMinPartitionNum = sqlConf.filesMinPartitionNum,
      leafNodeDefaultParallelism = sqlConf.getConf(SQLConf.LEAF_NODE_DEFAULT_PARALLELISM),
      defaultParallelism = intConf("spark.default.parallelism"),
      executorInstances = intConf("spark.executor.instances"),
      executorCores = intConf("spark.executor.cores"),
      registeredCores = registeredCores)
  }
}

/** Learned split sizing and per-split observation for a GPU DSv2 file scan. */
trait HistorySizedFileScan extends FileScan with ScanWithMetrics {

  @transient private lazy val historyKey: Option[String] = ScanSplitHeuristic.historyKey(
    None, fileIndex.rootPaths)

  /** (split, decoded, listed) per split read; None while history is off. */
  @transient private lazy val splitReads: Option[CollectionAccumulator[(Int, Long, Long)]] =
    historyKey.map(_ => sparkSession.sparkContext.collectionAccumulator[(Int, Long, Long)])

  /** Listed bytes per split of the latest planning. */
  @transient @volatile private var plannedListed: Array[Long] = Array.empty

  override def planInputPartitions(): Array[InputPartition] = {
    val planned = super.planInputPartitions()
    historyKey.fold(planned) { key =>
      def listed(p: InputPartition): Long = p match {
        case fp: FilePartition => FilePartitionShims.getFiles(fp).map(_.length).sum
        case _ => 0L
      }
      val split = ScanSplitHeuristic.learnedFileSplit(
        Some(key), sparkSession, planned.map(listed).sum, 0L)
      val parts = ScanSplitHeuristic.resizeSplits(sparkSession, planned, split, isSplitable)
      plannedListed = parts.map(listed)
      parts
    }
  }

  /** Wrap `createReaderFactory`'s result: registers this scan and observes its splits. */
  protected def observed(factory: PartitionReaderFactory): PartitionReaderFactory =
    splitReads.fold(factory) { acc =>
      ScanSplitHeuristic.registerFileScan(
        historyKey, plannedListed, splitReads, sparkSession.sparkContext)
      new SplitObservingReaderFactory(
        factory, metrics.getOrElse(GpuMetric.GPU_OUTPUT_BATCH_BYTES, NoopMetric), acc)
    }
}

/** Records each file split's decoded bytes; listed bytes are filled in on the driver. */
class SplitObservingReaderFactory(
    factory: PartitionReaderFactory,
    decoded: GpuMetric,
    acc: CollectionAccumulator[(Int, Long, Long)]) extends PartitionReaderFactory {

  override def createReader(p: InputPartition): PartitionReader[InternalRow] =
    factory.createReader(p)

  override def createColumnarReader(p: InputPartition): PartitionReader[ColumnarBatch] =
    p match {
      case fp: FilePartition =>
        ScanSplitHeuristic.observeReader(factory.createColumnarReader(p), fp.index, 0L, decoded,
          acc)
      case _ => factory.createColumnarReader(p)
    }

  override def supportColumnarReads(p: InputPartition): Boolean = factory.supportColumnarReads(p)
}
