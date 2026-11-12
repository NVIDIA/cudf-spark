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
import java.util.Locale

import scala.collection.JavaConverters._

import com.nvidia.spark.history.{DimValue, MetricStore, MetricStores, Retention}
import com.nvidia.spark.rapids.{GpuMetric, NoopMetric, RapidsConf, ScanWithMetrics}
import org.apache.hadoop.fs.Path

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection
import org.apache.spark.sql.connector.metric.CustomTaskMetric
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader,
  PartitionReaderFactory}
import org.apache.spark.sql.execution.datasources.FilePartition
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
 * The user-tunable limits of a learned split, read once per scan from `RapidsConf`. The
 * `spark.rapids.sql.history.scanSplit.*` entries are the only place their defaults live.
 *
 * @param minBytes lower bound of a learned split (`minBytes`)
 * @param maxBytes upper bound of a learned split (`maxBytes`)
 * @param maxAge how old the table's last reading may be (`maxAgeDays`)
 */
final case class ScanSplitLimits(minBytes: Long, maxBytes: Long, maxAge: Duration)

object ScanSplitLimits {
  def apply(conf: RapidsConf): ScanSplitLimits = ScanSplitLimits(
    conf.historyScanSplitMinBytes, conf.historyScanSplitMaxBytes, conf.historyScanSplitMaxAge)
}

/**
 * Inputs for sizing one scan, or for recording it when its query ends (see
 * `ScanContext.forObservation`).
 *
 * @param table history key
 * @param listedBytes on-disk bytes the scan lists; bounds the split
 * @param batchSizeBytes the GPU target batch size
 * @param minPartitionNum the parallelism a learned split keeps, as Spark resolves it
 * @param maxSplitBytes the static split, which is Spark's own value
 * @param limits the bounds of a learned split and the maximum age of a reading
 * @param plannedSplits number of splits planned, for the recording
 * @param drainSplits returns and clears the (split, decoded, listed) reads
 * @param isCurrent false once the scan was re-planned; only the current planning is recorded
 */
final case class ScanContext(
    table: String,
    listedBytes: Long,
    batchSizeBytes: Long,
    minPartitionNum: Long,
    maxSplitBytes: Long,
    limits: ScanSplitLimits,
    plannedSplits: Int = 0,
    drainSplits: () => Seq[(Int, Long, Long)] = () => Seq.empty,
    isCurrent: () => Boolean = () => true)

object ScanContext {

  /**
   * A scan queued only to be recorded when its query ends. Nothing is sized for it, so the sizing
   * inputs are placeholders that are never read.
   */
  def forObservation(
      table: String,
      plannedSplits: Int,
      drainSplits: () => Seq[(Int, Long, Long)]): ScanContext =
    ScanContext(
      table = table,
      listedBytes = 0L,
      batchSizeBytes = 0L,
      minPartitionNum = 0L,
      maxSplitBytes = 0L,
      limits = ScanSplitLimits(0L, 0L, Duration.ZERO),
      plannedSplits = plannedSplits,
      drainSplits = drainSplits)
}

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

  override protected def planningMaxAge(ctx: ScanContext): Option[Duration] =
    Some(ctx.limits.maxAge)

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
      ctx.limits.minBytes, ctx.limits.maxBytes)

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
   *
   * Limitations (the key identifies the table, not what a query reads from it):
   *  - The recorded ratio (decoded bytes / on-disk bytes) depends on the columns a scan
   *    projects and the filters it pushes down, but neither is part of the key. Scans of the
   *    same table that read different columns or apply different filters share one key, and
   *    the latest recording wins. Adding a fingerprint of the read schema and the pushed
   *    filters to the key is future work.
   *  - A table read by path is keyed by its root paths as written, so a different spelling of
   *    the same data (another partition directory, an expanded glob, a new file list) is a
   *    different key and starts with no history.
   *  - DSv2 file scans are always keyed by path, never by table name, so the same table read
   *    through v1 and through DSv2 keeps two separate histories.
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
        limits = ScanSplitLimits(rapidsConf))
    }, System.currentTimeMillis())

  /** Queues a file scan for recording at query end; its split i lists `listed(i)` bytes. */
  def registerFileScan(
      key: Option[String],
      listed: Array[Long],
      acc: Option[CollectionAccumulator[(Int, Long, Long)]],
      sc: SparkContext): Unit =
    for (name <- key; splits <- acc) {
      register(HistoryObservations.currentExecutionId(sc),
        ScanContext.forObservation(name, listed.length, () => drainSplits(splits).map {
          case (i, decoded, _) => (i, decoded, listed.lift(i).getOrElse(0L))
        }))
    }

  /**
   * Parallelism for the ceiling, resolved as Spark does for file scans in
   * `FilePartition.maxSplitBytes`: `spark.sql.files.minPartitionNum`, else the leaf node default
   * parallelism (`spark.sql.leafNodeDefaultParallelism`, else the SparkContext's default
   * parallelism).
   *
   * Limitation: with nothing configured, the SparkContext's default parallelism is the number of
   * cores registered when the query plans, so the ceiling of a learned split can vary with
   * executor startup timing. Spark's own static split uses the same number.
   */
  def minPartitionNum(spark: SparkSession): Long = {
    val conf = spark.sessionState.conf
    conf.filesMinPartitionNum
      .orElse(conf.getConf(SQLConf.LEAF_NODE_DEFAULT_PARALLELISM))
      .getOrElse(spark.sparkContext.defaultParallelism).toLong
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

  /**
   * Spark's `FileScan.partitions` with one change: the split size comes from
   * `ScanSplitHeuristic.learnedFileSplit`, whose static value is the `maxSplitBytes` Spark
   * computes, so with no usable history the partitions are exactly Spark's. Without a history
   * key, or if a read partition column is not found, it is Spark's own method.
   *
   * Why this is a copy of Spark's method body: Spark computes `maxSplitBytes` inside
   * `FileScan.partitions` and gives a subclass no way to pass one in. Calling `super.partitions`
   * and adjusting the result would mean re-cutting files Spark has already cut, which depends on
   * how Spark happened to cut them. So the steps around the split size are reproduced here: listing
   * the files, mapping the read partition columns, and pruning each directory's partition values
   * (Spark's own `GenerateUnsafeProjection` step). Everything else calls Spark or the shims:
   * `FilePartition.maxSplitBytes`, `FilePartitionShims.splitFilesBy` and
   * `FilePartition.getFilePartitions`. If Spark ever lets a scan override how `maxSplitBytes` is
   * computed, this override can be removed.
   */
  override protected def partitions: Seq[FilePartition] = historyKey.fold(super.partitions) { key =>
    val selectedPartitions = fileIndex.listFiles(partitionFilters, dataFilters)
    val partitionAttributes = fileIndex.partitionSchema.map { f =>
      AttributeReference(f.name, f.dataType, f.nullable, f.metadata)()
    }
    def normalized(name: String): String = {
      if (sparkSession.sessionState.conf.caseSensitiveAnalysis) {
        name
      } else {
        name.toLowerCase(Locale.ROOT)
      }
    }
    val attributeByName = partitionAttributes.map(a => normalized(a.name) -> a).toMap
    val readPartitionAttributes =
      readPartitionSchema.flatMap(f => attributeByName.get(normalized(f.name)))
    if (readPartitionAttributes.length != readPartitionSchema.length) {
      super.partitions // a column Spark itself rejects with its own error
    } else {
      val sparkSplit = FilePartition.maxSplitBytes(sparkSession, selectedPartitions)
      val split = ScanSplitHeuristic.learnedFileSplit(Some(key), sparkSession,
        selectedPartitions.flatMap(_.files.map(_.getLen)).sum, sparkSplit)
      // Prune partition values to the read columns, as Spark does.
      lazy val partitionValueProject =
        GenerateUnsafeProjection.generate(readPartitionAttributes, partitionAttributes)
      val splitFiles = FilePartitionShims.splitFilesBy(sparkSession, selectedPartitions,
        isSplitable(_), split, partition =>
          if (readPartitionAttributes != partitionAttributes) {
            partitionValueProject(partition.values).copy()
          } else {
            partition.values
          })
      val parts = FilePartition.getFilePartitions(sparkSession, splitFiles, split)
      plannedListed = parts.map(p => FilePartitionShims.getFiles(p).map(_.length).sum).toArray
      parts
    }
  }

  /** Wrap `createReaderFactory`'s result so that each split's decoded bytes are observed. */
  protected def observed(factory: PartitionReaderFactory): PartitionReaderFactory =
    splitReads.fold(factory) { acc =>
      new SplitObservingReaderFactory(
        factory, metrics.getOrElse(GpuMetric.GPU_OUTPUT_BATCH_BYTES, NoopMetric), acc)
    }

  /**
   * Queues this scan for recording when the current SQL execution ends. The scan's exec calls
   * this every time it executes, after the partitions are planned, as the v1 scan does. A
   * plan that is executed again is therefore recorded again, and so is a scan whose reader
   * factory was built outside any SQL execution.
   */
  def registerForExecution(sc: SparkContext): Unit =
    ScanSplitHeuristic.registerFileScan(historyKey, plannedListed, splitReads, sc)
}

object HistorySizedFileScan {

  /** Called by a GPU batch scan exec for its `scan`; does nothing for a scan without history. */
  def registerForExecution(scan: AnyRef, sc: SparkContext): Unit = scan match {
    case history: HistorySizedFileScan => history.registerForExecution(sc)
    case _ =>
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
