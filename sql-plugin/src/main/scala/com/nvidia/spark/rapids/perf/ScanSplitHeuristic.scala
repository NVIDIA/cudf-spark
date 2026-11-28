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

import java.time.Duration
import java.util.Locale

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

import com.nvidia.spark.history.{DimensionSpec, DimMatch, DimValue, MetricStore, MetricStores,
  Retention}
import com.nvidia.spark.rapids.{GpuMetric, NoopMetric, RapidsConf, ScanWithMetrics}

import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection
import org.apache.spark.sql.connector.metric.CustomTaskMetric
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader,
  PartitionReaderFactory}
import org.apache.spark.sql.execution.datasources.{FilePartition, PartitionedFile}
import org.apache.spark.sql.execution.datasources.v2.FileScan
import org.apache.spark.sql.execution.rapids.shims.FilePartitionShims
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.LongAccumulator

/**
 * Decode expansion: GPU output batch bytes / on-disk bytes of the scan's planned files. One
 * observation per completed scan, stamped at SQL execution end.
 *
 * Dimensions, see `ScanHistoryKey`: `plan` and `literals`, the two hashes of a `PlanKey`, and
 * `size`, the base 2 logarithm of the listed bytes, rounded down. Compared as exact values, so
 * decide and observe must derive them identically.
 */
object ScanExpansionRatio extends HistoryMetric {

  override val id: Int = 1

  override val name: String = "scan.decode_expansion_ratio"

  override val version: Int = 1

  /** The name of the dimension that holds `PlanKey.plan`, a LONG. */
  val planDimension: String = "plan"

  /** The name of the dimension that holds `PlanKey.literals`, a LONG. */
  val literalsDimension: String = "literals"

  /** The name of the dimension that holds the size bucket, a LONG. */
  val sizeDimension: String = "size"

  override val dimensions: Seq[DimensionSpec] = Seq(
    new DimensionSpec(planDimension, DimValue.Kind.LONG),
    new DimensionSpec(literalsDimension, DimValue.Kind.LONG),
    new DimensionSpec(sizeDimension, DimValue.Kind.LONG))

  override val retention: Retention = new Retention(Duration.ofDays(7), Duration.ofDays(14))
}

/**
 * Where a scan stands in history: what it reads, and how much it lists, in buckets of a factor of
 * two so that a scan finds the scans of about its size.
 *
 * @param key what the scan reads
 * @param size `floor(log2(listedBytes))`
 */
final case class ScanHistoryKey(key: PlanKey, size: Long) {

  /** Every dimension of the metric, for recording. */
  def dimensions: Map[String, DimValue] = Map(
    ScanExpansionRatio.planDimension -> DimValue.of(key.plan),
    ScanExpansionRatio.literalsDimension -> DimValue.of(key.literals),
    ScanExpansionRatio.sizeDimension -> DimValue.of(size))

  /**
   * The tiers of the search list, most specific first, for one lookup: the store returns every
   * tier's readings, each reading in the first tier that matches it, and the aggregate picks.
   *  1. the same plan and literals, the same size bucket;
   *  2. the same plan and literals, a neighbouring size bucket;
   *  3. the same plan, any literals, the same or a neighbouring size bucket.
   * A scan of the same query that ran on another day has other literals, so it finds the earlier
   * days in tier 3.
   */
  def searchList: Seq[Map[String, DimMatch]] = {
    val plan = ScanExpansionRatio.planDimension -> DimMatch.equalTo(DimValue.of(key.plan))
    val literals =
      ScanExpansionRatio.literalsDimension -> DimMatch.equalTo(DimValue.of(key.literals))
    def sizes(buckets: Seq[Long]) = ScanExpansionRatio.sizeDimension ->
      DimMatch.in(buckets.map(bucket => DimValue.of(bucket)).asJava)
    Seq(
      Map(plan, literals, ScanExpansionRatio.sizeDimension -> DimMatch.equalTo(DimValue.of(size))),
      Map(plan, literals, sizes(neighbours)),
      Map(plan, sizes((size +: neighbours).sorted)))
  }

  private def neighbours: Seq[Long] = Seq(size - 1, size + 1).filter(_ >= 0L)
}

object ScanHistoryKey {

  /** The key of a scan that lists `listedBytes`; None if it lists nothing. */
  def of(key: PlanKey, listedBytes: Long): Option[ScanHistoryKey] =
    if (listedBytes > 0L) {
      Some(ScanHistoryKey(key, java.lang.Long.SIZE - 1L -
        java.lang.Long.numberOfLeadingZeros(listedBytes)))
    } else {
      None
    }
}

/**
 * The user-tunable limits of a learned split, read once per scan from `RapidsConf`. The
 * `spark.rapids.sql.history.scanSplit.*` entries are the only place their defaults live.
 *
 * @param minBytes lower bound of a learned split (`minBytes`)
 * @param maxBytes upper bound of a learned split (`maxBytes`)
 * @param maxAge how old a reading may be (`maxAgeDays`)
 * @param aggregate how the readings of the first search tier that has any are combined
 *                  (`aggregate`)
 */
final case class ScanSplitLimits(
    minBytes: Long,
    maxBytes: Long,
    maxAge: Duration,
    aggregate: RatioAggregate = RatioAggregate.Latest)

object ScanSplitLimits {
  def apply(conf: RapidsConf): ScanSplitLimits = ScanSplitLimits(
    conf.historyScanSplitMinBytes, conf.historyScanSplitMaxBytes, conf.historyScanSplitMaxAge,
    RatioAggregate.byName(conf.historyScanSplitAggregate))
}

/**
 * Inputs for sizing one scan, or for recording it when its query ends (see
 * `ScanContext.forObservation`).
 *
 * @param key history key
 * @param listedBytes on-disk bytes the scan lists; bounds the split
 * @param batchSizeBytes the GPU target batch size
 * @param minPartitionNum the parallelism a learned split keeps, as Spark resolves it
 * @param maxSplitBytes the static split, which is Spark's own value
 * @param limits the bounds of a learned split and the maximum age of a reading
 * @param plannedSplits number of splits planned, for the recording
 * @param drainReads returns and clears what the tasks read
 * @param isCurrent false once the scan was re-planned; only the current planning is recorded
 */
final case class ScanContext(
    key: ScanHistoryKey,
    listedBytes: Long,
    batchSizeBytes: Long,
    minPartitionNum: Long,
    maxSplitBytes: Long,
    limits: ScanSplitLimits,
    plannedSplits: Int = 0,
    drainReads: () => ReadTotals = () => ReadTotals(0L, 0L, 0L),
    isCurrent: () => Boolean = () => true)

object ScanContext {

  /**
   * A scan queued only to be recorded when its query ends. Nothing is sized for it, so the sizing
   * inputs are placeholders that are never read.
   */
  def forObservation(
      key: ScanHistoryKey,
      plannedSplits: Int,
      drainReads: () => ReadTotals): ScanContext =
    ScanContext(
      key = key,
      listedBytes = 0L,
      batchSizeBytes = 0L,
      minPartitionNum = 0L,
      maxSplitBytes = 0L,
      limits = ScanSplitLimits(0L, 0L, Duration.ZERO),
      plannedSplits = plannedSplits,
      drainReads = drainReads)
}

/** Sizes a scan's split from the decode-expansion ratio of earlier scans that read the same. */
class ScanSplitHeuristic(store: () => MetricStore) extends Logging {

  def name: String = "scan.split"

  /**
   * Whether this is switched on in `conf`. Read once at driver plugin init, and only consulted
   * when a history provider is requested.
   */
  def enabled(conf: RapidsConf): Boolean = conf.historyScanSplitEnabled

  /** The families this reads and writes. */
  def metrics: Seq[HistoryMetric] = Seq(ScanExpansionRatio)

  private val history = new MetricHistory(ScanExpansionRatio, store)

  /** Set while history-backed planning is on; otherwise the static split stands. */
  @volatile private var active: Option[HistoryPolicy] = None

  def enable(policy: HistoryPolicy): Unit = active = Some(policy)

  def disable(): Unit = active = None

  def isEnabled: Boolean = active.isDefined

  /** Declares the family up front. */
  def declare(): Unit = active.foreach(policy => history.declare(policy))

  /** For tests. */
  private[perf] def historyOf: MetricHistory = history

  /**
   * The ratio the aggregate chooses from the readings of every tier of the search list (see
   * `ScanHistoryKey.searchList`), and the number of its tier (from 1). One request and one
   * response. None if no tier gives a ratio, or the lookup failed.
   */
  private def lookup(
      key: ScanHistoryKey,
      nowMs: Long,
      policy: HistoryPolicy,
      limits: ScanSplitLimits): Option[(Double, Int)] =
    history.lookup(key.searchList, nowMs, policy, Some(limits.maxAge))
      .flatMap(limits.aggregate.choose)
      .collect { case (ratio, tier) if MetricHistory.isUsable(ratio) => (ratio, tier + 1) }

  /**
   * History-based or static, never a blend: the static split is `ctx.maxSplitBytes`, which is
   * Spark's own.
   */
  def decide(ctx: ScanContext, nowMs: Long): Long = active match {
    case None => ctx.maxSplitBytes
    case Some(policy) =>
      lookup(ctx.key, nowMs, policy, ctx.limits) match {
        case Some((ratio, tier)) =>
          val raw = ScanSplitSizer.rawSplit(ratio, ctx.batchSizeBytes)
          logInfo(s"scan.split: ${describe(ctx.key)} tier=$tier ratio=$ratio " +
            s"targetBatch=${ctx.batchSizeBytes} rawSplit=$raw")
          ScanSplitSizer.bound(raw, ctx.listedBytes, ctx.minPartitionNum, ctx.maxSplitBytes,
            ctx.limits.minBytes, ctx.limits.maxBytes)
        case None => ctx.maxSplitBytes
      }
  }

  /** The key for the log: the hashes in full, so that a tool can find the scans of one query. */
  private def describe(key: ScanHistoryKey): String =
    s"${key.key.source} literals=${key.key.literalsText} size=${key.size} " +
      s"plan=${key.key.planHex} lit=${key.key.literalsHex}"

  /** Queues `ctx` for observation at execution end; untracked outside a SQL execution. */
  def register(executionId: Option[Long], ctx: ScanContext): Unit = {
    if (isEnabled && shouldObserve(ctx)) {
      executionId match {
        case None => logDebug(s"$name planned outside a SQL execution; not tracked for history")
        case Some(id) => ExecutionEndCallbacks.register(id, () => recordAll(ctx))
      }
    }
  }

  /** Records what `ctx` observed; called at execution end. */
  private[perf] def recordAll(ctx: ScanContext): Unit = active.foreach { policy =>
    val atMs = System.currentTimeMillis()
    observe(ctx).foreach(ratio => history.record(ctx.key.dimensions, ratio, atMs, policy))
  }

  /**
   * File-source split; `sparkMaxSplitBytes` for scans without a key or while history is off.
   *
   * @param key what the scan reads
   * @param listedBytes on-disk bytes the scan lists; only evaluated when history is used
   * @param context only called when history is used
   */
  def fileSourceSplit(
      key: Option[PlanKey],
      sparkMaxSplitBytes: Long,
      listedBytes: => Long,
      context: ScanHistoryKey => ScanContext,
      nowMs: Long): Long = key match {
    case Some(scanKey) if isEnabled =>
      ScanHistoryKey.of(scanKey, listedBytes) match {
        case Some(historyKey) =>
          val ctx = context(historyKey)
          val decided = decide(ctx, nowMs)
          if (decided == sparkMaxSplitBytes) {
            logInfo(s"scan.split: ${describe(historyKey)} listed=${ctx.listedBytes} " +
              "-> spark default")
          } else {
            logInfo(s"scan.split: ${describe(historyKey)} listed=${ctx.listedBytes} " +
              s"-> split=$decided bytes")
          }
          decided
        case None => sparkMaxSplitBytes
      }
    case _ => sparkMaxSplitBytes
  }

  /** The current planning's ratio, or logs why not. */
  private def observe(ctx: ScanContext): Option[Double] = {
    if (!ctx.isCurrent()) {
      logDebug(s"scan.split: ${describe(ctx.key)} not recorded: superseded planning")
      None
    } else {
      val reads = ctx.drainReads()
      val result = if (reads.splitsRead == 0L) {
        Left("planned but not executed in this query")
      } else {
        ScanSplitHeuristic.ratioFromReads(ctx.plannedSplits, reads)
      }
      result match {
        case Right(ratio) => Some(ratio)
        case Left(reason) =>
          logInfo(s"scan.split: ${describe(ctx.key)} not recorded: $reason")
          None
      }
    }
  }

  private def shouldObserve(ctx: ScanContext): Boolean = ctx.plannedSplits > 0
}

/**
 * What a scan's tasks read, over every read to the end: the decoded bytes, the on-disk bytes of
 * the splits read, and how many splits that is.
 */
final case class ReadTotals(decodedBytes: Long, listedBytes: Long, splitsRead: Long)

/**
 * Running totals of a scan's tasks in Spark's own accumulators, so the size does not grow with
 * the number of tasks.
 */
final class SplitTotals(
    decoded: LongAccumulator,
    listed: LongAccumulator,
    finished: LongAccumulator) extends Serializable {

  /** Counts one split read to the end: `listedBytes` on disk, `decodedBytes` decoded. */
  def add(decodedBytes: Long, listedBytes: Long): Unit = {
    decoded.add(decodedBytes)
    listed.add(listedBytes)
    finished.add(1L)
  }

  /** Returns and clears the totals atomically. */
  def drain(): ReadTotals = synchronized {
    val totals = ReadTotals(decoded.sum, listed.sum, finished.sum)
    decoded.reset()
    listed.reset()
    finished.reset()
    totals
  }
}

object SplitTotals {
  def create(sc: SparkContext): SplitTotals =
    new SplitTotals(sc.longAccumulator, sc.longAccumulator, sc.longAccumulator)
}

object ScanSplitHeuristic extends Logging {

  /** The production heuristic: reads and writes the installed history store. */
  val instance: ScanSplitHeuristic = new ScanSplitHeuristic(() => MetricStores.current())

  /**
   * Created when a split's reader opens; on exhaustion adds the growth of `decoded` and the split's
   * `listed` bytes to `totals`, and counts the split, once. A task reads its split sequentially,
   * so the growth is the split's. Metrics may start at -1.
   */
  def recordSplitOnExhaustion(
      listed: Long,
      decoded: GpuMetric,
      totals: SplitTotals): Boolean => Boolean = {
    val start = decoded.value.max(0L)
    var recorded = false // `hasNext` is called again after the data ends
    hasMore => {
      if (!hasMore && !recorded) {
        recorded = true
        totals.add(decoded.value.max(0L) - start, listed)
      }
      hasMore
    }
  }

  /** Wraps `reader` to record its split's decoded and listed bytes when it is exhausted. */
  def observeReader(
      reader: PartitionReader[ColumnarBatch],
      listed: Long,
      decoded: GpuMetric,
      totals: SplitTotals): PartitionReader[ColumnarBatch] = {
    val record = recordSplitOnExhaustion(listed, decoded, totals)
    new PartitionReader[ColumnarBatch] {
      override def next(): Boolean = record(reader.next())
      override def get(): ColumnarBatch = reader.get()
      override def close(): Unit = reader.close()
      override def currentMetricsValues(): Array[CustomTaskMetric] = reader.currentMetricsValues()
    }
  }

  /**
   * Wraps a per-file read function to record each file when it is read to the end: its decoded
   * bytes, and its own length as the listed bytes. Returns `read` unchanged when `totals` is
   * empty, which is while history is off.
   *
   * @param decoded only evaluated when `totals` is defined
   */
  def observeFileRead[T](
      read: PartitionedFile => Iterator[T],
      totals: Option[SplitTotals],
      decoded: => GpuMetric): PartitionedFile => Iterator[T] =
    totals.fold(read) { t =>
      val metric = decoded
      file => {
        val record = recordSplitOnExhaustion(file.length, metric, t)
        val rows = read(file)
        new Iterator[T] {
          override def hasNext: Boolean = record(rows.hasNext)
          override def next(): T = rows.next()
        }
      }
    }

  /**
   * sum(decoded) / sum(listed) over the splits read to the end, if at least `planned` were read.
   * Fewer means part of the scan never ran, so there is no ratio.
   *
   * More reads than planned are fine: every read adds its own decoded and listed bytes, so a
   * range partitioner's sampling pass, which reads every split before the real stage does, or a
   * retried or speculative task that reads a split again, is counted for what it read.
   *
   * The totals do not say which splits were read, so a split read twice that stands in for one
   * never read is not caught.
   */
  def ratioFromReads(planned: Int, reads: ReadTotals): Either[String, Double] = {
    if (planned <= 0) {
      Left("nothing planned")
    } else if (reads.splitsRead < planned) {
      Left(s"read ${reads.splitsRead} of $planned planned splits to the end")
    } else if (reads.listedBytes <= 0L) {
      Left("no listed bytes in the splits read")
    } else if (reads.decodedBytes <= 0L) {
      Left("no decoded bytes (decoded-bytes metric off or zero)")
    } else {
      Right(reads.decodedBytes.toDouble / reads.listedBytes.toDouble)
    }
  }

  /**
   * The history key of a scan, from `derive` (see `PlanKey`) while history is on; None while
   * it is off, or if the scan has no key or the key cannot be derived. Deriving a key never
   * fails the query.
   *
   * Limitations:
   *  - A table read by path is identified by its root paths as written, so a different spelling
   *    of the same data (another partition directory, an expanded glob, a new file list) is a
   *    different key and starts with no history.
   *  - DSv2 file scans are always identified by path, never by table name, so the same table
   *    read through v1 and through DSv2 keeps two separate histories.
   */
  def historyKey(derive: => Option[PlanKey]): Option[PlanKey] = {
    val key = if (instance.isEnabled) derived(derive) else None
    key.foreach { k =>
      logDebug(s"scan.split: key plan=${k.planHex} literals=${k.literalsHex} ${k.text}")
    }
    key
  }

  /** `derive`, or None, with the reason logged, if it has no key or fails. Never throws. */
  private[perf] def derived(derive: => Option[PlanKey]): Option[PlanKey] = {
    try {
      val key = derive
      if (key.isEmpty) {
        logInfo("scan.split: not recorded: the scan has no key")
      }
      key
    } catch {
      case NonFatal(e) =>
        logInfo(s"scan.split: not recorded: the key could not be derived (${e.getClass.getName})")
        None
    }
  }

  /** Learned split for `key` in `spark`, else `sparkMaxSplitBytes`. */
  def learnedFileSplit(
      key: Option[PlanKey],
      spark: SparkSession,
      listedBytes: => Long,
      sparkMaxSplitBytes: Long): Long = {
    lazy val listed = listedBytes
    instance.fileSourceSplit(key, sparkMaxSplitBytes, listed, scanHistoryKey => {
      val rapidsConf = new RapidsConf(spark.sessionState.conf)
      ScanContext(
        key = scanHistoryKey,
        listedBytes = listed,
        batchSizeBytes = rapidsConf.gpuTargetBatchSizeBytes,
        minPartitionNum = minPartitionNum(spark),
        maxSplitBytes = sparkMaxSplitBytes,
        limits = ScanSplitLimits(rapidsConf))
    }, System.currentTimeMillis())
  }

  /**
   * Queues a file scan of `planned` splits for recording at query end.
   *
   * @param listedBytes on-disk bytes of the planned splits; the size bucket of the recording
   */
  def registerFileScan(
      key: Option[PlanKey],
      planned: Int,
      listedBytes: => Long,
      totals: Option[SplitTotals],
      sc: SparkContext): Unit =
    for (scanKey <- key; read <- totals; sized <- ScanHistoryKey.of(scanKey, listedBytes)) {
      instance.register(ExecutionEndCallbacks.currentExecutionId(sc),
        ScanContext.forObservation(sized, planned, () => read.drain()))
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

  @transient private lazy val historyKey: Option[PlanKey] = ScanSplitHeuristic.historyKey(
    PlanKey.forFileScan(None, fileIndex.rootPaths, readDataSchema, partitionFilters,
      dataFilters))

  /** Decoded bytes and splits read; None while history is off. */
  @transient private lazy val splitReads: Option[SplitTotals] =
    historyKey.map(_ => SplitTotals.create(sparkSession.sparkContext))

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
    splitReads.fold(factory) { totals =>
      new SplitObservingReaderFactory(
        factory, metrics.getOrElse(GpuMetric.GPU_OUTPUT_BATCH_BYTES, NoopMetric), totals)
    }

  /**
   * Queues this scan for recording when the current SQL execution ends. The scan's exec calls
   * this every time it executes, after the partitions are planned, as the v1 scan does. A
   * plan that is executed again is therefore recorded again, and so is a scan whose reader
   * factory was built outside any SQL execution.
   */
  def registerForExecution(sc: SparkContext): Unit =
    ScanSplitHeuristic.registerFileScan(historyKey, plannedListed.length, plannedListed.sum,
      splitReads, sc)
}

object HistorySizedFileScan {

  /** Called by a GPU batch scan exec for its `scan`; does nothing for a scan without history. */
  def registerForExecution(scan: AnyRef, sc: SparkContext): Unit = scan match {
    case history: HistorySizedFileScan => history.registerForExecution(sc)
    case _ =>
  }
}

/** Records each file split's decoded and listed bytes when it is read to the end. */
class SplitObservingReaderFactory(
    factory: PartitionReaderFactory,
    decoded: GpuMetric,
    totals: SplitTotals) extends PartitionReaderFactory {

  override def createReader(p: InputPartition): PartitionReader[InternalRow] =
    factory.createReader(p)

  override def createColumnarReader(p: InputPartition): PartitionReader[ColumnarBatch] =
    p match {
      case fp: FilePartition =>
        // The same sum the scan's planning takes for this partition's listed bytes.
        val listed = FilePartitionShims.getFiles(fp).map(_.length).sum
        ScanSplitHeuristic.observeReader(factory.createColumnarReader(p), listed, decoded, totals)
      case _ => factory.createColumnarReader(p)
    }

  override def supportColumnarReads(p: InputPartition): Boolean = factory.supportColumnarReads(p)
}
