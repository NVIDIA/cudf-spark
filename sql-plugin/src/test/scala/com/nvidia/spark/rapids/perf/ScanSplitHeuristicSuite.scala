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

import java.nio.charset.StandardCharsets
import java.time.Duration
import java.util.Collections

import scala.collection.JavaConverters._

import com.nvidia.spark.history.{DimValue, HistoryMetricCatalog, MetricStore, MetricStores,
  Observation, SchemaStatus, Status, SummaryResponse}
import com.nvidia.spark.rapids.{GpuMetric, LocalGpuMetric}
import com.nvidia.spark.rapids.shims.PartitionedFileUtilsShim
import org.apache.hadoop.fs.Path
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader}
import org.apache.spark.sql.execution.datasources.{FilePartition, PartitionedFile}
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.CollectionAccumulator

/** Decide and observe against an in-memory store. Nothing here starts Spark. */
class ScanSplitHeuristicSuite extends AnyFunSuite {

  private val GiB = 1024L * 1024 * 1024
  private val MiB = 1024L * 1024
  private val table = "cat.db.events"
  private val now = 1790000000000L
  private val policy = HistoryPolicy(Duration.ofMillis(100), Duration.ofSeconds(5))

  private def heuristic(store: MetricStore, enabled: Boolean = true): ScanSplitHeuristic = {
    val h = new ScanSplitHeuristic(() => store)
    if (enabled) {
      h.enable(policy)
    }
    h
  }

  /** Spark's split when history does not decide. */
  private val Static = 128 * MiB

  /** Decides the file-source split for `key` over 960 GiB on 96 slots, 1 GiB batches. */
  private def decide(h: ScanSplitHeuristic, key: String = table, nowMs: Long = now): Long =
    h.fileSourceSplit(Some(key), Static,
      name => ScanContext(name, 960 * GiB, GiB, 96L, Static), nowMs)

  private def observation(key: String, value: Double, atMs: Long): Observation =
    new Observation(ScanExpansionRatio.metric,
      Collections.singletonMap(ScanExpansionRatio.dimension, DimValue.of(key)), value, atMs)

  /** A store that already holds `values` for `table`, recorded at the given times. */
  private def storeWith(values: (Double, Long)*): FakeMetricStore = {
    val store = new FakeMetricStore
    store.declare(Collections.singletonList(ScanExpansionRatio.schema), Duration.ofSeconds(1))
    values.foreach { case (v, at) => store.record(observation(table, v, at)) }
    store.declareCalls.clear()
    store.offered.clear()
    store
  }

  /** A one-split scan of `table` whose split decoded `decoded` of its `listed` bytes. */
  private def scanContext(decoded: Long, listed: Long = 100 * MiB): ScanContext =
    ScanContext(table, 0L, 0L, 0L, 0L, 1, () => Seq((0, decoded, listed)))

  test("the metric is named from its production catalog entry") {
    val entry = HistoryMetricCatalog.production().find(ScanExpansionRatio.id)
    assert(entry.isPresent, s"metric ID ${ScanExpansionRatio.id} is not in the catalog")
    assert(ScanExpansionRatio.name == "scan.decode_expansion_ratio")
    assert(!entry.get.retired())
  }

  test("decides from the table's last reading") {
    val store = storeWith(4.0 -> (now - 1000))
    assert(decide(heuristic(store)) == 256 * MiB)
  }

  test("summary request: table bound, window [now - 7 days, now + 1), limit 1") {
    val store = storeWith(4.0 -> (now - 1000))
    decide(heuristic(store))
    assert(store.summarizeCalls.size == 1)
    val (request, timeout) = store.summarizeCalls.head
    assert(request.metric() == ScanExpansionRatio.metric)
    assert(request.bound() == Collections.singletonMap("table", DimValue.of(table)))
    // Without a setting the window is the metric's declared planning age.
    assert(request.fromMs() == now - Duration.ofDays(7).toMillis)
    assert(request.toMs() == now + 1)
    assert(request.limit() == 1)
    assert(timeout == policy.planningTimeout)
  }

  /** Like `decide`, with the configured maximum age of a reading. */
  private def decideWithin(h: ScanSplitHeuristic, maxAge: Duration): Long =
    h.fileSourceSplit(Some(table), Static,
      name => ScanContext(name, 960 * GiB, GiB, 96L, Static, maxAge = Some(maxAge)), now)

  test("a reading older than the planning age is ignored unless the age is raised") {
    val store = storeWith(4.0 -> (now - Duration.ofDays(9).toMillis))
    assert(decide(heuristic(store)) == Static)
    assert(decideWithin(heuristic(store), Duration.ofDays(10)) == 256 * MiB)
    // A shorter age drops a reading that the default would still use.
    val recent = storeWith(4.0 -> (now - Duration.ofDays(3).toMillis))
    assert(decide(heuristic(recent)) == 256 * MiB)
    assert(decideWithin(heuristic(recent), Duration.ofDays(2)) == Static)
  }

  test("the configured age sets the lookup window and is capped at the store retention") {
    val store = storeWith(4.0 -> (now - Duration.ofDays(20).toMillis))
    assert(decideWithin(heuristic(store), Duration.ofDays(30)) == Static)
    assert(store.summarizeCalls.last._1.fromMs() == now - Duration.ofDays(14).toMillis)
    decideWithin(heuristic(store), Duration.ofDays(3))
    assert(store.summarizeCalls.last._1.fromMs() == now - Duration.ofDays(3).toMillis)
    // Changing the lookup age never changes what is declared.
    assert(store.declareCalls.nonEmpty)
    assert(store.declareCalls.forall(_._1.asScala.forall(
      _.recommendedRetention() == ScanExpansionRatio.retention)))
  }

  test("uses the most recent reading, not an average of history") {
    val store = storeWith(1.0 -> (now - 3000), 8.0 -> (now - 1000), 2.0 -> (now - 2000))
    assert(decide(heuristic(store)) == 128 * MiB)
  }

  test("equal timestamps resolve to the later-recorded reading") {
    val store = storeWith(1.0 -> (now - 1000), 4.0 -> (now - 1000))
    assert(decide(heuristic(store)) == 256 * MiB)
  }

  test("readings older than the planning age are ignored") {
    val store = storeWith(4.0 -> (now - Duration.ofDays(8).toMillis))
    assert(decide(heuristic(store)) == Static)
  }

  test("no history for the table means no decision") {
    val store = storeWith(4.0 -> (now - 1000))
    assert(decide(heuristic(store), "cat.db.other") == Static)
    assert(decide(heuristic(new FakeMetricStore)) == Static)
  }

  test("table keys are exact: case differs, history differs") {
    val store = storeWith(4.0 -> (now - 1000))
    assert(decide(heuristic(store), table.toUpperCase(java.util.Locale.ROOT)) ==
      Static)
  }

  test("disabled heuristic never touches the store") {
    val store = storeWith(4.0 -> (now - 1000))
    assert(decide(heuristic(store, enabled = false)) == Static)
    assert(store.declareCalls.isEmpty && store.summarizeCalls.isEmpty)
  }

  test("built-in no-op store: no decision, one log line") {
    val noop = MetricStores.current()
    assume(noop.info().description().contains("no-op"), "another store is installed")
    val h = heuristic(noop)
    assert(decide(h) == Static)
    assert(decide(h) == Static)
    assert(h.historyOf(ScanExpansionRatio).reportedReasons == Set("lookup-UNAVAILABLE"))
  }

  test("rejected declaration: no lookup, no record, one log line per code") {
    Seq(SchemaStatus.Code.INCOMPATIBLE, SchemaStatus.Code.INVALID_REQUEST,
        SchemaStatus.Code.UNAVAILABLE, SchemaStatus.Code.DENIED).foreach { code =>
      val store = storeWith(4.0 -> (now - 1000))
      store.declareCode = code
      val h = heuristic(store)
      assert(decide(h) == Static, code)
      assert(decide(h) == Static, code)
      h.recordAll(scanContext(decoded = 400 * MiB))
      assert(store.declareCalls.size == 1, s"$code: declared once and cached")
      assert(store.summarizeCalls.isEmpty, code)
      assert(store.offered.isEmpty, code)
      assert(h.historyOf(ScanExpansionRatio).reportedReasons == Set(s"declare-$code"))
    }
  }

  test("accepted with a warning still decides") {
    val store = storeWith(4.0 -> (now - 1000))
    store.declareReason = "first effective retention remains unchanged"
    val h = heuristic(store)
    assert(decide(h) == 256 * MiB)
    assert(h.historyOf(ScanExpansionRatio).reportedReasons == Set("declare-ACCEPTED"))
  }

  test("summarize errors and timeouts: no decision, one log line per code") {
    Seq(Status.Code.NOT_DECLARED, Status.Code.INVALID_REQUEST, Status.Code.DEADLINE_EXCEEDED,
        Status.Code.UNAVAILABLE, Status.Code.DENIED).foreach { code =>
      val store = storeWith(4.0 -> (now - 1000))
      store.summarizeError = Some(Status.of(code, "injected"))
      val h = heuristic(store)
      assert(decide(h) == Static, code)
      assert(decide(h) == Static, code)
      assert(h.historyOf(ScanExpansionRatio).reportedReasons == Set(s"lookup-$code"))
    }
  }

  test("malformed or failing summarize: no decision") {
    val malformed = Seq(
      FakeMetricStore.emptyResponses,
      java.util.Arrays.asList(SummaryResponse.ok(null), SummaryResponse.ok(null)),
      null)
    malformed.foreach { responses =>
      val store = storeWith(4.0 -> (now - 1000))
      store.summarizeRaw = Some(responses)
      val h = heuristic(store)
      assert(decide(h) == Static)
      assert(h.historyOf(ScanExpansionRatio).reportedReasons == Set("lookup-malformed"))
    }
    val store = storeWith(4.0 -> (now - 1000))
    store.summarizeThrows = Some(new IllegalStateException("boom"))
    val h = heuristic(store)
    assert(decide(h) == Static)
    assert(h.historyOf(ScanExpansionRatio).reportedReasons ==
      Set("lookup-java.lang.IllegalStateException"))
  }

  test("malformed table names are not tracked") {
    val store = storeWith(4.0 -> (now - 1000))
    val h = heuristic(store)
    val badUtf16 = "db.\ud800"
    assert(decide(h, badUtf16) == Static)
    h.recordAll(scanContext(400 * MiB).copy(table = badUtf16))
    assert(store.summarizeCalls.isEmpty && store.offered.isEmpty)
    assert(h.historyOf(ScanExpansionRatio).reportedReasons == Set("untrackable-key"))
  }

  test("over-long table names are tracked under their capped key") {
    val store = new FakeMetricStore
    val h = heuristic(store)
    val long = "t" * 300
    h.recordAll(scanContext(400 * MiB).copy(table = long))
    assert(store.stored.map(_.dimensions().get("table")) ==
      Seq(DimValue.of(ScanSplitHeuristic.capped(long))))
    assert(decide(h, long, store.stored.head.timestampMs() + 1) == 256 * MiB)
  }

  private def utf8(s: String): Int = s.getBytes(StandardCharsets.UTF_8).length

  test("capped: keys at the cap are kept, longer ones hashed with a readable prefix") {
    val limit = ScanSplitHeuristic.MAX_KEY_BYTES
    assert(limit == 253)
    val atCap = "k" * limit
    assert(ScanSplitHeuristic.capped(atCap) eq atCap)
    val long = "cat.db." + "x" * 400
    val c = ScanSplitHeuristic.capped(long)
    assert(c == ScanSplitHeuristic.capped(long), "deterministic")
    assert(utf8(c) == limit)
    assert(c.matches("cat\\.db\\.x+#[0-9a-f]{16}"))
    assert(ScanSplitHeuristic.capped(long + "y") != c)
    // Never cuts a multi-byte character: 2-byte and 4-byte code points.
    Seq("\u00e9", "\ud83d\ude00").foreach { ch =>
      val capped = ScanSplitHeuristic.capped(ch * 200)
      val prefix = capped.substring(0, capped.indexOf('#'))
      assert(prefix.codePoints().allMatch(_ == ch.codePointAt(0)))
      assert(utf8(capped) <= limit)
      assert(DimValue.of(capped) != null)
    }
  }

  private def p(s: String): Path = new Path(s)

  test("scanKey: catalog name, else sorted distinct normalized roots") {
    assert(ScanSplitHeuristic.scanKey(Some(table), fail("roots read")) ==
      Right(table))
    val key = ScanSplitHeuristic.scanKey(None,
      Seq(p("s3a://b/t2/"), p("s3a://user:secret@b/t1"), p("s3a://b/t2")))
    assert(key == Right("path:s3a://b/t1,s3a://b/t2"))
    assert(ScanSplitHeuristic.scanKey(None, Seq(p("s3a://b/t1"))) !=
      ScanSplitHeuristic.scanKey(None, Seq(p("s3a://b/t1"), p("s3a://b/t3"))))
    val roots = (0 until 20).map(i => p(s"hdfs://nn/warehouse/some/long/table/path/dt=$i"))
    val capped = ScanSplitHeuristic.scanKey(None, roots).toOption.get
    assert(capped.startsWith("path:hdfs://nn/warehouse/") && utf8(capped) <= 253)
  }

  test("scanKey: no usable key without roots; a file list keys on its exact files") {
    assert(ScanSplitHeuristic.scanKey(None, Nil) == Left("no root paths"))
    assert(ScanSplitHeuristic.scanKey(None, Seq(p("file:/d/b.parquet"), p("file:/d/a.parquet"))) ==
      Right("path:file:/d/a.parquet,file:/d/b.parquet"))
    assert(ScanSplitHeuristic.scanKey(None, Seq(p("file:/d/a.parquet"))) !=
      ScanSplitHeuristic.scanKey(None, Seq(p("file:/d/a.parquet"), p("file:/d/b.parquet"))))
  }

  private def chunk(path: String, start: Long, length: Long, part: Int = 0): PartitionedFile =
    PartitionedFileUtilsShim.newPartitionedFile(InternalRow(part), path, start, length)

  private def spans(files: Seq[PartitionedFile]): Seq[(String, Long, Long)] =
    files.map(f => (f.filePath.toString, f.start, f.length))

  test("resizeSplits: no learned split leaves the planned partitions unchanged") {
    val planned: Array[InputPartition] = Array(FilePartition(0, Array(chunk("file:/d/a", 0, 10))))
    assert(ScanSplitHeuristic.resizeSplits(null, planned, 0L, _ => true) eq planned)
    assert(ScanSplitHeuristic.resizeSplits(null, planned, -1L, _ => true) eq planned)
  }

  test("recut: re-joins a file's contiguous chunks and cuts it at the learned split") {
    // Spark's planning sorts by size, so a file's chunks need not be adjacent.
    val planned = Seq(chunk("file:/d/a", 0, 4), chunk("file:/d/b", 0, 3, part = 1),
      chunk("file:/d/a", 8, 2), chunk("file:/d/a", 4, 4))
    val cut = ScanSplitHeuristic.recut(planned, 6L, _ => true)
    assert(spans(cut) == Seq(("file:/d/a", 0L, 6L), ("file:/d/a", 6L, 4L), ("file:/d/b", 0L, 3L)))
    assert(cut.map(_.partitionValues.getInt(0)) == Seq(0, 0, 1))
    val whole = ScanSplitHeuristic.recut(planned, 100L, _ => true)
    assert(spans(whole) == Seq(("file:/d/a", 0L, 10L), ("file:/d/b", 0L, 3L)))
  }

  test("recut: unsplitable files are joined but not cut; gaps are not joined") {
    val planned = Seq(chunk("file:/d/a.gz", 0, 4), chunk("file:/d/a.gz", 4, 4))
    assert(spans(ScanSplitHeuristic.recut(planned, 2L, _ => false)) ==
      Seq(("file:/d/a.gz", 0L, 8L)))
    val gap = Seq(chunk("file:/d/a", 0, 4), chunk("file:/d/a", 6, 4))
    assert(spans(ScanSplitHeuristic.recut(gap, 100L, _ => true)) ==
      Seq(("file:/d/a", 0L, 4L), ("file:/d/a", 6L, 4L)))
  }

  test("observeReader records the split's decoded bytes once the reader is exhausted") {
    val metric = new LocalGpuMetric
    val acc = new CollectionAccumulator[(Int, Long, Long)]
    var closed = false
    val inner = new PartitionReader[ColumnarBatch] {
      private var left = 2
      override def next(): Boolean = {
        left -= 1
        if (left >= 0) metric += 5L
        left >= 0
      }
      override def get(): ColumnarBatch = null
      override def close(): Unit = closed = true
    }
    val reader = ScanSplitHeuristic.observeReader(inner, 3, 7L, metric, acc)
    assert(reader.next() && reader.next())
    assert(acc.value.isEmpty)
    assert(!reader.next())
    reader.close()
    assert(closed)
    assert(acc.value.asScala == Seq((3, 10L, 7L)))
  }

  test("observe records decoded / listed under the table") {
    val store = new FakeMetricStore
    val h = heuristic(store)
    h.recordAll(scanContext(decoded = 400 * MiB))
    assert(store.stored.size == 1)
    val o = store.stored.head
    assert(o.metric() == ScanExpansionRatio.metric)
    assert(o.dimensions() == Collections.singletonMap("table", DimValue.of(table)))
    assert(o.value() == 4.0)
    // The next decision uses it.
    assert(decide(h, table, o.timestampMs() + 1) == 256 * MiB)
  }

  test("ratio of a complete scan: decoded over listed, summed over its splits") {
    val entries = Seq((0, 100 * MiB, 25 * MiB), (1, 200 * MiB, 50 * MiB), (2, 100 * MiB, 25 * MiB))
    assert(ScanSplitHeuristic.ratioFromSplits(3, entries) == Right(4.0))
  }

  test("the ratio uses the listed bytes of the splits that ran, whatever was registered") {
    val entries = Seq((0, 30 * MiB, 10 * MiB), (1, 90 * MiB, 30 * MiB))
    assert(ScanSplitHeuristic.ratioFromSplits(2, entries) == Right(3.0))
  }

  test("a split read twice counts once, with its first value") {
    // A range partitioner's sampling job reads every split before the real stage does.
    val once = Seq((0, 100 * MiB, 50 * MiB), (1, 300 * MiB, 50 * MiB))
    assert(ScanSplitHeuristic.ratioFromSplits(2, once ++ once) == Right(4.0))
    val retried = once :+ ((1, 900 * MiB, 50 * MiB))
    assert(ScanSplitHeuristic.ratioFromSplits(2, retried) == Right(4.0))
  }

  test("no ratio unless exactly the planned splits were read, and why") {
    def reason(planned: Int, entries: (Int, Long, Long)*): Either[String, Double] =
      ScanSplitHeuristic.ratioFromSplits(planned, entries)
    assert(reason(3, (0, MiB, MiB), (2, MiB, MiB)) ==
      Left("read 2 of 3 planned splits to the end"))
    assert(reason(3, (0, MiB, MiB), (1, MiB, MiB), (3, MiB, MiB)) ==
      Left("read split indices outside 0..2"))
    assert(reason(0) == Left("nothing planned"))
    assert(reason(1, (0, MiB, 0L)) == Left("no listed bytes in the splits read"))
    assert(reason(1, (0, 0L, MiB)) == Left("no decoded bytes (decoded-bytes metric off or zero)"))
  }

  test("an earlier planning of a scan is never evaluated") {
    val store = new FakeMetricStore
    val h = heuristic(store)
    val earlier = scanContext(decoded = 400 * MiB)
      .copy(isCurrent = () => false, drainSplits = () => fail("an earlier planning was read"))
    h.recordAll(earlier)
    assert(store.offered.isEmpty)
    h.recordAll(scanContext(decoded = 200 * MiB))
    assert(store.stored.map(_.value()) == Seq(2.0))
  }

  test("observe skips a scan that did not read every split, or never ran") {
    val store = new FakeMetricStore
    val h = heuristic(store)
    h.recordAll(scanContext(decoded = 400 * MiB).copy(plannedSplits = 2))
    h.recordAll(scanContext(decoded = 400 * MiB).copy(drainSplits = () => Seq.empty))
    assert(store.offered.isEmpty)
  }

  private def splitOf(metric: GpuMetric, acc: CollectionAccumulator[(Int, Long, Long)]) =
    ScanSplitHeuristic.recordSplitOnExhaustion(7, 21L, metric, acc)

  test("split bytes: recorded at exhaustion, excluding bytes before open; repeats don't count") {
    val metric = new LocalGpuMetric
    metric += 100L // an earlier split of the scan in the same task
    val acc = new CollectionAccumulator[(Int, Long, Long)]
    val record = splitOf(metric, acc)
    metric += 40L
    assert(record(true))
    metric += 2L
    assert(!record(false))
    assert(!record(false)) // asked again after the end: same value again
    assert(acc.value.asScala == Seq((7, 42L, 21L), (7, 42L, 21L)))
    // The driver counts split 7 once: splits 0-6 empty, 42 bytes over 21 listed.
    val entries = (0 until 7).map(i => (i, 0L, 0L)) ++ acc.value.asScala
    assert(ScanSplitHeuristic.ratioFromSplits(8, entries) == Right(2.0))
  }

  test("split bytes: nothing for a split closed early or failing; an empty split records 0") {
    val metric = new LocalGpuMetric
    val acc = new CollectionAccumulator[(Int, Long, Long)]
    val abandoned = splitOf(metric, acc)
    metric += 10L
    abandoned(true) // then closed, or the next read threw: no exhaustion
    assert(acc.value.isEmpty)
    splitOf(metric, acc)(false)
    assert(acc.value.asScala == Seq((7, 0L, 21L)))
  }

  test("split bytes: a size metric still at its -1 start counts as 0") {
    val metric = new LocalGpuMetric
    metric.set(-1L)
    val acc = new CollectionAccumulator[(Int, Long, Long)]
    val record = splitOf(metric, acc)
    metric.set(5L) // the first add moves -1 to 0 before adding
    record(false)
    assert(acc.value.asScala == Seq((7, 5L, 21L)))
  }

  test("draining returns the entries and leaves the accumulator empty") {
    val acc = new CollectionAccumulator[(Int, Long, Long)]
    acc.add((0, 1L, 3L))
    acc.add((0, 2L, 3L))
    assert(ScanSplitHeuristic.drainSplits(acc) == Seq((0, 1L, 3L), (0, 2L, 3L)))
    assert(acc.value.isEmpty)
  }

  test("file-source hook: catalog tables only, and only while enabled") {
    val store = storeWith(4.0 -> (now - 1000))
    def ctx(name: String): ScanContext = ScanContext(name, 960 * GiB, GiB, 96L, 123L)
    val noContext: String => ScanContext = _ => fail("context built without a decision")
    val disabled = heuristic(store, enabled = false)
    assert(disabled.fileSourceSplit(Some(table), 123L, noContext, now) == 123L)
    val h = heuristic(store)
    assert(h.fileSourceSplit(None, 123L, noContext, now) == 123L)
    assert(store.summarizeCalls.isEmpty)
    assert(h.fileSourceSplit(Some(table), 123L, ctx, now) == 256 * MiB)
    assert(h.fileSourceSplit(Some("cat.db.other"), 123L, ctx, now) == 123L)
  }

  test("minPartitionNum reads the session settings, falling back to executor slots") {
    val conf = new org.apache.spark.sql.internal.SQLConf
    def partitions: Long = ScanSplitHeuristic.minPartitionNum(conf, 7)
    assert(partitions == 7L)
    conf.setConfString("spark.executor.instances", "6")
    assert(partitions == 7L, "both slot settings are needed")
    conf.setConfString("spark.executor.cores", " 16 ")
    assert(partitions == 96L)
    conf.setConfString("spark.default.parallelism", "200")
    assert(partitions == 200L)
    conf.setConfString("spark.sql.leafNodeDefaultParallelism", "40")
    assert(partitions == 40L)
    conf.setConfString("spark.sql.files.minPartitionNum", "500")
    assert(partitions == 500L)
  }
}
