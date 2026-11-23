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

import com.nvidia.spark.history.{DimValue, MetricStore, MetricStores, Observation, SchemaStatus,
  Status, SummaryResponse}
import com.nvidia.spark.rapids.LocalGpuMetric
import com.nvidia.spark.rapids.shims.PartitionedFileUtilsShim
import org.apache.hadoop.fs.Path
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.LongAccumulator

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

  /** The default settings of `spark.rapids.sql.history.scanSplit.*`, stated here by the suite. */
  private val Limits = ScanSplitLimits(64 * MiB, 4 * GiB, Duration.ofDays(7))

  /** Decides the file-source split for `key` over 960 GiB on 96 slots, 1 GiB batches. */
  private def decide(h: ScanSplitHeuristic, key: String = table, nowMs: Long = now): Long =
    h.fileSourceSplit(Some(key), Static,
      name => ScanContext(name, 960 * GiB, GiB, 96L, Static, Limits), nowMs)

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
    ScanContext.forObservation(table, 1, () => ReadTotals(decoded, listed, 1L))

  test("the metric's id and name are permanent, and are in the catalog the plugin supplies") {
    // Stored observations are keyed by the id, so changing either value is a breaking change.
    assert(ScanExpansionRatio.id == 1)
    assert(ScanExpansionRatio.name == "scan.decode_expansion_ratio")
    val entry = HistoryHeuristics.catalog.find(ScanExpansionRatio.id)
    assert(entry.isPresent, s"metric ID ${ScanExpansionRatio.id} is not in the catalog")
    assert(entry.get.name == ScanExpansionRatio.name)
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
    // The window is the configured maximum age, 7 days by default.
    assert(request.fromMs() == now - Duration.ofDays(7).toMillis)
    assert(request.toMs() == now + 1)
    assert(request.limit() == 1)
    assert(timeout == policy.planningTimeout)
  }

  /** Like `decide`, with the configured maximum age of a reading. */
  private def decideWithin(h: ScanSplitHeuristic, maxAge: Duration): Long =
    h.fileSourceSplit(Some(table), Static,
      name => ScanContext(name, 960 * GiB, GiB, 96L, Static, Limits.copy(maxAge = maxAge)), now)

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
    assert(h.historyOf.reportedReasons == Set("lookup-UNAVAILABLE"))
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
      assert(h.historyOf.reportedReasons == Set(s"declare-$code"))
    }
  }

  test("accepted with a warning still decides") {
    val store = storeWith(4.0 -> (now - 1000))
    store.declareReason = "first effective retention remains unchanged"
    val h = heuristic(store)
    assert(decide(h) == 256 * MiB)
    assert(h.historyOf.reportedReasons == Set("declare-ACCEPTED"))
  }

  test("summarize errors and timeouts: no decision, one log line per code") {
    Seq(Status.Code.NOT_DECLARED, Status.Code.INVALID_REQUEST, Status.Code.DEADLINE_EXCEEDED,
        Status.Code.UNAVAILABLE, Status.Code.DENIED).foreach { code =>
      val store = storeWith(4.0 -> (now - 1000))
      store.summarizeError = Some(Status.of(code, "injected"))
      val h = heuristic(store)
      assert(decide(h) == Static, code)
      assert(decide(h) == Static, code)
      assert(h.historyOf.reportedReasons == Set(s"lookup-$code"))
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
      assert(h.historyOf.reportedReasons == Set("lookup-malformed"))
    }
    val store = storeWith(4.0 -> (now - 1000))
    store.summarizeThrows = Some(new IllegalStateException("boom"))
    val h = heuristic(store)
    assert(decide(h) == Static)
    assert(h.historyOf.reportedReasons ==
      Set("lookup-java.lang.IllegalStateException"))
  }

  test("malformed table names are not tracked") {
    val store = storeWith(4.0 -> (now - 1000))
    val h = heuristic(store)
    val badUtf16 = "db.\ud800"
    assert(decide(h, badUtf16) == Static)
    h.recordAll(scanContext(400 * MiB).copy(table = badUtf16))
    assert(store.summarizeCalls.isEmpty && store.offered.isEmpty)
    assert(h.historyOf.reportedReasons == Set("untrackable-key"))
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

  test("observeReader records the split's decoded bytes once the reader is exhausted") {
    val metric = new LocalGpuMetric
    val totals = newTotals()
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
    val reader = ScanSplitHeuristic.observeReader(inner, 7L, metric, totals)
    assert(reader.next() && reader.next())
    assert(totals.drain() == ReadTotals(0L, 0L, 0L))
    assert(!reader.next())
    reader.close()
    assert(closed)
    assert(totals.drain() == ReadTotals(10L, 7L, 1L))
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

  test("ratio of a complete scan: decoded over listed, over all its splits") {
    assert(ScanSplitHeuristic.ratioFromReads(3, ReadTotals(400 * MiB, 100 * MiB, 3L)) ==
      Right(4.0))
  }

  test("a scan read in two passes has the ratio of one pass") {
    // A range partitioner's sampling job reads every split before the real stage does.
    assert(ScanSplitHeuristic.ratioFromReads(3, ReadTotals(800 * MiB, 200 * MiB, 6L)) ==
      Right(4.0))
  }

  test("a split read again counts for what it read, in decoded and in listed bytes") {
    // Splits of 20, 30 and 50 MiB decode at 2x, 4x and 6x (40, 120 and 300 MiB). A retried or
    // speculative task reads the 50 MiB split again: 760 MiB decoded over 150 MiB listed.
    assert(ScanSplitHeuristic.ratioFromReads(3, ReadTotals(760 * MiB, 150 * MiB, 4L)) ==
      Right(760.0 / 150.0))
  }

  test("no ratio unless every planned split was read, and why") {
    def reason(planned: Int, listed: Long, decoded: Long, read: Long): Either[String, Double] =
      ScanSplitHeuristic.ratioFromReads(planned, ReadTotals(decoded, listed, read))
    assert(reason(3, MiB, MiB, 2L) == Left("read 2 of 3 planned splits to the end"))
    assert(reason(0, MiB, MiB, 1L) == Left("nothing planned"))
    assert(reason(1, 0L, MiB, 1L) == Left("no listed bytes in the splits read"))
    assert(reason(1, MiB, 0L, 1L) == Left("no decoded bytes (decoded-bytes metric off or zero)"))
  }

  test("an earlier planning of a scan is never evaluated") {
    val store = new FakeMetricStore
    val h = heuristic(store)
    val earlier = scanContext(decoded = 400 * MiB)
      .copy(isCurrent = () => false, drainReads = () => fail("an earlier planning was read"))
    h.recordAll(earlier)
    assert(store.offered.isEmpty)
    h.recordAll(scanContext(decoded = 200 * MiB))
    assert(store.stored.map(_.value()) == Seq(2.0))
  }

  test("observe skips a scan that did not read every split, or never ran") {
    val store = new FakeMetricStore
    val h = heuristic(store)
    h.recordAll(scanContext(decoded = 400 * MiB).copy(plannedSplits = 2))
    h.recordAll(scanContext(decoded = 400 * MiB).copy(drainReads = () => ReadTotals(0L, 0L, 0L)))
    assert(store.offered.isEmpty)
  }

  private def newTotals(): SplitTotals =
    new SplitTotals(new LongAccumulator, new LongAccumulator, new LongAccumulator)

  test("split bytes: recorded at exhaustion, excluding bytes before open; once per split") {
    val metric = new LocalGpuMetric
    metric += 100L // an earlier split of the scan in the same task
    val totals = newTotals()
    val record = ScanSplitHeuristic.recordSplitOnExhaustion(21L, metric, totals)
    metric += 40L
    assert(record(true))
    metric += 2L
    assert(!record(false))
    assert(!record(false)) // asked again after the end: not counted again
    assert(totals.drain() == ReadTotals(42L, 21L, 1L))
  }

  test("split bytes: nothing for a split closed early or failing; an empty split records 0") {
    val metric = new LocalGpuMetric
    val totals = newTotals()
    val abandoned = ScanSplitHeuristic.recordSplitOnExhaustion(21L, metric, totals)
    metric += 10L
    abandoned(true) // then closed, or the next read threw: no exhaustion
    assert(totals.drain() == ReadTotals(0L, 0L, 0L))
    ScanSplitHeuristic.recordSplitOnExhaustion(21L, metric, totals)(false)
    assert(totals.drain() == ReadTotals(0L, 21L, 1L))
  }

  test("split bytes: a size metric still at its -1 start counts as 0") {
    val metric = new LocalGpuMetric
    metric.set(-1L)
    val totals = newTotals()
    val record = ScanSplitHeuristic.recordSplitOnExhaustion(21L, metric, totals)
    metric.set(5L) // the first add moves -1 to 0 before adding
    record(false)
    assert(totals.drain() == ReadTotals(5L, 21L, 1L))
  }

  private def file(length: Long): PartitionedFile =
    PartitionedFileUtilsShim.newPartitionedFile(
      InternalRow.empty, s"file:/tmp/f-$length", 0L, length)

  /** Reads `rows` rows from a file, each decoding 5 bytes; the rows are 0 until `rows`. */
  private def reading(metric: LocalGpuMetric)(rows: PartitionedFile => Int)
    : PartitionedFile => Iterator[Int] =
    f => Iterator.range(0, rows(f)).map { i =>
      metric += 5L
      i
    }

  test("a per-file read records each file's own length and its decoded bytes at its end") {
    val metric = new LocalGpuMetric
    val totals = newTotals()
    val read = reading(metric)(f => if (f.length == 100L) 2 else 1)
    val wrapped = ScanSplitHeuristic.observeFileRead(read, Some(totals), metric)
    // The rows come through unchanged.
    assert(wrapped(file(100L)).toList == List(0, 1))
    assert(wrapped(file(40L)).toList == List(0))
    assert(totals.drain() == ReadTotals(15L, 140L, 2L))
  }

  test("a file read again is recorded again, for what it read") {
    val metric = new LocalGpuMetric
    val totals = newTotals()
    val wrapped = ScanSplitHeuristic.observeFileRead(reading(metric)(_ => 1), Some(totals), metric)
    wrapped(file(100L)).toList
    wrapped(file(100L)).toList
    assert(totals.drain() == ReadTotals(10L, 200L, 2L))
  }

  test("a per-file read records nothing for a file that is not read to the end") {
    val metric = new LocalGpuMetric
    val totals = newTotals()
    val wrapped = ScanSplitHeuristic.observeFileRead(reading(metric)(_ => 3), Some(totals), metric)
    val rows = wrapped(file(100L))
    assert(rows.next() == 0)
    assert(totals.drain() == ReadTotals(0L, 0L, 0L))
  }

  test("a per-file read counts an empty file, and counts a file once however often it is asked") {
    val metric = new LocalGpuMetric
    val totals = newTotals()
    val wrapped = ScanSplitHeuristic.observeFileRead(reading(metric)(_ => 0), Some(totals), metric)
    val rows = wrapped(file(100L))
    assert(!rows.hasNext && !rows.hasNext)
    assert(totals.drain() == ReadTotals(0L, 100L, 1L))
  }

  test("a per-file read excludes bytes decoded before the file was opened") {
    val metric = new LocalGpuMetric
    metric += 100L // an earlier file of the same task
    val totals = newTotals()
    val wrapped = ScanSplitHeuristic.observeFileRead(reading(metric)(_ => 2), Some(totals), metric)
    wrapped(file(100L)).toList
    assert(totals.drain() == ReadTotals(10L, 100L, 1L))
  }

  test("with no totals the read function is returned as it is, and the metric is not looked up") {
    val read: PartitionedFile => Iterator[Int] = _ => Iterator.empty
    assert(ScanSplitHeuristic.observeFileRead(read, None, fail("the metric was evaluated")) eq read)
  }

  test("totals add up over splits, and draining returns them and leaves nothing") {
    val totals = newTotals()
    totals.add(30L, 20L)
    totals.add(12L, 10L)
    assert(totals.drain() == ReadTotals(42L, 30L, 2L))
    assert(totals.drain() == ReadTotals(0L, 0L, 0L))
  }

  test("file-source hook: catalog tables only, and only while enabled") {
    val store = storeWith(4.0 -> (now - 1000))
    def ctx(name: String): ScanContext = ScanContext(name, 960 * GiB, GiB, 96L, 123L, Limits)
    val noContext: String => ScanContext = _ => fail("context built without a decision")
    val disabled = heuristic(store, enabled = false)
    assert(disabled.fileSourceSplit(Some(table), 123L, noContext, now) == 123L)
    val h = heuristic(store)
    assert(h.fileSourceSplit(None, 123L, noContext, now) == 123L)
    assert(store.summarizeCalls.isEmpty)
    assert(h.fileSourceSplit(Some(table), 123L, ctx, now) == 256 * MiB)
    assert(h.fileSourceSplit(Some("cat.db.other"), 123L, ctx, now) == 123L)
  }

}
