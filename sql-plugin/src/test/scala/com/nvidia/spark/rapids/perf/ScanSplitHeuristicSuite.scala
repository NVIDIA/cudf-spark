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
import java.util.Collections

import scala.collection.JavaConverters._

import com.nvidia.spark.history.{DimMatch, DimValue, LookupResponse, MetricStore, MetricStores,
  Observation, SchemaStatus, Status}
import com.nvidia.spark.rapids.LocalGpuMetric
import com.nvidia.spark.rapids.shims.PartitionedFileUtilsShim
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
  private val now = 1790000000000L
  private val policy = HistoryPolicy(Duration.ofMillis(100), Duration.ofSeconds(5))

  /** A scan of a table that filters on a date, as it ran on one day. */
  private val scan = PlanKey(0x1234567890abcdefL, -0x0fedcba098765432L)

  /** The same scan on another day. */
  private val nextDay = PlanKey(scan.plan, 42L)

  /** Every decision below lists 960 GiB, which is in the size bucket of 2^39. */
  private val Listed = 960 * GiB
  private val Bucket = 39L

  private def sized(key: PlanKey, bucket: Long = Bucket): ScanHistoryKey =
    ScanHistoryKey(key, bucket)

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
  private def decide(
      h: ScanSplitHeuristic,
      key: PlanKey = scan,
      nowMs: Long = now,
      limits: ScanSplitLimits = Limits): Long =
    h.fileSourceSplit(Some(key), Static, Listed,
      hk => ScanContext(hk, Listed, GiB, 96L, Static, limits), nowMs)

  private def observation(key: ScanHistoryKey, value: Double, atMs: Long): Observation =
    new Observation(ScanExpansionRatio.metric, key.dimensions.asJava, value, atMs)

  /** A store that already holds these readings, each recorded a second ago. */
  private def storeHolding(readings: (ScanHistoryKey, Double)*): FakeMetricStore = {
    val store = new FakeMetricStore
    store.declare(Collections.singletonList(ScanExpansionRatio.schema), Duration.ofSeconds(1))
    readings.foreach { case (key, v) => store.record(observation(key, v, now - 1000)) }
    store.declareCalls.clear()
    store.offered.clear()
    store
  }

  /** A store that already holds `values` for `scan`, recorded at the given times. */
  private def storeWith(values: (Double, Long)*): FakeMetricStore = {
    val store = new FakeMetricStore
    store.declare(Collections.singletonList(ScanExpansionRatio.schema), Duration.ofSeconds(1))
    values.foreach { case (v, at) => store.record(observation(sized(scan), v, at)) }
    store.declareCalls.clear()
    store.offered.clear()
    store
  }

  /** A one-split scan of `scan` whose split decoded `decoded` of its `listed` bytes. */
  private def scanContext(decoded: Long, listed: Long = 100 * MiB): ScanContext =
    ScanContext.forObservation(sized(scan), 1, () => ReadTotals(decoded, listed, 1L))

  test("the metric's id and name are permanent, and are in the catalog the plugin supplies") {
    // Stored observations are keyed by the id, so changing either value is a breaking change.
    assert(ScanExpansionRatio.id == 1)
    assert(ScanExpansionRatio.name == "scan.decode_expansion_ratio")
    val entry = HistoryHeuristics.catalog.find(ScanExpansionRatio.id)
    assert(entry.isPresent, s"metric ID ${ScanExpansionRatio.id} is not in the catalog")
    assert(entry.get.name == ScanExpansionRatio.name)
    assert(!entry.get.retired())
  }

  test("the metric's dimensions are the plan, the literals and the size: floor(log2 bytes)") {
    assert(ScanExpansionRatio.dimensions.map(d => d.name() -> d.kind()) == Seq(
      "plan" -> DimValue.Kind.LONG,
      "literals" -> DimValue.Kind.LONG,
      "size" -> DimValue.Kind.LONG))
    def bucket(bytes: Long): Option[Long] = ScanHistoryKey.of(scan, bytes).map(_.size)
    assert(bucket(0L).isEmpty && bucket(-5L).isEmpty)
    assert(Seq(1L, 3L, GiB - 1, GiB, Long.MaxValue).map(bucket) ==
      Seq(0L, 1L, 29L, 30L, 62L).map(Some(_)))
  }

  test("search list: exact, then the size neighbours, then any literals within one bucket") {
    val list = sized(scan).searchList
    def one(value: DimValue): DimMatch = DimMatch.equalTo(value)
    def oneOf(buckets: Long*): DimMatch = DimMatch.in(buckets.map(b => DimValue.of(b)).asJava)
    val plan = "plan" -> one(DimValue.of(scan.plan))
    val literals = "literals" -> one(DimValue.of(scan.literals))
    assert(list == Seq(
      Map(plan, literals, "size" -> one(DimValue.of(39L))),
      Map(plan, literals, "size" -> oneOf(38L, 40L)),
      Map(plan, "size" -> oneOf(38L, 39L, 40L))))
    // There is no bucket below 0.
    assert(sized(scan, 0L).searchList(1) == Map(plan, literals, "size" -> oneOf(1L)))
    assert(sized(scan, 0L).searchList(2) == Map(plan, "size" -> oneOf(0L, 1L)))
  }

  test("decides from a reading of the same scan") {
    val store = storeWith(4.0 -> (now - 1000))
    assert(decide(heuristic(store)) == 256 * MiB)
  }

  test("one request carries the whole search list, over the planning window") {
    val store = storeWith(4.0 -> (now - 1000))
    decide(heuristic(store))
    assert(store.lookupCalls.size == 1, "one request, one call")
    assert(store.summarizeCalls.isEmpty, "no request per tier")
    val (request, timeout) = store.lookupCalls.head
    assert(timeout == policy.planningTimeout)
    assert(request.metric() == ScanExpansionRatio.metric)
    // The window is the configured maximum age, 7 days by default.
    assert(request.fromMs() == now - Duration.ofDays(7).toMillis)
    assert(request.toMs() == now + 1)
    // The default reads only the newest observation of the winning tier, also where the
    // literals are left out.
    assert(request.limit() == 1)
    assert(request.tiers().asScala == sized(scan).searchList.map(_.asJava))
  }

  /** Like `decide`, with the configured maximum age of a reading. */
  private def decideWithin(h: ScanSplitHeuristic, maxAge: Duration): Long =
    decide(h, limits = Limits.copy(maxAge = maxAge))

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
    assert(store.lookupCalls.last._1.fromMs() == now - Duration.ofDays(14).toMillis)
    decideWithin(heuristic(store), Duration.ofDays(3))
    assert(store.lookupCalls.last._1.fromMs() == now - Duration.ofDays(3).toMillis)
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

  test("another plan, even by one bit, has no history, whatever its literals") {
    val store = storeHolding(sized(scan) -> 4.0)
    val otherPlan = PlanKey(scan.plan ^ 1L, scan.literals)
    assert(decide(heuristic(store), otherPlan) == Static)
    assert(decide(heuristic(store), PlanKey(otherPlan.plan, nextDay.literals)) == Static)
    assert(decide(heuristic(new FakeMetricStore)) == Static)
  }

  test("the aggregate combines the readings of the first tier that has any") {
    // Tier 3 holds three readings in three size buckets: 2, 4 and 6, the newest being 4.
    val store = new FakeMetricStore
    store.declare(Collections.singletonList(ScanExpansionRatio.schema), Duration.ofSeconds(1))
    Seq((Bucket - 1, 2.0, 3000L), (Bucket, 4.0, 1000L), (Bucket + 1, 6.0, 2000L)).foreach {
      case (bucket, ratio, age) =>
        store.record(observation(sized(nextDay, bucket), ratio, now - age))
    }
    assert(Limits.aggregate == RatioAggregate.Latest)
    Seq(RatioAggregate.Latest -> 256 * MiB, RatioAggregate.Mean -> 256 * MiB,
        RatioAggregate.Min -> 512 * MiB, RatioAggregate.Max -> (GiB / 6.0).toLong,
        // 2, 4 and 6 are within a factor of three, so consistent takes their mean.
        RatioAggregate.Consistent -> 256 * MiB).foreach {
      case (aggregate, expected) =>
        assert(decide(heuristic(store), limits = Limits.copy(aggregate = aggregate)) == expected,
          aggregate.name)
    }
  }

  test("consistent: readings within a factor of three give their mean, others Spark's split") {
    val consistent = Limits.copy(aggregate = RatioAggregate.Consistent)
    val agree = storeHolding(sized(scan) -> 3.0)
    agree.record(observation(sized(scan), 5.0, now - 500))
    assert(decide(heuristic(agree), limits = consistent) == 256 * MiB)
    val disagree = storeHolding(sized(scan) -> 1.0)
    disagree.record(observation(sized(scan), 4.0, now - 500))
    assert(decide(heuristic(disagree), limits = consistent) == Static)
  }

  test("disabled heuristic never touches the store") {
    val store = storeWith(4.0 -> (now - 1000))
    assert(decide(heuristic(store, enabled = false)) == Static)
    assert(store.declareCalls.isEmpty && store.lookupCalls.isEmpty)
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
      assert(store.lookupCalls.isEmpty, code)
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

  test("malformed or failing lookup: no decision") {
    val malformed: Seq[java.util.List[LookupResponse]] = Seq(
      Collections.emptyList[LookupResponse](),
      java.util.Arrays.asList(LookupResponse.none(), LookupResponse.none()),
      null)
    malformed.foreach { responses =>
      val store = storeWith(4.0 -> (now - 1000))
      store.lookupRaw = Some(responses)
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

  test("a key that cannot be derived is no key, and never an exception") {
    val key = PlanKey(7L, PlanKey.NONE)
    assert(ScanSplitHeuristic.derived(Some(key)) == Some(key))
    assert(ScanSplitHeuristic.derived(None).isEmpty)
    assert(ScanSplitHeuristic.derived(throw new IllegalStateException("boom")).isEmpty)
    assert(ScanSplitHeuristic.derived(throw new NoSuchElementException("no part")).isEmpty)
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

  test("observe records decoded / listed under the scan's key") {
    val store = new FakeMetricStore
    val h = heuristic(store)
    h.recordAll(scanContext(decoded = 400 * MiB))
    assert(store.stored.size == 1)
    val o = store.stored.head
    assert(o.metric() == ScanExpansionRatio.metric)
    assert(o.dimensions() == sized(scan).dimensions.asJava)
    assert(o.value() == 4.0)
    // The next decision uses it.
    assert(decide(h, scan, o.timestampMs() + 1) == 256 * MiB)
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

  test("file-source hook: scans with a key and listed bytes, and only while enabled") {
    val store = storeWith(4.0 -> (now - 1000))
    def ctx(hk: ScanHistoryKey): ScanContext =
      ScanContext(hk, Listed, GiB, 96L, 123L, Limits)
    val noContext: ScanHistoryKey => ScanContext = _ => fail("context built without a decision")
    val disabled = heuristic(store, enabled = false)
    assert(disabled.fileSourceSplit(Some(scan), 123L, Listed, noContext, now) == 123L)
    val h = heuristic(store)
    assert(h.fileSourceSplit(None, 123L, Listed, noContext, now) == 123L)
    assert(h.fileSourceSplit(Some(scan), 123L, 0L, noContext, now) == 123L, "nothing listed")
    assert(store.lookupCalls.isEmpty)
    assert(h.fileSourceSplit(Some(scan), 123L, Listed, ctx, now) == 256 * MiB)
    assert(h.fileSourceSplit(Some(PlanKey(0L, 0L)), 123L, Listed, ctx, now) == 123L)
  }

}
