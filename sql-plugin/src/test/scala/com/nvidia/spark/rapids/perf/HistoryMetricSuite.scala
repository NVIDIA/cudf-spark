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

import scala.collection.JavaConverters._

import com.nvidia.spark.history.{DimensionSpec, DimMatch, DimValue, Retention}
import org.scalatest.funsuite.AnyFunSuite

/** The catalog the plugin builds from its metric families. */
class HistoryMetricSuite extends AnyFunSuite {

  private def family(familyId: Int, familyName: String, familyVersion: Int = 1): HistoryMetric =
    new HistoryMetric {
      override val id: Int = familyId
      override val name: String = familyName
      override val version: Int = familyVersion
      override val dimensions: Seq[DimensionSpec] =
        Seq(new DimensionSpec("table", DimValue.Kind.STRING))
      override val retention: Retention = new Retention(Duration.ofDays(7), Duration.ofDays(14))
    }

  test("the catalog has one live entry per family, whatever its versions") {
    val catalog = HistoryMetric.catalogOf(
      Seq(family(9, "b.second"), family(3, "a.first", 1), family(3, "a.first", 2)))

    assert(catalog.entries().size == 2)
    assert(catalog.find(3).get.name == "a.first")
    assert(!catalog.find(3).get.retired())
  }

  test("two families may not share an id or a name") {
    intercept[IllegalArgumentException] {
      HistoryMetric.catalogOf(Seq(family(1, "a.metric"), family(1, "b.metric")))
    }
    intercept[IllegalArgumentException] {
      HistoryMetric.catalogOf(Seq(family(1, "a.metric"), family(2, "a.metric")))
    }
  }

  /** A family keyed by a table and a number of read columns: a key may have several parts. */
  private object TwoDimensional extends HistoryMetric {
    override val id: Int = 7
    override val name: String = "test.two_dimensions"
    override val version: Int = 1
    override val dimensions: Seq[DimensionSpec] = Seq(
      new DimensionSpec("table", DimValue.Kind.STRING),
      new DimensionSpec("columns", DimValue.Kind.LONG))
    override val retention: Retention = new Retention(Duration.ofDays(7), Duration.ofDays(14))
  }

  private val policy = HistoryPolicy(Duration.ofSeconds(1), Duration.ofSeconds(1))

  private def key(table: String, columns: Long): Map[String, DimValue] =
    Map("table" -> DimValue.of(table), "columns" -> DimValue.of(columns))

  private def is(value: DimValue): DimMatch = DimMatch.equalTo(value)

  private def oneOf(values: Long*): DimMatch = DimMatch.in(values.map(v => DimValue.of(v)).asJava)

  /** A history with two readings: (t, 3) = 4.0 and (t, 5) = 2.0, the second one newer. */
  private def withTwoReadings(now: Long): (FakeMetricStore, MetricHistory) = {
    val store = new FakeMetricStore
    val history = new MetricHistory(TwoDimensional, () => store)
    assert(history.declare(policy))
    history.record(key("t", 3L), 4.0, now - 2000, policy)
    history.record(key("t", 5L), 2.0, now - 1000, policy)
    assert(store.stored.size == 2)
    (store, history)
  }

  test("one lookup returns every tier's readings, each in the first tier that matches it") {
    val now = System.currentTimeMillis()
    val (store, history) = withTwoReadings(now)
    val tiers = Seq(
      Map("table" -> is(DimValue.of("t")), "columns" -> is(DimValue.of(4L))),
      Map("table" -> is(DimValue.of("t")), "columns" -> oneOf(3L, 4L, 5L)),
      Map("table" -> is(DimValue.of("t"))))
    // The first tier matches nothing, the second both readings (newest first), so the third,
    // which would match both too, gets none.
    assert(history.lookup(tiers, now, policy).contains(Vector(Vector(), Vector(2.0, 4.0),
      Vector())))
    assert(store.lookupCalls.size == 1 && store.summarizeCalls.isEmpty, "one request")
    // A tier that names every dimension finds only its own reading.
    val exact = Map("table" -> is(DimValue.of("t")), "columns" -> is(DimValue.of(3L)))
    assert(history.lookup(Seq(exact), now, policy).contains(Vector(Vector(4.0))))
    // A tier may leave a dimension out; a reading it takes is not repeated in a later tier.
    val byTable = Map("table" -> is(DimValue.of("t")))
    assert(history.lookup(Seq(byTable, exact), now, policy)
      .contains(Vector(Vector(2.0, 4.0), Vector())))
    // No tier with a reading is an answer, not a failure.
    assert(history.lookup(Seq(Map("table" -> is(DimValue.of("u")))), now, policy)
      .contains(Vector(Vector())))
    assert(history.reportedReasons.isEmpty)
  }

  test("a reading that is not a usable ratio is dropped from its tier") {
    import com.nvidia.spark.history.{LookupResponse, Reading}
    val now = System.currentTimeMillis()
    val (store, history) = withTwoReadings(now)
    store.lookupRaw = Some(java.util.Collections.singletonList(LookupResponse.ok(
      java.util.Collections.singletonList(java.util.Arrays.asList(
        Reading.of(-1.0, now), Reading.of(0.0, now), Reading.of(3.0, now - 1))))))
    assert(history.lookup(Seq(Map("table" -> is(DimValue.of("t")))), now, policy)
      .contains(Vector(Vector(3.0))))
  }

  test("an error status, a malformed response or a store without lookup gives no answer") {
    import com.nvidia.spark.history.{LookupResponse, Reading, Status}
    val now = System.currentTimeMillis()
    val byTable = Seq(Map("table" -> is(DimValue.of("t"))))
    val unavailable = Status.of(Status.Code.UNAVAILABLE, "lookup is not supported")
    val cases: Seq[(String, FakeMetricStore => Unit)] = Seq(
      "lookup-DENIED" -> { s: FakeMetricStore =>
        s.summarizeError = Some(Status.of(Status.Code.DENIED, "injected")) },
      "lookup-UNAVAILABLE" -> { s: FakeMetricStore =>
        s.lookupRaw =
          Some(java.util.Collections.singletonList(LookupResponse.error(unavailable))) },
      "lookup-malformed" -> { s: FakeMetricStore =>
        s.lookupRaw = Some(java.util.Collections.emptyList[LookupResponse]()) },
      // One list for each of two tiers, for a request of one tier.
      "lookup-malformed" -> { s: FakeMetricStore =>
        s.lookupRaw = Some(java.util.Collections.singletonList(LookupResponse.ok(
          java.util.Arrays.asList(java.util.Collections.emptyList[Reading](),
            java.util.Collections.emptyList[Reading]())))) })
    cases.foreach { case (reason, breakStore) =>
      val (store, history) = withTwoReadings(now)
      breakStore(store)
      assert(history.lookup(byTable, now, policy).isEmpty, reason)
      assert(history.reportedReasons == Set(reason))
    }
  }

  test("a key that is not the family's dimensions in name and kind is not tracked") {
    val store = new FakeMetricStore
    val history = new MetricHistory(TwoDimensional, () => store)
    val now = System.currentTimeMillis()
    assert(history.declare(policy))
    history.record(Map("table" -> DimValue.of("t")), 4.0, now, policy)
    history.record(Map("table" -> DimValue.of(1L), "columns" -> DimValue.of(3L)), 4.0, now, policy)
    // A lookup may name fewer dimensions, but each one in its declared kind.
    assert(history.lookup(Seq(Map("table" -> is(DimValue.of(1L)))), now, policy).isEmpty)
    assert(store.offered.isEmpty && store.summarizeCalls.isEmpty && store.lookupCalls.isEmpty)
    assert(history.reportedReasons == Set("key-mismatch"))
  }

  test("the registered heuristics form a valid catalog") {
    // Building it throws if two registered families clash on an id or a name.
    assert(!HistoryHeuristics.catalog.entries().isEmpty)
  }
}
