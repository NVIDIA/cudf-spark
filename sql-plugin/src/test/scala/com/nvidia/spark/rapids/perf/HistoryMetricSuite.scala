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

import com.nvidia.spark.history.{DimensionSpec, DimValue, Retention}
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

  test("a family may have several dimensions, and a lookup binds all of them") {
    val store = new FakeMetricStore
    val history = new MetricHistory(TwoDimensional, () => store)
    val now = System.currentTimeMillis()
    assert(history.declare(policy))
    history.record(key("t", 3L), 4.0, now - 1000, policy)
    history.record(key("t", 5L), 2.0, now - 1000, policy)
    assert(store.stored.size == 2)
    assert(history.latest(key("t", 3L), now, policy).contains(4.0))
    assert(history.latest(key("t", 5L), now, policy).contains(2.0))
    assert(history.latest(key("u", 3L), now, policy).isEmpty)
    assert(store.summarizeCalls.head._1.bound().size() == 2)
  }

  test("a key that is not the family's dimensions in name and kind is not tracked") {
    val store = new FakeMetricStore
    val history = new MetricHistory(TwoDimensional, () => store)
    val now = System.currentTimeMillis()
    assert(history.declare(policy))
    history.record(Map("table" -> DimValue.of("t")), 4.0, now, policy)
    history.record(Map("table" -> DimValue.of(1L), "columns" -> DimValue.of(3L)), 4.0, now, policy)
    assert(history.latest(Map("table" -> DimValue.of("t")), now, policy).isEmpty)
    assert(store.offered.isEmpty && store.summarizeCalls.isEmpty)
    assert(history.reportedReasons == Set("key-mismatch"))
  }

  test("the registered heuristics form a valid catalog") {
    // Building it throws if two registered families clash on an id or a name.
    assert(!HistoryHeuristics.catalog.entries().isEmpty)
  }
}
