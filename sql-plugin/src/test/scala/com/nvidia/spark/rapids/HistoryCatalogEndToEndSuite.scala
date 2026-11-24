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

package com.nvidia.spark.rapids

import java.nio.file.Files
import java.time.Duration
import java.util

import com.nvidia.spark.history._
import com.nvidia.spark.rapids.perf.{HistoryHeuristics, ScanExpansionRatio}
import org.scalatest.funsuite.AnyFunSuite

/**
 * The plugin's own catalog through the real local provider and a real SQLite file, wired as the
 * driver plugin wires them.
 */
class HistoryCatalogEndToEndSuite extends AnyFunSuite {

  private def withManager[T](catalog: HistoryMetricCatalog, path: Option[String] = None)(
      body: => T): T = {
    val configuration = new util.HashMap[String, String]()
    path.foreach(configuration.put("spark.rapids.sql.history.metrics.local.path", _))
    val manager = new HistoryMetricsManager
    try {
      manager.initialize(
        configuration, "application-1", "attempt-1", "26.12.0-test", "local", catalog)
      body
    } finally {
      manager.shutdown()
    }
  }

  private def declare(schema: MetricSchema): SchemaStatus.Code =
    MetricStores.current().declare(
      util.Collections.singletonList(schema), Duration.ofSeconds(5)).get(0).code()

  test("the plugin's metric is declared through the real local provider and other ids are not") {
    withManager(HistoryHeuristics.catalog) {
      assert(declare(ScanExpansionRatio.schema) == SchemaStatus.Code.ACCEPTED)
      val other = new MetricSchema(
        new MetricVersionId(ScanExpansionRatio.id + 1, 1),
        util.Collections.singletonList(new DimensionSpec("table", DimValue.Kind.STRING)),
        new Retention(Duration.ofDays(7), Duration.ofDays(14)))
      assert(declare(other) == SchemaStatus.Code.INVALID_REQUEST)
    }
  }

  test("a catalog that contradicts the one stored in the file leaves history off") {
    val database = Files.createTempFile("history-catalog", ".db")
    val path = Some(database.toString)
    val initial = MetricStores.current()
    try {
      withManager(HistoryHeuristics.catalog, path) {
        assert(declare(ScanExpansionRatio.schema) == SchemaStatus.Code.ACCEPTED)
      }
      val renamed = HistoryMetricCatalog.of(util.Collections.singletonList(
        new HistoryMetricCatalog.MetricDefinition(ScanExpansionRatio.id, "renamed", false)))
      withManager(renamed, path) {
        assert(MetricStores.current() eq initial, "the open fails, so the no-op store stays")
      }
      withManager(HistoryHeuristics.catalog, path) {
        assert(MetricStores.current() ne initial, "the stored catalog still opens")
      }
    } finally {
      Files.deleteIfExists(database)
    }
  }
}
