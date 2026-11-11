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

import com.nvidia.spark.history.Retention
import org.scalatest.funsuite.AnyFunSuite

/** The catalog the plugin builds from its metric families. */
class HistoryMetricSuite extends AnyFunSuite {

  private def family(familyId: Int, familyName: String, familyVersion: Int = 1): HistoryMetric =
    new HistoryMetric {
      override val id: Int = familyId
      override val name: String = familyName
      override val version: Int = familyVersion
      override val dimension: String = "table"
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

  test("the registered heuristics form a valid catalog") {
    // Building it throws if two registered families clash on an id or a name.
    assert(!HistoryHeuristics.catalog.entries().isEmpty)
  }
}
