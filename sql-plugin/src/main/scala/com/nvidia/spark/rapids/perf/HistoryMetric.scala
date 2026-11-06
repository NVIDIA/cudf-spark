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

import java.util.Collections

import com.nvidia.spark.history.{DimensionSpec, DimValue, HistoryMetricCatalog, MetricSchema,
  MetricVersionId, Retention}

/**
 * Contract of one governed history metric family at one version: id, single dimension and
 * retention. No store access.
 *
 * Exactly one STRING dimension.
 */
trait HistoryMetric {

  /** Must match the API's production catalog entry. */
  def id: Int

  /**
   * From the production catalog entry for `id`. Absent or retired families are named by id; the
   * store rejects their declaration.
   */
  final lazy val name: String = {
    val entry = HistoryMetricCatalog.production().find(id)
    if (entry.isPresent && !entry.get.retired()) entry.get.name() else s"uncatalogued-metric-$id"
  }

  /** Any change in meaning needs a new version. */
  def version: Int

  def dimension: String

  /**
   * Fixed by the first accepted declaration. Its planning age bounds lookups.
   */
  def retention: Retention

  final lazy val metric: MetricVersionId = new MetricVersionId(id, version)

  final lazy val schema: MetricSchema = new MetricSchema(
    metric,
    Collections.singletonList(new DimensionSpec(dimension, DimValue.Kind.STRING)),
    retention)
}
