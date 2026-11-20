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

import scala.collection.JavaConverters._

import com.nvidia.spark.history.{DimensionSpec, HistoryMetricCatalog, MetricSchema,
  MetricVersionId, Retention}

/**
 * Contract of one history metric family at one version: id, name, dimensions and retention. No
 * store access.
 */
trait HistoryMetric {

  /**
   * The family's permanent id. Stored observations are keyed by it, so it is never reused or
   * changed, and no two families share one.
   */
  def id: Int

  /** The family's permanent name, shared by all of its versions. Used in logs. */
  def name: String

  /** Any change in meaning needs a new version. */
  def version: Int

  /**
   * The named, typed dimensions of the key, up to `MetricSchema.MAX_DIMENSIONS`; the history API
   * rejects more, or a repeated name. A key holds one value for each of them. Adding or changing
   * one is a change in meaning, so it needs a new version.
   */
  def dimensions: Seq[DimensionSpec]

  /**
   * Fixed by the first accepted declaration. Its planning age bounds lookups.
   */
  def retention: Retention

  final lazy val metric: MetricVersionId = new MetricVersionId(id, version)

  final lazy val schema: MetricSchema = new MetricSchema(metric, dimensions.asJava, retention)
}

object HistoryMetric {

  /**
   * The catalog the plugin gives its history provider: one entry per family, however many
   * versions of it are given. Throws `IllegalArgumentException` if two families share an id or a
   * name, or an id or name is invalid.
   */
  def catalogOf(metrics: Seq[HistoryMetric]): HistoryMetricCatalog =
    HistoryMetricCatalog.of(metrics.map(m => (m.id, m.name)).distinct.map {
      case (id, name) => new HistoryMetricCatalog.MetricDefinition(id, name, false)
    }.asJava)
}
