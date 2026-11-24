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

import com.nvidia.spark.history.Summary

/**
 * Which readings of the winning tier size a scan, and how they become one ratio. The history store
 * answers a lookup with one summary of the winning tier's newest `limit` readings (count, mean,
 * minimum and maximum), so those are what can be offered.
 *
 * @param name the name in the configuration
 * @param limit how many of the newest readings of the winning tier the store summarizes; 0 for
 *              all of them in the lookup window
 */
sealed abstract class RatioAggregate(val name: String, val limit: Int) {

  /** The ratio of `summary`, the store's summary of the winning tier's readings. */
  def value(summary: Summary): Double
}

object RatioAggregate {

  /**
   * The newest reading of the winning tier. The store reads only that one observation, so the
   * lookup stays a bounded read.
   */
  case object Latest extends RatioAggregate("latest", 1) {
    override def value(summary: Summary): Double = summary.mean()
  }

  /** The mean of every reading of the winning tier in the window. */
  case object Mean extends RatioAggregate("mean", 0) {
    override def value(summary: Summary): Double = summary.mean()
  }

  /** The smallest reading, which gives the largest split. */
  case object Min extends RatioAggregate("min", 0) {
    override def value(summary: Summary): Double = summary.min()
  }

  /** The largest reading, which gives the smallest split, the cautious choice. */
  case object Max extends RatioAggregate("max", 0) {
    override def value(summary: Summary): Double = summary.max()
  }

  val all: Seq[RatioAggregate] = Seq(Latest, Mean, Min, Max)

  /** Each aggregate by its name in the configuration, which only admits these names. */
  val byName: Map[String, RatioAggregate] = all.map(a => a.name -> a).toMap
}
