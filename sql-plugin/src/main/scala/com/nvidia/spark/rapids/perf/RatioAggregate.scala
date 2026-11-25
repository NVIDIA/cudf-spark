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

/**
 * Which readings size a scan, and how they become one ratio. The history store answers a lookup
 * with every tier's readings in the window, newest first, each reading in its closest tier; the
 * aggregate picks a tier and combines its readings.
 *
 * @param name the name in the configuration
 */
sealed abstract class RatioAggregate(val name: String) {

  /**
   * The ratio and the index of the tier it comes from, or None for no ratio (the scan keeps
   * Spark's split). By default the closest tier that has readings.
   *
   * @param tiers the readings of each tier, closest tier first, each tier newest first
   */
  final def choose(tiers: Seq[Seq[Double]]): Option[(Double, Int)] = {
    val index = tiers.indexWhere(r => r.nonEmpty && accepts(r))
    if (index < 0) None else Some((value(tiers(index)), index))
  }

  /** Whether one tier's readings, never empty, may give the ratio; by default any. */
  protected def accepts(readings: Seq[Double]): Boolean = true

  /** The ratio of one tier's readings, newest first; never empty. */
  protected def value(readings: Seq[Double]): Double
}

object RatioAggregate {

  /** The newest reading of the closest tier. */
  case object Latest extends RatioAggregate("latest") {
    override protected def value(readings: Seq[Double]): Double = readings.head
  }

  /** The mean of the closest tier's readings. */
  case object Mean extends RatioAggregate("mean") {
    override protected def value(readings: Seq[Double]): Double = mean(readings)
  }

  /** The smallest reading, which gives the largest split. */
  case object Min extends RatioAggregate("min") {
    override protected def value(readings: Seq[Double]): Double = readings.min
  }

  /** The largest reading, which gives the smallest split, the cautious choice. */
  case object Max extends RatioAggregate("max") {
    override protected def value(readings: Seq[Double]): Double = readings.max
  }

  /**
   * The mean of the closest tier whose readings agree, every one within `MaxSpread` times the
   * others; a single reading agrees. None when no tier agrees, so the scan keeps Spark's split.
   */
  case object Consistent extends RatioAggregate("consistent") {
    override protected def accepts(readings: Seq[Double]): Boolean =
      readings.max <= MaxSpread * readings.min

    override protected def value(readings: Seq[Double]): Double = mean(readings)
  }

  /** How far apart, as a factor, the readings `Consistent` averages may be. */
  val MaxSpread: Double = 3.0

  val all: Seq[RatioAggregate] = Seq(Latest, Mean, Min, Max, Consistent)

  /** Each aggregate by its name in the configuration, which only admits these names. */
  val byName: Map[String, RatioAggregate] = all.map(a => a.name -> a).toMap

  private def mean(readings: Seq[Double]): Double = readings.sum / readings.size
}
