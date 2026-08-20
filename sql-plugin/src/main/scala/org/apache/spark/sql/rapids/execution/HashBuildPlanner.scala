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

package org.apache.spark.sql.rapids.execution

import com.nvidia.spark.rapids.{GpuBuildLeft, GpuBuildRight, GpuBuildSide, GpuExpression}

import org.apache.spark.sql.types.{BooleanType, ByteType, DoubleType, FloatType, IntegerType,
  LongType, ShortType}

sealed trait BuildStatus

object BuildStatus {
  case object Cold extends BuildStatus
  case object Building extends BuildStatus
  case object Ready extends BuildStatus
  case object Evicted extends BuildStatus
}

private[execution] case class BuildDemand(probeCount: Long, probeRows: Long)

private[execution] object BuildDemand {
  val Empty: BuildDemand = BuildDemand(0L, 0L)
}

/** Identifies one executor-local run of a join to track build demands. */
case class HashBuildDemandId(joinId: Int, stageId: Int, stageAttempt: Int)

/**
 * Selects the physical build side for one probe. FIXED or SMALLEST selection modes retain their
 * ordinary choice. Under AUTO, if we already have a cached build, or if the offered side is
 * the smaller side, that side is used. Otherwise, we use a heuristic described below.
 *
 * When there is no build in the cache we have two choices. Define the following:
 *
 *   B = total number of rows in the broadcast side
 *   S_i = number of rows in each stream side batch
 *   M = number of stream side batches
 *   q = normalized cost of probing one row, relative to building one row
 *
 * At the start of the join, our two choices are to 1) build B once and probe with each S_i,
 * or 2) build each S_i and probe with B. The cumulative costs for these two options are:
 *
 *   reuse = B + q * sum(S_i)
 *   onDemand = sum(S_i + q * B)
 *
 * We do not know the values of M nor sum(S_i) upfront. So instead we store accumulators for
 * M and sum(S_i) via `probeCount` and `probeRows` respectively, and use a rent-or-buy
 * heuristic. For a single batch, we compute the additional cost of renting instead of
 * using an already-built broadcast table as:
 *
 *   rent_i = onDemand_i - reuse_i
 *          = (S_i + q * B) - q * S_i
 *          = (1 - q) * S_i + q * B
 *
 * When the rent exceeds the cost of buying, i.e. sum(rent_i) >= B, we will switch from
 * doing onDemand builds to building and caching the broadcast side.
 */
object HashBuildPlanner {
  // WARNING: Magic numbers below.
  // These values are based on cuDF hash build/probe benchmarks. Assuming the cost of building
  // a hash table is 1.0 per row, q estimates the relative cost of probing one row. This assumes
  // the build and probe costs scale linearly in the number of rows. There is a difference in
  // numeric vs. non-numeric build keys because numeric keys can go through a faster primitive
  // probe path in cuDF.
  private[execution] val NumericProbeCost: Double = 0.12
  private[execution] val NonNumericProbeCost: Double = 0.4

  private[execution] def hasNumericKeys(keys: Seq[GpuExpression]): Boolean = {
    keys.nonEmpty && keys.forall { key =>
      key.dataType match {
        case BooleanType | ByteType | ShortType | IntegerType | LongType | FloatType | DoubleType =>
          true
        case _ => false
      }
    }
  }

  // Currently probeCost just depends on numericKeys since that was found to be a good
  // divider of variance, but can depend on other factors (such as distinct vs. non-distinct).
  private def probeCost(numericKeys: Boolean): Double = {
    if (numericKeys) NumericProbeCost else NonNumericProbeCost
  }

  private def admit(
      offeredRows: Long,
      demand: BuildDemand,
      numericKeys: Boolean): Boolean = {
    if (demand.probeCount == 0) {
      false
    } else {
      val q = probeCost(numericKeys)
      val storedCost = offeredRows + q * demand.probeRows
      val onDemandCost = demand.probeRows + q * offeredRows * demand.probeCount
      storedCost <= onDemandCost
    }
  }

  def select(
      selection: JoinBuildSideSelection.JoinBuildSideSelection,
      planBuildSide: GpuBuildSide,
      leftRowCount: Long,
      rightRowCount: Long,
      offeredSide: GpuBuildSide,
      status: BuildStatus,
      numericKeys: Boolean,
      demand: BuildDemand): GpuBuildSide = {
    val ordinarySide = JoinBuildSideSelection.selectPhysicalBuildSide(
      selection, planBuildSide, leftRowCount, rightRowCount)
    if (selection != JoinBuildSideSelection.AUTO || ordinarySide == offeredSide) {
      ordinarySide
    } else {
      val offeredRows = offeredSide match {
        case GpuBuildLeft => leftRowCount
        case GpuBuildRight => rightRowCount
      }
      status match {
        case BuildStatus.Ready | BuildStatus.Building => offeredSide
        case BuildStatus.Cold | BuildStatus.Evicted if admit(
            offeredRows, demand, numericKeys) => offeredSide
        case _ => ordinarySide
      }
    }
  }
}
