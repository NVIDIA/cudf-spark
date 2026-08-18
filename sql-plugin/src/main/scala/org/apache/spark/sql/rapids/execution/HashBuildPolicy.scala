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

/** Identifies one executor-local run of a join for rent-or-buy accounting. */
case class HashBuildDemandId(joinId: Int, stageId: Int, stageAttempt: Int)

/**
 * Cost comparison for deciding when repeated on-demand builds justify constructing a reusable
 * offered build. For accumulated demand `(probeCount, probeRows)` and relative per-row probe cost
 * `q`, the alternatives are:
 *
 *   reusable = offeredRows + q * probeRows
 *   on-demand = probeRows + q * offeredRows * probeCount
 *
 * Admission occurs once the reusable alternative is no more expensive than continuing to build
 * each probe batch independently.
 */
private[execution] object HashBuildCost {
  // Calibrated from local reusable hash-join build/probe benchmarks. Hash construction is
  // normalized to 1.0 per row and q estimates probe cost relative to construction cost.
  val NumericProbeCost: Double = 0.4
  val NonNumericProbeCost: Double = 1.0

  def hasNumericKeys(keys: Seq[GpuExpression]): Boolean = keys.nonEmpty && keys.forall { key =>
    key.dataType match {
      case BooleanType | ByteType | ShortType | IntegerType | LongType | FloatType | DoubleType =>
        true
      case _ => false
    }
  }

  private def probeCost(numericKeys: Boolean): Double = {
    if (numericKeys) NumericProbeCost else NonNumericProbeCost
  }

  def admit(
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
}

/**
 * Selects the physical build side for one probe. Explicit selection modes retain their ordinary
 * choice. AUTO immediately uses an offered build that is already resident/in flight, or that is
 * the ordinary choice; a cold or evicted non-ordinary offer is admitted only after observed demand
 * satisfies [[HashBuildCost]].
 */
object HashBuildPlanner {
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
        case BuildStatus.Cold | BuildStatus.Evicted if HashBuildCost.admit(
            offeredRows, demand, numericKeys) => offeredSide
        case _ => ordinarySide
      }
    }
  }
}
