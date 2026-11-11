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

import com.nvidia.spark.rapids.{GpuBuildLeft, GpuBuildRight, GpuBuildSide}

import org.apache.spark.sql.catalyst.plans.{InnerLike, JoinType, LeftOuter, RightOuter}
import org.apache.spark.sql.types.DataType

/**
 * A request to execute one equi-hash join operation through `HashProbeBackend`.
 * `requiredBuildSide` specifies whether the requested operation has a physical side constraint,
 * e.g. left or right outer/semi/anti or a known-distinct build side. `exactCountJoinType`
 * specifies when the backend can provide an exact output count before execution, and which
 * JoinType requests that count.
 */
sealed trait BackendJoinRequest {
  def requiredBuildSide: Option[GpuBuildSide]
  def exactCountJoinType: Option[JoinType]
}

private[execution] object BackendJoinRequest {
  case object Inner extends BackendJoinRequest {
    override val requiredBuildSide: Option[GpuBuildSide] = None
    override val exactCountJoinType: Option[JoinType] =
      Some(org.apache.spark.sql.catalyst.plans.Inner)
  }

  case object LeftOuter extends BackendJoinRequest {
    override val requiredBuildSide: Option[GpuBuildSide] = Some(GpuBuildRight)
    override val exactCountJoinType: Option[JoinType] =
      Some(org.apache.spark.sql.catalyst.plans.LeftOuter)
  }

  case object RightOuter extends BackendJoinRequest {
    override val requiredBuildSide: Option[GpuBuildSide] = Some(GpuBuildLeft)
    override val exactCountJoinType: Option[JoinType] =
      Some(org.apache.spark.sql.catalyst.plans.RightOuter)
  }

  case object LeftSemi extends BackendJoinRequest {
    override val requiredBuildSide: Option[GpuBuildSide] = Some(GpuBuildRight)
    override val exactCountJoinType: Option[JoinType] = None
  }

  case object LeftAnti extends BackendJoinRequest {
    override val requiredBuildSide: Option[GpuBuildSide] = Some(GpuBuildRight)
    override val exactCountJoinType: Option[JoinType] = None
  }

  /** Request that executes using a build side whose join keys are known to be distinct. */
  sealed trait Distinct extends BackendJoinRequest {
    final override val exactCountJoinType: Option[JoinType] = None
  }

  object Distinct {
    final case class Inner(buildSide: GpuBuildSide) extends Distinct {
      override val requiredBuildSide: Option[GpuBuildSide] = Some(buildSide)
    }

    case object LeftOuter extends Distinct {
      override val requiredBuildSide: Option[GpuBuildSide] = Some(GpuBuildRight)
    }

    case object RightOuter extends Distinct {
      override val requiredBuildSide: Option[GpuBuildSide] = Some(GpuBuildLeft)
    }

    case object LeftSemi extends Distinct {
      override val requiredBuildSide: Option[GpuBuildSide] = Some(GpuBuildRight)
    }

    case object LeftAnti extends Distinct {
      override val requiredBuildSide: Option[GpuBuildSide] = Some(GpuBuildRight)
    }

    private def requestFor(
        joinType: JoinType,
        planBuildSide: GpuBuildSide): Option[Distinct] = joinType match {
      case _: InnerLike => Some(Inner(planBuildSide))
      case org.apache.spark.sql.catalyst.plans.LeftOuter => Some(LeftOuter)
      case org.apache.spark.sql.catalyst.plans.RightOuter => Some(RightOuter)
      case org.apache.spark.sql.catalyst.plans.LeftSemi => Some(LeftSemi)
      case org.apache.spark.sql.catalyst.plans.LeftAnti => Some(LeftAnti)
      case _ => None
    }

    def supports(joinType: JoinType, planBuildSide: GpuBuildSide): Boolean = {
      requestFor(joinType, planBuildSide).exists(_.requiredBuildSide.contains(planBuildSide))
    }

    /** Map a Catalyst join type and its known-distinct build side to a backend request. */
    def apply(joinType: JoinType, planBuildSide: GpuBuildSide): Distinct = {
      val request = requestFor(joinType, planBuildSide).getOrElse {
        throw new IllegalStateException(s"unsupported distinct backend join request $joinType")
      }
      require(request.requiredBuildSide.contains(planBuildSide),
        s"$joinType distinct join does not support build side $planBuildSide")
      request
    }
  }

  /** Map a directly executed Catalyst join type to its backend request. */
  def direct(joinType: JoinType): BackendJoinRequest = joinType match {
    case _: InnerLike => Inner
    case org.apache.spark.sql.catalyst.plans.LeftOuter => LeftOuter
    case org.apache.spark.sql.catalyst.plans.RightOuter => RightOuter
    case org.apache.spark.sql.catalyst.plans.LeftSemi => LeftSemi
    case org.apache.spark.sql.catalyst.plans.LeftAnti => LeftAnti
    case other => throw new IllegalStateException(s"unsupported backend join request $other")
  }
}

/**
 * Enumeration of join strategies that can be used for join operations.
 */
object JoinStrategy extends Enumeration {
  type JoinStrategy = Value
  /**
   * AUTO: Use heuristics to automatically determine the best join strategy.
   * This is the default and will evolve over time as we add more heuristics.
   */
  val AUTO = Value("AUTO")
  /**
   * INNER_HASH_WITH_POST: Force inner hash join with post-processing to convert to other
   * join types and apply join filtering. This performs an inner hash join first, then
   * applies post-processing transformations to produce the desired join type (e.g., left outer,
   * semi, anti) and applies any conditional filters.
   */
  val INNER_HASH_WITH_POST = Value("INNER_HASH_WITH_POST")
  /**
   * INNER_SORT_WITH_POST: Force inner sort-merge join with post-processing to convert to other
   * join types and apply join filtering. This performs an inner sort-merge join first, then
   * applies post-processing transformations to produce the desired join type. Falls back to
   * INNER_HASH_WITH_POST when ARRAY or STRUCT key types are present.
   */
  val INNER_SORT_WITH_POST = Value("INNER_SORT_WITH_POST")
  /**
   * HASH_ONLY: Force the use of traditional hash join only.
   */
  val HASH_ONLY = Value("HASH_ONLY")

  /**
   * Select the effective join strategy based on heuristics.
   *
   * Heuristics applied when strategy is AUTO:
   * 1. For conditional joins (with AST) when join type is INNER, LEFT_OUTER, or RIGHT_OUTER:
   *    Use INNER_HASH_WITH_POST as it provides better performance.
   * 2. For unconditional LEFT_OUTER/RIGHT_OUTER joins when build side selection is AUTO or
   *    SMALLEST:
   *    If the smaller side is NOT the fixed build side, switch to INNER_HASH_WITH_POST
   *    to enable dynamic build side selection.
   *
   * @param configuredStrategy the strategy from the config
   * @param joinType the type of join being performed
   * @param hasCondition whether the join has an AST condition
   * @param buildSideSelection the build side selection strategy
   * @param leftRowCount the row count of the left side
   * @param rightRowCount the row count of the right side
   * @return the effective strategy to use
   */
  def selectStrategy(
      configuredStrategy: JoinStrategy,
      joinType: JoinType,
      hasCondition: Boolean,
      buildSideSelection: JoinBuildSideSelection.JoinBuildSideSelection,
      leftRowCount: Long,
      rightRowCount: Long): JoinStrategy = {
    if (configuredStrategy != AUTO) {
      // If not AUTO, use the configured strategy as-is
      configuredStrategy
    } else if (hasCondition) {
      // Conditional joins with INNER, LEFT_OUTER, or RIGHT_OUTER,
      // INNER_HASH_WITH_POST provides better performance
      joinType match {
        case _: InnerLike | LeftOuter | RightOuter =>
          INNER_HASH_WITH_POST
        case _ =>
          // Fall through to dynamic build side heuristic
          selectStrategyForDynamicBuildSide(joinType, hasCondition, buildSideSelection,
            leftRowCount, rightRowCount)
      }
    } else {
      selectStrategyForDynamicBuildSide(joinType, hasCondition, buildSideSelection,
        leftRowCount, rightRowCount)
    }
  }

  /**
   * Selects a join strategy that may enable dynamic build side selection for outer joins.
   * For unconditional LEFT_OUTER/RIGHT_OUTER joins when build side selection is AUTO or SMALLEST,
   * if the smaller side is not the fixed build side, switches to INNER_HASH_WITH_POST to allow
   * the inner join to dynamically select the optimal (smaller) build side.
   */
  private def selectStrategyForDynamicBuildSide(
      joinType: JoinType,
      hasCondition: Boolean,
      buildSideSelection: JoinBuildSideSelection.JoinBuildSideSelection,
      leftRowCount: Long,
      rightRowCount: Long): JoinStrategy = {
    if (!hasCondition &&
        (buildSideSelection == JoinBuildSideSelection.AUTO ||
         buildSideSelection == JoinBuildSideSelection.SMALLEST)) {
      joinType match {
        case LeftOuter if leftRowCount < rightRowCount =>
          // For LEFT OUTER, the data movement build side must be right (GpuBuildRight).
          // But left is smaller, so we can benefit from dynamic physical build side selection.
          INNER_HASH_WITH_POST
        case RightOuter if rightRowCount < leftRowCount =>
          // For RIGHT OUTER, the data movement build side must be left (GpuBuildLeft).
          // But right is smaller, so we can benefit from dynamic physical build side selection.
          INNER_HASH_WITH_POST
        case _ =>
          // Default: use HASH_ONLY (traditional hash join)
          HASH_ONLY
      }
    } else {
      // Default: use HASH_ONLY (traditional hash join)
      HASH_ONLY
    }
  }
}

/**
 * Enumeration of build side selection strategies for join operations.
 */
object JoinBuildSideSelection extends Enumeration {
  type JoinBuildSideSelection = Value
  /**
   * AUTO: Use heuristics to automatically determine the best build side.
   * This will consider whether there is a cached build side and the observed demand.
   * Without a reusable build provider, AUTO selects the smaller side.
   */
  val AUTO = Value("AUTO")
  /**
   * FIXED: Use the build side as specified by the query plan.
   * This preserves the original Spark behavior without any dynamic selection.
   */
  val FIXED = Value("FIXED")
  /**
   * SMALLEST: Always select the side with the smallest row count as the build side.
   * This is determined on a batch-by-batch basis at join time.
   */
  val SMALLEST = Value("SMALLEST")

  /**
   * Determine the physical build side for a join operation based on the selection strategy.
   *
   * This selects which side the join algorithm will use as its build table, which may differ
   * from the data movement build side (which side was materialized/buffered/broadcast).
   *
   * @param selection the build side selection strategy (AUTO, FIXED, or SMALLEST)
   * @param planBuildSide the build side from the query plan (used for FIXED, or as a hint for
   *                      AUTO/SMALLEST strategies)
   * @param leftRowCount the row count of the left side
   * @param rightRowCount the row count of the right side
   * @return the physical build side for the join algorithm to use
   */
  def selectPhysicalBuildSide(
      selection: JoinBuildSideSelection,
      planBuildSide: GpuBuildSide,
      leftRowCount: Long,
      rightRowCount: Long): GpuBuildSide = {
    selection match {
      case FIXED => planBuildSide
      case AUTO | SMALLEST =>
        // This is the ordinary non-cache-aware choice. Providers may override AUTO afterward.
        if (leftRowCount < rightRowCount) GpuBuildLeft else GpuBuildRight
    }
  }
}

/**
 * Options to control join behavior.
 * @param strategy the join strategy to use (AUTO, INNER_HASH_WITH_POST, INNER_SORT_WITH_POST,
 *                 or HASH_ONLY)
 * @param buildSideSelection the build side selection strategy (AUTO, FIXED, or SMALLEST)
 * @param targetSize the target batch size in bytes for the join operation
 * @param logCardinalityEnabled whether to log cardinality statistics for debugging
 * @param sizeEstimateThreshold the threshold used to decide when to skip the expensive join
 *                              output size estimation (defaults to 0.75)
 */
case class JoinOptions(
    strategy: JoinStrategy.JoinStrategy,
    buildSideSelection: JoinBuildSideSelection.JoinBuildSideSelection,
    targetSize: Long,
    logCardinalityEnabled: Boolean,
    sizeEstimateThreshold: Double)

/**
 * Statistics for join cardinality logging to help diagnose performance issues.
 * @param leftRowCount number of rows on the left side
 * @param rightRowCount number of rows on the right side
 * @param leftDistinctCount distinct count of left join keys
 * @param rightDistinctCount distinct count of right join keys
 * @param leftNullCounts null counts for each left key column
 * @param rightNullCounts null counts for each right key column
 * @param leftKeyTypes data types of the left join keys
 * @param rightKeyTypes data types of the right join keys
 */
case class JoinCardinalityStats(
    leftRowCount: Long,
    rightRowCount: Long,
    leftDistinctCount: Long,
    rightDistinctCount: Long,
    leftNullCounts: Seq[Long],
    rightNullCounts: Seq[Long],
    leftKeyTypes: Seq[DataType],
    rightKeyTypes: Seq[DataType])
