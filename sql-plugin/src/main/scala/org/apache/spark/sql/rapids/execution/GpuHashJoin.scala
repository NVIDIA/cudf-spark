/*
 * Copyright (c) 2020-2026, NVIDIA CORPORATION. All rights reserved.
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

import ai.rapids.cudf.{ColumnView, DType, GatherMap, NullEquality, OutOfBoundsPolicy, Scalar, Table}
import ai.rapids.cudf.ast.CompiledExpression
import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.RmmRapidsRetryIterator.{withRestoreOnRetry, withRetryNoSplit}
import com.nvidia.spark.rapids.jni.{GpuOOM, JoinPrimitives}
import com.nvidia.spark.rapids.shims.ShimBinaryExecNode

import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Expression, NamedExpression}
import org.apache.spark.sql.catalyst.plans.{Cross, ExistenceJoin, FullOuter, Inner, InnerLike, JoinType, LeftAnti, LeftExistence, LeftOuter, LeftSemi, RightOuter}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch

object JoinTypeChecks {
  def tagForGpu(joinType: JoinType, meta: RapidsMeta[_, _, _]): Unit = {
    val conf = meta.conf
    joinType match {
      case Inner if !conf.areInnerJoinsEnabled =>
        meta.willNotWorkOnGpu("inner joins have been disabled. To enable set " +
            s"${RapidsConf.ENABLE_INNER_JOIN.key} to true")
      case Cross if !conf.areCrossJoinsEnabled =>
        meta.willNotWorkOnGpu("cross joins have been disabled. To enable set " +
            s"${RapidsConf.ENABLE_CROSS_JOIN.key} to true")
      case LeftOuter if !conf.areLeftOuterJoinsEnabled =>
        meta.willNotWorkOnGpu("left outer joins have been disabled. To enable set " +
            s"${RapidsConf.ENABLE_LEFT_OUTER_JOIN.key} to true")
      case RightOuter if !conf.areRightOuterJoinsEnabled =>
        meta.willNotWorkOnGpu("right outer joins have been disabled. To enable set " +
            s"${RapidsConf.ENABLE_RIGHT_OUTER_JOIN.key} to true")
      case FullOuter if !conf.areFullOuterJoinsEnabled =>
        meta.willNotWorkOnGpu("full outer joins have been disabled. To enable set " +
            s"${RapidsConf.ENABLE_FULL_OUTER_JOIN.key} to true")
      case LeftSemi if !conf.areLeftSemiJoinsEnabled =>
        meta.willNotWorkOnGpu("left semi joins have been disabled. To enable set " +
            s"${RapidsConf.ENABLE_LEFT_SEMI_JOIN.key} to true")
      case LeftAnti if !conf.areLeftAntiJoinsEnabled =>
        meta.willNotWorkOnGpu("left anti joins have been disabled. To enable set " +
            s"${RapidsConf.ENABLE_LEFT_ANTI_JOIN.key} to true")
      case ExistenceJoin(_) if !conf.areExistenceJoinsEnabled =>
        meta.willNotWorkOnGpu("existence joins have been disabled. To enable set " +
            s"${RapidsConf.ENABLE_EXISTENCE_JOIN.key} to true")
      case _ => // not disabled
    }
  }

  val LEFT_KEYS = "leftKeys"
  val RIGHT_KEYS = "rightKeys"
  val CONDITION = "condition"

  private[this] val cudfSupportedKeyTypes =
    (TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128 + TypeSig.STRUCT).nested()
  private[this] val sparkSupportedJoinKeyTypes = TypeSig.all - TypeSig.MAP.nested()

  private[this] val joinRideAlongTypes =
    (cudfSupportedKeyTypes + TypeSig.DECIMAL_128 + TypeSig.BINARY +
        TypeSig.ARRAY + TypeSig.MAP).nested()

  val equiJoinExecChecks: ExecChecks = ExecChecks(
    joinRideAlongTypes,
    TypeSig.all,
    Map(
      LEFT_KEYS -> InputCheck(cudfSupportedKeyTypes, sparkSupportedJoinKeyTypes),
      RIGHT_KEYS -> InputCheck(cudfSupportedKeyTypes, sparkSupportedJoinKeyTypes),
      CONDITION -> InputCheck(TypeSig.BOOLEAN, TypeSig.BOOLEAN)))

  def equiJoinMeta(leftKeys: Seq[BaseExprMeta[_]],
      rightKeys: Seq[BaseExprMeta[_]],
      condition: Option[BaseExprMeta[_]]): Map[String, Seq[BaseExprMeta[_]]] = {
    Map(
      LEFT_KEYS -> leftKeys,
      RIGHT_KEYS -> rightKeys,
      CONDITION -> condition.toSeq)
  }

  val nonEquiJoinChecks: ExecChecks = ExecChecks(
    joinRideAlongTypes,
    TypeSig.all,
    Map(CONDITION -> InputCheck(TypeSig.BOOLEAN, TypeSig.BOOLEAN,
      notes = List("A non-inner join only is supported if the condition expression can be " +
          "converted to a GPU AST expression"))))

  def nonEquiJoinMeta(condition: Option[BaseExprMeta[_]]): Map[String, Seq[BaseExprMeta[_]]] =
    Map(CONDITION -> condition.toSeq)
}

object GatherMapsResult {
  def apply(left: GatherMap, right: GatherMap): GatherMapsResult =
    new GatherMapsResult(Some(left), Some(right))

  def makeFromLeft(left: GatherMap): GatherMapsResult =
    new GatherMapsResult(Some(left), None)

  def makeFromRight(right: GatherMap): GatherMapsResult =
    new GatherMapsResult(None, Some(right))
}

/**
 * Holds the result of a left and right gather map
 */
class GatherMapsResult(private var leftOpt: Option[GatherMap],
                       private var rightOpt: Option[GatherMap]) extends AutoCloseable {

  def left: GatherMap = leftOpt.get
  def hasLeft: Boolean = leftOpt.isDefined
  def releaseLeft(): GatherMap = {
    val ret = leftOpt.get
    leftOpt = None
    ret
  }

  def right: GatherMap = rightOpt.get
  def hasRight: Boolean = rightOpt.isDefined
  def releaseRight(): GatherMap = {
    val ret = rightOpt.get
    rightOpt = None
    ret
  }

  override def close(): Unit = {
    leftOpt.foreach(_.close)
    rightOpt.foreach(_.close)
  }
}

object JoinImpl {
  /**
   * Filter the results of an inner join using a compiled AST condition. This requires that the
   * AST condition is compiled for left table, then right table.
   * @param inner the results of an inner join
   * @param leftTable the left table columns used by the AST.
   * @param rightTable the right table columns used by the AST.
   * @param compiledCondition the compiled AST predicate
   * @return the gather maps filtered by teh AST expression.
   */
  def filterInnerJoinWithAST(inner: GatherMapsResult,
                             leftTable: Table,
                             rightTable: Table,
                             compiledCondition: CompiledExpression): GatherMapsResult = {
    val arrayRet = JoinPrimitives.filterGatherMapsByAST(
      inner.left, inner.right, leftTable, rightTable, compiledCondition)
    GatherMapsResult(arrayRet(0), arrayRet(1))
  }

  /**
   * Filter the results of an inner join using a compiled AST condition. This requires that the
   * AST condition is compiled for right table, then left table (swapped from the expected way).
   * @param inner the results of an inner join
   * @param leftTable the left table columns used by the AST.
   * @param rightTable the right table columns used by the AST.
   * @param compiledCondition the compiled AST predicate
   * @return the gather maps filtered by teh AST expression.
   */
  def filterInnerJoinWithSwappedAST(inner: GatherMapsResult,
                                    leftTable: Table,
                                    rightTable: Table,
                                    compiledCondition: CompiledExpression): GatherMapsResult = {
    // The compiled condition has the left and right tables swapped, but we cannot change
    // that, so swap the input tables, then swap back the outputs to properly match
    val arrayRet = JoinPrimitives.filterGatherMapsByAST(
      inner.right, inner.left, rightTable, leftTable, compiledCondition)
    GatherMapsResult(arrayRet(1), arrayRet(0))
  }

  /**
   * Applies the AST filter to the inner gather maps result.
   * Detects how the AST is compiled based on earlier rules.
   * This is fragile and needs to be used with caution. This is
   * not the same for all paths through the join code.
   * @param inner the set of maps to filter
   * @param leftTable the left AST related columns
   * @param rightTable the right AST related columns
   * @param compiledCondition the condition
   * @param joinType the type of join being done
   * @param buildSide the build side of the join
   * @return a new set of filtered gather maps
   */
  def filterInnerJoinWithASTSwapByJoinTypeAndBuildSide(
      inner: GatherMapsResult,
      leftTable: Table,
      rightTable: Table,
      compiledCondition: CompiledExpression,
      joinType: JoinType,
      buildSide: GpuBuildSide): GatherMapsResult = {
    // For the AST to work properly we have to match the rules that the condition was compiled for
    // InnerLike && GpuBuildRight | LeftOuter | LeftSemi | LeftAnti =>
    //   LEFT, RIGHT
    // InnerLike && GpuBuildLeft | RightOuter=>
    //   RIGHT, LEFT
    val conditionCompiledRightLeft = joinType match {
      case _: InnerLike if buildSide == GpuBuildLeft => true
      case RightOuter => true
      case _ => false
    }
    if (conditionCompiledRightLeft) {
      filterInnerJoinWithSwappedAST(inner, leftTable, rightTable, compiledCondition)
    } else {
      filterInnerJoinWithAST(inner, leftTable, rightTable, compiledCondition)
    }
  }

  /**
   * Applies the AST filter to the inner gather maps result.
   * Detects how the AST is compiled based on earlier rules.
   * This is fragile and needs to be used with caution. This is
   * not the same for all paths through the join code.
   * @param inner the set of maps to filter
   * @param leftTable the left AST related columns
   * @param rightTable the right AST related columns
   * @param compiledCondition the condition
   * @param subJoinType the type of join being done
   * @return a new set of filtered gather maps
   */
  def filterInnerJoinWithASTSwapBySubJoinType(inner: GatherMapsResult,
                                              leftTable: Table,
                                              rightTable: Table,
                                              compiledCondition: CompiledExpression,
                                              subJoinType: JoinType) = {
    // For AST to work we have to match the order of the left and right tables
    // that it was compiled for
    // In this case it is
    // subJoinType == LeftOuter | Inner => LEFT, RIGHT
    // subJoinType == RightOuter => RIGHT, LEFT
    // But that is not consistent everywhere
    if (subJoinType == RightOuter) {
      filterInnerJoinWithSwappedAST(inner, leftTable, rightTable, compiledCondition)
    } else {
      filterInnerJoinWithAST(inner, leftTable, rightTable, compiledCondition)
    }
  }

  /**
   * Convert the results of an inner join to a left outer join
   * @param inner the inner join results
   * @param leftRowCount the number of rows in the left hand table
   * @param rightRowCount the number of rows in the right hand table
   * @return a gather maps result for a left outer join
   */
  def makeLeftOuter(inner: GatherMapsResult,
                    leftRowCount: Int, rightRowCount: Int): GatherMapsResult = {
    val arrayRet = JoinPrimitives.makeLeftOuter(
      inner.left, inner.right, leftRowCount, rightRowCount)
    GatherMapsResult(arrayRet(0), arrayRet(1))
  }

  /**
   * Convert the results of an inner join to a right outer join
   * @param inner the inner join results
   * @param leftRowCount the number of rows in the left hand table
   * @param rightRowCount the number of rows in the right hand table
   * @return a gather maps result for a right outer join
   */
  def makeRightOuter(inner: GatherMapsResult,
                     leftRowCount: Int, rightRowCount: Int): GatherMapsResult = {
    // Switching left and right to allow us to use makeLeftOuter as a stand in for
    // makeRightOuter
    val arrayRet = JoinPrimitives.makeLeftOuter(
      inner.right, inner.left, rightRowCount, leftRowCount)
    // Then switch the results back so we are good
    GatherMapsResult(arrayRet(1), arrayRet(0))
  }

  /**
   * Convert the results of an inner join to a left semi join
   * @param inner the inner join results
   * @param leftRowCount the number of rows in the left hand table
   * @return a gather maps result for a left semi join
   */
  def makeLeftSemi(inner: GatherMapsResult, leftRowCount: Int): GatherMapsResult = {
    val leftRet = JoinPrimitives.makeSemi(inner.left, leftRowCount)
    GatherMapsResult.makeFromLeft(leftRet)
  }

  /**
   * Convert the results of an inner join to a left anti join
   * @param inner the inner join results
   * @param leftRowCount the number of rows in the left hand table
   * @return a gather maps result for a left anti join
   */
  def makeLeftAnti(inner: GatherMapsResult, leftRowCount: Int): GatherMapsResult = {
    val leftRet = JoinPrimitives.makeAnti(inner.left, leftRowCount)
    GatherMapsResult.makeFromLeft(leftRet)
  }

  /**
   * Do an inner hash join with the build table as the left table.
   * @param leftKeys the left equality join keys
   * @param rightKeys the right equality join keys
   * @param compareNullsEqual true if nulls should be compared as equal
   * @return the gather maps to use
   */
  def innerHashJoinBuildLeft(leftKeys: Table, rightKeys: Table,
                             compareNullsEqual: Boolean): GatherMapsResult = {
    // The build table is always the right table, so switch the tables passed in
    val arrayRet = JoinPrimitives.hashInnerJoin(rightKeys, leftKeys, compareNullsEqual)
    // Then switch the gather maps result
    GatherMapsResult(arrayRet(1), arrayRet(0))
  }

  /**
   * Do an inner hash join with the build table as the left table. Note that the
   * AST complied expression must also be compiled for right table then left table.
   * @param leftKeys the left equality join keys
   * @param rightKeys the right equality join keys
   * @param leftTable the left AST related columns
   * @param rightTable the right AST related columns
   * @param compiledCondition the condition
   * @param nullEquality if nulls should eb equal or not
   * @return the gather maps to use
   */
  def innerHashJoinBuildLeft(leftKeys: Table, rightKeys: Table,
                              leftTable: Table, rightTable: Table,
                              compiledCondition: CompiledExpression,
                              nullEquality: NullEquality): GatherMapsResult = {
    // Even though it's an inner join, we need to switch the join order since the
    // condition has been compiled to expect the build side on the left and the stream
    // side on the right.
    val arrayRet = Table.mixedInnerJoinGatherMaps(rightKeys, leftKeys, rightTable, leftTable,
      compiledCondition, nullEquality)
    // Reverse the output of the join, because we expect the right gather map to
    // always be on the right.
    GatherMapsResult(arrayRet(1), arrayRet(0))
  }

  /**
   * Do an inner hash join with the build table as the right table.
   * @param leftKeys the left equality join keys
   * @param rightKeys the right equality join keys
   * @param compareNullsEqual true if nulls should be compared as equal
   * @return the gather maps to use
   */
  def innerHashJoinBuildRight(leftKeys: Table, rightKeys: Table,
                              compareNullsEqual: Boolean): GatherMapsResult = {
    // The build table is always the right table
    val arrayRet = JoinPrimitives.hashInnerJoin(leftKeys, rightKeys, compareNullsEqual)
    GatherMapsResult(arrayRet(0), arrayRet(1))
  }

  /**
   * Do an inner hash join with the build table as the right table. Note that the
   * AST complied expression must also be compiled for left table then right table.
   * @param leftKeys the left equality join keys
   * @param rightKeys the right equality join keys
   * @param leftTable the left AST related columns
   * @param rightTable the right AST related columns
   * @param compiledCondition the condition
   * @param nullEquality if nulls should eb equal or not
   * @return the gather maps to use
   */
  def innerHashJoinBuildRight(leftKeys: Table, rightKeys: Table,
                              leftTable: Table, rightTable: Table,
                              compiledCondition: CompiledExpression,
                              nullEquality: NullEquality): GatherMapsResult = {
    val arrayRet = Table.mixedInnerJoinGatherMaps(leftKeys, rightKeys, leftTable, rightTable,
      compiledCondition, nullEquality)
    GatherMapsResult(arrayRet(0), arrayRet(1))
  }

  /**
   * Do an inner hash join with build side selection based on the config.
   *
   * @param leftKeys the left equality join keys
   * @param rightKeys the right equality join keys
   * @param compareNullsEqual true if nulls should be compared as equal
   * @param buildSideSelection the build side selection strategy (AUTO, FIXED, or SMALLEST)
   * @param suggestedBuildSide the suggested build side from the query plan. This is only used
   *                           directly when buildSideSelection is FIXED. For AUTO or SMALLEST
   *                           strategies, this serves as a hint but the actual physical build side
   *                           will be determined dynamically based on row counts. The returned
   *                           gather maps maintain consistent left/right semantics regardless of
   *                           which physical build side is chosen.
   * @return the gather maps to use
   */
  def innerHashJoin(leftKeys: Table, rightKeys: Table,
                    compareNullsEqual: Boolean,
                    buildSideSelection: JoinBuildSideSelection.JoinBuildSideSelection,
                    suggestedBuildSide: GpuBuildSide): GatherMapsResult = {
    // Select the physical build side for the join algorithm based on the strategy.
    // This may differ from suggestedBuildSide when using AUTO or SMALLEST strategies.
    val selectedBuildSide = JoinBuildSideSelection.selectPhysicalBuildSide(
      buildSideSelection, suggestedBuildSide, leftKeys.getRowCount, rightKeys.getRowCount)
    selectedBuildSide match {
      case GpuBuildLeft => innerHashJoinBuildLeft(leftKeys, rightKeys, compareNullsEqual)
      case GpuBuildRight => innerHashJoinBuildRight(leftKeys, rightKeys, compareNullsEqual)
    }
  }

  /**
   * Do an inner sort join with the build table as the left table.
   * @param leftKeys the left equality join keys
   * @param rightKeys the right equality join keys
   * @param compareNullsEqual true if nulls should be compared as equal
   * @return the gather maps to use
   */
  def innerSortJoinBuildLeft(leftKeys: Table, rightKeys: Table,
                             compareNullsEqual: Boolean): GatherMapsResult = {
    // The build table is always the right table, so switch the tables passed in
    val arrayRet = JoinPrimitives.sortMergeInnerJoin(rightKeys, leftKeys,
      false, false, compareNullsEqual)
    // Then switch the gather maps result
    GatherMapsResult(arrayRet(1), arrayRet(0))
  }

  /**
   * Do an inner sort join with the build table as the right table.
   * @param leftKeys the left equality join keys
   * @param rightKeys the right equality join keys
   * @param compareNullsEqual true if nulls should be compared as equal
   * @return the gather maps to use
   */
  def innerSortJoinBuildRight(leftKeys: Table, rightKeys: Table,
                              compareNullsEqual: Boolean): GatherMapsResult = {
    // The build table is always the right table
    val arrayRet = JoinPrimitives.sortMergeInnerJoin(leftKeys, rightKeys,
      false, false, compareNullsEqual)
    GatherMapsResult(arrayRet(0), arrayRet(1))
  }

  /**
   * Do an inner sort join with build side selection based on the config.
   *
   * @param leftKeys the left equality join keys
   * @param rightKeys the right equality join keys
   * @param compareNullsEqual true if nulls should be compared as equal
   * @param buildSideSelection the build side selection strategy (AUTO, FIXED, or SMALLEST)
   * @param suggestedBuildSide the suggested build side from the query plan. This is only used
   *                           directly when buildSideSelection is FIXED. For AUTO or SMALLEST
   *                           strategies, this serves as a hint but the actual physical build side
   *                           will be determined dynamically based on row counts. The returned
   *                           gather maps maintain consistent left/right semantics regardless of
   *                           which physical build side is chosen.
   * @return the gather maps to use
   */
  def innerSortJoin(leftKeys: Table, rightKeys: Table,
                    compareNullsEqual: Boolean,
                    buildSideSelection: JoinBuildSideSelection.JoinBuildSideSelection,
                    suggestedBuildSide: GpuBuildSide): GatherMapsResult = {
    // Select the physical build side for the join algorithm based on the strategy.
    // This may differ from suggestedBuildSide when using AUTO or SMALLEST strategies.
    val selectedBuildSide = JoinBuildSideSelection.selectPhysicalBuildSide(
      buildSideSelection, suggestedBuildSide, leftKeys.getRowCount, rightKeys.getRowCount)
    selectedBuildSide match {
      case GpuBuildLeft => innerSortJoinBuildLeft(leftKeys, rightKeys, compareNullsEqual)
      case GpuBuildRight => innerSortJoinBuildRight(leftKeys, rightKeys, compareNullsEqual)
    }
  }

  /**
   * Do a left outer hash join with the build table as the right table.
   * @param leftKeys the left equality join keys
   * @param rightKeys the right equality join keys
   * @param compareNullsEqual true if nulls should be compared as equal
   * @return the gather maps to use
   */
  def leftOuterHashJoinBuildRight(leftKeys: Table, rightKeys: Table,
                                  compareNullsEqual: Boolean): GatherMapsResult = {
    val arrayRet = leftKeys.leftJoinGatherMaps(rightKeys, compareNullsEqual)
    GatherMapsResult(arrayRet(0), arrayRet(1))
  }

  /**
   * Do a left outer hash join with the build table as the right table. Note that the
   * AST complied expression must also be compiled for left table then right table.
   * @param leftKeys the left equality join keys
   * @param rightKeys the right equality join keys
   * @param leftTable the left AST related columns
   * @param rightTable the right AST related columns
   * @param compiledCondition the condition
   * @param nullEquality if nulls should eb equal or not
   * @return the gather maps to use
   */
  def leftOuterHashJoinBuildRight(leftKeys: Table, rightKeys: Table,
                                  leftTable: Table, rightTable: Table,
                                  compiledCondition: CompiledExpression,
                                  nullEquality: NullEquality): GatherMapsResult = {
    val arrayRet = Table.mixedLeftJoinGatherMaps(leftKeys, rightKeys, leftTable, rightTable,
      compiledCondition, nullEquality)
    GatherMapsResult(arrayRet(0), arrayRet(1))
  }

  /**
   * Do a right outer hash join with the build table as the left table.
   * @param leftKeys the left equality join keys
   * @param rightKeys the right equality join keys
   * @param compareNullsEqual true if nulls should be compared as equal
   * @return the gather maps to use
   */
  def rightOuterHashJoinBuildLeft(leftKeys: Table, rightKeys: Table,
                                  compareNullsEqual: Boolean): GatherMapsResult = {
    // Reverse the input to the join so we can use left outer to computer right outer
    val arrayRet = rightKeys.leftJoinGatherMaps(leftKeys, compareNullsEqual)
    // Then switch the output so the gather maps are correct
    GatherMapsResult(arrayRet(1), arrayRet(0))
  }

  /**
   * Do a right outer hash join with the build table as the left table. Note that the
   * AST complied expression must also be compiled for right table then left table.
   * @param leftKeys the left equality join keys
   * @param rightKeys the right equality join keys
   * @param leftTable the left AST related columns
   * @param rightTable the right AST related columns
   * @param compiledCondition the condition
   * @param nullEquality if nulls should eb equal or not
   * @return the gather maps to use
   */
  def rightOuterHashJoinBuildLeft(leftKeys: Table, rightKeys: Table,
                                  leftTable: Table, rightTable: Table,
                                  compiledCondition: CompiledExpression,
                                  nullEquality: NullEquality): GatherMapsResult = {
    // Reverse the output of the join, because we expect the right gather map to
    // always be on the right
    val arrayRet = Table.mixedLeftJoinGatherMaps(rightKeys, leftKeys, rightTable, leftTable,
      compiledCondition, nullEquality)
    GatherMapsResult(arrayRet(1), arrayRet(0))
  }

  /**
   * Do a left semi hash join with the build table as the right table.
   * @param leftKeys the left equality join keys
   * @param rightKeys the right equality join keys
   * @param compareNullsEqual true if nulls should be compared as equal
   * @return the gather map to use for the left table
   */
  def leftSemiHashJoinBuildRight(leftKeys: Table, rightKeys: Table,
                                  compareNullsEqual: Boolean): GatherMapsResult = {
    val leftRet = leftKeys.leftSemiJoinGatherMap(rightKeys, compareNullsEqual)
    GatherMapsResult.makeFromLeft(leftRet)
  }

  /**
   * Do a left semi hash join with the build table as the right table. Note that the
   * AST complied expression must also be compiled for left table then right table.
   * @param leftKeys the left equality join keys
   * @param rightKeys the right equality join keys
   * @param leftTable the left AST related columns
   * @param rightTable the right AST related columns
   * @param compiledCondition the condition
   * @param nullEquality if nulls should eb equal or not
   * @return the gather map to use for the left table
   */
  def leftSemiHashJoinBuildRight(leftKeys: Table, rightKeys: Table,
                                 leftTable: Table, rightTable: Table,
                                 compiledCondition: CompiledExpression,
                                 nullEquality: NullEquality): GatherMapsResult = {
    val leftRet = Table.mixedLeftSemiJoinGatherMap(leftKeys, rightKeys, leftTable, rightTable,
      compiledCondition, nullEquality)
    GatherMapsResult.makeFromLeft(leftRet)
  }

  /**
   * Do a left anti hash join with the build table as the right table.
   * @param leftKeys the left equality join keys
   * @param rightKeys the right equality join keys
   * @param compareNullsEqual true if nulls should be compared as equal
   * @return the gather map to use for the left table
   */
  def leftAntiHashJoinBuildRight(leftKeys: Table, rightKeys: Table,
                                 compareNullsEqual: Boolean): GatherMapsResult = {
    val leftRet = leftKeys.leftAntiJoinGatherMap(rightKeys, compareNullsEqual)
    GatherMapsResult.makeFromLeft(leftRet)
  }

  /**
   * Do a left anti hash join with the build table as the right table. Note that the
   * AST complied expression must also be compiled for left table then right table.
   * @param leftKeys the left equality join keys
   * @param rightKeys the right equality join keys
   * @param leftTable the left AST related columns
   * @param rightTable the right AST related columns
   * @param compiledCondition the condition
   * @param nullEquality if nulls should eb equal or not
   * @return the gather map to use for the left table
   */
  def leftAntiHashJoinBuildRight(leftKeys: Table, rightKeys: Table,
                                 leftTable: Table, rightTable: Table,
                                 compiledCondition: CompiledExpression,
                                 nullEquality: NullEquality): GatherMapsResult = {
    val leftRet = Table.mixedLeftAntiJoinGatherMap(leftKeys, rightKeys, leftTable, rightTable,
      compiledCondition, nullEquality)
    GatherMapsResult.makeFromLeft(leftRet)
  }
}

object GpuHashJoin {
  /**
   * Captures the normalized join condition and child plans after extracting any non-AST
   * expressions that can be safely computed on one side of the join.
   *
   * `joinCondition` is evaluated inside the hash join. `filterCondition` is the original
   * condition when it cannot be rewritten for in-join AST evaluation. `left` and `right`
   * may include projection wrappers that compute extracted expressions before the join.
   * `finalProjectList` removes those temporary projected columns from the join output.
   */
  case class ExtractedJoinCondition(
      joinCondition: Option[Expression],
      filterCondition: Option[Expression],
      left: SparkPlan,
      right: SparkPlan,
      finalProjectList: Option[List[NamedExpression]]) {
    def projectIfNeeded(joinExec: GpuExec): GpuExec = {
      finalProjectList.map(GpuProjectExec(_, joinExec)).getOrElse(joinExec)
    }
  }

  /**
   * Computes the visible output attributes for a hash join, including the nullability changes
   * required by outer joins and the additional marker column produced by existence joins.
   *
   * This is shared by join exec nodes and by the final projection used after condition
   * extraction so that temporary projection columns do not leak into the user-visible schema.
   */
  def output(joinType: JoinType, left: Seq[Attribute], right: Seq[Attribute]): Seq[Attribute] = {
    joinType match {
      case _: InnerLike =>
        left ++ right
      case LeftOuter =>
        left ++ right.map(_.withNullability(true))
      case RightOuter =>
        left.map(_.withNullability(true)) ++ right
      case j: ExistenceJoin =>
        left :+ j.exists
      case LeftExistence(_) =>
        left
      case FullOuter =>
        left.map(_.withNullability(true)) ++ right.map(_.withNullability(true))
      case x =>
        throw new IllegalArgumentException(s"GpuHashJoin should not take $x as the JoinType")
    }
  }

  private def canConditionBeRewrittenAsAst(
      conditionMeta: Option[BaseExprMeta[_]],
      left: Seq[Attribute],
      right: Seq[Attribute]): Boolean = {
    conditionMeta.forall { condition =>
      AstUtil.canExtractNonAstConditionIfNeed(
        condition, left.map(_.exprId), right.map(_.exprId))
    }
  }

  private def canConditionBeRewrittenAsAst(
      meta: SparkPlanMeta[_],
      conditionMeta: Option[BaseExprMeta[_]]): Boolean = {
    val Seq(left, right) = meta.childPlans
    canConditionBeRewrittenAsAst(conditionMeta, left.outputAttributes, right.outputAttributes)
  }

  def extractJoinConditionIfNeeded(
      conditionMeta: Option[BaseExprMeta[_]],
      joinType: JoinType,
      left: SparkPlan,
      right: SparkPlan): ExtractedJoinCondition = {
    val condition = conditionMeta.map(_.convertToGpu())
    if (conditionMeta.forall(_.canThisBeAst)) {
      ExtractedJoinCondition(condition, None, left, right, None)
    } else if (canConditionBeRewrittenAsAst(conditionMeta, left.output, right.output)) {
      val (remains, leftExpr, rightExpr) =
        AstUtil.extractNonAstFromJoinCond(conditionMeta, left.output, right.output)
      val leftChild =
        if (leftExpr.nonEmpty) GpuProjectExec(leftExpr ++ left.output.toList, left) else left
      val rightChild =
        if (rightExpr.nonEmpty) GpuProjectExec(rightExpr ++ right.output.toList, right) else right
      val finalProjectList = if (leftExpr.isEmpty && rightExpr.isEmpty) {
        None
      } else {
        Some(output(joinType, left.output, right.output).toList)
      }
      ExtractedJoinCondition(remains, None, leftChild, rightChild, finalProjectList)
    } else {
      ExtractedJoinCondition(None, condition, left, right, None)
    }
  }

  // Designed for the null-aware anti-join in GpuBroadcastHashJoin.
  def anyNullInKey(cb: ColumnarBatch, boundKeys: Seq[GpuExpression]): Boolean = {
    withResource(GpuProjectExec.project(cb, boundKeys)) { keysCb =>
      (0 until keysCb.numCols()).exists(keysCb.column(_).hasNull)
    }
  }

  // For the join on struct, it's equal for nulls in child columns of struct column, it's not
  // equal for the root struct when meets both nulls.
  // So for join types other than FullOuter and join on struct column with nullable child,
  // we simply set compareNullsEqual as true, and Spark plan already filter out the nulls for root
  // struct column.
  // For details, see https://github.com/NVIDIA/spark-rapids/issues/2126.
  // For Non-nested keys, it is also correctly processed, because compareNullsEqual will be set to
  // false which is the semantic of join: null != null when join
  def compareNullsEqual(
      joinType: JoinType,
      buildKeys: Seq[Expression]): Boolean = (joinType != FullOuter) &&
    GpuHashJoin.anyNullableStructChild(buildKeys)

  // For full outer and outer joins with build side matching outer side, we need to keep the
  // nulls in the build table and compare nulls as equal.
  // Note: for outer joins with build side matching outer side and join on struct column with child
  // is nullable, MUST not filter out null records from build table.
  def buildSideNeedsNullFilter(
      joinType: JoinType,
      compareNullsEqual: Boolean, // from function: compareNullsEqual
      buildSide: GpuBuildSide,
      buildKeys:Seq[Expression]): Boolean = {
    val needFilterOutNull = joinType match {
      case FullOuter => false
      case LeftOuter if buildSide == GpuBuildLeft => false
      case RightOuter if buildSide == GpuBuildRight => false
      case _ => true
    }
    needFilterOutNull && compareNullsEqual && buildKeys.exists(_.nullable)
  }

  def tagJoin(
      meta: SparkPlanMeta[_],
      joinType: JoinType,
      buildSide: GpuBuildSide,
      leftKeys: Seq[Expression],
      rightKeys: Seq[Expression],
      conditionMeta: Option[BaseExprMeta[_]]): Unit = {
    val keyDataTypes = (leftKeys ++ rightKeys).map(_.dataType)

    JoinTypeChecks.tagForGpu(joinType, meta)

    // Check for struct keys with different field names.
    // Spark 4.0+ (SPARK-51738) allows struct comparisons where field names differ,
    // but the GPU implementation currently requires matching field names.
    // Fall back to CPU for this case until proper GPU support is added.
    // See https://github.com/NVIDIA/spark-rapids/issues/13100
    if (!leftKeys.zip(rightKeys).forall { case (l, r) =>
        l.dataType.sameType(r.dataType) }) {
      meta.willNotWorkOnGpu("join keys have different types or struct field names differ, " +
        "which is not yet supported on GPU")
    }

    val canConditionBeRewrittenAsAst = GpuHashJoin.canConditionBeRewrittenAsAst(
      meta, conditionMeta)

    joinType match {
      case _: InnerLike =>
      case RightOuter | LeftOuter | LeftSemi | LeftAnti | ExistenceJoin(_) =>
        if (!canConditionBeRewrittenAsAst) {
          conditionMeta.foreach(meta.requireAstForGpuOn)
        }
      case FullOuter =>
        if (!canConditionBeRewrittenAsAst) {
          conditionMeta.foreach(meta.requireAstForGpuOn)
        }
        // FullOuter join cannot support with struct keys as two issues below
        //  * https://github.com/NVIDIA/spark-rapids/issues/2126
        //  * https://github.com/rapidsai/cudf/issues/7947
        if (keyDataTypes.exists(_.isInstanceOf[StructType])) {
          meta.willNotWorkOnGpu(s"$joinType joins currently do not support with struct keys")
        }
      case _ =>
        meta.willNotWorkOnGpu(s"$joinType currently is not supported")
    }
  }

  // This is used by sort join and broadcast join
  def tagBuildSide(meta: SparkPlanMeta[_], joinType: JoinType, buildSide: GpuBuildSide): Unit = {
    buildSide match {
      case GpuBuildLeft if !canBuildLeft(joinType) =>
        meta.willNotWorkOnGpu(s"$joinType does not support left-side build")
      case GpuBuildRight if !canBuildRight(joinType) =>
        meta.willNotWorkOnGpu(s"$joinType does not support right-side build")
      case _ =>
    }
  }

  /** Determine if this type of join supports using the right side of the join as the build side. */
  def canBuildRight(joinType: JoinType): Boolean = joinType match {
    case _: InnerLike | LeftOuter | LeftSemi | LeftAnti | FullOuter | _: ExistenceJoin => true
    case _ => false
  }

  /** Determine if this type of join supports using the left side of the join as the build side. */
  def canBuildLeft(joinType: JoinType): Boolean = joinType match {
    case _: InnerLike | RightOuter | FullOuter => true
    case _ => false
  }

  def extractTopLevelAttributes(
      exprs: Seq[Expression],
      includeAlias: Boolean): Seq[Option[Attribute]] =
    exprs.map {
      case a: AttributeReference => Some(a.toAttribute)
      case GpuAlias(a: AttributeReference, _) if includeAlias => Some(a.toAttribute)
      case _ => None
    }

  /**
   * Filter rows from the batch where any of the keys are null.
   */
  def filterNullsWithRetryAndClose(
      sb: SpillableColumnarBatch,
      boundKeys: Seq[Expression]): ColumnarBatch = {
    withRetryNoSplit(sb) { _ =>
      withResource(sb.getColumnarBatch()) { cb =>
        filterNulls(cb, boundKeys)
      }
    }
  }

  private def filterNulls(cb: ColumnarBatch, boundKeys: Seq[Expression]): ColumnarBatch = {
    var mask: ai.rapids.cudf.ColumnVector = null
    try {
      withResource(GpuProjectExec.project(cb, boundKeys)) { keys =>
        val keyColumns = GpuColumnVector.extractBases(keys)
        keyColumns.foreach { column =>
          if (column.hasNulls) {
            withResource(column.isNotNull) { nn =>
              if (mask == null) {
                mask = nn.incRefCount()
              } else {
                mask = withResource(mask) { _ =>
                  mask.and(nn)
                }
              }
            }
          }
        }
      }

      if (mask == null) {
        // There was nothing to filter.
        GpuColumnVector.incRefCounts(cb)
      } else {
        val colTypes = GpuColumnVector.extractTypes(cb)
        withResource(GpuColumnVector.from(cb)) { tbl =>
          withResource(tbl.filter(mask)) { filtered =>
            GpuColumnVector.from(filtered, colTypes)
          }
        }
      }
    } finally {
      if (mask != null) {
        mask.close()
      }
    }
  }

  /**
   * Given sequence of expressions, detect whether there exists any StructType expressions
   * who contains nullable child columns.
   * Since cuDF can not match nullable children as Spark during join, we detect them before join
   * to apply some walking around strategies. For some details, please refer the issue:
   * https://github.com/NVIDIA/spark-rapids/issues/2126.
   *
   * NOTE that this does not work for arrays of Structs or Maps that are not supported as join keys
   * yet.
   */
  def anyNullableStructChild(expressions: Seq[Expression]): Boolean = {
    def anyNullableChild(struct: StructType): Boolean = {
      struct.fields.exists { field =>
        if (field.nullable) {
          true
        } else field.dataType match {
          case structType: StructType =>
            anyNullableChild(structType)
          case _ => false
        }
      }
    }

    expressions.map(_.dataType).exists {
      case st: StructType =>
        anyNullableChild(st)
      case _ => false
    }
  }

  // scalastyle:off line.size.limit
  /**
   * The function is copied from Spark 3.2:
   *   https://github.com/apache/spark/blob/v3.2.2/sql/core/src/main/scala/org/apache/spark/sql/execution/joins/HashJoin.scala#L709-L713
   *
   * Returns whether the keys can be rewritten as a packed long. If
   * they can, we can assume that they are packed when we extract them out.
   */
  // scalastyle:on
  def canRewriteAsLongType(keys: Seq[Expression]): Boolean = {
    // TODO: support BooleanType, DateType and TimestampType
    keys.forall(_.dataType.isInstanceOf[IntegralType]) &&
      keys.map(_.dataType.defaultSize).sum <= 8
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
   * Currently behaves the same as SMALLEST, but the heuristics may change in the future
   * to consider factors beyond just row count.
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
        // Both AUTO and SMALLEST currently select the smaller side
        // AUTO may evolve to use additional heuristics in the future
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

/**
 * Class to hold statistics on the build-side batch of a hash join.
 * @param streamMagnificationFactor estimated magnification of a stream batch during join
 * @param isDistinct true if all build-side join keys are distinct
 */
case class JoinBuildSideStats(streamMagnificationFactor: Double, isDistinct: Boolean)

object JoinBuildSideStats {
  def fromBatch(batch: ColumnarBatch,
                boundBuildKeys: Seq[GpuExpression]): JoinBuildSideStats = {
    // This is okay because the build keys must be deterministic
    withResource(GpuProjectExec.project(batch, boundBuildKeys)) { buildKeys =>
      // Based off of the keys on the build side guess at how many output rows there
      // will be for each input row on the stream side. This does not take into account
      // the join type, data skew or even if the keys actually match.
      withResource(GpuColumnVector.from(buildKeys)) { keysTable =>
        val builtCount = keysTable.distinctCount(NullEquality.EQUAL)
        val isDistinct = builtCount == buildKeys.numRows()
        val magnificationFactor = buildKeys.numRows().toDouble / builtCount
        JoinBuildSideStats(magnificationFactor, isDistinct)
      }
    }
  }
}

/**
 * Base iterator for hash joins.
 *
 * @param buildSide the side that was materialized/buffered for this join (data movement decision).
 *                  This is distinct from the physical build side that the join algorithm selects
 *                  when using AUTO or SMALLEST build side selection strategies.
 */
abstract class BaseHashJoinIterator(
    built: LazySpillableColumnarBatch,
    boundBuiltKeys: Seq[GpuExpression],
    buildStatsOpt: Option[JoinBuildSideStats],
    stream: Iterator[LazySpillableColumnarBatch],
    boundStreamKeys: Seq[GpuExpression],
    streamAttributes: Seq[Attribute],
    joinOptions: JoinOptions,
    joinType: JoinType,
    buildSide: GpuBuildSide,
    conditionForLogging: Option[Expression],
    opTime: GpuMetric,
    joinTime: GpuMetric)
    extends SplittableJoinIterator(
      NvtxRegistry.JOIN_GATHER,
      stream,
      streamAttributes,
      built,
      joinOptions.targetSize,
      joinOptions.sizeEstimateThreshold,
      opTime = opTime,
      joinTime = joinTime) {
  // We can cache this because the build side is not changing
  protected lazy val buildStats: JoinBuildSideStats = buildStatsOpt.getOrElse {
    joinType match {
      case _: InnerLike | LeftOuter | RightOuter | FullOuter =>
        built.checkpoint()
        withRetryNoSplit {
          withRestoreOnRetry(built) {
            JoinBuildSideStats.fromBatch(built.getBatch, boundBuiltKeys)
          }
        }
      case _ =>
        // existence joins don't change size
        JoinBuildSideStats(1.0, isDistinct = false)
    }
  }

  /**
   * Check if sort join is supported for the given key expressions.
   * Sort join does not support ARRAY or STRUCT types in join keys.
   */
  protected def isSortJoinSupported(keys: Seq[GpuExpression]): Boolean = {
    !keys.exists { expr =>
      expr.dataType match {
        case _: ArrayType | _: StructType => true
        case _ => false
      }
    }
  }

  /**
   * Compute cardinality statistics for both sides of the join.
   * This is used for diagnostic logging when logJoinCardinality is enabled.
   */
  protected def computeCardinalityStats(
      leftKeys: Table,
      rightKeys: Table): JoinCardinalityStats = {
    val leftRowCount = leftKeys.getRowCount
    val rightRowCount = rightKeys.getRowCount
    val leftDistinctCount = leftKeys.distinctCount(NullEquality.EQUAL)
    val rightDistinctCount = rightKeys.distinctCount(NullEquality.EQUAL)
    
    // Compute null counts for each key column
    val leftNullCounts = (0 until leftKeys.getNumberOfColumns).map { i =>
      leftKeys.getColumn(i).getNullCount
    }
    val rightNullCounts = (0 until rightKeys.getNumberOfColumns).map { i =>
      rightKeys.getColumn(i).getNullCount
    }
    
    val leftKeyTypes = boundBuiltKeys.map(_.dataType)
    val rightKeyTypes = boundStreamKeys.map(_.dataType)
    
    JoinCardinalityStats(
      leftRowCount,
      rightRowCount,
      leftDistinctCount,
      rightDistinctCount,
      leftNullCounts,
      rightNullCounts,
      leftKeyTypes,
      rightKeyTypes)
  }

  /**
   * Log join cardinality information if logging is enabled.
   * This helps diagnose performance issues by showing key statistics about the join.
   * @param leftKeys the left side join keys
   * @param rightKeys the right side join keys
   * @param implementation the actual join implementation being used
   * @param originalJoinType the original join type before any transformations (None if unchanged)
   */
  protected def logJoinCardinality(
      leftKeys: Table,
      rightKeys: Table,
      implementation: String,
      originalJoinType: Option[JoinType] = None): Unit = {
    if (joinOptions.logCardinalityEnabled) {
      try {
        val stats = computeCardinalityStats(leftKeys, rightKeys)
        val taskContext = org.apache.spark.TaskContext.get()
        val taskInfo = if (taskContext != null) {
          s"Task: stageId=${taskContext.stageId()}, " +
          s"partitionId=${taskContext.partitionId()}, " +
          s"attemptNumber=${taskContext.attemptNumber()}"
        } else {
          "Task: No TaskContext available"
        }
        
        val conditionStr = conditionForLogging.map(_.toString).getOrElse("None")
        
        val joinTypeStr = originalJoinType match {
          case Some(origType) if origType != joinType =>
            s"$origType (transformed to $joinType)"
          case _ => joinType.toString
        }
        
        // Format null counts with column types
        val leftNullInfo = stats.leftKeyTypes.zip(stats.leftNullCounts).map {
          case (dtype, nullCount) =>
            s"$dtype: $nullCount nulls"
        }.mkString(", ")
        
        val rightNullInfo = stats.rightKeyTypes.zip(stats.rightNullCounts).map {
          case (dtype, nullCount) =>
            s"$dtype: $nullCount nulls"
        }.mkString(", ")
        
        logWarning(s"Join Starting - $taskInfo\n" +
          s"  JoinType: $joinTypeStr\n" +
          s"  BuildSide: $buildSide\n" +
          s"  Implementation: $implementation\n" +
          s"  Condition: $conditionStr\n" +
          s"  Left keys: ${stats.leftKeyTypes.mkString(", ")}\n" +
          s"  Left nulls: $leftNullInfo\n" +
          s"  Right keys: ${stats.rightKeyTypes.mkString(", ")}\n" +
          s"  Right nulls: $rightNullInfo\n" +
          s"  Left rows: ${stats.leftRowCount}, distinct: ${stats.leftDistinctCount}\n" +
          s"  Right rows: ${stats.rightRowCount}, distinct: ${stats.rightDistinctCount}")
      } catch {
        case e: Exception =>
          logWarning(s"Failed to compute join cardinality statistics: ${e.getMessage}")
      }
    }
  }

  /**
   * Log join completion if logging is enabled.
   * This helps identify if a join has hung or completed successfully.
   */
  protected def logJoinCompletion(): Unit = {
    if (joinOptions.logCardinalityEnabled) {
      try {
        val taskContext = org.apache.spark.TaskContext.get()
        val taskInfo = if (taskContext != null) {
          s"Task: stageId=${taskContext.stageId()}, " +
          s"partitionId=${taskContext.partitionId()}, " +
          s"attemptNumber=${taskContext.attemptNumber()}"
        } else {
          "Task: No TaskContext available"
        }
        
        logWarning(s"Join Gather Maps Completed - $taskInfo")
      } catch {
        case e: Exception =>
          logWarning(s"Failed to log join completion: ${e.getMessage}")
      }
    }
  }

  /**
   * Convert inner join maps to the target join type using post-processing.
   * This method handles all join type conversions and properly manages resources.
   * 
   * @param innerMaps the gather maps from an inner join (must have left at index 0, right at index 1)
   * @param leftRowCount total row count of the left table
   * @param rightRowCount total row count of the right table
   * @param targetJoinType the desired join type to convert to
   * @param errorContext additional context for error messages (e.g., "INNER_HASH_WITH_POST")
   * @return Array of gather maps for the target join type
   */
  protected def convertInnerJoinMapsToTargetType(
      innerMaps: GatherMapsResult,
      leftRowCount: Long,
      rightRowCount: Long,
      targetJoinType: JoinType,
      errorContext: String): GatherMapsResult = {
    targetJoinType match {
      case _: InnerLike => innerMaps
      case LeftOuter =>
        withResource(innerMaps) { _ =>
          JoinImpl.makeLeftOuter(innerMaps, leftRowCount.toInt, rightRowCount.toInt)
        }
      case RightOuter =>
        withResource(innerMaps) { _ =>
          JoinImpl.makeRightOuter(innerMaps, leftRowCount.toInt, rightRowCount.toInt)
        }
      case LeftSemi =>
        withResource(innerMaps) { _ =>
          JoinImpl.makeLeftSemi(innerMaps, leftRowCount.toInt)
        }
      case LeftAnti =>
        withResource(innerMaps) { _ =>
          JoinImpl.makeLeftAnti(innerMaps, leftRowCount.toInt)
        }
      case _ =>
        innerMaps.close()
        throw new NotImplementedError(
          s"Join $targetJoinType with $errorContext is not currently supported")
    }
  }

  override def computeNumJoinRows(cb: LazySpillableColumnarBatch): Long = {
    // TODO: Replace this estimate with exact join row counts using the corresponding cudf APIs
    //       being added in https://github.com/rapidsai/cudf/issues/9053.
    joinType match {
      // Full Outer join is implemented via LeftOuter/RightOuter, so use same estimate.
      case _: InnerLike | LeftOuter | RightOuter | FullOuter =>
        Math.ceil(cb.numRows * buildStats.streamMagnificationFactor).toLong
      case _ => cb.numRows
    }
  }

  override def createGatherer(
      cb: LazySpillableColumnarBatch,
      numJoinRows: Option[Long]): Option[JoinGatherer] = {
    // cb will be closed by the caller, so use a spill-only version here
    val spillOnlyCb = LazySpillableColumnarBatch.spillOnly(cb)
    val batches = Seq(built, spillOnlyCb)
    batches.foreach(_.checkpoint())
    try {
      withRetryNoSplit {
        withRestoreOnRetry(batches) {
          // We need a new LSCB that will be taken over by the gatherer, or closed
          closeOnExcept(LazySpillableColumnarBatch(spillOnlyCb.getBatch, "stream_data")) {
            streamBatch =>
              // the original stream data batch is not spillable until
              // we ask it to be right here, because we called `getBatch` on it
              // to make up the new lazy spillable (`streamBatch`)
              spillOnlyCb.allowSpilling()

              withResource(GpuProjectExec.project(built.getBatch, boundBuiltKeys)) { builtKeys =>
                // ensure that the build data can be spilled
                built.allowSpilling()
                joinGatherer(builtKeys, built, streamBatch)
              }
          }
        }
      }
    } catch {
      // This should work for all join types. There should be no need to do this for any
      // of the existence joins because the output rows will never be larger than the
      // input rows on the stream side.
      case oom @ (_ : OutOfMemoryError | _: GpuOOM) if joinType.isInstanceOf[InnerLike]
          || joinType == LeftOuter
          || joinType == RightOuter
          || joinType == FullOuter =>
        // Because this is just an estimate, it is possible for us to get this wrong, so
        // make sure we at least split the batch in half.
        val numBatches = Math.max(2, estimatedNumBatches(spillOnlyCb))

        // Split batch and return no gatherer so the outer loop will try again
        splitAndSave(spillOnlyCb.getBatch, numBatches, Some(oom))
        None
    }
  }

  /**
   * Perform a hash join, returning a gatherer if there is a join result.
   *
   * @param leftKeys table of join keys from the left table
   * @param leftData batch containing the full data from the left table
   * @param rightKeys table of join keys from the right table
   * @param rightData batch containing the full data from the right table
   * @return join gatherer if there are join results
   */
  protected def joinGathererLeftRight(
      leftKeys: Table,
      leftData: LazySpillableColumnarBatch,
      rightKeys: Table,
      rightData: LazySpillableColumnarBatch): Option[JoinGatherer]

  private def joinGathererLeftRight(
      leftKeys: ColumnarBatch,
      leftData: LazySpillableColumnarBatch,
      rightKeys: ColumnarBatch,
      rightData: LazySpillableColumnarBatch): Option[JoinGatherer] = {
    withResource(GpuColumnVector.from(leftKeys)) { leftKeysTab =>
      withResource(GpuColumnVector.from(rightKeys)) { rightKeysTab =>
        joinGathererLeftRight(leftKeysTab, leftData, rightKeysTab, rightData)
      }
    }
  }

  private def joinGatherer(
      buildKeys: ColumnarBatch,
      buildData: LazySpillableColumnarBatch,
      streamKeys: ColumnarBatch,
      streamData: LazySpillableColumnarBatch): Option[JoinGatherer] = {
    buildSide match {
      case GpuBuildLeft =>
        joinGathererLeftRight(buildKeys, buildData, streamKeys, streamData)
      case GpuBuildRight =>
        joinGathererLeftRight(streamKeys, streamData, buildKeys, buildData)
    }
  }

  private def joinGatherer(
      buildKeys: ColumnarBatch,
      buildData: LazySpillableColumnarBatch,
      streamCb: LazySpillableColumnarBatch): Option[JoinGatherer] = {
    withResource(GpuProjectExec.project(streamCb.getBatch, boundStreamKeys)) { streamKeys =>
      // ensure we make the stream side spillable again
      streamCb.allowSpilling()
      joinGatherer(buildKeys, LazySpillableColumnarBatch.spillOnly(buildData), streamKeys, streamCb)
    }
  }

  private def estimatedNumBatches(cb: LazySpillableColumnarBatch): Int = joinType match {
    case _: InnerLike | LeftOuter | RightOuter | FullOuter =>
      // We want the gather map size to be around the target size. There are two gather maps
      // that are made up of ints, so estimate how many rows per batch on the stream side
      // will produce the desired gather map size.
      val approximateStreamRowCount = ((joinOptions.targetSize.toDouble / 2) /
          DType.INT32.getSizeInBytes) / buildStats.streamMagnificationFactor
      val estimatedRowsPerStreamBatch = Math.min(Int.MaxValue, approximateStreamRowCount)
      Math.ceil(cb.numRows / estimatedRowsPerStreamBatch).toInt
    case _ => 1
  }
}

/**
 * An iterator that does a hash join against a stream of batches.
 */
class HashJoinIterator(
    built: LazySpillableColumnarBatch,
    val boundBuiltKeys: Seq[GpuExpression],
    buildStatsOpt: Option[JoinBuildSideStats],
    private val stream: Iterator[LazySpillableColumnarBatch],
    val boundStreamKeys: Seq[GpuExpression],
    val streamAttributes: Seq[Attribute],
    joinOptions: JoinOptions,
    val joinType: JoinType,
    val buildSide: GpuBuildSide,
    val compareNullsEqual: Boolean, // This is a workaround to how cudf support joins for structs
    conditionForLogging: Option[Expression],
    opTime: GpuMetric,
    private val joinTime: GpuMetric)
    extends BaseHashJoinIterator(
      built,
      boundBuiltKeys,
      buildStatsOpt,
      stream,
      boundStreamKeys,
      streamAttributes,
      joinOptions,
      joinType,
      buildSide,
      conditionForLogging,
      opTime = opTime,
      joinTime = joinTime) {
  override protected def joinGathererLeftRight(
      leftKeys: Table,
      leftData: LazySpillableColumnarBatch,
      rightKeys: Table,
      rightData: LazySpillableColumnarBatch): Option[JoinGatherer] = {
    NvtxIdWithMetrics(NvtxRegistry.HASH_JOIN_GATHER_MAP, joinTime) {
      // hack to work around unique_join not handling empty tables
      if (joinType.isInstanceOf[InnerLike] &&
        (leftKeys.getRowCount == 0 || rightKeys.getRowCount == 0)) {
        None
      } else {
        // Join strategy dispatching:
        // PRIORITY 1: Distinct join optimization (overrides all strategies)
        // PRIORITY 2: Strategy-based dispatching for non-distinct joins
        
        val maps = if (buildStats.isDistinct) {
          // Distinct join optimizations (highest priority, overrides strategy)
          logJoinCardinality(leftKeys, rightKeys, "distinct")
          val result = joinType match {
            case LeftOuter =>
              val rightRet = leftKeys.leftDistinctJoinGatherMap(rightKeys, compareNullsEqual)
              GatherMapsResult.makeFromRight(rightRet)
            case RightOuter =>
              val leftRet = rightKeys.leftDistinctJoinGatherMap(leftKeys, compareNullsEqual)
              GatherMapsResult.makeFromLeft(leftRet)
            case _: InnerLike =>
              val arrayRet = if (buildSide == GpuBuildRight) {
                leftKeys.innerDistinctJoinGatherMaps(rightKeys, compareNullsEqual)
              } else {
                rightKeys.innerDistinctJoinGatherMaps(leftKeys, compareNullsEqual).reverse
              }
              GatherMapsResult(arrayRet(0), arrayRet(1))
            case _ =>
              // Fall through to strategy-based dispatching for non-outer joins
              computeNonDistinctJoin(leftKeys, rightKeys, leftData, rightData)
          }
          logJoinCompletion()
          result
        } else {
          // Non-distinct joins: use strategy-based dispatching
          computeNonDistinctJoin(leftKeys, rightKeys, leftData, rightData)
        }
        
        makeGatherer(maps, leftData, rightData, joinType)
      }
    }
  }

  private def computeNonDistinctJoin(
      leftKeys: Table,
      rightKeys: Table,
      leftData: LazySpillableColumnarBatch,
      rightData: LazySpillableColumnarBatch): GatherMapsResult = {
    // Apply heuristics to select the effective strategy
    val effectiveStrategy = JoinStrategy.selectStrategy(
      joinOptions.strategy,
      joinType,
      hasCondition = false,  // This is called for unconditional joins
      joinOptions.buildSideSelection,
      leftKeys.getRowCount,
      rightKeys.getRowCount)

    effectiveStrategy match {
      case JoinStrategy.INNER_HASH_WITH_POST =>
        // Use composable JNI APIs: inner join -> convert to target join type
        computeNonCondInnerHashWithPost(leftKeys, rightKeys)
      case JoinStrategy.INNER_SORT_WITH_POST =>
        // Check if sort join is supported (no ARRAY/STRUCT types)
        val leftKeysSupported = isSortJoinSupported(boundBuiltKeys)
        val rightKeysSupported = isSortJoinSupported(boundStreamKeys)
        if (leftKeysSupported && rightKeysSupported) {
          computeNonCondInnerSortWithPost(leftKeys, rightKeys)
        } else {
          // Log warning and fall back to hash join
          if (!leftKeysSupported || !rightKeysSupported) {
            logWarning(s"INNER_SORT_WITH_POST strategy requested but join keys contain " +
              s"ARRAY or STRUCT types which are not supported for sort joins. " +
              s"Falling back to INNER_HASH_WITH_POST strategy.")
          }
          computeNonCondInnerHashWithPost(leftKeys, rightKeys, isFallback = true)
        }
      case _ =>
        // Use existing hash join methods (for HASH_ONLY and when AUTO doesn't trigger heuristics)
        computeWithHashJoin(leftKeys, rightKeys)
    }
  }

  private def computeNonCondInnerHashWithPost(
      leftKeys: Table,
      rightKeys: Table,
      isFallback: Boolean = false): GatherMapsResult = {
    val implName = if (isFallback) {
      "INNER_HASH_WITH_POST (fallback from INNER_SORT_WITH_POST)"
    } else {
      "INNER_HASH_WITH_POST"
    }
    logJoinCardinality(leftKeys, rightKeys, implName)

    val innerMaps = JoinImpl.innerHashJoin(leftKeys, rightKeys, compareNullsEqual,
      joinOptions.buildSideSelection, buildSide)

    val leftRowCount = leftKeys.getRowCount
    val rightRowCount = rightKeys.getRowCount
    val result = convertInnerJoinMapsToTargetType(innerMaps, leftRowCount, rightRowCount,
      joinType, "INNER_HASH_WITH_POST strategy")
    logJoinCompletion()
    result
  }

  private def computeNonCondInnerSortWithPost(
      leftKeys: Table,
      rightKeys: Table): GatherMapsResult = {
    logJoinCardinality(leftKeys, rightKeys, "INNER_SORT_WITH_POST")
    
    val innerMaps = JoinImpl.innerSortJoin(leftKeys, rightKeys, compareNullsEqual,
      joinOptions.buildSideSelection, buildSide)

    val leftRowCount = leftKeys.getRowCount
    val rightRowCount = rightKeys.getRowCount
    val result = convertInnerJoinMapsToTargetType(innerMaps, leftRowCount, rightRowCount,
      joinType, "INNER_SORT_WITH_POST strategy")
    logJoinCompletion()
    result
  }

  private def computeWithHashJoin(
      leftKeys: Table,
      rightKeys: Table): GatherMapsResult = {
    logJoinCardinality(leftKeys, rightKeys, "hash join")
    
    val result = joinType match {
      case LeftOuter =>
        JoinImpl.leftOuterHashJoinBuildRight(leftKeys, rightKeys, compareNullsEqual)
      case RightOuter =>
        JoinImpl.rightOuterHashJoinBuildLeft(leftKeys, rightKeys, compareNullsEqual)
      case _: InnerLike =>
        JoinImpl.innerHashJoin(leftKeys, rightKeys, compareNullsEqual,
          joinOptions.buildSideSelection, buildSide)
      case LeftSemi =>
        JoinImpl.leftSemiHashJoinBuildRight(leftKeys, rightKeys, compareNullsEqual)
      case LeftAnti =>
        JoinImpl.leftAntiHashJoinBuildRight(leftKeys, rightKeys, compareNullsEqual)
      case _ =>
        throw new NotImplementedError(s"Join Type ${joinType.getClass} is not currently" +
          s" supported")
    }
    logJoinCompletion()
    result
  }
}

/**
 * An iterator that does a hash join against a stream of batches with an inequality condition.
 * The LazyCompiledCondition will be closed when this iterator is closed.
 */
class ConditionalHashJoinIterator(
    built: LazySpillableColumnarBatch,
    boundBuiltKeys: Seq[GpuExpression],
    buildStatsOpt: Option[JoinBuildSideStats],
    stream: Iterator[LazySpillableColumnarBatch],
    boundStreamKeys: Seq[GpuExpression],
    streamAttributes: Seq[Attribute],
    lazyCompiledCondition: LazyCompiledCondition,
    joinOptions: JoinOptions,
    joinType: JoinType,
    buildSide: GpuBuildSide,
    compareNullsEqual: Boolean, // This is a workaround to how cudf support joins for structs
    conditionForLogging: Option[Expression],
    opTime: GpuMetric,
    joinTime: GpuMetric)
    extends BaseHashJoinIterator(
      built,
      boundBuiltKeys,
      buildStatsOpt,
      stream,
      boundStreamKeys,
      streamAttributes,
      joinOptions,
      joinType,
      buildSide,
      conditionForLogging,
      opTime = opTime,
      joinTime = joinTime) {

  // The AST condition is compiled based on the data movement build side.
  // For INNER_HASH_WITH_POST and INNER_SORT_WITH_POST strategies, the physical build side
  // selection happens within the inner join, but the AST is always compiled for the data
  // movement build side and used in post-filtering (where only left/right table order matters).
  // For mixed join fallback with InnerLike joins, we compile dynamically based on the
  // physical build side selected per-batch.
  override protected def joinGathererLeftRight(
      leftKeys: Table,
      leftData: LazySpillableColumnarBatch,
      rightKeys: Table,
      rightData: LazySpillableColumnarBatch): Option[JoinGatherer] = {
    val nullEquality = if (compareNullsEqual) NullEquality.EQUAL else NullEquality.UNEQUAL
    NvtxIdWithMetrics(NvtxRegistry.HASH_JOIN_GATHER_MAP, joinTime) {
      withResource(GpuColumnVector.from(leftData.getBatch)) { leftTable =>
        withResource(GpuColumnVector.from(rightData.getBatch)) { rightTable =>
          // Apply heuristics to select the effective strategy for conditional joins
          val effectiveStrategy = JoinStrategy.selectStrategy(
            joinOptions.strategy,
            joinType,
            hasCondition = true,  // This is a conditional join
            joinOptions.buildSideSelection,
            leftKeys.getRowCount,
            rightKeys.getRowCount)

          // Join strategy dispatching for conditional joins:
          val maps = effectiveStrategy match {
            case JoinStrategy.INNER_HASH_WITH_POST =>
              // Use composable JNI APIs: inner join -> filter -> convert to target join type
              computeInnerHashWithPost(leftKeys, rightKeys, leftTable, rightTable, nullEquality)
            case JoinStrategy.INNER_SORT_WITH_POST =>
              // Check if sort join is supported (no ARRAY/STRUCT types)
              val leftKeysSupported = isSortJoinSupported(boundBuiltKeys)
              val rightKeysSupported = isSortJoinSupported(boundStreamKeys)
              if (leftKeysSupported && rightKeysSupported) {
                computeInnerSortWithPost(leftKeys, rightKeys, leftTable, rightTable, nullEquality)
              } else {
                // Log warning and fall back to hash join
                logWarning(s"INNER_SORT_WITH_POST strategy requested but join keys contain " +
                  s"ARRAY or STRUCT types which are not supported for sort joins. " +
                  s"Falling back to INNER_HASH_WITH_POST strategy.")
                computeInnerHashWithPost(leftKeys, rightKeys, leftTable, rightTable, nullEquality, 
                  isFallback = true)
              }
            case _ =>
              // Use existing mixed join methods (for HASH_ONLY and when AUTO doesn't trigger)
              computeWithMixedJoin(leftKeys, rightKeys, leftTable, rightTable, nullEquality)
          }          
          makeGatherer(maps, leftData, rightData, joinType)
        }
      }
    }
  }

  private def computeInnerHashWithPost(
      leftKeys: Table,
      rightKeys: Table,
      leftTable: Table,
      rightTable: Table,
      nullEquality: NullEquality,
      isFallback: Boolean = false): GatherMapsResult = {
    val implName = if (isFallback) {
      "INNER_HASH_WITH_POST (conditional, fallback from INNER_SORT_WITH_POST)"
    } else {
      "INNER_HASH_WITH_POST (conditional)"
    }
    logJoinCardinality(leftKeys, rightKeys, implName)
    
    val leftRowCount = leftKeys.getRowCount
    val rightRowCount = rightKeys.getRowCount

    val innerMaps = JoinImpl.innerHashJoin(leftKeys, rightKeys,
      nullEquality == NullEquality.EQUAL, joinOptions.buildSideSelection, buildSide)

    val compiledCondition = lazyCompiledCondition.getForBuildSide(buildSide)

    val filteredMaps = withResource(innerMaps) { _ =>
      JoinImpl.filterInnerJoinWithASTSwapByJoinTypeAndBuildSide(innerMaps, leftTable, rightTable,
        compiledCondition, joinType, buildSide)
    }

    val result = convertInnerJoinMapsToTargetType(filteredMaps, leftRowCount, rightRowCount,
      joinType, "INNER_HASH_WITH_POST strategy")
    logJoinCompletion()
    result
  }

  private def computeInnerSortWithPost(
      leftKeys: Table,
      rightKeys: Table,
      leftTable: Table,
      rightTable: Table,
      nullEquality: NullEquality): GatherMapsResult = {
    logJoinCardinality(leftKeys, rightKeys, "INNER_SORT_WITH_POST (conditional)")
    
    val leftRowCount = leftKeys.getRowCount
    val rightRowCount = rightKeys.getRowCount

    val innerMaps = JoinImpl.innerSortJoin(leftKeys, rightKeys,
      nullEquality == NullEquality.EQUAL, joinOptions.buildSideSelection, buildSide)

    val compiledCondition = lazyCompiledCondition.getForBuildSide(buildSide)

    val filteredMaps = withResource(innerMaps) { _ =>
      JoinImpl.filterInnerJoinWithASTSwapByJoinTypeAndBuildSide(innerMaps, leftTable, rightTable,
        compiledCondition, joinType, buildSide)
    }

    val result = convertInnerJoinMapsToTargetType(filteredMaps, leftRowCount, rightRowCount,
      joinType, "INNER_SORT_WITH_POST strategy")
    logJoinCompletion()
    result
  }

  private def computeWithMixedJoin(
      leftKeys: Table,
      rightKeys: Table,
      leftTable: Table,
      rightTable: Table,
      nullEquality: NullEquality): GatherMapsResult = {
    logJoinCardinality(leftKeys, rightKeys, "mixed join (conditional)")
    
    val result = joinType match {
      case _: InnerLike =>
        // For inner joins, use dynamic build side selection
        val selectedBuildSide = JoinBuildSideSelection.selectPhysicalBuildSide(
          joinOptions.buildSideSelection, buildSide,
          leftKeys.getRowCount, rightKeys.getRowCount)
        selectedBuildSide match {
          case GpuBuildLeft =>
            JoinImpl.innerHashJoinBuildLeft(leftKeys, rightKeys, leftTable, rightTable,
              lazyCompiledCondition.getForBuildLeft, nullEquality)
          case GpuBuildRight =>
            JoinImpl.innerHashJoinBuildRight(leftKeys, rightKeys, leftTable, rightTable,
              lazyCompiledCondition.getForBuildRight, nullEquality)
        }
      case LeftOuter =>
        JoinImpl.leftOuterHashJoinBuildRight(leftKeys, rightKeys, leftTable, rightTable,
          lazyCompiledCondition.getForBuildRight, nullEquality)
      case RightOuter =>
        JoinImpl.rightOuterHashJoinBuildLeft(leftKeys, rightKeys, leftTable, rightTable,
          lazyCompiledCondition.getForBuildLeft, nullEquality)
      case LeftSemi =>
        JoinImpl.leftSemiHashJoinBuildRight(leftKeys, rightKeys, leftTable, rightTable,
          lazyCompiledCondition.getForBuildRight, nullEquality)
      case LeftAnti =>
        JoinImpl.leftAntiHashJoinBuildRight(leftKeys, rightKeys, leftTable, rightTable,
          lazyCompiledCondition.getForBuildRight, nullEquality)
      case _ =>
        throw new NotImplementedError(s"Join $joinType $buildSide is not currently supported")
    }
    logJoinCompletion()
    result
  }

  override def close(): Unit = {
    if (!closed) {
      super.close()
      lazyCompiledCondition.close()
    }
  }
}

/**
 * An iterator that does the stream-side only of a hash join. Using full join as an example,
 * it performs the left or right outer join for the stream side's view of a full outer join.
 * As the join is performed, the build-side rows that are referenced during the join are tracked
 * and can be retrieved after the iteration has completed to assist in performing the anti-join
 * needed to produce the final results needed for the full outer join.
 *
 * @param joinType the type of join being performed
 * @param built spillable form of the build side table. This will be closed by the iterator.
 * @param boundBuiltKeys bound expressions for the build side equi-join keys
 * @param buildStatsOpt statistics computed for the build side batch, if any
 * @param buildSideTrackerInit initial value of the build side row tracker, if any. This will be
 *                             closed by the iterator.
 * @param stream iterator to produce batches for the stream side table
 * @param boundStreamKeys bound expressions for the stream side equi-join keys
 * @param streamAttributes schema of the stream side table
 * @param compiledCondition compiled AST expression for the inequality condition of the join,
 *                          if any. NOTE: This will *not* be closed by the iterator.
 * @param targetSize target GPU batch size in bytes
 * @param buildSide which side of the join is being used for the build side
 * @param compareNullsEqual whether to compare nulls as equal during the join
 * @param opTime metric to update for total operation time
 * @param joinTime metric to update for join time
 */
class HashJoinStreamSideIterator(
    joinType: JoinType,
    built: LazySpillableColumnarBatch,
    boundBuiltKeys: Seq[GpuExpression],
    buildStatsOpt: Option[JoinBuildSideStats],
    buildSideTrackerInit: Option[SpillableColumnarBatch],
    stream: Iterator[LazySpillableColumnarBatch],
    boundStreamKeys: Seq[GpuExpression],
    streamAttributes: Seq[Attribute],
    lazyCompiledCondition: Option[LazyCompiledCondition],
    joinOptions: JoinOptions,
    buildSide: GpuBuildSide,
    compareNullsEqual: Boolean, // This is a workaround to how cudf support joins for structs
    conditionForLogging: Option[Expression],
    opTime: GpuMetric,
    joinTime: GpuMetric)
    extends BaseHashJoinIterator(
      built,
      boundBuiltKeys,
      buildStatsOpt,
      stream,
      boundStreamKeys,
      streamAttributes,
      joinOptions,
      joinType,
      buildSide,
      conditionForLogging,
      opTime = opTime,
      joinTime = joinTime) {
  // Determine the type of join to use as we iterate through the stream-side batches.
  // Each of these outer joins can be implemented in terms of another join as we iterate
  // through the stream-side batches and then emit a final anti-join batch based on which
  // build-side rows were left untouched by the stream-side iteration.
  // FullOuter joins are implemented in terms of LeftOuter + RightAnti or RightOuter + LeftAnti
  // LeftOuter joins are implemented in terms of Inner + RightAnti
  // RightOuter joins are implemented in terms of Inner + LeftAnti
  private val subJoinType = joinType match {
    case FullOuter if buildSide == GpuBuildRight => LeftOuter
    case FullOuter if buildSide == GpuBuildLeft => RightOuter
    case LeftOuter if buildSide == GpuBuildLeft => Inner
    case RightOuter if buildSide == GpuBuildRight => Inner
    case t =>
      throw new IllegalStateException(s"unsupported join type $t with $buildSide")
  }

  private val nullEquality = if (compareNullsEqual) NullEquality.EQUAL else NullEquality.UNEQUAL

  // The cudf API build side is determined by subJoinType, not the original buildSide:
  // - LeftOuter uses leftOuterHashJoinBuildRight (expects BuildRight AST)
  // - RightOuter uses rightOuterHashJoinBuildLeft (expects BuildLeft AST)
  // - Inner uses innerHashJoinBuildRight (expects BuildRight AST)
  private val cudfBuildSide: GpuBuildSide = subJoinType match {
    case LeftOuter => GpuBuildRight
    case RightOuter => GpuBuildLeft
    case Inner => GpuBuildRight
    case t => throw new IllegalStateException(s"unexpected subJoinType: $t")
  }

  private[this] var builtSideTracker: Option[SpillableColumnarBatch] = buildSideTrackerInit

  private def unconditionalJoinGatherMaps(
      leftKeys: Table, rightKeys: Table): GatherMapsResult = {
    // Pass the original joinType if it was transformed to subJoinType
    val originalJoinType = if (joinType != subJoinType) Some(joinType) else None

    // Apply heuristics to select the effective strategy for unconditional joins
    // Note: subJoinType is used for strategy selection since that's what we're actually executing
    val effectiveStrategy = JoinStrategy.selectStrategy(
      joinOptions.strategy,
      subJoinType,
      hasCondition = false,
      joinOptions.buildSideSelection,
      leftKeys.getRowCount,
      rightKeys.getRowCount)
    
    effectiveStrategy match {
      case JoinStrategy.INNER_HASH_WITH_POST =>
        // Use composable JNI APIs
        computeUnconditionalInnerHashWithPost(leftKeys, rightKeys, originalJoinType)
      case JoinStrategy.INNER_SORT_WITH_POST =>
        // Check if sort join is supported (no ARRAY/STRUCT types)
        val leftKeysSupported = isSortJoinSupported(boundBuiltKeys)
        val rightKeysSupported = isSortJoinSupported(boundStreamKeys)
        if (leftKeysSupported && rightKeysSupported) {
          computeUnconditionalInnerSortWithPost(leftKeys, rightKeys, originalJoinType)
        } else {
          // Log warning and fall back to hash join
          logWarning(s"INNER_SORT_WITH_POST strategy requested but join keys contain " +
            s"ARRAY or STRUCT types which are not supported for sort joins. " +
            s"Falling back to INNER_HASH_WITH_POST strategy.")
          computeUnconditionalInnerHashWithPost(leftKeys, rightKeys, originalJoinType, 
            isFallback = true)
        }
      case _ =>
        // Use existing hash join methods
        computeUnconditionalHashJoin(leftKeys, rightKeys, originalJoinType)
    }
  }

  private def computeUnconditionalInnerHashWithPost(
      leftKeys: Table,
      rightKeys: Table,
      originalJoinType: Option[JoinType],
      isFallback: Boolean = false): GatherMapsResult = {
    val implName = if (isFallback) {
      s"INNER_HASH_WITH_POST (outer: $joinType, fallback from INNER_SORT_WITH_POST)"
    } else {
      s"INNER_HASH_WITH_POST (outer: $joinType)"
    }
    logJoinCardinality(leftKeys, rightKeys, implName, originalJoinType)

    val innerMaps = JoinImpl.innerHashJoin(leftKeys, rightKeys, compareNullsEqual,
      joinOptions.buildSideSelection, cudfBuildSide)

    val leftRowCount = leftKeys.getRowCount
    val rightRowCount = rightKeys.getRowCount

    val result = convertInnerJoinMapsToTargetType(innerMaps, leftRowCount, rightRowCount,
      subJoinType, "INNER_HASH_WITH_POST strategy")
    logJoinCompletion()
    result
  }

  private def computeUnconditionalInnerSortWithPost(
      leftKeys: Table,
      rightKeys: Table,
      originalJoinType: Option[JoinType]): GatherMapsResult = {
    logJoinCardinality(leftKeys, rightKeys, s"INNER_SORT_WITH_POST (outer: $joinType)", 
      originalJoinType)

    val innerMaps = JoinImpl.innerSortJoin(leftKeys, rightKeys, compareNullsEqual,
      joinOptions.buildSideSelection, cudfBuildSide)

    val leftRowCount = leftKeys.getRowCount
    val rightRowCount = rightKeys.getRowCount

    val result = convertInnerJoinMapsToTargetType(innerMaps, leftRowCount, rightRowCount,
      subJoinType, "INNER_SORT_WITH_POST strategy")
    logJoinCompletion()
    result
  }

  private def computeUnconditionalHashJoin(
      leftKeys: Table,
      rightKeys: Table,
      originalJoinType: Option[JoinType]): GatherMapsResult = {
    logJoinCardinality(leftKeys, rightKeys, s"hash join (outer: $joinType)", originalJoinType)
    
    val result = subJoinType match {
      case LeftOuter =>
        JoinImpl.leftOuterHashJoinBuildRight(leftKeys, rightKeys, compareNullsEqual)
      case RightOuter =>
        JoinImpl.rightOuterHashJoinBuildLeft(leftKeys, rightKeys, compareNullsEqual)
      case Inner =>
        JoinImpl.innerHashJoin(leftKeys, rightKeys, compareNullsEqual,
          joinOptions.buildSideSelection, cudfBuildSide)
      case t =>
        throw new IllegalStateException(s"unsupported join type: $t")
    }
    logJoinCompletion()
    result
  }

  private def conditionalJoinGatherMaps(
      leftKeys: Table,
      leftData: LazySpillableColumnarBatch,
      rightKeys: Table,
      rightData: LazySpillableColumnarBatch,
      lazyCondition: LazyCompiledCondition): GatherMapsResult = {
    // Pass the original joinType if it was transformed to subJoinType
    val originalJoinType = if (joinType != subJoinType) Some(joinType) else None
    
    withResource(GpuColumnVector.from(leftData.getBatch)) { leftTable =>
      withResource(GpuColumnVector.from(rightData.getBatch)) { rightTable =>
        // Apply heuristics to select the effective strategy for conditional joins
        val effectiveStrategy = JoinStrategy.selectStrategy(
          joinOptions.strategy,
          subJoinType,
          hasCondition = true,
          joinOptions.buildSideSelection,
          leftKeys.getRowCount,
          rightKeys.getRowCount)

        effectiveStrategy match {
          case JoinStrategy.INNER_HASH_WITH_POST =>
            // Use composable JNI APIs
            computeConditionalInnerHashWithPost(leftKeys, rightKeys, leftTable, rightTable,
              lazyCondition, originalJoinType)
          case JoinStrategy.INNER_SORT_WITH_POST =>
            // Check if sort join is supported (no ARRAY/STRUCT types)
            val leftKeysSupported = isSortJoinSupported(boundBuiltKeys)
            val rightKeysSupported = isSortJoinSupported(boundStreamKeys)
            if (leftKeysSupported && rightKeysSupported) {
              computeConditionalInnerSortWithPost(leftKeys, rightKeys, leftTable, rightTable,
                lazyCondition, originalJoinType)
            } else {
              // Log warning and fall back to hash join
              logWarning(s"INNER_SORT_WITH_POST strategy requested but join keys contain " +
                s"ARRAY or STRUCT types which are not supported for sort joins. " +
                s"Falling back to INNER_HASH_WITH_POST strategy.")
              computeConditionalInnerHashWithPost(leftKeys, rightKeys, leftTable, rightTable,
                lazyCondition, originalJoinType, isFallback = true)
            }
          case _ =>
            // Use existing mixed join methods
            computeConditionalMixedJoin(leftKeys, rightKeys, leftTable, rightTable,
              lazyCondition, originalJoinType)
        }
      }
    }
  }

  private def computeConditionalInnerHashWithPost(
      leftKeys: Table,
      rightKeys: Table,
      leftTable: Table,
      rightTable: Table,
      lazyCondition: LazyCompiledCondition,
      originalJoinType: Option[JoinType],
      isFallback: Boolean = false): GatherMapsResult = {
    val implName = if (isFallback) {
      s"INNER_HASH_WITH_POST (outer: $joinType, conditional, fallback from INNER_SORT_WITH_POST)"
    } else {
      s"INNER_HASH_WITH_POST (outer: $joinType, conditional)"
    }
    logJoinCardinality(leftKeys, rightKeys, implName, originalJoinType)

    val innerMaps = JoinImpl.innerHashJoin(leftKeys, rightKeys, compareNullsEqual,
      joinOptions.buildSideSelection, cudfBuildSide)

    val compiledCondition = lazyCondition.getForBuildSide(cudfBuildSide)

    val filteredMaps = withResource(innerMaps) { _ =>
      JoinImpl.filterInnerJoinWithASTSwapBySubJoinType(innerMaps, leftTable, rightTable,
        compiledCondition, subJoinType)
    }

    val leftRowCount = leftTable.getRowCount
    val rightRowCount = rightTable.getRowCount

    val result = convertInnerJoinMapsToTargetType(filteredMaps, leftRowCount, rightRowCount,
      subJoinType, "INNER_HASH_WITH_POST strategy")
    logJoinCompletion()
    result
  }

  private def computeConditionalInnerSortWithPost(
      leftKeys: Table,
      rightKeys: Table,
      leftTable: Table,
      rightTable: Table,
      lazyCondition: LazyCompiledCondition,
      originalJoinType: Option[JoinType]): GatherMapsResult = {
    logJoinCardinality(leftKeys, rightKeys, s"INNER_SORT_WITH_POST (outer: $joinType, conditional)",
      originalJoinType)

    val innerMaps = JoinImpl.innerSortJoin(leftKeys, rightKeys, compareNullsEqual,
      joinOptions.buildSideSelection, cudfBuildSide)

    val compiledCondition = lazyCondition.getForBuildSide(cudfBuildSide)

    val filteredMaps = withResource(innerMaps) { _ =>
      JoinImpl.filterInnerJoinWithASTSwapBySubJoinType(innerMaps, leftTable, rightTable,
        compiledCondition, subJoinType)
    }

    val leftRowCount = leftTable.getRowCount
    val rightRowCount = rightTable.getRowCount

    val result = convertInnerJoinMapsToTargetType(filteredMaps, leftRowCount, rightRowCount,
      subJoinType, "INNER_SORT_WITH_POST strategy")
    logJoinCompletion()
    result
  }

  private def computeConditionalMixedJoin(
      leftKeys: Table,
      rightKeys: Table,
      leftTable: Table,
      rightTable: Table,
      lazyCondition: LazyCompiledCondition,
      originalJoinType: Option[JoinType]): GatherMapsResult = {
    logJoinCardinality(leftKeys, rightKeys, s"mixed join (outer: $joinType, conditional)",
      originalJoinType)
    
    val result = subJoinType match {
      case LeftOuter =>
        JoinImpl.leftOuterHashJoinBuildRight(leftKeys, rightKeys, leftTable, rightTable,
          lazyCondition.getForBuildRight, nullEquality)
      case RightOuter =>
        JoinImpl.rightOuterHashJoinBuildLeft(leftKeys, rightKeys, leftTable, rightTable,
          lazyCondition.getForBuildLeft, nullEquality)
      case Inner =>
        // For inner sub-joins, use dynamic build side selection
        // For sub-joins, the plan build side is cudfBuildSide (GpuBuildRight for Inner)
        val selectedBuildSide = JoinBuildSideSelection.selectPhysicalBuildSide(
          joinOptions.buildSideSelection, cudfBuildSide,
          leftKeys.getRowCount, rightKeys.getRowCount)
        selectedBuildSide match {
          case GpuBuildLeft =>
            JoinImpl.innerHashJoinBuildLeft(leftKeys, rightKeys, leftTable, rightTable,
              lazyCondition.getForBuildLeft, nullEquality)
          case GpuBuildRight =>
            JoinImpl.innerHashJoinBuildRight(leftKeys, rightKeys, leftTable, rightTable,
              lazyCondition.getForBuildRight, nullEquality)
        }
      case t =>
        throw new IllegalStateException(s"unsupported join type: $t")
    }
    logJoinCompletion()
    result
  }

  override protected def joinGathererLeftRight(
      leftKeys: Table,
      leftData: LazySpillableColumnarBatch,
      rightKeys: Table,
      rightData: LazySpillableColumnarBatch): Option[JoinGatherer] = {
    NvtxIdWithMetrics(NvtxRegistry.FULL_HASH_JOIN_GATHER_MAP, joinTime) {
      val maps = lazyCompiledCondition.map { lazyCondition =>
        conditionalJoinGatherMaps(leftKeys, leftData, rightKeys, rightData, lazyCondition)
      }.getOrElse {
        unconditionalJoinGatherMaps(leftKeys, rightKeys)
      }
      
      withResource(maps) { _ =>
        val lazyLeftMap = LazySpillableGatherMap(maps.left, "left_map")
        val lazyRightMap = LazySpillableGatherMap(maps.right, "right_map")
        NvtxIdWithMetrics(NvtxRegistry.UPDATE_TRACKING_MASK, joinTime) {
          closeOnExcept(Seq(lazyLeftMap, lazyRightMap)) { _ =>
            updateTrackingMask(if (buildSide == GpuBuildRight) lazyRightMap else lazyLeftMap)
          }
        }
        val (leftOutOfBoundsPolicy, rightOutOfBoundsPolicy) = subJoinType match {
          case LeftOuter => (OutOfBoundsPolicy.DONT_CHECK, OutOfBoundsPolicy.NULLIFY)
          case RightOuter => (OutOfBoundsPolicy.NULLIFY, OutOfBoundsPolicy.DONT_CHECK)
          case Inner => (OutOfBoundsPolicy.DONT_CHECK, OutOfBoundsPolicy.DONT_CHECK)
          case t => throw new IllegalStateException(s"unsupported join type $t")
        }
        val gatherer = JoinGatherer(lazyLeftMap, leftData, lazyRightMap, rightData,
          leftOutOfBoundsPolicy, rightOutOfBoundsPolicy)
        if (gatherer.isDone) {
          // Nothing matched...
          gatherer.close()
          None
        } else {
          Some(gatherer)
        }
      }
    }
  }

  // Need to avoid close on exhaust so others can access the built side tracker after iteration.
  override protected val shouldAutoCloseOnExhaust: Boolean = false

  /**
   *  Retrieve the tracking data for the build side rows that have been referenced during the
   *  join. This is normally called after iteration has completed. The caller takes ownership
   *  of the resulting data and is responsible for closing it.
   */
  def releaseBuiltSideTracker(): Option[SpillableColumnarBatch] = {
    val result = builtSideTracker
    builtSideTracker = None
    result
  }

  override def close(): Unit = {
    if (!closed) {
      super.close()
      builtSideTracker.foreach(_.close())
      builtSideTracker = None
      lazyCompiledCondition.foreach(_.close())
    }
  }

  private def trueColumnTable(numRows: Int): Table = {
    withResource(Scalar.fromBool(true)) { trueScalar =>
      withResource(ai.rapids.cudf.ColumnVector.fromScalar(trueScalar, numRows)) {
        new Table(_)
      }
    }
  }

  // Create a boolean column to indicate which gather map rows are valid.
  private def validIndexMask(gatherView: ColumnView): ColumnView = {
    withResource(Scalar.fromInt(Int.MinValue)) { invalidIndex =>
      gatherView.notEqualTo(invalidIndex)
    }
  }

  /**
   * Update the tracking mask for the build side.
   */
  private def updateTrackingMask(buildSideGatherMap: LazySpillableGatherMap): Unit = {
    // Filter the build side gather map to remove invalid indices
    val numGatherMapRows = buildSideGatherMap.getRowCount.toInt
    val filteredGatherMap = {
      withResource(buildSideGatherMap.toColumnView(0, numGatherMapRows)) { gatherView =>
        withResource(gatherView.copyToColumnVector()) { gatherVec =>
          withResource(new Table(gatherVec)) { gatherTab =>
            withResource(validIndexMask(gatherView)) { mask =>
              gatherTab.filter(mask)
            }
          }
        }
      }
    }
    // Update all hits in the gather map with false (no longer needed) in the tracking table
    val updatedTrackingTable = withResource(filteredGatherMap) { filteredMap =>
      // Get the current tracking table, or all true table to start with
      val builtTrackingTable = builtSideTracker.map { spillableBatch =>
        withResource(spillableBatch) { scb =>
          withResource(scb.getColumnarBatch()) { trackingBatch =>
            GpuColumnVector.from(trackingBatch)
          }
        }
      }.getOrElse {
        trueColumnTable(built.numRows)
      }
      withResource(builtTrackingTable) { trackingTable =>
        withResource(Scalar.fromBool(false)) { falseScalar =>
          Table.scatter(Array(falseScalar), filteredMap.getColumn(0), trackingTable)
        }
      }
    }
    val previousTracker = builtSideTracker
    builtSideTracker = withResource(updatedTrackingTable) { _ =>
      Some(SpillableColumnarBatch(
        GpuColumnVector.from(updatedTrackingTable, Array[DataType](DataTypes.BooleanType)),
        SpillPriorities.ACTIVE_ON_DECK_PRIORITY))
    }
    // If we throw above, we should not close the existing tracker
    previousTracker.foreach(_.close())
  }
}

/**
 * An iterator that does a hash outer join against a stream of batches where either the join
 * type is a full outer join or the join type is a left or right outer join and the build side
 * matches the outer join side.  It does this by doing a subset of the original join (e.g.:
 * left outer for a full outer join) and keeping track of the hits on the build side.  It then
 * produces a final batch of all the build side rows that were not already included.
 *
 * @param joinType the type of join to perform
 * @param built spillable form of the build side table. This will be closed by the iterator.
 * @param boundBuiltKeys bound expressions for the build side equi-join keys
 * @param buildStats statistics computed for the build side batch, if any
 * @param buildSideTrackerInit initial value of the build side row tracker, if any. This will be
 *                             closed by the iterator.
 * @param stream iterator to produce batches for the stream side table
 * @param boundStreamKeys bound expressions for the stream side equi-join keys
 * @param streamAttributes schema of the stream side table
 * @param boundCondition expression for the inequality condition of the join, if any
 * @param targetSize target GPU batch size in bytes
 * @param buildSide which side of the join is being used for the build side
 * @param compareNullsEqual whether to compare nulls as equal during the join
 * @param opTime metric to update for total operation time
 * @param joinTime metric to update for join time
 */
class HashOuterJoinIterator(
    joinType: JoinType,
    built: LazySpillableColumnarBatch,
    boundBuiltKeys: Seq[GpuExpression],
    buildStats: Option[JoinBuildSideStats],
    buildSideTrackerInit: Option[SpillableColumnarBatch],
    stream: Iterator[LazySpillableColumnarBatch],
    boundStreamKeys: Seq[GpuExpression],
    streamAttributes: Seq[Attribute],
    lazyCompiledCondition: Option[LazyCompiledCondition],
    joinOptions: JoinOptions,
    buildSide: GpuBuildSide,
    compareNullsEqual: Boolean, // This is a workaround to how cudf support joins for structs
    conditionForLogging: Option[Expression],
    opTime: GpuMetric,
    joinTime: GpuMetric) extends Iterator[ColumnarBatch] with TaskAutoCloseableResource {

  private val streamJoinIter = new HashJoinStreamSideIterator(joinType, built, boundBuiltKeys,
    buildStats, buildSideTrackerInit, stream, boundStreamKeys, streamAttributes, 
    lazyCompiledCondition,
    joinOptions, buildSide, compareNullsEqual, conditionForLogging, opTime, joinTime)

  private var finalBatch: Option[ColumnarBatch] = None

  override def hasNext: Boolean = {
    if (streamJoinIter.hasNext || finalBatch.isDefined) {
      true
    } else {
      finalBatch = getFinalBatch()
      // Now that we've manifested the final batch, we can close the stream iterator early to free
      // GPU resources.
      streamJoinIter.close()
      finalBatch.isDefined
    }
  }

  override def next(): ColumnarBatch = {
    if (!hasNext) {
      throw new NoSuchElementException("batches exhausted")
    }
    if (streamJoinIter.hasNext) {
      streamJoinIter.next()
    } else {
      val batch = finalBatch.get
      finalBatch = None
      batch
    }
  }

  override def close(): Unit = {
    if (!closed) {
      super.close()
      streamJoinIter.close()
      finalBatch.foreach(_.close())
      finalBatch = None
    }
  }

  private def getFinalBatch(): Option[ColumnarBatch] = {
    NvtxIdWithMetrics(NvtxRegistry.GET_FINAL_BATCH, joinTime) {
      streamJoinIter.releaseBuiltSideTracker() match {
        case None => None
        case Some(tracker) =>
          val filteredBatch = withResource(tracker) { scb =>
            withResource(scb.getColumnarBatch()) { trackerBatch =>
              withResource(GpuColumnVector.from(trackerBatch)) { trackerTab =>
                val batch = built.getBatch
                withResource(GpuColumnVector.from(batch)) { builtTable =>
                  withResource(builtTable.filter(trackerTab.getColumn(0))) { filterTab =>
                    GpuColumnVector.from(filterTab, GpuColumnVector.extractTypes(batch))
                  }
                }
              }
            }
          }
          // Combine build-side columns with null columns for stream side
          withResource(filteredBatch) { builtBatch =>
            val numFilterRows = builtBatch.numRows()
            if (numFilterRows > 0) {
              val streamColumns = streamAttributes.safeMap { attr =>
                GpuColumnVector.fromNull(numFilterRows, attr.dataType)
              }
              withResource(new ColumnarBatch(streamColumns.toArray, numFilterRows)) { streamBatch =>
                buildSide match {
                  case GpuBuildRight =>
                    Some(GpuColumnVector.combineColumns(streamBatch, builtBatch))
                  case GpuBuildLeft =>
                    Some(GpuColumnVector.combineColumns(builtBatch, streamBatch))
                }
              }
            } else {
              None
            }
          }
      }
    }
  }
}

class HashedExistenceJoinIterator(
  spillableBuiltBatch: LazySpillableColumnarBatch,
  boundBuildKeys: Seq[GpuExpression],
  lazyStream: Iterator[LazySpillableColumnarBatch],
  boundStreamKeys: Seq[GpuExpression],
  lazyCompiledCondition: Option[LazyCompiledCondition],
  buildSide: GpuBuildSide,
  compareNullsEqual: Boolean,
  opTime: GpuMetric,
  joinTime: GpuMetric
) extends ExistenceJoinIterator(spillableBuiltBatch, lazyStream, opTime, joinTime) {

  // Get the compiled condition for our build side
  val compiledConditionRes: Option[CompiledExpression] = 
    lazyCompiledCondition.map(_.getForBuildSide(buildSide))

  private def leftKeysTable(leftColumnarBatch: ColumnarBatch): Table = {
    withResource(GpuProjectExec.project(leftColumnarBatch, boundStreamKeys)) {
      GpuColumnVector.from(_)
    }
  }

  private def rightKeysTable(): Table = {
    withResource(GpuProjectExec.project(spillableBuiltBatch.getBatch, boundBuildKeys)) {
      GpuColumnVector.from(_)
    }
  }

  private def conditionalBatchLeftSemiJoin(
    leftColumnarBatch: ColumnarBatch,
    leftKeysTab: Table,
    rightKeysTab: Table,
    compiledCondition: CompiledExpression): GatherMap = {
    val result = withResource(GpuColumnVector.from(leftColumnarBatch)) { leftTab =>
      withResource(GpuColumnVector.from(spillableBuiltBatch.getBatch)) { rightTab =>
        JoinImpl.leftSemiHashJoinBuildRight(
          leftKeysTab,
          rightKeysTab,
          leftTab,
          rightTab,
          compiledCondition,
          if (compareNullsEqual) NullEquality.EQUAL else NullEquality.UNEQUAL)
        }
    }
    withResource(result) { _ =>
      result.releaseLeft()
    }
  }

  private def unconditionalBatchLeftSemiJoin(
    leftKeysTab: Table,
    rightKeysTab: Table
  ): GatherMap = {
    withResource(JoinImpl.leftSemiHashJoinBuildRight(leftKeysTab, rightKeysTab,
      compareNullsEqual)) { result =>
      result.releaseLeft()
    }
  }

  override def existsScatterMap(leftColumnarBatch: ColumnarBatch): GatherMap = {
    NvtxIdWithMetrics(NvtxRegistry.EXISTENCE_JOIN_SCATTER_MAP, joinTime) {
      withResource(leftKeysTable(leftColumnarBatch)) { leftKeysTab =>
        withResource(rightKeysTable()) { rightKeysTab =>
          compiledConditionRes.map { compiledCondition =>
            conditionalBatchLeftSemiJoin(leftColumnarBatch, leftKeysTab, rightKeysTab,
              compiledCondition)
          }.getOrElse {
            unconditionalBatchLeftSemiJoin(leftKeysTab, rightKeysTab)
          }
        }
      }
    }
  }

  override def close(): Unit = {
    if (!closed) {
      super.close()
      lazyCompiledCondition.foreach(_.close())
    }
  }
}

trait GpuJoinExec extends ShimBinaryExecNode with GpuExec {
  def joinType: JoinType
  def condition: Option[Expression]
  def leftKeys: Seq[Expression]
  def rightKeys: Seq[Expression]
  def isSkewJoin: Boolean = false
}

trait GpuHashJoin extends GpuJoinExec {
  /**
   * The build side determines which side of the join is materialized/buffered/broadcast.
   * This is a data movement decision made at query planning time.
   *
   * IMPORTANT: This is distinct from the physical build side that the join algorithm may select
   * when buildSideSelection is AUTO or SMALLEST. When those strategies are used:
   * - `buildSide` still determines which side to materialize (e.g., which to broadcast)
   * - The join algorithm may independently choose which materialized data to use as the build
   *   table based on runtime row counts
   * - The join abstractions (GatherMapsResult) ensure correct semantics regardless of the
   *   algorithm's internal choice
   */
  def buildSide: GpuBuildSide

  protected lazy val (buildPlan, streamedPlan) = buildSide match {
    case GpuBuildLeft => (left, right)
    case GpuBuildRight => (right, left)
  }

  protected lazy val (buildKeys, streamedKeys) = {
    require(leftKeys.length == rightKeys.length &&
        leftKeys.map(_.dataType)
            .zip(rightKeys.map(_.dataType))
            .forall(types => types._1.sameType(types._2)),
      "Join keys from two sides should have same length and types")
    buildSide match {
      case GpuBuildLeft => (leftKeys, rightKeys)
      case GpuBuildRight => (rightKeys, leftKeys)
    }
  }

  override def output: Seq[Attribute] = GpuHashJoin.output(joinType, left.output, right.output)

  // If we have a single batch streamed in then we will produce a single batch of output
  // otherwise it can get smaller or bigger, we just don't know.  When we support out of
  // core joins this will change
  override def outputBatching: CoalesceGoal = {
    val batching = buildSide match {
      case GpuBuildLeft => GpuExec.outputBatching(right)
      case GpuBuildRight => GpuExec.outputBatching(left)
    }
    if (batching.isInstanceOf[RequireSingleBatchLike]) {
      RequireSingleBatch
    } else {
      null
    }
  }

  def dedupDataFromKeys(
      rightOutput: Seq[Attribute],
      rightKeys: Seq[Expression],
      leftKeys: Seq[Expression]): (Seq[Attribute], Seq[NamedExpression]) = {
    // This means that we need a mapping from what we remove on the right to what in leftData can
    // provide it. These are still symbolic references, so we are going to convert everything into
    // attributes, and use it to make out mapping.
    val leftKeyAttributes = GpuHashJoin.extractTopLevelAttributes(leftKeys, includeAlias = true)
    val rightKeyAttributes = GpuHashJoin.extractTopLevelAttributes(rightKeys, includeAlias = false)
    val zippedKeysMapping = rightKeyAttributes.zip(leftKeyAttributes)
    val rightToLeftKeyMap = zippedKeysMapping.filter {
      case (Some(_), Some(_: AttributeReference)) => true
      case _ => false
    }.map {
      case (Some(right), Some(left)) => (right.exprId, left)
      case _ => throw new IllegalStateException("INTERNAL ERROR THIS SHOULD NOT BE REACHABLE")
    }.toMap

    val rightData = rightOutput.filterNot(att => rightToLeftKeyMap.contains(att.exprId))
    val remappedRightOutput = rightOutput.map { att =>
      rightToLeftKeyMap.get(att.exprId)
          .map(leftAtt => GpuAlias(leftAtt, att.name)(att.exprId))
          .getOrElse(att)
    }
    (rightData, remappedRightOutput)
  }

  protected lazy val compareNullsEqual: Boolean =
    GpuHashJoin.compareNullsEqual(joinType, buildKeys)

  protected lazy val (boundBuildKeys, boundStreamKeys) = {
    val lkeys = GpuBindReferences.bindGpuReferences(leftKeys, left.output, allMetrics)
    val rkeys = GpuBindReferences.bindGpuReferences(rightKeys, right.output, allMetrics)

    buildSide match {
      case GpuBuildLeft => (lkeys, rkeys)
      case GpuBuildRight => (rkeys, lkeys)
    }
  }

  protected lazy val (numFirstConditionTableColumns, boundCondition) = {
    val (buildOutput, streamOutput) = buildSide match {
      case GpuBuildRight => (right.output, left.output)
      case GpuBuildLeft => (left.output, right.output)
    }
    val boundCondition = condition.map { c =>
      GpuBindReferences.bindGpuReference(c, streamOutput ++ buildOutput,
        allMetrics)
    }
    (streamOutput.size, boundCondition)
  }

  /**
   * Condition bound to left.output ++ right.output.
   * LazyCompiledCondition will transform it for BuildLeft by rewriting ordinals.
   */
  protected lazy val boundConditionLeftRight: Option[GpuExpression] = {
    condition.map { c =>
      GpuBindReferences.bindGpuReference(c, left.output ++ right.output, allMetrics)
    }
  }

  def doJoin(
      builtBatch: ColumnarBatch,
      stream: Iterator[ColumnarBatch],
      joinOptions: JoinOptions,
      numOutputRows: GpuMetric,
      numOutputBatches: GpuMetric,
      opTime: GpuMetric,
      joinTime: GpuMetric): Iterator[ColumnarBatch] = {

    val filterOutNull = GpuHashJoin.buildSideNeedsNullFilter(joinType, compareNullsEqual,
      buildSide, buildKeys)

    val nullFiltered = if (filterOutNull) {
      val sb = closeOnExcept(builtBatch)(
        SpillableColumnarBatch(_, SpillPriorities.ACTIVE_ON_DECK_PRIORITY))
      GpuHashJoin.filterNullsWithRetryAndClose(sb, boundBuildKeys)
    } else {
      builtBatch
    }

    val spillableBuiltBatch = withResource(nullFiltered) {
      LazySpillableColumnarBatch(_, "built")
    }

    val lazyStream = stream.map { cb =>
      withResource(cb) { cb =>
        LazySpillableColumnarBatch(cb, "stream_batch")
      }
    }

    // The HashJoinIterator takes ownership of the built keys and built data. It will close
    // them when it is done
    val joinIterator = joinType match {
      case ExistenceJoin(_) =>
        // Create a new LazyCompiledCondition for this iterator (it takes ownership)
        val lazyCond = boundConditionLeftRight.map { cond =>
          LazyCompiledCondition(cond, left.output.size, right.output.size)
        }
        new HashedExistenceJoinIterator(
          spillableBuiltBatch,
          boundBuildKeys,
          lazyStream,
          boundStreamKeys,
          lazyCond,
          buildSide,
          compareNullsEqual,
          opTime,
          joinTime)
      case FullOuter =>
        // Create a new LazyCompiledCondition for this iterator (it takes ownership)
        val lazyCond = boundConditionLeftRight.map { cond =>
          LazyCompiledCondition(cond, left.output.size, right.output.size)
        }
        new HashOuterJoinIterator(joinType, spillableBuiltBatch, boundBuildKeys, None, None,
          lazyStream, boundStreamKeys, streamedPlan.output,
          lazyCond, joinOptions, buildSide,
          compareNullsEqual, condition, opTime, joinTime)
      case _ =>
        if (boundConditionLeftRight.isDefined) {
          // ConditionalHashJoinIterator will close the LazyCompiledCondition
          // Create a new LazyCompiledCondition for this iterator (it takes ownership)
          val lazyCond = LazyCompiledCondition(
            boundConditionLeftRight.get,
            left.output.size,
            right.output.size)
          new ConditionalHashJoinIterator(spillableBuiltBatch, boundBuildKeys, None,
            lazyStream, boundStreamKeys, streamedPlan.output, lazyCond,
            joinOptions, joinType, buildSide,
            compareNullsEqual, condition, opTime, joinTime)
        } else {
          new HashJoinIterator(spillableBuiltBatch, boundBuildKeys, None,
            lazyStream, boundStreamKeys, streamedPlan.output, joinOptions,
            joinType, buildSide, compareNullsEqual, condition, opTime, joinTime)
        }
    }

    joinIterator.map { cb =>
      numOutputRows += cb.numRows()
      numOutputBatches += 1
      cb
    }
  }
}
