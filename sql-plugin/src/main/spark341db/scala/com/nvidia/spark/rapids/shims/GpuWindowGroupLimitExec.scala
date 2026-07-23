/*
 * Copyright (c) 2024-2026, NVIDIA CORPORATION.
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

/*** spark-rapids-shim-json-lines
{"spark": "341db"}
{"spark": "350"}
{"spark": "350db143"}
{"spark": "351"}
{"spark": "352"}
{"spark": "353"}
{"spark": "354"}
{"spark": "355"}
{"spark": "356"}
{"spark": "357"}
{"spark": "358"}
{"spark": "400"}
{"spark": "400db173"}
{"spark": "401"}
{"spark": "402"}
{"spark": "403"}
{"spark": "404"}
{"spark": "411"}
{"spark": "412"}
{"spark": "413"}
{"spark": "420"}
spark-rapids-shim-json-lines ***/

package com.nvidia.spark.rapids.shims

import ai.rapids.cudf
import ai.rapids.cudf.GroupByOptions
import com.nvidia.spark.rapids.{BaseExprMeta, DataFromReplacementRule, GpuBindReferences, GpuBoundReference, GpuColumnVector, GpuExec, GpuExpression, GpuMetric, GpuOverrides, GpuProjectExec, RapidsConf, RapidsMeta, SparkPlanMeta, SpillableColumnarBatch, SpillPriorities}
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.RmmRapidsRetryIterator.{splitSpillableInHalfByRows, withRetry}
import com.nvidia.spark.rapids.window.{GpuDenseRank, GpuRank, GpuRowNumber}

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Alias, And, Attribute, AttributeReference, DenseRank, EqualTo, Expression, GreaterThan, GreaterThanOrEqual, LessThan, LessThanOrEqual, Literal, NamedExpression, Rank, RowNumber, SortOrder, WindowExpression, WindowSpecDefinition}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.{FilterExec, SparkPlan}
import org.apache.spark.sql.execution.window.{Final, Partial, WindowExec, WindowGroupLimitExec, WindowGroupLimitMode}
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.vectorized.ColumnarBatch

sealed trait RankFunctionType
case object RankFunction extends RankFunctionType
case object DenseRankFunction extends RankFunctionType
case object RowNumberFunction extends RankFunctionType

class GpuWindowGroupLimitExecMeta(limitExec: WindowGroupLimitExec,
                                  conf: RapidsConf,
                                  parent: Option[RapidsMeta[_, _, _]],
                                  rule: DataFromReplacementRule)
    extends SparkPlanMeta[WindowGroupLimitExec](limitExec, conf, parent, rule) {

  private val partitionSpec: Seq[BaseExprMeta[Expression]] =
    limitExec.partitionSpec.map(GpuOverrides.wrapExpr(_, conf, Some(this)))

  private val orderSpec: Seq[BaseExprMeta[SortOrder]] =
    limitExec.orderSpec.map(GpuOverrides.wrapExpr(_, conf, Some(this)))

  private val rankFunction = GpuOverrides.wrapExpr(limitExec.rankLikeFunction, conf, Some(this))

  override def tagPlanForGpu(): Unit = {
    wrapped.rankLikeFunction match {
      case DenseRank(_) =>
      case Rank(_) =>
      case RowNumber() =>
      case _ => willNotWorkOnGpu("Only rank, dense_rank and row_number are " +
                                 "currently supported for window group limits")
    }

    wrapped.mode match {
      case Partial =>
      case Final =>
        // Check if this Final limit is redundant because it will be followed by
        // a WindowExec + FilterExec that does the same work
        if (conf.isWindowGroupLimitOptEnabled && isFinalLimitRedundant) {
          shouldBeRemoved("redundant Final WindowGroupLimit: subsequent WindowExec and " +
            "FilterExec will perform the same rank computation and filtering")
        }
      case _ => willNotWorkOnGpu("Unsupported WindowGroupLimitMode: " +
                                 s"${wrapped.mode.getClass.getName}")
    }
  }

  /**
   * Helper method to check if two sequences of expressions are semantically equal.
   */
  private def specsMatchSemantically[T <: Expression](
      left: Seq[T],
      right: Seq[T]): Boolean = {
    left.length == right.length &&
    left.zip(right).forall { case (l, r) => l.semanticEquals(r) }
  }

  /**
   * Helper method to find a parent plan of a specific type in the plan hierarchy.
   */
  private def findParentOfType[T <: SparkPlan : Manifest]: Option[T] = {
    parent.flatMap { p =>
      p.wrapped match {
        case plan: T => Some(plan)
        case _ => None
      }
    }
  }

  /**
   * Helper method to find a grandparent plan of a specific type in the plan hierarchy.
   */
  private def findGrandparentOfType[T <: SparkPlan : Manifest]: Option[T] = {
    parent.flatMap(_.parent).flatMap { gp =>
      gp.wrapped match {
        case plan: T => Some(plan)
        case _ => None
      }
    }
  }

  /**
   * Checks if both partition and order specs match semantically.
   */
  private def specsMatchCompletely(windowExec: WindowExec): Boolean = {
    specsMatchSemantically(windowExec.partitionSpec, limitExec.partitionSpec) &&
    specsMatchSemantically(windowExec.orderSpec, limitExec.orderSpec)
  }

  /**
   * Checks if this Final WindowGroupLimitExec is redundant.
   *
   * The Final limit is redundant when:
   * 1. The parent is a WindowExec that computes the same rank function with same specs
   * 2. The grandparent is a FilterExec that filters on the rank result
   *
   * When both conditions are met, the WindowExec + FilterExec will do the same
   * rank computation and filtering that this Final limit would do, making this
   * operator a no-op that we can skip.
   */
  private def isFinalLimitRedundant: Boolean = {
    findParentOfType[WindowExec] match {
      case Some(windowExec) => checkWindowExecRedundancy(windowExec)
      case None => false
    }
  }

  /**
   * Checks if the given WindowExec makes this Final limit redundant.
   * This is broken out as a separate method for clarity and to allow early returns.
   */
  private def checkWindowExecRedundancy(windowExec: WindowExec): Boolean = {
    // Check if WindowExec has matching partition and order specs
    if (!specsMatchCompletely(windowExec)) {
      return false
    }

    // Find the matching rank expression once and cache it
    val matchingRankExprOpt = findMatchingRankExpression(windowExec)
    if (matchingRankExprOpt.isEmpty) {
      return false
    }

    // Check if grandparent is a FilterExec
    val grandparentFilterOpt = findGrandparentOfType[FilterExec]
    if (grandparentFilterOpt.isEmpty) {
      return false
    }

    // For the optimization to be safe, we need a FilterExec as grandparent
    // The filter should reference the rank output and enforce the limit
    val (rankExprId, _) = matchingRankExprOpt.get
    WindowGroupLimitFilterMatcher.filterIsAtLeastAsRestrictive(
      grandparentFilterOpt.get.condition, rankExprId, limitExec.limit)
  }

  /**
   * Finds a window expression in the WindowExec that matches the rank function
   * in this WindowGroupLimitExec.
   *
   * Uses reflection to access WindowExec's window expressions because different Spark
   * versions use different method names:
   * - Apache Spark versions (3.5.0, 4.0.0, etc.) use `windowExpression`
   * - Databricks versions (3.4.1db, 3.5.0db143, etc.) use `projectList`
   *
   * This shim covers both Apache Spark and Databricks versions, so reflection is
   * necessary to handle both cases. This follows the same pattern as
   * GpuWindowExecMeta.getWindowExpression.
   *
   * @return Option of (expression id, rank function type) if found
   */
  private def findMatchingRankExpression(windowExec: WindowExec): 
      Option[(Long, Expression)] = {
    // Get window expressions from WindowExec via reflection
    val windowExprs = try {
      // Apache Spark versions use windowExpression
      val method = windowExec.getClass.getMethod("windowExpression")
      method.invoke(windowExec).asInstanceOf[Seq[NamedExpression]]
    } catch {
      case _: NoSuchMethodException =>
        try {
          // Databricks versions use projectList
          val method = windowExec.getClass.getMethod("projectList")
          method.invoke(windowExec).asInstanceOf[Seq[NamedExpression]]
        } catch {
          case _: NoSuchMethodException => return None
        }
    }

    // Find window expression with matching rank function
    windowExprs.collectFirst {
      case alias @ Alias(WindowExpression(rankFunc, spec: WindowSpecDefinition), _)
          if matchesRankFunction(rankFunc) && matchesWindowSpec(spec) =>
        (alias.exprId.id, rankFunc)
    }
  }

  /**
   * Checks if the given expression matches the rank function type in this limit.
   */
  private def matchesRankFunction(expr: Expression): Boolean = {
    (expr, limitExec.rankLikeFunction) match {
      case (_: Rank, _: Rank) => true
      case (_: DenseRank, _: DenseRank) => true
      case (_: RowNumber, _: RowNumber) => true
      case _ => false
    }
  }

  /**
   * Checks if the window spec matches the partition and order specs of this limit.
   */
  private def matchesWindowSpec(spec: WindowSpecDefinition): Boolean = {
    specsMatchSemantically(spec.partitionSpec, limitExec.partitionSpec) &&
    specsMatchSemantically(spec.orderSpec, limitExec.orderSpec)
  }

  override def convertToGpu(): GpuExec = {
    GpuWindowGroupLimitExec(partitionSpec.map(_.convertToGpu()),
                            orderSpec.map(_.convertToGpu().asInstanceOf[SortOrder]),
                            rankFunction.convertToGpu(),
                            limitExec.limit,
                            limitExec.mode,
                            childPlans.head.convertIfNeeded())
  }
}

class GpuWindowGroupLimitingIterator(input: Iterator[ColumnarBatch],
                                     boundPartitionSpec: Seq[GpuExpression],
                                     boundOrderSpec: Seq[SortOrder],
                                     rankFunction: RankFunctionType,
                                     limit: Int,
                                     numOutputBatches: GpuMetric,
                                     numOutputRows: GpuMetric)
  extends Iterator[ColumnarBatch]
  with Logging {

  override def hasNext: Boolean = input.hasNext

  // Caches input column schema on first read.
  private var inputTypes: Option[Array[DataType]] = None

  private val partByPositions: Array[Int] =
    boundPartitionSpec.map {
      _.asInstanceOf[GpuBoundReference].ordinal
    }.toArray
  private val sortColumns: Seq[Expression] = boundOrderSpec.map {
    _.child
  }

  private def readInputBatch: SpillableColumnarBatch = {
    def optionallySetInputTypes(inputCB: ColumnarBatch): Unit = {
      if (inputTypes.isEmpty) {
        inputTypes = Some(GpuColumnVector.extractTypes(inputCB))
      }
    }

    val inputCB = input.next()
    optionallySetInputTypes(inputCB)
    SpillableColumnarBatch(inputCB, SpillPriorities.ACTIVE_BATCHING_PRIORITY)
  }

  private var subIterator: Option[Iterator[ColumnarBatch]] = None

  override def next(): ColumnarBatch = {

    if (!hasNext) {
      throw new NoSuchElementException()
    }

    if (subIterator.exists(_.hasNext)) {
      subIterator.map(_.next).get
    }
    else {
      val iter = withRetry(readInputBatch, splitSpillableInHalfByRows) { spillableInputCB =>
        withResource(spillableInputCB.getColumnarBatch()) { inputCB =>
          val filterMap = withResource(calculateRank(rankFunction, inputCB)) { ranks =>
            withResource(cudf.Scalar.fromInt(limit)) { limitScalar =>
              // For a query with `WHERE rank < n`, the limit passed to the exec
              // is `n - 1`.  The comparison should be `LESS_EQUAL`.
              ranks.binaryOp(cudf.BinaryOp.LESS_EQUAL, limitScalar, cudf.DType.BOOL8)
            }
          }
          withResource(filterMap) { _ =>
            withResource(GpuColumnVector.from(inputCB)) { inputTable =>
              withResource(inputTable.filter(filterMap)) {
                GpuColumnVector.from(_, inputTypes.get)
              }
            }
          }
        }
      }
      val result = iter.next()
      subIterator = Some(iter)
      numOutputBatches += 1
      numOutputRows += result.numRows()
      result
    }
  }

  private def hasGroupingColumns: Boolean = partByPositions.nonEmpty

  private def getRankFunction(rankFunctionType: RankFunctionType) = {
    rankFunctionType match {
      case RankFunction => GpuRank(sortColumns)
      case DenseRankFunction => GpuDenseRank(sortColumns)
      case RowNumberFunction => GpuRowNumber
      case _ => throw new IllegalArgumentException("Unexpected ranking function")
    }
  }

  private def calculateRank(rankFunctionType: RankFunctionType,
                            inputCB: ColumnarBatch): cudf.ColumnVector = {
    if (hasGroupingColumns) {
      calculateRankWithGroupByScan(rankFunctionType, inputCB)
    }
    else {
      calculateRankWithScan(rankFunctionType, inputCB)
    }
  }

  private def extractSortColumn(inputCB: ColumnarBatch, sortColumnProjection: Seq[Expression]) =
    withResource(GpuProjectExec.project(inputCB, sortColumnProjection)) {
      sortColumnCB =>
        withResource(GpuColumnVector.from(sortColumnCB)) { sortColumnTable =>
          require(sortColumnTable.getNumberOfColumns == 1,
            "Expected single (consolidated) sort-by column.")
          sortColumnTable.getColumn(0).incRefCount()
        }
    }

  private def calculateRankWithGroupByScan(rankFunctionType: RankFunctionType,
                                           inputCB: ColumnarBatch): cudf.ColumnVector = {
    // For multiple order-by columns order-by columns, the group-scan's input projection
    // is a single STRUCT column (containing the order-by columns as members).
    val rankFunction = getRankFunction(rankFunctionType)
    val sortColumnProjection = rankFunction.groupByScanInputProjection(isRunningBatched = false)

    // Append the projected sort-column to the end of the input table.
    val gbyScanInputTable = withResource(GpuColumnVector.from(inputCB)) { inputTable =>
      withResource(extractSortColumn(inputCB, sortColumnProjection)) { sortColumn =>
        val columnsWithSortByAdded = Range(0, inputTable.getNumberOfColumns).map {
          inputTable.getColumn
        }.toArray :+ sortColumn
        new cudf.Table(columnsWithSortByAdded: _*)
      }
    }

    // Now, perform groupBy-scan:
    val sortColumnIndex = gbyScanInputTable.getNumberOfColumns - 1 // Last column.
    val gbyScanAggregation = rankFunction.groupByScanAggregation(isRunningBatched = false).head.agg
    val sortedGroupByOptions = GroupByOptions.builder().withKeysSorted(true).build
    withResource(gbyScanInputTable) { _ =>
      withResource(gbyScanInputTable.groupBy(sortedGroupByOptions, partByPositions: _*)
        .scan(gbyScanAggregation.onColumn(sortColumnIndex))) { gbyScanOutput =>
        // The last column should be the grouped ranks.
        gbyScanOutput.getColumn(gbyScanOutput.getNumberOfColumns - 1).incRefCount()
      }
    }
  }

  private def calculateRankWithScan(rankFunctionType: RankFunctionType,
                                    inputCB: ColumnarBatch): cudf.ColumnVector = {

    // For multiple order-by columns order-by columns, the group-scan's input projection
    // is a single STRUCT column (containing the order-by columns as members).
    val rankFunction = getRankFunction(rankFunctionType)
    val sortColumnProjection = rankFunction.scanInputProjection(isRunningBatched = false)
    val scanAggregation = rankFunction.scanAggregation(isRunningBatched = false).head.agg
    withResource(extractSortColumn(inputCB, sortColumnProjection)) { sortColumn =>
      sortColumn.scan(scanAggregation, cudf.ScanType.INCLUSIVE,
                      cudf.NullPolicy.INCLUDE)
    }
  }
}

/**
 * Analog of o.a.s.s.e.window.WindowGroupLimitExec.  Responsible for top-k limit push-down,
 * for queries that contain boolean filter predicates on the result of ranking window functions
 * such as `RANK()`, `DENSE_RANK()`, and `ROW_NUMBER()`.
 *
 * @see <a href="https://issues.apache.org/jira/browse/SPARK-37099">SPARK-37099</a>
 *
 * Consider this query:
 * <pre>{@code
 * SELECT * FROM (
 *   SELECT *, RANK() OVER (PARTITION BY a ORDER BY b) rnk
 *   FROM input_table
 * ) WHERE rnk < 5;
 * }</pre>
 *
 * This translates to the following plan:
 * <pre>
 * == Physical Plan ==
 * *(3) Filter (rnk#22 < 5)
 *      +- Window [rank(b#1) windowspecdefinition(a#0L, b#1 ASC NULLS FIRST,
 *                                                specifiedwindowframe(RowFrame,
                                                    unboundedpreceding$(),
 *                                                  currentrow$())) AS rnk#22],
 *                [a#0L],
 *                [b#1 ASC NULLS FIRST]
 *         +- WindowGroupLimit [a#0L], [b#1 ASC NULLS FIRST], rank(b#1), 4, Final
 *            +- *(2) Sort [a#0L ASC NULLS FIRST, b#1 ASC NULLS FIRST], false, 0
 *               +- Exchange hashpartitioning(a#0L, 1), ENSURE_REQUIREMENTS, [plan_id=113]
 *                  +- WindowGroupLimit [a#0L], [b#1 ASC NULLS FIRST], rank(b#1), 4, Partial
 *                     +- *(1) Sort [a#0L ASC NULLS FIRST, b#1 ASC NULLS FIRST], false, 0
 *                        +- *(1) ColumnarToRow
 *                           +- FileScan parquet [a#0L,b#1,c#2L] ...
 * </pre>
 *
 * WindowGroupLimitExec works by using an appropriate row-wise iterator to go row-by-row, keeping
 * track of the current rank, and skipping over the rows that do not satisfy the rank predicate
 * (i.e. "< 5").
 *
 * GpuWindowGroupLimitExec differs slightly from Apache Spark in that it cannot go row-by-row.
 * Instead, it calculates the ranks for all the rows in the column, together, and then filters out
 * any that do not satisfy the predicate.
 *
 * The performance speedup comes from two sources:
 * 1. Preventing `WindowGroupLimit` from falling off the GPU, thereby eliminating *two*
 *    row-column-row transpose operations.
 * 2. Basic rank computations via cudf.ColumnVector scan operations, which tend to be fast.
 *
 * Note: No "running window" optimizations are required.  Each column batch is filtered
 * independently. If the rank-limit is `5`, only 5 ranks per group are permitted per column batch.
 * The final window aggregation and the subsequent filter will produce the right result downstream,
 * but without the costly shuffles.
 */
case class GpuWindowGroupLimitExec(
    gpuPartitionSpec: Seq[Expression],
    gpuOrderSpec: Seq[SortOrder],
    rankLikeFunction: Expression,
    limit: Int,
    mode: WindowGroupLimitMode,
    child: SparkPlan) extends ShimUnaryExecNode with GpuExec {

  override def output: Seq[Attribute] = child.output

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  override def outputPartitioning: Partitioning = child.outputPartitioning

  private def getRankFunctionType(expr: Expression): RankFunctionType = expr match {
    case GpuRank(_) => RankFunction
    case GpuDenseRank(_) => DenseRankFunction
    case GpuRowNumber => RowNumberFunction
    case _ =>
      throw new UnsupportedOperationException("Only rank, dense_rank and row_number are " +
                                              "currently supported for group limits")
  }

  override protected def internalDoExecuteColumnar(): RDD[ColumnarBatch] = {
    val numOutputBatches = gpuLongMetric(GpuMetric.NUM_OUTPUT_BATCHES)
    val numOutputRows = gpuLongMetric(GpuMetric.NUM_OUTPUT_ROWS)

    val boundPartitionSpec = GpuBindReferences.bindGpuReferences(gpuPartitionSpec, child.output,
      allMetrics)
    val boundOrderSpec = GpuBindReferences.bindReferences(gpuOrderSpec, child.output, allMetrics)

    child.executeColumnar().mapPartitions { input =>
      new GpuWindowGroupLimitingIterator(input,
                                         boundPartitionSpec,
                                         boundOrderSpec,
                                         getRankFunctionType(rankLikeFunction),
                                         limit,
                                         numOutputBatches,
                                         numOutputRows)
    }
  }

  override protected def doExecute(): RDD[InternalRow] =
    throw new UnsupportedOperationException("Row-wise execution unsupported!")
}

/**
 * Utility object for checking if a filter condition on a rank column is at least
 * as restrictive as a WindowGroupLimit. This is extracted to allow unit testing.
 *
 * For the optimization to be safe, the filter must keep the same or fewer rows
 * than the WindowGroupLimit. The WindowGroupLimit keeps rows where rank <= limit.
 *
 * Supports patterns matching Spark's InferWindowGroupLimit.extractLimits:
 * - rank <= n, rank < n, rank = n
 * - n >= rank, n > rank, n = rank
 * - AND conditions (extracts minimum limit)
 */
object WindowGroupLimitFilterMatcher {

  /**
   * Checks if the given filter condition limits the rank column to at most `limit` rows.
   *
   * @param condition The filter condition expression
   * @param rankExprId The expression ID of the rank column
   * @param limit The WindowGroupLimit's limit value
   * @return true if the filter is at least as restrictive as rank <= limit
   */
  def filterIsAtLeastAsRestrictive(
      condition: Expression,
      rankExprId: Long,
      limit: Int): Boolean = {

    // Check if expr IS the rank column directly (not just contains it).
    // Spark's InferWindowGroupLimit uses semanticEquals(attr), which checks identity.
    // Using references.exists would incorrectly match COALESCE(rank, 0) or CAST(rank).
    def isRankColumn(expr: Expression): Boolean = expr match {
      case attr: AttributeReference => attr.exprId.id == rankExprId
      case _ => false
    }

    def extractLiteralInt(expr: Expression): Option[Int] = expr match {
      case Literal(value: Int, _) => Some(value)
      case Literal(value: Long, _) if value.isValidInt => Some(value.toInt)
      case _ => None
    }

    /**
     * Extract the effective limit from a single predicate.
     * Returns Some(effectiveLimit) if this predicate limits the rank, None otherwise.
     *
     * Following Spark's InferWindowGroupLimit.extractLimits logic:
     * - EqualTo: limit = value (keeps only that rank)
     * - LessThan: limit = value - 1 (rank < n means rank <= n-1)
     * - LessThanOrEqual: limit = value
     * - GreaterThan (reversed): limit = value - 1
     * - GreaterThanOrEqual (reversed): limit = value
     */
    def extractLimit(predicate: Expression): Option[Int] = predicate match {
      // rank = n: keeps only rank n. Effective limit is n.
      case EqualTo(left, right) if isRankColumn(left) =>
        extractLiteralInt(right)
      case EqualTo(left, right) if isRankColumn(right) =>
        extractLiteralInt(left)

      // rank <= n: keeps ranks 1..n. Effective limit is n.
      case LessThanOrEqual(left, right) if isRankColumn(left) =>
        extractLiteralInt(right)

      // rank < n: equivalent to rank <= n-1. Effective limit is n-1.
      case LessThan(left, right) if isRankColumn(left) =>
        extractLiteralInt(right).map(_ - 1)

      // n >= rank: same as rank <= n. Effective limit is n.
      case GreaterThanOrEqual(left, right) if isRankColumn(right) =>
        extractLiteralInt(left)

      // n > rank: same as rank < n, equivalent to rank <= n-1. Effective limit is n-1.
      case GreaterThan(left, right) if isRankColumn(right) =>
        extractLiteralInt(left).map(_ - 1)

      case _ => None
    }

    /**
     * Split a condition into conjunctive predicates (AND conditions).
     * For example: "a AND b AND c" becomes Seq(a, b, c)
     * A simple condition "a" becomes Seq(a)
     */
    def splitConjunctivePredicates(cond: Expression): Seq[Expression] = {
      cond match {
        case And(left, right) =>
          splitConjunctivePredicates(left) ++ splitConjunctivePredicates(right)
        case other => Seq(other)
      }
    }

    // Split the filter condition into conjunctive predicates and extract limits
    val predicates = splitConjunctivePredicates(condition)
    val limits = predicates.flatMap(extractLimit)

    // If we found any rank-limiting predicates, use the minimum (most restrictive)
    // The filter is safe if this minimum limit is <= the WindowGroupLimit's limit
    if (limits.nonEmpty) {
      limits.min <= limit
    } else {
      false
    }
  }
}
