/*
 * Copyright (c) 2020-2026, NVIDIA CORPORATION.
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

import ai.rapids.cudf
import ai.rapids.cudf.{ast, GatherMap, OutOfBoundsPolicy, Scalar, Table}
import ai.rapids.cudf.ast.CompiledExpression
import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.RapidsPluginImplicits.AutoCloseableProducingArray
import com.nvidia.spark.rapids.RmmRapidsRetryIterator.{withRestoreOnRetry, withRetry, withRetryNoSplit}
import com.nvidia.spark.rapids.shims.{GpuBroadcastJoinMeta, ShimBinaryExecNode}

import org.apache.spark.TaskContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, NamedExpression}
import org.apache.spark.sql.catalyst.plans.{ExistenceJoin, FullOuter, InnerLike, JoinType, LeftAnti, LeftExistence, LeftOuter, LeftSemi, RightOuter}
import org.apache.spark.sql.catalyst.plans.physical.{BroadcastDistribution, Distribution, IdentityBroadcastMode, Partitioning, UnspecifiedDistribution}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.BroadcastQueryStageExec
import org.apache.spark.sql.execution.exchange.ReusedExchangeExec
import org.apache.spark.sql.execution.joins.BroadcastNestedLoopJoinExec
import org.apache.spark.sql.types.{BooleanType, DataType, StructType}
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}

abstract class GpuBroadcastNestedLoopJoinMetaBase(
    join: BroadcastNestedLoopJoinExec,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
    extends GpuBroadcastJoinMeta[BroadcastNestedLoopJoinExec](join, conf, parent, rule) {

  val conditionMeta: Option[BaseExprMeta[_]] =
    join.condition.map(GpuOverrides.wrapExpr(_, conf, Some(this)))

  val gpuBuildSide: GpuBuildSide = GpuJoinUtils.getGpuBuildSide(join.buildSide)

  private var taggedForAstCheck = false

  // Avoid checking multiple times
  private var isAstCond = false

  /**
   * Check whether condition can be ast-able. It includes two cases: 1) all join conditions are
   * ast-able; 2) join conditions are ast-able after split and push down to child plans.
   */
  protected def canJoinCondAstAble(): Boolean = {
    if (!taggedForAstCheck) {
      val Seq(leftPlan, rightPlan) = childPlans
      conditionMeta match {
        case Some(e) => isAstCond = AstUtil.canExtractNonAstConditionIfNeed(
          e, leftPlan.outputAttributes.map(_.exprId), rightPlan.outputAttributes.map(_.exprId))
        case None => isAstCond = true
      }
      taggedForAstCheck = true
    }
    isAstCond
  }

  override def namedChildExprs: Map[String, Seq[BaseExprMeta[_]]] =
    JoinTypeChecks.nonEquiJoinMeta(conditionMeta)

  override val childExprs: Seq[BaseExprMeta[_]] = conditionMeta.toSeq

  override protected def runChildExprBridgeOptimization(): Unit = {
    conditionMeta.foreach { cond =>
      val leftExprIds = join.left.output.map(_.exprId)
      val rightExprIds = join.right.output.map(_.exprId)
      if (AstUtil.canExtractNonAstConditionIfNeed(cond, leftExprIds, rightExprIds)) {
        GpuCpuBridgeOptimizer.checkAndOptimizeNonAstSubtrees(cond)
      } else {
        // Inner joins can consume a bridged post-filter. Joins that require an AST condition
        // will call requireAstForGpuOn later and reject GPU execution if the bridge remains.
        GpuCpuBridgeOptimizer.checkAndOptimizeExpressionMetas(Seq(cond))
      }
    }
  }

  override def tagPlanForGpu(): Unit = {
    JoinTypeChecks.tagForGpu(join.joinType, this)
    join.joinType match {
      case _: InnerLike =>
      case LeftOuter | RightOuter | LeftSemi | LeftAnti | ExistenceJoin(_) =>
        // First to check whether can be split if not ast-able. If false, then check requireAst to
        // send not-work-on-GPU reason if not replace-able.
        conditionMeta.foreach(cond => if (!canJoinCondAstAble()) requireAstForGpuOn(cond))
      case _ => willNotWorkOnGpu(s"${join.joinType} currently is not supported")
    }
    join.joinType match {
      case LeftOuter | LeftSemi | LeftAnti if gpuBuildSide == GpuBuildLeft =>
        willNotWorkOnGpu(s"build left not supported for ${join.joinType}")
      case RightOuter if gpuBuildSide == GpuBuildRight =>
        willNotWorkOnGpu(s"build right not supported for ${join.joinType}")
      case _ =>
    }

    val Seq(leftPlan, rightPlan) = childPlans
    val buildSide = gpuBuildSide match {
      case GpuBuildLeft => leftPlan
      case GpuBuildRight => rightPlan
    }

    if (!canBuildSideBeReplaced(buildSide)) {
      willNotWorkOnGpu("the broadcast for this join must be on the GPU too")
    }

    if (!canThisBeReplaced) {
      buildSide.willNotWorkOnGpu(
        "the BroadcastNestedLoopJoin this feeds is not on the GPU")
    }
  }

  // Called in runAfterTagRules for a special post tagging for this broadcast join.
  def checkTagForBuildSide(): Unit = {
    val Seq(leftChild, rightChild) = childPlans
    val buildSideMeta = gpuBuildSide match {
      case GpuBuildLeft => leftChild
      case GpuBuildRight => rightChild
    }
    // Check both of the conditions to avoid duplicate reason string.
    if (!canThisBeReplaced && canBuildSideBeReplaced(buildSideMeta)) {
      buildSideMeta.willNotWorkOnGpu("the BroadcastNestedLoopJoin this feeds is not on the GPU")
    }
    if (canThisBeReplaced && !canBuildSideBeReplaced(buildSideMeta)) {
      willNotWorkOnGpu("the broadcast for this join must be on the GPU too")
    }
  }
}

/**
 * An iterator that does a cross join against a stream of batches.
 */
class CrossJoinIterator(
    builtBatch: LazySpillableColumnarBatch,
    stream: Iterator[LazySpillableColumnarBatch],
    targetSize: Long,
    sizeEstimateThreshold: Double,
    buildSide: GpuBuildSide,
    opTime: GpuMetric,
    joinTime: GpuMetric)
    extends AbstractGpuJoinIterator(
      NvtxRegistry.JOIN_GATHER,
      targetSize,
      sizeEstimateThreshold,
      opTime,
      joinTime) {
  override def close(): Unit = {
    if (!closed) {
      super.close()
      builtBatch.close()
    }
  }

  override def hasNextStreamBatch: Boolean = stream.hasNext

  override def setupNextGatherer(): Option[JoinGatherer] = {
    val streamBatch = stream.next()

    // Don't include stream in op time.
    opTime.ns {
      // Don't close the built side because it will be used for each stream and closed
      // when the iterator is done.
      val (leftBatch, rightBatch) = buildSide match {
        case GpuBuildLeft => (LazySpillableColumnarBatch.spillOnly(builtBatch), streamBatch)
        case GpuBuildRight => (streamBatch, LazySpillableColumnarBatch.spillOnly(builtBatch))
      }

      val leftMap = LazySpillableGatherMap.leftCross(leftBatch.numRows, rightBatch.numRows)
      val rightMap = LazySpillableGatherMap.rightCross(leftBatch.numRows, rightBatch.numRows)

      // Cross joins do not need to worry about bounds checking because the gather maps
      // are generated using mod and div based on the number of rows on the left and
      // right, so we specify here `DONT_CHECK` for all.
      val joinGatherer = (leftBatch.numCols, rightBatch.numCols) match {
        case (_, 0) =>
          rightBatch.close()
          rightMap.close()
          JoinGatherer(leftMap, leftBatch, OutOfBoundsPolicy.DONT_CHECK)
        case (0, _) =>
          leftBatch.close()
          leftMap.close()
          JoinGatherer(rightMap, rightBatch, OutOfBoundsPolicy.DONT_CHECK)
        case (_, _) =>
          JoinGatherer(leftMap, leftBatch, rightMap, rightBatch,
            OutOfBoundsPolicy.DONT_CHECK, OutOfBoundsPolicy.DONT_CHECK)
      }
      if (joinGatherer.isDone) {
        joinGatherer.close()
        None
      } else {
        Some(joinGatherer)
      }
    }
  }
}

class ConditionalNestedLoopJoinIterator(
    joinType: JoinType,
    buildSide: GpuBuildSide,
    builtBatch: LazySpillableColumnarBatch,
    stream: Iterator[LazySpillableColumnarBatch],
    streamAttributes: Seq[Attribute],
    targetSize: Long,
    sizeEstimateThreshold: Double,
    condition: ast.CompiledExpression,
    opTime: GpuMetric,
    joinTime: GpuMetric)
    extends SplittableJoinIterator(
      NvtxRegistry.JOIN_GATHER,
      stream,
      streamAttributes,
      builtBatch,
      targetSize,
      sizeEstimateThreshold,
      opTime = opTime,
      joinTime = joinTime) {
  override def close(): Unit = {
    if (!closed) {
      super.close()
      condition.close()
    }
  }

  override def computeNumJoinRows(scb: LazySpillableColumnarBatch): Long = {
    scb.checkpoint()
    builtBatch.checkpoint()
    withRetryNoSplit {
      withRestoreOnRetry(Seq(builtBatch, scb)) {
        withResource(GpuColumnVector.from(builtBatch.getBatch)) { builtTable =>
          withResource(GpuColumnVector.from(scb.getBatch)) { streamTable =>
            val (left, right) = buildSide match {
              case GpuBuildLeft => (builtTable, streamTable)
              case GpuBuildRight => (streamTable, builtTable)
            }
            joinType match {
              case _: InnerLike => left.conditionalInnerJoinRowCount(right, condition)
              case LeftOuter => left.conditionalLeftJoinRowCount(right, condition)
              case RightOuter => right.conditionalLeftJoinRowCount(left, condition)
              case LeftSemi => left.conditionalLeftSemiJoinRowCount(right, condition)
              case LeftAnti => left.conditionalLeftAntiJoinRowCount(right, condition)
              case _ => throw new IllegalStateException(s"Unsupported join type $joinType")
            }
          }
        }
      }
    }
  }

  override def createGatherer(
      cb: LazySpillableColumnarBatch,
      numJoinRows: Option[Long]): Option[JoinGatherer] = {
    if (numJoinRows.contains(0)) {
      // nothing matched
      return None
    }
    // cb will be closed by the caller, so use a spill-only version here
    val spillOnlyCb = LazySpillableColumnarBatch.spillOnly(cb)
    val batches = Seq(builtBatch, spillOnlyCb)
    batches.foreach(_.checkpoint())
    withRetryNoSplit {
      withRestoreOnRetry(batches) {
        withResource(GpuColumnVector.from(builtBatch.getBatch)) { builtTable =>
          withResource(GpuColumnVector.from(cb.getBatch)) { streamTable =>
          // We need a new LSCB that will be taken over by the gatherer, or closed
          closeOnExcept(LazySpillableColumnarBatch(spillOnlyCb.getBatch, "stream_data")) {
              streamBatch =>
                val builtSpillOnly = LazySpillableColumnarBatch.spillOnly(builtBatch)
                val (leftTable, leftBatch, rightTable, rightBatch) = buildSide match {
                  case GpuBuildLeft => (builtTable, builtSpillOnly, streamTable, streamBatch)
                  case GpuBuildRight => (streamTable, streamBatch, builtTable, builtSpillOnly)
                }
                val maps = computeGatherMaps(leftTable, rightTable, numJoinRows)
                makeGatherer(maps, leftBatch, rightBatch, joinType)
            }
          }
        }
      }
    }
  }

  private def computeGatherMaps(
      left: Table,
      right: Table,
      numJoinRows: Option[Long]): GatherMapsResult = {
    joinType match {
      case _: InnerLike =>
        val arraysRet = numJoinRows.map { rowCount =>
          left.conditionalInnerJoinGatherMaps(right, condition, rowCount)
        }.getOrElse {
          left.conditionalInnerJoinGatherMaps(right, condition)
        }
        GatherMapsResult(arraysRet(0), arraysRet(1))
      case LeftOuter =>
        val arraysRet = numJoinRows.map { rowCount =>
          left.conditionalLeftJoinGatherMaps(right, condition, rowCount)
        }.getOrElse {
          left.conditionalLeftJoinGatherMaps(right, condition)
        }
        GatherMapsResult(arraysRet(0), arraysRet(1))
      case RightOuter =>
        // switch the inputs to the join so we can use left join for right
        val arraysRet = numJoinRows.map { rowCount =>
          right.conditionalLeftJoinGatherMaps(left, condition, rowCount)
        }.getOrElse {
          right.conditionalLeftJoinGatherMaps(left, condition)
        }
        // Then switch them back
        GatherMapsResult(arraysRet(1), arraysRet(0))
      case LeftSemi =>
        val leftGather = numJoinRows.map { rowCount =>
          left.conditionalLeftSemiJoinGatherMap(right, condition, rowCount)
        }.getOrElse {
          left.conditionalLeftSemiJoinGatherMap(right, condition)
        }
        GatherMapsResult.makeFromLeft(leftGather)
      case LeftAnti =>
        val leftGatherer = numJoinRows.map { rowCount =>
          left.conditionalLeftAntiJoinGatherMap(right, condition, rowCount)
        }.getOrElse {
          left.conditionalLeftAntiJoinGatherMap(right, condition)
        }
        GatherMapsResult.makeFromLeft(leftGatherer)
      case _ => throw new IllegalStateException(s"Unsupported join type $joinType")
    }
  }
}

object GpuBroadcastNestedLoopJoinExecBase {
  def nestedLoopJoin(
      joinType: JoinType,
      buildSide: GpuBuildSide,
      numFirstTableColumns: Int,
      builtBatch: LazySpillableColumnarBatch,
      stream: Iterator[LazySpillableColumnarBatch],
      streamAttributes: Seq[Attribute],
      targetSize: Long,
      sizeEstimateThreshold: Double,
      boundCondition: Option[GpuExpression],
      numOutputRows: GpuMetric,
      numOutputBatches: GpuMetric,
      opTime: GpuMetric,
      joinTime: GpuMetric): Iterator[ColumnarBatch] = {
    val joinIterator = if (boundCondition.isEmpty) {
      // Semi and anti nested loop joins without a condition are degenerate joins and should have
      // been handled at a higher level rather than calling this method.
      assert(joinType.isInstanceOf[InnerLike], s"Unexpected unconditional join type: $joinType")
      new CrossJoinIterator(builtBatch, stream, targetSize, sizeEstimateThreshold, buildSide,
        opTime, joinTime)
    } else {
      if (joinType.isInstanceOf[ExistenceJoin]) {
        if (builtBatch.numCols == 0) {
          degenerateExistsJoinIterator(stream, builtBatch, boundCondition.get)
        } else {
          val compiledAst = boundCondition.get.convertToAst(numFirstTableColumns).compile()
          new ConditionalNestedLoopExistenceJoinIterator(
            builtBatch, stream, compiledAst, opTime, joinTime)
        }
      } else {
        if (isDegenerateLeftOuterJoin(joinType, buildSide, builtBatch, streamAttributes)) {
          degenerateLeftOuterJoinIterator(stream, streamAttributes, builtBatch,
            boundCondition.get)
        } else {
          val compiledAst = boundCondition.get.convertToAst(numFirstTableColumns).compile()
          new ConditionalNestedLoopJoinIterator(joinType, buildSide, builtBatch,
            stream, streamAttributes, targetSize, sizeEstimateThreshold, compiledAst,
            opTime = opTime, joinTime = joinTime)
        }
      }
    }
    joinIterator.map { cb =>
        numOutputRows += cb.numRows()
        numOutputBatches += 1
        cb
    }
  }

  // A degenerate outer join means either the build or stream side has no columns.
  // This handles degenerate cases for left-outer join with BuildRight and
  // right-outer join with BuildLeft (which are semantically equivalent - both
  // keep all stream rows and add nulls from build when no match).
  private def isDegenerateLeftOuterJoin(joinType: JoinType, side: GpuBuildSide,
      builtBatch: LazySpillableColumnarBatch, streamAtts: Seq[Attribute]): Boolean = {
    joinType match {
      case LeftOuter if side == GpuBuildRight =>
        // build or stream has no columns
        builtBatch.numCols == 0 || streamAtts.isEmpty
      case RightOuter if side == GpuBuildLeft =>
        // build or stream has no columns
        builtBatch.numCols == 0 || streamAtts.isEmpty
      case _ => false
    }
  }

  private def degenerateLeftOuterJoinIterator(
      stream: Iterator[LazySpillableColumnarBatch],
      streamAttrs: Seq[Attribute],
      builtBatch: LazySpillableColumnarBatch,
      boundCondition: GpuExpression): Iterator[ColumnarBatch] = {
    // Supports both LeftOuter+BuildRight and RightOuter+BuildLeft.
    // In both cases, all stream rows are kept with nulls from build when no match.
    if (streamAttrs.isEmpty) { // Stream has no columns
      new Iterator[ColumnarBatch] with TaskAutoCloseableResource {
        override def hasNext: Boolean = stream.hasNext

        override def next(): ColumnarBatch = {
          withResource(stream.next()) { streamSpill =>
            repeat(streamSpill.numRows)
          }
        }

        // Since the stream has no columns, so the matched rows from the build side
        // are always the same for all the stream rows. Cache it.
        private val rightMatchedCb: LazySpillableColumnarBatch = use({
          val filteredBatch = withRetryNoSplit(builtBatch) { _ =>
            GpuFilter(builtBatch.getBatch, boundCondition, NoopMetric, NoopMetric, NoopMetric)
          }
          val matchedBatch = if (filteredBatch.numRows() <= 0) {
            // no match rows, return one null row
            filteredBatch.close()
            val oneNullCols = builtBatch.dataTypes.safeMap { tp =>
              GpuColumnVector.fromNull(1, tp)
            }
            new ColumnarBatch(oneNullCols.asInstanceOf[Array[ColumnVector]], 1)
          } else {
            filteredBatch
          }
          withResource(matchedBatch) { _ =>
            LazySpillableColumnarBatch(matchedBatch, "degen nested-loop matched batch")
          }
        })

        private val repeat: Int => ColumnarBatch = {
          if (rightMatchedCb.numCols > 0) {
            (numRows) => withRetryNoSplit[ColumnarBatch] {
              withResource(GpuColumnVector.from(rightMatchedCb.getBatch)) { matchedTbl =>
                // no columns in stream, so the output is equal to repeating the matched
                // built table "numRows" (the rows number of a stream batch) times.
                withResource(matchedTbl.repeat(numRows)) { tbl =>
                  rightMatchedCb.allowSpilling()
                  GpuColumnVector.from(tbl, rightMatchedCb.dataTypes)
                }
              }
            }
          } else { // no columns
            (numRows) => new ColumnarBatch(Array(), (numRows * rightMatchedCb.numRows))
          }
        }
      }
    } else { // (streamAttrs.nonEmpty && builtBatch.numCols == 0), aka build has no columns.
      stream.map { scb =>
        withRetryNoSplit(scb) { _ =>
          // Build has no columns (but may have rows), so each matched stream row will
          // match "builtBatch.numRows" times. And keep the notmatched rows unchanged,
          // aka 1 for the repeat count.
          val repeatCnts = if (builtBatch.numRows == 0) {
            // Build side has no rows, so all stream rows should appear once (unmatched)
            withResource(Scalar.fromInt(1)) { oneScalar =>
              cudf.ColumnVector.fromScalar(oneScalar, scb.numRows)
            }
          } else {
            withResource(boundCondition.columnarEval(scb.getBatch)) { matched =>
              withResource(Scalar.fromInt(builtBatch.numRows)) { matchedRowsCnt =>
                withResource(Scalar.fromInt(1)) { notmatchedRowsCnt =>
                  matched.getBase.ifElse(matchedRowsCnt, notmatchedRowsCnt)
                }
              }
            }
          }
          val retTbl = withResource(repeatCnts) { _ =>
            withResource(GpuColumnVector.from(scb.getBatch))(_.repeat(repeatCnts))
          }
          withResource(retTbl)(_ => GpuColumnVector.from(retTbl, scb.dataTypes))
        }
      }
    }
  }

  private def degenerateExistsJoinIterator(
      stream: Iterator[LazySpillableColumnarBatch],
      builtBatch: LazySpillableColumnarBatch,
      boundCondition: GpuExpression): Iterator[ColumnarBatch] = {
    new Iterator[ColumnarBatch] {
      override def hasNext: Boolean = stream.hasNext

      override def next(): ColumnarBatch = {
        withResource(stream.next()) { streamSpillable =>
          val streamBatch = streamSpillable.getBatch
          val existsCol: ColumnVector = if (builtBatch.numRows == 0) {
            withResource(Scalar.fromBool(false)) { falseScalar =>
              GpuColumnVector.from(
                cudf.ColumnVector.fromScalar(falseScalar, streamBatch.numRows),
                BooleanType)
            }
          } else {
            withResource(boundCondition.columnarEval(streamBatch)) { condEval =>
              withResource(Scalar.fromBool(false)) { falseScalar =>
                GpuColumnVector.from(condEval.getBase.replaceNulls(falseScalar), BooleanType)
              }
            }
          }
          withResource(new ColumnarBatch(Array(existsCol), streamBatch.numRows)) { existsBatch =>
            GpuColumnVector.combineColumns(streamBatch, existsBatch)
          }
        }
      }
    }
  }

  def output(joinType: JoinType, left: Seq[Attribute], right: Seq[Attribute]): Seq[Attribute] = {
    joinType match {
      case _: InnerLike => left ++ right
      case LeftOuter => left ++ right.map(_.withNullability(true))
      case RightOuter => left.map(_.withNullability(true)) ++ right
      case FullOuter =>
        left.map(_.withNullability(true)) ++ right.map(_.withNullability(true))
      case j: ExistenceJoin => left :+ j.exists
      case LeftExistence(_) => left
      case x =>
        throw new IllegalArgumentException(
          s"BroadcastNestedLoopJoin should not take $x as the JoinType")
    }
  }

  def divideIntoBatches(
      rowCounts: RDD[Long],
      targetSizeBytes: Long,
      numOutputRows: GpuMetric,
      numOutputBatches: GpuMetric): RDD[ColumnarBatch] = {
    // Hash aggregate explodes the rows out, so if we go too large
    // it can blow up. The size of a Long is 8 bytes so we just go with
    // that as our estimate, no nulls.
    val maxRowCount = targetSizeBytes / 8

    def divideIntoBatches(rows: Long): Iterable[ColumnarBatch] = {
      val numBatches = (rows + maxRowCount - 1) / maxRowCount
      (0L until numBatches).map(i => {
        val ret = new ColumnarBatch(new Array[ColumnVector](0))
        if ((i + 1) * maxRowCount > rows) {
          ret.setNumRows((rows - (i * maxRowCount)).toInt)
        } else {
          ret.setNumRows(maxRowCount.toInt)
        }
        numOutputRows += ret.numRows()
        numOutputBatches += 1
        // grab the semaphore for downstream processing
        GpuSemaphore.acquireIfNecessary(TaskContext.get())
        ret
      })
    }

    rowCounts.flatMap(divideIntoBatches)
  }
}

// postBuildCondition is the post-broadcast project condition. It's used to re-construct a tiered
// project to handle pre-built batch. It will be removed after code refactor to decouple
// broadcast and nested loop join.
abstract class GpuBroadcastNestedLoopJoinExecBase(
    left: SparkPlan,
    right: SparkPlan,
    joinType: JoinType,
    gpuBuildSide: GpuBuildSide,
    condition: Option[Expression],
    postBuildCondition: List[NamedExpression],
    targetSizeBytes: Long) extends ShimBinaryExecNode with GpuExec {

  import GpuMetric._

  override protected def doExecute(): RDD[InternalRow] =
    throw new IllegalStateException("This should only be called from columnar")

  override val outputRowsLevel: MetricsLevel = ESSENTIAL_LEVEL
  override val outputBatchesLevel: MetricsLevel = MODERATE_LEVEL
  override lazy val additionalMetrics: Map[String, GpuMetric] = Map(
    OP_TIME_LEGACY -> createNanoTimingMetric(DEBUG_LEVEL, DESCRIPTION_OP_TIME_LEGACY),
    BUILD_DATA_SIZE -> createSizeMetric(MODERATE_LEVEL, DESCRIPTION_BUILD_DATA_SIZE),
    BUILD_TIME -> createNanoTimingMetric(MODERATE_LEVEL, DESCRIPTION_BUILD_TIME),
    JOIN_TIME -> createNanoTimingMetric(DEBUG_LEVEL, DESCRIPTION_JOIN_TIME),
    CPU_BRIDGE_PROCESSING_TIME -> createNanoTimingMetric(DEBUG_LEVEL, 
      DESCRIPTION_CPU_BRIDGE_PROCESSING_TIME),
    CPU_BRIDGE_WAIT_TIME -> createNanoTimingMetric(DEBUG_LEVEL, 
      DESCRIPTION_CPU_BRIDGE_WAIT_TIME))

  /** BuildRight means the right relation <=> the broadcast relation. */
  val (streamed, buildPlan) = gpuBuildSide match {
    case GpuBuildRight => (left, right)
    case GpuBuildLeft => (right, left)
  }

  def broadcastExchange: GpuBroadcastExchangeExecBase = getBroadcastPlan(buildPlan) match {
    case bqse: BroadcastQueryStageExec if bqse.plan.isInstanceOf[GpuBroadcastExchangeExecBase] =>
      bqse.plan.asInstanceOf[GpuBroadcastExchangeExecBase]
    case bqse: BroadcastQueryStageExec if bqse.plan.isInstanceOf[ReusedExchangeExec] =>
      bqse.plan.asInstanceOf[ReusedExchangeExec].child.asInstanceOf[GpuBroadcastExchangeExecBase]
    case gpu: GpuBroadcastExchangeExecBase => gpu
    case reused: ReusedExchangeExec => reused.child.asInstanceOf[GpuBroadcastExchangeExecBase]
  }

  private[this] def getBroadcastPlan(plan: SparkPlan): SparkPlan = {
    plan match {
      // In case has post broadcast project. It happens when join condition contains non-AST
      // expression which results in a project right after broadcast.
      case plan: GpuProjectExec => plan.child
      case _ => plan
    }
  }

  override def requiredChildDistribution: Seq[Distribution] = gpuBuildSide match {
    case GpuBuildLeft =>
      BroadcastDistribution(IdentityBroadcastMode) :: UnspecifiedDistribution :: Nil
    case GpuBuildRight =>
      UnspecifiedDistribution :: BroadcastDistribution(IdentityBroadcastMode) :: Nil
  }

  // Match Spark's BroadcastNestedLoopJoinExec.outputPartitioning
  override def outputPartitioning: Partitioning = (joinType, gpuBuildSide) match {
    case (_: InnerLike, _) | (LeftOuter, GpuBuildRight) | (RightOuter, GpuBuildLeft) |
         (LeftSemi, GpuBuildRight) | (LeftAnti, GpuBuildRight) =>
      streamed.outputPartitioning
    case _ => super.outputPartitioning
  }

  override def output: Seq[Attribute] = {
    GpuBroadcastNestedLoopJoinExecBase.output(joinType, left.output, right.output)
  }

  protected def computeBroadcastBuildRowCount(
      broadcastRelation: Broadcast[Any],
      buildTime: GpuMetric,
      buildDataSize: GpuMetric): Int = {
    NvtxIdWithMetrics(NvtxRegistry.BUILD_JOIN_TABLE, buildTime) {
      buildDataSize += 0
      GpuBroadcastHelper.getBroadcastBatchNumRows(broadcastRelation)
    }
  }

  protected def computeBuildRowCount(
      relation: Any,
      buildTime: GpuMetric,
      buildDataSize: GpuMetric): Int = {
    // NOTE: pattern matching doesn't work here because of type-invariance
    val broadcastRelation = relation.asInstanceOf[Broadcast[Any]]
    computeBroadcastBuildRowCount(broadcastRelation, buildTime, buildDataSize)
  }

  protected def getBroadcastRelation(): Any = {
    broadcastExchange.executeColumnarBroadcast[Any]()
  }

  private def isUnconditionalJoin(condition: Option[GpuExpression]): Boolean = {
    condition.forall {
      case GpuLiteral(true, BooleanType) =>
        // Spark can generate a degenerate conditional join when the join keys are constants
        true
      case GpuAlias(e: GpuExpression, _) => isUnconditionalJoin(Some(e))
      case _ => false
    }
  }

  override def internalDoExecuteColumnar(): RDD[ColumnarBatch] = {
    // Determine which table will be first in the join and bind the references accordingly
    // so the AST column references match the appropriate table.
    val (firstTable, secondTable) = joinType match {
      case RightOuter => (right, left)
      case _ => (left, right)
    }
    val numFirstTableColumns = firstTable.output.size
    val boundCondition = condition.map {
      GpuBindReferences.bindGpuReference(_, firstTable.output ++ secondTable.output, allMetrics)
    }

    val broadcastRelation = getBroadcastRelation()

    // Sometimes Spark specifies a true condition for a row-count-only join.
    // This can happen when the join keys are detected to be constant.
    if (isUnconditionalJoin(boundCondition)) {
      doUnconditionalJoin(broadcastRelation)
    } else {
      doConditionalJoin(broadcastRelation, boundCondition, numFirstTableColumns)
    }
  }

  private def leftExistenceJoin(
      relation: Any,
      exists: Boolean,
      buildTime: GpuMetric,
      buildDataSize: GpuMetric): RDD[ColumnarBatch] = {
    assert(gpuBuildSide == GpuBuildRight)
    streamed.executeColumnar().mapPartitionsInternal { streamedIter =>
      val buildRows = computeBuildRowCount(relation, buildTime, buildDataSize)
      if (buildRows > 0 == exists) {
        streamedIter
      } else {
        Iterator.empty
      }
    }
  }

  protected def makeBuiltBatchAndStreamIter(
      relation: Any,
      streamIter: Iterator[ColumnarBatch],
      buildSchema: StructType,
      buildTime: GpuMetric,
      buildDataSize: GpuMetric): (ColumnarBatch, Iterator[ColumnarBatch]) = {
    GpuBroadcastHelper.getBroadcastBuiltBatchAndStreamIter(
      relation.asInstanceOf[Broadcast[Any]],
      buildSchema,
      streamIter,
      Some(buildTime),
      Some(buildDataSize))
  }

  private def buildSidePostProjection: Option[ColumnarBatch => ColumnarBatch] = {
    buildPlan match {
      case p: GpuProjectExec =>
        // Need to manually do project columnar execution other than calling child's
        // internalDoExecuteColumnar. This is to workaround especial handle to build broadcast
        // batch.
        val proj = GpuBindReferences.bindGpuReferencesTiered(
          postBuildCondition, p.child.output, conf, allMetrics)
        val fn = (batch: ColumnarBatch) => {
          withResource(batch)(proj.project)
        }
        Some(fn)
      case _ =>
        None
    }
  }

  private def doUnconditionalJoin(relation: Any): RDD[ColumnarBatch] = {
    if (output.isEmpty) {
      doUnconditionalJoinRowCount(relation)
    } else {
      val numOutputRows = gpuLongMetric(NUM_OUTPUT_ROWS)
      val numOutputBatches = gpuLongMetric(NUM_OUTPUT_BATCHES)
      val buildTime = gpuLongMetric(BUILD_TIME)
      val opTime = gpuLongMetric(OP_TIME_LEGACY)
      val buildDataSize = gpuLongMetric(BUILD_DATA_SIZE)
      val localJoinType = joinType

      val joinIterator: RDD[ColumnarBatch] = joinType match {
        case ExistenceJoin(_) =>
          doUnconditionalExistenceJoin(relation, buildTime, buildDataSize)
        case LeftSemi =>
          if (gpuBuildSide == GpuBuildRight) {
            leftExistenceJoin(relation, exists=true, buildTime, buildDataSize)
          } else {
            left.executeColumnar()
          }
        case LeftAnti =>
          if (gpuBuildSide == GpuBuildRight) {
            leftExistenceJoin(relation, exists=false, buildTime, buildDataSize)
          } else {
            // degenerate case, no rows are returned.
            val childRDD = left.executeColumnar()
            new GpuCoalesceExec.EmptyRDDWithPartitions(sparkContext, childRDD.getNumPartitions)
          }
        case _ =>
          // Everything else is treated like an unconditional cross join
          val buildSide = gpuBuildSide
          val joinTime = gpuLongMetric(JOIN_TIME)
          val buildSchema = buildPlan.schema
          val postProjectionAndClose = buildSidePostProjection
          val sizeEstimateThreshold = RapidsConf.JOIN_GATHERER_SIZE_ESTIMATE_THRESHOLD.get(conf)
          streamed.executeColumnar().mapPartitions { streamedIter =>
            // Will materialize the stream batch before materializing the build batch.
            val (buildBatch, bufferedStream) = makeBuiltBatchAndStreamIter(
              relation,
              streamedIter,
              buildSchema,
              buildTime,
              buildDataSize)
            val prjBuildBatch = postProjectionAndClose.map(_(buildBatch)).getOrElse(buildBatch)

            val lazyStream = bufferedStream.map { cb =>
              withResource(cb) { cb =>
                LazySpillableColumnarBatch(cb, "stream_batch")
              }
            }
            val spillableBuiltBatch = withResource(prjBuildBatch) {
              LazySpillableColumnarBatch(_, "built_batch")
            }

            localJoinType match {
              case LeftOuter if spillableBuiltBatch.numRows == 0 =>
                withResource(spillableBuiltBatch) { _ =>
                  new EmptyOuterNestedLoopJoinIterator(bufferedStream,
                    spillableBuiltBatch.dataTypes,
                    isStreamFirst = true)
                }
              case RightOuter if spillableBuiltBatch.numRows == 0 =>
                withResource(spillableBuiltBatch) { _ =>
                  new EmptyOuterNestedLoopJoinIterator(bufferedStream,
                    spillableBuiltBatch.dataTypes,
                    isStreamFirst = false)
                }
              case _ =>
                new CrossJoinIterator(
                  spillableBuiltBatch,
                  lazyStream,
                  targetSizeBytes,
                  sizeEstimateThreshold,
                  buildSide,
                  opTime = opTime,
                  joinTime = joinTime)
            }
          }
      }
      joinIterator.map { cb =>
        numOutputRows += cb.numRows()
        numOutputBatches += 1
        cb
      }
    }
  }

  /**
   * Special-case handling of an unconditional existence join that just needs to output the left
   * table along with an existence column that is all true if the right table has any rows or
   * all false otherwise.
   */
  private def doUnconditionalExistenceJoin(
      relation: Any,
      buildTime: GpuMetric,
      buildDataSize: GpuMetric): RDD[ColumnarBatch] = {
    def addExistsColumn(iter: Iterator[ColumnarBatch], exists: Boolean): Iterator[ColumnarBatch] = {
      iter.flatMap { batch =>
        val spillable = SpillableColumnarBatch(batch, SpillPriorities.ACTIVE_ON_DECK_PRIORITY)
        withRetry(spillable, RmmRapidsRetryIterator.splitSpillableInHalfByRows) { spillBatch =>
          withResource(spillBatch.getColumnarBatch()) { batch =>
            closeOnExcept(GpuColumnVector.incRefCounts(batch)) { _ =>
              val newCols = new Array[ColumnVector](batch.numCols + 1)
              (0 until newCols.length - 1).foreach { i =>
                newCols(i) = batch.column(i)
              }
              val existsCol = withResource(Scalar.fromBool(exists)) { existsScalar =>
                GpuColumnVector.from(cudf.ColumnVector.fromScalar(existsScalar, batch.numRows),
                  BooleanType)
              }
              newCols(batch.numCols) = existsCol
              new ColumnarBatch(newCols, batch.numRows)
            }
          }
        }
      }
    }

    if (gpuBuildSide == GpuBuildRight) {
      left.executeColumnar.mapPartitions { iter =>
        val buildHasRows = computeBuildRowCount(relation, buildTime, buildDataSize) > 0
        addExistsColumn(iter, buildHasRows)
      }
    } else {
      // try to check cheaply whether there are any rows in the streamed table at all
      val streamTakePlan = GpuColumnarToRowExec(GpuLocalLimitExec(1, streamed))
      val streamExists = !streamTakePlan.executeTake(1).isEmpty
      val leftRDD = GpuBroadcastHelper.asRDD(sparkContext,relation.asInstanceOf[Broadcast[Any]])
      leftRDD.mapPartitions { iter =>
        addExistsColumn(iter, streamExists)
      }
    }
  }

  /** Special-case handling of an unconditional join that just needs to output a row count. */
  private def doUnconditionalJoinRowCount(relation: Any): RDD[ColumnarBatch] = {
    if (joinType == LeftAnti) {
      // degenerate case, no rows are returned.
      left.executeColumnar().mapPartitions { _ =>
        Iterator.single(new ColumnarBatch(Array(), 0))
      }
    } else {
      lazy val buildCount = if (joinType == LeftSemi || joinType.isInstanceOf[ExistenceJoin]) {
        // one-to-one mapping from input rows to output rows
        1
      } else {
        val buildTime = gpuLongMetric(BUILD_TIME)
        val buildDataSize = gpuLongMetric(BUILD_DATA_SIZE)
        computeBuildRowCount(relation, buildTime, buildDataSize)
      }

      def getRowCountAndClose(cb: ColumnarBatch): Long = {
        val ret = cb.numRows()
        cb.close()
        GpuSemaphore.releaseIfNecessary(TaskContext.get())
        ret
      }

      val numOutputRows = gpuLongMetric(NUM_OUTPUT_ROWS)
      val numOutputBatches = gpuLongMetric(NUM_OUTPUT_BATCHES)
      val counts = streamed.executeColumnar().map(getRowCountAndClose)
      GpuBroadcastNestedLoopJoinExecBase.divideIntoBatches(
        counts.map(s => s * buildCount),
        targetSizeBytes,
        numOutputRows,
        numOutputBatches)
    }
  }

  private def doConditionalJoin(
      relation: Any,
      boundCondition: Option[GpuExpression],
      numFirstTableColumns: Int): RDD[ColumnarBatch] = {
    val buildTime = gpuLongMetric(BUILD_TIME)
    val buildDataSize = gpuLongMetric(BUILD_DATA_SIZE)
    val streamAttributes = streamed.output
    val numOutputRows = gpuLongMetric(NUM_OUTPUT_ROWS)
    val numOutputBatches = gpuLongMetric(NUM_OUTPUT_BATCHES)
    val opTime = gpuLongMetric(OP_TIME_LEGACY)
    val joinTime = gpuLongMetric(JOIN_TIME)
    val nestedLoopJoinType = joinType
    val buildSide = gpuBuildSide
    val buildSchema = buildPlan.schema
    val postProjectionAndClose = buildSidePostProjection
    val sizeEstimateThreshold = RapidsConf.JOIN_GATHERER_SIZE_ESTIMATE_THRESHOLD.get(conf)
    streamed.executeColumnar().mapPartitions { streamedIter =>
      // Will materialize the stream batch before materializing the build batch.
      val (buildBatch, bufferedStream) = makeBuiltBatchAndStreamIter(
        relation,
        streamedIter,
        buildSchema,
        buildTime,
        buildDataSize)
      val prjBuildBatch = postProjectionAndClose.map(_(buildBatch)).getOrElse(buildBatch)

      val lazyStream = bufferedStream.map { cb =>
        withResource(cb) { cb =>
          LazySpillableColumnarBatch(cb, "stream_batch")
        }
      }
      val spillableBuiltBatch = withResource(prjBuildBatch) {
        LazySpillableColumnarBatch(_, "built_batch")
      }

      GpuBroadcastNestedLoopJoinExecBase.nestedLoopJoin(
        nestedLoopJoinType, buildSide, numFirstTableColumns,
        spillableBuiltBatch,
        lazyStream, streamAttributes, targetSizeBytes, sizeEstimateThreshold, boundCondition,
        numOutputRows = numOutputRows,
        numOutputBatches = numOutputBatches,
        opTime = opTime,
        joinTime = joinTime)
    }
  }
}

class ConditionalNestedLoopExistenceJoinIterator(
    spillableBuiltBatch: LazySpillableColumnarBatch,
    lazyStream: Iterator[LazySpillableColumnarBatch],
    condition: CompiledExpression,
    opTime: GpuMetric,
    joinTime: GpuMetric
) extends ExistenceJoinIterator(spillableBuiltBatch, lazyStream, opTime, joinTime) {

  use(condition)

  override def existsScatterMap(leftColumnarBatch: ColumnarBatch): GatherMap = {
    NvtxIdWithMetrics(NvtxRegistry.EXISTENCE_JOIN_SCATTER_MAP, joinTime) {
      withResource(GpuColumnVector.from(leftColumnarBatch)) { leftTab =>
        withResource(GpuColumnVector.from(spillableBuiltBatch.getBatch)) { rightTab =>
          leftTab.conditionalLeftSemiJoinGatherMap(rightTab, condition)
        }
      }
    }
  }
}

/** Iterator for producing batches from an outer join where the build-side table is empty. */
class EmptyOuterNestedLoopJoinIterator(
    streamIter: Iterator[ColumnarBatch],
    buildTypes: Array[DataType],
    isStreamFirst: Boolean) extends Iterator[ColumnarBatch] {
  override def hasNext: Boolean = streamIter.hasNext

  override def next(): ColumnarBatch = {
    withResource(streamIter.next()) { streamBatch =>
      withResource(buildNullBatch(streamBatch.numRows())) { nullBatch =>
        if (isStreamFirst) {
          GpuColumnVector.combineColumns(streamBatch, nullBatch)
        } else {
          GpuColumnVector.combineColumns(nullBatch, streamBatch)
        }
      }
    }
  }

  private def buildNullBatch(numRows: Int): ColumnarBatch = {
    val cols: Array[ColumnVector] = buildTypes.safeMap { dt =>
      GpuColumnVector.fromNull(numRows, dt)
    }
    new ColumnarBatch(cols, numRows)
  }
}
