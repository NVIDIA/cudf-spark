/*
 * Copyright (c) 2023-2025, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.delta

import scala.collection.mutable.ArrayBuffer

import ai.rapids.cudf.{NvtxColor, Table}
import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.AssertUtils.assertInTests
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.ScalableTaskCompletion.onTaskCompletion

import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet, Expression}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, UnaryNode}
import org.apache.spark.sql.execution.{SparkPlan, SparkStrategy, UnaryExecNode}
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.vectorized.ColumnarBatch

object RapidsProcessDeltaMergeJoinStrategy extends SparkStrategy {
  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case p: RapidsProcessDeltaMergeJoin =>
      Seq(RapidsProcessDeltaMergeJoinExec(
        planLater(p.child),
        p.output,
        targetRowHasNoMatch = p.targetRowHasNoMatch,
        sourceRowHasNoMatch = p.sourceRowHasNoMatch,
        matchedConditions = p.matchedConditions,
        matchedOutputs = p.matchedOutputs,
        notMatchedConditions = p.notMatchedConditions,
        notMatchedOutputs = p.notMatchedOutputs,
        notMatchedBySourceConditions = p.notMatchedBySourceConditions,
        notMatchedBySourceOutputs = p.notMatchedBySourceOutputs,
        noopCopyOutput = p.noopCopyOutput,
        deleteRowOutput = p.deleteRowOutput))
    case _ => Nil
  }
}

case class RapidsProcessDeltaMergeJoin(
    child: LogicalPlan,
    override val output: Seq[Attribute],
    targetRowHasNoMatch: Expression,
    sourceRowHasNoMatch: Expression,
    matchedConditions: Seq[Expression],
    matchedOutputs: Seq[Seq[Seq[Expression]]],
    notMatchedConditions: Seq[Expression],
    notMatchedOutputs: Seq[Seq[Seq[Expression]]],
    notMatchedBySourceConditions: Seq[Expression],
    notMatchedBySourceOutputs: Seq[Seq[Seq[Expression]]],
    noopCopyOutput: Seq[Expression],
    deleteRowOutput: Seq[Expression]) extends UnaryNode {

  @transient
  override lazy val references: AttributeSet = inputSet

  override protected def withNewChildInternal(newChild: LogicalPlan): LogicalPlan = {
    copy(child = newChild)
  }
}

case class RapidsProcessDeltaMergeJoinExec(
    child: SparkPlan,
    override val output: Seq[Attribute],
    targetRowHasNoMatch: Expression,
    sourceRowHasNoMatch: Expression,
    matchedConditions: Seq[Expression],
    matchedOutputs: Seq[Seq[Seq[Expression]]],
    notMatchedBySourceConditions: Seq[Expression],
    notMatchedBySourceOutputs: Seq[Seq[Seq[Expression]]],
    notMatchedConditions: Seq[Expression],
    notMatchedOutputs: Seq[Seq[Seq[Expression]]],
    noopCopyOutput: Seq[Expression],
    deleteRowOutput: Seq[Expression]) extends UnaryExecNode {

  override protected def doExecute(): RDD[InternalRow] = {
    throw new IllegalStateException("Should have been replaced by a GpuRapidsProcessMergeJoinExec")
  }

  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan = {
    copy(child = newChild)
  }
}

class RapidsProcessDeltaMergeJoinMeta(
    p: RapidsProcessDeltaMergeJoinExec,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
    extends SparkPlanMeta[RapidsProcessDeltaMergeJoinExec](p, conf, parent, rule) {

  // handling child expressions manually since they're grouped into separate sequences
  override val childExprs: Seq[BaseExprMeta[_]] = Seq.empty

  override def convertToGpu(): GpuExec = {
    GpuRapidsProcessDeltaMergeJoinExec(
      childPlans.head.convertIfNeeded(),
      p.output,
      targetRowHasNoMatch = convertExprToGpu(p.targetRowHasNoMatch),
      sourceRowHasNoMatch = convertExprToGpu(p.sourceRowHasNoMatch),
      matchedConditions = p.matchedConditions.map(convertExprToGpu),
      matchedOutputs = p.matchedOutputs.map(_.map(_.map(convertExprToGpu))),
      notMatchedConditions = p.notMatchedConditions.map(convertExprToGpu),
      notMatchedOutputs = p.notMatchedOutputs.map(_.map(_.map(convertExprToGpu))),
      notMatchedBySourceConditions = p.notMatchedBySourceConditions.map(convertExprToGpu),
      notMatchedBySourceOutputs = p.notMatchedBySourceOutputs.map(_.map(_.map(convertExprToGpu))),
      noopCopyOutput = p.noopCopyOutput.map(convertExprToGpu),
      deleteRowOutput = p.deleteRowOutput.map(convertExprToGpu))
  }

  private def convertExprToGpu(e: Expression): Expression = {
    val meta = GpuOverrides.wrapExpr(e, conf, None)
    meta.tagForGpu()
    assertInTests(meta.canExprTreeBeReplaced, meta.toString)
    meta.convertToGpu()
  }
}

object GpuDeltaMergeConstants {
  val ROW_DROPPED_COL = "_row_dropped_"
}

case class GpuRapidsProcessDeltaMergeJoinExec(
    child: SparkPlan,
    override val output: Seq[Attribute],
    targetRowHasNoMatch: Expression,
    sourceRowHasNoMatch: Expression,
    matchedConditions: Seq[Expression],
    matchedOutputs: Seq[Seq[Seq[Expression]]],
    notMatchedConditions: Seq[Expression],
    notMatchedOutputs: Seq[Seq[Seq[Expression]]],
    notMatchedBySourceConditions: Seq[Expression],
    notMatchedBySourceOutputs: Seq[Seq[Seq[Expression]]],
    noopCopyOutput: Seq[Expression],
    deleteRowOutput: Seq[Expression]) extends UnaryExecNode with GpuExec {
  require(matchedConditions.length == matchedOutputs.length)
  require(notMatchedConditions.length == notMatchedOutputs.length)

  // TODO add support for notMatchedBy*
  // see https://github.com/NVIDIA/spark-rapids/issues/8415
  require(notMatchedBySourceConditions.isEmpty)
  require(notMatchedBySourceOutputs.isEmpty)

  private lazy val inputTypes: Array[DataType] = GpuColumnVector.extractTypes(child.schema)
  private lazy val outputExprs: Seq[GpuBoundReference] = output.zipWithIndex.map {
    case (attr, idx) =>
      GpuBoundReference(idx, attr.dataType, attr.nullable)(attr.exprId, attr.name)
  }
  private lazy val boundTargetRowHasNoMatch = bindForGpu(targetRowHasNoMatch)
  private lazy val boundSourceRowHasNoMatch = bindForGpu(sourceRowHasNoMatch)
  private lazy val boundMatchedConditions = matchedConditions.map(bindForGpu)
  private lazy val boundMatchedOutputs = matchedOutputs.map(_.map(_.map(bindForGpu)))
  private lazy val boundNotMatchedConditions = notMatchedConditions.map(bindForGpu)
  private lazy val boundNotMatchedOutputs = notMatchedOutputs.map(_.map(_.map(bindForGpu)))
  private lazy val boundNoopCopyOutput = noopCopyOutput.map(bindForGpu)
  private lazy val boundDeleteRowOutput = deleteRowOutput.map(bindForGpu)

  override lazy val additionalMetrics: Map[String, GpuMetric] = {
    import GpuMetric._
    Map(OP_TIME_LEGACY -> createNanoTimingMetric(DEBUG_LEVEL, DESCRIPTION_OP_TIME_LEGACY))
  }

  private def bindForGpu(e: Expression): GpuExpression = {
    GpuBindReferences.bindGpuReference(e, child.output, allMetrics)
  }

  override protected def doExecute(): RDD[InternalRow] = {
    throw new IllegalStateException("Row-based execution should not occur for this class")
  }

  override protected def internalDoExecuteColumnar(): RDD[ColumnarBatch] = {
    val localInputTypes = inputTypes
    val localOutput = output
    val localOutputExprs = outputExprs
    val localTargetRowHasNoMatch = boundTargetRowHasNoMatch
    val localSourceRowHasNoMatch = boundSourceRowHasNoMatch
    val localMatchedConditions = boundMatchedConditions
    val localMatchedOutputs = boundMatchedOutputs
    val localNotMatchedConditions = boundNotMatchedConditions
    val localNotMatchedOutputs = boundNotMatchedOutputs
    val localNoopCopyOutput = boundNoopCopyOutput
    val localDeleteRowOutput = boundDeleteRowOutput
    child.executeColumnar().mapPartitions { iter =>
      new GpuRapidsProcessDeltaMergeJoinIterator(
        iter = iter,
        inputTypes = localInputTypes,
        output = localOutput,
        outputExprs = localOutputExprs,
        targetRowHasNoMatch = localTargetRowHasNoMatch,
        sourceRowHasNoMatch = localSourceRowHasNoMatch,
        matchedConditions = localMatchedConditions,
        matchedOutputs = localMatchedOutputs,
        notMatchedConditions = localNotMatchedConditions,
        notMatchedOutputs = localNotMatchedOutputs,
        noopCopyOutput = localNoopCopyOutput,
        deleteRowOutput = localDeleteRowOutput,
        allMetrics)
    }
  }

  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan = {
    copy(child = newChild)
  }
}

class GpuRapidsProcessDeltaMergeJoinIterator(
    iter: Iterator[ColumnarBatch],
    inputTypes: Array[DataType],
    output: Seq[Attribute],
    outputExprs: Seq[GpuExpression],
    targetRowHasNoMatch: GpuExpression,
    sourceRowHasNoMatch: GpuExpression,
    matchedConditions: Seq[GpuExpression],
    matchedOutputs: Seq[Seq[Seq[GpuExpression]]],
    notMatchedConditions: Seq[GpuExpression],
    notMatchedOutputs: Seq[Seq[Seq[GpuExpression]]],
    noopCopyOutput: Seq[GpuExpression],
    deleteRowOutput: Seq[GpuExpression],
    metrics: Map[String, GpuMetric])
    extends Iterator[ColumnarBatch] with AutoCloseable {

  private[this] val intermediateTypes: Array[DataType] = noopCopyOutput.map(_.dataType).toArray
  private[this] var nextBatch: Option[ColumnarBatch] = None
  private[this] val opTime = metrics.getOrElse(GpuMetric.OP_TIME_LEGACY, NoopMetric)
  private[this] val numOutputRows = metrics.getOrElse(GpuMetric.NUM_OUTPUT_ROWS, NoopMetric)

  // Don't install the callback if in a unit test
  Option(TaskContext.get()).foreach { tc =>
    onTaskCompletion(tc) {
      close()
    }
  }

  override def hasNext: Boolean = {
    nextBatch = nextBatch.orElse(processNextBatch())
    nextBatch.isDefined
  }

  override def next(): ColumnarBatch = {
    if (!hasNext) {
      throw new NoSuchElementException("no more batches")
    }
    val result = nextBatch.get
    nextBatch = None
    result
  }

  override def close(): Unit = {
    nextBatch.foreach(_.close())
    nextBatch = None
  }

  private def processNextBatch(): Option[ColumnarBatch] = {
    while (iter.hasNext) {
        val input = iter.next()
        val batch = withResource(new NvtxWithMetrics("process merge", NvtxColor.CYAN, opTime)) {
          _ => processSingleBatch(input)
        }
      if (batch.numRows > 0) {
        numOutputRows += batch.numRows
        return Some(batch)
      } else {
        batch.close()
      }
    }
    None
  }

  private def processSingleBatch(input: ColumnarBatch): ColumnarBatch = {
    val (targetNoMatchBatch, targetMatchBatch) =
      splitBatchAndClose(input, inputTypes, targetRowHasNoMatch)
    val noopCopyBatch = closeOnExcept(targetMatchBatch) { _ =>
      GpuProjectExec.projectAndClose(targetNoMatchBatch, noopCopyOutput, NoopMetric)
    }
    val bigTable = withResource(noopCopyBatch) { _ =>
      val (sourceNoMatchBatch, sourceMatchBatch) =
        splitBatchAndClose(targetMatchBatch, inputTypes, sourceRowHasNoMatch)
      val sourceNotMatchedBatches = closeOnExcept(sourceMatchBatch) { _ =>
        processProjectionSeries(sourceNoMatchBatch,
          notMatchedConditions, notMatchedOutputs, deleteRowOutput)
      }
      withResource(sourceNotMatchedBatches) { _ =>
        val sourceMatchedBatches = processProjectionSeries(sourceMatchBatch,
          matchedConditions, matchedOutputs, noopCopyOutput)
        withResource(sourceMatchedBatches) { _ =>
          val allBatches = (noopCopyBatch +: sourceNotMatchedBatches) ++ sourceMatchedBatches
          // annoyingly Table.concatenate does not gracefully handle the degenerate case
          if (allBatches.size == 1) {
            GpuColumnVector.from(allBatches.head)
          } else {
            withResource(allBatches.safeMap(GpuColumnVector.from)) { allTables =>
              Table.concatenate(allTables: _*)
            }
          }
        }
      }
    }
    val shouldNotDeleteBatch = withResource(bigTable) { _ =>
      // If ROW_DROPPED_COL is not in output schema
      // then CDC must be disabled and it's the column after our output cols
      val shouldDeleteColumnIndex =
        output.zipWithIndex.find(_._1.name == GpuDeltaMergeConstants.ROW_DROPPED_COL).map(_._2)
            .getOrElse(output.size)
      val shouldDeleteColumn = bigTable.getColumn(shouldDeleteColumnIndex)
      withResource(shouldDeleteColumn.not()) { notDeleteColumn =>
        withResource(bigTable.filter(notDeleteColumn)) { notDeleteTable =>
          GpuColumnVector.from(notDeleteTable, intermediateTypes)
        }
      }
    }
    GpuProjectExec.projectAndClose(shouldNotDeleteBatch, outputExprs, NoopMetric)
  }

  private def processProjectionSeries(
      input: ColumnarBatch,
      conditions: Seq[Expression],
      outputs: Seq[Seq[Seq[Expression]]],
      default: Seq[Expression]): Seq[ColumnarBatch] = {
    closeOnExcept(new ArrayBuffer[ColumnarBatch]) { results =>
      var leftOverBatch = input
      conditions.zip(outputs).foreach { case (condition, output) =>
        closeOnExcept(leftOverBatch) { _ =>
          if (leftOverBatch.numRows() > 0) {
            val (matchBatch, notMatchBatch) =
              splitBatchAndClose(leftOverBatch, inputTypes, condition)
            leftOverBatch = notMatchBatch
            withResource(matchBatch) { _ =>
              output.foreach { exprs =>
                results.append(GpuProjectExec.project(matchBatch, exprs))
              }
            }
          }
        }
      }
      withResource(leftOverBatch) { _ =>
        if (leftOverBatch.numRows() > 0) {
          results.append(GpuProjectExec.project(leftOverBatch, default))
        }
      }
      results.toSeq
    }
  }

  private def splitBatchAndClose(
      input: ColumnarBatch,
      inputTypes: Array[DataType],
      predicate: Expression): (ColumnarBatch, ColumnarBatch) = {
    withResource(input) { _ =>
      withResource(GpuColumnVector.from(input)) { inTable =>
        val predCol = predicate.columnarEval(input)
        val matchedBatch = closeOnExcept(predCol) { _ =>
          withResource(inTable.filter(predCol.getBase)) { matchedTable =>
            GpuColumnVector.from(matchedTable, inputTypes)
          }
        }
        closeOnExcept(matchedBatch) { _ =>
          val notPredCol = withResource(predCol) { _ =>
            predCol.getBase.not()
          }
          val notMatchedBatch = withResource(notPredCol) { _ =>
            withResource(inTable.filter(notPredCol)) { notMatchedTable =>
              GpuColumnVector.from(notMatchedTable, inputTypes)
            }
          }
          (matchedBatch, notMatchedBatch)
        }
      }
    }
  }
}
