/*
 * Copyright (c) 2025-2026, NVIDIA CORPORATION.
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
{"spark": "350"}
{"spark": "351"}
{"spark": "352"}
{"spark": "353"}
{"spark": "354"}
{"spark": "355"}
{"spark": "356"}
{"spark": "357"}
{"spark": "358"}
{"spark": "400"}
{"spark": "401"}
{"spark": "402"}
{"spark": "403"}
{"spark": "411"}
{"spark": "412"}
{"spark": "413"}
{"spark": "420"}
spark-rapids-shim-json-lines ***/
package org.apache.spark.sql.execution.datasources.v2

import scala.annotation.tailrec
import scala.collection.mutable.ArrayBuffer

import ai.rapids.cudf.{ColumnVector, NvtxColor, Scalar}
import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.Arm._
import com.nvidia.spark.rapids.RapidsPluginImplicits.ReallyAGpuExpression
import com.nvidia.spark.rapids.RmmRapidsRetryIterator.withRetryNoSplit
import com.nvidia.spark.rapids.shims.{ShimExpression, ShimUnaryExecNode}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet, Expression}
import org.apache.spark.sql.catalyst.plans.logical.MergeRows.ROW_ID
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.v2.GpuMergeRowsExec.GpuInstruction
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch



/**
 * GPU version of MergeRowsExec.
 * 
 * This operator processes the output of a join between source and target tables,
 * applying merge logic to determine which rows to insert, update, or delete.
 * 
 *
 * @param isTargetRowPresent GPU expression indicating if target row is present in join
 * @param isSourceRowPresent GPU expression indicating if source row is present in join
 * @param matchedInstructions Sequence of instructions for MATCHED clauses
 * @param notMatchedInstructions Sequence of instructions for NOT MATCHED clauses
 * @param notMatchedBySourceInstructions Sequence of instructions for NOT MATCHED BY SOURCE clauses
 * @param checkCardinality Whether to check for cardinality violations (multiple matches)
 * @param output Output attributes
 * @param child Child plan providing joined data
 */
case class GpuMergeRowsExec(
    isTargetRowPresent: Expression,
    isSourceRowPresent: Expression,
    matchedInstructions: Seq[GpuInstruction],
    notMatchedInstructions: Seq[GpuInstruction],
    notMatchedBySourceInstructions: Seq[GpuInstruction],
    checkCardinality: Boolean,
    output: Seq[Attribute],
    child: SparkPlan) extends ShimUnaryExecNode with GpuExec {

  import GpuMetric._

  @transient override lazy val producedAttributes: AttributeSet = {
    AttributeSet(output.filterNot(attr => inputSet.contains(attr)))
  }

  @transient
  override lazy val references: AttributeSet = {
    val usedExprs = if (checkCardinality) {
      val rowIdAttr = child.output.find(attr => conf.resolver(attr.name, ROW_ID))
      assert(rowIdAttr.isDefined, "Cannot find row ID attr")
      rowIdAttr.get +: expressions
    } else {
      expressions
    }
    AttributeSet.fromAttributeSets(usedExprs.map(_.references)) -- producedAttributes
  }

  override lazy val additionalMetrics: Map[String, GpuMetric] = Map(
    OP_TIME_LEGACY -> createNanoTimingMetric(MODERATE_LEVEL, DESCRIPTION_OP_TIME_LEGACY),
    NUM_OUTPUT_ROWS -> createMetric(ESSENTIAL_LEVEL, DESCRIPTION_NUM_OUTPUT_ROWS),
    NUM_OUTPUT_BATCHES -> createMetric(MODERATE_LEVEL, DESCRIPTION_NUM_OUTPUT_BATCHES)
  )

  override def supportsColumnar: Boolean = true

  override protected def internalDoExecuteColumnar(): RDD[ColumnarBatch] = {
    val numOutputRows = gpuLongMetric(NUM_OUTPUT_ROWS)
    val numOutputBatches = gpuLongMetric(NUM_OUTPUT_BATCHES)
    val opTime = gpuLongMetric(OP_TIME_LEGACY)

    val dataTypes = GpuColumnVector.extractTypes(child.schema)

    val boundTargetRowPresent = GpuBindReferences.bindGpuReference(isTargetRowPresent,
      child.output, allMetrics)
    val boundSourceRowPresent = GpuBindReferences.bindGpuReference(isSourceRowPresent,
      child.output, allMetrics)

    val boundMatchedInsts = GpuBindReferences.bindGpuReferences(matchedInstructions,
        child.output, allMetrics)
      .asInstanceOf[Seq[GpuInstruction]]
    val boundNotMatchedInsts = GpuBindReferences.bindGpuReferences(notMatchedInstructions,
      child.output, allMetrics).asInstanceOf[Seq[GpuInstruction]]
    val boundMatchedBySourceInsts = GpuBindReferences.bindGpuReferences(
      notMatchedBySourceInstructions, child.output, allMetrics)
      .asInstanceOf[Seq[GpuInstruction]]

    child.executeColumnar().mapPartitions { iter =>
      new GpuMergeBatchIterator(
        dataTypes,
        iter,
        boundTargetRowPresent,
        boundSourceRowPresent,
        boundMatchedInsts,
        boundNotMatchedInsts,
        boundMatchedBySourceInsts,
        numOutputRows,
        numOutputBatches,
        opTime)
    }
  }

  override def withNewChildInternal(newChild: SparkPlan): SparkPlan = {
    copy(child = newChild)
  }

  override def doExecute(): RDD[InternalRow] =
    throw new IllegalStateException(s"Row-based execution should not occur for $this")
}

object GpuMergeRowsExec {

  sealed trait GpuInstruction extends GpuUnevaluable with ShimExpression {
    def condition: Expression

    def outputs: Seq[Seq[Expression]]

    def evaluateCondition(batch: ColumnarBatch): GpuColumnVector = {
      condition.columnarEval(batch)
    }

    def applyOutputs(batch: ColumnarBatch): Seq[ColumnarBatch] = {
      outputs.map(output => GpuProjectExec.project(batch, output))
    }

    override def nullable: Boolean = false
    override def dataType: DataType = NullType
  }

  case class GpuKeep(condition: Expression, output: Seq[Expression]) extends GpuInstruction {

    override def children: Seq[Expression] = Seq(condition) ++ output
    override def outputs: Seq[Seq[Expression]] = Seq(output)
  }

  case class GpuDiscard(condition: Expression) extends GpuInstruction {

    override def outputs: Seq[Seq[Expression]] = Seq.empty

    override def children: Seq[Expression] = Seq(condition)
  }

  case class GpuSplit(condition: Expression, extraOutput: Seq[Expression],
                      output: Seq[Expression]) extends GpuInstruction {

    override def outputs: Seq[Seq[Expression]] = Seq(extraOutput, output)

    override def children: Seq[Expression] = Seq(condition) ++ extraOutput ++ output
  }
}



/**
 * GPU version of MergeRowIterator. Processes columnar batches for MERGE operations.
 * Similar to Spark's MergeRowIterator but operates on batches instead of rows.
 *
 * @param inputDataTypes Spark data types of input iterator.
 * @param inputIter Iterator of input columnar batches
 * @param isTargetRowPresent Bound GPU expression to check if target row is present
 * @param isSourceRowPresent Bound GPU expression to check if source row is present
 * @param matchedInstructionExecs Executors for MATCHED instructions
 * @param notMatchedInstructionExecs Executors for NOT MATCHED instructions
 * @param notMatchedBySourceInstructionExecs Executors for NOT MATCHED BY SOURCE instructions
 * @param numOutputRows Metric for output rows
 * @param numOutputBatches Metric for output batches
 * @param opTime Metric for operation time
 */
class GpuMergeBatchIterator(
    inputDataTypes: Array[DataType],
    inputIter: Iterator[ColumnarBatch],
    isTargetRowPresent: GpuExpression,
    isSourceRowPresent: GpuExpression,
    matchedInstructionExecs: Seq[GpuInstruction],
    notMatchedInstructionExecs: Seq[GpuInstruction],
    notMatchedBySourceInstructionExecs: Seq[GpuInstruction],
    numOutputRows: GpuMetric,
    numOutputBatches: GpuMetric,
    opTime: GpuMetric) extends Iterator[ColumnarBatch] {

  override def hasNext: Boolean = inputIter.hasNext

  override def next(): ColumnarBatch = {

    val batch = inputIter.next()
    withResource(new NvtxWithMetrics("GpuMergeBatchIterator", NvtxColor.CYAN, opTime)) { _ =>
      val result = processBatch(batch)
      numOutputBatches += 1
      numOutputRows += result.numRows()
      result
    }
  }

  /**
   * Process a single columnar batch, applying merge logic.
   */
  private def processBatch(batch: ColumnarBatch): ColumnarBatch = {

    withRetryNoSplit(batch) { _ =>
      // Evaluate presence flags
      val outputs = new ArrayBuffer[SpillableColumnarBatch](
        matchedInstructionExecs.size + notMatchedInstructionExecs.size
          + notMatchedBySourceInstructionExecs.size)

      closeOnExcept(outputs) { _ =>
        val sourcePresentCol = isSourceRowPresent.columnarEval(batch)
        withResource(sourcePresentCol) { sourcePresent =>
          require(!sourcePresent.hasNull, "Source cannot have null")
          val targetPresentCol = isTargetRowPresent.columnarEval(batch)
          withResource(targetPresentCol) { targetPresent =>
            require(!targetPresent.hasNull, "Target cannot have null")

            val matchedMask = sourcePresent.getBase.and(targetPresent.getBase)
            processInstructionSet(inputDataTypes, outputs, batch,
              matchedMask, matchedInstructionExecs)

            val notMatchedMask = withResource(targetPresent.getBase.not()) { noTargetMask =>
              sourcePresent.getBase.and(noTargetMask)
            }
            processInstructionSet(inputDataTypes, outputs, batch, notMatchedMask,
                notMatchedInstructionExecs)

            val notMatchedBySrcMask = withResource(sourcePresent.getBase.not()) { noSourceMask =>
              targetPresent.getBase.and(noSourceMask)
            }
            processInstructionSet(inputDataTypes, outputs, batch, notMatchedBySrcMask,
                notMatchedBySourceInstructionExecs)
          }
        }
      }
      // TODO: There is a small chance that the output batch is much larger input batch,
      //  e.g. all cases need to update split, which leads to a result batch whose row
      //  count is twice of input batch. We need to handle this case by splitting the outputs
      // https://github.com/NVIDIA/spark-rapids/issues/13712
      withResource(GpuBatchUtils.concatSpillBatchesAndClose(outputs.toSeq)) { spillable =>
        spillable.get.getColumnarBatch()
      }
    }
  }

  /**
   * Process a set of instructions for rows matching the given mask.
   */
  @tailrec
  private def processInstructionSet(
     dataTypes: Array[DataType],
     outputs: ArrayBuffer[SpillableColumnarBatch],
     batch: ColumnarBatch,
     mask: ColumnVector,
     instructionExecs: Seq[GpuInstruction]): Unit = {

    if (instructionExecs.isEmpty) {
      mask.close()
      return
    }

    val instructionExec = instructionExecs.head

    val (condMask, nextMask) = withResource(mask) { _ =>
      withResource(instructionExec.evaluateCondition(batch)) { cond =>
        // cond.and(mask) may contain NULLs if cond has NULLs
        // Replace NULLs with FALSE since NULL condition means "condition not satisfied"
        val condMask = withResource(cond.getBase.and(mask)) { condAndMask =>
          withResource(Scalar.fromBool(false)) { falseScalar =>
            condAndMask.replaceNulls(falseScalar)
          }
        }
        closeOnExcept(condMask) { _ =>
          // nextMask includes rows where condition was FALSE or NULL
          // Since condMask has NULLs replaced with FALSE, NOT(condMask) correctly
          // includes rows that were NULL in the original condition
          val nextMask = withResource(condMask.not()) { notCondMask =>
            mask.and(notCondMask)
          }
          (condMask, nextMask)
        }
      }
    }

    closeOnExcept(nextMask) { _ =>
      withResource(condMask) { _ =>
        val thisFilteredBatch = GpuColumnVector.filter(batch, dataTypes, condMask)
        withResource(thisFilteredBatch) { _ =>
          outputs ++= instructionExec.applyOutputs(thisFilteredBatch)
            .map(SpillableColumnarBatch
              .apply(_, SpillPriorities.ACTIVE_ON_DECK_PRIORITY))
        }
      }
    }

    processInstructionSet(dataTypes, outputs, batch, nextMask, instructionExecs.tail)
  }
}

