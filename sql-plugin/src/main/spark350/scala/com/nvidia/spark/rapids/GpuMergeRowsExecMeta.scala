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
package com.nvidia.spark.rapids

import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.MergeRows.{Discard, Instruction, Keep, Split}
import org.apache.spark.sql.execution.datasources.v2.{GpuMergeRowsExec, MergeRowsExec}
import org.apache.spark.sql.execution.datasources.v2.GpuMergeRowsExec.{GpuDiscard, GpuInstruction, GpuKeep, GpuSplit}

/**
 * Abstract base meta class for MergeRows Instruction expressions.
 * 
 * Instructions (Keep, Discard, Split) are expressions used in MergeRowsExec that
 * contain:
 * - A condition expression to evaluate
 * - Output expressions (Seq[Seq[Expression]]) to project when the condition is
 *   true. Each Seq[Expression] represents one set of output columns
 * 
 * This base class handles the common logic of wrapping child expressions and
 * converting them to GPU.
 */
abstract class InstructionExprMeta[INPUT <: Instruction](
    instruction: INPUT,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
  extends ExprMeta[INPUT](instruction, conf, parent, rule)

/**
 * Meta class for Keep instruction.
 * 
 * Keep instructions represent MATCHED and NOT MATCHED clauses that keep/update
 * rows. They evaluate a condition and project output expressions for matching rows.
 */
class GpuKeepInstructionMeta(
    keep: Keep,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
  extends InstructionExprMeta[Keep](keep, conf, parent, rule) {

  override def convertToGpuImpl(): GpuExpression = {
    val gpuCondition = childExprs.head.convertToGpu()
    val gpuOutputs = childExprs.tail.map(_.convertToGpu())
    GpuKeep(gpuCondition, gpuOutputs)
  }
}

/**
 * Meta class for Discard instruction.
 * 
 * Discard instructions represent MATCHED clauses that delete rows.
 * They evaluate a condition to determine which rows to discard.
 */
class GpuDiscardInstructionMeta(
    discard: Discard,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
  extends InstructionExprMeta[Discard](discard, conf, parent, rule) {

  override def convertToGpuImpl(): GpuExpression = {
    val gpuCondition = childExprs.head.convertToGpu()
    GpuDiscard(gpuCondition)
  }
}

/**
 * Meta class for Split instruction.
 * 
 * Split instructions represent clauses that can produce multiple output rows from
 * a single input row. They evaluate a condition and can generate multiple sets of
 * output expressions.
 */
class GpuSplitInstructionMeta(
    split: Split,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
  extends InstructionExprMeta[Split](split, conf, parent, rule) {

  override def convertToGpuImpl(): GpuExpression = {
    val gpuCondition = childExprs.head.convertToGpu()

    val (outputsPart, otherOutputsPart) = childExprs.tail.splitAt(childExprs.tail.length / 2)
    val gpuOutputs = outputsPart.map(_.convertToGpu())
    val gpuOtherOutputs = otherOutputsPart.map(_.convertToGpu())

    GpuSplit(gpuCondition, gpuOutputs, gpuOtherOutputs)
  }
}

/**
 * Meta class for MergeRowsExec.
 * 
 * MergeRowsExec is a regular physical plan executor (like ProjectExec, FilterExec)
 * that processes merge row logic - it's not a table-specific V2 command.
 */
class GpuMergeRowsExecMeta(
    mergeRows: MergeRowsExec,
    conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
  extends SparkPlanMeta[MergeRowsExec](mergeRows, conf, p, rule) {

  override def convertToGpu(): GpuExec = {
    val gpuChild = childPlans.head.convertIfNeeded()

    // Convert presence expressions
    val gpuIsSourceRowPresent = childExprs(0).convertToGpu()
    val gpuIsTargetRowPresent = childExprs(1).convertToGpu()

    var prevExprCnt = 2
    // Convert instruction sets - they remain as Instruction expressions with GPU children
    val gpuMatchedInstructions = childExprs
      .slice(prevExprCnt, prevExprCnt + mergeRows.matchedInstructions.length)
      .map(_.convertToGpu().asInstanceOf[GpuInstruction])

    prevExprCnt += mergeRows.matchedInstructions.length
    val gpuNotMatchedInstructions = childExprs
      .slice(prevExprCnt, prevExprCnt + mergeRows.notMatchedInstructions.length)
      .map(_.convertToGpu().asInstanceOf[GpuInstruction])

    prevExprCnt += mergeRows.notMatchedInstructions.length
    val gpuNotMatchedBySourceInstructions = childExprs
      .slice(prevExprCnt, prevExprCnt + mergeRows.notMatchedBySourceInstructions.length)
      .map(_.convertToGpu().asInstanceOf[GpuInstruction])

    prevExprCnt += mergeRows.notMatchedBySourceInstructions.length
    val gpuOutput = childExprs
      .slice(prevExprCnt, prevExprCnt + mergeRows.output.length)
      .map(_.convertToGpu().asInstanceOf[Attribute])

    GpuMergeRowsExec(
      gpuIsTargetRowPresent,
      gpuIsSourceRowPresent,
      gpuMatchedInstructions,
      gpuNotMatchedInstructions,
      gpuNotMatchedBySourceInstructions,
      mergeRows.checkCardinality,
      gpuOutput,
      gpuChild)
  }
}

