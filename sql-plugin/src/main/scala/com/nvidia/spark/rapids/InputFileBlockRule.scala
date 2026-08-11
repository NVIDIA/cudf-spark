/*
 * Copyright (c) 2021-2023, NVIDIA CORPORATION.
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
package com.nvidia.spark.rapids

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.catalyst.expressions.{Expression, InputFileBlockLength, InputFileBlockStart, InputFileName}
import org.apache.spark.sql.execution.{FileSourceScanExec, LeafExecNode, SparkPlan}
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeLike, ShuffleExchangeLike}
import org.apache.spark.sql.rapids.{GpuInputFileBlockLength, GpuInputFileBlockStart, GpuInputFileName}

/**
 * A rule prevents the plans [SparkPlan (with first input_file_xxx expression), FileScan)
 * from running on GPU.
 * For more details, please go to https://github.com/NVIDIA/spark-rapids/issues/3333.
 */
object InputFileBlockRule {
  private type PlanMeta = SparkPlanMeta[SparkPlan]

  def apply(plan: PlanMeta): Unit = {
    // key: the SparkPlanMeta where has the first input_file_xxx expression
    // value: an array of the SparkPlanMeta chain [SparkPlan (with first input_file_xxx), FileScan)
    val resultOps = mutable.LinkedHashMap[PlanMeta, ArrayBuffer[PlanMeta]]()
    recursivelyResolve(plan, None, resultOps)

    // If we've found some chains, we should prevent the transition.
    resultOps.foreach { case (_, metas) =>
      metas.foreach(_.willNotWorkOnGpu("GPU plans may get incorrect file name" +
        ", or file start or file length from a CPU scan"))
    }
  }

  /**
   * Recursively apply the rule on the plan
   * @param plan the plan to be resolved.
   * @param key  the SparkPlanMeta with the first input_file_xxx
   * @param resultOps the found SparkPlan chain
   */
  private def recursivelyResolve(plan: PlanMeta, key: Option[PlanMeta],
      resultOps: mutable.LinkedHashMap[PlanMeta, ArrayBuffer[PlanMeta]]): Unit = {
    plan.wrapped match {
      case _: ShuffleExchangeLike => // Exchange will invalid the input_file_xxx
        key.map(p => resultOps.remove(p)) // Remove the chain from Map
        plan.childPlans.foreach(p => recursivelyResolve(p, None, resultOps))
      case _: FileSourceScanExec | _: BatchScanExec =>
        if (plan.canThisBeReplaced) { // FileScan can be replaced
          key.map(p => resultOps.remove(p)) // Remove the chain from Map
        }
      case _: BroadcastExchangeLike =>
        // noop: Don't go any further, the file info cannot come from a broadcast.
      case _: LeafExecNode => // We've reached the LeafNode but without any FileScan
        key.map(p => resultOps.remove(p)) // Remove the chain from Map
      case _ =>
        val newKey = if (key.isDefined) {
          // The node is in the middle of chain [SparkPlan with input_file_xxx, FileScan)
          resultOps.getOrElseUpdate(key.get, new ArrayBuffer[PlanMeta]) += plan
          key
        } else { // There is no parent node who has input_file_xxx
          if (hasInputFileExpression(plan.wrapped)) {
            // Current node has input_file_xxx. Mark it as the first node with input_file_xxx
            resultOps.getOrElseUpdate(plan, new ArrayBuffer[PlanMeta]) += plan
            Some(plan)
          } else {
            None
          }
        }
        plan.childPlans.foreach(p => recursivelyResolve(p, newKey, resultOps))
    }
  }

  private def hasInputFileExpression(expr: Expression): Boolean = expr match {
    case _: InputFileName => true
    case _: InputFileBlockStart => true
    case _: InputFileBlockLength => true
    case _: GpuInputFileName => true
    case _: GpuInputFileBlockStart => true
    case _: GpuInputFileBlockLength => true
    case e => e.children.exists(hasInputFileExpression)
  }

  /** Whether a plan has any InputFile{Name, BlockStart, BlockLength} expression. */
  def hasInputFileExpression(plan: SparkPlan): Boolean = {
    plan.expressions.exists(hasInputFileExpression)
  }

}
