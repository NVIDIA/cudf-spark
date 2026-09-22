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
{"spark": "330db"}
{"spark": "332db"}
{"spark": "350db143"}
{"spark": "400db173"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.shims

import com.nvidia.spark.rapids._

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical.SinglePartition
import org.apache.spark.sql.execution.{ColumnarToRowTransition, SparkPlan}
import org.apache.spark.sql.execution.adaptive.ShuffleQueryStageExec
import org.apache.spark.sql.execution.exchange.{EXECUTOR_BROADCAST, ShuffleExchangeExec, ShuffleExchangeLike}
import org.apache.spark.sql.rapids.{GpuCheckOverflowInTableInsert, GpuElementAtMeta}
import org.apache.spark.sql.rapids.execution.{GpuBroadcastHashJoinExec, GpuBroadcastNestedLoopJoinExec}

trait Spark330PlusDBShims extends Spark321PlusDBShims {
  override def getExprs: Map[Class[_ <: Expression], ExprRule[_ <: Expression]] = {
    val shimExprs: Map[Class[_ <: Expression], ExprRule[_ <: Expression]] = Seq(
      GpuOverrides.expr[CheckOverflowInTableInsert](
        "Casting a numeric value as another numeric type in store assignment",
        ExprChecks.unaryProjectInputMatchesOutput(
          TypeSig.all,
          TypeSig.all),
        (t, conf, p, r) => new UnaryExprMeta[CheckOverflowInTableInsert](t, conf, p, r) {
          override def convertToGpu(child: Expression): GpuExpression = {
            child match {
              case c: GpuCast => GpuCheckOverflowInTableInsert(c, t.columnName)
              case _ =>
                throw new IllegalStateException("Expression child is not of Type GpuCast")
            }
          }
        }),
      GpuElementAtMeta.elementAtRule(true)
    ).map(r => (r.getClassFor.asSubclass(classOf[Expression]), r)).toMap
    super.getExprs ++ shimExprs ++ YearMonthIntervalShims.exprs ++
      DayTimeIntervalShims.exprs ++ RoundingShims.exprs
  }

  override def getExecs: Map[Class[_ <: SparkPlan], ExecRule[_ <: SparkPlan]] =
    super.getExecs ++ PythonMapInArrowExecShims.execs

  override def reproduceEmptyStringBug: Boolean = false

  override def isExecutorBroadcastShuffle(shuffle: ShuffleExchangeLike): Boolean = {
    shuffle.shuffleOrigin.equals(EXECUTOR_BROADCAST)
  }

  override def shuffleParentReadsShuffleData(shuffle: ShuffleExchangeLike,
      parent: SparkPlan): Boolean = {
    parent match {
      case _: GpuBroadcastHashJoinExec =>
        shuffle.shuffleOrigin.equals(EXECUTOR_BROADCAST)
      case _: GpuBroadcastNestedLoopJoinExec =>
        shuffle.shuffleOrigin.equals(EXECUTOR_BROADCAST)
      case _ => false
    }
  }

  /*
   * Explicitly add in the CPU exchange for executor broadcast. Generally
   * we expect the plan to be passed in to be a GPU columnar to row but
   * we are not explicitly limiting it.
   */
  override def addExecBroadcastShuffle(p: SparkPlan): SparkPlan = {
    ShuffleExchangeExec(SinglePartition, p, EXECUTOR_BROADCAST)
  }

  override def addRowShuffleToQueryStageTransitionIfNeeded(c2r: ColumnarToRowTransition,
      sqse: ShuffleQueryStageExec): SparkPlan = {
    val plan = GpuTransitionOverrides.getNonQueryStagePlan(sqse)
    plan match {
      case shuffle: ShuffleExchangeLike if shuffle.shuffleOrigin.equals(EXECUTOR_BROADCAST) =>
        addExecBroadcastShuffle(c2r)
      case _ => c2r
    }
  }
}
