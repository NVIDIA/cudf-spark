/*
 * Copyright (c) 2023-2026, NVIDIA CORPORATION.
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

import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.{BroadcastQueryStageExec, ShuffleQueryStageExec}
import org.apache.spark.sql.execution.exchange.ReusedExchangeExec
import org.apache.spark.sql.rapids.execution.{GpuBroadcastExchangeExec, GpuCustomShuffleReaderExec, GpuShuffleExchangeExecBase}

abstract class GpuBroadcastJoinMeta[INPUT <: SparkPlan](plan: INPUT,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
  extends SparkPlanMeta[INPUT](plan, conf, parent, rule) {

  def canBuildSideBeReplaced(buildSide: SparkPlanMeta[_]): Boolean = {
    buildSide.wrapped match {
      case bqse: BroadcastQueryStageExec => bqse.plan.isInstanceOf[GpuBroadcastExchangeExec] ||
          bqse.plan.isInstanceOf[ReusedExchangeExec] &&
          bqse.plan.asInstanceOf[ReusedExchangeExec]
              .child.isInstanceOf[GpuBroadcastExchangeExec]
      case sqse: ShuffleQueryStageExec => sqse.plan.isInstanceOf[GpuShuffleExchangeExecBase] ||
          sqse.plan.isInstanceOf[ReusedExchangeExec] &&
          sqse.plan.asInstanceOf[ReusedExchangeExec]
              .child.isInstanceOf[GpuShuffleExchangeExecBase]
      case reused: ReusedExchangeExec => reused.child.isInstanceOf[GpuBroadcastExchangeExec] ||
          reused.child.isInstanceOf[GpuShuffleExchangeExecBase]
      case _: GpuBroadcastExchangeExec | _: GpuShuffleExchangeExecBase => true
      case _ => buildSide.canThisBeReplaced
    }
  }

  def verifyBuildSideWasReplaced(buildSide: SparkPlan): Unit = {
    def isOnGpu(sqse: ShuffleQueryStageExec): Boolean = sqse.plan match {
      case _: GpuShuffleExchangeExecBase => true
      case ReusedExchangeExec(_, _: GpuShuffleExchangeExecBase) => true
      case _ => false
    }
    val buildSideOnGpu = buildSide match {
      case bqse: BroadcastQueryStageExec => bqse.plan.isInstanceOf[GpuBroadcastExchangeExec] ||
          bqse.plan.isInstanceOf[ReusedExchangeExec] &&
              bqse.plan.asInstanceOf[ReusedExchangeExec]
                  .child.isInstanceOf[GpuBroadcastExchangeExec]
      case sqse: ShuffleQueryStageExec => isOnGpu(sqse)
      case reused: ReusedExchangeExec => reused.child.isInstanceOf[GpuBroadcastExchangeExec] ||
          reused.child.isInstanceOf[GpuShuffleExchangeExecBase]
      case _: GpuBroadcastExchangeExec | _: GpuShuffleExchangeExecBase => true
      case GpuCustomShuffleReaderExec(sqse: ShuffleQueryStageExec, _) => isOnGpu(sqse)
      case _ => false
    }
    if (!buildSideOnGpu) {
      throw new IllegalStateException(s"the broadcast must be on the GPU too")
    }
  }
}
