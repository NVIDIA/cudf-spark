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
{"spark": "340"}
{"spark": "341"}
{"spark": "342"}
{"spark": "343"}
{"spark": "344"}
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
package com.nvidia.spark.rapids.shims

import com.nvidia.spark.rapids._

import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.BroadcastQueryStageExec
import org.apache.spark.sql.execution.exchange.ReusedExchangeExec
import org.apache.spark.sql.rapids.execution.GpuBroadcastExchangeExec

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
      case reused: ReusedExchangeExec => reused.child.isInstanceOf[GpuBroadcastExchangeExec]
      case _: GpuBroadcastExchangeExec => true
      case _ => buildSide.canThisBeReplaced
    }
  }

  def verifyBuildSideWasReplaced(buildSide: SparkPlan): Unit = {
    val buildSideOnGpu = buildSide match {
      case bqse: BroadcastQueryStageExec => bqse.plan.isInstanceOf[GpuBroadcastExchangeExec] ||
          bqse.plan.isInstanceOf[ReusedExchangeExec] &&
              bqse.plan.asInstanceOf[ReusedExchangeExec]
                  .child.isInstanceOf[GpuBroadcastExchangeExec]
      case reused: ReusedExchangeExec => reused.child.isInstanceOf[GpuBroadcastExchangeExec]
      case _: GpuBroadcastExchangeExec => true
      case _ => false
    }
    if (!buildSideOnGpu) {
      throw new IllegalStateException(s"the broadcast must be on the GPU too")
    }
  }
}
