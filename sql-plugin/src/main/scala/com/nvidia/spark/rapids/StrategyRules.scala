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

package com.nvidia.spark.rapids

import com.nvidia.spark.rapids.delta.DeltaProvider

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.{SparkPlan, SparkStrategy}

/**
 * Provides a Strategy that can implement rules for translating
 * custom logical plan nodes to physical plan nodes.
 * @note This is instantiated via reflection from ShimLoader.
 */
class StrategyRules extends SparkStrategy {

  private lazy val strategies: Seq[SparkStrategy] = {
    // Currently we only have custom plan nodes that originate from
    // DeltaLake, but if we add other custom plan nodes later
    // their strategies can be appended here.
    DeltaProvider().getStrategyRules
  }

  override def apply(plan: LogicalPlan): Seq[SparkPlan] = {
    val rapidsConf = new RapidsConf(plan.conf)
    if (rapidsConf.isSqlEnabled && rapidsConf.isSqlExecuteOnGPU) {
      // Using view since the strategies are first fit.
      strategies.view.map(_(plan)).find(_.nonEmpty).getOrElse(Nil)
    } else {
      Nil
    }
  }
}
