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

import com.nvidia.spark.rapids.{ExecChecks, ExecRule, GpuOverrides, TypeSig}

import org.apache.spark.sql.execution.{SparkPlan, SparkStrategy}

abstract class DeltaProviderImplBase extends DeltaProvider {

  override def getExecRules: Map[Class[_ <: SparkPlan], ExecRule[_ <: SparkPlan]] = {
    Seq(
      GpuOverrides.exec[RapidsProcessDeltaMergeJoinExec](
        "Post-processing of join as part of Delta Lake table merge",
        ExecChecks(TypeSig.all, TypeSig.all),
        (wrapped, conf, p, r) => new RapidsProcessDeltaMergeJoinMeta(wrapped, conf, p, r)
      ),
      GpuOverrides.exec[RapidsDeltaWriteExec](
        "GPU write into a Delta Lake table",
        ExecChecks.hiddenHack(),
        (wrapped, conf, p, r) => new RapidsDeltaWriteExecMeta(wrapped, conf, p, r)).invisible()
    ).collect { case r if r != null => (r.getClassFor.asSubclass(classOf[SparkPlan]), r) }.toMap
  }

  override def getStrategyRules: Seq[SparkStrategy] = Seq(
    RapidsProcessDeltaMergeJoinStrategy,
    RapidsDeltaWriteStrategy)
}
