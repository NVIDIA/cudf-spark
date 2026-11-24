/*
 * Copyright (c) 2026, NVIDIA CORPORATION.
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

import com.databricks.sql.transaction.tahoe.GenerateIdentityValues
import com.nvidia.spark.rapids._

// A named meta class so that its name shows in stack traces (issue #10838).
case class GpuGenerateIdentityValuesMeta(
    cpu: GenerateIdentityValues,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends ExprMeta[GenerateIdentityValues](cpu, conf, p, r) {
  override def tagExprForGpu(): Unit = {
    val gen = cpu.generator
    GpuGenerateIdentityValues.unsafeToAccelerate(gen.start, gen.step, gen.highWaterMarkOpt)
      .foreach(willNotWorkOnGpu)
  }
  override def convertToGpuImpl(): GpuExpression = {
    val gen = cpu.generator
    GpuGenerateIdentityValues(gen.start, gen.step, gen.highWaterMarkOpt,
      conf.isRetryContextCheckEnabled)
  }
}

object GpuGenerateIdentityValuesRule {
  val exprRule: ExprRule[GenerateIdentityValues] =
    GpuOverrides.expr[GenerateIdentityValues](
      "Generates the values of a Delta identity column",
      ExprChecks.projectOnly(TypeSig.LONG, TypeSig.LONG),
      (cpu, conf, parent, rule) => GpuGenerateIdentityValuesMeta(cpu, conf, parent, rule))
}
