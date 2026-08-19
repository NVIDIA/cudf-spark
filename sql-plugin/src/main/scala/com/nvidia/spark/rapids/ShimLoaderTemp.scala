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

package com.nvidia.spark.rapids

import com.nvidia.spark.rapids.delta.DeltaProbe
import com.nvidia.spark.rapids.iceberg.IcebergProbe

import org.apache.spark.sql.rapids.{AdaptiveSparkPlanHelperShim, ExecutionPlanCaptureCallbackBase}

// TODO contains signatures that cannot yet be moved to sql-plugin-api
object ShimLoaderTemp {
  //
  // Reflection-based API with Spark to switch the classloader used by the caller
  //

  def newOptimizerClass(className: String): Optimizer = {
    ShimReflectionUtils.newInstanceOf[Optimizer](className)
  }

  def newExplainPlan(): ExplainPlanBase = {
    ShimReflectionUtils.newInstanceOf[ExplainPlanBase]("com.nvidia.spark.rapids.ExplainPlanImpl")
  }

  def newHiveProvider(): HiveProvider= {
    ShimReflectionUtils.
      newInstanceOf[HiveProvider]("org.apache.spark.sql.hive.rapids.HiveProviderImpl")
  }

  def newAvroProvider(): AvroProvider = ShimReflectionUtils.newInstanceOf[AvroProvider](
    "org.apache.spark.sql.rapids.AvroProviderImpl")

  def newDeltaProbe(): DeltaProbe = ShimReflectionUtils.newInstanceOf[DeltaProbe](
    "com.nvidia.spark.rapids.delta.DeltaProbeImpl")

  def newIcebergProbe(): IcebergProbe = ShimReflectionUtils.newInstanceOf[IcebergProbe](
    "com.nvidia.spark.rapids.iceberg.IcebergProbeImpl")

  def newPlanShims(): PlanShims = ShimReflectionUtils.newInstanceOf[PlanShims](
    "com.nvidia.spark.rapids.shims.PlanShimsImpl"
  )

  def newAdaptiveSparkPlanHelperShim(): AdaptiveSparkPlanHelperShim =
    ShimReflectionUtils.newInstanceOf[AdaptiveSparkPlanHelperShim](
      "com.nvidia.spark.rapids.AdaptiveSparkPlanHelperImpl"
    )

  def newExecutionPlanCaptureCallbackBase(): ExecutionPlanCaptureCallbackBase =
    ShimReflectionUtils.
        newInstanceOf[ExecutionPlanCaptureCallbackBase](
          "org.apache.spark.sql.rapids.ShimmedExecutionPlanCaptureCallbackImpl")
}
