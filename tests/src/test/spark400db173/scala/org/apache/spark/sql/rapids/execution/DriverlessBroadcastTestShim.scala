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
/*** spark-rapids-shim-json-lines
{"spark":["400db173","410db183"],"carryForward":"none"}
spark-rapids-shim-json-lines ***/
package org.apache.spark.sql.rapids.execution

import com.nvidia.spark.rapids.GpuBuildRight

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.execution.{LocalTableScanExec, SparkPlan}
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, BroadcastQueryStageExec}

/** Isolates Spark constructor and cancellation differences used by the shared tests. */
object DriverlessBroadcastTestShim {
  def scan(output: Seq[Attribute], rows: Seq[InternalRow]): LocalTableScanExec =
    LocalTableScanExec(output, rows, None)

  def join(probe: SparkPlan, build: SparkPlan): GpuBroadcastHashJoinExec =
    GpuBroadcastHashJoinExec(probe.output, build.output, Inner, GpuBuildRight,
      None, probe, build, false, false)

  def stage(plan: SparkPlan, canonicalized: SparkPlan): BroadcastQueryStageExec = {
    val session = SparkSession.active
    val key = "spark.sql.adaptive.enabled"
    val previous = session.conf.get(key)
    try {
      session.conf.set(key, "true")
      // Obtain a real adaptive context without materializing a query or using a null context.
      val adaptive = session.range(1).repartition(1).queryExecution.executedPlan
        .asInstanceOf[AdaptiveSparkPlanExec]
      BroadcastQueryStageExec(1, plan, canonicalized, isSparkExchange = false)(adaptive.context)
    } finally {
      session.conf.set(key, previous)
    }
  }

  def cancel(stage: BroadcastQueryStageExec): Unit = stage.cancel()
}
