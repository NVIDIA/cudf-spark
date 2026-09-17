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
{"spark": "420"}
{"spark": "500"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids

import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, EqualTo, IsNull, Or}
import org.apache.spark.sql.catalyst.plans.{Inner, LeftAnti}
import org.apache.spark.sql.catalyst.plans.logical.{Join, JoinHint, LocalRelation}
import org.apache.spark.sql.catalyst.plans.physical.{BroadcastMode, BroadcastPartitioning,
  IdentityBroadcastMode}
import org.apache.spark.sql.execution.{ColumnarToRowExec, LocalTableScanExec, SparkPlan}
import org.apache.spark.sql.execution.adaptive.{BroadcastQueryStageExec, LogicalQueryStage,
  LogicalQueryStageStrategy}
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeExec, ReusedExchangeExec}
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, BroadcastNestedLoopJoinExec,
  HashedRelationBroadcastMode}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.execution.{GpuBroadcastExchangeExec, GpuBroadcastToRowExec}
import org.apache.spark.sql.types.IntegerType

class GpuBroadcastToRowExecSuite extends SparkQueryCompareTestSuite {

  private val adaptiveConf = new SparkConf()
    .set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, "true")

  private def newGpuBroadcast(
      relation: LocalRelation,
      mode: BroadcastMode): GpuBroadcastExchangeExec = {
    val scan = LocalTableScanExec(relation.output, Nil, None)
    val cpuBroadcast = BroadcastExchangeExec(mode, scan)
    GpuBroadcastExchangeExec(mode, scan)(cpuBroadcast)
  }

  private def rewriteBroadcastStage(
      stagePlan: SparkPlan,
      mode: BroadcastMode): BroadcastQueryStageExec = {
    val stage = BroadcastQueryStageExec(0, stagePlan, stagePlan.canonicalized)
    val rewritten = new GpuTransitionOverrides()
      .optimizeAdaptiveTransitions(ColumnarToRowExec(stage), None)

    rewritten match {
      case rewrittenStage: BroadcastQueryStageExec =>
        rewrittenStage.plan match {
          case broadcastToRow: GpuBroadcastToRowExec =>
            assert(broadcastToRow.broadcastMode === mode)
          case other =>
            fail(s"Expected GpuBroadcastToRowExec, found:\n$other")
        }
        val expected = BroadcastPartitioning(mode)
        assert(rewrittenStage.broadcast.outputPartitioning === expected)
        rewrittenStage
      case other =>
        fail(s"Expected BroadcastQueryStageExec, found:\n$other")
    }
  }

  private def broadcastStage(
      relation: LocalRelation,
      mode: BroadcastMode,
      reuse: Boolean): LogicalQueryStage = {
    val gpuBroadcast = newGpuBroadcast(relation, mode)
    val stagePlan = if (reuse) {
      ReusedExchangeExec(gpuBroadcast.output, gpuBroadcast)
    } else {
      gpuBroadcast
    }
    LogicalQueryStage(relation, rewriteBroadcastStage(stagePlan, mode))
  }

  test("SPARK-56737: AQE broadcast rewrites preserve every broadcast mode") {
    withGpuSparkSession(_ => {
      val relation = LocalRelation(AttributeReference("a", IntegerType)())
      val modes = Seq[BroadcastMode](
        IdentityBroadcastMode,
        HashedRelationBroadcastMode(relation.output),
        HashedRelationBroadcastMode(relation.output, isNullAware = true))

      for {
        mode <- modes
        reuse <- Seq(false, true)
      } {
        broadcastStage(relation, mode, reuse)
      }
    }, adaptiveConf)
  }

  test("SPARK-56737: LogicalQueryStageStrategy recognizes rewritten broadcast stages") {
    withGpuSparkSession(_ => {
      Seq(false, true).foreach { reuse =>
        val left = LocalRelation(AttributeReference("l", IntegerType)())
        val right = LocalRelation(AttributeReference("r", IntegerType)())
        val regularHashedMode = HashedRelationBroadcastMode(left.output)
        val nullAwareHashedMode =
          HashedRelationBroadcastMode(left.output, isNullAware = true)

        val equiJoin = Join(
          broadcastStage(left, regularHashedMode, reuse),
          right,
          Inner,
          Some(EqualTo(left.output.head, right.output.head)),
          JoinHint.NONE)
        assert(LogicalQueryStageStrategy(equiJoin).head.isInstanceOf[BroadcastHashJoinExec])

        val equiJoinWithNullAwareStage = equiJoin.copy(
          left = broadcastStage(left, nullAwareHashedMode, reuse))
        assert(LogicalQueryStageStrategy(equiJoinWithNullAwareStage).isEmpty)

        val naajCondition = Or(
          EqualTo(left.output.head, right.output.head),
          IsNull(EqualTo(left.output.head, right.output.head)))
        val nullAwareAntiJoin = Join(
          left,
          broadcastStage(right,
            HashedRelationBroadcastMode(right.output, isNullAware = true), reuse),
          LeftAnti,
          Some(naajCondition),
          JoinHint.NONE)
        val naaj = LogicalQueryStageStrategy(nullAwareAntiJoin).head
          .asInstanceOf[BroadcastHashJoinExec]
        assert(naaj.isNullAwareAntiJoin)

        val nullAwareAntiJoinWithRegularStage = nullAwareAntiJoin.copy(
          right = broadcastStage(right, HashedRelationBroadcastMode(right.output), reuse))
        assert(LogicalQueryStageStrategy(nullAwareAntiJoinWithRegularStage).isEmpty)

        val identityJoin = Join(
          broadcastStage(left, IdentityBroadcastMode, reuse),
          right,
          Inner,
          None,
          JoinHint.NONE)
        assert(LogicalQueryStageStrategy(identityJoin).head
          .isInstanceOf[BroadcastNestedLoopJoinExec])
      }
    }, adaptiveConf)
  }
}
