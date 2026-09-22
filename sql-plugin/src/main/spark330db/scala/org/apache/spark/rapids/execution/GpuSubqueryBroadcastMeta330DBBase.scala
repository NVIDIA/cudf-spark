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
package org.apache.spark.sql.rapids.execution

import com.nvidia.spark.rapids.{BaseExprMeta, DataFromReplacementRule, RapidsConf, RapidsMeta, SparkPlanMeta}

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.physical.IdentityBroadcastMode
import org.apache.spark.sql.execution.{SparkPlan, SubqueryBroadcastExec}
import org.apache.spark.sql.execution.adaptive.{BroadcastQueryStageExec}
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeExec, ReusedExchangeExec}
import org.apache.spark.sql.execution.joins.HashedRelationBroadcastMode

abstract class GpuSubqueryBroadcastMeta330DBBase(s: SubqueryBroadcastExec,
                                        conf: RapidsConf,
                                        p: Option[RapidsMeta[_, _, _]],
                                        r: DataFromReplacementRule) extends
  SparkPlanMeta[SubqueryBroadcastExec](s, conf, p, r) {
  protected var broadcastBuilder: () => SparkPlan = _

  override val childExprs: Seq[BaseExprMeta[_]] = Nil

  override val childPlans: Seq[SparkPlanMeta[SparkPlan]] = Nil

  override def tagPlanForGpu(): Unit = s.child match {
    // DPP: For AQE off, in this case, we handle DPP by converting the underlying
    // BroadcastExchangeExec to GpuBroadcastExchangeExec.
    // This is slightly different from the Apache Spark case, because Spark
    // sends the underlying plan into the plugin in advance via the PlanSubqueries rule.
    // Here, we have the full non-GPU subquery plan, so we convert the whole
    // thing.
    case ex @ BroadcastExchangeExec(_, child) =>
      val exMeta = new GpuBroadcastMeta(ex.copy(child = child), conf, p, r)
      exMeta.tagForGpu()
      if (exMeta.canThisBeReplaced) {
        broadcastBuilder = () => exMeta.convertToGpu()
      } else {
        willNotWorkOnGpu("underlying BroadcastExchange can not run in the GPU.")
      }
    // DPP: For AQE on, we have an almost completely different scenario then before,
    // Databricks uses a BroadcastQueryStageExec and either:
    //  1) provide an underlying BroadcastExchangeExec that we will have to convert
    //     somehow
    //  2) might already do the reuse work for us. The ReusedExchange is now a
    //     part of the SubqueryBroadcast, so we send it back here as underlying the
    //     GpuSubqueryBroadcastExchangeExec
    case bqse: BroadcastQueryStageExec =>
      bqse.plan match {
        case ex: BroadcastExchangeExec =>
          val exMeta = new GpuBroadcastMeta(ex, conf, p, r)
          exMeta.tagForGpu()
          if (exMeta.canThisBeReplaced) {
            broadcastBuilder = () => exMeta.convertToGpu()
          } else {
            willNotWorkOnGpu("underlying BroadcastExchange can not run in the GPU.")
          }
        case reuse: ReusedExchangeExec =>
          reuse.child match {
            case _: GpuBroadcastExchangeExec =>
              // A BroadcastExchange has already been replaced, so it can run on the GPU
              broadcastBuilder = () => reuse
            case _ =>
              willNotWorkOnGpu("underlying BroadcastExchange can not run in the GPU.")
          }
      }
    case _ =>
      willNotWorkOnGpu("the subquery to broadcast can not entirely run in the GPU.")
  }
  /**
   * Simply returns the original plan. Because its only child, BroadcastExchange, doesn't
   * need to change if SubqueryBroadcastExec falls back to the CPU.
   */
  override def convertToCpu(): SparkPlan = s

  /** Extract the broadcast mode key expressions if there are any. */
  protected def getBroadcastModeKeyExprs: Option[Seq[Expression]] = {
    val broadcastMode = s.child match {
      case b: BroadcastExchangeExec =>
        b.mode
      case bqse: BroadcastQueryStageExec =>
        bqse.plan match {
          case b: BroadcastExchangeExec =>
            b.mode
          case reuse: ReusedExchangeExec =>
            reuse.child match {
              case g: GpuBroadcastExchangeExec =>
                g.mode
            }
          case _ =>
            throw new AssertionError("should not reach here")
        }
    }

    broadcastMode match {
      case HashedRelationBroadcastMode(keys, _) => Some(keys)
      case IdentityBroadcastMode => None
      case m => throw new UnsupportedOperationException(s"Unknown broadcast mode $m")
    }
  }
}

