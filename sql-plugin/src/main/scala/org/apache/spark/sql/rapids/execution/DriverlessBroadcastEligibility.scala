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
package org.apache.spark.sql.rapids.execution

import java.lang.{Boolean => JBoolean}
import java.util.{Collections, IdentityHashMap}

import scala.collection.mutable.ArrayBuffer

import com.nvidia.spark.rapids.{GpuBuildLeft, GpuBuildRight, RapidsConf}

import org.apache.spark.sql.execution.{ExecSubqueryExpression, SparkPlan}
import org.apache.spark.sql.execution.adaptive.{BroadcastQueryStageExec, QueryStageExec}
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeExec, ReusedExchangeExec}
import org.apache.spark.sql.rapids.GpuShuffleEnv
import org.apache.spark.sql.rapids.shims.SparkSessionUtils

/** Shared planning checks; each planning phase analyzes its current consumer graph. */
private[execution] object DriverlessBroadcastEligibility {
  def gpuBuildReadRejections(conf: RapidsConf): Seq[String] =
    DriverlessBroadcastPolicy.rejections(conf) ++
      (if (!GpuShuffleEnv.useGPUShuffle(conf)) Seq("GPU shuffle is unavailable") else Seq.empty)

  private[execution] def planRejections(plan: SparkPlan, conf: RapidsConf): Seq[String] = {
    gpuBuildReadRejections(conf) ++ Seq(
      // ContextCleaner unregisters build shuffles and retires executor cache entries.
      if (SparkSessionUtils.sessionFromPlan(plan).sparkContext.cleaner.isEmpty) {
        Some("Spark context cleaner is disabled")
      } else None,
      if (conf.get(RapidsConf.HASH_TABLE_REUSE)) Some("hash-table reuse is enabled") else None
    ).flatten
  }

  private[execution] def joinRejections(join: GpuBroadcastHashJoinExec): Seq[String] = Seq(
    // The custom RDD does not execute the base join's empty/all-null NAAJ handling.
    if (join.isNullAwareAntiJoin) Some("null-aware anti join") else None,
    if (join.isSkewJoin) Some("skew-optimized broadcast join") else None,
    if (join.exists(_.expressions.exists(_.exists(_.isInstanceOf[ExecSubqueryExpression])))) {
      Some("join contains a subquery")
    } else None
  ).flatten

  /** Broadcast consumers other than eligible GPU joins require a real payload, not a token. */
  private[execution] def sharedBuilds(plan: SparkPlan): Seq[SparkPlan] = {
    val blocked = ArrayBuffer.empty[SparkPlan]
    val normal = Collections.newSetFromMap(
      new IdentityHashMap[SparkPlan, JBoolean]())
    val subqueries = Collections.newSetFromMap(
      new IdentityHashMap[SparkPlan, JBoolean]())
    val builds = Collections.newSetFromMap(
      new IdentityHashMap[SparkPlan, JBoolean]())
    def canonical(build: SparkPlan): Unit = build match {
      case exchange: GpuBroadcastExchangeExec => blocked += exchange.cpuCanonical.canonicalized
      case exchange: BroadcastExchangeExec =>
        blocked += exchange.canonicalized
      case stage: BroadcastQueryStageExec => canonical(stage.plan)
      case reused: ReusedExchangeExec => canonical(reused.child)
      case _ =>
    }
    def visit(node: SparkPlan, inSubquery: Boolean, gpuBuild: Boolean = false): Unit = {
      val seen = if (inSubquery) subqueries else if (gpuBuild) builds else normal
      if (seen.add(node)) {
        if (inSubquery || !gpuBuild) canonical(node)
        node match {
          case join: GpuBroadcastHashJoinExec if joinRejections(join).isEmpty =>
            val (build, stream) = join.buildSide match {
              case GpuBuildLeft => (join.left, join.right)
              case GpuBuildRight => (join.right, join.left)
            }
            visit(build, inSubquery, gpuBuild = true)
            visit(stream, inSubquery)
          case stage: QueryStageExec => visit(stage.plan, inSubquery, gpuBuild)
          case reused: ReusedExchangeExec => visit(reused.child, inSubquery, gpuBuild)
          case _ => node.children.foreach(visit(_, inSubquery))
        }
        node.expressions.foreach(_.foreach {
          case subquery: ExecSubqueryExpression => visit(subquery.plan, inSubquery = true)
          case _ =>
        })
      }
    }
    visit(plan, inSubquery = false)
    blocked.toSeq
  }

  final class Analysis(val reasons: Seq[String], blocked: Seq[SparkPlan]) {
    def rejection(join: GpuBroadcastHashJoinExec,
        exchange: Option[GpuBroadcastExchangeExec]): Option[String] = {
      reasons.headOption.orElse(joinRejections(join).headOption).orElse {
        exchange match {
          case Some(build) if blocked.exists(_.fastEquals(build.cpuCanonical.canonicalized)) =>
            Some("broadcast has a standard-payload consumer")
          case Some(build) => DriverlessBroadcastBuildAdmission.rejectionReason(build.child)
          case None => Some("no eligible driverless build exchange")
        }
      }
    }
  }

  def analyze(plan: SparkPlan, conf: RapidsConf): Analysis =
    new Analysis(planRejections(plan, conf), sharedBuilds(plan))
}
