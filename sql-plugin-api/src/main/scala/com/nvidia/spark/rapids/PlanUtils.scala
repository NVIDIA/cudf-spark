/*
 * Copyright (c) 2021-2023, NVIDIA CORPORATION.
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

import scala.collection.mutable.ListBuffer
import scala.util.Try

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, BroadcastQueryStageExec, ShuffleQueryStageExec}

object PlanUtils {
  def getBaseNameFromClass(planClassStr: String): String = {
    val firstDotIndex = planClassStr.lastIndexOf(".")
    if (firstDotIndex != -1) planClassStr.substring(firstDotIndex + 1) else planClassStr
  }

  /**
   * Determines if plan is either fallbackCpuClass or a subclass thereof
   *
   * Useful subclass expression are LeafLike
   *
   * @param plan
   * @param fallbackCpuClass
   * @return
   */
  def sameClass(plan: SparkPlan, fallbackCpuClass: String): Boolean = {
    val planClass = plan.getClass
    val execNameWithoutPackage = getBaseNameFromClass(planClass.getName)
    execNameWithoutPackage == fallbackCpuClass ||
      plan.getClass.getName == fallbackCpuClass ||
      Try(ShimReflectionUtils.loadClass(fallbackCpuClass))
        .map(_.isAssignableFrom(planClass))
        .getOrElse(false)
  }

  /**
   * Return list of matching predicates present in the expression
   */
  def findExpressions(exp: Expression, predicate: Expression => Boolean): Seq[Expression] = {
    def recurse(
        exp: Expression,
        predicate: Expression => Boolean,
        accum: ListBuffer[Expression]): Seq[Expression] = {
      exp match {
        case _ if predicate(exp) =>
          accum += exp
          exp.children.flatMap(p => recurse(p, predicate, accum)).headOption
        case other =>
          other.children.flatMap(p => recurse(p, predicate, accum)).headOption
      }
      accum.toSeq
    }
    recurse(exp, predicate, new ListBuffer[Expression]())
  }
  
  /**
   * Return list of matching predicates present in the plan
   * This is in shim due to changes in ShuffleQueryStageExec between Spark versions.
   */
  def findOperators(plan: SparkPlan, predicate: SparkPlan => Boolean): Seq[SparkPlan] = {
    def recurse(
        plan: SparkPlan,
        predicate: SparkPlan => Boolean,
        accum: ListBuffer[SparkPlan]): Seq[SparkPlan] = {
      plan match {
        case _ if predicate(plan) =>
          accum += plan
          plan.children.flatMap(p => recurse(p, predicate, accum)).headOption
        case a: AdaptiveSparkPlanExec => recurse(a.executedPlan, predicate, accum)
        case qs: BroadcastQueryStageExec => recurse(qs.broadcast, predicate, accum)
        case qs: ShuffleQueryStageExec => recurse(qs.shuffle, predicate, accum)
        case other => other.children.flatMap(p => recurse(p, predicate, accum)).headOption
      }
      accum.toSeq
    }
    recurse(plan, predicate, new ListBuffer[SparkPlan]())
  }
}
