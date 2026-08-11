/*
 * Copyright (c) 2021, NVIDIA CORPORATION.
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

package com.nvidia.spark.udf

import ai.rapids.cudf.{NvtxColor, NvtxRange}
import com.nvidia.spark.rapids.RapidsConf

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.{Expression, NamedExpression, ScalaUDF}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.rapids.GpuScalaUDF.getRapidsUDFInstance


case class LogicalPlanRules() extends Rule[LogicalPlan] with Logging {
  def replacePartialFunc(plan: LogicalPlan): PartialFunction[Expression, Expression] = {
    case d: Expression => {
      val nvtx = new NvtxRange("replace UDF", NvtxColor.BLUE)
      try {
        attemptToReplaceExpression(plan, d)
      } finally {
        nvtx.close()
      }
    }
  }

  def attemptToReplaceExpression(plan: LogicalPlan, exp: Expression): Expression = {
    val conf = new RapidsConf(plan.conf)
    // iterating over NamedExpression
    exp match {
      // Check if this UDF implements RapidsUDF interface. If so, the UDF has already provided a
      // columnar execution that could run on GPU, then no need to translate it to Catalyst
      // expressions. If not, compile it.
      case f: ScalaUDF if getRapidsUDFInstance(f.function).isEmpty =>
        GpuScalaUDFLogical(f).compile(conf.isTestEnabled)
      case _ =>
        if (exp == null) {
          exp
        } else {
          try {
            if (exp.children != null && !exp.children.contains(null)) {
              exp.withNewChildren(exp.children.map(c => {
                if (c != null && c.isInstanceOf[Expression]) {
                  attemptToReplaceExpression(plan, c)
                } else {
                  c
                }
              }))
            } else {
              exp
            }
          } catch {
            case _: NullPointerException => {
              exp
            }
          }
        }
    }
  }

  override def apply(plan: LogicalPlan): LogicalPlan = {
    val conf = new RapidsConf(plan.conf)
    if (conf.isUdfCompilerEnabled) {
      plan match {
        case project: Project =>
          Project(project.projectList.map(e => attemptToReplaceExpression(plan, e))
              .asInstanceOf[Seq[NamedExpression]], apply(project.child))
        case x => {
          x.transformExpressions(replacePartialFunc(plan))
        }
      }
    } else {
      plan
    }
  }
}
