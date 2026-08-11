/*
 * Copyright (c) 2024, NVIDIA CORPORATION.
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

package org.apache.spark.sql.nvidia

import com.nvidia.spark.rapids.RapidsConf

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.{Expression, ScalaUDF}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule

case class LogicalPlanRules() extends Rule[LogicalPlan] with Logging {
  val replacePartialFunc: PartialFunction[Expression, Expression] = {
    case f: ScalaUDF if DFUDF.getDFUDF(f.function).isDefined =>
      DFUDF.getDFUDF(f.function).map {
        dfudf => DFUDFShims.columnToExpr(
          dfudf(f.children.map(DFUDFShims.exprToColumn(_)).toArray))
      }.getOrElse{
        throw new IllegalStateException("Inconsistent results when extracting df_udf")
      }
  }

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (RapidsConf.DFUDF_ENABLED.get(plan.conf)) {
      plan.transformExpressions(replacePartialFunc)
    } else {
      plan
    }
  }
}
