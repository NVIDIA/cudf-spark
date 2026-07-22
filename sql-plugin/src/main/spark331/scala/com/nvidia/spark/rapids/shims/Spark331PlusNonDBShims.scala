/*
 * Copyright (c) 2022-2026, NVIDIA CORPORATION.
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
{"spark": "331"}
{"spark": "332"}
{"spark": "333"}
{"spark": "334"}
{"spark": "340"}
{"spark": "341"}
{"spark": "342"}
{"spark": "343"}
{"spark": "344"}
{"spark": "350"}
{"spark": "351"}
{"spark": "352"}
{"spark": "353"}
{"spark": "354"}
{"spark": "355"}
{"spark": "356"}
{"spark": "357"}
{"spark": "358"}
{"spark": "400"}
{"spark": "401"}
{"spark": "402"}
{"spark": "403"}
{"spark": "411"}
{"spark": "412"}
{"spark": "413"}
{"spark": "420"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.shims

import com.nvidia.spark.rapids.{ExprChecks, ExprRule, GpuCast, GpuExpression, GpuOverrides, TypeSig, UnaryExprMeta}

import org.apache.spark.sql.catalyst.expressions.{CheckOverflowInTableInsert, Expression}
import org.apache.spark.sql.rapids.GpuCheckOverflowInTableInsert

trait Spark331PlusNonDBShims extends Spark330PlusNonDBShims {
  override def getExprs: Map[Class[_ <: Expression], ExprRule[_ <: Expression]] = {
    val map: Map[Class[_ <: Expression], ExprRule[_ <: Expression]] = Seq(
      // Add expression CheckOverflowInTableInsert starting Spark-3.3.1+
      // Accepts all types as input as the child Cast does the type checking and the calculations.
      GpuOverrides.expr[CheckOverflowInTableInsert](
        "Casting a numeric value as another numeric type in store assignment",
        ExprChecks.unaryProjectInputMatchesOutput(
          TypeSig.all,
          TypeSig.all),
        (t, conf, p, r) => new UnaryExprMeta[CheckOverflowInTableInsert](t, conf, p, r) {
          override def convertToGpu(child: Expression): GpuExpression = {
            child match {
              case c: GpuCast => GpuCheckOverflowInTableInsert(c, t.columnName)
              case _ =>
                throw new IllegalStateException("Expression child is not of Type GpuCast")
            }
          }
        })
    ).map(r => (r.getClassFor.asSubclass(classOf[Expression]), r)).toMap
    super.getExprs ++ map
  }
}
