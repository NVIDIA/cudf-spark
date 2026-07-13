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
{"spark": "330"}
{"spark": "330db"}
{"spark": "331"}
{"spark": "332"}
{"spark": "332db"}
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
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.shims

import com.nvidia.spark.rapids.{ExprRule, GpuOverrides}
import com.nvidia.spark.rapids.{ExprChecks, GpuExpression, TypeSig, UnaryExprMeta}

import org.apache.spark.sql.catalyst.expressions.{Expression, RaiseError}
import org.apache.spark.sql.rapids.shims.GpuRaiseError

object RaiseErrorShim {
  val exprs: Map[Class[_ <: Expression], ExprRule[_ <: Expression]] = {
    Seq(GpuOverrides.expr[RaiseError](
      "Throw an exception",
      ExprChecks.unaryProject(
        TypeSig.NULL, TypeSig.NULL,
        TypeSig.STRING, TypeSig.STRING),
      (a, conf, p, r) => new UnaryExprMeta[RaiseError](a, conf, p, r) {
        override def convertToGpu(child: Expression): GpuExpression = GpuRaiseError(child)
      })).map(r => (r.getClassFor.asSubclass(classOf[Expression]), r)).toMap
  }
}
