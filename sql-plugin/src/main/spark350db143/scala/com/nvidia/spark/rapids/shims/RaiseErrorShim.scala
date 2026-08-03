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
{"spark": "350db143"}
{"spark": "400"}
{"spark": "400db173"}
{"spark": "401"}
{"spark": "402"}
{"spark": "403"}
{"spark": "404"}
{"spark": "411"}
{"spark": "412"}
{"spark": "413"}
{"spark": "420"}
spark-rapids-shim-json-lines ***/

package com.nvidia.spark.rapids.shims

import com.nvidia.spark.rapids.{ExprRule, GpuOverrides}
import com.nvidia.spark.rapids.{ExprChecks, TypeEnum, TypeSig}

import org.apache.spark.sql.catalyst.expressions.{Expression, RaiseError}
import org.apache.spark.sql.rapids.shims.RaiseErrorMeta

object RaiseErrorShim {
  val exprs: Map[Class[_ <: Expression], ExprRule[_ <: Expression]] = {
    Seq(GpuOverrides.expr[RaiseError](
      "Throw an exception",
      ExprChecks.binaryProject(
        TypeSig.NULL, TypeSig.NULL,
        // In Databricks 14.3 and Spark 4.0, RaiseError forwards the lhs expression
        // (i.e. the error-class) as a scalar value.  A vector/column here would be surprising.
        ("errorClass", TypeSig.lit(TypeEnum.STRING), TypeSig.STRING),
        ("errorParams", TypeSig.MAP.nested(TypeSig.STRING), TypeSig.MAP.nested(TypeSig.STRING)),
      ),
      (a, conf, p, r) => new RaiseErrorMeta(a, conf, p, r)
    )).map(r => (r.getClassFor.asSubclass(classOf[Expression]), r)).toMap
  }
}
