/*
 * Copyright (c) 2025-2026, NVIDIA CORPORATION.
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
{"spark": "400"}
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

import com.nvidia.spark.rapids._

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.objects.Invoke
import org.apache.spark.sql.rapids.shims.InvokeExprMeta

/**
 * Shared 4.0.x shims for expressions and exec rules that are common.
 */
trait Spark400PlusCommonShims extends Spark350PlusNonDBShims {
  override def getExprs: Map[Class[_ <: Expression], ExprRule[_ <: Expression]] = {
    val shimExprs: Map[Class[_ <: Expression], ExprRule[_ <: Expression]] = Seq(
      GpuOverrides.expr[Invoke](
        "Calls the specified function on an object. This is a wrapper to other expressions, so " +
          "can not know the details in advance. E.g.: between is replaced by " +
          "And(GreaterThanOrEqual(ref, lower), LessThanOrEqual(ref, upper);  StructToJson is " +
          "replaced by Invoke(Literal(StructsToJsonEvaluator), evaluate, string_type, arguments)",
        InvokeCheck,
        InvokeExprMeta)
        .note("The supported types are not deterministic since it's a dynamic expression")
    ).map(r => (r.getClassFor.asSubclass(classOf[Expression]), r)).toMap
    super.getExprs ++ shimExprs
  }
}
