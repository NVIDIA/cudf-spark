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
{"spark": "401"}
{"spark": "402"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.shims

import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.{HashExprChecks, Murmur3HashExprMeta, XxHash64ExprMeta}

import org.apache.spark.sql.catalyst.expressions.{CollationAwareMurmur3Hash, CollationAwareXxHash64, Expression}

object SparkShimImpl extends Spark400PlusCommonShims {
  override def getExprs: Map[Class[_ <: Expression], ExprRule[_ <: Expression]] = {
    val shimExprs: Map[Class[_ <: Expression], ExprRule[_ <: Expression]] = Seq(
      GpuOverrides.expr[CollationAwareMurmur3Hash](
        "Collation-aware murmur3 hash operator",
        HashExprChecks.murmur3ProjectChecks,
        Murmur3HashExprMeta.apply
      ),
      GpuOverrides.expr[CollationAwareXxHash64](
        "Collation-aware xxhash64 operator",
        HashExprChecks.xxhash64ProjectChecks,
        XxHash64ExprMeta.apply
      )
    ).map(r => (r.getClassFor.asSubclass(classOf[Expression]), r)).toMap
    super.getExprs ++ shimExprs
  }
}
