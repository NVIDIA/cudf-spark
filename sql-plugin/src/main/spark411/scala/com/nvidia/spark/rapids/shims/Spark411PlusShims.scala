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

/*** spark-rapids-shim-json-lines
{"spark": "411"}
{"spark": "412"}
{"spark": "420"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.shims

import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.{HashExprChecks, Murmur3HashExprMeta, XxHash64ExprMeta}

import org.apache.spark.sql.catalyst.expressions.{CollationAwareMurmur3Hash, CollationAwareXxHash64,
  Expression}
import org.apache.spark.sql.execution.{OneRowRelationExec, SparkPlan}

/**
 * Shared Spark 4.1.x shim rules.
 *
 * Spark 4.1.1 and later Spark 4.1.x versions share the same collation-aware hash expressions,
 * TimestampAddInterval support, and OneRowRelationExec handling. Keeping those rules here avoids
 * duplicating the same registrations in each version-specific SparkShimImpl while still allowing a
 * later shim to override only the expressions whose behavior changed.
 */
trait Spark411PlusShims extends Spark400PlusCommonShims with RebaseShims {
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
    // Include TimeAddShims here because TimestampAddInterval landed in the shared 4.1.x surface
    // rather than being a one-version-only expression.
    super.getExprs ++ shimExprs ++ TimeAddShims.exprs
  }

  override def getExecs: Map[Class[_ <: SparkPlan], ExecRule[_ <: SparkPlan]] = {
    val shimExecs: Map[Class[_ <: SparkPlan], ExecRule[_ <: SparkPlan]] = Seq(
      // OneRowRelationExec is a new class in Spark 4.1.0 for single-row queries (e.g. SELECT 1)
      // GPU version produces a single ColumnarBatch with one row and zero columns
      GpuOverrides.exec[OneRowRelationExec](
        "Single row relation for literal queries without FROM clause",
        ExecChecks(TypeSig.all, TypeSig.all),
        (exec, conf, parent, rule) => new GpuOneRowRelationExecMeta(exec, conf, parent, rule))
    ).map(r => (r.getClassFor.asSubclass(classOf[SparkPlan]), r)).toMap
    super.getExecs ++ shimExecs
  }
}
