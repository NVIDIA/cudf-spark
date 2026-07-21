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
{"spark": "400db173"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.shims

import com.databricks.sql.transaction.tahoe.perf.DeltaOptimizedWritePartitioning
import com.nvidia.spark.rapids._

import org.apache.spark.sql.catalyst.expressions.{CollationAwareMurmur3Hash, CollationAwareXxHash64, Expression}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.{OneRowRelationExec, SparkPlan}

object SparkShimImpl extends Spark400PlusDBShims {

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

  override def getPartitionings: Map[Class[_ <: Partitioning],
      PartRule[_ <: Partitioning]] = {
    val shimPartitionings = Seq(
      GpuOverrides.part[DeltaOptimizedWritePartitioning](
        "Delta optimized write partitioning",
        PartChecks(),
        (partitioning, conf, parent, rule) =>
          new PartMeta[DeltaOptimizedWritePartitioning](
              partitioning, conf, parent, rule) {
            // Keep DBR's partitioning as the CPU output partitioning on the converted exchange
            // so its DELTA_OPTIMIZED_WRITE AQE/skew rules still see the native contract. The
            // shuffle dependency itself uses the equivalent physical hash partitioning on GPU.
            override val childParts: Seq[PartMeta[_]] = Seq(GpuOverrides.wrapPart(
              partitioning.getPhysicalPartitioning, this.conf, Some(this)))

            override def convertToGpu(): GpuPartitioning = childParts.head.convertToGpu()
          })
    ).map(r => (r.getClassFor.asSubclass(classOf[Partitioning]), r)).toMap
    super.getPartitionings ++ shimPartitionings
  }
}
