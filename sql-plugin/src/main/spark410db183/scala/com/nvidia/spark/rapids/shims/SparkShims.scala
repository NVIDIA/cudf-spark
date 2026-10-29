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
{"spark": "410db183"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.shims

import com.databricks.sql.transaction.tahoe.perf.DeltaOptimizedWritePartitioning
import com.nvidia.spark.rapids._

import org.apache.spark.sql.catalyst.expressions.{CollationAwareMurmur3Hash,
  CollationAwareXxHash64, Expression}
import org.apache.spark.sql.catalyst.plans.logical.MergeRows.{Discard, Keep, Split}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.{OneRowRelationExec, SparkPlan}
import org.apache.spark.sql.execution.datasources.v2.MergeRowsExec

object SparkShimImpl extends Spark400PlusDBShims with HyperbolicExpressionShims {

  private def createOneRowRelationExecMeta(
      exec: OneRowRelationExec,
      conf: com.nvidia.spark.rapids.RapidsConf,
      parent: Option[com.nvidia.spark.rapids.RapidsMeta[_, _, _]],
      rule: com.nvidia.spark.rapids.DataFromReplacementRule) =
    new GpuOneRowRelationExecMeta(exec, conf, parent, rule)

  private def createMergeRowsExecMeta(
      exec: MergeRowsExec,
      conf: com.nvidia.spark.rapids.RapidsConf,
      parent: Option[com.nvidia.spark.rapids.RapidsMeta[_, _, _]],
      rule: com.nvidia.spark.rapids.DataFromReplacementRule) =
    new GpuMergeRowsExecMeta(exec, conf, parent, rule)

  private def createKeepMeta(
      keep: Keep,
      conf: com.nvidia.spark.rapids.RapidsConf,
      parent: Option[com.nvidia.spark.rapids.RapidsMeta[_, _, _]],
      rule: com.nvidia.spark.rapids.DataFromReplacementRule) =
    new GpuKeepInstructionMeta(keep, conf, parent, rule)

  private def createDiscardMeta(
      discard: Discard,
      conf: com.nvidia.spark.rapids.RapidsConf,
      parent: Option[com.nvidia.spark.rapids.RapidsMeta[_, _, _]],
      rule: com.nvidia.spark.rapids.DataFromReplacementRule) =
    new GpuDiscardInstructionMeta(discard, conf, parent, rule)

  private def createSplitMeta(
      split: Split,
      conf: com.nvidia.spark.rapids.RapidsConf,
      parent: Option[com.nvidia.spark.rapids.RapidsMeta[_, _, _]],
      rule: com.nvidia.spark.rapids.DataFromReplacementRule) =
    new GpuSplitInstructionMeta(split, conf, parent, rule)

  private class DeltaOptimizedWritePartitioningMetaImpl(
      partitioning: DeltaOptimizedWritePartitioning,
      conf: com.nvidia.spark.rapids.RapidsConf,
      parent: Option[com.nvidia.spark.rapids.RapidsMeta[_, _, _]],
      rule: com.nvidia.spark.rapids.DataFromReplacementRule)
      extends PartMeta[DeltaOptimizedWritePartitioning](
              partitioning, conf, parent, rule) {
            // Preserve DBR's marker for its optimized-write AQE and skew rules while the
            // converted exchange executes the equivalent physical hash partitioning on GPU.
            override val childParts: Seq[PartMeta[_]] = Seq(GpuOverrides.wrapPart(
              partitioning.getPhysicalPartitioning, this.conf, Some(this)))

            override def convertToGpu(): GpuPartitioning = childParts.head.convertToGpu()
          }

  private def createDeltaOptimizedWritePartitioningMeta(
      partitioning: DeltaOptimizedWritePartitioning,
      conf: com.nvidia.spark.rapids.RapidsConf,
      parent: Option[com.nvidia.spark.rapids.RapidsMeta[_, _, _]],
      rule: com.nvidia.spark.rapids.DataFromReplacementRule) =
    new DeltaOptimizedWritePartitioningMetaImpl(partitioning, conf, parent, rule)

  override def getExecs: Map[Class[_ <: SparkPlan], ExecRule[_ <: SparkPlan]] = {
    val shimExecs: Map[Class[_ <: SparkPlan], ExecRule[_ <: SparkPlan]] = Seq(
      GpuOverrides.exec[OneRowRelationExec](
        "Single row relation for literal queries without FROM clause",
        ExecChecks(TypeSig.all, TypeSig.all),
        createOneRowRelationExecMeta),
      GpuOverrides.exec[MergeRowsExec](
        "Process merge rows for copy-on-write MERGE operations",
        ExecChecks((TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 +
          TypeSig.STRUCT + TypeSig.MAP + TypeSig.ARRAY + TypeSig.BINARY +
          GpuTypeShims.additionalCommonOperatorSupportedTypes).nested(),
          TypeSig.all),
        createMergeRowsExecMeta)
    ).map(rule => (rule.getClassFor.asSubclass(classOf[SparkPlan]), rule)).toMap
    super.getExecs ++ shimExecs
  }

  override def getExprs: Map[Class[_ <: Expression], ExprRule[_ <: Expression]] = {
    val shimExprs: Map[Class[_ <: Expression], ExprRule[_ <: Expression]] = Seq(
      GpuOverrides.expr[CollationAwareMurmur3Hash](
        "Collation-aware murmur3 hash operator",
        HashExprChecks.murmur3ProjectChecks,
        Murmur3HashExprMeta.apply),
      GpuOverrides.expr[CollationAwareXxHash64](
        "Collation-aware xxhash64 operator",
        HashExprChecks.xxhash64ProjectChecks,
        XxHash64ExprMeta.apply),
      GpuOverrides.expr[Keep](
        "Keep instruction for MERGE operations - keeps/updates rows based on condition",
        ExprChecks.projectOnly(
          TypeSig.all,
          TypeSig.all,
          Seq(ParamCheck("condition", TypeSig.all, TypeSig.all)),
          Some(RepeatingParamCheck("outputs", TypeSig.all, TypeSig.all))),
        createKeepMeta),
      GpuOverrides.expr[Discard](
        "Discard instruction for MERGE operations - discards rows based on condition",
        ExprChecks.projectOnly(
          TypeSig.all,
          TypeSig.all,
          Seq(ParamCheck("condition", TypeSig.all, TypeSig.all))),
        createDiscardMeta),
      GpuOverrides.expr[Split](
        "Split instruction for MERGE operations - splits rows into multiple outputs",
        ExprChecks.projectOnly(
          TypeSig.all,
          TypeSig.all,
          Seq(ParamCheck("condition", TypeSig.all, TypeSig.all)),
          Some(RepeatingParamCheck("outputs", TypeSig.all, TypeSig.all))),
        createSplitMeta)
    ).map(rule => (rule.getClassFor.asSubclass(classOf[Expression]), rule)).toMap
    super.getExprs ++ shimExprs
  }

  override def getPartitionings: Map[Class[_ <: Partitioning],
      PartRule[_ <: Partitioning]] = {
    val shimPartitionings = Seq(
      GpuOverrides.part[DeltaOptimizedWritePartitioning](
        "Delta optimized write partitioning",
        PartChecks(),
        createDeltaOptimizedWritePartitioningMeta)
    ).map(rule => (rule.getClassFor.asSubclass(classOf[Partitioning]), rule)).toMap
    super.getPartitionings ++ shimPartitionings
  }
}
