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
{"spark": "330db"}
{"spark": "332db"}
{"spark": "350db143"}
{"spark": "400db173"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.shims

import com.databricks.sql.execution.window.RunningWindowFunctionExec
import com.databricks.sql.optimizer.{EphemeralSubstring, PlanDynamicPruningFilters}
import com.nvidia.spark.rapids._
import org.apache.hadoop.fs.FileStatus

import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.memory.TaskMemoryManager
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical.BroadcastMode
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.adaptive._
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.exchange._
import org.apache.spark.sql.execution.window._
import org.apache.spark.sql.rapids.GpuSubstring
import org.apache.spark.sql.rapids.execution.GpuSubqueryBroadcastMeta
import org.apache.spark.sql.rapids.execution.shims.ReuseGpuBroadcastExchangeAndSubquery
import org.apache.spark.sql.rapids.shims._
import org.apache.spark.sql.types._


trait Spark321PlusDBShims extends SparkShims
  with Spark321PlusShims {
  override def isCastingStringToNegDecimalScaleSupported: Boolean = true

  override def getFileScanRDD(
      sparkSession: SparkSession,
      readFunction: PartitionedFile => Iterator[InternalRow],
      filePartitions: Seq[FilePartition],
      readDataSchema: StructType,
      metadataColumns: Seq[AttributeReference],
      fileFormat: Option[FileFormat]): RDD[InternalRow] = {
    new GpuFileScanRDD(sparkSession, readFunction, filePartitions)
  }

  override def broadcastModeTransform(mode: BroadcastMode, rows: Array[InternalRow]): Any = {
    // In some cases we can be asked to transform when there's no task context, which appears to
    // be new behavior since Databricks 10.4. A task memory manager must be passed, so if one is
    // not available we construct one from the main memory manager using a task attempt ID of 0.
    val memoryManager = Option(TaskContext.get).map(_.taskMemoryManager()).getOrElse {
      new TaskMemoryManager(SparkEnv.get.memoryManager, 0)
    }
    mode.transform(rows, memoryManager)
  }

  override def newBroadcastQueryStageExec(
      old: BroadcastQueryStageExec,
      newPlan: SparkPlan): BroadcastQueryStageExec =
    QueryStageExecShims.createBroadcastQueryStageExec(
      old, old.id, newPlan, old.originalPlan, old.isSparkExchange)

  override def filesFromFileIndex(fileCatalog: PartitioningAwareFileIndex): Seq[FileStatus] = {
    fileCatalog.allFiles().map(_.toFileStatus)
  }

  override def neverReplaceShowCurrentNamespaceCommand: ExecRule[_ <: SparkPlan] = null

  override def isWindowFunctionExec(plan: SparkPlan): Boolean =
    plan.isInstanceOf[WindowExecBase] || plan.isInstanceOf[RunningWindowFunctionExec]

  override def applyShimPlanRules(plan: SparkPlan, conf: RapidsConf): SparkPlan = {
    if (plan.conf.adaptiveExecutionEnabled) {
      plan // AQE+DPP cooperation ensures the optimization runs early
    } else {
      val sparkSession = plan.session
      val rules = Seq(
        PlanDynamicPruningFilters(sparkSession)
      )
      rules.foldLeft(plan) { case (sp, rule) =>
        rule.apply(sp)
      }
    }
  }

  override def applyPostShimPlanRules(plan: SparkPlan): SparkPlan = {
    val rules = Seq(
      ReuseGpuBroadcastExchangeAndSubquery
    )
    rules.foldLeft(plan) { case (sp, rule) =>
      rule.apply(sp)
    }
  }

  private val shimExecs: Map[Class[_ <: SparkPlan], ExecRule[_ <: SparkPlan]] = {
    Seq(
      GpuOverrides.exec[SubqueryBroadcastExec](
        "Plan to collect and transform the broadcast key values",
        ExecChecks(TypeSig.all, TypeSig.all),
        (s, conf, p, r) => new GpuSubqueryBroadcastMeta(s, conf, p, r)),
      GpuOverrides.exec[RunningWindowFunctionExec](
        "Databricks-specific window function exec, for \"running\" windows, " +
            "i.e. (UNBOUNDED PRECEDING TO CURRENT ROW)",
        ExecChecks(
          (TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128 +
              TypeSig.STRUCT + TypeSig.ARRAY + TypeSig.MAP).nested(),
          TypeSig.all,
          Map("partitionSpec" ->
              InputCheck(TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128,
                TypeSig.all))),
        (runningWindowFunctionExec, conf, p, r) =>
          new GpuRunningWindowExecMeta(runningWindowFunctionExec, conf, p, r)
      )
    ).map(r => (r.getClassFor.asSubclass(classOf[SparkPlan]), r)).toMap
  }

  override def getExecs: Map[Class[_ <: SparkPlan], ExecRule[_ <: SparkPlan]] =
    super.getExecs ++ shimExecs

  override def getExprs: Map[Class[_ <: Expression], ExprRule[_ <: Expression]] = {
    val exprs: Map[Class[_ <: Expression], ExprRule[_ <: Expression]] = Seq(
      GpuOverrides.expr[EphemeralSubstring](
        "Ephemeral version of substring operator",
        ExprChecks.projectOnly(TypeSig.STRING, TypeSig.STRING + TypeSig.BINARY,
          Seq(ParamCheck("str", TypeSig.STRING, TypeSig.STRING + TypeSig.BINARY),
            ParamCheck("pos", TypeSig.INT, TypeSig.INT),
            ParamCheck("len", TypeSig.INT, TypeSig.INT))),
        (in, conf, p, r) => new TernaryExprMeta[EphemeralSubstring](in, conf, p, r) {
          override def convertToGpu(
              column: Expression,
              position: Expression,
              length: Expression): GpuExpression =
            GpuSubstring(column, position, length)
        })
    ).map(r => (r.getClassFor.asSubclass(classOf[Expression]), r)).toMap
    exprs ++ super.getExprs
  }

  /**
   * Case class ShuffleQueryStageExec holds an additional field shuffleOrigin
   * affecting the unapply method signature
   */
  override def reusedExchangeExecPfn: PartialFunction[SparkPlan, ReusedExchangeExec] = {
    case ShuffleQueryStageExec(_, e: ReusedExchangeExec, _, _) => e
    case BroadcastQueryStageExec(_, e: ReusedExchangeExec, _, _) => e
  }

  override def reproduceEmptyStringBug: Boolean = true
}
