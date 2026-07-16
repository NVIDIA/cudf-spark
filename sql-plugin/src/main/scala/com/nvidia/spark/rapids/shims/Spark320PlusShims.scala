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

package com.nvidia.spark.rapids.shims

import scala.annotation.nowarn

import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.GpuOverrides.exec

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.{InternalRow, TableIdentifier}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.Average
import org.apache.spark.sql.catalyst.plans.physical.BroadcastMode
import org.apache.spark.sql.catalyst.trees.TreeNode
import org.apache.spark.sql.catalyst.util.DateFormatter
import org.apache.spark.sql.connector.read.Scan
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.adaptive._
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.execution.datasources.{FilePartition, PartitionedFile}
import org.apache.spark.sql.execution.datasources.v2.{AppendDataExecV1, AtomicCreateTableAsSelectExec, AtomicReplaceTableAsSelectExec, OverwriteByExpressionExecV1}
import org.apache.spark.sql.execution.datasources.v2.csv.CSVScan
import org.apache.spark.sql.execution.datasources.v2.orc.OrcScan
import org.apache.spark.sql.execution.datasources.v2.parquet.ParquetScan
import org.apache.spark.sql.execution.exchange.BroadcastExchangeExec
import org.apache.spark.sql.execution.joins._
import org.apache.spark.sql.execution.window.WindowExecBase
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids._
import org.apache.spark.sql.rapids.aggregate._
import org.apache.spark.sql.rapids.execution._
import org.apache.spark.sql.rapids.shims.SparkSessionUtils
import org.apache.spark.sql.rapids.shims.TrampolineConnectShims.SparkSession

/**
 * Shim base class that can be compiled with every supported 3.2.0+
 */
trait Spark320PlusShims extends SparkShims with RebaseShims
    with WindowInPandasShims with Logging {


  override final def aqeShuffleReaderExec: ExecRule[_ <: SparkPlan] = exec[AQEShuffleReadExec](
    "A wrapper of shuffle query stage",
    ExecChecks((TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128 + TypeSig.ARRAY +
      TypeSig.STRUCT + TypeSig.MAP + TypeSig.BINARY).nested(), TypeSig.all),
    (exec, conf, p, r) => new GpuCustomShuffleReaderMeta(exec, conf, p, r))

  override def isEmptyRelation(relation: Any): Boolean = relation match {
    case EmptyHashedRelation => true
    case arr: Array[InternalRow] if arr.isEmpty => true
    case _ => false
  }

  override def tryTransformIfEmptyRelation(mode: BroadcastMode): Option[Any] = {
    Some(broadcastModeTransform(mode, Array.empty)).filter(isEmptyRelation)
  }

  override final def isExchangeOp(plan: SparkPlanMeta[_]): Boolean = {
    // if the child query stage already executed on GPU then we need to keep the
    // next operator on GPU in these cases
    SQLConf.get.adaptiveExecutionEnabled && (plan.wrapped match {
      case _: AQEShuffleReadExec
           | _: ShuffledHashJoinExec
           | _: BroadcastHashJoinExec
           | _: BroadcastExchangeExec
           | _: BroadcastNestedLoopJoinExec => true
      case _ => false
    })
  }

  override final def isAqePlan(p: SparkPlan): Boolean = p match {
    case _: AdaptiveSparkPlanExec |
         _: QueryStageExec |
         _: AQEShuffleReadExec => true
    case _ => false
  }

  override def getDateFormatter(): DateFormatter = {
    // TODO verify
    DateFormatter()
  }

  override def isCustomReaderExec(x: SparkPlan): Boolean = x match {
    case _: GpuCustomShuffleReaderExec | _: AQEShuffleReadExec => true
    case _ => false
  }

  override def v1RepairTableCommand(tableName: TableIdentifier): RunnableCommand =
    RepairTableCommand(tableName,
      // These match the one place that this is called, if we start to call this in more places
      // we will need to change the API to pass these values in.
      enableAddPartitions = true,
      enableDropPartitions = false)

  override def shouldFailDivOverflow: Boolean = SQLConf.get.ansiEnabled

  override def getPartitionFiles(partition: FilePartition): Seq[PartitionedFile] = {
    (partition.files: @nowarn(
      "msg=files in trait FilePartitionBase is deprecated"))
  }

  def leafNodeDefaultParallelism(ss: SparkSession): Int = {
    SparkSessionUtils.leafNodeDefaultParallelism(ss)
  }

  override def isWindowFunctionExec(plan: SparkPlan): Boolean = plan.isInstanceOf[WindowExecBase]

  override def getExprs: Map[Class[_ <: Expression], ExprRule[_ <: Expression]] = Seq(
    GpuOverrides.expr[Cast](
      "Convert a column of one type of data into another type",
      new CastChecks(),
      (cast, conf, p, r) => {
        new CastExprMeta[Cast](cast,
          AnsiCastShim.getEvalMode(cast), conf, p, r,
          doFloatToIntCheck = true, stringToAnsiDate = true)
      }),
    GpuOverrides.expr[Average](
      "Average aggregate operator",
      ExprChecks.fullAgg(
        TypeSig.DOUBLE + TypeSig.DECIMAL_128,
        TypeSig.DOUBLE + TypeSig.DECIMAL_128,
        // NullType is not technically allowed by Spark, but in practice in 3.2.0
        // it can show up
        Seq(ParamCheck("input",
          TypeSig.integral + TypeSig.fp + TypeSig.DECIMAL_128 + TypeSig.NULL,
          TypeSig.numericAndInterval + TypeSig.NULL))),
      (a, conf, p, r) => new AggExprMeta[Average](a, conf, p, r) {
        private val ansiEnabled = SQLConf.get.ansiEnabled

        override def tagAggForGpu(): Unit = {
          GpuOverrides.checkAndTagFloatAgg(a.child.dataType, this.conf, this)

          // Check if this Average expression is in TRY mode context
          if (TryModeShim.isTryMode(a)) {
            willNotWorkOnGpu("try_avg is not supported on GPU")
          }
        }

        override def convertToGpu(childExprs: Seq[Expression]): GpuExpression =
          GpuAverage(childExprs.head, ansiEnabled)

        override def needsAnsiCheck: Boolean = false
      }),
    GpuOverrides.expr[Abs](
      "Absolute value",
      ExprChecks.unaryProjectAndAstInputMatchesOutput(
        TypeSig.implicitCastsAstTypes, TypeSig.gpuNumeric,
        TypeSig.cpuNumeric),
      (a, conf, p, r) => new UnaryAstExprMeta[Abs](a, conf, p, r) {
        val ansiEnabled = SQLConf.get.ansiEnabled

        override def tagSelfForAst(): Unit = {
          if (ansiEnabled && GpuAnsi.needBasicOpOverflowCheck(a.dataType)) {
            willNotWorkInAst("AST unary minus does not support ANSI mode.")
          }
        }

        // ANSI support for ABS was added in 3.2.0 SPARK-33275
        override def convertToGpu(child: Expression): GpuExpression = GpuAbs(child, ansiEnabled)
      })
    // TimeAdd moved to TimeAddShims to handle version differences
  ).map(r => (r.getClassFor.asSubclass(classOf[Expression]), r))
    .toMap[Class[_ <: Expression], ExprRule[_ <: Expression]] ++
    TimeAddShims.exprs ++ Seq(
    GpuOverrides.expr[SpecifiedWindowFrame](
      "Specification of the width of the group (or \"frame\") of input rows " +
        "around which a window function is evaluated",
      ExprChecks.projectOnly(
        TypeSig.CALENDAR + TypeSig.NULL + TypeSig.integral + TypeSig.DAYTIME,
        TypeSig.numericAndInterval,
        Seq(
          ParamCheck("lower",
            TypeSig.CALENDAR + TypeSig.NULL + TypeSig.integral + TypeSig.DAYTIME
              + TypeSig.DECIMAL_128 + TypeSig.FLOAT + TypeSig.DOUBLE,
            TypeSig.numericAndInterval),
          ParamCheck("upper",
            TypeSig.CALENDAR + TypeSig.NULL + TypeSig.integral + TypeSig.DAYTIME
              + TypeSig.DECIMAL_128 + TypeSig.FLOAT + TypeSig.DOUBLE,
            TypeSig.numericAndInterval))),
      (windowFrame, conf, p, r) => new GpuSpecifiedWindowFrameMeta(windowFrame, conf, p, r)),
    GpuOverrides.expr[WindowExpression](
      "Calculates a return value for every input row of a table based on a group (or " +
        "\"window\") of rows",
      ExprChecks.windowOnly(
        TypeSig.all,
        TypeSig.all,
        Seq(ParamCheck("windowFunction", TypeSig.all, TypeSig.all),
          ParamCheck("windowSpec",
            TypeSig.CALENDAR + TypeSig.NULL + TypeSig.integral + TypeSig.DECIMAL_64 +
              TypeSig.DAYTIME, TypeSig.numericAndInterval))),
      (windowExpression, conf, p, r) => new GpuWindowExpressionMeta(windowExpression, conf, p, r))
  ).map(r => (r.getClassFor.asSubclass(classOf[Expression]), r))
    .toMap[Class[_ <: Expression], ExprRule[_ <: Expression]]

  override def getExecs: Map[Class[_ <: SparkPlan], ExecRule[_ <: SparkPlan]] = {
    val maps: Map[Class[_ <: SparkPlan], ExecRule[_ <: SparkPlan]] = Seq(
      exec[AppendDataExecV1](
        "Append data into a datasource V2 table using the V1 write interface",
        ExecChecks((TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 +
          TypeSig.STRUCT + TypeSig.MAP + TypeSig.ARRAY + TypeSig.BINARY +
          GpuTypeShims.additionalCommonOperatorSupportedTypes).nested(),
          TypeSig.all),
        (p, conf, parent, r) => new AppendDataExecV1Meta(p, conf, parent, r)),
      exec[AtomicCreateTableAsSelectExec](
        "Create table as select for datasource V2 tables that support staging table creation",
        ExecChecks((TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.STRUCT +
          TypeSig.MAP + TypeSig.ARRAY + TypeSig.BINARY +
          GpuTypeShims.additionalCommonOperatorSupportedTypes).nested(),
          TypeSig.all),
        (e, conf, p, r) => new AtomicCreateTableAsSelectExecMeta(e, conf, p, r)),
      exec[AtomicReplaceTableAsSelectExec](
        "Replace table as select for datasource V2 tables that support staging table creation",
        ExecChecks((TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.STRUCT +
          TypeSig.MAP + TypeSig.ARRAY + TypeSig.BINARY +
          GpuTypeShims.additionalCommonOperatorSupportedTypes).nested(),
          TypeSig.all),
        (e, conf, p, r) => new AtomicReplaceTableAsSelectExecMeta(e, conf, p, r)),
      exec[OverwriteByExpressionExecV1](
        "Overwrite into a datasource V2 table using the V1 write interface",
        ExecChecks((TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 +
          TypeSig.STRUCT + TypeSig.MAP + TypeSig.ARRAY + TypeSig.BINARY +
          GpuTypeShims.additionalCommonOperatorSupportedTypes).nested(),
          TypeSig.all),
        (p, conf, parent, r) => new OverwriteByExpressionExecV1Meta(p, conf, parent, r))
      // WindowInPandasExec moved to WindowInPandasExecShims to handle version differences
    ).map(r => (r.getClassFor.asSubclass(classOf[SparkPlan]), r)).toMap
    maps ++ ScanExecShims.execs ++ WindowInPandasExecShims.execs
  }

  override def getScans: Map[Class[_ <: Scan], ScanRule[_ <: Scan]] = Seq(
    GpuOverrides.scan[ParquetScan](
      "Parquet parsing",
      (a, conf, p, r) => new RapidsParquetScanMeta(a, conf, p, r)),
    GpuOverrides.scan[OrcScan](
      "ORC parsing",
      (a, conf, p, r) => new RapidsOrcScanMeta(a, conf, p, r)),
    GpuOverrides.scan[CSVScan](
      "CSV parsing",
      (a, conf, p, r) => new RapidsCsvScanMeta(a, conf, p, r))
  ).map(r => (r.getClassFor.asSubclass(classOf[Scan]), r)).toMap

  /** dropped by SPARK-34234 */
  override def attachTreeIfSupported[TreeType <: TreeNode[_], A](
      tree: TreeType,
      msg: String)(
      f: => A
  ): A = {
    identity(f)
  }

  override def hasAliasQuoteFix: Boolean = true

  override def hasCastFloatTimestampUpcast: Boolean = true

  override def findOperators(plan: SparkPlan, predicate: SparkPlan => Boolean): Seq[SparkPlan] = {
    OperatorsUtilShims.findOperators(plan, predicate)
  }

  override def skipAssertIsOnTheGpu(plan: SparkPlan): Boolean = plan match {
    case _: CommandResultExec => true
    case _ => false
  }

  override def getAdaptiveInputPlan(adaptivePlan: AdaptiveSparkPlanExec): SparkPlan = {
    adaptivePlan.initialPlan
  }

  override def columnarAdaptivePlan(a: AdaptiveSparkPlanExec,
      goal: CoalesceSizeGoal): SparkPlan = {
    a.copy(supportsColumnar = true)
  }

  override def supportsColumnarAdaptivePlans: Boolean = true

  override def reproduceEmptyStringBug: Boolean = false
}
