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

import ai.rapids.cudf.{ColumnVector, ColumnView}
import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.GpuOverrides.exec
import com.nvidia.spark.rapids.lore.GpuLoreReplayExec

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.{InternalRow, TableIdentifier}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.{Average, Sum}
import org.apache.spark.sql.catalyst.plans.physical.BroadcastMode
import org.apache.spark.sql.catalyst.trees.{Origin, TreeNode}
import org.apache.spark.sql.catalyst.util.DateFormatter
import org.apache.spark.sql.connector.metric.CustomMetric
import org.apache.spark.sql.connector.read.Scan
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.adaptive._
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.execution.datasources.{FilePartition, PartitionedFile}
import org.apache.spark.sql.execution.datasources.v2.{AppendDataExecV1, AtomicCreateTableAsSelectExec, AtomicReplaceTableAsSelectExec, BatchScanExec, OverwriteByExpressionExecV1}
import org.apache.spark.sql.execution.datasources.v2.csv.CSVScan
import org.apache.spark.sql.execution.datasources.v2.orc.OrcScan
import org.apache.spark.sql.execution.datasources.v2.parquet.ParquetScan
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeExec, ShuffleExchangeExec}
import org.apache.spark.sql.execution.joins._
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.execution.window.WindowExecBase
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids._
import org.apache.spark.sql.rapids.aggregate._
import org.apache.spark.sql.rapids.execution._
import org.apache.spark.sql.rapids.shims.{GpuCastToNumberErrorShim, OriginContextShim, RapidsErrorUtils, SparkSessionUtils}
import org.apache.spark.sql.rapids.shims.TrampolineConnectShims.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.SerializableConfiguration

/**
 * Shim base class that can be compiled with every supported 3.2.0+
 */
trait Spark320PlusShims extends SparkShims with RebaseShims
    with WindowInPandasShims with Logging {


  override final def createSqlMetric(context: SparkContext, name: String): SQLMetric =
    SQLMetrics.createMetric(context, name)

  override final def createNanoTimingSqlMetric(context: SparkContext, name: String): SQLMetric =
    SQLMetrics.createNanoTimingMetric(context, name)

  override final def createSizeSqlMetric(context: SparkContext, name: String): SQLMetric =
    SQLMetrics.createSizeMetric(context, name)

  override final def createAverageSqlMetric(context: SparkContext, name: String): SQLMetric =
    SQLMetrics.createAverageMetric(context, name)

  override final def createTimingSqlMetric(context: SparkContext, name: String): SQLMetric =
    SQLMetrics.createTimingMetric(context, name)

  override final def createV2CustomSqlMetric(
      context: SparkContext,
      metric: CustomMetric): SQLMetric =
    SQLMetrics.createV2CustomMetric(context, metric)

  private def gatherCommutativeShim(
      expr: Expression,
      rule: PartialFunction[Expression, Seq[Expression]]): Seq[Expression] = expr match {
    case c if rule.isDefinedAt(c) => rule(c).flatMap(gatherCommutativeShim(_, rule))
    case other => other :: Nil
  }

  private def orderCommutativeShim(
      expr: Expression,
      rule: PartialFunction[Expression, Seq[Expression]]): Seq[Expression] =
    gatherCommutativeShim(expr, rule).sortBy(_.hashCode())

  override final def newGpuLoreReplayExec(
      idxInParent: Int,
      parentRootPath: String,
      hadoopConf: Broadcast[SerializableConfiguration]): SparkPlan =
    GpuLoreReplayExec(idxInParent, parentRootPath, hadoopConf)

  override final def canonicalizeShimExpression(expr: Expression): Option[Expression] = expr match {
    case add @ GpuAdd(_, _, failOnError) =>
      Some(orderCommutativeShim(add, { case GpuAdd(left, right, _) => Seq(left, right) })
        .reduce((left, right) => GpuAdd(left, right, failOnError)(add.origin)))
    case multiply @ GpuMultiply(_, _, failOnError) =>
      Some(orderCommutativeShim(multiply,
        { case GpuMultiply(left, right, _) => Seq(left, right) })
        .reduce((left, right) => GpuMultiply(left, right, failOnError)(multiply.origin)))
    case bitwiseOr: GpuBitwiseOr =>
      Some(orderCommutativeShim(bitwiseOr, { case GpuBitwiseOr(left, right) => Seq(left, right) })
        .reduce(GpuBitwiseOr))
    case bitwiseAnd: GpuBitwiseAnd =>
      Some(orderCommutativeShim(bitwiseAnd,
        { case GpuBitwiseAnd(left, right) => Seq(left, right) }).reduce(GpuBitwiseAnd))
    case bitwiseXor: GpuBitwiseXor =>
      Some(orderCommutativeShim(bitwiseXor,
        { case GpuBitwiseXor(left, right) => Seq(left, right) }).reduce(GpuBitwiseXor))
    case _ => None
  }

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

  override final def isAggregateInPandasExec(plan: SparkPlan): Boolean =
    AggregateInPandasExecShims.isAggregateInPandasExec(plan)

  override final def getAggregateInPandasGroupingExpressions(
      plan: SparkPlan): Seq[NamedExpression] =
    AggregateInPandasExecShims.getGroupingExpressions(plan)

  override final def jsonPathParserMaxPathDepth: Int = JsonPathParser.MAX_PATH_DEPTH

  override final def supportsAnsiCastFloatToTimestamp(): Boolean =
    AnsiUtil.supportsAnsiCastFloatToTimestamp()

  override final def castFloatToTimestampAnsi(
      floatInput: ColumnView,
      toType: DataType): ColumnVector =
    AnsiUtil.castFloatToTimestampAnsi(floatInput, toType)

  override final def isSupportedDayTimeType(dt: DataType): Boolean =
    GpuTypeShims.isSupportedDayTimeType(dt)

  override final def isSupportedYearMonthType(dt: DataType): Boolean =
    GpuTypeShims.isSupportedYearMonthType(dt)

  override final def hasSideEffectsIfCastIntToDayTime(dt: DataType): Boolean =
    GpuTypeShims.hasSideEffectsIfCastIntToDayTime(dt)

  override final def hasSideEffectsIfCastIntToYearMonth(dt: DataType): Boolean =
    GpuTypeShims.hasSideEffectsIfCastIntToYearMonth(dt)

  override final def hasSideEffectsIfCastFloatToTimestamp: Boolean =
    GpuTypeShims.hasSideEffectsIfCastFloatToTimestamp

  override final def toDayTimeIntervalString(
      input: ColumnView,
      dataType: DataType): ColumnVector =
    GpuIntervalUtils.toDayTimeIntervalString(input, dataType)

  override final def castStringToDayTimeIntervalWithThrow(
      input: ColumnView,
      dataType: DataType): ColumnVector =
    GpuIntervalUtils.castStringToDayTimeIntervalWithThrow(input, dataType)

  override final def dayTimeIntervalToLong(
      input: ColumnView,
      dataType: DataType): ColumnVector =
    GpuIntervalUtils.dayTimeIntervalToLong(input, dataType)

  override final def dayTimeIntervalToInt(
      input: ColumnView,
      dataType: DataType): ColumnVector =
    GpuIntervalUtils.dayTimeIntervalToInt(input, dataType)

  override final def dayTimeIntervalToShort(
      input: ColumnView,
      dataType: DataType): ColumnVector =
    GpuIntervalUtils.dayTimeIntervalToShort(input, dataType)

  override final def dayTimeIntervalToByte(
      input: ColumnView,
      dataType: DataType): ColumnVector =
    GpuIntervalUtils.dayTimeIntervalToByte(input, dataType)

  override final def longToDayTimeInterval(
      input: ColumnView,
      dataType: DataType): ColumnVector =
    GpuIntervalUtils.longToDayTimeInterval(input, dataType)

  override final def intToDayTimeInterval(
      input: ColumnView,
      dataType: DataType): ColumnVector =
    GpuIntervalUtils.intToDayTimeInterval(input, dataType)

  override final def yearMonthIntervalToLong(
      input: ColumnView,
      dataType: DataType): ColumnVector =
    GpuIntervalUtils.yearMonthIntervalToLong(input, dataType)

  override final def yearMonthIntervalToInt(
      input: ColumnView,
      dataType: DataType): ColumnVector =
    GpuIntervalUtils.yearMonthIntervalToInt(input, dataType)

  override final def yearMonthIntervalToShort(
      input: ColumnView,
      dataType: DataType): ColumnVector =
    GpuIntervalUtils.yearMonthIntervalToShort(input, dataType)

  override final def yearMonthIntervalToByte(
      input: ColumnView,
      dataType: DataType): ColumnVector =
    GpuIntervalUtils.yearMonthIntervalToByte(input, dataType)

  override final def longToYearMonthInterval(
      input: ColumnView,
      dataType: DataType): ColumnVector =
    GpuIntervalUtils.longToYearMonthInterval(input, dataType)

  override final def intToYearMonthInterval(
      input: ColumnView,
      dataType: DataType): ColumnVector =
    GpuIntervalUtils.intToYearMonthInterval(input, dataType)

  override final def castDecimalToString(
      decimalInput: ColumnView,
      usePlainString: Boolean): ColumnVector =
    GpuCastShims.CastDecimalToString(decimalInput, usePlainString)

  override final def originContextSummary(origin: Origin): String =
    OriginContextShim.contextSummary(origin)

  override final def cannotChangeDecimalPrecisionError(
      value: Decimal,
      toType: DecimalType,
      origin: Origin): Throwable =
    RapidsErrorUtils.cannotChangeDecimalPrecisionError(
      value, toType, OriginContextShim.queryContext(origin))

  override final def invalidInputInCastToNumberError(
      to: DataType,
      s: UTF8String,
      origin: Origin): Throwable =
    GpuCastToNumberErrorShim.invalidInputInCastToNumberError(
      to, s, OriginContextShim.queryContext(origin))

  override final def arithmeticOverflowError(
      message: String,
      hint: String,
      origin: Origin): Throwable =
    RapidsErrorUtils.arithmeticOverflowError(
      message, hint, OriginContextShim.queryContext(origin))

  override final def invalidInputSyntaxForBooleanError(
      s: UTF8String,
      origin: Origin): Throwable =
    RapidsErrorUtils.invalidInputSyntaxForBooleanError(
      s, OriginContextShim.queryContext(origin))

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
    GpuOverrides.expr[Sum](
      "Sum aggregate operator",
      ExprChecks.fullAgg(
        TypeSig.LONG + TypeSig.DOUBLE + TypeSig.DECIMAL_128,
        TypeSig.LONG + TypeSig.DOUBLE + TypeSig.DECIMAL_128,
        Seq(ParamCheck("input", TypeSig.gpuNumeric, TypeSig.cpuNumeric))),
      (a, conf, p, r) => new AggExprMeta[Sum](a, conf, p, r) {
        override def tagAggForGpu(): Unit = {
          val inputDataType = a.child.dataType
          GpuOverrides.checkAndTagFloatAgg(inputDataType, this.conf, this)

          // Check if this Sum expression is in TRY mode context
          if (TryModeShim.isTryMode(a)) {
            willNotWorkOnGpu("try_sum is not supported on GPU")
          }
        }

        override def needsAnsiCheck: Boolean = false

        override def convertToGpu(childExprs: Seq[Expression]): GpuExpression =
          GpuSum(childExprs.head, a.dataType)
      }),
    GpuOverrides.expr[BitwiseAnd](
      "Returns the bitwise AND of the operands",
      ExprChecks.binaryProjectAndAst(
        TypeSig.implicitCastsAstTypes, TypeSig.integral, TypeSig.integral,
        ("lhs", TypeSig.integral, TypeSig.integral),
        ("rhs", TypeSig.integral, TypeSig.integral)),
      (a, conf, p, r) => new BinaryAstExprMeta[BitwiseAnd](a, conf, p, r) {
        override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
          GpuBitwiseAnd(lhs, rhs)
      }),
    GpuOverrides.expr[BitwiseOr](
      "Returns the bitwise OR of the operands",
      ExprChecks.binaryProjectAndAst(
        TypeSig.implicitCastsAstTypes, TypeSig.integral, TypeSig.integral,
        ("lhs", TypeSig.integral, TypeSig.integral),
        ("rhs", TypeSig.integral, TypeSig.integral)),
      (a, conf, p, r) => new BinaryAstExprMeta[BitwiseOr](a, conf, p, r) {
        override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
          GpuBitwiseOr(lhs, rhs)
      }),
    GpuOverrides.expr[BitwiseXor](
      "Returns the bitwise XOR of the operands",
      ExprChecks.binaryProjectAndAst(
        TypeSig.implicitCastsAstTypes, TypeSig.integral, TypeSig.integral,
        ("lhs", TypeSig.integral, TypeSig.integral),
        ("rhs", TypeSig.integral, TypeSig.integral)),
      (a, conf, p, r) => new BinaryAstExprMeta[BitwiseXor](a, conf, p, r) {
        override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
          GpuBitwiseXor(lhs, rhs)
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
      }),
    GpuOverrides.expr[GetMapValue](
      "Gets Value from a Map based on a key",
      ExprChecks.binaryProject(
        (TypeSig.commonCudfTypes + TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.NULL +
          TypeSig.DECIMAL_128 + TypeSig.MAP + TypeSig.BINARY).nested(),
        TypeSig.all,
        ("map", TypeSig.MAP.nested(TypeSig.commonCudfTypes + TypeSig.ARRAY + TypeSig.STRUCT +
          TypeSig.NULL + TypeSig.DECIMAL_128 + TypeSig.MAP + TypeSig.BINARY),
          TypeSig.MAP.nested(TypeSig.all)),
        ("key", TypeSig.commonCudfTypes + TypeSig.DECIMAL_128, TypeSig.all)),
      (in, conf, p, r) => new GetMapValueMeta(in, conf, p, r) {}),
    GpuOverrides.expr[Conv](
      desc = "Convert string representing a number from one base to another",
      pluginChecks = ExprChecks.projectOnly(
        outputCheck = TypeSig.STRING,
        paramCheck = Seq(
          ParamCheck(
            name = "num",
            cudf = TypeSig.STRING,
            spark = TypeSig.STRING),
          ParamCheck(
            name = "from_base",
            cudf = TypeSig.INT,
            spark = TypeSig.INT),
          ParamCheck(
            name = "to_base",
            cudf = TypeSig.INT,
            spark = TypeSig.INT)),
        sparkOutputSig = TypeSig.STRING),
      (convExpr, conf, parentMetaOpt, dataFromReplacementRule) =>
        new GpuConvMeta(convExpr, conf, parentMetaOpt, dataFromReplacementRule))
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
    val maps: Map[Class[_ <: SparkPlan], ExecRule[_ <: SparkPlan]] = (Seq(
      exec[BatchScanExec](
        "The backend for most file input",
        ExecChecks(
          (TypeSig.commonCudfTypes + TypeSig.STRUCT + TypeSig.MAP + TypeSig.ARRAY +
            TypeSig.DECIMAL_128 + TypeSig.BINARY).nested(),
          TypeSig.all),
        (p, conf, parent, r) => new BatchScanExecMeta(p, conf, parent, r)),
      exec[ShuffleExchangeExec](
        "The backend for most data being exchanged between processes",
        ExecChecks((TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128 +
            TypeSig.BINARY + TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.MAP +
            GpuTypeShims.additionalArithmeticSupportedTypes).nested()
            .withPsNote(TypeEnum.STRUCT, "Round-robin partitioning is not supported for nested " +
                s"structs if ${SQLConf.SORT_BEFORE_REPARTITION.key} is true")
            .withPsNote(
              Seq(TypeEnum.MAP),
              "Round-robin partitioning is not supported if " +
                s"${SQLConf.SORT_BEFORE_REPARTITION.key} is true"),
          TypeSig.all),
        (shuffle, conf, p, r) => new GpuShuffleMeta(shuffle, conf, p, r)),
      exec[BroadcastHashJoinExec](
        "Implementation of join using broadcast data",
        JoinTypeChecks.equiJoinExecChecks,
        (join, conf, p, r) => new GpuBroadcastHashJoinMeta(join, conf, p, r)),
      exec[BroadcastNestedLoopJoinExec](
        "Implementation of join using brute force. Full outer joins and joins where the " +
            "broadcast side matches the join side (e.g.: LeftOuter with left broadcast) are not " +
            "supported",
        JoinTypeChecks.nonEquiJoinChecks,
        (join, conf, p, r) => new GpuBroadcastNestedLoopJoinMeta(join, conf, p, r)),
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
    ) ++ AggregateInPandasExecShims.execRule.toSeq)
      .map(r => (r.getClassFor.asSubclass(classOf[SparkPlan]), r)).toMap
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
