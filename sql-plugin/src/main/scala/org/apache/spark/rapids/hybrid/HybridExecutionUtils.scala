/*
 * Copyright (c) 2024-2025, NVIDIA CORPORATION.
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

package org.apache.spark.rapids.hybrid

import java.util.Locale

import ai.rapids.cudf.DType
import com.nvidia.spark.rapids.{RapidsConf, VersionUtils}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, UnresolvedHint}
import org.apache.spark.sql.catalyst.trees.TreePattern
import org.apache.spark.sql.execution.{FileSourceScanExec, FilterExec, SparkPlan}
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.execution.TrampolineUtil
import org.apache.spark.sql.types._

object HybridExecutionUtils extends PredicateHelper {
  
  private val HYBRID_JAR_PLUGIN_CLASS_NAME = "com.nvidia.spark.rapids.hybrid.HybridPluginWrapper"

  /**
   * Check if the Hybrid jar is in the classpath,
   * report error if not
   */
  def checkHybridJarInClassPath(): Unit = {
    try {
      Class.forName(HYBRID_JAR_PLUGIN_CLASS_NAME)
    } catch {
      case e: ClassNotFoundException => throw new RuntimeException(
        "Hybrid jar is not in the classpath, Please add Hybrid jar into the class path, or " +
            "Please disable Hybrid feature by setting " +
            "spark.rapids.sql.hybrid.parquet.enabled=false", e)
    }
  }

  /**
   * Determine if the given FileSourceScanExec is supported by HybridScan.
   */
  def useHybridScan(conf: RapidsConf, fsse: FileSourceScanExec): Boolean = {
    // Hybrid scan can be enabled either by setting the global SQL config(HYBRID_PARQUET_READER)
    // or by adding the specific SQL hint(HYBRID_SCAN_HINT) upon desired tables.
    if (!conf.useHybridParquetReader) {
      if (fsse.relation.options.contains(HybridExecOverrides.HYBRID_SCAN_TAG)) {
        logInfo(fsse.nodeName + " carries SQL HINT(" + HybridExecOverrides.HYBRID_SCAN_HINT +
          "), will use HybridScan on this plan if it can be handled by HybridScan")
      } else {
        return false
      }
    } else {
      logInfo(s"HybridScan is enabled by ${RapidsConf.HYBRID_PARQUET_READER.key}, " +
        s"then check if ${fsse.nodeName} can be handled by HybridScan")
    }

    require(conf.loadHybridBackend,
      "Hybrid backend was NOT loaded during the launch of spark-rapids plugin")

    // Currently, only support reading Parquet
    if (fsse.relation.fileFormat.getClass != classOf[ParquetFileFormat]) {
      logWarning(s"Fallback to GpuScan because the file format is not Parquet: ${fsse.nodeName}")
      return false
    }

    // Fallback to GpuScan over the `select count(1)` cases
    if (fsse.output.isEmpty || fsse.requiredSchema.isEmpty) {
      logWarning(s"Fallback to GpuScan over the `select count(1)` cases: ${fsse.nodeName}")
      return false
    }

    // Check if data types of all fields are supported by HybridParquetReader
    fsse.requiredSchema.foreach { field =>
      val canNotSupport = TrampolineUtil.dataTypeExistsRecursively(field.dataType, {
        // Currently, under some circumstance, the native backend may return incorrect results
        // over MapType nested by nested types. To guarantee the correctness, disable this pattern
        // entirely.
        // TODO: figure out the root cause and support it
        case ArrayType(_: MapType, _) =>
          logWarning(s"Fallback to GpuScan because Array of Map is not supported: $field")
          true
        case MapType(_: MapType, _, _) | MapType(_, _: MapType, _) =>
          logWarning(s"Fallback to GpuScan because Map of Map is not supported: $field")
          true
        case st: StructType if st.exists(_.dataType.isInstanceOf[MapType]) =>
          logWarning(s"Fallback to GpuScan because Struct of Map is not supported: $field")
          true
        // TODO: support DECIMAL with negative scale
        case dt: DecimalType if dt.scale < 0 =>
          logWarning(
            s"Fallback to GpuScan because Decimal of negative scale is not supported: $field")
          true
        // TODO: support DECIMAL128
        case dt: DecimalType if dt.precision > DType.DECIMAL64_MAX_PRECISION =>
          logWarning(s"Fallback to GpuScan because Decimal128 is not supported: $field")
          true
        // TODO: support BinaryType
        case _: BinaryType =>
          logWarning(s"Fallback to GpuScan because BinaryType is not supported: $field")
          true
        // TODO: support CalendarIntervalType
        case _: CalendarIntervalType =>
          logWarning(s"Fallback to GpuScan because CalendarIntervalType is not supported: $field")
          true
        // TODO: support DayTimeIntervalType
        case _: DayTimeIntervalType =>
          logWarning(s"Fallback to GpuScan because DayTimeIntervalType is not supported: $field")
          true
        // TODO: support YearMonthIntervalType
        case _: YearMonthIntervalType =>
          logWarning(s"Fallback to GpuScan because YearMonthIntervalType is not supported: $field")
          true
        // TODO: support UDT
        case _: UserDefinedType[_] =>
          logWarning(s"Fallback to GpuScan because UDT is not supported: $field")
          true
        case _ =>
          false
      })
      if (canNotSupport) {
        return false
      }
    }

    true
  }

  /**
   * Check if runtimes are satisfied, including:
   * - Spark distribution is not CDH or Databricks
   * - Hybrid jar in the classpath
   * - Scala version is 2.12
   * - Parquet V1 data source
   */
  def checkRuntimes(v1DataSourceList: String): Unit = {
    checkNotRunningCDHorDatabricks()
    HybridExecutionUtils.checkHybridJarInClassPath()
    checkScalaVersion()
    checkV1Datasource(v1DataSourceList)
  }

  /**
   * Check Spark distribution is not CDH or Databricks,
   * report error if it is
   */
  private def checkNotRunningCDHorDatabricks(): Unit = {
    if (VersionUtils.isCloudera || VersionUtils.isDataBricks) {
      throw new RuntimeException("Hybrid feature does not support Cloudera/Databricks " +
          "Spark releases, Please disable Hybrid feature by setting " +
          "spark.rapids.sql.hybrid.parquet.enabled=false")
    }
  }

  /**
   * Hybrid feature only supports Scala 2.12 version,
   * report error if not
   */
  private def checkScalaVersion(): Unit = {
    val scalaVersion = scala.util.Properties.versionString
    if (!scalaVersion.startsWith("version 2.12")) {
      throw new RuntimeException(s"Hybrid feature only supports Scala 2.12 version, " +
          s"but got $scalaVersion")
    }
  }

  /**
   * Hybrid feature only supports v1 datasource,
   * report error if it's not satisfied
   */
  private def checkV1Datasource(v1SourceList: String): Unit = {
    // check spark.sql.sources.useV1SourceList contains parquet
    if(!v1SourceList.contains("parquet")) {
      throw new RuntimeException(s"Hybrid feature only supports v1 datasource, " +
        s"please set spark.sql.sources.useV1SourceList=parquet")
    }
  }

  // scalastyle:off line.size.limit
  // from https://github.com/apache/incubator-gluten/blob/branch-1.2/docs/velox-backend-support-progress.md
  // and test_hybrid_parquet_filter_pushdown_more_exprs.py
  // scalastyle:on
  val ansiOn = Seq(
    classOf[Acos],
    classOf[AddMonths],
    classOf[Alias],
    classOf[And],
    classOf[ArrayContains],
    classOf[ArrayDistinct],
    classOf[ArrayExcept],
    classOf[ArrayExists],
    classOf[ArrayForAll],
    classOf[ArrayMax],
    classOf[ArrayMin],
    classOf[ArrayPosition],
    classOf[ArrayRemove],
    classOf[ArrayRepeat],
    classOf[ArraySort],
    classOf[ArraysZip],
    classOf[Ascii],
    classOf[Asin],
    classOf[Atan],
    classOf[Atan2],
    classOf[Atanh],
    classOf[AttributeReference],
    classOf[BitLength],
    classOf[BitwiseAnd],
    classOf[BitwiseOr],
    classOf[BitwiseXor],
    classOf[Ceil],
    classOf[Chr],
    classOf[Concat],
    classOf[Cos],
    classOf[Cosh],
    classOf[CreateArray],
    classOf[CreateMap],
    classOf[CreateNamedStruct],
    classOf[DateAdd],
    classOf[DateDiff],
    classOf[DateFromUnixDate],
    classOf[DateSub],
    classOf[DayOfMonth],
    classOf[DayOfWeek],
    classOf[DayOfYear],
    classOf[ElementAt],
    classOf[EqualNullSafe],
    classOf[EqualTo],
    classOf[Exp],
    classOf[Expm1],
    classOf[FindInSet],
    classOf[Flatten],
    classOf[Floor],
    classOf[GetJsonObject],
    classOf[GreaterThan],
    classOf[GreaterThanOrEqual],
    classOf[Greatest],
    classOf[Hex],
    classOf[If],
    classOf[IsNaN],
    classOf[IsNotNull],
    classOf[IsNull],
    classOf[LambdaFunction],
    classOf[LastDay],
    classOf[Least],
    classOf[Left],
    classOf[Length],
    classOf[LessThan],
    classOf[LessThanOrEqual],
    classOf[Like],
    classOf[Literal],
    classOf[Logarithm],
    classOf[Log10],
    classOf[Log2],
    classOf[Lower],
    classOf[MapFromArrays],
    classOf[MapKeys],
    classOf[MapValues],
    classOf[MapZipWith],
    classOf[MonotonicallyIncreasingID],
    classOf[Month],
    classOf[NamedLambdaVariable],
    classOf[NaNvl],
    classOf[NextDay],
    classOf[Not],
    classOf[Or],
    classOf[Overlay],
    classOf[Pi],
    classOf[Pow],
    classOf[Quarter],
    classOf[Remainder],
    classOf[Reverse],
    classOf[Rint],
    classOf[Round],
    classOf[Second],
    classOf[ShiftLeft],
    classOf[ShiftRight],
    classOf[Size],
    classOf[SortArray],
    classOf[SoundEx],
    classOf[SparkPartitionID],
    classOf[StringInstr],
    classOf[StringLPad],
    classOf[StringRPad],
    classOf[StringReplace],
    classOf[StringToMap],
    classOf[StringTrim],
    classOf[StringTrimLeft],
    classOf[StringTrimRight],
    classOf[Substring],
    classOf[SubstringIndex],
    classOf[UnaryPositive],
    classOf[Unhex],
    classOf[UnixMicros],
    classOf[UnixMillis],
    classOf[UnixSeconds],
    classOf[Upper],
    classOf[WeekDay],
    classOf[WeekOfYear],
    classOf[Year],
    classOf[ZipWith]
  )

  // Some functions are fully supported with ANSI mode off
  lazy val ansiOff = Seq(
    classOf[Remainder],
    classOf[Multiply],
    classOf[Add],
    classOf[Subtract],
    classOf[Divide],
    classOf[Abs],
    classOf[Pmod]
  )

  def supportedByHybridFilters(whitelistExprsName: String): Seq[Class[_]] = {
    val whitelistExprs: Seq[Class[_]] = whitelistExprsName.split(",").map { exprName =>
      try {
        Some(Class.forName("org.apache.spark.sql.catalyst.expressions." + exprName))
      } catch {
        case _: ClassNotFoundException => None
      }
    }.collect { case Some(cls) => cls }

    if (SQLConf.get.ansiEnabled) {
      ansiOn ++ whitelistExprs
    } else {
      ansiOn ++ ansiOff ++ whitelistExprs
    }
  }

  def canBePushedToHybrid(child: SparkPlan, conf: RapidsConf): String = {
    child match {
      case fsse: FileSourceScanExec if useHybridScan(conf, fsse) =>
        conf.pushDownFiltersToHybrid
      case _ => "OFF"
    }
  }

  def isTimestampCondition(expr: Expression): Boolean = {
    expr.references.exists(attr => attr.dataType == TimestampType)
  }

  def isCastSupportedByHybrid(childDataType: DataType, dataType: DataType): Boolean = {
    (childDataType, dataType) match {
      case (_, BooleanType) => false
      case (DateType, StringType) => true
      case (DateType, _) => false
      case (ArrayType(_, _), _) => false
      case (MapType(_, _, _), _) => false
      case (StructType(_), _) => false
      case (_, _) => true
    }
  }

  def isExprSupportedByHybridScan(condition: Expression, whitelistExprsName: String): Boolean = {
    condition match {
      case filter if isTimestampCondition(filter) => false // Timestamp is not fully supported in Hybrid Filter
      case filter if HybridExecutionUtils.supportedByHybridFilters(whitelistExprsName)
          .exists(_.isInstance(filter)) =>
        val childrenSupported = filter.children.forall(
            isExprSupportedByHybridScan(_, whitelistExprsName))
        childrenSupported
      case Cast(child, dataType, _, _) if isCastSupportedByHybrid(child.dataType, dataType) => {
        isExprSupportedByHybridScan(child, whitelistExprsName)
      }
      case _ => false
    }
  }

  /** 
   * Search the plan for FilterExec whose child is a FileSourceScanExec if Hybrid Scan is enabled
   * and decide whether to push down a filter to the GPU or not according to whether CPU/GPU
   * support it. After that we can remove the condition from one side to avoid duplicate execution
   * or unnecessary fallback/crash.
   */
  def hybridScanFilterSplit(plan: SparkPlan, conf: RapidsConf): SparkPlan = {
    plan.transformUp {
      case filter: FilterExec => {
        lazy val filters = splitConjunctivePredicates(filter.condition)
        val canBePushed = HybridExecutionUtils.canBePushedToHybrid(filter.child, conf)
        (filter.child, canBePushed) match {
          case (fsse: FileSourceScanExec, "GPU") => {
            // remainingFilters shoule be empty, filter to make it more robust
            val remainingFilters = fsse.dataFilters.filterNot(filters.contains)
            val updatedFsseChild = fsse.copy(dataFilters = remainingFilters)
            val updatedFilter = FilterExec(filter.condition, updatedFsseChild)
            updatedFilter
          }
          case (fsse: FileSourceScanExec, "CPU") => {
            val (supportedConditions, notSupportedConditions) = filters.partition(
                isExprSupportedByHybridScan(_, conf.hybridExprsWhitelist))
            notSupportedConditions match {
              case Nil =>
                // NOTICE: it is essential to align the output to the filter's output. Otherwise,
                // when AQE is enabled, an extra unwanted broadcast exchange will be injected due
                // to the mismatch between the output of ScanNode and the input of the child plan(
                // which was supposed to be connected to the FilterNode).
                // For more details, please refer
                // https://github.com/NVIDIA/spark-rapids/issues/12267
                fsse.copy(
                  dataFilters = supportedConditions,
                  output = filter.output)
              case _ =>
                FilterExec(
                  notSupportedConditions.reduceLeft(And),
                  fsse.copy(dataFilters = supportedConditions))
            }
          }
          case _ => filter
        }
      }
    }
  }

  def tryToApplyHybridScanRules(plan: SparkPlan, conf: RapidsConf): SparkPlan = {
    if (!conf.loadHybridBackend ||
      conf.pushDownFiltersToHybrid == RapidsConf.HybridFilterPushdownType.OFF.toString) {
      return plan
    }
    hybridScanFilterSplit(plan, conf)
  }
}

object HybridExecOverrides extends Logging {
  // The SQL hint enables HybridScan for specific tables even if HYBRID_PARQUET_READER is disabled
  val HYBRID_SCAN_HINT = "HYBRID_SCAN"

  // The tag is used to mark the table to be scanned by HybridScan
  val HYBRID_SCAN_TAG = "HYBRID_SCAN_ENABLED"

  /**
   * Resolve HybridScanHint by pushing it down to connected LogicalRelations as an extra option of
   * HadoopFsRelation, so that the information can be transferred to the corresponding SparkPlan
   * along with the HadoopFsRelation and be read by GpuOverrides.
   *
   * NOTE: Invalid hints will be removed by the following rule: `ResolveHints.RemoveAllHints`
   */
  def resolveHybridScanHint(plan: LogicalPlan): LogicalPlan = {
    plan.resolveOperatorsWithPruning(_.containsPattern(TreePattern.UNRESOLVED_HINT)) {
      case UnresolvedHint(n, Nil, child) if n.toUpperCase(Locale.ROOT).equals(HYBRID_SCAN_HINT) =>
        child.transformUp {
          case rel: LogicalRelation if rel.relation.isInstanceOf[HadoopFsRelation] =>
            val hdfsRel = rel.relation.asInstanceOf[HadoopFsRelation]
            val newOptions = hdfsRel.options.updated(HYBRID_SCAN_TAG, "")
            val newRelation = hdfsRel.copy(options = newOptions)(hdfsRel.sparkSession)
            rel.copy(relation = newRelation)
        }
    }
  }
}
