/*
 * Copyright (c) 2019-2026, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids

import java.time.ZoneId

import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag
import scala.util.control.NonFatal

import ai.rapids.cudf.DType
import com.nvidia.spark.rapids.RapidsConf.{SUPPRESS_PLANNING_FAILURE, TEST_CONF}
import com.nvidia.spark.rapids.jni.GpuTimeZoneDB
import com.nvidia.spark.rapids.lore.GpuLore
import com.nvidia.spark.rapids.shims._
import com.nvidia.spark.rapids.window.{GpuDenseRank, GpuLag, GpuLead, GpuPercentRank, GpuRank, GpuRowNumber, GpuSpecialFrameBoundary, GpuWindowExecMeta, GpuWindowSpecDefinitionMeta}
import org.apache.hadoop.fs.Path

import org.apache.spark.internal.Logging
import org.apache.spark.rapids.hybrid.HybridExecutionUtils
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.expressions.objects.StaticInvoke
import org.apache.spark.sql.catalyst.expressions.rapids.TimeStamp
import org.apache.spark.sql.catalyst.json.rapids.GpuJsonScan
import org.apache.spark.sql.catalyst.json.rapids.GpuJsonScan.JsonToStructsReaderType
import org.apache.spark.sql.catalyst.optimizer.NormalizeNaNAndZero
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.util.{ArrayData, DateTimeUtils}
import org.apache.spark.sql.connector.read.Scan
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, BroadcastQueryStageExec, QueryStageExec, ShuffleQueryStageExec}
import org.apache.spark.sql.execution.aggregate.{HashAggregateExec, ObjectHashAggregateExec, SortAggregateExec}
import org.apache.spark.sql.execution.columnar.InMemoryTableScanExec
import org.apache.spark.sql.execution.command.{DataWritingCommand, DataWritingCommandExec, ExecutedCommandExec, RunnableCommand}
import org.apache.spark.sql.execution.datasources.{InsertIntoHadoopFsRelationCommand, SaveIntoDataSourceCommand}
import org.apache.spark.sql.execution.datasources.csv.CSVFileFormat
import org.apache.spark.sql.execution.datasources.json.JsonFileFormat
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.execution.datasources.text.TextFileFormat
import org.apache.spark.sql.execution.datasources.v2._
import org.apache.spark.sql.execution.datasources.v2.csv.CSVScan
import org.apache.spark.sql.execution.datasources.v2.json.JsonScan
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeExec, ReusedExchangeExec, ShuffleExchangeExec}
import org.apache.spark.sql.execution.joins._
import org.apache.spark.sql.execution.python._
import org.apache.spark.sql.execution.window.WindowExec
import org.apache.spark.sql.hive.rapids.GpuHiveOverrides
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids._
import org.apache.spark.sql.rapids.aggregate._
import org.apache.spark.sql.rapids.catalyst.expressions.GpuRand
import org.apache.spark.sql.rapids.execution._
import org.apache.spark.sql.rapids.execution.python._
import org.apache.spark.sql.rapids.execution.python.GpuFlatMapGroupsInPandasExecMeta
import org.apache.spark.sql.rapids.shims.{GpuAscii, GpuMapInPandasExecMeta}
import org.apache.spark.sql.rapids.zorder.ZOrderRules
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}

/**
 * Base class for all ReplacementRules
 * @param doWrap wraps a part of the plan in a [[RapidsMeta]] for further processing.
 * @param desc a description of what this part of the plan does.
 * @param tag metadata used to determine what INPUT is at runtime.
 * @tparam INPUT the exact type of the class we are wrapping.
 * @tparam BASE the generic base class for this type of stage, i.e. SparkPlan, Expression, etc.
 * @tparam WRAP_TYPE base class that should be returned by doWrap.
 */
abstract class ReplacementRule[INPUT <: BASE, BASE, WRAP_TYPE <: RapidsMeta[INPUT, BASE, _]](
    protected var doWrap: (
        INPUT,
        RapidsConf,
        Option[RapidsMeta[_, _, _]],
        DataFromReplacementRule) => WRAP_TYPE,
    protected var desc: String,
    protected val checks: Option[TypeChecks[_]],
    final val tag: ClassTag[INPUT]) extends DataFromReplacementRule {

  private var _noteDoc: Option[String] = None
  private var _incompatDoc: Option[String] = None
  private var _disabledDoc: Option[String] = None
  private var _visible: Boolean = true

  def isVisible: Boolean = _visible
  def description: String = desc

  override def noteDoc: Option[String] = _noteDoc
  override def incompatDoc: Option[String] = _incompatDoc
  override def disabledMsg: Option[String] = _disabledDoc
  override def getChecks: Option[TypeChecks[_]] = checks

  /**
   * Mark this expression with an extra note.
   * @param str a description of the extra note.
   * @return this for chaining.
   */
  final def note(str: String) : this.type = {
    _noteDoc = Some(str)
    this
  }

  /**
   * Mark this expression as incompatible with the original Spark version
   * @param str a description of how it is incompatible.
   * @return this for chaining.
   */
  final def incompat(str: String) : this.type = {
    _incompatDoc = Some(str)
    this
  }

  /**
   * Mark this expression as disabled by default.
   * @param str a description of why it is disabled by default.
   * @return this for chaining.
   */
  final def disabledByDefault(str: String) : this.type = {
    _disabledDoc = Some(str)
    this
  }

  final def invisible(): this.type = {
    _visible = false
    this
  }

  /**
   * Provide a function that will wrap a spark type in a [[RapidsMeta]] instance that is used for
   * conversion to a GPU version.
   * @param func the function
   * @return this for chaining.
   */
  final def wrap(func: (
      INPUT,
      RapidsConf,
      Option[RapidsMeta[_, _, _]],
      DataFromReplacementRule) => WRAP_TYPE): this.type = {
    doWrap = func
    this
  }

  /**
   * Set a description of what the operation does.
   * @param str the description.
   * @return this for chaining
   */
  final def desc(str: String): this.type = {
    this.desc = str
    this
  }

  private var confKeyCache: String = null
  protected val confKeyPart: String

  override def confKey: String = {
    if (confKeyCache == null) {
      confKeyCache = "spark.rapids.sql." + confKeyPart + "." + tag.runtimeClass.getSimpleName
    }
    confKeyCache
  }

  def notes(): Option[String] = {
    val msg = if (incompatDoc.isDefined) {
      Some(s"This is not 100% compatible with the Spark version because ${incompatDoc.get}")
    } else if (disabledMsg.isDefined) {
      Some(s"This is disabled by default because ${disabledMsg.get}")
    } else {
      None
    }

    if (msg.isEmpty && noteDoc.isEmpty) {
      None
    } else {
      Some(noteDoc.getOrElse("") + msg.getOrElse(""))
    }
  }

  def confHelp(asTable: Boolean = false, sparkSQLFunctions: Option[String] = None): Unit = {
    if (_visible) {
      val notesMsg = notes()
      if (asTable) {
        import ConfHelper.makeConfAnchor
        print(s"${makeConfAnchor(confKey)}")
        if (sparkSQLFunctions.isDefined) {
          print(s"|${sparkSQLFunctions.get}")
        }
        val incompatOps = RapidsConf.INCOMPATIBLE_OPS.asInstanceOf[ConfEntryWithDefault[Boolean]]
        val expressionEnabled = disabledMsg.isEmpty &&
          (incompatDoc.isEmpty || incompatOps.defaultValue)
        print(s"|$desc|$expressionEnabled|")
        if (notesMsg.isDefined) {
          print(s"${notesMsg.get}")
        } else {
          print("None")
        }
        println("|")
      } else {
        println(s"$confKey:")
        println(s"\tEnable (true) or disable (false) the $tag $operationName.")
        if (sparkSQLFunctions.isDefined) {
          println(s"\tsql function: ${sparkSQLFunctions.get}")
        }
        println(s"\t$desc")
        if (notesMsg.isDefined) {
          println(s"\t${notesMsg.get}")
        }
        println(s"\tdefault: ${notesMsg.isEmpty}")
        println()
      }
    }
  }

  final def wrap(
      op: BASE,
      conf: RapidsConf,
      parent: Option[RapidsMeta[_, _, _]],
      r: DataFromReplacementRule): WRAP_TYPE = {
    doWrap(op.asInstanceOf[INPUT], conf, parent, r)
  }

  def getClassFor: Class[_] = tag.runtimeClass
}

/**
 * Holds everything that is needed to replace an Expression with a GPU enabled version.
 */
class ExprRule[INPUT <: Expression](
    doWrap: (
        INPUT,
        RapidsConf,
        Option[RapidsMeta[_, _, _]],
        DataFromReplacementRule) => BaseExprMeta[INPUT],
    desc: String,
    checks: Option[ExprChecks],
    tag: ClassTag[INPUT])
  extends ReplacementRule[INPUT, Expression, BaseExprMeta[INPUT]](
    doWrap, desc, checks, tag) {

  override val confKeyPart = "expression"
  override val operationName = "Expression"
}

/**
 * Holds everything that is needed to replace a `Scan` with a GPU enabled version.
 */
class ScanRule[INPUT <: Scan](
    doWrap: (
        INPUT,
        RapidsConf,
        Option[RapidsMeta[_, _, _]],
        DataFromReplacementRule) => ScanMeta[INPUT],
    desc: String,
    tag: ClassTag[INPUT])
  extends ReplacementRule[INPUT, Scan, ScanMeta[INPUT]](
    doWrap, desc, None, tag) {

  override val confKeyPart: String = "input"
  override val operationName: String = "Input"
}

/**
 * Holds everything that is needed to replace a `Partitioning` with a GPU enabled version.
 */
class PartRule[INPUT <: Partitioning](
    doWrap: (
        INPUT,
        RapidsConf,
        Option[RapidsMeta[_, _, _]],
        DataFromReplacementRule) => PartMeta[INPUT],
    desc: String,
    checks: Option[PartChecks],
    tag: ClassTag[INPUT])
  extends ReplacementRule[INPUT, Partitioning, PartMeta[INPUT]](
    doWrap, desc, checks, tag) {

  override val confKeyPart: String = "partitioning"
  override val operationName: String = "Partitioning"
}

/**
 * Holds everything that is needed to replace a `SparkPlan` with a GPU enabled version.
 */
class ExecRule[INPUT <: SparkPlan](
    doWrap: (
        INPUT,
        RapidsConf,
        Option[RapidsMeta[_, _, _]],
        DataFromReplacementRule) => SparkPlanMeta[INPUT],
    desc: String,
    checks: Option[ExecChecks],
    tag: ClassTag[INPUT])
  extends ReplacementRule[INPUT, SparkPlan, SparkPlanMeta[INPUT]](
    doWrap, desc, checks, tag) {

  // TODO finish this...

  override val confKeyPart: String = "exec"
  override val operationName: String = "Exec"
}

/**
 * Holds everything that is needed to replace a `DataWritingCommand` with a
 * GPU enabled version.
 */
class DataWritingCommandRule[INPUT <: DataWritingCommand](
    doWrap: (
        INPUT,
        RapidsConf,
        Option[RapidsMeta[_, _, _]],
        DataFromReplacementRule) => DataWritingCommandMeta[INPUT],
    desc: String,
    tag: ClassTag[INPUT])
    extends ReplacementRule[INPUT, DataWritingCommand, DataWritingCommandMeta[INPUT]](
      doWrap, desc, None, tag) {

  override val confKeyPart: String = "output"
  override val operationName: String = "Output"
}

final class InsertIntoHadoopFsRelationCommandMeta(
    cmd: InsertIntoHadoopFsRelationCommand,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
    extends DataWritingCommandMeta[InsertIntoHadoopFsRelationCommand](cmd, conf, parent, rule) {

  private var fileFormat: Option[ColumnarFileFormat] = None

  override def tagSelfForGpuInternal(): Unit = {
    if (BucketingUtilsShim.isHiveHashBucketing(cmd.options)) {
      BucketingUtilsShim.tagForHiveBucketingWrite(this, cmd.bucketSpec, cmd.outputColumns,
        conf.isForceHiveHashForBucketedWrite)
    } else {
      BucketIdMetaUtils.tagForBucketingWrite(this, cmd.bucketSpec, cmd.outputColumns)
    }
    val spark = SparkSession.active
    val formatCls = cmd.fileFormat.getClass
    fileFormat = if (classOf[CSVFileFormat].isAssignableFrom(formatCls)) {
      willNotWorkOnGpu("CSV output is not supported")
      None
    } else if (formatCls == classOf[JsonFileFormat]) {
      willNotWorkOnGpu("JSON output is not supported")
      None
    } else if (GpuOrcFileFormat.isSparkOrcFormat(formatCls)) {
      GpuOrcFileFormat.tagGpuSupport(this, spark, cmd.options, cmd.query.schema)
    } else if (formatCls == classOf[ParquetFileFormat]) {
      GpuParquetFileFormat.tagGpuSupport(this, spark, cmd.options, cmd.query.schema)
    } else if (formatCls == classOf[TextFileFormat]) {
      willNotWorkOnGpu("text output is not supported")
      None
    } else {
      willNotWorkOnGpu(s"unknown file format: ${formatCls.getCanonicalName}")
      None
    }
  }

  override def convertToGpu(): GpuDataWritingCommand = {
    val format = fileFormat.getOrElse(
      throw new IllegalStateException("fileFormat missing, tagSelfForGpu not called?"))

    GpuInsertIntoHadoopFsRelationCommand(
      cmd.outputPath,
      cmd.staticPartitions,
      cmd.ifPartitionNotExists,
      cmd.partitionColumns,
      cmd.bucketSpec,
      format,
      cmd.options,
      cmd.query,
      cmd.mode,
      cmd.catalogTable,
      cmd.fileIndex,
      cmd.outputColumnNames,
      conf.stableSort,
      conf.concurrentWriterPartitionFlushSize,
      conf.outputDebugDumpPrefix)
  }
}

/**
 * Holds everything that is needed to replace a `RunnableCommand` with a
 * GPU enabled version.
 */
class RunnableCommandRule[INPUT <: RunnableCommand](
    doWrap: (
        INPUT,
            RapidsConf,
            Option[RapidsMeta[_, _, _]],
            DataFromReplacementRule) => RunnableCommandMeta[INPUT],
    desc: String,
    tag: ClassTag[INPUT])
    extends ReplacementRule[INPUT, RunnableCommand, RunnableCommandMeta[INPUT]](
      doWrap, desc, None, tag) {

  override val confKeyPart: String = "command"
  override val operationName: String = "Command"
}

/**
 * Listener trait so that tests can confirm that the expected optimizations are being applied
 */
trait GpuOverridesListener {
  def optimizedPlan(
      plan: SparkPlanMeta[SparkPlan],
      sparkPlan: SparkPlan,
      costOptimizations: Seq[Optimization]): Unit
}

sealed trait FileFormatType
object CsvFormatType extends FileFormatType {
  override def toString = "CSV"
}
object HiveDelimitedTextFormatType extends FileFormatType {
  override def toString = "HiveText"
}
object ParquetFormatType extends FileFormatType {
  override def toString = "Parquet"
}
object OrcFormatType extends FileFormatType {
  override def toString = "ORC"
}
object JsonFormatType extends FileFormatType {
  override def toString = "JSON"
}
object AvroFormatType extends FileFormatType {
  override def toString = "Avro"
}
object IcebergFormatType extends FileFormatType {
  override def toString = "Iceberg"
}
object DeltaFormatType extends FileFormatType {
  override def toString = "Delta"
}

sealed trait FileFormatOp
object ReadFileOp extends FileFormatOp {
  override def toString = "read"
}
object WriteFileOp extends FileFormatOp {
  override def toString = "write"
}

object GpuOverrides extends Logging {
  val FLOAT_DIFFERS_GROUP_INCOMPAT =
    "when enabling these, there may be extra groups produced for floating point grouping " +
    "keys (e.g. -0.0, and 0.0)"
  val CASE_MODIFICATION_INCOMPAT =
    "the Unicode version used by cuDF and the JVM may differ, resulting in some " +
    "corner-case characters not changing case correctly."
  val UTC_TIMEZONE_ID = ZoneId.of("UTC").normalized()
  // Based on https://docs.oracle.com/javase/8/docs/api/java/util/regex/Pattern.html
  private[this] lazy val regexList: Seq[String] = Seq("\\", "\u0000", "\\x", "\t", "\n", "\r",
    "\f", "\\a", "\\e", "\\cx", "[", "]", "^", "&", ".", "*", "\\d", "\\D", "\\h", "\\H", "\\s",
    "\\S", "\\v", "\\V", "\\w", "\\w", "\\p", "$", "\\b", "\\B", "\\A", "\\G", "\\Z", "\\z", "\\R",
    "?", "+", "|", "(", ")", "{", "}", "\\k", "\\Q", "\\E", ":", "!", "<=", ">")
  val regexMetaChars = ".$^[]\\|?*+(){}"
  /**
   * Provides a way to log an info message about how long an operation took in milliseconds.
   */
  def logDuration[T](shouldLog: Boolean, msg: Double => String)(block: => T): T = {
    val start = System.nanoTime()
    val ret = block
    val end = System.nanoTime()
    if (shouldLog) {
      val timeTaken = (end - start).toDouble / java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(1)
      logInfo(msg(timeTaken))
    }
    ret
  }

  val gpuCommonTypes = TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128

  val pluginSupportedOrderableSig: TypeSig = (gpuCommonTypes + TypeSig.STRUCT).nested()

  // this listener mechanism is global and is intended for use by unit tests only
  private lazy val listeners: ListBuffer[GpuOverridesListener] =
    new ListBuffer[GpuOverridesListener]()

  def addListener(listener: GpuOverridesListener): Unit = {
    listeners += listener
  }

  def removeListener(listener: GpuOverridesListener): Unit = {
    listeners -= listener
  }

  def removeAllListeners(): Unit = {
    listeners.clear()
  }

  private def convertPartToGpuIfPossible(part: Partitioning, conf: RapidsConf): Partitioning = {
    part match {
      case _: GpuPartitioning => part
      case _ =>
        val wrapped = wrapPart(part, conf, None)
        wrapped.tagForGpu()
        if (wrapped.canThisBeReplaced) {
          wrapped.convertToGpu()
        } else {
          part
        }
    }
  }

  /**
   * Removes unnecessary CPU shuffles that Spark can add to the plan when it does not realize
   * a GPU partitioning satisfies a CPU distribution because CPU and GPU expressions are not
   * semantically equal.
   */
  def removeExtraneousShuffles(plan: SparkPlan, conf: RapidsConf): SparkPlan = {
    plan.transformUp {
      case cpuShuffle: ShuffleExchangeExec =>
        cpuShuffle.child match {
          case sqse: ShuffleQueryStageExec =>
            GpuTransitionOverrides.getNonQueryStagePlan(sqse) match {
              case gpuShuffle: GpuShuffleExchangeExecBase =>
                val converted = convertPartToGpuIfPossible(cpuShuffle.outputPartitioning, conf)
                if (converted == gpuShuffle.outputPartitioning) {
                  sqse
                } else {
                  cpuShuffle
                }
              case _ => cpuShuffle
            }
          case _ => cpuShuffle
        }
    }
  }

  /**
   * On some Spark platforms, AQE planning can result in old CPU exchanges being placed in the
   * plan even after they have been replaced previously. This looks for subquery reuses of CPU
   * exchanges that can be replaced with recently planned GPU exchanges that match the original
   * CPU plan
   */
  def fixupCpuReusedExchanges(plan: SparkPlan): SparkPlan = {
    plan.transformUp {
      case bqse: BroadcastQueryStageExec =>
        bqse.plan match {
          case ReusedExchangeExec(output, b: BroadcastExchangeExec) =>
            val cpuCanonical = b.canonicalized.asInstanceOf[BroadcastExchangeExec]
            val gpuExchange = ExchangeMappingCache.findGpuExchangeReplacement(cpuCanonical)
            gpuExchange.map { g =>
              SparkShimImpl.newBroadcastQueryStageExec(bqse, ReusedExchangeExec(output, g))
            }.getOrElse(bqse)
          case _ => bqse
        }
    }
  }

  /**
   * Searches the plan for ReusedExchangeExec instances containing a GPU shuffle where the
   * output types between the two plan nodes do not match. In such a case the ReusedExchangeExec
   * will be updated to match the GPU shuffle output types.
   */
  def fixupReusedExchangeOutputs(plan: SparkPlan): SparkPlan = {
    def outputTypesMatch(a: Seq[Attribute], b: Seq[Attribute]): Boolean =
      a.corresponds(b)((x, y) => x.dataType == y.dataType)
    plan.transformUp {
      case sqse: ShuffleQueryStageExec =>
        sqse.plan match {
          case ReusedExchangeExec(output, gsee: GpuShuffleExchangeExecBase) if (
              !outputTypesMatch(output, gsee.output)) =>
            val newOutput = sqse.plan.output.zip(gsee.output).map { case (c, g) =>
              assert(c.isInstanceOf[AttributeReference] && g.isInstanceOf[AttributeReference],
                s"Expected AttributeReference but found $c and $g")
              AttributeReference(c.name, g.dataType, c.nullable, c.metadata)(c.exprId, c.qualifier)
            }
            AQEUtils.newReuseInstance(sqse, newOutput)
          case _ => sqse
        }
    }
  }

  @scala.annotation.tailrec
  def extractLit(exp: Expression): Option[Literal] = exp match {
    case l: Literal => Some(l)
    case a: Alias => extractLit(a.child)
    case _ => None
  }

  def isOfType(l: Option[Literal], t: DataType): Boolean = l.exists(_.dataType == t)

  def isStringLit(exp: Expression): Boolean =
    isOfType(extractLit(exp), StringType)

  def extractStringLit(exp: Expression): Option[String] = extractLit(exp) match {
    case Some(Literal(v: UTF8String, StringType)) =>
      val s = if (v == null) null else v.toString
      Some(s)
    case _ => None
  }

  def isLit(exp: Expression): Boolean = extractLit(exp).isDefined

  def isNullLit(lit: Literal): Boolean = {
    lit.value == null
  }

  def isSupportedStringReplacePattern(strLit: String): Boolean = {
    // check for regex special characters, except for \u0000, \n, \r, and \t which we can support
    val supported = Seq("\u0000", "\n", "\r", "\t")
    !regexList.filterNot(supported.contains(_)).exists(pattern => strLit.contains(pattern))
  }

  def isSupportedStringReplacePattern(exp: Expression): Boolean = {
    extractLit(exp) match {
      case Some(Literal(null, _)) => false
      case Some(Literal(value: UTF8String, DataTypes.StringType)) =>
        val strLit = value.toString
        if (strLit.isEmpty) {
          false
        } else {
          isSupportedStringReplacePattern(strLit)
        }
      case _ => false
    }
  }

  def isUTCTimezone(timezoneId: ZoneId): Boolean = {
    timezoneId.normalized() == UTC_TIMEZONE_ID
  }

  def isUTCTimezone(timezoneIdStr: String): Boolean = {
    isUTCTimezone(GpuTimeZoneDB.getZoneId(timezoneIdStr))
  }

  def isUTCTimezone(): Boolean = {
    val zoneId = DateTimeUtils.getZoneId(SQLConf.get.sessionLocalTimeZone)
    isUTCTimezone(zoneId.normalized())
  }

  def areAllSupportedTypes(types: DataType*): Boolean = types.forall(isSupportedType(_))

  /**
   * Is this particular type supported or not.
   * @param dataType the type to check
   * @param allowNull should NullType be allowed
   * @param allowDecimal should DecimalType be allowed
   * @param allowBinary should BinaryType be allowed
   * @param allowCalendarInterval should CalendarIntervalType be allowed
   * @param allowArray should ArrayType be allowed
   * @param allowStruct should StructType be allowed
   * @param allowStringMaps should a Map[String, String] specifically be allowed
   * @param allowMaps should MapType be allowed generically
   * @param allowNesting should nested types like array struct and map allow nested types
   *                     within them, or only primitive types.
   * @return true if it is allowed else false
   */
  def isSupportedType(dataType: DataType,
      allowNull: Boolean = false,
      allowDecimal: Boolean = false,
      allowBinary: Boolean = false,
      allowCalendarInterval: Boolean = false,
      allowArray: Boolean = false,
      allowStruct: Boolean = false,
      allowStringMaps: Boolean = false,
      allowMaps: Boolean = false,
      allowNesting: Boolean = false): Boolean = {
    def checkNested(dataType: DataType): Boolean = {
      isSupportedType(dataType,
        allowNull = allowNull,
        allowDecimal = allowDecimal,
        allowBinary = allowBinary && allowNesting,
        allowCalendarInterval = allowCalendarInterval && allowNesting,
        allowArray = allowArray && allowNesting,
        allowStruct = allowStruct && allowNesting,
        allowStringMaps = allowStringMaps && allowNesting,
        allowMaps = allowMaps && allowNesting,
        allowNesting = allowNesting)
    }
    dataType match {
      case BooleanType => true
      case ByteType => true
      case ShortType => true
      case IntegerType => true
      case LongType => true
      case FloatType => true
      case DoubleType => true
      case DateType => true
      case TimestampType => true
      case StringType => true
      case dt: DecimalType if allowDecimal => dt.precision <= DType.DECIMAL64_MAX_PRECISION
      case NullType => allowNull
      case BinaryType => allowBinary
      case CalendarIntervalType => allowCalendarInterval
      case ArrayType(elementType, _) if allowArray => checkNested(elementType)
      case MapType(StringType, StringType, _) if allowStringMaps => true
      case MapType(keyType, valueType, _) if allowMaps =>
        checkNested(keyType) && checkNested(valueType)
      case StructType(fields) if allowStruct =>
        fields.map(_.dataType).forall(checkNested)
      case _ => false
    }
  }

  /**
   * Checks to see if any expressions are a String Literal
   */
  def isAnyStringLit(expressions: Seq[Expression]): Boolean =
    expressions.exists(isStringLit)

  def isOrContainsFloatingPoint(dataType: DataType): Boolean =
    TrampolineUtil.dataTypeExistsRecursively(dataType, dt => dt == FloatType || dt == DoubleType)

  def isOrContainsDateOrTimestamp(dataType: DataType): Boolean =
    TrampolineUtil.dataTypeExistsRecursively(dataType, dt => dt == TimestampType || dt == DateType)

  def isOrContainsTimestamp(dataType: DataType): Boolean =
    TrampolineUtil.dataTypeExistsRecursively(dataType, dt => dt == TimestampType)

  /** Tries to predict whether an adaptive plan will end up with data on the GPU or not. */
  def probablyGpuPlan(adaptivePlan: AdaptiveSparkPlanExec, conf: RapidsConf): Boolean = {
    def findRootProcessingNode(plan: SparkPlan): SparkPlan = plan match {
      case p: AdaptiveSparkPlanExec => findRootProcessingNode(p.executedPlan)
      case p: QueryStageExec => findRootProcessingNode(p.plan)
      case p: ReusedSubqueryExec => findRootProcessingNode(p.child)
      case p: ReusedExchangeExec => findRootProcessingNode(p.child)
      case p => p
    }

    val aqeSubPlan = findRootProcessingNode(adaptivePlan.executedPlan)
    aqeSubPlan match {
      case _: GpuExec =>
        // plan is already on the GPU
        true
      case p =>
        // see if the root processing node of the current subplan will translate to the GPU
        val meta = GpuOverrides.wrapAndTagPlan(p, conf)
        meta.canThisBeReplaced
    }
  }

  def checkAndTagFloatAgg(dataType: DataType, conf: RapidsConf, meta: RapidsMeta[_,_,_]): Unit = {
    if (!conf.isFloatAggEnabled && isOrContainsFloatingPoint(dataType)) {
      meta.willNotWorkOnGpu("the GPU will aggregate floating point values in" +
          " parallel and the result is not always identical each time. This can cause" +
          " some Spark queries to produce an incorrect answer if the value is computed" +
          " more than once as part of the same query.  To enable this anyways set" +
          s" ${RapidsConf.ENABLE_FLOAT_AGG} to true.")
    }
  }

  /**
   * Helper function specific to ANSI mode for the aggregate functions that should
   * fallback, since we don't have the same overflow checks that Spark provides in
   * the CPU
   * @param checkType Something other than `None` triggers logic to detect whether
   *                  the agg should fallback in ANSI mode. Otherwise (None), it's
   *                  an automatic fallback.
   * @param meta agg expression meta
   */
  def checkAndTagAnsiAgg(checkType: Option[DataType], meta: AggExprMeta[_]): Unit = {
    val failOnError = SQLConf.get.ansiEnabled
    if (failOnError) {
      if (checkType.isDefined) {
        val typeToCheck = checkType.get
        val failedType = typeToCheck match {
          case _: DecimalType | LongType | IntegerType | ShortType | ByteType => true
          case _ =>  false
        }
        if (failedType) {
          meta.willNotWorkOnGpu(
            s"ANSI mode not supported for ${meta.expr} with $typeToCheck result type")
        }
      } else {
        // Average falls into this category, where it produces Doubles, but
        // internally it uses Double and Long, and Long could overflow (technically)
        // and failOnError given that it is based on catalyst Add.
        meta.willNotWorkOnGpu(
          s"ANSI mode not supported for ${meta.expr}")
      }
    }
  }

  def expr[INPUT <: Expression](
      desc: String,
      pluginChecks: ExprChecks,
      doWrap: (INPUT, RapidsConf, Option[RapidsMeta[_, _, _]], DataFromReplacementRule)
          => BaseExprMeta[INPUT])
      (implicit tag: ClassTag[INPUT]): ExprRule[INPUT] = {
    assert(desc != null)
    assert(doWrap != null)
    new ExprRule[INPUT](doWrap, desc, Some(pluginChecks), tag)
  }

  def scan[INPUT <: Scan](
      desc: String,
      doWrap: (INPUT, RapidsConf, Option[RapidsMeta[_, _, _]], DataFromReplacementRule)
          => ScanMeta[INPUT])
      (implicit tag: ClassTag[INPUT]): ScanRule[INPUT] = {
    assert(desc != null)
    assert(doWrap != null)
    new ScanRule[INPUT](doWrap, desc, tag)
  }

  def part[INPUT <: Partitioning](
      desc: String,
      checks: PartChecks,
      doWrap: (INPUT, RapidsConf, Option[RapidsMeta[_, _, _]], DataFromReplacementRule)
          => PartMeta[INPUT])
      (implicit tag: ClassTag[INPUT]): PartRule[INPUT] = {
    assert(desc != null)
    assert(doWrap != null)
    new PartRule[INPUT](doWrap, desc, Some(checks), tag)
  }

  /**
   * Create an exec rule that should never be replaced, because it is something that should always
   * run on the CPU, or should just be ignored totally for what ever reason.
   */
  def neverReplaceExec[INPUT <: SparkPlan](desc: String)
      (implicit tag: ClassTag[INPUT]): ExecRule[INPUT] = {
    assert(desc != null)
    def doWrap(
        exec: INPUT,
        conf: RapidsConf,
        p: Option[RapidsMeta[_, _, _]],
        cc: DataFromReplacementRule) =
      new DoNotReplaceOrWarnSparkPlanMeta[INPUT](exec, conf, p)
    new ExecRule[INPUT](doWrap, desc, None, tag).invisible()
  }

  def exec[INPUT <: SparkPlan](
      desc: String,
      pluginChecks: ExecChecks,
      doWrap: (INPUT, RapidsConf, Option[RapidsMeta[_, _, _]], DataFromReplacementRule)
          => SparkPlanMeta[INPUT])
    (implicit tag: ClassTag[INPUT]): ExecRule[INPUT] = {
    assert(desc != null)
    assert(doWrap != null)
    new ExecRule[INPUT](doWrap, desc, Some(pluginChecks), tag)
  }

  def dataWriteCmd[INPUT <: DataWritingCommand](
      desc: String,
      doWrap: (INPUT, RapidsConf, Option[RapidsMeta[_, _, _]], DataFromReplacementRule)
          => DataWritingCommandMeta[INPUT])
      (implicit tag: ClassTag[INPUT]): DataWritingCommandRule[INPUT] = {
    assert(desc != null)
    assert(doWrap != null)
    new DataWritingCommandRule[INPUT](doWrap, desc, tag)
  }

  def wrapExpr[INPUT <: Expression](
      expr: INPUT,
      conf: RapidsConf,
      parent: Option[RapidsMeta[_, _, _]]): BaseExprMeta[INPUT] =
    expressions.get(expr.getClass)
      .map(r => r.wrap(expr, conf, parent, r).asInstanceOf[BaseExprMeta[INPUT]])
      .getOrElse(new RuleNotFoundExprMeta(expr, conf, parent))

  val jsonStructReadTypes: TypeSig = (TypeSig.STRUCT + TypeSig.ARRAY +
      TypeSig.STRING + TypeSig.integral + TypeSig.fp + TypeSig.DECIMAL_128 + TypeSig.BOOLEAN +
      TypeSig.DATE + TypeSig.TIMESTAMP).nested()
    .withPsNote(TypeEnum.DATE, "DATE is not supported by default due to compatibility")
    .withPsNote(TypeEnum.TIMESTAMP, "TIMESTAMP is not supported by default due to compatibility")

  lazy val fileFormats: Map[FileFormatType, Map[FileFormatOp, FileFormatChecks]] = Map(
    (CsvFormatType, FileFormatChecks(
      cudfRead = TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 +
        GpuTypeShims.additionalCsvSupportedTypes,
      cudfWrite = TypeSig.none,
      sparkSig = TypeSig.cpuAtomics)),
    (HiveDelimitedTextFormatType, FileFormatChecks(
      // Keep the supported types in sync with GpuHiveTextFileUtils.isSupportedType.
      cudfRead = TypeSig.commonCudfTypes + TypeSig.DECIMAL_128,
      cudfWrite = TypeSig.commonCudfTypes + TypeSig.DECIMAL_128,
      sparkSig = TypeSig.all)),
    (DeltaFormatType, FileFormatChecks(
      cudfRead = (TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.STRUCT +
          TypeSig.ARRAY + TypeSig.MAP + TypeSig.BINARY +
          GpuTypeShims.additionalParquetSupportedTypes).nested(),
      cudfWrite = (TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.STRUCT +
          TypeSig.ARRAY + TypeSig.MAP + TypeSig.BINARY +
          GpuTypeShims.additionalParquetSupportedTypes).nested(),
      sparkSig = (TypeSig.cpuAtomics + TypeSig.STRUCT + TypeSig.ARRAY + TypeSig.MAP +
          TypeSig.UDT + GpuTypeShims.additionalParquetSupportedTypes).nested())),
    (ParquetFormatType, FileFormatChecks(
      cudfRead = (TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.STRUCT +
          TypeSig.ARRAY + TypeSig.MAP + TypeSig.BINARY +
          GpuTypeShims.additionalParquetSupportedTypes).nested(),
      cudfWrite = (TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.STRUCT +
          TypeSig.ARRAY + TypeSig.MAP + TypeSig.BINARY +
          GpuTypeShims.additionalParquetSupportedTypes).nested(),
      sparkSig = (TypeSig.cpuAtomics + TypeSig.STRUCT + TypeSig.ARRAY + TypeSig.MAP +
          TypeSig.UDT + GpuTypeShims.additionalParquetSupportedTypes).nested())),
    (OrcFormatType, FileFormatChecks(
      cudfRead = (TypeSig.commonCudfTypes + TypeSig.ARRAY + TypeSig.DECIMAL_128 +
          TypeSig.STRUCT + TypeSig.MAP).nested(),
      cudfWrite = (TypeSig.commonCudfTypes + TypeSig.ARRAY +
          // Note Map is not put into nested, now CUDF only support single level map
          TypeSig.STRUCT + TypeSig.DECIMAL_128).nested() + TypeSig.MAP,
      sparkSig = (TypeSig.cpuAtomics + TypeSig.STRUCT + TypeSig.ARRAY + TypeSig.MAP +
          TypeSig.UDT).nested())),
    (JsonFormatType, FileFormatChecks(
      cudfRead = jsonStructReadTypes,
      cudfWrite = TypeSig.none,
      sparkSig = (TypeSig.cpuAtomics + TypeSig.STRUCT + TypeSig.ARRAY + TypeSig.MAP +
        TypeSig.UDT).nested())),
    (AvroFormatType, FileFormatChecks(
      cudfRead = TypeSig.BOOLEAN + TypeSig.BYTE + TypeSig.SHORT + TypeSig.INT + TypeSig.LONG +
        TypeSig.FLOAT + TypeSig.DOUBLE + TypeSig.STRING,
      cudfWrite = TypeSig.none,
      sparkSig = (TypeSig.cpuAtomics + TypeSig.STRUCT + TypeSig.ARRAY + TypeSig.MAP +
        TypeSig.UDT).nested())),
    (IcebergFormatType, FileFormatChecks(
      cudfRead = (TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.STRUCT + TypeSig.BINARY +
          TypeSig.ARRAY + TypeSig.MAP + GpuTypeShims.additionalParquetSupportedTypes).nested(),
      cudfWrite = (TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.STRUCT +
          TypeSig.ARRAY + TypeSig.MAP + TypeSig.BINARY +
          GpuTypeShims.additionalParquetSupportedTypes).nested(),
      sparkSig = (TypeSig.cpuAtomics + TypeSig.STRUCT + TypeSig.ARRAY + TypeSig.MAP +
          TypeSig.BINARY + TypeSig.UDT + GpuTypeShims.additionalParquetSupportedTypes).nested())))

  val commonExpressions: Map[Class[_ <: Expression], ExprRule[_ <: Expression]] = Seq(
    expr[Literal](
      "Holds a static value from the query",
      ExprChecks.projectAndAst(
        TypeSig.astTypes,
        (TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128 + TypeSig.CALENDAR
          + TypeSig.BINARY + TypeSig.ARRAY + TypeSig.MAP + TypeSig.STRUCT + TypeSig.ansiIntervals)
          .nested(TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128 + TypeSig.BINARY +
            TypeSig.ARRAY + TypeSig.MAP + TypeSig.STRUCT),
        TypeSig.all),
      (lit, conf, p, r) => new LiteralExprMeta(lit, conf, p, r)),
    expr[Signum](
      "Returns -1.0, 0.0 or 1.0 as expr is negative, 0 or positive",
      ExprChecks.mathUnary,
      (a, conf, p, r) => new UnaryExprMeta[Signum](a, conf, p, r) {
        override def convertToGpu(child: Expression): GpuExpression = GpuSignum(child)
      }),
    expr[Alias](
      "Gives a column a name",
      ExprChecks.unaryProjectAndAstInputMatchesOutput(
        TypeSig.astTypes + GpuTypeShims.additionalCommonOperatorSupportedTypes,
        (TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.MAP + TypeSig.ARRAY + TypeSig.STRUCT
            + TypeSig.DECIMAL_128 + TypeSig.BINARY
            + GpuTypeShims.additionalCommonOperatorSupportedTypes).nested(),
        TypeSig.all),
      (a, conf, p, r) => new UnaryAstExprMeta[Alias](a, conf, p, r) {
        override def convertToGpu(child: Expression): GpuExpression =
          GpuAlias(child, a.name)(a.exprId, a.qualifier, a.explicitMetadata)
      }),
    expr[BoundReference](
      "Reference to a bound variable",
      ExprChecks.projectAndAst(
        TypeSig.astTypes + GpuTypeShims.additionalCommonOperatorSupportedTypes,
        (TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.MAP + TypeSig.ARRAY + TypeSig.STRUCT +
          TypeSig.DECIMAL_128 + TypeSig.BINARY +
          GpuTypeShims.additionalCommonOperatorSupportedTypes).nested(),
        TypeSig.all),
      (currentRow, conf, p, r) => new ExprMeta[BoundReference](currentRow, conf, p, r) {
        // BoundReference should not be directly wrapped in a bridge (unit test compatibility)
        override def isBridgeCompatible: Boolean = false
        
        override def convertToGpuImpl(): GpuExpression = GpuBoundReference(
          currentRow.ordinal, currentRow.dataType, currentRow.nullable)(
          NamedExpression.newExprId, "")
      }),
    expr[AttributeReference](
      "References an input column",
      ExprChecks.projectAndAst(
        TypeSig.astTypes + GpuTypeShims.additionalArithmeticSupportedTypes,
        (TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.MAP + TypeSig.ARRAY +
            TypeSig.STRUCT + TypeSig.DECIMAL_128 + TypeSig.BINARY +
            GpuTypeShims.additionalArithmeticSupportedTypes).nested(),
        TypeSig.all),
        (att, conf, p, r) => new BaseExprMeta[AttributeReference](att, conf, p, r) {
          // This is the only NOOP operator.  It goes away when things are bound
          override def convertToGpuImpl(): Expression = att

          // There are so many of these that we don't need to print them out, unless it
          // will not work on the GPU
          override def print(append: StringBuilder, depth: Int, all: Boolean): Unit = {
            if (!this.canThisBeReplaced || cannotRunOnGpuBecauseOfSparkPlan) {
              super.print(append, depth, all)
            }
          }
        }),

    expr[ToDegrees](
      "Converts radians to degrees",
      ExprChecks.mathUnary,
      (a, conf, p, r) => new UnaryExprMeta[ToDegrees](a, conf, p, r) {
        override def convertToGpu(child: Expression): GpuToDegrees = GpuToDegrees(child)
      }),
    expr[ToRadians](
      "Converts degrees to radians",
      ExprChecks.mathUnary,
      (a, conf, p, r) => new UnaryExprMeta[ToRadians](a, conf, p, r) {
        override def convertToGpu(child: Expression): GpuToRadians = GpuToRadians(child)
      }),
    expr[Bin](
      "Returns the string representation of the long value `expr` represented in binary",
      ExprChecks.unaryProject(TypeSig.STRING, TypeSig.STRING, TypeSig.LONG, TypeSig.LONG),
      (a, conf, p, r) => new UnaryExprMeta[Bin](a, conf, p, r) {
        override def convertToGpu(child: Expression): GpuBin = GpuBin(child)
      }),
    expr[Hex](
      "Returns the hex string representation of a value",
      ExprChecks.unaryProject(TypeSig.STRING, TypeSig.STRING,
        TypeSig.LONG + TypeSig.STRING + TypeSig.BINARY,
        TypeSig.LONG + TypeSig.STRING + TypeSig.BINARY),
      (a, conf, p, r) => new UnaryExprMeta[Hex](a, conf, p, r) {
        override def convertToGpu(child: Expression): GpuExpression = GpuHex(child)
      }),
    expr[WindowExpression](
      "Calculates a return value for every input row of a table based on a group (or " +
        "\"window\") of rows",
      ExprChecks.windowOnly(
        TypeSig.all,
        TypeSig.all,
        Seq(ParamCheck("windowFunction", TypeSig.all, TypeSig.all),
          ParamCheck("windowSpec",
            TypeSig.CALENDAR + TypeSig.NULL + TypeSig.integral + TypeSig.DECIMAL_64,
            TypeSig.numericAndInterval))),
      (windowExpression, conf, p, r) => new GpuWindowExpressionMeta(windowExpression, conf, p, r)),
    expr[SpecifiedWindowFrame](
      "Specification of the width of the group (or \"frame\") of input rows " +
        "around which a window function is evaluated",
      ExprChecks.projectOnly(
        TypeSig.CALENDAR + TypeSig.NULL + TypeSig.integral,
        TypeSig.numericAndInterval,
        Seq(
          ParamCheck("lower",
            TypeSig.CALENDAR + TypeSig.NULL + TypeSig.integral + TypeSig.DECIMAL_128 +
              TypeSig.FLOAT + TypeSig.DOUBLE,
            TypeSig.numericAndInterval),
          ParamCheck("upper",
            TypeSig.CALENDAR + TypeSig.NULL + TypeSig.integral + TypeSig.DECIMAL_128 +
              TypeSig.FLOAT + TypeSig.DOUBLE,
            TypeSig.numericAndInterval))),
      (windowFrame, conf, p, r) => new GpuSpecifiedWindowFrameMeta(windowFrame, conf, p, r) ),
    expr[WindowSpecDefinition](
      "Specification of a window function, indicating the partitioning-expression, the row " +
        "ordering, and the width of the window",
      WindowSpecCheck,
      (windowSpec, conf, p, r) => new GpuWindowSpecDefinitionMeta(windowSpec, conf, p, r)),
    expr[CurrentRow.type](
      "Special boundary for a window frame, indicating stopping at the current row",
      ExprChecks.projectOnly(TypeSig.NULL, TypeSig.NULL),
      (currentRow, conf, p, r) => new ExprMeta[CurrentRow.type](currentRow, conf, p, r) {
        override def convertToGpuImpl(): GpuExpression = GpuSpecialFrameBoundary(currentRow)
      }),
    expr[UnboundedPreceding.type](
      "Special boundary for a window frame, indicating all rows preceding the current row",
      ExprChecks.projectOnly(TypeSig.NULL, TypeSig.NULL),
      (unboundedPreceding, conf, p, r) =>
        new ExprMeta[UnboundedPreceding.type](unboundedPreceding, conf, p, r) {
          override def convertToGpuImpl(): GpuExpression =
            GpuSpecialFrameBoundary(unboundedPreceding)
        }),
    expr[UnboundedFollowing.type](
      "Special boundary for a window frame, indicating all rows preceding the current row",
      ExprChecks.projectOnly(TypeSig.NULL, TypeSig.NULL),
      (unboundedFollowing, conf, p, r) =>
        new ExprMeta[UnboundedFollowing.type](unboundedFollowing, conf, p, r) {
          override def convertToGpuImpl(): GpuExpression =
            GpuSpecialFrameBoundary(unboundedFollowing)
        }),
    expr[RowNumber](
      "Window function that returns the index for the row within the aggregation window",
      ExprChecks.windowOnly(TypeSig.INT, TypeSig.INT,
        repeatingParamCheck =
          Some(RepeatingParamCheck("ordering",
            TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL,
            TypeSig.all))),
      (rowNumber, conf, p, r) => new ExprMeta[RowNumber](rowNumber, conf, p, r) {
        override def convertToGpuImpl(): GpuExpression = GpuRowNumber
      }),
    expr[Rank](
      "Window function that returns the rank value within the aggregation window",
      ExprChecks.windowOnly(TypeSig.INT, TypeSig.INT,
        repeatingParamCheck =
          Some(RepeatingParamCheck("ordering",
            TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL,
            TypeSig.all))),
      (rank, conf, p, r) => new ExprMeta[Rank](rank, conf, p, r) {
        override def convertToGpuImpl(): GpuExpression = GpuRank(childExprs.map(_.convertToGpu()))
      }),
    expr[DenseRank](
      "Window function that returns the dense rank value within the aggregation window",
      ExprChecks.windowOnly(TypeSig.INT, TypeSig.INT,
        repeatingParamCheck =
          Some(RepeatingParamCheck("ordering",
            TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL,
            TypeSig.all))),
      (denseRank, conf, p, r) => new ExprMeta[DenseRank](denseRank, conf, p, r) {
        override def convertToGpuImpl(): GpuExpression =
          GpuDenseRank(childExprs.map(_.convertToGpu()))
      }),
    expr[PercentRank](
      "Window function that returns the percent rank value within the aggregation window",
      ExprChecks.windowOnly(TypeSig.DOUBLE, TypeSig.DOUBLE,
        repeatingParamCheck =
          Some(RepeatingParamCheck("ordering",
            TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL,
            TypeSig.all))),
      (percentRank, conf, p, r) => new ExprMeta[PercentRank](percentRank, conf, p, r) {
        override def convertToGpuImpl(): GpuExpression =
          GpuPercentRank(childExprs.map(_.convertToGpu()))
      }),
    expr[Lead](
      "Window function that returns N entries ahead of this one",
      ExprChecks.windowOnly(
        (TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL +
          TypeSig.ARRAY + TypeSig.STRUCT).nested(),
        TypeSig.all,
        Seq(
          ParamCheck("input",
            (TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 +
              TypeSig.NULL + TypeSig.ARRAY + TypeSig.STRUCT).nested(),
            TypeSig.all),
          ParamCheck("offset", TypeSig.INT, TypeSig.INT),
          ParamCheck("default",
            (TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL +
              TypeSig.ARRAY + TypeSig.STRUCT).nested(),
            TypeSig.all)
        )
      ),
      (lead, conf, p, r) => new OffsetWindowFunctionMeta[Lead](lead, conf, p, r) {
        override def convertToGpuImpl(): GpuExpression =
          GpuLead(input.convertToGpu(), offset.convertToGpu(), default.convertToGpu())
      }),
    expr[Lag](
      "Window function that returns N entries behind this one",
      ExprChecks.windowOnly(
        (TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL +
          TypeSig.ARRAY + TypeSig.STRUCT).nested(),
        TypeSig.all,
        Seq(
          ParamCheck("input",
            (TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 +
              TypeSig.NULL + TypeSig.ARRAY + TypeSig.STRUCT).nested(),
            TypeSig.all),
          ParamCheck("offset", TypeSig.INT, TypeSig.INT),
          ParamCheck("default",
            (TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL +
              TypeSig.ARRAY + TypeSig.STRUCT).nested(),
            TypeSig.all)
        )
      ),
      (lag, conf, p, r) => new OffsetWindowFunctionMeta[Lag](lag, conf, p, r) {
        override def convertToGpuImpl(): GpuExpression =
          GpuLag(input.convertToGpu(), offset.convertToGpu(), default.convertToGpu())
      }),
    expr[PreciseTimestampConversion](
      "Expression used internally to convert the TimestampType to Long and back without losing " +
          "precision, i.e. in microseconds. Used in time windowing",
      ExprChecks.unaryProject(
        TypeSig.TIMESTAMP + TypeSig.LONG,
        TypeSig.TIMESTAMP + TypeSig.LONG,
        TypeSig.TIMESTAMP + TypeSig.LONG,
        TypeSig.TIMESTAMP + TypeSig.LONG),
      (a, conf, p, r) => new UnaryExprMeta[PreciseTimestampConversion](a, conf, p, r) {
        override def convertToGpu(child: Expression): GpuExpression =
          GpuPreciseTimestampConversion(child, a.fromType, a.toType)
      }),
    expr[UnaryMinus](
      "Negate a numeric value",
      ExprChecks.unaryProjectAndAstInputMatchesOutput(
        TypeSig.implicitCastsAstTypes,
        TypeSig.gpuNumeric + GpuTypeShims.additionalArithmeticSupportedTypes,
        TypeSig.numericAndInterval),
      (a, conf, p, r) => new UnaryAstExprMeta[UnaryMinus](a, conf, p, r) {
        val ansiEnabled = SQLConf.get.ansiEnabled

        override def tagSelfForAst(): Unit = {
          if (ansiEnabled && GpuAnsi.needBasicOpOverflowCheck(a.dataType)) {
            willNotWorkInAst("AST unary minus does not support ANSI mode.")
          }
        }

        override def convertToGpu(child: Expression): GpuExpression =
          GpuUnaryMinus(child, ansiEnabled)
      }),
    expr[UnaryPositive](
      "A numeric value with a + in front of it",
      ExprChecks.unaryProjectAndAstInputMatchesOutput(
        TypeSig.astTypes + GpuTypeShims.additionalArithmeticSupportedTypes,
        TypeSig.gpuNumeric + GpuTypeShims.additionalArithmeticSupportedTypes,
        TypeSig.numericAndInterval),
      (a, conf, p, r) => new ExprMeta[UnaryPositive](a, conf, p, r) {
        override val isFoldableNonLitAllowed: Boolean = true
        override def convertToGpuImpl(): GpuExpression =
          GpuUnaryPositive(childExprs.head.convertToGpu())
      }),
    expr[Year](
      "Returns the year from a date or timestamp",
      ExprChecks.unaryProject(TypeSig.INT, TypeSig.INT, TypeSig.DATE, TypeSig.DATE),
      (a, conf, p, r) => new UnaryExprMeta[Year](a, conf, p, r) {
        override def convertToGpu(child: Expression): GpuExpression = GpuYear(child)
      }),
    expr[Month](
      "Returns the month from a date or timestamp",
      ExprChecks.unaryProject(TypeSig.INT, TypeSig.INT, TypeSig.DATE, TypeSig.DATE),
      (a, conf, p, r) => new UnaryExprMeta[Month](a, conf, p, r) {
        override def convertToGpu(child: Expression): GpuExpression = GpuMonth(child)
      }),
    expr[Quarter](
      "Returns the quarter of the year for date, in the range 1 to 4",
      ExprChecks.unaryProject(TypeSig.INT, TypeSig.INT, TypeSig.DATE, TypeSig.DATE),
      (a, conf, p, r) => new UnaryExprMeta[Quarter](a, conf, p, r) {
        override def convertToGpu(child: Expression): GpuExpression = GpuQuarter(child)
      }),
    expr[DayOfMonth](
      "Returns the day of the month from a date or timestamp",
      ExprChecks.unaryProject(TypeSig.INT, TypeSig.INT, TypeSig.DATE, TypeSig.DATE),
      (a, conf, p, r) => new UnaryExprMeta[DayOfMonth](a, conf, p, r) {
        override def convertToGpu(child: Expression): GpuExpression = GpuDayOfMonth(child)
      }),
    expr[DayOfYear](
      "Returns the day of the year from a date or timestamp",
      ExprChecks.unaryProject(TypeSig.INT, TypeSig.INT, TypeSig.DATE, TypeSig.DATE),
      (a, conf, p, r) => new UnaryExprMeta[DayOfYear](a, conf, p, r) {
        override def convertToGpu(child: Expression): GpuExpression = GpuDayOfYear(child)
      }),
    expr[SecondsToTimestamp](
      "Converts the number of seconds from unix epoch to a timestamp",
      ExprChecks.unaryProject(TypeSig.TIMESTAMP, TypeSig.TIMESTAMP,
      TypeSig.gpuNumeric, TypeSig.cpuNumeric),
      (a, conf, p, r) => new UnaryExprMeta[SecondsToTimestamp](a, conf, p, r) {
        override def convertToGpu(child: Expression): GpuExpression =
          GpuSecondsToTimestamp(child)
      }),
    expr[MillisToTimestamp](
      "Converts the number of milliseconds from unix epoch to a timestamp",
      ExprChecks.unaryProject(TypeSig.TIMESTAMP, TypeSig.TIMESTAMP,
      TypeSig.integral, TypeSig.integral),
      (a, conf, p, r) => new UnaryExprMeta[MillisToTimestamp](a, conf, p, r) {
        override def convertToGpu(child: Expression): GpuExpression =
          GpuMillisToTimestamp(child)
      }),
    expr[MicrosToTimestamp](
      "Converts the number of microseconds from unix epoch to a timestamp",
      ExprChecks.unaryProject(TypeSig.TIMESTAMP, TypeSig.TIMESTAMP,
      TypeSig.integral, TypeSig.integral),
      (a, conf, p, r) => new UnaryExprMeta[MicrosToTimestamp](a, conf, p, r) {
        override def convertToGpu(child: Expression): GpuExpression =
          GpuMicrosToTimestamp(child)
      }),
    expr[Acos](
      "Inverse cosine",
      ExprChecks.mathUnaryWithAst,
      (a, conf, p, r) => new UnaryAstExprMeta[Acos](a, conf, p, r) {
        override def convertToGpu(child: Expression): GpuExpression = GpuAcos(child)
      }),
    expr[Acosh](
      "Inverse hyperbolic cosine",
      ExprChecks.mathUnaryWithAst,
      (a, conf, p, r) => new UnaryAstExprMeta[Acosh](a, conf, p, r) {
        override def convertToGpu(child: Expression): GpuExpression =
          if (this.conf.includeImprovedFloat) {
            GpuAcoshImproved(child)
          } else {
            GpuAcoshCompat(child)
          }
      }),
    expr[Asin](
      "Inverse sine",
      ExprChecks.mathUnaryWithAst,
      (a, conf, p, r) => new UnaryAstExprMeta[Asin](a, conf, p, r) {
        override def convertToGpu(child: Expression): GpuExpression = GpuAsin(child)
      }),
    expr[Asinh](
      "Inverse hyperbolic sine",
      ExprChecks.mathUnaryWithAst,
      (a, conf, p, r) => new UnaryAstExprMeta[Asinh](a, conf, p, r) {
        override def convertToGpu(child: Expression): GpuExpression =
          if (this.conf.includeImprovedFloat) {
            GpuAsinhImproved(child)
          } else {
            GpuAsinhCompat(child)
          }

        override def tagSelfForAst(): Unit = {
          if (!this.conf.includeImprovedFloat) {
            // AST is not expressive enough yet to implement the conditional expression needed
            // to emulate Spark's behavior
            willNotWorkInAst("asinh is not AST compatible unless " +
                s"${RapidsConf.IMPROVED_FLOAT_OPS.key} is enabled")
          }
        }
      }),
    expr[Sqrt](
      "Square root",
      ExprChecks.mathUnaryWithAst,
      (a, conf, p, r) => new UnaryAstExprMeta[Sqrt](a, conf, p, r) {
        override def convertToGpu(child: Expression): GpuExpression = GpuSqrt(child)
      }),
    expr[Cbrt](
      "Cube root",
      ExprChecks.mathUnaryWithAst,
      (a, conf, p, r) => new UnaryAstExprMeta[Cbrt](a, conf, p, r) {
        override def convertToGpu(child: Expression): GpuExpression = GpuCbrt(child)
      }),
    expr[Hypot](
      "Pythagorean addition (Hypotenuse) of real numbers",
      ExprChecks.binaryProject(
        TypeSig.DOUBLE,
        TypeSig.DOUBLE,
        ("lhs", TypeSig.DOUBLE, TypeSig.DOUBLE),
        ("rhs", TypeSig.DOUBLE, TypeSig.DOUBLE)),
      (a, conf, p, r) => new BinaryExprMeta[Hypot](a, conf, p, r) {
        override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
          GpuHypot(lhs, rhs)
      }),
    expr[Floor](
      "Floor of a number",
      ExprChecks.unaryProjectInputMatchesOutput(
        TypeSig.DOUBLE + TypeSig.LONG + TypeSig.DECIMAL_128,
        TypeSig.DOUBLE + TypeSig.LONG + TypeSig.DECIMAL_128),
      (a, conf, p, r) => new UnaryExprMeta[Floor](a, conf, p, r) {
        override def tagExprForGpu(): Unit = {
          a.dataType match {
            case dt: DecimalType =>
              val precision = GpuFloorCeil.unboundedOutputPrecision(dt)
              if (precision > DType.DECIMAL128_MAX_PRECISION) {
                willNotWorkOnGpu(s"output precision $precision would require overflow " +
                    s"checks, which are not supported yet")
              }
            case _ => // NOOP
          }
        }

        override def convertToGpu(child: Expression): GpuExpression = {
          // use Spark `Floor.dataType` to keep consistent between Spark versions.
          GpuFloor(child, a.dataType)
        }
      }),
    expr[Ceil](
      "Ceiling of a number",
      ExprChecks.unaryProjectInputMatchesOutput(
        TypeSig.DOUBLE + TypeSig.LONG + TypeSig.DECIMAL_128,
        TypeSig.DOUBLE + TypeSig.LONG + TypeSig.DECIMAL_128),
      (a, conf, p, r) => new UnaryExprMeta[Ceil](a, conf, p, r) {
        override def tagExprForGpu(): Unit = {
          a.dataType match {
            case dt: DecimalType =>
              val precision = GpuFloorCeil.unboundedOutputPrecision(dt)
              if (precision > DType.DECIMAL128_MAX_PRECISION) {
                willNotWorkOnGpu(s"output precision $precision would require overflow " +
                    s"checks, which are not supported yet")
              }
            case _ => // NOOP
          }
        }

        override def convertToGpu(child: Expression): GpuExpression = {
          // use Spark `Ceil.dataType` to keep consistent between Spark versions.
          GpuCeil(child, a.dataType)
        }
      }),
    expr[Not](
      "Boolean not operator",
      ExprChecks.unaryProjectAndAstInputMatchesOutput(
        TypeSig.astTypes, TypeSig.BOOLEAN, TypeSig.BOOLEAN),
      (a, conf, p, r) => new UnaryAstExprMeta[Not](a, conf, p, r) {
        override def convertToGpu(child: Expression): GpuExpression = GpuNot(child)
      }),
    expr[IsNull](
      "Checks if a value is null",
      ExprChecks.unaryProjectAndAst(TypeSig.astTypes, TypeSig.BOOLEAN, TypeSig.BOOLEAN,
        (TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.MAP + TypeSig.ARRAY +
            TypeSig.STRUCT + TypeSig.DECIMAL_128 + TypeSig.BINARY +
            GpuTypeShims.additionalPredicateSupportedTypes).nested(),
        TypeSig.all),
      (a, conf, p, r) => new UnaryAstExprMeta[IsNull](a, conf, p, r) {
        override def convertToGpu(child: Expression): GpuExpression = GpuIsNull(child)
      }),
    expr[IsNotNull](
      "Checks if a value is not null",
      ExprChecks.unaryProjectAndAst(TypeSig.astTypes, TypeSig.BOOLEAN, TypeSig.BOOLEAN,
        (TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.MAP + TypeSig.ARRAY +
            TypeSig.STRUCT + TypeSig.DECIMAL_128 + TypeSig.BINARY +
            GpuTypeShims.additionalPredicateSupportedTypes).nested(),
        TypeSig.all),
      (a, conf, p, r) => new UnaryAstExprMeta[IsNotNull](a, conf, p, r) {
        override def convertToGpu(child: Expression): GpuExpression = GpuIsNotNull(child)
      }),
    expr[IsNaN](
      "Checks if a value is NaN",
      ExprChecks.unaryProject(TypeSig.BOOLEAN, TypeSig.BOOLEAN,
        TypeSig.DOUBLE + TypeSig.FLOAT, TypeSig.DOUBLE + TypeSig.FLOAT),
      (a, conf, p, r) => new UnaryExprMeta[IsNaN](a, conf, p, r) {
        override def convertToGpu(child: Expression): GpuExpression = GpuIsNan(child)
      }),
    expr[Rint](
      "Rounds up a double value to the nearest double equal to an integer",
      ExprChecks.mathUnaryWithAst,
      (a, conf, p, r) => new UnaryAstExprMeta[Rint](a, conf, p, r) {
        override def convertToGpu(child: Expression): GpuExpression = GpuRint(child)
      }),
    expr[AtLeastNNonNulls](
      "Checks if number of non null/Nan values is greater than a given value",
      ExprChecks.projectOnly(TypeSig.BOOLEAN, TypeSig.BOOLEAN,
        repeatingParamCheck = Some(RepeatingParamCheck("input",
          (TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128 + TypeSig.BINARY +
              TypeSig.MAP + TypeSig.ARRAY + TypeSig.STRUCT).nested(),
          TypeSig.all))),
      (a, conf, p, r) => new ExprMeta[AtLeastNNonNulls](a, conf, p, r) {
        def convertToGpuImpl(): GpuExpression =
          GpuAtLeastNNonNulls(a.n, childExprs.map(_.convertToGpu()))
      }),
    expr[DateAdd](
      "Returns the date that is num_days after start_date",
      ExprChecks.binaryProject(TypeSig.DATE, TypeSig.DATE,
        ("startDate", TypeSig.DATE, TypeSig.DATE),
        ("days",
            TypeSig.INT + TypeSig.SHORT + TypeSig.BYTE,
            TypeSig.INT + TypeSig.SHORT + TypeSig.BYTE)),
      (a, conf, p, r) => new BinaryExprMeta[DateAdd](a, conf, p, r) {
        override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
          GpuDateAdd(lhs, rhs)
      }),
    expr[DateSub](
      "Returns the date that is num_days before start_date",
      ExprChecks.binaryProject(TypeSig.DATE, TypeSig.DATE,
        ("startDate", TypeSig.DATE, TypeSig.DATE),
        ("days",
            TypeSig.INT + TypeSig.SHORT + TypeSig.BYTE,
            TypeSig.INT + TypeSig.SHORT + TypeSig.BYTE)),
      (a, conf, p, r) => new BinaryExprMeta[DateSub](a, conf, p, r) {
        override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
          GpuDateSub(lhs, rhs)
      }),
    expr[NaNvl](
      "Evaluates to `left` iff left is not NaN, `right` otherwise",
      ExprChecks.binaryProject(TypeSig.fp, TypeSig.fp,
        ("lhs", TypeSig.fp, TypeSig.fp),
        ("rhs", TypeSig.fp, TypeSig.fp)),
      (a, conf, p, r) => new BinaryExprMeta[NaNvl](a, conf, p, r) {
        override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
          GpuNaNvl(lhs, rhs)
      }),
    expr[ShiftLeft](
      "Bitwise shift left (<<)",
      ExprChecks.binaryProject(TypeSig.INT + TypeSig.LONG, TypeSig.INT + TypeSig.LONG,
        ("value", TypeSig.INT + TypeSig.LONG, TypeSig.INT + TypeSig.LONG),
        ("amount", TypeSig.INT, TypeSig.INT)),
      (a, conf, p, r) => new BinaryExprMeta[ShiftLeft](a, conf, p, r) {
        override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
          GpuShiftLeft(lhs, rhs)
      }),
    expr[ShiftRight](
      "Bitwise shift right (>>)",
      ExprChecks.binaryProject(TypeSig.INT + TypeSig.LONG, TypeSig.INT + TypeSig.LONG,
        ("value", TypeSig.INT + TypeSig.LONG, TypeSig.INT + TypeSig.LONG),
        ("amount", TypeSig.INT, TypeSig.INT)),
      (a, conf, p, r) => new BinaryExprMeta[ShiftRight](a, conf, p, r) {
        override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
          GpuShiftRight(lhs, rhs)
      }),
    expr[ShiftRightUnsigned](
      "Bitwise unsigned shift right (>>>)",
      ExprChecks.binaryProject(TypeSig.INT + TypeSig.LONG, TypeSig.INT + TypeSig.LONG,
        ("value", TypeSig.INT + TypeSig.LONG, TypeSig.INT + TypeSig.LONG),
        ("amount", TypeSig.INT, TypeSig.INT)),
      (a, conf, p, r) => new BinaryExprMeta[ShiftRightUnsigned](a, conf, p, r) {
        override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
          GpuShiftRightUnsigned(lhs, rhs)
      }),
    expr[BitwiseAnd](
      "Returns the bitwise AND of the operands",
      ExprChecks.binaryProjectAndAst(
        TypeSig.implicitCastsAstTypes, TypeSig.integral, TypeSig.integral,
        ("lhs", TypeSig.integral, TypeSig.integral),
        ("rhs", TypeSig.integral, TypeSig.integral)),
      (a, conf, p, r) => new BinaryAstExprMeta[BitwiseAnd](a, conf, p, r) {
        override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
          GpuBitwiseAnd(lhs, rhs)
      }),
    expr[BitwiseOr](
      "Returns the bitwise OR of the operands",
      ExprChecks.binaryProjectAndAst(
        TypeSig.implicitCastsAstTypes, TypeSig.integral, TypeSig.integral,
        ("lhs", TypeSig.integral, TypeSig.integral),
        ("rhs", TypeSig.integral, TypeSig.integral)),
      (a, conf, p, r) => new BinaryAstExprMeta[BitwiseOr](a, conf, p, r) {
        override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
          GpuBitwiseOr(lhs, rhs)
      }),
    expr[BitwiseXor](
      "Returns the bitwise XOR of the operands",
      ExprChecks.binaryProjectAndAst(
        TypeSig.implicitCastsAstTypes, TypeSig.integral, TypeSig.integral,
        ("lhs", TypeSig.integral, TypeSig.integral),
        ("rhs", TypeSig.integral, TypeSig.integral)),
      (a, conf, p, r) => new BinaryAstExprMeta[BitwiseXor](a, conf, p, r) {
        override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
          GpuBitwiseXor(lhs, rhs)
      }),
    expr[BitwiseNot](
      "Returns the bitwise NOT of the operands",
      ExprChecks.unaryProjectAndAstInputMatchesOutput(
        TypeSig.implicitCastsAstTypes, TypeSig.integral, TypeSig.integral),
      (a, conf, p, r) => new UnaryAstExprMeta[BitwiseNot](a, conf, p, r) {
        override def convertToGpu(child: Expression): GpuExpression = GpuBitwiseNot(child)
      }),
    expr[BitwiseCount](
      "Returns the number of bits that are set in the input as unsigned 64-bit integer",
      ExprChecks.unaryProject(
        TypeSig.INT, TypeSig.INT,
        TypeSig.integral + TypeSig.BOOLEAN, TypeSig.integral + TypeSig.BOOLEAN),
      (a, conf, p, r) => new UnaryExprMeta[BitwiseCount](a, conf, p, r) {
        override def convertToGpu(child: Expression): GpuExpression = GpuBitwiseCount(child)
      }),
    expr[BitAndAgg](
      "Returns the bitwise AND of all non-null input values",
      ExprChecks.reductionAndGroupByAgg(
        TypeSig.integral, TypeSig.integral,
        Seq(ParamCheck("input", TypeSig.integral, TypeSig.integral))),
      (a, conf, p, r) => new AggExprMeta[BitAndAgg](a, conf, p, r) {
        override def convertToGpu(childExprs: Seq[Expression]): GpuExpression =
          GpuBitAndAgg(childExprs.head)

        override def needsAnsiCheck: Boolean = false
      }),
    expr[BitOrAgg](
      "Returns the bitwise OR of all non-null input values",
      ExprChecks.reductionAndGroupByAgg(
        TypeSig.integral, TypeSig.integral,
        Seq(ParamCheck("input", TypeSig.integral, TypeSig.integral))),
      (a, conf, p, r) => new AggExprMeta[BitOrAgg](a, conf, p, r) {
        override def convertToGpu(childExprs: Seq[Expression]): GpuExpression =
          GpuBitOrAgg(childExprs.head)

        override def needsAnsiCheck: Boolean = false
      }),
    expr[BitXorAgg](
      "Returns the bitwise XOR of all non-null input values",
      ExprChecks.reductionAndGroupByAgg(
        TypeSig.integral, TypeSig.integral,
        Seq(ParamCheck("input", TypeSig.integral, TypeSig.integral))),
      (a, conf, p, r) => new AggExprMeta[BitXorAgg](a, conf, p, r) {
        override def convertToGpu(childExprs: Seq[Expression]): GpuExpression =
          GpuBitXorAgg(childExprs.head)

        override def needsAnsiCheck: Boolean = false
      }),
    expr[Coalesce] (
      "Returns the first non-null argument if exists. Otherwise, null",
      ExprChecks.projectOnly(
        (gpuCommonTypes + TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.BINARY +
          TypeSig.MAP + GpuTypeShims.additionalArithmeticSupportedTypes).nested(),
        TypeSig.all,
        repeatingParamCheck = Some(RepeatingParamCheck("param",
          (gpuCommonTypes + TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.BINARY +
            TypeSig.MAP + GpuTypeShims.additionalArithmeticSupportedTypes).nested(),
          TypeSig.all))),
      (a, conf, p, r) => new ExprMeta[Coalesce](a, conf, p, r) {
        // Allow foldable non-literal Coalesce (e.g. coalesce(cast(null as bigint), -1001)):
        // AQE can regenerate these after ConstantFolding ran; GpuCoalesce evaluates them on GPU.
        override val isFoldableNonLitAllowed: Boolean = true
        override def convertToGpuImpl(): GpuExpression =
          GpuCoalesce(childExprs.map(_.convertToGpu()))
      }),
    expr[Least] (
      "Returns the least value of all parameters, skipping null values",
      ExprChecks.projectOnly(
        TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128, TypeSig.orderable,
        repeatingParamCheck = Some(RepeatingParamCheck("param",
          TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128,
          TypeSig.orderable))),
      (a, conf, p, r) => new ExprMeta[Least](a, conf, p, r) {
        override def convertToGpuImpl(): GpuExpression = GpuLeast(childExprs.map(_.convertToGpu()))
      }),
    expr[Greatest] (
      "Returns the greatest value of all parameters, skipping null values",
      ExprChecks.projectOnly(
        TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128, TypeSig.orderable,
        repeatingParamCheck = Some(RepeatingParamCheck("param",
          TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128,
          TypeSig.orderable))),
      (a, conf, p, r) => new ExprMeta[Greatest](a, conf, p, r) {
        override def convertToGpuImpl(): GpuExpression =
          GpuGreatest(childExprs.map(_.convertToGpu()))
      }),
    expr[Atan](
      "Inverse tangent",
      ExprChecks.mathUnaryWithAst,
      (a, conf, p, r) => new UnaryAstExprMeta[Atan](a, conf, p, r) {
        override def convertToGpu(child: Expression): GpuExpression = GpuAtan(child)
      }),
    expr[Atanh](
      "Inverse hyperbolic tangent",
      ExprChecks.mathUnaryWithAst,
      (a, conf, p, r) => new UnaryAstExprMeta[Atanh](a, conf, p, r) {
        override def convertToGpu(child: Expression): GpuExpression = GpuAtanh(child)
      }),
    expr[Cos](
      "Cosine",
      ExprChecks.mathUnaryWithAst,
      (a, conf, p, r) => new UnaryAstExprMeta[Cos](a, conf, p, r) {
        override def convertToGpu(child: Expression): GpuExpression = GpuCos(child)
      }),
    expr[Exp](
      "Euler's number e raised to a power",
      ExprChecks.mathUnaryWithAst,
      (a, conf, p, r) => new UnaryAstExprMeta[Exp](a, conf, p, r) {
        override def convertToGpu(child: Expression): GpuExpression = GpuExp(child)
      }),
    expr[Expm1](
      "Euler's number e raised to a power minus 1",
      ExprChecks.mathUnaryWithAst,
      (a, conf, p, r) => new UnaryAstExprMeta[Expm1](a, conf, p, r) {
        override def convertToGpu(child: Expression): GpuExpression = GpuExpm1(child)
      }),
    expr[InitCap](
      "Returns str with the first letter of each word in uppercase. " +
      "All other letters are in lowercase",
      ExprChecks.unaryProjectInputMatchesOutput(TypeSig.STRING, TypeSig.STRING),
      (a, conf, p, r) => new UnaryExprMeta[InitCap](a, conf, p, r) {
        override def convertToGpu(child: Expression): GpuExpression = GpuInitCap(child)
      }).incompat(CASE_MODIFICATION_INCOMPAT),
    expr[Log](
      "Natural log",
      ExprChecks.mathUnary,
      (a, conf, p, r) => new UnaryExprMeta[Log](a, conf, p, r) {
        override def convertToGpu(child: Expression): GpuExpression = GpuLog(child)
      }),
    expr[Log1p](
      "Natural log 1 + expr",
      ExprChecks.mathUnary,
      (a, conf, p, r) => new UnaryExprMeta[Log1p](a, conf, p, r) {
        override def convertToGpu(child: Expression): GpuExpression = {
          // No need for overflow checking on the GpuAdd in Double as Double handles overflow
          // the same in all modes.
          GpuLog(GpuAdd(child, GpuLiteral(1d, DataTypes.DoubleType), false)())
        }
      }),
    expr[Log2](
      "Log base 2",
      ExprChecks.mathUnary,
      (a, conf, p, r) => new UnaryExprMeta[Log2](a, conf, p, r) {
        override def convertToGpu(child: Expression): GpuExpression =
          GpuLogarithm(child, GpuLiteral(2d, DataTypes.DoubleType))
      }),
    expr[Log10](
      "Log base 10",
      ExprChecks.mathUnary,
      (a, conf, p, r) => new UnaryExprMeta[Log10](a, conf, p, r) {
        override def convertToGpu(child: Expression): GpuExpression =
          GpuLogarithm(child, GpuLiteral(10d, DataTypes.DoubleType))
      }),
    expr[Logarithm](
      "Log variable base",
      ExprChecks.binaryProject(TypeSig.DOUBLE, TypeSig.DOUBLE,
        ("value", TypeSig.DOUBLE, TypeSig.DOUBLE),
        ("base", TypeSig.DOUBLE, TypeSig.DOUBLE)),
      (a, conf, p, r) => new BinaryExprMeta[Logarithm](a, conf, p, r) {
        override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
          // the order of the parameters is transposed intentionally
          GpuLogarithm(rhs, lhs)
      }),
    expr[Sin](
      "Sine",
      ExprChecks.mathUnaryWithAst,
      (a, conf, p, r) => new UnaryAstExprMeta[Sin](a, conf, p, r) {
        override def convertToGpu(child: Expression): GpuExpression = GpuSin(child)
      }),
    expr[Sinh](
      "Hyperbolic sine",
      ExprChecks.mathUnaryWithAst,
      (a, conf, p, r) => new UnaryAstExprMeta[Sinh](a, conf, p, r) {
        override def convertToGpu(child: Expression): GpuExpression = GpuSinh(child)
      }),
    expr[Cosh](
      "Hyperbolic cosine",
      ExprChecks.mathUnaryWithAst,
      (a, conf, p, r) => new UnaryAstExprMeta[Cosh](a, conf, p, r) {
        override def convertToGpu(child: Expression): GpuExpression = GpuCosh(child)
      }),
    expr[Cot](
      "Cotangent",
      ExprChecks.mathUnaryWithAst,
      (a, conf, p, r) => new UnaryAstExprMeta[Cot](a, conf, p, r) {
        override def convertToGpu(child: Expression): GpuExpression = GpuCot(child)
      }),
    expr[Tanh](
      "Hyperbolic tangent",
      ExprChecks.mathUnaryWithAst,
      (a, conf, p, r) => new UnaryAstExprMeta[Tanh](a, conf, p, r) {
        override def convertToGpu(child: Expression): GpuExpression = GpuTanh(child)
      }),
    expr[Tan](
      "Tangent",
      ExprChecks.mathUnaryWithAst,
      (a, conf, p, r) => new UnaryAstExprMeta[Tan](a, conf, p, r) {
        override def convertToGpu(child: Expression): GpuExpression = GpuTan(child)
      }),
    expr[NormalizeNaNAndZero](
      "Normalize NaN and zero",
      ExprChecks.unaryProjectInputMatchesOutput(
        TypeSig.DOUBLE + TypeSig.FLOAT,
        TypeSig.DOUBLE + TypeSig.FLOAT),
      (a, conf, p, r) => new UnaryExprMeta[NormalizeNaNAndZero](a, conf, p, r) {
        override def convertToGpu(child: Expression): GpuExpression =
          GpuNormalizeNaNAndZero(child)
      }),
    expr[KnownFloatingPointNormalized](
      "Tag to prevent redundant normalization",
      ExprChecks.unaryProjectInputMatchesOutput(TypeSig.all, TypeSig.all),
      (a, conf, p, r) => new UnaryExprMeta[KnownFloatingPointNormalized](a, conf, p, r) {
        override def convertToGpu(child: Expression): GpuExpression =
          GpuKnownFloatingPointNormalized(child)
      }),
    expr[KnownNotNull](
      "Tag an expression as known to not be null",
      ExprChecks.unaryProjectInputMatchesOutput(
        (TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.BINARY + TypeSig.CALENDAR +
          TypeSig.ARRAY + TypeSig.MAP + TypeSig.STRUCT).nested(), TypeSig.all),
      (k, conf, p, r) => new UnaryExprMeta[KnownNotNull](k, conf, p, r) {
        override def convertToGpu(child: Expression): GpuExpression =
          GpuKnownNotNull(child)
      }),
    expr[DateDiff](
      "Returns the number of days from startDate to endDate",
      ExprChecks.binaryProject(TypeSig.INT, TypeSig.INT,
        ("lhs", TypeSig.DATE, TypeSig.DATE),
        ("rhs", TypeSig.DATE, TypeSig.DATE)),
      (a, conf, p, r) => new BinaryExprMeta[DateDiff](a, conf, p, r) {
        override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression = {
          GpuDateDiff(lhs, rhs)
        }
    }),
    // TimeAdd moved to TimeAddShims to handle API differences across Spark versions
    expr[DateAddInterval](
      "Adds interval to date",
      ExprChecks.binaryProject(TypeSig.DATE, TypeSig.DATE,
        ("start", TypeSig.DATE, TypeSig.DATE),
        ("interval", TypeSig.lit(TypeEnum.CALENDAR)
          .withPsNote(TypeEnum.CALENDAR, "month intervals are not supported"),
          TypeSig.CALENDAR)),
      (dateAddInterval, conf, p, r) =>
        new BinaryExprMeta[DateAddInterval](dateAddInterval, conf, p, r) {
          override def tagExprForGpu(): Unit = {
            GpuOverrides.extractLit(dateAddInterval.interval).foreach { lit =>
              val intvl = lit.value.asInstanceOf[CalendarInterval]
              if (intvl.months != 0) {
                willNotWorkOnGpu("interval months isn't supported")
              }
            }
          }

          override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
            GpuDateAddInterval(lhs, rhs)
        }),
    expr[DateFormatClass](
      "Converts timestamp to a value of string in the format specified by the date format",
      ExprChecks.binaryProject(TypeSig.STRING, TypeSig.STRING,
        ("timestamp", TypeSig.TIMESTAMP, TypeSig.TIMESTAMP),
        ("strfmt", TypeSig.lit(TypeEnum.STRING)
            .withPsNote(TypeEnum.STRING, "A limited number of formats are supported"),
            TypeSig.STRING)),
      (a, conf, p, r) => new UnixTimeExprMeta[DateFormatClass](a, conf, p, r) {
        override def isTimeZoneSupported = true
        override protected def allowLegacyFormattingOnlyFormats: Boolean = true
        override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
          GpuDateFormatClass(lhs, rhs, strfFormat, a.timeZoneId)
      }
    ),
    expr[ToUnixTimestamp](
      "Returns the UNIX timestamp of the given time",
      ExprChecks.binaryProject(TypeSig.LONG, TypeSig.LONG,
        ("timeExp",
            TypeSig.STRING + TypeSig.DATE + TypeSig.TIMESTAMP,
            TypeSig.STRING + TypeSig.DATE + TypeSig.TIMESTAMP),
        ("format", TypeSig.lit(TypeEnum.STRING)
            .withPsNote(TypeEnum.STRING, "A limited number of formats are supported"),
            TypeSig.STRING)),
      (a, conf, p, r) => new UnixTimeExprMeta[ToUnixTimestamp](a, conf, p, r) {
        // String type is not supported yet for non-UTC timezone.
        override def isTimeZoneSupported = true
        override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression = {
          GpuToUnixTimestamp(lhs, rhs, sparkFormat, strfFormat, a.timeZoneId)
        }
      }),
    expr[UnixTimestamp](
      "Returns the UNIX timestamp of current or specified time",
      ExprChecks.binaryProject(TypeSig.LONG, TypeSig.LONG,
        ("timeExp",
            TypeSig.STRING + TypeSig.DATE + TypeSig.TIMESTAMP,
            TypeSig.STRING + TypeSig.DATE + TypeSig.TIMESTAMP),
        ("format", TypeSig.lit(TypeEnum.STRING)
            .withPsNote(TypeEnum.STRING, "A limited number of formats are supported"),
            TypeSig.STRING)),
      (a, conf, p, r) => new UnixTimeExprMeta[UnixTimestamp](a, conf, p, r) {
        override def isTimeZoneSupported = true
        override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression = {
          GpuUnixTimestamp(lhs, rhs, sparkFormat, strfFormat, a.timeZoneId)
        }
      }),
    expr[Hour](
      "Returns the hour component of the string/timestamp",
      ExprChecks.unaryProject(TypeSig.INT, TypeSig.INT,
        TypeSig.TIMESTAMP, TypeSig.TIMESTAMP),
      (hour, conf, p, r) => new UnaryExprMeta[Hour](hour, conf, p, r) {
        override def isTimeZoneSupported = true
        override def convertToGpu(expr: Expression): GpuExpression = GpuHour(expr, hour.timeZoneId)
      }),
    expr[Minute](
      "Returns the minute component of the string/timestamp",
      ExprChecks.unaryProject(TypeSig.INT, TypeSig.INT,
        TypeSig.TIMESTAMP, TypeSig.TIMESTAMP),
      (minute, conf, p, r) => new UnaryExprMeta[Minute](minute, conf, p, r) {
        override def isTimeZoneSupported = true
        override def convertToGpu(expr: Expression): GpuExpression =
          GpuMinute(expr, minute.timeZoneId)
      }),
    expr[Second](
      "Returns the second component of the string/timestamp",
      ExprChecks.unaryProject(TypeSig.INT, TypeSig.INT,
        TypeSig.TIMESTAMP, TypeSig.TIMESTAMP),
      (second, conf, p, r) => new UnaryExprMeta[Second](second, conf, p, r) {
        override def isTimeZoneSupported = true
        override def convertToGpu(expr: Expression): GpuExpression =
          GpuSecond(expr, second.timeZoneId)
      }),
    expr[WeekDay](
      "Returns the day of the week (0 = Monday...6=Sunday)",
      ExprChecks.unaryProject(TypeSig.INT, TypeSig.INT,
        TypeSig.DATE, TypeSig.DATE),
      (a, conf, p, r) => new UnaryExprMeta[WeekDay](a, conf, p, r) {
        override def convertToGpu(expr: Expression): GpuExpression =
          GpuWeekDay(expr)
      }),
    expr[DayOfWeek](
      "Returns the day of the week (1 = Sunday...7=Saturday)",
      ExprChecks.unaryProject(TypeSig.INT, TypeSig.INT,
        TypeSig.DATE, TypeSig.DATE),
      (a, conf, p, r) => new UnaryExprMeta[DayOfWeek](a, conf, p, r) {
        override def convertToGpu(expr: Expression): GpuExpression =
          GpuDayOfWeek(expr)
      }),
    expr[LastDay](
      "Returns the last day of the month which the date belongs to",
      ExprChecks.unaryProjectInputMatchesOutput(TypeSig.DATE, TypeSig.DATE),
      (a, conf, p, r) => new UnaryExprMeta[LastDay](a, conf, p, r) {
        override def convertToGpu(expr: Expression): GpuExpression =
          GpuLastDay(expr)
      }),
    expr[FromUnixTime](
      "Get the string from a unix timestamp",
      ExprChecks.binaryProject(TypeSig.STRING, TypeSig.STRING,
        ("sec", TypeSig.LONG, TypeSig.LONG),
        ("format", TypeSig.lit(TypeEnum.STRING)
            .withPsNote(TypeEnum.STRING, "Only a limited number of formats are supported"),
            TypeSig.STRING)),
      (a, conf, p, r) => new FromUnixTimeMeta(a ,conf ,p ,r)),
    expr[FromUTCTimestamp](
      "Render the input UTC timestamp in the input timezone",
      ExprChecks.binaryProject(TypeSig.TIMESTAMP, TypeSig.TIMESTAMP,
        ("timestamp", TypeSig.TIMESTAMP, TypeSig.TIMESTAMP),
        ("timezone", TypeSig.lit(TypeEnum.STRING)
          .withPsNote(TypeEnum.STRING,
            "Only non-DST(Daylight Savings Time) timezones are supported"),
          TypeSig.lit(TypeEnum.STRING))),
      (a, conf, p, r) => new FromUTCTimestampExprMeta(a, conf, p, r)
    ),
    expr[ToUTCTimestamp](
      "Render the input timestamp in UTC",
      ExprChecks.binaryProject(TypeSig.TIMESTAMP, TypeSig.TIMESTAMP,
        ("timestamp", TypeSig.TIMESTAMP, TypeSig.TIMESTAMP),
        ("timezone", TypeSig.lit(TypeEnum.STRING)
          .withPsNote(TypeEnum.STRING,
            "Only non-DST(Daylight Savings Time) timezones are supported"),
          TypeSig.lit(TypeEnum.STRING))),
      (a, conf, p, r) => new ToUTCTimestampExprMeta(a, conf, p, r)
    ),
    expr[MonthsBetween](
      "If `timestamp1` is later than `timestamp2`, then the result " +
        "is positive. If `timestamp1` and `timestamp2` are on the same day of month, or both " +
        "are the last day of month, time of day will be ignored. Otherwise, the difference is " +
        "calculated based on 31 days per month, and rounded to 8 digits unless roundOff=false.",
      ExprChecks.projectOnly(TypeSig.DOUBLE, TypeSig.DOUBLE,
        Seq(ParamCheck("timestamp1", TypeSig.TIMESTAMP, TypeSig.TIMESTAMP),
          ParamCheck("timestamp2", TypeSig.TIMESTAMP, TypeSig.TIMESTAMP),
          ParamCheck("round", TypeSig.lit(TypeEnum.BOOLEAN), TypeSig.BOOLEAN))),
      (a, conf, p, r) => new MonthsBetweenExprMeta(a, conf, p, r)
    ),
    expr[TruncDate](
      "Truncate the date to the unit specified by the given string format",
      ExprChecks.binaryProject(TypeSig.DATE, TypeSig.DATE,
        ("date", TypeSig.DATE, TypeSig.DATE),
        ("format", TypeSig.STRING, TypeSig.STRING)),
      (a, conf, p, r) => new TruncDateExprMeta(a, conf, p, r)
    ),
    expr[TruncTimestamp](
      "Truncate the timestamp to the unit specified by the given string format",
      ExprChecks.binaryProject(TypeSig.TIMESTAMP, TypeSig.TIMESTAMP,
        ("format", TypeSig.STRING, TypeSig.STRING),
        ("date", TypeSig.TIMESTAMP, TypeSig.TIMESTAMP)),
      (a, conf, p, r) => new TruncTimestampExprMeta(a, conf, p, r)
    ),
    expr[Pmod](
      "Pmod",
      // Decimal support disabled https://github.com/NVIDIA/spark-rapids/issues/7553
      ExprChecks.binaryProject(TypeSig.integral + TypeSig.fp, TypeSig.cpuNumeric,
        ("lhs", (TypeSig.integral + TypeSig.fp).withPsNote(TypeEnum.DECIMAL,
          s"decimals with precision ${DecimalType.MAX_PRECISION} are not supported"),
            TypeSig.cpuNumeric),
        ("rhs", (TypeSig.integral + TypeSig.fp), TypeSig.cpuNumeric)),
      (a, conf, p, r) => new BinaryExprMeta[Pmod](a, conf, p, r) {
        override def tagExprForGpu(): Unit = {
          a.dataType match {
            case dt: DecimalType if dt.precision == DecimalType.MAX_PRECISION =>
              willNotWorkOnGpu("pmod at maximum decimal precision is not supported")
            case _ =>
          }
        }
        override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
          GpuPmod(lhs, rhs)
      }),
    expr[Add](
      "Addition",
      ExprChecks.binaryProjectAndAst(
        TypeSig.implicitCastsAstTypes,
        TypeSig.gpuNumeric + GpuTypeShims.additionalArithmeticSupportedTypes,
        TypeSig.numericAndInterval,
        ("lhs", TypeSig.gpuNumeric + GpuTypeShims.additionalArithmeticSupportedTypes,
            TypeSig.numericAndInterval),
        ("rhs", TypeSig.gpuNumeric + GpuTypeShims.additionalArithmeticSupportedTypes,
            TypeSig.numericAndInterval)),
      (a, conf, p, r) => new BinaryAstExprMeta[Add](a, conf, p, r) {
        private val ansiEnabled = SQLConf.get.ansiEnabled

        override def tagExprForGpu(): Unit = {
          // Check if this Add expression is in TRY mode context
          if (TryModeShim.isTryMode(a)) {
            willNotWorkOnGpu("try_add is not supported on GPU")
          }
        }

        override def tagSelfForAst(): Unit = {
          if (ansiEnabled && GpuAnsi.needBasicOpOverflowCheck(a.dataType)) {
            willNotWorkInAst("AST Addition does not support ANSI mode.")
          }
        }

        override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
          GpuAdd(lhs, rhs, ansiEnabled)(a.origin)
      }),
    expr[Subtract](
      "Subtraction",
      ExprChecks.binaryProjectAndAst(
        TypeSig.implicitCastsAstTypes,
        TypeSig.gpuNumeric + GpuTypeShims.additionalArithmeticSupportedTypes,
        TypeSig.numericAndInterval,
        ("lhs", TypeSig.gpuNumeric + GpuTypeShims.additionalArithmeticSupportedTypes,
            TypeSig.numericAndInterval),
        ("rhs", TypeSig.gpuNumeric + GpuTypeShims.additionalArithmeticSupportedTypes,
            TypeSig.numericAndInterval)),
      (a, conf, p, r) => new BinaryAstExprMeta[Subtract](a, conf, p, r) {
        private val ansiEnabled = SQLConf.get.ansiEnabled

        override def tagExprForGpu(): Unit = {
          // Check if this Subtract expression is in TRY mode context
          if (TryModeShim.isTryMode(a)) {
            willNotWorkOnGpu("try_subtract is not supported on GPU")
          }
        }

        override def tagSelfForAst(): Unit = {
          if (ansiEnabled && GpuAnsi.needBasicOpOverflowCheck(a.dataType)) {
            willNotWorkInAst("AST Subtraction does not support ANSI mode.")
          }
        }

        override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
          GpuSubtract(lhs, rhs, ansiEnabled)(a.origin)
      }),
    expr[And](
      "Logical AND",
      ExprChecks.binaryProjectAndAst(TypeSig.BOOLEAN, TypeSig.BOOLEAN, TypeSig.BOOLEAN,
        ("lhs", TypeSig.BOOLEAN, TypeSig.BOOLEAN),
        ("rhs", TypeSig.BOOLEAN, TypeSig.BOOLEAN)),
      (a, conf, p, r) => new BinaryExprMeta[And](a, conf, p, r) {
        override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
          GpuAnd(lhs, rhs)
      }),
    expr[Or](
      "Logical OR",
      ExprChecks.binaryProjectAndAst(TypeSig.BOOLEAN, TypeSig.BOOLEAN, TypeSig.BOOLEAN,
        ("lhs", TypeSig.BOOLEAN, TypeSig.BOOLEAN),
        ("rhs", TypeSig.BOOLEAN, TypeSig.BOOLEAN)),
      (a, conf, p, r) => new BinaryExprMeta[Or](a, conf, p, r) {
        override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
          GpuOr(lhs, rhs)
      }),
    expr[EqualNullSafe](
      "Check if the values are equal including nulls <=>",
      ExprChecks.binaryProject(
        TypeSig.BOOLEAN, TypeSig.BOOLEAN,
        ("lhs", (TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128 +
            GpuTypeShims.additionalPredicateSupportedTypes + TypeSig.STRUCT).nested(),
            TypeSig.comparable),
        ("rhs", (TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128 +
            GpuTypeShims.additionalPredicateSupportedTypes + TypeSig.STRUCT).nested(),
            TypeSig.comparable)),
      (a, conf, p, r) => new BinaryExprMeta[EqualNullSafe](a, conf, p, r) {
        override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
          GpuEqualNullSafe(lhs, rhs)
      }),
    expr[EqualTo](
      "Check if the values are equal",
      ExprChecks.binaryProjectAndAst(
        TypeSig.comparisonAstTypes,
        TypeSig.BOOLEAN, TypeSig.BOOLEAN,
        ("lhs", (TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128 +
            GpuTypeShims.additionalPredicateSupportedTypes + TypeSig.STRUCT).nested(),
            TypeSig.comparable),
        ("rhs", (TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128 +
            GpuTypeShims.additionalPredicateSupportedTypes + TypeSig.STRUCT).nested(),
            TypeSig.comparable)),
      (a, conf, p, r) => new BinaryAstExprMeta[EqualTo](a, conf, p, r) {
        override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
          GpuEqualTo(lhs, rhs)
      }),
    expr[GreaterThan](
      "> operator",
      ExprChecks.binaryProjectAndAst(
        TypeSig.comparisonAstTypes,
        TypeSig.BOOLEAN, TypeSig.BOOLEAN,
        ("lhs", (TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128 +
            GpuTypeShims.additionalPredicateSupportedTypes + TypeSig.STRUCT).nested(),
            TypeSig.orderable),
        ("rhs", (TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128 +
            GpuTypeShims.additionalPredicateSupportedTypes + TypeSig.STRUCT).nested(),
            TypeSig.orderable)),
      (a, conf, p, r) => new BinaryAstExprMeta[GreaterThan](a, conf, p, r) {
        override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
          GpuStringInstr.optimizeContains(GpuGreaterThan(lhs, rhs))
      }),
    expr[GreaterThanOrEqual](
      ">= operator",
      ExprChecks.binaryProjectAndAst(
        TypeSig.comparisonAstTypes,
        TypeSig.BOOLEAN, TypeSig.BOOLEAN,
        ("lhs", (TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128 +
            GpuTypeShims.additionalPredicateSupportedTypes + TypeSig.STRUCT).nested(),
            TypeSig.orderable),
        ("rhs", (TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128 +
            GpuTypeShims.additionalPredicateSupportedTypes + TypeSig.STRUCT).nested(),
            TypeSig.orderable)),
      (a, conf, p, r) => new BinaryAstExprMeta[GreaterThanOrEqual](a, conf, p, r) {
        override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
          GpuStringInstr.optimizeContains(GpuGreaterThanOrEqual(lhs, rhs))
      }),
    expr[In](
      "IN operator",
      ExprChecks.projectOnly(TypeSig.BOOLEAN, TypeSig.BOOLEAN,
        Seq(ParamCheck("value", TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128,
          TypeSig.comparable)),
        Some(RepeatingParamCheck("list",
          (TypeSig.commonCudfTypes + TypeSig.DECIMAL_128).withAllLit(),
          TypeSig.comparable))),
      (in, conf, p, r) => new ExprMeta[In](in, conf, p, r) {
        override def convertToGpuImpl(): GpuExpression =
          GpuInSet(childExprs.head.convertToGpu(), in.list.asInstanceOf[Seq[Literal]].map(_.value))
      }),
    expr[InSet](
      "INSET operator",
      ExprChecks.unaryProject(TypeSig.BOOLEAN, TypeSig.BOOLEAN,
        TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128, TypeSig.comparable),
      (in, conf, p, r) => new ExprMeta[InSet](in, conf, p, r) {
        override def convertToGpuImpl(): GpuExpression =
          GpuInSet(childExprs.head.convertToGpu(), in.hset.toSeq)
      }),
    expr[LessThan](
      "< operator",
      ExprChecks.binaryProjectAndAst(
        TypeSig.comparisonAstTypes,
        TypeSig.BOOLEAN, TypeSig.BOOLEAN,
        ("lhs", (TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128 +
            GpuTypeShims.additionalPredicateSupportedTypes + TypeSig.STRUCT).nested(),
            TypeSig.orderable),
        ("rhs", (TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128 +
            GpuTypeShims.additionalPredicateSupportedTypes + TypeSig.STRUCT).nested(),
            TypeSig.orderable)),
      (a, conf, p, r) => new BinaryAstExprMeta[LessThan](a, conf, p, r) {
        override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
          GpuStringInstr.optimizeContains(GpuLessThan(lhs, rhs))
      }),
    expr[LessThanOrEqual](
      "<= operator",
      ExprChecks.binaryProjectAndAst(
        TypeSig.comparisonAstTypes,
        TypeSig.BOOLEAN, TypeSig.BOOLEAN,
        ("lhs", (TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128 +
            GpuTypeShims.additionalPredicateSupportedTypes + TypeSig.STRUCT).nested(),
            TypeSig.orderable),
        ("rhs", (TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128 +
            GpuTypeShims.additionalPredicateSupportedTypes + TypeSig.STRUCT).nested(),
            TypeSig.orderable)),
      (a, conf, p, r) => new BinaryAstExprMeta[LessThanOrEqual](a, conf, p, r) {
        override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
          GpuStringInstr.optimizeContains(GpuLessThanOrEqual(lhs, rhs))
      }),
    expr[CaseWhen](
      "CASE WHEN expression",
      CaseWhenCheck,
      (a, conf, p, r) => new ExprMeta[CaseWhen](a, conf, p, r) {
        override def convertToGpuImpl(): GpuExpression = {
          val branches = childExprs.grouped(2).flatMap {
            case Seq(cond, value) => Some((cond.convertToGpu(), value.convertToGpu()))
            case Seq(_) => None
          }.toArray.toSeq  // force materialization to make the seq serializable
          val elseValue = if (childExprs.size % 2 != 0) {
            Some(childExprs.last.convertToGpu())
          } else {
            None
          }
          GpuCaseWhen(branches, elseValue, this.conf.caseWhenFuseEnabled)
        }
      }),
    expr[If](
      "IF expression",
      ExprChecks.projectOnly(
        (gpuCommonTypes + TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.MAP +
            TypeSig.BINARY + GpuTypeShims.additionalCommonOperatorSupportedTypes).nested(),
        TypeSig.all,
        Seq(ParamCheck("predicate", TypeSig.BOOLEAN, TypeSig.BOOLEAN),
          ParamCheck("trueValue",
            (gpuCommonTypes + TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.MAP +
                TypeSig.BINARY + GpuTypeShims.additionalCommonOperatorSupportedTypes).nested(),
            TypeSig.all),
          ParamCheck("falseValue",
            (gpuCommonTypes + TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.MAP +
                TypeSig.BINARY + GpuTypeShims.additionalCommonOperatorSupportedTypes).nested(),
            TypeSig.all))),
      (a, conf, p, r) => new ExprMeta[If](a, conf, p, r) {
        override def convertToGpuImpl(): GpuExpression = {
          val Seq(boolExpr, trueExpr, falseExpr) = childExprs.map(_.convertToGpu())
          GpuIf(boolExpr, trueExpr, falseExpr)
        }
      }),
    expr[Pow](
      "lhs ^ rhs",
      ExprChecks.binaryProjectAndAst(
        TypeSig.implicitCastsAstTypes, TypeSig.DOUBLE, TypeSig.DOUBLE,
        ("lhs", TypeSig.DOUBLE, TypeSig.DOUBLE),
        ("rhs", TypeSig.DOUBLE, TypeSig.DOUBLE)),
      (a, conf, p, r) => new BinaryAstExprMeta[Pow](a, conf, p, r) {
        override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
          GpuPow(lhs, rhs)
      }),
    expr[AggregateExpression](
      "Aggregate expression",
      // Let the underlying expression checks decide whether this can be on the GPU.
      ExprChecks.fullAgg(
        TypeSig.all,
        TypeSig.all,
        Seq(ParamCheck("aggFunc", TypeSig.all, TypeSig.all)),
        Some(RepeatingParamCheck("filter", TypeSig.BOOLEAN, TypeSig.BOOLEAN))),
      (a, conf, p, r) => new ExprMeta[AggregateExpression](a, conf, p, r) {
        private val filter: Option[BaseExprMeta[_]] =
          a.filter.map(GpuOverrides.wrapExpr(_, this.conf, Some(this)))
        private val childrenExprMeta: Seq[BaseExprMeta[Expression]] =
          a.children.map(GpuOverrides.wrapExpr(_, this.conf, Some(this)))
        override val childExprs: Seq[BaseExprMeta[_]] =
          childrenExprMeta ++ filter.toSeq

        override def convertToGpuImpl(): GpuExpression = {
          // handle the case AggregateExpression has the resultIds parameter where its
          // Seq[ExprIds] instead of single ExprId.
          val resultId = try {
            val resultMethod = a.getClass.getMethod("resultId")
            resultMethod.invoke(a).asInstanceOf[ExprId]
          } catch {
            case _: Exception =>
              val resultMethod = a.getClass.getMethod("resultIds")
              resultMethod.invoke(a).asInstanceOf[Seq[ExprId]].head
          }
          GpuAggregateExpression(childExprs.head.convertToGpu().asInstanceOf[GpuAggregateFunction],
            a.mode, a.isDistinct, filter.map(_.convertToGpu()), resultId)
        }
      }),
    expr[SortOrder](
      "Sort order",
      ExprChecks.projectOnly(
        pluginSupportedOrderableSig + TypeSig.ARRAY.nested(gpuCommonTypes)
            .withPsNote(TypeEnum.ARRAY, "STRUCT is not supported as a child type for ARRAY"),
        TypeSig.orderable,
        Seq(ParamCheck(
          "input",
          pluginSupportedOrderableSig + TypeSig.ARRAY.nested(gpuCommonTypes)
             .withPsNote(TypeEnum.ARRAY, "STRUCT is not supported as a child type for ARRAY"),
          TypeSig.orderable))),
      GpuSortOrderMeta),
    expr[PivotFirst](
      "PivotFirst operator",
      ExprChecks.reductionAndGroupByAgg(
        TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128 +
          TypeSig.ARRAY.nested(TypeSig.commonCudfTypes + TypeSig.DECIMAL_128),
        TypeSig.all,
        Seq(ParamCheck(
          "pivotColumn",
          (TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128),
          TypeSig.all),
          ParamCheck("valueColumn",
          TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128,
          TypeSig.all))),
      (pivot, conf, p, r) => new ImperativeAggExprMeta[PivotFirst](pivot, conf, p, r) {
        override def tagAggForGpu(): Unit = {
          // If pivotColumnValues doesn't have distinct values, fall back to CPU
          if (pivot.pivotColumnValues.distinct.lengthCompare(pivot.pivotColumnValues.length) != 0) {
            willNotWorkOnGpu("PivotFirst does not work on the GPU when there are duplicate" +
                " pivot values provided")
          }
        }
        override def convertToGpu(childExprs: Seq[Expression]): GpuExpression = {
          val Seq(pivotColumn, valueColumn) = childExprs
          GpuPivotFirst(pivotColumn, valueColumn, pivot.pivotColumnValues)
        }

        // Pivot does not overflow, so it doesn't need the ANSI check
        override val needsAnsiCheck: Boolean = false
      }),
    expr[Count](
      "Count aggregate operator",
      ExprChecks.fullAgg(
        TypeSig.LONG, TypeSig.LONG,
        repeatingParamCheck = Some(RepeatingParamCheck(
          "input", TypeSig.all, TypeSig.all))),
      (count, conf, p, r) => new AggExprMeta[Count](count, conf, p, r) {

        // Spark Count agg returns Long and does not check Ansi mode and overflow
        override def needsAnsiCheck: Boolean = false

        override def tagAggForGpu(): Unit = {
          if (count.children.size > 1) {
            willNotWorkOnGpu("count of multiple columns not supported")
          }
        }
        override def convertToGpu(childExprs: Seq[Expression]): GpuExpression =
          GpuCount(childExprs)
      }),
    expr[Max](
      "Max aggregate operator",
      ExprChecksImpl(
        ExprChecks.reductionAndGroupByAgg(
          (TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL + TypeSig.STRUCT +
            TypeSig.ARRAY).nested(),
          TypeSig.orderable,
          Seq(ParamCheck("input",
            (TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL + TypeSig.STRUCT +
              TypeSig.ARRAY).nested(),
            TypeSig.orderable))).asInstanceOf[ExprChecksImpl].contexts
          ++
          ExprChecks.windowOnly(
            (TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL),
            TypeSig.orderable,
            Seq(ParamCheck("input",
              (TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL),
              TypeSig.orderable))).asInstanceOf[ExprChecksImpl].contexts),
      (max, conf, p, r) => new AggExprMeta[Max](max, conf, p, r) {
        override def convertToGpu(childExprs: Seq[Expression]): GpuExpression =
          GpuMax(childExprs.head)

        // Max does not overflow, so it doesn't need the ANSI check
        override val needsAnsiCheck: Boolean = false
      }),
    expr[Min](
      "Min aggregate operator",
      ExprChecksImpl(
        ExprChecks.reductionAndGroupByAgg(
          (TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL + TypeSig.STRUCT +
              TypeSig.ARRAY).nested(),
          TypeSig.orderable,
          Seq(ParamCheck("input",
            (TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL + TypeSig.STRUCT +
              TypeSig.ARRAY).nested(),
            TypeSig.orderable))).asInstanceOf[ExprChecksImpl].contexts
          ++
          ExprChecks.windowOnly(
            (TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL),
            TypeSig.orderable,
            Seq(ParamCheck("input",
              (TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL),
              TypeSig.orderable))).asInstanceOf[ExprChecksImpl].contexts),
      (a, conf, p, r) => new AggExprMeta[Min](a, conf, p, r) {
        override def convertToGpu(childExprs: Seq[Expression]): GpuExpression =
          GpuMin(childExprs.head)

        // Min does not overflow, so it doesn't need the ANSI check
        override val needsAnsiCheck: Boolean = false
      }),
    expr[Sum](
      "Sum aggregate operator",
      ExprChecks.fullAgg(
        TypeSig.LONG + TypeSig.DOUBLE + TypeSig.DECIMAL_128,
        TypeSig.LONG + TypeSig.DOUBLE + TypeSig.DECIMAL_128,
        Seq(ParamCheck("input", TypeSig.gpuNumeric, TypeSig.cpuNumeric))),
      (a, conf, p, r) => new AggExprMeta[Sum](a, conf, p, r) {
        override def tagAggForGpu(): Unit = {
          val inputDataType = a.child.dataType
          checkAndTagFloatAgg(inputDataType, this.conf, this)

          // Check if this Sum expression is in TRY mode context
          if (TryModeShim.isTryMode(a)) {
            willNotWorkOnGpu("try_sum is not supported on GPU")
          }
        }

        override def needsAnsiCheck: Boolean = false

        override def convertToGpu(childExprs: Seq[Expression]): GpuExpression =
          GpuSum(childExprs.head, a.dataType)
      }),
    expr[NthValue](
      "nth window operator",
      ExprChecks.windowOnly(
        (TypeSig.STRUCT + TypeSig.ARRAY + TypeSig.MAP + TypeSig.BINARY +
            TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128).nested(),
        TypeSig.all,
        Seq(ParamCheck("input",
          (TypeSig.STRUCT + TypeSig.ARRAY + TypeSig.MAP + TypeSig.BINARY +
              TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128).nested(),
          TypeSig.all),
          ParamCheck("offset", TypeSig.lit(TypeEnum.INT), TypeSig.lit(TypeEnum.INT)))
      ),
      (a, conf, p, r) => new AggExprMeta[NthValue](a, conf, p, r) {
        override def convertToGpu(childExprs: Seq[Expression]): GpuExpression =
          GpuNthValue(childExprs.head, a.offset, a.ignoreNulls)

        // nth does not overflow, so it doesn't need the ANSI check
        override val needsAnsiCheck: Boolean = false
      }),
    expr[First](
      "first aggregate operator",
      ExprChecks.fullAgg(
        (TypeSig.STRUCT + TypeSig.ARRAY + TypeSig.MAP + TypeSig.BINARY +
            TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128).nested(),
        TypeSig.all,
        Seq(ParamCheck("input",
          (TypeSig.STRUCT + TypeSig.ARRAY + TypeSig.MAP + TypeSig.BINARY +
              TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128).nested(),
          TypeSig.all))
      ),
      (a, conf, p, r) => new AggExprMeta[First](a, conf, p, r) {
        override def convertToGpu(childExprs: Seq[Expression]): GpuExpression =
          GpuFirst(childExprs.head, a.ignoreNulls)

        // First does not overflow, so it doesn't need the ANSI check
        override val needsAnsiCheck: Boolean = false
      }),
    expr[Last](
    "last aggregate operator",
      ExprChecks.fullAgg(
        (TypeSig.STRUCT + TypeSig.ARRAY + TypeSig.MAP + TypeSig.BINARY +
            TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128).nested(),
        TypeSig.all,
        Seq(ParamCheck("input",
          (TypeSig.STRUCT + TypeSig.ARRAY + TypeSig.MAP + TypeSig.BINARY +
              TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128).nested(),
          TypeSig.all))
      ),
      (a, conf, p, r) => new AggExprMeta[Last](a, conf, p, r) {
        override def convertToGpu(childExprs: Seq[Expression]): GpuExpression =
          GpuLast(childExprs.head, a.ignoreNulls)

        // Last does not overflow, so it doesn't need the ANSI check
        override val needsAnsiCheck: Boolean = false
      }),
    expr[MaxBy](
      "MaxBy aggregate operator. It may produce different results than CPU when " +
        "multiple rows in a group have same minimum value in the ordering column and " +
        "different associated values in the value column.",
      ExprChecks.reductionAndGroupByAgg(
        (TypeSig.STRUCT + TypeSig.ARRAY + TypeSig.MAP + TypeSig.BINARY +
          TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128).nested(),
        TypeSig.all,
        Seq(
          ParamCheck("value", (TypeSig.STRUCT + TypeSig.ARRAY + TypeSig.MAP + TypeSig.BINARY
            + TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128).nested(),
            TypeSig.all),
          ParamCheck("ordering", (TypeSig.commonCudfTypes - TypeSig.fp + TypeSig.DECIMAL_128 +
            TypeSig.NULL + TypeSig.STRUCT + TypeSig.ARRAY).nested(
              TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 +
              TypeSig.NULL + TypeSig.STRUCT + TypeSig.ARRAY),
            TypeSig.orderable))
      ),
      (maxBy, conf, p, r) => new AggExprMeta[MaxBy](maxBy, conf, p, r) {

        override def convertToGpu(childExprs: Seq[Expression]): GpuExpression = {
          // Only two children (value expression, ordering expression)
          require(childExprs.length == 2)
          GpuMaxBy(childExprs.head, childExprs.last)
        }

        // MaxBy does not overflow, so it doesn't need the ANSI check
        override val needsAnsiCheck: Boolean = false
      }),
    expr[MinBy](
      "MinBy aggregate operator. It may produce different results than CPU when " +
        "multiple rows in a group have same minimum value in the ordering column and " +
        "different associated values in the value column.",
      ExprChecks.reductionAndGroupByAgg(
        (TypeSig.STRUCT + TypeSig.ARRAY + TypeSig.MAP + TypeSig.BINARY +
          TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128).nested(),
        TypeSig.all,
        Seq(
          ParamCheck("value", (TypeSig.STRUCT + TypeSig.ARRAY + TypeSig.MAP + TypeSig.BINARY
            + TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128).nested(),
            TypeSig.all),
          ParamCheck("ordering", (TypeSig.commonCudfTypes - TypeSig.fp + TypeSig.DECIMAL_128 +
            TypeSig.NULL + TypeSig.STRUCT + TypeSig.ARRAY).nested(
              TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 +
              TypeSig.NULL + TypeSig.STRUCT + TypeSig.ARRAY),
            TypeSig.orderable))
      ),
      (minBy, conf, p, r) => new AggExprMeta[MinBy](minBy, conf, p, r) {

        override def convertToGpu(childExprs: Seq[Expression]): GpuExpression = {
          // Only two children (value expression, ordering expression)
          require(childExprs.length == 2)
          GpuMinBy(childExprs.head, childExprs.last)
        }

        // MinBy does not overflow, so it doesn't need the ANSI check
        override val needsAnsiCheck: Boolean = false
      }),
    expr[BRound](
      "Round an expression to d decimal places using HALF_EVEN rounding mode",
      ExprChecks.binaryProject(
        TypeSig.gpuNumeric, TypeSig.cpuNumeric,
        ("value", TypeSig.gpuNumeric +
            TypeSig.psNote(TypeEnum.FLOAT, "result may round slightly differently") +
            TypeSig.psNote(TypeEnum.DOUBLE, "result may round slightly differently"),
            TypeSig.cpuNumeric),
        ("scale", TypeSig.lit(TypeEnum.INT), TypeSig.lit(TypeEnum.INT))),
      (a, conf, p, r) => new GpuBRoundMeta(a, conf, p, r) {
        override def tagExprForGpu(): Unit = {
          a.child.dataType match {
            case FloatType | DoubleType if !this.conf.isIncompatEnabled =>
              willNotWorkOnGpu("rounding floating point numbers may be slightly off " +
                  s"compared to Spark's result, to enable set ${RapidsConf.INCOMPATIBLE_OPS}")
            case _ => // NOOP
          }
        }
      }),
    expr[Round](
      "Round an expression to d decimal places using HALF_UP rounding mode",
      ExprChecks.binaryProject(
        TypeSig.gpuNumeric, TypeSig.cpuNumeric,
        ("value", TypeSig.gpuNumeric +
            TypeSig.psNote(TypeEnum.FLOAT, "result may round slightly differently") +
            TypeSig.psNote(TypeEnum.DOUBLE, "result may round slightly differently"),
            TypeSig.cpuNumeric),
        ("scale", TypeSig.lit(TypeEnum.INT), TypeSig.lit(TypeEnum.INT))),
      (a, conf, p, r) => new GpuRoundMeta(a, conf, p, r) {
        override def tagExprForGpu(): Unit = {
          a.child.dataType match {
            case FloatType | DoubleType if !this.conf.isIncompatEnabled =>
              willNotWorkOnGpu("rounding floating point numbers may be slightly off " +
                  s"compared to Spark's result, to enable set ${RapidsConf.INCOMPATIBLE_OPS}")
            case _ => // NOOP
          }
        }
      }),
    expr[PythonUDF](
      "UDF run in an external python process. Does not actually run on the GPU, but " +
          "the transfer of data to/from it can be accelerated",
      ExprChecks.fullAggAndProject(
        // Different types of Pandas UDF support different sets of output type. Please refer to
        //   https://github.com/apache/spark/blob/master/python/pyspark/sql/udf.py#L98
        // for more details.
        // It is impossible to specify the exact type signature for each Pandas UDF type in a single
        // expression 'PythonUDF'.
        // So use the 'unionOfPandasUdfOut' to cover all types for Spark. The type signature of
        // plugin is also an union of all the types of Pandas UDF.
        (TypeSig.commonCudfTypes + TypeSig.ARRAY).nested() + TypeSig.STRUCT,
        TypeSig.unionOfPandasUdfOut,
        repeatingParamCheck = Some(RepeatingParamCheck(
          "param",
          (TypeSig.commonCudfTypes + TypeSig.ARRAY + TypeSig.STRUCT).nested(),
          TypeSig.all))),
      (a, conf, p, r) => new ExprMeta[PythonUDF](a, conf, p, r) {
        override def replaceMessage: String = "not block GPU acceleration"
        override def noReplacementPossibleMessage(reasons: String): String =
          s"blocks running on GPU because $reasons"

        override def convertToGpuImpl(): GpuExpression =
          GpuPythonUDF(a.name, a.func, a.dataType,
            childExprs.map(_.convertToGpu()),
            a.evalType, a.udfDeterministic, a.resultId)
        }),
    GpuScalaUDFMeta.exprMeta,
    expr[Rand](
      "Generate a random column with i.i.d. uniformly distributed values in [0, 1)",
      ExprChecks.projectOnly(TypeSig.DOUBLE, TypeSig.DOUBLE,
        Seq(ParamCheck("seed",
          (TypeSig.INT + TypeSig.LONG).withAllLit(),
          (TypeSig.INT + TypeSig.LONG).withAllLit()))),
      (a, conf, p, r) => new UnaryExprMeta[Rand](a, conf, p, r) {
        override def convertToGpu(child: Expression): GpuExpression =
          GpuRand(child, this.conf.isRetryContextCheckEnabled)
      }),
    expr[SparkPartitionID] (
      "Returns the current partition id",
      ExprChecks.projectOnly(TypeSig.INT, TypeSig.INT),
      (a, conf, p, r) => new ExprMeta[SparkPartitionID](a, conf, p, r) {
        override def convertToGpuImpl(): GpuExpression = GpuSparkPartitionID()
      }),
    expr[MonotonicallyIncreasingID] (
      "Returns monotonically increasing 64-bit integers",
      ExprChecks.projectOnly(TypeSig.LONG, TypeSig.LONG),
      (a, conf, p, r) => new ExprMeta[MonotonicallyIncreasingID](a, conf, p, r) {
        override def convertToGpuImpl(): GpuExpression = GpuMonotonicallyIncreasingID()
      }),
    expr[InputFileName] (
      "Returns the name of the file being read, or empty string if not available",
      ExprChecks.projectOnly(TypeSig.STRING, TypeSig.STRING),
      (a, conf, p, r) => new ExprMeta[InputFileName](a, conf, p, r) {
        override def convertToGpuImpl(): GpuExpression = GpuInputFileName()
      }),
    expr[InputFileBlockStart] (
      "Returns the start offset of the block being read, or -1 if not available",
      ExprChecks.projectOnly(TypeSig.LONG, TypeSig.LONG),
      (a, conf, p, r) => new ExprMeta[InputFileBlockStart](a, conf, p, r) {
        override def convertToGpuImpl(): GpuExpression = GpuInputFileBlockStart()
      }),
    expr[InputFileBlockLength] (
      "Returns the length of the block being read, or -1 if not available",
      ExprChecks.projectOnly(TypeSig.LONG, TypeSig.LONG),
      (a, conf, p, r) => new ExprMeta[InputFileBlockLength](a, conf, p, r) {
        override def convertToGpuImpl(): GpuExpression = GpuInputFileBlockLength()
      }),
    expr[Md5] (
      "MD5 hash operator",
      ExprChecks.unaryProject(TypeSig.STRING, TypeSig.STRING,
        TypeSig.BINARY, TypeSig.BINARY),
      (a, conf, p, r) => new UnaryExprMeta[Md5](a, conf, p, r) {
        override def convertToGpu(child: Expression): GpuExpression = GpuMd5(child)
      }),
    expr[Sha1] (
      "Sha1 hash operator",
      ExprChecks.unaryProject(TypeSig.STRING, TypeSig.STRING,
        TypeSig.BINARY, TypeSig.BINARY),
      (a, conf, p, r) => new UnaryExprMeta[Sha1](a, conf, p, r) {
        override def convertToGpu(child: Expression): GpuExpression = GpuSha1(child)
      }),
    expr[Sha2] (
      "Sha2 hash operator",
      ExprChecks.binaryProject(
        TypeSig.STRING,
        TypeSig.STRING,
        ("input", TypeSig.BINARY, TypeSig.BINARY),
        ("bitLength", TypeSig.lit(TypeEnum.INT), TypeSig.lit(TypeEnum.INT))),
      (a, conf, p, r) => new GpuSha2.Meta(a, conf, p, r)
    ),
    expr[Upper](
      "String uppercase operator",
      ExprChecks.unaryProjectInputMatchesOutput(TypeSig.STRING, TypeSig.STRING),
      (a, conf, p, r) => new UnaryExprMeta[Upper](a, conf, p, r) {
        override def convertToGpu(child: Expression): GpuExpression = GpuUpper(child)
      })
      .incompat(CASE_MODIFICATION_INCOMPAT),
    expr[Lower](
      "String lowercase operator",
      ExprChecks.unaryProjectInputMatchesOutput(TypeSig.STRING, TypeSig.STRING),
      (a, conf, p, r) => new UnaryExprMeta[Lower](a, conf, p, r) {
        override def convertToGpu(child: Expression): GpuExpression = GpuLower(child)
      })
      .incompat(CASE_MODIFICATION_INCOMPAT),
    expr[StringLPad](
      "Pad a string on the left",
      ExprChecks.projectOnly(TypeSig.STRING, TypeSig.STRING,
        Seq(ParamCheck("str", TypeSig.STRING, TypeSig.STRING),
          ParamCheck("len", TypeSig.lit(TypeEnum.INT), TypeSig.INT),
          ParamCheck("pad", TypeSig.lit(TypeEnum.STRING), TypeSig.STRING))),
      (in, conf, p, r) => new TernaryExprMeta[StringLPad](in, conf, p, r) {
        override def tagExprForGpu(): Unit = {
          extractLit(in.pad).foreach { padLit =>
            if (padLit.value != null &&
                padLit.value.asInstanceOf[UTF8String].toString.length != 1) {
              willNotWorkOnGpu("only a single character is supported for pad")
            }
          }
        }
        override def convertToGpu(
            str: Expression,
            width: Expression,
            pad: Expression): GpuExpression =
          GpuStringLPad(str, width, pad)
      }),
    expr[StringRPad](
      "Pad a string on the right",
      ExprChecks.projectOnly(TypeSig.STRING, TypeSig.STRING,
        Seq(ParamCheck("str", TypeSig.STRING, TypeSig.STRING),
          ParamCheck("len", TypeSig.lit(TypeEnum.INT), TypeSig.INT),
          ParamCheck("pad", TypeSig.lit(TypeEnum.STRING), TypeSig.STRING))),
      (in, conf, p, r) => new TernaryExprMeta[StringRPad](in, conf, p, r) {
        override def tagExprForGpu(): Unit = {
          extractLit(in.pad).foreach { padLit =>
            if (padLit.value != null &&
                padLit.value.asInstanceOf[UTF8String].toString.length != 1) {
              willNotWorkOnGpu("only a single character is supported for pad")
            }
          }
        }
        override def convertToGpu(
            str: Expression,
            width: Expression,
            pad: Expression): GpuExpression =
          GpuStringRPad(str, width, pad)
      }),
    expr[StringSplit](
       "Splits `str` around occurrences that match `regex`",
      // Java's split API produces different behaviors than cudf when splitting with empty pattern
      ExprChecks.projectOnly(TypeSig.ARRAY.nested(TypeSig.STRING),
        TypeSig.ARRAY.nested(TypeSig.STRING),
        Seq(ParamCheck("str", TypeSig.STRING, TypeSig.STRING),
          ParamCheck("regexp", TypeSig.lit(TypeEnum.STRING)
              .withPsNote(TypeEnum.STRING, "very limited subset of regex supported"),
            TypeSig.STRING),
          ParamCheck("limit", TypeSig.lit(TypeEnum.INT), TypeSig.INT))),
      (in, conf, p, r) => new GpuStringSplitMeta(in, conf, p, r)),
    expr[GetStructField](
      "Gets the named field of the struct",
      ExprChecks.unaryProject(
        (TypeSig.commonCudfTypes + TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.MAP + TypeSig.NULL +
            TypeSig.DECIMAL_128 + TypeSig.BINARY).nested(),
        TypeSig.all,
        TypeSig.STRUCT.nested(TypeSig.commonCudfTypes + TypeSig.ARRAY +
            TypeSig.STRUCT + TypeSig.MAP + TypeSig.NULL + TypeSig.DECIMAL_128 + TypeSig.BINARY),
        TypeSig.STRUCT.nested(TypeSig.all)),
      (expr, conf, p, r) => new UnaryExprMeta[GetStructField](expr, conf, p, r) {
        override def convertToGpu(arr: Expression): GpuExpression =
          GpuGetStructField(arr, expr.ordinal, expr.name)
      }),
    expr[GetArrayItem](
      "Gets the field at `ordinal` in the Array",
      ExprChecks.binaryProject(
        (TypeSig.commonCudfTypes + TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.NULL +
            TypeSig.DECIMAL_128 + TypeSig.MAP + TypeSig.BINARY).nested(),
        TypeSig.all,
        ("array", TypeSig.ARRAY.nested(TypeSig.commonCudfTypes + TypeSig.ARRAY +
            TypeSig.STRUCT + TypeSig.NULL + TypeSig.DECIMAL_128 + TypeSig.MAP + TypeSig.BINARY),
            TypeSig.ARRAY.nested(TypeSig.all)),
        ("ordinal", TypeSig.integral, TypeSig.integral)),
      (in, conf, p, r) => new BinaryExprMeta[GetArrayItem](in, conf, p, r) {
        override def convertToGpu(arr: Expression, ordinal: Expression): GpuExpression =
          GpuGetArrayItem(arr, ordinal, in.failOnError)
      }),
    expr[GetMapValue](
      "Gets Value from a Map based on a key",
      ExprChecks.binaryProject(
        (TypeSig.commonCudfTypes + TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.NULL +
          TypeSig.DECIMAL_128 + TypeSig.MAP + TypeSig.BINARY).nested(),
        TypeSig.all,
        ("map", TypeSig.MAP.nested(TypeSig.commonCudfTypes + TypeSig.ARRAY + TypeSig.STRUCT +
          TypeSig.NULL + TypeSig.DECIMAL_128 + TypeSig.MAP + TypeSig.BINARY),
          TypeSig.MAP.nested(TypeSig.all)),
        ("key", TypeSig.commonCudfTypes + TypeSig.DECIMAL_128, TypeSig.all)),
      (in, conf, p, r) => new GetMapValueMeta(in, conf, p, r){}),
    GpuElementAtMeta.elementAtRule(false),
    expr[MapKeys](
      "Returns an unordered array containing the keys of the map",
      ExprChecks.unaryProject(
        TypeSig.ARRAY.nested(TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL +
            TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.BINARY).nested(),
        TypeSig.ARRAY.nested(TypeSig.all - TypeSig.MAP), // Maps cannot have other maps as keys
        TypeSig.MAP.nested(TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL +
            TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.MAP + TypeSig.BINARY),
        TypeSig.MAP.nested(TypeSig.all)),
      (in, conf, p, r) => new UnaryExprMeta[MapKeys](in, conf, p, r) {
        override def convertToGpu(child: Expression): GpuExpression =
          GpuMapKeys(child)
      }),
    expr[MapValues](
      "Returns an unordered array containing the values of the map",
      ExprChecks.unaryProject(
        TypeSig.ARRAY.nested(TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL +
            TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.MAP + TypeSig.BINARY),
        TypeSig.ARRAY.nested(TypeSig.all),
        TypeSig.MAP.nested(TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL +
            TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.MAP + TypeSig.BINARY),
        TypeSig.MAP.nested(TypeSig.all)),
      (in, conf, p, r) => new UnaryExprMeta[MapValues](in, conf, p, r) {
        override def convertToGpu(child: Expression): GpuExpression =
          GpuMapValues(child)
      }),
    expr[MapEntries](
      "Returns an unordered array of all entries in the given map",
      ExprChecks.unaryProject(
        // Technically the return type is an array of struct, but we cannot really express that
        TypeSig.ARRAY.nested(TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL +
            TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.MAP + TypeSig.BINARY),
        TypeSig.ARRAY.nested(TypeSig.all),
        TypeSig.MAP.nested(TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL +
            TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.MAP + TypeSig.BINARY),
        TypeSig.MAP.nested(TypeSig.all)),
      (in, conf, p, r) => new UnaryExprMeta[MapEntries](in, conf, p, r) {
        override def convertToGpu(child: Expression): GpuExpression =
          GpuMapEntries(child)
      }),
    expr[MapFromEntries](
      "Creates a map from an array of entries (structs of key-value pairs)",
      ExprChecks.unaryProject(
        TypeSig.MAP.nested(TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 +
            TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.MAP + TypeSig.BINARY),
        TypeSig.MAP.nested(TypeSig.all),
        // Input is an array of struct<key, value>
        // The struct fields can contain the supported types
        TypeSig.ARRAY.nested(TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 +
            TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.MAP + TypeSig.BINARY),
        TypeSig.ARRAY.nested(TypeSig.all)),
      (in, conf, p, r) => new UnaryExprMeta[MapFromEntries](in, conf, p, r) {
        override def tagExprForGpu(): Unit = {
          // Spark 4.1+ returns an enum value instead of String, so use toString first
          SQLConf.get.getConf(SQLConf.MAP_KEY_DEDUP_POLICY).toString.toUpperCase match {
            case "EXCEPTION" | "LAST_WIN" => // Good we can support this
            case other =>
              willNotWorkOnGpu(s"$other is not supported for config setting" +
                  s" ${SQLConf.MAP_KEY_DEDUP_POLICY.key}")
          }
        }
        override def convertToGpu(child: Expression): GpuExpression =
          GpuMapFromEntries(child)
      }),
    expr[StringToMap](
      "Creates a map after splitting the input string into pairs of key-value strings",
      // Java's split API produces different behaviors than cudf when splitting with empty pattern
      ExprChecks.projectOnly(TypeSig.MAP.nested(TypeSig.STRING), TypeSig.MAP.nested(TypeSig.STRING),
        Seq(ParamCheck("str", TypeSig.STRING, TypeSig.STRING),
          ParamCheck("pairDelim", TypeSig.lit(TypeEnum.STRING), TypeSig.lit(TypeEnum.STRING)),
          ParamCheck("keyValueDelim", TypeSig.lit(TypeEnum.STRING), TypeSig.lit(TypeEnum.STRING)))),
      (in, conf, p, r) => new GpuStringToMapMeta(in, conf, p, r)),
    expr[ArrayMin](
      "Returns the minimum value in the array",
      ExprChecks.unaryProject(
        TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL,
        TypeSig.orderable,
        TypeSig.ARRAY.nested(TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL),
        TypeSig.ARRAY.nested(TypeSig.orderable)),
      (in, conf, p, r) => new UnaryExprMeta[ArrayMin](in, conf, p, r) {
        override def convertToGpu(child: Expression): GpuExpression =
          GpuArrayMin(child)
      }),
    expr[ArrayMax](
      "Returns the maximum value in the array",
      ExprChecks.unaryProject(
        TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL,
        TypeSig.orderable,
        TypeSig.ARRAY.nested(TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL),
        TypeSig.ARRAY.nested(TypeSig.orderable)),
      (in, conf, p, r) => new UnaryExprMeta[ArrayMax](in, conf, p, r) {
        override def convertToGpu(child: Expression): GpuExpression =
          GpuArrayMax(child)
      }),
    expr[ArrayRepeat](
      "Returns the array containing the given input value (left) count (right) times",
      ExprChecks.binaryProject(
        TypeSig.ARRAY.nested(TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL
          + TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.MAP),
        TypeSig.ARRAY.nested(TypeSig.all),
        ("left", (TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL
          + TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.MAP).nested(), TypeSig.all),
        ("right", TypeSig.integral, TypeSig.integral)),
      (in, conf, p, r) => new BinaryExprMeta[ArrayRepeat](in, conf, p, r) {
        override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
          GpuArrayRepeat(lhs, rhs)
      }
    ),
    expr[CreateNamedStruct](
      "Creates a struct with the given field names and values",
      CreateNamedStructCheck,
      (in, conf, p, r) => new ExprMeta[CreateNamedStruct](in, conf, p, r) {
        override def convertToGpuImpl(): GpuExpression =
          GpuCreateNamedStruct(childExprs.map(_.convertToGpu()))
      }),
    expr[ArrayContains](
      "Returns a boolean if the array contains the passed in key",
      ExprChecks.binaryProject(
        TypeSig.BOOLEAN,
        TypeSig.BOOLEAN,
        ("array", TypeSig.ARRAY.nested(TypeSig.commonCudfTypes + TypeSig.NULL),
          TypeSig.ARRAY.nested(TypeSig.all)),
        ("key", TypeSig.commonCudfTypes, TypeSig.all)),
      (in, conf, p, r) => new BinaryExprMeta[ArrayContains](in, conf, p, r) {
        override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
          GpuArrayContains(lhs, rhs)
      }),
    expr[ArrayPosition](
      "Returns the (1-based) index of the first matching element of the array as long, " +
        "or 0 if no match is found.",
      ExprChecks.binaryProject(
        TypeSig.LONG,
        TypeSig.LONG,
        ("array", TypeSig.ARRAY.nested(TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL
            + TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.BINARY),
          TypeSig.ARRAY.nested(TypeSig.orderable)),
        ("key", (TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL +
            TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.BINARY).nested(),
          TypeSig.orderable)),
      (in, conf, p, r) => new GpuArrayPositionMeta(in, conf, p, r)),
    expr[SortArray](
      "Returns a sorted array with the input array and the ascending / descending order",
      ExprChecks.binaryProject(
        TypeSig.ARRAY.nested(TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128 +
            TypeSig.STRUCT),
        TypeSig.ARRAY.nested(TypeSig.all),
        ("array", TypeSig.ARRAY.nested(
          TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128 + TypeSig.STRUCT),
            TypeSig.ARRAY.nested(TypeSig.all)),
        ("ascendingOrder", TypeSig.lit(TypeEnum.BOOLEAN), TypeSig.lit(TypeEnum.BOOLEAN))),
      (sortExpression, conf, p, r) => new BinaryExprMeta[SortArray](sortExpression, conf, p, r) {
        override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression = {
          GpuSortArray(lhs, rhs)
        }
      }
    ),
    expr[ArraySort](
      "Sorts the input array in ascending order with nulls last according to the natural " +
          "ordering of the elements (the default comparator). A custom comparator, or a nested " +
          "element type (array or struct), falls back to the CPU",
      ExprChecks.projectOnly(
        TypeSig.ARRAY.nested(TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128),
        TypeSig.ARRAY.nested(TypeSig.all),
        Seq(
          ParamCheck("array",
            TypeSig.ARRAY.nested(TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128),
            TypeSig.ARRAY.nested(TypeSig.all)))),
      (in, conf, p, r) => new ExprMeta[ArraySort](in, conf, p, r) {
        // Wrap only the array; listSortRows applies the default comparator natively, so the
        // comparator lambda is excluded from children (neither converted nor type-checked).
        // This must wrap exactly one child to stay 1:1 with the single ParamCheck above, which
        // the framework pairs to childExprs positionally.
        override val childExprs: Seq[BaseExprMeta[_]] =
          Seq(GpuOverrides.wrapExpr(in.arguments.head, this.conf, Some(this)))

        override def tagExprForGpu(): Unit = {
          if (!GpuArraySort.isDefaultComparator(in)) {
            willNotWorkOnGpu("array_sort with a custom comparator function is not supported " +
                "on the GPU; only the default ordering (ascending, nulls last) is supported")
          }
        }
        override def convertToGpuImpl(): GpuExpression =
          GpuArraySort(childExprs.head.convertToGpu())
      }),
    expr[CreateArray](
      "Returns an array with the given elements",
      ExprChecks.projectOnly(
        TypeSig.ARRAY.nested(TypeSig.gpuNumeric +
          TypeSig.NULL + TypeSig.STRING + TypeSig.BOOLEAN + TypeSig.DATE + TypeSig.TIMESTAMP +
          TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.BINARY),
        TypeSig.ARRAY.nested(TypeSig.all),
        repeatingParamCheck = Some(RepeatingParamCheck("arg",
          TypeSig.gpuNumeric + TypeSig.NULL + TypeSig.STRING +
              TypeSig.BOOLEAN + TypeSig.DATE + TypeSig.TIMESTAMP + TypeSig.STRUCT + TypeSig.BINARY +
              TypeSig.ARRAY.nested(TypeSig.gpuNumeric + TypeSig.NULL + TypeSig.STRING +
                TypeSig.BOOLEAN + TypeSig.DATE + TypeSig.TIMESTAMP + TypeSig.STRUCT +
                  TypeSig.ARRAY + TypeSig.BINARY),
          TypeSig.all))),
      (in, conf, p, r) => new ExprMeta[CreateArray](in, conf, p, r) {

        override def tagExprForGpu(): Unit = {
          wrapped.dataType match {
            case ArrayType(ArrayType(ArrayType(_, _), _), _) =>
              willNotWorkOnGpu("Only support to create array or array of array, Found: " +
                s"${wrapped.dataType}")
            case _ =>
          }
        }

        override def convertToGpuImpl(): GpuExpression =
          GpuCreateArray(childExprs.map(_.convertToGpu()), wrapped.useStringTypeWhenEmpty)
      }),
    expr[ArrayDistinct](
      "Removes duplicate values from the array",
      ExprChecks.unaryProject(
        TypeSig.ARRAY.nested(TypeSig.orderable),
        TypeSig.ARRAY.nested(TypeSig.orderable),
        TypeSig.ARRAY.nested(TypeSig.orderable),
        TypeSig.ARRAY.nested(TypeSig.orderable)),
      GpuArrayDistinctMeta),
    expr[Flatten](
      "Creates a single array from an array of arrays",
      ExprChecks.unaryProject(
        TypeSig.ARRAY.nested(TypeSig.all),
        TypeSig.ARRAY.nested(TypeSig.all),
        TypeSig.ARRAY.nested(TypeSig.all),
        TypeSig.ARRAY.nested(TypeSig.all)),
      (a, conf, p, r) => new UnaryExprMeta[Flatten](a, conf, p, r) {
        override def convertToGpu(child: Expression): GpuExpression =
          GpuFlattenArray(child)
      }),
    expr[LambdaFunction](
      "Holds a higher order SQL function",
      ExprChecks.projectOnly(
        (TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL + TypeSig.BINARY +
            TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.MAP).nested(),
        TypeSig.all,
        Seq(ParamCheck("function",
          (TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL + TypeSig.BINARY +
              TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.MAP).nested(),
          TypeSig.all)),
        Some(RepeatingParamCheck("arguments",
          (TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL + TypeSig.BINARY +
              TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.MAP).nested(),
          TypeSig.all))),
      (in, conf, p, r) => new ExprMeta[LambdaFunction](in, conf, p, r) {
        override def convertToGpuImpl(): GpuExpression = {
          val func = childExprs.head
          val args = childExprs.tail
          GpuLambdaFunction(func.convertToGpu(),
            args.map(_.convertToGpu().asInstanceOf[NamedExpression]),
            in.hidden)
        }
      }),
    expr[NamedLambdaVariable](
      "A parameter to a higher order SQL function",
      ExprChecks.projectOnly(
        (TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL + TypeSig.BINARY +
            TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.MAP).nested(),
        TypeSig.all),
      (in, conf, p, r) => new ExprMeta[NamedLambdaVariable](in, conf, p, r) {
        override def convertToGpuImpl(): GpuExpression = {
          GpuNamedLambdaVariable(in.name, in.dataType, in.nullable, in.exprId)
        }
      }),
    expr[ArrayTransform](
      "Transform elements in an array using the transform function. This is similar to a `map` " +
          "in functional programming",
      ExprChecks.projectOnly(TypeSig.ARRAY.nested(TypeSig.commonCudfTypes +
        TypeSig.DECIMAL_128 + TypeSig.NULL + TypeSig.BINARY + TypeSig.ARRAY + TypeSig.STRUCT +
        TypeSig.MAP),
        TypeSig.ARRAY.nested(TypeSig.all),
        Seq(
          ParamCheck("argument",
            TypeSig.ARRAY.nested(TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL +
                TypeSig.BINARY + TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.MAP),
            TypeSig.ARRAY.nested(TypeSig.all)),
          ParamCheck("function",
            (TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL +
                TypeSig.BINARY + TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.MAP).nested(),
            TypeSig.all))),
      (in, conf, p, r) => new ExprMeta[ArrayTransform](in, conf, p, r) {
        override def convertToGpuImpl(): GpuExpression = {
          GpuArrayTransform(childExprs.head.convertToGpu(), childExprs(1).convertToGpu())
        }
      }),
     expr[ArrayExists](
      "Return true if any element satisfies the predicate LambdaFunction",
      ExprChecks.projectOnly(TypeSig.BOOLEAN, TypeSig.BOOLEAN,
        Seq(
          ParamCheck("argument",
            TypeSig.ARRAY.nested(TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL +
                TypeSig.BINARY + TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.MAP),
            TypeSig.ARRAY.nested(TypeSig.all)),
          ParamCheck("function", TypeSig.BOOLEAN, TypeSig.BOOLEAN))),
      (in, conf, p, r) => new ExprMeta[ArrayExists](in, conf, p, r) {
        override def convertToGpuImpl(): GpuExpression = {
          GpuArrayExists(
            childExprs.head.convertToGpu(),
            childExprs(1).convertToGpu(),
            SQLConf.get.getConf(SQLConf.LEGACY_ARRAY_EXISTS_FOLLOWS_THREE_VALUED_LOGIC)
          )
        }
      }),
    expr[ArrayFilter](
      "Filter an input array using a given predicate",
      ExprChecks.projectOnly(TypeSig.ARRAY.nested(TypeSig.commonCudfTypes +
        TypeSig.DECIMAL_128 + TypeSig.NULL + TypeSig.BINARY + TypeSig.ARRAY + TypeSig.STRUCT +
        TypeSig.MAP),
        TypeSig.ARRAY.nested(TypeSig.all),
        Seq(
          ParamCheck("argument",
            TypeSig.ARRAY.nested(TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL +
              TypeSig.BINARY + TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.MAP),
            TypeSig.ARRAY.nested(TypeSig.all)),
          ParamCheck("function", TypeSig.BOOLEAN, TypeSig.BOOLEAN))),
      (in, conf, p, r) => new ExprMeta[ArrayFilter](in, conf, p, r) {
        override def convertToGpuImpl(): GpuExpression = {
          GpuArrayFilter(
            childExprs.head.convertToGpu(),
            childExprs(1).convertToGpu()
          )
        }
      }),
    expr[ArrayAggregate](
      "Aggregate elements in an array using an accumulator function and finishing " +
          "transformation. Currently only lambdas of the form (acc, x) -> op(acc, g(x)) with " +
          "an identity finish are executed on the GPU, where op is one of SUM/PRODUCT/MAX/" +
          "MIN/ALL/ANY. If/CaseWhen branches are accepted as long as each branch is itself " +
          "op-of-acc (or bare acc) with op consistent across branches; other shapes fall " +
          "back to CPU. SUM/PRODUCT on float/double share the same parallel-reduction " +
          "non-determinism as GpuSum and are gated by " +
          "spark.rapids.sql.variableFloatAgg.enabled. SUM/PRODUCT on integer/decimal in " +
          "ANSI mode fall back to CPU because cuDF segmented reduce wraps on overflow " +
          "instead of raising.",
      ExprChecks.projectOnly(
        TypeSig.commonCudfTypes + TypeSig.DECIMAL_128,
        TypeSig.all,
        Seq(
          ParamCheck("argument",
            TypeSig.ARRAY.nested(TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL +
                TypeSig.BINARY + TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.MAP),
            TypeSig.ARRAY.nested(TypeSig.all)),
          ParamCheck("zero",
            TypeSig.commonCudfTypes + TypeSig.DECIMAL_128,
            TypeSig.all),
          ParamCheck("merge",
            TypeSig.commonCudfTypes + TypeSig.DECIMAL_128,
            TypeSig.all),
          ParamCheck("finish",
            TypeSig.commonCudfTypes + TypeSig.DECIMAL_128,
            TypeSig.all))),
      (in, conf, p, r) => new GpuArrayAggregateMeta(in, conf, p, r)),
    // TODO: fix the signature https://github.com/NVIDIA/spark-rapids/issues/5327
    expr[ArraysZip](
      "Returns a merged array of structs in which the N-th struct contains" +
        " all N-th values of input arrays.",
      ExprChecks.projectOnly(TypeSig.ARRAY.nested(
        TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL + TypeSig.BINARY +
          TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.MAP),
        TypeSig.ARRAY.nested(TypeSig.all),
        repeatingParamCheck = Some(RepeatingParamCheck("children",
          TypeSig.ARRAY.nested(TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL +
            TypeSig.BINARY + TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.MAP),
          TypeSig.ARRAY.nested(TypeSig.all)))),
      (in, conf, p, r) => new ExprMeta[ArraysZip](in, conf, p, r) {
        override def convertToGpuImpl(): GpuExpression = {
          GpuArraysZip(childExprs.map(_.convertToGpu()))
        }
      }
    ),
    expr[ArrayExcept](
      "Returns an array of the elements in array1 but not in array2, without duplicates",
      ExprChecks.binaryProject(
        TypeSig.ARRAY.nested(TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL),
        TypeSig.ARRAY.nested(TypeSig.all),
        ("array1",
            TypeSig.ARRAY.nested(TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL),
            TypeSig.ARRAY.nested(TypeSig.all)),
        ("array2",
            TypeSig.ARRAY.nested(TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL),
            TypeSig.ARRAY.nested(TypeSig.all))),
      (in, conf, p, r) => new BinaryExprMeta[ArrayExcept](in, conf, p, r) {
        override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression = {
          GpuArrayExcept(lhs, rhs)
        }
      }
    ).incompat("the GPU implementation treats -0.0 and 0.0 as equal, but the CPU " +
        "implementation currently does not (see SPARK-39845). Also, Apache Spark " +
        "3.1.3 fixed issue SPARK-36741 where NaNs in these set like operators were " +
        "not treated as being equal. We have chosen to break with compatibility for " +
        "the older versions of Spark in this instance and handle NaNs the same as 3.1.3+"),
    expr[ArrayIntersect](
      "Returns an array of the elements in the intersection of array1 and array2, without" +
        " duplicates",
      ExprChecks.binaryProject(
        TypeSig.ARRAY.nested(TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL),
        TypeSig.ARRAY.nested(TypeSig.all),
        ("array1",
            TypeSig.ARRAY.nested(TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL),
            TypeSig.ARRAY.nested(TypeSig.all)),
        ("array2",
            TypeSig.ARRAY.nested(TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL),
            TypeSig.ARRAY.nested(TypeSig.all))),
      (in, conf, p, r) => new BinaryExprMeta[ArrayIntersect](in, conf, p, r) {
        override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression = {
          GpuArrayIntersect(lhs, rhs)
        }
      }
    ).incompat("the GPU implementation treats -0.0 and 0.0 as equal, but the CPU " +
        "implementation currently does not (see SPARK-39845). Also, Apache Spark " +
        "3.1.3 fixed issue SPARK-36741 where NaNs in these set like operators were " +
        "not treated as being equal. We have chosen to break with compatibility for " +
        "the older versions of Spark in this instance and handle NaNs the same as 3.1.3+"),
    expr[ArrayUnion](
      "Returns an array of the elements in the union of array1 and array2, without duplicates.",
      ExprChecks.binaryProject(
        TypeSig.ARRAY.nested(TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL),
        TypeSig.ARRAY.nested(TypeSig.all),
        ("array1",
            TypeSig.ARRAY.nested(TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL),
            TypeSig.ARRAY.nested(TypeSig.all)),
        ("array2",
            TypeSig.ARRAY.nested(TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL),
            TypeSig.ARRAY.nested(TypeSig.all))),
      (in, conf, p, r) => new BinaryExprMeta[ArrayUnion](in, conf, p, r) {
        override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression = {
          GpuArrayUnion(lhs, rhs)
        }
      }
    ).incompat("the GPU implementation treats -0.0 and 0.0 as equal, but the CPU " +
        "implementation currently does not (see SPARK-39845). Also, Apache Spark " +
        "3.1.3 fixed issue SPARK-36741 where NaNs in these set like operators were " +
        "not treated as being equal. We have chosen to break with compatibility for " +
        "the older versions of Spark in this instance and handle NaNs the same as 3.1.3+"),
    expr[ArraysOverlap](
      "Returns true if a1 contains at least a non-null element present also in a2. If the arrays " +
      "have no common element and they are both non-empty and either of them contains a null " +
      "element null is returned, false otherwise.",
      ExprChecks.binaryProject(TypeSig.BOOLEAN, TypeSig.BOOLEAN,
        ("array1",
            TypeSig.ARRAY.nested(TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL),
            TypeSig.ARRAY.nested(TypeSig.all)),
        ("array2",
            TypeSig.ARRAY.nested(TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL),
            TypeSig.ARRAY.nested(TypeSig.all))),
      (in, conf, p, r) => new BinaryExprMeta[ArraysOverlap](in, conf, p, r) {
        override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression = {
          GpuArraysOverlap(lhs, rhs)
        }
      }
    ).incompat("the GPU implementation treats -0.0 and 0.0 as equal, but the CPU " +
        "implementation currently does not (see SPARK-39845). Also, Apache Spark " +
        "3.1.3 fixed issue SPARK-36741 where NaNs in these set like operators were " +
        "not treated as being equal. We have chosen to break with compatibility for " +
        "the older versions of Spark in this instance and handle NaNs the same as 3.1.3+"),
    expr[ArrayRemove](
      "Returns the array after removing all elements that equal to the input element (right) " +
      "from the input array (left)",
      ExprChecks.binaryProject(
        TypeSig.ARRAY.nested(TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL +
          TypeSig.STRUCT),
        TypeSig.ARRAY.nested(TypeSig.all),
        ("array",
          TypeSig.ARRAY.nested(TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL +
            TypeSig.STRUCT),
          TypeSig.all),
        ("element",
          (TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL +
            TypeSig.STRUCT).nested(),
          TypeSig.all)),
      (in, conf, p, r) => new BinaryExprMeta[ArrayRemove](in, conf, p, r) {
        override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
          GpuArrayRemove(lhs, rhs)
      }
    ),
    expr[MapFromArrays](
      "Creates a new map from two arrays",
      ExprChecks.binaryProject(
        TypeSig.MAP.nested(TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 +
          TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.MAP),
        // Map values may themselves be maps. Map keys remain constrained by the key-array check.
        TypeSig.MAP.nested(TypeSig.all),
        ("keys",
          TypeSig.ARRAY.nested(
            TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 +
              TypeSig.ARRAY + TypeSig.STRUCT),
          TypeSig.ARRAY.nested(TypeSig.all - TypeSig.MAP)),
        ("values",
          TypeSig.ARRAY.nested(TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 +
            TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.MAP),
          TypeSig.ARRAY.nested(TypeSig.all))),
      GpuMapFromArraysMeta
    ),
    expr[TransformKeys](
      "Transform keys in a map using a transform function",
      ExprChecks.projectOnly(TypeSig.MAP.nested(TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 +
          TypeSig.NULL + TypeSig.BINARY + TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.MAP),
        TypeSig.MAP.nested(TypeSig.all),
        Seq(
          ParamCheck("argument",
            TypeSig.MAP.nested(TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL +
                TypeSig.BINARY + TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.MAP),
            TypeSig.MAP.nested(TypeSig.all)),
          ParamCheck("function",
            // We need to be able to check for duplicate keys (equality)
            TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL,
            TypeSig.all - TypeSig.MAP.nested()))),
      (in, conf, p, r) => new ExprMeta[TransformKeys](in, conf, p, r) {
        override def tagExprForGpu(): Unit = {
          // Spark 4.1+ returns an enum value instead of String, so use toString first
          SQLConf.get.getConf(SQLConf.MAP_KEY_DEDUP_POLICY).toString.toUpperCase match {
            case "EXCEPTION"| "LAST_WIN" => // Good we can support this
            case other =>
              willNotWorkOnGpu(s"$other is not supported for config setting" +
                  s" ${SQLConf.MAP_KEY_DEDUP_POLICY.key}")
          }
        }
        override def convertToGpuImpl(): GpuExpression = {
          GpuTransformKeys(childExprs.head.convertToGpu(), childExprs(1).convertToGpu())
        }
      }),
    expr[TransformValues](
      "Transform values in a map using a transform function",
      ExprChecks.projectOnly(TypeSig.MAP.nested(TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 +
          TypeSig.NULL + TypeSig.BINARY + TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.MAP),
        TypeSig.MAP.nested(TypeSig.all),
        Seq(
          ParamCheck("argument",
            TypeSig.MAP.nested(TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL +
                TypeSig.BINARY + TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.MAP),
            TypeSig.MAP.nested(TypeSig.all)),
          ParamCheck("function",
            (TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL +
                TypeSig.BINARY + TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.MAP).nested(),
            TypeSig.all))),
      (in, conf, p, r) => new ExprMeta[TransformValues](in, conf, p, r) {
        override def convertToGpuImpl(): GpuExpression = {
          GpuTransformValues(childExprs.head.convertToGpu(), childExprs(1).convertToGpu())
        }
      }),
    expr[MapZipWith](
      "Filters entries in a map using the function",
      ExprChecks.projectOnly(TypeSig.MAP.nested(TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 +
          TypeSig.NULL + TypeSig.BINARY + TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.MAP),
        TypeSig.MAP.nested(TypeSig.all),
        Seq(
          ParamCheck("argument1",
            TypeSig.MAP.nested(TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL +
                TypeSig.BINARY + TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.MAP),
            TypeSig.MAP.nested(TypeSig.all)),
          ParamCheck("argument2",
            TypeSig.MAP.nested(TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL +
                TypeSig.BINARY + TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.MAP),
            TypeSig.MAP.nested(TypeSig.all)),
          ParamCheck("function",
            (TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL +
                TypeSig.BINARY + TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.MAP).nested(),
            TypeSig.all))),
      (in, conf, p, r) => new ExprMeta[MapZipWith](in, conf, p, r) {
        override def convertToGpuImpl(): GpuExpression = {
          GpuMapZipWith(childExprs.head.convertToGpu(), 
          childExprs(1).convertToGpu(), childExprs(2).convertToGpu())
        }
      }),
    expr[MapFilter](
      "Filters entries in a map using the function",
      ExprChecks.projectOnly(TypeSig.MAP.nested(TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 +
          TypeSig.NULL + TypeSig.BINARY + TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.MAP),
        TypeSig.MAP.nested(TypeSig.all),
        Seq(
          ParamCheck("argument",
            TypeSig.MAP.nested(TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL +
                TypeSig.BINARY + TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.MAP),
            TypeSig.MAP.nested(TypeSig.all)),
          ParamCheck("function", TypeSig.BOOLEAN, TypeSig.BOOLEAN))),
      (in, conf, p, r) => new ExprMeta[MapFilter](in, conf, p, r) {
        override def convertToGpuImpl(): GpuExpression = {
          GpuMapFilter(childExprs.head.convertToGpu(), childExprs(1).convertToGpu())
        }
      }),
    expr[StringLocate](
      "Substring search operator",
      ExprChecks.projectOnly(TypeSig.INT, TypeSig.INT,
        Seq(ParamCheck("substr", TypeSig.lit(TypeEnum.STRING), TypeSig.STRING),
          ParamCheck("str", TypeSig.STRING, TypeSig.STRING),
          ParamCheck("start", TypeSig.lit(TypeEnum.INT), TypeSig.INT))),
      (in, conf, p, r) => new TernaryExprMeta[StringLocate](in, conf, p, r) {
        override def convertToGpu(
            val0: Expression,
            val1: Expression,
            val2: Expression): GpuExpression =
          GpuStringLocate(val0, val1, val2)
      }),
    expr[StringInstr](
      "Instr string operator",
      ExprChecks.projectOnly(TypeSig.INT, TypeSig.INT,
        Seq(ParamCheck("str", TypeSig.STRING, TypeSig.STRING),
            ParamCheck("substr", TypeSig.lit(TypeEnum.STRING), TypeSig.STRING))),
      (in, conf, p, r) => new BinaryExprMeta[StringInstr](in, conf, p, r) {
        override def convertToGpu(
            str: Expression,
            substr: Expression): GpuExpression =
          GpuStringInstr(str, substr)
      }),
    expr[Substring](
      "Substring operator",
      ExprChecks.projectOnly(TypeSig.STRING, TypeSig.STRING + TypeSig.BINARY,
        Seq(ParamCheck("str", TypeSig.STRING, TypeSig.STRING + TypeSig.BINARY),
          ParamCheck("pos", TypeSig.INT, TypeSig.INT),
          ParamCheck("len", TypeSig.INT, TypeSig.INT))),
      (in, conf, p, r) => new TernaryExprMeta[Substring](in, conf, p, r) {
        override def convertToGpu(
            column: Expression,
            position: Expression,
            length: Expression): GpuExpression =
          GpuSubstring(column, position, length)
      }),
    expr[SubstringIndex](
      "substring_index operator",
      ExprChecks.projectOnly(TypeSig.STRING, TypeSig.STRING,
        Seq(ParamCheck("str", TypeSig.STRING, TypeSig.STRING),
          ParamCheck("delim", TypeSig.lit(TypeEnum.STRING), TypeSig.STRING),
          ParamCheck("count", TypeSig.lit(TypeEnum.INT), TypeSig.INT))),
      (in, conf, p, r) => new SubstringIndexMeta(in, conf, p, r)),
    expr[StringRepeat](
      "StringRepeat operator that repeats the given strings with numbers of times " +
        "given by repeatTimes",
      ExprChecks.projectOnly(TypeSig.STRING, TypeSig.STRING,
        Seq(ParamCheck("input", TypeSig.STRING, TypeSig.STRING),
          ParamCheck("repeatTimes", TypeSig.INT, TypeSig.INT))),
      (in, conf, p, r) => new BinaryExprMeta[StringRepeat](in, conf, p, r) {
        override def convertToGpu(
            input: Expression,
            repeatTimes: Expression): GpuExpression = GpuStringRepeat(input, repeatTimes)
      }),
    expr[StringReplace](
      "StringReplace operator",
      ExprChecks.projectOnly(TypeSig.STRING, TypeSig.STRING,
        Seq(ParamCheck("src", TypeSig.STRING, TypeSig.STRING),
          ParamCheck("search", TypeSig.STRING, TypeSig.STRING),
          ParamCheck("replace", TypeSig.STRING, TypeSig.STRING))),
      (in, conf, p, r) => new TernaryExprMeta[StringReplace](in, conf, p, r) {
        override def convertToGpu(
            column: Expression,
            target: Expression,
            replace: Expression): GpuExpression =
          GpuStringReplace(column, target, replace)
      }),
    expr[StringTrim](
      "StringTrim operator",
      ExprChecks.projectOnly(TypeSig.STRING, TypeSig.STRING,
        Seq(ParamCheck("src", TypeSig.STRING, TypeSig.STRING)),
        // Should really be an OptionalParam
        Some(RepeatingParamCheck("trimStr", TypeSig.lit(TypeEnum.STRING), TypeSig.STRING))),
      (in, conf, p, r) => new String2TrimExpressionMeta[StringTrim](in, conf, p, r) {
        override def convertToGpu(
            column: Expression,
            target: Option[Expression] = None): GpuExpression =
          GpuStringTrim(column, target)
      }),
    expr[StringTrimLeft](
      "StringTrimLeft operator",
      ExprChecks.projectOnly(TypeSig.STRING, TypeSig.STRING,
        Seq(ParamCheck("src", TypeSig.STRING, TypeSig.STRING)),
        // Should really be an OptionalParam
        Some(RepeatingParamCheck("trimStr", TypeSig.lit(TypeEnum.STRING), TypeSig.STRING))),
      (in, conf, p, r) =>
        new String2TrimExpressionMeta[StringTrimLeft](in, conf, p, r) {
          override def convertToGpu(
            column: Expression,
            target: Option[Expression] = None): GpuExpression =
            GpuStringTrimLeft(column, target)
        }),
    expr[StringTrimRight](
      "StringTrimRight operator",
      ExprChecks.projectOnly(TypeSig.STRING, TypeSig.STRING,
        Seq(ParamCheck("src", TypeSig.STRING, TypeSig.STRING)),
        // Should really be an OptionalParam
        Some(RepeatingParamCheck("trimStr", TypeSig.lit(TypeEnum.STRING), TypeSig.STRING))),
      (in, conf, p, r) =>
        new String2TrimExpressionMeta[StringTrimRight](in, conf, p, r) {
          override def convertToGpu(
              column: Expression,
              target: Option[Expression] = None): GpuExpression =
            GpuStringTrimRight(column, target)
        }),
    expr[StringTranslate](
      "StringTranslate operator",
      ExprChecks.projectOnly(TypeSig.STRING, TypeSig.STRING,
        Seq(ParamCheck("input", TypeSig.STRING, TypeSig.STRING),
          ParamCheck("from", TypeSig.lit(TypeEnum.STRING), TypeSig.STRING),
          ParamCheck("to", TypeSig.lit(TypeEnum.STRING), TypeSig.STRING))),
      (in, conf, p, r) => new TernaryExprMeta[StringTranslate](in, conf, p, r) {
        override def convertToGpu(
            input: Expression,
            from: Expression,
            to: Expression): GpuExpression =
          GpuStringTranslate(input, from, to)
      }).incompat("the GPU implementation supports all unicode code points. In Spark versions " +
          "< 3.2.0, translate() does not support unicode characters with code point >= U+10000 " +
          "(See SPARK-34094)"),
    expr[StartsWith](
      "Starts with",
      ExprChecks.binaryProject(TypeSig.BOOLEAN, TypeSig.BOOLEAN,
        ("src", TypeSig.STRING, TypeSig.STRING),
        ("search", TypeSig.lit(TypeEnum.STRING), TypeSig.STRING)),
      (a, conf, p, r) => new BinaryExprMeta[StartsWith](a, conf, p, r) {
        override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
          GpuStartsWith(lhs, rhs)
      }),
    expr[EndsWith](
      "Ends with",
      ExprChecks.binaryProject(TypeSig.BOOLEAN, TypeSig.BOOLEAN,
        ("src", TypeSig.STRING, TypeSig.STRING),
        ("search", TypeSig.lit(TypeEnum.STRING), TypeSig.STRING)),
      (a, conf, p, r) => new BinaryExprMeta[EndsWith](a, conf, p, r) {
        override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
          GpuEndsWith(lhs, rhs)
      }),
    expr[Concat](
      "List/String concatenate",
      ExprChecks.projectOnly((TypeSig.STRING + TypeSig.ARRAY).nested(
        TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128 +
            TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.MAP + TypeSig.BINARY),
        (TypeSig.STRING + TypeSig.BINARY + TypeSig.ARRAY).nested(TypeSig.all),
        repeatingParamCheck = Some(RepeatingParamCheck("input",
          (TypeSig.STRING + TypeSig.ARRAY).nested(
            TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128 +
                TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.MAP + TypeSig.BINARY),
          (TypeSig.STRING + TypeSig.BINARY + TypeSig.ARRAY).nested(TypeSig.all)))),
      (a, conf, p, r) => new ComplexTypeMergingExprMeta[Concat](a, conf, p, r) {
        override def convertToGpu(child: Seq[Expression]): GpuExpression = GpuConcat(child)
      }),
    expr[Conv](
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
        new GpuConvMeta(convExpr, conf, parentMetaOpt, dataFromReplacementRule)
    ),
    expr[FormatNumber](
      "Formats the number x like '#,###,###.##', rounded to d decimal places.",
      ExprChecks.binaryProject(TypeSig.STRING, TypeSig.STRING,
        ("x", TypeSig.gpuNumeric, TypeSig.cpuNumeric),
        ("d", TypeSig.lit(TypeEnum.INT), TypeSig.INT+TypeSig.STRING)),
      (in, conf, p, r) => new BinaryExprMeta[FormatNumber](in, conf, p, r) {
        override def tagExprForGpu(): Unit = {
          in.children.head.dataType match {
            case FloatType | DoubleType if !this.conf.isFloatFormatNumberEnabled =>
              willNotWorkOnGpu("format_number with floating point types on the GPU returns " +
                  "results that have a different precision than the default results of Spark. " +
                  "To enable this operation on the GPU, set" +
                  s" ${RapidsConf.ENABLE_FLOAT_FORMAT_NUMBER} to true.")
            case _ =>
          }
        }
        override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
          GpuFormatNumber(lhs, rhs)
      }
    ),
    expr[MapConcat](
      "Returns the union of all the given maps",
      ExprChecks.projectOnly(TypeSig.MAP.nested(TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 +
          TypeSig.NULL + TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.MAP),
        TypeSig.MAP.nested(TypeSig.all),
        repeatingParamCheck = Some(RepeatingParamCheck("input",
          TypeSig.MAP.nested(TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 +
          TypeSig.NULL + TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.MAP),
          TypeSig.MAP.nested(TypeSig.all)))),
      (a, conf, p, r) => new ComplexTypeMergingExprMeta[MapConcat](a, conf, p, r) {
        override def convertToGpu(child: Seq[Expression]): GpuExpression = GpuMapConcat(child)
      }),
    expr[Slice](
      "Subsets array x starting from index start (array indices start at 1, " +
        "or starting from the end if start is negative) with the specified length.",
      ExprChecks.projectOnly(TypeSig.ARRAY.nested(TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 +
          TypeSig.NULL + TypeSig.BINARY + TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.MAP),
        TypeSig.ARRAY.nested(TypeSig.all),
        Seq(
          ParamCheck("x",
            TypeSig.ARRAY.nested(TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL +
                TypeSig.BINARY + TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.MAP),
            TypeSig.ARRAY.nested(TypeSig.all)),
          ParamCheck("start", TypeSig.INT, TypeSig.INT),
          ParamCheck("length", TypeSig.INT, TypeSig.INT))),
      (in, conf, p, r) => new TernaryExprMeta[Slice](in, conf, p, r) {
        override def convertToGpu(
            x: Expression,
            start: Expression,
            length: Expression): GpuExpression =
          GpuSlice(x, start, length)
      }),
    expr[ArrayJoin](
      "Concatenates the elements of the given array using the delimiter and an optional " +
        "string to replace nulls. If no value is set for nullReplacement, any null value " +
        "is filtered.",
      ExprChecks.projectOnly(TypeSig.STRING, TypeSig.STRING,
        Seq(ParamCheck("array",
          TypeSig.ARRAY.nested(TypeSig.STRING),
          TypeSig.ARRAY.nested(TypeSig.STRING)),
          ParamCheck("delimiter",
            TypeSig.STRING,
            TypeSig.STRING)),
        repeatingParamCheck = Some(RepeatingParamCheck("nullReplacement",
          TypeSig.lit(TypeEnum.STRING),
          TypeSig.STRING))),
      (a, conf, p, r) => new ExprMeta[ArrayJoin](a, conf, p, r) {
        override def tagExprForGpu(): Unit = {
          if (a.children.size > 3) {
            willNotWorkOnGpu(s"array_join has more parameters than we expected " +
              s"to see. Found ${a.children.size}")
          }
        }
        override def convertToGpuImpl(): GpuExpression =
          GpuArrayJoin(childExprs.map(_.convertToGpu()))
      }
    ),
    expr[ConcatWs](
      "Concatenates multiple input strings or array of strings into a single " +
        "string using a given separator",
      ExprChecks.projectOnly(TypeSig.STRING, TypeSig.STRING,
        repeatingParamCheck = Some(RepeatingParamCheck("input",
          (TypeSig.STRING + TypeSig.ARRAY).nested(TypeSig.STRING),
          (TypeSig.STRING + TypeSig.ARRAY).nested(TypeSig.STRING)))),
      (a, conf, p, r) => new ExprMeta[ConcatWs](a, conf, p, r) {
        override def tagExprForGpu(): Unit = {
          if (a.children.size <= 1) {
            // If only a separator specified and its a column, Spark returns an empty
            // string for all entries unless they are null, then it returns null.
            // This seems like edge case so instead of handling on GPU just fallback.
            willNotWorkOnGpu("Only specifying separator column not supported on GPU")
          }
        }
        override final def convertToGpuImpl(): GpuExpression =
          GpuConcatWs(childExprs.map(_.convertToGpu()))
      }),
    expr[Murmur3Hash](
      "Murmur3 hash operator",
      HashExprChecks.murmur3ProjectChecks,
      Murmur3HashExprMeta.apply),
    expr[XxHash64](
      "xxhash64 hash operator",
      HashExprChecks.xxhash64ProjectChecks,
      XxHash64ExprMeta.apply),
    expr[HiveHash](
      "hive hash operator",
      ExprChecks.projectOnly(TypeSig.INT, TypeSig.INT,
        repeatingParamCheck = Some(RepeatingParamCheck("input",
          (TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.STRUCT + TypeSig.ARRAY).nested() +
              TypeSig.psNote(TypeEnum.ARRAY, "The nesting depth has a certain limit") +
              TypeSig.psNote(TypeEnum.STRUCT, "The nesting depth has a certain limit"),
          TypeSig.all))),
      (a, conf, p, r) => new ExprMeta[HiveHash](a, conf, p, r) {
        override def tagExprForGpu(): Unit = {
          def getMaxStackDepth(inputType: DataType): Int = {
            inputType match {
              case at: ArrayType => 1 + getMaxStackDepth(at.elementType)
              case st: StructType =>
                1 + st.map(f => getMaxStackDepth(f.dataType)).max
              case _ => 0 // primitive types
            }
          }
          val maxDepth = a.children.map(c => getMaxStackDepth(c.dataType)).max
          val supportedDepth = XxHash64Utils.MAX_STACK_DEPTH
          if (maxDepth > supportedDepth) {
            willNotWorkOnGpu(s"the data type requires a stack size of $maxDepth, " +
              s"which exceeds the GPU limit of $supportedDepth")
          }
        }

        def convertToGpuImpl(): GpuExpression =
          GpuHiveHash(childExprs.map(_.convertToGpu()))
      }),
    expr[Contains](
      "Contains",
      ExprChecks.binaryProject(TypeSig.BOOLEAN, TypeSig.BOOLEAN,
        ("src", TypeSig.STRING, TypeSig.STRING),
        ("search", TypeSig.STRING, TypeSig.STRING)),
      (a, conf, p, r) => new BinaryExprMeta[Contains](a, conf, p, r) {
        override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
          GpuContains(lhs, rhs)
      }),
    expr[Like](
      "Like",
      ExprChecks.binaryProject(TypeSig.BOOLEAN, TypeSig.BOOLEAN,
        ("src", TypeSig.STRING, TypeSig.STRING),
        ("search", TypeSig.lit(TypeEnum.STRING), TypeSig.STRING)),
      (a, conf, p, r) => new BinaryExprMeta[Like](a, conf, p, r) {
        override def tagExprForGpu(): Unit = {
          a.right match {
            case Literal(v: UTF8String, _) =>
              val pattern = v.toString
              val esc = a.escapeChar
              var i = 0
              while (i < pattern.length) {
                if (pattern.charAt(i) == esc) {
                  val j = i + 1
                  if (j >= pattern.length) {
                    willNotWorkOnGpu(
                      "invalid LIKE escape pattern")
                    return
                  }
                  val c = pattern.charAt(j)
                  if (c != '_' && c != '%' && c != esc) {
                    willNotWorkOnGpu(
                      "invalid LIKE escape pattern")
                    return
                  }
                  i = j + 1
                } else {
                  i += 1
                }
              }
            case _ =>
          }
        }
        override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
          GpuLike(lhs, rhs, a.escapeChar)
      }),
    expr[RLike](
      "Regular expression version of Like",
      ExprChecks.binaryProject(TypeSig.BOOLEAN, TypeSig.BOOLEAN,
        ("str", TypeSig.STRING, TypeSig.STRING),
        ("regexp", TypeSig.lit(TypeEnum.STRING), TypeSig.STRING)),
      (a, conf, p, r) => new GpuRLikeMeta(a, conf, p, r)),
    expr[RegExpReplace](
      "String replace using a regular expression pattern",
      ExprChecks.projectOnly(TypeSig.STRING, TypeSig.STRING,
        Seq(ParamCheck("str", TypeSig.STRING, TypeSig.STRING),
          ParamCheck("regex", TypeSig.lit(TypeEnum.STRING), TypeSig.STRING),
          ParamCheck("rep", TypeSig.lit(TypeEnum.STRING), TypeSig.STRING),
          ParamCheck("pos", TypeSig.lit(TypeEnum.INT)
              .withPsNote(TypeEnum.INT, "only a value of 1 is supported"),
            TypeSig.lit(TypeEnum.INT)))),
      (a, conf, p, r) => new GpuRegExpReplaceMeta(a, conf, p, r)),
    expr[RegExpExtract](
      "Extract a specific group identified by a regular expression",
      ExprChecks.projectOnly(TypeSig.STRING, TypeSig.STRING,
        Seq(ParamCheck("str", TypeSig.STRING, TypeSig.STRING),
          ParamCheck("regexp", TypeSig.lit(TypeEnum.STRING), TypeSig.STRING),
          ParamCheck("idx", TypeSig.lit(TypeEnum.INT),
            TypeSig.lit(TypeEnum.INT)))),
      (a, conf, p, r) => new GpuRegExpExtractMeta(a, conf, p, r)),
    expr[RegExpExtractAll](
      "Extract all strings matching a regular expression corresponding to the regex group index",
      ExprChecks.projectOnly(TypeSig.ARRAY.nested(TypeSig.STRING),
        TypeSig.ARRAY.nested(TypeSig.STRING),
        Seq(ParamCheck("str", TypeSig.STRING, TypeSig.STRING),
          ParamCheck("regexp", TypeSig.lit(TypeEnum.STRING), TypeSig.STRING),
          ParamCheck("idx", TypeSig.lit(TypeEnum.INT), TypeSig.INT))),
      (a, conf, p, r) => new GpuRegExpExtractAllMeta(a, conf, p, r)),
    expr[ParseUrl](
      "Extracts a part from a URL",
      ExprChecks.projectOnly(TypeSig.STRING, TypeSig.STRING,
        Seq(ParamCheck("url", TypeSig.STRING, TypeSig.STRING),
          ParamCheck("partToExtract", TypeSig.lit(TypeEnum.STRING).withPsNote(
            TypeEnum.STRING, "only support partToExtract = PROTOCOL | HOST | QUERY | PATH"),
            TypeSig.STRING)),
          // Should really be an OptionalParam
          Some(RepeatingParamCheck("key", TypeSig.STRING, TypeSig.STRING))),
      (a, conf, p, r) => new ExprMeta[ParseUrl](a, conf, p, r) {
        override def tagExprForGpu(): Unit = {

          extractStringLit(a.children(1)) match {
            // In Spark, the key in parse_url could act like a regex, but GPU will match the key
            // exactly. When key is literal, GPU will check if the key contains regex special and
            // fallbcak to CPU if it does, but we are not able to fallback when key is column.
            // see Spark issue: https://issues.apache.org/jira/browse/SPARK-44500
            case Some("QUERY") if (a.children.size == 3) => {
              extractLit(a.children(2)).foreach { key =>
                if (key.value != null) {
                  val keyStr = key.value.asInstanceOf[UTF8String].toString
                  if (regexMetaChars.exists(keyStr.contains(_))) {
                    willNotWorkOnGpu(s"Key $keyStr could act like a regex which is not " +
                        "supported on GPU")
                  }
                }
              }
            }
            case Some(part) if GpuParseUrl.isSupportedPart(part) =>
            case Some(other) =>
              willNotWorkOnGpu(s"Part to extract $other is not supported on GPU")
            case None =>
              // Should never get here, but just in case
              willNotWorkOnGpu("GPU only supports a literal for the part to extract")
          }
        }

        override def convertToGpuImpl(): GpuExpression = {
          GpuParseUrl(childExprs.map(_.convertToGpu()), a.failOnError)
        }
      }),
    expr[Length](
      "String character length or binary byte length",
      ExprChecks.unaryProject(TypeSig.INT, TypeSig.INT,
        TypeSig.STRING, TypeSig.STRING + TypeSig.BINARY),
      (a, conf, p, r) => new UnaryExprMeta[Length](a, conf, p, r) {
        override def convertToGpu(child: Expression): GpuExpression = GpuLength(child)
      }),
    expr[Size](
      "The size of an array or a map",
      ExprChecks.unaryProject(TypeSig.INT, TypeSig.INT,
        (TypeSig.ARRAY + TypeSig.MAP).nested(TypeSig.commonCudfTypes + TypeSig.NULL
            + TypeSig.DECIMAL_128 + TypeSig.BINARY + TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.MAP),
        (TypeSig.ARRAY + TypeSig.MAP).nested(TypeSig.all)),
      (a, conf, p, r) => new UnaryExprMeta[Size](a, conf, p, r) {
        override def convertToGpu(child: Expression): GpuExpression =
          GpuSize(child, a.legacySizeOfNull)
      }),
    expr[Reverse](
      "Returns a reversed string or an array with reverse order of elements",
      ExprChecks.unaryProject(TypeSig.STRING + TypeSig.ARRAY.nested(TypeSig.all),
        TypeSig.STRING + TypeSig.ARRAY.nested(TypeSig.all),
        TypeSig.STRING + TypeSig.ARRAY.nested(TypeSig.all),
        TypeSig.STRING + TypeSig.ARRAY.nested(TypeSig.all)),
      (a, conf, p, r) => new UnaryExprMeta[Reverse](a, conf, p, r) {
        override def convertToGpu(input: Expression): GpuExpression =
          GpuReverse(input)
      }),
    expr[UnscaledValue](
      "Convert a Decimal to an unscaled long value for some aggregation optimizations",
      ExprChecks.unaryProject(TypeSig.LONG, TypeSig.LONG,
        TypeSig.DECIMAL_64, TypeSig.DECIMAL_128),
      (a, conf, p, r) => new UnaryExprMeta[UnscaledValue](a, conf, p, r) {
        override val isFoldableNonLitAllowed: Boolean = true
        override def convertToGpu(child: Expression): GpuExpression = GpuUnscaledValue(child)
      }),
    expr[MakeDecimal](
      "Create a Decimal from an unscaled long value for some aggregation optimizations",
      ExprChecks.unaryProject(TypeSig.DECIMAL_64, TypeSig.DECIMAL_128,
        TypeSig.LONG, TypeSig.LONG),
      (a, conf, p, r) => new UnaryExprMeta[MakeDecimal](a, conf, p, r) {
        override def convertToGpu(child: Expression): GpuExpression =
          GpuMakeDecimal(child, a.precision, a.scale, a.nullOnOverflow)
      }),
    expr[Explode](
      "Given an input array produces a sequence of rows for each value in the array",
      ExprChecks.unaryProject(
        // Here is a walk-around representation, since multi-level nested type is not supported yet.
        // related issue: https://github.com/NVIDIA/spark-rapids/issues/1901
        TypeSig.ARRAY.nested(TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128 +
            TypeSig.BINARY + TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.MAP),
        TypeSig.ARRAY.nested(TypeSig.all),
        (TypeSig.ARRAY + TypeSig.MAP).nested(TypeSig.commonCudfTypes + TypeSig.NULL +
            TypeSig.DECIMAL_128 + TypeSig.BINARY + TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.MAP),
        (TypeSig.ARRAY + TypeSig.MAP).nested(TypeSig.all)),
      (a, conf, p, r) => new GeneratorExprMeta[Explode](a, conf, p, r) {
        override val supportOuter: Boolean = true
        override def convertToGpuImpl(): GpuExpression = GpuExplode(childExprs.head.convertToGpu())
      }),
    expr[PosExplode](
      "Given an input array produces a sequence of rows for each value in the array",
      ExprChecks.unaryProject(
        // Here is a walk-around representation, since multi-level nested type is not supported yet.
        // related issue: https://github.com/NVIDIA/spark-rapids/issues/1901
        TypeSig.ARRAY.nested(TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128 +
            TypeSig.BINARY + TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.MAP),
        TypeSig.ARRAY.nested(TypeSig.all),
        (TypeSig.ARRAY + TypeSig.MAP).nested(TypeSig.commonCudfTypes + TypeSig.NULL +
            TypeSig.DECIMAL_128 + TypeSig.BINARY + TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.MAP),
        (TypeSig.ARRAY + TypeSig.MAP).nested(TypeSig.all)),
      (a, conf, p, r) => new GeneratorExprMeta[PosExplode](a, conf, p, r) {
        override val supportOuter: Boolean = true
        override def convertToGpuImpl(): GpuExpression =
          GpuPosExplode(childExprs.head.convertToGpu())
      }),
    expr[Stack](
      "Separates expr1, ..., exprk into n rows.",
      ExprChecks.projectOnly(
        TypeSig.ARRAY.nested(TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128 +
            TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.MAP),
        TypeSig.ARRAY.nested(TypeSig.all),
        Seq(ParamCheck("n", TypeSig.lit(TypeEnum.INT), TypeSig.INT)),
        Some(RepeatingParamCheck("expr",
          (TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128 +
              TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.MAP).nested(),
          TypeSig.all))),
      (a, conf, p, r) => new GpuStackMeta(a, conf, p, r)
    ),
    expr[ReplicateRows](
      "Given an input row replicates the row N times",
      ExprChecks.projectOnly(
        TypeSig.ARRAY.nested(TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128 +
            TypeSig.ARRAY + TypeSig.STRUCT),
        TypeSig.ARRAY.nested(TypeSig.all),
        repeatingParamCheck = Some(RepeatingParamCheck("input",
          (TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128 +
              TypeSig.ARRAY + TypeSig.STRUCT).nested(),
          TypeSig.all))),
      (a, conf, p, r) => new ReplicateRowsExprMeta[ReplicateRows](a, conf, p, r) {
        override def convertToGpu(childExpr: Seq[Expression]): GpuExpression =
          GpuReplicateRows(childExpr)
      }),
    expr[CollectList](
      "Collect a list of non-unique elements, not supported in reduction",
      ExprChecks.fullAgg(
        TypeSig.ARRAY.nested(TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.BINARY +
            TypeSig.NULL + TypeSig.STRUCT + TypeSig.ARRAY + TypeSig.MAP)
            .withPsNote(TypeEnum.ARRAY, "window operations are disabled by default due " +
                "to extreme memory usage"),
        TypeSig.ARRAY.nested(TypeSig.all),
        Seq(ParamCheck("input",
          (TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.BINARY +
              TypeSig.NULL + TypeSig.STRUCT + TypeSig.ARRAY + TypeSig.MAP).nested(),
          TypeSig.all))),
      (c, conf, p, r) => new TypedImperativeAggExprMeta[CollectList](c, conf, p, r) {
        override def tagAggForGpu(): Unit = {
          if (context == WindowAggExprContext && !this.conf.isWindowCollectListEnabled) {
            willNotWorkOnGpu("collect_list is disabled for window operations because " +
                "the output explodes in size proportional to the window size squared. If " +
                "you know the window is small you can try it by setting " +
                s"${RapidsConf.ENABLE_WINDOW_COLLECT_LIST} to true")
          }
        }

        override def convertToGpu(childExprs: Seq[Expression]): GpuExpression =
          GpuCollectList(childExprs.head, c.mutableAggBufferOffset, c.inputAggBufferOffset,
            TypeUtilsShims.collectListIgnoreNulls(c))

        override def aggBufferAttribute: AttributeReference = {
          val aggBuffer = c.aggBufferAttributes.head
          aggBuffer.copy(dataType = c.dataType)(aggBuffer.exprId, aggBuffer.qualifier)
        }

        override def createCpuToGpuBufferConverter(): CpuToGpuAggregateBufferConverter =
          new CpuToGpuCollectBufferConverter(c.child.dataType,
            !TypeUtilsShims.collectListIgnoreNulls(c))

        override def createGpuToCpuBufferConverter(): GpuToCpuAggregateBufferConverter =
          new GpuToCpuCollectBufferConverter()

        override val supportBufferConversion: Boolean = true

        // Last does not overflow, so it doesn't need the ANSI check
        override val needsAnsiCheck: Boolean = false
      }),
    expr[CollectSet](
      "Collect a set of unique elements, not supported in reduction",
      ExprChecks.fullAgg(
        TypeSig.ARRAY.nested(TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 +
            TypeSig.NULL + TypeSig.STRUCT + TypeSig.ARRAY)
            .withPsNote(TypeEnum.ARRAY, "window operations are disabled by default due " +
                "to extreme memory usage"),
        TypeSig.ARRAY.nested(TypeSig.all),
        Seq(ParamCheck("input",
          (TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 +
            TypeSig.NULL +
            TypeSig.STRUCT +
            TypeSig.ARRAY).nested(),
          TypeSig.all))),
      (c, conf, p, r) => new TypedImperativeAggExprMeta[CollectSet](c, conf, p, r) {
        override def tagAggForGpu(): Unit = {
          if (context == WindowAggExprContext && !this.conf.isWindowCollectSetEnabled) {
            willNotWorkOnGpu("collect_set is disabled for window operations because " +
                "the output can explode in size proportional to the window size squared. If " +
                "you know the window is small you can try it by setting " +
                s"${RapidsConf.ENABLE_WINDOW_COLLECT_SET} to true")
          }
        }

        override def convertToGpu(childExprs: Seq[Expression]): GpuExpression =
          GpuCollectSet(childExprs.head, c.mutableAggBufferOffset, c.inputAggBufferOffset)

        override def aggBufferAttribute: AttributeReference = {
          val aggBuffer = c.aggBufferAttributes.head
          aggBuffer.copy(dataType = c.dataType)(aggBuffer.exprId, aggBuffer.qualifier)
        }

        override def createCpuToGpuBufferConverter(): CpuToGpuAggregateBufferConverter =
          new CpuToGpuCollectBufferConverter(c.child.dataType)

        override def createGpuToCpuBufferConverter(): GpuToCpuAggregateBufferConverter =
          new GpuToCpuCollectBufferConverter()

        override val supportBufferConversion: Boolean = true

        // Last does not overflow, so it doesn't need the ANSI check
        override val needsAnsiCheck: Boolean = false
      }),
    expr[StddevPop](
      "Aggregation computing population standard deviation",
      ExprChecks.groupByOnly(
        TypeSig.DOUBLE, TypeSig.DOUBLE,
        Seq(ParamCheck("input", TypeSig.DOUBLE, TypeSig.DOUBLE))),
      (a, conf, p, r) => new AggExprMeta[StddevPop](a, conf, p, r) {
        override def convertToGpu(childExprs: Seq[Expression]): GpuExpression = {
          val legacyStatisticalAggregate = SQLConf.get.legacyStatisticalAggregate
          GpuStddevPop(childExprs.head, !legacyStatisticalAggregate)
        }
      }),
    expr[StddevSamp](
      "Aggregation computing sample standard deviation",
      ExprChecks.fullAgg(
          TypeSig.DOUBLE, TypeSig.DOUBLE,
          Seq(ParamCheck("input", TypeSig.DOUBLE,
            TypeSig.DOUBLE))),
        (a, conf, p, r) => new AggExprMeta[StddevSamp](a, conf, p, r) {
          override def convertToGpu(childExprs: Seq[Expression]): GpuExpression = {
            val legacyStatisticalAggregate = SQLConf.get.legacyStatisticalAggregate
            GpuStddevSamp(childExprs.head, !legacyStatisticalAggregate)
          }
        }),
    expr[VariancePop](
      "Aggregation computing population variance",
      ExprChecks.groupByOnly(
        TypeSig.DOUBLE, TypeSig.DOUBLE,
        Seq(ParamCheck("input", TypeSig.DOUBLE, TypeSig.DOUBLE))),
      (a, conf, p, r) => new AggExprMeta[VariancePop](a, conf, p, r) {
        override def convertToGpu(childExprs: Seq[Expression]): GpuExpression = {
          val legacyStatisticalAggregate = SQLConf.get.legacyStatisticalAggregate
          GpuVariancePop(childExprs.head, !legacyStatisticalAggregate)
        }
      }),
    expr[VarianceSamp](
      "Aggregation computing sample variance",
      ExprChecks.groupByOnly(
        TypeSig.DOUBLE, TypeSig.DOUBLE,
        Seq(ParamCheck("input", TypeSig.DOUBLE, TypeSig.DOUBLE))),
      (a, conf, p, r) => new AggExprMeta[VarianceSamp](a, conf, p, r) {
        override def convertToGpu(childExprs: Seq[Expression]): GpuExpression = {
          val legacyStatisticalAggregate = SQLConf.get.legacyStatisticalAggregate
          GpuVarianceSamp(childExprs.head, !legacyStatisticalAggregate)
        }
      }),
    expr[Percentile](
      "Aggregation computing exact percentile",
      ExprChecks.reductionAndGroupByAgg(
        // The output can be a single number or array depending on whether percentiles param
        // is a single number or an array.
        TypeSig.DOUBLE + TypeSig.ARRAY.nested(TypeSig.DOUBLE),
        TypeSig.DOUBLE + TypeSig.ARRAY.nested(TypeSig.DOUBLE),
        Seq(
          // ANSI interval types are new in Spark 3.2.0 and are not yet supported by the
          // current GPU implementation.
          ParamCheck("input", TypeSig.integral + TypeSig.fp, TypeSig.integral + TypeSig.fp),
          ParamCheck("percentage",
            TypeSig.lit(TypeEnum.DOUBLE) + TypeSig.ARRAY.nested(TypeSig.lit(TypeEnum.DOUBLE)),
            TypeSig.DOUBLE + TypeSig.ARRAY.nested(TypeSig.DOUBLE)),
          ParamCheck("frequency",
            TypeSig.LONG + TypeSig.ARRAY.nested(TypeSig.LONG),
            TypeSig.LONG + TypeSig.ARRAY.nested(TypeSig.LONG)))),
      (c, conf, p, r) => new TypedImperativeAggExprMeta[Percentile](c, conf, p, r) {
        override def tagAggForGpu(): Unit = {
          // Check if the input percentage can be supported on GPU.
          GpuOverrides.extractLit(childExprs(1).wrapped.asInstanceOf[Expression]) match {
            case None =>
              willNotWorkOnGpu("percentile on GPU only supports literal percentages")
            case Some(Literal(null, _)) =>
              willNotWorkOnGpu("percentile on GPU only supports non-null literal percentages")
            case Some(Literal(a: ArrayData, _)) => {
              if((0 until a.numElements).exists(a.isNullAt)) {
                willNotWorkOnGpu(
                  "percentile on GPU does not support percentage arrays containing nulls")
              }
              if (a.toDoubleArray().exists(percentage => percentage < 0.0 || percentage > 1.0)) {
                willNotWorkOnGpu(
                  "percentile requires the input percentages given in the range [0, 1]")
              }
            }
            case Some(_) => // This is fine
          }
        }

        override def convertToGpu(childExprs: Seq[Expression]): GpuExpression = {
          val exprMeta = p.get.asInstanceOf[BaseExprMeta[_]]
          val isReduction = exprMeta.context match {
            case ReductionAggExprContext => true
            case GroupByAggExprContext => false
            case _ => throw new IllegalStateException(
              s"Invalid aggregation context: ${exprMeta.context}")
          }
          GpuPercentile(childExprs.head, childExprs(1).asInstanceOf[GpuLiteral], childExprs(2),
            isReduction)
        }
        // Declare the data type of the internal buffer so it can be serialized and
        // deserialized correctly during shuffling.
        override def aggBufferAttribute: AttributeReference = {
          val aggBuffer = c.aggBufferAttributes.head
          val dataType: DataType = ArrayType(StructType(Seq(
            StructField("value", childExprs.head.dataType),
            StructField("frequency", LongType))), containsNull = false)
          aggBuffer.copy(dataType = dataType)(aggBuffer.exprId, aggBuffer.qualifier)
        }

        override val needsAnsiCheck: Boolean = false
        override val supportBufferConversion: Boolean = true
        override def createCpuToGpuBufferConverter(): CpuToGpuAggregateBufferConverter =
          CpuToGpuPercentileBufferConverter(childExprs.head.dataType)
        override def createGpuToCpuBufferConverter(): GpuToCpuAggregateBufferConverter =
          GpuToCpuPercentileBufferConverter(childExprs.head.dataType)
      }),
    expr[ApproximatePercentile](
      "Approximate percentile",
      ExprChecks.reductionAndGroupByAgg(
        // note that output can be single number or array depending on whether percentiles param
        // is a single number or an array
        TypeSig.gpuNumeric +
            TypeSig.ARRAY.nested(TypeSig.gpuNumeric),
        TypeSig.cpuNumeric + TypeSig.DATE + TypeSig.TIMESTAMP + TypeSig.ARRAY.nested(
          TypeSig.cpuNumeric + TypeSig.DATE + TypeSig.TIMESTAMP),
        Seq(
          ParamCheck("input",
            TypeSig.gpuNumeric,
            TypeSig.cpuNumeric + TypeSig.DATE + TypeSig.TIMESTAMP),
          ParamCheck("percentage",
            TypeSig.DOUBLE + TypeSig.ARRAY.nested(TypeSig.DOUBLE),
            TypeSig.DOUBLE + TypeSig.ARRAY.nested(TypeSig.DOUBLE)),
          ParamCheck("accuracy", TypeSig.INT, TypeSig.INT))),
      (c, conf, p, r) => new TypedImperativeAggExprMeta[ApproximatePercentile](c, conf, p, r) {

        override def tagAggForGpu(): Unit = {
          // check if the percentile expression can be supported on GPU
          childExprs(1).wrapped match {
            case lit: Literal => lit.value match {
              case null =>
                willNotWorkOnGpu(
                  "approx_percentile on GPU only supports non-null literal percentiles")
              case a: ArrayData if a.numElements == 0 =>
                willNotWorkOnGpu(
                  "approx_percentile on GPU does not support empty percentiles arrays")
              case a: ArrayData if (0 until a.numElements).exists(a.isNullAt) =>
                willNotWorkOnGpu(
                  "approx_percentile on GPU does not support percentiles arrays containing nulls")
              case _ =>
                // this is fine
            }
            case _ =>
              willNotWorkOnGpu("approx_percentile on GPU only supports literal percentiles")
          }
        }

        override def convertToGpu(childExprs: Seq[Expression]): GpuExpression =
          GpuApproximatePercentile(childExprs.head,
              childExprs(1).asInstanceOf[GpuLiteral],
              childExprs(2).asInstanceOf[GpuLiteral])

        override def aggBufferAttribute: AttributeReference = {
          // Spark's ApproxPercentile has an aggregation buffer named "buf" with type "BinaryType"
          // so we need to replace that here with the GPU aggregation buffer reference, which is
          // a t-digest type
          val aggBuffer = c.aggBufferAttributes.head
          aggBuffer.copy(dataType = CudfTDigest.dataType)(aggBuffer.exprId, aggBuffer.qualifier)
        }
      }).incompat("the GPU implementation of approx_percentile is not bit-for-bit " +
          s"compatible with Apache Spark"),
    expr[GetJsonObject](
      "Extracts a json object from path",
      ExprChecks.projectOnly(
        TypeSig.STRING, TypeSig.STRING, Seq(ParamCheck("json", TypeSig.STRING, TypeSig.STRING),
          ParamCheck("path", TypeSig.lit(TypeEnum.STRING), TypeSig.STRING))),
      (a, conf, p, r) => new GpuGetJsonObjectMeta(a, conf, p, r)),
    expr[JsonToStructs](
      "Returns a struct value with the given `jsonStr` and `schema`",
      ExprChecks.projectOnly(
        TypeSig.STRUCT.nested(jsonStructReadTypes) +
          TypeSig.MAP.nested(TypeSig.STRING + TypeSig.ARRAY.nested(TypeSig.STRING))
            .withPsNote(TypeEnum.MAP,
          "MAP only supports keys of STRING type and values that are of STRING type " +
            "or ARRAY of STRING type, and is only supported at the top level"),
        (TypeSig.STRUCT + TypeSig.MAP + TypeSig.ARRAY).nested(TypeSig.all),
        Seq(ParamCheck("jsonStr", TypeSig.STRING, TypeSig.STRING))),
      (a, conf, p, r) => new UnaryExprMeta[JsonToStructs](a, conf, p, r) {
        def hasDuplicateFieldNames(dt: DataType): Boolean =
          TrampolineUtil.dataTypeExistsRecursively(dt, {
            case st: StructType =>
              val fn = st.fieldNames
              fn.length != fn.distinct.length
            case _ => false
          })

        def hasDateTimeType(dt: DataType): Boolean =
          TrampolineUtil.dataTypeExistsRecursively(dt, t =>
            t.isInstanceOf[DateType] || t.isInstanceOf[TimestampType]
          )

        override def tagExprForGpu(): Unit = {
          a.schema match {
            // from_json to a MAP is a "raw" extraction: values (and, for ARRAY<STRING>, array
            // elements) are raw JSON text. Verified vs Spark 3.5.5, it diverges on only two cases:
            // escapes are not unescaped, and object/nested-array elements stay as raw JSON
            // substrings. Scalar elements, whole-row null on a non-array value, and
            // document-order duplicate keys all match Spark. See docs/compatibility.md and
            // GpuJsonToStructs.
            case MapType(StringType, StringType, _) => ()
            case MapType(StringType, ArrayType(StringType, _), _) => ()
            case st: StructType =>
              if (hasDuplicateFieldNames(st)) {
                willNotWorkOnGpu("from_json on GPU does not support duplicate field " +
                    "names in a struct")
              }
              if (hasDateTimeType(st) && !this.conf.isJsonDateTimeReadEnabled) {
                willNotWorkOnGpu("from_json on GPU does not support DateType or TimestampType" +
                  " by default due to compatibility. " +
                  "Set `spark.rapids.sql.json.read.datetime.enabled` to `true` to enable them.")
              }
            case _ =>
              willNotWorkOnGpu("from_json on GPU only supports MapType<StringType, StringType>, " +
                "MapType<StringType, ArrayType[StringType]>, or StructType schema")
          }
          GpuJsonScan.tagSupport(SQLConf.get, JsonToStructsReaderType, a.dataType, a.dataType,
            a.options, this)
        }

        override def convertToGpu(child: Expression): GpuExpression =
          // GPU implementation currently does not support duplicated json key names in input
          GpuJsonToStructs(a.schema, a.options, child, a.timeZoneId)
      }),
    expr[StructsToJson](
      "Converts structs to JSON text format",
      ExprChecks.projectOnly(
        TypeSig.STRING,
        TypeSig.STRING,
        Seq(ParamCheck("struct",
          (TypeSig.BOOLEAN + TypeSig.STRING + TypeSig.integral + TypeSig.FLOAT +
            TypeSig.DOUBLE + TypeSig.DATE + TypeSig.TIMESTAMP +
            TypeSig.DECIMAL_128 +
            TypeSig.STRUCT + TypeSig.ARRAY + TypeSig.MAP).nested(),
          (TypeSig.BOOLEAN + TypeSig.STRING + TypeSig.integral + TypeSig.FLOAT +
            TypeSig.DOUBLE + TypeSig.DATE + TypeSig.TIMESTAMP +
            TypeSig.DECIMAL_128 +
            TypeSig.STRUCT + TypeSig.ARRAY + TypeSig.MAP).nested()
        ))),
      (a, conf, p, r) => new GpuStructsToJsonMeta(a, conf, p, r))
        .disabledByDefault("it is currently in beta and undergoes continuous enhancements."+
      " Please consult the "+
      "[compatibility documentation](../compatibility.md#json-supporting-types)"+
      " to determine whether you can enable this configuration for your use case"),
    expr[JsonTuple](
      "Returns a tuple like the function get_json_object, but it takes multiple names. " +
        "All the input parameters and output column types are string.",
      ExprChecks.projectOnly(
        TypeSig.ARRAY.nested(TypeSig.STRUCT + TypeSig.STRING),
        TypeSig.ARRAY.nested(TypeSig.STRUCT + TypeSig.STRING),
        Seq(ParamCheck("json", TypeSig.STRING, TypeSig.STRING)),
        Some(RepeatingParamCheck("field", TypeSig.lit(TypeEnum.STRING), TypeSig.STRING))),
      (a, conf, p, r) => new GeneratorExprMeta[JsonTuple](a, conf, p, r) {
        override def tagExprForGpu(): Unit = {
          if (childExprs.length >= 50) {
            // If the number of field parameters is too large, fall back to CPU to avoid
            // potential performance problems.
            willNotWorkOnGpu("JsonTuple with large number of fields is not supported on GPU")
          }
        }
        override def convertToGpuImpl(): GpuExpression =
          GpuJsonTuple(childExprs.map(_.convertToGpu()))
      }
    ),
    expr[org.apache.spark.sql.execution.ScalarSubquery](
      "Subquery that will return only one row and one column",
      ExprChecks.projectOnly(
        (TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128 + TypeSig.BINARY +
            TypeSig.ARRAY + TypeSig.MAP + TypeSig.STRUCT).nested(),
        TypeSig.all,
        Nil, None),
      (a, conf, p, r) =>
        new ExprMeta[org.apache.spark.sql.execution.ScalarSubquery](a, conf, p, r) {
          override def convertToGpuImpl(): GpuExpression = GpuScalarSubquery(a.plan, a.exprId)
        }
    ),
    expr[CreateMap](
      desc = "Create a map",
      CreateMapCheck,
      (a, conf, p, r) => new ExprMeta[CreateMap](a, conf, p, r) {
        override def convertToGpuImpl(): GpuExpression =
          GpuCreateMap(childExprs.map(_.convertToGpu()))
      }
    ),
    expr[Sequence](
      desc = "Sequence",
      ExprChecks.projectOnly(
        TypeSig.ARRAY.nested(TypeSig.integral), TypeSig.ARRAY.nested(TypeSig.integral +
          TypeSig.TIMESTAMP + TypeSig.DATE),
        Seq(ParamCheck("start", TypeSig.integral, TypeSig.integral + TypeSig.TIMESTAMP +
          TypeSig.DATE),
          ParamCheck("stop", TypeSig.integral, TypeSig.integral + TypeSig.TIMESTAMP +
            TypeSig.DATE)),
        Some(RepeatingParamCheck("step", TypeSig.integral, TypeSig.integral + TypeSig.CALENDAR))),
      (a, conf, p, r) => new GpuSequenceMeta(a, conf, p, r)
    ),
    expr[BitLength](
      "The bit length of string data",
      ExprChecks.unaryProject(
        TypeSig.INT, TypeSig.INT,
        TypeSig.STRING, TypeSig.STRING + TypeSig.BINARY),
      (a, conf, p, r) => new UnaryExprMeta[BitLength](a, conf, p, r) {
        override def convertToGpu(child: Expression): GpuExpression = GpuBitLength(child)
      }),
    expr[OctetLength](
      "The byte length of string data",
      ExprChecks.unaryProject(
        TypeSig.INT, TypeSig.INT,
        TypeSig.STRING, TypeSig.STRING + TypeSig.BINARY),
      (a, conf, p, r) => new UnaryExprMeta[OctetLength](a, conf, p, r) {
        override def convertToGpu(child: Expression): GpuExpression = GpuOctetLength(child)
      }),
    expr[Ascii](
      "The numeric value of the first character of string data.",
      ExprChecks.unaryProject(TypeSig.INT, TypeSig.INT, TypeSig.STRING, TypeSig.STRING),
      (a, conf, p, r) => new UnaryExprMeta[Ascii](a, conf, p, r) {
        override def convertToGpu(child: Expression): GpuExpression = GpuAscii(child)
      }).disabledByDefault("it only supports strings starting with ASCII or Latin-1 characters " +
        "after Spark 3.2.3, 3.3.1 and 3.4.0. Otherwise the results will not match the CPU."),
    expr[GetArrayStructFields](
      "Extracts the `ordinal`-th fields of all array elements for the data with the type of" +
        " array of struct",
      ExprChecks.unaryProject(
        TypeSig.ARRAY.nested((TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL +
            TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.MAP + TypeSig.BINARY).nested()),
        TypeSig.ARRAY.nested(TypeSig.all),
        // we should allow all supported types for the children types signature of the nested
        // struct, even only a struct child is allowed for the array here. Since TypeSig supports
        // only one level signature for nested type.
        TypeSig.ARRAY.nested((TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL +
            TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.MAP + TypeSig.BINARY).nested()),
        TypeSig.ARRAY.nested(TypeSig.all)),
      (e, conf, p, r) => new GpuGetArrayStructFieldsMeta(e, conf, p, r)
    ),
    expr[DynamicPruningExpression](
      "Dynamic pruning expression marker",
      ExprChecks.unaryProject(TypeSig.all, TypeSig.all, TypeSig.BOOLEAN, TypeSig.BOOLEAN),
      (a, conf, p, r) => new UnaryExprMeta[DynamicPruningExpression](a, conf, p, r) {
        override def convertToGpu(child: Expression): GpuExpression = {
          GpuDynamicPruningExpression(child)
        }
      }),
    expr[HyperLogLogPlusPlus](
      "Aggregation approximate count distinct",
      ExprChecks.reductionAndGroupByAgg(TypeSig.LONG, TypeSig.LONG,
        // HyperLogLogPlusPlus depends on Xxhash64
        // HyperLogLogPlusPlus supports all the types that Xxhash 64 supports
        Seq(ParamCheck("input",XxHash64Shims.supportedTypes, TypeSig.all))),
      (a, conf, p, r) => new UnaryExprMeta[HyperLogLogPlusPlus](a, conf, p, r) {

        // It's the same as Xxhash64
        override def tagExprForGpu(): Unit = {
          val maxDepth = a.children.map(
            c => XxHash64Utils.computeMaxStackSize(c.dataType)).max
          if (maxDepth > XxHash64Utils.MAX_STACK_DEPTH) {
            willNotWorkOnGpu(s"The data type requires a stack depth of $maxDepth, " +
                s"which exceeds the GPU limit of ${XxHash64Utils.MAX_STACK_DEPTH}. " +
                "The algorithm to calculate stack depth: " +
                "1: Primitive type counts 1 depth; " +
                "2: Array of Structure counts:  1  + depthOf(Structure); " +
                "3: Array of Other counts: depthOf(Other); " +
                "4: Structure counts: 1 + max of depthOf(child); " +
                "5: Map counts: 2 + max(depthOf(key), depthOf(value)); "
            )
          }
          val precision = GpuHyperLogLogPlusPlus.computePrecision(a.relativeSD)
          // Spark supports precision range: [4, Infinity)
          // cuCollection supports precision range: [4, 18]
          // Spark-Rapids only supports precision range: [4, 14],
          // GPU does not perform well for precision > 14.
          if (precision < 4 || precision > 14) {
            willNotWorkOnGpu(s"The precision $precision from relativeSD ${a.relativeSD} is out of" +
              s" range, GPU only supports precision range [4, 14].")
          }
        }

        override def convertToGpu(child: Expression): GpuExpression = {
          GpuHyperLogLogPlusPlus(child, a.relativeSD)
        }
      }
    ),
    expr[Uuid](
      desc = "Uuid",
      ExprChecks.projectOnly(TypeSig.STRING, TypeSig.STRING),
      (a, conf, p, r) => new GpuUuidMeta(a, conf, p, r)
    ),
    expr[StaticInvoke](
      desc = "StaticInvoke",
      StaticInvokeCheck,
      (a, conf, p, r) => new StaticInvokeMeta(a, conf, p, r)
    ).note("The supported types are not deterministic since it's a dynamic expression"),
    SparkShimImpl.ansiCastRule
  ).collect { case r if r != null => (r.getClassFor.asSubclass(classOf[Expression]), r)}.toMap

  // Shim expressions should be last to allow overrides with shim-specific versions
  val expressions: Map[Class[_ <: Expression], ExprRule[_ <: Expression]] =
    commonExpressions ++ TimeStamp.getExprs ++ GpuHiveOverrides.exprs ++
        ZOrderRules.exprs ++ DecimalArithmeticOverrides.exprs ++
        BloomFilterShims.exprs ++ StringDecodeShims.exprs ++
        InSubqueryShims.exprs ++ RaiseErrorShim.exprs ++
        ExternalSource.exprRules ++ SparkShimImpl.getExprs

  def wrapScan[INPUT <: Scan](
      scan: INPUT,
      conf: RapidsConf,
      parent: Option[RapidsMeta[_, _, _]]): ScanMeta[INPUT] =
    scans.get(scan.getClass)
      .map(r => r.wrap(scan, conf, parent, r).asInstanceOf[ScanMeta[INPUT]])
      .getOrElse(new RuleNotFoundScanMeta(scan, conf, parent))

  val commonScans: Map[Class[_ <: Scan], ScanRule[_ <: Scan]] = Seq(
    GpuOverrides.scan[CSVScan](
      "CSV parsing",
      (a, conf, p, r) => new ScanMeta[CSVScan](a, conf, p, r) {
        override def tagSelfForGpu(): Unit = GpuCSVScan.tagSupport(this)

        override def convertToGpu(): GpuScan =
          GpuCSVScan(a.sparkSession,
            a.fileIndex,
            a.dataSchema,
            a.readDataSchema,
            a.readPartitionSchema,
            a.options,
            a.partitionFilters,
            a.dataFilters,
            this.conf.maxReadBatchSizeRows,
            this.conf.maxReadBatchSizeBytes,
            this.conf.maxGpuColumnSizeBytes)
      }),
    GpuOverrides.scan[JsonScan](
      "Json parsing",
      (a, conf, p, r) => new ScanMeta[JsonScan](a, conf, p, r) {
        override def tagSelfForGpu(): Unit = GpuJsonScan.tagSupport(this)

        override def convertToGpu(): GpuScan =
          GpuJsonScan(a.sparkSession,
            a.fileIndex,
            a.dataSchema,
            a.readDataSchema,
            a.readPartitionSchema,
            a.options,
            a.partitionFilters,
            a.dataFilters,
            this.conf.maxReadBatchSizeRows,
            this.conf.maxReadBatchSizeBytes,
            this.conf.maxGpuColumnSizeBytes)
      })).map(r => (r.getClassFor.asSubclass(classOf[Scan]), r)).toMap

  val scans: Map[Class[_ <: Scan], ScanRule[_ <: Scan]] =
    commonScans ++ SparkShimImpl.getScans ++ ExternalSource.getScans

  def wrapPart[INPUT <: Partitioning](
      part: INPUT,
      conf: RapidsConf,
      parent: Option[RapidsMeta[_, _, _]]): PartMeta[INPUT] =
    parts.get(part.getClass)
      .map(r => r.wrap(part, conf, parent, r).asInstanceOf[PartMeta[INPUT]])
      .getOrElse(new RuleNotFoundPartMeta(part, conf, parent))

  private val commonParts : Map[Class[_ <: Partitioning], PartRule[_ <: Partitioning]] = Seq(
    part[HashPartitioning](
      "Hash based partitioning",
      // This needs to match what murmur3 supports.
      PartChecks(RepeatingParamCheck("hash_key",
        (TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128 +
            TypeSig.STRUCT + TypeSig.ARRAY).nested() +
            TypeSig.psNote(TypeEnum.ARRAY, "Arrays of structs are not supported"),
        TypeSig.all)
      ),
      (hp, conf, p, r) => new PartMeta[HashPartitioning](hp, conf, p, r) {
        override val childExprs: Seq[BaseExprMeta[_]] =
          hp.expressions.map(GpuOverrides.wrapExpr(_, this.conf, Some(this)))

        private lazy val hashMode = GpuHashPartitioningBase.hashModeFromCpu(hp, this.conf)

        override def tagPartForGpu(): Unit = {
          this.hashMode match {
            case HiveMode =>
              val hh = HiveHash(hp.expressions)
              val hfMeta = GpuOverrides.wrapExpr(hh, this.conf, None)
              hfMeta.tagForGpu()
              if (!hfMeta.canThisBeReplaced) {
                willNotWorkOnGpu(s"the hash function: ${hh.getClass.getSimpleName}" +
                  s" can not run on GPU. Details: ${hfMeta.explain(all = false)}")
              }
            case Murmur3Mode =>
              val arrayWithStructsHashing = hp.expressions.exists(e =>
                TrampolineUtil.dataTypeExistsRecursively(e.dataType,
                  {
                    case ArrayType(_: StructType, _) => true
                    case _ => false
                  })
              )
              if (arrayWithStructsHashing) {
                willNotWorkOnGpu("hashing arrays with structs is not supported")
              }
            case _ =>
              willNotWorkOnGpu(s"Hash function $hashMode is not supported on GPU")
          }
        }

        override def convertToGpu(): GpuPartitioning =
          GpuHashPartitioning(childExprs.map(_.convertToGpu()), hp.numPartitions,
            this.hashMode)
      }),
    part[RangePartitioning](
      "Range partitioning",
      PartChecks(RepeatingParamCheck("order_key",
        pluginSupportedOrderableSig + TypeSig.ARRAY.nested(gpuCommonTypes)
           .withPsNote(TypeEnum.ARRAY, "STRUCT is not supported as a child type for ARRAY"),
        TypeSig.orderable)),
      (rp, conf, p, r) => new PartMeta[RangePartitioning](rp, conf, p, r) {
        override val childExprs: Seq[BaseExprMeta[_]] =
          rp.ordering.map(GpuOverrides.wrapExpr(_, this.conf, Some(this)))

        override def convertToGpu(): GpuPartitioning = {
          if (rp.numPartitions > 1) {
            val gpuOrdering = childExprs.map(_.convertToGpu()).asInstanceOf[Seq[SortOrder]]
            GpuRangePartitioning(gpuOrdering, rp.numPartitions)
          } else {
            GpuSinglePartitioning
          }
        }
      }),
    part[RoundRobinPartitioning](
      "Round robin partitioning",
      PartChecks(),
      (rrp, conf, p, r) => new PartMeta[RoundRobinPartitioning](rrp, conf, p, r) {
        override def convertToGpu(): GpuPartitioning = {
          GpuRoundRobinPartitioning(rrp.numPartitions)
        }
      }),
    part[SinglePartition.type](
      "Single partitioning",
      PartChecks(),
      (sp, conf, p, r) => new PartMeta[SinglePartition.type](sp, conf, p, r) {
        override def convertToGpu(): GpuPartitioning = GpuSinglePartitioning
      })
  ).map(r => (r.getClassFor.asSubclass(classOf[Partitioning]), r)).toMap

  val parts : Map[Class[_ <: Partitioning], PartRule[_ <: Partitioning]] =
    commonParts ++ SparkShimImpl.getPartitionings

  def wrapDataWriteCmds[INPUT <: DataWritingCommand](
      writeCmd: INPUT,
      conf: RapidsConf,
      parent: Option[RapidsMeta[_, _, _]]): DataWritingCommandMeta[INPUT] =
    dataWriteCmds.get(writeCmd.getClass)
      .map(r => r.wrap(writeCmd, conf, parent, r).asInstanceOf[DataWritingCommandMeta[INPUT]])
      .getOrElse(new RuleNotFoundDataWritingCommandMeta(writeCmd, conf, parent))

  val commonDataWriteCmds: Map[Class[_ <: DataWritingCommand],
      DataWritingCommandRule[_ <: DataWritingCommand]] = Seq(
    dataWriteCmd[InsertIntoHadoopFsRelationCommand](
      "Write to Hadoop filesystem",
      (a, conf, p, r) => new InsertIntoHadoopFsRelationCommandMeta(a, conf, p, r))
  ).map(r => (r.getClassFor.asSubclass(classOf[DataWritingCommand]), r)).toMap

  val dataWriteCmds: Map[Class[_ <: DataWritingCommand],
      DataWritingCommandRule[_ <: DataWritingCommand]] =
    commonDataWriteCmds ++ GpuHiveOverrides.dataWriteCmds ++ SparkShimImpl.getDataWriteCmds

  def runnableCmd[INPUT <: RunnableCommand](
      desc: String,
      doWrap: (INPUT, RapidsConf, Option[RapidsMeta[_, _, _]], DataFromReplacementRule)
          => RunnableCommandMeta[INPUT])
      (implicit tag: ClassTag[INPUT]): RunnableCommandRule[INPUT] = {
    require(desc != null)
    require(doWrap != null)
    new RunnableCommandRule[INPUT](doWrap, desc, tag)
  }

  def wrapRunnableCmd[INPUT <: RunnableCommand](
      cmd: INPUT,
      conf: RapidsConf,
      parent: Option[RapidsMeta[_, _, _]]): RunnableCommandMeta[INPUT] =
    runnableCmds.get(cmd.getClass)
        .map(r => r.wrap(cmd, conf, parent, r).asInstanceOf[RunnableCommandMeta[INPUT]])
        .getOrElse(new RuleNotFoundRunnableCommandMeta(cmd, conf, parent))

  val commonRunnableCmds: Map[Class[_ <: RunnableCommand],
    RunnableCommandRule[_ <: RunnableCommand]] =
    Seq(
      runnableCmd[SaveIntoDataSourceCommand](
        "Write to a data source",
        (a, conf, p, r) => new SaveIntoDataSourceCommandMeta(a, conf, p, r))
    ).map(r => (r.getClassFor.asSubclass(classOf[RunnableCommand]), r)).toMap

  val runnableCmds = commonRunnableCmds ++
    GpuHiveOverrides.runnableCmds ++
      ExternalSource.runnableCmds ++
      SparkShimImpl.getRunnableCmds

  def wrapPlan[INPUT <: SparkPlan](
      plan: INPUT,
      conf: RapidsConf,
      parent: Option[RapidsMeta[_, _, _]]): SparkPlanMeta[INPUT]  =
    execs.get(plan.getClass)
      .map(r => r.wrap(plan, conf, parent, r).asInstanceOf[SparkPlanMeta[INPUT]])
      .getOrElse(new RuleNotFoundSparkPlanMeta(plan, conf, parent))

  val commonExecs: Map[Class[_ <: SparkPlan], ExecRule[_ <: SparkPlan]] = Seq(
    exec[GenerateExec] (
      "The backend for operations that generate more output rows than input rows like explode",
      ExecChecks(
        (TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128 + TypeSig.BINARY +
            TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.MAP).nested(),
        TypeSig.all),
      (gen, conf, p, r) => new GpuGenerateExecSparkPlanMeta(gen, conf, p, r)),
    exec[ProjectExec](
      "The backend for most select, withColumn and dropColumn statements",
      ExecChecks(
        (TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.STRUCT + TypeSig.MAP +
            TypeSig.ARRAY + TypeSig.DECIMAL_128 + TypeSig.BINARY +
            GpuTypeShims.additionalCommonOperatorSupportedTypes).nested(),
        TypeSig.all),
      (proj, conf, p, r) => new GpuProjectExecMeta(proj, conf, p, r)),
    exec[RangeExec](
      "The backend for range operator",
      ExecChecks(TypeSig.LONG, TypeSig.LONG),
      (range, conf, p, r) => {
        new SparkPlanMeta[RangeExec](range, conf, p, r) {
          override def convertToGpu(): GpuExec =
            GpuRangeExec(range.start, range.end, range.step, range.numSlices, range.output,
              this.conf.gpuTargetBatchSizeBytes)
        }
      }),
    exec[BatchScanExec](
      "The backend for most file input",
      ExecChecks(
        (TypeSig.commonCudfTypes + TypeSig.STRUCT + TypeSig.MAP + TypeSig.ARRAY +
          TypeSig.DECIMAL_128 + TypeSig.BINARY).nested(),
        TypeSig.all),
      (p, conf, parent, r) => new BatchScanExecMeta(p, conf, parent, r)),
    exec[CoalesceExec](
      "The backend for the dataframe coalesce method",
      ExecChecks((gpuCommonTypes + TypeSig.STRUCT + TypeSig.ARRAY +
          TypeSig.MAP + TypeSig.BINARY + GpuTypeShims.additionalArithmeticSupportedTypes).nested(),
        TypeSig.all),
      (coalesce, conf, parent, r) => new SparkPlanMeta[CoalesceExec](coalesce, conf, parent, r) {
        override def convertToGpu(): GpuExec =
          GpuCoalesceExec(coalesce.numPartitions, childPlans.head.convertIfNeeded())
      }),
    exec[DataWritingCommandExec](
      "Writing data",
      ExecChecks((TypeSig.commonCudfTypes + TypeSig.DECIMAL_128.withPsNote(
          TypeEnum.DECIMAL, "128bit decimal only supported for Orc and Parquet") +
          TypeSig.STRUCT + TypeSig.MAP + TypeSig.ARRAY + TypeSig.BINARY +
          GpuTypeShims.additionalCommonOperatorSupportedTypes).nested(),
        TypeSig.all),
      (p, conf, parent, r) => new SparkPlanMeta[DataWritingCommandExec](p, conf, parent, r) {
        override val childDataWriteCmds: scala.Seq[DataWritingCommandMeta[_]] =
          Seq(GpuOverrides.wrapDataWriteCmds(p.cmd, this.conf, Some(this)))

        override def convertToGpu(): GpuExec =
          GpuDataWritingCommandExec(childDataWriteCmds.head.convertToGpu(),
            childPlans.head.convertIfNeeded())
      }),
    exec[ExecutedCommandExec](
      "Eagerly executed commands",
      ExecChecks(TypeSig.all, TypeSig.all),
      (p, conf, parent, r) => new ExecutedCommandExecMeta(p, conf, parent, r)),
    exec[TakeOrderedAndProjectExec](
      "Take the first limit elements as defined by the sortOrder, and do projection if needed",
      // The SortOrder TypeSig will govern what types can actually be used as sorting key data type.
      // The types below are allowed as inputs and outputs.
      ExecChecks((pluginSupportedOrderableSig +
          TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.MAP).nested(), TypeSig.all),
      GpuTakeOrderedAndProjectExecMeta),
    exec[LocalLimitExec](
      "Per-partition limiting of results",
      ExecChecks((TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL +
          TypeSig.STRUCT + TypeSig.ARRAY + TypeSig.MAP).nested(),
        TypeSig.all),
      (localLimitExec, conf, p, r) =>
        new SparkPlanMeta[LocalLimitExec](localLimitExec, conf, p, r) {
          override def convertToGpu(): GpuExec =
            GpuLocalLimitExec(localLimitExec.limit, childPlans.head.convertIfNeeded())
        }),
    exec[GlobalLimitExec](
      "Limiting of results across partitions",
      ExecChecks((TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL +
          TypeSig.STRUCT + TypeSig.ARRAY + TypeSig.MAP).nested(),
        TypeSig.all),
      (globalLimitExec, conf, p, r) =>
        new SparkPlanMeta[GlobalLimitExec](globalLimitExec, conf, p, r) {
          override def convertToGpu(): GpuExec =
            GpuGlobalLimitExec(globalLimitExec.limit, childPlans.head.convertIfNeeded(), 0)
        }),
    exec[CollectLimitExec](
      "Reduce to single partition and apply limit",
      ExecChecks((TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL +
          TypeSig.STRUCT + TypeSig.ARRAY + TypeSig.MAP).nested(),
        TypeSig.all),
      (collectLimitExec, conf, p, r) => new GpuCollectLimitMeta(collectLimitExec, conf, p, r))
        .disabledByDefault("Collect Limit replacement can be slower on the GPU, if huge number " +
            "of rows in a batch it could help by limiting the number of rows transferred from " +
            "GPU to CPU"),
    exec[FilterExec](
      "The backend for most filter statements",
      ExecChecks((TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.STRUCT + TypeSig.MAP +
          TypeSig.ARRAY + TypeSig.DECIMAL_128 + TypeSig.BINARY +
          GpuTypeShims.additionalCommonOperatorSupportedTypes).nested(), TypeSig.all),
      GpuFilterExecMeta),
    exec[ShuffleExchangeExec](
      "The backend for most data being exchanged between processes",
      ExecChecks((TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128 + TypeSig.BINARY +
          TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.MAP +
          GpuTypeShims.additionalArithmeticSupportedTypes).nested()
          .withPsNote(TypeEnum.STRUCT, "Round-robin partitioning is not supported for nested " +
              s"structs if ${SQLConf.SORT_BEFORE_REPARTITION.key} is true")
          .withPsNote(
            Seq(TypeEnum.MAP),
            "Round-robin partitioning is not supported if " +
              s"${SQLConf.SORT_BEFORE_REPARTITION.key} is true"),
        TypeSig.all),
      (shuffle, conf, p, r) => new GpuShuffleMeta(shuffle, conf, p, r)),
    exec[UnionExec](
      "The backend for the union operator",
      ExecChecks((TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128 +
          TypeSig.MAP + TypeSig.ARRAY + TypeSig.STRUCT).nested()
        .withPsNote(TypeEnum.STRUCT,
          "unionByName will not optionally impute nulls for missing struct fields " +
          "when the column is a struct and there are non-overlapping fields"), TypeSig.all),
      (union, conf, p, r) => new SparkPlanMeta[UnionExec](union, conf, p, r) {
        override def convertToGpu(): GpuExec =
          GpuUnionExec(childPlans.map(_.convertIfNeeded()))
      }),
    exec[BroadcastExchangeExec](
      "The backend for broadcast exchange of data",
      ExecChecks((TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128 + TypeSig.BINARY +
          TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.MAP).nested(TypeSig.commonCudfTypes +
          TypeSig.NULL + TypeSig.DECIMAL_128 + TypeSig.STRUCT),
        TypeSig.all),
      (exchange, conf, p, r) => new GpuBroadcastMeta(exchange, conf, p, r)),
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
    exec[CartesianProductExec](
      "Implementation of join using brute force",
      ExecChecks((TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128 + TypeSig.BINARY +
          TypeSig.ARRAY + TypeSig.MAP + TypeSig.STRUCT)
          .nested(TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128 + TypeSig.BINARY +
              TypeSig.ARRAY + TypeSig.MAP + TypeSig.STRUCT),
        TypeSig.all),
      (join, conf, p, r) => new SparkPlanMeta[CartesianProductExec](join, conf, p, r) {
        val condition: Option[BaseExprMeta[_]] =
          join.condition.map(GpuOverrides.wrapExpr(_, this.conf, Some(this)))

        override val childExprs: Seq[BaseExprMeta[_]] = condition.toSeq

        override def convertToGpu(): GpuExec = {
          val Seq(left, right) = childPlans.map(_.convertIfNeeded())
          val joinExec = GpuCartesianProductExec(
            left,
            right,
            None,
            this.conf.gpuTargetBatchSizeBytes)
          // The GPU does not yet support conditional joins, so conditions are implemented
          // as a filter after the join when possible.
          condition.map(c => GpuFilterExec(c.convertToGpu(),
            joinExec)()).getOrElse(joinExec)
        }
      }),
    exec[HashAggregateExec](
      "The backend for hash based aggregations",
      ExecChecks(
        (TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128 + TypeSig.BINARY +
          TypeSig.MAP + TypeSig.ARRAY + TypeSig.STRUCT)
            .nested()
            .withPsNote(TypeEnum.MAP,
              "not allowed for grouping expressions")
            .withPsNote(TypeEnum.ARRAY,
              "not allowed for grouping expressions if containing Struct as child")
            .withPsNote(TypeEnum.STRUCT,
              "not allowed for grouping expressions if containing Array, Map, or Binary as child"),
        TypeSig.all),
      (agg, conf, p, r) => new GpuHashAggregateMeta(agg, conf, p, r)),
    exec[ObjectHashAggregateExec](
      "The backend for hash based aggregations supporting TypedImperativeAggregate functions",
      ExecChecks(
        // note that binary input is allowed here but there are additional checks later on to
        // check that we have can support binary in the context of aggregate buffer conversions
        (TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128 +
          TypeSig.MAP + TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.BINARY)
            .nested()
            .withPsNote(TypeEnum.BINARY, "not allowed for grouping expressions and " +
              "only allowed when aggregate buffers can be converted between CPU and GPU")
            .withPsNote(Seq(TypeEnum.ARRAY, TypeEnum.MAP),
              "not allowed for grouping expressions")
            .withPsNote(TypeEnum.STRUCT,
              "not allowed for grouping expressions if containing Array, Map, or Binary as child"),
        TypeSig.all),
      (agg, conf, p, r) => new GpuObjectHashAggregateExecMeta(agg, conf, p, r)),
    exec[ShuffledHashJoinExec](
      "Implementation of join using hashed shuffled data",
      JoinTypeChecks.equiJoinExecChecks,
      (join, conf, p, r) => new GpuShuffledHashJoinMeta(join, conf, p, r)),
    exec[SortAggregateExec](
      "The backend for sort based aggregations",
      ExecChecks(
        (TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128 +
            TypeSig.MAP + TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.BINARY)
            .nested()
            .withPsNote(Seq(TypeEnum.ARRAY, TypeEnum.MAP),
              "not allowed for grouping expressions")
            .withPsNote(TypeEnum.STRUCT,
              "not allowed for grouping expressions if containing Array, Map, or Binary as child"),
        TypeSig.all),
      (agg, conf, p, r) => new GpuSortAggregateExecMeta(agg, conf, p, r)),
    exec[SortExec](
      "The backend for the sort operator",
      // The SortOrder TypeSig will govern what types can actually be used as sorting key data type.
      // The types below are allowed as inputs and outputs.
      ExecChecks((pluginSupportedOrderableSig + TypeSig.ARRAY +
          TypeSig.STRUCT +TypeSig.MAP + TypeSig.BINARY).nested(), TypeSig.all),
      (sort, conf, p, r) => new GpuSortMeta(sort, conf, p, r)),
    exec[SortMergeJoinExec](
      "Sort merge join, replacing with shuffled hash join",
      JoinTypeChecks.equiJoinExecChecks,
      (join, conf, p, r) => new GpuSortMergeJoinMeta(join, conf, p, r)),
    exec[ExpandExec](
      "The backend for the expand operator",
      ExecChecks(
        (TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128 +
            TypeSig.STRUCT + TypeSig.ARRAY + TypeSig.MAP + TypeSig.BINARY).nested(),
        TypeSig.all),
      (expand, conf, p, r) => new GpuExpandExecMeta(expand, conf, p, r)),
    exec[WindowExec](
      "Window-operator backend",
      ExecChecks((TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128 +
          TypeSig.STRUCT + TypeSig.ARRAY + TypeSig.MAP + TypeSig.BINARY).nested(),
        TypeSig.all,
        Map("partitionSpec" ->
            InputCheck(
                TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128 +
                TypeSig.STRUCT.nested(TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128),
            TypeSig.all))),
      (windowOp, conf, p, r) =>
        new GpuWindowExecMeta(windowOp, conf, p, r)
    ),
    exec[SampleExec](
      "The backend for the sample operator",
      ExecChecks((TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.STRUCT + TypeSig.MAP +
        TypeSig.ARRAY + TypeSig.DECIMAL_128 + GpuTypeShims.additionalCommonOperatorSupportedTypes)
          .nested(), TypeSig.all),
      (sample, conf, p, r) => new GpuSampleExecMeta(sample, conf, p, r)
    ),
    exec[SubqueryBroadcastExec](
      "Plan to collect and transform the broadcast key values",
      ExecChecks(TypeSig.all, TypeSig.all),
      (s, conf, p, r) => new GpuSubqueryBroadcastMeta(s, conf, p, r)
    ),
    SparkShimImpl.aqeShuffleReaderExec,
    // AggregateInPandasExec renamed to ArrowAggregatePythonExec in Spark 4.1.0
    AggregateInPandasExecShims.execRule.orNull,
    exec[ArrowEvalPythonExec](
      "The backend of the Scalar Pandas UDFs. Accelerates the data transfer between the" +
        " Java process and the Python process. It also supports scheduling GPU resources" +
        " for the Python process when enabled",
      ExecChecks(
        (TypeSig.commonCudfTypes + TypeSig.ARRAY + TypeSig.STRUCT).nested(),
        TypeSig.all),
      (e, conf, p, r) =>
        new SparkPlanMeta[ArrowEvalPythonExec](e, conf, p, r) {
          val udfs: Seq[BaseExprMeta[PythonUDF]] =
            e.udfs.map(GpuOverrides.wrapExpr(_, this.conf, Some(this)))
          val resultAttrs: Seq[BaseExprMeta[Attribute]] =
            e.resultAttrs.map(GpuOverrides.wrapExpr(_, this.conf, Some(this)))
          override val childExprs: Seq[BaseExprMeta[_]] = udfs ++ resultAttrs

          override def replaceMessage: String = "partially run on GPU"
          override def noReplacementPossibleMessage(reasons: String): String =
            s"cannot run even partially on the GPU because $reasons"

          override def convertToGpu(): GpuExec =
            GpuArrowEvalPythonExec(udfs.map(_.convertToGpu()).asInstanceOf[Seq[GpuPythonUDF]],
              resultAttrs.map(_.convertToGpu()).asInstanceOf[Seq[Attribute]],
              childPlans.head.convertIfNeeded(),
              e.evalType)
        }),
    exec[FlatMapCoGroupsInPandasExec](
      "The backend for CoGrouped Aggregation Pandas UDF. Accelerates the data transfer" +
        " between the Java process and the Python process. It also supports scheduling GPU" +
        " resources for the Python process when enabled.",
      ExecChecks(TypeSig.commonCudfTypes, TypeSig.all),
      (flatCoPy, conf, p, r) => new GpuFlatMapCoGroupsInPandasExecMeta(flatCoPy, conf, p, r))
        .disabledByDefault("Performance is not ideal with many small groups"),
    exec[FlatMapGroupsInPandasExec](
      "The backend for Flat Map Groups Pandas UDF, Accelerates the data transfer between the" +
        " Java process and the Python process. It also supports scheduling GPU resources" +
        " for the Python process when enabled.",
      ExecChecks(TypeSig.commonCudfTypes, TypeSig.all),
      (flatPy, conf, p, r) => new GpuFlatMapGroupsInPandasExecMeta(flatPy, conf, p, r)),
    exec[MapInPandasExec](
      "The backend for Map Pandas Iterator UDF. Accelerates the data transfer between the" +
        " Java process and the Python process. It also supports scheduling GPU resources" +
        " for the Python process when enabled.",
      ExecChecks((TypeSig.commonCudfTypes + TypeSig.ARRAY + TypeSig.STRUCT).nested(),
        TypeSig.all),
      (mapPy, conf, p, r) => new GpuMapInPandasExecMeta(mapPy, conf, p, r)),
    exec[InMemoryTableScanExec](
      "Implementation of InMemoryTableScanExec to use GPU accelerated caching",
      ExecChecks((TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.STRUCT + TypeSig.ARRAY +
          TypeSig.MAP + GpuTypeShims.additionalCommonOperatorSupportedTypes).nested(), TypeSig.all),
      (scan, conf, p, r) => new InMemoryTableScanMeta(scan, conf, p, r)),
    neverReplaceExec[AlterNamespaceSetPropertiesExec]("Namespace metadata operation"),
    neverReplaceExec[CreateNamespaceExec]("Namespace metadata operation"),
    neverReplaceExec[DescribeNamespaceExec]("Namespace metadata operation"),
    neverReplaceExec[DropNamespaceExec]("Namespace metadata operation"),
    neverReplaceExec[SetCatalogAndNamespaceExec]("Namespace metadata operation"),
    SparkShimImpl.neverReplaceShowCurrentNamespaceCommand,
    ShowNamespacesExecShims.neverReplaceExec.orNull,
    neverReplaceExec[AlterTableExec]("Table metadata operation"),
    neverReplaceExec[CreateTableExec]("Table metadata operation"),
    neverReplaceExec[DeleteFromTableExec]("Table metadata operation"),
    neverReplaceExec[DescribeTableExec]("Table metadata operation"),
    neverReplaceExec[DropTableExec]("Table metadata operation"),
    neverReplaceExec[AtomicReplaceTableExec]("Table metadata operation"),
    neverReplaceExec[RefreshTableExec]("Table metadata operation"),
    neverReplaceExec[RenameTableExec]("Table metadata operation"),
    neverReplaceExec[ReplaceTableExec]("Table metadata operation"),
    neverReplaceExec[ShowTablePropertiesExec]("Table metadata operation"),
    neverReplaceExec[ShowTablesExec]("Table metadata operation"),
    neverReplaceExec[AdaptiveSparkPlanExec]("Wrapper for adaptive query plan"),
    neverReplaceExec[BroadcastQueryStageExec]("Broadcast query stage"),
    neverReplaceExec[ShuffleQueryStageExec]("Shuffle query stage")
  ).collect { case r if r != null => (r.getClassFor.asSubclass(classOf[SparkPlan]), r) }.toMap

  lazy val execs: Map[Class[_ <: SparkPlan], ExecRule[_ <: SparkPlan]] =
    commonExecs ++ GpuHiveOverrides.execs ++ ExternalSource.execRules ++
      SparkShimImpl.getExecs // Shim execs at the end; shims get the last word in substitutions.

  def getTimeParserPolicy: TimeParserPolicy = {
    val policy = SQLConf.get.getConfString(SQLConf.LEGACY_TIME_PARSER_POLICY.key, "EXCEPTION")
    policy.toUpperCase match {
      case "LEGACY" => LegacyTimeParserPolicy
      case "EXCEPTION" => ExceptionTimeParserPolicy
      case "CORRECTED" => CorrectedTimeParserPolicy
    }
  }

  def wrapAndTagPlan(plan: SparkPlan, conf: RapidsConf): SparkPlanMeta[SparkPlan] = {
    val wrap = GpuOverrides.wrapPlan(plan, conf, None)
    wrap.tagForGpu()
    wrap
  }

  private def doConvertPlan(wrap: SparkPlanMeta[SparkPlan], conf: RapidsConf,
      optimizations: Seq[Optimization]): SparkPlan = {
    val convertedPlan = wrap.convertIfNeeded()
    val sparkPlan = addSortsIfNeeded(convertedPlan, conf)
    GpuOverrides.listeners.foreach(_.optimizedPlan(wrap, sparkPlan, optimizations))
    sparkPlan
  }

  private def getOptimizations(wrap: SparkPlanMeta[SparkPlan],
      conf: RapidsConf): Seq[Optimization] = {
    if (conf.optimizerEnabled) {
      // we need to run these rules both before and after CBO because the cost
      // is impacted by forcing operators onto CPU due to other rules that we have
      wrap.runAfterTagRules()
      val optimizer = try {
        ShimLoaderTemp.newOptimizerClass(conf.optimizerClassName)
      } catch {
        case e: Exception =>
          throw new RuntimeException(s"Failed to create optimizer ${conf.optimizerClassName}", e)
      }
      optimizer.optimize(conf, wrap)
    } else {
      Seq.empty
    }
  }

  private def addSortsIfNeeded(plan: SparkPlan, conf: RapidsConf): SparkPlan = {
    plan.transformUp {
      case operator: SparkPlan =>
        ensureOrdering(operator, conf)
    }
  }

  // copied from Spark EnsureRequirements but only does the ordering checks and
  // check to convert any SortExec added to GpuSortExec
  private def ensureOrdering(operator: SparkPlan, conf: RapidsConf): SparkPlan = {
    val requiredChildOrderings: Seq[Seq[SortOrder]] = operator.requiredChildOrdering
    var children: Seq[SparkPlan] = operator.children
    assert(requiredChildOrderings.length == children.length)

    // Now that we've performed any necessary shuffles, add sorts to guarantee output orderings:
    children = children.zip(requiredChildOrderings).map { case (child, requiredOrdering) =>
      // If child.outputOrdering already satisfies the requiredOrdering, we do not need to sort.
      if (SortOrder.orderingSatisfies(child.outputOrdering, requiredOrdering)) {
        child
      } else {
        val sort = SortExec(requiredOrdering, global = false, child = child)
        // just specifically check Sort to see if we can change Sort to GPUSort
        val sortMeta = new GpuSortMeta(sort, conf, None, new SortDataFromReplacementRule)
        sortMeta.initReasons()
        sortMeta.tagPlanForGpu()
        if (sortMeta.canThisBeReplaced) {
          sortMeta.convertToGpu()
        } else {
          sort
        }
      }
    }
    operator.withNewChildren(children)
  }

  private final class SortDataFromReplacementRule extends DataFromReplacementRule {
    override val operationName: String = "Exec"
    override def confKey = "spark.rapids.sql.exec.SortExec"

    override def getChecks: Option[TypeChecks[_]] = None
  }

  /**
   * Only run the explain and don't actually convert or run on GPU.
   * This gets the plan from the dataframe so it's after catalyst has run through all the
   * rules to modify the plan. This means we have to try to undo some of the last rules
   * to make it close to when the columnar rules would normally run on the plan.
   */
  def explainPotentialGpuPlan(df: DataFrame, explain: String): String = {
    val plan = df.queryExecution.executedPlan
    val conf = new RapidsConf(plan.conf)
    val updatedPlan = prepareExplainOnly(plan)
    // Here we look for subqueries to pull out and do the explain separately on them.
    val subQueryExprs = getSubQueriesFromPlan(plan)
    val preparedSubPlans = subQueryExprs.map(_.plan).map(prepareExplainOnly(_))
    val subPlanExplains = preparedSubPlans.map(explainSinglePlan(_, conf, explain))
    val topPlanExplain = explainSinglePlan(updatedPlan, conf, explain)
    (subPlanExplains :+ topPlanExplain).mkString("\n")
  }

  private def explainSinglePlan(updatedPlan: SparkPlan, conf: RapidsConf,
      explain: String): String = {
    val wrap = wrapAndTagPlan(updatedPlan, conf)
    val reasonsToNotReplaceEntirePlan = wrap.getReasonsNotToReplaceEntirePlan
    if (conf.allowDisableEntirePlan && reasonsToNotReplaceEntirePlan.nonEmpty) {
      "Can't replace any part of this plan due to: " +
        s"${reasonsToNotReplaceEntirePlan.mkString(",")}"
    } else {
      wrap.runAfterTagRules()
      wrap.tagForExplain()
      val shouldExplainAll = explain.equalsIgnoreCase("ALL")
      wrap.explain(shouldExplainAll)
    }
  }

  /**
   * Use explain mode on an active SQL plan as its processed through catalyst.
   * This path is the same as being run through the plugin running on hosts with
   * GPUs.
   */
  private def explainCatalystSQLPlan(updatedPlan: SparkPlan, conf: RapidsConf): Unit = {
    // Since we set "NOT_ON_GPU" as the default value of spark.rapids.sql.explain, here we keep
    // "ALL" as default value of "explainSetting", unless spark.rapids.sql.explain is changed
    // by the user.
    val explainSetting = if (conf.shouldExplain &&
      conf.isConfExplicitlySet(RapidsConf.EXPLAIN.key)) {
      conf.explain
    } else {
      "ALL"
    }
    val explainOutput = explainSinglePlan(updatedPlan, conf, explainSetting)
    if (explainOutput.nonEmpty) {
      logWarning(s"\n$explainOutput")
    }
  }

  private def getSubqueryExpressions(e: Expression): Seq[ExecSubqueryExpression] = {
    val childExprs = e.children.flatMap(getSubqueryExpressions(_))
    val res = e match {
      case sq: ExecSubqueryExpression => Seq(sq)
      case _ => Seq.empty
    }
    childExprs ++ res
  }

  private def getSubQueriesFromPlan(plan: SparkPlan): Seq[ExecSubqueryExpression] = {
    val childPlans = plan.children.flatMap(getSubQueriesFromPlan)
    val pSubs = plan.expressions.flatMap(getSubqueryExpressions)
    childPlans ++ pSubs
  }

  private def prepareExplainOnly(plan: SparkPlan): SparkPlan = {
    // Strip out things that would have been added after our GPU plugin would have
    // processed the plan.
    // AQE we look at the input plan so pretty much just like if AQE wasn't enabled.
    val planAfter = plan.transformUp {
      case ia: InputAdapter => prepareExplainOnly(ia.child)
      case ws: WholeStageCodegenExec => prepareExplainOnly(ws.child)
      case c2r: ColumnarToRowExec => prepareExplainOnly(c2r.child)
      case re: ReusedExchangeExec => prepareExplainOnly(re.child)
      case aqe: AdaptiveSparkPlanExec =>
        prepareExplainOnly(SparkShimImpl.getAdaptiveInputPlan(aqe))
      case sub: SubqueryExec => prepareExplainOnly(sub.child)
    }
    planAfter
  }
}

/**
 * Note, this class should not be referenced directly in source code.
 * It should be loaded by reflection using ShimLoader.newInstanceOf, see ./docs/dev/shims.md
 */
protected class ExplainPlanImpl extends ExplainPlanBase {
  override def explainPotentialGpuPlan(df: DataFrame, explain: String): String = {
    GpuOverrides.explainPotentialGpuPlan(df, explain)
  }
}

// work around any GpuOverride failures
object GpuOverrideUtil extends Logging {
  def tryOverride(fn: SparkPlan => SparkPlan): SparkPlan => SparkPlan = { plan =>
    val planOriginal = plan.clone()
    val failOnError = TEST_CONF.get(plan.conf) || !SUPPRESS_PLANNING_FAILURE.get(plan.conf)
    try {
      fn(plan)
    } catch {
      case NonFatal(t) if !failOnError =>
        logWarning("Failed to apply GPU overrides, falling back on the original plan: " + t, t)
        planOriginal
      case fatal: Throwable =>
        logError("Encountered an exception applying GPU overrides " + fatal, fatal)
        throw fatal
    }
  }
}

/** Tag the initial plan when AQE is enabled */
case class GpuQueryStagePrepOverrides() extends Rule[SparkPlan] with Logging {
  override def apply(sparkPlan: SparkPlan): SparkPlan = GpuOverrideUtil.tryOverride { plan =>
    // Exposing a bare exchange at the root is only valid while AQE is preparing a
    // query stage. Tag the exchanges seen in this rule so transition cleanup can
    // distinguish that path from final adaptive plan execution.
    GpuTransitionOverrides.tagAqeQueryStageExchanges(plan)
    // Note that we disregard the GPU plan returned here and instead rely on side effects of
    // tagging the underlying SparkPlan.
    GpuOverrides().applyWithContext(plan, Some("AQE Query Stage Prep"))
    // return the original plan which is now modified as a side-effect of invoking GpuOverrides
    plan
  }(sparkPlan)
}

case class GpuOverrides() extends Rule[SparkPlan] with Logging {

  // Spark calls this method once for the whole plan when AQE is off. When AQE is on, it
  // gets called once for each query stage (where a query stage is an `Exchange`).
  override def apply(sparkPlan: SparkPlan): SparkPlan = applyWithContext(sparkPlan, None)

  def applyWithContext(sparkPlan: SparkPlan, context: Option[String]): SparkPlan =
      GpuOverrideUtil.tryOverride { plan =>
    val conf = new RapidsConf(plan.conf)
    if (conf.isSqlEnabled && conf.isSqlExecuteOnGPU) {
      GpuOverrides.logDuration(conf.shouldExplain,
        t => f"Plan conversion to the GPU took $t%.2f ms") {
        var updatedPlan = updateForAdaptivePlan(plan, conf)
        updatedPlan = HybridExecutionUtils.tryToApplyHybridScanRules(updatedPlan, conf)
        updatedPlan = SparkShimImpl.applyShimPlanRules(updatedPlan, conf)
        updatedPlan = applyOverrides(updatedPlan, conf)
        if (conf.logQueryTransformations) {
          val logPrefix = context.map(str => s"[$str]").getOrElse("")
          logWarning(s"${logPrefix}Transformed query:" +
            s"\nOriginal Plan:\n$plan\nTransformed Plan:\n$updatedPlan")
        }
        updatedPlan
      }
    } else if (conf.isSqlEnabled && conf.isSqlExplainOnlyEnabled) {
      // this mode logs the explain output and returns the original CPU plan
      var updatedPlan = updateForAdaptivePlan(plan, conf)
      updatedPlan = HybridExecutionUtils.tryToApplyHybridScanRules(updatedPlan, conf)
      updatedPlan = SparkShimImpl.applyShimPlanRules(updatedPlan, conf)
      GpuOverrides.explainCatalystSQLPlan(updatedPlan, conf)
      plan
    } else {
      plan
    }
  }(sparkPlan)

  private def updateForAdaptivePlan(plan: SparkPlan, conf: RapidsConf): SparkPlan = {
    if (plan.conf.adaptiveExecutionEnabled) {
      // AQE can cause Spark to inject undesired CPU shuffles into the plan because GPU and CPU
      // distribution expressions are not semantically equal.
      var newPlan = GpuOverrides.removeExtraneousShuffles(plan, conf)

      // Some Spark implementations are caching CPU exchanges for reuse which can be problematic
      // when the RAPIDS Accelerator replaces the original exchange.
      if (conf.isAqeExchangeReuseFixupEnabled && plan.conf.exchangeReuseEnabled) {
        newPlan = GpuOverrides.fixupCpuReusedExchanges(newPlan)
      }

      // AQE can cause ReusedExchangeExec instance to cache the wrong aggregation buffer type
      // compared to the desired buffer type from a reused GPU shuffle.
      GpuOverrides.fixupReusedExchangeOutputs(newPlan)
    } else {
      plan
    }
  }

  /**
   *  Determine whether query is running against Delta Lake _delta_log JSON files or
   *  if Delta is doing stats collection that ends up hardcoding the use of AQE,
   *  even though the AQE setting is disabled. To protect against the latter, we
   *  check for a ScalaUDF using a tahoe.Snapshot function and if we ever see
   *  an AdaptiveSparkPlan on a Spark version we don't expect, fallback to the
   *  CPU for those plans.
   *  Note that the Delta Lake delta log checkpoint parquet files are just inefficient
   *  to have to copy the data to GPU and then back off after it does the scan on
   *  Delta Table Checkpoint, so have the entire plan fallback to CPU at that point.
   */
  def isDeltaLakeMetadataQuery(plan: SparkPlan, detectDeltaCheckpoint: Boolean): Boolean = {
    val deltaLogScans = PlanUtils.findOperators(plan, {
      case f: FileSourceScanExec if DeltaLakeUtils.isDatabricksDeltaLakeScan(f) =>
        logDebug(s"Fallback for FileSourceScanExec with _databricks_internal: $f")
        true
      case f: FileSourceScanExec =>
        val checkDeltaFunc = (name: String) => if (detectDeltaCheckpoint) {
          name.contains("/_delta_log/") && (name.endsWith(".json") ||
            (name.endsWith(".parquet") && new Path(name).getName().contains("checkpoint")))
        } else {
          name.contains("/_delta_log/") && name.endsWith(".json")
        }

        // example filename: "file:/tmp/delta-table/_delta_log/00000000000000000000.json"
        val found = StaticPartitionShims.getStaticPartitions(f.relation).map { parts =>
          parts.exists { part =>
            SparkShimImpl.getPartitionFiles(part).exists(partFile =>
              checkDeltaFunc(partFile.filePath.toString))
          }
        }.getOrElse {
          f.relation.location.rootPaths.exists { path =>
            checkDeltaFunc(path.toString)
          }
        }
        if (found) {
          logDebug(s"Fallback for FileSourceScanExec delta log: $f")
        }
        found
      case rdd: RDDScanExec =>
        // example rdd name: "Delta Table State #1 - file:///tmp/delta-table/_delta_log" or
        // "Scan ExistingRDD Delta Table Checkpoint with Stats #1 -
        // file:///tmp/delta-table/_delta_log"
        val found = rdd.inputRDD != null &&
          rdd.inputRDD.name != null &&
          (rdd.inputRDD.name.startsWith("Delta Table State")
            || rdd.inputRDD.name.startsWith("Delta Table Checkpoint")) &&
          rdd.inputRDD.name.endsWith("/_delta_log")
        if (found) {
          logDebug(s"Fallback for RDDScanExec delta log: $rdd")
        }
        found
      case aqe: AdaptiveSparkPlanExec if
        !AQEUtils.isAdaptiveExecutionSupportedInSparkVersion(plan.conf) =>
        logDebug(s"AdaptiveSparkPlanExec found on unsupported Spark Version: $aqe")
        true
      case project: ProjectExec if
        !AQEUtils.isAdaptiveExecutionSupportedInSparkVersion(plan.conf) =>
        val foundExprs = project.expressions.flatMap { e =>
          PlanUtils.findExpressions(e, {
            case udf: ScalaUDF =>
              val functionClass = udf.function.getClass
              val functionClassName = Option(functionClass.getCanonicalName)
                .getOrElse(functionClass.getName)
              val contains = functionClassName.contains("tahoe.Snapshot")
              if (contains) {
                logDebug(s"Found ScalaUDF with tahoe.Snapshot: $udf," +
                  s" function class name is: $functionClassName")
              }
              contains
            case _ => false
          })
        }
        if (foundExprs.nonEmpty) {
          logDebug(s"Project with Snapshot ScalaUDF: $project")
        }
        foundExprs.nonEmpty
      case mp: MapPartitionsExec if mp.func.toString.contains(".tahoe.Snapshot") =>
        true
      case _ =>
        false
    })
    deltaLogScans.nonEmpty
  }

  private def applyOverrides(plan: SparkPlan, conf: RapidsConf): SparkPlan = {
    val wrap = GpuOverrides.wrapAndTagPlan(plan, conf)
    val detectDeltaCheckpoint = conf.isDetectDeltaCheckpointQueries
    if (conf.isDetectDeltaLogQueries && isDeltaLakeMetadataQuery(plan, detectDeltaCheckpoint)) {
      wrap.entirePlanWillNotWork("Delta Lake metadata queries are not efficient on GPU")
    }
    val reasonsToNotReplaceEntirePlan = wrap.getReasonsNotToReplaceEntirePlan
    if (conf.allowDisableEntirePlan && reasonsToNotReplaceEntirePlan.nonEmpty) {
      if (conf.shouldExplain) {
        logWarning("Can't replace any part of this plan due to: " +
            s"${reasonsToNotReplaceEntirePlan.mkString(",")}")
      }
      plan
    } else {
      val optimizations = GpuOverrides.getOptimizations(wrap, conf)
      wrap.runAfterTagRules()
      if (conf.shouldExplain) {
        wrap.tagForExplain()
        val explain = wrap.explain(conf.shouldExplainAll)
        if (explain.nonEmpty) {
          logWarning(s"\n$explain")
          if (conf.optimizerShouldExplainAll && optimizations.nonEmpty) {
            logWarning(s"Cost-based optimizations applied:\n${optimizations.mkString("\n")}")
          }
        }
      }
      val convertedPlan = GpuOverrides.doConvertPlan(wrap, conf, optimizations)
      if (conf.isTagLoreIdEnabled) {
        GpuLore.tagForLore(convertedPlan, conf)
      } else {
        convertedPlan
      }
    }
  }
}
