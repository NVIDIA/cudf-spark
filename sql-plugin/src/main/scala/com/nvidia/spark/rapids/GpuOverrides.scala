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
import org.apache.hadoop.fs.Path

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.rapids.TimeStamp
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.connector.read.Scan
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, BroadcastQueryStageExec, QueryStageExec, ShuffleQueryStageExec}
import org.apache.spark.sql.execution.command.{DataWritingCommand, RunnableCommand}
import org.apache.spark.sql.execution.datasources.{InsertIntoHadoopFsRelationCommand, SaveIntoDataSourceCommand}
import org.apache.spark.sql.execution.datasources.csv.CSVFileFormat
import org.apache.spark.sql.execution.datasources.json.JsonFileFormat
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.execution.datasources.text.TextFileFormat
import org.apache.spark.sql.execution.datasources.v2.csv.CSVScan
import org.apache.spark.sql.execution.datasources.v2.json.JsonScan
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeExec, ReusedExchangeExec, ShuffleExchangeExec}
import org.apache.spark.sql.hive.rapids.GpuHiveOverrides
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids._
import org.apache.spark.sql.rapids.execution._
import org.apache.spark.sql.rapids.shims.SparkSessionUtils
import org.apache.spark.sql.rapids.zorder.ZOrderRules
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

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
        ConsoleOutput.write(s"${makeConfAnchor(confKey)}")
        if (sparkSQLFunctions.isDefined) {
          ConsoleOutput.write(s"|${sparkSQLFunctions.get}")
        }
        val incompatOps = RapidsConf.INCOMPATIBLE_OPS.asInstanceOf[ConfEntryWithDefault[Boolean]]
        val expressionEnabled = disabledMsg.isEmpty &&
          (incompatDoc.isEmpty || incompatOps.defaultValue)
        ConsoleOutput.write(s"|$desc|$expressionEnabled|")
        if (notesMsg.isDefined) {
          ConsoleOutput.write(s"${notesMsg.get}")
        } else {
          ConsoleOutput.write("None")
        }
        ConsoleOutput.writeLine("|")
      } else {
        ConsoleOutput.writeLine(s"$confKey:")
        ConsoleOutput.writeLine(s"\tEnable (true) or disable (false) the $tag $operationName.")
        if (sparkSQLFunctions.isDefined) {
          ConsoleOutput.writeLine(s"\tsql function: ${sparkSQLFunctions.get}")
        }
        ConsoleOutput.writeLine(s"\t$desc")
        if (notesMsg.isDefined) {
          ConsoleOutput.writeLine(s"\t${notesMsg.get}")
        }
        ConsoleOutput.writeLine(s"\tdefault: ${notesMsg.isEmpty}")
        ConsoleOutput.writeLine()
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

case class InsertIntoHadoopFsRelationCommandMeta(
    cmd: InsertIntoHadoopFsRelationCommand,
    override val conf: RapidsConf,
    override val parent: Option[RapidsMeta[_, _, _]],
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
          GpuTypeShims.additionalParquetCommonSupportedTypes).nested(),
      cudfWrite = (TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.STRUCT +
          TypeSig.ARRAY + TypeSig.MAP + TypeSig.BINARY +
          GpuTypeShims.additionalParquetCommonSupportedTypes).nested(),
      sparkSig = (TypeSig.cpuAtomics + TypeSig.STRUCT + TypeSig.ARRAY + TypeSig.MAP +
          TypeSig.UDT + GpuTypeShims.additionalParquetSupportedTypes).nested())),
    (ParquetFormatType, FileFormatChecks(
      cudfRead = (TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.STRUCT +
          TypeSig.ARRAY + TypeSig.MAP + TypeSig.BINARY +
          GpuTypeShims.additionalParquetReadSupportedTypes).nested(),
      cudfWrite = (TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.STRUCT +
          TypeSig.ARRAY + TypeSig.MAP + TypeSig.BINARY +
          GpuTypeShims.additionalParquetWriteSupportedTypes).nested(),
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
          TypeSig.ARRAY + TypeSig.MAP +
          GpuTypeShims.additionalParquetCommonSupportedTypes).nested(),
      cudfWrite = (TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.STRUCT +
          TypeSig.ARRAY + TypeSig.MAP + TypeSig.BINARY +
          GpuTypeShims.additionalParquetCommonSupportedTypes).nested(),
      sparkSig = (TypeSig.cpuAtomics + TypeSig.STRUCT + TypeSig.ARRAY + TypeSig.MAP +
          TypeSig.BINARY + TypeSig.UDT + GpuTypeShims.additionalParquetSupportedTypes).nested())))

  val commonExpressions: Map[Class[_ <: Expression], ExprRule[_ <: Expression]] = (
    GpuCoreExpressionOverrides.rules ++
      GpuMathAndDateExpressionOverrides.rules ++
      GpuComparisonAndAggregateExpressionOverrides.rules ++
      GpuCollectionExpressionOverrides.rules ++
      GpuStringAndAggregateExpressionOverrides.rules ++
      GpuMiscExpressionOverrides.rules
  ).collect { case r if r != null => (r.getClassFor.asSubclass(classOf[Expression]), r)}.toMap

  // Shim expressions should be last to allow overrides with shim-specific versions
  lazy val expressions: Map[Class[_ <: Expression], ExprRule[_ <: Expression]] =
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
      CSVScanRuleMeta),
    GpuOverrides.scan[JsonScan](
      "Json parsing",
      JsonScanRuleMeta)).map(r => (r.getClassFor.asSubclass(classOf[Scan]), r)).toMap

  lazy val scans: Map[Class[_ <: Scan], ScanRule[_ <: Scan]] =
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
      HashPartitioningRuleMeta),
    part[RangePartitioning](
      "Range partitioning",
      PartChecks(RepeatingParamCheck("order_key",
        pluginSupportedOrderableSig + TypeSig.ARRAY.nested(gpuCommonTypes)
           .withPsNote(TypeEnum.ARRAY, "STRUCT is not supported as a child type for ARRAY"),
        TypeSig.orderable)),
      RangePartitioningRuleMeta),
    part[RoundRobinPartitioning](
      "Round robin partitioning",
      PartChecks(),
      RoundRobinPartitioningRuleMeta),
    part[SinglePartition.type](
      "Single partitioning",
      PartChecks(),
      SinglePartitionRuleMeta)
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
      InsertIntoHadoopFsRelationCommandMeta)
  ).map(r => (r.getClassFor.asSubclass(classOf[DataWritingCommand]), r)).toMap

  lazy val dataWriteCmds: Map[Class[_ <: DataWritingCommand],
      DataWritingCommandRule[_ <: DataWritingCommand]] =
    commonDataWriteCmds ++ GpuHiveOverrides.dataWriteCmds ++ SparkShimImpl.getDataWriteCmds ++
      ExternalSource.dataWriteCmds

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
        SaveIntoDataSourceCommandConstructorRuleMeta)
    ).map(r => (r.getClassFor.asSubclass(classOf[RunnableCommand]), r)).toMap

  lazy val runnableCmds = commonRunnableCmds ++
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

  val commonExecs: Map[Class[_ <: SparkPlan], ExecRule[_ <: SparkPlan]] =
    GpuExecOverrides.rules.collect {
      case r if r != null => (r.getClassFor.asSubclass(classOf[SparkPlan]), r)
    }.toMap

  lazy val execs: Map[Class[_ <: SparkPlan], ExecRule[_ <: SparkPlan]] =
    commonExecs ++ GpuHiveOverrides.execs ++ ExternalSource.execRules ++
      SparkShimImpl.getExecs // Shim execs at the end; shims get the last word in substitutions.

  def getTimeParserPolicy: TimeParserPolicy = {
    val policy = SQLConf.get.legacyTimeParserPolicy.toString
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
      wrap.explain(RapidsConf.shouldExplainAll(explain))
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
  def withActiveSession[T](sparkSession: SparkSession)(body: => T): T = {
    if (sparkSession == null) {
      // A session is only captured for rules registered through ColumnarOverrideRules.
      // Direct callers (including tests) intentionally preserve the existing unscoped behavior.
      body
    } else {
      SparkSessionUtils.withActiveSession(sparkSession)(body)
    }
  }

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
case class GpuQueryStagePrepOverrides(sparkSession: SparkSession)
    extends Rule[SparkPlan] with Logging {
  override def apply(sparkPlan: SparkPlan): SparkPlan =
      GpuOverrideUtil.withActiveSession(sparkSession) {
    GpuOverrideUtil.tryOverride { plan =>
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
}

case class GpuOverrides(sparkSession: SparkSession = null) extends Rule[SparkPlan] with Logging {

  // Spark calls this method once for the whole plan when AQE is off. When AQE is on, it
  // gets called once for each query stage (where a query stage is an `Exchange`).
  override def apply(sparkPlan: SparkPlan): SparkPlan =
    GpuOverrideUtil.withActiveSession(sparkSession) {
      applyWithContext(sparkPlan, None)
    }

  def applyWithContext(sparkPlan: SparkPlan, context: Option[String]): SparkPlan =
      GpuOverrideUtil.tryOverride { plan =>
    val conf = new RapidsConf(plan.conf)
    if (conf.isSqlEnabled && conf.isSqlExecuteOnGPU) {
      GpuOverrides.logDuration(conf.shouldExplain,
        t => f"Plan conversion to the GPU took $t%.2f ms") {
        var updatedPlan = updateForAdaptivePlan(plan, conf)
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
