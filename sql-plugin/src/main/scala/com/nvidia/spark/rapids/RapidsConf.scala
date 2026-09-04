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

import java.io.{File, FileOutputStream}
import java.util

import scala.collection.JavaConverters._
import scala.collection.mutable.{HashMap, ListBuffer}

import ai.rapids.cudf.Cuda
import com.nvidia.spark.rapids.jni.RmmSpark.OomInjectionType
import com.nvidia.spark.rapids.jni.kudo.DumpOption
import com.nvidia.spark.rapids.lore.{LoreId, OutputLoreId}

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.network.util.{ByteUnit, JavaUtils}
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.RapidsPrivateUtil
import org.apache.spark.sql.rapids.execution.{JoinBuildSideSelection, JoinOptions, JoinStrategy}

object ConfHelper {
  def toBoolean(s: String, key: String): Boolean = {
    try {
      s.trim.toBoolean
    } catch {
      case _: IllegalArgumentException =>
        throw new IllegalArgumentException(s"$key should be boolean, but was $s")
    }
  }

  def toInteger(s: String, key: String): Integer = {
    try {
      s.trim.toInt
    } catch {
      case _: IllegalArgumentException =>
        throw new IllegalArgumentException(s"$key should be integer, but was $s")
    }
  }

  def toLong(s: String, key: String): Long = {
    try {
      s.trim.toLong
    } catch {
      case _: IllegalArgumentException =>
        throw new IllegalArgumentException(s"$key should be long, but was $s")
    }
  }

  def toDouble(s: String, key: String): Double = {
    try {
      s.trim.toDouble
    } catch {
      case _: IllegalArgumentException =>
        throw new IllegalArgumentException(s"$key should be double, but was $s")
    }
  }

  def stringToSeq(str: String): Seq[String] = {
    // Here 'split' returns a mutable array, 'toList' will convert it into a immutable list
    str.split(",").map(_.trim()).filter(_.nonEmpty).toList
  }

  def stringToSeq[T](str: String, converter: String => T): Seq[T] = {
    stringToSeq(str).map(converter)
  }

  def seqToString[T](v: Seq[T], stringConverter: T => String): String = {
    v.map(stringConverter).mkString(",")
  }

  def byteFromString(str: String, unit: ByteUnit): Long = {
    val (input, multiplier) =
      if (str.nonEmpty && str.head == '-') {
        (str.substring(1), -1)
      } else {
        (str, 1)
      }
    multiplier * JavaUtils.byteStringAs(input, unit)
  }

  def makeConfAnchor(key: String, text: String = null): String = {
    val t = if (text != null) text else key
    // The anchor cannot be too long, so for now
    val a = key.replaceFirst("spark.rapids.", "")
    "<a name=\"" + s"$a" + "\"></a>" + t
  }

  def getSqlFunctionsForClass[T](exprClass: Class[T]): Option[Seq[String]] = {
    sqlFunctionsByClass.get(exprClass.getCanonicalName)
  }

  lazy val sqlFunctionsByClass: Map[String, Seq[String]] = {
    val functionsByClass = new HashMap[String, Seq[String]]
    FunctionRegistry.expressions.foreach { case (sqlFn, (expressionInfo, _)) =>
      val className = expressionInfo.getClassName
      val fnSeq = functionsByClass.getOrElse(className, Seq[String]())
      val fnCleaned = if (sqlFn != "|") {
        sqlFn
      } else {
        "\\|"
      }
      functionsByClass.update(className, fnSeq :+ s"`$fnCleaned`")
    }
    functionsByClass.mapValues(_.sorted).toMap
  }
}

abstract class ConfEntry[T](val key: String, val converter: String => T, val doc: String,
    val isInternal: Boolean, val isStartUpOnly: Boolean, val isCommonlyUsed: Boolean) {

  def get(conf: Map[String, String]): T
  def get(conf: SQLConf): T
  def help(asTable: Boolean = false): Unit

  override def toString: String = key
}

class ConfEntryWithDefault[T](key: String, converter: String => T, doc: String,
    isInternal: Boolean, isStartupOnly: Boolean, isCommonlyUsed: Boolean = false,
    val defaultValue: T)
  extends ConfEntry[T](key, converter, doc, isInternal, isStartupOnly, isCommonlyUsed) {

  override def get(conf: Map[String, String]): T = {
    conf.get(key).map(converter).getOrElse(defaultValue)
  }

  override def get(conf: SQLConf): T = {
    val tmp = conf.getConfString(key, null)
    if (tmp == null) {
      defaultValue
    } else {
      converter(tmp)
    }
  }

  override def help(asTable: Boolean = false): Unit = {
    if (!isInternal) {
      val startupOnlyStr = if (isStartupOnly) "Startup" else "Runtime"
      if (asTable) {
        import ConfHelper.makeConfAnchor
        ConsoleOutput.writeLine(s"${makeConfAnchor(key)}|$doc|$defaultValue|$startupOnlyStr")
      } else {
        ConsoleOutput.writeLine(s"$key:")
        ConsoleOutput.writeLine(s"\t$doc")
        ConsoleOutput.writeLine(s"\tdefault $defaultValue")
        ConsoleOutput.writeLine(s"\ttype $startupOnlyStr")
        ConsoleOutput.writeLine()
      }
    }
  }
}

class OptionalConfEntry[T](key: String, val rawConverter: String => T, doc: String,
    isInternal: Boolean, isStartupOnly: Boolean, isCommonlyUsed: Boolean = false)
  extends ConfEntry[Option[T]](key, s => Some(rawConverter(s)), doc, isInternal,
  isStartupOnly, isCommonlyUsed) {

  override def get(conf: Map[String, String]): Option[T] = {
    conf.get(key).map(rawConverter)
  }

  override def get(conf: SQLConf): Option[T] = {
    val tmp = conf.getConfString(key, null)
    if (tmp == null) {
      None
    } else {
      Some(rawConverter(tmp))
    }
  }

  override def help(asTable: Boolean = false): Unit = {
    if (!isInternal) {
      val startupOnlyStr = if (isStartupOnly) "Startup" else "Runtime"
      if (asTable) {
        import ConfHelper.makeConfAnchor
        ConsoleOutput.writeLine(s"${makeConfAnchor(key)}|$doc|None|$startupOnlyStr")
      } else {
        ConsoleOutput.writeLine(s"$key:")
        ConsoleOutput.writeLine(s"\t$doc")
        ConsoleOutput.writeLine("\tNone")
        ConsoleOutput.writeLine(s"\ttype $startupOnlyStr")
        ConsoleOutput.writeLine()
      }
    }
  }
}

class TypedConfBuilder[T](
    val parent: ConfBuilder,
    val converter: String => T,
    val stringConverter: T => String) {

  def this(parent: ConfBuilder, converter: String => T) = {
    this(parent, converter, Option(_).map(_.toString).orNull)
  }

  /** Apply a transformation to the user-provided values of the config entry. */
  def transform(fn: T => T): TypedConfBuilder[T] = {
    new TypedConfBuilder(parent, s => fn(converter(s)), stringConverter)
  }

  /** Checks if the user-provided value for the config matches the validator. */
  def checkValue(validator: T => Boolean, errorMsg: String): TypedConfBuilder[T] = {
    transform { v =>
      if (!validator(v)) {
        throw new IllegalArgumentException(errorMsg)
      }
      v
    }
  }

  /** Check that user-provided values for the config match a pre-defined set. */
  def checkValues(validValues: Set[T]): TypedConfBuilder[T] = {
    transform { v =>
      if (!validValues.contains(v)) {
        throw new IllegalArgumentException(
          s"The value of ${parent.key} should be one of ${validValues.mkString(", ")}, but was $v")
      }
      v
    }
  }

  def createWithDefault(value: T): ConfEntryWithDefault[T] = {
    // 'converter' will check the validity of default 'value', if it's not valid,
    // then 'converter' will throw an exception
    val transformedValue = converter(stringConverter(value))
    val ret = new ConfEntryWithDefault[T](parent.key, converter,
      parent.doc, parent.isInternal, parent.isStartupOnly, parent.isCommonlyUsed, transformedValue)
    parent.register(ret)
    ret
  }

  /** Turns the config entry into a sequence of values of the underlying type. */
  def toSequence: TypedConfBuilder[Seq[T]] = {
    new TypedConfBuilder(parent, ConfHelper.stringToSeq(_, converter),
      ConfHelper.seqToString(_, stringConverter))
  }

  def createOptional: OptionalConfEntry[T] = {
    val ret = new OptionalConfEntry[T](parent.key, converter,
      parent.doc, parent.isInternal, parent.isStartupOnly, parent.isCommonlyUsed)
    parent.register(ret)
    ret
  }
}

class ConfBuilder(val key: String, val register: ConfEntry[_] => Unit) {

  import ConfHelper._

  var doc: String = null
  var isInternal: Boolean = false
  var isStartupOnly: Boolean = false
  var isCommonlyUsed: Boolean = false

  def doc(data: String): ConfBuilder = {
    this.doc = data
    this
  }

  def internal(): ConfBuilder = {
    this.isInternal = true
    this
  }

  def startupOnly(): ConfBuilder = {
    this.isStartupOnly = true
    this
  }

  def commonlyUsed(): ConfBuilder = {
    this.isCommonlyUsed = true
    this
  }

  def booleanConf: TypedConfBuilder[Boolean] = {
    new TypedConfBuilder[Boolean](this, toBoolean(_, key))
  }

  def bytesConf(unit: ByteUnit): TypedConfBuilder[Long] = {
    new TypedConfBuilder[Long](this, byteFromString(_, unit))
  }

  def integerConf: TypedConfBuilder[Integer] = {
    new TypedConfBuilder[Integer](this, toInteger(_, key))
  }

  def longConf: TypedConfBuilder[Long] = {
    new TypedConfBuilder[Long](this, toLong(_, key))
  }

  def doubleConf: TypedConfBuilder[Double] = {
    new TypedConfBuilder(this, toDouble(_, key))
  }

  def stringConf: TypedConfBuilder[String] = {
    new TypedConfBuilder[String](this, identity[String])
  }
}

object RapidsReaderType extends Enumeration {
  type RapidsReaderType = Value
  val AUTO, COALESCING, MULTITHREADED, PERFILE = Value
}

object RapidsConf extends Logging with RapidsConfEntries {
  object HybridFilterPushdownType extends Enumeration {
    val CPU, GPU, OFF = Value
  }

  object RapidsShuffleManagerMode extends Enumeration {
    val UCX, CACHE_ONLY, MULTITHREADED = Value
  }

  object ShuffleKudoMode extends Enumeration {
    val CPU, GPU = Value
  }

  object AllowMultipleJars extends Enumeration {
    val ALWAYS, SAME_REVISION, NEVER = Value
  }

  object ParquetFooterReaderType extends Enumeration {
    val JAVA, NATIVE, AUTO = Value
  }

  private lazy val registeredConfs = new ListBuffer[ConfEntry[_]]()

  private def register(entry: ConfEntry[_]): Unit = {
    registeredConfs += entry
  }

  def conf(key: String): ConfBuilder = {
    new ConfBuilder(key, register)
  }

  // default value for the OOM injection logic (no injection, for regular operation)
  private val noInjection = OomInjectionConf(
    numOoms = 0,
    skipCount = 0,
    oomInjectionFilter = OomInjectionType.CPU_OR_GPU,
    withSplit = false)

  // A java property that is set to true when we are running in tests.
  // We will use this property to check for oom injection configs in SQLConf, and to turn on
  // the rapids-specific assertInTests function, only if we are running tests. 
  // This is set to true in integration_tests/run_pyspark_from_build.sh, and in the 
  // pom for both scalatest and javatest.
  lazy val runningTests = {
    val res = System.getProperty("com.nvidia.spark.rapids.runningTests", "false")
      .toLowerCase.toBoolean
    if (!res) {
      logDebug("OOM injection in tests disable. To enable, set java property " +
        "`-Dcom.nvidia.spark.rapids.runningTest=true`, and configure using " +
        s"${TEST_RETRY_OOM_INJECTION_MODE.key}.")
    }
    res
  }

  /**
   * Convert a configured injection string to the injection configuration OomInjection,
   * if enabled via com.nvidia.spark.rapids.runningTests java property.
   *
   * The new format is a CSV in any order
   *  "num_ooms=<integer>,skip=<integer>,type=<string value of OomInjectionType>"
   *
   * "type" maps to OomInjectionType to run count against oomCount and skipCount
   * "num_ooms" maps to oomCount (default 1), the number of allocations resulting in an OOM
   * "skip" maps to skipCount (default 0), the number of matching  allocations to skip before
   * injecting an OOM at the skip+1st allocation.
   * *split* maps to withSplit (default false), determining whether to inject
   * *SplitAndRetryOOM instead of plain *RetryOOM exceptions
   *
   * For backwards compatibility support existing binary configuration
   *   "false", disabled, i.e. oomCount=0, skipCount=0, injectionType=None
   *   "true" or anything else but "false"  yields the default
   *      oomCount=1, skipCount=0, injectionType=CPU_OR_GPU, withSplit=false
   */
  def testRetryOOMInjectionMode(): OomInjectionConf = {
    if (!runningTests) {
      // this is the common case
      noInjection
    } else {
      TEST_RETRY_OOM_INJECTION_MODE.get(SQLConf.get).toLowerCase match {
        case "false" => noInjection
        case "true" =>
          OomInjectionConf(numOoms = 1, skipCount = 0,
            oomInjectionFilter = OomInjectionType.CPU_OR_GPU, withSplit = false)
        case injectConfStr =>
          val injectConfMap = injectConfStr.split(',').map(_.split('=')).collect {
            case Array(k, v) => k -> v
          }.toMap
          val numOoms = injectConfMap.getOrElse("num_ooms", 1.toString)
          val skipCount = injectConfMap.getOrElse("skip", 0.toString)
          val oomFilterStr = injectConfMap
            .getOrElse("type", OomInjectionType.CPU_OR_GPU.toString)
            .toUpperCase()
          val oomFilter = OomInjectionType.valueOf(oomFilterStr)
          val withSplit = injectConfMap.getOrElse("split", false.toString)
          val ret = OomInjectionConf(
            numOoms = numOoms.toInt,
            skipCount = skipCount.toInt,
            oomInjectionFilter = oomFilter,
            withSplit = withSplit.toBoolean
          )
          logDebug(s"Parsed ${ret} from ${injectConfStr} via injectConfMap=${injectConfMap}");
          ret
      }
    }
  }

  private def printSectionHeader(category: String): Unit =
    ConsoleOutput.writeLine(s"\n### $category")

  private def printToggleHeader(category: String): Unit = {
    printSectionHeader(category)
    ConsoleOutput.writeLine("Name | Description | Default Value | Notes")
    ConsoleOutput.writeLine("-----|-------------|---------------|------------------")
  }

  private def printToggleHeaderWithSqlFunction(category: String): Unit = {
    printSectionHeader(category)
    ConsoleOutput.writeLine("Name | SQL Function(s) | Description | Default Value | Notes")
    ConsoleOutput.writeLine("-----|-----------------|-------------|---------------|------")
  }

  def help(asTable: Boolean = false): Unit = {
    helpCommon(asTable)
    helpAdvanced(asTable)
  }

  def helpCommon(asTable: Boolean = false): Unit = {
    if (asTable) {
      ConsoleOutput.writeLine("---")
      ConsoleOutput.writeLine("layout: page")
      ConsoleOutput.writeLine("title: Configuration")
      ConsoleOutput.writeLine("nav_order: 4")
      ConsoleOutput.writeLine("---")
      MarkdownUtils.printApacheSparkVersion("RapidsConf.helpCommon")
      // scalastyle:off line.size.limit
      ConsoleOutput.writeLine("""# NVIDIA cuDF plugin for Apache Spark Configuration
        |The following is the list of options that `rapids-plugin-4-spark` supports.
        |
        |On startup use: `--conf [conf key]=[conf value]`. For example:
        |
        |```
        |${SPARK_HOME}/bin/spark-shell --jars rapids-4-spark_2.12-26.10.0-SNAPSHOT-cuda12.jar \
        |--conf spark.plugins=com.nvidia.spark.SQLPlugin \
        |--conf spark.rapids.sql.concurrentGpuTasks=2
        |```
        |
        |At runtime use: `spark.conf.set("[conf key]", [conf value])`. For example:
        |
        |```
        |scala> spark.conf.set("spark.rapids.sql.concurrentGpuTasks", 2)
        |```
        |
        | All configs can be set on startup, but some configs, especially for shuffle, will not
        | work if they are set at runtime. Please check the column of "Applicable at" to see
        | when the config can be set. "Startup" means only valid on startup, "Runtime" means
        | valid on both startup and runtime.
        |""".stripMargin)
      // scalastyle:on line.size.limit
      ConsoleOutput.writeLine("\n## General Configuration\n")
      ConsoleOutput.writeLine("Name | Description | Default Value | Applicable at")
      ConsoleOutput.writeLine("-----|-------------|--------------|--------------")
    } else {
      ConsoleOutput.writeLine("Commonly Used cuDF plugin Configs:")
    }
    val allConfs = registeredConfs.clone()
    allConfs.append(RapidsPrivateUtil.getPrivateConfigs(): _*)
    val outputConfs = allConfs.filter(_.isCommonlyUsed)
    outputConfs.sortBy(_.key).foreach(_.help(asTable))
    if (asTable) {
      // scalastyle:off line.size.limit
      ConsoleOutput.writeLine("""
        |For more advanced configs, please refer to the [NVIDIA cuDF plugin for Apache Spark Advanced Configuration](./additional-functionality/advanced_configs.md) page.
        |""".stripMargin)
      // scalastyle:on line.size.limit
    }
  }

  def helpAdvanced(asTable: Boolean = false): Unit = {
    if (asTable) {
      ConsoleOutput.writeLine("---")
      ConsoleOutput.writeLine("layout: page")
      // print advanced configuration
      ConsoleOutput.writeLine("title: Advanced Configuration")
      ConsoleOutput.writeLine("parent: Additional Functionality")
      ConsoleOutput.writeLine("nav_order: 10")
      ConsoleOutput.writeLine("---")
      MarkdownUtils.printApacheSparkVersion("RapidsConf.helpAdvanced")
      // scalastyle:off line.size.limit
      ConsoleOutput.writeLine("""# NVIDIA cuDF plugin for Apache Spark Advanced Configuration
        |Most users will not need to modify the configuration options listed below.
        |They are documented here for completeness and advanced usage.
        |
        |The following configuration options are supported by the cuDF plugin.
        |
        |For commonly used configurations and examples of setting options, please refer to the
        |[NVIDIA cuDF plugin for Apache Spark Configuration](../configs.md) page.
        |""".stripMargin)
      // scalastyle:on line.size.limit
      ConsoleOutput.writeLine("\n## Advanced Configuration\n")

      ConsoleOutput.writeLine("Name | Description | Default Value | Applicable at")
      ConsoleOutput.writeLine("-----|-------------|--------------|--------------")
    } else {
      ConsoleOutput.writeLine("Advanced cuDF Plugin Configs:")
    }
    val allConfs = registeredConfs.clone()
    allConfs.append(RapidsPrivateUtil.getPrivateConfigs(): _*)
    val outputConfs = allConfs.filterNot(_.isCommonlyUsed)
    outputConfs.sortBy(_.key).foreach(_.help(asTable))
    if (asTable) {
      ConsoleOutput.writeLine("")
      // scalastyle:off line.size.limit
      ConsoleOutput.writeLine("""## Supported GPU Operators and Fine Tuning
        |The cuDF plugin can be configured to enable or disable specific
        |GPU accelerated expressions.  Enabled expressions are candidates for GPU execution. If the
        |expression is configured as disabled, the accelerator plugin will not attempt replacement,
        |and it will run on the CPU.
        |
        |Please leverage the [`spark.rapids.sql.explain`](#sql.explain) setting to get
        |feedback from the plugin as to why parts of a query may not be executing on the GPU.
        |
        |**NOTE:** Setting
        |[`spark.rapids.sql.incompatibleOps.enabled=true`](#sql.incompatibleOps.enabled)
        |will enable all the settings in the table below which are not enabled by default due to
        |incompatibilities.""".stripMargin)
      // scalastyle:on line.size.limit

      printToggleHeaderWithSqlFunction("Expressions\n")
    }
    GpuOverrides.expressions.values.toSeq.sortBy(_.tag.toString).foreach { rule =>
      val sqlFunctions =
        ConfHelper.getSqlFunctionsForClass(rule.tag.runtimeClass).map(_.mkString(", "))

      // this is only for formatting, this is done to ensure the table has a column for a
      // row where there isn't a SQL function
      rule.confHelp(asTable, Some(sqlFunctions.getOrElse(" ")))
    }
    if (asTable) {
      printToggleHeader("Execution\n")
    }
    GpuOverrides.execs.values.toSeq.sortBy(_.tag.toString).foreach(_.confHelp(asTable))
    if (asTable) {
      printToggleHeader("Commands\n")
    }
    GpuOverrides.commonRunnableCmds.values.toSeq.sortBy(_.tag.toString).foreach(_.confHelp(asTable))
    if (asTable) {
      printToggleHeader("Scans\n")
    }
    GpuOverrides.scans.values.toSeq.sortBy(_.tag.toString).foreach(_.confHelp(asTable))
    if (asTable) {
      printToggleHeader("Partitioning\n")
    }
    GpuOverrides.parts.values.toSeq.sortBy(_.tag.toString).foreach(_.confHelp(asTable))
  }
  def main(args: Array[String]): Unit = {
    // Include the configs in PythonConfEntries
    com.nvidia.spark.rapids.python.PythonConfEntries.init()
    val configs = new FileOutputStream(new File(args(0)))
    Console.withOut(configs) {
      Console.withErr(configs) {
        RapidsConf.helpCommon(true)
      }
    }
    val advanced = new FileOutputStream(new File(args(1)))
    Console.withOut(advanced) {
      Console.withErr(advanced) {
        RapidsConf.helpAdvanced(true)
      }
    }
  }

  /**
   * Get join options based on the configuration.
   * @param conf the SQL configuration
   * @param targetSize the target batch size in bytes to use for the join
   * @return JoinOptions configured based on the join strategy and targetSize
   */
  def getJoinOptions(
      conf: SQLConf,
      targetSize: Long): JoinOptions = {
    val strategyStr = JOIN_STRATEGY.get(conf)
    val strategy = JoinStrategy.withName(strategyStr)
    val buildSideStr = JOIN_BUILD_SIDE.get(conf)
    val buildSideSelection = JoinBuildSideSelection.withName(buildSideStr)
    val logCardinality = LOG_JOIN_CARDINALITY.get(conf)
    val sizeEstimateThreshold = JOIN_GATHERER_SIZE_ESTIMATE_THRESHOLD.get(conf)
    JoinOptions(strategy, buildSideSelection, targetSize, logCardinality, sizeEstimateThreshold)
  }
}

class RapidsConf(conf: Map[String, String]) extends Logging {

  import ConfHelper._
  import RapidsConf._

  def this(sqlConf: SQLConf) = {
    this(sqlConf.getAllConfs)
  }

  def this(sparkConf: SparkConf) = {
    this(Map(sparkConf.getAll: _*))
  }

  def get[T](entry: ConfEntry[T]): T = {
    entry.get(conf)
  }

  def getStr(key: String): Option[String] = conf.get(key)

  lazy val rapidsConfMap: util.Map[String, String] = conf.filterKeys(
    _.startsWith("spark.rapids.")).toMap.asJava

  lazy val metricsLevel: String = get(METRICS_LEVEL)

  lazy val profilePath: Option[String] = get(PROFILE_PATH)

  lazy val profileExecutors: String = get(PROFILE_EXECUTORS)

  lazy val profileTimeRangesSeconds: Option[String] = get(PROFILE_TIME_RANGES_SECONDS)

  lazy val profileJobs: Option[String] = get(PROFILE_JOBS)

  lazy val profileStages: Option[String] = get(PROFILE_STAGES)

  lazy val profileTaskLimitPerStage: Int = get(PROFILE_TASK_LIMIT_PER_STAGE)

  lazy val profileDriverPollMillis: Int = get(PROFILE_DRIVER_POLL_MILLIS)

  lazy val profileAsyncAllocCapture: Boolean = get(PROFILE_ASYNC_ALLOC_CAPTURE)

  lazy val profileCompression: String = get(PROFILE_COMPRESSION)

  lazy val profileFlushPeriodMillis: Int = get(PROFILE_FLUSH_PERIOD_MILLIS)

  lazy val profileWriteBufferSize: Long = get(PROFILE_WRITE_BUFFER_SIZE)

  lazy val asyncProfilerPathPrefix: Option[String] = get(ASYNC_PROFILER_PATH_PREFIX)

  lazy val asyncProfilerExecutors: String = get(ASYNC_PROFILER_EXECUTORS)

  lazy val asyncProfilerProfileOptions: String = get(ASYNC_PROFILER_PROFILE_OPTIONS)

  lazy val asyncProfilerJfrCompression: Boolean = get(ASYNC_PROFILER_JFR_COMPRESSION)

  lazy val asyncProfilerStageEpochInterval: Int = get(ASYNC_PROFILER_STAGE_EPOCH_INTERVAL)

  lazy val isSqlEnabled: Boolean = get(SQL_ENABLED)

  lazy val isAcceleratedColumnarToRowEnabled: Boolean = get(ACCELERATED_COLUMNAR_TO_ROW_ENABLED)

  lazy val isSqlExecuteOnGPU: Boolean = get(SQL_MODE).equals("executeongpu")

  lazy val isSqlExplainOnlyEnabled: Boolean = get(SQL_MODE).equals("explainonly")

  lazy val isUdfCompilerEnabled: Boolean = get(UDF_COMPILER_ENABLED)

  lazy val isDfUdfEnabled: Boolean = get(DFUDF_ENABLED)

  lazy val exportColumnarRdd: Boolean = get(EXPORT_COLUMNAR_RDD)

  lazy val shuffledHashJoinOptimizeShuffle: Boolean = get(SHUFFLED_HASH_JOIN_OPTIMIZE_SHUFFLE)

  lazy val useShuffledSymmetricHashJoin: Boolean = get(USE_SHUFFLED_SYMMETRIC_HASH_JOIN)

  lazy val useShuffledAsymmetricHashJoin: Boolean =
    get(USE_SHUFFLED_ASYMMETRIC_HASH_JOIN)

  lazy val joinOuterMagnificationThreshold: Int = get(JOIN_OUTER_MAGNIFICATION_THRESHOLD)

  lazy val bucketJoinIoPrefetch: Boolean = get(BUCKET_JOIN_IO_PREFETCH)

  lazy val logJoinCardinality: Boolean = get(LOG_JOIN_CARDINALITY)

  lazy val joinGathererSizeEstimateThreshold: Double = get(JOIN_GATHERER_SIZE_ESTIMATE_THRESHOLD)

  /**
   * Get join options based on the current configuration.
   * @param targetSize the target batch size in bytes to use for the join
   * @return JoinOptions configured based on the join strategy and targetSize
   */
  def getJoinOptions(targetSize: Long): JoinOptions = {
    val strategyStr = get(JOIN_STRATEGY)
    val strategy = JoinStrategy.withName(strategyStr)
    val buildSideStr = get(JOIN_BUILD_SIDE)
    val buildSideSelection = JoinBuildSideSelection.withName(buildSideStr)
    val logCardinality = get(LOG_JOIN_CARDINALITY)
    val sizeEstimateThreshold = get(JOIN_GATHERER_SIZE_ESTIMATE_THRESHOLD)
    JoinOptions(strategy, buildSideSelection, targetSize, logCardinality, sizeEstimateThreshold)
  }

  lazy val sizedJoinPartitionAmplification: Double = get(SIZED_JOIN_PARTITION_AMPLIFICATION)

  lazy val stableSort: Boolean = get(STABLE_SORT)

  lazy val isFileScanPrunePartitionEnabled: Boolean = get(FILE_SCAN_PRUNE_PARTITION_ENABLED)

  lazy val isIncompatEnabled: Boolean = get(INCOMPATIBLE_OPS)

  lazy val incompatDateFormats: Boolean = get(INCOMPATIBLE_DATE_FORMATS)

  lazy val includeImprovedFloat: Boolean = get(IMPROVED_FLOAT_OPS)

  lazy val pinnedPoolSize: Long = get(PINNED_POOL_SIZE)

  lazy val pinnedPoolCuioDefault: Boolean = get(PINNED_POOL_SET_CUIO_DEFAULT)

  lazy val pinnedPoolParallelInitThreads: String = get(PINNED_POOL_PARALLEL_INIT_THREADS)

  lazy val offHeapLimitEnabled: Boolean = get(OFF_HEAP_LIMIT_ENABLED)

  lazy val offHeapLimit: Option[Long] = get(OFF_HEAP_LIMIT_SIZE)

  lazy val cgroupsMemoryLimitPath: Option[String] = get(CGROUPS_MEMORY_LIMIT_PATH)

  lazy val cgroupsMemoryUsagePath: Option[String] = get(CGROUPS_MEMORY_USAGE_PATH)

  lazy val perTaskOverhead: Long = get(TASK_OVERHEAD_SIZE)

  lazy val concurrentGpuTasks: Option[Integer] = get(CONCURRENT_GPU_TASKS)

  lazy val maxConcurrentGpuTasks: Integer = get(MAX_CONCURRENT_GPU_TASKS)

  lazy val isTestEnabled: Boolean = get(TEST_CONF)

  lazy val isRetryContextCheckEnabled: Boolean = get(TEST_RETRY_CONTEXT_CHECK_ENABLED)

  lazy val isFoldableNonLitAllowed: Boolean = get(FOLDABLE_NON_LIT_ALLOWED)

  lazy val asyncWriteMaxInFlightHostMemoryBytes: Long =
    get(ASYNC_WRITE_MAX_IN_FLIGHT_HOST_MEMORY_BYTES)

  lazy val asyncReadMaxInFlightHostMemoryBytes: Long =
    get(ASYNC_READ_MAX_IN_FLIGHT_HOST_MEMORY_BYTES)

  lazy val testingAllowedNonGpu: Seq[String] = get(TEST_ALLOWED_NONGPU)

  lazy val validateExecsInGpuPlan: Seq[String] = get(TEST_VALIDATE_EXECS_ONGPU)

  lazy val logQueryTransformations: Boolean = get(LOG_TRANSFORMATIONS)

  lazy val rmmDebugLocation: String = get(RMM_DEBUG)

  lazy val sparkRmmDebugLocation: String = get(SPARK_RMM_STATE_DEBUG)

  lazy val sparkRmmStateEnable: Boolean = get(SPARK_RMM_STATE_ENABLE)

  lazy val gpuOomDumpDir: Option[String] = get(GPU_OOM_DUMP_DIR)

  lazy val gpuOomMaxRetries: Int = get(GPU_OOM_MAX_RETRIES)

  lazy val isR2cRetryEnabled: Boolean = get(ENABLE_R2C_RETRY)

  lazy val gpuCoreDumpDir: Option[String] = get(GPU_COREDUMP_DIR)

  lazy val gpuCoreDumpPipePattern: String = get(GPU_COREDUMP_PIPE_PATTERN)

  lazy val isGpuCoreDumpFull: Boolean = get(GPU_COREDUMP_FULL)

  lazy val isGpuCoreDumpCompressed: Boolean = get(GPU_COREDUMP_COMPRESS)

  lazy val gpuCoreDumpCompressionCodec: String = get(GPU_COREDUMP_COMPRESSION_CODEC)

  lazy val isUvmEnabled: Boolean = get(UVM_ENABLED)

  lazy val rmmPool: String = {
    var pool = get(RMM_POOL)
    if ("ASYNC".equalsIgnoreCase(pool)) {
      val driverVersion = Cuda.getDriverVersion
      val runtimeVersion = Cuda.getRuntimeVersion
      var fallbackMessage: Option[String] = None
      if (runtimeVersion < 11020 || driverVersion < 11020) {
        fallbackMessage = Some("CUDA runtime/driver does not support the ASYNC allocator")
      } else if (driverVersion < 11050) {
        fallbackMessage = Some("CUDA drivers before 11.5 have known incompatibilities with " +
          "the ASYNC allocator")
      }
      if (fallbackMessage.isDefined) {
        logWarning(s"${fallbackMessage.get}, falling back to ARENA")
        pool = "ARENA"
      }
    }
    pool
  }

  lazy val rmmExactAlloc: Option[Long] = get(RMM_EXACT_ALLOC)

  lazy val rmmAllocFraction: Double = get(RMM_ALLOC_FRACTION)

  lazy val rmmAllocMaxFraction: Double = get(RMM_ALLOC_MAX_FRACTION)

  lazy val rmmAllocMinFraction: Double = get(RMM_ALLOC_MIN_FRACTION)

  lazy val rmmAllocReserve: Long = get(RMM_ALLOC_RESERVE)

  lazy val integratedGpuMemoryFraction: Double = get(INTEGRATED_GPU_MEMORY_FRACTION)

  lazy val hostSpillStorageSize: Long = get(HOST_SPILL_STORAGE_SIZE)

  lazy val partialFileBufferInitialSize: Long = get(PARTIAL_FILE_BUFFER_INITIAL_SIZE)

  lazy val partialFileBufferMaxSize: Long = get(PARTIAL_FILE_BUFFER_MAX_SIZE)

  lazy val partialFileBufferMemoryThreshold: Double = get(PARTIAL_FILE_BUFFER_MEMORY_THRESHOLD)

  lazy val isUnspillEnabled: Boolean = get(UNSPILL)

  lazy val needDecimalGuarantees: Boolean = get(NEED_DECIMAL_OVERFLOW_GUARANTEES)

  lazy val gpuTargetBatchSizeBytes: Long = get(GPU_BATCH_SIZE_BYTES)

  lazy val isWindowCollectListEnabled: Boolean = get(ENABLE_WINDOW_COLLECT_LIST)

  lazy val isWindowCollectSetEnabled: Boolean = get(ENABLE_WINDOW_COLLECT_SET)

  lazy val isWindowUnboundedAggEnabled: Boolean = get(ENABLE_WINDOW_UNBOUNDED_AGG)

  lazy val isWindowGroupLimitOptEnabled: Boolean = get(ENABLE_WINDOW_GROUP_LIMIT_OPT)

  lazy val isFloatAggEnabled: Boolean = get(ENABLE_FLOAT_AGG)

  lazy val explain: String = get(EXPLAIN)

  lazy val shouldExplain: Boolean = !explain.equalsIgnoreCase("NONE")

  lazy val shouldExplainAll: Boolean = explain.equalsIgnoreCase("ALL")

  lazy val chunkedReaderEnabled: Boolean = get(CHUNKED_READER)

  lazy val limitChunkedReaderMemoryUsage: Boolean = {
    val hasLimit = get(LIMIT_CHUNKED_READER_MEMORY_USAGE)
    val deprecatedConf = get(CHUNKED_SUBPAGE_READER)
    if (deprecatedConf.isDefined) {
      logWarning(s"'${CHUNKED_SUBPAGE_READER.key}' is deprecated and is replaced by " +
        s"'${LIMIT_CHUNKED_READER_MEMORY_USAGE}'.")
      if (hasLimit.isDefined && hasLimit.get != deprecatedConf.get) {
        throw new IllegalStateException(s"Both '${CHUNKED_SUBPAGE_READER.key}' and " +
          s"'${LIMIT_CHUNKED_READER_MEMORY_USAGE.key}' are set but using different values.")
      }
    }
    hasLimit.getOrElse(deprecatedConf.getOrElse(true))
  }

  lazy val chunkedReaderMemoryUsageRatio: Double = get(CHUNKED_READER_MEMORY_USAGE_RATIO)

  lazy val maxReadBatchSizeRows: Int = get(MAX_READER_BATCH_SIZE_ROWS)

  lazy val maxReadBatchSizeBytes: Long = get(MAX_READER_BATCH_SIZE_BYTES)

  lazy val maxGpuColumnSizeBytes: Long = get(MAX_GPU_COLUMN_SIZE_BYTES)

  lazy val outputDebugDumpPrefix: Option[String] = get(OUTPUT_DEBUG_DUMP_PREFIX)

  lazy val parquetDebugDumpPrefix: Option[String] = get(PARQUET_DEBUG_DUMP_PREFIX)

  lazy val parquetDebugDumpAlways: Boolean = get(PARQUET_DEBUG_DUMP_ALWAYS)

  lazy val orcDebugDumpPrefix: Option[String] = get(ORC_DEBUG_DUMP_PREFIX)

  lazy val orcDebugDumpAlways: Boolean = get(ORC_DEBUG_DUMP_ALWAYS)

  lazy val avroDebugDumpPrefix: Option[String] = get(AVRO_DEBUG_DUMP_PREFIX)

  lazy val avroDebugDumpAlways: Boolean = get(AVRO_DEBUG_DUMP_ALWAYS)

  lazy val useHybridParquetReader: Boolean = get(HYBRID_PARQUET_READER)

  lazy val hybridParquetPreloadBatches: Int = get(HYBRID_PARQUET_PRELOAD_CAP)

  lazy val loadHybridBackend: Boolean = get(LOAD_HYBRID_BACKEND)

  lazy val pushDownFiltersToHybrid: String = get(PUSH_DOWN_FILTERS_TO_HYBRID)

  lazy val hybridExprsWhitelist: String = get(HYBRID_EXPRS_WHITELIST)

  lazy val hashAggReplaceMode: String = get(HASH_AGG_REPLACE_MODE)

  lazy val partialMergeDistinctEnabled: Boolean = get(PARTIAL_MERGE_DISTINCT_ENABLED)

  lazy val enableReplaceSortMergeJoin: Boolean = get(ENABLE_REPLACE_SORTMERGEJOIN)

  lazy val enableHashOptimizeSort: Boolean = get(ENABLE_HASH_OPTIMIZE_SORT)

  lazy val enableFoldLocalAggregate: Boolean = get(ENABLE_FOLD_LOCAL_AGGREGATE)

  lazy val areInnerJoinsEnabled: Boolean = get(ENABLE_INNER_JOIN)

  lazy val areCrossJoinsEnabled: Boolean = get(ENABLE_CROSS_JOIN)

  lazy val areLeftOuterJoinsEnabled: Boolean = get(ENABLE_LEFT_OUTER_JOIN)

  lazy val areRightOuterJoinsEnabled: Boolean = get(ENABLE_RIGHT_OUTER_JOIN)

  lazy val areFullOuterJoinsEnabled: Boolean = get(ENABLE_FULL_OUTER_JOIN)

  lazy val areLeftSemiJoinsEnabled: Boolean = get(ENABLE_LEFT_SEMI_JOIN)

  lazy val areLeftAntiJoinsEnabled: Boolean = get(ENABLE_LEFT_ANTI_JOIN)

  lazy val areExistenceJoinsEnabled: Boolean = get(ENABLE_EXISTENCE_JOIN)

  lazy val isCastDecimalToFloatEnabled: Boolean = get(ENABLE_CAST_DECIMAL_TO_FLOAT)

  lazy val isCastFloatToDecimalEnabled: Boolean = get(ENABLE_CAST_FLOAT_TO_DECIMAL)

  lazy val isCastFloatToStringEnabled: Boolean = get(ENABLE_CAST_FLOAT_TO_STRING)

  lazy val isFloatFormatNumberEnabled: Boolean = get(ENABLE_FLOAT_FORMAT_NUMBER)

  lazy val isCastStringToTimestampEnabled: Boolean = get(ENABLE_CAST_STRING_TO_TIMESTAMP)

  lazy val hasExtendedYearValues: Boolean = get(HAS_EXTENDED_YEAR_VALUES)

  lazy val isCastStringToFloatEnabled: Boolean = get(ENABLE_CAST_STRING_TO_FLOAT)

  lazy val isCastFloatToIntegralTypesEnabled: Boolean = get(ENABLE_CAST_FLOAT_TO_INTEGRAL_TYPES)

  lazy val isProjectAstEnabled: Boolean = get(ENABLE_PROJECT_AST)

  lazy val isTieredProjectEnabled: Boolean = get(ENABLE_TIERED_PROJECT)

  lazy val isCombinedExpressionsEnabled: Boolean = get(ENABLE_COMBINED_EXPRESSIONS)

  lazy val isRlikeRegexRewriteEnabled: Boolean = get(ENABLE_RLIKE_REGEX_REWRITE)

  lazy val isExpandPreprojectEnabled: Boolean = get(ENABLE_EXPAND_PREPROJECT)

  lazy val isCoalesceAfterExpandEnabled: Boolean = get(ENABLE_COALESCE_AFTER_EXPAND)

  lazy val multiThreadReadNumThreads: Int = {
    // Use the largest value set among all the options.
    val deprecatedConfs = Seq(
      PARQUET_MULTITHREAD_READ_NUM_THREADS,
      ORC_MULTITHREAD_READ_NUM_THREADS,
      AVRO_MULTITHREAD_READ_NUM_THREADS)
    val values = get(MULTITHREAD_READ_NUM_THREADS) +: deprecatedConfs.flatMap { deprecatedConf =>
      val confValue = get(deprecatedConf)
      confValue.foreach { _ =>
        logWarning(s"$deprecatedConf is deprecated, use $MULTITHREAD_READ_NUM_THREADS. " +
          "Conflicting multithreaded read thread count settings will use the largest value.")
      }
      confValue
    }
    values.max
  }

  lazy val numFilesFilterParallel: Int = get(NUM_FILES_FILTER_PARALLEL)

  lazy val enableMultiThreadReadMemoryLimit: Boolean = get(MULTITHREAD_READ_MEMORY_LIMIT_ENABLED)

  lazy val multiThreadReadMemoryLimit: Long = get(MULTITHREAD_READ_MEMORY_LIMIT_SIZE)

  lazy val multiThreadReadMemoryAcquireTimeout: Long =
    get(MULTITHREAD_READ_MEMORY_LIMIT_ACQUISITION_TIMEOUT)

  lazy val multiThreadReadStageLevelPool: Boolean =
    get(MULTITHREAD_READ_MEMORY_LIMIT_TEST_PER_STAGE_POOL)

  lazy val isParquetEnabled: Boolean = get(ENABLE_PARQUET)

  lazy val isParquetInt96WriteEnabled: Boolean = get(ENABLE_PARQUET_INT96_WRITE)

  lazy val parquetWriterRowGroupSizeRows: Option[Integer] =
    get(PARQUET_WRITER_ROW_GROUP_SIZE_ROWS)

  lazy val parquetWriterRowGroupSizeBytes: Option[Long] =
    get(PARQUET_WRITER_ROW_GROUP_SIZE_BYTES)

  lazy val parquetReaderFooterType: ParquetFooterReaderType.Value = {
    get(PARQUET_READER_FOOTER_TYPE) match {
      case "AUTO" => ParquetFooterReaderType.AUTO
      case "NATIVE" => ParquetFooterReaderType.NATIVE
      case "JAVA" => ParquetFooterReaderType.JAVA
      case other =>
        throw new IllegalArgumentException(s"Internal Error $other is not supported for " +
            s"${PARQUET_READER_FOOTER_TYPE.key}")
    }
  }

  lazy val isParquetPerFileReadEnabled: Boolean =
    RapidsReaderType.withName(get(PARQUET_READER_TYPE)) == RapidsReaderType.PERFILE

  lazy val isParquetAutoReaderEnabled: Boolean =
    RapidsReaderType.withName(get(PARQUET_READER_TYPE)) == RapidsReaderType.AUTO

  lazy val isParquetCoalesceFileReadEnabled: Boolean = isParquetAutoReaderEnabled ||
    RapidsReaderType.withName(get(PARQUET_READER_TYPE)) == RapidsReaderType.COALESCING

  lazy val isParquetMultiThreadReadEnabled: Boolean = isParquetAutoReaderEnabled ||
    RapidsReaderType.withName(get(PARQUET_READER_TYPE)) == RapidsReaderType.MULTITHREADED

  lazy val parquetDecompressCpu: Boolean = get(PARQUET_DECOMPRESS_CPU)

  lazy val parquetDecompressCpuSnappy: Boolean = get(PARQUET_DECOMPRESS_CPU_SNAPPY)

  lazy val parquetDecompressCpuZstd: Boolean = get(PARQUET_DECOMPRESS_CPU_ZSTD)

  lazy val maxNumParquetFilesParallel: Int = get(PARQUET_MULTITHREAD_READ_MAX_NUM_FILES_PARALLEL)

  lazy val isParquetReadEnabled: Boolean = get(ENABLE_PARQUET_READ)

  lazy val getMultithreadedCombineThreshold: Long =
    get(READER_MULTITHREADED_COMBINE_THRESHOLD)

  lazy val getMultithreadedCombineWaitTime: Int =
    get(READER_MULTITHREADED_COMBINE_WAIT_TIME)

  lazy val getMultithreadedReaderKeepOrder: Boolean =
    get(READER_MULTITHREADED_READ_KEEP_ORDER)

  lazy val isParquetWriteEnabled: Boolean = get(ENABLE_PARQUET_WRITE)

  lazy val isOrcEnabled: Boolean = get(ENABLE_ORC)

  lazy val isOrcReadEnabled: Boolean = get(ENABLE_ORC_READ)

  lazy val isOrcWriteEnabled: Boolean = get(ENABLE_ORC_WRITE)

  lazy val isOrcFloatTypesToStringEnable: Boolean = get(ENABLE_ORC_FLOAT_TYPES_TO_STRING)

  lazy val isOrcPerFileReadEnabled: Boolean =
    RapidsReaderType.withName(get(ORC_READER_TYPE)) == RapidsReaderType.PERFILE

  lazy val isOrcAutoReaderEnabled: Boolean =
    RapidsReaderType.withName(get(ORC_READER_TYPE)) == RapidsReaderType.AUTO

  lazy val isOrcCoalesceFileReadEnabled: Boolean = isOrcAutoReaderEnabled ||
    RapidsReaderType.withName(get(ORC_READER_TYPE)) == RapidsReaderType.COALESCING

  lazy val isOrcMultiThreadReadEnabled: Boolean = isOrcAutoReaderEnabled ||
    RapidsReaderType.withName(get(ORC_READER_TYPE)) == RapidsReaderType.MULTITHREADED

  lazy val maxNumOrcFilesParallel: Int = get(ORC_MULTITHREAD_READ_MAX_NUM_FILES_PARALLEL)

  lazy val testOrcStripeSizeRows: Option[Integer] = get(TEST_ORC_STRIPE_SIZE_ROWS)

  lazy val isOrcBoolTypeEnabled: Boolean = get(ENABLE_ORC_BOOL)

  lazy val isCsvEnabled: Boolean = get(ENABLE_CSV)

  lazy val isCsvReadEnabled: Boolean = get(ENABLE_CSV_READ)

  lazy val isCsvFloatReadEnabled: Boolean = get(ENABLE_READ_CSV_FLOATS)

  lazy val isCsvDoubleReadEnabled: Boolean = get(ENABLE_READ_CSV_DOUBLES)

  lazy val isCsvDecimalReadEnabled: Boolean = get(ENABLE_READ_CSV_DECIMALS)

  lazy val isJsonEnabled: Boolean = get(ENABLE_JSON)

  lazy val isJsonReadEnabled: Boolean = get(ENABLE_JSON_READ)

  lazy val isJsonFloatReadEnabled: Boolean = get(ENABLE_READ_JSON_FLOATS)

  lazy val isJsonDoubleReadEnabled: Boolean = get(ENABLE_READ_JSON_DOUBLES)

  lazy val isJsonDecimalReadEnabled: Boolean = get(ENABLE_READ_JSON_DECIMALS)

  lazy val isJsonDateTimeReadEnabled: Boolean = get(ENABLE_READ_JSON_DATE_TIME)

  lazy val isAvroEnabled: Boolean = get(ENABLE_AVRO)

  lazy val isAvroReadEnabled: Boolean = get(ENABLE_AVRO_READ)

  lazy val isAvroPerFileReadEnabled: Boolean =
    RapidsReaderType.withName(get(AVRO_READER_TYPE)) == RapidsReaderType.PERFILE

  lazy val isAvroAutoReaderEnabled: Boolean =
    RapidsReaderType.withName(get(AVRO_READER_TYPE)) == RapidsReaderType.AUTO

  lazy val isAvroCoalesceFileReadEnabled: Boolean = isAvroAutoReaderEnabled ||
    RapidsReaderType.withName(get(AVRO_READER_TYPE)) == RapidsReaderType.COALESCING

  lazy val isAvroMultiThreadReadEnabled: Boolean = isAvroAutoReaderEnabled ||
    RapidsReaderType.withName(get(AVRO_READER_TYPE)) == RapidsReaderType.MULTITHREADED

  lazy val maxNumAvroFilesParallel: Int = get(AVRO_MULTITHREAD_READ_MAX_NUM_FILES_PARALLEL)

  lazy val isDeltaWriteEnabled: Boolean = get(ENABLE_DELTA_WRITE)

  lazy val isIcebergEnabled: Boolean = get(ENABLE_ICEBERG)

  lazy val isIcebergReadEnabled: Boolean = get(ENABLE_ICEBERG_READ)

  lazy val isIcebergV3Enabled: Boolean = get(ENABLE_ICEBERG_V3)

  lazy val validateIcebergDeletionVectorCrc: Boolean =
    get(VALIDATE_ICEBERG_DELETION_VECTOR_CRC)

  lazy val isIcebergWriteEnabled: Boolean = get(ENABLE_ICEBERG_WRITE)

  lazy val isHiveDelimitedTextEnabled: Boolean = get(ENABLE_HIVE_TEXT)

  lazy val isHiveDelimitedTextReadEnabled: Boolean = get(ENABLE_HIVE_TEXT_READ)

  lazy val isHiveDelimitedTextWriteEnabled: Boolean = get(ENABLE_HIVE_TEXT_WRITE)

  lazy val shouldHiveReadFloats: Boolean = get(ENABLE_READ_HIVE_FLOATS)

  lazy val shouldHiveReadDoubles: Boolean = get(ENABLE_READ_HIVE_DOUBLES)

  lazy val shouldHiveReadDecimals: Boolean = get(ENABLE_READ_HIVE_DECIMALS)

  lazy val shuffleManagerEnabled: Boolean = get(SHUFFLE_MANAGER_ENABLED)

  lazy val shuffleManagerMode: String = get(SHUFFLE_MANAGER_MODE)

  lazy val shuffleTransportClassName: String = get(SHUFFLE_TRANSPORT_CLASS_NAME)

  lazy val shuffleTransportEarlyStartHeartbeatInterval: Int = get(
    SHUFFLE_TRANSPORT_EARLY_START_HEARTBEAT_INTERVAL)

  lazy val shuffleTransportEarlyStartHeartbeatTimeout: Int = get(
    SHUFFLE_TRANSPORT_EARLY_START_HEARTBEAT_TIMEOUT)

  lazy val shuffleTransportEarlyStart: Boolean = get(SHUFFLE_TRANSPORT_EARLY_START)

  lazy val shuffleTransportMaxReceiveInflightBytes: Long = get(
    SHUFFLE_TRANSPORT_MAX_RECEIVE_INFLIGHT_BYTES)

  lazy val shuffleUcxActiveMessagesForceRndv: Boolean = get(SHUFFLE_UCX_ACTIVE_MESSAGES_FORCE_RNDV)

  lazy val shuffleUcxUseWakeup: Boolean = get(SHUFFLE_UCX_USE_WAKEUP)

  lazy val shuffleUcxListenerStartPort: Int = get(SHUFFLE_UCX_LISTENER_START_PORT)

  lazy val shuffleUcxMgmtHost: String = get(SHUFFLE_UCX_MGMT_SERVER_HOST)

  lazy val shuffleUcxMgmtConnTimeout: Int = get(SHUFFLE_UCX_MGMT_CONNECTION_TIMEOUT)

  lazy val shuffleUcxBounceBuffersSize: Long = get(SHUFFLE_UCX_BOUNCE_BUFFERS_SIZE)

  lazy val shuffleUcxDeviceBounceBuffersCount: Int = get(SHUFFLE_UCX_BOUNCE_BUFFERS_DEVICE_COUNT)

  lazy val shuffleUcxHostBounceBuffersCount: Int = get(SHUFFLE_UCX_BOUNCE_BUFFERS_HOST_COUNT)

  lazy val shuffleMaxClientThreads: Int = get(SHUFFLE_MAX_CLIENT_THREADS)

  lazy val shuffleMaxClientTasks: Int = get(SHUFFLE_MAX_CLIENT_TASKS)

  lazy val shuffleClientThreadKeepAliveTime: Int = get(SHUFFLE_CLIENT_THREAD_KEEPALIVE)

  lazy val shuffleMaxServerTasks: Int = get(SHUFFLE_MAX_SERVER_TASKS)

  lazy val shuffleMaxMetadataSize: Long = get(SHUFFLE_MAX_METADATA_SIZE)

  lazy val shuffleCompressionCodec: String = get(SHUFFLE_COMPRESSION_CODEC)

  lazy val shuffleCompressionLz4ChunkSize: Long = get(SHUFFLE_COMPRESSION_LZ4_CHUNK_SIZE)

  lazy val shuffleCompressionZstdChunkSize: Long = get(SHUFFLE_COMPRESSION_ZSTD_CHUNK_SIZE)

  lazy val shuffleCompressionMaxBatchMemory: Long = get(SHUFFLE_COMPRESSION_MAX_BATCH_MEMORY)

  lazy val shuffleMultiThreadedMaxBytesInFlight: Long =
    get(SHUFFLE_MULTITHREADED_MAX_BYTES_IN_FLIGHT)

  lazy val shuffleMultiThreadedWriterThreads: Int = get(SHUFFLE_MULTITHREADED_WRITER_THREADS)

  lazy val shuffleMultiThreadedReaderThreads: Int = get(SHUFFLE_MULTITHREADED_READER_THREADS)

  lazy val shuffleParitioningMaxCpuBatchSize: Long = get(SHUFFLE_PARTITIONING_MAX_CPU_BATCH_SIZE)

  lazy val shuffleKudoSerializerEnabled: Boolean = get(SHUFFLE_KUDO_SERIALIZER_ENABLED)

  lazy val shuffleKudoWriteMode: ShuffleKudoMode.Value =
    ShuffleKudoMode.withName(get(SHUFFLE_KUDO_WRITE_MODE))

  lazy val shuffleKudoReadMode: ShuffleKudoMode.Value =
    ShuffleKudoMode.withName(get(SHUFFLE_KUDO_READ_MODE))

  lazy val shuffleKudoMeasureBufferCopyEnabled: Boolean =
    get(SHUFFLE_KUDO_SERIALIZER_MEASURE_BUFFER_COPY_ENABLED)

  lazy val shuffleAsyncReadEnabled: Boolean = get(SHUFFLE_ASYNC_READ_ENABLED)

  lazy val shuffleKudoSerializerDebugMode: DumpOption = {
    val mode = get(SHUFFLE_KUDO_SERIALIZER_DEBUG_MODE)
    mode match {
      case "NEVER" => DumpOption.Never
      case "ALWAYS" => DumpOption.Always
      case "ONFAILURE" => DumpOption.OnFailure
      case _ => {
        logWarning(s"Invalid value for ${SHUFFLE_KUDO_SERIALIZER_DEBUG_MODE.key}: $mode. " +
          "Using default value: Never")
        DumpOption.Never
      }
    }
  }

  lazy val shuffleKudoSerializerDebugDumpPrefix: Option[String] = {
    val prefix = get(SHUFFLE_KUDO_SERIALIZER_DEBUG_DUMP_PREFIX)
    shuffleKudoSerializerDebugMode match {
      case DumpOption.Never => prefix
      case _ => {
        prefix match {
          case None => {
            logWarning("spark.rapids.shuffle.kudo.serializer.debug.dump.path.prefix is not set, " +
              "so Kudo serializer debug will not be enabled")
            None
          }
          case Some(p) => {
            if (p.isEmpty) {
              logWarning("spark.rapids.shuffle.kudo.serializer.debug.dump.path.prefix is empty, " +
                "so Kudo serializer debug will not be enabled")
              Some("")
            } else {
              Some(p)
            }
          }
        }
      }
    }
  }

  def isUCXShuffleManagerMode: Boolean =
    RapidsShuffleManagerMode
      .withName(get(SHUFFLE_MANAGER_MODE)) == RapidsShuffleManagerMode.UCX

  def isMultiThreadedShuffleManagerMode: Boolean =
    RapidsShuffleManagerMode
      .withName(get(SHUFFLE_MANAGER_MODE)) == RapidsShuffleManagerMode.MULTITHREADED

  def isMultithreadedShuffleSkipMergeEnabled: Boolean = get(MULTITHREADED_SHUFFLE_SKIP_MERGE)

  def isCacheOnlyShuffleManagerMode: Boolean =
    RapidsShuffleManagerMode
      .withName(get(SHUFFLE_MANAGER_MODE)) == RapidsShuffleManagerMode.CACHE_ONLY

  def isGPUShuffle: Boolean = isUCXShuffleManagerMode || isCacheOnlyShuffleManagerMode

  def shuffleKudoGpuSerializerEnabled: Boolean = shuffleKudoSerializerEnabled &&
    shuffleKudoWriteMode == ShuffleKudoMode.GPU

  def shuffleKudoGpuSerializerReadEnabled: Boolean = shuffleKudoSerializerEnabled &&
    shuffleKudoReadMode == ShuffleKudoMode.GPU

  lazy val shimsProviderOverride: Option[String] = get(SHIMS_PROVIDER_OVERRIDE)

  lazy val cudfVersionOverride: Boolean = get(CUDF_VERSION_OVERRIDE)

  lazy val allowMultipleJars: AllowMultipleJars.Value = {
    get(ALLOW_MULTIPLE_JARS) match {
      case "ALWAYS" => AllowMultipleJars.ALWAYS
      case "NEVER" => AllowMultipleJars.NEVER
      case "SAME_REVISION" => AllowMultipleJars.SAME_REVISION
      case other =>
        throw new IllegalArgumentException(s"Internal Error $other is not supported for " +
            s"${ALLOW_MULTIPLE_JARS.key}")
    }
  }

  lazy val allowDisableEntirePlan: Boolean = get(ALLOW_DISABLE_ENTIRE_PLAN)

  lazy val useArrowCopyOptimization: Boolean = get(USE_ARROW_OPT)

  lazy val getCloudSchemes: Seq[String] =
    DEFAULT_CLOUD_SCHEMES ++ get(CLOUD_SCHEMES).getOrElse(Seq.empty)

  lazy val optimizerEnabled: Boolean = get(OPTIMIZER_ENABLED)

  lazy val optimizerExplain: String = get(OPTIMIZER_EXPLAIN)

  lazy val optimizerShouldExplainAll: Boolean = optimizerExplain.equalsIgnoreCase("ALL")

  lazy val optimizerClassName: String = get(OPTIMIZER_CLASS_NAME)

  lazy val defaultRowCount: Long = get(OPTIMIZER_DEFAULT_ROW_COUNT)

  lazy val defaultCpuOperatorCost: Double = get(OPTIMIZER_DEFAULT_CPU_OPERATOR_COST)

  lazy val defaultCpuExpressionCost: Double = get(OPTIMIZER_DEFAULT_CPU_EXPRESSION_COST)

  lazy val defaultGpuOperatorCost: Double = get(OPTIMIZER_DEFAULT_GPU_OPERATOR_COST)

  lazy val defaultGpuExpressionCost: Double = get(OPTIMIZER_DEFAULT_GPU_EXPRESSION_COST)

  lazy val cpuReadMemorySpeed: Double = get(OPTIMIZER_CPU_READ_SPEED)

  lazy val cpuWriteMemorySpeed: Double = get(OPTIMIZER_CPU_WRITE_SPEED)

  lazy val gpuReadMemorySpeed: Double = get(OPTIMIZER_GPU_READ_SPEED)

  lazy val gpuWriteMemorySpeed: Double = get(OPTIMIZER_GPU_WRITE_SPEED)

  lazy val driverTimeZone: Option[String] = get(DRIVER_TIMEZONE)

  lazy val isRangeWindowByteEnabled: Boolean = get(ENABLE_RANGE_WINDOW_BYTES)

  lazy val isRangeWindowShortEnabled: Boolean = get(ENABLE_RANGE_WINDOW_SHORT)

  lazy val isRangeWindowIntEnabled: Boolean = get(ENABLE_RANGE_WINDOW_INT)

  lazy val isRangeWindowLongEnabled: Boolean = get(ENABLE_RANGE_WINDOW_LONG)

  lazy val isRangeWindowFloatEnabled: Boolean = get(ENABLE_RANGE_WINDOW_FLOAT)

  lazy val isRangeWindowDoubleEnabled: Boolean = get(ENABLE_RANGE_WINDOW_DOUBLE)

  lazy val isRangeWindowDecimalEnabled: Boolean = get(ENABLE_RANGE_WINDOW_DECIMAL)

  lazy val batchedBoundedRowsWindowMax: Int = get(BATCHED_BOUNDED_ROW_WINDOW_MAX)

  lazy val allowSinglePassPartialSortAgg: Boolean = get(ENABLE_SINGLE_PASS_PARTIAL_SORT_AGG)

  lazy val forceSinglePassPartialSortAgg: Boolean = get(FORCE_SINGLE_PASS_PARTIAL_SORT_AGG)

  lazy val skipAggPassReductionRatio: Double = get(SKIP_AGG_PASS_REDUCTION_RATIO)

  lazy val isRegExpEnabled: Boolean = get(ENABLE_REGEXP)

  lazy val getSparkGpuResourceName: String = get(SPARK_GPU_RESOURCE_NAME)

  lazy val isCpuBasedUDFEnabled: Boolean = get(ENABLE_CPU_BASED_UDF)

  lazy val isFastSampleEnabled: Boolean = get(ENABLE_FAST_SAMPLE)

  lazy val isForceHiveHashForBucketedWrite: Boolean = get(FORCE_HIVE_HASH_FOR_BUCKETED_WRITE)

  lazy val isDetectDeltaLogQueries: Boolean = get(DETECT_DELTA_LOG_QUERIES)

  lazy val isDetectDeltaCheckpointQueries: Boolean = get(DETECT_DELTA_CHECKPOINT_QUERIES)

  lazy val concurrentWriterPartitionFlushSize: Long = get(CONCURRENT_WRITER_PARTITION_FLUSH_SIZE)

  lazy val isAqeExchangeReuseFixupEnabled: Boolean = get(ENABLE_AQE_EXCHANGE_REUSE_FIXUP)

  lazy val isNonAqeBroadcastReuseFixupEnabled: Boolean =
    get(ENABLE_NON_AQE_BROADCAST_REUSE_FIXUP)

  lazy val chunkedPackPoolSize: Long = get(CHUNKED_PACK_POOL_SIZE)

  lazy val chunkedPackBounceBufferSize: Long = get(CHUNKED_PACK_BOUNCE_BUFFER_SIZE)

  lazy val isProjectSplitRetryEnabled: Boolean = get(PROJECT_SPLIT_RETRY_ENABLED)

  lazy val chunkedPackBounceBufferCount: Int = get(CHUNKED_PACK_BOUNCE_BUFFER_COUNT)

  lazy val spillToDiskBounceBufferSize: Long = get(SPILL_TO_DISK_BOUNCE_BUFFER_SIZE)

  lazy val spillToDiskBounceBufferCount: Int = get(SPILL_TO_DISK_BOUNCE_BUFFER_COUNT)

  lazy val splitUntilSizeOverride: Option[Long] = get(SPLIT_UNTIL_SIZE_OVERRIDE)

  lazy val skipGpuArchCheck: Boolean = get(SKIP_GPU_ARCH_CHECK)

  lazy val testGetJsonObjectSavePath: Option[String] = get(TEST_GET_JSON_OBJECT_SAVE_PATH)

  lazy val testGetJsonObjectSaveRows: Int = get(TEST_GET_JSON_OBJECT_SAVE_ROWS)

  lazy val isDeltaLowShuffleMergeEnabled: Boolean = get(ENABLE_DELTA_LOW_SHUFFLE_MERGE)

  lazy val isDeltaDeletionVectorPredicatePushdownEnabled: Boolean =
    get(DELTA_DELETION_VECTOR_PREDICATE_PUSHDOWN)

  lazy val isHashFuncPartitioningEnabled: Boolean = get(ENABLE_HASH_FUNCTION_IN_PARTITIONING)

  lazy val isTagLoreIdEnabled: Boolean = get(TAG_LORE_ID_ENABLED)

  lazy val loreDumpIds: Map[LoreId, OutputLoreId] = get(LORE_DUMP_IDS)
    .map(OutputLoreId.parse)
    .getOrElse(Map.empty)

  lazy val loreDumpPath: Option[String] = get(LORE_DUMP_PATH)

  lazy val loreSkipDumpingPlan: Boolean = get(LORE_SKIP_DUMPING_PLAN)

  lazy val loreDumpNonStrictMode: Boolean = get(LORE_NON_STRICT_MODE)

  lazy val loreParquetUseOriginalNames: Boolean = get(LORE_PARQUET_USE_ORIGINAL_NAMES)

  lazy val caseWhenFuseEnabled: Boolean = get(CASE_WHEN_FUSE)

  lazy val isAsyncOutputWriteEnabled: Boolean = get(ENABLE_ASYNC_OUTPUT_WRITE)

  private val optimizerDefaults = Map(
    // this is not accurate because CPU projections do have a cost due to appending values
    // to each row that is produced, but this needs to be a really small number because
    // GpuProject cost is zero (in our cost model) and we don't want to encourage moving to
    // the GPU just to do a trivial projection, so we pretend the overhead of a
    // CPU projection (beyond evaluating the expressions) is also zero
    "spark.rapids.sql.optimizer.cpu.exec.ProjectExec" -> "0",
    // The cost of a GPU projection is mostly the cost of evaluating the expressions
    // to produce the projected columns
    "spark.rapids.sql.optimizer.gpu.exec.ProjectExec" -> "0",
    // union does not further process data produced by its children
    "spark.rapids.sql.optimizer.cpu.exec.UnionExec" -> "0",
    "spark.rapids.sql.optimizer.gpu.exec.UnionExec" -> "0"
  )

  def isOperatorEnabled(key: String, incompat: Boolean, isDisabledByDefault: Boolean): Boolean = {
    val default = !(isDisabledByDefault || incompat) || (incompat && isIncompatEnabled)
    conf.get(key).map(toBoolean(_, key)).getOrElse(default)
  }

  /**
   * Get the GPU cost of an expression, for use in the cost-based optimizer.
   */
  def getGpuExpressionCost(operatorName: String): Option[Double] = {
    val key = s"spark.rapids.sql.optimizer.gpu.expr.$operatorName"
    getOptionalCost(key)
  }

  /**
   * Get the GPU cost of an operator, for use in the cost-based optimizer.
   */
  def getGpuOperatorCost(operatorName: String): Option[Double] = {
    val key = s"spark.rapids.sql.optimizer.gpu.exec.$operatorName"
    getOptionalCost(key)
  }

  /**
   * Get the CPU cost of an expression, for use in the cost-based optimizer.
   */
  def getCpuExpressionCost(operatorName: String): Option[Double] = {
    val key = s"spark.rapids.sql.optimizer.cpu.expr.$operatorName"
    getOptionalCost(key)
  }

  /**
   * Get the CPU cost of an operator, for use in the cost-based optimizer.
   */
  def getCpuOperatorCost(operatorName: String): Option[Double] = {
    val key = s"spark.rapids.sql.optimizer.cpu.exec.$operatorName"
    getOptionalCost(key)
  }

  private def getOptionalCost(key: String) = {
    // user-provided value takes precedence, then look in defaults map
    conf.get(key).orElse(optimizerDefaults.get(key)).map(toDouble(_, key))
  }

  /**
   * To judge whether "key" is explicitly set by the users.
   */
  def isConfExplicitlySet(key: String): Boolean = {
    conf.contains(key)
  }

  // CPU-GPU Bridge Configuration accessors

  lazy val isCpuBridgeEnabled: Boolean = get(ENABLE_CPU_BRIDGE)

  lazy val bridgeDisallowList: Set[String] = {
    val listString = get(BRIDGE_DISALLOW_LIST)
    if (listString.nonEmpty) {
      listString.split(",").map(_.trim).filter(_.nonEmpty).toSet
    } else {
      Set.empty
    }
  }

  /**
   * Get the CPU bridge thread pool size override, if configured.
   */
  def getCpuBridgeThreadPoolSize: Option[Int] = get(CPU_BRIDGE_THREAD_POOL_SIZE).map(_.toInt)
}

case class OomInjectionConf(
  numOoms: Int,
  skipCount: Int,
  withSplit: Boolean,
  oomInjectionFilter: OomInjectionType
)
