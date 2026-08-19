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

import java.io.IOException
import java.nio.charset.{Charset, StandardCharsets}
import java.util.Locale

import scala.collection.JavaConverters._

import ai.rapids.cudf
import ai.rapids.cudf.{ColumnVector, DType, Scalar, Schema, Table}
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.jni.CastStrings
import com.nvidia.spark.rapids.shims.{ColumnDefaultValuesShims, ShimFilePartitionReaderFactory}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.csv.{CSVOptions, GpuCsvUtils}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.util.PermissiveMode
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.execution.datasources.{PartitionedFile, PartitioningAwareFileIndex}
import org.apache.spark.sql.execution.datasources.csv.CSVDataSource
import org.apache.spark.sql.execution.datasources.v2._
import org.apache.spark.sql.execution.datasources.v2.csv.CSVScan
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.LegacyTimeParserPolicy
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.SerializableConfiguration

trait ScanWithMetrics {
  //this is initialized by the exec post creation
  var metrics : Map[String, GpuMetric] = Map.empty
}

object GpuCSVScan extends Logging {
  private def tryLoadCharset(name: String): Option[Charset] = {
    try {
      Some(Charset.forName(name))
    } catch {
      case _: IllegalArgumentException =>
        logWarning(s"Failed to load charset for '$name'")
        None
    }
  }

  private def legacyCharsetEnabled(): Boolean = {
    try {
      // Use reflection to determine if this conf is defined to avoid the
      // complicated shim things due to only a boolean diff in CSV read.
      SQLConf.getClass.getMethod("LEGACY_JAVA_CHARSETS")
      // Spark 4.0.0 or later, since this conf is defined, so get the value from it.
      SQLConf.get.getConfString("spark.sql.legacy.javaCharsets", "false").toBoolean
    } catch {
      case _: NoSuchMethodException =>
        // Spark before 4.0.0, no this conf, so always returns true
        true
    }
  }

  private val utf8Charsets: Set[Charset] = Set(
    StandardCharsets.UTF_8,
    StandardCharsets.US_ASCII)

  // May have more in the future
  private def supportedCharsets(): Set[Charset] = {
    // Spark restricts charsets in CSV from 4.0.0.
    // See https://issues.apache.org/jira/browse/SPARK-48857
    if (legacyCharsetEnabled()) {
      utf8Charsets ++ tryLoadCharset("GBK")
    } else {
      utf8Charsets
    }
  }

  private def isSupportedCharset(name: String): Boolean = {
    try {
      name != null && supportedCharsets().contains(Charset.forName(name))
    } catch {
      case _: IllegalArgumentException => false
    }
  }

  def isUTF8Charset(charset: Charset): Boolean = utf8Charsets.contains(charset)

  def unsupportedLocaleForDecimalMessage(locale: Locale): String =
    s"GpuCSVScan only supports Locale.US decimal parsing, got $locale"

  def tagSupport(scanMeta: ScanMeta[CSVScan]) : Unit = {
    val scan = scanMeta.wrapped
    tagSupport(
      scan.sparkSession,
      scan.dataSchema,
      scan.readDataSchema,
      scan.options.asScala.toMap,
      scanMeta)
  }

  def tagSupport(
      sparkSession: SparkSession,
      dataSchema: StructType,
      readSchema: StructType,
      options: Map[String, String],
      meta: RapidsMeta[_, _, _]): Unit = {
    val parsedOptions: CSVOptions = new CSVOptions(
      options,
      columnPruning = sparkSession.sessionState.conf.csvColumnPruning,
      sparkSession.sessionState.conf.sessionLocalTimeZone,
      sparkSession.sessionState.conf.columnNameOfCorruptRecord)

    if (!meta.conf.isCsvEnabled) {
      meta.willNotWorkOnGpu("CSV input and output has been disabled. To enable set" +
        s"${RapidsConf.ENABLE_CSV} to true")
    }

    if (!meta.conf.isCsvReadEnabled) {
      meta.willNotWorkOnGpu("CSV input has been disabled. To enable set" +
        s"${RapidsConf.ENABLE_CSV_READ} to true")
    }

    if (!parsedOptions.enforceSchema) {
      meta.willNotWorkOnGpu("GpuCSVScan always enforces schemas")
    }

    if (dataSchema == null || dataSchema.isEmpty) {
      meta.willNotWorkOnGpu("GpuCSVScan requires a specified data schema")
    }

    if (parsedOptions.delimiter.length > 1) {
      meta.willNotWorkOnGpu("GpuCSVScan does not support multi-character delimiters")
    }

    if (parsedOptions.delimiter.codePointAt(0) > 127) {
      meta.willNotWorkOnGpu("GpuCSVScan does not support non-ASCII delimiters")
    }

    if (parsedOptions.quote > 127) {
      meta.willNotWorkOnGpu("GpuCSVScan does not support non-ASCII quote chars")
    }

    if (parsedOptions.comment > 127) {
      meta.willNotWorkOnGpu("GpuCSVScan does not support non-ASCII comment chars")
    }

    if (parsedOptions.escape != '\\') {
      meta.willNotWorkOnGpu("GpuCSVScan does not support modified escape chars")
    }

    if (parsedOptions.charToEscapeQuoteEscaping.isDefined) {
      meta.willNotWorkOnGpu("GPU CSV Parsing does not support charToEscapeQuoteEscaping")
    }

    if (!isSupportedCharset(parsedOptions.charset)) {
      meta.willNotWorkOnGpu(s"GpuCSVScan only supports " +
        s"${supportedCharsets().mkString("[", ", ", "]")} encoded data")
    }

    // TODO parsedOptions.ignoreLeadingWhiteSpaceInRead cudf always does this, but not for strings
    // TODO parsedOptions.ignoreTrailingWhiteSpaceInRead cudf always does this, but not for strings
    // TODO parsedOptions.multiLine cudf always does this, but it is not the default and it is not
    //  consistent

    if (parsedOptions.lineSeparator.getOrElse("\n") != "\n") {
      meta.willNotWorkOnGpu("GpuCSVScan only supports \"\\n\" as a line separator")
    }

    if (parsedOptions.parseMode != PermissiveMode) {
      meta.willNotWorkOnGpu("GpuCSVScan only supports Permissive CSV parsing")
    }

    // TODO parsedOptions.nanValue This is here by default so we should support it, but cudf
    // make it null https://github.com/NVIDIA/spark-rapids/issues/125
    parsedOptions.positiveInf.toLowerCase() match {
      case "inf" | "+inf" | "infinity" | "+infinity" =>
      case _ =>
        meta.willNotWorkOnGpu(s"the positive infinity value '${parsedOptions.positiveInf}'" +
            s" is not supported'")
    }
    parsedOptions.negativeInf.toLowerCase() match {
      case "-inf" | "-infinity" =>
      case _ =>
        meta.willNotWorkOnGpu(s"the positive infinity value '${parsedOptions.positiveInf}'" +
            s" is not supported'")
    }
    if (parsedOptions.maxCharsPerColumn != -1) {
      meta.willNotWorkOnGpu("GpuCSVScan does not support maxCharsPerColumn")
    }

    // parsedOptions.maxColumns was originally a performance optimization but is not used any more

    val types = readSchema.map(_.dataType).toSet
    if (GpuOverrides.getTimeParserPolicy == LegacyTimeParserPolicy &&
        (types.contains(DateType) ||
        types.contains(TimestampType))) {
      // Spark's CSV parser will parse the string "2020-50-16" to the date 2024/02/16 when
      // timeParserPolicy is set to LEGACY mode and we would reject this as an invalid date
      // so we fall back to CPU
      meta.willNotWorkOnGpu(s"GpuCSVScan does not support timeParserPolicy=LEGACY")
    }

    if (types.contains(DateType)) {
      GpuTextBasedDateUtils.tagCudfFormat(meta,
        GpuCsvUtils.dateFormatInRead(parsedOptions), parseString = true)

      // For date type, timezone needs to be checked also. This is because JVM timezone is used
      // to get days offset before rebasing Julian to Gregorian in Spark while not in Rapids.
      //
      // In details, for CSV data format, Spark uses dateFormatter to parse string as date data
      // type which utilizes [[org.apache.spark.sql.catalyst.DateFormatter]]. And CSV format
      // (e.g., [[UnivocityParser]]), it uses [[LegacyFastDateFormatter]] which is based on
      // Apache Commons FastDateFormat. It parse string into Java util.Date base on JVM default
      // timezone. From Java util.Date, it's converted into java.sql.Date type.
      // By leveraging [[JavaDateTimeUtils]], it finally do `rebaseJulianToGregorianDays`
      // considering its offset to UTC timezone.
      if (!GpuOverrides.isUTCTimezone(parsedOptions.zoneId)) {
        meta.willNotWorkOnGpu(s"Not supported timezone type ${parsedOptions.zoneId}.")
      }
    }

    if (types.contains(TimestampType)) {
      GpuTextBasedDateUtils.tagCudfFormat(meta,
        GpuCsvUtils.timestampFormatInRead(parsedOptions), parseString = true)

      if (!GpuOverrides.isUTCTimezone(parsedOptions.zoneId)) {
        meta.willNotWorkOnGpu(s"Not supported timezone type ${parsedOptions.zoneId}.")
      }
    }
    // TODO parsedOptions.emptyValueInRead

    if (GpuCsvUtils.enableDateTimeParsingFallback(parsedOptions) &&
        (types.contains(DateType) || types.contains(TimestampType))) {
      meta.willNotWorkOnGpu(s"GpuCSVScan does not support enableDateTimeParsingFallback")
    }

    if (!meta.conf.isCsvFloatReadEnabled && types.contains(FloatType)) {
      meta.willNotWorkOnGpu("CSV reading is not 100% compatible when reading floats. " +
        s"To enable it please set ${RapidsConf.ENABLE_READ_CSV_FLOATS} to true.")
    }

    if (!meta.conf.isCsvDoubleReadEnabled && types.contains(DoubleType)) {
      meta.willNotWorkOnGpu("CSV reading is not 100% compatible when reading doubles. " +
        s"To enable it please set ${RapidsConf.ENABLE_READ_CSV_DOUBLES} to true.")
    }

    if (!meta.conf.isCsvDecimalReadEnabled && types.exists(_.isInstanceOf[DecimalType])) {
      meta.willNotWorkOnGpu("CSV reading is not 100% compatible when reading decimals. " +
        s"To enable it please set ${RapidsConf.ENABLE_READ_CSV_DECIMALS} to true.")
    }

    if (parsedOptions.locale != Locale.US && types.exists(_.isInstanceOf[DecimalType])) {
      meta.willNotWorkOnGpu(GpuCSVScan.unsupportedLocaleForDecimalMessage(parsedOptions.locale))
    }

    if (ColumnDefaultValuesShims.hasExistenceDefaultValues(readSchema)) {
      meta.willNotWorkOnGpu("GpuCSVScan does not support default values in schema")
    }

    FileFormatChecks.tag(meta, readSchema, CsvFormatType, ReadFileOp)
  }
}

case class GpuCSVScan(
    sparkSession: SparkSession,
    fileIndex: PartitioningAwareFileIndex,
    dataSchema: StructType, // original schema passed in by the user (all the data)
    readDataSchema: StructType, // schema for data being read (including dropped columns)
    readPartitionSchema: StructType, // schema for the parts that come from the file path
    options: CaseInsensitiveStringMap,
    partitionFilters: Seq[Expression],
    dataFilters: Seq[Expression],
    maxReaderBatchSizeRows: Integer,
    maxReaderBatchSizeBytes: Long,
    maxGpuColumnSizeBytes: Long)
  extends TextBasedFileScan(sparkSession, options) with GpuScan {

  private lazy val parsedOptions: CSVOptions = new CSVOptions(
    options.asScala.toMap,
    columnPruning = sparkSession.sessionState.conf.csvColumnPruning,
    sparkSession.sessionState.conf.sessionLocalTimeZone,
    sparkSession.sessionState.conf.columnNameOfCorruptRecord)

  override def isSplitable(path: Path): Boolean = {
    CSVDataSource(parsedOptions).isSplitable && super.isSplitable(path)
  }

  override def getFileUnSplittableReason(path: Path): String = {
    assert(!isSplitable(path))
    if (!super.isSplitable(path)) {
      super.getFileUnSplittableReason(path)
    } else {
      "the csv datasource is set multiLine mode"
    }
  }

  override def createReaderFactory(): PartitionReaderFactory = {
    val caseSensitiveMap = options.asCaseSensitiveMap.asScala.toMap
    // Hadoop Configurations are case sensitive.
    val hadoopConf = sparkSession.sessionState.newHadoopConfWithOptions(caseSensitiveMap)
    val broadcastedConf = sparkSession.sparkContext.broadcast(
      new SerializableConfiguration(hadoopConf))

    GpuCSVPartitionReaderFactory(sparkSession.sessionState.conf, broadcastedConf,
      dataSchema, readDataSchema, readPartitionSchema, parsedOptions, maxReaderBatchSizeRows,
      maxReaderBatchSizeBytes, maxGpuColumnSizeBytes, metrics, options.asScala.toMap)
  }

  // overrides nothing in 330
  def withFilters(
      partitionFilters: Seq[Expression], dataFilters: Seq[Expression]): FileScan =
    this.copy(partitionFilters = partitionFilters, dataFilters = dataFilters)

  override def withInputFile(): GpuScan = this

  override def equals(obj: Any): Boolean = obj match {
    case c: GpuCSVScan =>
      super.equals(c) && dataSchema == c.dataSchema && options == c.options &&
      maxReaderBatchSizeRows == c.maxReaderBatchSizeRows &&
      maxReaderBatchSizeBytes == c.maxReaderBatchSizeBytes &&
        maxGpuColumnSizeBytes == c.maxGpuColumnSizeBytes
    case _ => false
  }

  override def hashCode(): Int = super.hashCode()
}

case class GpuCSVPartitionReaderFactory(
    sqlConf: SQLConf,
    broadcastedConf: Broadcast[SerializableConfiguration],
    dataSchema: StructType,
    readDataSchema: StructType,
    partitionSchema: StructType, // TODO need to filter these out, or support pulling them in.
                                 // These are values from the file name/path itself
    parsedOptions: CSVOptions,
    maxReaderBatchSizeRows: Integer,
    maxReaderBatchSizeBytes: Long,
    maxGpuColumnSizeBytes: Long,
    metrics: Map[String, GpuMetric],
    @transient params: Map[String, String]) extends ShimFilePartitionReaderFactory(params) {

  override def buildReader(partitionedFile: PartitionedFile): PartitionReader[InternalRow] = {
    throw new IllegalStateException("ROW BASED PARSING IS NOT SUPPORTED ON THE GPU...")
  }

  override def buildColumnarReader(partFile: PartitionedFile): PartitionReader[ColumnarBatch] = {
    val conf = broadcastedConf.value.value
    val reader = new PartitionReaderWithBytesRead(new CSVPartitionReader(conf, partFile, dataSchema,
      readDataSchema, parsedOptions, maxReaderBatchSizeRows, maxReaderBatchSizeBytes, metrics))
    ColumnarPartitionReaderWithPartitionValues.newReader(partFile, reader, partitionSchema,
      maxGpuColumnSizeBytes)
  }
}

abstract class CSVPartitionReaderBase[BUFF <: LineBufferer, FACT <: LineBuffererFactory[BUFF]](
    conf: Configuration,
    partFile: PartitionedFile,
    dataSchema: StructType,
    readDataSchema: StructType,
    parsedOptions: CSVOptions,
    maxRowsPerChunk: Integer,
    maxBytesPerChunk: Long,
    execMetrics: Map[String, GpuMetric],
    bufferFactory: FACT) extends
    GpuTextBasedPartitionReader[BUFF, FACT](conf, partFile,
      dataSchema, readDataSchema, parsedOptions.lineSeparatorInRead, maxRowsPerChunk,
      maxBytesPerChunk, execMetrics, bufferFactory, Charset.forName(parsedOptions.charset)) {

  /**
   * File format short name used for logging and other things to uniquely identity
   * which file format is being used.
   *
   * @return the file format short name
   */
  override def getFileFormatShortName: String = "CSV"

  /**
   * CSV supports "true" and "false" (case-insensitive) as valid boolean values.
   */
  override def castStringToBool(input: ColumnVector): ColumnVector = {
    val lowerStripped = withResource(input.strip()) {
      _.lower()
    }

    val (isTrue, isValidBool) = withResource(lowerStripped) { _ =>
      val isTrueRes = withResource(Scalar.fromString(true.toString)) {
        lowerStripped.equalTo
      }
      val isValidBoolRes = closeOnExcept(isTrueRes) { _ =>
        withResource(ColumnVector.fromStrings(true.toString, false.toString)) {
          lowerStripped.contains
        }
      }
      (isTrueRes, isValidBoolRes)
    }
    withResource(Seq(isTrue, isValidBool)) { _ =>
      withResource(Scalar.fromNull(DType.BOOL8)) {
        isValidBool.ifElse(isTrue, _)
      }
    }
  }

  override def castStringToDecimal(input: ColumnVector, dt: DecimalType): ColumnVector = {
    if (parsedOptions.locale == Locale.US) {
      withResource(Scalar.fromString(",")) { groupingSeparator =>
        withResource(Scalar.fromString("")) { emptyString =>
          withResource(input.stringReplace(groupingSeparator, emptyString)) { withoutGrouping =>
            CastStrings.toDecimal(withoutGrouping, false, dt.precision, -dt.scale)
          }
        }
      }
    } else {
      throw new IllegalStateException(
        GpuCSVScan.unsupportedLocaleForDecimalMessage(parsedOptions.locale))
    }
  }

  override def dateFormat: Option[String] = Some(GpuCsvUtils.dateFormatInRead(parsedOptions))
  override def timestampFormat: String = GpuCsvUtils.timestampFormatInRead(parsedOptions)
}


object CSVPartitionReader {
  def readToTable(
      dataBufferer: HostLineBufferer,
      cudfSchema: Schema,
      decodeTime: GpuMetric,
      csvOpts: cudf.CSVOptions.Builder,
      formatName: String,
      partFile: PartitionedFile): Table = {
    val dataSize = dataBufferer.getLength
    try {
      RmmRapidsRetryIterator.withRetryNoSplit(dataBufferer.getBufferAndRelease) { dataBuffer =>
        NvtxIdWithMetrics(NvtxRegistry.CSV_DECODE, decodeTime) {
          Table.readCSV(cudfSchema, csvOpts.build, dataBuffer, 0, dataSize)
        }
      }
    } catch {
      case e: Exception =>
        throw new IOException(s"Error when processing file [$partFile]", e)
    }
  }
}

class CSVPartitionReader(
    conf: Configuration,
    partFile: PartitionedFile,
    dataSchema: StructType,
    readDataSchema: StructType,
    parsedOptions: CSVOptions,
    maxRowsPerChunk: Integer,
    maxBytesPerChunk: Long,
    execMetrics: Map[String, GpuMetric]) extends
  CSVPartitionReaderBase[HostLineBufferer,
    LineBuffererFactory[HostLineBufferer]](conf, partFile,
    dataSchema, readDataSchema, parsedOptions, maxRowsPerChunk,
    maxBytesPerChunk, execMetrics,
    // In multiLine mode, empty lines within quoted fields are
    // legitimate data and must not be filtered out.
    if (parsedOptions.multiLine) HostLineBuffererFactory
    else FilterCsvEmptyHostLineBuffererFactory) {

  def buildCsvOptions(
      parsedOptions: CSVOptions,
      schema: StructType,
      hasHeader: Boolean): cudf.CSVOptions.Builder = {
    val builder = cudf.CSVOptions.builder()
    builder.withDelim(parsedOptions.delimiter.charAt(0))
    builder.hasHeader(hasHeader)
    // TODO parsedOptions.parseMode
    builder.withQuote(parsedOptions.quote)
    builder.withComment(parsedOptions.comment)
    builder.withNullValue(parsedOptions.nullValue)
    builder.includeColumn(schema.fields.map(_.name): _*)
    builder
  }

  /**
   * Read the host buffer to GPU table
   *
   * @param dataBufferer   buffered data to be parsed
   * @param cudfDataSchema     the cudf schema of the data
   * @param readDataSchema the Spark schema describing what will be read
   * @param isFirstChunk   if it is the first chunk
   * @return table
   */
  override def readToTable(
      dataBufferer: HostLineBufferer,
      cudfDataSchema: Schema,
      readDataSchema: StructType,
      cudfReadDataSchema: Schema,
      isFirstChunk: Boolean,
      decodeTime: GpuMetric): Table = {
    val hasHeader = isFirstChunk && parsedOptions.headerFlag
    val csvOpts = buildCsvOptions(parsedOptions, readDataSchema, hasHeader)
    CSVPartitionReader.readToTable(dataBufferer, cudfDataSchema, decodeTime, csvOpts,
      getFileFormatShortName, partFile)
  }
}
