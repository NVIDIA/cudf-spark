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

package org.apache.spark.sql.catalyst.json.rapids

import java.io.IOException
import java.nio.charset.StandardCharsets
import java.util.Locale

import scala.collection.JavaConverters._

import ai.rapids.cudf
import ai.rapids.cudf.{Schema, Table}
import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.shims.{ColumnDefaultValuesShims, ShimFilePartitionReaderFactory}
import org.apache.hadoop.conf.Configuration

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.json.{GpuJsonUtils, JSONOptions, JSONOptionsInRead}
import org.apache.spark.sql.catalyst.util.PermissiveMode
import org.apache.spark.sql.connector.read.{PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.execution.QueryExecutionException
import org.apache.spark.sql.execution.datasources.{PartitionedFile, PartitioningAwareFileIndex}
import org.apache.spark.sql.execution.datasources.v2.{FileScan, TextBasedFileScan}
import org.apache.spark.sql.execution.datasources.v2.json.JsonScan
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.GpuJsonReadCommon
import org.apache.spark.sql.rapids.execution.TrampolineUtil
import org.apache.spark.sql.rapids.shims.GpuJsonToStructsShim
import org.apache.spark.sql.types.{DataType, DateType, DecimalType, DoubleType, FloatType, StructType, TimestampType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.SerializableConfiguration

object GpuJsonScan {

  sealed trait JsonReaderType
  case object JsonScanReaderType extends JsonReaderType {
    override def toString: String = "JsonScan"
  }
  case object JsonToStructsReaderType extends JsonReaderType {
    override def toString: String = "JsonToStructs"
  }
  case object JsonFileFormatReaderType extends JsonReaderType {
    override def toString: String = "JsonFileFormat"
  }

  def tagSupport(scanMeta: ScanMeta[JsonScan]) : Unit = {
    val scan = scanMeta.wrapped
    tagSupport(
      scan.sparkSession.sessionState.conf,
      JsonScanReaderType,
      scan.dataSchema,
      scan.readDataSchema,
      scan.options.asScala.toMap,
      scanMeta)
  }

  def tagSupportOptions(op: JsonReaderType,
                        options: JSONOptionsInRead,
                        meta: RapidsMeta[_, _, _]): Unit = {
    if (options.multiLine) {
      meta.willNotWorkOnGpu(s"$op does not support multiLine")
    }

    // {"name": /* hello */ "Reynold Xin"} is not supported by CUDF
    if (options.allowComments) {
      meta.willNotWorkOnGpu(s"$op does not support allowComments")
    }

    // {name: 'Reynold Xin'} is not supported by CUDF
    if (options.allowUnquotedFieldNames) {
      meta.willNotWorkOnGpu(s"$op does not support allowUnquotedFieldNames")
    }

    // {"name": "Cazen Lee", "price": "\$10"} is not supported by CUDF
    if (options.allowBackslashEscapingAnyCharacter) {
      meta.willNotWorkOnGpu(s"$op does not support allowBackslashEscapingAnyCharacter")
    }

    // {"a":null, "b":1, "c":3.0}, Spark will drop column `a` if dropFieldIfAllNull is enabled.
    if (options.dropFieldIfAllNull) {
      meta.willNotWorkOnGpu(s"$op does not support dropFieldIfAllNull")
    }

    if (options.parseMode != PermissiveMode) {
      meta.willNotWorkOnGpu(s"$op only supports Permissive JSON parsing")
    }

    if (options.lineSeparator.getOrElse("\n") != "\n") {
      meta.willNotWorkOnGpu(op + " only supports \"\\n\" as a line separator")
    }

    options.encoding.foreach(enc =>
      if (enc != StandardCharsets.UTF_8.name() && enc != StandardCharsets.US_ASCII.name()) {
      meta.willNotWorkOnGpu(s"$op only supports UTF8 or US-ASCII encoded data")
    })
  }

  def tagSupport(conf: SQLConf,
                 op: JsonReaderType,
                 dataSchema: DataType,
                 readSchema: DataType,
                 options: Map[String, String],
                 meta: RapidsMeta[_, _, _]): Unit = {
    val parsedOptions = new JSONOptionsInRead(
      options,
      conf.sessionLocalTimeZone,
      conf.columnNameOfCorruptRecord)

    op match {
      case JsonScanReaderType | JsonFileFormatReaderType =>
        if (!meta.conf.isJsonEnabled) {
          meta.willNotWorkOnGpu("JSON input and output has been disabled. To enable set " +
              s"${RapidsConf.ENABLE_JSON} to true")
        }

        if (!meta.conf.isJsonReadEnabled) {
          meta.willNotWorkOnGpu("JSON input has been disabled. To enable set " +
              s"${RapidsConf.ENABLE_JSON_READ} to true.")
        }
      case _ => // Ignored
    }

    tagSupportOptions(op, parsedOptions, meta)
    val hasDates = TrampolineUtil.dataTypeExistsRecursively(readSchema, _.isInstanceOf[DateType])
    val hasTimestamps = TrampolineUtil.dataTypeExistsRecursively(readSchema,
      _.isInstanceOf[TimestampType])
    val hasFloats = TrampolineUtil.dataTypeExistsRecursively(readSchema,
      _.isInstanceOf[FloatType])
    val hasDoubles = TrampolineUtil.dataTypeExistsRecursively(readSchema,
      _.isInstanceOf[DoubleType])
    val hasDecimals = TrampolineUtil.dataTypeExistsRecursively(readSchema,
      _.isInstanceOf[DecimalType])

    if (hasDates) {
      GpuTextBasedDateUtils.tagCudfFormat(meta,
        GpuJsonUtils.dateFormatInRead(parsedOptions), parseString = true)

      GpuJsonToStructsShim.tagDateFormatSupportFromScan(meta,
        GpuJsonUtils.optionalDateFormatInRead(parsedOptions))

      // For date type, timezone needs to be checked also. This is because JVM timezone is used
      // to get days offset before rebasing Julian to Gregorian in Spark while not in Rapids.
      //
      // In details, for Json data format, Spark uses dateFormatter to parse string as date data
      // type which utilizes [[org.apache.spark.sql.catalyst.DateFormatter]]. For Json format, it
      // uses [[LegacyFastDateFormatter]] which is based on Apache Commons FastDateFormat. It parse
      // string into Java util.Date base on JVM default timezone. From Java util.Date, it's
      // converted into java.sql.Date type. By leveraging [[JavaDateTimeUtils]], it finally do
      // `rebaseJulianToGregorianDays` considering its offset to UTC timezone.
      if(!GpuOverrides.isUTCTimezone(parsedOptions.zoneId)){
        meta.willNotWorkOnGpu(s"Not supported timezone type ${parsedOptions.zoneId}.")
      }
    }

    if (hasDates || hasTimestamps) {
      if (!meta.conf.isJsonDateTimeReadEnabled) {
        meta.willNotWorkOnGpu("JsonScan on GPU does not support DateType or TimestampType" +
          " by default due to compatibility. " +
          "Set `spark.rapids.sql.json.read.datetime.enabled` to `true` to enable them.")
      }

      if (!GpuOverrides.isUTCTimezone(parsedOptions.zoneId)) {
        meta.willNotWorkOnGpu(s"Not supported timezone type ${parsedOptions.zoneId}.")
      }

      GpuTextBasedDateUtils.tagCudfFormat(meta,
        GpuJsonUtils.timestampFormatInRead(parsedOptions), parseString = true)
    }

    if (GpuJsonUtils.enableDateTimeParsingFallback(parsedOptions) &&
        (hasDates || hasTimestamps)) {
      meta.willNotWorkOnGpu(s"$op does not support enableDateTimeParsingFallback")
    }

    if (!meta.conf.isJsonFloatReadEnabled && hasFloats) {
      meta.willNotWorkOnGpu("JSON reading is not 100% compatible when reading floats. " +
        s"To enable it please set ${RapidsConf.ENABLE_READ_JSON_FLOATS} to true.")
    }

    if (!meta.conf.isJsonDoubleReadEnabled && hasDoubles) {
      meta.willNotWorkOnGpu("JSON reading is not 100% compatible when reading doubles. " +
        s"To enable it please set ${RapidsConf.ENABLE_READ_JSON_DOUBLES} to true.")
    }

    if (!meta.conf.isJsonDecimalReadEnabled && hasDecimals) {
      meta.willNotWorkOnGpu("JSON reading is not 100% compatible when reading decimals. " +
        s"To enable it please set ${RapidsConf.ENABLE_READ_JSON_DECIMALS} to true.")
    }

    // Technically this is a problem for dates/timestamps too, but we don't support the formats
    //  that are impacted by the locale
    if (hasDecimals && parsedOptions.locale != Locale.US) {
      meta.willNotWorkOnGpu(s"decimal parsing is only supported when the local is set " +
          s"to US, but we found ${parsedOptions.locale}")
    }

    dataSchema match {
      case st: StructType =>
        if (st.fieldNames.contains(parsedOptions.columnNameOfCorruptRecord)) {
          meta.willNotWorkOnGpu(s"$op does not support Corrupt Record")
        }
        if (ColumnDefaultValuesShims.hasExistenceDefaultValues(st)) {
          meta.willNotWorkOnGpu(s"$op does not support default values in schema")
        }
      case _ => //Ignored
    }

    readSchema match {
      case st: StructType =>
        FileFormatChecks.tag(meta, st, JsonFormatType, ReadFileOp)
      case _ =>
      //This is for JsonToStructs when parsing a ArrayType or a MapType.
      // ArrayType is not supported yet, and MapType only deals with String to String, so we
      // are good to go. In the future we might want to wrap these in a StructType so
      // we can get a full set of tests.
    }
  }
}

case class GpuJsonScan(
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

  private lazy val parsedOptions: JSONOptions = new JSONOptions(
    options.asScala.toMap,
    sparkSession.sessionState.conf.sessionLocalTimeZone,
    sparkSession.sessionState.conf.columnNameOfCorruptRecord)

  // overrides nothing in 330
  def withFilters(partitionFilters: Seq[Expression],
      dataFilters: Seq[Expression]): FileScan = {
    this.copy(partitionFilters = partitionFilters, dataFilters = dataFilters)
  }

  override def createReaderFactory(): PartitionReaderFactory = {
    val caseSensitiveMap = options.asCaseSensitiveMap.asScala.toMap
    // Hadoop Configurations are case sensitive.
    val hadoopConf = sparkSession.sessionState.newHadoopConfWithOptions(caseSensitiveMap)
    val broadcastedConf = sparkSession.sparkContext.broadcast(
      new SerializableConfiguration(hadoopConf))

    GpuJsonPartitionReaderFactory(sparkSession.sessionState.conf, broadcastedConf,
      dataSchema, readDataSchema, readPartitionSchema, parsedOptions, maxReaderBatchSizeRows,
      maxReaderBatchSizeBytes, maxGpuColumnSizeBytes, metrics, options.asScala.toMap)
  }

  override def withInputFile(): GpuScan = this
}

case class GpuJsonPartitionReaderFactory(
    sqlConf: SQLConf,
    broadcastedConf: Broadcast[SerializableConfiguration],
    dataSchema: StructType,
    readDataSchema: StructType,
    partitionSchema: StructType, // TODO need to filter these out, or support pulling them in.
                                 // These are values from the file name/path itself
    parsedOptions: JSONOptions,
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
    val reader = new PartitionReaderWithBytesRead(new JsonPartitionReader(conf, partFile,
      dataSchema, readDataSchema, parsedOptions, maxReaderBatchSizeRows, maxReaderBatchSizeBytes,
      metrics))
    ColumnarPartitionReaderWithPartitionValues.newReader(partFile, reader, partitionSchema,
      maxGpuColumnSizeBytes)
  }
}

object JsonPartitionReader {
  def readToTable(
      dataBufferer: HostLineBufferer,
      cudfSchema: Schema,
      decodeTime: GpuMetric,
      jsonOpts:  cudf.JSONOptions,
      formatName: String,
      partFile: PartitionedFile): Table = {
    val dataSize = dataBufferer.getLength
    // cuDF does not yet support reading a subset of columns so we have
    // to apply the read schema projection here
    try {
      RmmRapidsRetryIterator.withRetryNoSplit(dataBufferer.getBufferAndRelease) { dataBuffer =>
        NvtxIdWithMetrics(NvtxRegistry.JSON_DECODE_SCAN, decodeTime) {
          try {
            Table.readJSON(cudfSchema, jsonOpts, dataBuffer, 0, dataSize, dataBufferer.getNumLines):
              @scala.annotation.nowarn(
                "cat=deprecation&msg=method readJSON in class Table is deprecated"
              )
          } catch {
            case e: AssertionError if e.getMessage == "CudfColumns can't be null or empty" =>
              // this happens when every row in a JSON file is invalid (or we are
              // trying to read a non-JSON file format as JSON)
              throw new IOException(s"Error when processing file [$partFile]", e)
          }
        }
      }
    } catch {
      case e: Exception =>
        throw new IOException(s"Error when processing file [$partFile]", e)
    }
  }
}

class JsonPartitionReader(
    conf: Configuration,
    partFile: PartitionedFile,
    dataSchema: StructType,
    readDataSchema: StructType,
    parsedOptions: JSONOptions,
    maxRowsPerChunk: Integer,
    maxBytesPerChunk: Long,
    execMetrics: Map[String, GpuMetric])
  extends GpuTextBasedPartitionReader[HostLineBufferer,
    FilterEmptyHostLineBuffererFactory.type](conf,
    partFile, dataSchema, readDataSchema, parsedOptions.lineSeparatorInRead, maxRowsPerChunk,
    maxBytesPerChunk, execMetrics, FilterEmptyHostLineBuffererFactory) {

  def buildJsonOptions(parsedOptions: JSONOptions): cudf.JSONOptions =
    GpuJsonReadCommon.cudfJsonOptions(parsedOptions)

  /**
   * Read the host buffer to GPU table
   *
   * @param dataBuffer     host buffer to be read
   * @param dataSize       the size of host buffer
   * @param cudfDataSchema     the cudf schema of the data
   * @param readDataSchema the Spark schema describing what will be read
   * @param hasHeader      if it has header
   * @return table
   */
  override def readToTable(
      dataBufferer: HostLineBufferer,
      cudfDataSchema: Schema,
      readDataSchema: StructType,
      cudfReadDataSchema: Schema,
      hasHeader: Boolean,
      decodeTime: GpuMetric): Table = {
    val jsonOpts = buildJsonOptions(parsedOptions)
    val jsonTbl = JsonPartitionReader.readToTable(dataBufferer, cudfReadDataSchema, decodeTime,
      jsonOpts, getFileFormatShortName, partFile)
    withResource(jsonTbl) { tbl =>
      val cudfColumnNames = cudfReadDataSchema.getColumnNames
      val columns = readDataSchema.map { field =>
        val i = cudfColumnNames.indexOf(field.name)
        if (i == -1) {
          throw new IllegalStateException(
            s"read schema contains field named '${field.name}' that is not in the data schema")
        }
        tbl.getColumn(i)
      }
      new Table(columns: _*)
    }
  }

  override def getCudfSchema(dataSchema: StructType): Schema =
    GpuJsonReadCommon.makeSchema(dataSchema)

  override def castTableToDesiredTypes(input: Table, dataSchema: StructType): Table = {
    withResource(GpuJsonReadCommon.convertTableToDesiredType(input, dataSchema, parsedOptions)) {
      cols =>
        new Table(cols.toSeq: _*)
    }
  }

  /**
   * File format short name used for logging and other things to uniquely identity
   * which file format is being used.
   *
   * @return the file format short name
   */
  override def getFileFormatShortName: String = "JSON"

  /**
   * Handle the table decoded by GPU
   *
   * @param readDataSchema the Spark schema describing what will be read
   * @param table          the table decoded by GPU
   * @return the new optional Table
   */
  override def handleResult(readDataSchema: StructType, table: Table): Option[Table] = {
    val tableCols = table.getNumberOfColumns

    // For the GPU resource handling convention, we should close input table and return a new
    // table just like below code. But for optimization, we just return the input table.
    // withResource(table) { _
    //  val cols = (0 until  table.getNumberOfColumns).map(i => table.getColumn(i))
    //  Some(new Table(cols: _*))
    // }
    if (readDataSchema.length == tableCols) {
      return Some(table)
    }

    // JSON will read all columns in dataSchema due to https://github.com/rapidsai/cudf/issues/9990,
    // but actually only the columns in readDataSchema are needed, we need to do the "column" prune,
    // Once the FEA is shipped in CUDF, we should remove this hack.
    withResource(table) { _ =>
      val prunedCols = readDataSchema.fieldNames.map { name =>
        val optionIndex = dataSchema.getFieldIndex(name)
        if (optionIndex.isEmpty || optionIndex.get >= tableCols) {
          throw new QueryExecutionException(s"Something wrong for $name columns in readDataSchema")
        }
        optionIndex.get
      }

      val prunedColumnVectors = prunedCols.map(i => table.getColumn(i))
      Some(new Table(prunedColumnVectors: _*))
    }
  }

  // TODO need to rethink how we want to handle casting data from one type to another, but probably
  //  only after we have nested support added in.
  //  https://github.com/NVIDIA/spark-rapids/issues/10539
  override def castStringToBool(input: cudf.ColumnVector): cudf.ColumnVector =
    throw new IllegalStateException("THIS SHOULD NOT BE CALLED")

  override def dateFormat: Option[String] =
    throw new IllegalStateException("THIS SHOULD NOT BE CALLED")

  override def timestampFormat: String =
    throw new IllegalStateException("THIS SHOULD NOT BE CALLED")

}
