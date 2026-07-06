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

import scala.util.Try

import ai.rapids.cudf._
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.RapidsPluginImplicits.AutoCloseableProducingArray
import com.nvidia.spark.rapids.jni.DateTimeRebase
import com.nvidia.spark.rapids.jni.fileio.RapidsFileIO
import com.nvidia.spark.rapids.shims._
import com.nvidia.spark.rapids.shims.parquet._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.{Job, OutputCommitter, TaskAttemptContext}
import org.apache.parquet.hadoop.{ParquetOutputCommitter, ParquetOutputFormat, ParquetWriter}
import org.apache.parquet.hadoop.ParquetOutputFormat.JobSummaryLevel
import org.apache.parquet.hadoop.codec.CodecConfig
import org.apache.parquet.hadoop.util.ContextUtil

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.execution.datasources.DataSourceUtils
import org.apache.spark.sql.execution.datasources.parquet.{ParquetOptions, ParquetWriteSupport}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.ParquetOutputTimestampType
import org.apache.spark.sql.rapids.ColumnarWriteTaskStatsTracker
import org.apache.spark.sql.rapids.execution.TrampolineUtil
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch

object GpuParquetFileFormat {
  private val DefaultParquetBlockSize = ParquetWriter.DEFAULT_BLOCK_SIZE.toLong

  private def isDefaultParquetBlockSize(value: String): Boolean =
    Try(value.trim.toLong).toOption.contains(DefaultParquetBlockSize)

  def parquetBlockSizeWarning(
      conf: Configuration,
      options: Map[String, String]): Option[String] = {
    options.get(ParquetOutputFormat.BLOCK_SIZE)
      .orElse(Option(conf.get(ParquetOutputFormat.BLOCK_SIZE)))
      .filterNot(isDefaultParquetBlockSize)
      .map { value =>
        s"${ParquetOutputFormat.BLOCK_SIZE} is set to $value, but the RAPIDS GPU Parquet " +
          "writer does not apply Spark CPU writer row group sizing semantics for this setting. " +
          s"Set ${RapidsConf.ENABLE_PARQUET_WRITE.key}=false to use Spark CPU Parquet " +
          "writer behavior. To tune RAPIDS GPU writer row group sizing, use internal " +
          s"configs ${RapidsConf.PARQUET_WRITER_ROW_GROUP_SIZE_ROWS.key} and " +
          s"${RapidsConf.PARQUET_WRITER_ROW_GROUP_SIZE_BYTES.key}; these are cuDF-specific " +
          s"and are not equivalent to ${ParquetOutputFormat.BLOCK_SIZE}."
      }
  }

  def tagGpuSupport(
      meta: RapidsMeta[_, _, _],
      spark: SparkSession,
      options: Map[String, String],
      schema: StructType): Option[GpuParquetFileFormat] = {

    val sqlConf = spark.sessionState.conf

    val parquetOptions = new ParquetOptions(options, sqlConf)

    // lookup encryption keys in the options, then Hadoop conf, then Spark runtime conf
    def lookupEncryptionConfig(key: String): String = {
      options.getOrElse(key, {
        val hadoopConf = spark.sparkContext.hadoopConfiguration.get(key, "")
        if (hadoopConf.nonEmpty) {
          hadoopConf
        } else {
          spark.conf.get(key, "")
        }
      })
    }

    val columnEncryption = lookupEncryptionConfig("parquet.encryption.column.keys")
    val footerEncryption = lookupEncryptionConfig("parquet.encryption.footer.key")

    if (columnEncryption.nonEmpty || footerEncryption.nonEmpty) {
      meta.willNotWorkOnGpu("Encryption is not yet supported on GPU. If encrypted Parquet " +
          "writes are not required unset the \"parquet.encryption.column.keys\" and " +
          "\"parquet.encryption.footer.key\" in Parquet options")
    }

    if (!meta.conf.isParquetEnabled) {
      meta.willNotWorkOnGpu("Parquet input and output has been disabled. To enable set" +
        s"${RapidsConf.ENABLE_PARQUET} to true")
    }

    if (!meta.conf.isParquetWriteEnabled) {
      meta.willNotWorkOnGpu("Parquet output has been disabled. To enable set" +
        s"${RapidsConf.ENABLE_PARQUET_WRITE} to true")
    }

    // Check if bloom filter is enabled for any columns. If yes, then disable GPU write.
    // For Parquet tables, bloom filters are enabled for column `col` by setting
    // `parquet.bloom.filter.enabled#col` to `true` in `options` or table properties.
    // Refer to https://spark.apache.org/docs/3.2.0/sql-data-sources-load-save-functions.html
    // for further details.
    options.foreach {
      case (key, _) if key.startsWith("parquet.bloom.filter.enabled#") =>
        meta.willNotWorkOnGpu(s"Bloom filter write for Parquet is not yet supported on GPU. " +
          s"If bloom filter is not required, unset $key")
      case _ =>
    }

    FileFormatChecks.tag(meta, schema, ParquetFormatType, WriteFileOp)

    parseCompressionType(parquetOptions.compressionCodecClassName)
      .getOrElse(meta.willNotWorkOnGpu(
        s"compression codec ${parquetOptions.compressionCodecClassName} is not supported"))

    if (sqlConf.writeLegacyParquetFormat) {
      meta.willNotWorkOnGpu("Spark legacy format is not supported")
    }

    if (!meta.conf.isParquetInt96WriteEnabled && sqlConf.parquetOutputTimestampType ==
      ParquetOutputTimestampType.INT96) {
      meta.willNotWorkOnGpu(s"Writing INT96 is disabled, if you want to enable it turn it on by " +
        s"setting the ${RapidsConf.ENABLE_PARQUET_INT96_WRITE} to true. NOTE: check " +
        "out the compatibility.md to know about the limitations associated with INT96 writer")
    }

    val schemaHasTimestamps = schema.exists { field =>
      TrampolineUtil.dataTypeExistsRecursively(field.dataType, _.isInstanceOf[TimestampType])
    }
    if (schemaHasTimestamps &&
      !isOutputTimestampTypeSupported(sqlConf.parquetOutputTimestampType)) {
      meta.willNotWorkOnGpu(s"Output timestamp type " +
        s"${sqlConf.parquetOutputTimestampType} is not supported")
    }

    val schemaHasDates = schema.exists { field =>
      TrampolineUtil.dataTypeExistsRecursively(field.dataType, _.isInstanceOf[DateType])
    }
    if (schemaHasDates || schemaHasTimestamps) {
      val int96RebaseMode = DateTimeRebaseMode.fromName(
        SparkShimImpl.int96ParquetRebaseWrite(sqlConf))
      val dateTimeRebaseMode = DateTimeRebaseMode.fromName(
        SparkShimImpl.parquetRebaseWrite(sqlConf))

      if ((int96RebaseMode == DateTimeRebaseLegacy || dateTimeRebaseMode == DateTimeRebaseLegacy)
        && !GpuOverrides.isUTCTimezone()) {
        meta.willNotWorkOnGpu("Only UTC timezone is supported in LEGACY rebase mode. " +
          s"Current timezone settings: (JVM : ${ZoneId.systemDefault()}, " +
          s"session: ${SQLConf.get.sessionLocalTimeZone}). " +
          " Set both of the timezones to UTC to enable LEGACY rebase support.")
      }
    }

    if (meta.canThisBeReplaced) {
      Some(new GpuParquetFileFormat)
    } else {
      None
    }
  }

  def parseCompressionType(compressionType: String): Option[CompressionType] = {
    compressionType match {
      case "NONE" | "UNCOMPRESSED" => Some(CompressionType.NONE)
      case "SNAPPY" => Some(CompressionType.SNAPPY)
      case "ZSTD" => Some(CompressionType.ZSTD)
      case _ => None
    }
  }

  def isOutputTimestampTypeSupported(
     outputTimestampType: ParquetOutputTimestampType.Value): Boolean = {
    outputTimestampType match {
      case ParquetOutputTimestampType.TIMESTAMP_MICROS |
           ParquetOutputTimestampType.TIMESTAMP_MILLIS |
           ParquetOutputTimestampType.INT96 => true
      case _ => false
    }
  }
}

class GpuParquetFileFormat extends ColumnarFileFormat with Logging {
  /**
   * Prepares a write job and returns an [[ColumnarOutputWriterFactory]].  Client side job
   * preparation can be put here.  For example, user defined output committer can be configured
   * here by setting the output committer class in the conf of
   * spark.sql.sources.outputCommitterClass.
   */
  @scala.annotation.nowarn(
    "msg=value ENABLE_JOB_SUMMARY in class ParquetOutputFormat is deprecated"
  )
  override def prepareWrite(
      sparkSession: SparkSession,
      job: Job,
      options: Map[String, String],
      dataSchema: StructType): ColumnarOutputWriterFactory = {
    val sqlConf = sparkSession.sessionState.conf
    val parquetOptions = new ParquetOptions(options, sqlConf)

    val conf = ContextUtil.getConfiguration(job)
    GpuParquetFileFormat.parquetBlockSizeWarning(conf, options).foreach { warning =>
      logWarning(warning)
    }

    val outputTimestampType = sqlConf.parquetOutputTimestampType
    val dateTimeRebaseMode = DateTimeRebaseMode.fromName(
      sparkSession.sqlContext.getConf(SparkShimImpl.parquetRebaseWriteKey))
    val timestampRebaseMode = if (outputTimestampType.equals(ParquetOutputTimestampType.INT96)) {
      DateTimeRebaseMode.fromName(
        sparkSession.sqlContext.getConf(SparkShimImpl.int96ParquetRebaseWriteKey))
    } else {
      dateTimeRebaseMode
    }

    val committerClass =
      conf.getClass(
        SQLConf.PARQUET_OUTPUT_COMMITTER_CLASS.key,
        classOf[ParquetOutputCommitter],
        classOf[OutputCommitter])

    if (conf.get(SQLConf.PARQUET_OUTPUT_COMMITTER_CLASS.key) == null) {
      logInfo("Using default output committer for Parquet: " +
          classOf[ParquetOutputCommitter].getCanonicalName)
    } else {
      logInfo("Using user defined output committer for Parquet: " +
        committerClass.getCanonicalName)
    }

    conf.setClass(
      SQLConf.OUTPUT_COMMITTER_CLASS.key,
      committerClass,
      classOf[OutputCommitter])

    // We're not really using `ParquetOutputFormat[Row]` for writing data here.  The reason why
    // we set it here is to setup the output committer class to `ParquetOutputCommitter`, which is
    // bundled with `ParquetOutputFormat[Row]`.
    job.setOutputFormatClass(classOf[ParquetOutputFormat[Row]])

    ParquetOutputFormat.setWriteSupportClass(job, classOf[ParquetWriteSupport])

    // This metadata is useful for keeping UDTs like Vector/Matrix.
    ParquetWriteSupport.setSchema(dataSchema, conf)

    if (sqlConf.writeLegacyParquetFormat) {
      throw new UnsupportedOperationException("Spark legacy output format not supported")
    }
    // Sets flags for `ParquetWriteSupport`, which converts Catalyst schema to Parquet
    // schema and writes actual rows to Parquet files.
    conf.set(
      SQLConf.PARQUET_WRITE_LEGACY_FORMAT.key,
      sqlConf.writeLegacyParquetFormat.toString)

    if(!GpuParquetFileFormat.isOutputTimestampTypeSupported(outputTimestampType)) {
      val hasTimestamps = dataSchema.exists { field =>
        TrampolineUtil.dataTypeExistsRecursively(field.dataType, _.isInstanceOf[TimestampType])
      }
      if (hasTimestamps) {
        throw new UnsupportedOperationException(
          s"Unsupported output timestamp type: $outputTimestampType")
      }
    }
    conf.set(SQLConf.PARQUET_OUTPUT_TIMESTAMP_TYPE.key, outputTimestampType.toString)

    ParquetFieldIdShims.setupParquetFieldIdWriteConfig(conf, sqlConf)
    val parquetFieldIdWriteEnabled = ParquetFieldIdShims.getParquetIdWriteEnabled(conf, sqlConf)

    ParquetTimestampNTZShims.setupTimestampNTZConfig(conf, sqlConf)

    ParquetVariantShims.setupParquetVariantConfig(conf, sqlConf)

    // Sets compression scheme
    conf.set(ParquetOutputFormat.COMPRESSION, parquetOptions.compressionCodecClassName)

    val compressionType =
      GpuParquetFileFormat.parseCompressionType(parquetOptions.compressionCodecClassName)
        .getOrElse(
          throw new UnsupportedOperationException(
            s"compression codec ${parquetOptions.compressionCodecClassName} is not supported"))

    // SPARK-15719: Disables writing Parquet summary files by default.
    if (conf.get(ParquetOutputFormat.JOB_SUMMARY_LEVEL) == null
        && conf.get(ParquetOutputFormat.ENABLE_JOB_SUMMARY) == null) {
      conf.setEnum(ParquetOutputFormat.JOB_SUMMARY_LEVEL, JobSummaryLevel.NONE)
    }

    if (ParquetOutputFormat.getJobSummaryLevel(conf) != JobSummaryLevel.NONE
        && !classOf[ParquetOutputCommitter].isAssignableFrom(committerClass)) {
      // output summary is requested, but the class is not a Parquet Committer
      logWarning(s"Committer $committerClass is not a ParquetOutputCommitter and cannot" +
          s" create job summaries. " +
          s"Set Parquet option ${ParquetOutputFormat.JOB_SUMMARY_LEVEL} to NONE.")
    }

    val asyncOutputWriteEnabled = RapidsConf.ENABLE_ASYNC_OUTPUT_WRITE.get(sqlConf)
    // holdGpuBetweenBatches is on by default if asyncOutputWriteEnabled is on
    val holdGpuBetweenBatches = RapidsConf.ASYNC_QUERY_OUTPUT_WRITE_HOLD_GPU_IN_TASK.get(sqlConf)
      .getOrElse(asyncOutputWriteEnabled)
    val parquetWriterRowGroupSizeRows =
      RapidsConf.PARQUET_WRITER_ROW_GROUP_SIZE_ROWS.get(sqlConf)
    val parquetWriterRowGroupSizeBytes =
      RapidsConf.PARQUET_WRITER_ROW_GROUP_SIZE_BYTES.get(sqlConf)
    val parquetWriterMaxDictionarySize =
      RapidsConf.PARQUET_WRITER_MAX_DICTIONARY_SIZE.get(sqlConf)
    val parquetWriterDictionaryPolicy =
      RapidsConf.PARQUET_WRITER_DICTIONARY_POLICY.get(sqlConf)
        .map(ParquetWriterOptions.DictionaryPolicy.valueOf)

    new ColumnarOutputWriterFactory {
        override def newInstance(
          path: String,
          dataSchema: StructType,
          context: TaskAttemptContext,
          statsTrackers: Seq[ColumnarWriteTaskStatsTracker],
          debugOutputPath: Option[String],
          fileIO: RapidsFileIO): ColumnarOutputWriter = {
        new GpuParquetWriter(path, dataSchema, compressionType, outputTimestampType.toString,
          dateTimeRebaseMode, timestampRebaseMode, context, parquetFieldIdWriteEnabled,
          parquetWriterRowGroupSizeRows, parquetWriterRowGroupSizeBytes,
          parquetWriterMaxDictionarySize, parquetWriterDictionaryPolicy, statsTrackers,
          debugOutputPath, holdGpuBetweenBatches, asyncOutputWriteEnabled, fileIO)
      }

      override def getFileExtension(context: TaskAttemptContext): String = {
        CodecConfig.from(context).getCodec.getExtension + ".parquet"
      }

      override def partitionFlushSize(context: TaskAttemptContext): Long =
        parquetWriterRowGroupSizeBytes.getOrElse(
          context.getConfiguration.getLong("write.parquet.row-group-size-bytes",
            128L * 1024L * 1024L)) // 128M
    }
  }
}

class GpuParquetWriter(
    override val path: String,
    dataSchema: StructType,
    compressionType: CompressionType,
    outputTimestampType: String,
    dateRebaseMode: DateTimeRebaseMode,
    timestampRebaseMode: DateTimeRebaseMode,
    context: TaskAttemptContext,
    parquetFieldIdEnabled: Boolean,
    parquetWriterRowGroupSizeRows: Option[Integer],
    parquetWriterRowGroupSizeBytes: Option[Long],
    parquetWriterMaxDictionarySize: Option[Long],
    parquetWriterDictionaryPolicy: Option[ParquetWriterOptions.DictionaryPolicy],
    statsTrackers: Seq[ColumnarWriteTaskStatsTracker],
    debugDumpPath: Option[String],
    holdGpuBetweenBatches: Boolean,
    useAsyncWrite: Boolean,
    fileIO: RapidsFileIO)
  extends ColumnarOutputWriter(context, dataSchema, NvtxRegistry.FILE_FORMAT_WRITE, true,
    statsTrackers, debugDumpPath, holdGpuBetweenBatches, useAsyncWrite, fileIO) {
  override def throwIfRebaseNeededInExceptionMode(batch: ColumnarBatch): Unit = {
    val cols = GpuColumnVector.extractBases(batch)
    cols.foreach { col =>
      if (dateRebaseMode == DateTimeRebaseException &&
        DateTimeRebaseUtils.isDateRebaseNeededInWrite(col)) {
        throw DataSourceUtils.newRebaseExceptionInWrite("Parquet")
      }
      else if (timestampRebaseMode == DateTimeRebaseException &&
               DateTimeRebaseUtils.isTimeRebaseNeededInWrite(col)) {
        throw DataSourceUtils.newRebaseExceptionInWrite("Parquet")
      }
    }
  }

  override def transformAndClose(batch: ColumnarBatch): ColumnarBatch = {
    withResource(batch) { _ =>
      val transformedCols = GpuColumnVector.extractColumns(batch).safeMap { cv =>
        new GpuColumnVector(cv.dataType, deepTransformColumn(cv.getBase, cv.dataType))
            .asInstanceOf[org.apache.spark.sql.vectorized.ColumnVector]
      }
      new ColumnarBatch(transformedCols, batch.numRows())
    }
  }

  private def deepTransformColumn(cv: ColumnVector, dt: DataType): ColumnVector = {
    ColumnCastUtil.deepTransform(cv, Some(dt)) {
      case (cv, _) if cv.getType.isTimestampType =>
        if(cv.getType == DType.TIMESTAMP_DAYS) {
          if (dateRebaseMode == DateTimeRebaseLegacy) {
            DateTimeRebase.rebaseGregorianToJulian(cv)
          } else {
            cv.copyToColumnVector()
          }
        } else { /* timestamp */
          val typeMillis = ParquetOutputTimestampType.TIMESTAMP_MILLIS.toString
          if (timestampRebaseMode == DateTimeRebaseLegacy) {
            val rebasedTimestampAsMicros = if(cv.getType == DType.TIMESTAMP_MICROSECONDS) {
              DateTimeRebase.rebaseGregorianToJulian(cv)
            } else {
              withResource(cv.castTo(DType.TIMESTAMP_MICROSECONDS)) { cvAsMicros =>
                DateTimeRebase.rebaseGregorianToJulian(cvAsMicros)
              }
            }
            if(outputTimestampType.equals(typeMillis)) {
              withResource(rebasedTimestampAsMicros) { rebasedTs =>
                rebasedTs.castTo(DType.TIMESTAMP_MILLISECONDS) }
            } else { /* outputTimestampType is either micros, or int96 */
              rebasedTimestampAsMicros
            }
          } else {  /* timestampRebaseMode is not LEGACY */
            outputTimestampType match {
              case `typeMillis` if cv.getType != DType.TIMESTAMP_MILLISECONDS =>
                cv.castTo(DType.TIMESTAMP_MILLISECONDS)

              // Here outputTimestampType is either micros, or int96.
              case _ => cv.copyToColumnVector() /* the input is unchanged */
            }
          }
        }

      // Decimal types are checked and transformed only for the top level column because we don't
      // have access to Spark's data type of the nested column.
      case (cv, Some(d: DecimalType)) =>
        // There is a bug in Spark that causes a problem if we write Decimals with
        // precision < 10 as Decimal64.
        // https://issues.apache.org/jira/browse/SPARK-34167
        if (d.precision <= Decimal.MAX_INT_DIGITS) {
          cv.castTo(DType.create(DType.DTypeEnum.DECIMAL32, -d.scale))
        } else if (d.precision <= Decimal.MAX_LONG_DIGITS) {
          cv.castTo(DType.create(DType.DTypeEnum.DECIMAL64, -d.scale))
        } else {
          // Here, decimal should be in DECIMAL128 so the input will be unchanged.
          cv.copyToColumnVector()
        }
    }
  }

  override val tableWriter: TableWriter = {
    val writeContext = new ParquetWriteSupport().init(conf)
    val builder = SchemaUtils
      .writerOptionsFromSchema(ParquetWriterOptions.builder(), dataSchema,
        nullable = false,
        ParquetOutputTimestampType.INT96 == SQLConf.get.parquetOutputTimestampType,
        parquetFieldIdEnabled)
      .withMetadata(writeContext.getExtraMetaData)
      .withCompressionType(compressionType)
    parquetWriterRowGroupSizeRows.foreach { rowGroupSizeRows =>
      builder.withRowGroupSizeRows(rowGroupSizeRows)
    }
    parquetWriterRowGroupSizeBytes.foreach { rowGroupSizeBytes =>
      builder.withRowGroupSizeBytes(rowGroupSizeBytes)
    }
    parquetWriterMaxDictionarySize.foreach { maxDictionarySize =>
      builder.withMaxDictionarySize(maxDictionarySize)
    }
    parquetWriterDictionaryPolicy.foreach { dictionaryPolicy =>
      builder.withDictionaryPolicy(dictionaryPolicy)
    }
    Table.writeParquetChunked(builder.build(), this)
  }
}
