/*
 * Copyright (c) 2023-2026, NVIDIA CORPORATION.
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

package org.apache.spark.sql.hive.rapids

import java.nio.charset.{Charset, StandardCharsets}
import java.util.Locale

import ai.rapids.cudf.{CompressionType, CSVWriterOptions, DType, ParquetWriterOptions, QuoteStyle, Scalar, Table, TableWriter => CudfTableWriter}
import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.jni.CastStrings
import com.nvidia.spark.rapids.jni.fileio.RapidsFileIO
import com.nvidia.spark.rapids.shims.BucketingUtilsShim
import org.apache.hadoop.mapreduce.{Job, TaskAttemptContext}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.parquet.ParquetOptions
import org.apache.spark.sql.hive.rapids.shims.GpuInsertIntoHiveTableMeta
import org.apache.spark.sql.rapids.ColumnarWriteTaskStatsTracker
import org.apache.spark.sql.rapids.execution.TrampolineUtil
import org.apache.spark.sql.types.{DataType, Decimal, DecimalType, StringType, StructType}
import org.apache.spark.sql.vectorized.ColumnarBatch

object GpuHiveFileFormat extends Logging {
  private val parquetOutputFormatClass =
    "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"
  private val parquetSerdeClass =
    "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"

  def tagGpuSupport(meta: GpuInsertIntoHiveTableMeta): Option[ColumnarFileFormat] = {
    val insertCmd = meta.wrapped
    // Bucketing write
    BucketingUtilsShim.tagForHiveBucketingWrite(meta, insertCmd.table.bucketSpec,
      insertCmd.outputColumns, meta.conf.isForceHiveHashForBucketedWrite)

    // Infer the file format from the serde string, similar as what Spark does in
    // RelationConversions for Hive.
    val serde = insertCmd.table.storage.serde.getOrElse("").toLowerCase(Locale.ROOT)
    val tempFileFormat = if (serde.contains("parquet")) {
      // Parquet specific tagging
      tagGpuSupportForParquet(meta)
    } else {
      // Default to text file format
      tagGpuSupportForText(meta)
    }

    if (meta.canThisBeReplaced) {
      Some(tempFileFormat)
    } else {
      None
    }
  }

  private def tagGpuSupportForParquet(meta: GpuInsertIntoHiveTableMeta): ColumnarFileFormat = {
    val insertCmd = meta.wrapped
    val storage = insertCmd.table.storage

    if (storage.outputFormat.getOrElse("") != parquetOutputFormatClass) {
      meta.willNotWorkOnGpu(s"unsupported output format found: ${storage.outputFormat}, " +
        s"only $parquetOutputFormatClass is currently supported for Parquet")
    }
    if (storage.serde.getOrElse("") != parquetSerdeClass) {
      meta.willNotWorkOnGpu(s"unsupported serde found: ${storage.serde}, " +
        s"only $parquetSerdeClass is currently supported for Parquet")
    }

    // Decimal type check
    val hasIntOrLongBackedDec = insertCmd.query.schema.exists { field =>
      TrampolineUtil.dataTypeExistsRecursively(field.dataType, {
        case dec: DecimalType if dec.precision <= Decimal.MAX_LONG_DIGITS => true
        case _ => false
      })
    }
    if (hasIntOrLongBackedDec) {
      meta.willNotWorkOnGpu("decimals that fit in a long are not supported " +
        s"for Parquet. Hive always writes decimals as binary arrays but the GPU writes them " +
        s"as integral types")
    }

    FileFormatChecks.tag(meta, insertCmd.table.schema, ParquetFormatType, WriteFileOp)

    // Compression type
    val parquetOptions = new ParquetOptions(insertCmd.table.properties, insertCmd.conf)
    val compressionType =
      GpuParquetFileFormat.parseCompressionType(parquetOptions.compressionCodecClassName)
        .getOrElse {
          meta.willNotWorkOnGpu("compression codec " +
            s"${parquetOptions.compressionCodecClassName} is not supported for Parquet")
          CompressionType.NONE
        }
    new GpuHiveParquetFileFormat(compressionType)
  }

  private def tagGpuSupportForText(meta: GpuInsertIntoHiveTableMeta): ColumnarFileFormat = {
    import org.apache.spark.sql.hive.rapids.GpuHiveTextFileUtils._
    if (!meta.conf.isHiveDelimitedTextEnabled) {
      meta.willNotWorkOnGpu("Hive text I/O has been disabled. To enable this, " +
        s"set ${RapidsConf.ENABLE_HIVE_TEXT} to true")
    }
    if (!meta.conf.isHiveDelimitedTextWriteEnabled) {
      meta.willNotWorkOnGpu("writing Hive delimited text tables has been disabled, " +
        s"to enable this, set ${RapidsConf.ENABLE_HIVE_TEXT_WRITE} to true")
    }

    val insertCommand = meta.wrapped
    val storage  = insertCommand.table.storage
    if (storage.outputFormat.getOrElse("") != textOutputFormat) {
      meta.willNotWorkOnGpu(s"unsupported output-format found: ${storage.outputFormat}, " +
        s"only $textOutputFormat is currently supported for text")
    }
    if (storage.serde.getOrElse("") != lazySimpleSerDe) {
      meta.willNotWorkOnGpu(s"unsupported serde found: ${storage.serde}, " +
        s"only $lazySimpleSerDe is currently supported for text")
    }

    // The check for serialization key here differs slightly from the read-side check in
    // HiveProviderImpl::getExecs():
    //   1. On the read-side, we do a strict check for `serialization.format == 1`, denoting
    //      '^A'-separated text.  All other formatting is unsupported.
    //   2. On the write-side too, we support only `serialization.format == 1`.  But if
    //      `serialization.format` hasn't been set yet, it is still treated as `^A` separated.
    //
    // On the write side, there are a couple of scenarios to consider:
    //   1. If the destination table exists beforehand, `serialization.format` should have been
    //      set already, to a non-empty value.  This will look like:
    //      ```sql
    //      CREATE TABLE destination_table( col INT, ... );  --> serialization.format=1
    //      INSERT INTO TABLE destination_table SELECT * FROM ...
    //      ```
    //   2. If the destination table is being created as part of a CTAS, without an explicit
    //      format specified, then Spark leaves `serialization.format` unpopulated, until *AFTER*
    //      the write operation is completed.  Such a query might look like:
    //      ```sql
    //      CREATE TABLE destination_table AS SELECT * FROM ...
    //      --> serialization.format is absent from Storage Properties. "1" is inferred.
    //      ```
    //   3. If the destination table is being created as part of a CTAS, with a non-default
    //      text format specified explicitly, then the non-default `serialization.format` is made
    //      available as part of the destination table's storage properties.  Such a table creation
    //      might look like:
    //      ```sql
    //      CREATE TABLE destination_table
    //        ROW FORMAT DELIMITED FIELDS TERMINATED BY `,` STORED AS TEXTFILE
    //        AS SELECT * FROM ...
    //      --> serialization.format="", field.delim=",".  Unsupported case.
    //      ```
    // All these cases may be covered by explicitly checking for `serialization.format=1`.
    val serializationFormat = storage.properties.getOrElse(serializationKey, "1")
    if (serializationFormat != ctrlASeparatedFormat) {
      meta.willNotWorkOnGpu(s"unsupported serialization format found: " +
        s"$serializationFormat, " +
        s"only \'^A\' separated text output (i.e. serialization.format=1) " +
        s"is currently supported")
    }

    val lineTerminator = storage.properties.getOrElse(lineDelimiterKey, newLine)
    if (lineTerminator != newLine) {
      meta.willNotWorkOnGpu(s"unsupported line terminator found: " +
        s"$lineTerminator, " +
        s"only newline (\'\\n\') separated text output is currently supported")
    }

    if (!storage.properties.getOrElse(escapeDelimiterKey, "").equals("")) {
      meta.willNotWorkOnGpu("escapes are not currently supported")
      // "serialization.escape.crlf" matters only if escapeDelimiterKey is set
    }

    val charset = Charset.forName(
      storage.properties.getOrElse("serialization.encoding", "UTF-8"))
    if (!charset.equals(StandardCharsets.UTF_8)) {
      meta.willNotWorkOnGpu("only UTF-8 is supported as the charset")
    }

    if (insertCommand.conf.getConfString("hive.exec.compress.output", "false").toBoolean) {
      meta.willNotWorkOnGpu("compressed output is not supported, " +
        "set hive.exec.compress.output to false to enable writing Hive text via GPU")
    }

    FileFormatChecks.tag(meta, insertCommand.table.schema, HiveDelimitedTextFormatType,
      WriteFileOp)

    new GpuHiveTextFileFormat()
  }
}

class GpuHiveParquetFileFormat(compType: CompressionType) extends ColumnarFileFormat
    with Logging with Serializable {

  override def toString: String = "Parquet"

  override def prepareWrite(sparkSession: SparkSession, job: Job,
      options: Map[String, String], dataSchema: StructType): ColumnarOutputWriterFactory = {

    // Avoid referencing the outer object.
    val compressionType = compType
    GpuParquetFileFormat.parquetBlockSizeWarning(job.getConfiguration, options).foreach {
      warning => logWarning(warning)
    }
    val parquetWriterRowGroupSizeRows =
      RapidsConf.PARQUET_WRITER_ROW_GROUP_SIZE_ROWS.get(sparkSession.sessionState.conf)
    val parquetWriterRowGroupSizeBytes =
      RapidsConf.PARQUET_WRITER_ROW_GROUP_SIZE_BYTES.get(sparkSession.sessionState.conf)
    val parquetWriterMaxDictionarySize =
      RapidsConf.PARQUET_WRITER_MAX_DICTIONARY_SIZE.get(sparkSession.sessionState.conf)
    val parquetWriterDictionaryPolicy =
      RapidsConf.PARQUET_WRITER_DICTIONARY_POLICY.get(sparkSession.sessionState.conf)
        .map(ParquetWriterOptions.DictionaryPolicy.valueOf)
    new ColumnarOutputWriterFactory {
      override def getFileExtension(context: TaskAttemptContext): String =
        compressionType match {
          case CompressionType.NONE => ".parquet"
          case ct => s".${ct.name().toLowerCase(Locale.ROOT)}.parquet"
        }

      override def newInstance(path: String,
          dataSchema: StructType,
          context: TaskAttemptContext,
          statsTrackers: Seq[ColumnarWriteTaskStatsTracker],
          debugOutputPath: Option[String],
          fileIO: RapidsFileIO): ColumnarOutputWriter = {
        new GpuHiveParquetWriter(path, dataSchema, context, compressionType, statsTrackers,
          debugOutputPath, parquetWriterRowGroupSizeRows, parquetWriterRowGroupSizeBytes,
          parquetWriterMaxDictionarySize, parquetWriterDictionaryPolicy, fileIO)
      }

      override def partitionFlushSize(context: TaskAttemptContext): Long =
        parquetWriterRowGroupSizeBytes.getOrElse(super.partitionFlushSize(context))
    }
  }
}

class GpuHiveParquetWriter(override val path: String, dataSchema: StructType,
    context: TaskAttemptContext, compType: CompressionType,
    statsTrackers: Seq[ColumnarWriteTaskStatsTracker],
    debugOutputPath: Option[String],
    parquetWriterRowGroupSizeRows: Option[Integer],
    parquetWriterRowGroupSizeBytes: Option[Long],
    parquetWriterMaxDictionarySize: Option[Long],
    parquetWriterDictionaryPolicy: Option[ParquetWriterOptions.DictionaryPolicy],
    fileIO: RapidsFileIO)
  extends ColumnarOutputWriter(context, dataSchema, NvtxRegistry.FILE_FORMAT_WRITE, true,
    statsTrackers, debugOutputPath, false, false, fileIO) {

  override protected val tableWriter: CudfTableWriter = {
    val optionsBuilder = SchemaUtils
      .writerOptionsFromSchema(ParquetWriterOptions.builder(), dataSchema,
        nullable = false,
        writeInt96 = true,      // Hive 1.2 write timestamp as INT96
        parquetFieldIdEnabled = false)
      .withCompressionType(compType)
    parquetWriterRowGroupSizeRows.foreach { rowGroupSizeRows =>
      optionsBuilder.withRowGroupSizeRows(rowGroupSizeRows)
    }
    parquetWriterRowGroupSizeBytes.foreach { rowGroupSizeBytes =>
      optionsBuilder.withRowGroupSizeBytes(rowGroupSizeBytes)
    }
    parquetWriterMaxDictionarySize.foreach { maxDictionarySize =>
      optionsBuilder.withMaxDictionarySize(maxDictionarySize)
    }
    parquetWriterDictionaryPolicy.foreach { dictionaryPolicy =>
      optionsBuilder.withDictionaryPolicy(dictionaryPolicy)
    }
    Table.writeParquetChunked(optionsBuilder.build(), this)
  }

}

class GpuHiveTextFileFormat extends ColumnarFileFormat with Logging with Serializable {

  override def toString: String = "HiveText"

  override def supportDataType(dataType: DataType): Boolean =
    GpuHiveTextFileUtils.isSupportedType(dataType)

  override def prepareWrite(sparkSession: SparkSession,
                            job: Job,
                            options: Map[String, String],
                            dataSchema: StructType): ColumnarOutputWriterFactory = {
    new ColumnarOutputWriterFactory {
      override def getFileExtension(context: TaskAttemptContext): String = ".txt"

      override def newInstance(path: String,
                               dataSchema: StructType,
                               context: TaskAttemptContext,
          statsTrackers: Seq[ColumnarWriteTaskStatsTracker],
                               debugOutputPath: Option[String],
        fileIO: RapidsFileIO): ColumnarOutputWriter = {
        new GpuHiveTextWriter(path, dataSchema, context, statsTrackers, debugOutputPath, fileIO)
      }
    }
  }
}

class GpuHiveTextWriter(override val path: String,
                        dataSchema: StructType,
                        context: TaskAttemptContext,
    statsTrackers: Seq[ColumnarWriteTaskStatsTracker],
                        debugOutputPath: Option[String],
    fileIO: RapidsFileIO)
  extends ColumnarOutputWriter(context, dataSchema, NvtxRegistry.FILE_FORMAT_WRITE, false,
    statsTrackers, debugOutputPath, false, false, fileIO) {

  /**
   * This reformats columns, to iron out inconsistencies between
   * CUDF serialization results and the values expected by Apache Spark
   * (and Apache Hive's) `LazySimpleSerDe`.
   *
   * This writer currently reformats timestamp and floating point
   * columns.
   */
  override def transformAndClose(cb: ColumnarBatch): ColumnarBatch = {
    withResource(cb) { _ =>
      withResource(GpuColumnVector.from(cb)) { table =>
        val columns = for (i <- 0 until table.getNumberOfColumns) yield {
          table.getColumn(i) match {
            case c if c.getType.hasTimeResolution =>
              // By default, the CUDF CSV writer writes timestamps in the following format:
              //   "2020-09-16T22:32:01.123456Z"
              // Hive's LazySimpleSerDe format expects timestamps to be formatted thus:
              //   "uuuu-MM-dd HH:mm:ss[.SSS...]"
              // (Specifically, no `T` between `dd` and `HH`, and no `Z` at the end.)
              val col = withResource(c.asStrings("%Y-%m-%d %H:%M:%S.%f")) { asStrings =>
                withResource(Scalar.fromString("\\N")) { nullString =>
                  asStrings.replaceNulls(nullString)
                }
              }
              GpuColumnVector.from(col, StringType)
            case c if c.getType == DType.FLOAT32 || c.getType == DType.FLOAT64 =>
              val col = CastStrings.fromFloat(c)
              GpuColumnVector.from(col, StringType)
            case c =>
              GpuColumnVector.from(c.incRefCount(), cb.column(i).dataType())
          }
        }
        new ColumnarBatch(columns.toArray, cb.numRows())
      }
    }
  }

  override val tableWriter: CudfTableWriter = {
    val writeOptions = CSVWriterOptions.builder()
      .withFieldDelimiter('\u0001')
      .withRowDelimiter("\n")
      .withIncludeHeader(false)
      .withTrueValue("true")
      .withFalseValue("false")
      .withNullValue("\\N")
      .withQuoteStyle(QuoteStyle.NONE)
      .build

    Table.getCSVBufferWriter(writeOptions, this)
  }
}

