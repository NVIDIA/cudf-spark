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

package com.nvidia.spark.rapids.parquet

import java.io.{Closeable, EOFException, FileNotFoundException, InputStream, IOException, OutputStream}
import java.net.URI
import java.nio.{ByteBuffer, ByteOrder}
import java.nio.channels.SeekableByteChannel
import java.nio.charset.StandardCharsets
import java.util.{Collections, Locale}

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.language.implicitConversions

import ai.rapids.cudf._
import ai.rapids.cudf.{ParquetChunkedReader => JniParquetChunkedReader}
import com.github.luben.zstd.ZstdDecompressCtx
import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.GpuMetric._
import com.nvidia.spark.rapids.RapidsConf.ParquetFooterReaderType
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.RmmRapidsRetryIterator.withRetryNoSplit
import com.nvidia.spark.rapids.filecache.FileCache
import com.nvidia.spark.rapids.fileio.hadoop.HadoopFileIO
import com.nvidia.spark.rapids.io.async._
import com.nvidia.spark.rapids.jni.{DateTimeRebase, ParquetFooter, RmmSpark}
import com.nvidia.spark.rapids.jni.fileio.{RapidsFileIO, RapidsInputFile}
import com.nvidia.spark.rapids.jni.fileio.RapidsInputFile.CopyRange
import com.nvidia.spark.rapids.parquet.ParquetPartitionReader.{LocalCopy, PARQUET_MAGIC}
import com.nvidia.spark.rapids.shims.{ColumnDefaultValuesShims, GpuParquetCrypto, GpuTypeShims, ShimFilePartitionReaderFactory, SparkShimImpl}
import com.nvidia.spark.rapids.shims.parquet.{GpuParquetUtilsShims, ParquetLegacyNanoAsLongShims, ParquetSchemaClipShims, ParquetStringPredShims}
import org.apache.commons.io.output.{CountingOutputStream, NullOutputStream}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.IOUtils
import org.apache.parquet.bytes.BytesUtils
import org.apache.parquet.column.ColumnDescriptor
import org.apache.parquet.filter2.predicate.FilterApi
import org.apache.parquet.format.Util
import org.apache.parquet.format.converter.ParquetMetadataConverter
import org.apache.parquet.hadoop.{ParquetFileReader, ParquetInputFormat}
import org.apache.parquet.hadoop.ParquetFileWriter.MAGIC
import org.apache.parquet.hadoop.metadata._
import org.apache.parquet.io.{InputFile, SeekableInputStream => ParquetSeekableInputStream}
import org.apache.parquet.schema.{DecimalMetadata, GroupType, LogicalTypeAnnotation, MessageType, OriginalType, PrimitiveType, Type}
import org.apache.parquet.schema.LogicalTypeAnnotation.{DateLogicalTypeAnnotation, IntLogicalTypeAnnotation, TimestampLogicalTypeAnnotation}
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.xerial.snappy.Snappy

import org.apache.spark.TaskContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.execution.QueryExecutionException
import org.apache.spark.sql.execution.datasources.{DataSourceUtils, PartitionedFile, PartitioningAwareFileIndex, SchemaColumnConvertNotSupportedException}
import org.apache.spark.sql.execution.datasources.v2.FileScan
import org.apache.spark.sql.execution.datasources.v2.parquet.ParquetScan
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.{isTimestampNTZ, GpuTaskMetrics}
import org.apache.spark.sql.rapids.execution.TrampolineUtil
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector => SparkVector}
import org.apache.spark.util.SerializableConfiguration

/**
 * Base GpuParquetScan used for common code across Spark versions. Gpu version of
 * Spark's 'ParquetScan'.
 *
 * @param sparkSession SparkSession.
 * @param hadoopConf Hadoop configuration.
 * @param fileIndex File index of the relation.
 * @param dataSchema Schema of the data.
 * @param readDataSchema Schema to read.
 * @param readPartitionSchema Partition schema.
 * @param pushedFilters Filters on non-partition columns.
 * @param options Parquet option settings.
 * @param partitionFilters Filters on partition columns.
 * @param dataFilters File source metadata filters.
 * @param rapidsConf Rapids configuration.
 * @param queryUsesInputFile This is a parameter to easily allow turning it
 *                               off in GpuTransitionOverrides if InputFileName,
 *                               InputFileBlockStart, or InputFileBlockLength are used
 */
case class GpuParquetScan(
    sparkSession: SparkSession,
    hadoopConf: Configuration,
    fileIndex: PartitioningAwareFileIndex,
    dataSchema: StructType,
    readDataSchema: StructType,
    readPartitionSchema: StructType,
    pushedFilters: Array[Filter],
    options: CaseInsensitiveStringMap,
    partitionFilters: Seq[Expression],
    dataFilters: Seq[Expression],
    rapidsConf: RapidsConf,
    queryUsesInputFile: Boolean = false)
  extends FileScan with GpuScan with Logging {

  override def isSplitable(path: Path): Boolean = true

  override def createReaderFactory(): PartitionReaderFactory = {
    val broadcastedConf = sparkSession.sparkContext.broadcast(
      new SerializableConfiguration(hadoopConf))

    if (rapidsConf.isParquetPerFileReadEnabled) {
      logInfo("Using the original per file parquet reader")
      GpuParquetPartitionReaderFactory(sparkSession.sessionState.conf, broadcastedConf,
        dataSchema, readDataSchema, readPartitionSchema, pushedFilters, rapidsConf, metrics,
        options.asScala.toMap)
    } else {
      val poolConfBuilder = ThreadPoolConfBuilder(rapidsConf)
      GpuParquetMultiFilePartitionReaderFactory(sparkSession.sessionState.conf, broadcastedConf,
        dataSchema, readDataSchema, readPartitionSchema, pushedFilters, rapidsConf,
        poolConfBuilder,
        metrics, queryUsesInputFile)
    }
  }

  override def equals(obj: Any): Boolean = obj match {
    case p: GpuParquetScan =>
      super.equals(p) && dataSchema == p.dataSchema && options == p.options &&
          equivalentFilters(pushedFilters, p.pushedFilters) &&
          queryUsesInputFile == p.queryUsesInputFile
    case _ => false
  }

  override def hashCode(): Int = getClass.hashCode()

  override def description(): String = {
    super.description() + ", PushedFilters: " + seqToString(pushedFilters)
  }

  // overrides nothing in 330
  def withFilters(
      partitionFilters: Seq[Expression], dataFilters: Seq[Expression]): FileScan =
    this.copy(partitionFilters = partitionFilters, dataFilters = dataFilters)

  override def withInputFile(): GpuScan = copy(queryUsesInputFile = true)
}

object GpuParquetScan {
  def tagSupport(scanMeta: ScanMeta[ParquetScan]): Unit = {
    val scan = scanMeta.wrapped
    val schema = StructType(scan.readDataSchema ++ scan.readPartitionSchema)
    tagSupport(scan.sparkSession, schema, scanMeta)
  }

  def tagSupport(sparkSession: SparkSession, readSchema: StructType,
      meta: RapidsMeta[_, _, _]): Unit = {
    if (ParquetLegacyNanoAsLongShims.legacyParquetNanosAsLong) {
      meta.willNotWorkOnGpu("GPU does not support spark.sql.legacy.parquet.nanosAsLong")
    }

    if (!meta.conf.isParquetEnabled) {
      meta.willNotWorkOnGpu("Parquet input and output has been disabled. To enable set" +
        s"${RapidsConf.ENABLE_PARQUET} to true")
    }

    if (!meta.conf.isParquetReadEnabled) {
      meta.willNotWorkOnGpu("Parquet input has been disabled. To enable set" +
        s"${RapidsConf.ENABLE_PARQUET_READ} to true")
    }

    FileFormatChecks.tag(meta, readSchema, ParquetFormatType, ReadFileOp)

    // Currently timestamp conversion is not supported.
    // If support needs to be added then we need to follow the logic in Spark's
    // ParquetPartitionReaderFactory and VectorizedColumnReader which essentially
    // does the following:
    //   - check if Parquet file was created by "parquet-mr"
    //   - if not then look at SQLConf.SESSION_LOCAL_TIMEZONE and assume timestamps
    //     were written in that timezone and convert them to UTC timestamps.
    // Essentially this should boil down to a vector subtract of the scalar delta
    // between the configured timezone's delta from UTC on the timestamp data.
    val schemaHasTimestamps = readSchema.exists { field =>
      TrampolineUtil.dataTypeExistsRecursively(field.dataType, _.isInstanceOf[TimestampType])
    }
    if (schemaHasTimestamps && sparkSession.sessionState.conf.isParquetINT96TimestampConversion) {
      meta.willNotWorkOnGpu("GpuParquetScan does not support int96 timestamp conversion")
    }

    if (ColumnDefaultValuesShims.hasExistenceDefaultValues(readSchema)) {
      meta.willNotWorkOnGpu("GpuParquetScan does not support default values in schema")
    }

  }

  /**
   * This estimates the number of nodes in a parquet footer schema based off of the parquet spec
   * Specifically https://github.com/apache/parquet-format/blob/master/LogicalTypes.md
   */
  private def numNodesEstimate(dt: DataType): Long = dt match {
    case StructType(fields) =>
      // A struct has a group node that holds the children
      1 + fields.map(f => numNodesEstimate(f.dataType)).sum
    case ArrayType(elementType, _) =>
      // A List/Array has one group node to tag it as a list and another one
      // that is marked as repeating.
      2 + numNodesEstimate(elementType)
    case MapType(keyType, valueType, _) =>
      // A Map has one group node to tag it as a map and another one
      // that is marked as repeating, but holds the key/value
      2 + numNodesEstimate(keyType) + numNodesEstimate(valueType)
    case _ =>
      // All the other types are just value types and are represented by a non-group node
      1
  }

  /**
   * Adjust the footer reader type based off of a heuristic.
   */
  def footerReaderHeuristic(
      inputValue: ParquetFooterReaderType.Value,
      data: StructType,
      read: StructType,
      useFieldId: Boolean): ParquetFooterReaderType.Value = {
    val canUseNative = !(useFieldId && ParquetSchemaClipShims.hasFieldIds(read))
    if (!canUseNative) {
      // Native does not support field IDs yet
      ParquetFooterReaderType.JAVA
    } else {
      inputValue match {
        case ParquetFooterReaderType.AUTO =>
          val dnc = numNodesEstimate(data)
          val rnc = numNodesEstimate(read)
          if (rnc.toDouble / dnc <= 0.5 && dnc - rnc > 10) {
            ParquetFooterReaderType.NATIVE
          } else {
            ParquetFooterReaderType.JAVA
          }
        case other => other
      }
    }
  }

  /**
   * Read the given byte ranges from `inputFile` into `output` using the
   * vectored API. Returns the total number of bytes read.
   *
   * Wall time is accumulated to READ_FS_TIME. WRITE_BUFFER_TIME has no
   * analog on this path -- readVectored writes directly into `output` in
   * a single bundled call -- and remains scoped to the CPU decompress
   * path in copyAndUncompressBlocksData.
   */
  private[parquet] def readRangesToHostMemory(
      inputFile: RapidsInputFile,
      output: HostMemoryBuffer,
      ranges: Seq[CopyRange],
      metrics: Map[String, GpuMetric]): Long = {
    if (ranges.isEmpty) {
      0L
    } else {
      metrics.getOrElse(READ_FS_TIME, NoopMetric).ns {
        inputFile.readVectored(output, ranges.asJava)
      }
      ranges.map(_.getLength).sum
    }
  }

  def throwIfRebaseNeededInExceptionMode(table: Table, dateRebaseMode: DateTimeRebaseMode,
      timestampRebaseMode: DateTimeRebaseMode): Unit = {
    (0 until table.getNumberOfColumns).foreach { i =>
      val col = table.getColumn(i)
      if (dateRebaseMode == DateTimeRebaseException &&
        DateTimeRebaseUtils.isDateRebaseNeededInRead(col)) {
        throw DataSourceUtils.newRebaseExceptionInRead("Parquet")
      } else if (timestampRebaseMode == DateTimeRebaseException &&
        DateTimeRebaseUtils.isTimeRebaseNeededInRead(col)) {
        throw DataSourceUtils.newRebaseExceptionInRead("Parquet")
      }
    }
  }

  def rebaseDateTime(table: Table, dateRebaseMode: DateTimeRebaseMode,
      timestampRebaseMode: DateTimeRebaseMode): Table = {
    val dateRebaseNeeded = dateRebaseMode == DateTimeRebaseLegacy
    val timeRebaseNeeded = timestampRebaseMode == DateTimeRebaseLegacy

    lazy val tableHasDate = (0 until table.getNumberOfColumns).exists { i =>
      checkTypeRecursively(table.getColumn(i), { dt => dt == DType.TIMESTAMP_DAYS })
    }
    lazy val tableHasTimestamp = (0 until table.getNumberOfColumns).exists { i =>
      checkTypeRecursively(table.getColumn(i), { dt => dt == DType.TIMESTAMP_MICROSECONDS })
    }

    if ((dateRebaseNeeded && tableHasDate) || (timeRebaseNeeded && tableHasTimestamp)) {
      // Need to close the input table when returning a new table.
      withResource(table) { tmpTable =>
        val newColumns = (0 until tmpTable.getNumberOfColumns).map { i =>
          deepTransformRebaseDateTime(tmpTable.getColumn(i), dateRebaseNeeded, timeRebaseNeeded)
        }
        withResource(newColumns) { newCols =>
          new Table(newCols: _*)
        }
      }
    } else {
      table
    }
  }

  private def checkTypeRecursively(input: ColumnView, f: DType => Boolean): Boolean = {
    val dt = input.getType
    if (dt.isTimestampType && dt != DType.TIMESTAMP_DAYS && dt != DType.TIMESTAMP_MICROSECONDS) {
      // There should be something wrong here since timestamps other than DAYS should already
      // been converted into MICROSECONDS when reading Parquet files.
      throw new IllegalStateException(s"Unexpected date/time type: $dt " +
        "(expected TIMESTAMP_DAYS or TIMESTAMP_MICROSECONDS)")
    }
    dt match {
      case DType.LIST | DType.STRUCT => (0 until input.getNumChildren).exists(i =>
        withResource(input.getChildColumnView(i)) { child =>
          checkTypeRecursively(child, f)
        })
      case t: DType => f(t)
    }
  }

  private def deepTransformRebaseDateTime(cv: ColumnVector, dateRebaseNeeded: Boolean,
      timeRebaseNeeded: Boolean): ColumnVector = {
    ColumnCastUtil.deepTransform(cv) {
      case (cv, _) if cv.getType.isTimestampType =>
        // cv type is guaranteed to be either TIMESTAMP_DAYS or TIMESTAMP_MICROSECONDS,
        // since we already checked it in `checkTypeRecursively`.
        if ((cv.getType == DType.TIMESTAMP_DAYS && dateRebaseNeeded) ||
          cv.getType == DType.TIMESTAMP_MICROSECONDS && timeRebaseNeeded) {
          DateTimeRebase.rebaseJulianToGregorian(cv)
        } else {
          cv.copyToColumnVector()
        }
    }
  }
}

// contains meta about all the blocks in a file
case class ParquetFileInfoWithBlockMeta(filePath: Path, blocks: collection.Seq[BlockMetaData],
    partValues: InternalRow, schema: MessageType, readSchema: StructType,
    dateRebaseMode: DateTimeRebaseMode, timestampRebaseMode: DateTimeRebaseMode,
    hasInt96Timestamps: Boolean,
    // Row number of the first row in each block considering all rows in blocks.
    // If non-empty, its size should be the same as blocks.size
    blocksFirstRowIndices: Seq[Long] = Seq.empty) {
  if (blocksFirstRowIndices.nonEmpty) {
    require(blocks.length == blocksFirstRowIndices.length, s"Blocks length ${blocks.length} not " +
      s"matching first row length ${blocksFirstRowIndices.length}")
  }
}

private[rapids] case class BlockMetaWithPartFile(
    meta: ParquetFileInfoWithBlockMeta,
    file: PartitionedFile)

/**
 * A parquet compatible stream that allows reading from a HostMemoryBuffer to Parquet.
 * The majority of the code here was copied from Parquet's DelegatingSeekableInputStream with
 * minor modifications to have it be make it Scala and call into the
 * HostMemoryInputStreamMixIn's state.
 */
class HMBSeekableInputStream(
    val hmb: HostMemoryBuffer,
    val hmbLength: Long) extends ParquetSeekableInputStream
    with HostMemoryInputStreamMixIn {
  private val temp = new Array[Byte](8192)

  override def seek(offset: Long): Unit = {
    pos = offset
  }

  @throws[IOException]
  override def readFully(buffer: Array[Byte]): Unit = {
    val amountRead = read(buffer)
    val remaining = buffer.length - amountRead
    if (remaining > 0) {
      throw new EOFException("Reached the end of stream with " + remaining + " bytes left to read")
    }
  }

  @throws[IOException]
  override def readFully(buffer: Array[Byte], offset: Int, length: Int): Unit = {
    val amountRead = read(buffer, offset, length)
    val remaining = length - amountRead
    if (remaining > 0) {
      throw new EOFException("Reached the end of stream with " + remaining + " bytes left to read")
    }
  }

  @throws[IOException]
  override def read(buf: ByteBuffer): Int =
    if (buf.hasArray) {
      readHeapBuffer(buf)
    } else {
      readDirectBuffer(buf)
    }

  @throws[IOException]
  override def readFully(buf: ByteBuffer): Unit = {
    if (buf.hasArray) {
      readFullyHeapBuffer(buf)
    } else {
      readFullyDirectBuffer(buf)
    }
  }

  private def readHeapBuffer(buf: ByteBuffer) = {
    val bytesRead = read(buf.array, buf.arrayOffset + buf.position(), buf.remaining)
    if (bytesRead < 0) {
      bytesRead
    } else {
      buf.position(buf.position() + bytesRead)
      bytesRead
    }
  }

  private def readFullyHeapBuffer(buf: ByteBuffer): Unit = {
    readFully(buf.array, buf.arrayOffset + buf.position(), buf.remaining)
    buf.position(buf.limit)
  }

  private def readDirectBuffer(buf: ByteBuffer): Int = {
    var nextReadLength = Math.min(buf.remaining, temp.length)
    var totalBytesRead = 0
    var bytesRead = 0
    totalBytesRead = 0
    bytesRead = read(temp, 0, nextReadLength)
    while (bytesRead == temp.length) {
      buf.put(temp)
      totalBytesRead += bytesRead

      nextReadLength = Math.min(buf.remaining, temp.length)
      bytesRead = read(temp, 0, nextReadLength)
    }
    if (bytesRead < 0) {
      if (totalBytesRead == 0) {
        -1
      } else {
        totalBytesRead
      }
    } else {
      buf.put(temp, 0, bytesRead)
      totalBytesRead += bytesRead
      totalBytesRead
    }
  }

  private def readFullyDirectBuffer(buf: ByteBuffer): Unit = {
    var nextReadLength = Math.min(buf.remaining, temp.length)
    var bytesRead = 0
    bytesRead = 0
    bytesRead = read(temp, 0, nextReadLength)
    while (nextReadLength > 0 && bytesRead >= 0) {
      buf.put(temp, 0, bytesRead)

      nextReadLength = Math.min(buf.remaining, temp.length)
      bytesRead = read(temp, 0, nextReadLength)
    }
    if (bytesRead < 0 && buf.remaining > 0) {
      throw new EOFException("Reached the end of stream with " +
          buf.remaining + " bytes left to read")
    }
  }
}

class HMBInputFile(buffer: HostMemoryBuffer) extends InputFile {

  override def getLength: Long = buffer.getLength

  override def newStream(): ParquetSeekableInputStream = new HMBSeekableInputStream(buffer,
    getLength)
}

protected case class GpuParquetFileFilterHandler(
    @transient sqlConf: SQLConf,
    metrics: Map[String, GpuMetric]) extends Logging {

  private val FOOTER_LENGTH_SIZE = 4
  private val isCaseSensitive = sqlConf.caseSensitiveAnalysis
  private val enableParquetFilterPushDown: Boolean = sqlConf.parquetFilterPushDown
  private val pushDownDate = sqlConf.parquetFilterPushDownDate
  private val pushDownTimestamp = sqlConf.parquetFilterPushDownTimestamp
  private val pushDownDecimal = sqlConf.parquetFilterPushDownDecimal
  // From Spark 340, more string predicates are supported as push-down filters, so this
  // flag is renamed to 'xxxxStringPredicate' and specified by another config.
  private val pushDownStringPredicate = ParquetStringPredShims.pushDown(sqlConf)
  private val pushDownInFilterThreshold = sqlConf.parquetFilterPushDownInFilterThreshold
  private val datetimeRebaseMode = SparkShimImpl.parquetRebaseRead(sqlConf)
  private val int96RebaseMode = SparkShimImpl.int96ParquetRebaseRead(sqlConf)
  private val readUseFieldId = ParquetSchemaClipShims.useFieldId(sqlConf)
  private val ignoreMissingParquetFieldId = ParquetSchemaClipShims.ignoreMissingIds(sqlConf)

  private val PARQUET_ENCRYPTION_CONFS = Seq("parquet.encryption.kms.client.class",
    "parquet.encryption.kms.client.class", "parquet.crypto.factory.class")

  private def isParquetTimeInInt96(parquetType: Type): Boolean = {
    parquetType match {
      case p:PrimitiveType =>
        p.getPrimitiveTypeName == PrimitiveTypeName.INT96
      case g:GroupType => //GroupType
        g.getFields.asScala.exists(t => isParquetTimeInInt96(t))
      case _ => false
    }
  }

  /**
   * Convert the spark data type to something that the native processor can understand.
   */
  private def convertToParquetNative(schema: DataType): ParquetFooter.SchemaElement = {
    schema match {
      case cst: StructType =>
        val schemaBuilder = ParquetFooter.StructElement.builder()
        cst.fields.foreach { field =>
          schemaBuilder.addChild(field.name, convertToParquetNative(field.dataType))
        }
        schemaBuilder.build()
      case _: NumericType | BinaryType | BooleanType | DateType | TimestampType | StringType =>
        new ParquetFooter.ValueElement()
      case at: ArrayType =>
        new ParquetFooter.ListElement(convertToParquetNative(at.elementType))
      case mt: MapType =>
        new ParquetFooter.MapElement(
          convertToParquetNative(mt.keyType),
          convertToParquetNative(mt.valueType))
      case other =>
        throw new UnsupportedOperationException(s"Need some help here $other...")
    }
  }

  private def convertToFooterSchema(schema: StructType): ParquetFooter.StructElement = {
    convertToParquetNative(schema).asInstanceOf[ParquetFooter.StructElement]
  }

  private def getFooterBuffer(
      fileIO: RapidsFileIO,
      filePath: Path,
      conf: Configuration,
      metrics: Map[String, GpuMetric]): HostMemoryBuffer = {
    val inputFile = fileIO.newInputFile(filePath)
    withResource(ParquetFooterUtils.getFooterBuffer(inputFile, metrics,
        readFooterBuffer(fileIO, filePath, conf))) { hmb =>
      // buffer includes header and trailing length and magic, stripped here
      hmb.slice(MAGIC.length, hmb.getLength - Integer.BYTES - MAGIC.length)
    }
  }

  private def readFooterBuffer(
      fileIO: RapidsFileIO,
      filePath: Path,
      conf: Configuration): HostMemoryBuffer = {
    if (fileIO.isInstanceOf[HadoopFileIO]) {
      // We should remove this after https://github.com/NVIDIA/spark-rapids/issues/13306 is
      // implemented.
      val result = PerfIO.readParquetFooterBuffer(filePath, conf, verifyParquetMagic)
      val scheme = filePath.toUri.getScheme
      if (scheme != null && scheme.startsWith("s3")) {
        GpuTaskMetrics.get.recordPerfioS3BackendOnce()
      } else if (result.isDefined && (scheme == "gs" || scheme == "gcs")) {
        GpuTaskMetrics.get.recordPerfioGCSBackendOnce()
      }
      result.getOrElse(readFooterBufUsingHadoop(fileIO, filePath))
    } else {
      readFooterBufUsingHadoop(fileIO, filePath)
    }
  }

  private def readFooterBufUsingHadoop(fileIO: RapidsFileIO, filePath: Path): HostMemoryBuffer = {
    val inputFile = fileIO.newInputFile(filePath)
    // Much of this code came from the parquet_mr projects ParquetFileReader, and was modified
    // to match our needs.
    val fileLen = inputFile.getLength
    // MAGIC + data + footer + footerIndex + MAGIC
    if (fileLen < MAGIC.length + FOOTER_LENGTH_SIZE + MAGIC.length) {
      throw new RuntimeException(s"$filePath is not a Parquet file (too small length: $fileLen )")
    }
    NvtxRegistry.PARQUET_READ_FOOTER_BYTES {
      // Step 1: read the trailing footer-length int + final MAGIC via readTail.
      // For the IcebergS3InputFile path this is a single suffix-range GET
      // (Range: bytes=-N) — no separate file-length round-trip beyond getLength
      // above, no inputStream.seek + IOUtils.readFully loop. HadoopInputFile
      // falls back to the default RapidsInputFile.readTail (open + seek + read).
      val trailerLen = FOOTER_LENGTH_SIZE + MAGIC.length
      val (footerLength, trailerMagic) = withResource(
          HostAlloc.alloc(trailerLen, preferPinned = false)) { trailerBuf =>
        inputFile.readTail(trailerLen, trailerBuf)
        val view = trailerBuf.asByteBuffer(0, trailerLen).order(ByteOrder.LITTLE_ENDIAN)
        val len = view.getInt()
        val m = new Array[Byte](MAGIC.length)
        view.get(m)
        (len, m)
      }
      verifyParquetMagic(filePath, trailerMagic)
      val footerLengthIndex = fileLen - trailerLen
      val footerIndex = footerLengthIndex - footerLength
      if (footerIndex < MAGIC.length || footerIndex >= footerLengthIndex) {
        throw new RuntimeException(s"corrupted file: the footer index is not within " +
          s"the file: $footerIndex")
      }
      val hmbLength = (fileLen - footerIndex).toInt
      closeOnExcept(HostAlloc.alloc(hmbLength + MAGIC.length, preferPinned = false)) { outBuffer =>
        // Buffer layout:
        //   [MAGIC at offset 0] + [file[footerIndex .. fileLen) at offset MAGIC.length]
        outBuffer.asByteBuffer(0, MAGIC.length).put(MAGIC)
        // Step 2: vectored read of [footerIndex .. fileLen) into outBuffer at MAGIC.length.
        // Routes through IcebergS3InputFile.readVectored on S3, using the tuned
        // async client; HadoopInputFile uses the RapidsInputFile interface default.
        val ranges = new java.util.ArrayList[CopyRange](1)
        ranges.add(new CopyRange(footerIndex, hmbLength, MAGIC.length))
        inputFile.readVectored(outBuffer, ranges)
        outBuffer
      }
    }
  }


  private def verifyParquetMagic(filePath: Path, magic: Array[Byte]): Unit = {
    ParquetFooterUtils.verifyParquetMagic(filePath, magic)
  }

  private def readAndFilterFooter(
      fileIO: RapidsFileIO,
      file: PartitionedFile,
      conf : Configuration,
      readDataSchema: StructType,
      filePath: Path): ParquetFooter = {
    val footerSchema = convertToFooterSchema(readDataSchema)
    val footerBuffer = getFooterBuffer(fileIO, filePath, conf, metrics)
    withResource(footerBuffer) { footerBuffer =>
      NvtxRegistry.PARQUET_PARSE_FILTER_FOOTER {
        // In the future, if we know we're going to read the entire file,
        // i.e.: having file length from file stat == amount to read,
        // then sending -1 as length to the native footer parser allows it to
        // skip the row group filtering.
        val len = file.length

        ParquetFooter.readAndFilter(footerBuffer, file.start, len,
          footerSchema, !isCaseSensitive)
      }
    }
  }

  @scala.annotation.nowarn(
    "msg=method readFooter in class ParquetFileReader is deprecated"
  )
  private def readAndSimpleFilterFooter(
      fileIO: RapidsFileIO,
      file: PartitionedFile,
      conf : Configuration,
      filePath: Path): ParquetMetadata = {
    //noinspection ScalaDeprecation
    NvtxRegistry.PARQUET_READ_FOOTER {
      val inputFile = fileIO.newInputFile(filePath)
      withResource(ParquetFooterUtils.getFooterBuffer(inputFile, metrics,
          readFooterBuffer(fileIO, filePath, conf))) { hmb =>
        ParquetFileReader.readFooter(new HMBInputFile(hmb),
          ParquetMetadataConverter.range(file.start, file.start + file.length))
      }
    }
  }

  @scala.annotation.nowarn
  def filterBlocks(
      fileIO: RapidsFileIO,
      footerReader: ParquetFooterReaderType.Value,
      file: PartitionedFile,
      conf: Configuration,
      filters: Array[Filter],
      readDataSchema: StructType): ParquetFileInfoWithBlockMeta = {
    NvtxRegistry.PARQUET_FILTER_BLOCKS {
      val filePath = new Path(new URI(file.filePath.toString()))
      // Make sure we aren't trying to read encrypted files. For now, remove the related
      // parquet confs from the hadoop configuration and try to catch the resulting
      // exception and print a useful message
      PARQUET_ENCRYPTION_CONFS.foreach { encryptConf =>
        if (conf.get(encryptConf) != null) {
          conf.unset(encryptConf)
        }
      }
      val footer: ParquetMetadata = try {
        footerReader match {
          case ParquetFooterReaderType.NATIVE =>
            val (serialized, rowIndexOffsets) = withResource(readAndFilterFooter(fileIO, file,
              conf, readDataSchema, filePath)) { tableFooter =>
                if (tableFooter.getNumColumns <= 0) {
                  // Special case because java parquet reader does not like having 0 columns.
                  val numRows = tableFooter.getNumRows
                  val block = new BlockMetaData()
                  block.setRowCount(numRows)
                  // Fix up the row index offset
                  val offsets = tableFooter.getRowIndexOffsets
                  if (offsets.nonEmpty) {
                    GpuParquetUtilsShims.setRowIndexOffset(block, offsets(0))
                  }
                  val schema = new MessageType("root")
                  return ParquetFileInfoWithBlockMeta(filePath, Seq(block), file.partitionValues,
                    schema, readDataSchema, DateTimeRebaseLegacy, DateTimeRebaseLegacy,
                    hasInt96Timestamps = false)
                }

                (tableFooter.serializeThriftFile(), tableFooter.getRowIndexOffsets)
            }
            val footer = withResource(serialized) { serialized =>
              NvtxRegistry.PARQUET_READ_FILTERED_FOOTER {
                val inputFile = new HMBInputFile(serialized)

                // We already filtered the ranges so no need to do more here...
                ParquetFileReader.readFooter(inputFile, ParquetMetadataConverter.NO_FILTER)
              }
            }
            // Fix up row index offsets. ParquetFileReader.readFooter computes row
            // index offsets assuming the row groups it receives are the entire file.
            // The native footer reader filters row groups by byte range in C++ before
            // serialization, so readFooter can see only a subset and compute wrong
            // offsets (e.g. always 0 for a single surviving row group). This breaks
            // any logic relying on file-global row indices such as deletion vector
            // filtering.
            // So we compute the correct offsets from all row groups before filtering,
            // and then apply them to the filtered blocks.
            val blocks = footer.getBlocks
            require(blocks.size() == rowIndexOffsets.length,
              s"Row index offsets length (${rowIndexOffsets.length}) != " +
              s"blocks count (${blocks.size()}) for $filePath " +
              s"[range=${file.start}:${file.length}]")
            blocks.asScala.zip(rowIndexOffsets).foreach { case (block, offset) =>
              GpuParquetUtilsShims.setRowIndexOffset(block, offset)
            }
            footer
          case _ =>
            readAndSimpleFilterFooter(fileIO, file, conf, filePath)
        }
      } catch {
        case e if GpuParquetCrypto.isColumnarCryptoException(e) =>
          throw new RuntimeException("The GPU does not support reading encrypted Parquet " +
            "files. To read encrypted or columnar encrypted files, disable the GPU Parquet " +
            s"reader via ${RapidsConf.ENABLE_PARQUET_READ.key}.", e)
      }

      val fileSchema = footer.getFileMetaData.getSchema

      // check spark.sql.parquet.fieldId.read.ignoreMissing
      ParquetSchemaClipShims.checkIgnoreMissingIds(ignoreMissingParquetFieldId, fileSchema,
        readDataSchema)

      val pushedFilters = if (enableParquetFilterPushDown) {
        val parquetFilters = SparkShimImpl.getParquetFilters(fileSchema, pushDownDate,
          pushDownTimestamp, pushDownDecimal, pushDownStringPredicate, pushDownInFilterThreshold,
          isCaseSensitive, footer.getFileMetaData.getKeyValueMetaData.get, datetimeRebaseMode)
        filters.flatMap(parquetFilters.createFilter).reduceOption(FilterApi.and)
      } else {
        None
      }

      val blocks = if (pushedFilters.isDefined) {
        NvtxRegistry.PARQUET_GET_BLOCKS_WITH_FILTER {
          // Use the ParquetFileReader to perform dictionary-level filtering
          ParquetInputFormat.setFilterPredicate(conf, pushedFilters.get)
          //noinspection ScalaDeprecation
          withResource(new ParquetFileReader(conf, footer.getFileMetaData, filePath,
            footer.getBlocks, Collections.emptyList[ColumnDescriptor])) { parquetReader =>
            parquetReader.getRowGroups
          }
        }
      } else {
        footer.getBlocks
      }

      val (clipped, clippedSchema) =
        NvtxRegistry.PARQUET_CLIP_SCHEMA {
          val clippedSchema = ParquetSchemaUtils.clipParquetSchema(
            fileSchema, readDataSchema, isCaseSensitive, readUseFieldId)
          // Check if the read schema is compatible with the file schema.
          checkSchemaCompat(clippedSchema, readDataSchema,
            (t: Type, d: DataType) => throwTypeIncompatibleError(t, d, file.filePath.toString()),
            isCaseSensitive, readUseFieldId)
          val clipped = GpuParquetUtils.clipBlocksToSchema(clippedSchema, blocks, isCaseSensitive)
          (clipped, clippedSchema)
        }
      val hasDateTimeInReadSchema = DataTypeUtils.hasDateOrTimestampType(readDataSchema)
      val dateRebaseModeForThisFile = DateTimeRebaseUtils.datetimeRebaseMode(
          footer.getFileMetaData.getKeyValueMetaData.get,
          datetimeRebaseMode,
          hasDateTimeInReadSchema)
      val hasInt96Timestamps = isParquetTimeInInt96(fileSchema)
      val timestampRebaseModeForThisFile = if (hasInt96Timestamps) {
        DateTimeRebaseUtils.int96RebaseMode(
          footer.getFileMetaData.getKeyValueMetaData.get, int96RebaseMode)
      } else {
        dateRebaseModeForThisFile
      }

      ParquetFileInfoWithBlockMeta(filePath, clipped, file.partitionValues,
        clippedSchema, readDataSchema, dateRebaseModeForThisFile,
        timestampRebaseModeForThisFile, hasInt96Timestamps)
    }
  }

  /**
   * Recursively check if the read schema is compatible with the file schema. The errorCallback
   * will be invoked to throw an exception once any incompatible type pairs are found.
   *
   * Any element in the read schema that are missing from the file schema are ignored.
   *
   * The function only accepts top-level schemas, which means structures of root columns. Based
   * on this assumption, it can infer root types from input schemas.
   *
   * @param fileType input file's Parquet schema
   * @param readType spark type read from Parquet file
   * @param errorCallback call back function to throw exception if type mismatch
   * @param rootFileType file type of each root column
   * @param rootReadType read type of each root column
   */
  private def checkSchemaCompat(fileType: Type,
                                readType: DataType,
                                errorCallback: (Type, DataType) => Unit,
                                isCaseSensitive: Boolean,
                                useFieldId: Boolean,
                                rootFileType: Option[Type] = None,
                                rootReadType: Option[DataType] = None): Unit = {
    readType match {
      case struct: StructType =>
        val fileFieldMap = fileType.asGroupType().getFields.asScala
          .map { f =>
            (if (isCaseSensitive) f.getName else f.getName.toLowerCase(Locale.ROOT)) -> f
          }.toMap

        val fieldIdToFieldMap = ParquetSchemaClipShims.fieldIdToFieldMap(useFieldId, fileType)

        def getParquetType(f: StructField): Option[Type] = {
          if (useFieldId && ParquetSchemaClipShims.hasFieldId(f)) {
            // use field ID and Spark schema specified field ID
            fieldIdToFieldMap.get(ParquetSchemaClipShims.getFieldId(f))
          } else {
            fileFieldMap.get(if (isCaseSensitive) f.name else f.name.toLowerCase(Locale.ROOT))
          }
        }
        struct.fields.foreach { f =>
          getParquetType(f).foreach { fieldType =>
            checkSchemaCompat(fieldType,
              f.dataType,
              errorCallback,
              isCaseSensitive,
              useFieldId,
              // Record root types for each column, so as to throw a readable exception
              // over nested types.
              Some(rootFileType.getOrElse(fieldType)),
              Some(rootReadType.getOrElse(f.dataType)))
          }
        }
      case array: ArrayType =>
        if (fileType.isPrimitive) {
          if (fileType.getRepetition == Type.Repetition.REPEATED) {
            checkSchemaCompat(fileType, array.elementType, errorCallback, isCaseSensitive,
              useFieldId, rootFileType, rootReadType)
          } else {
            errorCallback(fileType, readType)
          }
        } else {
          val fileGroupType = fileType.asGroupType()
          if (fileGroupType.isRepetition(Type.Repetition.REPEATED) &&
              fileGroupType.getOriginalType != OriginalType.LIST &&
              fileGroupType.getOriginalType != OriginalType.MAP) {
            // Unannotated 1-level legacy list: the REPEATED group itself is the element,
            // regardless of how many fields it has. Without this branch, a single-field
            // repeated group (e.g. `repeated group g { optional int32 someId; }`) is
            // mis-interpreted as a 2/3-level LIST wrapper and its primitive child is
            // treated as a group, producing ClassCastException. The cuDF reader handles
            // the actual decoding (see rapidsai/cudf#22541 for the matching cuDF-side fix).
            // A LIST/MAP-annotated REPEATED group is the inner level of a legacy 2-level
            // nested list and is excluded here so it falls through to isElementType below
            // (#11589, #11592).
            checkSchemaCompat(fileGroupType, array.elementType, errorCallback, isCaseSensitive,
              useFieldId, rootFileType, rootReadType)
          } else {
            val repeatedType = fileGroupType.getType(0)
            val childType =
              if (isElementType(repeatedType, fileType.getName)) {
                // Legacy element, per Parquet LogicalType backward compatibility rules.
                // Retain the child as the element type.
                repeatedType
              }
              else {
                // Conforms to current Parquet LogicalType rules.
                // Unwrap child group layer, and use grandchild's element type.
                repeatedType.asGroupType().getType(0)
              }
            checkSchemaCompat(childType, array.elementType, errorCallback, isCaseSensitive,
              useFieldId, rootFileType, rootReadType)
          }
        }

      case map: MapType =>
        val parquetMap = fileType.asGroupType().getType(0).asGroupType()
        val parquetMapKey = parquetMap.getType(0)
        val parquetMapValue = parquetMap.getType(1)
        checkSchemaCompat(parquetMapKey, map.keyType, errorCallback, isCaseSensitive, useFieldId,
          rootFileType, rootReadType)
        checkSchemaCompat(parquetMapValue, map.valueType, errorCallback, isCaseSensitive,
          useFieldId, rootFileType, rootReadType)

      case dt =>
        checkPrimitiveCompat(fileType.asPrimitiveType(),
          dt,
          () => errorCallback(rootFileType.get, rootReadType.get))
    }
  }

  // scalastyle:off
  // Implement Parquet LIST backwards-compatibility rules.
  // See: https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#backward-compatibility-rules
  // Inspired by Apache Spark's ParquetSchemaConverter.isElementType():
  // https://github.com/apache/spark/blob/3a0e6bde2aaa11e1165f4fde040ff02e1743795e/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/parquet/ParquetSchemaConverter.scala#L413
  // scalastyle:on
  private def isElementType(repeatedType: Type, parentName: String): Boolean = {
    {
      // For legacy 2-level list types with primitive element type, e.g.:
      //
      //    // ARRAY<INT> (nullable list, non-null elements)
      //    optional group my_list (LIST) {
      //      repeated int32 element;
      //    }
      //
      repeatedType.isPrimitive
    } || {
      // For legacy 2-level list types whose element type is a group type with 2 or more fields,
      // e.g.:
      //
      //    // ARRAY<STRUCT<str: STRING, num: INT>> (nullable list, non-null elements)
      //    optional group my_list (LIST) {
      //      repeated group element {
      //        required binary str (UTF8);
      //        required int32 num;
      //      };
      //    }
      //
      repeatedType.asGroupType().getFieldCount > 1
    } || {
      // For legacy 2-level list types generated by parquet-avro (Parquet version < 1.6.0), e.g.:
      //
      //    // ARRAY<STRUCT<str: STRING>> (nullable list, non-null elements)
      //    optional group my_list (LIST) {
      //      repeated group array {
      //        required binary str (UTF8);
      //      };
      //    }
      //
      repeatedType.getName == "array"
    } || {
      // For Parquet data generated by parquet-thrift, e.g.:
      //
      //    // ARRAY<STRUCT<str: STRING>> (nullable list, non-null elements)
      //    optional group my_list (LIST) {
      //      repeated group my_list_tuple {
      //        required binary str (UTF8);
      //      };
      //    }
      //
      repeatedType.getName == s"${parentName}_tuple"
    }
  }

  def isUnsignedIntTypeMatched(lt: LogicalTypeAnnotation,
                               bitWidth: Int): Boolean = lt match {
    case it: IntLogicalTypeAnnotation
      if !it.isSigned && it.getBitWidth == bitWidth => true
    case _ => false
  }

  def isTimestampTypeMatched(lt: LogicalTypeAnnotation,
                             tu: LogicalTypeAnnotation.TimeUnit): Boolean = lt match {
    case tt: TimestampLogicalTypeAnnotation
      if tt.getUnit == tu => true
    case _ => false
  }

  def isDateTypeMatched(lt: LogicalTypeAnnotation): Boolean =
    lt.isInstanceOf[DateLogicalTypeAnnotation]

  /**
   * Check the compatibility over primitive types. This function refers to the `getUpdater` method
   * of org.apache.spark.sql.execution.datasources.parquet.ParquetVectorUpdaterFactory.
   *
   * To avoid unnecessary pattern matching, this function is designed to return or throw ASAP.
   *
   * This function uses some deprecated Parquet APIs, because Spark 3.1 is relied on parquet-mr
   * of an older version.
   */
  @scala.annotation.nowarn("msg=method getDecimalMetadata in class PrimitiveType is deprecated")
  private def checkPrimitiveCompat(pt: PrimitiveType,
                                   dt: DataType,
                                   errorCallback: () => Unit): Unit = {
    // Note that a lot of this code is based off of what is in the java class
    // org.apache.spark.sql.execution.datasources.parquet.ParquetVectorUpdaterFactory
    // It also needs to be kept in sync with ParquetSchemaUtils.evolveSchemaCasts
    pt.getPrimitiveTypeName match {
      case PrimitiveTypeName.BOOLEAN if dt == DataTypes.BooleanType =>
        return

      case PrimitiveTypeName.INT32 => {
        val lt = pt.getLogicalTypeAnnotation
        if (dt == DataTypes.ByteType ||
          dt == DataTypes.ShortType ||
          dt == DataTypes.IntegerType ||
          dt == DataTypes.LongType ||
          dt == DataTypes.DoubleType ||
          dt == DataTypes.DateType ||
          (isTimestampNTZ(dt) && isDateTypeMatched(lt)) ||
          GpuTypeShims.isSupportedYearMonthType(dt) ||
          canReadAsDecimal(pt, dt)) {
          return
        }
      }
      case PrimitiveTypeName.INT64 =>
        val lt = pt.getLogicalTypeAnnotation
        // In the code a TimeType is also supported
        if (dt == DataTypes.LongType ||
          // Day-time interval type is stored as int64 in parquet
          GpuTypeShims.isSupportedDayTimeType(dt) ||
          canReadAsDecimal(pt, dt) ||
          (isLongDecimal(dt) && isUnsignedIntTypeMatched(lt, 64)) ||
          ((dt == DataTypes.TimestampType || isTimestampNTZ(dt)) &&
            (isTimestampTypeMatched(lt, LogicalTypeAnnotation.TimeUnit.MICROS) ||
              isTimestampTypeMatched(lt, LogicalTypeAnnotation.TimeUnit.MILLIS)))) {
          return
        }

      case PrimitiveTypeName.FLOAT if dt == DataTypes.FloatType || dt == DataTypes.DoubleType =>
        return

      case PrimitiveTypeName.DOUBLE if dt == DataTypes.DoubleType =>
        return

      case PrimitiveTypeName.INT96 if dt == DataTypes.TimestampType ||
        isTimestampNTZ(dt) =>
        return

      case PrimitiveTypeName.BINARY =>
        if (dt == DataTypes.StringType ||
          dt == DataTypes.BinaryType ||
          canReadAsDecimal(pt, dt)) {
          return
        }

      case PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY =>
        if (canReadAsDecimal(pt, dt) ||
          dt == DataTypes.BinaryType) {
          return
        }
      case _ =>
    }

    // If we get here, it means the combination of Spark and Parquet type is invalid or not
    // supported.
    errorCallback()
  }

  private def throwTypeIncompatibleError(parquetType: Type,
                                         sparkType: DataType,
                                         filePath: String): Unit = {
    // Match the format produced by Spark's vectorized reader
    // (ParquetVectorUpdaterFactory#constructConvertNotSupportedException):
    // the column path is bracket-wrapped (Java Arrays.toString shape) and
    // the Parquet type is the primitive type name (e.g. "INT32") rather
    // than the full schema literal ("required int32 a"). For non-primitive
    // root types we fall back to toString.
    val columnPath = s"[${parquetType.getName}]"
    val foundType = if (parquetType.isPrimitive) {
      // .name() avoids the ambiguous PrimitiveTypeName.toString(ColumnReader) overload
      parquetType.asPrimitiveType().getPrimitiveTypeName.name()
    } else {
      parquetType.toString
    }
    val exception = new SchemaColumnConvertNotSupportedException(
      columnPath,
      foundType,
      sparkType.catalogString)

    // A copy of QueryExecutionErrors.unsupportedSchemaColumnConvertError introduced in 3.2+
    // TODO: replace with unsupportedSchemaColumnConvertError after we deprecate Spark 3.1
    val message = "Parquet column cannot be converted in " +
      s"file $filePath. Column: $columnPath, " +
      s"Expected: ${sparkType.catalogString}, Found: $foundType"
    throw new QueryExecutionException(message, exception)
  }

  private def isLongDecimal(dt: DataType): Boolean =
    dt match {
      case d: DecimalType => d.precision == 20 && d.scale == 0
      case _ => false
    }

  // TODO: Since we have deprecated Spark 3.1, fetch decimal meta with DecimalLogicalTypeAnnotation
  @scala.annotation.nowarn("msg=method getDecimalMetadata in class PrimitiveType is deprecated")
  private def canReadAsDecimal(pt: PrimitiveType, dt: DataType): Boolean = {
    (DecimalType.is32BitDecimalType(dt)
      || DecimalType.is64BitDecimalType(dt)
      || DecimalType.isByteArrayDecimalType(dt)) && isValidDecimalType(pt.getDecimalMetadata)
  }

  // TODO: After we deprecate Spark 3.1, fetch decimal meta with DecimalLogicalTypeAnnotation
  @scala.annotation.nowarn("msg=class DecimalMetadata in package schema is deprecated")
  private def isValidDecimalType(metadata: DecimalMetadata): Boolean = {
    metadata != null
  }
}

/**
 * Similar to GpuParquetPartitionReaderFactory but extended for reading multiple files
 * in an iteration. This will allow us to read multiple small files and combine them
 * on the CPU side before sending them down to the GPU.
 */
abstract class AbstractGpuParquetMultiFilePartitionReaderFactory(
    @transient sqlConf: SQLConf,
    broadcastedConf: Broadcast[SerializableConfiguration],
    dataSchema: StructType,
    readDataSchema: StructType,
    partitionSchema: StructType,
    filters: Array[Filter],
    @transient rapidsConf: RapidsConf,
    poolConfBuilder: ThreadPoolConfBuilder,
    metrics: Map[String, GpuMetric],
    queryUsesInputFile: Boolean)
  extends MultiFilePartitionReaderFactoryBase(sqlConf, broadcastedConf, rapidsConf) {

  // we make sure we mark this as a transient lazy val, so we only materialize it
  // from a task when we need to create the fileIO instance. This stops a regression
  // when we materialize the hadoop conf eagerly, see:
  // https://github.com/NVIDIA/spark-rapids/issues/13353
  @transient protected lazy val fileIO = new HadoopFileIO(broadcastedConf.value.value)
  protected val isCaseSensitive = sqlConf.caseSensitiveAnalysis
  protected val debugDumpPrefix = rapidsConf.parquetDebugDumpPrefix
  protected val debugDumpAlways = rapidsConf.parquetDebugDumpAlways
  protected val maxNumFileProcessed = rapidsConf.maxNumParquetFilesParallel
  protected val ignoreMissingFiles = sqlConf.ignoreMissingFiles
  protected val ignoreCorruptFiles = sqlConf.ignoreCorruptFiles
  protected val filterHandler = GpuParquetFileFilterHandler(sqlConf, metrics)
  protected val readUseFieldId = ParquetSchemaClipShims.useFieldId(sqlConf)
  protected val footerReadType = GpuParquetScan.footerReaderHeuristic(
    rapidsConf.parquetReaderFooterType, dataSchema, readDataSchema, readUseFieldId)
  protected val numFilesFilterParallel = rapidsConf.numFilesFilterParallel
  protected val combineThresholdSize =
    RapidsConf.PARQUET_MULTITHREADED_COMBINE_THRESHOLD.get(sqlConf)
      .map { deprecatedVal =>
        logWarning(s"${RapidsConf.PARQUET_MULTITHREADED_COMBINE_THRESHOLD} is deprecated, " +
          s"use ${RapidsConf.READER_MULTITHREADED_COMBINE_THRESHOLD} instead. But for now " +
          "the deprecated one will be honored if both are set.")
        deprecatedVal
      }.getOrElse(rapidsConf.getMultithreadedCombineThreshold)
  protected val combineWaitTime =
    RapidsConf.PARQUET_MULTITHREADED_COMBINE_WAIT_TIME.get(sqlConf)
      .map { f =>
        logWarning(s"${RapidsConf.PARQUET_MULTITHREADED_COMBINE_WAIT_TIME} is deprecated, " +
          s"use ${RapidsConf.READER_MULTITHREADED_COMBINE_WAIT_TIME} instead. But for now " +
          "the deprecated one will be honored if both are set.")
        f.intValue()
      }.getOrElse(rapidsConf.getMultithreadedCombineWaitTime)
  protected val keepReadsInOrderFromConf =
    RapidsConf.PARQUET_MULTITHREADED_READ_KEEP_ORDER.get(sqlConf)
      .map { deprecatedVal =>
        logWarning(s"${RapidsConf.PARQUET_MULTITHREADED_READ_KEEP_ORDER} is deprecated, " +
          s"use ${RapidsConf.READER_MULTITHREADED_READ_KEEP_ORDER} instead. But for now " +
          "the deprecated one will be honored if both are set.")
        deprecatedVal
      }.getOrElse(rapidsConf.getMultithreadedReaderKeepOrder)
  protected val compressCfg = CpuCompressionConfig.forParquet(rapidsConf)

  // We can't use the coalescing files reader when InputFileName, InputFileBlockStart,
  // or InputFileBlockLength because we are combining all the files into a single buffer
  // and we don't know which file is associated with each row.
  override val canUseCoalesceFilesReader: Boolean =
    rapidsConf.isParquetCoalesceFileReadEnabled && !(queryUsesInputFile || ignoreCorruptFiles)

  override val canUseMultiThreadReader: Boolean = rapidsConf.isParquetMultiThreadReadEnabled

  protected def createBaseMultiFileCloudReader(
      fileIO: RapidsFileIO,
      conf: Configuration,
      files: Array[PartitionedFile],
      filterFunc: PartitionedFile => ParquetFileInfoWithBlockMeta,
      isSchemaCaseSensitive: Boolean,
      debugDumpPrefix: Option[String],
      debugDumpAlways: Boolean,
      maxReadBatchSizeRows: Integer,
      maxReadBatchSizeBytes: Long,
      targetBatchSizeBytes: Long,
      maxGpuColumnSizeBytes: Long,
      useChunkedReader: Boolean,
      maxChunkedReaderMemoryUsageSizeBytes: Long,
      compressCfg: CpuCompressionConfig,
      execMetrics: Map[String, GpuMetric],
      partitionSchema: StructType,
      poolConf: ThreadPoolConf,
      maxNumFileProcessed: Int,
      ignoreMissingFiles: Boolean,
      ignoreCorruptFiles: Boolean,
      useFieldId: Boolean,
      queryUsesInputFile: Boolean,
      keepReadsInOrder: Boolean,
      combineConf: CombineConf
  ): AbstractMultiFileCloudParquetPartitionReader

  /**
   * Build the PartitionReader for cloud reading
   *
   * @param files files to be read
   * @param conf  configuration
   * @return cloud reading PartitionReader
   */
  override def buildBaseColumnarReaderForCloud(
      files: Array[PartitionedFile],
      conf: Configuration): PartitionReader[ColumnarBatch] = {
    val filterFunc = (file: PartitionedFile) => {
      // we need to copy the Hadoop Configuration because filter push down can mutate it,
      // which can affect other threads.
      filterHandler.filterBlocks(fileIO, footerReadType, file, new Configuration(conf),
        filters, readDataSchema)
    }
    val combineConf = CombineConf(combineThresholdSize, combineWaitTime)
    val poolConf = poolConfBuilder.build()
    val reader = createBaseMultiFileCloudReader(fileIO, conf, files, filterFunc,
      isCaseSensitive,
      debugDumpPrefix, debugDumpAlways, maxReadBatchSizeRows, maxReadBatchSizeBytes,
      targetBatchSizeBytes, maxGpuColumnSizeBytes,
      useChunkedReader, maxChunkedReaderMemoryUsageSizeBytes, compressCfg,
      metrics, partitionSchema,
      poolConf,
      maxNumFileProcessed, ignoreMissingFiles,
      ignoreCorruptFiles, readUseFieldId, queryUsesInputFile, keepReadsInOrderFromConf,
      combineConf)
    // NOTE: Initialize must happen after the initialization of the reader, to ensure everything
    // inside the reader being fully initialized.
    if (conf.getBoolean("rapids.sql.scan.prefetch", false)) {
      reader.eagerPrefetchInit()
    }
    reader
  }

  private def filterBlocksForCoalescingReader(
      footerReadType: ParquetFooterReaderType.Value,
      file: PartitionedFile,
      conf: Configuration,
      filters: Array[Filter],
      readDataSchema: StructType): BlockMetaWithPartFile = {
    try {
      logDebug(s"Filtering blocks for coalescing reader, file: ${file.filePath}")
      val meta = filterHandler.filterBlocks(fileIO, footerReadType, file, conf, filters,
        readDataSchema)
      BlockMetaWithPartFile(meta, file)
    } catch {
      case e: FileNotFoundException if ignoreMissingFiles =>
        logWarning(s"Skipped missing file: ${file.filePath}", e)
        val meta = ParquetFileInfoWithBlockMeta(
          new Path(new URI(file.filePath.toString())), Seq.empty,
          file.partitionValues, null, null, DateTimeRebaseLegacy, DateTimeRebaseLegacy,
          hasInt96Timestamps = false)
        BlockMetaWithPartFile(meta, file)
      // Throw FileNotFoundException even if `ignoreCorruptFiles` is true
      case e: FileNotFoundException if !ignoreMissingFiles => throw e
      // If ignoreMissingFiles=true, this case will never be reached. But it's ok
      // to leave this branch here.
      case e@(_: RuntimeException | _: IOException) if ignoreCorruptFiles =>
        logWarning(
          s"Skipped the rest of the content in the corrupted file: ${file.filePath}", e)
        val meta = ParquetFileInfoWithBlockMeta(
          new Path(new URI(file.filePath.toString())), Seq.empty,
          file.partitionValues, null, null, DateTimeRebaseLegacy, DateTimeRebaseLegacy,
          hasInt96Timestamps = false)
        BlockMetaWithPartFile(meta, file)
    }
  }

  private class CoalescingFilterRunner(
      footerReadType: ParquetFooterReaderType.Value,
      taskContext: TaskContext,
      files: Array[PartitionedFile],
      conf: Configuration,
      filters: Array[Filter],
      readDataSchema: StructType) extends UnboundedAsyncRunner[Array[BlockMetaWithPartFile]]
      with Logging {

    override def callImpl(): Array[BlockMetaWithPartFile] = {
      TrampolineUtil.setTaskContext(taskContext)
      try {
        files.map { file =>
          filterBlocksForCoalescingReader(footerReadType, file, conf, filters, readDataSchema)
        }
      } finally {
        TrampolineUtil.unsetTaskContext()
      }
    }
  }

  /**
   * Reads footer metadata for all files in preparation for a coalescing read.
   * Handles missing/corrupt files according to [[ignoreMissingFiles]] / [[ignoreCorruptFiles]]
   * and performs the footer reads in parallel when [[numFilesFilterParallel]] > 0.
   */
  protected def readBlockMetasForCoalescing(
      files: Array[PartitionedFile],
      conf: Configuration,
      poolConf: ThreadPoolConf): Array[BlockMetaWithPartFile] = {
    if (numFilesFilterParallel > 0) {
      val tc = TaskContext.get()
      val threadPool = MultiFileReaderThreadPool.getOrCreateThreadPool(poolConf)
      files.grouped(numFilesFilterParallel).map { fileGroup =>
        // we need to copy the Hadoop Configuration because filter push down can mutate it,
        // which can affect other threads.
        threadPool.submit(
          new CoalescingFilterRunner(footerReadType, tc, fileGroup, new Configuration(conf),
            filters, readDataSchema))
      }.toArray.flatMap { future =>
        future.get().data
      }
    } else {
      // We need to copy the Hadoop Configuration because filter push down can mutate it. In
      // this case we are serially iterating through the files so each one mutating it serially
      // doesn't affect the filter of the other files. We just need to make sure it's copied
      // once so other tasks don't modify the same conf.
      val hadoopConf = new Configuration(conf)
      files.map { file =>
        filterBlocksForCoalescingReader(footerReadType, file,
          hadoopConf, filters, readDataSchema)
      }
    }
  }

  /**
   * Build the PartitionReader for coalescing reading
   *
   * @param files files to be read
   * @param conf  the configuration
   * @return coalescing reading PartitionReader
   */
  override def buildBaseColumnarReaderForCoalescing(
      files: Array[PartitionedFile],
      conf: Configuration): PartitionReader[ColumnarBatch] = {
    val poolConf = poolConfBuilder.build()
    val clippedBlocks = ArrayBuffer[ParquetSingleDataBlockMeta]()

    metrics.getOrElse(FILTER_TIME, NoopMetric).ns {
      metrics.getOrElse(SCAN_TIME, NoopMetric).ns {
        val metaAndFilesArr = readBlockMetasForCoalescing(files, conf, poolConf)
        metaAndFilesArr.foreach { metaAndFile =>
          val singleFileInfo = metaAndFile.meta
          clippedBlocks ++= singleFileInfo.blocks.map(block =>
            ParquetSingleDataBlockMeta(
              singleFileInfo.filePath,
              ParquetDataBlock(block, compressCfg),
              metaAndFile.file.partitionValues,
              ParquetSchemaWrapper(singleFileInfo.schema),
              singleFileInfo.readSchema,
              new ParquetExtraInfo(singleFileInfo.dateRebaseMode,
                singleFileInfo.timestampRebaseMode,
                singleFileInfo.hasInt96Timestamps)))
        }
      }
    }

    new MultiFileParquetPartitionReader(fileIO, conf, files, clippedBlocks.toSeq, isCaseSensitive,
      debugDumpPrefix, debugDumpAlways, maxReadBatchSizeRows, maxReadBatchSizeBytes,
      targetBatchSizeBytes, maxGpuColumnSizeBytes,
      useChunkedReader, maxChunkedReaderMemoryUsageSizeBytes, compressCfg,
      metrics, partitionSchema, poolConf, ignoreMissingFiles, ignoreCorruptFiles,
      readUseFieldId)
  }

  /**
   * File format short name used for logging and other things to uniquely identity
   * which file format is being used.
   *
   * @return the file format short name
   */
  override final def getFileFormatShortName: String = "Parquet"

}

case class GpuParquetMultiFilePartitionReaderFactory(
    @transient sqlConf: SQLConf,
    broadcastedConf: Broadcast[SerializableConfiguration],
    dataSchema: StructType,
    readDataSchema: StructType,
    partitionSchema: StructType,
    filters: Array[Filter],
    @transient rapidsConf: RapidsConf,
    poolConfBuilder: ThreadPoolConfBuilder,
    metrics: Map[String, GpuMetric],
    queryUsesInputFile: Boolean)
  extends AbstractGpuParquetMultiFilePartitionReaderFactory(sqlConf, broadcastedConf,
    dataSchema, readDataSchema, partitionSchema, filters, rapidsConf, poolConfBuilder,
    metrics, queryUsesInputFile) {

  override protected def createBaseMultiFileCloudReader(
      fileIO: RapidsFileIO,
      conf: Configuration,
      files: Array[PartitionedFile],
      filterFunc: PartitionedFile => ParquetFileInfoWithBlockMeta,
      isSchemaCaseSensitive: Boolean,
      debugDumpPrefix: Option[String],
      debugDumpAlways: Boolean,
      maxReadBatchSizeRows: Integer,
      maxReadBatchSizeBytes: Long,
      targetBatchSizeBytes: Long,
      maxGpuColumnSizeBytes: Long,
      useChunkedReader: Boolean,
      maxChunkedReaderMemoryUsageSizeBytes: Long,
      compressCfg: CpuCompressionConfig,
      execMetrics: Map[String, GpuMetric],
      partitionSchema: StructType,
      poolConf: ThreadPoolConf,
      maxNumFileProcessed: Int,
      ignoreMissingFiles: Boolean,
      ignoreCorruptFiles: Boolean,
      useFieldId: Boolean,
      queryUsesInputFile: Boolean,
      keepReadsInOrder: Boolean,
      combineConf: CombineConf
  ): AbstractMultiFileCloudParquetPartitionReader = {
    new MultiFileCloudParquetPartitionReader(
      fileIO,
      conf,
      files,
      filterFunc,
      isSchemaCaseSensitive,
      debugDumpPrefix,
      debugDumpAlways,
      maxReadBatchSizeRows,
      maxReadBatchSizeBytes,
      targetBatchSizeBytes,
      maxGpuColumnSizeBytes,
      useChunkedReader,
      maxChunkedReaderMemoryUsageSizeBytes,
      compressCfg,
      execMetrics,
      partitionSchema,
      poolConf,
      maxNumFileProcessed,
      ignoreMissingFiles,
      ignoreCorruptFiles,
      useFieldId,
      queryUsesInputFile,
      keepReadsInOrder,
      combineConf)
  }
}

abstract class GpuParquetPartitionReaderFactoryBase(
    sqlConf: SQLConf,
    broadcastedConf: Broadcast[SerializableConfiguration],
    dataSchema: StructType,
    readDataSchema: StructType,
    partitionSchema: StructType,
    rapidsConf: RapidsConf,
    metrics: Map[String, GpuMetric],
    params: Map[String, String])
  extends ShimFilePartitionReaderFactory(params) {

  // we make sure we mark this as a transient lazy val, so we only materialize it
  // from a task when we need to create the fileIO instance. This stops a regression
  // when we materialize the hadoop conf eagerly, see:
  // https://github.com/NVIDIA/spark-rapids/issues/13353
  @transient protected lazy val fileIO = new HadoopFileIO(broadcastedConf.value.value)
  protected val isCaseSensitive = sqlConf.caseSensitiveAnalysis
  protected val debugDumpPrefix = rapidsConf.parquetDebugDumpPrefix
  protected val debugDumpAlways = rapidsConf.parquetDebugDumpAlways
  protected val maxReadBatchSizeRows = rapidsConf.maxReadBatchSizeRows
  protected val maxReadBatchSizeBytes = rapidsConf.maxReadBatchSizeBytes
  protected val targetSizeBytes = rapidsConf.gpuTargetBatchSizeBytes
  protected val maxGpuColumnSizeBytes = rapidsConf.maxGpuColumnSizeBytes
  protected val useChunkedReader = rapidsConf.chunkedReaderEnabled
  protected val maxChunkedReaderMemoryUsageSizeBytes =
    if(rapidsConf.limitChunkedReaderMemoryUsage) {
      (rapidsConf.chunkedReaderMemoryUsageRatio * targetSizeBytes).toLong
    } else {
      0L
    }
  protected val filterHandler = GpuParquetFileFilterHandler(sqlConf, metrics)
  protected val readUseFieldId = ParquetSchemaClipShims.useFieldId(sqlConf)
  protected val footerReadType = GpuParquetScan.footerReaderHeuristic(
    rapidsConf.parquetReaderFooterType, dataSchema, readDataSchema, readUseFieldId)
  protected val compressCfg = CpuCompressionConfig.forParquet(rapidsConf)

  override def supportColumnarReads(partition: InputPartition): Boolean = true

  override def buildReader(partitionedFile: PartitionedFile): PartitionReader[InternalRow] = {
    throw new IllegalStateException("GPU column parser called to read rows")
  }

  override def buildColumnarReader(
      partitionedFile: PartitionedFile): PartitionReader[ColumnarBatch] = {
    val reader = new PartitionReaderWithBytesRead(buildBaseColumnarParquetReader(partitionedFile))
    ColumnarPartitionReaderWithPartitionValues.newReader(partitionedFile, reader, partitionSchema,
      maxGpuColumnSizeBytes)
  }

  protected def buildBaseColumnarParquetReader(file: PartitionedFile)
  : PartitionReader[ColumnarBatch]
}

case class GpuParquetPartitionReaderFactory(
    @transient sqlConf: SQLConf,
    broadcastedConf: Broadcast[SerializableConfiguration],
    dataSchema: StructType,
    readDataSchema: StructType,
    partitionSchema: StructType,
    filters: Array[Filter],
    @transient rapidsConf: RapidsConf,
    metrics: Map[String, GpuMetric],
    @transient params: Map[String, String])
  extends GpuParquetPartitionReaderFactoryBase(
    sqlConf, broadcastedConf, dataSchema, readDataSchema, partitionSchema,
    rapidsConf, metrics, params) with Logging {

  override protected def buildBaseColumnarParquetReader(
      file: PartitionedFile): PartitionReader[ColumnarBatch] = {
    // we need to copy the Hadoop Configuration because filter push down can mutate it,
    // which can affect other tasks.
    val conf = new Configuration(broadcastedConf.value.value)
    val startTime = System.nanoTime()
    val singleFileInfo = filterHandler.filterBlocks(fileIO, footerReadType, file, conf, filters,
      readDataSchema)
    metrics.get(FILTER_TIME).foreach {
      _ += (System.nanoTime() - startTime)
    }
    new ParquetPartitionReader(fileIO, conf, file, singleFileInfo.filePath, singleFileInfo.blocks,
      singleFileInfo.schema, isCaseSensitive, readDataSchema, debugDumpPrefix, debugDumpAlways,
      maxReadBatchSizeRows, maxReadBatchSizeBytes, targetSizeBytes,
      useChunkedReader, maxChunkedReaderMemoryUsageSizeBytes, compressCfg,
      metrics, singleFileInfo.dateRebaseMode,
      singleFileInfo.timestampRebaseMode, singleFileInfo.hasInt96Timestamps, readUseFieldId)
  }
}

case class CpuCompressionConfig(
    decompressSnappyCpu: Boolean,
    decompressZstdCpu: Boolean) {
  val decompressAnyCpu: Boolean = decompressSnappyCpu || decompressZstdCpu
}

object CpuCompressionConfig {
  def forParquet(conf: RapidsConf): CpuCompressionConfig = {
    val cpuEnable = conf.parquetDecompressCpu
    CpuCompressionConfig(
      decompressSnappyCpu = cpuEnable && conf.parquetDecompressCpuSnappy,
      decompressZstdCpu = cpuEnable && conf.parquetDecompressCpuZstd)
  }

  def disabled(): CpuCompressionConfig = CpuCompressionConfig(false, false)
}

trait ParquetPartitionReaderBase extends Logging with ScanWithMetrics
    with MultiFileReaderFunctions {
  // the size of Parquet magic (at start+end) and footer length values
  val PARQUET_META_SIZE: Long = 4 + 4 + 4

  def fileIO: RapidsFileIO
  // Configuration

  def conf: Configuration
  def execMetrics: Map[String, GpuMetric]

  def isSchemaCaseSensitive: Boolean

  def compressCfg: CpuCompressionConfig

  val copyBufferSize = conf.getInt("parquet.read.allocation.size", 8 * 1024 * 1024)

  def checkIfNeedToSplitBlocks(currentDateRebaseMode: DateTimeRebaseMode,
      nextDateRebaseMode: DateTimeRebaseMode,
      currentTimestampRebaseMode: DateTimeRebaseMode,
      nextTimestampRebaseMode: DateTimeRebaseMode,
      currentSchema: SchemaBase,
      nextSchema: SchemaBase,
      currentFilePath: String,
      nextFilePath: String): Boolean = {
    // We need to ensure all files we are going to combine have the same datetime
    // rebase mode.
    if (currentDateRebaseMode != nextDateRebaseMode &&
      currentTimestampRebaseMode != nextTimestampRebaseMode) {
      logInfo(s"datetime rebase mode for the next file ${nextFilePath} is different " +
        s"then current file ${currentFilePath}, splitting into another batch.")
      return true
    }

    if (!currentSchema.equals(nextSchema)) {
      logInfo(s"File schema for the next file ${nextFilePath}" +
        s" schema ${nextSchema} doesn't match current ${currentFilePath}" +
        s" schema ${currentSchema}, splitting it into another batch!")
      return true
    }
    false
  }

  @scala.annotation.nowarn(
    "msg=constructor NullOutputStream in class NullOutputStream is deprecated"
  )
  protected def calculateParquetFooterSize(
      currentChunkedBlocks: Seq[BlockMetaData],
      schema: MessageType): Long = {
    // Calculate size of the footer metadata.
    // This uses the column metadata from the original file, but that should
    // always be at least as big as the updated metadata in the output.
    val out = new CountingOutputStream(new NullOutputStream)
    writeFooter(out, currentChunkedBlocks, schema)
    out.getByteCount
  }

  /**
   * Calculate an amount of extra memory if we are combining multiple files together.
   * We want to add extra memory because the ColumnChunks saved in the footer have 2 fields
   * file_offset and data_page_offset that get much larger when we are combining files.
   * Here we estimate that by taking the number of columns * number of blocks which should be
   * the number of column chunks and then saying there are 2 fields that could be larger and
   * assume max size of those would be 8 bytes worst case. So we probably allocate to much here
   * but it shouldn't be by a huge amount and its better then having to realloc and copy.
   *
   * @param numCols the number of columns
   * @param numBlocks the total number of blocks to be combined
   * @return amount of extra memory to allocate
   */
  def calculateExtraMemoryForParquetFooter(numCols: Int, numBlocks: Int): Int = {
    val numColumnChunks = numCols * numBlocks
    numColumnChunks * 2 * 8
  }

  protected def calculateParquetOutputSize(
      currentChunkedBlocks: Seq[BlockMetaData],
      schema: MessageType): Long = {
    // start with the size of Parquet magic (at start+end) and footer length values
    val headerSize: Long = PARQUET_META_SIZE
    val blocksSize = ParquetPartitionReader.computeOutputSize(currentChunkedBlocks, compressCfg)
    val footerSize = calculateParquetFooterSize(currentChunkedBlocks, schema)
    // There are always cases where the size of the offsets might change.
    val numCols = currentChunkedBlocks.head.getColumns().size()
    val extraMemory = calculateExtraMemoryForParquetFooter(numCols, currentChunkedBlocks.size)
    headerSize + blocksSize + footerSize + extraMemory
  }

  protected def writeFooter(
      out: OutputStream,
      blocks: Seq[BlockMetaData],
      schema: MessageType): Unit = {
    val fileMeta = new FileMetaData(schema, Collections.emptyMap[String, String],
      ParquetPartitionReader.PARQUET_CREATOR)
    val metadataConverter = new ParquetMetadataConverter
    val footer = new ParquetMetadata(fileMeta, blocks.asJava)
    val meta = metadataConverter.toParquetMetadata(ParquetPartitionReader.PARQUET_VERSION, footer)
    org.apache.parquet.format.Util.writeFileMetaData(meta, out)
  }

  /**
   * Computes new block metadata to reflect where the blocks and columns will appear in the
   * computed Parquet file.
   *
   * @param blocks block metadata from the original file(s) that will appear in the computed file
   * @param realStartOffset starting file offset of the first block
   * @return updated block metadata
   */
  @scala.annotation.nowarn(
    "msg=method getPath in class ColumnChunkMetaData is deprecated"
  )
  protected def computeBlockMetaData(
      blocks: Seq[BlockMetaData],
      realStartOffset: Long): Seq[BlockMetaData] = {
    val outputBlocks = new ArrayBuffer[BlockMetaData](blocks.length)
    var totalBytesToCopy = 0L
    blocks.foreach { block =>
      val columns = block.getColumns.asScala
      val outputColumns = new ArrayBuffer[ColumnChunkMetaData](columns.length)
      columns.foreach { column =>
        // update column metadata to reflect new position in the output file
        val startPosCol = column.getStartingPos
        val offsetAdjustment = realStartOffset + totalBytesToCopy - startPosCol
        val newDictOffset = if (column.getDictionaryPageOffset > 0) {
          column.getDictionaryPageOffset + offsetAdjustment
        } else {
          0
        }
        val columnSize = column.getTotalSize
        //noinspection ScalaDeprecation
        outputColumns += ColumnChunkMetaData.get(
          column.getPath,
          column.getPrimitiveType,
          column.getCodec,
          column.getEncodingStats,
          column.getEncodings,
          column.getStatistics,
          column.getStartingPos + offsetAdjustment,
          newDictOffset,
          column.getValueCount,
          columnSize,
          column.getTotalUncompressedSize)
        totalBytesToCopy += columnSize
      }
      outputBlocks += GpuParquetUtils.newBlockMeta(block, outputColumns.toSeq)
    }
    outputBlocks.toSeq
  }

  /**
   * Copies the data corresponding to the clipped blocks in the original file and compute the
   * block metadata for the output. The output blocks will contain the same column chunk
   * metadata but with the file offsets updated to reflect the new position of the column data
   * as written to the output.
   *
   * @param filePath the path to the Parquet file
   * @param out the output stream to receive the data
   * @param blocks block metadata from the original file that will appear in the computed file
   * @param realStartOffset starting file offset of the first block
   * @return updated block metadata corresponding to the output
   */
  protected def copyBlocksData(
      filePath: Path,
      out: HostMemoryOutputStream,
      blocks: Seq[BlockMetaData],
      realStartOffset: Long,
      metrics: Map[String, GpuMetric]): Seq[BlockMetaData] = {
    val startPos = out.getPos
    val inputFile = fileIO.newInputFile(filePath)
    val remoteItems = new ArrayBuffer[CopyRange](blocks.length)
    var totalBytesToCopy = 0L
    withResource(new ArrayBuffer[LocalCopy](blocks.length)) { localItems =>
      blocks.foreach { block =>
        block.getColumns.asScala.foreach { column =>
          val columnSize = column.getTotalSize
          val outputOffset = totalBytesToCopy + startPos
          val channel = FileCache.get.getDataRangeChannel(inputFile,
            column.getStartingPos, columnSize)
          if (channel.isDefined) {
            localItems += LocalCopy(channel.get, columnSize, outputOffset)
          } else {
            remoteItems += new CopyRange(column.getStartingPos, columnSize, outputOffset)
          }
          totalBytesToCopy += columnSize
        }
      }
      localItems.foreach { localItem =>
        copyLocal(localItem, out, metrics)
        localItem.close()
      }
    }
    copyRemoteBlocksData(remoteItems.toSeq, filePath,
      inputFile, out, metrics)
    // fixup output pos after blocks were copied possibly out of order
    out.seek(startPos + totalBytesToCopy)
    computeBlockMetaData(blocks, realStartOffset)
  }

  private class BufferedFileInput(
      fileIO: RapidsFileIO,
      filePath: Path,
      blocks: Seq[BlockMetaData],
      metrics: Map[String, GpuMetric]) extends InputStream {
    private[this] val in = fileIO.newInputFile(filePath).open()
    private[this] val buffer: Array[Byte] = new Array[Byte](copyBufferSize)
    private[this] var bufferSize: Int = 0
    private[this] var bufferFilePos: Long = in.getPos
    private[this] var bufferPos: Int = 0
    private[this] val columnIter = blocks.flatMap(_.getColumns.asScala).iterator
    private[this] var currentColumn: Option[ColumnChunkMetaData] = None
    private[this] val readTime: GpuMetric = metrics.getOrElse(READ_FS_TIME, NoopMetric)

    override def read(): Int = {
      while (bufferPos == bufferSize) {
        fillBuffer()
      }
      val result = buffer(bufferPos)
      bufferPos += 1
      result
    }

    override def read(b: Array[Byte]): Int = read(b, 0, b.length)

    override def read(dest: Array[Byte], off: Int, len: Int): Int = {
      var bytesLeft = len
      while (bytesLeft > 0) {
        if (bufferPos == bufferSize) {
          fillBuffer()
        }
        val numBytes = Math.min(bytesLeft, bufferSize - bufferPos)
        System.arraycopy(buffer, bufferPos, dest,  off + len - bytesLeft, numBytes)
        bufferPos += numBytes
        bytesLeft -= numBytes
      }
      len
    }

    def read(out: HostMemoryOutputStream, len: Long): Unit = {
      var bytesLeft = len
      while (bytesLeft > 0) {
        if (bufferPos == bufferSize) {
          fillBuffer()
        }
        // downcast is safe because bufferSize is an int
        val numBytes = Math.min(bytesLeft, bufferSize - bufferPos).toInt
        out.write(buffer, bufferPos, numBytes)
        bufferPos += numBytes
        bytesLeft -= numBytes
      }
    }

    def read(out: HostMemoryBuffer, len: Long): Unit = {
      var bytesLeft = len
      while (bytesLeft > 0) {
        if (bufferPos == bufferSize) {
          fillBuffer()
        }
        // downcast is safe because bufferSize is an int
        val numBytes = Math.min(bytesLeft, bufferSize - bufferPos).toInt
        out.setBytes(len - bytesLeft, buffer, bufferPos, numBytes)
        bufferPos += numBytes
        bytesLeft -= numBytes
      }
    }

    override def skip(n: Long): Long = {
      seek(getPos + n)
      n
    }

    def getPos: Long = bufferFilePos + bufferPos

    def seek(desiredPos: Long): Unit = {
      require(desiredPos >= getPos, "Only supports seeking forward")
      val posDiff = desiredPos - bufferFilePos
      if (posDiff >= 0 && posDiff < bufferSize) {
        bufferPos = posDiff.toInt
      } else {
        in.seek(desiredPos)
        bufferFilePos = desiredPos
        bufferSize = 0
        bufferPos = 0
      }
    }

    override def close(): Unit = {
      readTime.ns {
        in.close()
      }
    }

    private def fillBuffer(): Unit = {
      // TODO: Add FileCache support https://github.com/NVIDIA/spark-rapids/issues/11775
      var bytesToCopy = currentColumn.map { c =>
        Math.max(0, c.getStartingPos + c.getTotalSize - getPos)
      }.getOrElse(0L)
      var done = bytesToCopy >= buffer.length
      while (!done && columnIter.hasNext) {
        val column = columnIter.next()
        currentColumn = Some(column)
        done = if (getPos + bytesToCopy == column.getStartingPos) {
          bytesToCopy += column.getTotalSize
          bytesToCopy >= buffer.length
        } else {
          true
        }
      }
      if (bytesToCopy <= 0) {
        throw new EOFException("read beyond column data range")
      }
      bufferFilePos = in.getPos
      bufferPos = 0
      bufferSize = Math.min(bytesToCopy, buffer.length).toInt
      readTime.ns {
        IOUtils.readFully(in, buffer, 0, bufferSize)
      }
    }
  }

  /**
   * Copies the data corresponding to the clipped blocks in the original file and compute the
   * block metadata for the output. The output blocks will contain the same column chunk
   * metadata but with the file offsets updated to reflect the new position of the column data
   * as written to the output.
   *
   * @param filePath the path to the Parquet file
   * @param out the output stream to receive the data
   * @param blocks block metadata from the original file that will appear in the computed file
   * @param realStartOffset starting file offset of the first block
   * @return updated block metadata corresponding to the output
   */
  protected def copyAndUncompressBlocksData(
      filePath: Path,
      out: HostMemoryOutputStream,
      blocks: Seq[BlockMetaData],
      realStartOffset: Long,
      metrics: Map[String, GpuMetric],
      compressCfg: CpuCompressionConfig): Seq[BlockMetaData] = {
    val outStartPos = out.getPos
    val writeTime = metrics.getOrElse(WRITE_BUFFER_TIME, NoopMetric)
    withResource(new BufferedFileInput(fileIO, filePath, blocks, metrics)) { in =>
      val newBlocks = blocks.map { block =>
        val newColumns = block.getColumns.asScala.map { column =>
          var columnTotalSize = column.getTotalSize
          var columnCodec = column.getCodec
          val columnStartingPos = realStartOffset + out.getPos - outStartPos
          val columnDictOffset = if (column.getDictionaryPageOffset > 0) {
            column.getDictionaryPageOffset + columnStartingPos - column.getStartingPos
          } else {
            0
          }
          writeTime.ns {
            columnCodec match {
              case CompressionCodecName.SNAPPY if compressCfg.decompressSnappyCpu =>
                val columnStartPos = out.getPos
                decompressSnappy(in, out, column)
                columnCodec = CompressionCodecName.UNCOMPRESSED
                columnTotalSize = out.getPos - columnStartPos
              case CompressionCodecName.ZSTD if compressCfg.decompressZstdCpu =>
                val columnStartPos = out.getPos
                decompressZstd(in, out, column)
                columnCodec = CompressionCodecName.UNCOMPRESSED
                columnTotalSize = out.getPos - columnStartPos
              case _ =>
                in.seek(column.getStartingPos)
                in.read(out, columnTotalSize)
            }
          }
          ColumnChunkMetaData.get(
            column.getPath,
            column.getPrimitiveType,
            columnCodec,
            column.getEncodingStats,
            column.getEncodings,
            column.getStatistics,
            columnStartingPos,
            columnDictOffset,
            column.getValueCount,
            columnTotalSize,
            columnTotalSize)
        }
        GpuParquetUtils.newBlockMeta(block, newColumns.toSeq)
      }
      newBlocks
    }
  }

  private def decompressSnappy(
      in: BufferedFileInput,
      out: HostMemoryOutputStream,
      column: ColumnChunkMetaData): Unit = {
    val endPos = column.getStartingPos + column.getTotalSize
    in.seek(column.getStartingPos)
    var inData: Option[HostMemoryBuffer] = None
    try {
      while (in.getPos != endPos) {
        val pageHeader = Util.readPageHeader(in)
        val compressedSize = pageHeader.getCompressed_page_size
        val uncompressedSize = pageHeader.getUncompressed_page_size
        pageHeader.unsetCrc()
        pageHeader.setCompressed_page_size(uncompressedSize)
        Util.writePageHeader(pageHeader, out)
        if (inData.map(_.getLength).getOrElse(0L) < compressedSize) {
          inData.foreach(_.close())
          inData = Some(HostMemoryBuffer.allocate(compressedSize, false))
        }
        inData.foreach { compressedBuffer =>
          in.read(compressedBuffer, compressedSize)
          val bbIn = compressedBuffer.asByteBuffer(0, compressedSize)
          val bbOut = out.writeAsByteBuffer(uncompressedSize)
          Snappy.uncompress(bbIn, bbOut)
        }
      }
    } finally {
      inData.foreach(_.close())
    }
  }

  private def decompressZstd(
      in: BufferedFileInput,
      out: HostMemoryOutputStream,
      column: ColumnChunkMetaData): Unit = {
    val endPos = column.getStartingPos + column.getTotalSize
    in.seek(column.getStartingPos)
    var inData: Option[HostMemoryBuffer] = None
    try {
      withResource(new ZstdDecompressCtx()) { ctx =>
        while (in.getPos != endPos) {
          val pageHeader = Util.readPageHeader(in)
          val compressedSize = pageHeader.getCompressed_page_size
          val uncompressedSize = pageHeader.getUncompressed_page_size
          pageHeader.unsetCrc()
          pageHeader.setCompressed_page_size(uncompressedSize)
          Util.writePageHeader(pageHeader, out)
          if (inData.map(_.getLength).getOrElse(0L) < compressedSize) {
            inData.foreach(_.close())
            inData = Some(HostMemoryBuffer.allocate(compressedSize, false))
          }
          inData.foreach { compressedBuffer =>
            in.read(compressedBuffer, compressedSize)
            val bbIn = compressedBuffer.asByteBuffer(0, compressedSize)
            val bbOut = out.writeAsByteBuffer(uncompressedSize)
            ctx.decompress(bbOut, bbIn)
          }
        }
      }
    } finally {
      inData.foreach(_.close())
    }
  }

  private def copyRemoteBlocksData(
      remoteCopies: Seq[CopyRange],
      filePath: Path,
      inputFile: RapidsInputFile,
      out: HostMemoryOutputStream,
      metrics: Map[String, GpuMetric]): Long = {
    if (remoteCopies.isEmpty) {
      return 0L
    }

    val coalescedRanges = coalesceReads(remoteCopies)
    val scheme = filePath.toUri.getScheme
    if (scheme != null && scheme.startsWith("s3")) {
      GpuTaskMetrics.get.recordPerfioS3BackendOnce()
    }
    val totalBytesCopied = GpuParquetScan.readRangesToHostMemory(
      inputFile, out.buffer, coalescedRanges, metrics)

    // try to cache the remote ranges that were copied
    remoteCopies.foreach { range =>
      metrics.getOrElse(GpuMetric.FILECACHE_DATA_RANGE_MISSES, NoopMetric) += 1
      metrics.getOrElse(GpuMetric.FILECACHE_DATA_RANGE_MISSES_SIZE, NoopMetric) += range.getLength
      val cacheToken = FileCache.get.startDataRangeCache(
        inputFile, range.getInputOffset, range.getLength)
      // If we get a filecache token then we can complete the caching by providing the data.
      // If we do not get a token then we should not cache this data.
      cacheToken.foreach { token =>
        token.complete(out.buffer.slice(range.getOutputOffset, range.getLength))
      }
    }
    totalBytesCopied
  }

  private def coalesceReads(ranges: Seq[CopyRange]): Seq[CopyRange] = {
    val coalesced = new ArrayBuffer[CopyRange](ranges.length)
    var currentRange: CopyRange = null
    var currentRangeEnd = 0L

    def addCurrentRange(): Unit = {
      if (currentRange != null) {
        val rangeLength = currentRangeEnd - currentRange.getInputOffset
        if (rangeLength == currentRange.getLength) {
          coalesced += currentRange
        } else {
          coalesced += new CopyRange(
            currentRange.getInputOffset, rangeLength, currentRange.getOutputOffset)
        }
        currentRange = null
        currentRangeEnd = 0L
      }
    }

    ranges.foreach { c =>
      if (c.getInputOffset == currentRangeEnd) {
        currentRangeEnd += c.getLength
      } else {
        addCurrentRange()
        currentRange = c
        currentRangeEnd = c.getInputOffset + c.getLength
      }
    }
    addCurrentRange()
    coalesced.toSeq
  }

  private def copyLocal(
      item: LocalCopy,
      out: HostMemoryOutputStream,
      metrics: Map[String, GpuMetric]): Long = {
    metrics.getOrElse(GpuMetric.FILECACHE_DATA_RANGE_HITS, NoopMetric) += 1
    metrics.getOrElse(GpuMetric.FILECACHE_DATA_RANGE_HITS_SIZE, NoopMetric) += item.length
    metrics.getOrElse(GpuMetric.FILECACHE_DATA_RANGE_READ_TIME, NoopMetric).ns {
      out.seek(item.outputOffset)
      out.copyFromChannel(item.channel, item.length)
    }
    item.length
  }

  protected def readPartFile(
      blocks: Seq[BlockMetaData],
      clippedSchema: MessageType,
      filePath: Path): (SpillableHostBuffer, Seq[BlockMetaData]) = {
    NvtxRegistry.PARQUET_BUFFER_FILE_SPLIT {
      // Track the actual read buffer size, since some columns or partitions may be pruned
      execMetrics.get("readBufferSize").foreach { metric =>
        blocks.foreach { block =>
          block.getColumns.asScala.foreach { column =>
            metric += column.getTotalSize
          }
        }
      }

      val estTotalSize = calculateParquetOutputSize(blocks, clippedSchema)
      val outHostBuf = withRetryNoSplit[HostMemoryBuffer] {
        HostMemoryBuffer.allocate(estTotalSize)
      }
      closeOnExcept(outHostBuf) { hmb =>
        val out = new HostMemoryOutputStream(hmb)
        out.write(ParquetPartitionReader.PARQUET_MAGIC)
        val outputBlocks = if (compressCfg.decompressAnyCpu) {
          copyAndUncompressBlocksData(filePath, out, blocks, out.getPos, metrics, compressCfg)
        } else {
          copyBlocksData(filePath, out, blocks, out.getPos, metrics)
        }
        val footerPos = out.getPos
        writeFooter(out, outputBlocks, clippedSchema)
        BytesUtils.writeIntLittleEndian(out, (out.getPos - footerPos).toInt)
        out.write(ParquetPartitionReader.PARQUET_MAGIC)
        // check we didn't go over memory
        if (out.getPos > estTotalSize) {
          throw new QueryExecutionException(s"Calculated buffer size $estTotalSize is too " +
              s"small, actual written: ${out.getPos}")
        }
        (SpillableHostBuffer(hmb, out.getPos, SpillPriorities.ACTIVE_BATCHING_PRIORITY),
          outputBlocks)
      }
    }
  }

  protected def populateCurrentBlockChunk(
      blockIter: BufferedIterator[BlockMetaData],
      maxReadBatchSizeRows: Int,
      maxReadBatchSizeBytes: Long,
      readDataSchema: StructType): Seq[BlockMetaData] = {
    val currentChunk = new ArrayBuffer[BlockMetaData]
    var numRows: Long = 0
    var numBytes: Long = 0
    var numParquetBytes: Long = 0

    @tailrec
    def readNextBatch(): Unit = {
      if (blockIter.hasNext) {
        val peekedRowGroup = blockIter.head
        if (peekedRowGroup.getRowCount > Integer.MAX_VALUE) {
          throw new UnsupportedOperationException("Too many rows in split")
        }
        if (numRows == 0 || numRows + peekedRowGroup.getRowCount <= maxReadBatchSizeRows) {
          val estimatedBytes = GpuBatchUtils.estimateGpuMemory(readDataSchema,
            peekedRowGroup.getRowCount)
          if (numBytes == 0 || numBytes + estimatedBytes <= maxReadBatchSizeBytes) {
            currentChunk += blockIter.next()
            numRows += currentChunk.last.getRowCount
            numParquetBytes += currentChunk.last.getTotalByteSize
            numBytes += estimatedBytes
            readNextBatch()
          }
        }
      }
    }
    readNextBatch()
    logDebug(s"Loaded $numRows rows from Parquet. Parquet bytes read: $numParquetBytes. " +
      s"Estimated GPU bytes: $numBytes")
    currentChunk.toSeq
  }

  /**
   * Take case-sensitive into consideration when getting the data reading column names
   * before sending parquet-formatted buffer to cudf.
   * Also clips the column names if `useFieldId` is true.
   *
   * @param readDataSchema Spark schema to read
   * @param fileSchema the schema of the dumped parquet-formatted buffer, already removed unmatched
   *
   * @param isCaseSensitive if it is case sensitive
   * @param useFieldId if enabled `spark.sql.parquet.fieldId.read.enabled`
   * @return a sequence of tuple of column names following the order of readDataSchema
   */
  protected def toCudfColumnNames(
      readDataSchema: StructType,
      fileSchema: MessageType,
      isCaseSensitive: Boolean,
      useFieldId: Boolean): Seq[String] = {

    // map from field ID to the parquet column name
    val fieldIdToNameMap = ParquetSchemaClipShims.fieldIdToNameMap(useFieldId, fileSchema)

    // if use field id, clip the unused reading column names
    // e.g.:  reading schema is:
    //  StructType(
    //    StructField("mapped_c1", IntegerType, metadata={'parquet.field.id': 1}),
    //    StructField("mapped_c2", IntegerType, metadata={'parquet.field.id': 55}),
    //    StructField("c3", IntegerType))
    //  File schema is:
    //    message spark_schema {
    //      optional int32 c1 = 1 (field ID is 1),
    //      optional int32 c2 = 2 (field ID is 2),
    //      optional int32 c3,
    //    }
    //  ID = 55 not matched, returns ["c1", "c3"]

    // excludes unmatched columns
    val clippedReadFields = readDataSchema.fields.filter(f => !(useFieldId &&
        ParquetSchemaClipShims.hasFieldId(f) &&
        !fieldIdToNameMap.contains(ParquetSchemaClipShims.getFieldId(f))))

    if (!isCaseSensitive) {
      val fields = fileSchema.asGroupType().getFields.asScala.map(_.getName).toSet
      val m = CaseInsensitiveMap(fields.zip(fields).toMap)
      // The schemas may be different among parquet files, so some column names may not be existing
      // in the CaseInsensitiveMap. In that case, we just use the field name of readDataSchema
      //
      // For hive special case, the readDataSchema is lower case, we need to do
      // the case insensitive conversion
      // See https://github.com/NVIDIA/spark-rapids/pull/3982#issue-770410779
      clippedReadFields.map { f =>
        if (useFieldId && ParquetSchemaClipShims.hasFieldId(f)) {
          // find the parquet column name
          fieldIdToNameMap(ParquetSchemaClipShims.getFieldId(f))
        } else {
          m.get(f.name).getOrElse(f.name)
        }
      }
    } else {
      clippedReadFields.map { f =>
        if (useFieldId && ParquetSchemaClipShims.hasFieldId(f)) {
          fieldIdToNameMap(ParquetSchemaClipShims.getFieldId(f))
        } else {
          f.name
        }
      }
    }
  }

  def getParquetOptions(
      readDataSchema: StructType,
      clippedSchema: MessageType,
      useFieldId: Boolean): ParquetOptions = {
    val includeColumns = toCudfColumnNames(readDataSchema, clippedSchema,
      isSchemaCaseSensitive, useFieldId)
    ParquetOptions.builder()
        .withTimeUnit(DType.TIMESTAMP_MICROSECONDS)
        .includeColumn(includeColumns : _*)
        .build()
  }

  /** conversions used by multithreaded reader and coalescing reader */
  implicit def toBlockMetaData(block: DataBlockBase): BlockMetaData =
    block.asInstanceOf[ParquetDataBlock].dataBlock

  implicit def toDataBlockBase(blocks: Seq[BlockMetaData]): Seq[DataBlockBase] =
    blocks.map(b => ParquetDataBlock(b, compressCfg))

  implicit def toBlockMetaDataSeq(blocks: Seq[DataBlockBase]): Seq[BlockMetaData] =
    blocks.map(_.asInstanceOf[ParquetDataBlock].dataBlock)
}

// Parquet schema wrapper
case class ParquetSchemaWrapper(schema: MessageType) extends SchemaBase {
  override def isEmpty: Boolean = schema.getFields.isEmpty
}

// Parquet BlockMetaData wrapper
case class ParquetDataBlock(
    dataBlock: BlockMetaData,
    compressCfg: CpuCompressionConfig) extends DataBlockBase {
  override def getRowCount: Long = dataBlock.getRowCount
  override def getReadDataSize: Long = dataBlock.getTotalByteSize
  override def getBlockSize: Long = {
    ParquetPartitionReader.computeOutputSize(dataBlock, compressCfg)
  }
}

/** Parquet extra information containing rebase modes and whether there is int96 timestamp */
class ParquetExtraInfo(val dateRebaseMode: DateTimeRebaseMode,
    val timestampRebaseMode: DateTimeRebaseMode,
    val hasInt96Timestamps: Boolean) extends ExtraInfo

// contains meta about a single block in a file
case class ParquetSingleDataBlockMeta(
  filePath: Path,
  dataBlock: ParquetDataBlock,
  partitionValues: InternalRow,
  schema: ParquetSchemaWrapper,
  readSchema: StructType,
  extraInfo: ParquetExtraInfo) extends SingleDataBlockInfo

/**
 * Abstract base class for coalescing Parquet partition readers.
 *
 * Provides all the Parquet-specific block-copy machinery shared between the plain coalescing
 * reader and the Delta-aware coalescing reader: header/footer writing, block-split predicate,
 * batch runner, and the [[populateCurrentBlockChunk]] template method with the
 * [[augmentChunkMeta]] extension point.
 *
 * Both [[MultiFileParquetPartitionReader]] and the Delta-aware coalescing reader extend this
 * class. Neither extends the other.
 *
 * @param conf the Hadoop configuration
 * @param clippedBlocks the block metadata from the original Parquet file that has been clipped
 *                      to only contain the column chunks to be read
 * @param isSchemaCaseSensitive whether schema is case sensitive
 * @param maxReadBatchSizeRows soft limit on the maximum number of rows the reader reads per batch
 * @param maxReadBatchSizeBytes soft limit on the maximum number of bytes the reader reads per batch
 * @param targetBatchSizeBytes the target size of a batch
 * @param maxGpuColumnSizeBytes the maximum size of a GPU column
 * @param execMetrics metrics
 * @param partitionSchema Schema of partitions.
 * @param poolConf thread pool configuration.
 * @param ignoreMissingFiles Whether to ignore missing files
 * @param ignoreCorruptFiles Whether to ignore corrupt files
 */
abstract class MultiFileCoalescingParquetPartitionReaderBase(
    override val fileIO: RapidsFileIO,
    override val conf: Configuration,
    clippedBlocks: Seq[ParquetSingleDataBlockMeta],
    override val isSchemaCaseSensitive: Boolean,
    maxReadBatchSizeRows: Integer,
    maxReadBatchSizeBytes: Long,
    targetBatchSizeBytes: Long,
    maxGpuColumnSizeBytes: Long,
    override val compressCfg: CpuCompressionConfig,
    override val execMetrics: Map[String, GpuMetric],
    partitionSchema: StructType,
    poolConf: ThreadPoolConf,
    ignoreMissingFiles: Boolean,
    ignoreCorruptFiles: Boolean)
  extends MultiFileCoalescingPartitionReaderBase(conf, clippedBlocks,
    partitionSchema, maxReadBatchSizeRows, maxReadBatchSizeBytes, maxGpuColumnSizeBytes,
    poolConf, execMetrics)
  with ParquetPartitionReaderBase {

  // Some implicits to convert the base class to the sub-class and vice versa
  implicit def toMessageType(schema: SchemaBase): MessageType =
    schema.asInstanceOf[ParquetSchemaWrapper].schema

  implicit def ParquetSingleDataBlockMeta(in: ExtraInfo): ParquetExtraInfo =
    in.asInstanceOf[ParquetExtraInfo]

  // The runner to copy blocks to offset of HostMemoryBuffer.
  // Submit as an unbounded task since the buffer has already been allocated.
  // TODO: Make MultiFileCoalesceReader host memory bounded
  class ParquetCopyBlocksRunner(
      taskContext: TaskContext,
      file: Path,
      outhmb: HostMemoryBuffer,
      blocks: ArrayBuffer[DataBlockBase],
      offset: Long,
      compressCfg: CpuCompressionConfig)
    extends UnboundedAsyncRunner[(Seq[DataBlockBase], Long)] {

    override def callImpl(): (Seq[DataBlockBase], Long) = {
      TrampolineUtil.setTaskContext(taskContext)
      // Mark the async thread as a pool thread within the RetryFramework
      RmmSpark.poolThreadWorkingOnTask(taskContext.taskAttemptId())
      try {
        val startBytesRead = fileSystemBytesRead()
        val outputBlocks = withResource(outhmb) { _ =>
          withResource(new HostMemoryOutputStream(outhmb)) { out =>
            if (compressCfg.decompressAnyCpu) {
              copyAndUncompressBlocksData(file, out, blocks.toSeq, offset, metrics, compressCfg)
            } else {
              copyBlocksData(file, out, blocks.toSeq, offset, metrics)
            }
          }
        }
        val bytesRead = fileSystemBytesRead() - startBytesRead
        (outputBlocks, bytesRead)
      } catch {
        case e: FileNotFoundException if ignoreMissingFiles =>
          logWarning(s"Skipped missing file: ${file.toString}", e)
          (Seq.empty, 0)
        // Throw FileNotFoundException even if `ignoreCorruptFiles` is true
        case e: FileNotFoundException if !ignoreMissingFiles => throw e
        case e @ (_: RuntimeException | _: IOException) if ignoreCorruptFiles =>
          logWarning(
            s"Skipped the rest of the content in the corrupted file: ${file.toString}", e)
          // It leave the empty hole for the re-composed parquet file if we skip
          // the corrupted file. But it should be ok since there is no meta pointing to that "hole"
          (Seq.empty, 0)
      } finally {
        RmmSpark.poolThreadFinishedForTask(taskContext.taskAttemptId())
        TrampolineUtil.unsetTaskContext()
      }
    }
  }

  override def checkIfNeedToSplitDataBlock(currentBlockInfo: SingleDataBlockInfo,
      nextBlockInfo: SingleDataBlockInfo): Boolean = {
    checkIfNeedToSplitBlocks(currentBlockInfo.extraInfo.dateRebaseMode,
      nextBlockInfo.extraInfo.dateRebaseMode,
      currentBlockInfo.extraInfo.timestampRebaseMode,
      nextBlockInfo.extraInfo.timestampRebaseMode,
      currentBlockInfo.schema,
      nextBlockInfo.schema,
      currentBlockInfo.filePath.toString,
      nextBlockInfo.filePath.toString
    )
  }

  override def calculateEstimatedBlocksOutputSize(batchContext: BatchContext): Long = {
    val allBlocks = batchContext.origChunkedBlocks.values.flatten.toSeq
    // Some Parquet versions sanity check the block metadata, and since the blocks could be from
    // multiple files they will not pass the checks as they are.
    val blockStartOffset = ParquetPartitionReader.PARQUET_MAGIC.length
    val updatedBlocks = computeBlockMetaData(allBlocks, blockStartOffset)
    calculateParquetOutputSize(updatedBlocks, batchContext.schema)
  }

  override def getBatchRunner(
      taskContext: TaskContext,
      file: Path,
      outhmb: HostMemoryBuffer,
      blocks: ArrayBuffer[DataBlockBase],
      offset: Long,
      batchContext: BatchContext): AsyncRunner[(Seq[DataBlockBase], Long)] = {
    // Track the actual read buffer size, since some columns or partitions may be pruned
    execMetrics.get("readBufferSize").foreach { metric =>
      blocks.foreach { block =>
        block.getColumns.asScala.foreach { column =>
          metric += column.getTotalSize
        }
      }
    }

    new ParquetCopyBlocksRunner(taskContext, file, outhmb, blocks, offset, compressCfg)
  }

  override final def getFileFormatShortName: String = "Parquet"

  protected var currentTargetBatchSize = targetBatchSizeBytes

  override final def startNewBufferRetry: Unit = {
    currentTargetBatchSize = targetBatchSizeBytes
  }

  override def writeFileHeader(buffer: HostMemoryBuffer, bContext: BatchContext): Long = {
    withResource(new HostMemoryOutputStream(buffer)) { out =>
      out.write(ParquetPartitionReader.PARQUET_MAGIC)
      out.getPos
    }
  }

  override def calculateFinalBlocksOutputSize(footerOffset: Long,
      blocks: collection.Seq[DataBlockBase], bContext: BatchContext): Long = {

    val actualFooterSize = calculateParquetFooterSize(blocks.toSeq, bContext.schema)
    // 4 + 4 is for writing size and the ending PARQUET_MAGIC.
    footerOffset + actualFooterSize + 4 + 4
  }

  override def writeFileFooter(buffer: HostMemoryBuffer, bufferSize: Long, footerOffset: Long,
      blocks: Seq[DataBlockBase], bContext: BatchContext): (HostMemoryBuffer, Long) = {

    val lenLeft = bufferSize - footerOffset

    val finalSize = closeOnExcept(buffer) { _ =>
      withResource(buffer.slice(footerOffset, lenLeft)) { finalizehmb =>
        withResource(new HostMemoryOutputStream(finalizehmb)) { footerOut =>
          writeFooter(footerOut, blocks, bContext.schema)
          BytesUtils.writeIntLittleEndian(footerOut, footerOut.getPos.toInt)
          footerOut.write(ParquetPartitionReader.PARQUET_MAGIC)
          footerOffset + footerOut.getPos
        }
      }
    }
    (buffer, finalSize)
  }

  /**
   * Template method: assembles the current block chunk via the base-class implementation, then
   * calls [[augmentChunkMeta]] so subclasses can augment the resulting [[CurrentChunkMeta]]
   * without duplicating the assembly loop.
   */
  final override def populateCurrentBlockChunk(): CurrentChunkMeta = {
    val meta = super.populateCurrentBlockChunk()
    augmentChunkMeta(meta)
  }

  /**
   * Extension point called after each batch is assembled by [[populateCurrentBlockChunk]].
   * Subclasses override this to inject additional per-batch metadata (e.g. DV entries) into
   * the returned [[CurrentChunkMeta]] without touching the assembly loop.
   * Default implementation returns the meta unchanged.
   */
  protected def augmentChunkMeta(meta: CurrentChunkMeta): CurrentChunkMeta = meta
}

/**
 * A PartitionReader that can read multiple Parquet files up to the certain size. It will
 * coalesce small files together and copy the block data in a separate thread pool to speed
 * up processing the small files before sending down to the GPU.
 *
 * Efficiently reading a Parquet split on the GPU requires re-constructing the Parquet file
 * in memory that contains just the column chunks that are needed. This avoids sending
 * unnecessary data to the GPU and saves GPU memory.
 *
 * @param conf the Hadoop configuration
 * @param splits the partitioned files to read
 * @param clippedBlocks the block metadata from the original Parquet file that has been clipped
 *                      to only contain the column chunks to be read
 * @param isSchemaCaseSensitive whether schema is case sensitive
 * @param debugDumpPrefix a path prefix to use for dumping the fabricated Parquet data
 * @param debugDumpAlways whether to debug dump always or only on errors
 * @param maxReadBatchSizeRows soft limit on the maximum number of rows the reader reads per batch
 * @param maxReadBatchSizeBytes soft limit on the maximum number of bytes the reader reads per batch
 * @param targetBatchSizeBytes the target size of a batch
 * @param maxGpuColumnSizeBytes the maximum size of a GPU column
 * @param useChunkedReader whether to read Parquet by chunks or read all at once
 * @param maxChunkedReaderMemoryUsageSizeBytes soft limit on the number of bytes of internal memory
 *                                             usage that the reader will use
 * @param execMetrics metrics
 * @param partitionSchema Schema of partitions.
 * @param poolConf thread pool configuration.
 * @param ignoreMissingFiles Whether to ignore missing files
 * @param ignoreCorruptFiles Whether to ignore corrupt files
 */
class MultiFileParquetPartitionReader(
    fileIO: RapidsFileIO,
    conf: Configuration,
    splits: Array[PartitionedFile],
    clippedBlocks: Seq[ParquetSingleDataBlockMeta],
    isSchemaCaseSensitive: Boolean,
    debugDumpPrefix: Option[String],
    debugDumpAlways: Boolean,
    maxReadBatchSizeRows: Integer,
    maxReadBatchSizeBytes: Long,
    targetBatchSizeBytes: Long,
    maxGpuColumnSizeBytes: Long,
    useChunkedReader: Boolean,
    maxChunkedReaderMemoryUsageSizeBytes: Long,
    compressCfg: CpuCompressionConfig,
    execMetrics: Map[String, GpuMetric],
    partitionSchema: StructType,
    poolConf: ThreadPoolConf,
    ignoreMissingFiles: Boolean,
    ignoreCorruptFiles: Boolean,
    useFieldId: Boolean)
  extends MultiFileCoalescingParquetPartitionReaderBase(fileIO, conf, clippedBlocks,
    isSchemaCaseSensitive, maxReadBatchSizeRows, maxReadBatchSizeBytes, targetBatchSizeBytes,
    maxGpuColumnSizeBytes, compressCfg, execMetrics, partitionSchema, poolConf,
    ignoreMissingFiles, ignoreCorruptFiles) {

  override def readBufferToTablesAndClose(dataBuffer: HostMemoryBuffer, dataSize: Long,
      clippedSchema: SchemaBase, readDataSchema: StructType,
      extraInfo: ExtraInfo): GpuDataProducer[Table] = {

    val parseOpts = getParquetOptions(readDataSchema, clippedSchema, useFieldId)

    // About to start using the GPU
    GpuSemaphore.acquireIfNecessary(TaskContext.get())

    MakeParquetTableProducer(useChunkedReader, maxChunkedReaderMemoryUsageSizeBytes,
      conf, currentTargetBatchSize, parseOpts,
      Array(dataBuffer), metrics,
      extraInfo.dateRebaseMode, extraInfo.timestampRebaseMode,
      extraInfo.hasInt96Timestamps, isSchemaCaseSensitive, useFieldId, readDataSchema,
      clippedSchema, splits, debugDumpPrefix, debugDumpAlways)
  }
}

/**
 * Trait to hold metadata only with no buffer.
 * This is used for the multi-file reader to track the metadata even when there is no data to read.
 */
trait HostMemoryEmptyMetaData extends HostMemoryBuffersWithMetaDataBase {
  def bufferSize: Long
  def dateRebaseMode: DateTimeRebaseMode
  def timestampRebaseMode: DateTimeRebaseMode
  def hasInt96Timestamps: Boolean
  def clippedSchema: MessageType
  def readSchema: StructType
  def numRows: Long
  override def memBuffersAndSizes: Array[SingleHMBAndMeta] =
    Array(SingleHMBAndMeta.empty(numRows))
}

/**
 * Trait to hold host memory buffers with metadata for the multi-file reader.
 */
trait HostMemoryBuffersWithMetaData extends HostMemoryBuffersWithMetaDataBase {
  def dateRebaseMode: DateTimeRebaseMode
  def timestampRebaseMode: DateTimeRebaseMode
  def hasInt96Timestamps: Boolean
  def clippedSchema: MessageType
  def readSchema: StructType

  /**
   * Consumes the first buffer in the [[memBuffersAndSizes]] array and updates the metadata
   * accordingly.
   *
   * @return a copy of this object with the first buffer consumed and the metadata updated
   */
  def consumeHeadBuffer(): HostMemoryBuffersWithMetaData
}

/**
 * A PartitionReader that can read multiple Parquet files in parallel. This is most efficient
 * running in a cloud environment where the I/O of reading is slow.
 *
 * Efficiently reading a Parquet split on the GPU requires re-constructing the Parquet file
 * in memory that contains just the column chunks that are needed. This avoids sending
 * unnecessary data to the GPU and saves GPU memory.
 *
 * @param conf the Hadoop configuration
 * @param files the partitioned files to read
 * @param filterFunc a function to filter the necessary blocks from a given file
 * @param isSchemaCaseSensitive whether schema is case sensitive
 * @param debugDumpPrefix a path prefix to use for dumping the fabricated Parquet data
 * @param debugDumpAlways whether to debug dump always or only on errors
 * @param maxReadBatchSizeRows soft limit on the maximum number of rows the reader reads per batch
 * @param maxReadBatchSizeBytes soft limit on the maximum number of bytes the reader reads per batch
 * @param targetBatchSizeBytes the target size of the batch
 * @param maxGpuColumnSizeBytes the maximum size of a GPU column
 * @param useChunkedReader whether to read Parquet by chunks or read all at once
 * @param maxChunkedReaderMemoryUsageSizeBytes soft limit on the number of bytes of internal memory
 *                                             usage that the reader will use
 * @param execMetrics metrics
 * @param partitionSchema Schema of partitions.
 * @param poolConf thread pool configuration
 * @param maxNumFileProcessed the maximum number of files to read on the CPU side and waiting to be
 *                            processed on the GPU. This affects the amount of host memory used.
 * @param ignoreMissingFiles Whether to ignore missing files
 * @param ignoreCorruptFiles Whether to ignore corrupt files
 * @param useFieldId Whether to use field id for column matching
 * @param queryUsesInputFile Whether the query requires the input file name functionality
 * @param keepReadsInOrder Whether to require the files to be read in the same order as Spark.
 *                         Defaults to true for formats that don't explicitly handle this.
 * @param combineConf configs relevant to combination
 */
abstract class AbstractMultiFileCloudParquetPartitionReader(
    override val fileIO: RapidsFileIO,
    override val conf: Configuration,
    files: Array[PartitionedFile],
    filterFunc: PartitionedFile => ParquetFileInfoWithBlockMeta,
    override val isSchemaCaseSensitive: Boolean,
    debugDumpPrefix: Option[String],
    debugDumpAlways: Boolean,
    maxReadBatchSizeRows: Integer,
    maxReadBatchSizeBytes: Long,
    targetBatchSizeBytes: Long,
    maxGpuColumnSizeBytes: Long,
    useChunkedReader: Boolean,
    maxChunkedReaderMemoryUsageSizeBytes: Long,
    override val compressCfg: CpuCompressionConfig,
    override val execMetrics: Map[String, GpuMetric],
    partitionSchema: StructType,
    poolConf: ThreadPoolConf,
    maxNumFileProcessed: Int,
    ignoreMissingFiles: Boolean,
    ignoreCorruptFiles: Boolean,
    useFieldId: Boolean,
    queryUsesInputFile: Boolean,
    keepReadsInOrder: Boolean,
    combineConf: CombineConf)
  extends MultiFileCloudPartitionReaderBase(conf,
    files, poolConf, maxNumFileProcessed, null,
    execMetrics, maxReadBatchSizeRows, maxReadBatchSizeBytes, ignoreCorruptFiles,
    keepReadsInOrder, combineConf)
  with ParquetPartitionReaderBase {

  // TODO: replace the config maxNumFileProcessed with the dynamic resource bounded controller,
  // after the ResourceBoundedThreadPool are supported for all readers.
  require(files.length <= maxNumFileProcessed,
    "maxNumFileProcessed should be NOT applied for MultiFileCloudParquetPartitionReader")

  def checkIfNeedToSplit(current: HostMemoryBuffersWithMetaData,
      next: HostMemoryBuffersWithMetaData): Boolean = {

    checkIfNeedToSplitBlocks(current.dateRebaseMode,
      next.dateRebaseMode,
      current.timestampRebaseMode,
      next.timestampRebaseMode,
      ParquetSchemaWrapper(current.clippedSchema),
      ParquetSchemaWrapper(next.clippedSchema),
      current.partitionedFile.filePath.toString(),
      next.partitionedFile.filePath.toString()
    )
  }

  override def canUseCombine: Boolean = {
    if (queryUsesInputFile) {
      logDebug("Query uses input file name, can't use combine mode")
      false
    } else {
      combineConf.combineThresholdSize > 0
    }
  }

  // assumes all these are ok to combine and have the same metadata for schema
  // and *RebaseMode and timestamp type settings
  private def doCombineHMBs(combinedMeta: CombinedMeta): HostMemoryBuffersWithMetaDataBase = {
    val toCombineHmbs = combinedMeta.toCombine.filterNot(_.isInstanceOf[HostMemoryEmptyMetaData])
    val metaToUse = combinedMeta.firstNonEmpty
    logDebug(s"Using Combine mode and actually combining, num files ${toCombineHmbs.size} " +
      s"files: ${toCombineHmbs.map(_.partitionedFile.filePath).mkString(",")}")
    val startCombineTime = System.currentTimeMillis()

    // since we know not all of them are empty and we know all these have the same schema since
    // we already separated, just use the clippedSchema from metadata
    val schemaToUse = metaToUse.clippedSchema
    val combined = closeOnExcept(new ArrayBuffer[HostMemoryBuffer]) { buffers =>
      var offset: Long = PARQUET_MAGIC.size
      val allOutputBlocks = new ArrayBuffer[BlockMetaData]()

      // zero-copy the data
      toCombineHmbs.map { hbWithMeta =>
        hbWithMeta.memBuffersAndSizes.map { hmbInfo =>
          val columnDataSize = hmbInfo.blockMeta.map { meta =>
            meta.getColumns.asScala.map(_.getTotalSize).sum
          }.sum
          if (columnDataSize > 0 && hmbInfo.hmbs.nonEmpty) {
            val bytesToSlice = if (buffers.isEmpty) {
              columnDataSize + PARQUET_MAGIC.size
            } else {
              columnDataSize
            }
            val sliceOffset = if (buffers.isEmpty) 0 else PARQUET_MAGIC.size
            require(hmbInfo.hmbs.length == 1)
            buffers += SpillableHostBuffer.sliceWithRetry(hmbInfo.hmbs.head, sliceOffset,
              bytesToSlice)
          }
          val outputBlocks = computeBlockMetaData(hmbInfo.blockMeta, offset)
          allOutputBlocks ++= outputBlocks
          offset += columnDataSize
          hmbInfo.close()
        }
      }
      if (buffers.isEmpty) {
        closeOnExcept(HostMemoryBuffer.allocate(PARQUET_MAGIC.size)) { hmb =>
          hmb.setBytes(0, PARQUET_MAGIC, 0, PARQUET_MAGIC.size);
          buffers += hmb
        }
      }
      // using all of the actual combined output blocks meta calculate what the footer size
      // will really be
      val actualFooterSize = calculateParquetFooterSize(allOutputBlocks.toSeq, schemaToUse)
      val footerBuf = HostMemoryBuffer.allocate(actualFooterSize + 8)
      buffers += footerBuf
      withResource(new HostMemoryOutputStream(footerBuf)) { footerOut =>
        writeFooter(footerOut, allOutputBlocks.toSeq, schemaToUse)
        BytesUtils.writeIntLittleEndian(footerOut, footerOut.getPos.toInt)
        footerOut.write(ParquetPartitionReader.PARQUET_MAGIC)
        offset += footerOut.getPos
      }
      val spHostBufs = buffers.map(b =>
        SpillableHostBuffer(b, b.getLength, SpillPriorities.ACTIVE_BATCHING_PRIORITY))
      val newHmbBufferInfo = SingleHMBAndMeta(spHostBufs.toArray, offset,
        combinedMeta.allPartValues.map(_._1).sum, Seq.empty)
      val newHmbMeta = newCombinedHMBWithMetaData(combinedMeta, newHmbBufferInfo, offset)
      // Combine the metrics from all the parts
      val filterTime = combinedMeta.toCombine.map(_.getFilterTime).sum
      val bufferTime = combinedMeta.toCombine.map(_.getBufferTime).sum
      newHmbMeta.setExecutionTime(filterTime, bufferTime)
      val scheduleTime = combinedMeta.toCombine.map(_.getScheduleTime).sum
      newHmbMeta.setScheduleTime(scheduleTime)
      // Combine the release callbacks from all the parts
      combinedMeta.toCombine.foreach { hmb =>
        hmb.combineReleaseCallbacks(newHmbMeta)
      }
      newHmbMeta
    }
    logDebug(s"Took ${(System.currentTimeMillis() - startCombineTime)} " +
      s"ms to do combine of ${toCombineHmbs.size} files, " +
      s"task id: ${TaskContext.get().taskAttemptId()}")
    combined
  }

  private def computeCombineHMBMeta(
      input: Array[HostMemoryBuffersWithMetaDataBase]): (CombinedMeta, CombinedEmptyMeta) = {
    val allPartValues = new ArrayBuffer[(Long, InternalRow)]()
    var allEmpty = true
    val emptyMetas: ArrayBuffer[HostMemoryEmptyMetaData] = ArrayBuffer.empty
    var emptyNumRows = 0L
    var emptyBufferSize = 0L
    var emptyTotalBytesRead = 0L
    val toCombine = ArrayBuffer[HostMemoryBuffersWithMetaDataBase]()
    // this is only used when keepReadsInOrder is false
    val leftOversWhenNotKeepReadsInOrder = ArrayBuffer[HostMemoryBuffersWithMetaDataBase]()
    var firstNonEmpty: HostMemoryBuffersWithMetaData = null
    var numCombined: Int = 0
    var needsSplit = false
    var iterLoc = 0
    // iterating through this to handle the case we want to keep the files
    // in the same order as Spark
    while (!needsSplit && iterLoc < input.size) {
      val result = input(iterLoc)
      result match {
        case emptyHMData: HostMemoryEmptyMetaData =>
          emptyMetas += emptyHMData
          val totalNumRows = computeNumRowsAlive(result.memBuffersAndSizes.map(_.numRows).sum,
            emptyHMData)
          val partValues = result.partitionedFile.partitionValues
          allPartValues.append((totalNumRows, partValues))
          emptyBufferSize += emptyHMData.bufferSize
          emptyNumRows += emptyHMData.numRows
          emptyTotalBytesRead += emptyHMData.bytesRead
          toCombine += emptyHMData
          numCombined += 1
        case hmWithData: HostMemoryBuffersWithMetaData =>
          allEmpty = false
          if (firstNonEmpty != null && checkIfNeedToSplit(firstNonEmpty, hmWithData)) {
            // if we need to keep the same order as Spark we just stop here and put rest in
            // leftOverFiles, but if we don't then continue so we combine as much as possible
            if (keepReadsInOrder) {
              needsSplit = true
              combineLeftOverFiles = Some(input.drop(numCombined))
            } else {
              leftOversWhenNotKeepReadsInOrder += hmWithData
            }
          } else {
            if (firstNonEmpty == null) {
              firstNonEmpty = hmWithData
            }
            numCombined += 1
            toCombine += hmWithData
            val partValues = hmWithData.partitionedFile.partitionValues
            val totalNumRows = computeNumRowsAlive(hmWithData.memBuffersAndSizes.map(_.numRows).sum,
              hmWithData)
            allPartValues.append((totalNumRows, partValues))
          }
        case _ => throw new RuntimeException("Unknown HostMemoryBuffersWithMetaDataBase")
      }
      iterLoc += 1
    }
    if (!keepReadsInOrder && leftOversWhenNotKeepReadsInOrder.nonEmpty) {
      combineLeftOverFiles = Some(leftOversWhenNotKeepReadsInOrder.toArray)
    }
    val combinedMeta = CombinedMeta(allPartValues.toArray, toCombine.toArray, firstNonEmpty)
    val combinedEmptyMeta = CombinedEmptyMeta(emptyNumRows, emptyBufferSize,
      emptyTotalBytesRead, allEmpty, emptyMetas.toArray)
    (combinedMeta, combinedEmptyMeta)
  }

  override def combineHMBs(
      input: Array[HostMemoryBuffersWithMetaDataBase]): HostMemoryBuffersWithMetaDataBase = {
    if (input.size == 1) {
      input.head
    } else {
      val (combinedMeta, combinedEmptyMeta) = computeCombineHMBMeta(input)

      if (combinedEmptyMeta.allEmpty) {
        newCombinedHMEmptyMetadata(combinedEmptyMeta, combinedMeta)
      } else {
        doCombineHMBs(combinedMeta)
      }
    }
  }

  protected case class CombinedEmptyMeta(emptyNumRows: Long, emptyBufferSize: Long,
      emptyTotalBytesRead: Long, allEmpty: Boolean, emptyMetas: Array[HostMemoryEmptyMetaData]) {
    def metaForEmpty: HostMemoryEmptyMetaData = {
      // we might have multiple EmptyMetaData results. If some are due to ignoring
      // missing files and others are row counts, we want to make sure we
      // take the metadata information from the ones with row counts because
      // the ones from ignoring missing files has less information with it.
      emptyMetas.find(_.numRows > 0).orElse(emptyMetas.headOption).orNull
    }
  }

  /**
   * Metadata combined from an array of HostMemoryBuffersWithMetaDataBase
   *
   * @param allPartValues array of pairs of the number of rows and the partition value
   * @param toCombine the HostMemoryBuffersWithMetaDataBase to combine
   * @param firstNonEmpty the first HostMemoryBuffersWithMetaData with data
   */
  protected case class CombinedMeta(allPartValues: Array[(Long, InternalRow)],
      toCombine: Array[HostMemoryBuffersWithMetaDataBase],
      firstNonEmpty: HostMemoryBuffersWithMetaData)

  protected def newHMBWithMetaDataForChunks(
      partitionedFile: PartitionedFile,
      memBuffersAndSizes: Array[SingleHMBAndMeta],
      bytesRead: Long,
      fileBlockMeta: ParquetFileInfoWithBlockMeta
  ): HostMemoryBuffersWithMetaData

  protected def newCombinedHMBWithMetaData(
      combinedMeta: CombinedMeta,
      newHmbBufferInfo: SingleHMBAndMeta,
      offset: Long
  ): HostMemoryBuffersWithMetaData

  protected def newHMEmptyMetadataForChunks(
      partitionedFile: PartitionedFile,
      bufferSize: Long,
      bytesRead: Long,
      dateRebaseMode: DateTimeRebaseMode,
      timestampRebaseMode: DateTimeRebaseMode,
      hasInt96Timestamps: Boolean,
      clippedSchema: MessageType,
      readSchema: StructType,
      numRows: Long,
      blocks: collection.Seq[BlockMetaData]
  ): HostMemoryEmptyMetaData

  protected def newCombinedHMEmptyMetadata(emptyMeta: CombinedEmptyMeta,
      nonEmptyMeta: CombinedMeta): HostMemoryEmptyMetaData

  /**
   * Computes the number of rows alive in the output table. This should be always the same as
   * the given totalNumRows for Parquet files. But for some formats like Delta Lake, they can
   * have deletion vectors that represent the deleted rows in the table.
   */
  protected def computeNumRowsAlive(
      totalNumRows: Long,
      metadata: HostMemoryBuffersWithMetaDataBase
  ): Int

  private class ReadBatchRunner(
      file: PartitionedFile,
      filterFunc: PartitionedFile => ParquetFileInfoWithBlockMeta,
      taskContext: TaskContext) extends MemoryBoundedAsyncRunner[BufferInfo] with Logging {

    // Set TaskContext in terms of an AsyncRunner
    override def sparkTaskContext: Option[TaskContext] = Some(taskContext)

    // We don't know the actual file buffer size after column pruning and row-group filtering
    // before reading, so we just use the raw size as the resource requirement.
    override val requiredMemoryBytes: Long = file.length

    // BufferInfo will not be closed until it is transferred onto device side. So, builds
    // DecayReleaseResult with a release callback rather than FastReleaseResult.
    override protected def buildResult(resultData: BufferInfo,
        metrics: AsyncMetrics): RunnerResult = {
      val releaseCallback: () => Unit = () => {
        if (this.isHoldingResource) {
          this.withStateLock { _ =>
            this.releaseResourceCallback()
          }
        }
      }
      new DecayReleaseResult[BufferInfo](resultData, metrics, releaseCallback)
    }

    private var blockChunkIter: BufferedIterator[BlockMetaData] = null

    /**
     * Returns the host memory buffers and file metadata for the file processed.
     * If there was an error then the error field is set. If there were no blocks the buffer
     * is returned as null.  If there were no columns but rows (count() operation) then the
     * buffer is null and the size is the number of rows.
     *
     * Note that the TaskContext is not set in these threads and should not be used.
     */
    override def callImpl(): HostMemoryBuffersWithMetaDataBase = {
      TrampolineUtil.setTaskContext(taskContext)
      // Mark the async thread as a pool thread within the RetryFramework
      RmmSpark.poolThreadWorkingOnTask(taskContext.taskAttemptId())
      try {
        doRead()
      } catch {
        case e: FileNotFoundException if ignoreMissingFiles =>
          logWarning(s"Skipped missing file: ${file.filePath}", e)
          newHMEmptyMetadataForChunks(file, 0, 0,
            DateTimeRebaseLegacy, DateTimeRebaseLegacy,
            hasInt96Timestamps = false, null, null, 0, Seq.empty)
        // Throw FileNotFoundException even if `ignoreCorruptFiles` is true
        case e: FileNotFoundException if !ignoreMissingFiles => throw e
        case e @ (_: RuntimeException | _: IOException) if ignoreCorruptFiles =>
          logWarning(
            s"Skipped the rest of the content in the corrupted file: ${file.filePath}", e)
          newHMEmptyMetadataForChunks(file, 0, 0,
            DateTimeRebaseLegacy, DateTimeRebaseLegacy,
            hasInt96Timestamps = false, null, null, 0, Seq.empty)
      } finally {
        RmmSpark.poolThreadFinishedForTask(taskContext.taskAttemptId())
        TrampolineUtil.unsetTaskContext()
      }
    }

    private def doRead(): HostMemoryBuffersWithMetaDataBase = {
      val startingBytesRead = fileSystemBytesRead()
      val hostBuffers = new ArrayBuffer[SingleHMBAndMeta]
      var filterTime = 0L
      var bufferStartTime = 0L
      val result = try {
        val filterStartTime = System.nanoTime()
        val fileBlockMeta = filterFunc(file)
        filterTime = System.nanoTime() - filterStartTime
        bufferStartTime = System.nanoTime()
        if (fileBlockMeta.blocks.isEmpty) {
          val bytesRead = fileSystemBytesRead() - startingBytesRead
          // no blocks so return null buffer and size 0
          newHMEmptyMetadataForChunks(file, 0, bytesRead,
            fileBlockMeta.dateRebaseMode, fileBlockMeta.timestampRebaseMode,
            fileBlockMeta.hasInt96Timestamps, fileBlockMeta.schema, fileBlockMeta.readSchema, 0,
            Seq.empty)
        } else {
          blockChunkIter = fileBlockMeta.blocks.iterator.buffered
          if (isDone) {
            val bytesRead = fileSystemBytesRead() - startingBytesRead
            // got close before finishing
            newHMEmptyMetadataForChunks(file, 0, bytesRead,
              fileBlockMeta.dateRebaseMode, fileBlockMeta.timestampRebaseMode,
              fileBlockMeta.hasInt96Timestamps, fileBlockMeta.schema, fileBlockMeta.readSchema, 0,
              Seq.empty)
          } else {
            if (fileBlockMeta.schema.getFieldCount == 0) {
              val bytesRead = fileSystemBytesRead() - startingBytesRead
              val numRows = fileBlockMeta.blocks.map(_.getRowCount).sum.toInt
              newHMEmptyMetadataForChunks(file, 0, bytesRead,
                fileBlockMeta.dateRebaseMode, fileBlockMeta.timestampRebaseMode,
                fileBlockMeta.hasInt96Timestamps, fileBlockMeta.schema, fileBlockMeta.readSchema,
                numRows, fileBlockMeta.blocks)
            } else {
              val filePath = new Path(new URI(file.filePath.toString()))
              while (blockChunkIter.hasNext) {
                val blocksToRead = populateCurrentBlockChunk(blockChunkIter,
                  maxReadBatchSizeRows, maxReadBatchSizeBytes, fileBlockMeta.readSchema)
                val (dataBuffer, blockMeta) =
                  readPartFile(blocksToRead, fileBlockMeta.schema, filePath)
                val numRows = blocksToRead.map(_.getRowCount).sum.toInt
                hostBuffers += SingleHMBAndMeta(Array(dataBuffer), dataBuffer.length,
                  numRows, blockMeta)
              }
              val bytesRead = fileSystemBytesRead() - startingBytesRead
              if (isDone) {
                // got close before finishing
                hostBuffers.safeClose()
                newHMEmptyMetadataForChunks(file, 0, bytesRead,
                  fileBlockMeta.dateRebaseMode, fileBlockMeta.timestampRebaseMode,
                  fileBlockMeta.hasInt96Timestamps, fileBlockMeta.schema,
                  fileBlockMeta.readSchema, 0, Seq.empty)
              } else {
                newHMBWithMetaDataForChunks(file, hostBuffers.toArray,
                  bytesRead, fileBlockMeta)
              }
            }
          }
        }
      } catch {
        case e: Throwable =>
          hostBuffers.safeClose(e)
          throw e
      }
      val bufferTime = System.nanoTime() - bufferStartTime
      result.setExecutionTime(filterTime, bufferTime)
      result
    }
  }

  /**
   * File reading logic wrapped an async task which will be scheduled and executed in a
   * MemoryBoundedThreadExecutor. The larger the file, the lower priority of this task.
   *
   * @param tc      task context to use
   * @param file    file to be read
   * @param conf    configuration
   * @param filters push down filters
   * @return AsyncRunner[HostMemoryBuffersWithMetaDataBase]
   */
  override def getBatchRunner(
      tc: TaskContext,
      file: PartitionedFile,
      conf: Configuration,
      filters: Array[Filter]): AsyncRunner[HostMemoryBuffersWithMetaDataBase] = {
    new ReadBatchRunner(file, filterFunc, tc)
  }

  /**
   * File format short name used for logging and other things to uniquely identity
   * which file format is being used.
   *
   * @return the file format short name
   */
  override final def getFileFormatShortName: String = "Parquet"

  /**
   * Decode HostMemoryBuffers by GPU
   *
   * @param fileBufsAndMeta the file HostMemoryBuffer read from a PartitionedFile
   * @return Option[ColumnarBatch]
   */
  override def readBatches(fileBufsAndMeta: HostMemoryBuffersWithMetaDataBase):
  Iterator[ColumnarBatch] = fileBufsAndMeta match {
    case meta: HostMemoryEmptyMetaData =>
      // Not reading any data, but add in partition data if needed
      val rows = computeNumRowsAlive(meta.numRows, meta)
      val origBatch = if (meta.readSchema.isEmpty) {
        new ColumnarBatch(Array.empty, rows)
      } else {
        // Someone is going to process this data, even if it is just a row count
        GpuSemaphore.acquireIfNecessary(TaskContext.get())
        val nullColumns = meta.readSchema.fields.safeMap(f =>
          GpuColumnVector.fromNull(rows, f.dataType).asInstanceOf[SparkVector])
        new ColumnarBatch(nullColumns, rows)
      }

      // we have to add partition values here for this batch, we already verified that
      // its not different for all the blocks in this batch
      meta.allPartValues match {
        case Some(partRowsAndValues) =>
          val (rowsPerPart, partValues) = partRowsAndValues.unzip
          // rowsPerPart has been adjusted already to account only the alive rows.
          BatchWithPartitionDataUtils.addPartitionValuesToBatch(origBatch, rowsPerPart,
            partValues, partitionSchema, maxGpuColumnSizeBytes)
        case None =>
          BatchWithPartitionDataUtils.addSinglePartitionValueToBatch(origBatch,
            meta.partitionedFile.partitionValues, partitionSchema, maxGpuColumnSizeBytes)
      }

    case buffer: HostMemoryBuffersWithMetaData =>
      val memBuffersAndSize = buffer.memBuffersAndSizes
      val batchIter = try {
        readBufferToBatches(buffer)
      } finally {
        // If there are more buffers, we will release the resource after reading all batches,
        // in case of releasing the resource too early.
        if (memBuffersAndSize.length == 1) {
          // Release the virtual budget as closing the entire structure
          buffer.close()
        }
      }
      if (memBuffersAndSize.length > 1) {
        currentFileHostBuffers = Some(buffer.consumeHeadBuffer())
        buffer.combineReleaseCallbacks(currentFileHostBuffers.get)
      } else {
        currentFileHostBuffers = None
      }
      batchIter
    case _ => throw new RuntimeException("Wrong HostMemoryBuffersWithMetaData")
  }

  protected def readBufferToBatches(buffer: HostMemoryBuffersWithMetaData): Iterator[ColumnarBatch]
}

class MultiFileCloudParquetPartitionReader(
    override val fileIO: RapidsFileIO,
    override val conf: Configuration,
    files: Array[PartitionedFile],
    filterFunc: PartitionedFile => ParquetFileInfoWithBlockMeta,
    override val isSchemaCaseSensitive: Boolean,
    debugDumpPrefix: Option[String],
    debugDumpAlways: Boolean,
    maxReadBatchSizeRows: Integer,
    maxReadBatchSizeBytes: Long,
    targetBatchSizeBytes: Long,
    maxGpuColumnSizeBytes: Long,
    useChunkedReader: Boolean,
    maxChunkedReaderMemoryUsageSizeBytes: Long,
    override val compressCfg: CpuCompressionConfig,
    override val execMetrics: Map[String, GpuMetric],
    partitionSchema: StructType,
    poolConf: ThreadPoolConf,
    maxNumFileProcessed: Int,
    ignoreMissingFiles: Boolean,
    ignoreCorruptFiles: Boolean,
    useFieldId: Boolean,
    queryUsesInputFile: Boolean,
    keepReadsInOrder: Boolean,
    combineConf: CombineConf)
  extends AbstractMultiFileCloudParquetPartitionReader(fileIO, conf, files, filterFunc,
    isSchemaCaseSensitive, debugDumpPrefix, debugDumpAlways, maxReadBatchSizeRows,
    maxReadBatchSizeBytes, targetBatchSizeBytes, maxGpuColumnSizeBytes, useChunkedReader,
    maxChunkedReaderMemoryUsageSizeBytes, compressCfg, execMetrics, partitionSchema,
    poolConf, maxNumFileProcessed, ignoreMissingFiles, ignoreCorruptFiles, useFieldId,
    queryUsesInputFile, keepReadsInOrder, combineConf) {

  override protected def readBufferToBatches(buffer: HostMemoryBuffersWithMetaData)
  : Iterator[ColumnarBatch] = {
    val memBuffersAndSize = buffer.memBuffersAndSizes
    val hmbAndInfo = memBuffersAndSize.head

    val dateRebaseMode: DateTimeRebaseMode = buffer.dateRebaseMode
    val timestampRebaseMode: DateTimeRebaseMode = buffer.timestampRebaseMode
    val hasInt96Timestamps: Boolean = buffer.hasInt96Timestamps
    val clippedSchema: MessageType = buffer.clippedSchema
    val readDataSchema: StructType = buffer.readSchema
    val partedFile: PartitionedFile = buffer.partitionedFile
    val hostBuffers = hmbAndInfo.hmbs
    val allPartValues: Option[Array[(Long, InternalRow)]] = buffer.allPartValues

    val parseOpts = closeOnExcept(hostBuffers) { _ =>
      getParquetOptions(readDataSchema, clippedSchema, useFieldId)
    }
    val colTypes = readDataSchema.fields.map(f => f.dataType)

    withResource(hostBuffers) { _ =>
      RmmRapidsRetryIterator.withRetryNoSplit {
        // The MakeParquetTableProducer will close the input buffers
        val hostBufs = hostBuffers.safeMap(_.getDataHostBuffer())
        // Duplicate request is ok, and start to use the GPU just after the host
        // buffer is ready to not block CPU things.
        GpuSemaphore.acquireIfNecessary(TaskContext.get())

        val tableReader = MakeParquetTableProducer(useChunkedReader,
          maxChunkedReaderMemoryUsageSizeBytes,
          conf, targetBatchSizeBytes,
          parseOpts,
          hostBufs, metrics,
          dateRebaseMode, timestampRebaseMode, hasInt96Timestamps,
          isSchemaCaseSensitive, useFieldId, readDataSchema, clippedSchema, files,
          debugDumpPrefix, debugDumpAlways)

        val batchIter = CachedGpuBatchIterator(tableReader, colTypes)

        if (allPartValues.isDefined) {
          val allPartInternalRows = allPartValues.get.map(_._2)
          val rowsPerPartition = allPartValues.get.map(_._1)
          new GpuColumnarBatchWithPartitionValuesIterator(batchIter, allPartInternalRows,
            rowsPerPartition, partitionSchema, maxGpuColumnSizeBytes)
        } else {
          // this is a bit weird, we don't have number of rows when allPartValues isn't
          // filled in so can't use GpuColumnarBatchWithPartitionValuesIterator
          batchIter.flatMap { batch =>
            // we have to add partition values here for this batch, we already verified that
            // its not different for all the blocks in this batch
            BatchWithPartitionDataUtils.addSinglePartitionValueToBatch(batch,
              partedFile.partitionValues, partitionSchema, maxGpuColumnSizeBytes)
          }
        }
      }
    }
  }

  private case class ParquetHostMemoryEmptyMetaData(
      override val partitionedFile: PartitionedFile,
      bufferSize: Long,
      override val bytesRead: Long,
      dateRebaseMode: DateTimeRebaseMode,
      timestampRebaseMode: DateTimeRebaseMode,
      hasInt96Timestamps: Boolean,
      clippedSchema: MessageType,
      readSchema: StructType,
      numRows: Long,
      override val allPartValues: Option[Array[(Long, InternalRow)]] = None)
    extends HostMemoryEmptyMetaData {
  }

  private case class ParquetHostMemoryBuffersWithMetaData(
      override val partitionedFile: PartitionedFile,
      override val memBuffersAndSizes: Array[SingleHMBAndMeta],
      override val bytesRead: Long,
      dateRebaseMode: DateTimeRebaseMode,
      timestampRebaseMode: DateTimeRebaseMode,
      hasInt96Timestamps: Boolean,
      clippedSchema: MessageType,
      readSchema: StructType,
      override val allPartValues: Option[Array[(Long, InternalRow)]]
  ) extends HostMemoryBuffersWithMetaData {

    override def consumeHeadBuffer(): HostMemoryBuffersWithMetaData = {
      require(memBuffersAndSizes.nonEmpty,
        "consumeHeadBuffer called on HostMemoryBuffersWithMetaData with no buffers")
      val remainingBuffers = if (memBuffersAndSizes.length > 1) {
        memBuffersAndSizes.drop(1)
      } else {
        Array.empty[SingleHMBAndMeta]
      }
      this.copy(memBuffersAndSizes = remainingBuffers)
    }
  }

  override protected def newHMBWithMetaDataForChunks(
      partitionedFile: PartitionedFile,
      memBuffersAndSizes: Array[SingleHMBAndMeta],
      bytesRead: Long,
      fileBlockMeta: ParquetFileInfoWithBlockMeta
  ): HostMemoryBuffersWithMetaData = {
    ParquetHostMemoryBuffersWithMetaData(partitionedFile, memBuffersAndSizes,
      bytesRead, fileBlockMeta.dateRebaseMode,
      fileBlockMeta.timestampRebaseMode, fileBlockMeta.hasInt96Timestamps,
      fileBlockMeta.schema, fileBlockMeta.readSchema, None)
  }

  override protected def newCombinedHMBWithMetaData(combinedMeta: CombinedMeta,
      newHmbBufferInfo: SingleHMBAndMeta, offset: Long): HostMemoryBuffersWithMetaData = {
    val metaToUse = combinedMeta.firstNonEmpty
    ParquetHostMemoryBuffersWithMetaData(
      metaToUse.partitionedFile,
      Array(newHmbBufferInfo),
      offset,
      metaToUse.dateRebaseMode,
      metaToUse.timestampRebaseMode,
      metaToUse.hasInt96Timestamps,
      metaToUse.clippedSchema,
      metaToUse.readSchema,
      Some(combinedMeta.allPartValues))
  }

  override protected def newHMEmptyMetadataForChunks(
      partitionedFile: PartitionedFile,
      bufferSize: Long,
      bytesRead: Long,
      dateRebaseMode: DateTimeRebaseMode,
      timestampRebaseMode: DateTimeRebaseMode,
      hasInt96Timestamps: Boolean,
      clippedSchema: MessageType,
      readSchema: StructType,
      numRows: Long,
      blocks: collection.Seq[BlockMetaData]
  ): HostMemoryEmptyMetaData = {
    ParquetHostMemoryEmptyMetaData(partitionedFile, bufferSize, bytesRead,
      dateRebaseMode, timestampRebaseMode, hasInt96Timestamps,
      clippedSchema, readSchema, numRows)
  }

  override protected def newCombinedHMEmptyMetadata(emptyMeta: CombinedEmptyMeta,
      nonEmptyMeta: CombinedMeta): HostMemoryEmptyMetaData = {
    val metaForEmpty = emptyMeta.metaForEmpty
    ParquetHostMemoryEmptyMetaData(metaForEmpty.partitionedFile, // just pick one since not used
      emptyMeta.emptyBufferSize,
      emptyMeta.emptyTotalBytesRead,
      metaForEmpty.dateRebaseMode, // these shouldn't matter since data is empty
      metaForEmpty.timestampRebaseMode, // these shouldn't matter since data is empty
      metaForEmpty.hasInt96Timestamps, // these shouldn't matter since data is empty
      metaForEmpty.clippedSchema,
      metaForEmpty.readSchema,
      emptyMeta.emptyNumRows,
      Some(nonEmptyMeta.allPartValues)
    )
  }

  override protected def computeNumRowsAlive(
      totalNumRows: Long,
      metadata: HostMemoryBuffersWithMetaDataBase): Int = {
    Math.toIntExact(totalNumRows)
  }
}

object MakeParquetTableProducer extends Logging {
  def apply(
      useChunkedReader: Boolean,
      maxChunkedReaderMemoryUsageSizeBytes: Long,
      conf: Configuration,
      chunkSizeByteLimit: Long,
      opts: ParquetOptions,
      buffers: Array[HostMemoryBuffer],
      metrics : Map[String, GpuMetric],
      dateRebaseMode: DateTimeRebaseMode,
      timestampRebaseMode: DateTimeRebaseMode,
      hasInt96Timestamps: Boolean,
      isSchemaCaseSensitive: Boolean,
      useFieldId: Boolean,
      readDataSchema: StructType,
      clippedParquetSchema: MessageType,
      splits: Array[PartitionedFile],
      debugDumpPrefix: Option[String],
      debugDumpAlways: Boolean
  ): GpuDataProducer[Table] = {
    debugDumpPrefix.foreach { prefix =>
      if (debugDumpAlways) {
        val p = DumpUtils.dumpBuffer(conf, buffers, prefix, ".parquet")
        logWarning(s"Wrote data for ${splits.mkString(", ")} to $p")
      }
    }
    if (useChunkedReader) {
      ParquetTableReader(conf, chunkSizeByteLimit, maxChunkedReaderMemoryUsageSizeBytes,
        opts, buffers, metrics, dateRebaseMode, timestampRebaseMode,
        isSchemaCaseSensitive, useFieldId, readDataSchema, clippedParquetSchema,
        splits, debugDumpPrefix, debugDumpAlways)
    } else {
      val table = withResource(buffers) { _ =>
        try {
          RmmRapidsRetryIterator.withRetryNoSplit[Table] {
            NvtxIdWithMetrics(NvtxRegistry.PARQUET_DECODE, metrics(GPU_DECODE_TIME)) {
              Table.readParquet(opts, buffers:_*)
            }
          }
        } catch {
          case e: Exception =>
            val dumpMsg = debugDumpPrefix.map { prefix =>
              if (!debugDumpAlways) {
                val p = DumpUtils.dumpBuffer(conf, buffers, prefix, ".parquet")
                s", data dumped to $p"
              } else {
                ""
              }
            }.getOrElse("")
            throw new IOException(s"Error when processing ${splits.mkString("; ")}$dumpMsg", e)
        }
      }
      closeOnExcept(table) { _ =>
        GpuParquetScan.throwIfRebaseNeededInExceptionMode(table, dateRebaseMode,
          timestampRebaseMode)
        if (readDataSchema.length < table.getNumberOfColumns) {
          throw new QueryExecutionException(s"Expected ${readDataSchema.length} columns " +
            s"but read ${table.getNumberOfColumns} from ${splits.mkString("; ")}")
        }
      }
      metrics(NUM_OUTPUT_BATCHES) += 1
      val evolvedSchemaTable = ParquetSchemaUtils.evolveSchemaIfNeededAndClose(table,
        clippedParquetSchema, readDataSchema, isSchemaCaseSensitive, useFieldId)
      val outputTable = GpuParquetScan.rebaseDateTime(evolvedSchemaTable, dateRebaseMode,
        timestampRebaseMode)
      new SingleGpuDataProducer(outputTable)
    }
  }
}

/**
 * An interator that reads Parquet files in chunks and produce a [[Table]] for each chunk.
 */
trait ChunkedReader extends AutoCloseable {
  def hasNext: Boolean
  def next: Table
}

/**
 * A simple wrapper to adapt the JniParquetChunkedReader to the ChunkedReader interface.
 */
case class ParquetChunkedReader(delegate: JniParquetChunkedReader) extends ChunkedReader {
  override def hasNext: Boolean = delegate.hasNext
  override def next: Table = delegate.readChunk()
  override def close(): Unit = delegate.close()
}

abstract class AbstractParquetTableReader(
    conf: Configuration,
    chunkSizeByteLimit: Long,
    maxChunkedReaderMemoryUsageSizeBytes: Long,
    opts: ParquetOptions,
    buffers: Array[HostMemoryBuffer],
    metrics : Map[String, GpuMetric],
    dateRebaseMode: DateTimeRebaseMode,
    timestampRebaseMode: DateTimeRebaseMode,
    isSchemaCaseSensitive: Boolean,
    useFieldId: Boolean,
    readDataSchema: StructType,
    clippedParquetSchema: MessageType,
    splits: Array[PartitionedFile],
    debugDumpPrefix: Option[String],
    debugDumpAlways: Boolean) extends GpuDataProducer[Table] with Logging {

  protected val reader: ChunkedReader

  private[this] lazy val splitsString = splits.mkString("; ")

  // Should be lazy since the reader is not defined. Otherwise in practise, a native
  // chunk reader will be leaked.
  protected lazy val resources: Seq[AutoCloseable] = Seq(reader) ++ buffers

  override def hasNext: Boolean = reader.hasNext

  protected def postProcessChunk(chunk: Table): Table

  override def next: Table = {
    val table = NvtxIdWithMetrics(NvtxRegistry.PARQUET_DECODE, metrics(GPU_DECODE_TIME)) {
      try {
        reader.next
      } catch {
        case e: Exception =>
          val dumpMsg = debugDumpPrefix.map { prefix =>
            if (!debugDumpAlways) {
              val p = DumpUtils.dumpBuffer(conf, buffers, prefix, ".parquet")
              s", data dumped to $p"
            } else {
              ""
            }
          }.getOrElse("")
          throw new IOException(s"Error when processing $splitsString$dumpMsg", e)
      }
    }

    val postProcessedTable = postProcessChunk(table)

    closeOnExcept(postProcessedTable) { _ =>
      GpuParquetScan.throwIfRebaseNeededInExceptionMode(postProcessedTable, dateRebaseMode,
        timestampRebaseMode)
      if (readDataSchema.length < postProcessedTable.getNumberOfColumns) {
        throw new QueryExecutionException(s"Expected ${readDataSchema.length} columns " +
          s"but read ${postProcessedTable.getNumberOfColumns} from $splitsString")
      }
    }
    metrics(NUM_OUTPUT_BATCHES) += 1
    val evolvedSchemaTable = ParquetSchemaUtils.evolveSchemaIfNeededAndClose(postProcessedTable,
      clippedParquetSchema, readDataSchema, isSchemaCaseSensitive, useFieldId)
    GpuParquetScan.rebaseDateTime(evolvedSchemaTable, dateRebaseMode, timestampRebaseMode)
  }

  override def close(): Unit = {
    resources.safeClose()
  }
}

case class ParquetTableReader(
    conf: Configuration,
    chunkSizeByteLimit: Long,
    maxChunkedReaderMemoryUsageSizeBytes: Long,
    opts: ParquetOptions,
    buffers: Array[HostMemoryBuffer],
    metrics : Map[String, GpuMetric],
    dateRebaseMode: DateTimeRebaseMode,
    timestampRebaseMode: DateTimeRebaseMode,
    isSchemaCaseSensitive: Boolean,
    useFieldId: Boolean,
    readDataSchema: StructType,
    clippedParquetSchema: MessageType,
    splits: Array[PartitionedFile],
    debugDumpPrefix: Option[String],
    debugDumpAlways: Boolean
) extends AbstractParquetTableReader(conf, chunkSizeByteLimit, maxChunkedReaderMemoryUsageSizeBytes,
  opts, buffers, metrics, dateRebaseMode, timestampRebaseMode, isSchemaCaseSensitive, useFieldId,
  readDataSchema, clippedParquetSchema, splits, debugDumpPrefix, debugDumpAlways) {

  override protected val reader: ChunkedReader = ParquetChunkedReader(
    new JniParquetChunkedReader(chunkSizeByteLimit, maxChunkedReaderMemoryUsageSizeBytes,
      opts, buffers:_*)
  )

  override protected def postProcessChunk(chunk: Table): Table = chunk
}

/**
 * Base class for PartitionReaders that read a Parquet file split on the GPU.
 *
 * Efficiently reading a Parquet split on the GPU requires re-constructing the Parquet file
 * in memory that contains just the column chunks that are needed. This avoids sending
 * unnecessary data to the GPU and saves GPU memory.
 *
 * @param conf the Hadoop configuration
 * @param split the file split to read
 * @param filePath the path to the Parquet file
 * @param clippedBlocks the block metadata from the original Parquet file that has been clipped
 *                      to only contain the column chunks to be read
 * @param clippedParquetSchema the Parquet schema from the original Parquet file that has been
 *                             clipped to contain only the columns to be read
 * @param readDataSchema the Spark schema describing what will be read
 * @param debugDumpPrefix a path prefix to use for dumping the fabricated Parquet data or null
 * @param debugDumpAlways whether to debug dump always or only on errors
 */
abstract class AbstractParquetPartitionReader(
    override val fileIO: RapidsFileIO,
    override val conf: Configuration,
    split: PartitionedFile,
    filePath: Path,
    clippedBlocks: Iterable[BlockMetaData],
    clippedParquetSchema: MessageType,
    override val isSchemaCaseSensitive: Boolean,
    readDataSchema: StructType,
    debugDumpPrefix: Option[String],
    debugDumpAlways: Boolean,
    maxReadBatchSizeRows: Integer,
    maxReadBatchSizeBytes: Long,
    override val compressCfg: CpuCompressionConfig,
    override val execMetrics: Map[String, GpuMetric],
    useFieldId: Boolean) extends FilePartitionReaderBase(conf, execMetrics)
  with ParquetPartitionReaderBase {

  private val blockIterator:  BufferedIterator[BlockMetaData] = clippedBlocks.iterator.buffered

  override def next(): Boolean = {
    if (batchIter.hasNext) {
      return true
    }
    batchIter = EmptyGpuColumnarBatchIterator
    if (!isDone) {
      if (!blockIterator.hasNext) {
        isDone = true
      } else {
        batchIter = readBatches()
      }
    }

    // NOTE: At this point, the task may not have yet acquired the semaphore if `batch` is `None`.
    // We are not acquiring the semaphore here since this next() is getting called from
    // the `PartitionReaderIterator` which implements a standard iterator pattern, and
    // advertises `hasNext` as false when we return false here. No downstream tasks should
    // try to call next after `hasNext` returns false, and any task that produces some kind of
    // data when `hasNext` is false is responsible to get the semaphore themselves.
    batchIter.hasNext
  }

  private def readBatches(): Iterator[ColumnarBatch] = {
    NvtxRegistry.PARQUET_READ_BATCH {
      val currentChunkedBlocks = populateCurrentBlockChunk(blockIterator,
        maxReadBatchSizeRows, maxReadBatchSizeBytes, readDataSchema)
      if (clippedParquetSchema.getFieldCount == 0) {
        // not reading any data, so return a degenerate ColumnarBatch with the row count
        val numRows = computeNumRowsAlive(
          currentChunkedBlocks.map(_.getRowCount).sum, currentChunkedBlocks)
        if (numRows == 0) {
          EmptyGpuColumnarBatchIterator
        } else {
          // Someone is going to process this data, even if it is just a row count
          GpuSemaphore.acquireIfNecessary(TaskContext.get())
          val nullColumns = readDataSchema.safeMap(f =>
            GpuColumnVector.fromNull(numRows, f.dataType).asInstanceOf[SparkVector])
          new SingleGpuColumnarBatchIterator(new ColumnarBatch(nullColumns.toArray, numRows))
        }
      } else {
        val colTypes = readDataSchema.fields.map(f => f.dataType)
        val iter = if (currentChunkedBlocks.isEmpty) {
          CachedGpuBatchIterator(EmptyTableReader, colTypes)
        } else {
          val parseOpts = getParquetOptions(readDataSchema, clippedParquetSchema, useFieldId)
          val (dataBuffer, _) = metrics(BUFFER_TIME).ns {
            readPartFile(currentChunkedBlocks, clippedParquetSchema, filePath)
          }
          readBuffer(parseOpts, colTypes, currentChunkedBlocks, dataBuffer)
        }
        iter.map { batch =>
          logDebug(s"GPU batch size: ${GpuColumnVector.getTotalDeviceMemoryUsed(batch)} bytes")
          batch
        }
      }
    }
  }

  /**
   * Returns an iterator of ColumnarBatch by reading the given buffer. The buffer is expected to
   * contain the Parquet data for the column chunks described by the given block metadata. The
   * buffer will be closed after the iterator is fully consumed, or if there is an exception
   * during reading.
   */
  protected def readBuffer(
      parquetOpts: ParquetOptions,
      colTypes: Array[DataType],
      chunkedBlocks: Seq[BlockMetaData],
      dataBuffer: SpillableHostBuffer
  ): Iterator[ColumnarBatch]

  /**
   * Computes the number of rows alive in the output table. This is normally the same as
   * totalNumRows, but formats with row-level deletes can override it for zero-column reads.
   */
  protected def computeNumRowsAlive(
      totalNumRows: Long,
      chunkedBlocks: Seq[BlockMetaData]): Int = {
    Math.toIntExact(totalNumRows)
  }
}

class ParquetPartitionReader(
    override val fileIO: RapidsFileIO,
    override val conf: Configuration,
    split: PartitionedFile,
    filePath: Path,
    clippedBlocks: Iterable[BlockMetaData],
    clippedParquetSchema: MessageType,
    override val isSchemaCaseSensitive: Boolean,
    readDataSchema: StructType,
    debugDumpPrefix: Option[String],
    debugDumpAlways: Boolean,
    maxReadBatchSizeRows: Integer,
    maxReadBatchSizeBytes: Long,
    targetBatchSizeBytes: Long,
    useChunkedReader: Boolean,
    maxChunkedReaderMemoryUsageSizeBytes: Long,
    override val compressCfg: CpuCompressionConfig,
    override val execMetrics: Map[String, GpuMetric],
    dateRebaseMode: DateTimeRebaseMode,
    timestampRebaseMode: DateTimeRebaseMode,
    hasInt96Timestamps: Boolean,
    useFieldId: Boolean) extends AbstractParquetPartitionReader(
  fileIO, conf, split, filePath, clippedBlocks, clippedParquetSchema, isSchemaCaseSensitive,
  readDataSchema, debugDumpPrefix, debugDumpAlways, maxReadBatchSizeRows, maxReadBatchSizeBytes,
  compressCfg, execMetrics, useFieldId) {

  override protected def readBuffer(
      parquetOpts: ParquetOptions,
      colTypes: Array[DataType],
      chunkedBlocks: Seq[BlockMetaData],
      dataBuffer: SpillableHostBuffer
  ): Iterator[ColumnarBatch] = {
    if (dataBuffer.length == 0) {
      dataBuffer.close()
      CachedGpuBatchIterator(EmptyTableReader, colTypes)
    } else {
      RmmRapidsRetryIterator.withRetryNoSplit(dataBuffer) { _ =>
        // MakeParquetTableProducer will try to close the hostBuf
        val hostBuf = dataBuffer.getDataHostBuffer()
        // Duplicate request is ok, and start to use the GPU just after the host
        // buffer is ready to not block CPU things.
        GpuSemaphore.acquireIfNecessary(TaskContext.get())
        val producer = MakeParquetTableProducer(
          useChunkedReader, maxChunkedReaderMemoryUsageSizeBytes, conf, targetBatchSizeBytes,
          parquetOpts, Array(hostBuf), execMetrics, dateRebaseMode, timestampRebaseMode,
          hasInt96Timestamps, isSchemaCaseSensitive, useFieldId, readDataSchema,
          clippedParquetSchema, Array(split), debugDumpPrefix, debugDumpAlways)
        CachedGpuBatchIterator(producer, colTypes)
      }
    }
  }
}

object ParquetPartitionReader {
  private[parquet] val PARQUET_MAGIC = "PAR1".getBytes(StandardCharsets.US_ASCII)
  private[parquet] val PARQUET_CREATOR = "RAPIDS Spark Plugin"
  private[parquet] val PARQUET_VERSION = 1

  // Remote-range reads use RapidsInputFile.CopyRange (the jni-side type) directly
  // — no local case-class wrapper, no per-call conversion before readVectored.
  // LocalCopy stays separate because it carries a SeekableByteChannel and is
  // consumed by an entirely different code path (out-of-process file cache).
  private[parquet] case class LocalCopy(
      channel: SeekableByteChannel,
      length: Long,
      outputOffset: Long) extends Closeable {
    override def close(): Unit = {
      channel.close()
    }
  }

  private[parquet] def computeOutputSize(
      blocks: Seq[BlockMetaData],
      compressCfg: CpuCompressionConfig): Long = {
    blocks.map { block =>
      computeOutputSize(block, compressCfg)
    }.sum
  }

  private[parquet] def computeOutputSize(
      block: BlockMetaData,
      compressCfg: CpuCompressionConfig): Long = {
    if (compressCfg.decompressAnyCpu) {
      block.getColumns.asScala.map { c =>
        if ((c.getCodec == CompressionCodecName.SNAPPY && compressCfg.decompressSnappyCpu)
            || (c.getCodec == CompressionCodecName.ZSTD && compressCfg.decompressZstdCpu)) {
          // Page headers need to be rewritten when CPU decompresses, and that may
          // increase the size of the page header. Guess how many pages there may be
          // and add a fudge factor per page to try to avoid a late realloc+copy.
          // NOTE: Avoid using block.getTotalByteSize as that is the
          //       uncompressed size rather than the size in the file.
          val estimatedPageCount = (c.getTotalUncompressedSize / (1024 * 1024)) + 1
          c.getTotalUncompressedSize + estimatedPageCount * 8
        } else {
          c.getTotalSize
        }
      }.sum
    } else {
      block.getColumns.asScala.map(_.getTotalSize).sum
    }
  }
}
