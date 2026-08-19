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

package com.nvidia.spark.rapids

import java.net.URI
import java.nio.charset.{Charset, StandardCharsets}
import java.time.DateTimeException
import java.util
import java.util.Optional

import scala.collection.mutable.ListBuffer

import ai.rapids.cudf.{CaptureGroups, ColumnVector, DType, HostColumnVector, HostColumnVectorCore, HostMemoryBuffer, RegexProgram, Scalar, Schema, Table}
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.DateUtils.{toStrf, TimestampFormatConversionException}
import com.nvidia.spark.rapids.RmmRapidsRetryIterator.withRetryNoSplit
import com.nvidia.spark.rapids.jni.CastStrings
import com.nvidia.spark.rapids.shims.GpuTypeShims
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.compress.CompressionCodecFactory

import org.apache.spark.TaskContext
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.execution.QueryExecutionException
import org.apache.spark.sql.execution.datasources.{HadoopFileLinesReader, PartitionedFile}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.{ExceptionTimeParserPolicy, GpuToTimestamp, LegacyTimeParserPolicy}
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * We read text files in one line at a time from Spark. This provides an abstraction in how those
 * lines are buffered before being processed on the GPU.
 */
trait LineBufferer extends AutoCloseable {
  /**
   * Get the current size of the data that has been buffered.
   */
  def getLength: Long

  /**
   * Get the numnber of lines currently added to this that were not filtered out.
   */
  def getNumLines: Int

  /**
   * Add a new line of bytes to the data to process.
   */
  def add(line: Array[Byte], offset: Int, len: Int): Unit

  def isWhiteSpace(b: Byte): Boolean = {
    b == ' ' || b == '\t' || b == '\r' || b == '\n'
  }

  def isEmpty(line: Array[Byte], lineOffset: Int, lineLen: Int): Boolean = {
    (0 until lineLen).forall { idx =>
      isWhiteSpace(line(lineOffset + idx))
    }
  }
}

/**
 * Factory to create a LineBufferer instance that can be used to buffer lines being read in.
 */
trait LineBuffererFactory[BUFF <: LineBufferer] {
  def createBufferer(estimatedSize: Long, lineSeparatorInRead: Array[Byte]): BUFF
}

object HostLineBuffererFactory extends LineBuffererFactory[HostLineBufferer] {
  override def createBufferer(estimatedSize: Long,
                              lineSeparatorInRead: Array[Byte]): HostLineBufferer =
    new HostLineBufferer(estimatedSize, lineSeparatorInRead, false)
}

object FilterEmptyHostLineBuffererFactory extends LineBuffererFactory[HostLineBufferer] {
  override def createBufferer(estimatedSize: Long,
      lineSeparatorInRead: Array[Byte]): HostLineBufferer =
    new HostLineBufferer(estimatedSize, lineSeparatorInRead, true)
}

/**
 * Factory for CSV reading that filters empty lines using Java String.trim() compatible whitespace
 * (all chars <= 0x20). This matches Spark CPU's CSVExprUtils.filterCommentAndEmpty behavior which
 * uses line.trim.nonEmpty to filter blank lines.
 */
object FilterCsvEmptyHostLineBuffererFactory extends LineBuffererFactory[HostLineBufferer] {
  override def createBufferer(estimatedSize: Long,
      lineSeparatorInRead: Array[Byte]): HostLineBufferer =
    new HostLineBufferer(estimatedSize, lineSeparatorInRead, true) {
      // Match Java's String.trim() which treats all chars <= '\u0020' as whitespace.
      override def isWhiteSpace(b: Byte): Boolean = (b & 0xFF) <= 0x20
    }
}

/**
 * Buffer the lines in a single HostMemoryBuffer with the separator inserted inbetween each of
 * the lines.
 */
class HostLineBufferer(size: Long,
                       separator: Array[Byte],
                       filterEmpty: Boolean) extends LineBufferer {
  private var buffer = HostMemoryBuffer.allocate(size)
  private var location: Long = 0
  private var numLines: Int = 0

  def grow(needed: Long): Unit = {
    val newSize = math.max(buffer.getLength * 2, needed)
    closeOnExcept(HostMemoryBuffer.allocate(newSize)) { newBuff =>
      newBuff.copyFromHostBuffer(0, buffer, 0, buffer.getLength)
      buffer.close()
      buffer = newBuff
    }
  }

  override def getLength: Long = location

  override def getNumLines: Int = numLines

  override def add(line: Array[Byte], lineOffset: Int, lineLen: Int): Unit = {
    // Empty lines are filtered out
    if (!filterEmpty || !isEmpty(line, lineOffset, lineLen)) {
      numLines += 1
      val newTotal = location + lineLen + separator.length
      if (newTotal > buffer.getLength) {
        grow(newTotal)
      }
      buffer.setBytes(location, line, lineOffset, lineLen)
      location = location + lineLen
      buffer.setBytes(location, separator, 0, separator.length)
      location = location + separator.length
    }
  }

  def getBufferAndRelease: HostMemoryBuffer = {
    val ret = buffer
    buffer = null
    ret
  }

  override def close(): Unit = {
    if (buffer != null) {
      buffer.close()
      buffer = null
    }
  }
}

object HostStringColBuffererFactory extends LineBuffererFactory[HostStringColBufferer] {
  override def createBufferer(estimatedSize: Long,
      lineSeparatorInRead: Array[Byte]): HostStringColBufferer =
    new HostStringColBufferer(estimatedSize, lineSeparatorInRead)
}

/**
 * Buffer the lines as a HostColumnVector of strings, one per line.
 */
class HostStringColBufferer(size: Long, separator: Array[Byte]) extends LineBufferer {
  // We had to jump through some hoops so that we could grow the string columns dynamically
  //  might be nice to have this in CUDF, but this works fine too.
  private var dataBuffer = HostMemoryBuffer.allocate(size)
  private var dataLocation: Long = 0
  private var rowsAllocated: Int = Math.max((size / 200).toInt, 512)
  private var offsetsBuffer = HostMemoryBuffer.allocate((rowsAllocated + 1) *
      DType.INT32.getSizeInBytes)
  private var numRows: Int = 0

  override def getLength: Long = dataLocation

  override def getNumLines: Int = numRows

  override def add(line: Array[Byte], lineOffset: Int, lineLen: Int): Unit = {
    if (numRows + 1 > rowsAllocated) {
      val newRowsAllocated = math.min(rowsAllocated * 2, Int.MaxValue - 1)
      val tmpBuffer =
        HostMemoryBuffer.allocate((newRowsAllocated + 1) * DType.INT32.getSizeInBytes)
      tmpBuffer.copyFromHostBuffer(0, offsetsBuffer, 0, offsetsBuffer.getLength)
      offsetsBuffer.close()
      offsetsBuffer = tmpBuffer
      rowsAllocated = newRowsAllocated
    }

    if (dataLocation + lineLen > dataBuffer.getLength) {
      val newSize = math.max(dataBuffer.getLength * 2, lineLen)
      closeOnExcept(HostMemoryBuffer.allocate(newSize)) { newBuff =>
        newBuff.copyFromHostBuffer(0, dataBuffer, 0, dataLocation)
        dataBuffer.close()
        dataBuffer = newBuff
      }
    }
    dataBuffer.setBytes(dataLocation, line, lineOffset, lineLen)
    offsetsBuffer.setInt(numRows * DType.INT32.getSizeInBytes, dataLocation.toInt)
    dataLocation += lineLen
    numRows += 1
  }

  def getColumnAndRelease: ColumnVector = {
    // Set the last offset
    offsetsBuffer.setInt(numRows * DType.INT32.getSizeInBytes, dataLocation.toInt)
    withResource(new HostColumnVector(DType.STRING, numRows, Optional.of[java.lang.Long](0L),
      dataBuffer, null, offsetsBuffer, new util.ArrayList[HostColumnVectorCore]())) { hostRet =>
      dataBuffer = null
      offsetsBuffer = null
      hostRet.copyToDevice()
    }
  }

  override def close(): Unit = {
    if (dataBuffer != null) {
      dataBuffer.close()
      dataBuffer = null
    }

    if (offsetsBuffer != null) {
      offsetsBuffer.close()
      offsetsBuffer = null
    }
  }
}

object GpuTextBasedPartitionReader {
  def castStringToTimestamp(
      lhs: ColumnVector,
      sparkFormat: String,
      dtype: DType): ColumnVector = {

    val optionalSeconds = raw"(?:\:\d{2})?"
    val optionalMicros = raw"(?:\.\d{1,6})?"
    val twoDigits = raw"\d{2}"
    val fourDigits = raw"\d{4}"

    val regexRoot = sparkFormat
        .replace("'T'", "T")
        .replace("yyyy", fourDigits)
        .replace("MM", twoDigits)
        .replace("dd", twoDigits)
        .replace("HH", twoDigits)
        .replace("mm", twoDigits)
        .replace("[:ss]", optionalSeconds)
        .replace(":ss", optionalSeconds) // Spark always treats seconds portion as optional
        .replace("[.SSSXXX]", optionalMicros)
        .replace("[.SSS][XXX]", optionalMicros)
        .replace("[.SSS]", optionalMicros)
        .replace("[.SSSSSS]", optionalMicros)
        .replace(".SSSXXX", optionalMicros)
        .replace(".SSSSSS", optionalMicros)
        .replace(".SSS", optionalMicros)

    // Spark treats timestamp portion as optional always
    val regexOptionalTime = regexRoot.split('T') match {
      case Array(d, t) =>
        d + "(?:[ T]" + t + ")?"
      case _ =>
        regexRoot
    }
    val regex = regexOptionalTime + raw"Z?\Z"

    // get a list of all possible cuDF formats that we need to check for
    val cudfFormats = GpuTextBasedDateUtils.toCudfFormats(sparkFormat, parseString = true)


    // filter by regexp first to eliminate invalid entries
    val regexpFiltered = withResource(lhs.strip()) { stripped =>
      val prog = new RegexProgram(regex, CaptureGroups.NON_CAPTURE)
      withResource(stripped.matchesRe(prog)) { matchesRe =>
        withResource(Scalar.fromNull(DType.STRING)) { nullString =>
          matchesRe.ifElse(stripped, nullString)
        }
      }
    }

    // fix timestamps that have milliseconds but no microseconds
    // example ".296" => ".296000"
    val sanitized = withResource(regexpFiltered) { _ =>
      // cannot replace with back-refs directly because cuDF cannot support "\1000\2" so we
      // first substitute with a placeholder and then replace that. The placeholder value
      // `@` was chosen somewhat arbitrarily but should be safe since we do not support any
      // date/time formats that contain the `@` character
      val placeholder = "@"
      val prog = new RegexProgram(raw"(\.\d{3})(Z?)\Z")
      withResource(regexpFiltered.stringReplaceWithBackrefs(prog, raw"\1$placeholder\2")) { tmp =>
        withResource(Scalar.fromString(placeholder)) { from =>
          withResource(Scalar.fromString("000")) { to =>
            tmp.stringReplace(from, to)
          }
        }
      }
    }

    def isTimestamp(fmt: String): ColumnVector = {
      val pos = fmt.indexOf('T')
      if (pos == -1) {
        sanitized.isTimestamp(fmt)
      } else {
        // Spark supports both ` ` and `T` as the delimiter so we have to test
        // for both formats when calling `isTimestamp` in cuDF but the
        // `asTimestamp` method ignores the delimiter so we only need to call that
        // with one format
        val withSpaceDelim = fmt.substring(0, pos) + ' ' + fmt.substring(pos + 1)
        withResource(sanitized.isTimestamp(fmt)) { isValidFmt1 =>
          withResource(sanitized.isTimestamp(withSpaceDelim)) { isValidFmt2 =>
            isValidFmt1.or(isValidFmt2)
          }
        }
      }
    }

    def asTimestampOrNull(fmt: String): ColumnVector = {
      withResource(Scalar.fromNull(dtype)) { nullScalar =>
        withResource(isTimestamp(fmt)) { isValid =>
          withResource(sanitized.asTimestamp(dtype, fmt)) { ts =>
            isValid.ifElse(ts, nullScalar)
          }
        }
      }
    }

    def asTimestampOr(fmt: String, orValue: ColumnVector): ColumnVector = {
      withResource(orValue) { _ =>
        withResource(isTimestamp(fmt)) { isValid =>
          withResource(sanitized.asTimestamp(dtype, fmt)) { ts =>
            isValid.ifElse(ts, orValue)
          }
        }
      }
    }

    withResource(sanitized) { _ =>
      if (cudfFormats.length == 1) {
        asTimestampOrNull(cudfFormats.head)
      } else {
        cudfFormats.tail.foldLeft(asTimestampOrNull(cudfFormats.head)) { (input, fmt) =>
          asTimestampOr(fmt, input)
        }
      }
    }
  }
}

/**
 * The text based PartitionReader
 * @param conf the Hadoop configuration
 * @param partFile file split to read
 * @param dataSchema schema of the data
 * @param readDataSchema the Spark schema describing what will be read
 * @param lineSeparatorInRead An optional byte line sep.
 * @param maxRowsPerChunk maximum number of rows to read in a batch
 * @param maxBytesPerChunk maximum number of bytes to read in a batch
 * @param execMetrics metrics to update during read
 */
abstract class GpuTextBasedPartitionReader[BUFF <: LineBufferer, FACT <: LineBuffererFactory[BUFF]](
    conf: Configuration,
    partFile: PartitionedFile,
    dataSchema: StructType,
    readDataSchema: StructType,
    lineSeparatorInRead: Option[Array[Byte]],
    maxRowsPerChunk: Integer,
    maxBytesPerChunk: Long,
    execMetrics: Map[String, GpuMetric],
    bufferFactory: FACT,
    charset: Charset = StandardCharsets.UTF_8)
  extends PartitionReader[ColumnarBatch] with ScanWithMetrics {
  import GpuMetric._

  private var batch: Option[ColumnarBatch] = None
  private val lineReader = new HadoopFileLinesReader(partFile, lineSeparatorInRead, conf)
  private var isFirstChunkForIterator: Boolean = true
  private var isExhausted: Boolean = false

  metrics = execMetrics

  private lazy val estimatedHostBufferSize: Long = {
    // URI-decode the file path so paths containing escaped glob metacharacters (e.g.
    // `%5Babc%5D` for `[abc]`) resolve to a real FileStatus instead of FileNotFound.
    val rawPath = new Path(new URI(partFile.filePath.toString()))
    val fs = rawPath.getFileSystem(conf)
    val path = fs.makeQualified(rawPath)
    val fileSize = fs.getFileStatus(path).getLen
    val codecFactory = new CompressionCodecFactory(conf)
    val codec = codecFactory.getCodec(path)
    if (codec != null) {
      // wild guess that compression is 2X or less
      partFile.length * 2
    } else if (partFile.start + partFile.length == fileSize) {
      // last split doesn't need to read an additional record
      partFile.length
    } else {
      // wild guess for extra space needed for the record after the split end offset
      partFile.length + 128 * 1024
    }
  }

  private lazy val toUTF8Bytes: Text => (Array[Byte], Int) =
    if (GpuCSVScan.isUTF8Charset(charset)) {
      // Already utf8, return it directly
      line => (line.getBytes, line.getLength)
    } else {
      // Do the decoding and encoding on CPU now, but better support the translation on GPU.
      line => {
        val utf8Bytes = new String(line.getBytes, 0, line.getLength, charset)
          .getBytes(StandardCharsets.UTF_8)
        (utf8Bytes, utf8Bytes.length)
      }
    }

  private def readPartFile(): (BUFF, Long) = {
    NvtxRegistry.BUFFER_FILE_SPLIT_TEXT {
      isFirstChunkForIterator = false
      val separator = lineSeparatorInRead.getOrElse(Array('\n'.toByte))
      var succeeded = false
      var totalSize: Long = 0L
      var totalRows: Integer = 0
      val hmb = bufferFactory.createBufferer(estimatedHostBufferSize, separator)
      try {
        while (lineReader.hasNext
          && totalRows != maxRowsPerChunk
          && totalSize <= maxBytesPerChunk /* soft limit and returns at least one row */) {
          val (lineBytes, bytesLen) = toUTF8Bytes(lineReader.next())
          hmb.add(lineBytes, 0, bytesLen)
          totalRows = hmb.getNumLines
          totalSize = hmb.getLength
        }
        //Indicate this is the last chunk
        isExhausted = !lineReader.hasNext
        succeeded = true
      } finally {
        if (!succeeded) {
          hmb.close()
        }
      }
      (hmb, totalSize)
    }
  }

  private def readBatch(): Option[ColumnarBatch] = {
    NvtxRegistry.FILE_FORMAT_READ_BATCH {
      val isFirstChunk = partFile.start == 0 && isFirstChunkForIterator
      val table = readToTable(isFirstChunk)
      try {
        if (readDataSchema.isEmpty) {
          table.map(t => new ColumnarBatch(Array.empty, t.getRowCount.toInt))
        } else {
          table.map(GpuColumnVector.from(_, readDataSchema.toArray.map(_.dataType)))
        }
      } finally {
        metrics(NUM_OUTPUT_BATCHES) += 1
        table.foreach(_.close())
      }
    }
  }

  def getCudfSchema(dataSchema: StructType): Schema = {
    // read boolean and numeric columns as strings in cuDF
    val dataSchemaWithStrings = StructType(dataSchema.fields
        .map(f => {
          f.dataType match {
            case DataTypes.BooleanType | DataTypes.ByteType | DataTypes.ShortType |
                 DataTypes.IntegerType | DataTypes.LongType | DataTypes.FloatType |
                 DataTypes.DoubleType | _: DecimalType | DataTypes.DateType |
                 DataTypes.TimestampType =>
              f.copy(dataType = DataTypes.StringType)
            case other if GpuTypeShims.supportCsvRead(other) =>
              f.copy(dataType = DataTypes.StringType)
            case _ =>
              f
          }
        }))
    GpuColumnVector.from(dataSchemaWithStrings)
  }

  // accessible to children for OOM unit tests
  protected def castToOutputTypesWithRetryAndClose(table: Table,
      readSchema: StructType): Table = {
    val stbl = closeOnExcept(table) { _ =>
      SpillableTable(table, SpillPriorities.ACTIVE_BATCHING_PRIORITY)
    }
    withRetryNoSplit(stbl) { attempt =>
      withResource(attempt.getTable())(castTableToDesiredTypes(_, readSchema))
    }
  }

  def castTableToDesiredTypes(table: Table, readSchema: StructType): Table = {
    val columns = new ListBuffer[ColumnVector]()
    // Table increases the ref counts on the columns so we have
    // to close them after creating the table
    withResource(columns) { _ =>
      for (i <- 0 until table.getNumberOfColumns) {
        val castColumn = readSchema.fields(i).dataType match {
          case DataTypes.BooleanType =>
            castStringToBool(table.getColumn(i))
          case DataTypes.ByteType =>
            castStringToInt(table.getColumn(i), DType.INT8)
          case DataTypes.ShortType =>
            castStringToInt(table.getColumn(i), DType.INT16)
          case DataTypes.IntegerType =>
            castStringToInt(table.getColumn(i), DType.INT32)
          case DataTypes.LongType =>
            castStringToInt(table.getColumn(i), DType.INT64)
          case DataTypes.FloatType =>
            castStringToFloat(table.getColumn(i), DType.FLOAT32)
          case DataTypes.DoubleType =>
            castStringToFloat(table.getColumn(i), DType.FLOAT64)
          case dt: DecimalType =>
            castStringToDecimal(table.getColumn(i), dt)
          case DataTypes.DateType =>
            castStringToDate(table.getColumn(i), DType.TIMESTAMP_DAYS)
          case DataTypes.TimestampType =>
            castStringToTimestamp(table.getColumn(i),
              timestampFormat, DType.TIMESTAMP_MICROSECONDS)
          case other if GpuTypeShims.supportCsvRead(other) =>
            GpuTypeShims.csvRead(table.getColumn(i), other)
          case _ =>
            table.getColumn(i).incRefCount()
        }
        columns += castColumn
      }
      new Table(columns.toSeq: _*)
    }
  }

  private def readToTable(isFirstChunk: Boolean): Option[Table] = {
    val (dataBuffer, dataSize) = metrics(BUFFER_TIME).ns {
      readPartFile()
    }
    try {
      if (dataSize == 0) {
        None
      } else {
        val newReadDataSchema: StructType = if (readDataSchema.isEmpty) {
          val smallestField =
            dataSchema.min(Ordering.by[StructField, Integer](_.dataType.defaultSize))
          StructType(Seq(smallestField))
        } else {
          readDataSchema
        }

        val cudfSchema = getCudfSchema(dataSchema)
        val cudfReadSchema = getCudfSchema(newReadDataSchema)

        // about to start using the GPU
        GpuSemaphore.acquireIfNecessary(TaskContext.get())

        // The buffer that is sent down
        val table = readToTable(dataBuffer, cudfSchema, newReadDataSchema, cudfReadSchema,
          isFirstChunk, metrics(GPU_DECODE_TIME))

        // parse boolean and numeric columns that were read as strings
        val castTable =
          castToOutputTypesWithRetryAndClose(table, newReadDataSchema)

        handleResult(newReadDataSchema, castTable)
      }
    } finally {
      dataBuffer.close()
    }
  }

  def dateFormat: Option[String]
  def timestampFormat: String

  def castStringToDate(input: ColumnVector, dt: DType): ColumnVector = {
    val cudfFormat = DateUtils.toStrf(dateFormat.getOrElse("yyyy-MM-dd"), parseString = true)
    withResource(input.strip()) { stripped =>
      withResource(stripped.isTimestamp(cudfFormat)) { isDate =>
        if (GpuOverrides.getTimeParserPolicy == ExceptionTimeParserPolicy) {
          withResource(isDate.all()) { all =>
            if (all.isValid && !all.getBoolean) {
              throw new DateTimeException("One or more values is not a valid date")
            }
          }
        }
        withResource(stripped.asTimestamp(dt, cudfFormat)) { asDate =>
          withResource(Scalar.fromNull(dt)) { nullScalar =>
            isDate.ifElse(asDate, nullScalar)
          }
        }
      }
    }
  }

  def castStringToTimestamp(
      lhs: ColumnVector,
      sparkFormat: String,
      dtype: DType): ColumnVector = {
    GpuTextBasedPartitionReader.castStringToTimestamp(lhs, sparkFormat, dtype)
  }

  def castStringToBool(input: ColumnVector): ColumnVector

  def castStringToFloat(input: ColumnVector, dt: DType): ColumnVector = {
    CastStrings.toFloat(input, false, dt)
  }

  def castStringToDecimal(input: ColumnVector, dt: DecimalType): ColumnVector = {
    CastStrings.toDecimal(input, false, dt.precision, -dt.scale)
  }

  def castStringToInt(input: ColumnVector, intType: DType): ColumnVector = {
    withResource(input.isInteger(intType)) { isInt =>
      withResource(input.castTo(intType)) { asInt =>
        withResource(Scalar.fromNull(intType)) { nullValue =>
          isInt.ifElse(asInt, nullValue)
        }
      }
    }
  }

  /**
   * Read the host buffer to GPU table
   * @param dataBuffer where the data is buffered
   * @param cudfDataSchema the cudf schema of the data
   * @param readDataSchema the Spark schema describing what will be read
   * @param cudfReadDataSchema the cudf schema of just the data we want to read.
   * @param isFirstChunk if it is the first chunk
   * @return table
   */
  def readToTable(
    dataBuffer: BUFF,
    cudfDataSchema: Schema,
    readDataSchema: StructType,
    cudfReadDataSchema: Schema,
    isFirstChunk: Boolean,
    decodeTime: GpuMetric): Table

  /**
   * File format short name used for logging and other things to uniquely identity
   * which file format is being used.
   *
   * @return the file format short name
   */
  def getFileFormatShortName: String

  /**
   * Handle the table decoded by GPU
   *
   * Please note that, this function owns table which is supposed to be closed in this function
   * But for the optimization, we just return the original table.
   *
   * @param readDataSchema the Spark schema describing what will be read
   * @param table the table decoded by GPU
   * @return the new optional Table
   */
  def handleResult(readDataSchema: StructType, table: Table): Option[Table] = {
    val numColumns = table.getNumberOfColumns

    closeOnExcept(table) { _ =>
      if (readDataSchema.length != numColumns) {
        throw new QueryExecutionException(s"Expected ${readDataSchema.length} columns " +
          s"but only read ${table.getNumberOfColumns} from $partFile")
      }
    }

    // For the GPU resource handling convention, we should close input table and return a new
    // table just like below code. But for optimization, we just return the input table.
    // withResource(table) { _
    //  val cols = (0 until  table.getNumberOfColumns).map(i => table.getColumn(i))
    //  Some(new Table(cols: _*))
    // }
    if (table.getRowCount == 0) {
      // CSV reader can return empty table, close it and return None
      // E.g.: CSV file with only header and no data rows, empty table will be returned
      table.close()
      None
    } else {
      Some(table)
    }
  }

  override def next(): Boolean = {
    batch.foreach(_.close())
    batch = if (isExhausted) {
      None
    } else {
      readBatch()
    }

    // NOTE: At this point, the task may not have yet acquired the semaphore if `batch` is `None`.
    // We are not acquiring the semaphore here since this next() is getting called from
    // the `PartitionReaderIterator` which implements a standard iterator pattern, and
    // advertises `hasNext` as false when we return false here. No downstream tasks should
    // try to call next after `hasNext` returns false, and any task that produces some kind of
    // data when `hasNext` is false is responsible to get the semaphore themselves.
    batch.isDefined
  }

  override def get(): ColumnarBatch = {
    val ret = batch.getOrElse(throw new NoSuchElementException)
    batch = None
    ret
  }

  override def close(): Unit = {
    lineReader.close()
    batch.foreach(_.close())
    batch = None
    isExhausted = true
  }
}

object GpuTextBasedDateUtils {

  private val supportedDateFormats = Set(
    "yyyy-MM-dd",
    "yyyy/MM/dd",
    "yyyy-MM",
    "yyyy/MM",
    "MM-yyyy",
    "MM/yyyy",
    "MM-dd-yyyy",
    "MM/dd/yyyy",
    "dd-MM-yyyy",
    "dd/MM/yyyy"
  )

  private val supportedTsPortionFormats = Set(
    "HH:mm:ss.SSSXXX",
    "HH:mm:ss[.SSS][XXX]",
    "HH:mm:ss[.SSSXXX]",
    "HH:mm",
    "HH:mm:ss",
    "HH:mm[:ss]",
    "HH:mm:ss.SSS",
    "HH:mm:ss[.SSS]"
  )

  def tagCudfFormat(
      meta: RapidsMeta[_, _, _],
      sparkFormat: String,
      parseString: Boolean): Unit = {
    if (GpuOverrides.getTimeParserPolicy == LegacyTimeParserPolicy) {
      try {
        // try and convert the format to cuDF format - this will throw an exception if
        // the format contains unsupported characters or words
        toCudfFormats(sparkFormat, parseString)
        // format parsed ok but we have no 100% compatible formats in LEGACY mode
        if (GpuToTimestamp.LEGACY_COMPATIBLE_FORMATS.contains(sparkFormat)) {
          // LEGACY support has a number of issues that mean we cannot guarantee
          // compatibility with CPU
          // - we can only support 4 digit years but Spark supports a wider range
          // - we use a proleptic Gregorian calender but Spark uses a hybrid Julian+Gregorian
          //   calender in LEGACY mode
          if (SQLConf.get.ansiEnabled) {
            meta.willNotWorkOnGpu("LEGACY format in ANSI mode is not supported on the GPU")
          } else if (!meta.conf.incompatDateFormats) {
            meta.willNotWorkOnGpu(s"LEGACY format '$sparkFormat' on the GPU is not guaranteed " +
              s"to produce the same results as Spark on CPU. Set " +
              s"${RapidsConf.INCOMPATIBLE_DATE_FORMATS.key}=true to force onto GPU.")
          }
        } else {
          meta.willNotWorkOnGpu(s"LEGACY format '$sparkFormat' is not supported on the GPU.")
        }
      } catch {
        case e: TimestampFormatConversionException =>
          meta.willNotWorkOnGpu(s"Failed to convert ${e.reason} ${e.getMessage}")
      }
    } else {
      val parts = sparkFormat.split("'T'", 2)
      if (parts.isEmpty) {
        meta.willNotWorkOnGpu(s"the timestamp format '$sparkFormat' is not supported")
      }
      if (parts.headOption.exists(h => !supportedDateFormats.contains(h))) {
        meta.willNotWorkOnGpu(s"the timestamp format '$sparkFormat' is not supported")
      }
      if (parts.length > 1 && !supportedTsPortionFormats.contains(parts(1))) {
        meta.willNotWorkOnGpu(s"the timestamp format '$sparkFormat' is not supported")
      }
      try {
        // try and convert the format to cuDF format - this will throw an exception if
        // the format contains unsupported characters or words
        toCudfFormats(sparkFormat, parseString)
      } catch {
        case e: TimestampFormatConversionException =>
          meta.willNotWorkOnGpu(s"Failed to convert ${e.reason} ${e.getMessage}")
      }
    }
  }

  /**
   * Get the list of all cuDF formats that need to be checked for when parsing timestamps. The
   * returned formats must be ordered such that the first format is the most lenient and the
   * last is the least lenient.
   *
   * For example, the spark format `yyyy-MM-dd'T'HH:mm:ss[.SSS][XXX]` would result in the
   * following cuDF formats being returned, in this order:
   *
   * - `%Y-%m-%d`
   * - `%Y-%m-%dT%H:%M`
   * - `%Y-%m-%dT%H:%M:%S`
   * - `%Y-%m-%dT%H:%M:%S.%f`
   */
  def toCudfFormats(sparkFormat: String, parseString: Boolean): Seq[String] = {
    val hasZsuffix = sparkFormat.endsWith("Z")
    val formatRoot = if (hasZsuffix) {
      sparkFormat.substring(0, sparkFormat.length-1)
    } else {
      sparkFormat
    }

    // strip off suffixes that cuDF will not recognize
    val cudfSupportedFormat = formatRoot
      .replace("'T'", "T")
      .replace("[.SSSXXX]", "")
      .replace("[.SSS][XXX]", "")
      .replace("[.SSS]", "")
      .replace("[.SSSSSS]", "")
      .replace(".SSSXXX", "")
      .replace(".SSS", "")
      .replace("[:ss]", "")

    val cudfFormat = toStrf(cudfSupportedFormat, parseString)
    val suffix = if (hasZsuffix) "Z" else ""

    val optionalFractional = Seq("[.SSS][XXX]", "[.SSS]", "[.SSSSSS]", "[.SSS][XXX]",
      ".SSSXXX", ".SSS")
    val baseFormats = if (optionalFractional.exists(formatRoot.endsWith)) {
      val cudfFormat1 = cudfFormat + suffix
      val cudfFormat2 = cudfFormat + ".%f" + suffix
      Seq(cudfFormat1, cudfFormat2)
    } else if (formatRoot.endsWith("[:ss]")) {
      Seq(cudfFormat + ":%S" + suffix)
    } else {
      Seq(cudfFormat)
    }

    val pos = baseFormats.head.indexOf('T')
    val formatsIncludingDateOnly = if (pos == -1) {
      baseFormats
    } else {
      Seq(baseFormats.head.substring(0, pos)) ++ baseFormats
    }

    // seconds are always optional in Spark
    val formats = ListBuffer[String]()
    for (fmt <- formatsIncludingDateOnly) {
      if (fmt.contains(":%S") && !fmt.contains("%f")) {
        formats += fmt.replace(":%S", "")
      }
      formats += fmt
    }
    formats.toSeq
  }

}
