/*
 * Copyright (c) 2020-2026, NVIDIA CORPORATION.
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

import java.io.{InputStream, IOException}
import java.nio.ByteBuffer

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

import ai.rapids.cudf._
import ai.rapids.cudf.ParquetWriterOptions.StatisticsFrequency
import com.nvidia.spark.GpuCachedBatchSerializer
import com.nvidia.spark.rapids.{ColumnCastUtil, DecimalUtil, GpuColumnVector, GpuRowToColumnConverter, GpuSemaphore, RapidsConf, RequireSingleBatch, RowToColumnarIterator, SchemaUtils}
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.GpuColumnVector.GpuColumnarBatchBuilder
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.ScalableTaskCompletion.onTaskCompletion
import com.nvidia.spark.rapids.shims.{LegacyBehaviorPolicyShim, ParquetVariantShims, SparkShimImpl}
import com.nvidia.spark.rapids.shims.parquet.{ParquetFieldIdShims, ParquetLegacyNanoAsLongShims, ParquetTimestampNTZShims}
import org.apache.commons.io.output.ByteArrayOutputStream
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.RecordWriter
import org.apache.parquet.{HadoopReadOptions, ParquetReadOptions}
import org.apache.parquet.column.ParquetProperties
import org.apache.parquet.hadoop.{CodecFactory, MemoryManager, ParquetFileReader, ParquetFileWriter, ParquetInputFormat, ParquetOutputFormat, ParquetRecordWriter, ParquetWriter}
import org.apache.parquet.hadoop.ParquetFileWriter.Mode
import org.apache.parquet.hadoop.api.WriteSupport
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.io.{DelegatingPositionOutputStream, DelegatingSeekableInputStream, InputFile, OutputFile, PositionOutputStream}

import org.apache.spark.TaskContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Expression, GenericInternalRow, SpecializedGetters, UnsafeRow}
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, ArrayData, GenericArrayData, MapData}
import org.apache.spark.sql.columnar.CachedBatch
import org.apache.spark.sql.execution.datasources.parquet.{ParquetReadSupport, ParquetToSparkSchemaConverter, ParquetWriteSupport, ShimCurrentBatchIterator}
import org.apache.spark.sql.execution.datasources.parquet.rapids.ParquetRecordMaterializer
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.PCBSSchemaHelper
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.storage.StorageLevel
import org.apache.spark.unsafe.types.CalendarInterval

/**
 * copied from Spark org.apache.spark.util.ByteBufferInputStream
 */
private class ByteBufferInputStream(private var buffer: ByteBuffer)
    extends InputStream {

  override def read(): Int = {
    if (buffer == null || buffer.remaining() == 0) {
      cleanUp()
      -1
    } else {
      buffer.get() & 0xFF
    }
  }

  override def read(dest: Array[Byte]): Int = {
    read(dest, 0, dest.length)
  }

  override def read(dest: Array[Byte], offset: Int, length: Int): Int = {
    if (buffer == null || buffer.remaining() == 0) {
      cleanUp()
      -1
    } else {
      val amountToGet = math.min(buffer.remaining(), length)
      buffer.get(dest, offset, amountToGet)
      amountToGet
    }
  }

  override def skip(bytes: Long): Long = {
    if (buffer != null) {
      val amountToSkip = math.min(bytes, buffer.remaining).toInt
      buffer.position(buffer.position() + amountToSkip)
      if (buffer.remaining() == 0) {
        cleanUp()
      }
      amountToSkip
    } else {
      0L
    }
  }

  /**
   * Clean up the buffer, and potentially dispose of it using StorageUtils.dispose().
   */
  private def cleanUp(): Unit = {
    if (buffer != null) {
      buffer = null
    }
  }
}

class ByteArrayInputFile(buff: Array[Byte]) extends InputFile {

  override def getLength: Long = buff.length

  override def newStream(): org.apache.parquet.io.SeekableInputStream = {
    val byteBuffer = ByteBuffer.wrap(buff)
    new DelegatingSeekableInputStream(new ByteBufferInputStream(byteBuffer)) {
      override def getPos: Long = byteBuffer.position()

      override def seek(newPos: Long): Unit = {
        if (newPos > Int.MaxValue || newPos < Int.MinValue) {
          throw new IllegalStateException("seek value is out of supported range " + newPos)
        }
        byteBuffer.position(newPos.toInt)
      }
    }
  }
}

private object ByteArrayOutputFile {
  val BLOCK_SIZE: Int = 32 * 1024 * 1024 // 32M
}

private class ByteArrayOutputFile(stream: ByteArrayOutputStream) extends OutputFile {
  override def create(blockSizeHint: Long): PositionOutputStream = {
    new DelegatingPositionOutputStream(stream) {
      var pos = 0

      override def getPos: Long = pos

      override def write(b: Int): Unit = {
        super.write(b)
        pos += Integer.BYTES
      }

      override def write(b: Array[Byte]): Unit = {
        super.write(b)
        pos += b.length
      }

      override def write(b: Array[Byte], off: Int, len: Int): Unit = {
        super.write(b, off, len)
        pos += len
      }
    }
  }

  override def createOrOverwrite(blockSizeHint: Long): PositionOutputStream =
    throw new UnsupportedOperationException("Don't need to overwrite")

  override def supportsBlockSize(): Boolean = true

  override def defaultBlockSize(): Long = ByteArrayOutputFile.BLOCK_SIZE
}

private class ParquetBufferConsumer(val numRows: Int) extends HostBufferConsumer with
    AutoCloseable {
  @transient private[this] val offHeapBuffers = mutable.Queue[(HostMemoryBuffer, Long)]()
  private var buffer: Array[Byte] = _

  override def handleBuffer(buffer: HostMemoryBuffer, len: Long): Unit = {
    offHeapBuffers += Tuple2(buffer, len)
  }

  def getBuffer: Array[Byte] = {
    if (buffer == null) {
      writeBuffers()
    }
    buffer
  }

  def close(): Unit = {
    if (buffer == null) {
      writeBuffers()
    }
  }

  private def writeBuffers(): Unit = {
    val toProcess = offHeapBuffers.dequeueAll(_ => true)
    // We are making sure the input is smaller than 2gb so the parquet written should never be more
    // than Int.MAX_SIZE.
    val bytes = toProcess.map(_._2).sum

    // for now assert bytes are less than Int.MaxValue
    assert(bytes <= Int.MaxValue)
    buffer = new Array(bytes.toInt)
    try {
      var offset: Int = 0
      toProcess.foreach(ops => {
        val origBuffer = ops._1
        val len = ops._2.toInt
        origBuffer.asByteBuffer().get(buffer, offset, len)
        offset = offset + len
      })
    } finally {
      toProcess.map(_._1).safeClose()
    }
  }
}

private object ParquetCachedBatch {
  def apply(parquetBuff: ParquetBufferConsumer): ParquetCachedBatch = {
    new ParquetCachedBatch(parquetBuff.numRows, parquetBuff.getBuffer)
  }
}

case class ParquetCachedBatch(
    numRows: Int,
    buffer: Array[Byte]) extends CachedBatch {
  override def sizeInBytes: Long = buffer.length
}

/**
 * Spark wants the producer to close the batch. We have a listener in this iterator that will close
 * the batch after the task is completed
 */
private case class CloseableColumnBatchIterator(iter: Iterator[ColumnarBatch]) extends
    Iterator[ColumnarBatch] {
  var cb: ColumnarBatch = _

  private def closeCurrentBatch(): Unit = {
    if (cb != null) {
      cb.close()
      cb = null
    }
  }

  // Don't install the callback if in a unit test
  Option(TaskContext.get()).foreach { tc =>
    onTaskCompletion(tc) {
      closeCurrentBatch()
    }
  }

  override def hasNext: Boolean = iter.hasNext

  override def next(): ColumnarBatch = {
    closeCurrentBatch()
    cb = iter.next()
    cb
  }
}

/**
 * This class assumes, the data is Columnar and the plugin is on.
 * Note, this class should not be referenced directly in source code.
 * It should be loaded by reflection using ShimLoader.newInstanceOf, see ./docs/dev/shims.md
 */
class ParquetCachedBatchSerializer extends GpuCachedBatchSerializer {

  override def supportsColumnarInput(schema: Seq[Attribute]): Boolean = true

  override def supportsColumnarOutput(schema: StructType): Boolean = schema.fields.forall { f =>
    // only check spark b/c if we are on the GPU then we will be calling the gpu method regardless
    PCBSSchemaHelper.isTypeSupportedByColumnarSparkParquetWriter(f.dataType) ||
        f.dataType == DataTypes.NullType
  }

  def isSchemaSupportedByCudf(schema: Seq[Attribute]): Boolean = {
    schema.forall(field => isSupportedByCudf(field.dataType))
  }

  def isSupportedByCudf(dataType: DataType): Boolean = {
    dataType match {
      case a: ArrayType => isSupportedByCudf(a.elementType)
      case s: StructType => s.forall(field => isSupportedByCudf(field.dataType))
      case m: MapType => isSupportedByCudf(m.keyType) && isSupportedByCudf(m.valueType)
      case _ => GpuColumnVector.isNonNestedSupportedType(dataType)
    }
  }

  /**
   * Convert an `RDD[ColumnarBatch]` into an `RDD[CachedBatch]` in preparation for caching the data.
   * This method uses Parquet Writer on the GPU to write the cached batch
   *
   * @param input        the input `RDD` to be converted.
   * @param schema       the schema of the data being stored.
   * @param storageLevel where the data will be stored.
   * @param conf         the config for the query.
   * @return The data converted into a format more suitable for caching.
   */
  override def convertColumnarBatchToCachedBatch(input: RDD[ColumnarBatch],
      schema: Seq[Attribute],
      storageLevel: StorageLevel,
      conf: SQLConf): RDD[CachedBatch] = {

    val rapidsConf = new RapidsConf(conf)
    val useCompression = conf.useCompression
    val bytesAllowedPerBatch = getBytesAllowedPerBatch(conf)
    val (schemaWithUnambiguousNames, _) = getSupportedSchemaFromUnsupported(schema)
    val structSchema = schemaWithUnambiguousNames.toStructType
    if (rapidsConf.isSqlEnabled && rapidsConf.isSqlExecuteOnGPU &&
        isSchemaSupportedByCudf(schema)) {
      def putOnGpuIfNeeded(batch: ColumnarBatch): ColumnarBatch = {
        if (!batch.column(0).isInstanceOf[GpuColumnVector]) {
          // The input batch from CPU must NOT be closed, because the columns inside it
          // will be reused, and Spark expects the producer to close its batches.
          val numRows = batch.numRows()
          withResource(new GpuColumnarBatchBuilder(structSchema, numRows)) { gcbBuilder =>
            for (i <- 0 until batch.numCols()) {
              gcbBuilder.copyColumnar(batch.column(i), i, numRows)
            }
            gcbBuilder.build(numRows)
          }
        } else {
          batch
        }
      }

      input.flatMap(batch => {
        // If the structSchema is empty or the batch has no cols in that case return a batch with
        // just rows.
        if (batch.numCols() == 0 || structSchema.isEmpty) {
          val rows = batch.numRows()
          if (batch.numCols() > 0 && batch.column(0).isInstanceOf[GpuColumnVector]) {
            batch.close()
          }
          List(ParquetCachedBatch(rows, new Array[Byte](0)))
        } else {
          withResource(putOnGpuIfNeeded(batch)) { gpuCB =>
            compressColumnarBatchWithParquet(gpuCB, structSchema, schema.toStructType,
              bytesAllowedPerBatch, useCompression)
          }
        }
      })
    } else {
      val broadcastedConf = SparkSession.active.sparkContext.broadcast(conf.getAllConfs)
      input.mapPartitions {
        cbIter =>
          new CachedBatchIteratorProducer[ColumnarBatch](cbIter, schemaWithUnambiguousNames, schema,
            broadcastedConf).getColumnarBatchToCachedBatchIterator
      }
    }
  }

  private[rapids] def compressColumnarBatchWithParquet(
      oldGpuCB: ColumnarBatch,
      schema: StructType,
      origSchema: StructType,
      bytesAllowedPerBatch: Long,
      useCompression: Boolean): List[ParquetCachedBatch] = {
    if (oldGpuCB.numRows() == 0) {
      // return empty List of ParquetCachedBatch
      return List.empty[ParquetCachedBatch]
    }
    val estimatedRowSize = scala.Range(0, oldGpuCB.numCols()).map { idx =>
      oldGpuCB.column(idx).asInstanceOf[GpuColumnVector]
        .getBase.getDeviceMemorySize / oldGpuCB.numRows()
    }.sum

    val columns = for (i <- 0 until oldGpuCB.numCols()) yield {
      val gpuVector = oldGpuCB.column(i).asInstanceOf[GpuColumnVector]
      var dataType = origSchema(i).dataType
      val v = ColumnCastUtil.ifTrueThenDeepConvertTypeAtoTypeB(gpuVector.getBase,
        origSchema(i).dataType,
        // we are checking for scale > 0 because cudf and spark refer to scales as opposites
        // e.g. scale = -3 in Spark is scale = 3 in cudf
        (_, cv) => cv.getType.isDecimalType && cv.getType.getScale > 0,
        (_, cv) => {
          if (cv.getType.isBackedByLong) {
            dataType = LongType
            cv.bitCastTo(DType.INT64)
          } else {
            dataType = IntegerType
            cv.bitCastTo(DType.INT32)
          }
        }
      )
      GpuColumnVector.from(v, schema(i).dataType)
    }
    withResource(new ColumnarBatch(columns.toArray, oldGpuCB.numRows())) { gpuCB =>
      val rowsAllowedInBatch = (bytesAllowedPerBatch / estimatedRowSize).toInt
      val splitIndices = scala.Range(rowsAllowedInBatch, gpuCB.numRows(), rowsAllowedInBatch)
      val buffers = new ListBuffer[ParquetCachedBatch]
      if (splitIndices.nonEmpty) {
        val splitVectors = new ListBuffer[Array[ColumnVector]]
        try {
          for (index <- 0 until gpuCB.numCols()) {
            splitVectors +=
                gpuCB.column(index).asInstanceOf[GpuColumnVector].getBase.split(splitIndices: _*)
          }

          // Splitting the table
          // e.g. T0 = {col1, col2,...,coln} => split columns into 'm' cols =>
          // T00= {splitCol1(0), splitCol2(0),...,splitColn(0)}
          // T01= {splitCol1(1), splitCol2(1),...,splitColn(1)}
          // ...
          // T0m= {splitCol1(m), splitCol2(m),...,splitColn(m)}
          def makeTableForIndex(i: Int): Table = {
            val columns = splitVectors.indices.map(j => splitVectors(j)(i))
            new Table(columns: _*)
          }

          for (i <- splitVectors.head.indices) {
            withResource(makeTableForIndex(i)) { table =>
              val buffer = writeTableToCachedBatch(table, schema, useCompression)
              buffers += ParquetCachedBatch(buffer)
            }
          }
        } finally {
          splitVectors.foreach(array => array.safeClose())
        }
      } else {
        withResource(GpuColumnVector.from(gpuCB)) { table =>
          val buffer = writeTableToCachedBatch(table, schema, useCompression)
          buffers += ParquetCachedBatch(buffer)
        }
      }
      buffers.toList
    }
  }

  def getParquetWriterOptions(
      useCompression: Boolean,
      schema: StructType): ParquetWriterOptions = {
    val compressionType = if (useCompression) CompressionType.SNAPPY else CompressionType.NONE
    SchemaUtils
        .writerOptionsFromSchema(ParquetWriterOptions.builder(), schema, nullable = false,
          writeInt96 = false)
        .withCompressionType(compressionType)
        .withStatisticsFrequency(StatisticsFrequency.ROWGROUP).build()
  }

  private def writeTableToCachedBatch(
      table: Table,
      schema: StructType,
      useCompression: Boolean): ParquetBufferConsumer = {
    val buffer = new ParquetBufferConsumer(table.getRowCount.toInt)
    val opts = getParquetWriterOptions(useCompression, schema)
    withResource(Table.writeParquetChunked(opts, buffer)) { writer =>
      writer.write(table)
    }
    buffer
  }

  /**
   * This method decodes the CachedBatch leaving it on the GPU to avoid the extra copying back to
   * the host
   *
   * @param input              the cached batches that should be converted.
   * @param cacheAttributes    the attributes of the data in the batch.
   * @param selectedAttributes the fields that should be loaded from the data and the order they
   *                           should appear in the output batch.
   * @param conf               the configuration for the job.
   * @return an RDD of the input cached batches transformed into the ColumnarBatch format.
   */
  def gpuConvertCachedBatchToColumnarBatch(
      input: RDD[CachedBatch],
      cacheAttributes: Seq[Attribute],
      selectedAttributes: Seq[Attribute],
      conf: SQLConf): RDD[ColumnarBatch] = {
    // When no columns are selected (e.g., count-only scan or
    // cross-join side that needs only row count), return
    // row-only batches without decoding parquet data.
    if (selectedAttributes.isEmpty) {
      return input.map {
        case parquetCB: ParquetCachedBatch =>
          new ColumnarBatch(Array.empty, parquetCB.numRows)
        case other =>
          throw new IllegalStateException(
            s"Expected ParquetCachedBatch but got ${other.getClass}")
      }
    }
    val (cachedSchemaWithNames, selectedSchemaWithNames) =
      getSupportedSchemaFromUnsupported(cacheAttributes, selectedAttributes)
    convertCachedBatchToColumnarInternal(
      input,
      cachedSchemaWithNames,
      selectedSchemaWithNames,
      selectedAttributes)
  }

  private def convertCachedBatchToColumnarInternal(
      input: RDD[CachedBatch],
      cacheAttributes: Seq[Attribute],
      selectedAttributes: Seq[Attribute],
      originalSelectedAttributes: Seq[Attribute]): RDD[ColumnarBatch] = {

    val cbRdd: RDD[ColumnarBatch] = input.map {
      case parquetCB: ParquetCachedBatch if parquetCB.sizeInBytes == 0 =>
        GpuSemaphore.acquireIfNecessary(TaskContext.get())
        // If the buffer is empty, we have cached a batch with no columns, we don't need to decode
        // it, instead just return a ColumnarBatch with only rows
        withResource(new GpuColumnarBatchBuilder(originalSelectedAttributes.toStructType,
          parquetCB.numRows)) {
          builder => builder.build(parquetCB.numRows)
        }
      case parquetCB: ParquetCachedBatch =>
        GpuSemaphore.acquireIfNecessary(TaskContext.get())
        val parquetOptions = ParquetOptions.builder()
            .includeColumn(selectedAttributes.map(_.name).asJavaCollection).build()
        val table = try {
          Table.readParquet(parquetOptions, parquetCB.buffer, 0, parquetCB.sizeInBytes)
        } catch {
          case e: Exception =>
            throw new IOException("Error when processing file " +
                s"[range: 0-${parquetCB.sizeInBytes}]", e)
        }
        withResource(table) { table =>
          withResource {
            for (i <- 0 until table.getNumberOfColumns) yield {
              ColumnCastUtil.ifTrueThenDeepConvertTypeAtoTypeB(table.getColumn(i),
                originalSelectedAttributes(i).dataType,
                (dataType, _) => dataType match {
                  case d: DecimalType if d.scale < 0 => true
                  case _ => false
                },
                (dataType, cv) => {
                  dataType match {
                    case d: DecimalType =>
                      withResource(cv.bitCastTo(DecimalUtil.createCudfDecimal(d))) {
                        _.copyToColumnVector()
                      }
                    case _ =>
                      throw new IllegalStateException("We don't cast any type besides Decimal " +
                          "with scale < 0")
                  }
                }
              )
            }
          } { col =>
            withResource(new Table(col: _*)) { t =>
              GpuColumnVector.from(t, originalSelectedAttributes.map(_.dataType).toArray)
            }
          }
        }
      case _ =>
        throw new IllegalStateException("I don't know how to convert this batch")
    }
    cbRdd
  }

  private def getSelectedSchemaFromCachedSchema(
      selectedAttributes: Seq[Attribute],
      cacheAttributes: Seq[Attribute]): Seq[Attribute] = {
    selectedAttributes.map {
      a => cacheAttributes(cacheAttributes.map(_.exprId).indexOf(a.exprId))
    }
  }

  /**
   * Convert the cached data into a ColumnarBatch taking the result data back to the host
   *
   * @param input              the cached batches that should be converted.
   * @param cacheAttributes    the attributes of the data in the batch.
   * @param selectedAttributes the fields that should be loaded from the data and the order they
   *                           should appear in the output batch.
   * @param conf               the configuration for the job.
   * @return an RDD of the input cached batches transformed into the ColumnarBatch format.
   */
  override def convertCachedBatchToColumnarBatch(input: RDD[CachedBatch],
      cacheAttributes: Seq[Attribute],
      selectedAttributes: Seq[Attribute],
      conf: SQLConf): RDD[ColumnarBatch] = {
    // When no columns are selected, return row-only batches
    if (selectedAttributes.isEmpty) {
      return input.map {
        case parquetCB: ParquetCachedBatch =>
          new ColumnarBatch(Array.empty, parquetCB.numRows)
        case other =>
          throw new IllegalStateException(
            s"Expected ParquetCachedBatch but got ${other.getClass}")
      }
    }
    val rapidsConf = new RapidsConf(conf)
    val (cachedSchemaWithNames, selectedSchemaWithNames) =
      getSupportedSchemaFromUnsupported(cacheAttributes, selectedAttributes)
    if (rapidsConf.isSqlEnabled && rapidsConf.isSqlExecuteOnGPU &&
        isSchemaSupportedByCudf(cachedSchemaWithNames)) {
      val batches = convertCachedBatchToColumnarInternal(input, cachedSchemaWithNames,
        selectedSchemaWithNames, selectedAttributes)
      val cbRdd = batches.map(batch => {
        withResource(batch) { gpuBatch =>
          val cols = GpuColumnVector.extractColumns(gpuBatch)
          new ColumnarBatch(cols.safeMap(_.copyToHost()).toArray, gpuBatch.numRows())
        }
      })
      cbRdd.mapPartitions(iter => CloseableColumnBatchIterator(iter))
    } else {
      val origSelectedAttributesWithUnambiguousNames =
        sanitizeColumnNames(selectedAttributes, selectedSchemaWithNames)
      val broadcastedConf = SparkSession.active.sparkContext.broadcast(conf.getAllConfs)
      input.mapPartitions {
        cbIter => {
          new CachedBatchIteratorConsumer(cbIter, cachedSchemaWithNames, selectedSchemaWithNames,
            cacheAttributes, origSelectedAttributesWithUnambiguousNames, broadcastedConf)
            .getColumnBatchIterator
        }
      }
    }
  }

  /**
   * Convert the cached batch into `InternalRow`s.
   *
   * @param input              the cached batches that should be converted.
   * @param cacheAttributes    the attributes of the data in the batch.
   * @param selectedAttributes the field that should be loaded from the data and the order they
   *                           should appear in the output rows.
   * @param conf               the configuration for the job.
   * @return RDD of the rows that were stored in the cached batches.
   */
  override def convertCachedBatchToInternalRow(
      input: RDD[CachedBatch],
      cacheAttributes: Seq[Attribute],
      selectedAttributes: Seq[Attribute],
      conf: SQLConf): RDD[InternalRow] = {
    val (cachedSchemaWithNames, selectedSchemaWithNames) =
      getSupportedSchemaFromUnsupported(cacheAttributes, selectedAttributes)
    val newSelectedAttributes = sanitizeColumnNames(selectedAttributes, selectedSchemaWithNames)
    val broadcastedConf = SparkSession.active.sparkContext.broadcast(conf.getAllConfs)
    input.mapPartitions {
      cbIter => {
        new CachedBatchIteratorConsumer(cbIter, cachedSchemaWithNames, selectedSchemaWithNames,
          cacheAttributes, newSelectedAttributes, broadcastedConf).getInternalRowIterator
      }
    }
  }

  /**
   * Read the data from an input file and convert the result to rows. The data will not be
   * transformed in any way. It will just be read.
   */
  private class PcbFileToRowsIterator(inputFile: ByteArrayInputFile,
      cacheAttributes: Seq[Attribute],
      hadoopConf: Configuration,
      options: ParquetReadOptions) extends Iterator[InternalRow] with AutoCloseable {
    import org.apache.parquet.io.ColumnIOFactory

    private[this] val parquetFileReader = ParquetFileReader.open(inputFile, options)
    private[this] val parquetSchema = parquetFileReader.getFooter.getFileMetaData.getSchema
    private[this] val columnIO = new ColumnIOFactory().getColumnIO(parquetSchema)

    private[this] var pages: org.apache.parquet.column.page.PageReadStore = _
    private[this] var rowsLeftInRowGroup: Long = _
    private[this] var recordReader: org.apache.parquet.io.RecordReader[InternalRow] = _
    // Prime the data with the first row group
    goToNextRowGroup

    private[this] def goToNextRowGroup: Unit = {
      pages = parquetFileReader.readNextRowGroup()
      if (pages == null) {
        rowsLeftInRowGroup = 0
        recordReader = null
      } else {
        rowsLeftInRowGroup = pages.getRowCount
        recordReader = columnIO.getRecordReader(pages, new ParquetRecordMaterializer(parquetSchema,
          cacheAttributes.toStructType,
          new ParquetToSparkSchemaConverter(hadoopConf), None /*convertTz*/ ,
          LegacyBehaviorPolicyShim.CORRECTED_STR))
      }
    }

    override def hasNext: Boolean = pages != null

    override def next(): InternalRow = {
      if (!hasNext) {
        throw new NoSuchElementException()
      }
      val ret = recordReader.read()
      rowsLeftInRowGroup -= 1
      if (rowsLeftInRowGroup <= 0) {
        goToNextRowGroup
        if (pages == null) {
          close()
        }
      }
      ret
    }

    override def close(): Unit = {
      parquetFileReader.close()
    }
  }

  object PcbToRowsIterator {
    def apply(cbIter: Iterator[CachedBatch],
        cacheAttributes: Seq[Attribute],
        hadoopConf: Configuration,
        options: ParquetReadOptions): Iterator[InternalRow] = {
      new PcbToRowsIterator(cbIter, cacheAttributes, hadoopConf, options).flatten
    }
  }

  /**
   * Read the data from the CachedBatch iterator and return a row iterator for each batch.
   */
  private class PcbToRowsIterator private(
      cbIter: Iterator[CachedBatch],
      cacheAttributes: Seq[Attribute],
      hadoopConf: Configuration,
      options: ParquetReadOptions) extends Iterator[Iterator[InternalRow]] {
    val pcbIter = cbIter.map(_.asInstanceOf[ParquetCachedBatch])
    override def hasNext: Boolean = pcbIter.hasNext

    override def next(): Iterator[InternalRow] = {
      val parquetCachedBatch = pcbIter.next()
      if (parquetCachedBatch.sizeInBytes == 0) {
        // Edge case where we have stored an empty parquet file possibly a dataframe with no
        // columns
        new ColumnarBatch(null, parquetCachedBatch.numRows).rowIterator().asScala
      } else {
        val inputFile = new ByteArrayInputFile(parquetCachedBatch.buffer)
        new PcbFileToRowsIterator(inputFile, cacheAttributes, hadoopConf, options)
      }
    }
  }

  /**
   * This iterator wraps an `Iterator[InternalRow]` and will transform the rows so that
   * unsupported types are converted back to the original type that was cached.
   */
  private class UnsupportedRowTransformIterator(
      rowIter: Iterator[InternalRow],
      cacheAttributes: Seq[Attribute],
      origCacheSchema: Seq[Attribute],
      origRequestedSchema: Seq[Attribute]) extends UnsupportedDataHandlerIterator {

    val newRow = new GenericInternalRow(cacheAttributes.length)

    lazy val unsafeProjection =
      GenerateUnsafeProjection.generate(origRequestedSchema, origCacheSchema)

    override def hasNext: Boolean = rowIter.hasNext

    override def next(): InternalRow = {
      //read a row and convert it to what the caller is expecting
      val row = rowIter.next()
      handleInternalRow(origCacheSchema, row, newRow)
      unsafeProjection.apply(newRow)
    }

    override def handleInterval(
        data: SpecializedGetters,
        index: Int): CalendarInterval = {
      if (data.isNullAt(index)) {
        null
      } else {
        val structData = data.getStruct(index, 3)
        new CalendarInterval(structData.getInt(0),
          structData.getInt(1), structData.getLong(2))
      }
    }

    override def handleInternalRow(
        schema: Seq[Attribute],
        row: InternalRow,
        newRow: InternalRow): Unit = {
      schema.indices.foreach { index =>
        val dataType = schema(index).dataType
        if (row.isNullAt(index)) {
          newRow.setNullAt(index)
        } else {
          dataType match {
            case s@StructType(_) =>
              val supportedSchema =
                PCBSSchemaHelper.getSupportedDataType(dataType).asInstanceOf[StructType]
              val structRow =
                handleStruct(row.getStruct(index, supportedSchema.size), s, s)
              newRow.update(index, structRow)

            case a@ArrayType(_, _) =>
              val arrayData = row.getArray(index)
              newRow.update(index, handleArray(a.elementType, arrayData))

            case MapType(keyType, valueType, _) =>
              val mapData = row.getMap(index)
              newRow.update(index, handleMap(keyType, valueType, mapData))

            case CalendarIntervalType =>
              val interval = handleInterval(row, index)
              if (interval == null) {
                newRow.setNullAt(index)
              } else {
                newRow.setInterval(index, interval)
              }
            case d: DecimalType =>
              if (row.isNullAt(index)) {
                newRow.setDecimal(index, null, d.precision)
              } else {
                val dec = if (d.precision <= Decimal.MAX_INT_DIGITS) {
                  Decimal(row.getInt(index).toLong, d.precision, d.scale)
                } else {
                  Decimal(row.getLong(index), d.precision, d.scale)
                }
                newRow.update(index, dec)
              }
            case NullType =>
              newRow.setNullAt(index)
            case _ =>
              newRow.update(index, row.get(index, dataType))
          }
        }
      }
    }
  }

  private abstract class UnsupportedDataHandlerIterator extends Iterator[InternalRow] {

    def handleInternalRow(schema: Seq[Attribute], row: InternalRow, newRow: InternalRow): Unit

    def handleInterval(data: SpecializedGetters, index: Int): Any

    def handleStruct(
        data: InternalRow,
        origSchema: StructType,
        supportedSchema: StructType): InternalRow = {
      val structRow = InternalRow.fromSeq(supportedSchema)
      handleInternalRow(origSchema.map(field =>
        AttributeReference(field.name, field.dataType, field.nullable)()), data, structRow)
      structRow
    }

    def handleMap(
        keyType: DataType,
        valueType: DataType,
        mapData: MapData): MapData = {
      val keyData = mapData.keyArray()
      val newKeyData = handleArray(keyType, keyData)
      val valueData = mapData.valueArray()
      val newValueData = handleArray(valueType, valueData)
      new ArrayBasedMapData(newKeyData, newValueData)
    }

    def handleArray(
        dataType: DataType,
        arrayData: ArrayData): ArrayData = {
      dataType match {
        case s@StructType(_) =>
          val listBuffer = new ListBuffer[InternalRow]()
          val supportedSchema =
            PCBSSchemaHelper.getSupportedDataType(dataType).asInstanceOf[StructType]
          arrayData.foreach(supportedSchema, (_, data) => {
            val structRow =
              handleStruct(data.asInstanceOf[InternalRow], s, s)
            listBuffer += structRow.copy()
          })
          new GenericArrayData(listBuffer)

        case ArrayType(elementType, _) =>
          val arrayList = new ListBuffer[Any]()
          scala.Range(0, arrayData.numElements()).foreach { i =>
            val subArrayData = arrayData.getArray(i)
            arrayList.append(handleArray(elementType, subArrayData))
          }
          new GenericArrayData(arrayList)

        case m@MapType(_, _, _) =>
          val mapList =
            new ListBuffer[Any]()
          scala.Range(0, arrayData.numElements()).foreach { i =>
            val mapData = arrayData.getMap(i)
            mapList.append(handleMap(m.keyType, m.valueType, mapData))
          }
          new GenericArrayData(mapList)

        case CalendarIntervalType =>
          val citList = new ListBuffer[Any]()
          scala.Range(0, arrayData.numElements()).foreach { i =>
            val citRow = handleInterval(arrayData, i)
            citList += citRow
          }
          new GenericArrayData(citList)

        case _ =>
          arrayData
      }
    }
  }

  /**
   * Consumes the Iterator[CachedBatch] to return either Iterator[ColumnarBatch] or
   * Iterator[InternalRow]
   */
  private class CachedBatchIteratorConsumer(
      cbIter: Iterator[CachedBatch],
      cacheAttributes: Seq[Attribute],
      selectedAttributes: Seq[Attribute],
      origCacheSchema: Seq[Attribute],
      origRequestedSchema: Seq[Attribute],
      sharedConf: Broadcast[Map[String, String]]) {

    private val conf: SQLConf = getConfFromMap(sharedConf)
    private val hadoopConf: Configuration = getHadoopConf(origRequestedSchema.toStructType, conf)
    private val options: ParquetReadOptions = HadoopReadOptions.builder(hadoopConf).build()
    private lazy val hasUnsupportedType = origCacheSchema.exists { field =>
      !PCBSSchemaHelper.isTypeSupportedByParquet(field.dataType)
    }

    private lazy val allSupportedUnsafeProjection =
      GenerateUnsafeProjection.generate(selectedAttributes, cacheAttributes)

    def getInternalRowIterator: Iterator[InternalRow] = {
      val rawRows = PcbToRowsIterator(cbIter, cacheAttributes, hadoopConf,
        options)
      if (hasUnsupportedType) {
        new UnsupportedRowTransformIterator(rawRows, cacheAttributes, origCacheSchema,
          origRequestedSchema)
      } else {
        // Convert the result into unsafe rows, and drop columns we don't want to read
        rawRows.map(allSupportedUnsafeProjection)
      }
    }

    /**
     * This method returns a ColumnarBatch iterator over a CachedBatch.
     * Each CachedBatch => ColumnarBatch is a 1-1 conversion so its pretty straight forward
     */
    def getColumnBatchIterator: Iterator[ColumnarBatch] = new Iterator[ColumnarBatch] {
      var iter = getIterator

      def getIterator: Iterator[ColumnarBatch] = {
        if (!cbIter.hasNext) {
          Iterator.empty
        } else {
          val cachedBatch = cbIter.next().asInstanceOf[ParquetCachedBatch]
          if (cachedBatch.sizeInBytes == 0) {
            // Edge case where we have stored an empty parquet file possibly a dataframe with no
            // columns
            List(new ColumnarBatch(null, cachedBatch.numRows)).iterator
          } else {
            new ShimCurrentBatchIterator(
              cachedBatch,
              conf,
              selectedAttributes,
              options, hadoopConf)
          }
        }
      }

      override def hasNext: Boolean = {
        // go over the batch and get the next non-degenerate iterator
        // and return if it hasNext
        while ((iter == null || !iter.hasNext) && cbIter.hasNext) {
          iter = getIterator
        }
        iter != null && iter.hasNext
      }

      override def next(): ColumnarBatch = {
        // will return the next ColumnarBatch if hasNext() is true, otherwise throw
        if (hasNext) {
          iter.next()
        } else {
          throw new NoSuchElementException("no elements found")
        }
      }
    }
  }

  private def getConfFromMap(sharedConf: Broadcast[Map[String, String]]): SQLConf = {
    val conf = new SQLConf()
    sharedConf.value.foreach { case (k, v) => conf.setConfString(k, v) }
    conf
  }

  def getBytesAllowedPerBatch(conf: SQLConf): Long = {
    val gpuBatchSize = RapidsConf.GPU_BATCH_SIZE_BYTES.get(conf)
    // we are rough estimating 0.5% as meta_data_size. we can do better estimation in future
    val approxMetaDataSizeBytes = gpuBatchSize * 0.5/100
    (gpuBatchSize - approxMetaDataSizeBytes).toLong
  }

  /**
   * This is a private helper class to return Iterator to convert InternalRow or ColumnarBatch to
   * CachedBatch. There is no type checking so if the type of T is anything besides InternalRow
   * or ColumnarBatch then the behavior is undefined.
   *
   * @param iter             - an iterator over InternalRow or ColumnarBatch
   * @param cachedAttributes - Schema of the cached batch
   * @param sharedConf       - SQL conf
   * @tparam T - Strictly either InternalRow or ColumnarBatch
   */
  private[rapids] class CachedBatchIteratorProducer[T](
      iter: Iterator[T],
      cachedAttributes: Seq[Attribute],
      origCachedAttributes: Seq[Attribute],
      sharedConf: Broadcast[Map[String, String]]) {

    val conf: SQLConf = getConfFromMap(sharedConf)
    val bytesAllowedPerBatch = getBytesAllowedPerBatch(conf)
    val hadoopConf: Configuration = getHadoopConf(cachedAttributes.toStructType, conf)

    def getInternalRowToCachedBatchIterator: Iterator[CachedBatch] = {
      new InternalRowToCachedBatchIterator
    }

    def getColumnarBatchToCachedBatchIterator: Iterator[CachedBatch] = {
      new ColumnarBatchToCachedBatchIterator
    }

    /**
     * This class produces an Iterator[CachedBatch] from Iterator[InternalRow]. This is a n-1
     * relationship. Each partition represents a single parquet file, so we encode it
     * and return the CachedBatch when next is called.
     */
    class InternalRowToCachedBatchIterator extends Iterator[CachedBatch]() with AutoCloseable {

      var parquetOutputFileFormat = new ParquetOutputFileFormat()

      // For testing only
      private[rapids] def setParquetOutputFileFormat(p: ParquetOutputFileFormat): Unit = {
        parquetOutputFileFormat = p
      }

      // is there a type that spark doesn't support by default in the schema?
      val hasUnsupportedType: Boolean = origCachedAttributes.exists { attribute =>
        !PCBSSchemaHelper.isTypeSupportedByParquet(attribute.dataType)
      }

      override def close(): Unit = {
        // We don't have any off heap vectors, so nothing to clean
      }

      def getIterator: Iterator[InternalRow] = {
        if (!hasUnsupportedType) {
          iter.asInstanceOf[Iterator[InternalRow]]
        } else {
          new UnsupportedDataHandlerIterator {

            val wrappedIter: Iterator[InternalRow] = iter.asInstanceOf[Iterator[InternalRow]]

            val newRow: InternalRow = InternalRow.fromSeq(cachedAttributes)

            override def hasNext: Boolean = wrappedIter.hasNext

            override def next(): InternalRow = {
              val row = wrappedIter.next()
              handleInternalRow(origCachedAttributes, row, newRow)
              newRow
            }

            override def handleInterval(
                data: SpecializedGetters,
                index: Int): InternalRow = {
              val citRow = InternalRow(IntegerType, IntegerType, LongType)
              if (data.isNullAt(index)) {
                null
              } else {
                val cit = data.getInterval(index)
                citRow.setInt(0, cit.months)
                citRow.setInt(1, cit.days)
                citRow.setLong(2, cit.microseconds)
                citRow
              }
            }

            override def handleInternalRow(
                schema: Seq[Attribute],
                row: InternalRow,
                newRow: InternalRow): Unit = {
              schema.indices.foreach { index =>
                val dataType = schema(index).dataType
                if (row.isNullAt(index)) {
                  newRow.setNullAt(index)
                } else {
                  dataType match {
                    case s@StructType(_) =>
                      val newSchema =
                        PCBSSchemaHelper.getSupportedDataType(dataType).asInstanceOf[StructType]
                      val structRow =
                        handleStruct(row.getStruct(index, s.fields.length), s, newSchema)
                      newRow.update(index, structRow)

                    case ArrayType(arrayDataType, _) =>
                      val arrayData = row.getArray(index)
                      val newArrayData = handleArray(arrayDataType, arrayData)
                      newRow.update(index, newArrayData)

                    case MapType(keyType, valueType, _) =>
                      val mapData = row.getMap(index)
                      val map = handleMap(keyType, valueType, mapData)
                      newRow.update(index, map)

                    case CalendarIntervalType =>
                      val structData: InternalRow = handleInterval(row, index)
                      if (structData == null) {
                        newRow.setNullAt(index)
                      } else {
                        newRow.update(index, structData)
                      }

                    case d: DecimalType if d.scale < 0 =>
                      if (d.precision <= Decimal.MAX_INT_DIGITS) {
                        newRow.update(index, row.getDecimal(index, d.precision, d.scale)
                            .toUnscaledLong.toInt)
                      } else {
                        newRow.update(index, row.getDecimal(index, d.precision, d.scale)
                            .toUnscaledLong)
                      }

                    case _ =>
                      newRow.update(index, row.get(index, dataType))
                  }
                }
              }
            }
          }
        }
      }

      override def hasNext: Boolean = {
        if (queue.isEmpty && !iter.hasNext) {
          // iterator has exhausted, we should clean up
          close()
        }
        queue.nonEmpty || iter.hasNext
      }

      private val queue = new mutable.Queue[CachedBatch]()

      //estimate the size of a row
      val estimatedSize: Int = cachedAttributes.map { attr =>
        attr.dataType.defaultSize
      }.sum

      override def next(): CachedBatch = {
        if (queue.isEmpty) {
          queue ++= convertRowIteratorToCachedBatch(getIterator)
        }
        if (!queue.isEmpty) {
          queue.dequeue()
        } else {
          throw new IllegalStateException("Encountered an empty batch")
        }
      }

      protected def convertRowIteratorToCachedBatch(
          rowIterator: Iterator[InternalRow]): mutable.Queue[CachedBatch] = {
        val queue = new mutable.Queue[CachedBatch]()
        // to store a row if we have read it but there is no room in the parquet file to put it
        // we will put it in the next CachedBatch
        var leftOverRow: Option[InternalRow] = None
        while (rowIterator.hasNext || leftOverRow.nonEmpty) {
          // Each partition will be a single parquet file
          var rows = 0
          // at least a single block
          val stream = new ByteArrayOutputStream(ByteArrayOutputFile.BLOCK_SIZE)
          val outputFile: OutputFile = new ByteArrayOutputFile(stream)
          conf.setConfString(SparkShimImpl.parquetRebaseWriteKey,
            LegacyBehaviorPolicyShim.CORRECTED_STR)
          if (cachedAttributes.isEmpty) {
            // The schema is empty, most probably it is because of the edge case where there
            // are no columns in the Dataframe but has rows so let's create an empty CachedBatch
            // with rows
            queue += new ParquetCachedBatch(rowIterator.size, new Array[Byte](0))
            return queue
          }
          val recordWriter = SQLConf.withExistingConf(conf) {
            parquetOutputFileFormat.getRecordWriter(outputFile, hadoopConf)
          }
          var totalSize = 0
          while ((rowIterator.hasNext || leftOverRow.nonEmpty)
            && totalSize < bytesAllowedPerBatch) {

            val row = if (leftOverRow.nonEmpty) {
              val a = leftOverRow.get
              leftOverRow = None // reset value
              a
            } else {
              rowIterator.next()
            }
            totalSize += {
              row match {
                case r: UnsafeRow =>
                  r.getSizeInBytes
                case _ =>
                  estimatedSize
              }
            }
            if (totalSize <= bytesAllowedPerBatch) {
              rows += 1
              if (rows < 0) {
                throw new IllegalStateException("CachedBatch doesn't support rows larger " +
                  "than Int.MaxValue")
              }
              recordWriter.write(null, row)
            } else {
              leftOverRow = Some(if (row.isInstanceOf[UnsafeRow]) {
                row.copy()
              } else {
                row
              })
            }
          }
          // passing null as context isn't used in this method
          recordWriter.close(null)
          queue += ParquetCachedBatch(rows, stream.toByteArray)
        }
        queue
      }
    }

    /**
     * This class produces an Iterator[CachedBatch] from Iterator[ColumnarBatch]. This is a 1-1
     * relationship. Each ColumnarBatch is converted to a single ParquetCachedBatch when next()
     * is called on this iterator
     */
    class ColumnarBatchToCachedBatchIterator extends InternalRowToCachedBatchIterator
        with AutoCloseable {
      val hostBatches = new ListBuffer[ColumnarBatch]()

      // Don't install the callback if in a unit test
      Option(TaskContext.get()).foreach { tc =>
        onTaskCompletion(tc) {
          hostBatches.foreach(_.close())
          hostBatches.clear()
          close()
        }
      }

      override def next(): CachedBatch = myIter.next()

      override def hasNext: Boolean = myIter.hasNext

      val myIter = iter.asInstanceOf[Iterator[ColumnarBatch]].flatMap { batch =>
        val hostBatch = if (batch.column(0).isInstanceOf[GpuColumnVector]) {
          withResource(batch) { batch =>
            new ColumnarBatch(batch.safeMap(_.copyToHost()).toArray, batch.numRows())
          }
        } else {
          batch
        }
        hostBatches += hostBatch
        if (batch.numCols() == 0) {
          List(ParquetCachedBatch(batch.numRows(), new Array[Byte](0)))
        } else {
          convertRowIteratorToCachedBatch(hostBatch.rowIterator().asScala).toList
        }
      }

      override def close(): Unit = {
        hostBatches.foreach(_.close())
        hostBatches.clear()
      }
    }
  }

  // We want to change the original schema to have the new names as well
  private def sanitizeColumnNames(originalSchema: Seq[Attribute],
      schemaToCopyNamesFrom: Seq[Attribute]): Seq[Attribute] = {
    originalSchema.zip(schemaToCopyNamesFrom).map {
      case (origAttribute, newAttribute) => origAttribute.withName(newAttribute.name)
    }
  }

  private def getSupportedSchemaFromUnsupported(
      cachedAttributes: Seq[Attribute],
      requestedAttributes: Seq[Attribute] = Seq.empty): (Seq[Attribute], Seq[Attribute]) = {

    val newCachedAttributes =
      PCBSSchemaHelper.getSupportedSchemaFromUnsupported(cachedAttributes)

    val newRequestedAttributes =
      getSelectedSchemaFromCachedSchema(requestedAttributes, newCachedAttributes)

    (newCachedAttributes, newRequestedAttributes)
  }

  private def getHadoopConf(requestedSchema: StructType,
      sqlConf: SQLConf): Configuration = {

    val hadoopConf = new Configuration(false)
    hadoopConf.set(ParquetInputFormat.READ_SUPPORT_CLASS, classOf[ParquetReadSupport].getName)
    hadoopConf.set(
      ParquetReadSupport.SPARK_ROW_REQUESTED_SCHEMA,
      requestedSchema.json)
    hadoopConf.set(
      ParquetWriteSupport.SPARK_ROW_SCHEMA,
      requestedSchema.json)
    hadoopConf.set(
      SQLConf.SESSION_LOCAL_TIMEZONE.key,
      sqlConf.sessionLocalTimeZone)

    hadoopConf.setBoolean(SQLConf.PARQUET_BINARY_AS_STRING.key, false)
    hadoopConf.setBoolean(SQLConf.PARQUET_INT96_AS_TIMESTAMP.key, false)
    hadoopConf.setBoolean(SQLConf.CASE_SENSITIVE.key, false)

    hadoopConf.set(SparkShimImpl.parquetRebaseWriteKey, LegacyBehaviorPolicyShim.CORRECTED_STR)

    hadoopConf.set(SQLConf.PARQUET_OUTPUT_TIMESTAMP_TYPE.key,
      SQLConf.ParquetOutputTimestampType.TIMESTAMP_MICROS.toString)

    hadoopConf.setBoolean(SQLConf.PARQUET_WRITE_LEGACY_FORMAT.key, false)

    hadoopConf.set(ParquetOutputFormat.WRITER_VERSION,
      ParquetProperties.WriterVersion.PARQUET_1_0.toString)

    ParquetWriteSupport.setSchema(requestedSchema, hadoopConf)

    // From 3.3.0, Spark will check this filed ID config
    ParquetFieldIdShims.setupParquetFieldIdWriteConfig(hadoopConf, sqlConf)

    // timestamp_NTZ flag is introduced in Spark 3.4.0
    ParquetTimestampNTZShims.setupTimestampNTZConfig(hadoopConf, sqlConf)

    // From 3.3.2, Spark schema converter needs this conf
    ParquetLegacyNanoAsLongShims.setupLegacyParquetNanosAsLongForPCBS(hadoopConf)

    // From 4.1.1, Spark will check this variant config
    ParquetVariantShims.setupParquetVariantConfig(hadoopConf, sqlConf)

    hadoopConf
  }

  /**
   * Convert an `RDD[InternalRow]` into an `RDD[CachedBatch]` in preparation for caching the data.
   * We use the RowToColumnarIterator and convert each batch at a time
   *
   * @param input        the input `RDD` to be converted.
   * @param schema       the schema of the data being stored.
   * @param storageLevel where the data will be stored.
   * @param conf         the config for the query.
   * @return The data converted into a format more suitable for caching.
   */
  override def convertInternalRowToCachedBatch(
      input: RDD[InternalRow],
      schema: Seq[Attribute],
      storageLevel: StorageLevel,
      conf: SQLConf): RDD[CachedBatch] = {

    val rapidsConf = new RapidsConf(conf)
    val useCompression = conf.useCompression
    val bytesAllowedPerBatch = getBytesAllowedPerBatch(conf)
    val (schemaWithUnambiguousNames, _) = getSupportedSchemaFromUnsupported(schema)
    if (rapidsConf.isSqlEnabled && rapidsConf.isSqlExecuteOnGPU &&
        isSchemaSupportedByCudf(schema)) {
      val structSchema = schemaWithUnambiguousNames.toStructType
      val converters = new GpuRowToColumnConverter(structSchema)
      val batchSizeBytes = rapidsConf.gpuTargetBatchSizeBytes
      val enableR2cRetry = rapidsConf.isR2cRetryEnabled
      val columnarBatchRdd = input.mapPartitions(iter => {
        new RowToColumnarIterator(iter, structSchema, RequireSingleBatch, batchSizeBytes,
        converters, enableR2cRetry)
      })
      columnarBatchRdd.flatMap(cb => {
        withResource(cb)(cb => compressColumnarBatchWithParquet(cb, structSchema,
          schema.toStructType, bytesAllowedPerBatch, useCompression))
      })
    } else {
      val broadcastedConf = SparkSession.active.sparkContext.broadcast(conf.getAllConfs)
      input.mapPartitions {
        cbIter =>
          new CachedBatchIteratorProducer[InternalRow](cbIter, schemaWithUnambiguousNames, schema,
            broadcastedConf).getInternalRowToCachedBatchIterator
      }
    }
  }

  override def buildFilter(
      predicates: Seq[Expression],
      cachedAttributes: Seq[Attribute]): (Int, Iterator[CachedBatch]) => Iterator[CachedBatch] = {
    //essentially a noop
    (_: Int, b: Iterator[CachedBatch]) => b
  }
}

/**
 * Similar to ParquetFileFormat
 */
private[rapids] class ParquetOutputFileFormat {

  @scala.annotation.nowarn(
    "msg=constructor .* in class .* is deprecated"
  )
  def getRecordWriter(output: OutputFile, conf: Configuration): RecordWriter[Void, InternalRow] = {
    import ParquetOutputFormat._

    val blockSize = getLongBlockSize(conf)
    val maxPaddingSize =
      conf.getInt(MAX_PADDING_BYTES, ParquetWriter.MAX_PADDING_SIZE_DEFAULT)
    val validating = getValidation(conf)

    val writeSupport = new ParquetWriteSupport().asInstanceOf[WriteSupport[InternalRow]]
    val init = writeSupport.init(conf): @scala.annotation.nowarn(
      "cat=deprecation&msg=method init in class WriteSupport is deprecated"
    )
    val writer = new ParquetFileWriter(output, init.getSchema,
      Mode.CREATE, blockSize, maxPaddingSize)
    writer.start()

    val writerVersion =
      ParquetProperties.WriterVersion.fromString(conf.get(ParquetOutputFormat.WRITER_VERSION,
        ParquetProperties.WriterVersion.PARQUET_1_0.toString))

    val codecFactory = new CodecFactory(conf, getPageSize(conf))

    new ParquetRecordWriter[InternalRow](writer, writeSupport, init.getSchema,
      init.getExtraMetaData, blockSize, getPageSize(conf),
      codecFactory.getCompressor(CompressionCodecName.UNCOMPRESSED), getDictionaryPageSize(conf),
      getEnableDictionary(conf), validating, writerVersion,
      ParquetOutputFileFormat.getMemoryManager(conf))

  }
}

private object ParquetOutputFileFormat {
  var memoryManager: MemoryManager = _
  val DEFAULT_MEMORY_POOL_RATIO: Float = 0.95f
  val DEFAULT_MIN_MEMORY_ALLOCATION: Long = 1 * 1024 * 1024 // 1MB

  def getMemoryManager(conf: Configuration): MemoryManager = {
    synchronized {
      if (memoryManager == null) {
        import ParquetOutputFormat._
        val maxLoad = conf.getFloat(MEMORY_POOL_RATIO, DEFAULT_MEMORY_POOL_RATIO)
        val minAllocation = conf.getLong(MIN_MEMORY_ALLOCATION, DEFAULT_MIN_MEMORY_ALLOCATION)
        memoryManager = new MemoryManager(maxLoad, minAllocation)
      }
    }
    memoryManager
  }
}
