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

package com.nvidia.spark.rapids.delta.delta24x

import java.net.URI

import ai.rapids.cudf.{ColumnVector => CudfColumnVector, DeletionVector, HostMemoryBuffer,
  ParquetOptions, Scalar}
import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.GpuMetric._
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.delta.{GpuDeltaParquetFileFormat,
  RapidsDeletionVectorRowCountUtils}
import com.nvidia.spark.rapids.delta.GpuDeltaParquetFileFormatUtils.{METADATA_ROW_DEL_COL,
  METADATA_ROW_IDX_COL}
import com.nvidia.spark.rapids.delta.common.{DeletionVectorOutputColumns,
  MakeParquetTableWithDVProducer}
import com.nvidia.spark.rapids.jni.fileio.RapidsFileIO
import com.nvidia.spark.rapids.parquet._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.metadata.BlockMetaData
import org.apache.parquet.schema.MessageType

import org.apache.spark.TaskContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.PartitionReaderFactory
import org.apache.spark.sql.delta.{DeltaColumnMappingMode, IdMapping, RowIndexFilterType}
import org.apache.spark.sql.delta.DeltaParquetFileFormat.DeletionVectorDescriptorWithFilterType
import org.apache.spark.sql.delta.actions.{DeletionVectorDescriptor, Metadata}
import org.apache.spark.sql.delta.deletionvectors.{RoaringBitmapArray, RoaringBitmapArrayFormat}
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.GpuFileSourceScanExec
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector => SparkVector}
import org.apache.spark.util.SerializableConfiguration

private case class Delta24xDeletionVectorBitmapInfo(
    bitmap: RoaringBitmapArray,
    bytes: Array[Byte],
    isRetention: Boolean)

private case class Delta24xRowSelection(
    bitmap: RoaringBitmapArray,
    isRetention: Boolean,
    numRowsAlive: Long,
    rowGroupOffsets: Array[Long],
    rowGroupNumRows: Array[Int])

private object Delta24xDeletionVectorUtils {
  private val DELTA_BITMAP_MAGIC_NUMBER_BYTE_SIZE = 4

  private def lookup(
      file: PartitionedFile,
      delVecs: Option[Broadcast[Map[URI, DeletionVectorDescriptorWithFilterType]]])
  : Option[DeletionVectorDescriptorWithFilterType] = {
    delVecs.flatMap(_.value.get(new URI(file.filePath.toString)))
  }

  def bitmapInfo(
      file: PartitionedFile,
      delVecs: Option[Broadcast[Map[URI, DeletionVectorDescriptorWithFilterType]]],
      metrics: Option[Map[String, GpuMetric]] = None): Delta24xDeletionVectorBitmapInfo = {
    lookup(file, delVecs) match {
      case Some(dv) =>
        require(dv.descriptor.storageType == DeletionVectorDescriptor.INLINE_DV_MARKER,
          "Low shuffle merge requires inline deletion vectors")
        val filterType = dv.filterType
        require(filterType == RowIndexFilterType.IF_CONTAINED ||
          filterType == RowIndexFilterType.IF_NOT_CONTAINED,
          s"Unexpected low shuffle merge deletion-vector filter type: $filterType")
        val descriptorBytes = dv.descriptor.inlineData
        require(descriptorBytes.length >= DELTA_BITMAP_MAGIC_NUMBER_BYTE_SIZE,
          "Invalid inline deletion vector")
        val bitmap = RoaringBitmapArray.readFrom(descriptorBytes)
        // cuDF consumes the portable Roaring serialization without Delta's four-byte header.
        val portableBytes = bitmap.serializeAsByteArray(RoaringBitmapArrayFormat.Portable)
        metrics.foreach(_(DELETION_VECTOR_SIZE) += descriptorBytes.length)
        Delta24xDeletionVectorBitmapInfo(
          bitmap,
          portableBytes.drop(DELTA_BITMAP_MAGIC_NUMBER_BYTE_SIZE),
          filterType == RowIndexFilterType.IF_NOT_CONTAINED)
      case None if delVecs.isDefined =>
        throw new IllegalStateException(
          s"Missing low shuffle merge deletion vector for ${file.filePath}")
      case None =>
        val deltaBytes = new RoaringBitmapArray()
          .serializeAsByteArray(RoaringBitmapArrayFormat.Portable)
        Delta24xDeletionVectorBitmapInfo(new RoaringBitmapArray(),
          deltaBytes.drop(DELTA_BITMAP_MAGIC_NUMBER_BYTE_SIZE), isRetention = false)
    }
  }

  def rowGroupMetadata(blocks: collection.Seq[BlockMetaData]): (Array[Long], Array[Int]) =
    RapidsDeletionVectorRowCountUtils.getRowGroupMetadata(blocks)

  def numRowsAlive(
      bitmapInfo: Delta24xDeletionVectorBitmapInfo,
      rowGroupOffsets: Array[Long],
      rowGroupNumRows: Array[Int]): Long = {
    val totalRows = rowGroupNumRows.map(_.toLong).sum
    val markedRows = RapidsDeletionVectorRowCountUtils.countMarkedRows(
      bitmapInfo.bitmap.cardinality, rowGroupOffsets, rowGroupNumRows) { countMarkedRow =>
      bitmapInfo.bitmap.forEach { markedIndex: Long =>
        countMarkedRow(markedIndex)
      }
    }
    require(markedRows <= totalRows,
      s"Deletion-vector cardinality ($markedRows) exceeds selected row count ($totalRows)")
    if (bitmapInfo.isRetention) markedRows else totalRows - markedRows
  }

  def rowSelection(
      bitmapInfo: Delta24xDeletionVectorBitmapInfo,
      rowGroupOffsets: Array[Long],
      rowGroupNumRows: Array[Int]): Delta24xRowSelection = {
    Delta24xRowSelection(bitmapInfo.bitmap, bitmapInfo.isRetention,
      numRowsAlive(bitmapInfo, rowGroupOffsets, rowGroupNumRows),
      rowGroupOffsets, rowGroupNumRows)
  }

  private def filterRowIndexes(
      rowIndexes: CudfColumnVector,
      bitmap: RoaringBitmapArray,
      isRetention: Boolean): CudfColumnVector = {
    def filter(mask: CudfColumnVector): CudfColumnVector = {
      withResource(new ai.rapids.cudf.Table(rowIndexes)) { table =>
        withResource(table.filter(mask)) { filtered =>
          filtered.getColumn(0).incRefCount()
        }
      }
    }

    withResource(CudfColumnVector.fromLongs(bitmap.toArray: _*)) { markedIndexes =>
      withResource(rowIndexes.contains(markedIndexes)) { marked =>
        if (isRetention) {
          filter(marked)
        } else {
          withResource(marked.not()) { unmarked =>
            filter(unmarked)
          }
        }
      }
    }
  }

  private def selectedRowIndexes(selection: Delta24xRowSelection): CudfColumnVector = {
    require(selection.rowGroupOffsets.length == selection.rowGroupNumRows.length,
      "Deletion-vector row-group metadata must be aligned")
    val rowGroupRanges = selection.rowGroupOffsets.zip(selection.rowGroupNumRows)
    require(rowGroupRanges.sliding(2).forall {
      case Array((offset, numRows), (nextOffset, _)) => offset + numRows == nextOffset
      case _ => true
    }, "Metadata-only deletion-vector reads require contiguous row groups")
    val totalRows = selection.rowGroupNumRows.map(_.toLong).sum
    if (totalRows == 0) {
      CudfColumnVector.fromLongs()
    } else {
      withResource(Scalar.fromLong(selection.rowGroupOffsets.head)) { start =>
        withResource(CudfColumnVector.sequence(start, Math.toIntExact(totalRows))) { rowIndexes =>
          filterRowIndexes(rowIndexes, selection.bitmap, selection.isRetention)
        }
      }
    }
  }

  /**
   * Builds a batch for a metadata-only Parquet read, where there are no physical columns for
   * cuDF to decode. For each input selection, it generates the source row indexes covered by its
   * contiguous row groups and applies the deletion-vector bitmap. It concatenates the surviving
   * indexes in input order, then materializes the requested synthetic metadata columns; any other
   * fields are null placeholders that the caller may replace with partition values.
   *
   * @param readDataSchema schema and column order of the returned batch
   * @param selections deletion-vector selections for each input file or chunk, in output order
   * @param outputColumns positions and values of row-index and row-status metadata columns
   * @return a caller-owned GPU batch containing only rows that survive the selections
   */
  def metadataBatch(
      readDataSchema: StructType,
      selections: Array[Delta24xRowSelection],
      outputColumns: DeletionVectorOutputColumns): ColumnarBatch = {
    require(selections.nonEmpty, "Missing row selections for metadata-only Parquet read")
    val selectedByFile = selections.safeMap(selectedRowIndexes)
    val selected = withResource(selectedByFile) { columns =>
      if (columns.length == 1) {
        columns.head.incRefCount()
      } else {
        CudfColumnVector.concatenate(columns: _*)
      }
    }
    withResource(selected) { rowIndexes =>
      val numRows = Math.toIntExact(rowIndexes.getRowCount)
      val columns = readDataSchema.fields.zipWithIndex.safeMap { case (field, index) =>
        if (outputColumns.rowIndexColumn.contains(index)) {
          GpuColumnVector.from(rowIndexes.incRefCount(), field.dataType)
            .asInstanceOf[SparkVector]
        } else if (outputColumns.deletedColumn.contains(index)) {
          withResource(Scalar.fromBool(true)) { value =>
            GpuColumnVector.from(value, numRows, field.dataType).asInstanceOf[SparkVector]
          }
        } else if (outputColumns.keptColumn.contains(index)) {
          withResource(Scalar.fromBool(false)) { value =>
            GpuColumnVector.from(value, numRows, field.dataType).asInstanceOf[SparkVector]
          }
        } else {
          GpuColumnVector.fromNull(numRows, field.dataType).asInstanceOf[SparkVector]
        }
      }
      new ColumnarBatch(columns, numRows)
    }
  }

  def serializedBitmap(bytes: Array[Byte]): HostMemoryBuffer = {
    closeOnExcept(HostMemoryBuffer.allocate(bytes.length)) { buffer =>
      buffer.setBytes(0, bytes, 0, bytes.length)
      buffer
    }
  }

  /**
   * Describes how to replace low-shuffle merge metadata fields after cuDF applies a deletion
   * vector. The row-index field receives cuDF's selected row indexes. A row-deleted field is
   * marked as deleted when the scan retains marked rows, or kept when it drops marked rows. With
   * no deletion-vector map, all rows are retained and the field is marked as kept.
   *
   * @param readDataSchema schema whose metadata field positions will be populated
   * @param delVecs optional per-file deletion vectors and their row-index filter types; `None`
   *                means no rows are deleted
   * @return named output positions for row indexes and the selected rows' deletion status
   */
  def outputColumns(
      readDataSchema: StructType,
      delVecs: Option[Broadcast[Map[URI, DeletionVectorDescriptorWithFilterType]]])
  : DeletionVectorOutputColumns = {
    val rowIndex = readDataSchema.fieldNames.indexOf(METADATA_ROW_IDX_COL) match {
      case -1 => None
      case index => Some(index)
    }
    val (deletedColumn, keptColumn) =
      readDataSchema.fieldNames.indexOf(METADATA_ROW_DEL_COL) match {
        case -1 => None -> None
        case index =>
          val filterTypes = delVecs.toSeq.flatMap(_.value.values.map(_.filterType)).distinct
          require(filterTypes.length <= 1,
            "Low shuffle merge row-deletion scans require at most one " +
              "deletion-vector filter type")
          filterTypes.headOption match {
            case None | Some(RowIndexFilterType.IF_CONTAINED) => None -> Some(index)
            case Some(RowIndexFilterType.IF_NOT_CONTAINED) => Some(index) -> None
            case Some(other) => throw new IllegalArgumentException(
              s"Unexpected low shuffle merge deletion-vector filter type: $other")
          }
      }
    DeletionVectorOutputColumns(rowIndex, deletedColumn, keptColumn)
  }
}

private case class Delta24xSpillableDeletionVectorInfo(
    serializedBitmap: SpillableHostBuffer,
    rowSelection: Delta24xRowSelection) extends AutoCloseable {
  override def close(): Unit = serializedBitmap.close()
}

private object Delta24xSpillableDeletionVectorInfo {
  def apply(
      file: PartitionedFile,
      blocks: collection.Seq[BlockMetaData],
      delVecs: Option[Broadcast[Map[URI, DeletionVectorDescriptorWithFilterType]]],
      metrics: Map[String, GpuMetric]): Delta24xSpillableDeletionVectorInfo = {
    val bitmapInfo = Delta24xDeletionVectorUtils.bitmapInfo(file, delVecs, Some(metrics))
    val (rowGroupOffsets, rowGroupNumRows) =
      Delta24xDeletionVectorUtils.rowGroupMetadata(blocks)
    val rowSelection = Delta24xDeletionVectorUtils.rowSelection(
      bitmapInfo, rowGroupOffsets, rowGroupNumRows)
    val serialized = Delta24xDeletionVectorUtils.serializedBitmap(bitmapInfo.bytes)
    closeOnExcept(serialized) { _ =>
      new Delta24xSpillableDeletionVectorInfo(
        SpillableHostBuffer(serialized, serialized.getLength,
          SpillPriorities.ACTIVE_BATCHING_PRIORITY),
        rowSelection)
    }
  }
}

private class Delta24xSingleBufferDVMetadata(
    private var deletionVectorInfo: Option[Delta24xSpillableDeletionVectorInfo]) {
  def peek: Option[Delta24xSpillableDeletionVectorInfo] = deletionVectorInfo

  def take(): Option[Delta24xSpillableDeletionVectorInfo] = {
    val result = deletionVectorInfo
    deletionVectorInfo = None
    result
  }
}

private case class Delta24xDeletionVectorMetadata(
    metadatas: Array[Delta24xSingleBufferDVMetadata]) extends AutoCloseable {
  def peekInfos: Array[Delta24xSpillableDeletionVectorInfo] = metadatas.flatMap(_.peek)
  def takeInfos(): Array[Delta24xSpillableDeletionVectorInfo] = metadatas.flatMap(_.take())
  override def close(): Unit = takeInfos().safeClose()
}

private object Delta24xDeletionVectorMetadata {
  def apply(info: Option[Delta24xSpillableDeletionVectorInfo]): Delta24xDeletionVectorMetadata =
    Delta24xDeletionVectorMetadata(Array(new Delta24xSingleBufferDVMetadata(info)))

  def combine(metadata: Array[Delta24xDeletionVectorMetadata])
  : Delta24xDeletionVectorMetadata = {
    Delta24xDeletionVectorMetadata(metadata.flatMap(_.metadatas))
  }
}

private trait Delta24xHostMemoryMetadata extends HostMemoryBuffersWithMetaDataBase {
  def deletionVectorMetadata: Array[Delta24xDeletionVectorMetadata]

  protected def closeWithDeletionVectors(closeBase: => Unit): Unit = {
    var closeError: Throwable = null
    try {
      deletionVectorMetadata.safeClose()
    } catch {
      case t: Throwable => closeError = t
    }
    try {
      closeBase
    } catch {
      case t: Throwable if closeError != null => closeError.addSuppressed(t)
      case t: Throwable => closeError = t
    }
    if (closeError != null) {
      throw closeError
    }
  }
}

private case class Delta24xParquetHostMemoryEmptyMetaData(
    override val partitionedFile: PartitionedFile,
    bufferSize: Long,
    override val bytesRead: Long,
    dateRebaseMode: DateTimeRebaseMode,
    timestampRebaseMode: DateTimeRebaseMode,
    hasInt96Timestamps: Boolean,
    clippedSchema: MessageType,
    readSchema: StructType,
    numRows: Long,
    deletionVectorMetadata: Array[Delta24xDeletionVectorMetadata],
    override val allPartValues: Option[Array[(Long, InternalRow)]] = None)
  extends HostMemoryEmptyMetaData with Delta24xHostMemoryMetadata {
  override def close(): Unit = closeWithDeletionVectors(super.close())
}

private case class Delta24xParquetHostMemoryBuffersWithMetaData(
    override val partitionedFile: PartitionedFile,
    override val memBuffersAndSizes: Array[SingleHMBAndMeta],
    override val bytesRead: Long,
    dateRebaseMode: DateTimeRebaseMode,
    timestampRebaseMode: DateTimeRebaseMode,
    hasInt96Timestamps: Boolean,
    clippedSchema: MessageType,
    readSchema: StructType,
    override val allPartValues: Option[Array[(Long, InternalRow)]],
    deletionVectorMetadata: Array[Delta24xDeletionVectorMetadata])
  extends HostMemoryBuffersWithMetaData with Delta24xHostMemoryMetadata {

  override def consumeHeadBuffer(): HostMemoryBuffersWithMetaData = {
    require(memBuffersAndSizes.nonEmpty,
      "consumeHeadBuffer called on metadata with no Parquet buffers")
    require(memBuffersAndSizes.length == deletionVectorMetadata.length,
      "Parquet buffers and deletion-vector metadata must be aligned")
    this.copy(
      memBuffersAndSizes = memBuffersAndSizes.drop(1),
      deletionVectorMetadata = deletionVectorMetadata.drop(1))
  }

  override def close(): Unit = closeWithDeletionVectors(super.close())
}

private case class GpuDelta24xParquetPartitionReaderFactory(
    @transient sqlConf: SQLConf,
    broadcastedConf: Broadcast[SerializableConfiguration],
    dataSchema: StructType,
    readDataSchema: StructType,
    partitionSchema: StructType,
    filters: Array[Filter],
    @transient rapidsConf: RapidsConf,
    metrics: Map[String, GpuMetric],
    @transient params: Map[String, String],
    delVecs: Option[Broadcast[Map[URI, DeletionVectorDescriptorWithFilterType]]])
  extends GpuParquetPartitionReaderFactoryBase(sqlConf, broadcastedConf, dataSchema,
    readDataSchema, partitionSchema, rapidsConf, metrics = metrics, params = params) {

  override protected def buildBaseColumnarParquetReader(
      file: PartitionedFile): org.apache.spark.sql.connector.read.PartitionReader[ColumnarBatch] = {
    val conf = new Configuration(broadcastedConf.value.value)
    val startTime = System.nanoTime()
    val singleFileInfo = filterHandler.filterBlocks(fileIO, footerReadType, file, conf, filters,
      readDataSchema)
    metrics.get(FILTER_TIME).foreach(_ += System.nanoTime() - startTime)
    new Delta24xParquetPartitionReader(fileIO, conf, file, singleFileInfo.filePath,
      singleFileInfo.blocks, singleFileInfo.schema, isCaseSensitive, readDataSchema,
      debugDumpPrefix, debugDumpAlways, maxReadBatchSizeRows, maxReadBatchSizeBytes,
      targetSizeBytes, useChunkedReader, maxChunkedReaderMemoryUsageSizeBytes, compressCfg,
      metrics, singleFileInfo.dateRebaseMode, singleFileInfo.timestampRebaseMode,
      singleFileInfo.hasInt96Timestamps, readUseFieldId, delVecs)
  }
}

private class Delta24xParquetPartitionReader(
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
    useFieldId: Boolean,
    delVecs: Option[Broadcast[Map[URI, DeletionVectorDescriptorWithFilterType]]])
  extends AbstractParquetPartitionReader(
    fileIO, conf, split, filePath, clippedBlocks, clippedParquetSchema, isSchemaCaseSensitive,
    readDataSchema, debugDumpPrefix, debugDumpAlways, maxReadBatchSizeRows, maxReadBatchSizeBytes,
    compressCfg, execMetrics, useFieldId) {

  private val outputColumns = Delta24xDeletionVectorUtils.outputColumns(readDataSchema, delVecs)

  override protected def readBuffer(
      parquetOptions: ParquetOptions,
      columnTypes: Array[DataType],
      chunkedBlocks: Seq[BlockMetaData],
      dataBuffer: SpillableHostBuffer): Iterator[ColumnarBatch] = {
    if (dataBuffer.length == 0) {
      dataBuffer.close()
      CachedGpuBatchIterator(EmptyTableReader, columnTypes)
    } else {
      RmmRapidsRetryIterator.withRetryNoSplit(dataBuffer) { _ =>
        val spillableDV = Delta24xSpillableDeletionVectorInfo(
          split, chunkedBlocks, delVecs, execMetrics)
        withResource(spillableDV) { info =>
          GpuSemaphore.acquireIfNecessary(TaskContext.get())
          val hostDVBuffer = info.serializedBitmap.getDataHostBuffer()
          val (hostBuffers, hostDV) = closeOnExcept(hostDVBuffer) { _ =>
            val dataBuffers = Array(dataBuffer.getDataHostBuffer())
            closeOnExcept(dataBuffers) { _ =>
              dataBuffers -> new DeletionVector.DeletionVectorInfo(
                hostDVBuffer, info.rowSelection.isRetention,
                info.rowSelection.rowGroupOffsets, info.rowSelection.rowGroupNumRows)
            }
          }
          val producer = MakeParquetTableWithDVProducer(
            useChunkedReader, maxChunkedReaderMemoryUsageSizeBytes, conf,
            targetBatchSizeBytes, parquetOptions, hostBuffers, execMetrics,
            dateRebaseMode, timestampRebaseMode, isSchemaCaseSensitive, useFieldId,
            readDataSchema, clippedParquetSchema, Array(split), debugDumpPrefix,
            debugDumpAlways, Array(hostDV), outputColumns)
          CachedGpuBatchIterator(producer, columnTypes)
        }
      }
    }
  }

  override protected def readEmptyDataBatch(
      totalNumRows: Long,
      chunkedBlocks: Seq[BlockMetaData]): Iterator[ColumnarBatch] = {
    if (totalNumRows == 0) {
      EmptyGpuColumnarBatchIterator
    } else {
      val bitmapInfo = Delta24xDeletionVectorUtils.bitmapInfo(split, delVecs, Some(execMetrics))
      val (rowGroupOffsets, rowGroupNumRows) =
        Delta24xDeletionVectorUtils.rowGroupMetadata(chunkedBlocks)
      val selection = Delta24xDeletionVectorUtils.rowSelection(
        bitmapInfo, rowGroupOffsets, rowGroupNumRows)
      if (selection.numRowsAlive == 0) {
        EmptyGpuColumnarBatchIterator
      } else {
        GpuSemaphore.acquireIfNecessary(TaskContext.get())
        val batch = Delta24xDeletionVectorUtils.metadataBatch(
          readDataSchema, Array(selection), outputColumns)
        new SingleGpuColumnarBatchIterator(batch)
      }
    }
  }

  override protected def computeNumRowsAlive(
      totalNumRows: Long,
      chunkedBlocks: Seq[BlockMetaData]): Int = {
    if (totalNumRows == 0) {
      0
    } else {
      val bitmapInfo = Delta24xDeletionVectorUtils.bitmapInfo(split, delVecs)
      val (rowGroupOffsets, rowGroupNumRows) =
        Delta24xDeletionVectorUtils.rowGroupMetadata(chunkedBlocks)
      Math.toIntExact(Delta24xDeletionVectorUtils.numRowsAlive(
        bitmapInfo, rowGroupOffsets, rowGroupNumRows))
    }
  }
}

private case class GpuDelta24xParquetMultiFilePartitionReaderFactory(
    @transient sqlConf: SQLConf,
    broadcastedConf: Broadcast[SerializableConfiguration],
    dataSchema: StructType,
    readDataSchema: StructType,
    partitionSchema: StructType,
    filters: Array[Filter],
    @transient rapidsConf: RapidsConf,
    poolConfBuilder: ThreadPoolConfBuilder,
    metrics: Map[String, GpuMetric],
    queryUsesInputFile: Boolean,
    delVecs: Option[Broadcast[Map[URI, DeletionVectorDescriptorWithFilterType]]])
  extends AbstractGpuParquetMultiFilePartitionReaderFactory(sqlConf, broadcastedConf,
    dataSchema, readDataSchema, partitionSchema, filters, rapidsConf, poolConfBuilder,
    metrics, queryUsesInputFile) {

  override val canUseCoalesceFilesReader: Boolean = false
  override val canUseMultiThreadReader: Boolean = true

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
      combineConf: CombineConf): AbstractMultiFileCloudParquetPartitionReader = {
    new MultiFileCloudDelta24xParquetPartitionReader(
      fileIO, conf, files, filterFunc, isSchemaCaseSensitive, debugDumpPrefix,
      debugDumpAlways, maxReadBatchSizeRows, maxReadBatchSizeBytes, targetBatchSizeBytes,
      maxGpuColumnSizeBytes, useChunkedReader, maxChunkedReaderMemoryUsageSizeBytes,
      compressCfg, execMetrics, partitionSchema, poolConf, maxNumFileProcessed,
      ignoreMissingFiles, ignoreCorruptFiles, useFieldId, queryUsesInputFile,
      keepReadsInOrder, combineConf, readDataSchema, delVecs)
  }
}

private class MultiFileCloudDelta24xParquetPartitionReader(
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
    combineConf: CombineConf,
    readDataSchema: StructType,
    delVecs: Option[Broadcast[Map[URI, DeletionVectorDescriptorWithFilterType]]])
  extends AbstractMultiFileCloudParquetPartitionReader(fileIO, conf, files, filterFunc,
    isSchemaCaseSensitive, debugDumpPrefix, debugDumpAlways, maxReadBatchSizeRows,
    maxReadBatchSizeBytes, targetBatchSizeBytes, maxGpuColumnSizeBytes, useChunkedReader,
    maxChunkedReaderMemoryUsageSizeBytes, compressCfg, execMetrics, partitionSchema,
    poolConf, maxNumFileProcessed, ignoreMissingFiles, ignoreCorruptFiles, useFieldId,
    queryUsesInputFile, keepReadsInOrder, combineConf) {

  private val outputColumns =
    Delta24xDeletionVectorUtils.outputColumns(readDataSchema, delVecs)

  override def readBatches(
      fileBuffersAndMetadata: HostMemoryBuffersWithMetaDataBase): Iterator[ColumnarBatch] = {
    fileBuffersAndMetadata match {
      case metadata: Delta24xParquetHostMemoryEmptyMetaData =>
        withResource(metadata) { meta =>
          val selections = meta.deletionVectorMetadata.flatMap(_.peekInfos)
            .map(_.rowSelection)
          if (selections.map(_.numRowsAlive).sum == 0) {
            EmptyGpuColumnarBatchIterator
          } else {
            GpuSemaphore.acquireIfNecessary(TaskContext.get())
            val batch = Delta24xDeletionVectorUtils.metadataBatch(
              meta.readSchema, selections, outputColumns)
            meta.allPartValues match {
              case Some(partitionRowsAndValues) =>
                val (rowsPerPartition, partitionValues) = partitionRowsAndValues.unzip
                BatchWithPartitionDataUtils.addPartitionValuesToBatch(
                  batch, rowsPerPartition, partitionValues,
                  partitionSchema, maxGpuColumnSizeBytes)
              case None =>
                BatchWithPartitionDataUtils.addSinglePartitionValueToBatch(
                  batch, meta.partitionedFile.partitionValues,
                  partitionSchema, maxGpuColumnSizeBytes)
            }
          }
        }
      case _ => super.readBatches(fileBuffersAndMetadata)
    }
  }

  override protected def readBufferToBatches(
      buffer: HostMemoryBuffersWithMetaData): Iterator[ColumnarBatch] = {
    val deltaBuffer = buffer.asInstanceOf[Delta24xParquetHostMemoryBuffersWithMetaData]
    val hostBuffers = deltaBuffer.memBuffersAndSizes.head.hmbs
    val deletionVectorInfos = deltaBuffer.deletionVectorMetadata.head.takeInfos()
    require(deletionVectorInfos.nonEmpty,
      "Missing deletion-vector metadata for low shuffle merge Parquet read")
    val parquetOptions = closeOnExcept(hostBuffers) { _ =>
      closeOnExcept(deletionVectorInfos) { _ =>
        getParquetOptions(deltaBuffer.readSchema, deltaBuffer.clippedSchema, useFieldId)
      }
    }
    val columnTypes = deltaBuffer.readSchema.fields.map(_.dataType)

    withResource(hostBuffers) { _ =>
      withResource(deletionVectorInfos) { _ =>
        RmmRapidsRetryIterator.withRetryNoSplit {
          GpuSemaphore.acquireIfNecessary(TaskContext.get())
          val hostDataBuffers = hostBuffers.safeMap(_.getDataHostBuffer())
          makeBatchIterator(deltaBuffer, hostDataBuffers, deletionVectorInfos,
            parquetOptions, columnTypes)
        }
      }
    }
  }

  private def makeBatchIterator(
      deltaBuffer: Delta24xParquetHostMemoryBuffersWithMetaData,
      hostDataBuffers: Array[HostMemoryBuffer],
      deletionVectorInfos: Array[Delta24xSpillableDeletionVectorInfo],
      parquetOptions: ParquetOptions,
      columnTypes: Array[DataType]): Iterator[ColumnarBatch] = {
    val hostDeletionVectors = closeOnExcept(hostDataBuffers) { _ =>
      val hostDeletionVectorBuffers = deletionVectorInfos.safeMap {
        _.serializedBitmap.getDataHostBuffer()
      }
      closeOnExcept(hostDeletionVectorBuffers) { _ =>
        val hostDeletionVectors = hostDeletionVectorBuffers.zip(deletionVectorInfos)
          .map { case (hostBuffer, info) =>
            new DeletionVector.DeletionVectorInfo(
              hostBuffer, info.rowSelection.isRetention,
              info.rowSelection.rowGroupOffsets, info.rowSelection.rowGroupNumRows)
          }
        hostDeletionVectors
      }
    }
    val producer = MakeParquetTableWithDVProducer(
      useChunkedReader, maxChunkedReaderMemoryUsageSizeBytes, conf,
      targetBatchSizeBytes, parquetOptions, hostDataBuffers, execMetrics,
      deltaBuffer.dateRebaseMode, deltaBuffer.timestampRebaseMode,
      isSchemaCaseSensitive, useFieldId, deltaBuffer.readSchema,
      deltaBuffer.clippedSchema, files, debugDumpPrefix, debugDumpAlways,
      hostDeletionVectors, outputColumns)
    val batchIterator = CachedGpuBatchIterator(producer, columnTypes)

    deltaBuffer.allPartValues match {
      case Some(partitionRowsAndValues) =>
        val (rowsPerPartition, partitionValues) = partitionRowsAndValues.unzip
        new GpuColumnarBatchWithPartitionValuesIterator(
          batchIterator, partitionValues, rowsPerPartition,
          partitionSchema, maxGpuColumnSizeBytes)
      case None =>
        batchIterator.flatMap { batch =>
          BatchWithPartitionDataUtils.addSinglePartitionValueToBatch(
            batch, deltaBuffer.partitionedFile.partitionValues,
            partitionSchema, maxGpuColumnSizeBytes)
      }
    }
  }

  override protected def newHMBWithMetaDataForChunks(
      partitionedFile: PartitionedFile,
      memBuffersAndSizes: Array[SingleHMBAndMeta],
      bytesRead: Long,
      fileBlockMeta: ParquetFileInfoWithBlockMeta): HostMemoryBuffersWithMetaData = {
    val dvMetadata = memBuffersAndSizes.safeMap { buffer =>
      val blocks = buffer.blockMeta.map(_.asInstanceOf[ParquetDataBlock].dataBlock)
      Delta24xDeletionVectorMetadata(Some(
        Delta24xSpillableDeletionVectorInfo(partitionedFile, blocks, delVecs, execMetrics)))
    }
    Delta24xParquetHostMemoryBuffersWithMetaData(
      partitionedFile, memBuffersAndSizes, bytesRead, fileBlockMeta.dateRebaseMode,
      fileBlockMeta.timestampRebaseMode, fileBlockMeta.hasInt96Timestamps,
      fileBlockMeta.schema, fileBlockMeta.readSchema, None, dvMetadata)
  }

  override protected def newCombinedHMBWithMetaData(
      combinedMeta: CombinedMeta,
      newHmbBufferInfo: SingleHMBAndMeta,
      offset: Long): HostMemoryBuffersWithMetaData = {
    val metadataToUse = combinedMeta.firstNonEmpty
      .asInstanceOf[Delta24xParquetHostMemoryBuffersWithMetaData]
    val metadataToCombine = combinedMeta.toCombine.collect {
      case metadata: Delta24xParquetHostMemoryBuffersWithMetaData =>
        metadata.deletionVectorMetadata
    }.flatten
    Delta24xParquetHostMemoryBuffersWithMetaData(
      metadataToUse.partitionedFile, Array(newHmbBufferInfo), offset,
      metadataToUse.dateRebaseMode, metadataToUse.timestampRebaseMode,
      metadataToUse.hasInt96Timestamps, metadataToUse.clippedSchema,
      metadataToUse.readSchema, Some(combinedMeta.allPartValues),
      Array(Delta24xDeletionVectorMetadata.combine(metadataToCombine)))
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
      blocks: collection.Seq[BlockMetaData]): HostMemoryEmptyMetaData = {
    val info = if (numRows > 0) {
      Some(Delta24xSpillableDeletionVectorInfo(partitionedFile, blocks, delVecs, execMetrics))
    } else {
      None
    }
    Delta24xParquetHostMemoryEmptyMetaData(
      partitionedFile, bufferSize, bytesRead, dateRebaseMode, timestampRebaseMode,
      hasInt96Timestamps, clippedSchema, readSchema, numRows,
      Array(Delta24xDeletionVectorMetadata(info)))
  }

  override protected def newCombinedHMEmptyMetadata(
      emptyMeta: CombinedEmptyMeta,
      nonEmptyMeta: CombinedMeta): HostMemoryEmptyMetaData = {
    val metadataToUse = emptyMeta.metaForEmpty
      .asInstanceOf[Delta24xParquetHostMemoryEmptyMetaData]
    val metadataToCombine = emptyMeta.emptyMetas.map {
      _.asInstanceOf[Delta24xParquetHostMemoryEmptyMetaData].deletionVectorMetadata
    }.flatten
    Delta24xParquetHostMemoryEmptyMetaData(
      metadataToUse.partitionedFile, emptyMeta.emptyBufferSize,
      emptyMeta.emptyTotalBytesRead, metadataToUse.dateRebaseMode,
      metadataToUse.timestampRebaseMode, metadataToUse.hasInt96Timestamps,
      metadataToUse.clippedSchema, metadataToUse.readSchema, emptyMeta.emptyNumRows,
      Array(Delta24xDeletionVectorMetadata.combine(metadataToCombine)),
      Some(nonEmptyMeta.allPartValues))
  }

  override protected def computeNumRowsAlive(
      totalNumRows: Long,
      metadata: HostMemoryBuffersWithMetaDataBase): Int = {
    if (totalNumRows == 0) {
      0
    } else {
      val deletionVectorInfos = metadata.asInstanceOf[Delta24xHostMemoryMetadata]
        .deletionVectorMetadata.flatMap(_.peekInfos)
      val aliveRows = deletionVectorInfos.map(_.rowSelection.numRowsAlive).sum
      require(aliveRows <= totalNumRows,
        s"Alive row count ($aliveRows) exceeds selected row count ($totalNumRows)")
      Math.toIntExact(aliveRows)
    }
  }
}

case class GpuDelta24xParquetFileFormat(
    metadata: Metadata,
    isSplittable: Boolean,
    disablePushDown: Boolean,
    broadcastDvMap: Option[Broadcast[Map[URI, DeletionVectorDescriptorWithFilterType]]])
  extends GpuDeltaParquetFileFormat {

  override val columnMappingMode: DeltaColumnMappingMode = metadata.columnMappingMode
  override val referenceSchema: StructType = metadata.schema

  if (columnMappingMode == IdMapping) {
    val requiredReadConf = SQLConf.PARQUET_FIELD_ID_READ_ENABLED
    require(SparkSession.getActiveSession.exists(_.sessionState.conf.getConf(requiredReadConf)),
      s"${requiredReadConf.key} must be enabled to support Delta id column mapping mode")
    val requiredWriteConf = SQLConf.PARQUET_FIELD_ID_WRITE_ENABLED
    require(SparkSession.getActiveSession.exists(_.sessionState.conf.getConf(requiredWriteConf)),
      s"${requiredWriteConf.key} must be enabled to support Delta id column mapping mode")
  }

  override def isSplitable(
      sparkSession: SparkSession,
      options: Map[String, String],
      path: Path): Boolean = isSplittable

  private def isLowShuffleMetadataRead(schema: StructType): Boolean = {
    schema.fieldNames.exists(name => name == METADATA_ROW_IDX_COL || name == METADATA_ROW_DEL_COL)
  }

  override def createPartitionReaderFactory(
      sqlConf: SQLConf,
      broadcastedConf: Broadcast[SerializableConfiguration],
      dataSchema: StructType,
      readDataSchema: StructType,
      partitionSchema: StructType,
      filters: Seq[Filter],
      rapidsConf: RapidsConf,
      metrics: Map[String, GpuMetric],
      options: Map[String, String]): GpuParquetPartitionReaderFactoryBase = {
    if (isLowShuffleMetadataRead(readDataSchema)) {
      GpuDelta24xParquetPartitionReaderFactory(
        sqlConf, broadcastedConf, dataSchema, readDataSchema, partitionSchema,
        if (disablePushDown) Array.empty else filters.toArray,
        rapidsConf, metrics, options, broadcastDvMap)
    } else {
      super.createPartitionReaderFactory(sqlConf, broadcastedConf, dataSchema, readDataSchema,
        partitionSchema, filters, rapidsConf, metrics, options)
    }
  }

  override def buildReaderWithPartitionValuesAndMetrics(
      sparkSession: SparkSession,
      dataSchema: StructType,
      partitionSchema: StructType,
      requiredSchema: StructType,
      filters: Seq[Filter],
      options: Map[String, String],
      hadoopConf: Configuration,
      metrics: Map[String, GpuMetric]): PartitionedFile => Iterator[InternalRow] = {
    super.buildReaderWithPartitionValuesAndMetrics(
      sparkSession, dataSchema, partitionSchema, requiredSchema,
      if (disablePushDown) Seq.empty else filters, options, hadoopConf, metrics)
  }

  override def createMultiFileReaderFactory(
      broadcastedConf: Broadcast[SerializableConfiguration],
      pushedFilters: Array[Filter],
      fileScan: GpuFileSourceScanExec): PartitionReaderFactory = {
    if (isLowShuffleMetadataRead(fileScan.requiredSchema)) {
      GpuDelta24xParquetMultiFilePartitionReaderFactory(
        fileScan.conf, broadcastedConf, prepareSchema(fileScan.relation.dataSchema),
        prepareSchema(fileScan.requiredSchema), prepareSchema(fileScan.readPartitionSchema),
        if (disablePushDown) Array.empty else pushedFilters, fileScan.rapidsConf,
        ThreadPoolConfBuilder(fileScan.rapidsConf), fileScan.allMetrics,
        fileScan.queryUsesInputFile, broadcastDvMap)
    } else {
      super.createMultiFileReaderFactory(broadcastedConf, pushedFilters, fileScan)
    }
  }

  /**
   * We sometimes need to replace FileFormat within LogicalPlans, so we have to override
   * `equals` to ensure file format changes are captured
   */
  override def equals(other: Any): Boolean = {
    other match {
      case ff: GpuDelta24xParquetFileFormat =>
        ff.columnMappingMode == columnMappingMode &&
          ff.referenceSchema == referenceSchema &&
          ff.isSplittable == isSplittable
      case _ => false
    }
  }

  override def hashCode(): Int = getClass.getCanonicalName.hashCode()
}
