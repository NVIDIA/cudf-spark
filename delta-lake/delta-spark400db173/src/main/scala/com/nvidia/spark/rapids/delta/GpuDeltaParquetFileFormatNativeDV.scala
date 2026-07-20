/*
 * Copyright (c) 2026, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.delta

import java.io.IOException
import java.util.concurrent.Callable

import scala.collection.mutable.ArrayBuffer

import ai.rapids.cudf._
import com.databricks.sql.io.{RowIndexFilterProvider, RowIndexFilterType}
import com.databricks.sql.transaction.tahoe.{
  DeltaColumnMapping,
  DeltaColumnMappingMode,
  DeltaParquetFileFormat,
  IdMapping,
  NameMapping,
  NoMapping
}
import com.databricks.sql.transaction.tahoe.actions.{Metadata, Protocol}
import com.databricks.sql.transaction.tahoe.deletionvectors.RoaringBitmapArray
import com.databricks.sql.transaction.tahoe.schema.SchemaMergingUtils
import com.databricks.sql.transaction.tahoe.sources.DeltaSQLConf
import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.GpuMetric._
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.jni.fileio.RapidsFileIO
import com.nvidia.spark.rapids.parquet._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.metadata._
import org.apache.parquet.schema.MessageType

import org.apache.spark.TaskContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.execution.QueryExecutionException
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, PartitionedFile}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.GpuFileSourceScanExec
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.SerializableConfiguration

/**
 * This is the version 2 of the Delta Parquet file format implementation, which uses the
 * new deletion vector APIs in cuDF. Unlike the previous version where deletion vectors
 * are materialized into boolean columns and processed in a FilterExec, the deletion vectors
 * are passed to the Parquet reader and applied during reading with no materialization.
 *
 * Note that we do not support the DataSourceV2 API for Delta Lake tables yet.
 */
case class GpuDeltaParquetFileFormatNativeDV(
    @transient relation: HadoopFsRelation,
    protocol: Protocol,
    metadata: Metadata,
    generateRowIndexFilterId: Boolean = false,
    generateRowIndexFilterColumn: Boolean = false,
    nullableRowTrackingConstantFields: Boolean = false,
    nullableRowTrackingGeneratedFields: Boolean = false,
    optimizationsEnabled: Boolean = true,
    tablePath: Option[String] = None,
    isCDCRead: Boolean = false
) extends GpuDeltaParquetFileFormatBase with Logging {

  // Validate either we have all arguments for DV enabled read or none of them.

  if (hasDeletionVectorRead) {
    SparkSession.getActiveSession.map { session =>
      val useMetadataRowIndex =
        session.sessionState.conf.getConf(DeltaSQLConf.DELETION_VECTORS_USE_METADATA_ROW_INDEX)

      // We currently support the 'useMetadataRowIndex' mode only.
      // Tagging keeps the scan on CPU when 'useMetadataRowIndex = false'.
      require(useMetadataRowIndex,
        "useMetadataRowIndex must be enabled to support Delta table scan with deletion vectors")

      require(useMetadataRowIndex == optimizationsEnabled,
        "Wrong arguments for Delta table scan with deletion vectors")
    }
  }

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

  /**
   * Delta 3.3+ has an extra nested field-id metadata key that must not be passed into the
   * Parquet reader after name mapping rewrites.
   */
  override def prepareSchema(inputSchema: StructType): StructType = {
    val schema = DeltaColumnMapping.createPhysicalSchema(
      inputSchema, referenceSchema, columnMappingMode)
    if (columnMappingMode == NameMapping) {
      SchemaMergingUtils.transformColumns(schema) { (_, field, _) =>
        field.copy(metadata = new MetadataBuilder()
          .withMetadata(field.metadata)
          .remove(DeltaColumnMapping.PARQUET_FIELD_ID_METADATA_KEY)
          .remove(DeltaColumnMapping.PARQUET_FIELD_NESTED_IDS_METADATA_KEY)
          .build())
      }
    } else {
      schema
    }
  }

  /**
   * Helper method copied from Apache Spark
   * sql/catalyst/src/main/scala/org/apache/spark/sql/connector/catalog/CatalogV2Implicits.scala
   */
  private def quoteIfNeeded(part: String): String = {
    if (part.matches("[a-zA-Z0-9_]+") && !part.matches("\\d+")) {
      part
    } else {
      s"`${part.replace("`", "``")}`"
    }
  }

  /**
   * Prepares filters so that they can be pushed down into the Parquet reader.
   *
   * If column mapping is enabled, then logical column names in the filters will be replaced with
   * their corresponding physical column names. This is necessary as the Parquet files will use
   * physical column names, and the requested schema pushed down in the Parquet reader will also use
   * physical column names.
   */
  private def prepareFiltersForRead(filters: Seq[Filter]): Seq[Filter] = {
    if (!optimizationsEnabled) {
      Seq.empty
    } else if (columnMappingMode != NoMapping) {
      val physicalNameMap = DeltaColumnMapping.getLogicalNameToPhysicalNameMap(referenceSchema)
        .map {
          case (logicalName, physicalName) =>
            (logicalName.map(quoteIfNeeded).mkString("."),
              physicalName.map(quoteIfNeeded).mkString("."))
        }
      filters.flatMap(RapidsDeletionVectors.translateFilterForColumnMapping(_, physicalNameMap))
    } else {
      filters
    }
  }

  override def isSplitable(sparkSession: SparkSession,
      options: Map[String, String],
      path: Path): Boolean = optimizationsEnabled

  def hasTablePath: Boolean = tablePath.isDefined

  private def hasDeletionVectorRead: Boolean =
    generateRowIndexFilterId || generateRowIndexFilterColumn || tablePath.isDefined

  @transient private lazy val deletionVectorReadInfo: Option[RapidsDeletionVectorReadInfo] =
    RapidsDeletionVectors.buildDeletionVectorReadInfo(relation, tablePath)

  private def hasNonEmptyDeletionVectors(info: RapidsDeletionVectorReadInfo): Boolean = {
    info.filePathToDVMap.values.exists(_.descriptor.cardinality != 0) ||
      info.filePathToFilterProvider.values.exists(_.getCardinality != 0)
  }

  override def hashCode(): Int = getClass.getCanonicalName.hashCode()

  /////////////////////////////////
  //
  // Extensions for PERFILE reader
  //
  /////////////////////////////////

  override def createPartitionReaderFactory(sqlConf: SQLConf,
      broadcastedConf: Broadcast[SerializableConfiguration],
      dataSchema: StructType,
      readDataSchema: StructType,
      partitionSchema: StructType,
      filters: Seq[Filter],
      rapidsConf: RapidsConf,
      metrics: Map[String, GpuMetric],
      options: Map[String, String]) : GpuParquetPartitionReaderFactoryBase = {
    val dvReadInfo = deletionVectorReadInfo
    val effectiveTablePath = tablePath.orElse(
      dvReadInfo.filter(hasNonEmptyDeletionVectors).map(_.tablePath))
    GpuDeltaParquetPartitionReaderFactory(
      sqlConf,
      broadcastedConf,
      dataSchema,
      readDataSchema,
      partitionSchema,
      filters.toArray,
      rapidsConf,
      metrics,
      options,
      dvReadInfo,
      effectiveTablePath)
  }

  override def buildReaderWithPartitionValuesAndMetrics(
      sparkSession: SparkSession,
      dataSchema: StructType,
      partitionSchema: StructType,
      requiredSchema: StructType,
      filters: Seq[Filter],
      options: Map[String, String],
      hadoopConf: Configuration,
      metrics: Map[String, GpuMetric])
  : PartitionedFile => Iterator[InternalRow] = {
    super.buildReaderWithPartitionValuesAndMetrics(
      sparkSession,
      dataSchema,
      partitionSchema,
      requiredSchema,
      // Prepare filters for pushdown
      prepareFiltersForRead(filters),
      options,
      hadoopConf,
      metrics)
  }

  case class GpuDeltaParquetPartitionReaderFactory(
      @transient sqlConf: SQLConf,
      broadcastedConf: Broadcast[SerializableConfiguration],
      dataSchema: StructType,
      readDataSchema: StructType,
      partitionSchema: StructType,
      filters: Array[Filter],
      @transient rapidsConf: RapidsConf,
      metrics: Map[String, GpuMetric],
      params: Map[String, String],
      deletionVectorReadInfo: Option[RapidsDeletionVectorReadInfo],
      tablePathOpt: Option[String]
  ) extends GpuParquetPartitionReaderFactoryBase(sqlConf, broadcastedConf, dataSchema,
    readDataSchema, partitionSchema, rapidsConf, metrics = metrics, params = params) {

    protected override def buildBaseColumnarParquetReader(
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
      new DeltaParquetPartitionReader(fileIO, conf, file, singleFileInfo.filePath,
        singleFileInfo.blocks, singleFileInfo.schema, isCaseSensitive, readDataSchema,
        debugDumpPrefix, debugDumpAlways, maxReadBatchSizeRows, maxReadBatchSizeBytes,
        targetSizeBytes, useChunkedReader, maxChunkedReaderMemoryUsageSizeBytes, compressCfg,
        metrics, singleFileInfo.dateRebaseMode, singleFileInfo.timestampRebaseMode,
        singleFileInfo.hasInt96Timestamps, readUseFieldId, deletionVectorReadInfo, tablePathOpt)
    }
  }

  class DeltaParquetPartitionReader(
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
      deletionVectorReadInfo: Option[RapidsDeletionVectorReadInfo],
      tablePathOpt: Option[String])
    extends AbstractParquetPartitionReader(
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
          val maybeSerializedDV = tablePathOpt.map(tp =>
            RapidsDeletionVectors.loadDeletionVector(conf, split, tp, deletionVectorReadInfo))
          closeOnExcept(maybeSerializedDV) { _ =>
            val (rowGroupOffsets, rowGroupNumRows) =
              RapidsDeletionVectors.getRowGroupMetadata(chunkedBlocks)
            val maybeDvInfo = maybeSerializedDV.map(serializedDV =>
              new DeletionVector.DeletionVectorInfo(serializedDV, rowGroupOffsets, rowGroupNumRows))

            val hostBuf = dataBuffer.getDataHostBuffer()
            // Duplicate request is ok, and start to use the GPU just after the host
            // buffer is ready to not block CPU things.
            GpuSemaphore.acquireIfNecessary(TaskContext.get())
            val producer = if (maybeDvInfo.isDefined) {
              // MakeParquetTableWithDVProducer will try to close the hostBuf and dvInfo
              MakeParquetTableWithDVProducer(
                useChunkedReader,
                maxChunkedReaderMemoryUsageSizeBytes, conf,
                targetBatchSizeBytes, parquetOpts,
                Array(hostBuf), metrics,
                dateRebaseMode, timestampRebaseMode,
                isSchemaCaseSensitive,
                useFieldId, readDataSchema,
                clippedParquetSchema, Array(split),
                debugDumpPrefix, debugDumpAlways,
                deletionVectorInfos = Array(maybeDvInfo.get)
              )
            } else {
              // MakeParquetTableProducer will try to close the hostBuf
              MakeParquetTableProducer(useChunkedReader,
                maxChunkedReaderMemoryUsageSizeBytes, conf,
                targetBatchSizeBytes, parquetOpts,
                Array(hostBuf), metrics,
                dateRebaseMode, timestampRebaseMode,
                hasInt96Timestamps, isSchemaCaseSensitive,
                useFieldId, readDataSchema,
                clippedParquetSchema, Array(split),
                debugDumpPrefix, debugDumpAlways
              )
            }
            CachedGpuBatchIterator(producer, colTypes)
          }
        }
      }
    }

    override protected def computeNumRowsAlive(
        totalNumRows: Long,
        chunkedBlocks: Seq[BlockMetaData]): Int = {
      if (totalNumRows == 0 || tablePathOpt.isEmpty) {
        Math.toIntExact(totalNumRows)
      } else {
        val dv = RapidsDeletionVectors.lookupDeletionVector(split, deletionVectorReadInfo)
        if (dv.dvDescriptor.isEmpty && dv.filterType.isEmpty &&
            dv.rowIndexFilterProvider.isEmpty) {
          Math.toIntExact(totalNumRows)
        } else {
          val scalaBitmap = RapidsDeletionVectors.loadScalaBitmap(
            conf, dv.dvDescriptor, dv.filterType, dv.rowIndexFilterProvider, tablePathOpt.get)
          RapidsDeletionVectorRowCountUtils.computeNumRowsAlive(
            totalNumRows, scalaBitmap.cardinality, chunkedBlocks) { countDeletedRow =>
            scalaBitmap.forEach { deletedIndex: Long =>
              countDeletedRow(deletedIndex)
            }
          }
        }
      }
    }
  }

  ///////////////////////////////////////
  //
  // Extensions for multi-threaded reader
  //
  ///////////////////////////////////////

  /**
   * Spillable version of DeletionVector.DeletionVectorInfo.
   */
  case class SpillableDeletionVectorInfo(
      serializedBitmap: SpillableHostBuffer,
      // Pre-computed number of deleted rows across all row groups.
      numRowsDeleted: Long,
      // The offsets and numRows below are the original row group offsets and row counts
      // in the file. The combining process in multi-threaded reader involves re-organizing
      // row groups across files, but the offsets and numRows here are not changed even
      // after the combining.
      rowGroupOffsets: Array[Long],
      rowGroupNumRows: Array[Int]
  ) extends AutoCloseable {

    override def close(): Unit = {
      serializedBitmap.close()
    }
  }

  object SpillableDeletionVectorInfo {

    def apply(
        serializedBitmap: HostMemoryBuffer,
        scalaBitmap: RoaringBitmapArray,
        rowGroupOffsets: Array[Long],
        rowGroupNumRows: Array[Int]): SpillableDeletionVectorInfo = {
      val numRowsDeleted =
        RapidsDeletionVectors.countDeletedRows(scalaBitmap, rowGroupOffsets, rowGroupNumRows)
      new SpillableDeletionVectorInfo(
        SpillableHostBuffer(
          serializedBitmap,
          serializedBitmap.getLength(),
          SpillPriorities.ACTIVE_BATCHING_PRIORITY),
        numRowsDeleted,
        rowGroupOffsets,
        rowGroupNumRows)
    }
  }

  override def createMultiFileReaderFactory(
      broadcastedConf: Broadcast[SerializableConfiguration],
      pushedFilters: Array[Filter],
      fileScan: GpuFileSourceScanExec): PartitionReaderFactory = {
    val poolConf = ThreadPoolConfBuilder(fileScan.rapidsConf)
    val dvReadInfo = deletionVectorReadInfo
    val effectiveTablePath = tablePath.orElse(
      dvReadInfo.filter(hasNonEmptyDeletionVectors).map(_.tablePath))
    GpuDeltaParquetMultiFilePartitionReaderFactory(
      fileScan.conf,
      broadcastedConf,
      prepareSchema(fileScan.relation.dataSchema),
      prepareSchema(fileScan.requiredSchema),
      prepareSchema(fileScan.readPartitionSchema),
      prepareFiltersForRead(pushedFilters).toArray,
      fileScan.rapidsConf,
      poolConf,
      fileScan.allMetrics,
      fileScan.queryUsesInputFile,
      dvReadInfo,
      effectiveTablePath)
  }

  /**
   * Per-block DV info attached to each [[ParquetSingleDataBlockMeta]].
   * One instance per row group; multiple instances from the same file share the same
   * dvDescriptor but differ in rowGroupOffset/rowGroupNumRows.
   * Extends [[ParquetExtraInfo]] additively — no overrides.
   */
  class DeltaParquetExtraInfo(
      dateRebaseMode: DateTimeRebaseMode,
      timestampRebaseMode: DateTimeRebaseMode,
      hasInt96Timestamps: Boolean,
      // Base64-encoded DV descriptor string for this block's source file. None if no DV.
      // The filter type is always RowIndexFilterType.IF_CONTAINED.
      val dvDescriptor: Option[String],
      val rowIndexFilterProvider: Option[RowIndexFilterProvider],
      // Within-file row-index ordinal of this row group's first row.
      // Captured from BlockMetaData before any merging; invariant to computeBlockMetaData().
      val rowGroupOffset: Long,
      val rowGroupNumRows: Int
  ) extends ParquetExtraInfo(dateRebaseMode, timestampRebaseMode, hasInt96Timestamps)

  /**
   * Per-file DV entry assembled during [[augmentChunkMeta]].
   *
   * @param dvDescriptor base64-encoded DV descriptor for this file; None if no DV
   * @param rowIndexFilterProvider serialized row-index filter provider if no descriptor exists
   * @param rowGroupOffsets within-file row-index ordinals of each row group's first row
   * @param rowGroupNumRows number of rows in each row group
   * @param partitionIndex index into rowsPerPartition / allPartValues this file contributes to
   */
  case class PerFileDVEntry(
      dvDescriptor: Option[String],
      rowIndexFilterProvider: Option[RowIndexFilterProvider],
      rowGroupOffsets: Array[Long],
      rowGroupNumRows: Array[Int],
      partitionIndex: Int)

  /**
   * Per-file DV load result produced during [[prepareForDecode]].
   *
   * @param gpuBitmap serialized roaring bitmap buffer for the file's deletion vector
   * @param aliveCount number of alive (non-deleted) rows in the file
   */
  case class SerializedRoaringBitmap(gpuBitmap: SpillableHostBuffer, aliveCount: Long)

  /**
   * Per-batch DV info that replaces [[ParquetExtraInfo]] in [[CurrentChunkMeta]] after batch
   * assembly.  Two-phase construction:
   *  - [[perFileEntries]] is populated by [[augmentChunkMeta]].
   *  - [[loadedDVResults]] is filled in by [[prepareForDecode]] after the copy phase.
   *  [[perFileEntries]] and [[loadedDVResults]] are always parallel sequences of the same length.
   */
  case class DeltaBatchExtraInfo(
      override val dateRebaseMode: DateTimeRebaseMode,
      override val timestampRebaseMode: DateTimeRebaseMode,
      override val hasInt96Timestamps: Boolean,
      val perFileEntries: Seq[PerFileDVEntry],
      // Filled by prepareForDecode() after the copy phase; empty until then.
      val loadedDVResults: Seq[SerializedRoaringBitmap] = Seq.empty
  ) extends ParquetExtraInfo(dateRebaseMode, timestampRebaseMode, hasInt96Timestamps) {
    /**
     * True if at least one file in this batch carries a deletion vector descriptor.
     */
    lazy val hasDeletionVectors: Boolean = perFileEntries.exists { entry =>
      entry.dvDescriptor.isDefined || entry.rowIndexFilterProvider.isDefined
    }

    /**
     * Returns a copy of this instance with [[loadedDVResults]] set.
     */
    def withLoadedDVResults(loadedDVResults: Seq[SerializedRoaringBitmap]): DeltaBatchExtraInfo =
      this.copy(loadedDVResults = loadedDVResults)

    /**
     * Closes the DV bitmaps in [[loadedDVResults]].
     */
    override def close(): Unit = loadedDVResults.map(_.gpuBitmap).safeClose()
  }

  case class GpuDeltaParquetMultiFilePartitionReaderFactory(
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
      deletionVectorReadInfo: Option[RapidsDeletionVectorReadInfo],
      tablePathOpt: Option[String])
    extends AbstractGpuParquetMultiFilePartitionReaderFactory(sqlConf, broadcastedConf,
      dataSchema, readDataSchema, partitionSchema, filters, rapidsConf, poolConfBuilder,
      metrics, queryUsesInputFile) with Logging {

    logDebug("Using GpuDeltaParquetMultiFilePartitionReaderFactory for multi-threaded Parquet " +
      "reading with deletion vectors")

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
      new MultiFileCloudDeltaParquetPartitionReader(
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
        combineConf,
        deletionVectorReadInfo,
        tablePathOpt
      )
    }

    override def buildBaseColumnarReaderForCoalescing(
        files: Array[PartitionedFile],
        conf: Configuration): PartitionReader[ColumnarBatch] = {
      val poolConf = poolConfBuilder.build()
      val clippedBlocks = ArrayBuffer[ParquetSingleDataBlockMeta]()

      metrics.getOrElse(FILTER_TIME, NoopMetric).ns {
        metrics.getOrElse(SCAN_TIME, NoopMetric).ns {
          val metaAndFilesArr = readBlockMetasForCoalescing(files, conf, poolConf)
          metaAndFilesArr.foreach { metaAndFile =>
            val dv = RapidsDeletionVectors.lookupDeletionVector(
              metaAndFile.file, deletionVectorReadInfo)
            val dvDescriptorOpt = dv.dvDescriptor
            val filterTypeOpt = dv.filterType
            val rowIndexFilterProviderOpt = dv.rowIndexFilterProvider
            filterTypeOpt.foreach { ft =>
              require(ft == RowIndexFilterType.IF_CONTAINED,
                s"Unexpected DV filter type for coalescing reader: $ft")
            }
            rowIndexFilterProviderOpt.foreach { provider =>
              require(provider.getRowIndexFilterType == RowIndexFilterType.IF_CONTAINED,
                s"Unexpected DV filter type for coalescing reader: " +
                  s"${provider.getRowIndexFilterType}")
            }
            val singleFileInfo = metaAndFile.meta
            // Capture per-row-group offsets before any block merging occurs.
            val (rowGroupOffsets, rowGroupNumRows) =
              RapidsDeletionVectors.getRowGroupMetadata(singleFileInfo.blocks)
            clippedBlocks ++= singleFileInfo.blocks.zipWithIndex.map { case (block, i) =>
              ParquetSingleDataBlockMeta(
                singleFileInfo.filePath,
                ParquetDataBlock(block, compressCfg),
                metaAndFile.file.partitionValues,
                ParquetSchemaWrapper(singleFileInfo.schema),
                singleFileInfo.readSchema,
                new DeltaParquetExtraInfo(
                  singleFileInfo.dateRebaseMode,
                  singleFileInfo.timestampRebaseMode,
                  singleFileInfo.hasInt96Timestamps,
                  dvDescriptorOpt,
                  rowIndexFilterProviderOpt,
                  rowGroupOffsets(i),
                  rowGroupNumRows(i)))
            }
          }
        }
      }

      new MultiFileDeltaCoalescingParquetPartitionReader(fileIO, conf, files,
        clippedBlocks.toSeq, isCaseSensitive, debugDumpPrefix, debugDumpAlways,
        maxReadBatchSizeRows, maxReadBatchSizeBytes, targetBatchSizeBytes,
        maxGpuColumnSizeBytes, useChunkedReader, maxChunkedReaderMemoryUsageSizeBytes,
        compressCfg, metrics, partitionSchema, poolConf, ignoreMissingFiles,
        ignoreCorruptFiles, readUseFieldId, tablePathOpt)
    }
  }

  class MultiFileCloudDeltaParquetPartitionReader(
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
      deletionVectorReadInfo: Option[RapidsDeletionVectorReadInfo],
      tablePathOpt: Option[String])
    extends AbstractMultiFileCloudParquetPartitionReader(fileIO, conf, files, filterFunc,
      isSchemaCaseSensitive, debugDumpPrefix, debugDumpAlways, maxReadBatchSizeRows,
      maxReadBatchSizeBytes, targetBatchSizeBytes, maxGpuColumnSizeBytes, useChunkedReader,
      maxChunkedReaderMemoryUsageSizeBytes, compressCfg, execMetrics, partitionSchema,
      poolConf, maxNumFileProcessed, ignoreMissingFiles, ignoreCorruptFiles, useFieldId,
      queryUsesInputFile, keepReadsInOrder, combineConf) {

    override def readBatches(
        fileBufsAndMeta: HostMemoryBuffersWithMetaDataBase): Iterator[ColumnarBatch] = {
      fileBufsAndMeta match {
        case meta: DeltaParquetHostMemoryEmptyMetaData =>
          withResource(meta) { _ =>
            super.readBatches(fileBufsAndMeta)
          }
        case _ =>
          super.readBatches(fileBufsAndMeta)
      }
    }

    override protected def readBufferToBatches(
        buffer: HostMemoryBuffersWithMetaData): Iterator[ColumnarBatch] = {
      val deltaBuffer = buffer.asInstanceOf[DeltaParquetHostMemoryBuffersWithMetaData]
      val memBuffersAndSize = deltaBuffer.memBuffersAndSizes
      val hmbAndInfo = memBuffersAndSize.head

      val dateRebaseMode: DateTimeRebaseMode = deltaBuffer.dateRebaseMode
      val timestampRebaseMode: DateTimeRebaseMode = deltaBuffer.timestampRebaseMode
      val hasInt96Timestamps: Boolean = deltaBuffer.hasInt96Timestamps
      val clippedSchema: MessageType = deltaBuffer.clippedSchema
      val readDataSchema: StructType = deltaBuffer.readSchema
      val partedFile: PartitionedFile = deltaBuffer.partitionedFile
      val hostBuffers = hmbAndInfo.hmbs
      val allPartValues: Option[Array[(Long, InternalRow)]] = deltaBuffer.allPartValues
      val dvMetadata: DeletionVectorMetadata = deltaBuffer.dvMetadata.head

      val (parseOpts, colTypes, dvInfos) = closeOnExcept(hostBuffers) { _ =>
        val parsedOptions = getParquetOptions(readDataSchema, clippedSchema, useFieldId)
        val columnTypes = readDataSchema.fields.map(f => f.dataType)
        val deletionVectorInfos: Array[SpillableDeletionVectorInfo] =
          if (tablePathOpt.isDefined) {
            val filteredDvInfos = dvMetadata.takeDvInfos()

            closeOnExcept(filteredDvInfos) { _ =>
              require(filteredDvInfos.length == dvMetadata.metadatas.length,
                "Every DeletionVectorInfo must exist if tablePath is defined")
            }
            filteredDvInfos
          } else {
            Array()
          }
        (parsedOptions, columnTypes, deletionVectorInfos)
      }

      withResource(hostBuffers) { _ =>
        withResource(dvInfos) { _ =>
          RmmRapidsRetryIterator.withRetryNoSplit {
            val hostBufs = hostBuffers.safeMap(_.getDataHostBuffer())
            val hostDvInfos = dvInfos
              .map(spillableDvInfo =>
                new DeletionVector.DeletionVectorInfo(
                  spillableDvInfo.serializedBitmap.getDataHostBuffer(),
                  spillableDvInfo.rowGroupOffsets,
                  spillableDvInfo.rowGroupNumRows
                ))
            // Duplicate request is ok, and start to use the GPU just after the host
            // buffer is ready to not block CPU things.
            GpuSemaphore.acquireIfNecessary(TaskContext.get())

            val tableReader = if (tablePathOpt.isDefined) {
              // The MakeParquetTableWithDVProducer will close the input buffers
              MakeParquetTableWithDVProducer(
                useChunkedReader,
                maxChunkedReaderMemoryUsageSizeBytes,
                conf, targetBatchSizeBytes,
                parseOpts,
                hostBufs, metrics,
                dateRebaseMode, timestampRebaseMode,
                isSchemaCaseSensitive, useFieldId, readDataSchema, clippedSchema, files,
                debugDumpPrefix, debugDumpAlways,
                hostDvInfos)
            } else {
              // The MakeParquetTableProducer will close the input buffers
              MakeParquetTableProducer(
                useChunkedReader,
                maxChunkedReaderMemoryUsageSizeBytes,
                conf, targetBatchSizeBytes,
                parseOpts, hostBufs, metrics,
                dateRebaseMode, timestampRebaseMode,
                hasInt96Timestamps, isSchemaCaseSensitive,
                useFieldId, readDataSchema, clippedSchema,
                files, debugDumpPrefix, debugDumpAlways
              )
            }

            val batchIter = CachedGpuBatchIterator(tableReader, colTypes)

            if (allPartValues.isDefined) {
              val allPartInternalRows = allPartValues.get.map(_._2)
              // rowsPerPartition has been adjusted already to account only the alive rows.
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
    }

    private def closeWithDVMetadata(
        dvMetadata: Array[DeletionVectorMetadata],
        closeBase: => Unit): Unit = {
      var closeException: Throwable = null
      try {
        dvMetadata.safeClose()
      } catch {
        case t: Throwable => closeException = t
      }
      try {
        closeBase
      } catch {
        case t: Throwable if closeException != null => closeException.addSuppressed(t)
        case t: Throwable => closeException = t
      }
      if (closeException != null) {
        throw closeException
      }
    }

    /**
     * Deletion vector metadata for a single host memory buffer containing a part of data.
     */
    private class SingleBufferDVMetadata(
        private var maybeDvInfo: Option[SpillableDeletionVectorInfo]) {
      def peekDvInfo: Option[SpillableDeletionVectorInfo] = maybeDvInfo

      def takeDvInfo(): Option[SpillableDeletionVectorInfo] = {
        val ret = maybeDvInfo
        maybeDvInfo = None
        ret
      }
    }

    private case class DeletionVectorMetadata(
        metadatas: Array[SingleBufferDVMetadata]
    ) extends AutoCloseable {
      def takeDvInfos(): Array[SpillableDeletionVectorInfo] =
        metadatas.flatMap(_.takeDvInfo())

      def peekDvInfos: Array[SpillableDeletionVectorInfo] =
        metadatas.flatMap(_.peekDvInfo)

      override def close(): Unit = {
        takeDvInfos().safeClose()
      }
    }

    private object DeletionVectorMetadata {
      def forSingleBuffer(maybeDvInfo: Option[SpillableDeletionVectorInfo]) = {
        DeletionVectorMetadata(
          Array(
            new SingleBufferDVMetadata(maybeDvInfo)
          )
        )
      }

      def combine(metadatas: Array[DeletionVectorMetadata]): DeletionVectorMetadata = {
        DeletionVectorMetadata(metadatas.flatMap(_.metadatas))
      }
    }

    private case class DeltaParquetHostMemoryEmptyMetaData(
        override val partitionedFile: PartitionedFile,
        bufferSize: Long,
        override val bytesRead: Long,
        dateRebaseMode: DateTimeRebaseMode,
        timestampRebaseMode: DateTimeRebaseMode,
        hasInt96Timestamps: Boolean,
        clippedSchema: MessageType,
        readSchema: StructType,
        numRows: Long,
        dvMetadata: Array[DeletionVectorMetadata],
        override val allPartValues: Option[Array[(Long, InternalRow)]] = None)
      extends HostMemoryEmptyMetaData {
      override def close(): Unit = closeWithDVMetadata(dvMetadata, super.close())
    }

    private case class DeltaParquetHostMemoryBuffersWithMetaData(
        override val partitionedFile: PartitionedFile,
        override val memBuffersAndSizes: Array[SingleHMBAndMeta],
        override val bytesRead: Long,
        dateRebaseMode: DateTimeRebaseMode,
        timestampRebaseMode: DateTimeRebaseMode,
        hasInt96Timestamps: Boolean,
        clippedSchema: MessageType,
        readSchema: StructType,
        override val allPartValues: Option[Array[(Long, InternalRow)]],
        // deletion vector metadata. should be aligned with memBuffersAndSizes if deletion vectors
        // are present.
        dvMetadata: Array[DeletionVectorMetadata]
    ) extends HostMemoryBuffersWithMetaData {

      override def consumeHeadBuffer(): HostMemoryBuffersWithMetaData = {
        require(memBuffersAndSizes.nonEmpty,
          "consumeHeadBuffer called on HostMemoryBuffersWithMetaData with no buffers")
        require(memBuffersAndSizes.length == dvMetadata.length,
          "memBuffersAndSizes and dvMetadata should have the same length")
        val (remainingBuffers, newDvMetadata) = if (memBuffersAndSizes.length > 1) {
          (memBuffersAndSizes.drop(1), dvMetadata.drop(1))
        } else {
          (Array.empty[SingleHMBAndMeta], Array.empty[DeletionVectorMetadata])
        }
        this.copy(memBuffersAndSizes = remainingBuffers, dvMetadata = newDvMetadata)
      }

      override def close(): Unit = closeWithDVMetadata(dvMetadata, super.close())
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
      val (maybeSerializedDV, maybeScalaBitmap) = if (numRows > 0) {
        // numRows == 0 means the data is empty because of an empty file,
        // file not found, or a corrupted file. In all these cases, we don't
        // need to load deletion vectors.
        val maybeScalaBitmap = tablePathOpt.map(tp =>
          RapidsDeletionVectors.loadScalaBitmap(
            conf, partitionedFile, tp, deletionVectorReadInfo))
        // Load serializedDV at last which is stored in a HostBufferMemory, so that we will
        // not execute any other code before the serializedDV is wrapped within the withResource
        // clause.
        val maybeSerializedDV = tablePathOpt.map(tp =>
          RapidsDeletionVectors.loadDeletionVector(
            conf, partitionedFile, tp, deletionVectorReadInfo))
        (maybeSerializedDV, maybeScalaBitmap)
      } else {
        (None, None)
      }

      closeOnExcept(maybeSerializedDV) { _ =>
        val dvMetadata = DeletionVectorMetadata.forSingleBuffer(
          maybeSerializedDV.map{ serializedDV =>
            val (rowGroupOffsets, rowGroupNumRows) = RapidsDeletionVectors
              .getRowGroupMetadata(blocks)
            SpillableDeletionVectorInfo(
              serializedDV,
              maybeScalaBitmap.get,
              rowGroupOffsets,
              rowGroupNumRows)}
        )
        DeltaParquetHostMemoryEmptyMetaData(
          partitionedFile,
          bufferSize,
          bytesRead,
          dateRebaseMode,
          timestampRebaseMode,
          hasInt96Timestamps,
          clippedSchema,
          readSchema,
          numRows,
          Array(dvMetadata)
        )
      }
    }

    override protected def newCombinedHMEmptyMetadata(emptyMeta: CombinedEmptyMeta,
        nonEmptyMeta: CombinedMeta): HostMemoryEmptyMetaData = {
      val metaForEmpty = emptyMeta.metaForEmpty
      val toCombine = emptyMeta.emptyMetas.map(_.asInstanceOf[DeltaParquetHostMemoryEmptyMetaData])
      val combinedDVMeta = DeletionVectorMetadata.combine(toCombine.flatMap(_.dvMetadata))

      DeltaParquetHostMemoryEmptyMetaData(
        metaForEmpty.partitionedFile, // just pick one since not used
        emptyMeta.emptyBufferSize,
        emptyMeta.emptyTotalBytesRead,
        metaForEmpty.dateRebaseMode, // these shouldn't matter since data is empty
        metaForEmpty.timestampRebaseMode, // these shouldn't matter since data is empty
        metaForEmpty.hasInt96Timestamps, // these shouldn't matter since data is empty
        metaForEmpty.clippedSchema,
        metaForEmpty.readSchema,
        emptyMeta.emptyNumRows,
        Array(combinedDVMeta),
        Some(nonEmptyMeta.allPartValues)
      )
    }

    override protected def newHMBWithMetaDataForChunks(
        partitionedFile: PartitionedFile,
        memBuffersAndSize: Array[SingleHMBAndMeta],
        bytesRead: Long,
        fileBlockMeta: ParquetFileInfoWithBlockMeta
    ): HostMemoryBuffersWithMetaData = {
      val maybeScalaBitmap = tablePathOpt.map(tp =>
        RapidsDeletionVectors.loadScalaBitmap(conf, partitionedFile, tp, deletionVectorReadInfo))
      // Load serializedDV at last which is stored in a HostBufferMemory, so that we will
      // not execute any other code before the serializedDV is wrapped within the withResource
      // clause.
      val maybeSerializedDV = tablePathOpt.map(tp =>
        RapidsDeletionVectors.loadDeletionVector(conf, partitionedFile, tp, deletionVectorReadInfo))
      withResource(maybeSerializedDV) { _ =>
        val dvMetadataBuffer = ArrayBuffer.empty[DeletionVectorMetadata]
        val dvMetadataArray = closeOnExcept(dvMetadataBuffer) { dvMetadata =>
          memBuffersAndSize.foreach { singleHMBAndMeta =>
            val dataBlocks = singleHMBAndMeta.blockMeta
              .map(_.asInstanceOf[ParquetDataBlock].dataBlock)
            val (rowGroupOffsets, rowGroupNumRows) = RapidsDeletionVectors
              .getRowGroupMetadata(dataBlocks)
            val singleDvMetadata = DeletionVectorMetadata.forSingleBuffer(
              maybeSerializedDV.map { serializedDV =>
                serializedDV.incRefCount()
                closeOnExcept(serializedDV) { bitmap =>
                  SpillableDeletionVectorInfo(
                    bitmap,
                    maybeScalaBitmap.get,
                    rowGroupOffsets,
                    rowGroupNumRows)
                }
              })
            closeOnExcept(singleDvMetadata) { metadata =>
              dvMetadata += metadata
            }
          }
          dvMetadata.toArray
        }

        closeOnExcept(dvMetadataArray) { _ =>
          DeltaParquetHostMemoryBuffersWithMetaData(
            partitionedFile,
            memBuffersAndSize,
            bytesRead,
            fileBlockMeta.dateRebaseMode,
            fileBlockMeta.timestampRebaseMode,
            fileBlockMeta.hasInt96Timestamps,
            fileBlockMeta.schema,
            fileBlockMeta.readSchema,
            None,
            dvMetadataArray
          )
        }
      }
    }

    override protected def newCombinedHMBWithMetaData(
        combinedMeta: CombinedMeta,
        newHmbBufferInfo: SingleHMBAndMeta,
        offset: Long
    ): HostMemoryBuffersWithMetaData = {
      val metaToUse = combinedMeta.firstNonEmpty
      val toCombine = combinedMeta.toCombine
        .collect { case hmb: DeltaParquetHostMemoryBuffersWithMetaData => hmb }
      val combinedDVMeta = DeletionVectorMetadata.combine(toCombine.flatMap(_.dvMetadata))

      DeltaParquetHostMemoryBuffersWithMetaData(
        metaToUse.partitionedFile,
        Array(newHmbBufferInfo),
        offset,
        metaToUse.dateRebaseMode,
        metaToUse.timestampRebaseMode,
        metaToUse.hasInt96Timestamps,
        metaToUse.clippedSchema,
        metaToUse.readSchema,
        Some(combinedMeta.allPartValues),
        Array(combinedDVMeta)
      )
    }

    override protected def computeNumRowsAlive(
        totalNumRows: Long,
        metadata: HostMemoryBuffersWithMetaDataBase
    ): Int = {
      // totalNumRows can be 0 if the file is not found but ignoreMissingFiles is true,
      // or the file is empty.
      if (totalNumRows == 0) {
        return 0
      }

      val numDeletedRows = metadata match {
        case emptyMeta: DeltaParquetHostMemoryEmptyMetaData =>
          emptyMeta.dvMetadata.flatMap(_.peekDvInfos).map(_.numRowsDeleted).sum
        case buffersMeta: DeltaParquetHostMemoryBuffersWithMetaData =>
          buffersMeta.dvMetadata.flatMap(_.peekDvInfos).map(_.numRowsDeleted).sum
        case _ =>
          throw new IllegalArgumentException(s"Unexpected metadata type ${metadata.getClass()}")
      }

      require(numDeletedRows <= totalNumRows,
        s"Deletion vector cardinality ($numDeletedRows) exceeds file row count ($totalNumRows)")
      Math.toIntExact(totalNumRows - numDeletedRows)
    }
  }

  /////////////////////////////////////
  //
  // Extensions for coalescing reader
  //
  /////////////////////////////////////

  // Pipeline overview — DV metadata flows through four phases of the coalescing reader:
  //
  // ┌───────────────────────────────────────────────────────────────────────────────────┐
  // │ BLOCK COLLECTION PHASE  (serial)                                                  │
  // │                                                                                   │
  // │  buildBaseColumnarReaderForCoalescing()                                           │
  // │                                                                                   │
  // │  for each file:                                                                   │
  // │    dvDesc ← extractDVDescriptor(partitionedFile)                                  │
  // │    for each row group:                                                            │
  // │      ParquetSingleDataBlockMeta(...,                                              │
  // │        DeltaParquetExtraInfo(dvDesc, rowGroupOffset, rowGroupNumRows))            │
  // └──────────────────────────────────┬────────────────────────────────────────────────┘
  //                                    │ flat Seq[ParquetSingleDataBlockMeta]
  //                                    ▼
  // ┌───────────────────────────────────────────────────────────────────────────────────┐
  // │ BATCH ASSEMBLY PHASE  (serial)                                                    │
  // │                                                                                   │
  // │  augmentChunkMeta()                                                               │
  // │    builds Seq[PerFileDVEntry] from file-major block groups                        │
  // │    returns meta.copy(extraInfo = DeltaBatchExtraInfo(perFileEntries))              │
  // └──────────────────────────────────┬────────────────────────────────────────────────┘
  //                                    │ CurrentChunkMeta with DeltaBatchExtraInfo
  //                                    ▼
  // ┌───────────────────────────────────────────────────────────────────────────────────┐
  // │ COPY PHASE  (parallel, per-file — existing thread pool)                           │
  // │                                                                                   │
  // │  pre-allocate combined HostMemoryBuffer                                           │
  // │  ParquetCopyBlocksRunner per file → col_data slices in combined buffer            │
  // │  await all futures; write combined footer                                         │
  // └──────────────────────────────────┬────────────────────────────────────────────────┘
  //                                    │ combined buffer
  //                                    ▼
  // ┌───────────────────────────────────────────────────────────────────────────────────┐
  // │ DV LOAD PHASE  (parallel, per-file — thread pool)                                 │
  // │                                                                                   │
  // │  prepareForDecode()                                                               │
  // │  for each file (concurrent):                                                      │
  // │    loadDeletionVector() → SpillableHostBuffer (gpuBitmap)                         │
  // │    loadScalaBitmap()    → compute aliveCount                                      │
  // │    → SerializedRoaringBitmap(gpuBitmap, aliveCount)                               │
  // │  attach results: batchExtra.withLoadedDVResults(loaded)                           │
  // └──────────────────────────────────┬────────────────────────────────────────────────┘
  //                                    │ combined buffer + updated CurrentChunkMeta
  //                                    ▼
  // ┌───────────────────────────────────────────────────────────────────────────────────┐
  // │ GPU DECODE PHASE  (serial)                                                        │
  // │                                                                                   │
  // │  readBufferToTablesAndClose()                                                     │
  // │    loadedDVResults.map(_.gpuBitmap.getDataHostBuffer()) → DeletionVectorInfo[]    │
  // │    MakeParquetTableWithDVProducer(combinedBuffer, dvInfos) → filtered Table       │
  // │                                                                                   │
  // │  getRowsPerPartition()                                                            │
  // │    loadedDVResults.map(_.aliveCount) → alive row counts per partition              │
  // └───────────────────────────────────────────────────────────────────────────────────┘

  /**
   * Coalescing Parquet reader for Delta tables with Deletion Vector support.
   *
   * Overrides the standard coalescing pipeline hooks to:
   *  - collect per-block DV descriptors during batch assembly ([[augmentChunkMeta]])
   *  - load DV bitmaps concurrently after the copy phase ([[prepareForDecode]])
   *  - pass DV info to the cuDF Parquet reader ([[readBufferToTablesAndClose]])
   *  - substitute DV-filtered alive row counts for partition routing ([[getRowsPerPartition]])
   */
  class MultiFileDeltaCoalescingParquetPartitionReader(
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
      useFieldId: Boolean,
      tablePathOpt: Option[String])
    extends MultiFileCoalescingParquetPartitionReaderBase(fileIO, conf, clippedBlocks,
      isSchemaCaseSensitive, maxReadBatchSizeRows, maxReadBatchSizeBytes, targetBatchSizeBytes,
      maxGpuColumnSizeBytes, compressCfg, execMetrics, partitionSchema, poolConf,
      ignoreMissingFiles, ignoreCorruptFiles) {

    override protected def augmentChunkMeta(meta: CurrentChunkMeta): CurrentChunkMeta = {
      if (meta.currentChunk.isEmpty) return meta

      val fileEntries = meta.currentChunk.groupsByFile.values.zipWithIndex.map {
        case (groupWithPartition, partitionIndex) =>
          val group = groupWithPartition.fileBlockGroup
          require(group.blocks.nonEmpty, s"File group must contain blocks: ${group.filePath}")
          val firstExtra = group.blocks.head.extraInfo.asInstanceOf[DeltaParquetExtraInfo]
          val fileDesc = firstExtra.dvDescriptor
          val fileProvider = firstExtra.rowIndexFilterProvider
          val fileOffsets = ArrayBuffer[Long]()
          val fileNumRows = ArrayBuffer[Int]()

          group.blocks.foreach { block =>
            val extra = block.extraInfo.asInstanceOf[DeltaParquetExtraInfo]
            require(fileDesc == extra.dvDescriptor,
              s"Row groups within the same file must share the same DV descriptor: " +
                s"${group.filePath}")
            require(fileProvider == extra.rowIndexFilterProvider,
              s"Row groups within the same file must share the same DV filter provider: " +
                s"${group.filePath}")
            fileOffsets += extra.rowGroupOffset
            fileNumRows += extra.rowGroupNumRows
          }

          PerFileDVEntry(
            fileDesc, fileProvider, fileOffsets.toArray, fileNumRows.toArray, partitionIndex)
      }.toSeq

      val batchExtra = new DeltaBatchExtraInfo(
        meta.extraInfo.dateRebaseMode, meta.extraInfo.timestampRebaseMode,
        meta.extraInfo.hasInt96Timestamps, fileEntries)
      meta.copy(extraInfo = batchExtra)
    }

    /**
     * Loads DV bitmaps for all files in the batch concurrently after the copy phase.
     * Also computes per-file alive row counts (used later by [[getRowsPerPartition]]).
     * Fast path: if no file in the batch has a DV, returns meta unchanged.
     */
    override protected def prepareForDecode(meta: CurrentChunkMeta): CurrentChunkMeta = {
      val batchExtra = meta.extraInfo.asInstanceOf[DeltaBatchExtraInfo]
      if (!batchExtra.hasDeletionVectors) return meta

      val tp = tablePathOpt.getOrElse(
        throw new IllegalStateException(
          "tablePath must be set when deletion vectors are present"))

      // Submit all DV load tasks concurrently before awaiting any result.
      val threadPool = MultiFileReaderThreadPool.getOrCreateThreadPool(poolConf)
      val loadFutures = batchExtra.perFileEntries.map { entry =>
        threadPool.submit(new Callable[SerializedRoaringBitmap] {
          override def call(): SerializedRoaringBitmap = {
            val rawBitmap = RapidsDeletionVectors.loadDeletionVector(
              conf, entry.dvDescriptor, entry.rowIndexFilterProvider, tp)
            // DeltaBatchExtraInfo.close() releases the SpillableHostBuffer when the decode
            // phase completes (via withRetryNoSplit in readBatchData).
            val gpuBitmap = closeOnExcept(rawBitmap) { raw =>
              SpillableHostBuffer(raw, raw.getLength,
                SpillPriorities.ACTIVE_BATCHING_PRIORITY)
            }
            closeOnExcept(gpuBitmap) { _ =>
              val filterTypeOpt = entry.dvDescriptor.map(_ => RowIndexFilterType.IF_CONTAINED)
              val totalRows = entry.rowGroupNumRows.map(_.toLong).sum
              val numDeleted =
                if (entry.dvDescriptor.isEmpty && entry.rowIndexFilterProvider.isEmpty) {
                  0L
                } else {
                  val scalaBitmap = RapidsDeletionVectors.loadScalaBitmap(
                    conf, entry.dvDescriptor, filterTypeOpt, entry.rowIndexFilterProvider, tp)
                  RapidsDeletionVectors.countDeletedRows(
                    scalaBitmap, entry.rowGroupOffsets, entry.rowGroupNumRows)
                }
              require(numDeleted <= totalRows,
                s"Deletion vector cardinality ($numDeleted) exceeds " +
                  s"file row count ($totalRows)")
              SerializedRoaringBitmap(gpuBitmap, totalRows - numDeleted)
            }
          }
        })
      }

      // Await results; close all bitmaps (collected + uncollected futures) on failure.
      val loaded = new ArrayBuffer[SerializedRoaringBitmap]()
      var firstFailure: Throwable = null
      var wasInterrupted = false
      def recordFailure(t: Throwable): Unit = {
        if (firstFailure == null) {
          firstFailure = t
          loadFutures.foreach(_.cancel(true))
        } else {
          firstFailure.addSuppressed(t)
        }
      }

      loadFutures.foreach { future =>
        try {
          val result = future.get()
          if (firstFailure == null) {
            closeOnExcept(result.gpuBitmap) { _ =>
              loaded += result
            }
          } else {
            result.gpuBitmap.safeClose(firstFailure)
          }
        } catch {
          case t: InterruptedException =>
            wasInterrupted = true
            recordFailure(t)
          case t: java.util.concurrent.CancellationException =>
            if (firstFailure == null) {
              recordFailure(t)
            }
          case t: Throwable =>
            recordFailure(t)
        }
      }
      if (firstFailure != null) {
        loaded.map(_.gpuBitmap).safeClose(firstFailure)
        if (wasInterrupted) {
          Thread.currentThread().interrupt()
        }
        throw firstFailure
      }

      try {
        meta.copy(extraInfo = batchExtra.withLoadedDVResults(loaded.toSeq))
      } catch {
        case t: Throwable =>
          loaded.map(_.gpuBitmap).safeClose(t)
          throw t
      }
    }

    /**
     * Decodes the combined Parquet buffer, applying deletion vectors when present.
     * GPU bitmaps were pre-loaded by [[prepareForDecode]]; one entry per file in batch order.
     */
    override def readBufferToTablesAndClose(dataBuffer: HostMemoryBuffer, dataSize: Long,
        clippedSchema: SchemaBase, readDataSchema: StructType,
        extraInfo: ExtraInfo): GpuDataProducer[Table] = {
      val batchExtra = extraInfo.asInstanceOf[DeltaBatchExtraInfo]
      val parseOpts = getParquetOptions(readDataSchema, clippedSchema, useFieldId)
      GpuSemaphore.acquireIfNecessary(TaskContext.get())

      if (batchExtra.hasDeletionVectors) {
        require(tablePathOpt.isDefined,
          "tablePath must be set when a deletion vector descriptor is present")
        // loadedDVResults is parallel to perFileEntries: one bitmap per file in batch order.
        val dvInfos = batchExtra.loadedDVResults
          .zip(batchExtra.perFileEntries)
          .map { case (loaded, entry) =>
            new DeletionVector.DeletionVectorInfo(
              loaded.gpuBitmap.getDataHostBuffer(), entry.rowGroupOffsets, entry.rowGroupNumRows)
          }.toArray
        // MakeParquetTableWithDVProducer closes the dataBuffer and the bitmaps in dvInfos.
        MakeParquetTableWithDVProducer(useChunkedReader, maxChunkedReaderMemoryUsageSizeBytes,
          conf, currentTargetBatchSize, parseOpts,
          Array(dataBuffer), metrics,
          batchExtra.dateRebaseMode, batchExtra.timestampRebaseMode,
          isSchemaCaseSensitive, useFieldId, readDataSchema, clippedSchema,
          splits, debugDumpPrefix, debugDumpAlways, dvInfos)
      } else {
        // MakeParquetTableProducer closes the dataBuffer.
        MakeParquetTableProducer(useChunkedReader, maxChunkedReaderMemoryUsageSizeBytes,
          conf, currentTargetBatchSize, parseOpts,
          Array(dataBuffer), metrics,
          batchExtra.dateRebaseMode, batchExtra.timestampRebaseMode,
          batchExtra.hasInt96Timestamps, isSchemaCaseSensitive, useFieldId,
          readDataSchema, clippedSchema, splits, debugDumpPrefix, debugDumpAlways)
      }
    }

    /**
     * Returns per-partition alive row counts by summing the pre-computed [[aliveCount]] values
     * from [[prepareForDecode]] for each file's contributing partition.
     * Fast path: if no file has a DV, returns raw row counts unchanged (no I/O).
     */
    override protected def getRowsPerPartition(
        rawRowsPerPartition: Array[Long],
        allPartValues: Array[InternalRow],
        extraInfo: ExtraInfo): Array[Long] = {
      val batchExtra = extraInfo.asInstanceOf[DeltaBatchExtraInfo]
      if (!batchExtra.hasDeletionVectors) return rawRowsPerPartition

      val alivePerPartition = Array.fill(rawRowsPerPartition.length)(0L)
      batchExtra.loadedDVResults.zip(batchExtra.perFileEntries).foreach { case (result, entry) =>
        alivePerPartition(entry.partitionIndex) += result.aliveCount
      }
      alivePerPartition
    }
  }
}

/**
 * A simple wrapper to adapt the DeletionVector.ParquetChunkedReader to the ChunkedReader interface
 * expected by AbstractParquetTableReader.
 */
case class DeltaParquetChunkedReader(delegate: DeletionVector.ParquetChunkedReader)
  extends ChunkedReader {
  override def hasNext: Boolean = delegate.hasNext
  override def next: Table = delegate.readChunk()
  override def close(): Unit = delegate.close()
}

/**
 * A chunked reader for Parquet files with deletion vectors.
 */
case class DeltaParquetTableReader(
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
    debugDumpAlways: Boolean,
    dvInfos: Array[DeletionVector.DeletionVectorInfo]) extends AbstractParquetTableReader(
  conf, chunkSizeByteLimit, maxChunkedReaderMemoryUsageSizeBytes, opts, buffers, metrics,
  dateRebaseMode, timestampRebaseMode, isSchemaCaseSensitive, useFieldId, readDataSchema,
  clippedParquetSchema, splits, debugDumpPrefix, debugDumpAlways
) {

  logDebug("Using DeltaParquetTableReader for reading Parquet with deletion vectors")

  override protected val reader = DeltaParquetChunkedReader(
    DeletionVector.newParquetChunkedReader(chunkSizeByteLimit,
      maxChunkedReaderMemoryUsageSizeBytes, opts, buffers, dvInfos)
  )

  override protected lazy val resources: Seq[AutoCloseable] =
    Seq(reader) ++ buffers ++ dvInfos.map(_.serializedBitmap)

  private lazy val deletionVectorSkipRowIndexes =
    MakeParquetTableWithDVProducer.deletionVectorSkipRowIndexes(readDataSchema)

  override protected def postProcessChunk(chunk: Table): Table = {
    // The cuDF reader prepends an extra index column in the output table.
    // We need to drop it before returning as we don't use it.
    RapidsDeletionVectors.dropFirstColumn(chunk)
  }

  override def next: Table = {
    MakeParquetTableWithDVProducer.materializeDeletionVectorSkipRowColumnsAsFalseIfNeeded(
      super.next, deletionVectorSkipRowIndexes)
  }
}

object MakeParquetTableWithDVProducer extends Logging {
  private def isDeletionVectorSkipRowColumn(name: String): Boolean =
    name == DeltaParquetFileFormat.IS_ROW_DELETED_COLUMN_NAME ||
      name == GpuDeltaParquetFileFormat.EDGE_COMPUTED_COLUMN_SKIP_ROW

  private[delta] def deletionVectorSkipRowIndexes(readDataSchema: StructType): Array[Int] =
    readDataSchema.fields.zipWithIndex.collect {
      case (field, index) if isDeletionVectorSkipRowColumn(field.name) => index
    }

  // Returns the input table unchanged when the planner has pruned skip-row columns.
  // If replacement is needed, this closes the input table and returns a new one.
  private[delta] def materializeDeletionVectorSkipRowColumnsAsFalseIfNeeded(
      table: Table,
      skipRowIndexes: Array[Int]): Table = {
    if (skipRowIndexes.nonEmpty) {
      withResource(table) { tableToClose =>
        materializeDeletionVectorSkipRowColumnsAsFalse(tableToClose, skipRowIndexes)
      }
    } else {
      table
    }
  }

  private[delta] def materializeDeletionVectorSkipRowColumnsAsFalse(
      table: Table,
      skipRowIndexes: Array[Int]): Table = {
    require(skipRowIndexes.forall(_ < table.getNumberOfColumns),
      s"Expected skip-row indexes ${skipRowIndexes.mkString(",")} within " +
        s"${table.getNumberOfColumns} output columns")
    val numRows = Math.toIntExact(table.getRowCount)
    withResource(Scalar.fromBool(false)) { falseScalar =>
      val columns = new Array[ColumnVector](table.getNumberOfColumns)
      val replacementColumns = new ArrayBuffer[ColumnVector](skipRowIndexes.length)
      try {
        var i = 0
        while (i < table.getNumberOfColumns) {
          columns(i) = if (skipRowIndexes.contains(i)) {
            val replacement = ColumnVector.fromScalar(falseScalar, numRows)
            replacementColumns += replacement
            replacement
          } else {
            table.getColumn(i)
          }
          i += 1
        }
        new Table(columns: _*)
      } finally {
        replacementColumns.safeClose()
      }
    }
  }

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
      isSchemaCaseSensitive: Boolean,
      useFieldId: Boolean,
      readDataSchema: StructType,
      clippedParquetSchema: MessageType,
      splits: Array[PartitionedFile],
      debugDumpPrefix: Option[String],
      debugDumpAlways: Boolean,
      deletionVectorInfos: Array[DeletionVector.DeletionVectorInfo]
  ): GpuDataProducer[Table] = {

    require(deletionVectorInfos.nonEmpty,
      "MakeParquetTableWithDVProducer should be used only when deletion vectors are present")

    debugDumpPrefix.foreach { prefix =>
      if (debugDumpAlways) {
        val p = DumpUtils.dumpBuffer(conf, buffers, prefix, ".parquet")
        logWarning(s"Wrote data for ${splits.mkString(", ")} to $p")
      }
    }
    if (useChunkedReader) {
      DeltaParquetTableReader(conf, chunkSizeByteLimit, maxChunkedReaderMemoryUsageSizeBytes,
        opts, buffers, metrics, dateRebaseMode, timestampRebaseMode,
        isSchemaCaseSensitive, useFieldId, readDataSchema, clippedParquetSchema,
        splits, debugDumpPrefix, debugDumpAlways, deletionVectorInfos)
    } else {
      val skipRowIndexes = deletionVectorSkipRowIndexes(readDataSchema)
      val table = withResource(buffers) { _ =>
        withResource(deletionVectorInfos.map(_.serializedBitmap)) { _ =>
          try {
            RmmRapidsRetryIterator.withRetryNoSplit[Table] {
              NvtxIdWithMetrics(NvtxRegistry.PARQUET_DECODE, metrics(GPU_DECODE_TIME)) {
                DeletionVector.readParquet(opts, buffers, deletionVectorInfos)
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
      }
      // The cuDF reader prepends an extra index column in the output table.
      // We need to drop it before returning as we don't use it.
      val tableWithoutIndex = RapidsDeletionVectors.dropFirstColumn(table)
      closeOnExcept(tableWithoutIndex) { _ =>
        GpuParquetScan.throwIfRebaseNeededInExceptionMode(tableWithoutIndex, dateRebaseMode,
          timestampRebaseMode)
        if (readDataSchema.length < tableWithoutIndex.getNumberOfColumns) {
          throw new QueryExecutionException(s"Expected ${readDataSchema.length} columns " +
            s"but read ${tableWithoutIndex.getNumberOfColumns} from ${splits.mkString("; ")}")
        }
      }
      metrics(NUM_OUTPUT_BATCHES) += 1
      val evolvedSchemaTable = ParquetSchemaUtils.evolveSchemaIfNeededAndClose(tableWithoutIndex,
        clippedParquetSchema, readDataSchema, isSchemaCaseSensitive, useFieldId)
      val outputTable = GpuParquetScan.rebaseDateTime(evolvedSchemaTable, dateRebaseMode,
        timestampRebaseMode)
      new SingleGpuDataProducer(
        materializeDeletionVectorSkipRowColumnsAsFalseIfNeeded(outputTable, skipRowIndexes))
    }
  }
}
