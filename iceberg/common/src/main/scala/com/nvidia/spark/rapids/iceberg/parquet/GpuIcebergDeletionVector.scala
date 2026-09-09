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

package com.nvidia.spark.rapids.iceberg.parquet

import java.io.IOException

import scala.util.control.NonFatal

import ai.rapids.cudf.{DeletionVector, DType, HostMemoryBuffer, ParquetOptions, Table}
import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.GpuMetric._
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.iceberg.IcebergDeletionVector
import com.nvidia.spark.rapids.parquet.{GpuParquetScan, ParquetSchemaUtils}
import com.nvidia.spark.rapids.shims.parquet.GpuParquetUtilsShims
import org.apache.hadoop.conf.Configuration
import org.apache.parquet.hadoop.metadata.BlockMetaData
import org.apache.parquet.schema.MessageType

import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.types.StructType

/** Native cuDF Parquet deletion-vector support shared by the Iceberg readers. */
private[parquet] object GpuIcebergDeletionVector extends Logging {
  def rowGroupMetadata(blocks: collection.Seq[BlockMetaData]): (Array[Long], Array[Int]) = {
    val offsets = blocks.map(GpuParquetUtilsShims.getRowIndexOffset)
    require(!offsets.exists(_ < 0), "Found invalid deletion-vector row-group offset")
    val rowCounts = blocks.map(_.getRowCount)
    require(rowCounts.forall(_.isValidInt), "Found invalid deletion-vector row-group row count")
    (offsets.toArray, rowCounts.map(_.toInt).toArray)
  }

  /**
   * Creates a cuDF table producer. The returned tables contain a leading INT64 file-row-index
   * column followed by the evolved Parquet columns. This method takes ownership of `buffers`,
   * including when producer construction fails.
   */
  def makeProducer(
      useChunkedReader: Boolean,
      maxChunkedReaderMemoryUsageSizeBytes: Long,
      conf: Configuration,
      chunkSizeByteLimit: Long,
      opts: ParquetOptions,
      buffers: Array[HostMemoryBuffer],
      metrics: Map[String, GpuMetric],
      dateRebaseMode: DateTimeRebaseMode,
      timestampRebaseMode: DateTimeRebaseMode,
      isSchemaCaseSensitive: Boolean,
      useFieldId: Boolean,
      readDataSchema: StructType,
      clippedParquetSchema: MessageType,
      splits: Array[PartitionedFile],
      debugDumpPrefix: Option[String],
      debugDumpAlways: Boolean,
      deletionVector: IcebergDeletionVector,
      blocks: collection.Seq[BlockMetaData]): GpuDataProducer[Table] = {
    def makeDeletionVectorInfo(): DeletionVector.DeletionVectorInfo = {
      require(buffers.length == 1,
        s"Iceberg deletion-vector reads require one Parquet buffer, found ${buffers.length}")
      debugDumpPrefix.foreach { prefix =>
        if (debugDumpAlways) {
          val path = DumpUtils.dumpBuffer(conf, buffers, prefix, ".parquet")
          logWarning(s"Wrote data for ${splits.mkString(", ")} to $path")
        }
      }
      makeInfo(deletionVector, blocks)
    }

    if (useChunkedReader) {
      closeOnExcept(buffers) { _ =>
        val dvInfo = makeDeletionVectorInfo()
        closeOnExcept(dvInfo.serializedBitmap) { _ =>
          new ChunkedDeletionVectorProducer(
            maxChunkedReaderMemoryUsageSizeBytes, conf, chunkSizeByteLimit, opts, buffers, metrics,
            dateRebaseMode, timestampRebaseMode, isSchemaCaseSensitive, useFieldId,
            readDataSchema, clippedParquetSchema, splits, debugDumpPrefix, debugDumpAlways, dvInfo)
        }
      }
    } else {
      withResource(buffers) { _ =>
        val dvInfo = makeDeletionVectorInfo()
        withResource(dvInfo.serializedBitmap) { _ =>
          val rawTable = try {
            NvtxIdWithMetrics(NvtxRegistry.PARQUET_DECODE, metrics(GPU_DECODE_TIME)) {
              DeletionVector.readParquet(opts, buffers, Array(dvInfo))
            }
          } catch {
            case NonFatal(e) =>
              val dumpMessage = debugDumpPrefix.map { prefix =>
                if (!debugDumpAlways) {
                  val path = DumpUtils.dumpBuffer(conf, buffers, prefix, ".parquet")
                  s", data dumped to $path"
                } else {
                  ""
                }
              }.getOrElse("")
              throw new IOException(s"Error when processing ${splits.mkString("; ")}" +
                s"$dumpMessage", e)
          }
          new SingleGpuDataProducer(processTable(rawTable, readDataSchema, clippedParquetSchema,
            dateRebaseMode, timestampRebaseMode, isSchemaCaseSensitive, useFieldId, splits,
            metrics))
        }
      }
    }
  }

  private def makeInfo(
      deletionVector: IcebergDeletionVector,
      blocks: collection.Seq[BlockMetaData]): DeletionVector.DeletionVectorInfo = {
    val bitmap = deletionVector.serializedBitmap()
    bitmap.incRefCount()
    closeOnExcept(bitmap) { _ =>
      val (offsets, rowCounts) = rowGroupMetadata(blocks)
      new DeletionVector.DeletionVectorInfo(bitmap, false, offsets, rowCounts)
    }
  }

  private[parquet] def processTable(
      rawTable: Table,
      readDataSchema: StructType,
      clippedParquetSchema: MessageType,
      dateRebaseMode: DateTimeRebaseMode,
      timestampRebaseMode: DateTimeRebaseMode,
      isSchemaCaseSensitive: Boolean,
      useFieldId: Boolean,
      splits: Array[PartitionedFile],
      metrics: Map[String, GpuMetric]): Table = {
    require(rawTable.getNumberOfColumns > 0,
      "cuDF deletion-vector output did not contain the file-row-index column")
    withResource(rawTable) { table =>
      withResource(table.getColumn(0).castTo(DType.INT64)) { rowIndex =>
        val dataColumns = (1 until table.getNumberOfColumns).map(table.getColumn).toArray
        val dataTable = new Table(dataColumns: _*)
        closeOnExcept(dataTable) { _ =>
          GpuParquetScan.throwIfRebaseNeededInExceptionMode(
            dataTable, dateRebaseMode, timestampRebaseMode)
          if (readDataSchema.length < dataTable.getNumberOfColumns) {
            throw new IOException(s"Expected ${readDataSchema.length} columns but read " +
              s"${dataTable.getNumberOfColumns} from ${splits.mkString("; ")}")
          }
          metrics(NUM_OUTPUT_BATCHES) += 1
          val evolved = ParquetSchemaUtils.evolveSchemaIfNeededAndClose(
            dataTable, clippedParquetSchema, readDataSchema,
            isSchemaCaseSensitive, useFieldId)
          withResource(GpuParquetScan.rebaseDateTime(
              evolved, dateRebaseMode, timestampRebaseMode)) { rebased =>
            new Table((Array(rowIndex) ++
              (0 until rebased.getNumberOfColumns).map(rebased.getColumn)): _*)
          }
        }
      }
    }
  }

}

private class ChunkedDeletionVectorProducer(
    maxChunkedReaderMemoryUsageSizeBytes: Long,
    conf: Configuration,
    chunkSizeByteLimit: Long,
    opts: ParquetOptions,
    buffers: Array[HostMemoryBuffer],
    metrics: Map[String, GpuMetric],
    dateRebaseMode: DateTimeRebaseMode,
    timestampRebaseMode: DateTimeRebaseMode,
    isSchemaCaseSensitive: Boolean,
    useFieldId: Boolean,
    readDataSchema: StructType,
    clippedParquetSchema: MessageType,
    splits: Array[PartitionedFile],
    debugDumpPrefix: Option[String],
    debugDumpAlways: Boolean,
    dvInfo: DeletionVector.DeletionVectorInfo) extends GpuDataProducer[Table] {
  private val reader = DeletionVector.newParquetChunkedReader(
    chunkSizeByteLimit, maxChunkedReaderMemoryUsageSizeBytes, opts, buffers, Array(dvInfo))

  override def hasNext: Boolean = reader.hasNext

  override def next: Table = {
    val rawTable = try {
      NvtxIdWithMetrics(NvtxRegistry.PARQUET_DECODE, metrics(GPU_DECODE_TIME)) {
        reader.readChunk()
      }
    } catch {
      case NonFatal(e) =>
        val dumpMessage = debugDumpPrefix.map { prefix =>
          if (!debugDumpAlways) {
            val path = DumpUtils.dumpBuffer(conf, buffers, prefix, ".parquet")
            s", data dumped to $path"
          } else {
            ""
          }
        }.getOrElse("")
        throw new IOException(s"Error when processing ${splits.mkString("; ")}$dumpMessage", e)
    }
    GpuIcebergDeletionVector.processTable(rawTable, readDataSchema, clippedParquetSchema,
      dateRebaseMode, timestampRebaseMode, isSchemaCaseSensitive, useFieldId, splits, metrics)
  }

  override def close(): Unit = {
    (Seq(reader) ++ buffers ++ Seq(dvInfo.serializedBitmap)).safeClose()
  }
}
