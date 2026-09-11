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

package com.nvidia.spark.rapids.delta.common

import java.io.IOException

import scala.collection.mutable.ArrayBuffer

import ai.rapids.cudf._
import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.GpuMetric._
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.parquet._
import org.apache.hadoop.conf.Configuration
import org.apache.parquet.schema.MessageType

import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.QueryExecutionException
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.types.StructType

/**
 * Metadata columns to populate after a cuDF Parquet deletion-vector read.
 *
 * @param rowIndexColumn optional zero-based output position for cuDF's selected row indexes
 * @param deletedColumn optional zero-based output position to fill with `true`, indicating that
 *                      every selected row was marked by the deletion vector
 * @param keptColumn optional zero-based output position to fill with `false`, indicating that
 *                   every selected row was not marked by the deletion vector
 */
case class DeletionVectorOutputColumns(
    rowIndexColumn: Option[Int] = None,
    deletedColumn: Option[Int] = None,
    keptColumn: Option[Int] = None) {

  require(deletedColumn.isEmpty || keptColumn.isEmpty,
    "A deletion-vector batch cannot contain both deleted and kept rows")

  private val rowStatusColumn = deletedColumn.map(_ -> true).orElse(keptColumn.map(_ -> false))

  require(rowIndexColumn.forall(index => !rowStatusColumn.exists(_._1 == index)),
    "The row-index output cannot also be a boolean output")

  private[common] def needsRowIndex: Boolean = rowIndexColumn.isDefined

  /**
   * Replaces placeholder columns in a decoded table with deletion-vector metadata. cuDF's
   * deletion-vector reader prepends the selected source row indexes to its output. The caller
   * separates that column before schema evolution and supplies it here as `cudfRowIndex`.
   * This method casts those indexes to Spark's `LongType`, creates the configured constant row
   * status column, and rebuilds the table with both vectors in their requested output positions.
   *
   * Ownership of both inputs is transferred to this method. When no replacements are requested,
   * `table` is returned unchanged; otherwise it is closed after the rebuilt table takes its
   * columns. `cudfRowIndex` is always closed after its replacement vector is created.
   *
   * @param table decoded and schema-evolved table without cuDF's leading row-index column
   * @param cudfRowIndex selected source row indexes emitted by cuDF, present exactly when
   *                     `rowIndexColumn` is defined
   * @return a caller-owned table with the same row order and requested metadata columns populated
   */
  private[common] def populateAndClose(
      table: Table,
      cudfRowIndex: Option[ColumnVector]): Table = {
    if (rowIndexColumn.isEmpty && rowStatusColumn.isEmpty) {
      cudfRowIndex.foreach(_.close())
      table
    } else {
      val replacementColumns = new ArrayBuffer[ColumnVector]()
      try {
        withResource(table) { tableToClose =>
          require(rowIndexColumn.isDefined == cudfRowIndex.isDefined,
            "cuDF row-index output does not match the requested metadata columns")
          val outputColumnCount = tableToClose.getNumberOfColumns
          val replacementIndexes = rowIndexColumn.toSeq ++ rowStatusColumn.map(_._1)
          require(replacementIndexes.forall(index => index >= 0 && index < outputColumnCount),
            s"Metadata column indexes ${replacementIndexes.mkString(",")} are outside " +
              s"the $outputColumnCount-column output")

          val rowIndexReplacement = cudfRowIndex.map { indexColumn =>
            val replacement = indexColumn.castTo(DType.INT64)
            replacementColumns += replacement
            replacement
          }
          val rowStatusReplacement = rowStatusColumn.map { case (index, value) =>
            val replacement = withResource(Scalar.fromBool(value)) { scalar =>
              ColumnVector.fromScalar(scalar, Math.toIntExact(tableToClose.getRowCount))
            }
            replacementColumns += replacement
            index -> replacement
          }
          val columns = (0 until outputColumnCount).map { index =>
            if (rowIndexColumn.contains(index)) {
              rowIndexReplacement.get
            } else {
              rowStatusReplacement.collect {
                case (`index`, replacement) => replacement
              }.getOrElse(tableToClose.getColumn(index))
            }
          }
          new Table(columns: _*)
        }
      } finally {
        replacementColumns.safeClose()
        cudfRowIndex.foreach(_.close())
      }
    }
  }
}

/** Adapts the cuDF deletion-vector chunked reader to the plugin's chunked-reader interface. */
private case class DeltaParquetChunkedReader(delegate: DeletionVector.ParquetChunkedReader)
  extends ChunkedReader {
  override def hasNext: Boolean = delegate.hasNext
  override def next: Table = delegate.readChunk()
  override def close(): Unit = delegate.close()
}

/** A chunked Parquet reader that applies deletion vectors while decoding. */
private case class DeltaParquetTableReader(
    conf: Configuration,
    chunkSizeByteLimit: Long,
    maxChunkedReaderMemoryUsageSizeBytes: Long,
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
    dvInfos: Array[DeletionVector.DeletionVectorInfo],
    outputColumns: DeletionVectorOutputColumns) extends AbstractParquetTableReader(
  conf, chunkSizeByteLimit, maxChunkedReaderMemoryUsageSizeBytes, opts, buffers, metrics,
  dateRebaseMode, timestampRebaseMode, isSchemaCaseSensitive, useFieldId, readDataSchema,
  clippedParquetSchema, splits, debugDumpPrefix, debugDumpAlways) {

  logDebug("Using DeltaParquetTableReader for reading Parquet with deletion vectors")

  override protected val reader: ChunkedReader = DeltaParquetChunkedReader(
    DeletionVector.newParquetChunkedReader(chunkSizeByteLimit,
      maxChunkedReaderMemoryUsageSizeBytes, opts, buffers, dvInfos))

  override protected lazy val resources: Seq[AutoCloseable] =
    Seq(reader) ++ buffers ++ dvInfos.map(_.serializedBitmap)

  private var currentRowIndex: Option[ColumnVector] = None

  override protected def postProcessChunk(chunk: Table): Table = {
    require(currentRowIndex.isEmpty, "Previous cuDF row-index column was not consumed")
    val rowIndex = if (outputColumns.needsRowIndex) {
      Some(chunk.getColumn(0).incRefCount())
    } else {
      None
    }
    closeOnExcept(rowIndex) { _ =>
      val table = ParquetWithDeletionVectorUtils.dropFirstColumn(chunk)
      currentRowIndex = rowIndex
      table
    }
  }

  override def next: Table = {
    try {
      val table = super.next
      val rowIndex = currentRowIndex
      currentRowIndex = None
      outputColumns.populateAndClose(table, rowIndex)
    } catch {
      case t: Throwable =>
        currentRowIndex.foreach(_.close())
        currentRowIndex = None
        throw t
    }
  }

  override def close(): Unit = {
    currentRowIndex.foreach(_.close())
    currentRowIndex = None
    super.close()
  }
}

private object ParquetWithDeletionVectorUtils {
  def dropFirstColumn(table: Table): Table = {
    require(table.getNumberOfColumns > 0, "cuDF deletion-vector output has no row-index column")
    val columnIndexes = 1 until table.getNumberOfColumns
    withResource(table) { tableToClose =>
      new Table(columnIndexes.map(tableToClose.getColumn): _*)
    }
  }

  def separateRowIndex(
      table: Table,
      keepRowIndex: Boolean): (Table, Option[ColumnVector]) = {
    val rowIndex = if (keepRowIndex) Some(table.getColumn(0).incRefCount()) else None
    closeOnExcept(rowIndex) { _ =>
      dropFirstColumn(table) -> rowIndex
    }
  }
}

/** Builds a GPU table producer backed by cuDF's Parquet deletion-vector reader. */
object MakeParquetTableWithDVProducer extends Logging {
  def apply(
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
      deletionVectorInfos: Array[DeletionVector.DeletionVectorInfo],
      outputColumns: DeletionVectorOutputColumns = DeletionVectorOutputColumns()
  ): GpuDataProducer[Table] = {
    require(deletionVectorInfos.nonEmpty,
      "MakeParquetTableWithDVProducer should be used only when deletion vectors are present")

    debugDumpPrefix.foreach { prefix =>
      if (debugDumpAlways) {
        val path = DumpUtils.dumpBuffer(conf, buffers, prefix, ".parquet")
        logWarning(s"Wrote data for ${splits.mkString(", ")} to $path")
      }
    }

    if (useChunkedReader) {
      closeOnExcept(buffers) { _ =>
        closeOnExcept(deletionVectorInfos.map(_.serializedBitmap)) { _ =>
          DeltaParquetTableReader(conf, chunkSizeByteLimit,
            maxChunkedReaderMemoryUsageSizeBytes, opts, buffers, metrics, dateRebaseMode,
            timestampRebaseMode, isSchemaCaseSensitive, useFieldId, readDataSchema,
            clippedParquetSchema, splits, debugDumpPrefix, debugDumpAlways,
            deletionVectorInfos, outputColumns)
        }
      }
    } else {
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
              val dumpMessage = debugDumpPrefix.map { prefix =>
                if (!debugDumpAlways) {
                  val path = DumpUtils.dumpBuffer(conf, buffers, prefix, ".parquet")
                  s", data dumped to $path"
                } else {
                  ""
                }
              }.getOrElse("")
              throw new IOException(
                s"Error when processing ${splits.mkString("; ")}$dumpMessage", e)
          }
        }
      }
      val (tableWithoutIndex, rowIndex) =
        ParquetWithDeletionVectorUtils.separateRowIndex(table, outputColumns.needsRowIndex)
      closeOnExcept(rowIndex) { _ =>
        closeOnExcept(tableWithoutIndex) { _ =>
          GpuParquetScan.throwIfRebaseNeededInExceptionMode(tableWithoutIndex, dateRebaseMode,
            timestampRebaseMode)
          if (readDataSchema.length < tableWithoutIndex.getNumberOfColumns) {
            throw new QueryExecutionException(s"Expected ${readDataSchema.length} columns " +
              s"but read ${tableWithoutIndex.getNumberOfColumns} from ${splits.mkString("; ")}")
          }
          metrics(NUM_OUTPUT_BATCHES) += 1
          val evolvedSchemaTable = ParquetSchemaUtils.evolveSchemaIfNeededAndClose(
            tableWithoutIndex, clippedParquetSchema, readDataSchema, isSchemaCaseSensitive,
            useFieldId)
          val rebasedTable = GpuParquetScan.rebaseDateTime(evolvedSchemaTable, dateRebaseMode,
            timestampRebaseMode)
          val outputTable = outputColumns.populateAndClose(rebasedTable, rowIndex)
          GpuMetric.recordOutputBatchBytes(outputTable, metrics.get(GPU_OUTPUT_BATCH_BYTES))
          new SingleGpuDataProducer(outputTable)
        }
      }
    }
  }
}
