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

import java.util.{Map => JMap, Objects}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import com.nvidia.spark.rapids.{ConcatAndConsumeAll, DateTimeRebaseCorrected, GpuSemaphore,
  SpillableColumnarBatch, SpillPriorities}
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.RmmRapidsRetryIterator.withRetry
import com.nvidia.spark.rapids.iceberg.ShimUtils
import com.nvidia.spark.rapids.jni.GpuSplitAndRetryOOM
import com.nvidia.spark.rapids.parquet.ParquetFileInfoWithBlockMeta
import org.apache.hadoop.fs.Path
import org.apache.iceberg.{FileFormat, FileScanTask, MetadataColumns, Schema}
import org.apache.iceberg.expressions.Expressions
import org.apache.iceberg.shaded.org.apache.parquet.schema.{MessageType => ShadedMessageType,
  Type => ShadedType}
import org.apache.iceberg.spark.SparkSchemaUtil

import org.apache.spark.TaskContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.unsafe.types.UTF8String

object GpuIcebergMetadataReader {
  case class FileMetadata(path: String, rowCount: Long, constants: JMap[Integer, _])

  /** Prove eligibility from Iceberg metadata, without constructing or opening input files. */
  def plan(tasks: Seq[FileScanTask], schema: Schema,
      constants: FileScanTask => JMap[Integer, _]): Option[Seq[FileMetadata]] = {
    val fields = schema.columns().asScala
    val needsPhysicalMetadata = fields.exists { field =>
      Set(MetadataColumns.ROW_POSITION.fieldId(), MetadataColumns.IS_DELETED.fieldId(),
        ShimUtils.rowIdFieldId(), ShimUtils.lastUpdatedSequenceNumberFieldId())
        .contains(field.fieldId())
    }
    if (needsPhysicalMetadata || !tasks.forall(canUseRecordCount)) {
      None
    } else {
      val files = tasks.map { task =>
        FileMetadata(ShimUtils.locationOf(task.file()), task.file().recordCount(), constants(task))
      }
      if (files.forall { file =>
        fields.forall { field =>
          field.fieldId() == MetadataColumns.FILE_PATH.fieldId() ||
            file.constants.containsKey(field.fieldId())
        }
      }) {
        Some(files)
      } else {
        None
      }
    }
  }

  private def canUseRecordCount(task: FileScanTask): Boolean = {
    val file = task.file()
    // Offset-aware Iceberg tasks start at the first row group, after the Parquet header.
    // Such a task still represents every row if it extends through the end of the file.
    val firstOffset = Option(file.splitOffsets()).filterNot(_.isEmpty)
      .map(_.get(0).longValue()).getOrElse(0L)
    val wholeFile = task.start() >= 0 && task.start() <= firstOffset &&
      task.length() == file.fileSizeInBytes() - task.start()
    file.format() == FileFormat.PARQUET && file.recordCount() >= 0 && wholeFile &&
      task.deletes().isEmpty && task.residual() == Expressions.alwaysTrue()
  }

  // A conservative estimate for batch sizing, plus each variable-width leaf's data size.
  // Keep the leaves separate so a short value in one column cannot hide an oversized sibling.
  private case class RowSize(bytes: Long, leaves: Vector[Long]) {
    def +(other: RowSize): RowSize = RowSize(bytes + other.bytes, leaves ++ other.leaves)
  }

  private def rowSize(value: Any, dataType: DataType): RowSize = dataType match {
    case StringType | BinaryType =>
      val bytes = if (value == null) 0L else dataType match {
        case StringType => value.asInstanceOf[UTF8String].numBytes().toLong
        case _ => value.asInstanceOf[Array[Byte]].length.toLong
      }
      RowSize(bytes + 5, Vector(bytes)) // offsets and validity
    case struct: StructType =>
      struct.fields.zipWithIndex.foldLeft(RowSize(1, Vector.empty)) {
        case (size, (field, index)) =>
          val child = if (value == null) {
            null
          } else {
            value.asInstanceOf[InternalRow].get(index, field.dataType)
          }
          size + rowSize(child, field.dataType)
      }
    case _ => RowSize(dataType.defaultSize.toLong + 1, Vector.empty)
  }
}

/**
 * Materialize each file's constants before concatenating its rows with other files.
 * Pending work contains only metadata, so retries can split even a single large file without
 * retaining GPU allocations or reading a Parquet footer.
 */
class GpuIcebergMetadataReader(
    files: Seq[GpuIcebergMetadataReader.FileMetadata],
    override val conf: GpuIcebergParquetReaderConf) extends GpuIcebergParquetReader {
  import GpuIcebergMetadataReader._

  private val sparkSchema = SparkSchemaUtil.convert(conf.expectedSchema)
  private val sparkTypes = sparkSchema.fields.map(_.dataType)
  private val emptyParquetSchema = new ShadedMessageType("metadata", Seq.empty[ShadedType].asJava)
  private val maxRows = math.max(1, conf.maxBatchSizeRows)
  // The total byte bound also keeps string/binary offset columns below cuDF's row limit.
  private val maxBytes = math.max(1L,
    Seq(conf.maxBatchSizeBytes, conf.targetBatchSizeBytes, Int.MaxValue.toLong).min)
  private val columnLimit = math.min(conf.maxGpuColumnSizeBytes, Int.MaxValue.toLong)

  private class FileState(val file: FileMetadata) {
    private val values = conf.expectedSchema.columns().asScala.map { field =>
      if (file.constants.containsKey(field.fieldId())) {
        file.constants.get(field.fieldId())
      } else {
        UTF8String.fromString(new Path(file.path).toString)
      }
    }.toVector

    val size: RowSize = values.zip(sparkTypes)
      .foldLeft(RowSize(0, Vector.empty)) { case (size, (value, dataType)) =>
        size + rowSize(value, dataType)
      }

    def sameOutput(other: FileState): Boolean = values.zip(other.values).forall {
      case (left, right) =>
        Objects.deepEquals(left.asInstanceOf[AnyRef], right.asInstanceOf[AnyRef])
    }

    lazy val processor = new GpuParquetReaderPostProcessor(
      ParquetFileInfoWithBlockMeta(new Path(file.path), Seq.empty, InternalRow.empty,
        ShimUtils.unshadeParquetSchema(emptyParquetSchema), StructType(Nil),
        DateTimeRebaseCorrected, DateTimeRebaseCorrected, false, Seq.empty),
      file.constants, conf.expectedSchema, emptyParquetSchema, conf.metrics)
  }

  private case class FileRows(file: FileState, rows: Int)
  private case class Batch(parts: Vector[FileRows]) extends AutoCloseable {
    val rows: Int = parts.map(_.rows.toLong).sum.toInt
    override def close(): Unit = ()
  }

  private val fileIter = files.iterator.filter(_.rowCount > 0).map(new FileState(_)).buffered
  private var remainingRows = 0L
  private var closed = false

  private val groups = new Iterator[Batch] {
    override def hasNext: Boolean = !closed && fileIter.hasNext

    override def next(): Batch = {
      if (!hasNext) throw new NoSuchElementException("No more Iceberg metadata")
      val parts = ArrayBuffer.empty[FileRows]
      var rows = 0
      var bytes = 0L
      val leafBytes = Array.fill(fileIter.head.size.leaves.size)(0L)
      var full = false
      while (!full && fileIter.hasNext && rows < maxRows) {
        val file = fileIter.head
        if (remainingRows == 0) remainingRows = file.file.rowCount
        val byteRows = if (file.size.bytes == 0) maxRows.toLong else {
          val available = math.max(0L, (maxBytes - bytes) / file.size.bytes)
          // Like the data readers, allow one row larger than the soft batch byte limit.
          if (rows == 0) math.max(1L, available) else available
        }
        val columnRows = file.size.leaves.zip(leafBytes).foldLeft(maxRows.toLong) {
          case (limit, (size, used)) =>
            if (size == 0) limit else math.min(limit, (columnLimit - used) / size)
        }
        val take = Seq(remainingRows, (maxRows - rows).toLong, byteRows, columnRows).min.toInt
        if (take == 0) {
          require(rows > 0, "Iceberg metadata value exceeds the GPU column size limit")
          full = true
        } else {
          // Identical projected constants can share one materialization, including across files.
          parts.lastOption match {
            case Some(previous) if previous.file.sameOutput(file) =>
              parts(parts.size - 1) = previous.copy(rows = previous.rows + take)
            case _ => parts += FileRows(file, take)
          }
          rows += take
          bytes += file.size.bytes * take
          file.size.leaves.indices.foreach { i => leafBytes(i) += file.size.leaves(i) * take }
          remainingRows -= take
          if (remainingRows == 0) fileIter.next()
        }
      }
      Batch(parts.toVector)
    }
  }

  private def split(batch: Batch): Seq[Batch] = {
    if (batch.rows <= 1) throw new GpuSplitAndRetryOOM("Cannot split one metadata row")
    var leftRows = batch.rows / 2
    val left = ArrayBuffer.empty[FileRows]
    val right = ArrayBuffer.empty[FileRows]
    batch.parts.foreach { part =>
      val take = math.min(leftRows, part.rows)
      if (take > 0) left += part.copy(rows = take)
      if (take < part.rows) right += part.copy(rows = part.rows - take)
      leftRows -= take
    }
    Seq(Batch(left.toVector), Batch(right.toVector))
  }

  private val batches = withRetry(groups, split _) { batch =>
    GpuSemaphore.acquireIfNecessary(TaskContext.get())
    if (sparkTypes.isEmpty) {
      new ColumnarBatch(Array.empty, batch.rows)
    } else if (batch.parts.size == 1) {
      batch.parts.head.file.processor.process(new ColumnarBatch(Array.empty, batch.rows))
    } else {
      // Each part keeps its own post-processor until its constants have been materialized.
      // Spill completed parts while constructing the remaining parts of the output batch.
      val parts = batch.parts.safeMap { part =>
        val materialized = part.file.processor.process(new ColumnarBatch(Array.empty, part.rows))
        SpillableColumnarBatch(materialized, SpillPriorities.ACTIVE_BATCHING_PRIORITY)
      }
      withResource(parts) { _ =>
        ConcatAndConsumeAll.buildNonEmptyBatchFromTypes(
          parts.safeMap(_.getColumnarBatch()).toArray, sparkTypes)
      }
    }
  }

  override def hasNext: Boolean = !closed && batches.hasNext
  override def next(): ColumnarBatch = {
    if (!hasNext) throw new NoSuchElementException("No more Iceberg metadata batches")
    batches.next()
  }
  override def close(): Unit = {
    closed = true
    withResource(batches) { _ => }
  }
}
