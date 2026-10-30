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

package com.nvidia.spark.rapids.parquet

import scala.collection.mutable.ArrayBuffer

import ai.rapids.cudf.{ParquetChunkedReader => CudfParquetChunkedReader, ParquetOptions, Table}
import com.nvidia.spark.rapids.{GpuSemaphore, RetryableTableProducer}
import com.nvidia.spark.rapids.Arm.closeOnExcept
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.RmmRapidsRetryIterator.{withRestoreOnRetry, withRetryNoSplit}
import com.nvidia.spark.rapids.parquet.ParquetCachedBatchFiles.{FileInfo, StagedFile}

import org.apache.spark.TaskContext
import org.apache.spark.sql.columnar.CachedBatch
import org.apache.spark.sql.types.StructType

private[rapids] object ParquetCachedBatchReader {
  /**
   * The producer must be drained while holding the GPU semaphore, before returning to an RDD
   * iterator. Empty encoded records carry rows without creating a native reader.
   */
  final class ReadBatch private[parquet] (
      val numRows: Int,
      val producer: Option[RetryableTableProducer]) extends AutoCloseable {
    private var closed = false

    private[parquet] def isClosed: Boolean = closed

    override def close(): Unit = {
      if (!closed) {
        closed = true
        producer.foreach(_.close())
      }
    }
  }

  private[parquet] def fits(
      rows: Long, encoded: Long, estimated: Long, next: FileInfo, target: Long): Boolean = {
    next.rows <= Int.MaxValue.toLong - rows &&
      next.encodedBytes <= target - encoded &&
      next.estimatedDecodedBytes <= target - estimated
  }

  /** Encoded inputs remain owned until the producer closes. */
  private[parquet] final class TableProducer(
      openReader: () => ChunkedReader,
      resources: Seq[AutoCloseable]) extends ParquetChunkedTableProducer {
    override protected def createReader(): ChunkedReader = openReader()
    override protected def readerResources: Seq[AutoCloseable] = resources

    private def retry[T](body: => T): T = {
      checkpoint()
      withRetryNoSplit {
        withRestoreOnRetry(this) {
          GpuSemaphore.acquireIfNecessary(TaskContext.get())
          body
        }
      }
    }

    override def hasNext: Boolean = closeOnExcept(this) { _ =>
      retry { super.hasNext }
    }

    override def next: Table = closeOnExcept(this) { _ =>
      retry {
        if (!getReader.hasNext) {
          throw new NoSuchElementException("Cached Parquet reader exhausted")
        }
        super.next
      }
    }
  }
}

/**
 * Stages cache records one at a time and bundles them independently of their stored size.
 * Encoded and decoded-size estimates limit combining, but an indivisible record always progresses.
 */
private[rapids] final class ParquetCachedBatchReader(
    input: Iterator[CachedBatch],
    cacheSchema: StructType,
    options: ParquetOptions,
    targetBytes: Long,
    maxChunkReaderMemoryBytes: Long)
    extends Iterator[ParquetCachedBatchReader.ReadBatch] with AutoCloseable {
  import ParquetCachedBatchReader._

  require(targetBytes > 0, "Cached Parquet target size must be positive")
  require(maxChunkReaderMemoryBytes >= 0, "Chunked reader memory limit must be nonnegative")

  private var pendingFile: Option[StagedFile] = None
  private var pendingEmpty: Option[Int] = None
  private var activeBatch: Option[ReadBatch] = None
  private var closed = false

  Option(TaskContext.get()).foreach(_.addTaskCompletionListener[Unit](_ => close()))

  override def hasNext: Boolean = closeOnExcept(this) { _ =>
    !closed && (pendingFile.nonEmpty || pendingEmpty.nonEmpty || input.hasNext)
  }

  private def stageNext(): Either[Int, StagedFile] = input.next() match {
    case batch: ParquetCachedBatch =>
      require(batch.numRows >= 0, "Cached row count must be nonnegative")
      if (batch.buffer.isEmpty) {
        Left(batch.numRows)
      } else {
        val info = ParquetCachedBatchFiles.describe(batch, cacheSchema)
        Right(ParquetCachedBatchFiles.stage(batch, info))
      }
    case other =>
      throw new IllegalStateException(s"Expected ParquetCachedBatch but got ${other.getClass}")
  }

  override def next(): ReadBatch = closeOnExcept(this) { _ =>
    require(activeBatch.forall(_.isClosed),
      "The previous cached Parquet batch must be closed before reading another bundle")
    if (!hasNext) {
      throw new NoSuchElementException("Cached Parquet input exhausted")
    }
    val first = pendingEmpty match {
      case Some(rows) =>
        pendingEmpty = None
        Left(rows)
      case None =>
        pendingFile match {
          case Some(file) =>
            pendingFile = None
            Right(file)
          case None => stageNext()
        }
    }
    val batch = first match {
      case Left(rows) => new ReadBatch(rows, None)
      case Right(file) =>
        closeOnExcept(ArrayBuffer(file)) { files =>
          var rows = file.info.rows.toLong
          var encoded = file.info.encodedBytes.toLong
          var estimated = file.info.estimatedDecodedBytes
          var continue = true
          while (continue && rows < Int.MaxValue && encoded < targetBytes &&
              estimated < targetBytes && input.hasNext) {
            stageNext() match {
              case Left(emptyRows) =>
                pendingEmpty = Some(emptyRows)
                continue = false
              case Right(nextFile) =>
                if (fits(rows, encoded, estimated, nextFile.info, targetBytes) &&
                    nextFile.info.parquetSchema == file.info.parquetSchema) {
                  files += nextFile
                  rows += nextFile.info.rows
                  encoded += nextFile.info.encodedBytes
                  estimated += nextFile.info.estimatedDecodedBytes
                } else {
                  pendingFile = Some(nextFile)
                  continue = false
                }
            }
          }
          val logical = ParquetCachedBatchFiles.logicalFile(files.toVector)
          closeOnExcept(logical) { _ =>
            val producer = new TableProducer(
              () => ParquetChunkedReader(new CudfParquetChunkedReader(
                targetBytes, maxChunkReaderMemoryBytes, options, logical.buffers: _*)),
              Seq(logical) ++ files.toVector)
            new ReadBatch(rows.toInt, Some(producer))
          }
        }
    }
    activeBatch = Some(batch)
    batch
  }

  override def close(): Unit = {
    if (!closed) {
      closed = true
      val resources = activeBatch.toSeq ++ pendingFile.toSeq
      activeBatch = None
      pendingFile = None
      pendingEmpty = None
      resources.safeClose()
    }
  }
}
