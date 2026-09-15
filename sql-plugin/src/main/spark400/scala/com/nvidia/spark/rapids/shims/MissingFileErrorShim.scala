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

/*** spark-rapids-shim-json-lines
{"spark": "400"}
{"spark": "400db173"}
{"spark": "401"}
{"spark": "402"}
{"spark": "403"}
{"spark": "404"}
{"spark": "411"}
{"spark": "412"}
{"spark": "413"}
{"spark": "420"}
{"spark": "500"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.shims

import java.io.FileNotFoundException
import java.util.concurrent.ExecutionException

import com.nvidia.spark.rapids.GpuFileNotFoundException

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.execution.datasources.FilePartition
import org.apache.spark.sql.execution.datasources.v2.FileDataSourceV2
import org.apache.spark.sql.vectorized.ColumnarBatch

object MissingFileErrorShim {
  def wrapReaderFactory(readerFactory: PartitionReaderFactory): PartitionReaderFactory =
    new PartitionReaderFactory {
      override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
        val reader = try {
          readerFactory.createReader(partition)
        } catch {
          case error: FileNotFoundException => rethrowMissingFile(partition, error)
          case error: ExecutionException => rethrowMissingFile(partition, error)
        }
        wrapReader(partition, reader)
      }

      override def createColumnarReader(
          partition: InputPartition): PartitionReader[ColumnarBatch] = {
        val reader = try {
          readerFactory.createColumnarReader(partition)
        } catch {
          case error: FileNotFoundException => rethrowMissingFile(partition, error)
          case error: ExecutionException => rethrowMissingFile(partition, error)
        }
        wrapReader(partition, reader)
      }

      override def supportColumnarReads(partition: InputPartition): Boolean =
        readerFactory.supportColumnarReads(partition)
    }

  private def wrapReader[T](
      partition: InputPartition,
      reader: PartitionReader[T]): PartitionReader[T] = {
    new PartitionReader[T] {
      override def next(): Boolean = try {
        reader.next()
      } catch {
        case error: FileNotFoundException => rethrowMissingFile(partition, error)
        case error: ExecutionException => rethrowMissingFile(partition, error)
      }

      override def get(): T = try {
        reader.get()
      } catch {
        case error: FileNotFoundException => rethrowMissingFile(partition, error)
        case error: ExecutionException => rethrowMissingFile(partition, error)
      }

      override def close(): Unit = reader.close()
    }
  }

  private def rethrowMissingFile(partition: InputPartition, error: Throwable): Nothing = {
    error match {
      case missingFile: FileNotFoundException =>
        throw convertMissingFile(partition, missingFile)
      case wrapped: ExecutionException =>
        wrapped.getCause match {
          case missingFile: FileNotFoundException =>
            throw convertMissingFile(partition, missingFile)
          case _ => throw wrapped
        }
      case _ => throw error
    }
  }

  private def convertMissingFile(
      partition: InputPartition,
      error: FileNotFoundException): Throwable = {
    val (filePath, originalError) = error match {
      case GpuFileNotFoundException(path, originalException) =>
        (Some(path), originalException)
      case _ =>
        (singleFilePath(partition), error)
    }
    convert(filePath, originalError, includeRefreshHint = false)
  }

  private def singleFilePath(partition: InputPartition): Option[String] = {
    Option(partition).collect { case filePartition: FilePartition =>
      SparkShimImpl.getPartitionFiles(filePartition)
    }.filter(_.length == 1).map(_.head.filePath.toString)
  }

  def convert(
      filePath: Option[String],
      error: FileNotFoundException,
      includeRefreshHint: Boolean): Throwable = filePath match {
    case Some(path) => FileDataSourceV2.attachFilePath(path, error)
    case None => error
  }
}
