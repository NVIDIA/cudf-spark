/*
 * Copyright (c) 2020-2025, NVIDIA CORPORATION.
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

import scala.collection.mutable.ArrayBuffer

import ai.rapids.cudf.Table
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.RapidsPluginImplicits.AutoCloseableProducingSeq

import org.apache.spark.sql.types.{ArrayType, DataType, DataTypes, MapType, StructType}

/**
 * Utility class with methods for calculating various metrics about GPU memory usage
 * prior to allocation, along with some operations with batches.
 */
object GpuBatchUtils {

  /** Validity buffers are 64 byte aligned */
  val VALIDITY_BUFFER_BOUNDARY_BYTES = 64

  /** Validity buffers are 64 byte aligned and each byte represents 8 rows */
  val VALIDITY_BUFFER_BOUNDARY_ROWS = VALIDITY_BUFFER_BOUNDARY_BYTES * 8

  /** Number of bytes per offset (32 bit) */
  val OFFSET_BYTES = 4

  /** Estimate the number of rows required to meet a batch size limit */
  def estimateRowCount(
      desiredBatchSizeBytes: Long,
      currentBatchSize: Long,
      currentBatchRowCount: Long): Int = {
    assert(currentBatchRowCount > 0, "batch must contain at least one row")
    val targetRowCount: Long = if (currentBatchSize > desiredBatchSizeBytes) {
      currentBatchRowCount
    } else if (currentBatchSize == 0) {
      //  batch size can be 0 when doing a count() operation and the actual data isn't needed
      currentBatchRowCount
    } else {
      ((desiredBatchSizeBytes / currentBatchSize.floatValue()) * currentBatchRowCount).toLong
    }
    targetRowCount.min(Integer.MAX_VALUE).toInt
  }

  /** Estimate the amount of GPU memory a batch of rows will occupy once converted */
  def estimateGpuMemory(schema: StructType, rowCount: Long): Long = {
    schema.fields.indices.map(estimateGpuMemory(schema, _, rowCount)).sum
  }

  /** Estimate the amount of GPU memory a batch of rows will occupy once converted */
  def estimateGpuMemory(schema: StructType, columnIndex: Int, rowCount: Long): Long = {
    val field = schema.fields(columnIndex)
    estimateGpuMemory(field.dataType, field.nullable, rowCount)
  }

  /**
   * Get the minimum size a column could be that matches these conditions.
   */
  def minGpuMemory(dataType:DataType, nullable: Boolean, rowCount: Long,
      includeOffset: Boolean = true): Long = {
    val validityBufferSize = if (nullable) {
      calculateValidityBufferSize(rowCount)
    } else {
      0
    }

    val dataSize = dataType match {
      case DataTypes.BinaryType | DataTypes.StringType | _: MapType | _: ArrayType=>
        // For nested types (like list or string) the smallest possible size is when
        // each row is empty (length 0). In that case there is no data, just offsets
        // and all of the offsets are 0.
        if (includeOffset) calculateOffsetBufferSize(rowCount) else 0L
      case dt: StructType =>
        dt.fields.map { f =>
          minGpuMemory(f.dataType, f.nullable, rowCount, includeOffset)
        }.sum
      case dt =>
        dt.defaultSize * rowCount
    }
    dataSize + validityBufferSize
  }

  def estimateGpuMemory(dataType: DataType, nullable: Boolean, rowCount: Long): Long = {
    val validityBufferSize = if (nullable) {
      calculateValidityBufferSize(rowCount)
    } else {
      0
    }
    val dataSize = dataType match {
      case dt@DataTypes.BinaryType =>
        val offsetBufferSize = calculateOffsetBufferSize(rowCount)
        val dataSize = dt.defaultSize * rowCount
        dataSize + offsetBufferSize
      case dt@DataTypes.StringType =>
        val offsetBufferSize = calculateOffsetBufferSize(rowCount)
        val dataSize = dt.defaultSize * rowCount
        dataSize + offsetBufferSize
      case dt: MapType =>
        // The Spark default map size assumes one entry for good or bad
        calculateOffsetBufferSize(rowCount) +
            estimateGpuMemory(dt.keyType, false, rowCount) +
            estimateGpuMemory(dt.valueType, dt.valueContainsNull, rowCount)
      case dt: ArrayType =>
        // The Spark default array size assumes one entry for good or bad
        calculateOffsetBufferSize(rowCount) +
            estimateGpuMemory(dt.elementType, dt.containsNull, rowCount)
      case dt: StructType =>
        dt.fields.map { f =>
          estimateGpuMemory(f.dataType, f.nullable, rowCount)
        }.sum
      case dt =>
        dt.defaultSize * rowCount
    }
    dataSize + validityBufferSize
  }

  def calculateValidityBufferSize(rows: Long): Long = {
    roundToBoundary((rows + 7)/8, 64)
  }

  def calculateOffsetBufferSize(rows: Long): Long = {
    (rows+1) * 4 // 32 bit offsets
  }

  /**
   * Generate indices which evenly splitting input batch
   *
   * @param rows      number of rows of input batch
   * @param numSplits desired number of splits
   * @return splitting indices
   */
  def generateSplitIndices(rows: Long, numSplits: Int): Array[Int] = {
    require(rows > 0, s"invalid input rows $rows")
    require(numSplits > 0, s"invalid numSplits $numSplits")
    val baseIncrement = (rows / numSplits).toInt
    var extraIncrements = (rows % numSplits).toInt
    val indicesBuf = ArrayBuffer[Int]()
    (1 until numSplits).foldLeft(0) { case (last, _) =>
      val current = if (extraIncrements > 0) {
        extraIncrements -= 1
        last + baseIncrement + 1
      } else {
        last + baseIncrement
      }
      indicesBuf += current
      current
    }
    indicesBuf.toArray
  }

  def isVariableWidth(dt: DataType): Boolean = !isFixedWidth(dt)

  def isFixedWidth(dt: DataType): Boolean = dt match {
    case DataTypes.StringType | DataTypes.BinaryType => false
    case _: ArrayType  => false
    case _: StructType  => false
    case _: MapType  => false
    case _ => true
  }

  private def roundToBoundary(bytes: Long, boundary: Int): Long = {
    val remainder = bytes % boundary
    if (remainder > 0) {
      bytes + boundary - remainder
    } else {
      bytes
    }
  }

  /**
   * Concatenate the input batches into a single one.
   * The caller is responsible for closing the returned batch.
   *
   * @param spillBatches the batches to be concatenated, will be closed after the call
   *                     returns.
   * @return the concatenated SpillableColumnarBatch or None if the input is empty.
   */
  def concatSpillBatchesAndClose(
      spillBatches: Seq[SpillableColumnarBatch]): Option[SpillableColumnarBatch] = {
    val retBatch = if (spillBatches.length >= 2) {
      // two or more batches, concatenate them
      val (concatTable, types) = RmmRapidsRetryIterator.withRetryNoSplit(spillBatches) { _ =>
        withResource(spillBatches.safeMap(_.getColumnarBatch())) { batches =>
          val batchTypes = GpuColumnVector.extractTypes(batches.head)
          withResource(batches.safeMap(GpuColumnVector.from)) { tables =>
            (Table.concatenate(tables: _*), batchTypes)
          }
        }
      }
      // Make the concatenated table spillable.
      withResource(concatTable) { _ =>
        SpillableColumnarBatch(GpuColumnVector.from(concatTable, types),
          SpillPriorities.ACTIVE_BATCHING_PRIORITY)
      }
    } else if (spillBatches.length == 1) {
      // only one batch
      spillBatches.head
    } else null

    Option(retBatch)
  }
}
