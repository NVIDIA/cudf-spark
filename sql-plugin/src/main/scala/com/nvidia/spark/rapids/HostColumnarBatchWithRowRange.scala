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

package com.nvidia.spark.rapids

import java.util.{ArrayList, Optional}

import ai.rapids.cudf.{DType, HostColumnVector, HostColumnVectorCore, HostMemoryBuffer}
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.jni.GpuSplitAndRetryOOM

import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * A wrapper around HostColumnVector array that tracks a row range and supports splitting.
 * This is used for GPU OOM handling during Row-to-Columnar conversion.
 *
 * When GPU OOM occurs during host-to-GPU transfer, we can split this batch in half
 * and retry with smaller chunks. During slicing, the data buffer for fixed-width columns
 * is logically sliced (shared memory), but validity and offset buffers are copied to ensure
 * correct semantics for the sliced row range.
 *
 * Memory management uses reference counting: each instance increments the reference count
 * of the host columns on construction and decrements it on close. The host columns are
 * freed when the last reference is closed.
 *
 * @param hostColumns The underlying host column vectors (shared across splits)
 * @param startRow The starting row index (inclusive) for this range
 * @param numRows The number of rows in this range
 * @param dataTypes The Spark data types for each column
 */
class HostColumnarBatchWithRowRange private (
    val hostColumns: Array[HostColumnVector],
    val startRow: Int,
    val numRows: Int,
    val dataTypes: Array[DataType]) extends AutoCloseable {

  require(hostColumns != null, "hostColumns cannot be null")
  require(hostColumns.length == dataTypes.length,
    s"hostColumns length (${hostColumns.length}) must match dataTypes length (${dataTypes.length})")
  require(startRow >= 0, s"startRow must be non-negative, got $startRow")
  require(numRows >= 0, s"numRows must be non-negative, got $numRows")

  // Increment ref count for the host columns.
  hostColumns.foreach(_.incRefCount())

  /**
   * Copy the specified row range from host columns to GPU and return a ColumnarBatch.
   * This is the main entry point for GPU transfer.
   */
  def copyToGpu(): ColumnarBatch = {
    if (hostColumns.isEmpty) {
      return new ColumnarBatch(Array.empty, numRows)
    }

    val totalHostRows = hostColumns(0).getRowCount.toInt
    require(startRow <= totalHostRows,
      s"startRow=$startRow > totalHostRows=$totalHostRows")
    require(startRow + numRows <= totalHostRows,
      s"startRow+numRows=${startRow + numRows} > totalHostRows=$totalHostRows")

    if (startRow == 0 && numRows == totalHostRows) {
      copyAllToGpu()
    } else {
      copyRangeToGpu()
    }
  }

  private def copyAllToGpu(): ColumnarBatch = {
    val gpuColumns = new Array[org.apache.spark.sql.vectorized.ColumnVector](hostColumns.length)
    closeOnExcept(gpuColumns) { _ =>
      for (i <- hostColumns.indices) {
        closeOnExcept(hostColumns(i).copyToDevice()) { deviceColumn =>
          gpuColumns(i) = GpuColumnVector.from(deviceColumn, dataTypes(i))
        }
      }
      new ColumnarBatch(gpuColumns, numRows)
    }
  }

  private def copyRangeToGpu(): ColumnarBatch = {
    // Avoid copying full device columns and then slicing, which can still OOM.
    val gpuColumns = new Array[org.apache.spark.sql.vectorized.ColumnVector](hostColumns.length)
    closeOnExcept(gpuColumns) { _ =>
      for (i <- hostColumns.indices) {
        withResource(sliceHostColumn(hostColumns(i), startRow, numRows)) { slicedHost =>
          closeOnExcept(slicedHost.copyToDevice()) { deviceColumn =>
            gpuColumns(i) = GpuColumnVector.from(deviceColumn, dataTypes(i))
          }
        }
      }
      new ColumnarBatch(gpuColumns, numRows)
    }
  }

  /**
   * Create a host-side slice of the given column (by rows), returning a new HostColumnVector
   * that owns any buffers/slices it references. This is used to avoid full-column GPU copies
   * when retrying after GPU OOM.
   */
  private def sliceHostColumn(
      col: HostColumnVectorCore,
      startRow: Int,
      numRows: Int): HostColumnVector = {
    require(startRow >= 0, s"startRow must be non-negative, got $startRow")
    require(numRows >= 0, s"numRows must be non-negative, got $numRows")
    val totalRows = col.getRowCount.toInt
    require(startRow <= totalRows, s"startRow=$startRow > totalRows=$totalRows")
    require(startRow + numRows <= totalRows,
      s"startRow+numRows=${startRow + numRows} > totalRows=$totalRows")

    val dtype = col.getType

    // Slice validity (and compute nullCount for this slice)
    val (validSlice, nullCount) = sliceValidity(col, startRow, numRows)

    var dataSlice: HostMemoryBuffer = null
    var offsetsSlice: HostMemoryBuffer = null
    val children = new ArrayList[HostColumnVectorCore]()
    var success = false
    try {
      dtype match {
        case DType.LIST =>
          // Offsets map rows -> child element indices
          val origOffsets = col.getOffsets
          require(origOffsets != null, "LIST column offsets buffer is null")

          val startElem = origOffsets.getInt(startRow.toLong * 4L)
          val endElem = origOffsets.getInt((startRow + numRows).toLong * 4L)
          val numElems = endElem - startElem
          require(startElem >= 0 && endElem >= startElem,
            s"Invalid LIST offsets: startElem=$startElem endElem=$endElem")

          offsetsSlice = HostMemoryBuffer.allocate((numRows.toLong + 1L) * 4L)
          // Normalize offsets so the first element starts at 0
          var r = 0
          while (r <= numRows) {
            val v = origOffsets.getInt((startRow + r).toLong * 4L) - startElem
            offsetsSlice.setInt(r.toLong * 4L, v)
            r += 1
          }

          // Slice the child column by element indices
          val child = col.getChildColumnView(0)
          children.add(sliceHostColumn(child, startElem, numElems))

        case DType.STRUCT =>
          // STRUCT children all have the same row count
          var c = 0
          while (c < col.getNumChildren) {
            children.add(sliceHostColumn(col.getChildColumnView(c), startRow, numRows))
            c += 1
          }

        case DType.STRING =>
          val origOffsets = col.getOffsets
          require(origOffsets != null, "STRING column offsets buffer is null")
          val origData = col.getData

          val startByte = origOffsets.getInt(startRow.toLong * 4L)
          val endByte = origOffsets.getInt((startRow + numRows).toLong * 4L)
          val dataLen = endByte - startByte
          require(startByte >= 0 && endByte >= startByte,
            s"Invalid STRING offsets: startByte=$startByte endByte=$endByte")

          // Normalize offsets so the first string starts at 0
          offsetsSlice = HostMemoryBuffer.allocate((numRows.toLong + 1L) * 4L)
          var r = 0
          while (r <= numRows) {
            val v = origOffsets.getInt((startRow + r).toLong * 4L) - startByte
            offsetsSlice.setInt(r.toLong * 4L, v)
            r += 1
          }

          if (dataLen == 0) {
            if (numRows > nullCount) {
              // Existing empty strings, we must provide at least 1 byte of data.
              dataSlice = HostMemoryBuffer.allocate(1L)
              dataSlice.setByte(0L, 0.toByte)
            } else {
              // All rows are null (or 0 rows); safe to have no data buffer.
              dataSlice = null
            }
          } else {
            require(origData != null, "STRING column data buffer is null")
            dataSlice = origData.slice(startByte.toLong, dataLen.toLong)
          }

        case _ =>
          // Fixed-width (non-nested) types
          val origData = col.getData
          if (origData != null && numRows > 0) {
            val startByte = startRow.toLong * dtype.getSizeInBytes
            val dataLen = numRows.toLong * dtype.getSizeInBytes
            dataSlice = origData.slice(startByte, dataLen)
          }
      }

      val sliced = new HostColumnVector(
        dtype,
        numRows.toLong,
        Optional.of(nullCount: java.lang.Long),
        dataSlice,
        validSlice,
        offsetsSlice,
        children
      )
      success = true
      sliced
    } finally {
      if (!success) {
        if (dataSlice != null) dataSlice.close()
        if (validSlice != null) validSlice.close()
        if (offsetsSlice != null) offsetsSlice.close()
        val it = children.iterator()
        while (it.hasNext) {
          val c = it.next()
          if (c != null) c.close()
        }
      }
    }
  }

  /**
   * Slice a validity buffer (if present) for the given row range, returning (newValidity,
   * nullCount). If the input column has no validity buffer, returns (null, 0).
   */
  private def sliceValidity(
      col: HostColumnVectorCore,
      startRow: Int,
      numRows: Int): (HostMemoryBuffer, Long) = {
    if (numRows == 0 || !col.hasValidityVector) {
      return (null, 0L)
    }
    val validLen = validityBufferSize(numRows)
    // Default all rows to valid; null rows will be flipped to 0 bits
    val out = HostMemoryBuffer.allocate(validLen)
    val nullCount = closeOnExcept(out) { _ =>
      out.setMemory(0L, validLen, 0xFF.toByte)
      var count = 0L
      var r = 0
      while (r < numRows) {
        if (col.isNull((startRow + r).toLong)) {
          count += 1
          RapidsHostColumnBuilder.setNullAt(out, r.toLong)
        }
        r += 1
      }
      count
    }
    if (nullCount == 0L) {
      out.close()
      (null, 0L)
    } else {
      (out, nullCount)
    }
  }

  private def validityBufferSize(numRows: Int): Long = {
    // Matches cudf::bitmask_allocation_size_bytes (64-byte padding)
    val actualBytes = (numRows.toLong + 7L) >> 3
    ((actualBytes + 63L) >> 6) << 6
  }

  /**
   * Close this instance and decrement the reference count on the host columns.
   * The host columns will be freed when the last reference is closed.
   */
  override def close(): Unit = {
    hostColumns.safeClose()
  }

  override def toString: String = {
    val totalRows = if (hostColumns.nonEmpty) hostColumns(0).getRowCount else 0
    s"HostColumnarBatchWithRowRange(startRow=$startRow, numRows=$numRows, " +
      s"totalHostRows=$totalRows, numCols=${hostColumns.length})"
  }
}

object HostColumnarBatchWithRowRange {

  /**
   * Split a HostColumnarBatchWithRowRange in half by rows.
   * Returns two new HostColumnarBatchWithRowRange instances covering the first and second half.
   *
   * Memory management uses reference counting:
   * - The input batch is closed by this method (decrements ref count)
   * - Each new split increments the ref count on the shared host columns
   * - Host columns are freed when the last split is closed
   */
  def splitInHalf(batch: HostColumnarBatchWithRowRange): Seq[HostColumnarBatchWithRowRange] = {
    withResource(batch) { _ =>
      if (batch.numRows <= 1) {
        throw new GpuSplitAndRetryOOM(
          s"GPU OutOfMemory: cannot split host batch with only ${batch.numRows} row(s)")
      }

      val firstHalfRows = batch.numRows / 2
      val secondHalfRows = batch.numRows - firstHalfRows

      Seq(
        new HostColumnarBatchWithRowRange(
          batch.hostColumns, batch.startRow, firstHalfRows, batch.dataTypes),
        new HostColumnarBatchWithRowRange(
          batch.hostColumns, batch.startRow + firstHalfRows, secondHalfRows, batch.dataTypes)
      )
    }
  }

  /**
   * Create a HostColumnarBatchWithRowRange from freshly built host columns.
   *
   * This factory method is used when creating the initial batch from the builder.
   * It sets startRow to 0 since the host columns contain exactly the rows we need.
   * For split batches with non-zero startRow, use the private constructor via splitInHalf.
   *
   * Note: This increments the reference count on the host columns. The caller is still
   * responsible for closing their own reference to the host columns separately.
   */
  def apply(
      hostColumns: Array[HostColumnVector],
      numRows: Int,
      dataTypes: Array[DataType]): HostColumnarBatchWithRowRange = {
    hostColumns.zipWithIndex.foreach { case (col, i) =>
      require(col.getRowCount.toInt == numRows,
        s"numRows ($numRows) must match hostColumns[$i] rowCount (${col.getRowCount})")
    }
    // The builder that created these host columns owns the initial reference and will close it
    // when the builder is closed. We increment the reference count here to take a new independent
    // reference. This ensures the host columns remain valid for the lifetime of this
    // HostColumnarBatchWithRowRange instance, even after the builder releases its reference.
    new HostColumnarBatchWithRowRange(hostColumns, 0, numRows, dataTypes)
  }
}
