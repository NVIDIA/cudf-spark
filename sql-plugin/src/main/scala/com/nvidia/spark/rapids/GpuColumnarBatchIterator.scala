/*
 * Copyright (c) 2022-2023, NVIDIA CORPORATION.
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

import com.nvidia.spark.rapids.Arm.closeOnExcept
import com.nvidia.spark.rapids.ScalableTaskCompletion.onTaskCompletion

import org.apache.spark.TaskContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * An abstract columnar batch iterator that gives options for auto closing
 * when the associated task completes. Also provides idempotent close semantics.
 *
 * This iterator follows the semantics of GPU RDD columnar batch iterators too in that
 * if a batch is returned by next it is the responsibility of the receiver to close
 * it.
 *
 * Generally it is good practice if hasNext would return false than any outstanding resources
 * should be closed so waiting for an explicit close is not needed.
 *
 * @param closeWithTask should the Iterator be closed at task completion or not.
 */
abstract class GpuColumnarBatchIterator(closeWithTask: Boolean)
    extends Iterator[ColumnarBatch] with AutoCloseable {
  private var isClosed = false
  if (closeWithTask) {
    // Don't install the callback if in a unit test
    Option(TaskContext.get()).foreach { tc =>
      onTaskCompletion(tc) {
        close()
      }
    }
  }

  final override def close(): Unit = {
    if (!isClosed) {
      doClose()
    }
    isClosed = true
  }

  def doClose(): Unit
}

object EmptyGpuColumnarBatchIterator extends GpuColumnarBatchIterator(false) {
  override def hasNext: Boolean = false
  override def next(): ColumnarBatch = throw new NoSuchElementException()
  override def doClose(): Unit = {}
}

class SingleGpuColumnarBatchIterator(private var batch: ColumnarBatch)
    extends GpuColumnarBatchIterator(true) {
  override def hasNext: Boolean = batch != null

  override def next(): ColumnarBatch = {
    if (batch == null) {
      throw new NoSuchElementException()
    }
    val ret = batch
    batch = null
    ret
  }

  override def doClose(): Unit = {
    if (batch != null) {
      batch.close()
      batch = null
    }
  }
}

/**
 * An iterator that appends partition columns to each batch in the input iterator.
 *
 * This iterator will correctly handle multiple partition values for each partition column
 * for a chunked read.
 *
 * @param inputIter the input iterator of GPU columnar batch
 * @param partValues partition values collected from all the batches in the input iterator
 * @param partRowNums row numbers collected from all the batches in the input iterator, it
 *                    should have the same size with "partValues".
 * @param partSchema the partition schema
 * @param maxGpuColumnSizeBytes maximum number of bytes for a GPU column
 */
class GpuColumnarBatchWithPartitionValuesIterator(
    inputIter: Iterator[ColumnarBatch],
    partValues: Array[InternalRow],
    partRowNums: Array[Long],
    partSchema: StructType,
    maxGpuColumnSizeBytes: Long) extends Iterator[ColumnarBatch] {
  assert(partValues.length == partRowNums.length)

  private var leftValues: Array[InternalRow] = partValues
  private var leftRowNums: Array[Long] = partRowNums
  private var outputIter: Iterator[ColumnarBatch] = Iterator.empty

  override def hasNext: Boolean = outputIter.hasNext || inputIter.hasNext

  override def next(): ColumnarBatch = {
    if (!hasNext) {
      throw new NoSuchElementException()
    } else if (outputIter.hasNext) {
      outputIter.next()
    } else {
      val batch = inputIter.next()
      if (partSchema.nonEmpty) {
        val (readPartValues, readPartRows) = closeOnExcept(batch) { _ =>
          computeValuesAndRowNumsForBatch(batch.numRows())
        }
        outputIter = BatchWithPartitionDataUtils.addPartitionValuesToBatch(batch, readPartRows,
          readPartValues, partSchema, maxGpuColumnSizeBytes)
        outputIter.next()
      } else {
        batch
      }
    }
  }

  private[this] def computeValuesAndRowNumsForBatch(batchRowNum: Int):
      (Array[InternalRow], Array[Long]) = {
    val leftTotalRowNum = leftRowNums.sum
    if (leftTotalRowNum == batchRowNum) {
      // case a) All is read
      (leftValues, leftRowNums)
    } else if (leftTotalRowNum > batchRowNum) {
      // case b) Partial is read
      var consumedRowNum = 0L
      var pos = 0
      // 1: Locate the position for the current read
      while (consumedRowNum < batchRowNum) {
        // Not test "pos < leftRowNums.length" here because this is ensured
        // by "leftTotalRowNum > batchRowNum"
        consumedRowNum += leftRowNums(pos)
        pos += 1
      }
      // 2: Split the arrays of values and row numbers for the current read
      val (readValues, remainValues) = leftValues.splitAt(pos)
      val (readRowNums, remainRowNums) = leftRowNums.splitAt(pos)
      if (consumedRowNum == batchRowNum) {
        // Good luck! Just at the edge of a partition
        leftValues = remainValues
        leftRowNums = remainRowNums
      } else { // consumedRowNum > batchRowNum, and pos > 0
        // A worse case, inside a partition, need to correct the splits.
        // e.g.
        //    Row numbers: [2, 3, 2]
        //    Batch row number: 4,
        // The original split result is:  [2, 3] and [2]
        // And the corrected output is: [2, 2] and [1, 2]
        val remainRowNumForSplitPart = consumedRowNum - batchRowNum
        leftRowNums = remainRowNumForSplitPart +: remainRowNums
        readRowNums(pos - 1) = readRowNums(pos - 1) - remainRowNumForSplitPart
        leftValues = readValues(pos - 1) +: remainValues
      }
      (readValues, readRowNums)
    } else { // leftTotalRowNum < batchRowNum
      // This should not happen, so throw an exception
      throw new IllegalStateException(s"Partition row number <$leftTotalRowNum> " +
        s"does not match that of the read batch <$batchRowNum>.")
    }
  }
}
