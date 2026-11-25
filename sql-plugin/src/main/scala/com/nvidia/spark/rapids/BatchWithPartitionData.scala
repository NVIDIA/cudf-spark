/*
 * Copyright (c) 2023-2024, NVIDIA CORPORATION.
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

import ai.rapids.cudf.ColumnVector
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.RmmRapidsRetryIterator.withRetry
import com.nvidia.spark.rapids.jni.GpuSplitAndRetryOOM

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * Wrapper class that specifies how many rows to replicate
 * the partition value.
 */
case class PartitionRowData(rowValue: InternalRow, rowNum: Int)

object PartitionRowData {
  def from(rowValues: Array[InternalRow], rowNums: Array[Int]): Array[PartitionRowData] = {
    rowValues.zip(rowNums).map {
      case (rowValue, rowNum) => PartitionRowData(rowValue, rowNum)
    }
  }

  def from(rowValues: Array[InternalRow], rowNums: Array[Long]): Array[PartitionRowData] = {
    rowValues.zip(rowNums).map {
      case (rowValue, rowNum) =>
        require(rowNum <= Integer.MAX_VALUE, s"Row number $rowNum exceeds max value of an integer.")
        PartitionRowData(rowValue, rowNum.toInt)
    }
  }
}

/**
 * Class to wrap columnar batch and partition rows data and utility functions to merge them.
 *
 * @param inputBatch           Input ColumnarBatch.
 * @param partitionedRowsData  Array of [[PartitionRowData]], where each entry contains an
 *                             InternalRow and a row number pair. These pairs specify how many
 *                             rows to replicate the partition value.
 * @param partitionSchema      Schema of the partitioned data.
 */
case class BatchWithPartitionData(
    inputBatch: SpillableColumnarBatch,
    partitionedRowsData: Array[PartitionRowData],
    partitionSchema: StructType) extends AutoCloseable {

  /**
   * Merges the partitioned data with the input ColumnarBatch.
   *
   * @return Merged ColumnarBatch.
   */
  def mergeWithPartitionData(): ColumnarBatch = {
    withResource(inputBatch.getColumnarBatch()) { columnarBatch =>
      if (partitionSchema.isEmpty) {
        columnarBatch // No partition columns, return the original batch.
      } else {
        val partBatch = closeOnExcept(buildPartitionColumns()) { partitionColumns =>
          val numRows = partitionColumns.head.getRowCount.toInt
          new ColumnarBatch(partitionColumns.toArray, numRows)
        }
        withResource(partBatch) { _ =>
          assert(columnarBatch.numRows == partBatch.numRows)
          GpuColumnVector.combineColumns(columnarBatch, partBatch)
        }
      }
    }
  }

  /**
   * Builds GPU-based partition columns by mapping partitioned data to a sequence of
   * GpuColumnVectors.
   *
   * @return A sequence of GpuColumnVectors, one for each partitioned column in the schema.
   */
  private def buildPartitionColumns(): Seq[GpuColumnVector] = {
    closeOnExcept(new Array[GpuColumnVector](partitionSchema.length)) { partitionColumns =>
      for ((field, colIndex) <- partitionSchema.zipWithIndex) {
        val dataType = field.dataType
        // Create an array to hold the individual columns for each partition.
        val singlePartCols = partitionedRowsData.safeMap {
          case PartitionRowData(valueRow, rowNum) =>
            val singleValue = valueRow.get(colIndex, dataType)
            withResource(GpuScalar.from(singleValue, dataType)) { singleScalar =>
              // Create a column vector from the GPU scalar, associated with the row number.
              ColumnVector.fromScalar(singleScalar, rowNum)
            }
        }

        // Concatenate the individual columns into a single column vector.
        val concatenatedColumn = withResource(singlePartCols) { _ =>
          if (singlePartCols.length > 1) {
            ColumnVector.concatenate(singlePartCols: _*)
          } else {
            singlePartCols.head.incRefCount()
          }
        }
        partitionColumns(colIndex) = GpuColumnVector.from(concatenatedColumn, field.dataType)
      }
      partitionColumns
    }
  }

  override def close(): Unit = {
    inputBatch.close()
  }
}

/**
 * An iterator that provides ColumnarBatch instances by merging existing batches with partition
 * columns. Each partition value column added is within the CUDF column size limit.
 * It uses `withRetry` to support retry framework and may spill data if needed.
 *
 * @param batchesWithPartitionData List of [[BatchWithPartitionData]] instances to be
 *                                 iterated.
 */
class BatchWithPartitionDataIterator(batchesWithPartitionData: Seq[BatchWithPartitionData])
  extends GpuColumnarBatchIterator(true) {
  private val batchIter = batchesWithPartitionData.toIterator
  private val retryIter = withRetry(batchIter,
    BatchWithPartitionDataUtils.splitBatchInHalf) { attempt =>
    attempt.mergeWithPartitionData()
  }

  override def hasNext: Boolean = retryIter.hasNext

  override def next(): ColumnarBatch = {
    retryIter.next()
  }

  override def doClose(): Unit = {
    // Close the batches that have not been iterated.
    batchIter.foreach(_.close())
  }
}

object BatchWithPartitionDataUtils {
  /**
   * Splits partition data (values and row counts) into smaller batches, ensuring that
   * size of column is less than the maximum column size. Then, it utilizes these smaller
   * partitioned batches to split the input batch and merges them to generate
   * an Iterator of split ColumnarBatches.
   *
   * Using an Iterator ensures that the actual merging does not happen until
   * the batch is required, thus avoiding GPU memory wastage.
   *
   * @param batch                 Input batch, will be closed after the call returns
   * @param partitionValues       Partition values collected from the batch
   * @param partitionRows         Row numbers collected from the batch, and it should have
   *                              the same size with "partitionValues"
   * @param partitionSchema       Partition schema
   * @param maxGpuColumnSizeBytes Maximum number of bytes for a GPU column
   * @return a new columnar batch iterator with partition values
   */
  def addPartitionValuesToBatch(
      batch: ColumnarBatch,
      partitionRows: Array[Long],
      partitionValues: Array[InternalRow],
      partitionSchema: StructType,
      maxGpuColumnSizeBytes: Long): GpuColumnarBatchIterator = {
    if (partitionSchema.nonEmpty) {
      withResource(batch) { _ =>
        require(partitionRows.length == partitionValues.length, "Partition rows and values must" +
          " be of same length")
        val partitionRowData = PartitionRowData.from(partitionValues, partitionRows)
        val partitionedGroups = splitPartitionDataIntoGroups(partitionRowData, partitionSchema,
          maxGpuColumnSizeBytes)
        val splitBatches = splitAndCombineBatchWithPartitionData(batch, partitionedGroups,
          partitionSchema)
        new BatchWithPartitionDataIterator(splitBatches)
      }
    } else {
      new SingleGpuColumnarBatchIterator(batch)
    }
  }

  /**
   * Adds a single set of partition values to all rows in a ColumnarBatch ensuring that
   * size of column is less than the maximum column size.
   *
   * @return a new columnar batch iterator with partition values
   * @see [[com.nvidia.spark.rapids.BatchWithPartitionDataUtils.addPartitionValuesToBatch]]
   */
  def addSinglePartitionValueToBatch(
      batch: ColumnarBatch,
      partitionValues: InternalRow,
      partitionSchema: StructType,
      maxGpuColumnSizeBytes: Long): GpuColumnarBatchIterator = {
    addPartitionValuesToBatch(batch, Array(batch.numRows), Array(partitionValues), partitionSchema,
      maxGpuColumnSizeBytes)
  }

  /**
   * Splits partitions into smaller batches, ensuring that the batch size
   * for each column does not exceed the maximum column size limit.
   *
   * Data structures:
   *  - sizeOfBatch:   Array that stores the size of batches for each column.
   *  - currentBatch:  Array used to hold the rows and partition values of the current batch.
   *  - resultBatches: Array that stores the resulting batches after splitting.
   *
   * Algorithm:
   *  - Initialize `sizeOfBatch` and `resultBatches`.
   *  - Iterate through `partRows`:
   *    - Get rowsInPartition - This can either be rows from new partition or
   *      rows remaining to be processed if there was a split.
   *    - Calculate the maximum number of rows we can fit in current batch without exceeding limit.
   *    - if max rows that fit < rows in partition, we need to split:
   *      - Append entry (InternalRow, max rows that fit) to the current batch.
   *      - Append current batch to the result batches.
   *      - Reset variables.
   *    - If there was no split,
   *      - Append entry (InternalRow, rowsInPartition) to the current batch.
   *      - Update sizeOfBatch with size of partition for each column.
   *      - This implies all remaining rows can be added in current batch without exceeding limit.
   *
   * Example:
   * {{{
   * Input:
   *    partition rows:   [10, 40, 70, 10, 11]
   *    partition values: [ [abc, ab], [bc, ab], [abc, bc], [aa, cc], [ade, fd] ]
   *    limit:  300 bytes
   *
   * Result:
   * [
   *  [ (10, [abc, ab]), (40, [bc, ab]), (63, [abc, bc]) ],
   *  [ (7, [abc, bc]), (10, [aa, cc]), (11, [ade, fd]) ]
   * ]
   * }}}
   *
   * @return An array of batches, containing (row counts, partition values) pairs,
   *         such that each batch's size is less than column size limit.
   */
  def splitPartitionDataIntoGroups(
      partitionRowData: Array[PartitionRowData],
      partSchema: StructType,
      maxGpuColumnSizeBytes: Long): Array[Array[PartitionRowData]] = {
    val resultBatches = ArrayBuffer[Array[PartitionRowData]]()
    val currentBatch = ArrayBuffer[PartitionRowData]()
    val sizeOfBatch = Array.fill(partSchema.length)(0L)
    var partIndex = 0
    var splitOccurred = false
    var rowsInPartition = 0
    var processNextPartition = true
    while (partIndex < partitionRowData.length) {
      if (processNextPartition) {
        rowsInPartition = partitionRowData(partIndex).rowNum
      }
      val valuesInPartition = partitionRowData(partIndex).rowValue
      // Calculate the maximum number of rows that can fit in current batch.
      val maxRows = calculateMaxRows(rowsInPartition, valuesInPartition, partSchema,
        sizeOfBatch, maxGpuColumnSizeBytes)
      // Splitting occurs if for any column, maximum rows we can fit is less than rows in partition.
      splitOccurred = maxRows < rowsInPartition
      if (splitOccurred) {
        currentBatch.append(PartitionRowData(valuesInPartition, maxRows))
        resultBatches.append(currentBatch.toArray)
        currentBatch.clear()
        java.util.Arrays.fill(sizeOfBatch, 0)
        rowsInPartition -= maxRows
      } else {
        // If there was no split, all rows can fit in current batch.
        currentBatch.append(PartitionRowData(valuesInPartition, rowsInPartition))
        val partitionSizes = calculatePartitionSizes(rowsInPartition, valuesInPartition, partSchema)
        sizeOfBatch.indices.foreach(i => sizeOfBatch(i) += partitionSizes(i))
      }
      // If there was no split or there are no rows to process, get next partition
      // else process remaining rows in current partition.
      processNextPartition = !splitOccurred || rowsInPartition == 0
      if(processNextPartition) {
        // Process next partition
        partIndex += 1
      }
    }
    // Add the last remaining batch to resultBatches
    if (currentBatch.nonEmpty) {
      resultBatches.append(currentBatch.toArray)
    }
    resultBatches.toArray
  }

  /**
   * Calculates the partition size for each column as 'size of single value * number of rows'
   */
  private def calculatePartitionSizes(rowNum: Int, values: InternalRow,
      partSchema: StructType): Seq[Long] = {
    partSchema.zipWithIndex.map {
      case (field, colIndex) if field.dataType == StringType
        && !values.isNullAt(colIndex) =>
        // Assumption: Columns of other data type would not have fit already. Do nothing.
        val sizeOfSingleValue = values.getUTF8String(colIndex).numBytes
        rowNum * sizeOfSingleValue
      case _ => 0L
    }
  }

  /**
   * Calculate the maximum number of rows that can fit into a batch without exceeding limit.
   * This value is capped at row numbers in the partition.
   */
  private def calculateMaxRows(rowNum: Int, values: InternalRow, partSchema: StructType,
      sizeOfBatch: Array[Long], maxGpuColumnSizeBytes: Long): Int = {
    partSchema.zipWithIndex.map {
      case (field, colIndex) if field.dataType == StringType
        && !values.isNullAt(colIndex) =>
        val sizeOfSingleValue = values.getUTF8String(colIndex).numBytes
        if (sizeOfSingleValue == 0) {
          // All rows can fit
          rowNum
        } else {
          val availableSpace = maxGpuColumnSizeBytes - sizeOfBatch(colIndex)
          val maxRows = (availableSpace / sizeOfSingleValue).toInt
          // Cap it at rowNum to ensure it doesn't exceed the available rows in the partition
          Math.min(maxRows, rowNum)
        }
      case _ => rowNum
    }.min
  }

  /**
   * Splits the input ColumnarBatch into smaller batches, wraps these batches with partition
   * data, and returns them as a sequence of [[BatchWithPartitionData]].
   *
   * This function does not take ownership of `batch`, and callers should make sure to close.
   *
   * @note Partition values are merged with the columnar batches lazily by the resulting Iterator
   *       to save GPU memory.
   * @param batch                     Input ColumnarBatch.
   * @param listOfPartitionedRowsData Array of arrays of [[PartitionRowData]], where each entry
   *                                  contains a list of InternalRow and a row number pair. These
   *                                  pairs specify how many rows to replicate the partition value.
   *                                  See the example in `splitPartitionDataIntoGroups()`.
   * @param partitionSchema           Schema of partition ColumnVectors.
   * @return Sequence of [[BatchWithPartitionData]]
   */
  private def splitAndCombineBatchWithPartitionData(
      batch: ColumnarBatch,
      listOfPartitionedRowsData: Array[Array[PartitionRowData]],
      partitionSchema: StructType): Seq[BatchWithPartitionData] = {
    val splitIndices = calculateSplitIndices(listOfPartitionedRowsData)
    closeOnExcept(splitColumnarBatch(batch, splitIndices)) { splitColumnarBatches =>
      val totalRowsInSplitBatches = splitColumnarBatches.map(_.numRows()).sum
      assert(totalRowsInSplitBatches == batch.numRows())
      assert(splitColumnarBatches.length == listOfPartitionedRowsData.length)
      // Combine the split GPU ColumnVectors with partition ColumnVectors.
      splitColumnarBatches.zip(listOfPartitionedRowsData).map {
        case (spillableBatch, partitionedRowsData) =>
          BatchWithPartitionData(spillableBatch, partitionedRowsData, partitionSchema)
      }
    }
  }

  /**
   * Helper method to calculate split indices for dividing ColumnarBatch based on the row counts.
   *
   * Example,
   * {{{
   * Input - Array of arrays having (row counts, InternalRow) pairs:
   * [
   *  [ (10, [abc, ab]), (40, [bc, ab]), (63, [abc, bc]) ],
   *  [ (7, [abc, bc]), (10, [aa, cc]), (11, [ade, fd]) ],
   *  [ (4, [ac, gbc]), (7, [aa, tt]), (15, [ate, afd]) ]
   * ]
   *
   * Result -
   *  - Row counts for each batch: [ 10+40+30, 7+10+11, 4+7+15 ] = [ 80, 28, 26 ]
   *  - Split Indices: [ 80, 108 ]
   *  - ColumnarBatch with 134 rows will be split in three batches having
   *    `[ 80, 28, 26 ]` rows in each.
   *}}}
   *
   * @param listOfPartitionedRowsData Array of arrays where each entry contains 'list of partition
   *                                  values and number of times it is replicated'. See example in
   *                                  `splitPartitionDataIntoGroups()`.
   * @return A sequence of split indices.
   */
  private def calculateSplitIndices(
      listOfPartitionedRowsData: Array[Array[PartitionRowData]]): Seq[Int] = {
    // Calculate the row counts for each batch
    val rowCountsForEachBatch = listOfPartitionedRowsData.map(partitionData =>
      partitionData.map {
        case PartitionRowData(_, rowNum) => rowNum
      }.sum
    )
    // Calculate split indices using cumulative sum
    rowCountsForEachBatch.scanLeft(0)(_ + _).drop(1).dropRight(1)
  }

  /**
   * Split a ColumnarBatch into multiple ColumnarBatches based on indices.
   */
  private def splitColumnarBatch(input: ColumnarBatch,
      splitIndices: Seq[Int]): Seq[SpillableColumnarBatch] = {
    if (splitIndices.isEmpty) {
      Seq(SpillableColumnarBatch(GpuColumnVector.incRefCounts(input),
        SpillPriorities.ACTIVE_ON_DECK_PRIORITY))
    } else if (input.numCols() == 0) {
      // calculate the number of rows for each batch based on the split indices
      val batchRows =
        splitIndices.head +: splitIndices.zip(splitIndices.tail :+ input.numRows()).map {
          case (startRow, endRow) => endRow - startRow
        }
      batchRows.map(numRows => new JustRowsColumnarBatch(numRows))
    } else {
      val schema = GpuColumnVector.extractTypes(input)
      val splitInput = withResource(GpuColumnVector.from(input)) { table =>
        table.contiguousSplit(splitIndices: _*)
      }
      splitInput.safeMap { ct =>
        SpillableColumnarBatch(ct, schema, SpillPriorities.ACTIVE_BATCHING_PRIORITY)
      }
    }
  }

  /**
   * Splits an array of `PartitionRowData` into two halves of equal size.
   *
   * Example 1,
   * {{{
   * Input:
   *   partitionedRowsData: [ (1000, [ab, cd]), (2000, [def, add]) ]
   *   target rows: 1500
   *
   * Result:
   *   [
   *      [ (1000, [ab, cd]), (500 [def, add]) ],
   *      [ (1500, [def, add]) ]
   *   ]
   * }}}
   *
   * Example 2,
   * {{{
   * Input:
   *   partitionedRowsData: [ (1000, [ab, cd]) ]
   *   target rows: 500
   *
   * Result:
   *   [
   *      [ (500, [ab, cd]) ],
   *      [ (500, [ab, cd]) ]
   *   ]
   * }}}
   *
   * @note This function ensures that splitting is possible even in cases where there is a single
   * large partition until there is only one row.
   */
  def splitPartitionDataInHalf(
      partitionedRowsData: Array[PartitionRowData]): Array[Array[PartitionRowData]] = {
    val totalRows = partitionedRowsData.map(_.rowNum).sum
    if (totalRows <= 1) {
      // cannot split input with one row
      Array(partitionedRowsData)
    } else {
      var remainingRows = totalRows / 2
      var rowsAddedToLeft = 0
      var rowsAddedToRight = 0
      val leftHalf = ArrayBuffer[PartitionRowData]()
      val rightHalf = ArrayBuffer[PartitionRowData]()
      partitionedRowsData.foreach { partitionRow: PartitionRowData =>
        if (remainingRows > 0) {
          // Add rows to the left partition, up to the remaining rows available
          val rowsToAddToLeft = Math.min(partitionRow.rowNum, remainingRows)
          leftHalf += partitionRow.copy(rowNum = rowsToAddToLeft)
          rowsAddedToLeft += rowsToAddToLeft
          remainingRows -= rowsToAddToLeft
          if (remainingRows <= 0) {
            // Add remaining rows to the right partition
            val rowsToAddToRight = partitionRow.rowNum - rowsToAddToLeft
            rightHalf += partitionRow.copy(rowNum = rowsToAddToRight)
            rowsAddedToRight += rowsToAddToRight
          }
        } else {
          rightHalf += partitionRow
          rowsAddedToRight += partitionRow.rowNum
        }
      }
      assert((rowsAddedToLeft + rowsAddedToRight) == totalRows)
      Array(leftHalf.toArray, rightHalf.toArray)
    }
  }

  /**
   * Splits a `BatchWithPartitionData` into two halves, each containing roughly half the data.
   * This function is used by the retry framework.
   */
  def splitBatchInHalf: BatchWithPartitionData => Seq[BatchWithPartitionData] = {
    (batchWithPartData: BatchWithPartitionData) => {
      withResource(batchWithPartData) { _ =>
        // Split partition rows data into two halves
        val splitPartitionData = splitPartitionDataInHalf(batchWithPartData.partitionedRowsData)
        if (splitPartitionData.length < 2) {
          throw new GpuSplitAndRetryOOM("GPU OutOfMemory: cannot split input with one row")
        }
        // Split the batch into two halves
        withResource(batchWithPartData.inputBatch.getColumnarBatch()) { cb =>
          splitAndCombineBatchWithPartitionData(cb, splitPartitionData,
            batchWithPartData.partitionSchema)
        }
      }
    }
  }
}
