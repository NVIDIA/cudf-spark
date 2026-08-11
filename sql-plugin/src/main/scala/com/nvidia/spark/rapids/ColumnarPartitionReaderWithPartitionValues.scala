/*
 * Copyright (c) 2019-2025, NVIDIA CORPORATION.
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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch


/**
 * A wrapper reader that always appends partition values to the ColumnarBatch produced by the input
 * reader `fileReader`. Each scalar value is splatted to a column with the same number of
 * rows as the batch returned by the reader.
 */
class ColumnarPartitionReaderWithPartitionValues(
    fileReader: PartitionReader[ColumnarBatch],
    partitionValues: InternalRow,
    partitionSchema: StructType,
    maxGpuColumnSizeBytes: Long) extends PartitionReader[ColumnarBatch] {
  override def next(): Boolean = {
    outputIter.hasNext || fileReader.next()
  }
  private var outputIter: Iterator[ColumnarBatch] = Iterator.empty

  override def get(): ColumnarBatch = {
    if (partitionSchema.isEmpty) {
      fileReader.get()
    } else if (outputIter.hasNext) {
      outputIter.next()
    } else {
      val fileBatch: ColumnarBatch = fileReader.get()
      outputIter = BatchWithPartitionDataUtils.addPartitionValuesToBatch(fileBatch,
        Array(fileBatch.numRows), Array(partitionValues), partitionSchema, maxGpuColumnSizeBytes)
      outputIter.next()
    }
  }

  override def close(): Unit = {
    fileReader.close()
  }
}

object ColumnarPartitionReaderWithPartitionValues {
  def newReader(partFile: PartitionedFile,
      baseReader: PartitionReader[ColumnarBatch],
      partitionSchema: StructType,
      maxGpuColumnSizeBytes: Long): PartitionReader[ColumnarBatch] = {
    new ColumnarPartitionReaderWithPartitionValues(baseReader, partFile.partitionValues,
      partitionSchema, maxGpuColumnSizeBytes)
  }
}
