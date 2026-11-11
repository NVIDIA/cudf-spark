/*
 * Copyright (c) 2020-2023, NVIDIA CORPORATION.
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

import com.nvidia.spark.rapids.ScalableTaskCompletion.onTaskCompletion

import org.apache.spark.TaskContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.execution.datasources.v2.FilePartitionReaderFactory
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * An adaptor class that provides an Iterator interface for a PartitionReader.
 */
class PartitionReaderIterator(reader: PartitionReader[ColumnarBatch])
    extends Iterator[ColumnarBatch] with AutoCloseable {
  // Don't install the callback if in a unit test
  Option(TaskContext.get()).foreach { tc =>
    onTaskCompletion(tc) {
      close()
    }
  }

  var hasNextResult: Option[Boolean] = None

  override def hasNext: Boolean = {
    if (hasNextResult.isEmpty) {
      hasNextResult = Some(reader.next())
    }
    hasNextResult.get
  }

  override def next(): ColumnarBatch = {
    if (hasNextResult.isEmpty) {
      hasNextResult = Some(reader.next())
    }
    hasNextResult = None
    reader.get()
  }

  override def close(): Unit = {
    reader.close()
  }
}

object PartitionReaderIterator {
  def buildReader(factory: FilePartitionReaderFactory)
  : PartitionedFile => Iterator[InternalRow] = {
    file: PartitionedFile => {
      val reader = factory.buildColumnarReader(file)
      new PartitionReaderIterator(reader).asInstanceOf[Iterator[InternalRow]]
    }
  }
}
