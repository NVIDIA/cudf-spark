/*
 * Copyright (c) 2021-2026, NVIDIA CORPORATION.
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

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.hadoop.fs.Path

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType

case class CombineConf(
    combineThresholdSize: Long, // The size to combine to when combining small files
    combineWaitTime: Int) // The amount of time to wait for other files ready for combination.

// TODO: Refactor thread pool components into a common utility, since it is not specific to
//  multi-file reading.
trait ThreadPoolConf {
  /**
   * The maximum number of threads used by the thread pool, not necessarily the final number
   */
  def maxThreadNumber: Int

  /**
   * Whether to create pools for each Spark stage, only for testing for now
   */
  def stageLevelPool: Boolean
}

case class DefaultThreadPoolConf(
    maxThreadNumber: Int,
    stageLevelPool: Boolean) extends ThreadPoolConf

case class MemoryBoundedPoolConf(
    maxThreadNumber: Int,
    stageLevelPool: Boolean,
    memoryCapacity: Long, // The maximum host memory being used in bytes, must be > 0
    waitMemTimeoutMs: Long // The timeout for acquiring host memory in milliseconds
) extends ThreadPoolConf

// A trait to describe a data block info
trait DataBlockBase {
  // get the row number of this data block
  def getRowCount: Long
  // the data read size
  def getReadDataSize: Long
  // the block size to be used to slice the whole HostMemoryBuffer
  def getBlockSize: Long
}

/**
 * A common trait for different schema in the MultiFileCoalescingPartitionReaderBase.
 *
 * The sub-class should wrap the real schema for the specific file format
 */
trait SchemaBase {
  def isEmpty: Boolean
}

/**
 * A common trait for the extra information for different file format.
 */
trait ExtraInfo extends AutoCloseable {
  override def close(): Unit = {}
}

/**
 * A single block info of a file,
 * Eg, A parquet file has 3 RowGroup, then it will produce 3 SingleBlockInfoWithMeta
 */
trait SingleDataBlockInfo {
  def filePath: Path // file path info
  def partitionValues: InternalRow // partition value
  def dataBlock: DataBlockBase // a single block info of a single file
  def schema: SchemaBase // schema information
  def readSchema: StructType // read schema information
  def extraInfo: ExtraInfo // extra information
}

/**
 * A context lives during the whole process of reading partitioned files
 * to a batch buffer (aka HostMemoryBuffer) to build a memory file.
 * Children can extend this to add more necessary fields.
 * @param origChunkedBlocks mapping of file path to data blocks
 * @param schema schema info
 */
class BatchContext(
  val origChunkedBlocks: mutable.LinkedHashMap[Path, ArrayBuffer[DataBlockBase]],
  val schema: SchemaBase) {}
