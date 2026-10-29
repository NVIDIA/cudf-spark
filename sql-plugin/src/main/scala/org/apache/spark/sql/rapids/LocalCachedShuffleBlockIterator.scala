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

package org.apache.spark.sql.rapids

import com.nvidia.spark.rapids.ShuffleBufferCatalog

import org.apache.spark.shuffle.rapids.{RapidsShuffleFetchFailedException, ShuffleOutputGoneException}
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.storage.{BlockManagerId, ShuffleBlockId}

/**
 * Reads the batches of one shuffle block cached on this executor. When the catalog finds the
 * block's output gone, as `ShuffleOutputGoneException` says, the read ends in a fetch failure,
 * so Spark reruns the map that wrote it. Any other failure passes through unchanged.
 *
 * @param location where the block's MapStatus says it is, which Spark's
 *                 `ShuffleStatus.removeMapOutput` must match exactly to drop the map's output
 * @param mapIndex the index of the map that wrote the block
 */
private[rapids] class LocalCachedShuffleBlockIterator(
    catalog: ShuffleBufferCatalog,
    blockId: ShuffleBlockId,
    sparkTypes: Array[DataType],
    location: BlockManagerId,
    mapIndex: Int) extends Iterator[ColumnarBatch] {

  private[this] val batches: Iterator[ColumnarBatch] =
    translateOutputGoneToFetchFailure(catalog.getColumnarBatchIterator(blockId, sparkTypes))

  override def hasNext: Boolean = translateOutputGoneToFetchFailure(batches.hasNext)

  override def next(): ColumnarBatch = translateOutputGoneToFetchFailure(batches.next())

  private def translateOutputGoneToFetchFailure[T](body: => T): T =
    try {
      body
    } catch {
      // built only here, since its constructor marks the thread's task as fetch-failed
      case e: ShuffleOutputGoneException =>
        throw new RapidsShuffleFetchFailedException(location, blockId.shuffleId, blockId.mapId,
          mapIndex, blockId.reduceId,
          s"The cached shuffle output of $blockId is gone from this executor", e)
    }
}
