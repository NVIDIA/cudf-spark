/*
 * Copyright (c) 2023-2026, NVIDIA CORPORATION.
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
{"spark": "340"}
{"spark": "341"}
{"spark": "341db"}
{"spark": "342"}
{"spark": "343"}
{"spark": "344"}
{"spark": "350"}
{"spark": "350db143"}
{"spark": "351"}
{"spark": "352"}
{"spark": "353"}
{"spark": "354"}
{"spark": "355"}
{"spark": "356"}
{"spark": "357"}
{"spark": "358"}
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
spark-rapids-shim-json-lines ***/

package org.apache.spark.sql.rapids

import scala.collection
import scala.collection.mutable.ArrayBuffer

import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.shuffle.{RapidsShuffleIterator, RapidsShuffleTransport}

import org.apache.spark.{InterruptibleIterator, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.shuffle.{ShuffleReader, ShuffleReadMetricsReporter}
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.storage.{BlockId, BlockManagerId, ShuffleBlockBatchId, ShuffleBlockId}
import org.apache.spark.util.CompletionIterator

trait ShuffleMetricsUpdater {
  /**
   * Trait used as a way to expose the `ShuffleReadMetricsReporter` to the iterator.
   * @param fetchWaitTimeInMs this matches the CPU name (except for the units) but it is actually
   *                          the aggreagate amount of time a task is blocked, not working on
   *                          anything, waiting for data.
   * @param remoteBlocksFetched aggregate of number of `ShuffleBlockId`s fetched.
   * @param remoteBytesRead aggregate size of all contiguous buffers received
   * @param rowsFetched aggregate of number of rows received
   */
  def update(
    fetchWaitTimeInMs: Long,
    remoteBlocksFetched: Long,
    remoteBytesRead: Long,
    rowsFetched: Long): Unit
}

class RapidsCachingReader[K, C](
    rapidsConf: RapidsConf,
    localId: BlockManagerId,
    blocksByAddress: Iterator[(BlockManagerId, collection.Seq[(BlockId, Long, Int)])],
    context: TaskContext,
    metrics: ShuffleReadMetricsReporter,
    transport: Option[RapidsShuffleTransport],
    catalog: ShuffleBufferCatalog,
    sparkTypes: Array[DataType])
  extends ShuffleReader[K, C] with Logging {

  override def read(): Iterator[Product2[K, C]] = {
    NvtxRegistry.RAPIDS_CACHING_READER_READ.push()
    try {
      val blocksForRapidsTransport =
          new ArrayBuffer[(BlockManagerId, collection.Seq[(BlockId, Long, Int)])]()
      var cachedBatchIterator: Iterator[ColumnarBatch] = Iterator.empty
      val blocksByAddressMap: Map[BlockManagerId, collection.Seq[(BlockId, Long, Int)]] =
          blocksByAddress.toMap
      var numCachedBlocks: Int = 0

      blocksByAddressMap.keys.foreach(blockManagerId => {
        val blockInfos: collection.Seq[(BlockId, Long, Int)] = blocksByAddressMap(blockManagerId)

        logDebug("Trying to read block from manager: " + blockManagerId)
        if (blockManagerId.executorId == localId.executorId) {
          NvtxRegistry.RAPIDS_CACHING_READER_READ_LOCAL.push()
          try {
            cachedBatchIterator = blockInfos.iterator.flatMap { blockInfo =>
              val blockId = blockInfo._1
              val shuffleBufferHandles = blockId match {
                case sbbid: ShuffleBlockBatchId =>
                  (sbbid.startReduceId to sbbid.endReduceId).iterator.flatMap { reduceId =>
                    val sbid = ShuffleBlockId(sbbid.shuffleId, sbbid.mapId, reduceId)
                    numCachedBlocks += 1
                    catalog.getColumnarBatchIterator(sbid, sparkTypes)
                  }
                case sbid: ShuffleBlockId =>
                  numCachedBlocks += 1
                  catalog.getColumnarBatchIterator(sbid, sparkTypes)
                case _ => throw new IllegalArgumentException(
                  s"${blockId.getClass} $blockId is not currently supported")
              }

              shuffleBufferHandles
            }

            // Update the spill priorities of these buffers to indicate they are about
            // to be read and therefore should not be spilled if possible.
            // TODO: AB: shuffleBufferHandles.foreach(catalog.updateSpillPriorityForLocalRead)
            metrics.incLocalBlocksFetched(numCachedBlocks)
          } finally {
            NvtxRegistry.RAPIDS_CACHING_READER_READ_LOCAL.pop()
          }
        } else {
          require(
            blockManagerId.topologyInfo.isDefined &&
              blockManagerId.topologyInfo.get
                .startsWith(s"${RapidsShuffleTransport.BLOCK_MANAGER_ID_TOPO_PREFIX}="), {
              val enabledHint = if (rapidsConf.isUCXShuffleManagerMode) {
                "The shuffle transport is disabled. " +
                    s"Please set ${RapidsConf.SHUFFLE_MANAGER_MODE.key}=UCX to enable " +
                    "fetching remote blocks."
              } else {
                "This is unexpected behavior!"
              }
              s"Attempting to handle non-rapids enabled blocks from $blockManagerId. ${enabledHint}"
            })
          blocksForRapidsTransport.append((blockManagerId, blockInfos))
        }
      })

      logInfo(s"Will read ${numCachedBlocks} cached blocks, " +
        s"${blocksForRapidsTransport.size} remote blocks from the RapidsShuffleTransport. ")

      if (transport.isEmpty && blocksForRapidsTransport.nonEmpty) {
        throw new IllegalStateException("Had blocks marked for use with the " +
          "RapidsShuffleTransport, but the transport was not initialized")
      }

      val metricsUpdater = new ShuffleMetricsUpdater {
        override def update (fetchWaitTimeInMs: Long, remoteBlocksFetched: Long,
            remoteBytesRead: Long, rowsFetched: Long): Unit = {
          metrics.incFetchWaitTime(fetchWaitTimeInMs)
          metrics.incRemoteBlocksFetched(remoteBlocksFetched)
          metrics.incRemoteBytesRead(remoteBytesRead)
          metrics.incRecordsRead(rowsFetched)
        }
      }

      NvtxRegistry.RAPIDS_SHUFFLE_ITERATOR_PREP.push()
      try {
        val cachedIt = cachedBatchIterator.map { cb =>
          val cachedBytesRead = GpuColumnVector.getTotalDeviceMemoryUsed(cb)
          metrics.incLocalBytesRead(cachedBytesRead)
          metrics.incRecordsRead(cb.numRows())
          (0, cb)
        }.asInstanceOf[Iterator[(K, C)]]

        val cbArrayFromUcx: Iterator[(K, C)] = if (blocksForRapidsTransport.nonEmpty) {
          val rapidsShuffleIterator = new RapidsShuffleIterator(localId, rapidsConf, transport.get,
            blocksForRapidsTransport.toArray, metricsUpdater, sparkTypes, context.taskAttemptId())
          rapidsShuffleIterator.map(cb => {
            (0, cb)
          }).asInstanceOf[Iterator[(K, C)]]
        } else {
          Iterator.empty
        }

        val completionIter = CompletionIterator[(K, C), Iterator[(K, C)]](
          cachedIt ++ cbArrayFromUcx, {
            context.taskMetrics().mergeShuffleReadMetrics()
          })

        new InterruptibleIterator[(K, C)](context, completionIter)

      } finally {
        NvtxRegistry.RAPIDS_SHUFFLE_ITERATOR_PREP.pop()
      }
    } finally {
      NvtxRegistry.RAPIDS_CACHING_READER_READ.pop()
    }
  }
}
