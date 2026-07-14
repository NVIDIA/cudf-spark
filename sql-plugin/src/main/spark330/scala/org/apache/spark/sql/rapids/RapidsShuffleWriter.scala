/*
 * Copyright (c) 2024-2026, NVIDIA CORPORATION.
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
{"spark": "330"}
{"spark": "330db"}
{"spark": "331"}
{"spark": "332"}
{"spark": "332db"}
{"spark": "333"}
{"spark": "334"}
{"spark": "340"}
{"spark": "341"}
{"spark": "341db"}
{"spark": "342"}
{"spark": "343"}
{"spark": "344"}
{"spark": "350"}
{"spark": "351"}
{"spark": "352"}
{"spark": "353"}
{"spark": "354"}
{"spark": "355"}
{"spark": "356"}
{"spark": "357"}
{"spark": "358"}
{"spark": "400"}
{"spark": "401"}
{"spark": "402"}
{"spark": "403"}
{"spark": "411"}
{"spark": "412"}
{"spark": "413"}
{"spark": "420"}
spark-rapids-shim-json-lines ***/
package org.apache.spark.sql.rapids

import scala.collection.mutable.ArrayBuffer

import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.shuffle.{RapidsShuffleServer, RapidsShuffleTransport}

import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.shuffle.ShuffleWriter
import org.apache.spark.shuffle.api.ShuffleMapOutputWriter
import org.apache.spark.storage._


abstract class RapidsShuffleWriter[K, V]()
      extends ShuffleWriter[K, V]
        with Logging {
  protected var myMapStatus: Option[MapStatus] = None
  
  // Track all ShuffleMapOutputWriters created during write
  // Needed for proper cleanup on error or for partial files
  protected val mapOutputWriters = new ArrayBuffer[ShuffleMapOutputWriter]()
  /**
   * Are we in the process of stopping? Because map tasks can call stop() with success = true
   * and then call stop() with success = false if they get an exception, we want to make sure
   * we don't try deleting files, etc twice.
   */
  private var stopping = false

  def getMapStatus(
    loc: BlockManagerId,
    uncompressedSizes: Array[Long],
    mapTaskId: Long): MapStatus = {
    MapStatus(loc, uncompressedSizes, mapTaskId)
  }

  override def stop(success: Boolean): Option[MapStatus] = {
    if (stopping) {
      None
    } else {
      stopping = true
      if (success) {
        if (myMapStatus.isEmpty) {
          // should not happen, but adding it just in case (this differs from Spark)
          cleanupTempData()
          throw new IllegalStateException("Cannot call stop(true) without having called write()");
        }
        myMapStatus
      } else {
        cleanupTempData()
        None
      }
    }
  }
  
  private def cleanupTempData(): Unit = {
    // Abort all map output writers to clean up temp files
    mapOutputWriters.foreach { writer =>
      try {
        writer.abort(null)
      } catch {
        case e: Exception =>
          logWarning(s"Failed to abort map output writer: ${e.getMessage}")
      }
    }
    mapOutputWriters.clear()
  }
}

abstract class RapidsCachingWriterBase[K, V](
   blockManager: BlockManager,
   handle: GpuShuffleHandle[K, V],
   mapId: Long,
   rapidsShuffleServer: Option[RapidsShuffleServer],
   catalog: ShuffleBufferCatalog)
  extends ShuffleWriter[K, V]
    with Logging {
  protected val numParts = handle.dependency.partitioner.numPartitions
  protected val sizes = new Array[Long](numParts)

  /**
   * Used to remove shuffle buffers when the writing task detects an error, calling `stop(false)`
   */
  private def cleanStorage(): Unit = {
    catalog.removeCachedHandles()
  }

  override def stop(success: Boolean): Option[MapStatus] = {
    NvtxRegistry.RAPIDS_CACHING_WRITER_CLOSE.push()
    try {
      if (!success) {
        cleanStorage()
        None
      } else {
        // upon seeing this port, the other side will try to connect to the port
        // in order to establish an UCX endpoint (on demand), if the topology has "rapids" in it.
        val shuffleServerId = if (rapidsShuffleServer.isDefined) {
          val originalShuffleServerId = rapidsShuffleServer.get.originalShuffleServerId
          val server = rapidsShuffleServer.get
          BlockManagerId(
            originalShuffleServerId.executorId,
            originalShuffleServerId.host,
            originalShuffleServerId.port,
            Some(s"${RapidsShuffleTransport.BLOCK_MANAGER_ID_TOPO_PREFIX}=${server.getPort}"))
        } else {
          blockManager.shuffleServerId
        }
        logInfo(s"Done caching shuffle success=$success, server_id=$shuffleServerId, "
          + s"map_id=$mapId, sizes=${sizes.mkString(",")}")
        Some(MapStatus(shuffleServerId, sizes, mapId))
      }
    } finally {
      NvtxRegistry.RAPIDS_CACHING_WRITER_CLOSE.pop()
    }
  }

}
