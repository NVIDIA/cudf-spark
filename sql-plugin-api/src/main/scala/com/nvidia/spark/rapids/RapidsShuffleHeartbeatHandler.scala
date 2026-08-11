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

package com.nvidia.spark.rapids

import org.apache.spark.storage.BlockManagerId


/**
 * This is the first message sent from the executor to the driver.
 * @param id `BlockManagerId` for the executor
 */
case class RapidsExecutorStartupMsg(id: BlockManagerId)

/**
 * Executor heartbeat message.
 * This gives the driver an opportunity to respond with `RapidsExecutorUpdateMsg`
 */
case class RapidsExecutorHeartbeatMsg(id: BlockManagerId)

/**
 * Driver response to an startup or heartbeat message, with new (to the peer) executors
 * from the last heartbeat.
 */
case class RapidsExecutorUpdateMsg(ids: Array[BlockManagerId])

trait RapidsShuffleHeartbeatHandler {
  /** Called when a new peer is seen via heartbeats */
  def addPeer(peer: BlockManagerId): Unit
}

// ============================================================================
// Shuffle Cleanup RPC Messages
// ============================================================================

/**
 * Statistics for a single shuffle cleanup operation.
 *
 * @param shuffleId the shuffle ID that was cleaned up
 * @param bytesFromMemory bytes that were read from memory (never spilled to disk)
 * @param bytesFromDisk bytes that were read from disk (spilled at some point)
 * @param numExpansions number of buffer expansions that occurred
 * @param numSpills number of buffers that were spilled to disk
 * @param numForcedFileOnly number of buffers that used forced file-only mode
 */
case class ShuffleCleanupStats(
    shuffleId: Int,
    bytesFromMemory: Long,
    bytesFromDisk: Long,
    numExpansions: Int = 0,
    numSpills: Int = 0,
    numForcedFileOnly: Int = 0) extends Serializable

/**
 * Executor polls driver for shuffles that need to be cleaned up.
 *
 * @param executorId identifier for the executor
 */
case class RapidsShuffleCleanupPollMsg(executorId: String)

/**
 * Driver response with shuffle IDs that need cleanup.
 *
 * @param shuffleIds list of shuffle IDs to clean up
 */
case class RapidsShuffleCleanupResponseMsg(shuffleIds: Array[Int])

/**
 * Executor reports cleanup statistics to driver.
 *
 * @param executorId identifier for the executor
 * @param stats cleanup statistics for each shuffle
 */
case class RapidsShuffleCleanupStatsMsg(executorId: String, stats: Array[ShuffleCleanupStats])
