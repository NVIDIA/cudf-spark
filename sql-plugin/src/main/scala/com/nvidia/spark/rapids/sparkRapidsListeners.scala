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

package com.nvidia.spark.rapids

import org.apache.spark.scheduler.SparkListenerEvent

/**
 * Event posted when a shuffle is unregistered, containing disk I/O savings statistics.
 * This tracks how much data stayed in memory throughout the shuffle lifecycle,
 * avoiding disk writes compared to the baseline implementation.
 *
 * @param shuffleId The shuffle ID being unregistered
 * @param bytesFromMemory Bytes that were read from memory (never spilled to disk)
 * @param bytesFromDisk Bytes that were read from disk (spilled at some point)
 * @param numExpansions Number of buffer expansions that occurred
 * @param numSpills Number of buffers that were spilled to disk
 * @param numForcedFileOnly Number of buffers that used forced file-only mode
 */
case class SparkRapidsShuffleDiskSavingsEvent(
  shuffleId: Int,
  bytesFromMemory: Long,
  bytesFromDisk: Long,
  numExpansions: Int = 0,
  numSpills: Int = 0,
  numForcedFileOnly: Int = 0
) extends SparkListenerEvent

/**
 * Event posted once per SQL execution that owned a GPU shuffle, when the UCX shuffle reclaim
 * has an outcome for every shuffle the execution owned. Each skipped list names the shuffles
 * that wait for GC, by the rule that held each one back.
 *
 * @param executionId The SQL execution ID
 * @param endedAtMs When the execution ended
 * @param completedAtMs When the execution's last shuffle got its outcome, and no earlier than
 *                      `endedAtMs`
 * @param shuffles Number of GPU shuffles the execution owned
 * @param reclaimed Number of those shuffles the reclaim released to their executors
 * @param alreadyRemoved Number of those shuffles that GC or Spark's own cleanup removed first
 * @param skippedShared Shuffles that another execution, a job outside any execution or a late
 *                      job touched, or that the fence or the driver step left to GC
 * @param skippedUnconsumed Shuffles that nothing of the succeeded execution read
 * @param skippedCached Shuffles that a cached RDD depends on
 * @param skippedIndeterminate Shuffles whose map stage Spark marks indeterminate
 * @param executorsFailed Number of executor reclaims that failed
 * @param executorsTimedOut Number of executors that timed out, for either reason
 * @param executorsNeverPolled Number of the timed-out executors that were never handed the
 *                             shuffle
 * @param executorsDeferred Number of executors that deferred a reclaim, which may still hold
 *                          the execution's output
 * @param buffersFreed Number of buffers the executors freed
 * @param bytesFreed Bytes the executors freed
 * @param maxWaitMs Longest wait from the execution's end to a shuffle's dispatch
 * @param maxRemoveMs Longest time from a dispatch to its request's completion
 */
case class SparkRapidsShuffleReclaimEvent(
  executionId: Long,
  endedAtMs: Long,
  completedAtMs: Long,
  shuffles: Int,
  reclaimed: Int,
  alreadyRemoved: Int,
  skippedShared: Seq[Int],
  skippedUnconsumed: Seq[Int],
  skippedCached: Seq[Int],
  skippedIndeterminate: Seq[Int],
  executorsFailed: Int,
  executorsTimedOut: Int,
  executorsNeverPolled: Int,
  executorsDeferred: Int,
  buffersFreed: Long,
  bytesFreed: Long,
  maxWaitMs: Long,
  maxRemoveMs: Long
) extends SparkListenerEvent
