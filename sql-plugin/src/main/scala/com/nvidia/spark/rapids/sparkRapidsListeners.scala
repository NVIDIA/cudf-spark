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

case class SparkRapidsBuildInfoEvent(
  sparkRapidsBuildInfo: Map[String, String],
  sparkRapidsJniBuildInfo: Map[String, String],
  cudfBuildInfo: Map[String, String],
  sparkRapidsPrivateBuildInfo: Map[String, String]
) extends SparkListenerEvent

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
