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

package org.apache.spark.shuffle.sort.io

import java.util.{Map => JMap, Optional}

import com.google.common.annotations.VisibleForTesting

import org.apache.spark.{SparkConf, SparkEnv}
import org.apache.spark.internal.Logging
import org.apache.spark.shuffle.IndexShuffleBlockResolver
import org.apache.spark.shuffle.api.{ShuffleExecutorComponents, ShuffleMapOutputWriter, SingleSpillShuffleMapOutputWriter}
import org.apache.spark.storage.BlockManager

/**
 * RAPIDS-optimized executor components that creates RapidsLocalDiskShuffleMapOutputWriter
 * instances with host memory buffer support.
 */
class RapidsLocalDiskShuffleExecutorComponents(sparkConf: SparkConf)
  extends ShuffleExecutorComponents with Logging {

  private var blockManager: BlockManager = null
  private var blockResolver: IndexShuffleBlockResolver = null

  @VisibleForTesting
  def this(
      sparkConf: SparkConf,
      blockManager: BlockManager,
      blockResolver: IndexShuffleBlockResolver) = {
    this(sparkConf)
    this.blockManager = blockManager
    this.blockResolver = blockResolver
  }

  override def initializeExecutor(
      appId: String,
      execId: String,
      extraConfigs: JMap[String, String]): Unit = {
    blockManager = SparkEnv.get.blockManager
    if (blockManager == null) {
      throw new IllegalStateException("No blockManager available from the SparkEnv.")
    }
    blockResolver = new IndexShuffleBlockResolver(sparkConf, blockManager)
  }

  override def createMapOutputWriter(
      shuffleId: Int,
      mapTaskId: Long,
      numPartitions: Int): ShuffleMapOutputWriter = {
    if (blockResolver == null) {
      throw new IllegalStateException(
        "Executor components must be initialized before getting writers.")
    }
    
    new RapidsLocalDiskShuffleMapOutputWriter(
      shuffleId,
      mapTaskId,
      numPartitions,
      blockResolver,
      sparkConf)
  }

  override def createSingleFileMapOutputWriter(
      shuffleId: Int,
      mapId: Long): Optional[SingleSpillShuffleMapOutputWriter] = {
    if (blockResolver == null) {
      throw new IllegalStateException(
        "Executor components must be initialized before getting writers.")
    }
    Optional.of(new LocalDiskSingleSpillMapOutputWriter(shuffleId, mapId, blockResolver))
  }
}

