/*
 * Copyright (c) 2020-2022, NVIDIA CORPORATION.
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

import java.io.File

import com.nvidia.spark.rapids.RapidsSerializerManager

import org.apache.spark.SparkConf
import org.apache.spark.rapids.shims.storage.ShimDiskBlockManager
import org.apache.spark.storage.BlockId

/** Maps logical blocks to local disk locations. */
class RapidsDiskBlockManager(conf: SparkConf) {
  private[this] val blockManager = new ShimDiskBlockManager(conf, true)
  private[this] val serializerManager = new RapidsSerializerManager(conf)

  def getFile(blockId: BlockId): File = blockManager.getFile(blockId)

  def getFile(file: String): File = blockManager.getFile(file)

  def getSerializerManager(): RapidsSerializerManager = serializerManager
}
