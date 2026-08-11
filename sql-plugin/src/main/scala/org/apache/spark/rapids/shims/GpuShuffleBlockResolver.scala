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

package org.apache.spark.sql.rapids.shims

import com.nvidia.spark.rapids.ShuffleBufferCatalog

import org.apache.spark.network.buffer.ManagedBuffer
import org.apache.spark.network.shuffle.MergedBlockMeta
import org.apache.spark.shuffle.IndexShuffleBlockResolver
import org.apache.spark.sql.rapids.GpuShuffleBlockResolverBase
import org.apache.spark.storage.ShuffleMergedBlockId

class GpuShuffleBlockResolver(
    val resolver: IndexShuffleBlockResolver,
    catalog: ShuffleBufferCatalog)
    extends GpuShuffleBlockResolverBase(resolver, catalog) {

  /**
 * Retrieve the data for the specified merged shuffle block as multiple chunks.
 */
  def getMergedBlockData(
      blockId: ShuffleMergedBlockId,
      dirs: Option[Array[String]]): Seq[ManagedBuffer] = {
    throw new UnsupportedOperationException("TODO")
  }

  /**
   * Retrieve the meta data for the specified merged shuffle block.
   */
  def getMergedBlockMeta(
      blockId: ShuffleMergedBlockId,
      dirs: Option[Array[String]]): MergedBlockMeta = {
    throw new UnsupportedOperationException("TODO")
  }
}
