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

import org.apache.spark.sql.rapids.execution.BroadcastBuildIdentity
import org.apache.spark.storage.{BlockId, BlockManagerId, ShuffleBlockId}

/** The same immutable metadata supplies both cache identity and the shuffle reader. */
private[rapids] case class ShuffleReadSnapshot(
    blocks: Vector[(BlockManagerId, Vector[(BlockId, Long, Int)])]) {
  def buildIdentity(shuffleId: Int): BroadcastBuildIdentity = {
    val outputs = blocks.flatMap { case (address, blockInfos) =>
      blockInfos.map {
        case (block: ShuffleBlockId, _, mapIndex)
            if block.shuffleId == shuffleId && block.reduceId == 0 =>
          BroadcastBuildIdentity.MapOutput(mapIndex, block.mapId, address)
        case (block, _, _) =>
          throw new IllegalArgumentException(s"Unexpected driverless build block $block")
      }
    }
    BroadcastBuildIdentity(outputs)
  }
}

private[rapids] trait ShuffleReadSnapshotProvider {
  /** Capture before read() so payload iteration uses the same output identities. */
  def snapshot: ShuffleReadSnapshot
}
