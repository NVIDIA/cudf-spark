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
package org.apache.spark.sql.rapids.execution

import org.apache.spark.storage.BlockManagerId

/** Identifies source outputs independently of consumers and metadata enumeration order. */
private[rapids] final class BroadcastBuildIdentity private (
    val outputs: Vector[BroadcastBuildIdentity.MapOutput]) {
  override def equals(other: Any): Boolean = other match {
    case identity: BroadcastBuildIdentity => outputs == identity.outputs
    case _ => false
  }

  override def hashCode(): Int = outputs.hashCode()
}

private[rapids] object BroadcastBuildIdentity {
  case class MapOutput(mapIndex: Int, mapId: Long, location: BlockManagerId)

  def apply(outputs: Seq[MapOutput]): BroadcastBuildIdentity = {
    val ordered = outputs.sortBy(_.mapIndex).toVector
    require(ordered.map(_.mapIndex).distinct.size == ordered.size,
      "A driverless build must select exactly one output per mapper")
    new BroadcastBuildIdentity(ordered)
  }

  val empty: BroadcastBuildIdentity = apply(Seq.empty)
}
