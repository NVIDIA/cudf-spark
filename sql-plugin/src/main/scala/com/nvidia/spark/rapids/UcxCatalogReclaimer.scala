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

package com.nvidia.spark.rapids

import org.apache.spark.api.plugin.PluginContext

/**
 * UCX's reclaimer: reclaims the shuffles the driver releases to this executor from its
 * `ShuffleBufferCatalog`.
 *
 * @param isClosed the endpoint's closed check
 * @param reclaim frees what the executor holds of a shuffle for writers at or below a watermark
 * @param send sends one result to the driver
 */
class UcxCatalogReclaimer(
    isClosed: () => Boolean,
    reclaim: (Int, Long) => ShuffleReclaimResult,
    send: ShuffleReclaimResult => Unit) extends CatalogReclaimer {

  /** Reclaims the shuffles of one poll reply. */
  def reclaimRequests(requests: Array[ShuffleReclaimRequest]): Unit = ()
}

object UcxCatalogReclaimer {
  /** Sends each result to the driver through `pluginContext`. */
  def sendTo(pluginContext: PluginContext): ShuffleReclaimResult => Unit = _ => ()
}
