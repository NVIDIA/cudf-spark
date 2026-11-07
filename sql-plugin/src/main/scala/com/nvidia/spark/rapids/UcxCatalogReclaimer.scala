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

import scala.util.control.NonFatal

import org.apache.spark.SparkEnv
import org.apache.spark.api.plugin.PluginContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.rapids.{ProxyRapidsShuffleInternalManagerBase,
  RapidsShuffleInternalManagerBase}

/**
 * UCX's reclaimer: reclaims the shuffles the driver releases to this executor from its
 * `ShuffleBufferCatalog`, and sends each result as its reclaim ends. It has no final step, so
 * its endpoint adds no JVM shutdown hook.
 *
 * @param isClosed the endpoint's closed check. Once it reads true, no further reclaim starts.
 * @param reclaim frees what the executor holds of a shuffle for writers at or below a watermark,
 *                and frees no further buffer once its stop check reads true
 * @param send sends one result to the driver
 */
class UcxCatalogReclaimer(
    isClosed: () => Boolean,
    reclaim: (Int, Long, () => Boolean) => ShuffleReclaimResult =
      UcxCatalogReclaimer.reclaimOnShuffleManager,
    send: ShuffleReclaimResult => Unit) extends CatalogReclaimer with Logging {

  override def poll(executorId: String, ask: AnyRef => AnyRef): Unit = {
    if (!isClosed()) {
      ask(RapidsShuffleReclaimPollMsg(executorId)) match {
        case RapidsShuffleReclaimResponseMsg(requests) =>
          if (requests.nonEmpty) {
            logDebug(s"Received ${requests.length} shuffles to reclaim: " +
              s"${requests.map(_.shuffleId).mkString(", ")}")
          }
          reclaimRequests(requests)

        case other =>
          logWarning(s"Unexpected response from driver: $other")
      }
    }
  }

  override def finalStep: Option[() => Unit] = None

  /**
   * Reclaims the shuffles of one poll reply in order, sending each result before the next
   * reclaim starts. Once the endpoint closes, the rest of the reply gets no reclaim and no
   * result, since the driver drops a leaving executor from every request.
   */
  private[rapids] def reclaimRequests(requests: Array[ShuffleReclaimRequest]): Unit = {
    val pending = requests.iterator
    while (pending.hasNext && !isClosed()) {
      send(reclaimOne(pending.next()))
    }
  }

  private def reclaimOne(request: ShuffleReclaimRequest): ShuffleReclaimResult = {
    val startNanos = System.nanoTime()
    try {
      reclaim(request.shuffleId, request.watermark, () => false)
    } catch {
      case NonFatal(e) =>
        logWarning(s"Failed to reclaim shuffle ${request.shuffleId}", e)
        ShuffleReclaimResult(request.shuffleId, request.watermark, ShuffleReclaimOutcome.FAILED,
          buffers = 0, bytes = 0L, elapsedNanos = System.nanoTime() - startNanos,
          error = Some(s"${e.getClass.getName}: ${e.getMessage}"))
    }
  }
}

object UcxCatalogReclaimer {
  /** Sends each result to the driver through `pluginContext`, in a message of its own. */
  def sendTo(pluginContext: PluginContext): ShuffleReclaimResult => Unit = {
    val executorId = pluginContext.executorID()
    result => pluginContext.send(RapidsShuffleReclaimResultMsg(executorId, Array(result)))
  }

  /**
   * Reclaims through the executor's RAPIDS shuffle manager, found as its heartbeat finds it. An
   * executor without one holds nothing to reclaim.
   */
  @scala.annotation.nowarn("cat=deprecation&origin=org.apache.spark.SparkEnv.shuffleManager")
  private def reclaimOnShuffleManager(
      shuffleId: Int,
      watermark: Long,
      isStopped: () => Boolean): ShuffleReclaimResult = {
    Option(SparkEnv.get).map(_.shuffleManager) match {
      case Some(proxy: ProxyRapidsShuffleInternalManagerBase) =>
        proxy.getRealImpl.asInstanceOf[RapidsShuffleInternalManagerBase]
          .reclaimGpuShuffle(shuffleId, watermark)
      case _ =>
        ShuffleReclaimResult.absent(shuffleId, watermark)
    }
  }
}
