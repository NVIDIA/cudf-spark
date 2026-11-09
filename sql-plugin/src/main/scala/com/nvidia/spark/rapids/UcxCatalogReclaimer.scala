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
 * Reclaims the UCX shuffles the driver releases to this executor from its
 * `ShuffleBufferCatalog`, for a `ShuffleCleanupEndpoint`, and sends each result as its reclaim
 * ends.
 *
 * @param reclaim frees what the executor holds of a shuffle for writers at or below a watermark,
 *                and frees no further buffer once its stop check reads true
 * @param send sends one result to the driver
 */
class UcxCatalogReclaimer(
    reclaim: (Int, Long, () => Boolean) => ShuffleReclaimResult =
      UcxCatalogReclaimer.reclaimOnShuffleManager,
    send: ShuffleReclaimResult => Unit) extends Logging {

  /**
   * Polls the driver once and reclaims the shuffles its reply names, on the endpoint's poll
   * thread.
   *
   * @param executorId this executor's id, which the poll names
   * @param ask sends a message to the driver and returns its reply
   * @param isClosed the endpoint's closed check. Once it reads true, no further reclaim starts,
   *                 and the one in flight frees no further buffer.
   */
  def poll(executorId: String, ask: AnyRef => AnyRef, isClosed: () => Boolean): Unit = {
    if (!isClosed()) {
      ask(RapidsShuffleReclaimPollMsg(executorId)) match {
        case RapidsShuffleReclaimResponseMsg(requests) =>
          if (requests.nonEmpty) {
            logDebug(s"Received ${requests.length} shuffles to reclaim: " +
              s"${requests.map(_.shuffleId).mkString(", ")}")
          }
          reclaimRequests(requests, isClosed)

        case other =>
          logWarning(s"Unexpected response from driver: $other")
      }
    }
  }

  /**
   * Reclaims the shuffles of one poll reply in order, sending each result before the next
   * reclaim starts. Once the endpoint closes, the rest of the reply gets no reclaim and no
   * result, since the executor is stopping and its catalog goes with it.
   */
  private[rapids] def reclaimRequests(
      requests: Array[ShuffleReclaimRequest],
      isClosed: () => Boolean): Unit = {
    val pending = requests.iterator
    while (pending.hasNext && !isClosed()) {
      send(reclaimOne(pending.next(), isClosed))
    }
  }

  private def reclaimOne(
      request: ShuffleReclaimRequest,
      isClosed: () => Boolean): ShuffleReclaimResult = {
    val startNanos = System.nanoTime()
    try {
      reclaim(request.shuffleId, request.watermark, isClosed)
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
          .reclaimGpuShuffle(shuffleId, watermark, isStopped)
      case _ =>
        ShuffleReclaimResult.absent(shuffleId, watermark)
    }
  }
}
