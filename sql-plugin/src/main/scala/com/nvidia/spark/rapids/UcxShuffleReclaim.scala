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

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.api.plugin.PluginContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.rapids.{GpuShuffleEnv, GpuShuffleRegistry, UcxSqlEndListener}

/** The check that decides whether the UCX shuffle reclaim runs. */
private[rapids] object UcxShuffleReclaim {
  /**
   * Why the UCX shuffle reclaim must not run, or None when it may. The driver plugin and the
   * executor plugin both call this, so the two sides agree. It reads the configured shuffle
   * manager, since Spark 4 has not built one when the driver plugin starts.
   *
   * @param conf the RAPIDS configuration
   * @param sparkConf the Spark configuration, which names the shuffle manager
   * @param isDataBricks whether the plugin runs on a Databricks shim
   * @return the first check that failed, with the configured value it found, or None
   */
  def refusal(
      conf: RapidsConf,
      sparkConf: SparkConf,
      isDataBricks: Boolean): Option[String] = {
    val gateKey = RapidsConf.SHUFFLE_UCX_CLEANUP_ON_QUERY_END_ENABLED.key
    val notRun = s"$gateKey is true, but the UCX shuffle reclaim does not run"
    if (!conf.shuffleUcxCleanupOnQueryEndEnabled) {
      Some(s"$gateKey is false")
    } else if (!conf.isUCXShuffleManagerMode) {
      Some(s"$notRun: ${RapidsConf.SHUFFLE_MANAGER_MODE.key} is ${conf.shuffleManagerMode}, " +
        "and it runs only in UCX mode")
    } else if (!GpuShuffleEnv.isRapidsShuffleConfigured(sparkConf)) {
      val manager = sparkConf.getOption("spark.shuffle.manager").getOrElse("unset")
      Some(s"$notRun: spark.shuffle.manager is $manager, and it needs " +
        GpuShuffleEnv.RAPIDS_SHUFFLE_CLASS)
    } else if (isDataBricks) {
      Some(s"$notRun on Databricks")
    } else if (!conf.isSqlExecuteOnGPU) {
      // no GPU is set up then, so the executor's reclaim endpoint could not start its thread
      Some(s"$notRun: ${RapidsConf.SQL_MODE.key} is not executeOnGPU")
    } else {
      None
    }
  }
}

/** The driver side of the UCX shuffle reclaim, which the driver plugin starts and calls. */
private[rapids] object UcxShuffleReclaimDriver extends Logging {
  /**
   * Starts the reclaim when `UcxShuffleReclaim.refusal` admits it: the manager, the GPU shuffle
   * registry, and the UCX listener on the shared listener queue, as MULTITHREADED's listener
   * is. Logs the refusal when the gate key is on.
   *
   * @return the listener it registered, which the driver plugin shuts down
   */
  def start(
      sc: SparkContext,
      conf: RapidsConf,
      sparkConf: SparkConf): Option[UcxSqlEndListener] = {
    UcxShuffleReclaim.refusal(conf, sparkConf, VersionUtils.isDataBricks) match {
      case None =>
        ShuffleCleanupManager.init(sc)
        GpuShuffleRegistry.init()
        val listener = new UcxSqlEndListener(
          isCandidate = shuffleId => {
            val shuffleIds = GpuShuffleRegistry.get
            shuffleIds != null && shuffleIds.contains(shuffleId)
          },
          reclaim = (executionId, shuffleIds, watermark) =>
            Option(ShuffleCleanupManager.get).foreach(
              _.reclaimAfterExecution(executionId, shuffleIds, watermark)))
        sc.addSparkListener(listener)
        logInfo("ShuffleCleanupManager and listener initialized for UCX shuffle reclaim")
        Some(listener)
      case Some(reason) =>
        if (conf.shuffleUcxCleanupOnQueryEndEnabled) {
          logWarning(reason)
        }
        None
    }
  }

  /** Answers an executor's poll, with no request when no manager runs. */
  def handlePoll(msg: RapidsShuffleReclaimPollMsg): RapidsShuffleReclaimResponseMsg = {
    val manager = ShuffleCleanupManager.get
    if (manager != null) {
      manager.handleReclaimPoll(msg.executorId)
    } else {
      RapidsShuffleReclaimResponseMsg(Array.empty)
    }
  }

  /** Hands an executor's reclaim results to the manager, if one runs. */
  def handleResult(msg: RapidsShuffleReclaimResultMsg): Unit = {
    val manager = ShuffleCleanupManager.get
    if (manager != null) {
      manager.handleReclaimResult(msg.executorId, msg.results)
    }
  }
}

/** The executor side of the UCX shuffle reclaim, which the executor plugin starts. */
private[rapids] object UcxShuffleReclaimExecutor extends Logging {
  /**
   * Starts the executor's reclaim endpoint when `UcxShuffleReclaim.refusal` admits it. The
   * driver starts its manager on the same check, so the endpoint's polls find one. The executor
   * plugin calls this only where it sets up the GPU, which the test-only per-task RMM init skips,
   * and then the driver's requests expire.
   *
   * @return the started endpoint, which the executor plugin closes before the device shutdown
   */
  def start(
      pluginContext: PluginContext,
      conf: RapidsConf,
      sparkConf: SparkConf): Option[ShuffleCleanupEndpoint] = {
    if (UcxShuffleReclaim.refusal(conf, sparkConf, VersionUtils.isDataBricks).isEmpty) {
      logInfo("Initializing shuffle reclaim endpoint for UCX mode")
      val endpoint = new ShuffleCleanupEndpoint(pluginContext, ucxReclaimer =
        Some(new UcxCatalogReclaimer(send = UcxCatalogReclaimer.sendTo(pluginContext))))
      endpoint.start()
      Some(endpoint)
    } else {
      None
    }
  }
}
