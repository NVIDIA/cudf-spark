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

import scala.collection.mutable

import com.nvidia.spark.rapids.RapidsConf

import org.apache.spark.{SparkContext, SparkEnv, SparkException}
import org.apache.spark.internal.Logging

/** Application transport settings are immutable; SQL policy limits are captured per query. */
private[execution] object DriverlessBroadcastPolicy extends Logging {
  private val configurations = mutable.WeakHashMap.empty[SparkEnv, RapidsConf]

  private def applicationConf(env: SparkEnv): RapidsConf = configurations.synchronized {
    configurations.getOrElseUpdate(env, new RapidsConf(env.conf))
  }

  private def hosts(env: SparkEnv): Set[String] =
    env.blockManager.master.getMemoryStatus.keysIterator
      .filterNot(_.executorId == SparkContext.DRIVER_IDENTIFIER).map(_.host).toSet

  private[execution] def nodeRejection(hosts: Set[String], limit: Int): Option[String] = {
    if (hosts.size > limit) {
      Some(s"${hosts.size} executor hosts exceed driverlessBroadcast.maxNodes=$limit")
    } else None
  }

  def rejections(conf: RapidsConf): Seq[String] = Option(SparkEnv.get).map { env =>
    val startup = applicationConf(env)
    Seq(
      // The build reader consumes GPU shuffle blocks, not disk-shuffle ManagedBuffers.
      if (!startup.isUCXShuffleManagerMode) Some("UCX GPU shuffle mode is required") else None,
      if (env.conf.getBoolean("spark.dynamicAllocation.enabled", false)) {
        Some("dynamic allocation is unsupported by driverless broadcast")
      } else None,
      nodeRejection(hosts(env), conf.driverlessBroadcastMaxNodes)
    ).flatten
  }.getOrElse(Seq("SparkEnv is not initialized"))

  def checkExecutorLimit(limit: Int): Unit = {
    val env = SparkEnv.get
    nodeRejection(hosts(env), limit).foreach { reason =>
      throw new SparkException(s"Driverless broadcast cannot fetch a build: $reason. " +
        "Executor hosts grew beyond the supported limit after planning. Disable driverless " +
        "broadcast or qualify a larger limit before executing the query.")
    }
  }
}
