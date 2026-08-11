/*
 * Copyright (c) 2025, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.delta

import com.nvidia.spark.rapids.RapidsMeta

import org.apache.spark.sql.delta.{DeltaConfigs, DeltaLog, DeltaOptions}
import org.apache.spark.sql.delta.rapids.DeltaRuntimeShim
import org.apache.spark.sql.internal.SQLConf

object Delta20xConfigChecker extends DeltaConfigChecker {
  override def checkIncompatibleConfs(
      meta: RapidsMeta[_, _, _],
      deltaLog: Option[DeltaLog],
      sqlConf: SQLConf,
      options: Map[String, String]
  ): Unit = {
    def getSQLConf(key: String): Option[String] = {
      try {
        Option(sqlConf.getConfString(key))
      } catch {
        case _: NoSuchElementException => None
      }
    }

    val optimizeWriteEnabled = {
      val deltaOptions = new DeltaOptions(options, sqlConf)
      deltaOptions.optimizeWrite.orElse {
        getSQLConf("spark.databricks.delta.optimizeWrite.enabled").map(_.toBoolean).orElse {
          deltaLog.flatMap { log =>
            val metadata = DeltaRuntimeShim.unsafeVolatileSnapshotFromLog(log).metadata
            DeltaConfigs.AUTO_OPTIMIZE.fromMetaData(metadata).orElse {
              metadata.configuration.get("delta.autoOptimize.optimizeWrite").orElse {
                getSQLConf("spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite")
              }.map(_.toBoolean)
            }
          }
        }
      }.getOrElse(false)
    }
    if (optimizeWriteEnabled) {
      meta.willNotWorkOnGpu("optimized write of Delta Lake tables is not supported")
    }

    val autoCompactEnabled =
      getSQLConf("spark.databricks.delta.autoCompact.enabled").orElse {
        deltaLog.flatMap { log =>
          val metadata = DeltaRuntimeShim.unsafeVolatileSnapshotFromLog(log).metadata
          metadata.configuration.get("delta.autoOptimize.autoCompact").orElse {
            getSQLConf("spark.databricks.delta.properties.defaults.autoOptimize.autoCompact")
          }
        }
      }.exists(_.toBoolean)
    if (autoCompactEnabled) {
      meta.willNotWorkOnGpu("automatic compaction of Delta Lake tables is not supported")
    }
  }
}
