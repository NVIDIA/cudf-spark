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

package com.nvidia.spark.rapids.delta.delta42x

import com.nvidia.spark.rapids.RapidsMeta
import com.nvidia.spark.rapids.delta.DeltaConfigChecker

import org.apache.spark.sql.delta.{DeltaLog, DeltaOptions}
import org.apache.spark.sql.internal.SQLConf

object Delta42xConfigChecker extends DeltaConfigChecker {
  override def checkIncompatibleConfs(
      meta: RapidsMeta[_, _, _],
      deltaLog: Option[DeltaLog],
      sqlConf: SQLConf,
      options: Map[String, String]): Unit = {
    val deltaOptions = new DeltaOptions(options, sqlConf)
    if (deltaOptions.isReplaceOnOrUsingDefined) {
      meta.willNotWorkOnGpu("Delta 4.2 replaceOn and replaceUsing writes are not supported on GPU")
    }
    if (deltaOptions.targetAlias.isDefined) {
      meta.willNotWorkOnGpu("Delta 4.2 targetAlias writes are not supported on GPU")
    }
    if (deltaOptions.useNullIntolerantEqualityWithDPO.isDefined) {
      meta.willNotWorkOnGpu(
        "Delta 4.2 null-intolerant dynamic partition overwrite is not supported on GPU")
    }
    if (deltaLog.exists(_.unsafeVolatileSnapshot.isCatalogOwned)) {
      meta.willNotWorkOnGpu("Delta 4.2 catalog-managed table writes are not supported on GPU")
    }
  }
}
