/*
 * Copyright (c) 2025-2026, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.delta.shims

import com.databricks.sql.transaction.tahoe.commands.DeletionVectorUtils
import com.databricks.sql.transaction.tahoe.sources.DeltaSQLConf
import com.nvidia.spark.rapids.delta.{UpdateCommandEdgeMeta, UpdateCommandMeta}

object UpdateCommandMetaShim {
  def tagForGpu(meta: UpdateCommandMeta): Unit = {
    val deltaLog = meta.updateCmd.tahoeFileIndex.deltaLog
    val dvFeatureEnabled =
      DeletionVectorUtils.deletionVectorsWritable(deltaLog.unsafeVolatileSnapshot)

    if (dvFeatureEnabled && meta.updateCmd.conf.getConf(
      DeltaSQLConf.UPDATE_USE_PERSISTENT_DELETION_VECTORS)) {
      // https://github.com/NVIDIA/spark-rapids/issues/8654
      meta.willNotWorkOnGpu("Deletion vector writes are not supported on GPU")
    }
  }

  def tagForGpu(meta: UpdateCommandEdgeMeta): Unit = {
    val deltaLog = meta.updateCmd.tahoeFileIndex.deltaLog
    val dvFeatureEnabled =
      DeletionVectorUtils.deletionVectorsWritable(deltaLog.unsafeVolatileSnapshot)

    if (dvFeatureEnabled && meta.updateCmd.conf.getConf(
      DeltaSQLConf.UPDATE_USE_PERSISTENT_DELETION_VECTORS)) {
      // https://github.com/NVIDIA/spark-rapids/issues/8654
      meta.willNotWorkOnGpu("Deletion vector writes are not supported on GPU")
    }
  }
}
