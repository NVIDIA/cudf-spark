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

package com.nvidia.spark.rapids.delta.shims

import com.databricks.sql.io.skipping.liquid.ClusteredTableUtils
import com.databricks.sql.transaction.tahoe.DeltaLog
import com.databricks.sql.transaction.tahoe.commands.DeletionVectorUtils
import com.nvidia.spark.rapids.RapidsMeta
import com.nvidia.spark.rapids.delta.{OptimizeTableCommandEdgeMeta, OptimizeTableCommandMeta}

object OptimizeTableCommandMetaShim {
  def tagForGpu(meta: OptimizeTableCommandMeta, deltaLog: DeltaLog): Unit = {
    val cmd = meta.optimizeCmd
    tagForGpu(
      meta,
      deltaLog,
      cmd.zOrderBy.nonEmpty,
      cmd.optimizeContext.reorg.nonEmpty,
      cmd.optimizeContext.maxDeletedRowsRatio.nonEmpty,
      cmd.optimizeContext.isFull)
  }

  def tagForGpu(meta: OptimizeTableCommandEdgeMeta, deltaLog: DeltaLog): Unit = {
    val cmd = meta.optimizeCmd
    tagForGpu(
      meta,
      deltaLog,
      cmd.zOrderBy.nonEmpty,
      reorg = false,
      optimizeDeletedRows = false,
      cmd.isFull)
  }

  private def tagForGpu(
      meta: OptimizeTableCommandMeta,
      deltaLog: DeltaLog,
      hasZOrderBy: Boolean,
      reorg: Boolean,
      optimizeDeletedRows: Boolean,
      isFull: Boolean): Unit = {
    tagForGpuCommon(meta, deltaLog, hasZOrderBy, reorg, optimizeDeletedRows, isFull)
  }

  private def tagForGpu(
      meta: OptimizeTableCommandEdgeMeta,
      deltaLog: DeltaLog,
      hasZOrderBy: Boolean,
      reorg: Boolean,
      optimizeDeletedRows: Boolean,
      isFull: Boolean): Unit = {
    tagForGpuCommon(meta, deltaLog, hasZOrderBy, reorg, optimizeDeletedRows, isFull)
  }

  private def tagForGpuCommon(
      meta: RapidsMeta[_, _, _],
      deltaLog: DeltaLog,
      hasZOrderBy: Boolean,
      reorg: Boolean,
      optimizeDeletedRows: Boolean,
      isFull: Boolean): Unit = {
    val snapshot = deltaLog.unsafeVolatileSnapshot
    if (DeletionVectorUtils.deletionVectorsWritable(snapshot) ||
        !DeletionVectorUtils.isTableDVFree(snapshot)) {
      meta.willNotWorkOnGpu(
        "Delta OPTIMIZE on tables with deletion vectors is not supported on GPU")
    }
    if (hasZOrderBy) meta.willNotWorkOnGpu("Z-Order optimize is not supported on GPU")
    if (reorg) meta.willNotWorkOnGpu("Delta OPTIMIZE REORG is not supported on GPU")
    if (optimizeDeletedRows) meta.willNotWorkOnGpu(
      "Delta OPTIMIZE with deletion-vector cleanup is not supported on GPU")
    if (isFull) meta.willNotWorkOnGpu("Delta OPTIMIZE FULL is not supported on GPU")
    if (ClusteredTableUtils.isSupported(snapshot.protocol)) {
      meta.willNotWorkOnGpu("Delta OPTIMIZE on liquid clustered tables is not supported on GPU")
    }
  }
}
