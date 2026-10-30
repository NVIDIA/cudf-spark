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

import com.databricks.sql.transaction.tahoe.{DeltaTableUtils, RowTracking, Snapshot}
import com.databricks.sql.transaction.tahoe.sources.DeltaSQLConf
import com.nvidia.spark.rapids.RapidsMeta
import com.nvidia.spark.rapids.delta.{UpdateCommandEdgeMeta, UpdateCommandMeta}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.internal.SQLConf

object UpdateCommandMetaShim {
  def tagForGpu(meta: UpdateCommandMeta): Unit = {
    val cmd = meta.updateCmd
    tagForGpuCommon(meta, cmd.conf, cmd.tahoeFileIndex.deltaLog.unsafeVolatileSnapshot,
      cmd.target, cmd.condition, cmd.tahoeFileIndex.spark)
  }

  def tagForGpu(meta: UpdateCommandEdgeMeta): Unit = {
    val cmd = meta.updateCmd
    tagForGpuCommon(meta, cmd.conf, cmd.tahoeFileIndex.deltaLog.unsafeVolatileSnapshot,
      cmd.target, cmd.condition, cmd.tahoeFileIndex.spark)
  }

  private def tagForGpuCommon(
      meta: RapidsMeta[_, _, _],
      sqlConf: SQLConf,
      snapshot: Snapshot,
      target: LogicalPlan,
      condition: Option[Expression],
      spark: SparkSession): Unit = {
    val persistentDvEnabled = PersistentDeletionVectorMetaShim.tagForGpu(
      meta, sqlConf, snapshot, DeltaSQLConf.UPDATE_USE_PERSISTENT_DELETION_VECTORS, target.schema)
    if (persistentDvEnabled && RowTracking.isEnabled(snapshot.protocol, snapshot.metadata) &&
        hasDataPredicate(condition, snapshot.metadata.partitionColumns, spark)) {
      meta.willNotWorkOnGpu(
        "Persistent deletion vector UPDATE with row tracking is not supported on GPU")
    }
  }

  private def hasDataPredicate(
      condition: Option[Expression],
      partitionColumns: Seq[String],
      spark: SparkSession): Boolean = {
    val (_, dataPredicates) = DeltaTableUtils.splitMetadataAndDataPredicates(
      condition.getOrElse(Literal.TrueLiteral), partitionColumns, spark)
    dataPredicates.nonEmpty
  }
}
