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

import com.databricks.sql.transaction.tahoe.{RowTracking, Snapshot}
import com.databricks.sql.transaction.tahoe.sources.DeltaSQLConf
import com.nvidia.spark.rapids.RapidsMeta
import com.nvidia.spark.rapids.delta.{DeleteCommandEdgeMeta, DeleteCommandMeta}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType

/**
 * Both command variants share the same guard. The Delta DELETE integration runs on DBR 17.3
 * and 18.3 observed DeleteCommandEdge for the tested SQL; DeleteCommand also routes here when
 * the runtime emits it.
 */
object DeleteCommandMetaShim {
  def tagForGpu(meta: DeleteCommandMeta): Unit = {
    val cmd = meta.deleteCmd
    tagForGpuCommon(meta, cmd.conf, cmd.deltaLog.unsafeVolatileSnapshot,
      cmd.target.schema, cmd.condition)
  }

  def tagForGpu(meta: DeleteCommandEdgeMeta): Unit = {
    val cmd = meta.deleteCmd
    tagForGpuCommon(meta, cmd.conf, cmd.deltaLog.unsafeVolatileSnapshot,
      cmd.target.schema, cmd.condition)
  }

  private def tagForGpuCommon(
      meta: RapidsMeta[_, _, _],
      sqlConf: SQLConf,
      snapshot: Snapshot,
      targetSchema: StructType,
      condition: Option[Expression]): Unit = {
    val persistentDvEnabled = PersistentDeletionVectorMetaShim.tagForGpu(
      meta, sqlConf, snapshot, DeltaSQLConf.DELETE_USE_PERSISTENT_DELETION_VECTORS, targetSchema)
    // Whole-table and metadata-only deletes do not need physical indexes from a data scan.
    if (persistentDvEnabled && RowTracking.isEnabled(snapshot.protocol, snapshot.metadata) &&
        PersistentDeletionVectorMetaShim.hasDataPredicate(
          condition, snapshot.metadata.partitionColumns, SparkSession.active)) {
      meta.willNotWorkOnGpu(
        "Persistent deletion vector DELETE with row tracking is not supported on GPU")
    }
  }
}
