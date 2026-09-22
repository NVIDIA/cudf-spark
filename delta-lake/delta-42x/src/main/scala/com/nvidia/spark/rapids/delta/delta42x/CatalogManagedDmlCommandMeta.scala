/*
 * Copyright (c) 2026, NVIDIA CORPORATION.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nvidia.spark.rapids.delta.delta42x

import com.nvidia.spark.rapids.{DataFromReplacementRule, RapidsConf, RapidsMeta}
import com.nvidia.spark.rapids.delta.common.{DeleteCommandMeta => CommonDeleteCommandMeta,
  UpdateCommandMeta => CommonUpdateCommandMeta}

import org.apache.spark.sql.delta.commands.{DeleteCommand, DeletionVectorUtils, UpdateCommand}
import org.apache.spark.sql.delta.sources.DeltaSQLConf

class DeleteCommandMeta(
    deleteCmd: DeleteCommand,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
  extends CommonDeleteCommandMeta(deleteCmd, conf, parent, rule) {

  override def tagSelfForGpu(): Unit = {
    super.tagSelfForGpu()
    val snapshot = deleteCmd.deltaLog.unsafeVolatileSnapshot
    if (snapshot.isCatalogOwned &&
        DeletionVectorUtils.deletionVectorsWritable(snapshot) &&
        deleteCmd.conf.getConf(DeltaSQLConf.DELETE_USE_PERSISTENT_DELETION_VECTORS)) {
      willNotWorkOnGpu(
        "Persistent deletion-vector DELETE is not yet supported for catalog-managed tables")
    }
  }
}

class UpdateCommandMeta(
    updateCmd: UpdateCommand,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
  extends CommonUpdateCommandMeta(updateCmd, conf, parent, rule) {

  override def tagSelfForGpu(): Unit = {
    super.tagSelfForGpu()
    val snapshot = updateCmd.tahoeFileIndex.deltaLog.unsafeVolatileSnapshot
    if (snapshot.isCatalogOwned &&
        DeletionVectorUtils.deletionVectorsWritable(snapshot) &&
        updateCmd.conf.getConf(DeltaSQLConf.UPDATE_USE_PERSISTENT_DELETION_VECTORS)) {
      willNotWorkOnGpu(
        "Persistent deletion-vector UPDATE is not yet supported for catalog-managed tables")
    }
  }
}
