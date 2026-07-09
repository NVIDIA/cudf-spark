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

package com.nvidia.spark.rapids.delta.common

import com.nvidia.spark.rapids.{DataFromReplacementRule, RapidsConf, RapidsMeta, RunnableCommandMeta}
import com.nvidia.spark.rapids.delta.RapidsDeltaUtils

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.delta.commands.{DeletionVectorUtils, MergeIntoCommand}
import org.apache.spark.sql.delta.sources.DeltaSQLConf

abstract class MergeIntoCommandMetaBase(
    mergeCmd: MergeIntoCommand,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
  extends RunnableCommandMeta[MergeIntoCommand](mergeCmd, conf, parent, rule) with Logging {

  override def tagSelfForGpu(): Unit = {
    if (!conf.isDeltaWriteEnabled) {
      willNotWorkOnGpu("Delta Lake output acceleration has been disabled. To enable set " +
        s"${RapidsConf.ENABLE_DELTA_WRITE} to true")
    }
    if (mergeCmd.notMatchedBySourceClauses.nonEmpty) {
      // https://github.com/NVIDIA/spark-rapids/issues/8415
      willNotWorkOnGpu("notMatchedBySourceClauses not supported on GPU")
    }
    val deltaLog = mergeCmd.targetFileIndex.deltaLog
    val dvFeatureEnabled =
      DeletionVectorUtils.deletionVectorsWritable(deltaLog.unsafeVolatileSnapshot)

    if (dvFeatureEnabled && mergeCmd.conf.getConf(
      DeltaSQLConf.MERGE_USE_PERSISTENT_DELETION_VECTORS)) {
      // https://github.com/NVIDIA/spark-rapids/issues/8654
      willNotWorkOnGpu("Deletion vectors are not supported on GPU")
    }

    val targetSchema = mergeCmd.migratedSchema.getOrElse(mergeCmd.target.schema)
    RapidsDeltaUtils.tagForDeltaWrite(this, targetSchema, Some(deltaLog), Map.empty,
      SparkSession.active)
  }
}
