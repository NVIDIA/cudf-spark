/*
 * Copyright (c) 2023, NVIDIA CORPORATION.
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

import com.databricks.sql.transaction.tahoe.commands.{MergeIntoCommand, MergeIntoCommandEdge}
import com.nvidia.spark.rapids.{DataFromReplacementRule, RapidsConf, RapidsMeta, RunnableCommandMeta}
import com.nvidia.spark.rapids.delta.shims.MergeIntoCommandMetaShim

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.command.RunnableCommand

class MergeIntoCommandMeta(
    mergeCmd: MergeIntoCommand,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
    extends RunnableCommandMeta[MergeIntoCommand](mergeCmd, conf, parent, rule) {

  override def tagSelfForGpu(): Unit = {
    if (!conf.isDeltaWriteEnabled) {
      willNotWorkOnGpu("Delta Lake output acceleration has been disabled. To enable set " +
          s"${RapidsConf.ENABLE_DELTA_WRITE} to true")
    }
    MergeIntoCommandMetaShim.tagForGpu(this, mergeCmd)
    val targetSchema = mergeCmd.migratedSchema.getOrElse(mergeCmd.target.schema)
    val deltaLog = mergeCmd.targetFileIndex.deltaLog
    RapidsDeltaUtils.tagForDeltaWrite(this, targetSchema, Some(deltaLog), Map.empty,
      SparkSession.active)
  }

  override def convertToGpu(): RunnableCommand =
    MergeIntoCommandMetaShim.convertToGpu(mergeCmd, conf)
}

class MergeIntoCommandEdgeMeta(
    mergeCmd: MergeIntoCommandEdge,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
    extends RunnableCommandMeta[MergeIntoCommandEdge](mergeCmd, conf, parent, rule) {

  override def tagSelfForGpu(): Unit = {
    if (!conf.isDeltaWriteEnabled) {
      willNotWorkOnGpu("Delta Lake output acceleration has been disabled. To enable set " +
          s"${RapidsConf.ENABLE_DELTA_WRITE} to true")
    }
    MergeIntoCommandMetaShim.tagForGpu(this, mergeCmd)
    val targetSchema = mergeCmd.migratedSchema.getOrElse(mergeCmd.target.schema)
    val deltaLog = mergeCmd.targetFileIndex.deltaLog
    RapidsDeltaUtils.tagForDeltaWrite(this, targetSchema, Some(deltaLog), Map.empty,
      SparkSession.active)
  }

  override def convertToGpu(): RunnableCommand =
    MergeIntoCommandMetaShim.convertToGpu(mergeCmd, conf)
}
