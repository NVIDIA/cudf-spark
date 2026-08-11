/*
 * Copyright (c) 2023-2025, NVIDIA CORPORATION.
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

import com.databricks.sql.transaction.tahoe.commands.{UpdateCommand, UpdateCommandEdge}
import com.databricks.sql.transaction.tahoe.rapids.{GpuDeltaLog, GpuUpdateCommand}
import com.nvidia.spark.rapids.{DataFromReplacementRule, RapidsConf, RapidsMeta, RunnableCommandMeta}
import com.nvidia.spark.rapids.delta.shims.UpdateCommandMetaShim

import org.apache.spark.sql.execution.command.RunnableCommand

class UpdateCommandMeta(
    val updateCmd: UpdateCommand,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
    extends RunnableCommandMeta[UpdateCommand](updateCmd, conf, parent, rule) {

  override def tagSelfForGpu(): Unit = {
    if (!conf.isDeltaWriteEnabled) {
      willNotWorkOnGpu("Delta Lake output acceleration has been disabled. To enable set " +
          s"${RapidsConf.ENABLE_DELTA_WRITE} to true")
    }
    UpdateCommandMetaShim.tagForGpu(this)
    RapidsDeltaUtils.tagForDeltaWrite(this, updateCmd.target.schema,
      Some(updateCmd.tahoeFileIndex.deltaLog), Map.empty, updateCmd.tahoeFileIndex.spark)
  }

  override def convertToGpu(): RunnableCommand = {
    GpuUpdateCommand(
      new GpuDeltaLog(updateCmd.tahoeFileIndex.deltaLog, conf),
      updateCmd.tahoeFileIndex,
      updateCmd.target,
      updateCmd.updateExpressions,
      updateCmd.condition
    )
  }
}

class UpdateCommandEdgeMeta(
    val updateCmd: UpdateCommandEdge,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
    extends RunnableCommandMeta[UpdateCommandEdge](updateCmd, conf, parent, rule) {

  override def tagSelfForGpu(): Unit = {
    if (!conf.isDeltaWriteEnabled) {
      willNotWorkOnGpu("Delta Lake output acceleration has been disabled. To enable set " +
          s"${RapidsConf.ENABLE_DELTA_WRITE} to true")
    }
    UpdateCommandMetaShim.tagForGpu(this)
    RapidsDeltaUtils.tagForDeltaWrite(this, updateCmd.target.schema,
      Some(updateCmd.tahoeFileIndex.deltaLog), Map.empty, updateCmd.tahoeFileIndex.spark)
  }

  override def convertToGpu(): RunnableCommand = {
    GpuUpdateCommand(
      new GpuDeltaLog(updateCmd.tahoeFileIndex.deltaLog, conf),
      updateCmd.tahoeFileIndex,
      updateCmd.target,
      updateCmd.updateExpressions,
      updateCmd.condition
    )
  }
}
