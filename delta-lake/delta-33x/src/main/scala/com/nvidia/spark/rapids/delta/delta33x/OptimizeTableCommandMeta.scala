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

package com.nvidia.spark.rapids.delta.delta33x

import com.nvidia.spark.rapids.{DataFromReplacementRule, RapidsConf, RapidsMeta}
import com.nvidia.spark.rapids.delta.common.OptimizeTableCommandMetaBase

import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.commands.{DeltaCommand, OptimizeTableCommand}
import org.apache.spark.sql.delta.rapids.GpuOptimizeTableCommand
import org.apache.spark.sql.execution.command.RunnableCommand

class OptimizeTableCommandMeta(
    cmd: OptimizeTableCommand,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
  extends OptimizeTableCommandMetaBase(cmd, conf, parent, rule)
    with DeltaCommand {

  override protected def getDeltaLogForOptimize(): DeltaLog = {
    getDeltaTable(cmd.child, "OPTIMIZE").deltaLog
  }

  override def convertToGpu(): RunnableCommand = {
    GpuOptimizeTableCommand(cmd.child, cmd.userPartitionPredicates, cmd.optimizeContext)(
      cmd.zOrderBy)
  }
}
