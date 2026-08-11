/*
 * Copyright (c) 2023-2024, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.delta.delta24x

import com.nvidia.spark.rapids.{DataFromReplacementRule, RapidsConf, RapidsMeta, RapidsReaderType, RunnableCommandMeta}
import com.nvidia.spark.rapids.delta.RapidsDeltaUtils

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.delta.commands.MergeIntoCommand
import org.apache.spark.sql.delta.rapids.GpuDeltaLog
import org.apache.spark.sql.delta.rapids.delta24x.{GpuLowShuffleMergeCommand, GpuMergeIntoCommand}
import org.apache.spark.sql.execution.command.RunnableCommand

class MergeIntoCommandMeta(
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
    val targetSchema = mergeCmd.migratedSchema.getOrElse(mergeCmd.target.schema)
    val deltaLog = mergeCmd.targetFileIndex.deltaLog
    RapidsDeltaUtils.tagForDeltaWrite(this, targetSchema, Some(deltaLog),
      Map.empty, SparkSession.active)
  }

  override def convertToGpu(): RunnableCommand = {
    // TODO: Currently we only support low shuffler merge only when parquet per file read is enabled
    // due to the limitation of implementing row index metadata column.
    if (conf.isDeltaLowShuffleMergeEnabled) {
      if (conf.isParquetPerFileReadEnabled) {
        GpuLowShuffleMergeCommand(
          mergeCmd.source,
          mergeCmd.target,
          new GpuDeltaLog(mergeCmd.targetFileIndex.deltaLog, conf),
          mergeCmd.condition,
          mergeCmd.matchedClauses,
          mergeCmd.notMatchedClauses,
          mergeCmd.notMatchedBySourceClauses,
          mergeCmd.migratedSchema)(conf)
      } else {
        logWarning(s"""Low shuffle merge disabled since ${RapidsConf.PARQUET_READER_TYPE} is
          not set to ${RapidsReaderType.PERFILE}. Falling back to classic merge.""")
        GpuMergeIntoCommand(
          mergeCmd.source,
          mergeCmd.target,
          new GpuDeltaLog(mergeCmd.targetFileIndex.deltaLog, conf),
          mergeCmd.condition,
          mergeCmd.matchedClauses,
          mergeCmd.notMatchedClauses,
          mergeCmd.notMatchedBySourceClauses,
          mergeCmd.migratedSchema)(conf)
      }
    } else {
      GpuMergeIntoCommand(
        mergeCmd.source,
        mergeCmd.target,
        new GpuDeltaLog(mergeCmd.targetFileIndex.deltaLog, conf),
        mergeCmd.condition,
        mergeCmd.matchedClauses,
        mergeCmd.notMatchedClauses,
        mergeCmd.notMatchedBySourceClauses,
        mergeCmd.migratedSchema)(conf)
    }
  }

}
