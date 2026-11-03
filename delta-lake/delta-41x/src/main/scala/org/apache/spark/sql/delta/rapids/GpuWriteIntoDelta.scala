/*
 * Copyright (c) 2022-2026, NVIDIA CORPORATION.
 *
 * This file was derived from WriteIntoDelta.scala
 * in the Delta Lake project at https://github.com/delta-io/delta.
 *
 * Copyright (2021) The Delta Lake Project Authors.
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

package org.apache.spark.sql.delta.rapids

import scala.util.Try

import com.nvidia.spark.rapids.delta.DeltaWriteUtils.toBooleanOption

import org.apache.spark.sql.delta.DeltaOperations
import org.apache.spark.sql.delta.commands.WriteIntoDelta

/** GPU version of Delta Lake's WriteIntoDelta. */
case class GpuWriteIntoDelta(
    override val gpuDeltaLog: GpuDeltaLog,
    override val cpuWrite: WriteIntoDelta)
    extends GpuWriteIntoDeltaBase(gpuDeltaLog, cpuWrite)
      with GpuWriteIntoDeltaLike {

  override protected def getFilesToRemoveForOverwrite(
      txn: org.apache.spark.sql.delta.OptimisticTransaction,
      addFiles: Seq[org.apache.spark.sql.delta.actions.AddFile],
      useDynamicPartitionOverwriteMode: Boolean):
      Seq[org.apache.spark.sql.delta.actions.Action] = {
    if (useDynamicPartitionOverwriteMode) {
      val updatePartitions = addFiles.map(_.partitionValues).toSet
      txn.filterFiles(updatePartitions).map(_.remove)
    } else {
      txn.filterFiles().map(_.remove)
    }
  }

  override protected def registerWriteOperationMetrics(
      sparkSession: org.apache.spark.sql.SparkSession,
      txn: org.apache.spark.sql.delta.OptimisticTransaction,
      newFiles: Seq[org.apache.spark.sql.delta.actions.FileAction],
      deletedFiles: Seq[org.apache.spark.sql.delta.actions.Action],
      replaceWhere: Option[Seq[org.apache.spark.sql.catalyst.expressions.Expression]],
      replaceOnDataColsEnabled: Boolean): Unit = {
    if (replaceWhere.nonEmpty && replaceOnDataColsEnabled &&
        sparkSession.conf.get(
          org.apache.spark.sql.delta.sources.DeltaSQLConf.REPLACEWHERE_METRICS_ENABLED)) {
      registerReplaceWhereMetrics(sparkSession, txn, newFiles, deletedFiles)
    }
  }

  override protected def buildCommitMetadata: DeltaOperations.Operation = {
    DeltaOperations.Write(
      cpuWrite.mode,
      Option(cpuWrite.partitionColumns),
      cpuWrite.options.replaceWhere,
      cpuWrite.options.userMetadata,
      toBooleanOption(Try(cpuWrite.options.isDynamicPartitionOverwriteMode).getOrElse(false)),
      toBooleanOption(cpuWrite.options.canOverwriteSchema),
      toBooleanOption(cpuWrite.options.canMergeSchema))
  }

  override protected def copyWithCpuWrite(newCpuWrite: WriteIntoDelta): GpuWriteIntoDelta = {
    copy(cpuWrite = newCpuWrite)
  }
}
