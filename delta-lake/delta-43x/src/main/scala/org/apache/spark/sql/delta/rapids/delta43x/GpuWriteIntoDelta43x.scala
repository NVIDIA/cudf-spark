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

package org.apache.spark.sql.delta.rapids.delta43x

import scala.util.Try

import com.nvidia.spark.rapids.delta.DeltaWriteUtils.toBooleanOption

import org.apache.spark.sql.delta.DeltaOperations
import org.apache.spark.sql.delta.commands.WriteIntoDelta
import org.apache.spark.sql.delta.rapids.{GpuDeltaLog, GpuWriteIntoDeltaBase, GpuWriteIntoDeltaLike}

/**
 * GPU version of Delta 4.3's WriteIntoDelta.
 *
 * This class must have a different FQCN from GpuWriteIntoDelta because aggregate JARs contain both
 * the Delta 4.0/4.1 and Delta 4.3 adapters. Sharing an FQCN would cause one version-linked class to
 * replace the other during shading.
 */
case class GpuWriteIntoDelta43x(
    override val gpuDeltaLog: GpuDeltaLog,
    override val cpuWrite: WriteIntoDelta)
  extends GpuWriteIntoDeltaBase(gpuDeltaLog, cpuWrite)
    with GpuWriteIntoDeltaLike {

  override protected def getFilesToRemoveForOverwrite(
      txn: org.apache.spark.sql.delta.OptimisticTransaction,
      addFiles: Seq[org.apache.spark.sql.delta.actions.AddFile],
      useDynamicPartitionOverwriteMode: Boolean):
      Seq[org.apache.spark.sql.delta.actions.Action] = {
    if (!useDynamicPartitionOverwriteMode &&
        cpuWrite.options.useNullIntolerantEqualityWithDPO.isDefined) {
      throw org.apache.spark.sql.delta.DeltaErrors.illegalDeltaOptionException(
        name = org.apache.spark.sql.delta.DeltaOptions.USE_NULL_INTOLERANT_EQUALITY_WITH_DPO,
        input = cpuWrite.options.useNullIntolerantEqualityWithDPO.get.toString,
        explain = "This option should be specified only in Dynamic Partition Overwrite mode.")
    }

    if (useDynamicPartitionOverwriteMode) {
      val filesToFilter =
        if (cpuWrite.options.useNullIntolerantEqualityWithDPO.contains(true)) {
          addFiles.filter(_.partitionValues.values.forall(_ != null))
        } else {
          addFiles
        }
      txn.filterFiles(filesToFilter).map(_.remove)
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
    val shouldRecordReplaceWhereOpMetrics =
      replaceWhere.nonEmpty && replaceOnDataColsEnabled &&
        sparkSession.conf.get(
          org.apache.spark.sql.delta.sources.DeltaSQLConf.REPLACEWHERE_METRICS_ENABLED)
    val shouldRecordInsertReplaceOpMetrics =
      shouldRecordReplaceWhereOpMetrics || cpuWrite.options.isReplaceOnOrUsingDefined
    if (shouldRecordInsertReplaceOpMetrics) {
      registerInsertReplaceMetrics(sparkSession, txn, newFiles, deletedFiles)
    } else if (cpuWrite.mode == org.apache.spark.sql.SaveMode.Overwrite &&
        sparkSession.conf.get(
          org.apache.spark.sql.delta.sources.DeltaSQLConf.OVERWRITE_REMOVE_METRICS_ENABLED)) {
      registerOverwriteRemoveMetrics(sparkSession, txn, deletedFiles)
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

  override def withNewWriterConfiguration(
      updatedConfiguration: Map[String, String]): GpuWriteIntoDeltaLike = {
    copyWithCpuWrite(cpuWrite.copy(configuration = updatedConfiguration))
  }

  override protected def copyWithCpuWrite(newCpuWrite: WriteIntoDelta): GpuWriteIntoDelta43x = {
    copy(cpuWrite = newCpuWrite)
  }
}
